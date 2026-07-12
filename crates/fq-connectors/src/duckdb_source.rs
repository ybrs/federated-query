//! The DuckDB connector (catalog/statistics tier). Ports the metadata/stats/
//! estimate surface of `datasources/duckdb.py`.
//!
//! The data-plane `execute_query`/`get_query_schema` (Arrow) are deliberately not
//! here - the native Rust fetch path lives with fq-exec.

use std::sync::Mutex;

use duckdb::Connection;
use fq_catalog::{
    build_table_statistics, metadata_from_information_schema, CatalogError, DataSource,
    DataSourceCapability, RenderDialect, TableMetadata, TableStatistics,
};

/// A DuckDB data source. Holds the connection behind a `Mutex` because
/// `duckdb::Connection` is `Send` but not `Sync`, while `DataSource` is
/// `Send + Sync`; metadata/stats reads are infrequent (optimize time), so the
/// lock is never contended.
pub struct DuckDbSource {
    name: String,
    connection: Mutex<Connection>,
}

impl DuckDbSource {
    /// Open a DuckDB database file as a named source.
    pub fn open(name: impl Into<String>, path: &str) -> Result<Self, CatalogError> {
        let connection = Connection::open(path).map_err(|error| to_source_err(&error))?;
        Ok(Self {
            name: name.into(),
            connection: Mutex::new(connection),
        })
    }

    /// Clone this source's DuckDB connection. duckdb `try_clone` opens a new
    /// connection onto the SAME underlying database instance (no second file
    /// open, no new lock), so the exec data plane can share the ONE read-write
    /// instance the runtime opened here instead of opening the file again.
    pub fn clone_connection(&self) -> Result<Connection, CatalogError> {
        self.connection
            .lock()
            .unwrap()
            .try_clone()
            .map_err(|error| to_source_err(&error))
    }

    /// Open an in-memory DuckDB database (used by tests).
    pub fn open_in_memory(name: impl Into<String>) -> Result<Self, CatalogError> {
        let connection = Connection::open_in_memory().map_err(|error| to_source_err(&error))?;
        Ok(Self {
            name: name.into(),
            connection: Mutex::new(connection),
        })
    }

    /// Run a statement for its side effects (test setup helper).
    pub fn execute_batch(&self, sql: &str) -> Result<(), CatalogError> {
        self.connection
            .lock()
            .unwrap()
            .execute_batch(sql)
            .map_err(|error| to_source_err(&error))
    }

    /// Query a single column of strings via a prepared statement + params.
    fn query_strings(&self, sql: &str, params: &[&str]) -> Result<Vec<String>, CatalogError> {
        let guard = self.connection.lock().unwrap();
        let mut statement = guard.prepare(sql).map_err(|error| to_source_err(&error))?;
        let rows = statement
            .query_map(duckdb::params_from_iter(params), |row| {
                row.get::<_, String>(0)
            })
            .map_err(|error| to_source_err(&error))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|error| to_source_err(&error))?);
        }
        Ok(out)
    }
}

/// Map a duckdb driver error into the driver-agnostic catalog error.
fn to_source_err(error: &duckdb::Error) -> CatalogError {
    CatalogError::Source(error.to_string())
}

impl DataSource for DuckDbSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn render_dialect(&self) -> RenderDialect {
        RenderDialect::DuckDb
    }

    fn capabilities(&self) -> Vec<DataSourceCapability> {
        vec![
            DataSourceCapability::Aggregations,
            DataSourceCapability::Joins,
            DataSourceCapability::WindowFunctions,
            DataSourceCapability::Subqueries,
            DataSourceCapability::Cte,
            DataSourceCapability::Distinct,
            DataSourceCapability::Limit,
            DataSourceCapability::OrderBy,
            DataSourceCapability::ShipTarget,
        ]
    }

    fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
        self.query_strings("SELECT schema_name FROM information_schema.schemata", &[])
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<String>, CatalogError> {
        self.query_strings(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
            &[schema],
        )
    }

    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata, CatalogError> {
        let guard = self.connection.lock().unwrap();
        let mut statement = guard
            .prepare(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                 WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            )
            .map_err(|error| to_source_err(&error))?;
        let rows = statement
            .query_map(duckdb::params![schema, table], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|error| to_source_err(&error))?;
        let mut triples = Vec::new();
        for row in rows {
            triples.push(row.map_err(|error| to_source_err(&error))?);
        }
        Ok(metadata_from_information_schema(schema, table, &triples))
    }

    fn get_table_statistics(
        &self,
        schema: &str,
        table: &str,
        _columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        // METADATA ONLY - planning never scans data. The row count is a catalog
        // read (`duckdb_tables()`); per-column statistics (NDV, nulls, min/max)
        // are NOT measured here: DuckDB keeps no NDV in its catalog and any
        // aggregate over the table would be a plan-time data scan, O(data) per
        // plan. Column values come from the learned overlay (measured by real
        // executions) and filtered-row estimates from `estimate_scan_rows`
        // (the source planner's EXPLAIN); absent values are honest unknowns.
        let row_count = self.catalog_row_count(schema, table)?;
        Ok(Some(build_table_statistics(
            Some(row_count),
            std::collections::BTreeMap::new(),
        )))
    }

    /// DuckDB's planner estimate for a rendered scan, parsed from EXPLAIN's
    /// physical plan (its root operator's "~N rows" line) - never executes the
    /// scan. DuckDB keeps statistics on every native table, so unlike Postgres
    /// there is no statless case to refuse.
    fn estimate_scan_rows(
        &self,
        _schema: &str,
        _table: &str,
        sql: &str,
    ) -> Result<Option<i64>, CatalogError> {
        let guard = self.connection.lock().unwrap();
        let Ok(mut statement) = guard.prepare(&format!("EXPLAIN {sql}")) else {
            // A capability probe, not error hiding: a predicate shape this
            // rendering cannot express for DuckDB fails EXPLAIN here while the
            // query itself still executes via its normal path - the estimate is
            // then honestly absent, never fabricated.
            return Ok(None);
        };
        let Ok(rows) = statement.query_map([], |row| row.get::<_, String>(1)) else {
            return Ok(None);
        };
        for text in rows.flatten() {
            if let Some(estimate) = parse_explain_rows(&text) {
                return Ok(Some(estimate));
            }
        }
        Ok(None)
    }
}

impl DuckDbSource {
    /// The table's row count from `duckdb_tables()` - a catalog read, not a scan.
    /// Raises for a table DuckDB does not know (a guess would silently mis-cost
    /// every plan over it).
    fn catalog_row_count(&self, schema: &str, table: &str) -> Result<i64, CatalogError> {
        let guard = self.connection.lock().unwrap();
        let mut statement = guard
            .prepare(
                "SELECT estimated_size FROM duckdb_tables() \
                 WHERE schema_name = ? AND table_name = ?",
            )
            .map_err(|error| to_source_err(&error))?;
        let row_count = statement
            .query_row(duckdb::params![schema, table], |row| row.get::<_, i64>(0))
            .map_err(|_| {
                CatalogError::Source(format!(
                    "duckdb_tables() knows no table {schema}.{table} on {}",
                    self.name
                ))
            })?;
        Ok(row_count)
    }
}

/// Parse the first "~N rows" estimate out of one EXPLAIN box line, tolerating
/// thousands separators ("~6,001,215 rows").
fn parse_explain_rows(text: &str) -> Option<i64> {
    let position = text.find('~')?;
    let digits: String = text[position + 1..]
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == ',')
        .filter(char::is_ascii_digit)
        .collect();
    if digits.is_empty() {
        return None;
    }
    let after = &text[position + 1..];
    if !after.to_ascii_lowercase().contains("row") {
        return None;
    }
    digits.parse::<i64>().ok()
}
