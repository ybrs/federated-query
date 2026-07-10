//! The DuckDB connector (catalog/statistics tier). Ports the metadata/stats/
//! estimate surface of `datasources/duckdb.py`.
//!
//! The data-plane `execute_query`/`get_query_schema` (Arrow) are deliberately not
//! here - the native Rust fetch path lives with fq-exec.

use std::sync::Mutex;

use duckdb::types::Value;
use duckdb::Connection;
use fq_catalog::{
    build_column_statistics, build_table_statistics, metadata_from_information_schema,
    CatalogError, ColumnStatistics, DataSource, DataSourceCapability, RenderDialect, StatValue,
    TableMetadata, TableStatistics,
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

/// Decode a duckdb value into a `StatValue` for min/max stats; unsupported value
/// kinds (blobs, structs) yield None rather than a fabricated value.
fn stat_value(value: Value) -> Option<StatValue> {
    match value {
        Value::Boolean(boolean) => Some(StatValue::Boolean(boolean)),
        Value::TinyInt(number) => Some(StatValue::Integer(i64::from(number))),
        Value::SmallInt(number) => Some(StatValue::Integer(i64::from(number))),
        Value::Int(number) => Some(StatValue::Integer(i64::from(number))),
        Value::BigInt(number) => Some(StatValue::Integer(number)),
        Value::Float(number) => Some(StatValue::Float(f64::from(number))),
        Value::Double(number) => Some(StatValue::Float(number)),
        Value::Text(text) => Some(StatValue::Text(text)),
        _ => None,
    }
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
        columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        let row_count = self.catalog_row_count(schema, table)?;
        let column_stats = self.approx_column_statistics(schema, table, columns, row_count)?;
        Ok(Some(build_table_statistics(Some(row_count), column_stats)))
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

    /// Approximate NDV / null fraction / min / max for the requested columns, all
    /// gathered in one vectorized aggregate scan over the table.
    fn approx_column_statistics(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        row_count: i64,
    ) -> Result<std::collections::BTreeMap<String, ColumnStatistics>, CatalogError> {
        let mut stats = std::collections::BTreeMap::new();
        if columns.is_empty() {
            return Ok(stats);
        }
        let guard = self.connection.lock().unwrap();
        let sql = column_stats_query(schema, table, columns);
        let mut statement = guard.prepare(&sql).map_err(|error| to_source_err(&error))?;
        let values: Vec<Value> = statement
            .query_row([], |row| {
                let mut collected = Vec::with_capacity(columns.len() * 4);
                for index in 0..columns.len() * 4 {
                    collected.push(row.get::<_, Value>(index)?);
                }
                Ok(collected)
            })
            .map_err(|error| to_source_err(&error))?;
        for (index, column) in columns.iter().enumerate() {
            stats.insert(
                column.clone(),
                unpack_column_stat(&values[index * 4..index * 4 + 4], row_count),
            );
        }
        Ok(stats)
    }
}

/// Build the single aggregate scan: 4 measures per requested column
/// (approx distinct, non-null count, min, max).
fn column_stats_query(schema: &str, table: &str, columns: &[String]) -> String {
    let mut parts = Vec::with_capacity(columns.len() * 4);
    for column in columns {
        let quoted = format!("\"{column}\"");
        parts.push(format!("approx_count_distinct({quoted})"));
        parts.push(format!("count({quoted})"));
        parts.push(format!("min({quoted})"));
        parts.push(format!("max({quoted})"));
    }
    format!("SELECT {} FROM \"{schema}\".\"{table}\"", parts.join(", "))
}

/// Turn one column's four measures (ndv, non-null count, min, max) into stats.
fn unpack_column_stat(measures: &[Value], row_count: i64) -> ColumnStatistics {
    let num_distinct = match &measures[0] {
        Value::BigInt(number) => Some(*number),
        Value::Int(number) => Some(i64::from(*number)),
        _ => None,
    };
    let non_null = match &measures[1] {
        Value::BigInt(number) => *number,
        Value::Int(number) => i64::from(*number),
        _ => row_count,
    };
    let min_value = stat_value(measures[2].clone());
    let max_value = stat_value(measures[3].clone());
    build_column_statistics(
        num_distinct,
        row_count - non_null,
        row_count,
        min_value,
        max_value,
    )
}
