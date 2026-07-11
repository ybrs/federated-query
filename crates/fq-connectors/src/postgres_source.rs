//! The PostgreSQL connector (catalog/statistics tier). Ports the metadata/stats/
//! probe/estimate surface of `datasources/postgresql.py`.
//!
//! The data-plane fetch (psycopg2/ADBC -> Arrow) is deliberately not here - the
//! native Rust fetch path lives with fq-exec. This tier uses the sync `postgres`
//! client: metadata is `information_schema`, statistics are `reltuples` +
//! `pg_stats` (a never-ANALYZEd table is PROBED instead), and `estimate_scan_rows`
//! is `EXPLAIN (FORMAT JSON)`.

use std::sync::Mutex;

use fq_catalog::{
    build_table_statistics, map_native_type_default, metadata_from_information_schema,
    CatalogError, ColumnStatistics, DataSource, DataSourceCapability, RenderDialect, StatValue,
    TableMetadata, TableStatistics,
};
use fq_common::DataType;
use postgres::{Client, NoTls};

/// A never-ANALYZEd table at or below this size probes with one EXACT aggregate
/// scan; above it, a TABLESAMPLE row-count estimate only. A probe BUDGET, not a
/// statistic.
const PROBE_EXACT_BYTES: i64 = 256 * 1024 * 1024;

/// Block-sample size the large-table row-count probe targets; the TABLESAMPLE
/// percent is derived from it and clamped to [0.01, 100].
const PROBE_SAMPLE_TARGET_BYTES: f64 = 32.0 * 1024.0 * 1024.0;

/// Whether statless-table probing is on. `FEDQ_STATS_PROBE=0` is the kill switch;
/// anything else (including unset) is on.
fn probe_enabled() -> bool {
    std::env::var("FEDQ_STATS_PROBE").map_or(true, |value| value != "0")
}

/// A PostgreSQL data source. The client is held behind a `Mutex` (the sync
/// `postgres::Client` needs `&mut self` per query and is not `Sync`, while
/// `DataSource` is `Send + Sync`); metadata/stats reads are optimize-time.
pub struct PostgresSource {
    name: String,
    /// The schemas this source exposes (config-provided; pg does not enumerate).
    schemas: Vec<String>,
    client: Mutex<Client>,
}

impl PostgresSource {
    /// Connect to Postgres over `conn_str` (libpq form) and expose `schemas`.
    pub fn connect(
        name: impl Into<String>,
        conn_str: &str,
        schemas: Vec<String>,
    ) -> Result<Self, CatalogError> {
        let client = Client::connect(conn_str, NoTls).map_err(|error| to_source_err(&error))?;
        Ok(Self {
            name: name.into(),
            schemas,
            client: Mutex::new(client),
        })
    }

    /// Run `f` with the locked client, mapping any driver error.
    fn with_client<T>(
        &self,
        f: impl FnOnce(&mut Client) -> Result<T, postgres::Error>,
    ) -> Result<T, CatalogError> {
        let mut guard = self.client.lock().unwrap();
        f(&mut guard).map_err(|error| to_source_err(&error))
    }

    /// Run a statement for its side effects (test setup helper).
    pub fn execute(&self, sql: &str) -> Result<(), CatalogError> {
        self.with_client(|client| client.batch_execute(sql))
    }
}

/// Map a postgres driver error into the driver-agnostic catalog error.
fn to_source_err(error: &postgres::Error) -> CatalogError {
    CatalogError::Source(error.to_string())
}

/// Parse a pg_stats bound / probed min-max text into an orderable `StatValue`:
/// integer, then float, else the raw text (an ISO date/timestamp stays orderable
/// as text; the cost model parses temporal from it when it lands).
fn parse_bound(text: &str) -> StatValue {
    if let Ok(number) = text.parse::<i64>() {
        return StatValue::Integer(number);
    }
    if let Ok(number) = text.parse::<f64>() {
        return StatValue::Float(number);
    }
    StatValue::Text(text.to_string())
}

impl DataSource for PostgresSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn render_dialect(&self) -> RenderDialect {
        RenderDialect::Postgres
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
        // Postgres exposes the configured schema list, not a discovered one.
        Ok(self.schemas.clone())
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<String>, CatalogError> {
        self.with_client(|client| {
            let rows = client.query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name",
                &[&schema],
            )?;
            Ok(rows.iter().map(|row| row.get::<_, String>(0)).collect())
        })
    }

    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata, CatalogError> {
        let triples = self.with_client(|client| {
            let rows = client.query(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
                &[&schema, &table],
            )?;
            Ok(rows
                .iter()
                .map(|row| {
                    (
                        row.get::<_, String>(0),
                        row.get::<_, String>(1),
                        row.get::<_, String>(2),
                    )
                })
                .collect::<Vec<_>>())
        })?;
        Ok(metadata_from_information_schema(schema, table, &triples))
    }

    fn get_table_statistics(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        let row_count = self.catalog_row_count(schema, table)?;
        if row_count.is_none() && probe_enabled() {
            return Ok(Some(self.probe_statistics(schema, table, columns)?));
        }
        let column_stats = self.pg_stats_columns(schema, table, columns, row_count)?;
        Ok(Some(build_table_statistics(row_count, column_stats)))
    }

    fn estimate_scan_rows(
        &self,
        schema: &str,
        table: &str,
        sql: &str,
    ) -> Result<Option<i64>, CatalogError> {
        // Answer ONLY for an ANALYZEd table: on a statless table pg's planner
        // relays its own default selectivities (the fabricated priors this engine
        // removed), so the statless case stays on the probe + honest bounds.
        if self.catalog_row_count(schema, table)?.is_none() {
            return Ok(None);
        }
        Ok(self.explain_rows(sql))
    }

    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        // Postgres uuid arrives as text on the fetch path, so the catalog maps it
        // to VARCHAR too (else the two paths would disagree on the same column).
        let normalized = type_str
            .to_uppercase()
            .split('(')
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        if normalized == "UUID" {
            return Ok(DataType::Varchar);
        }
        map_native_type_default(type_str)
    }
}

impl PostgresSource {
    /// `reltuples` of the schema-qualified table, or None when unknown (-1 =
    /// never analyzed, or a missing relation). The pg_namespace join is required
    /// so a same-named table in another schema is not returned by accident.
    fn catalog_row_count(&self, schema: &str, table: &str) -> Result<Option<i64>, CatalogError> {
        self.with_client(|client| {
            let row = client.query_opt(
                "SELECT c.reltuples::bigint AS row_count \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&schema, &table],
            )?;
            Ok(row
                .map(|row| row.get::<_, i64>(0))
                .filter(|count| *count >= 0))
        })
    }

    /// The pg planner's root row estimate for a rendered scan, via EXPLAIN
    /// (FORMAT JSON). A predicate shape pg cannot express fails EXPLAIN; the
    /// estimate is then honestly absent (autocommit means no aborted-transaction
    /// cleanup is needed, unlike the pooled Python path).
    fn explain_rows(&self, sql: &str) -> Option<i64> {
        let explain_sql = format!("EXPLAIN (FORMAT JSON) {sql}");
        let payload = self
            .with_client(|client| {
                let row = client.query_one(&explain_sql, &[])?;
                Ok(row.get::<_, serde_json::Value>(0))
            })
            .ok()?;
        payload.get(0)?.get("Plan")?.get("Plan Rows")?.as_i64()
    }

    /// pg_stats rows for exactly the requested columns; a column ANALYZE has never
    /// seen is simply absent (the estimator substitutes a named default).
    fn pg_stats_columns(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        row_count: Option<i64>,
    ) -> Result<std::collections::BTreeMap<String, ColumnStatistics>, CatalogError> {
        let mut column_stats = std::collections::BTreeMap::new();
        if columns.is_empty() {
            return Ok(column_stats);
        }
        let rows = self.with_client(|client| {
            client.query(
                "SELECT attname, n_distinct, null_frac, avg_width, \
                 histogram_bounds::text::text[] AS bounds \
                 FROM pg_stats WHERE schemaname = $1 AND tablename = $2 AND attname = ANY($3)",
                &[&schema, &table, &columns],
            )
        })?;
        for row in &rows {
            let attname: String = row.get("attname");
            column_stats.insert(attname, decode_pg_stats_row(row, row_count));
        }
        Ok(column_stats)
    }
}

/// Decode one pg_stats row into engine column statistics.
fn decode_pg_stats_row(row: &postgres::Row, row_count: Option<i64>) -> ColumnStatistics {
    let n_distinct: f32 = row.get("n_distinct");
    let null_frac: f32 = row.get("null_frac");
    let avg_width: i32 = row.get("avg_width");
    let bounds: Option<Vec<String>> = row.get("bounds");
    let (min_value, max_value) = histogram_ends(bounds.as_deref());
    ColumnStatistics {
        num_distinct: decode_n_distinct(f64::from(n_distinct), row_count),
        null_fraction: f64::from(null_frac),
        avg_width: i64::from(avg_width),
        min_value,
        max_value,
    }
}

/// Decode pg_stats.n_distinct: >= 0 is an absolute count; < 0 is a fraction of
/// rows (unknown without a known row count - multiplying by a guess fabricates).
fn decode_n_distinct(n_distinct: f64, row_count: Option<i64>) -> Option<i64> {
    if n_distinct >= 0.0 {
        return Some(n_distinct as i64);
    }
    row_count.map(|count| (n_distinct.abs() * count as f64) as i64)
}

/// The first and last histogram bound parsed to orderable values, or (None, None)
/// when the column carries no histogram.
fn histogram_ends(bounds: Option<&[String]>) -> (Option<StatValue>, Option<StatValue>) {
    match bounds {
        Some(bounds) if !bounds.is_empty() => (
            Some(parse_bound(&bounds[0])),
            Some(parse_bound(&bounds[bounds.len() - 1])),
        ),
        _ => (None, None),
    }
}

impl PostgresSource {
    /// Measured statistics for a never-ANALYZEd table: an exact aggregate scan at
    /// or below the probe budget, else a TABLESAMPLE row-count estimate only.
    fn probe_statistics(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<TableStatistics, CatalogError> {
        let Some(size) = self.relation_size(schema, table)? else {
            // The relation does not exist here; report honest unknowns (the
            // binder rejects a truly missing table).
            return Ok(build_table_statistics(
                None,
                std::collections::BTreeMap::new(),
            ));
        };
        if size <= PROBE_EXACT_BYTES {
            self.probe_exact(schema, table, columns, size)
        } else {
            self.probe_sampled(schema, table, size)
        }
    }

    /// The relation's main-fork size in bytes (real file size, valid even when
    /// relpages is 0), or None for a missing relation.
    fn relation_size(&self, schema: &str, table: &str) -> Result<Option<i64>, CatalogError> {
        self.with_client(|client| {
            let row = client.query_opt(
                "SELECT pg_relation_size(c.oid) AS size \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&schema, &table],
            )?;
            Ok(row.map(|row| row.get::<_, i64>(0)))
        })
    }

    /// One exact aggregate scan over a small statless table: the row count and,
    /// per requested column, distinct/non-null counts and min/max (as text).
    fn probe_exact(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        size: i64,
    ) -> Result<TableStatistics, CatalogError> {
        let sql = probe_exact_sql(schema, table, columns);
        let row = self.with_client(|client| client.query_one(&sql, &[]))?;
        let rows: i64 = row.get("probe_rows");
        let mut stats = std::collections::BTreeMap::new();
        for (index, column) in columns.iter().enumerate() {
            stats.insert(column.clone(), unpack_probe_column(&row, index, rows));
        }
        Ok(TableStatistics {
            row_count: Some(rows),
            total_size_bytes: size,
            column_stats: stats,
        })
    }

    /// A TABLESAMPLE SYSTEM row-count estimate for a large statless table; column
    /// stats stay unknown (a sampled NDV is biased low and would fabricate).
    fn probe_sampled(
        &self,
        schema: &str,
        table: &str,
        size: i64,
    ) -> Result<TableStatistics, CatalogError> {
        let percent = (PROBE_SAMPLE_TARGET_BYTES / size as f64 * 100.0).clamp(0.01, 100.0);
        let sql = format!(
            "SELECT count(*) AS probe_rows FROM \"{schema}\".\"{table}\" \
             TABLESAMPLE SYSTEM ({percent})"
        );
        let sampled: i64 = self.with_client(|client| {
            let row = client.query_one(&sql, &[])?;
            Ok(row.get::<_, i64>("probe_rows"))
        })?;
        if sampled == 0 {
            return Ok(build_table_statistics(
                None,
                std::collections::BTreeMap::new(),
            ));
        }
        Ok(TableStatistics {
            row_count: Some((sampled as f64 * 100.0 / percent) as i64),
            total_size_bytes: size,
            column_stats: std::collections::BTreeMap::new(),
        })
    }
}

/// The single-scan probe query: count(*) plus four text-cast measures per
/// requested column, aliased by position for the unpack.
fn probe_exact_sql(schema: &str, table: &str, columns: &[String]) -> String {
    let mut parts = vec!["count(*) AS probe_rows".to_string()];
    for (index, column) in columns.iter().enumerate() {
        let quoted = format!("\"{}\"", column.replace('"', "\"\""));
        parts.push(format!("count(DISTINCT {quoted}) AS ndv_{index}"));
        parts.push(format!("count({quoted}) AS nonnull_{index}"));
        parts.push(format!("min({quoted})::text AS min_{index}"));
        parts.push(format!("max({quoted})::text AS max_{index}"));
    }
    format!("SELECT {} FROM \"{schema}\".\"{table}\"", parts.join(", "))
}

/// Per-column statistics from one probe row (exact NDV, null fraction, min/max).
fn unpack_probe_column(row: &postgres::Row, index: usize, rows: i64) -> ColumnStatistics {
    let ndv: i64 = row.get(format!("ndv_{index}").as_str());
    let non_null: i64 = row.get(format!("nonnull_{index}").as_str());
    let min_value: Option<String> = row.get(format!("min_{index}").as_str());
    let max_value: Option<String> = row.get(format!("max_{index}").as_str());
    let null_fraction = if rows > 0 {
        (rows - non_null) as f64 / rows as f64
    } else {
        0.0
    };
    ColumnStatistics {
        num_distinct: Some(ndv),
        null_fraction,
        avg_width: 10,
        min_value: min_value.as_deref().map(parse_bound),
        max_value: max_value.as_deref().map(parse_bound),
    }
}
