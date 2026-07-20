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

/// Map a postgres driver error into the driver-agnostic catalog error. The
/// driver's own `Display` is only the generic "db error"; the server's real
/// message and SQLSTATE live on the attached `DbError`, so surface them when
/// present rather than discarding the actual cause.
fn to_source_err(error: &postgres::Error) -> CatalogError {
    if let Some(db_error) = error.as_db_error() {
        return CatalogError::Source(format!(
            "{} (SQLSTATE {})",
            db_error.message(),
            db_error.code().code()
        ));
    }
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

    /// The version token is `relfilenode` plus the cumulative tuple-counter
    /// sum from `pg_stat_all_tables`. relfilenode alone catches rewrites
    /// (TRUNCATE / VACUUM FULL / CLUSTER) but misses ordinary DML, so the
    /// counter sum (`n_tup_ins + n_tup_upd + n_tup_del + n_dead_tup`) is
    /// required; ANY difference - including a counter reset that DECREASES the
    /// sum - compares unequal, which makes the refresh re-pull, the safe
    /// direction. The cumulative-statistics flush is asynchronous, so a token
    /// read racing a just-committed write may still show the old sum; that
    /// refresh skips and the NEXT one (after the flush) sees the change - a
    /// bounded staleness the operator accepts by relying on the skip. A table
    /// pg does not know returns None (the refresh then pulls and the pull
    /// itself raises loudly).
    fn source_token(&self, schema: &str, table: &str) -> Result<Option<String>, CatalogError> {
        self.with_client(|client| {
            let row = client.query_opt(
                "SELECT c.relfilenode::bigint, \
                        coalesce(s.n_tup_ins, 0) + coalesce(s.n_tup_upd, 0) + \
                        coalesce(s.n_tup_del, 0) + coalesce(s.n_dead_tup, 0) \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&schema, &table],
            )?;
            Ok(row.map(|row| {
                format!(
                    "pg:relfilenode={},tup={}",
                    row.get::<_, i64>(0),
                    row.get::<_, i64>(1)
                )
            }))
        })
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
        let column_types = self.probe_column_types(schema, table, columns)?;
        let sql = probe_exact_sql(schema, table, columns, &column_types);
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

    /// The Postgres internal type name (`udt_name`) of each requested column, in
    /// the same order as `columns`. This drives the type-aware probe measures.
    /// A requested column absent from the catalog is a caller inconsistency and
    /// raises rather than guessing a type.
    fn probe_column_types(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<Vec<String>, CatalogError> {
        let by_name: std::collections::BTreeMap<String, String> = self.with_client(|client| {
            let rows = client.query(
                "SELECT column_name, udt_name FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&schema, &table],
            )?;
            Ok(rows
                .iter()
                .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                .collect())
        })?;
        let mut types = Vec::with_capacity(columns.len());
        for column in columns {
            let udt = by_name.get(column).ok_or_else(|| {
                CatalogError::Source(format!(
                    "probe column \"{column}\" not found in {schema}.{table}"
                ))
            })?;
            types.push(udt.clone());
        }
        Ok(types)
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

/// The single-scan probe query: count(*) plus four measures per requested
/// column, aliased by position for the unpack. The measures are type-aware: a
/// non-null count is emitted for every type, but a distinct count is emitted
/// only for types Postgres can compare for equality, and min/max only for types
/// it can order. A type lacking either capability gets a NULL placeholder in the
/// same position so the positional unpack stays aligned and the missing measure
/// reads back as an honest unknown - Postgres has no min/max for booleans, byte
/// and bit strings, uuid, and the JSON/XML/geometric types, and no equality at
/// all for the JSON/XML/geometric types, so an unconditional aggregate on such a
/// column would fail the whole probe query.
fn probe_exact_sql(
    schema: &str,
    table: &str,
    columns: &[String],
    column_types: &[String],
) -> String {
    let mut parts = vec!["count(*) AS probe_rows".to_string()];
    for (index, column) in columns.iter().enumerate() {
        let quoted = format!("\"{}\"", column.replace('"', "\"\""));
        let udt = column_types[index].as_str();
        let ndv = if pg_type_has_equality(udt) {
            format!("count(DISTINCT {quoted})")
        } else {
            "NULL::bigint".to_string()
        };
        parts.push(format!("{ndv} AS ndv_{index}"));
        parts.push(format!("count({quoted}) AS nonnull_{index}"));
        let (min_measure, max_measure) = if pg_type_has_min_max(udt) {
            (
                format!("min({quoted})::text"),
                format!("max({quoted})::text"),
            )
        } else {
            ("NULL::text".to_string(), "NULL::text".to_string())
        };
        parts.push(format!("{min_measure} AS min_{index}"));
        parts.push(format!("{max_measure} AS max_{index}"));
    }
    format!("SELECT {} FROM \"{schema}\".\"{table}\"", parts.join(", "))
}

/// Whether Postgres has a `min`/`max` aggregate for a type (by `udt_name`).
/// This is an allowlist of the ordered scalar families - integer, floating and
/// exact numeric, money, character strings, the date/time types, and the
/// network address types. Every other type (booleans, byte/bit strings, uuid,
/// JSON/XML/geometric, and any type not listed) yields no min/max, so the probe
/// leaves its bounds unknown rather than emitting an aggregate Postgres rejects.
fn pg_type_has_min_max(udt_name: &str) -> bool {
    matches!(
        udt_name,
        "int2"
            | "int4"
            | "int8"
            | "float4"
            | "float8"
            | "numeric"
            | "money"
            | "text"
            | "varchar"
            | "bpchar"
            | "name"
            | "date"
            | "time"
            | "timetz"
            | "timestamp"
            | "timestamptz"
            | "interval"
            | "inet"
            | "cidr"
    )
}

/// Whether Postgres can compare a type for equality, which `count(DISTINCT ...)`
/// requires. The JSON document type, XML, and the geometric types carry no
/// default equality operator; every other type does, so distinct counting stays
/// on for them (including booleans, uuid, and jsonb, which order poorly or not
/// at all yet still hash for DISTINCT).
fn pg_type_has_equality(udt_name: &str) -> bool {
    !matches!(
        udt_name,
        "json" | "xml" | "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle"
    )
}

/// Per-column statistics from one probe row: null fraction (always), plus the
/// exact NDV and min/max where the column's type supports those measures (a NULL
/// placeholder reads back as an unknown for a type that does not).
fn unpack_probe_column(row: &postgres::Row, index: usize, rows: i64) -> ColumnStatistics {
    let num_distinct: Option<i64> = row.get(format!("ndv_{index}").as_str());
    let non_null: i64 = row.get(format!("nonnull_{index}").as_str());
    let min_value: Option<String> = row.get(format!("min_{index}").as_str());
    let max_value: Option<String> = row.get(format!("max_{index}").as_str());
    let null_fraction = if rows > 0 {
        (rows - non_null) as f64 / rows as f64
    } else {
        0.0
    };
    ColumnStatistics {
        num_distinct,
        null_fraction,
        avg_width: 10,
        min_value: min_value.as_deref().map(parse_bound),
        max_value: max_value.as_deref().map(parse_bound),
    }
}

#[cfg(test)]
mod tests {
    use super::{pg_type_has_equality, pg_type_has_min_max, probe_exact_sql};

    #[test]
    fn probe_sql_omits_min_max_for_unorderable_types_keeps_orderable() {
        let columns = vec!["flag".to_string(), "doc".to_string(), "amount".to_string()];
        let types = vec![
            "bool".to_string(),
            "json".to_string(),
            "numeric".to_string(),
        ];
        let sql = probe_exact_sql("public", "t", &columns, &types);
        // The orderable neighbour keeps real min/max and a distinct count.
        assert!(sql.contains("min(\"amount\")::text AS min_2"));
        assert!(sql.contains("max(\"amount\")::text AS max_2"));
        assert!(sql.contains("count(DISTINCT \"amount\") AS ndv_2"));
        // Boolean has no min/max but is still distinct-countable.
        assert!(!sql.contains("min(\"flag\")"));
        assert!(!sql.contains("max(\"flag\")"));
        assert!(sql.contains("NULL::text AS min_0"));
        assert!(sql.contains("NULL::text AS max_0"));
        assert!(sql.contains("count(DISTINCT \"flag\") AS ndv_0"));
        // json has neither min/max nor equality, so both are NULL placeholders.
        assert!(!sql.contains("min(\"doc\")"));
        assert!(!sql.contains("count(DISTINCT \"doc\")"));
        assert!(sql.contains("NULL::bigint AS ndv_1"));
        assert!(sql.contains("NULL::text AS min_1"));
        // A non-null count is kept for every column regardless of type.
        assert!(sql.contains("count(\"flag\") AS nonnull_0"));
        assert!(sql.contains("count(\"doc\") AS nonnull_1"));
        assert!(sql.contains("count(\"amount\") AS nonnull_2"));
    }

    #[test]
    fn type_capability_predicates_match_postgres() {
        assert!(pg_type_has_min_max("int4"));
        assert!(pg_type_has_min_max("timestamp"));
        assert!(!pg_type_has_min_max("bool"));
        assert!(!pg_type_has_min_max("uuid"));
        assert!(!pg_type_has_min_max("jsonb"));
        assert!(pg_type_has_equality("bool"));
        assert!(pg_type_has_equality("jsonb"));
        assert!(!pg_type_has_equality("json"));
        assert!(!pg_type_has_equality("point"));
    }
}
