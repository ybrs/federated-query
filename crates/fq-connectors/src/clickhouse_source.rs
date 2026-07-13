//! The ClickHouse connector (catalog/statistics tier).
//!
//! ClickHouse speaks over its HTTP interface; this tier drives it with the sync
//! `ureq` client (like the Postgres tier's sync `postgres` client), so fq-catalog
//! stays free of an async runtime. Metadata is `system.columns`, the row count is
//! `system.tables.total_rows`, and `estimate_scan_rows` parses `EXPLAIN ESTIMATE`.
//! Per-column distinct/null statistics are not offered cheaply by ClickHouse, so
//! this tier reports the row count only and leaves column stats absent (the
//! estimator substitutes named defaults) rather than fabricate them.
//!
//! The data-plane fetch (HTTP `FORMAT ArrowStream` -> Arrow) lives with fq-exec.

use fq_catalog::{
    build_table_statistics, CatalogError, ColumnMetadata, DataSource, DataSourceCapability,
    RenderDialect, TableMetadata, TableStatistics,
};
use fq_common::DataType;

/// A ClickHouse data source reached over the HTTP interface. The `ureq` agent
/// pools connections and is `Send + Sync`, so no interior mutability is needed
/// (unlike the sync Postgres client).
pub struct ClickHouseSource {
    name: String,
    /// The databases (ClickHouse's schemas) this source exposes.
    schemas: Vec<String>,
    /// The HTTP endpoint, e.g. `http://127.0.0.1:8123/`.
    base_url: String,
    /// Optional `(user, password)` sent as ClickHouse auth headers.
    auth: Option<(String, String)>,
    agent: ureq::Agent,
}

impl ClickHouseSource {
    /// Connect to ClickHouse at `base_url` and expose `schemas` (databases),
    /// verifying reachability with a `SELECT 1`.
    pub fn connect(
        name: impl Into<String>,
        base_url: impl Into<String>,
        user: Option<String>,
        password: Option<String>,
        schemas: Vec<String>,
    ) -> Result<Self, CatalogError> {
        let auth = match (user, password) {
            (Some(user), password) => Some((user, password.unwrap_or_default())),
            (None, _) => None,
        };
        let source = Self {
            name: name.into(),
            schemas,
            base_url: normalize_base_url(base_url.into()),
            auth,
            agent: ureq::Agent::new_with_defaults(),
        };
        // Fail loud now if the server is unreachable, rather than at first read.
        source.query_tsv("SELECT 1")?;
        Ok(source)
    }

    /// Run a statement for its side effects (test setup helper); the response
    /// body is discarded.
    pub fn execute(&self, sql: &str) -> Result<(), CatalogError> {
        self.query_tsv(sql)?;
        Ok(())
    }

    /// Run `sql` over HTTP and return the response body as text. Any non-success
    /// HTTP status carries ClickHouse's error text, which propagates as a source
    /// error rather than being swallowed.
    fn query_tsv(&self, sql: &str) -> Result<String, CatalogError> {
        let mut request = self.agent.post(&self.base_url);
        if let Some((user, password)) = &self.auth {
            request = request
                .header("X-ClickHouse-User", user)
                .header("X-ClickHouse-Key", password);
        }
        let mut response = request
            .send(sql)
            .map_err(|error| CatalogError::Source(format!("clickhouse http: {error}")))?;
        response
            .body_mut()
            .read_to_string()
            .map_err(|error| CatalogError::Source(format!("clickhouse read: {error}")))
    }
}

/// Ensure the base URL ends in a single `/` so query POSTs hit the root handler.
fn normalize_base_url(mut url: String) -> String {
    if !url.ends_with('/') {
        url.push('/');
    }
    url
}

/// Escape a single-quoted SQL string literal for a system-table filter.
fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

impl DataSource for ClickHouseSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn render_dialect(&self) -> RenderDialect {
        RenderDialect::ClickHouse
    }

    fn capabilities(&self) -> Vec<DataSourceCapability> {
        // ClickHouse pushes aggregates/joins/windows/CTEs; it is NOT a ship
        // target (the engine ships temp tables only into DuckDB and Postgres).
        vec![
            DataSourceCapability::Aggregations,
            DataSourceCapability::Joins,
            DataSourceCapability::WindowFunctions,
            DataSourceCapability::Subqueries,
            DataSourceCapability::Cte,
            DataSourceCapability::Distinct,
            DataSourceCapability::Limit,
            DataSourceCapability::OrderBy,
        ]
    }

    fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
        Ok(self.schemas.clone())
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<String>, CatalogError> {
        let sql = format!(
            "SELECT name FROM system.tables WHERE database = '{}' ORDER BY name FORMAT TabSeparated",
            escape_literal(schema)
        );
        let body = self.query_tsv(&sql)?;
        Ok(non_empty_lines(&body))
    }

    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata, CatalogError> {
        let sql = format!(
            "SELECT name, type FROM system.columns \
             WHERE database = '{}' AND table = '{}' ORDER BY position FORMAT TabSeparated",
            escape_literal(schema),
            escape_literal(table)
        );
        let body = self.query_tsv(&sql)?;
        Ok(metadata_from_system_columns(schema, table, &body))
    }

    fn get_table_statistics(
        &self,
        schema: &str,
        table: &str,
        _columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        let sql = format!(
            "SELECT total_rows FROM system.tables \
             WHERE database = '{}' AND name = '{}' FORMAT TabSeparated",
            escape_literal(schema),
            escape_literal(table)
        );
        let body = self.query_tsv(&sql)?;
        let row_count = parse_total_rows(&body);
        // Column stats are left absent: ClickHouse exposes no cheap per-column
        // distinct/null statistic, and a fabricated one is worse than none.
        Ok(Some(build_table_statistics(
            row_count,
            std::collections::BTreeMap::new(),
        )))
    }

    fn estimate_scan_rows(
        &self,
        _schema: &str,
        _table: &str,
        sql: &str,
    ) -> Result<Option<i64>, CatalogError> {
        let explain = format!("EXPLAIN ESTIMATE {sql} FORMAT TabSeparated");
        let body = self.query_tsv(&explain)?;
        Ok(parse_explain_estimate(&body))
    }

    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        map_clickhouse_type(type_str)
    }
}

/// The non-empty, whitespace-trimmed lines of a TabSeparated single-column body.
fn non_empty_lines(body: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            names.push(trimmed.to_string());
        }
    }
    names
}

/// Build `TableMetadata` from a `system.columns` TabSeparated `name<TAB>type`
/// body. Nullability is read from a `Nullable(...)` wrapper on the type.
fn metadata_from_system_columns(schema: &str, table: &str, body: &str) -> TableMetadata {
    let mut columns = Vec::new();
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let name = parts.next().unwrap_or("").to_string();
        let ty = parts.next().unwrap_or("").to_string();
        let nullable = ty.trim_start().starts_with("Nullable(");
        columns.push(ColumnMetadata::new(name, ty, nullable));
    }
    TableMetadata {
        schema_name: schema.to_string(),
        table_name: table.to_string(),
        columns,
        row_count: None,
        size_bytes: None,
    }
}

/// Parse `system.tables.total_rows`: a non-negative integer is the count; an
/// empty/NULL cell (an engine that does not track row counts, e.g. a view) is an
/// honest unknown.
fn parse_total_rows(body: &str) -> Option<i64> {
    body.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .and_then(|line| line.parse::<i64>().ok())
        .filter(|count| *count >= 0)
}

/// Parse `EXPLAIN ESTIMATE` output: TabSeparated rows of
/// `database, table, parts, rows, marks`; the total estimate is the sum of the
/// `rows` column. Returns None if the shape is unexpected (an honest abstention).
fn parse_explain_estimate(body: &str) -> Option<i64> {
    let mut total: i64 = 0;
    let mut seen = false;
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let rows = fields.get(3)?.trim().parse::<i64>().ok()?;
        total += rows;
        seen = true;
    }
    seen.then_some(total)
}

/// Map a ClickHouse native type name to an engine `DataType`. `Nullable(T)` and
/// `LowCardinality(T)` are transparent wrappers unwrapped to their inner type; an
/// unmodeled type (Array/Tuple/Map/Nested/IPv4/...) RAISES rather than coerce to
/// a wrong type.
fn map_clickhouse_type(type_str: &str) -> Result<DataType, CatalogError> {
    let inner = unwrap_type_modifiers(type_str.trim());
    let base = inner.split('(').next().unwrap_or("").trim().to_uppercase();
    map_clickhouse_base(&base)
        .ok_or_else(|| CatalogError::UnsupportedColumnType(type_str.to_string()))
}

/// Strip the transparent `Nullable(...)` / `LowCardinality(...)` wrappers,
/// returning the inner type name they wrap.
fn unwrap_type_modifiers(type_str: &str) -> &str {
    for wrapper in ["Nullable(", "LowCardinality("] {
        if let Some(rest) = type_str.strip_prefix(wrapper) {
            if let Some(inner) = rest.strip_suffix(')') {
                return unwrap_type_modifiers(inner.trim());
            }
        }
    }
    type_str
}

/// Map a bare (wrapper-stripped, uppercased, arg-stripped) ClickHouse base type.
fn map_clickhouse_base(base: &str) -> Option<DataType> {
    if let Some(numeric) = map_clickhouse_numeric(base) {
        return Some(numeric);
    }
    if let Some(temporal) = map_clickhouse_temporal(base) {
        return Some(temporal);
    }
    map_clickhouse_textual(base)
}

/// Map the integer / floating / decimal ClickHouse types. 64-bit-and-wider
/// integers map to BigInt; 8/16/32-bit to Integer; Float32 to Float, Float64 to
/// Double; every Decimal width to Decimal.
fn map_clickhouse_numeric(base: &str) -> Option<DataType> {
    match base {
        "INT8" | "INT16" | "INT32" | "UINT8" | "UINT16" | "UINT32" => Some(DataType::Integer),
        "INT64" | "INT128" | "INT256" | "UINT64" | "UINT128" | "UINT256" => Some(DataType::BigInt),
        "FLOAT32" => Some(DataType::Float),
        "FLOAT64" => Some(DataType::Double),
        "DECIMAL" | "DECIMAL32" | "DECIMAL64" | "DECIMAL128" | "DECIMAL256" => {
            Some(DataType::Decimal)
        }
        _ => None,
    }
}

/// Map the ClickHouse date/time types.
fn map_clickhouse_temporal(base: &str) -> Option<DataType> {
    match base {
        "DATE" | "DATE32" => Some(DataType::Date),
        "DATETIME" | "DATETIME64" => Some(DataType::Timestamp),
        _ => None,
    }
}

/// Map the ClickHouse string / boolean / enum / uuid types (all string-valued to
/// the engine). Returns None for anything else so the caller raises.
fn map_clickhouse_textual(base: &str) -> Option<DataType> {
    match base {
        "STRING" | "FIXEDSTRING" | "UUID" | "ENUM8" | "ENUM16" | "ENUM" => Some(DataType::Varchar),
        "BOOL" | "BOOLEAN" => Some(DataType::Boolean),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_integer_widths_to_integer_and_bigint() {
        assert_eq!(map_clickhouse_type("Int32").unwrap(), DataType::Integer);
        assert_eq!(map_clickhouse_type("UInt8").unwrap(), DataType::Integer);
        assert_eq!(map_clickhouse_type("UInt32").unwrap(), DataType::Integer);
        assert_eq!(map_clickhouse_type("Int64").unwrap(), DataType::BigInt);
        assert_eq!(map_clickhouse_type("UInt64").unwrap(), DataType::BigInt);
        assert_eq!(map_clickhouse_type("Int128").unwrap(), DataType::BigInt);
    }

    #[test]
    fn maps_floats_and_decimals() {
        assert_eq!(map_clickhouse_type("Float32").unwrap(), DataType::Float);
        assert_eq!(map_clickhouse_type("Float64").unwrap(), DataType::Double);
        assert_eq!(
            map_clickhouse_type("Decimal(18, 4)").unwrap(),
            DataType::Decimal
        );
        assert_eq!(
            map_clickhouse_type("Decimal64(4)").unwrap(),
            DataType::Decimal
        );
    }

    #[test]
    fn maps_strings_temporals_and_booleans() {
        assert_eq!(map_clickhouse_type("String").unwrap(), DataType::Varchar);
        assert_eq!(
            map_clickhouse_type("FixedString(16)").unwrap(),
            DataType::Varchar
        );
        assert_eq!(map_clickhouse_type("UUID").unwrap(), DataType::Varchar);
        assert_eq!(map_clickhouse_type("Date").unwrap(), DataType::Date);
        assert_eq!(
            map_clickhouse_type("DateTime64(3)").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(map_clickhouse_type("Bool").unwrap(), DataType::Boolean);
        assert_eq!(
            map_clickhouse_type("Enum8('a' = 1)").unwrap(),
            DataType::Varchar
        );
    }

    #[test]
    fn unwraps_nullable_and_lowcardinality() {
        assert_eq!(
            map_clickhouse_type("Nullable(Int32)").unwrap(),
            DataType::Integer
        );
        assert_eq!(
            map_clickhouse_type("LowCardinality(String)").unwrap(),
            DataType::Varchar
        );
        assert_eq!(
            map_clickhouse_type("LowCardinality(Nullable(String))").unwrap(),
            DataType::Varchar
        );
    }

    #[test]
    fn unmodeled_type_raises_with_its_name() {
        // Array/Tuple/Map/IPv4 are not modeled; a wrong coercion is worse than a
        // loud failure.
        let error = map_clickhouse_type("Array(String)").unwrap_err();
        assert!(format!("{error}").contains("Array(String)"));
        assert!(map_clickhouse_type("IPv4").is_err());
        assert!(map_clickhouse_type("Tuple(Int32, String)").is_err());
    }

    #[test]
    fn parses_explain_estimate_rows_column() {
        // database, table, parts, rows, marks -> sum of the rows column.
        let body = "fedq_test\tpeople\t1\t3\t1\n";
        assert_eq!(parse_explain_estimate(body), Some(3));
        let multi = "d\tt\t1\t10\t2\nd\tt\t1\t5\t1\n";
        assert_eq!(parse_explain_estimate(multi), Some(15));
        assert_eq!(parse_explain_estimate(""), None);
    }

    #[test]
    fn parses_total_rows_and_metadata() {
        assert_eq!(parse_total_rows("3\n"), Some(3));
        assert_eq!(parse_total_rows("\n"), None);
        let meta = metadata_from_system_columns("db", "t", "id\tUInt32\nname\tNullable(String)\n");
        assert_eq!(meta.columns.len(), 2);
        assert_eq!(meta.columns[0].name, "id");
        assert!(!meta.columns[0].nullable);
        assert!(meta.columns[1].nullable);
    }
}
