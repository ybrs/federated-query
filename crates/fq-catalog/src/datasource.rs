//! The datasource abstraction: the catalog/statistics tier of a connector.
//!
//! Ports the trait + value types + shared type-mapping of `datasources/base.py`.
//! This is the tier the catalog and cost model consume - metadata, statistics,
//! capabilities, native-type mapping - and it deliberately has NO Arrow types and
//! NO driver dependencies, so fq-catalog (and thus the binder) stays light. The
//! DATA-PLANE tier (Arrow fetch, ship, ctid-parallel) is a separate concern that
//! lives with fq-connectors / fq-exec.
//!
//! The concrete connectors (Postgres, DuckDB) live in fq-connectors and implement
//! `DataSource` here - dependency inversion, so the abstraction sits with its
//! consumer and fq-catalog needs no `duckdb`/`postgres` crate.

use fq_common::DataType;

use crate::error::CatalogError;

/// The SQL dialect a source renders pushed queries in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderDialect {
    Postgres,
    DuckDb,
    ClickHouse,
    MySql,
    /// The engine's materialized-view store, whose pushed SQL DataFusion
    /// executes over local Arrow IPC chunks.
    Materialized,
    /// A Parquet directory, whose pushed SQL DataFusion executes over the local
    /// files. Its rendering must match DataFusion's own semantics (in particular
    /// the DESC-default null placement), not another engine the query never runs
    /// on.
    Parquet,
}

/// Capabilities a data source may support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSourceCapability {
    Aggregations,
    Joins,
    WindowFunctions,
    Subqueries,
    Cte,
    Distinct,
    Limit,
    OrderBy,
    /// The source can receive a shipped relation as a session TEMP TABLE (dim
    /// shipping). Writable sources support it; read-only ones do not.
    ShipTarget,
}

/// A scalar min/max statistic value. Replaces Python's untyped `Any`; the cost
/// model reads these on a shared ordinal scale for range selectivity.
#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}

/// Metadata about a column (the source's own native type name, pre-mapping).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMetadata {
    pub name: String,
    /// The source's native type name (e.g. "integer", "character varying").
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub foreign_key: Option<String>,
}

impl ColumnMetadata {
    /// A plain (non-key) column descriptor with the given nullability.
    pub fn new(name: impl Into<String>, data_type: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            primary_key: false,
            foreign_key: None,
        }
    }
}

/// Metadata about a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnMetadata>,
    pub row_count: Option<i64>,
    pub size_bytes: Option<i64>,
}

/// Statistics about a column. `num_distinct` is None when the source cannot
/// provide it honestly (never fabricated - the estimator substitutes a named
/// default and records the provenance).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    pub num_distinct: Option<i64>,
    pub null_fraction: f64,
    /// Average size in bytes (a fixed placeholder here; sources expose no cheap
    /// width).
    pub avg_width: i64,
    pub min_value: Option<StatValue>,
    pub max_value: Option<StatValue>,
}

/// Statistics about a table. `row_count` is None when the source honestly does
/// not know it (e.g. Postgres reltuples = -1 before the first ANALYZE).
#[derive(Debug, Clone, PartialEq)]
pub struct TableStatistics {
    pub row_count: Option<i64>,
    pub total_size_bytes: i64,
    pub column_stats: std::collections::BTreeMap<String, ColumnStatistics>,
}

/// The catalog/statistics tier of a data source.
///
/// The concrete connectors (fq-connectors) implement this against real drivers.
/// Every method that touches the source returns `Result` so a driver failure
/// propagates loudly rather than being papered over.
pub trait DataSource: Send + Sync {
    /// The registered name of this data source.
    fn name(&self) -> &str;

    /// The dialect pushed queries render in for this source.
    fn render_dialect(&self) -> RenderDialect {
        RenderDialect::Postgres
    }

    /// The capabilities this source supports.
    fn capabilities(&self) -> Vec<DataSourceCapability>;

    /// Whether this source supports a capability.
    fn supports_capability(&self, capability: DataSourceCapability) -> bool {
        self.capabilities().contains(&capability)
    }

    /// List all available schemas.
    fn list_schemas(&self) -> Result<Vec<String>, CatalogError>;

    /// List the base tables in a schema.
    fn list_tables(&self, schema: &str) -> Result<Vec<String>, CatalogError>;

    /// Metadata (columns + native types) for a table.
    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata, CatalogError>;

    /// Catalog statistics for a table, lazily for the requested columns only.
    /// `columns` names exactly the columns the optimizer needs (join keys +
    /// filtered columns); empty requests the row count only. Unknown values are
    /// None, never fabricated. Returns None when the source has no statistics.
    fn get_table_statistics(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError>;

    /// The source planner's row estimate for a rendered single-table scan (its
    /// EXPLAIN output), or None when this source offers none. An honest
    /// abstention, never an error path.
    fn estimate_scan_rows(
        &self,
        _schema: &str,
        _table: &str,
        _sql: &str,
    ) -> Result<Option<i64>, CatalogError> {
        Ok(None)
    }

    /// Map this source's native column-type name to an engine `DataType`. The
    /// default covers the common SQL names; a source with extra native types
    /// (e.g. Postgres uuid) overrides and falls back here.
    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        map_native_type_default(type_str)
    }

    /// An opaque version token for one table, read ONLY by REFRESH (never on
    /// the query path): equal tokens mean the table has not changed since the
    /// token was captured, so a refresh skips its pull. A source that cannot
    /// answer cheaply returns None and the refresh pulls unconditionally - an
    /// absent token costs work, never a wrong answer. Token text is
    /// connector-private; the only defined operation on it is equality.
    fn source_token(&self, _schema: &str, _table: &str) -> Result<Option<String>, CatalogError> {
        Ok(None)
    }
}

/// The default native-type mapping (the base-class logic). Most specific first:
/// temporal (TIMESTAMP/DATETIME before DATE) then numeric (word-aware integer
/// match so POINT is not read as INT) then textual; an unmodeled type raises
/// rather than silently coercing to a string (a mistyped column is a wrong answer
/// with no error).
pub fn map_native_type_default(type_str: &str) -> Result<DataType, CatalogError> {
    let normalized = type_str
        .to_uppercase()
        .split('(')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    if let Some(temporal) = map_temporal_type(&normalized) {
        return Ok(temporal);
    }
    if let Some(numeric) = map_numeric_type(&normalized) {
        return Ok(numeric);
    }
    map_textual_type(&normalized)
}

/// Map date/time types, checking TIMESTAMP/DATETIME before DATE.
fn map_temporal_type(type_str: &str) -> Option<DataType> {
    if type_str.contains("TIMESTAMP") || type_str.contains("DATETIME") {
        return Some(DataType::Timestamp);
    }
    if type_str.contains("DATE") {
        return Some(DataType::Date);
    }
    if type_str.contains("TIME") {
        return Some(DataType::Timestamp);
    }
    None
}

/// Map numeric types; the integer match is word-aware (POINT contains INT).
fn map_numeric_type(type_str: &str) -> Option<DataType> {
    if type_str.contains("DOUBLE") || type_str.contains("NUMERIC") || type_str.contains("DECIMAL") {
        return Some(DataType::Double);
    }
    if type_str.contains("FLOAT") || type_str.contains("REAL") {
        return Some(DataType::Float);
    }
    if type_str.contains("BIGINT") || type_str.contains("INT8") || type_str.contains("BIGSERIAL") {
        return Some(DataType::BigInt);
    }
    if is_integer_type(type_str) {
        return Some(DataType::Integer);
    }
    None
}

/// Whether a type name denotes an integer (starts with INT, or a known synonym).
fn is_integer_type(type_str: &str) -> bool {
    type_str.starts_with("INT") || matches!(type_str, "SMALLINT" | "SERIAL" | "INTEGER")
}

/// Map boolean and string types; raise on a type the engine does not model.
fn map_textual_type(type_str: &str) -> Result<DataType, CatalogError> {
    if type_str.contains("BOOL") {
        return Ok(DataType::Boolean);
    }
    if type_str.contains("CHAR") || type_str.contains("TEXT") || type_str.contains("STRING") {
        return Ok(if type_str.contains("VAR") {
            DataType::Varchar
        } else {
            DataType::Text
        });
    }
    Err(CatalogError::UnsupportedColumnType(type_str.to_string()))
}

/// Build `TableMetadata` from information_schema `(name, data_type, is_nullable)`
/// rows; nullable follows the SQL `is_nullable = 'YES'` convention. Shared by
/// connectors that read information_schema verbatim.
pub fn metadata_from_information_schema(
    schema: &str,
    table: &str,
    rows: &[(String, String, String)],
) -> TableMetadata {
    let mut columns = Vec::with_capacity(rows.len());
    for (name, data_type, is_nullable) in rows {
        columns.push(ColumnMetadata::new(
            name.clone(),
            data_type.clone(),
            is_nullable == "YES",
        ));
    }
    TableMetadata {
        schema_name: schema.to_string(),
        table_name: table.to_string(),
        columns,
        row_count: None,
        size_bytes: None,
    }
}

/// Build `ColumnStatistics`, deriving the null fraction from a null count.
pub fn build_column_statistics(
    num_distinct: Option<i64>,
    null_count: i64,
    row_count: i64,
    min_value: Option<StatValue>,
    max_value: Option<StatValue>,
) -> ColumnStatistics {
    let null_fraction = if row_count > 0 {
        null_count as f64 / row_count as f64
    } else {
        0.0
    };
    ColumnStatistics {
        num_distinct,
        null_fraction,
        avg_width: 10,
        min_value,
        max_value,
    }
}

/// Wrap a row count and per-column stats into `TableStatistics`. Byte size is
/// approximated from the row count (0 when the count is unknown).
pub fn build_table_statistics(
    row_count: Option<i64>,
    column_stats: std::collections::BTreeMap<String, ColumnStatistics>,
) -> TableStatistics {
    let total_size_bytes = row_count.map_or(0, |count| count * 100);
    TableStatistics {
        row_count,
        total_size_bytes,
        column_stats,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_native_type_common_names() {
        // Integer family.
        assert_eq!(
            map_native_type_default("INTEGER").unwrap(),
            DataType::Integer
        );
        assert_eq!(map_native_type_default("BIGINT").unwrap(), DataType::BigInt);
        assert_eq!(
            map_native_type_default("SERIAL").unwrap(),
            DataType::Integer
        );
        assert_eq!(
            map_native_type_default("BIGSERIAL").unwrap(),
            DataType::BigInt
        );
        // Float family.
        assert_eq!(map_native_type_default("FLOAT").unwrap(), DataType::Float);
        assert_eq!(map_native_type_default("REAL").unwrap(), DataType::Float);
        assert_eq!(map_native_type_default("DOUBLE").unwrap(), DataType::Double);
        assert_eq!(
            map_native_type_default("NUMERIC").unwrap(),
            DataType::Double
        );
        assert_eq!(
            map_native_type_default("DECIMAL").unwrap(),
            DataType::Double
        );
        // String family.
        assert_eq!(
            map_native_type_default("VARCHAR").unwrap(),
            DataType::Varchar
        );
        assert_eq!(map_native_type_default("CHAR").unwrap(), DataType::Text);
        assert_eq!(map_native_type_default("TEXT").unwrap(), DataType::Text);
        // Boolean + temporal.
        assert_eq!(
            map_native_type_default("BOOLEAN").unwrap(),
            DataType::Boolean
        );
        assert_eq!(map_native_type_default("DATE").unwrap(), DataType::Date);
        assert_eq!(
            map_native_type_default("TIMESTAMP").unwrap(),
            DataType::Timestamp
        );
    }

    #[test]
    fn map_native_type_parameterized_name_strips_args() {
        // "DECIMAL(10, 2)" -> the base name DECIMAL.
        assert_eq!(
            map_native_type_default("DECIMAL(10, 2)").unwrap(),
            DataType::Double
        );
        assert_eq!(
            map_native_type_default("character varying(255)").unwrap(),
            DataType::Varchar
        );
    }

    #[test]
    fn unknown_type_raises_with_its_name() {
        let error = map_native_type_default("UNKNOWN_TYPE").unwrap_err();
        assert!(format!("{error}").contains("UNKNOWN_TYPE"));
    }
}
