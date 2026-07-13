//! The MySQL connector (catalog/statistics tier).
//!
//! Mirrors the Postgres tier: the sync `mysql` client held behind a `Mutex`
//! (a `mysql::Conn` needs `&mut self` per query and is not `Sync`, while
//! `DataSource` is `Send + Sync`). Metadata is `information_schema.columns`, the
//! row count is `information_schema.tables.TABLE_ROWS` (the engine's estimate),
//! and `estimate_scan_rows` parses `EXPLAIN FORMAT=JSON`. Per-column
//! distinct/null statistics are not offered cheaply, so this tier reports the row
//! count only (the estimator substitutes named defaults) rather than fabricate.
//!
//! The data-plane fetch (rows -> Arrow) lives with fq-exec.

use std::sync::Mutex;

use fq_catalog::{
    build_table_statistics, CatalogError, ColumnMetadata, DataSource, DataSourceCapability,
    RenderDialect, TableMetadata, TableStatistics,
};
use fq_common::DataType;
use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder};

/// A MySQL data source. The connection is held behind a `Mutex` (the sync client
/// is not `Sync`); metadata/stats reads are optimize-time.
pub struct MySqlSource {
    name: String,
    /// The schemas (databases) this source exposes.
    schemas: Vec<String>,
    conn: Mutex<Conn>,
}

impl MySqlSource {
    /// Connect to MySQL and expose `schemas`. `url` is a `mysql://` URL.
    pub fn connect(
        name: impl Into<String>,
        url: &str,
        schemas: Vec<String>,
    ) -> Result<Self, CatalogError> {
        let opts = mysql::Opts::from_url(url).map_err(|error| to_source_err(&error))?;
        let conn =
            Conn::new(OptsBuilder::from_opts(opts)).map_err(|error| to_source_err(&error))?;
        Ok(Self {
            name: name.into(),
            schemas,
            conn: Mutex::new(conn),
        })
    }

    /// Run `f` with the locked connection.
    fn with_conn<T>(
        &self,
        f: impl FnOnce(&mut Conn) -> Result<T, mysql::Error>,
    ) -> Result<T, CatalogError> {
        let mut guard = self.conn.lock().unwrap();
        f(&mut guard).map_err(|error| to_source_err(&error))
    }

    /// Run a statement for its side effects (test setup helper).
    pub fn execute(&self, sql: &str) -> Result<(), CatalogError> {
        self.with_conn(|conn| conn.query_drop(sql))
    }
}

/// Map a mysql driver error into the driver-agnostic catalog error.
fn to_source_err(error: &impl std::fmt::Display) -> CatalogError {
    CatalogError::Source(error.to_string())
}

impl DataSource for MySqlSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn render_dialect(&self) -> RenderDialect {
        RenderDialect::MySql
    }

    fn capabilities(&self) -> Vec<DataSourceCapability> {
        // MySQL 8 pushes aggregates/joins/windows/CTEs; it is not a ship target
        // (the engine ships temp tables only into DuckDB and Postgres).
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
        self.with_conn(|conn| {
            conn.exec_map(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
                (schema,),
                |name: String| name,
            )
        })
    }

    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata, CatalogError> {
        let triples = self.with_conn(|conn| {
            conn.exec_map(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                 WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                (schema, table),
                |(name, data_type, is_nullable): (String, String, String)| {
                    (name, data_type, is_nullable)
                },
            )
        })?;
        Ok(metadata_from_information_schema(schema, table, &triples))
    }

    fn get_table_statistics(
        &self,
        schema: &str,
        table: &str,
        _columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        let row_count = self.catalog_row_count(schema, table)?;
        // Column stats are left absent: MySQL exposes no cheap per-column
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
        let explain_sql = format!("EXPLAIN FORMAT=JSON {sql}");
        let json: Option<String> = self.with_conn(|conn| conn.query_first(&explain_sql))?;
        Ok(json.as_deref().and_then(parse_explain_rows))
    }

    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        map_mysql_type(type_str)
    }
}

impl MySqlSource {
    /// The engine's estimated row count for a table from
    /// `information_schema.tables.TABLE_ROWS`. For InnoDB this is an estimate,
    /// not exact - but it is the same class of cheap catalog value the Postgres
    /// tier reads from `reltuples`. None when the table is unknown.
    fn catalog_row_count(&self, schema: &str, table: &str) -> Result<Option<i64>, CatalogError> {
        let value: Option<Option<i64>> = self.with_conn(|conn| {
            conn.exec_first(
                "SELECT table_rows FROM information_schema.tables \
                 WHERE table_schema = ? AND table_name = ?",
                (schema, table),
            )
        })?;
        Ok(value.flatten().filter(|count| *count >= 0))
    }
}

/// Build `TableMetadata` from `information_schema` `(name, data_type, is_nullable)`
/// rows; nullable follows the SQL `is_nullable = 'YES'` convention.
fn metadata_from_information_schema(
    schema: &str,
    table: &str,
    rows: &[(String, String, String)],
) -> TableMetadata {
    let mut columns = Vec::with_capacity(rows.len());
    for (name, data_type, is_nullable) in rows {
        columns.push(ColumnMetadata::new(
            name.clone(),
            data_type.clone(),
            is_nullable.eq_ignore_ascii_case("YES"),
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

/// Parse the top-level cost estimate from `EXPLAIN FORMAT=JSON`: MySQL nests the
/// planner's estimate under `query_block.rows_produced_per_join` (a join) or the
/// table's `rows_examined_per_scan` (a single scan). Returns None if neither is
/// present (an honest abstention rather than an error).
fn parse_explain_rows(json: &str) -> Option<i64> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    let block = value.get("query_block")?;
    if let Some(rows) = block
        .get("rows_produced_per_join")
        .and_then(serde_json::Value::as_i64)
    {
        return Some(rows);
    }
    block
        .get("table")?
        .get("rows_examined_per_scan")
        .and_then(serde_json::Value::as_i64)
}

/// Map a MySQL `information_schema.data_type` (the base type name, no length) to
/// an engine `DataType`. An unmodeled type (blob/binary/bit/json/geometry/...)
/// RAISES rather than coerce to a wrong type.
fn map_mysql_type(type_str: &str) -> Result<DataType, CatalogError> {
    let base = type_str
        .split('(')
        .next()
        .unwrap_or("")
        .trim()
        .to_uppercase();
    map_mysql_base(&base).ok_or_else(|| CatalogError::UnsupportedColumnType(type_str.to_string()))
}

/// Map a bare (arg-stripped, uppercased) MySQL base type.
fn map_mysql_base(base: &str) -> Option<DataType> {
    if let Some(numeric) = map_mysql_numeric(base) {
        return Some(numeric);
    }
    if let Some(temporal) = map_mysql_temporal(base) {
        return Some(temporal);
    }
    map_mysql_textual(base)
}

/// Map the integer / fixed / floating MySQL numeric types. `year` is a 4-digit
/// integer; `bigint` is the only 64-bit integer.
fn map_mysql_numeric(base: &str) -> Option<DataType> {
    match base {
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "YEAR" => {
            Some(DataType::Integer)
        }
        "BIGINT" => Some(DataType::BigInt),
        "DECIMAL" | "NUMERIC" | "DEC" | "FIXED" => Some(DataType::Decimal),
        "FLOAT" => Some(DataType::Float),
        "DOUBLE" | "REAL" | "DOUBLE PRECISION" => Some(DataType::Double),
        _ => None,
    }
}

/// Map the MySQL date/time types. `time` carries no date, but the engine models
/// it as Timestamp (matching the shared default mapper).
fn map_mysql_temporal(base: &str) -> Option<DataType> {
    match base {
        "DATE" => Some(DataType::Date),
        "DATETIME" | "TIMESTAMP" | "TIME" => Some(DataType::Timestamp),
        _ => None,
    }
}

/// Map the MySQL string / boolean types. `char`/`varchar` are bounded strings;
/// the `text` family is unbounded; `enum`/`set` are string-valued.
fn map_mysql_textual(base: &str) -> Option<DataType> {
    match base {
        "CHAR" | "VARCHAR" | "ENUM" | "SET" => Some(DataType::Varchar),
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => Some(DataType::Text),
        "BOOL" | "BOOLEAN" => Some(DataType::Boolean),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_integer_families() {
        assert_eq!(map_mysql_type("int").unwrap(), DataType::Integer);
        assert_eq!(map_mysql_type("tinyint").unwrap(), DataType::Integer);
        assert_eq!(map_mysql_type("smallint").unwrap(), DataType::Integer);
        assert_eq!(map_mysql_type("mediumint").unwrap(), DataType::Integer);
        assert_eq!(map_mysql_type("year").unwrap(), DataType::Integer);
        assert_eq!(map_mysql_type("bigint").unwrap(), DataType::BigInt);
    }

    #[test]
    fn maps_decimal_float_double() {
        assert_eq!(map_mysql_type("decimal").unwrap(), DataType::Decimal);
        assert_eq!(map_mysql_type("float").unwrap(), DataType::Float);
        assert_eq!(map_mysql_type("double").unwrap(), DataType::Double);
        // information_schema.data_type omits the precision, but a full column
        // type is stripped to the base name anyway.
        assert_eq!(map_mysql_type("decimal(10,2)").unwrap(), DataType::Decimal);
    }

    #[test]
    fn maps_strings_temporals_and_booleans() {
        assert_eq!(map_mysql_type("varchar").unwrap(), DataType::Varchar);
        assert_eq!(map_mysql_type("char").unwrap(), DataType::Varchar);
        assert_eq!(map_mysql_type("enum").unwrap(), DataType::Varchar);
        assert_eq!(map_mysql_type("text").unwrap(), DataType::Text);
        assert_eq!(map_mysql_type("longtext").unwrap(), DataType::Text);
        assert_eq!(map_mysql_type("date").unwrap(), DataType::Date);
        assert_eq!(map_mysql_type("datetime").unwrap(), DataType::Timestamp);
        assert_eq!(map_mysql_type("timestamp").unwrap(), DataType::Timestamp);
        assert_eq!(map_mysql_type("boolean").unwrap(), DataType::Boolean);
    }

    #[test]
    fn unmodeled_type_raises_with_its_name() {
        // blob/binary/bit/json/geometry are not modeled; a wrong coercion is
        // worse than a loud failure.
        assert!(map_mysql_type("blob").is_err());
        assert!(map_mysql_type("varbinary").is_err());
        assert!(map_mysql_type("json").is_err());
        let error = map_mysql_type("geometry").unwrap_err();
        assert!(format!("{error}").contains("geometry"));
    }

    #[test]
    fn parses_explain_json_rows() {
        let scan = r#"{"query_block":{"table":{"rows_examined_per_scan":42}}}"#;
        assert_eq!(parse_explain_rows(scan), Some(42));
        let join = r#"{"query_block":{"rows_produced_per_join":7,"table":{}}}"#;
        assert_eq!(parse_explain_rows(join), Some(7));
        assert_eq!(parse_explain_rows(r#"{"query_block":{}}"#), None);
    }
}
