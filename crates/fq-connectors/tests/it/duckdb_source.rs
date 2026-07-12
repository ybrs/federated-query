//! Translation of the DuckDB half of `tests/test_datasources.py` plus the
//! DuckDB `map_native_type` cases from `tests/test_catalog.py`.
//!
//! Uses a real in-memory DuckDB (embedded, no server), mirroring the Python
//! fixture's create-schema-and-tables setup.

use fq_catalog::{Catalog, DataSource, DataSourceCapability, RenderDialect};
use fq_common::DataType;
use fq_connectors::DuckDbSource;
use std::sync::Arc;

/// A source seeded with main.test_table(id INTEGER, name VARCHAR, value DOUBLE)
/// and three rows - the Python `duckdb_datasource` fixture.
fn seeded_source() -> DuckDbSource {
    let source = DuckDbSource::open_in_memory("test_duck").expect("open");
    source
        .execute_batch(
            "CREATE SCHEMA IF NOT EXISTS main;
             CREATE TABLE IF NOT EXISTS main.test_table (
                 id INTEGER PRIMARY KEY, name VARCHAR, value DOUBLE
             );
             INSERT INTO main.test_table VALUES
                 (1, 'Alice', 100.5), (2, 'Bob', 200.75), (3, 'Carol', 150.25);",
        )
        .expect("seed");
    source
}

#[test]
fn test_render_dialect_is_duckdb() {
    assert_eq!(seeded_source().render_dialect(), RenderDialect::DuckDb);
}

#[test]
fn test_capabilities() {
    let capabilities = seeded_source().capabilities();
    for expected in [
        DataSourceCapability::Aggregations,
        DataSourceCapability::Joins,
        DataSourceCapability::WindowFunctions,
        DataSourceCapability::Subqueries,
        DataSourceCapability::Cte,
        DataSourceCapability::ShipTarget,
    ] {
        assert!(capabilities.contains(&expected), "missing {expected:?}");
    }
    assert!(seeded_source().supports_capability(DataSourceCapability::Joins));
}

#[test]
fn test_list_schemas_and_tables() {
    let source = seeded_source();
    assert!(source.list_schemas().unwrap().contains(&"main".to_string()));
    assert_eq!(
        source.list_tables("main").unwrap(),
        vec!["test_table".to_string()]
    );
}

#[test]
fn test_table_metadata() {
    let metadata = seeded_source()
        .get_table_metadata("main", "test_table")
        .expect("metadata");
    assert_eq!(metadata.schema_name, "main");
    assert_eq!(metadata.table_name, "test_table");
    let names: Vec<&str> = metadata.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name", "value"]);
    // id is NOT NULL (primary key), name/value are nullable.
    assert!(!metadata.columns[0].nullable);
}

#[test]
fn test_metadata_native_types_map_to_engine_types() {
    // The connector's native type names map through the default mapper.
    let source = seeded_source();
    let metadata = source.get_table_metadata("main", "test_table").unwrap();
    assert_eq!(
        source
            .map_native_type(&metadata.columns[0].data_type)
            .unwrap(),
        DataType::Integer
    );
    assert_eq!(
        source
            .map_native_type(&metadata.columns[1].data_type)
            .unwrap(),
        DataType::Varchar
    );
    assert_eq!(
        source
            .map_native_type(&metadata.columns[2].data_type)
            .unwrap(),
        DataType::Double
    );
}

#[test]
fn test_table_statistics_are_metadata_only() {
    // Planning never scans data: the row count is a duckdb_tables() catalog
    // read, and NO per-column statistics are measured (NDV comes from the
    // learned overlay; filtered-row estimates from EXPLAIN) - so requesting
    // columns must NOT trigger an aggregate scan or return column stats.
    let source = seeded_source();
    let stats = source
        .get_table_statistics("main", "test_table", &["id".to_string()])
        .expect("stats")
        .expect("some");
    assert_eq!(stats.row_count, Some(3));
    assert!(stats.column_stats.is_empty());
}

#[test]
fn test_estimate_scan_rows_parses_explain() {
    // The source planner's estimate for a rendered scan comes from EXPLAIN's
    // "~N rows" line - an O(1) catalog-statistics read, never an execution.
    let source = seeded_source();
    let estimate = source
        .estimate_scan_rows("main", "test_table", "SELECT id FROM main.test_table")
        .expect("estimate");
    assert_eq!(estimate, Some(3));
    // Unparseable probe SQL is an honest absence, not an error.
    let absent = source
        .estimate_scan_rows("main", "test_table", "SELECT nope FROM no_such_table")
        .expect("absent");
    assert_eq!(absent, None);
}

#[test]
fn test_table_statistics_row_count_only_when_no_columns() {
    let stats = seeded_source()
        .get_table_statistics("main", "test_table", &[])
        .unwrap()
        .unwrap();
    assert_eq!(stats.row_count, Some(3));
    assert!(stats.column_stats.is_empty());
}

#[test]
fn test_statistics_unknown_table_raises() {
    let error = seeded_source()
        .get_table_statistics("main", "nope", &[])
        .expect_err("unknown table must raise");
    assert!(format!("{error}").contains("nope"));
}

#[test]
fn test_load_metadata_through_catalog() {
    // End-to-end: register the real source and let the catalog introspect it.
    let source = seeded_source();
    let mut catalog = Catalog::new();
    catalog.register_datasource(Arc::new(source));
    catalog.load_metadata().expect("load");
    let table = catalog
        .get_table("test_duck", "main", "test_table")
        .expect("table");
    assert_eq!(table.columns.len(), 3);
    assert_eq!(
        table.get_column("value").unwrap().data_type,
        DataType::Double
    );
}
