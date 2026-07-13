//! Live-ClickHouse tests for the catalog/statistics tier.
//!
//! Runs against a ClickHouse HTTP endpoint (default `http://127.0.0.1:8123/`,
//! override with FQ_TEST_CLICKHOUSE_URL). Each test owns a uniquely-named
//! database, dropped on `Drop`, so parallel tests never collide. If ClickHouse is
//! unreachable the tests early-return (a no-op pass) rather than fail, so the
//! suite still runs without a server (the pure type-mapping coverage lives in the
//! crate's unit tests).

use std::sync::atomic::{AtomicU32, Ordering};

use fq_catalog::{DataSource, RenderDialect};
use fq_common::DataType;
use fq_connectors::ClickHouseSource;

/// The HTTP endpoint (override with FQ_TEST_CLICKHOUSE_URL).
fn base_url() -> String {
    std::env::var("FQ_TEST_CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123/".to_string())
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A live-ClickHouse fixture: a uniquely-named database seeded with DDL, dropped
/// on `Drop`. `None` when ClickHouse is unreachable.
struct ClickHouseFixture {
    source: ClickHouseSource,
    database: String,
}

impl ClickHouseFixture {
    /// Connect, create a unique database, run each `ddl` statement (with `{s}`
    /// replaced by the database name), and return the fixture - or None if the
    /// server is unreachable.
    fn new(ddl: &[&str]) -> Option<Self> {
        let database = format!(
            "fq_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let source =
            ClickHouseSource::connect("ch", base_url(), None, None, vec![database.clone()]).ok()?;
        source
            .execute(&format!("CREATE DATABASE \"{database}\""))
            .ok()?;
        for statement in ddl {
            source.execute(&statement.replace("{s}", &database)).ok()?;
        }
        Some(Self { source, database })
    }
}

impl Drop for ClickHouseFixture {
    fn drop(&mut self) {
        let _ = self
            .source
            .execute(&format!("DROP DATABASE IF EXISTS \"{}\"", self.database));
    }
}

/// A people table with a representative spread of ClickHouse types.
fn people_fixture() -> Option<ClickHouseFixture> {
    ClickHouseFixture::new(&[
        "CREATE TABLE {s}.people (id UInt32, name String, score Float64, \
         born Date, tag LowCardinality(String)) ENGINE = MergeTree ORDER BY id",
        "INSERT INTO {s}.people VALUES \
         (1,'a',1.5,'2020-01-01','x'),(2,'b',2.5,'2021-06-15','y'),(3,'c',3.5,'2022-12-31','x')",
    ])
}

#[test]
fn test_render_dialect_and_list_tables() {
    let Some(fixture) = people_fixture() else {
        eprintln!("clickhouse unreachable; skipping");
        return;
    };
    assert_eq!(fixture.source.render_dialect(), RenderDialect::ClickHouse);
    assert_eq!(
        fixture.source.list_tables(&fixture.database).unwrap(),
        vec!["people".to_string()]
    );
    assert_eq!(
        fixture.source.list_schemas().unwrap(),
        vec![fixture.database.clone()]
    );
}

#[test]
fn test_metadata_and_type_mapping() {
    let Some(fixture) = people_fixture() else {
        eprintln!("clickhouse unreachable; skipping");
        return;
    };
    let metadata = fixture
        .source
        .get_table_metadata(&fixture.database, "people")
        .unwrap();
    let names: Vec<&str> = metadata.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name", "score", "born", "tag"]);

    let mapped: Vec<DataType> = metadata
        .columns
        .iter()
        .map(|c| fixture.source.map_native_type(&c.data_type).unwrap())
        .collect();
    assert_eq!(
        mapped,
        vec![
            DataType::Integer,
            DataType::Varchar,
            DataType::Double,
            DataType::Date,
            DataType::Varchar, // LowCardinality(String) unwrapped
        ]
    );
}

#[test]
fn test_statistics_row_count() {
    let Some(fixture) = people_fixture() else {
        eprintln!("clickhouse unreachable; skipping");
        return;
    };
    let stats = fixture
        .source
        .get_table_statistics(&fixture.database, "people", &["id".to_string()])
        .unwrap()
        .unwrap();
    assert_eq!(stats.row_count, Some(3));
    // No cheap column stats are offered; the map is honestly empty.
    assert!(stats.column_stats.is_empty());
}

#[test]
fn test_estimate_scan_rows() {
    let Some(fixture) = people_fixture() else {
        eprintln!("clickhouse unreachable; skipping");
        return;
    };
    let sql = format!("SELECT * FROM \"{}\".\"people\"", fixture.database);
    let estimate = fixture
        .source
        .estimate_scan_rows(&fixture.database, "people", &sql)
        .unwrap();
    // EXPLAIN ESTIMATE on a MergeTree table yields a row estimate.
    assert_eq!(estimate, Some(3));
}
