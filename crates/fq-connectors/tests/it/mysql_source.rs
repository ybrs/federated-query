//! Live-MySQL tests for the catalog/statistics tier.
//!
//! Runs against a MySQL server (URL from FQ_TEST_MYSQL_URL, e.g.
//! `mysql://root@127.0.0.1:3306/`). Each test owns a uniquely-named database,
//! dropped on `Drop`. If MySQL is unreachable (or FQ_TEST_MYSQL_URL is unset) the
//! tests early-return (a no-op pass) rather than fail, so the suite still runs
//! without a server - the pure type-mapping coverage lives in the crate's unit
//! tests, which need no server.

use std::sync::atomic::{AtomicU32, Ordering};

use fq_catalog::{DataSource, RenderDialect};
use fq_common::DataType;
use fq_connectors::MySqlSource;

/// The base MySQL URL, or None when the env var is unset (skip - there is no
/// safe default MySQL to connect to, unlike the local trust-auth Postgres).
fn base_url() -> Option<String> {
    std::env::var("FQ_TEST_MYSQL_URL").ok()
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A live-MySQL fixture: a uniquely-named database seeded with DDL, dropped on
/// `Drop`. `None` when MySQL is unreachable or unconfigured.
struct MySqlFixture {
    source: MySqlSource,
    database: String,
}

impl MySqlFixture {
    /// Connect, create a unique database, run each `ddl` (with `{s}` replaced by
    /// the database name), and return the fixture - or None if unreachable.
    fn new(ddl: &[&str]) -> Option<Self> {
        let base = base_url()?;
        let database = format!(
            "fq_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        // Connect at the server root first to create the database, then re-point
        // the source at it (the URL carries a default database).
        let root = MySqlSource::connect("my", &base, vec![database.clone()]).ok()?;
        root.execute(&format!("CREATE DATABASE `{database}`"))
            .ok()?;
        let url = format!("{}/{database}", base.trim_end_matches('/'));
        let source = MySqlSource::connect("my", &url, vec![database.clone()]).ok()?;
        for statement in ddl {
            source.execute(&statement.replace("{s}", &database)).ok()?;
        }
        Some(Self { source, database })
    }
}

impl Drop for MySqlFixture {
    fn drop(&mut self) {
        let _ = self
            .source
            .execute(&format!("DROP DATABASE IF EXISTS `{}`", self.database));
    }
}

/// A people table with a representative spread of MySQL types.
fn people_fixture() -> Option<MySqlFixture> {
    MySqlFixture::new(&[
        "CREATE TABLE {s}.people (id INT PRIMARY KEY, name VARCHAR(50), \
         score DOUBLE, born DATE, note TEXT)",
        "INSERT INTO {s}.people VALUES \
         (1,'a',1.5,'2020-01-01','x'),(2,'b',2.5,'2021-06-15','y'),(3,'c',3.5,'2022-12-31','z')",
        "ANALYZE TABLE {s}.people",
    ])
}

#[test]
fn test_render_dialect_and_list_tables() {
    let Some(fixture) = people_fixture() else {
        eprintln!("mysql unreachable/unconfigured; skipping");
        return;
    };
    assert_eq!(fixture.source.render_dialect(), RenderDialect::MySql);
    assert_eq!(
        fixture.source.list_tables(&fixture.database).unwrap(),
        vec!["people".to_string()]
    );
}

#[test]
fn test_metadata_and_type_mapping() {
    let Some(fixture) = people_fixture() else {
        eprintln!("mysql unreachable/unconfigured; skipping");
        return;
    };
    let metadata = fixture
        .source
        .get_table_metadata(&fixture.database, "people")
        .unwrap();
    let names: Vec<&str> = metadata.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name", "score", "born", "note"]);

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
            DataType::Text,
        ]
    );
}

#[test]
fn test_statistics_row_count() {
    let Some(fixture) = people_fixture() else {
        eprintln!("mysql unreachable/unconfigured; skipping");
        return;
    };
    let stats = fixture
        .source
        .get_table_statistics(&fixture.database, "people", &["id".to_string()])
        .unwrap()
        .unwrap();
    // information_schema.table_rows is an estimate; on a 3-row InnoDB table
    // after ANALYZE it is 3.
    assert_eq!(stats.row_count, Some(3));
    assert!(stats.column_stats.is_empty());
}

#[test]
fn test_estimate_scan_rows() {
    let Some(fixture) = people_fixture() else {
        eprintln!("mysql unreachable/unconfigured; skipping");
        return;
    };
    let sql = format!("SELECT * FROM `{}`.`people`", fixture.database);
    let estimate = fixture
        .source
        .estimate_scan_rows(&fixture.database, "people", &sql)
        .unwrap();
    assert!(estimate.is_some());
}
