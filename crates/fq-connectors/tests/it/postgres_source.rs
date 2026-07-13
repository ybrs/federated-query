//! Translation of the Postgres half of `tests/test_datasources.py` /
//! `tests/test_statistics_layer.py` plus the pg uuid `map_native_type` override
//! (from `tests/test_catalog.py`).
//!
//! Runs against a live Postgres (the harness starts one on :5432). Each test owns
//! a uniquely-named schema, dropped on `Drop`, so parallel tests never collide.
//! If Postgres is unreachable the tests early-return (a no-op pass) rather than
//! fail, so the suite still runs in a pg-less environment.

use std::sync::atomic::{AtomicU32, Ordering};

use fq_catalog::{DataSource, RenderDialect, StatValue};
use fq_common::DataType;
use fq_connectors::PostgresSource;

/// The connection string (override with FQ_TEST_PG_URL); trust auth, no password.
fn conn_str() -> String {
    std::env::var("FQ_TEST_PG_URL")
        .unwrap_or_else(|_| "host=localhost port=5432 user=postgres dbname=postgres".to_string())
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A live-Postgres test fixture: a uniquely-named schema seeded with DDL, dropped
/// (CASCADE) on `Drop`. `None` when Postgres is unreachable.
struct PgFixture {
    source: PostgresSource,
    schema: String,
}

impl PgFixture {
    /// Connect, create a unique schema, run `ddl` (with `{s}` replaced by the
    /// schema name), and return the fixture - or None if pg is unreachable.
    fn new(ddl: &str) -> Option<Self> {
        let schema = format!(
            "fq_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let source = PostgresSource::connect("pg", &conn_str(), vec![schema.clone()]).ok()?;
        source
            .execute(&format!("CREATE SCHEMA \"{schema}\""))
            .ok()?;
        source.execute(&ddl.replace("{s}", &schema)).ok()?;
        Some(Self { source, schema })
    }
}

impl Drop for PgFixture {
    fn drop(&mut self) {
        let _ = self.source.execute(&format!(
            "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
            self.schema
        ));
    }
}

/// The people table, analyzed (3 distinct ids, a uuid column, no nulls).
fn people_fixture() -> Option<PgFixture> {
    PgFixture::new(
        "CREATE TABLE {s}.people (id integer PRIMARY KEY, name varchar, \
             uid uuid, score double precision);
         INSERT INTO {s}.people VALUES
             (1, 'a', gen_random_uuid(), 1.5),
             (2, 'b', gen_random_uuid(), 2.5),
             (3, 'c', gen_random_uuid(), 3.5);
         ANALYZE {s}.people;",
    )
}

#[test]
fn test_render_dialect_and_list_tables() {
    let Some(fixture) = people_fixture() else {
        eprintln!("postgres unreachable; skipping");
        return;
    };
    assert_eq!(fixture.source.render_dialect(), RenderDialect::Postgres);
    assert_eq!(
        fixture.source.list_tables(&fixture.schema).unwrap(),
        vec!["people".to_string()]
    );
    // list_schemas returns the configured schema list.
    assert_eq!(
        fixture.source.list_schemas().unwrap(),
        vec![fixture.schema.clone()]
    );
}

#[test]
fn test_metadata_and_uuid_type_mapping() {
    let Some(fixture) = people_fixture() else {
        eprintln!("postgres unreachable; skipping");
        return;
    };
    let metadata = fixture
        .source
        .get_table_metadata(&fixture.schema, "people")
        .unwrap();
    let names: Vec<&str> = metadata.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name", "uid", "score"]);

    // Each native type maps through the source; uuid uses the pg override.
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
            DataType::Varchar, // uuid -> VARCHAR (pg override)
            DataType::Double,
        ]
    );
}

#[test]
fn test_uuid_override_and_default_fallback() {
    let Some(fixture) = people_fixture() else {
        eprintln!("postgres unreachable; skipping");
        return;
    };
    // The override maps uuid to VARCHAR; the generic names fall through to the
    // shared default mapper.
    assert_eq!(
        fixture.source.map_native_type("uuid").unwrap(),
        DataType::Varchar
    );
    assert_eq!(
        fixture.source.map_native_type("integer").unwrap(),
        DataType::Integer
    );
}

#[test]
fn test_statistics_from_pg_catalog_when_analyzed() {
    let Some(fixture) = people_fixture() else {
        eprintln!("postgres unreachable; skipping");
        return;
    };
    let stats = fixture
        .source
        .get_table_statistics(&fixture.schema, "people", &["id".to_string()])
        .unwrap()
        .unwrap();
    // reltuples after ANALYZE on a 3-row table.
    assert_eq!(stats.row_count, Some(3));
    // pg_stats knows id: three distinct values, no nulls.
    let id_stats = &stats.column_stats["id"];
    assert_eq!(id_stats.num_distinct, Some(3));
    assert!((id_stats.null_fraction - 0.0).abs() < 1e-9);
    // The histogram/mcv ends decode to orderable ints.
    assert!(matches!(id_stats.min_value, Some(StatValue::Integer(_))));
}

#[test]
fn test_estimate_scan_rows_on_analyzed_table() {
    let Some(fixture) = people_fixture() else {
        eprintln!("postgres unreachable; skipping");
        return;
    };
    let sql = format!(
        "SELECT * FROM \"{}\".\"people\" WHERE id > 1",
        fixture.schema
    );
    let estimate = fixture
        .source
        .estimate_scan_rows(&fixture.schema, "people", &sql)
        .unwrap();
    // An analyzed table yields a planner estimate.
    assert!(estimate.is_some());
}

#[test]
fn test_probe_measures_statless_table() {
    // A table forced statless (reltuples = -1) is PROBED: an exact aggregate scan
    // measures the true row count rather than reporting unknown.
    let Some(fixture) = PgFixture::new(
        "CREATE TABLE {s}.cold (id integer, grp varchar);
         INSERT INTO {s}.cold VALUES (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'b');",
    ) else {
        eprintln!("postgres unreachable; skipping");
        return;
    };
    // Force the never-analyzed state (defend against autovacuum having run).
    fixture
        .source
        .execute(&format!(
            "UPDATE pg_class SET reltuples = -1, relpages = 0 \
             WHERE oid = '\"{}\".\"cold\"'::regclass",
            fixture.schema
        ))
        .unwrap();

    let stats = fixture
        .source
        .get_table_statistics(&fixture.schema, "cold", &["grp".to_string()])
        .unwrap()
        .unwrap();
    // The exact probe measured the true count and the column's distinct values.
    assert_eq!(stats.row_count, Some(5));
    assert_eq!(stats.column_stats["grp"].num_distinct, Some(2));
}

#[test]
fn test_source_token_is_stable_and_moves_on_a_rewrite() {
    let Some(fixture) = people_fixture() else {
        eprintln!("postgres unreachable; skipping");
        return;
    };
    let first = fixture
        .source
        .source_token(&fixture.schema, "people")
        .expect("token")
        .expect("a known pg table has a token");
    assert!(first.contains("relfilenode="), "{first}");
    // Two reads with no intervening change agree.
    let second = fixture
        .source
        .source_token(&fixture.schema, "people")
        .expect("token")
        .expect("token");
    assert_eq!(first, second);

    // TRUNCATE assigns a new relfilenode - a deterministic catalog change, no
    // dependence on the asynchronous statistics flush.
    fixture
        .source
        .execute(&format!("TRUNCATE {}.people", fixture.schema))
        .expect("truncate");
    let truncated = fixture
        .source
        .source_token(&fixture.schema, "people")
        .expect("token")
        .expect("token");
    assert_ne!(first, truncated);

    // A table pg does not know abstains (the refresh then pulls, and the pull
    // raises loudly on the missing relation).
    assert_eq!(
        fixture
            .source
            .source_token(&fixture.schema, "ghost")
            .expect("token"),
        None
    );
}
