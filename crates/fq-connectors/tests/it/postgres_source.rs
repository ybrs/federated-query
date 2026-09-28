//! Translation of the Postgres half of `tests/test_datasources.py` /
//! `tests/test_statistics_layer.py` plus the pg uuid `map_native_type` override
//! (from `tests/test_catalog.py`).
//!
//! Runs against the repository test Postgres selected by `PGPORT`. Each test owns
//! a uniquely-named schema, dropped on `Drop`, so parallel tests never collide.
//! If Postgres is unreachable, fixture construction fails the test.

use std::sync::atomic::{AtomicU32, Ordering};

use fq_catalog::{DataSource, RenderDialect, StatValue};
use fq_common::DataType;
use fq_connectors::PostgresSource;

/// The connection string (override with FQ_TEST_PG_URL); trust auth, no password.
fn conn_str() -> String {
    std::env::var("FQ_TEST_PG_URL").unwrap_or_else(|_| {
        let port = std::env::var("PGPORT").expect("PGPORT must select test PostgreSQL");
        format!("host=localhost port={port} user=postgres dbname=postgres")
    })
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A live-Postgres test fixture: a uniquely-named schema seeded with DDL, dropped
/// (CASCADE) on `Drop`.
struct PgFixture {
    source: PostgresSource,
    schema: String,
}

impl PgFixture {
    /// Connect, create a unique schema, run `ddl` (with `{s}` replaced by the
    /// schema name), and return the fixture.
    fn new(ddl: &str) -> Self {
        let schema = format!(
            "fq_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let source = PostgresSource::connect("pg", &conn_str(), vec![schema.clone()])
            .expect("connect to test PostgreSQL");
        source
            .execute(&format!("CREATE SCHEMA \"{schema}\""))
            .expect("create test schema");
        source
            .execute(&ddl.replace("{s}", &schema))
            .expect("create PostgreSQL fixture");
        Self { source, schema }
    }
}

impl Drop for PgFixture {
    fn drop(&mut self) {
        self.source
            .execute(&format!(
                "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                self.schema
            ))
            .expect("drop test schema");
    }
}

/// The people table, analyzed (3 distinct ids, a uuid column, no nulls).
fn people_fixture() -> PgFixture {
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
    let fixture = people_fixture();
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
    let fixture = people_fixture();
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
    let fixture = people_fixture();
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
    let fixture = people_fixture();
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
    let fixture = people_fixture();
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
    let fixture = PgFixture::new(
        "CREATE TABLE {s}.cold (id integer, grp varchar);
         INSERT INTO {s}.cold VALUES (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'b');",
    );
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
    let fixture = people_fixture();
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

/// A driver error surfaces the server's real message text and SQLSTATE, not the
/// driver's generic "db error" Display.
#[test]
fn db_error_surfaces_message_and_sqlstate() {
    let fixture = PgFixture::new("");
    let error = fixture
        .source
        .execute(&format!(
            "SELECT 1 FROM {}.no_such_relation_zzz",
            fixture.schema
        ))
        .expect_err("a query on a missing relation must fail");
    let text = error.to_string();
    assert!(text.contains("does not exist"), "message text lost: {text}");
    assert!(text.contains("SQLSTATE 42P01"), "sqlstate lost: {text}");
}

/// A never-ANALYZEd table with a boolean column probes without error: Postgres
/// has no min/max for booleans, so the probe omits those bounds yet still
/// returns the exact distinct count (booleans compare for equality).
#[test]
fn probe_handles_boolean_column() {
    let fixture = PgFixture::new(
        "CREATE TABLE {s}.flags (id int, active boolean); \
         INSERT INTO {s}.flags VALUES (1, true), (2, false), (3, true)",
    );
    let stats = fixture
        .source
        .get_table_statistics(
            &fixture.schema,
            "flags",
            &["id".to_string(), "active".to_string()],
        )
        .expect("probe must not fail on a boolean column")
        .expect("stats present");
    assert_eq!(stats.row_count, Some(3));
    let active = &stats.column_stats["active"];
    assert!(active.min_value.is_none(), "boolean has no min bound");
    assert!(active.max_value.is_none(), "boolean has no max bound");
    assert_eq!(active.num_distinct, Some(2));
    let id = &stats.column_stats["id"];
    assert!(id.min_value.is_some(), "orderable neighbour keeps its min");
}
