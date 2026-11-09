//! Durable metadata behavior and ownership over real SQLite catalogs.

use super::*;

/// Allocate a fresh SQLite path for one independently named test.
fn temp_db(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("fq_ds_{tag}_{}", std::process::id()));
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("test directory cleanup failed: {error}"),
    }
    std::fs::create_dir_all(&dir).expect("create dir");
    dir.join("store.sqlite").to_string_lossy().to_string()
}

/// Construct one source definition with explicit connection parameters.
fn sample() -> fq_common::DataSourceConfig {
    serde_json::from_str(
        "{\"name\":\"sales\",\"ty\":\"duckdb\",\"config\":{\"path\":\"/data/a.db\",         \"schemas\":[\"public\",\"staging\"]},\"capabilities\":[],\"change_keys\":{}}",
    )
    .expect("sample definition")
}

/// Verify insert and read back round trips.
#[test]
fn insert_and_read_back_round_trips() {
    let catalog = DatabaseCatalog::open(&temp_db("roundtrip")).expect("open");
    let ds = sample();
    assert!(catalog.insert_datasource(&ds).expect("insert"));
    let read = catalog
        .get_datasource("sales")
        .expect("get")
        .expect("present");
    assert_eq!(read.definition, ds);
    assert!(
        !read.created_at.is_empty(),
        "created_at is stamped on insert"
    );
    let all = catalog.list_datasources().expect("list");
    assert_eq!(all.len(), 1);
}

/// Verify insert or ignore makes the second writer lose.
#[test]
fn insert_or_ignore_makes_the_second_writer_lose() {
    let catalog = DatabaseCatalog::open(&temp_db("ignore")).expect("open");
    assert!(catalog.insert_datasource(&sample()).expect("first insert"));
    assert!(
        !catalog.insert_datasource(&sample()).expect("second insert"),
        "a duplicate name inserts nothing"
    );
}

/// Verify delete removes the row.
#[test]
fn delete_removes_the_row() {
    let catalog = DatabaseCatalog::open(&temp_db("delete")).expect("open");
    catalog.insert_datasource(&sample()).expect("insert");
    assert!(catalog.delete_datasource("sales").expect("delete"));
    assert!(catalog.get_datasource("sales").expect("get").is_none());
    assert!(
        !catalog.delete_datasource("sales").expect("delete again"),
        "deleting a missing name removes nothing"
    );
}

/// Database definitions persist without creating materialization or user tables.
#[test]
fn definitions_do_not_create_accelerator_or_identity_tables() {
    let path = temp_db("database_ownership");
    let catalog = DatabaseCatalog::open(&path).expect("open definitions");
    catalog
        .upsert_setting("optimizer.batch", "16")
        .expect("persist");
    assert_eq!(catalog.list_settings().expect("settings").len(), 1);
    let conn = Connection::open(path).expect("inspect schema");
    for table in ["materialized_views", "users", "grants"] {
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |row| row.get(0),
            )
            .expect("inspect table");
        assert_eq!(count, 0, "definitions must not own {table}");
    }
}

/// Corrupt stored source parameters raise instead of becoming an empty definition.
#[test]
fn malformed_source_metadata_raises() {
    let path = temp_db("corrupt_source");
    let catalog = DatabaseCatalog::open(&path).expect("open definitions");
    let conn = Connection::open(path).expect("open writer");
    conn.execute(
        "INSERT INTO dynamic_datasources(name,definition,created_at) VALUES ('bad','{','now')",
        [],
    )
    .expect("insert malformed row");
    assert!(matches!(
        catalog.list_datasources(),
        Err(StoreError::Json(_))
    ));
}

/// Local SELECT grants distinguish identifiers containing dots from separate components.
#[test]
fn local_grants_are_structured_and_separate() {
    use fq_common::database::PrincipalId;
    use fq_common::security::{GrantSubject, ObjectKey};
    let first = DatabaseCatalog::open(&temp_db("local_grants_a")).expect("first");
    let second = DatabaseCatalog::open(&temp_db("local_grants_b")).expect("second");
    let subject = GrantSubject::User(PrincipalId::new());
    let target = ObjectKey::Table {
        datasource: "a.b".to_string(),
        schema: "c".to_string(),
        table: "d".to_string(),
    };
    let other = ObjectKey::Table {
        datasource: "a".to_string(),
        schema: "b.c".to_string(),
        table: "d".to_string(),
    };
    first.grant(subject, &target, "root").expect("grant");
    let generation = first.acl_generation().expect("generation");
    first.grant(subject, &target, "root").expect("regrant");
    assert_eq!(first.acl_generation().expect("no mutation"), generation);
    let (observed, grants) = first.access_snapshot(subject).expect("snapshot");
    assert_eq!(observed, generation);
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].object, target);
    assert!(second.list_grants(None).expect("other database").is_empty());
    assert!(!first.revoke(subject, &other).expect("different components"));
    assert!(first.revoke(subject, &target).expect("revoke"));
    assert!(!first.revoke(subject, &target).expect("missing"));
}
