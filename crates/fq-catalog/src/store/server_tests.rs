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

/// Verify users round trip and first writer wins.
#[test]
fn users_round_trip_and_first_writer_wins() {
    let catalog = ServerCatalog::open(&temp_db("users")).expect("open");
    assert!(catalog
        .create_user("alice", "verifier-a", true, "bootstrap-import")
        .expect("create"));
    // A duplicate name inserts nothing (first-writer-wins).
    assert!(!catalog
        .create_user("alice", "verifier-b", false, "alice")
        .expect("dup"));
    let alice = catalog.get_user("alice").expect("get").expect("present");
    assert_eq!(alice.verifier, "verifier-a");
    assert!(alice.superuser);
    assert_eq!(catalog.count_superusers().expect("count"), 1);
}

/// Verify alter and drop user bump the generation.
#[test]
fn alter_and_drop_user_bump_the_generation() {
    let catalog = ServerCatalog::open(&temp_db("users_gen")).expect("open");
    let gen0 = catalog.acl_generation().expect("gen0");
    catalog
        .create_user("bob", "v", false, "root")
        .expect("create");
    assert!(catalog.acl_generation().expect("gen1") > gen0);
    let gen1 = catalog.acl_generation().expect("gen1");
    assert!(catalog.set_user_password("bob", "v2").expect("alter pw"));
    assert!(catalog.set_user_superuser("bob", true).expect("alter su"));
    assert!(catalog.acl_generation().expect("gen2") > gen1);
    assert!(
        catalog
            .get_user("bob")
            .expect("get")
            .expect("bob")
            .superuser
    );
    // A no-op alter of a missing user does not bump.
    let before = catalog.acl_generation().expect("before");
    assert!(!catalog.set_user_password("ghost", "x").expect("miss"));
    assert_eq!(catalog.acl_generation().expect("after"), before);
}

/// Looking up credentials requires neither event storage nor a materialized store.
#[test]
fn identity_store_does_not_create_result_tables() {
    let path = temp_db("identity_ownership");
    let catalog = ServerCatalog::open(&path).expect("open identities");
    assert!(catalog
        .create_user("admin", "verifier", true, "test")
        .expect("create"));
    let conn = Connection::open(path).expect("inspect schema");
    for table in [
        "materialized_views",
        "dynamic_datasources",
        "views",
        "engine_settings",
    ] {
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |row| row.get(0),
            )
            .expect("inspect table");
        assert_eq!(count, 0, "identities must not own {table}");
    }
}

/// A registered name cannot be reused until its retired identity is purged.
#[test]
fn database_lifecycle_preserves_identity_until_cleanup() {
    use fq_common::database::DatabaseState;
    let catalog = ServerCatalog::open(&temp_db("database_lifecycle")).expect("open");
    let first = catalog
        .reserve_database(fq_common::database::DatabaseId::new(), "project1", "admin")
        .expect("reserve");
    assert_eq!(first.state, DatabaseState::Creating);
    assert!(matches!(
        catalog.reserve_database(fq_common::database::DatabaseId::new(), "project1", "admin"),
        Err(StoreError::DatabaseExists(_))
    ));
    catalog.publish_database(first.id).expect("publish");
    assert_eq!(
        catalog.get_database("project1").expect("find").state,
        DatabaseState::Ready
    );
    assert!(catalog.purge_database(first.id).is_err());
    catalog.retire_database(first.id).expect("retire");
    catalog.purge_database(first.id).expect("purge");
    let replacement = catalog
        .reserve_database(fq_common::database::DatabaseId::new(), "project1", "admin")
        .expect("replace");
    assert_ne!(first.id, replacement.id);
}

/// Invalid lifecycle repetitions raise instead of reporting a successful no-op.
#[test]
fn database_state_transitions_fail_loudly() {
    let catalog = ServerCatalog::open(&temp_db("database_states")).expect("open");
    assert!(matches!(
        catalog.get_database("absent"),
        Err(StoreError::DatabaseMissing(_))
    ));
    let record = catalog
        .reserve_database(fq_common::database::DatabaseId::new(), "project", "admin")
        .expect("reserve");
    assert!(catalog.retire_database(record.id).is_err());
    catalog.publish_database(record.id).expect("publish");
    assert!(catalog.publish_database(record.id).is_err());
    catalog.retire_database(record.id).expect("retire");
    catalog.purge_database(record.id).expect("purge");
    assert!(catalog.purge_database(record.id).is_err());
}

/// Database privileges are isolated by database and survive display-name changes only by ID.
#[test]
fn database_access_uses_stable_principals() {
    use fq_common::database::DatabaseId;
    use fq_common::security::{DatabasePrivilege, GrantSubject};
    let catalog = ServerCatalog::open(&temp_db("access")).expect("open");
    catalog
        .create_user("alice", "v", false, "test")
        .expect("user");
    let alice = catalog.get_user("alice").expect("read").expect("alice");
    let first = catalog
        .reserve_database(DatabaseId::new(), "first", "test")
        .expect("first");
    let second = catalog
        .reserve_database(DatabaseId::new(), "second", "test")
        .expect("second");
    catalog
        .grant_database(
            first.id,
            GrantSubject::User(alice.id),
            DatabasePrivilege::Admin,
            "root",
        )
        .expect("admin");
    assert_eq!(
        catalog.database_access(first.id, alice.id).expect("read"),
        Some(DatabasePrivilege::Admin)
    );
    assert_eq!(
        catalog.database_access(second.id, alice.id).expect("read"),
        None
    );
    catalog.drop_user("alice").expect("drop");
    catalog
        .create_user("alice", "v2", false, "test")
        .expect("recreate");
    let replacement = catalog.get_user("alice").expect("read").expect("alice");
    assert_ne!(alice.id, replacement.id);
    assert!(catalog
        .get_user_by_id(alice.id)
        .expect("old identity")
        .is_none());
    assert_eq!(
        catalog
            .database_access(first.id, replacement.id)
            .expect("new access"),
        None
    );
    assert_eq!(
        catalog
            .database_access(first.id, alice.id)
            .expect("old access"),
        None
    );
}

/// PUBLIC grants only admit connections; revocation is exact and updates generation.
#[test]
fn public_connect_and_loud_revoke() {
    use fq_common::database::{DatabaseId, PrincipalId};
    use fq_common::security::{DatabasePrivilege, GrantSubject};
    let catalog = ServerCatalog::open(&temp_db("public_access")).expect("open");
    let db = catalog
        .reserve_database(DatabaseId::new(), "project", "test")
        .expect("db");
    assert!(matches!(
        catalog.grant_database(
            db.id,
            GrantSubject::Public,
            DatabasePrivilege::Admin,
            "root"
        ),
        Err(StoreError::PublicDatabaseAdmin)
    ));
    let before = catalog.acl_generation().expect("generation");
    catalog
        .grant_database(
            db.id,
            GrantSubject::Public,
            DatabasePrivilege::Connect,
            "root",
        )
        .expect("connect");
    assert!(catalog.acl_generation().expect("generation") > before);
    assert_eq!(
        catalog
            .database_access(db.id, PrincipalId::new())
            .expect("public"),
        Some(DatabasePrivilege::Connect)
    );
    assert!(catalog
        .revoke_database(db.id, GrantSubject::Public, DatabasePrivilege::Connect)
        .expect("revoke"));
    assert!(!catalog
        .revoke_database(db.id, GrantSubject::Public, DatabasePrivilege::Connect)
        .expect("missing"));
}

/// Concurrent administrators cannot leave a server without a surviving superuser.
#[test]
fn last_superuser_cannot_be_dropped_or_demoted() {
    let catalog = ServerCatalog::open(&temp_db("last_admin")).expect("open");
    catalog
        .create_user("root", "v", true, "test")
        .expect("root");
    assert!(matches!(
        catalog.drop_user("root"),
        Err(StoreError::LastSuperuser)
    ));
    assert!(matches!(
        catalog.set_user_superuser("root", false),
        Err(StoreError::LastSuperuser)
    ));
    catalog
        .create_user("other", "v", true, "root")
        .expect("other");
    assert!(catalog.set_user_superuser("root", false).expect("demote"));
    assert!(catalog.drop_user("root").expect("drop"));
    assert_eq!(catalog.count_superusers().expect("survivor"), 1);
}

/// Config credentials seed atomically once and never restore deleted accounts.
#[test]
fn bootstrap_is_atomic_and_does_not_resurrect_users() {
    use fq_common::UserCredential;
    let catalog = ServerCatalog::open(&temp_db("seed")).expect("open");
    let user = UserCredential {
        name: "seeded".to_string(),
        verifier: "v".to_string(),
        superuser: false,
    };
    assert!(catalog.seed_users(&[user.clone(), user.clone()]).is_err());
    assert!(catalog.list_users().expect("rolled back").is_empty());
    assert!(catalog
        .seed_users(std::slice::from_ref(&user))
        .expect("seed"));
    let original = catalog.get_user("seeded").expect("read").expect("seeded");
    assert!(!catalog
        .seed_users(std::slice::from_ref(&user))
        .expect("idempotent"));
    assert_eq!(
        catalog
            .get_user("seeded")
            .expect("read")
            .expect("seeded")
            .id,
        original.id
    );
    catalog.drop_user("seeded").expect("drop");
    assert!(!catalog.seed_users(&[user]).expect("no resurrection"));
    assert!(catalog.list_users().expect("empty").is_empty());
}
