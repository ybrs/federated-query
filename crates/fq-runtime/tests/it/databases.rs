//! Real SQL namespace separation over independently persisted database directories.

use arrow::array::{Array, Int64Array};
use fq_common::database::{DatabaseId, DatabasePaths};
use fq_common::{load_config, Config};
use fq_runtime::{DatabaseManager, Runtime, RuntimeError};
use std::path::PathBuf;

/// Create a fresh source-free configuration with durable database storage.
fn config() -> (PathBuf, Config) {
    let root = std::env::temp_dir().join(format!("fq_runtime_databases_{}", DatabaseId::new()));
    std::fs::create_dir(&root).expect("create fixture directory");
    let path = root.join("engine.yaml");
    std::fs::write(&path, "datasources: {}\n").expect("write config");
    (
        root,
        load_config(path.to_str().expect("UTF-8 config")).expect("load"),
    )
}

/// Extract a single scalar integer returned by real engine execution.
fn scalar(runtime: &Runtime, sql: &str) -> i64 {
    let (_, batches) = runtime.execute(sql).expect("execute scalar");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer");
    assert!(!column.is_null(0));
    column.value(0)
}

/// Identical relation names resolve to each database's own persisted definitions.
#[test]
fn views_and_materializations_are_database_local() {
    let (_, config) = config();
    let main = Runtime::from_config(&config).expect("main");
    main.execute("CREATE DATABASE project_a").expect("create A");
    main.execute("CREATE DATABASE project_b").expect("create B");
    let first = Runtime::from_config_in_database(&config, "project_a").expect("A");
    let second = Runtime::from_config_in_database(&config, "project_b").expect("B");
    first
        .execute("CREATE VIEW report AS SELECT CAST(11 AS BIGINT) AS value")
        .expect("view A");
    second
        .execute("CREATE VIEW report AS SELECT CAST(22 AS BIGINT) AS value")
        .expect("view B");
    first
        .execute("CREATE MATERIALIZED VIEW cached AS SELECT CAST(31 AS BIGINT) AS value")
        .expect("MV A");
    second
        .execute("CREATE MATERIALIZED VIEW cached AS SELECT CAST(42 AS BIGINT) AS value")
        .expect("MV B");
    assert_eq!(scalar(&first, "SELECT value FROM report"), 11);
    assert_eq!(scalar(&second, "SELECT value FROM report"), 22);
    assert_eq!(scalar(&first, "SELECT value FROM cached"), 31);
    assert_eq!(scalar(&second, "SELECT value FROM cached"), 42);
    assert!(main.execute("SELECT value FROM report").is_err());
}

/// DROP removes one database's files while another live database remains readable.
#[test]
fn database_drop_reclaims_only_its_own_directory() {
    let (_, config) = config();
    let main = Runtime::from_config(&config).expect("main");
    main.execute("CREATE DATABASE disposable").expect("create");
    let project = Runtime::from_config_in_database(&config, "disposable").expect("project");
    project
        .execute("CREATE MATERIALIZED VIEW cache AS SELECT CAST(7 AS BIGINT) AS value")
        .expect("MV");
    assert!(matches!(
        main.execute("DROP DATABASE disposable"),
        Err(RuntimeError::DatabaseInUse(_))
    ));
    let root = config
        .storage
        .resolve(config.source_path.as_deref())
        .expect("root")
        .expect("persistent");
    let manager = DatabaseManager::open(&root).expect("manager");
    let record = manager
        .server_catalog()
        .get_database("disposable")
        .expect("record");
    let directory = DatabasePaths::new(&root, record.id).directory;
    drop(project);
    main.execute("DROP DATABASE disposable").expect("drop");
    assert!(!directory.exists());
    assert_eq!(scalar(&main, "SELECT CAST(9 AS BIGINT)"), 9);
    assert!(Runtime::from_config_in_database(&config, "disposable").is_err());
}

/// Explicit unknown selection and duplicate lifecycle statements always raise.
#[test]
fn database_selection_and_ddl_fail_loudly() {
    let (_, config) = config();
    let main = Runtime::from_config(&config).expect("main");
    assert_eq!(main.database_name(), "main");
    assert!(Runtime::from_config_in_database(&config, "unknown").is_err());
    assert!(Runtime::from_config_in_database(&config, "").is_err());
    main.execute("CREATE DATABASE project").expect("create");
    assert!(main.execute("CREATE DATABASE project").is_err());
    assert!(main.execute("DROP DATABASE missing").is_err());
    assert!(main.execute("DROP DATABASE main").is_err());
    assert!(main
        .execute("CREATE DATABASE IF NOT EXISTS project")
        .is_err());
}

/// Generated SQL must retain expression-derived output names as identifiers.
#[test]
fn unaliased_constant_expression_executes() {
    let (_, config) = config();
    let runtime = Runtime::from_config(&config).expect("runtime");
    let catalog = std::sync::Arc::new(fq_catalog::Catalog::new());
    let parsed = fq_parse::parse_with_catalog("SELECT CAST(9 AS BIGINT)", &catalog).expect("parse");
    let bound = fq_bind::bind(&catalog, parsed).expect("bind");
    let physical = fq_physical::PhysicalPlanner::new(catalog, None)
        .plan(&bound)
        .expect("plan");
    let steps = fq_physical::build_steps(&physical).expect("steps");
    let result = runtime.execute("SELECT CAST(9 AS BIGINT)");
    assert!(
        result.is_ok(),
        "execution: {result:?}; fragments: {:?}",
        steps.fragments
    );
}

/// Build durable SCRAM metadata while retaining process-owner provisioning authority.
fn authenticated_fixture() -> (Config, Runtime) {
    let (_, mut config) = config();
    config.server.auth = fq_common::security::AuthMode::Scram;
    let verifier = fq_scram::derive_verifier_random_salt("rootpw", 4096)
        .expect("derive root verifier")
        .to_authid_string();
    config.server.users.push(fq_common::UserCredential {
        name: "root".to_string(),
        verifier,
        superuser: true,
    });
    let owner = Runtime::from_config(&config).expect("owner");
    (config, owner)
}

/// Resolve an identity once, using the same catalog the startup handshake reads.
fn principal(config: &Config, name: &str) -> fq_common::database::PrincipalId {
    fq_runtime::open_server_catalog(config)
        .expect("root catalog")
        .expect("persistent")
        .get_user(name)
        .expect("read user")
        .expect("user exists")
        .id
}

/// Local ADMIN can manage only the database selected at connection construction.
#[test]
fn database_admin_manages_only_its_selected_namespace() {
    let (config, owner) = authenticated_fixture();
    owner.execute("CREATE DATABASE first").expect("first");
    owner.execute("CREATE DATABASE second").expect("second");
    owner
        .execute("CREATE USER alice WITH PASSWORD 'pw'")
        .expect("alice");
    owner
        .execute("GRANT ADMIN ON DATABASE first TO alice")
        .expect("admin");
    let alice = principal(&config, "alice");
    let first = Runtime::from_config_authenticated(&config, "first", alice).expect("first session");
    first
        .execute("CREATE VIEW report AS SELECT CAST(11 AS BIGINT) AS value")
        .expect("local view");
    first
        .execute("CREATE MATERIALIZED VIEW cached AS SELECT CAST(12 AS BIGINT) AS value")
        .expect("local MV");
    assert_eq!(scalar(&first, "SELECT value FROM report"), 11);
    assert_eq!(scalar(&first, "SELECT value FROM cached"), 12);
    assert!(Runtime::from_config_authenticated(&config, "second", alice).is_err());
    for sql in [
        "CREATE DATABASE forbidden",
        "DROP DATABASE second",
        "CREATE USER bad WITH PASSWORD 'pw'",
        "GRANT ADMIN ON DATABASE second TO alice",
    ] {
        assert!(
            matches!(first.execute(sql), Err(RuntimeError::PermissionDenied(_))),
            "{sql}"
        );
    }
}

/// A database revoke denies every entry point, including constants and static descriptions.
#[test]
fn connect_revoke_denies_the_next_statement_without_reconnect() {
    let (config, owner) = authenticated_fixture();
    owner.execute("CREATE DATABASE project").expect("project");
    owner
        .execute("CREATE USER reader WITH PASSWORD 'pw'")
        .expect("reader");
    owner
        .execute("GRANT CONNECT ON DATABASE project TO reader")
        .expect("connect");
    let reader =
        Runtime::from_config_authenticated(&config, "project", principal(&config, "reader"))
            .expect("session");
    assert_eq!(scalar(&reader, "SELECT CAST(1 AS BIGINT)"), 1);
    owner
        .execute("REVOKE CONNECT ON DATABASE project FROM reader")
        .expect("revoke");
    for sql in ["SELECT CAST(1 AS BIGINT)", "SHOW VIEWS", "SHOW DATABASES"] {
        assert!(
            matches!(reader.execute(sql), Err(RuntimeError::PermissionDenied(_))),
            "{sql}"
        );
        assert!(
            matches!(reader.describe(sql), Err(RuntimeError::PermissionDenied(_))),
            "describe {sql}"
        );
    }
}

/// Dropping and recreating a username cannot reauthorize its old connection or grants.
#[test]
fn recreated_username_cannot_inherit_a_live_session() {
    let (config, owner) = authenticated_fixture();
    owner
        .execute("CREATE USER alice WITH PASSWORD 'pw'")
        .expect("alice");
    owner
        .execute("GRANT CONNECT ON DATABASE main TO alice")
        .expect("connect");
    let original = principal(&config, "alice");
    let session = Runtime::from_config_authenticated(&config, "main", original).expect("session");
    owner.execute("DROP USER alice").expect("drop");
    owner
        .execute("CREATE USER alice WITH PASSWORD 'pw'")
        .expect("recreate");
    owner
        .execute("GRANT ADMIN ON DATABASE main TO alice")
        .expect("new admin");
    assert_ne!(principal(&config, "alice"), original);
    assert!(matches!(
        session.execute("SELECT 1"),
        Err(RuntimeError::PermissionDenied(_))
    ));
    assert!(Runtime::from_config_authenticated(&config, "main", original).is_err());
    let replacement =
        Runtime::from_config_authenticated(&config, "main", principal(&config, "alice"))
            .expect("replacement");
    replacement
        .execute("CREATE VIEW allowed AS SELECT 1 AS value")
        .expect("new rights");
}
