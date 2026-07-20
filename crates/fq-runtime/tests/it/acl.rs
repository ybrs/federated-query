//! Bind-time access control end to end over a real DuckDB source: a superuser
//! creates users and grants; a restricted principal is authorized for a covering
//! grant at any of the three containment levels (and for PUBLIC), denied a
//! sibling table as a non-leaking not-found, and a superuser bypasses every
//! check; a REVOKE by a second session is visible at the principal's next query
//! through the generation counter; admin DDL is superuser-only; and a bootstrap
//! store with users but no superuser refuses to start.
//!
//! Enforcement runs at BIND time, so every assertion drives `describe` (plan
//! only, no data scan) - the exact hook a query's bind takes - except the
//! authorized happy path, which also `execute`s to prove rows still flow.

use std::collections::BTreeMap;
use std::path::PathBuf;

use fq_accel::Accelerator;
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
    UserCredential,
};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// A per-test sandbox holding a seeded DuckDB file and the config path the ACL
/// store hangs off.
struct Sandbox {
    dir: PathBuf,
}

impl Sandbox {
    /// Create a fresh empty sandbox dir under a unique temp path.
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("fq_acl_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create sandbox dir");
        Self { dir }
    }

    /// Seed a DuckDB file with two tables (`orders`, `lineitem`) in the default
    /// `main` schema and return its absolute path.
    fn seed_duck(&self) -> String {
        let path = self.dir.join("shop.duckdb");
        let path_str = path.to_string_lossy().to_string();
        let source = DuckDbSource::open("seed", &path_str).expect("open seed duckdb");
        source
            .execute_batch("CREATE TABLE orders (id INTEGER, total INTEGER)")
            .expect("create orders");
        source
            .execute_batch("INSERT INTO orders VALUES (1, 10), (2, 20)")
            .expect("insert orders");
        source
            .execute_batch("CREATE TABLE lineitem (id INTEGER, qty INTEGER)")
            .expect("create lineitem");
        drop(source);
        path_str
    }

    /// A config with the seeded DuckDB source and `server.users` set to one
    /// bootstrap superuser, so wire principals enforce their grants.
    fn config(&self) -> Config {
        let mut datasources = BTreeMap::new();
        datasources.insert("duck".to_string(), duck_bootstrap(&self.seed_duck()));
        Config {
            datasources,
            optimizer: OptimizerConfig::default(),
            executor: ExecutorConfig::default(),
            cost: CostConfig::default(),
            server: server_with_admin(),
            accelerator: fq_common::AcceleratorConfig::default(),
            catalog: fq_common::CatalogConfig::default(),
            source_path: Some(self.dir.join("config.yaml").to_string_lossy().to_string()),
        }
    }

    /// A runtime over the sandbox config (an embedded implicit superuser until a
    /// principal is set).
    fn runtime(&self) -> Runtime {
        Runtime::from_config(&self.config()).expect("from_config")
    }

    /// A second accelerator over this sandbox's store, standing in for another
    /// session that mutates grants concurrently.
    fn accelerator(&self) -> Accelerator {
        Accelerator::open(&self.dir.join("config.yaml")).expect("open accelerator")
    }
}

/// A DuckDB bootstrap `DataSourceConfig` reading `path`.
fn duck_bootstrap(path: &str) -> DataSourceConfig {
    let mut config = BTreeMap::new();
    config.insert("path".to_string(), Value::String(path.to_string()));
    DataSourceConfig {
        name: "duck".to_string(),
        ty: "duckdb".to_string(),
        config,
        capabilities: Vec::new(),
        change_keys: BTreeMap::new(),
    }
}

/// A server config listing one bootstrap superuser `admin`, so authentication
/// (and therefore enforcement for wire principals) is on.
fn server_with_admin() -> ServerConfig {
    let verifier = fq_scram::derive_verifier_random_salt("adminpw", 4096)
        .expect("derive")
        .to_authid_string();
    ServerConfig {
        users: vec![UserCredential {
            name: "admin".to_string(),
            verifier,
            superuser: true,
        }],
        scram_iterations: 4096,
    }
}

/// Run a DDL/admin statement as the embedded superuser, asserting success.
fn run(runtime: &Runtime, sql: &str) {
    runtime
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// Whether `describe` of `sql` binds (the principal is authorized for it).
fn can_see(runtime: &Runtime, sql: &str) -> bool {
    runtime.describe(sql).is_ok()
}

#[test]
fn a_table_grant_authorizes_that_table_and_denies_a_sibling() {
    let sandbox = Sandbox::new("table_grant");
    let runtime = sandbox.runtime();
    run(&runtime, "CREATE USER bob WITH PASSWORD 'pw'");
    run(&runtime, "GRANT SELECT ON TABLE duck.main.orders TO bob");
    runtime.set_principal("bob").expect("set principal");
    // Authorized: the granted table binds and executes.
    assert!(can_see(&runtime, "SELECT id FROM duck.main.orders"));
    let (_schema, batches) = runtime
        .execute("SELECT id FROM duck.main.orders")
        .expect("authorized query runs");
    let rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 2);
    // The sibling table is denied - and the deny is INDISTINGUISHABLE from a
    // genuinely missing table (existence non-leak).
    let denied = runtime
        .describe("SELECT id FROM duck.main.lineitem")
        .unwrap_err()
        .to_string();
    let missing = runtime
        .describe("SELECT id FROM duck.main.ghost")
        .unwrap_err()
        .to_string();
    assert!(denied.contains("not found"), "deny wording: {denied}");
    assert!(missing.contains("not found"), "missing wording: {missing}");
    assert_eq!(
        denied.replace("lineitem", "T"),
        missing.replace("ghost", "T"),
        "an unauthorized table must read exactly like a missing one"
    );
}

#[test]
fn a_datasource_grant_covers_every_table_under_it() {
    let sandbox = Sandbox::new("ds_grant");
    let runtime = sandbox.runtime();
    run(&runtime, "CREATE USER dana WITH PASSWORD 'pw'");
    run(&runtime, "GRANT SELECT ON DATASOURCE duck TO dana");
    runtime.set_principal("dana").expect("set principal");
    assert!(can_see(&runtime, "SELECT id FROM duck.main.orders"));
    assert!(can_see(&runtime, "SELECT id FROM duck.main.lineitem"));
}

#[test]
fn a_public_grant_authorizes_every_principal() {
    let sandbox = Sandbox::new("public_grant");
    let runtime = sandbox.runtime();
    run(&runtime, "CREATE USER eve WITH PASSWORD 'pw'");
    run(&runtime, "GRANT SELECT ON TABLE duck.main.orders TO PUBLIC");
    runtime.set_principal("eve").expect("set principal");
    // eve holds no grant of her own; PUBLIC's covers her.
    assert!(can_see(&runtime, "SELECT id FROM duck.main.orders"));
    assert!(!can_see(&runtime, "SELECT id FROM duck.main.lineitem"));
}

#[test]
fn a_superuser_bypasses_every_grant_check() {
    let sandbox = Sandbox::new("su_bypass");
    let runtime = sandbox.runtime();
    run(&runtime, "CREATE USER root WITH PASSWORD 'pw' SUPERUSER");
    runtime.set_principal("root").expect("set principal");
    // No grant to root, yet a superuser sees every table.
    assert!(can_see(&runtime, "SELECT id FROM duck.main.orders"));
    assert!(can_see(&runtime, "SELECT id FROM duck.main.lineitem"));
}

#[test]
fn a_revoke_by_another_session_denies_the_next_query() {
    let sandbox = Sandbox::new("live_revoke");
    let runtime = sandbox.runtime();
    run(&runtime, "CREATE USER carl WITH PASSWORD 'pw'");
    run(&runtime, "GRANT SELECT ON TABLE duck.main.orders TO carl");
    runtime.set_principal("carl").expect("set principal");
    assert!(can_see(&runtime, "SELECT id FROM duck.main.orders"));
    // A second session revokes; the generation bump makes it visible at carl's
    // NEXT query with no reconnect.
    let peer = sandbox.accelerator();
    assert!(peer
        .revoke("carl", "SELECT", "table", "duck.main.orders")
        .expect("revoke"));
    assert!(!can_see(&runtime, "SELECT id FROM duck.main.orders"));
}

#[test]
fn admin_ddl_is_superuser_only() {
    let sandbox = Sandbox::new("su_only");
    let runtime = sandbox.runtime();
    run(&runtime, "CREATE USER fred WITH PASSWORD 'pw'");
    runtime.set_principal("fred").expect("set principal");
    let error = runtime
        .execute("CREATE USER mallory WITH PASSWORD 'pw'")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("requires superuser"),
        "non-superuser DDL error: {error}"
    );
}

#[test]
fn bootstrap_refuses_to_start_without_a_superuser() {
    let sandbox = Sandbox::new("no_su");
    let accel = sandbox.accelerator();
    let verifier = fq_scram::derive_verifier_random_salt("pw", 4096)
        .expect("derive")
        .to_authid_string();
    let users = vec![UserCredential {
        name: "onlyuser".to_string(),
        verifier,
        superuser: false,
    }];
    fq_runtime::import_bootstrap_users(&accel, &users).expect("import");
    let error = fq_runtime::require_a_superuser(&accel)
        .unwrap_err()
        .to_string();
    assert!(error.contains("superuser"), "refuse-start message: {error}");
}
