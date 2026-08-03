//! Dense join graphs must PLAN, in a debug build, inside the planning
//! budget. The join-order DP's work is a function of the join GRAPH, not only
//! the atom count: a clique reaches ~1000 connected subsets where a chain has
//! ~78, and an uncapped DP over a 9-atom clique blew the budget in debug while
//! every chain passed. The budget itself is the assertion here: these queries
//! planning at all proves the work cap routed the dense region to greedy.

use std::collections::BTreeMap;
use std::time::Instant;

use fq_common::{Config, DataSourceConfig};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// Seed a fresh nation table at a unique temp path.
fn seed() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("fq_dense_probe_{}.duckdb", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let source = DuckDbSource::open("seed", path.to_str().expect("utf8")).expect("open");
    source
        .execute_batch(
            "CREATE TABLE nation (n_nationkey INTEGER, n_name VARCHAR); \
             INSERT INTO nation SELECT range, 'n' || range FROM range(25);",
        )
        .expect("seed");
    drop(source);
    path
}

/// A single-duck config over the seeded file.
fn config(path: &str) -> Config {
    let mut params = BTreeMap::new();
    params.insert("path".to_string(), Value::String(path.to_string()));
    let mut datasources = BTreeMap::new();
    datasources.insert(
        "duck".to_string(),
        DataSourceConfig {
            name: "duck".to_string(),
            ty: "duckdb".to_string(),
            config: params,
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    Config {
        datasources,
        optimizer: fq_common::OptimizerConfig::default(),
        executor: fq_common::ExecutorConfig::default(),
        cost: fq_common::CostConfig::default(),
        server: fq_common::ServerConfig::default(),
        accelerator: fq_common::AcceleratorConfig::default(),
        catalog: fq_common::CatalogConfig::default(),
        events: fq_common::EventsConfig::default(),
        diagnostics: fq_common::DiagnosticsConfig::default(),
        source_path: None,
    }
}

/// A clique join over n aliases of nation: every pair joined explicitly.
fn clique_sql(n: usize) -> String {
    let mut parts = vec!["duck.main.nation a0".to_string()];
    for i in 1..n {
        let mut conds = Vec::new();
        for j in 0..i {
            conds.push(format!("a{j}.n_nationkey = a{i}.n_nationkey"));
        }
        parts.push(format!(
            "JOIN duck.main.nation a{i} ON {}",
            conds.join(" AND ")
        ));
    }
    format!("EXPLAIN SELECT a0.n_nationkey FROM {}", parts.join(" "))
}

#[test]
fn dense_join_graphs_plan_inside_the_budget() {
    let path = seed();
    let runtime = Runtime::from_config(&config(path.to_str().expect("utf8"))).expect("runtime");
    for n in [8, 9, 10] {
        let sql = clique_sql(n);
        let started = Instant::now();
        runtime
            .execute(&sql)
            .unwrap_or_else(|error| panic!("clique n={n} must plan: {error}"));
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        println!("clique n={n}: planned {ms:.1}ms");
    }
}
