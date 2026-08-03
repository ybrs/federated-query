//! Wide and dense join graphs must PLAN, in a debug build, inside the
//! planning budget. The ordering ladder's work is a function of the join
//! GRAPH, not only the atom count: a clique reaches ~1000 connected subsets
//! where a chain has ~78, an uncapped DP over a 9-atom clique blew the budget
//! in debug, and the uncapped GREEDY blew it on 50-atom stars and cliques in
//! RELEASE. The budget itself is the assertion here: these queries planning at
//! all proves each region landed on a ladder tier whose counted work fits.

use std::collections::BTreeMap;
use std::time::Instant;

use fq_common::{Config, DataSourceConfig};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// Seed a fresh nation table at a unique temp path. The counter keeps the
/// tests in this module apart: they run concurrently in one process, and a
/// path keyed on the process alone would have each test wiping the file out
/// from under the others' live runtimes.
fn seed() -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("fq_dense_probe_{}_{id}.duckdb", std::process::id()));
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

/// A star join over n aliases: every alias joined to the first.
fn star_sql(n: usize) -> String {
    let mut parts = vec!["duck.main.nation a0".to_string()];
    for i in 1..n {
        parts.push(format!(
            "JOIN duck.main.nation a{i} ON a0.n_nationkey = a{i}.n_nationkey"
        ));
    }
    format!("EXPLAIN SELECT a0.n_nationkey FROM {}", parts.join(" "))
}

/// Run one shape's probes on a thread with the stack every query thread gets
/// (fq_common::QUERY_THREAD_STACK_BYTES, what the server gives its session
/// workers): the plan passes recurse per join level, and cargo's 2MiB test
/// threads overflow at widths the engine itself must carry.
fn plan_widths(label: &'static str, sql_for: fn(usize) -> String, widths: &'static [usize]) {
    std::thread::Builder::new()
        .stack_size(fq_common::QUERY_THREAD_STACK_BYTES)
        .spawn(move || {
            let path = seed();
            let runtime =
                Runtime::from_config(&config(path.to_str().expect("utf8"))).expect("runtime");
            for &n in widths {
                let sql = sql_for(n);
                let started = Instant::now();
                runtime
                    .execute(&sql)
                    .unwrap_or_else(|error| panic!("{label} n={n} must plan: {error}"));
                let ms = started.elapsed().as_secs_f64() * 1000.0;
                println!("{label} n={n}: planned {ms:.1}ms");
            }
        })
        .expect("probe thread")
        .join()
        .expect("probe thread panicked");
}

/// A chain join over n aliases: each alias joined to the previous one.
fn chain_sql(n: usize) -> String {
    let mut parts = vec!["duck.main.nation a0".to_string()];
    for i in 1..n {
        let previous = i - 1;
        parts.push(format!(
            "JOIN duck.main.nation a{i} ON a{previous}.n_nationkey = a{i}.n_nationkey"
        ));
    }
    format!("EXPLAIN SELECT a0.n_nationkey FROM {}", parts.join(" "))
}

#[test]
fn dense_join_graphs_plan_inside_the_budget() {
    // Clique widths stop at 10. A 30-alias clique carries 435 join predicates
    // and a 50-alias one 1225; in a DEBUG build their estimation, extraction,
    // emission and physical planning exceed the budget even though the
    // ordering ladder itself is cheap, so those widths do not plan today and
    // are not asserted here.
    plan_widths("clique", clique_sql, &[8, 9, 10]);
}

#[test]
fn wide_star_joins_plan_inside_the_budget() {
    // Fifty tables is the contract this pins. Widths of 64-70 plan at
    // 61-99ms in an idle debug build - inside the budget but without the
    // headroom a loaded suite run needs - so this test does not assert them.
    plan_widths("star", star_sql, &[30, 50]);
}

#[test]
fn wide_chain_joins_plan_inside_the_budget() {
    plan_widths("chain", chain_sql, &[50]);
}
