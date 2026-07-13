//! Cross-source CTE (WITH clause) execution, end to end through a `Runtime`.
//!
//! A CTE whose references span data sources cannot push to one source as a single
//! `WITH`. The producer path materializes the body ONCE (a `PhysicalCte`) and
//! every reference reads that one allocation (a `PhysicalCteScan`); the
//! CTE-vs-other-source joins run in the merge engine. Ports the six non-recursive
//! tests of `tests/e2e_pushdown/test_cross_source_ctes.py`.
//!
//! The two sources are two real DuckDB files (orders on one, customers on the
//! other); a join across them is cross-source, so it exercises the same producer/
//! reference path the Python multi-source fixture did. Each test seeds its OWN
//! files under its OWN datasource names, because the exec-plane datasource
//! registry is process-wide and keyed by name.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use fq_common::{Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// The orders table DDL + rows (order_id, customer_id, region) used by the ports.
const ORDERS_SQL: &str = "CREATE TABLE orders (\
    order_id INTEGER, product_id INTEGER, customer_id INTEGER, \
    quantity INTEGER, region VARCHAR); \
    INSERT INTO orders VALUES \
    (1, 101, 1, 3, 'NA'), (2, 102, 2, 5, 'EU'), (3, 103, 3, 2, 'APAC'), \
    (4, 104, 4, 1, 'NA'), (5, 101, 5, 4, 'EU'), (6, 102, 1, 7, 'NA'), \
    (7, 103, 2, 3, 'APAC'), (8, 104, 3, 6, 'EU'), (9, 105, 4, 9, 'NA'), \
    (10, 106, 5, 8, 'EU');";

/// The customers table DDL + rows (customer_id, segment).
const CUSTOMERS_SQL: &str = "CREATE TABLE customers (\
    customer_id INTEGER, segment VARCHAR, loyalty VARCHAR); \
    INSERT INTO customers VALUES \
    (1, 'enterprise', 'gold'), (2, 'enterprise', 'silver'), (3, 'smb', 'silver'), \
    (4, 'consumer', 'bronze'), (5, 'consumer', 'gold');";

/// A unique temp DuckDB file path (per process run), so parallel tests never share
/// a file or collide in the path-keyed exec cache.
fn temp_duck(tag: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir()
        .join(format!("fq_cte_{tag}_{pid}_{id}.duckdb"))
        .to_str()
        .expect("temp path is valid UTF-8")
        .to_string()
}

/// Create a DuckDB file and seed it with `ddl`, then close the seeding handle so
/// the runtime opens the finished file cleanly.
fn seed_duck(path: &str, ddl: &str) {
    let source = DuckDbSource::open("seed", path).expect("open seed duckdb");
    source.execute_batch(ddl).expect("seed duckdb");
    drop(source);
}

/// One DuckDB datasource block for a config.
fn duck_datasource(name: &str, path: &str) -> DataSourceConfig {
    let mut params = BTreeMap::new();
    params.insert("path".to_string(), Value::String(path.to_string()));
    DataSourceConfig {
        name: name.to_string(),
        ty: "duckdb".to_string(),
        config: params,
        capabilities: Vec::new(),
    }
}

/// A two-source runtime: `<tag>_orders` holds orders, `<tag>_customers` holds
/// customers. Returns the runtime and the two datasource names for the SQL.
fn two_source_runtime(tag: &str) -> (Runtime, String, String) {
    let orders_name = format!("{tag}_orders");
    let customers_name = format!("{tag}_customers");
    let orders_path = temp_duck(&orders_name);
    let customers_path = temp_duck(&customers_name);
    seed_duck(&orders_path, ORDERS_SQL);
    seed_duck(&customers_path, CUSTOMERS_SQL);

    let mut datasources = BTreeMap::new();
    datasources.insert(
        orders_name.clone(),
        duck_datasource(&orders_name, &orders_path),
    );
    datasources.insert(
        customers_name.clone(),
        duck_datasource(&customers_name, &customers_path),
    );
    let config = Config {
        datasources,
        optimizer: OptimizerConfig::default(),
        executor: ExecutorConfig::default(),
        cost: CostConfig::default(),
        source_path: None,
    };
    let runtime = Runtime::from_config(&config).expect("from_config");
    (runtime, orders_name, customers_name)
}

/// Downcast one column to `Int32Array`.
fn i32_col(batch: &RecordBatch, index: usize) -> &Int32Array {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("column is Int32")
}

/// Downcast one column to `StringArray`.
fn str_col(batch: &RecordBatch, index: usize) -> &StringArray {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column is Utf8")
}

/// The single integer column's values (Int32 or Int64) as i64, sorted. The merge
/// engine widens a constant projection to Int64 while a source scan keeps Int32,
/// so this reads either.
fn sorted_ints(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        push_ints(batch.column(0).as_ref(), &mut out);
    }
    out.sort_unstable();
    out
}

/// Append one integer column's values (Int32 or Int64) as i64.
fn push_ints(column: &dyn Array, out: &mut Vec<i64>) {
    if let Some(int32) = column.as_any().downcast_ref::<Int32Array>() {
        for row in 0..int32.len() {
            out.push(i64::from(int32.value(row)));
        }
        return;
    }
    let int64 = column
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column is Int32 or Int64");
    for row in 0..int64.len() {
        out.push(int64.value(row));
    }
}

/// The `(Int32, Utf8)` rows, sorted.
fn sorted_i32_str(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut out = Vec::new();
    for batch in batches {
        let keys = i32_col(batch, 0);
        let text = str_col(batch, 1);
        for row in 0..batch.num_rows() {
            out.push((keys.value(row), text.value(row).to_string()));
        }
    }
    out.sort();
    out
}

/// The EXPLAIN plan lines for `sql` (the `plan` text column, one line per row).
fn explain_lines(runtime: &Runtime, sql: &str) -> Vec<String> {
    let (_schema, batches) = runtime
        .execute(&format!("EXPLAIN {sql}"))
        .expect("explain query");
    let mut lines = Vec::new();
    for batch in &batches {
        let column = str_col(batch, 0);
        for row in 0..batch.num_rows() {
            lines.push(column.value(row).trim().to_string());
        }
    }
    lines
}

#[test]
fn body_single_source_child_joins_other_source() {
    // A single-source CTE body (orders WHERE region='EU') joined in the child
    // against a second source (customers).
    let (runtime, orders, customers) = two_source_runtime("cte1");
    let sql = format!(
        "WITH eu AS (\
           SELECT order_id, customer_id FROM {orders}.main.orders WHERE region = 'EU') \
         SELECT e.order_id, c.segment FROM eu e \
         JOIN {customers}.main.customers c ON e.customer_id = c.customer_id"
    );
    let (_schema, batches) = runtime.execute(&sql).expect("cross-source cte");
    // EU orders 2,5,8,10 -> customers 2,5,3,5 -> segments enterprise,consumer,smb,consumer.
    assert_eq!(
        sorted_i32_str(&batches),
        vec![
            (2, "enterprise".to_string()),
            (5, "consumer".to_string()),
            (8, "smb".to_string()),
            (10, "consumer".to_string()),
        ]
    );
}

#[test]
fn body_itself_cross_source() {
    // The CTE body is a cross-source join; the child filters its output.
    let (runtime, orders, customers) = two_source_runtime("cte2");
    let sql = format!(
        "WITH joined AS (\
           SELECT o.order_id, c.segment FROM {orders}.main.orders o \
           JOIN {customers}.main.customers c ON o.customer_id = c.customer_id) \
         SELECT order_id FROM joined WHERE segment = 'enterprise'"
    );
    let (_schema, batches) = runtime.execute(&sql).expect("cross-source cte body");
    // customers 1 and 2 are enterprise; their orders are 1, 2, 6, 7.
    assert_eq!(sorted_ints(&batches), vec![1, 2, 6, 7]);
}

#[test]
fn multiple_references_materialize_once() {
    // A CTE referenced twice is materialized once: one producer, two scans, both
    // sharing the SAME producer id in the plan dump.
    let (runtime, orders, customers) = two_source_runtime("cte3");
    let sql = format!(
        "WITH cust AS (\
           SELECT customer_id, segment FROM {customers}.main.customers \
           WHERE segment = 'enterprise') \
         SELECT o.order_id FROM {orders}.main.orders o \
         JOIN cust a ON o.customer_id = a.customer_id \
         JOIN cust b ON o.customer_id = b.customer_id"
    );
    let (_schema, batches) = runtime.execute(&sql).expect("multi-reference cte");
    assert_eq!(sorted_ints(&batches), vec![1, 2, 6, 7]);

    let lines = explain_lines(&runtime, &sql);
    let scans: Vec<&String> = lines
        .iter()
        .filter(|line| line.starts_with("CteScan ["))
        .collect();
    // Two references read the CTE.
    assert_eq!(scans.len(), 2, "two references in plan:\n{lines:#?}");
    // Every CTE producer AND reference line resolves to ONE shared-producer id, so
    // the body is materialized exactly once and both references read that one
    // allocation. (The producer prints under each reference in the tree dump; the
    // shared id, not a single printed line, is what proves the sharing.)
    let mut producer_ids = std::collections::BTreeSet::new();
    for line in &lines {
        if line.starts_with("Cte [") || line.starts_with("CteScan [") {
            producer_ids.insert(producer_id_of(line).to_string());
        }
    }
    assert_eq!(
        producer_ids.len(),
        1,
        "all references share one materialized producer:\n{lines:#?}"
    );
}

#[test]
fn cross_source_explicit_column_list() {
    // A cross-source CTE with an explicit column list relabels its output.
    let (runtime, orders, customers) = two_source_runtime("cte4");
    let sql = format!(
        "WITH t(cid, seg) AS (\
           SELECT customer_id, segment FROM {customers}.main.customers \
           WHERE segment = 'enterprise') \
         SELECT o.order_id, t.seg FROM {orders}.main.orders o \
         JOIN t ON o.customer_id = t.cid"
    );
    let (_schema, batches) = runtime.execute(&sql).expect("explicit column list cte");
    assert_eq!(
        sorted_i32_str(&batches),
        vec![
            (1, "enterprise".to_string()),
            (2, "enterprise".to_string()),
            (6, "enterprise".to_string()),
            (7, "enterprise".to_string()),
        ]
    );
}

#[test]
fn constant_cte_in_multi_source_catalog() {
    // A scan-less CTE in a multi-source catalog has no single source to push to,
    // so it materializes through the producer path: one producer, correct row.
    let (runtime, _orders, _customers) = two_source_runtime("cte5");
    let sql = "WITH x AS (SELECT 1 AS n) SELECT n FROM x";
    let (_schema, batches) = runtime.execute(sql).expect("constant cte");
    assert_eq!(sorted_ints(&batches), vec![1]);

    let lines = explain_lines(&runtime, sql);
    let producers: Vec<&String> = lines
        .iter()
        .filter(|line| line.starts_with("Cte ["))
        .collect();
    assert_eq!(producers.len(), 1, "one producer in plan:\n{lines:#?}");
}

#[test]
fn producer_and_scan_labels() {
    // The plan dump names the CTE on its producer and on every reference, so the
    // materialize-once structure is readable (ports test_producer_and_scan_repr).
    let (runtime, orders, customers) = two_source_runtime("cte6");
    let sql = format!(
        "WITH cust AS (\
           SELECT customer_id, segment FROM {customers}.main.customers \
           WHERE segment = 'enterprise') \
         SELECT o.order_id FROM {orders}.main.orders o \
         JOIN cust a ON o.customer_id = a.customer_id \
         JOIN cust b ON o.customer_id = b.customer_id"
    );
    let lines = explain_lines(&runtime, &sql);
    assert!(
        lines.iter().any(|line| line.starts_with("Cte [cust] #")),
        "producer labelled with its CTE name:\n{lines:#?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("CteScan [cust] #")),
        "reference labelled with its CTE name:\n{lines:#?}"
    );
}

/// The `#<id>` shared-producer id trailing a `Cte`/`CteScan` plan line.
fn producer_id_of(line: &str) -> &str {
    match line.rsplit_once('#') {
        Some((_, id)) => id,
        None => panic!("plan line has no shared-producer id: {line}"),
    }
}
