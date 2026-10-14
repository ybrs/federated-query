//! Compare nested Parquet queries with DuckDB's expression and expansion semantics.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::array::{Array, RecordBatch};
use arrow::util::display::array_value_to_string;
use fq_common::{load_config, DataType};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;

/// Allocate independent source names and state directories for concurrent tests.
fn sandbox() -> (PathBuf, String) {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let index = COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("nested_{}_{index}", std::process::id());
    let path = std::env::temp_dir().join(&name);
    std::fs::create_dir(&path).expect("create nested fixture directory");
    (path, name)
}

/// Build lists of structs with repeated entries, missing values, and empty parents.
fn fixture() -> (Runtime, DuckDbSource, String) {
    let (path, name) = sandbox();
    let source = DuckDbSource::open_in_memory("nested_oracle").expect("oracle");
    source.execute_batch(SEED).expect("seed nested data");
    source
        .execute_batch(&format!(
            "COPY events TO '{}' (FORMAT PARQUET)",
            path.join("events.parquet").display()
        ))
        .expect("write parquet");
    let config = write_fixture_config(&path, &name);
    let runtime =
        Runtime::from_config(&load_config(config.to_str().unwrap()).expect("load config"))
            .expect("register nested schema");
    (runtime, source, name)
}

/// Persist a config beside the fixture so views survive a fresh runtime.
fn write_fixture_config(path: &std::path::Path, name: &str) -> PathBuf {
    let config = path.join("config.yaml");
    std::fs::write(
        &config,
        format!(
            "datasources:\n  {name}:\n    type: parquet\n    dir: '{}'\n",
            path.display()
        ),
    )
    .expect("write config");
    config
}

/// Preserve nullable cells while comparing values independently of batch boundaries.
fn rows(batches: &[RecordBatch]) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    for batch in batches {
        for index in 0..batch.num_rows() {
            rows.push(row(batch, index));
        }
    }
    rows
}

/// Read one result row, keeping SQL NULL distinct from an empty string.
fn row(batch: &RecordBatch, index: usize) -> Vec<Option<String>> {
    let mut values = Vec::new();
    for column in batch.columns() {
        let value = if column.is_null(index) {
            None
        } else {
            Some(array_value_to_string(column, index).expect("format value"))
        };
        values.push(value);
    }
    values
}

/// Execute the same query against a federated Parquet source and the DuckDB oracle.
fn assert_oracle(runtime: &Runtime, source: &DuckDbSource, name: &str, sql: &str) {
    assert_oracle_sql(runtime, source, name, sql, sql);
}

/// Compare equivalent SQL forms when DuckDB's lateral binder restricts a valid predicate.
fn assert_oracle_sql(
    runtime: &Runtime,
    source: &DuckDbSource,
    name: &str,
    sql: &str,
    oracle_sql: &str,
) {
    let federated = sql.replace("events", &format!("{name}.main.events"));
    let (schema, actual) = runtime
        .execute(&federated)
        .unwrap_or_else(|error| panic!("{federated}: {error}"));
    let connection = source.clone_connection().expect("oracle connection");
    let mut statement = connection.prepare(oracle_sql).expect("oracle query");
    let reader = statement.query_arrow([]).expect("oracle execution");
    let expected_schema = reader.get_schema();
    let expected: Vec<RecordBatch> = reader.collect();
    assert_eq!(rows(&actual), rows(&expected), "{sql}");
    assert_result_schema(&schema, &expected_schema, sql);
}

/// Compare result width and scalar meanings independently of source batch boundaries.
fn assert_result_schema(
    schema: &arrow::datatypes::Schema,
    expected_schema: &arrow::datatypes::Schema,
    sql: &str,
) {
    assert_eq!(
        schema.fields().len(),
        expected_schema.fields().len(),
        "{sql}: result width"
    );
    for (actual, expected) in schema.fields().iter().zip(expected_schema.fields()) {
        assert_eq!(
            DataType::from_arrow(actual.data_type()),
            DataType::from_arrow(expected.data_type()),
            "{sql}: result type"
        );
    }
}

const SEED: &str = "CREATE TABLE events AS SELECT * FROM (VALUES
    (1, 'u1', 1000000::BIGINT, 'view', {'country':'DE'},
      [{'key':'screen', 'value':{'string_value':'home','int_value':NULL::BIGINT}},
       {'key':'screen', 'value':{'string_value':'again','int_value':NULL::BIGINT}}], [1,2], [10]),
    (2, 'u1', 2000000::BIGINT, 'buy', {'country':'US'},
      [{'key':'coins', 'value':{'string_value':NULL,'int_value':5::BIGINT}}], [3], [20,30]),
    (3, 'u2', 3000000::BIGINT, 'view', {'country':NULL}, [], [], [40]),
    (4, 'u2', 4000000::BIGINT, 'buy', NULL, NULL, NULL, NULL)
  ) v(id, actor, ts, event, geo, event_params, xs, ys)";

#[test]
fn scalar_projection_and_struct_fields_match_duckdb() {
    let (runtime, oracle, name) = fixture();
    for sql in [
        "SELECT id, actor FROM events ORDER BY id",
        "SELECT id, geo.country FROM events ORDER BY id",
        "SELECT e.id, e.geo.country FROM events e ORDER BY e.id",
        "SELECT id FROM events WHERE geo.country = 'DE' ORDER BY id",
    ] {
        assert_oracle(&runtime, &oracle, &name, sql);
    }
}

#[test]
fn select_lists_zip_with_null_padding_and_preserve_duplicates() {
    let (runtime, oracle, name) = fixture();
    for sql in [
        "SELECT id, UNNEST(xs) AS x, UNNEST(ys) AS y FROM events ORDER BY id, x, y",
        "SELECT id, UNNEST(xs) + 1 AS x FROM events ORDER BY id, x",
        "SELECT actor, UNNEST(ARRAY_AGG(id ORDER BY id)) AS x FROM events GROUP BY actor ORDER BY actor, x",
        "SELECT a.actor, p.item, p.pos FROM (SELECT actor, ARRAY_AGG(id ORDER BY id DESC) AS ids FROM events GROUP BY actor) a CROSS JOIN UNNEST(a.ids) WITH ORDINALITY AS p(item,pos) ORDER BY a.actor, p.pos",
        "SELECT UNNEST([1,2]) AS x, UNNEST([10]) AS y ORDER BY x",
        "SELECT UNNEST([]) AS x",
        "SELECT UNNEST(NULL) AS x",
        "SELECT UNNEST([NULL]) AS x",
        "SELECT UNNEST([1,2147483648]) AS x ORDER BY x",
        "SELECT DISTINCT UNNEST([1,1,2]) AS x ORDER BY x LIMIT 1 OFFSET 1",
        "SELECT __fedq_unnest_0.id, UNNEST(xs) AS x FROM events AS __fedq_unnest_0 ORDER BY id,x",
    ] {
        assert_oracle(&runtime, &oracle, &name, sql);
    }
}

#[test]
fn from_expansion_and_ordinality_match_duckdb() {
    let (runtime, oracle, name) = fixture();
    for sql in [
        "SELECT e.id, p.item.key, p.item.value.string_value FROM events e CROSS JOIN UNNEST(e.event_params) AS p(item) ORDER BY e.id, p.item.key, p.item.value.string_value",
        "SELECT e.id, p.item.key FROM events e CROSS JOIN UNNEST(e.event_params) AS p(item) ORDER BY e.id, p.item.key LIMIT 2",
        "SELECT e.id, x.item, y.item FROM events e CROSS JOIN UNNEST(e.xs) AS x(item) CROSS JOIN UNNEST(e.ys) AS y(item) ORDER BY e.id, x.item, y.item",
        "SELECT e.id, p.item.key, p.pos FROM events e LEFT JOIN UNNEST(e.event_params) WITH ORDINALITY AS p(item,pos) ON TRUE ORDER BY e.id, p.pos",
        "SELECT e.id, p.item.key, p.pos FROM events e LEFT JOIN UNNEST(e.event_params) WITH ORDINALITY AS p(item,pos) ON p.item.key = 'screen' ORDER BY e.id, p.pos",
        "SELECT * FROM UNNEST([1,2]) WITH ORDINALITY AS p(item,pos) ORDER BY pos",
    ] {
        assert_oracle(&runtime, &oracle, &name, sql);
    }
}

/// A false outer match preserves every parent even when its list contains elements.
#[test]
fn outer_false_predicate_preserves_every_parent() {
    let (runtime, oracle, name) = fixture();
    let sql = "SELECT e.id, p.item.key FROM events e LEFT JOIN UNNEST(e.event_params) AS p(item) ON FALSE ORDER BY e.id";
    let oracle_sql = "SELECT e.id, p.item.key FROM events e LEFT JOIN (SELECT UNNEST(event_params) AS item FROM events) p ON FALSE ORDER BY e.id";
    assert_oracle_sql(&runtime, &oracle, &name, sql, oracle_sql);
}

#[test]
fn invalid_nested_queries_raise() {
    let (runtime, _, name) = fixture();
    for sql in [
        "SELECT geo.missing FROM events",
        "SELECT e.geo.missing FROM events e",
        "SELECT bogus.geo.country FROM events e",
        "SELECT UNNEST(id) FROM events",
        "SELECT p.item.missing FROM events e CROSS JOIN UNNEST(e.event_params) AS p(item)",
        "SELECT * FROM UNNEST([1]) AS p(item,item)",
        "SELECT e.item FROM events e CROSS JOIN UNNEST(e.xs) AS e(item)",
    ] {
        let sql = sql.replace("events", &format!("{name}.main.events"));
        assert!(
            runtime.execute(&sql).is_err(),
            "invalid query returned rows: {sql}"
        );
    }
}

#[test]
fn nested_types_survive_union_views_and_runtime_restart() {
    let (runtime, oracle, name) = fixture();
    runtime.execute(&format!("CREATE VIEW combined AS SELECT * FROM {name}.main.events WHERE id < 3 UNION ALL SELECT * FROM {name}.main.events WHERE id >= 3")).expect("create combined view");
    drop(runtime);
    let config = std::env::temp_dir().join(&name).join("config.yaml");
    let runtime =
        Runtime::from_config(&load_config(config.to_str().unwrap()).expect("reload config"))
            .expect("reopen nested view");
    let sql = "SELECT id, UNNEST(xs) AS x FROM combined ORDER BY id, x";
    let (_, actual) = runtime.execute(sql).expect("expand nested view");
    let connection = oracle.clone_connection().expect("oracle connection");
    let mut statement = connection
        .prepare(&sql.replace("combined", "events"))
        .expect("oracle view");
    let expected: Vec<RecordBatch> = statement
        .query_arrow([])
        .expect("oracle execution")
        .collect();
    assert_eq!(rows(&actual), rows(&expected));
}
