//! Compare native JSON scalar, list and error behavior directly with DuckDB.

const PATH_DOCUMENT: &str =
    r#"{"a.b":{"x":[1,null,"three"]},"a/b":{"~x":7},"From":"A","from":"b","a":{"b":2},"b":1}"#;
const LOOKUP_PATHS: &[&str] = &[
    r#"$."a.b".x[0]"#,
    r#"$."a.b".x[-1]"#,
    r#"$."a.b".x[#-1]"#,
    r#"$."a.b".x[*]"#,
    "$.*",
    "$..b",
    "$..*",
    "$.**",
    "$..*.b",
    "$.**.b",
    r#"$."*""#,
    "*",
    "/a~1b/~0x",
    "From",
    "from",
    "",
    "$.missing",
];
const STRING_CASES: &[(&str, &str)] = &[
    (r#""GAME_START""#, "$"),
    (r#""line\n\u00e9\u0000end""#, "$"),
    (r#"{"b":2,"a":1}"#, "$"),
    (r#"{"x":1,"x":2}"#, "$.x"),
    ("1e2", "$"),
    ("1.00", "$"),
    ("1e400", "$"),
    ("NaN", "$"),
    ("18446744073709551616", "$"),
    ("null", "$"),
    ("[1,2]", "$[-0]"),
    ("[1,2]", "$[#-0]"),
    ("[1,2]", "$[#]"),
    ("[1,2]", "$[9223372036854775808]"),
    ("[1,2]", "$[9999999999999999999]"),
    (r#"{"a\\nb":1,"a\nb":2}"#, r#"$."a\nb""#),
    (r#"{"a\\u0062":3,"ab":4}"#, r#"$."a\u0062""#),
    (r#"{"a\"b":5}"#, r#"$."a\"b""#),
    (r#"{"a":{"x":{"b":1}},"z":{"b":2}}"#, "$..b"),
    ("true", "$"),
    (r#"{"a":1,}"#, "$"),
    (r#"{"a":"\u00e9", "b": 1e2}"#, "$"),
];

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::prelude::SessionContext;
use duckdb::Connection;
use std::sync::Arc;

/// Run native expressions with a small executor and the production UDF registration.
fn native(sql: &str) -> Result<Vec<RecordBatch>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let context = SessionContext::new();
    context.register_udf(super::udf());
    runtime.block_on(async { context.sql(sql).await?.collect().await })
}

/// Preserve NULLs and list values while ignoring source-specific output field names.
fn values(batches: &[RecordBatch]) -> Vec<Option<String>> {
    let mut values = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let column = batch.column(0);
            let value = if column.is_null(row) {
                None
            } else {
                Some(arrow::util::display::array_value_to_string(column, row).unwrap())
            };
            values.push(value);
        }
    }
    values
}

/// Compare SQL result families without assigning meaning to Arrow list-child field names.
fn result_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(child) => DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            result_type(child.data_type()),
            true,
        ))),
        other => other.clone(),
    }
}

/// Require the native scalar expression to match DuckDB's values and physical result family.
fn matches_oracle(sql: &str) {
    let connection = Connection::open_in_memory().unwrap();
    let mut statement = connection.prepare(sql).unwrap();
    let expected: Vec<_> = statement.query_arrow([]).unwrap().collect();
    let actual = native(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    assert_eq!(values(&actual), values(&expected), "{sql}");
    assert_eq!(
        result_type(actual[0].column(0).data_type()),
        result_type(expected[0].column(0).data_type()),
        "{sql}"
    );
}

/// Quote fixture values as SQL text without changing JSON escapes.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Exercise root decoding, ordered objects, duplicate keys, large numbers and path syntax.
#[test]
fn json_strings_match_duckdb_lookup_and_serialization() {
    for (document, path) in STRING_CASES {
        matches_oracle(&format!(
            "SELECT json_extract_string({}, {})",
            literal(document),
            literal(path)
        ));
    }
}

/// Cover array and object wildcards, recursive descent, JSON Pointer and case-sensitive keys.
#[test]
fn json_paths_match_duckdb() {
    for path in LOOKUP_PATHS {
        matches_oracle(&format!(
            "SELECT json_extract_string({}, {})",
            literal(PATH_DOCUMENT),
            literal(path)
        ));
    }
    matches_oracle("SELECT json_extract_string('[1,2]', -1)");
    matches_oracle("SELECT json_extract_string('{\"a\":1}', ARRAY['$.a','$.missing'])");
    matches_oracle("SELECT json_extract_path_text('{\"a\":1}', '$.a')");
    matches_oracle("SELECT json_extract_string(NULL, '$')");
    matches_oracle("SELECT json_extract_string('bad', NULL)");
}

/// Read dynamic paths row by row while preserving strings, missing paths and SQL NULLs.
#[test]
fn dynamic_json_paths_match_duckdb() {
    matches_oracle("SELECT json_extract_string(j,p) FROM (VALUES ('{\"a\":1}','$.a'), ('{\"A\":2}','$.a'), (NULL,'$'), ('bad',NULL)) AS input(j,p)");
}

/// Both engines must reject malformed JSON and unsupported path syntax.
#[test]
fn invalid_json_and_paths_raise() {
    for sql in [
        "SELECT json_extract_string('{', '$')",
        "SELECT json_extract_string('{}', '$[')",
        "SELECT json_extract_string('[]', '$[+1]')",
        "SELECT json_extract_string('[]', '$[-+1]')",
        "SELECT json_extract_string('[]', '$[10000000000000000000]')",
        "SELECT json_extract_string('{}', '$.*x')",
        "SELECT json_extract_string('{}', '$.\"\"')",
        "SELECT json_extract_string('{}', ARRAY['$.*'])",
        "SELECT json_extract_string('{}', ARRAY['$.a',NULL])",
        "SELECT json_extract_string('/*comment*/1', '$')",
    ] {
        let connection = Connection::open_in_memory().unwrap();
        assert!(
            connection.query_row(sql, [], |_| Ok(())).is_err(),
            "oracle accepted {sql}"
        );
        assert!(native(sql).is_err(), "native accepted {sql}");
    }
}
