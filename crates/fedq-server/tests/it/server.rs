//! End-to-end: start fedq-server on an ephemeral port over a tiny seeded DuckDB
//! config, connect with a real Postgres client (tokio-postgres), and assert the
//! simple and extended query protocols both work: plain and filtered SELECTs, a
//! bad query surfacing as a Postgres error without dropping the connection,
//! prepared statements with and without parameters, date/timestamp/decimal
//! columns rendering as text, and an unmapped parameter type being refused
//! loudly while the connection survives.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};

use fq_common::{Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig};
use fq_connectors::DuckDbSource;
use serde_yaml::Value;
use tokio::net::TcpListener;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

/// The seed tables for the server tests: `items` (four rows with an integer
/// quantity the filtered queries select on) and `events` (a date, a timestamp,
/// and a decimal column, to exercise the text renderings of those types).
const SEED_SQL: &str = "\
    CREATE TABLE items (id INTEGER, name VARCHAR, qty INTEGER); \
    INSERT INTO items VALUES \
    (1, 'apple', 5), (2, 'banana', 3), (3, 'cherry', 9), (4, 'date', 1); \
    CREATE TABLE events (id INTEGER, day DATE, occurred TIMESTAMP, amount DECIMAL(10,2)); \
    INSERT INTO events VALUES \
    (1, DATE '2021-01-05', TIMESTAMP '2021-01-05 12:34:56', 19.95), \
    (2, DATE '2022-07-13', TIMESTAMP '2022-07-13 00:00:00', 100.00);";

/// A unique temp DuckDB file path per process run, so parallel tests never share
/// a file or collide in the path-keyed exec cache.
fn temp_duck() -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir()
        .join(format!("fedq_server_{pid}_{id}.duckdb"))
        .to_str()
        .expect("temp path is valid UTF-8")
        .to_owned()
}

/// Create a DuckDB file and seed it, then close the seeding handle so the server
/// opens the finished file cleanly.
fn seed_duck(path: &str, ddl: &str) {
    let source = DuckDbSource::open("seed", path).expect("open seed duckdb");
    source.execute_batch(ddl).expect("seed duckdb");
    drop(source);
}

/// A single-DuckDB-source config under the datasource name `shop`, which the
/// `shop.main.items` table references resolve.
fn shop_config(path: &str) -> Config {
    let mut params = BTreeMap::new();
    params.insert("path".to_owned(), Value::String(path.to_owned()));
    let mut datasources = BTreeMap::new();
    datasources.insert(
        "shop".to_owned(),
        DataSourceConfig {
            name: "shop".to_owned(),
            ty: "duckdb".to_owned(),
            config: params,
            capabilities: Vec::new(),
        },
    );
    Config {
        datasources,
        optimizer: OptimizerConfig::default(),
        executor: ExecutorConfig::default(),
        cost: CostConfig::default(),
        source_path: None,
    }
}

/// Seed a fresh DuckDB fixture, start the server on an ephemeral port, and return
/// a connected Postgres client over trust auth.
async fn start_seeded_server() -> Client {
    let path = temp_duck();
    seed_duck(&path, SEED_SQL);
    let config = shop_config(&path);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(fedq_server::serve(config, listener));
    let conn_str = format!(
        "host=127.0.0.1 port={} user=postgres dbname=fedq",
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Collect the `Row` messages of a simple-query result as vectors of the
/// requested column values (as text), skipping command-complete markers.
fn rows(messages: &[SimpleQueryMessage], columns: usize) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            let mut values = Vec::with_capacity(columns);
            for index in 0..columns {
                values.push(row.get(index).expect("column present").to_owned());
            }
            out.push(values);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simple_query_serves_selects_and_surfaces_errors() {
    let client = start_seeded_server().await;

    // Plain SELECT: all four rows, in key order.
    let messages = client
        .simple_query("SELECT id, name, qty FROM shop.main.items ORDER BY id")
        .await
        .expect("plain select");
    let all = rows(&messages, 3);
    assert_eq!(
        all,
        vec![
            vec!["1".to_owned(), "apple".to_owned(), "5".to_owned()],
            vec!["2".to_owned(), "banana".to_owned(), "3".to_owned()],
            vec!["3".to_owned(), "cherry".to_owned(), "9".to_owned()],
            vec!["4".to_owned(), "date".to_owned(), "1".to_owned()],
        ],
    );

    // Filtered SELECT: only items with qty >= 5.
    let messages = client
        .simple_query("SELECT id, name FROM shop.main.items WHERE qty >= 5 ORDER BY id")
        .await
        .expect("filtered select");
    let filtered = rows(&messages, 2);
    assert_eq!(
        filtered,
        vec![
            vec!["1".to_owned(), "apple".to_owned()],
            vec!["3".to_owned(), "cherry".to_owned()],
        ],
    );

    // Error case: an invalid query must surface as a Postgres db error, not a
    // dropped connection.
    let error = client
        .simple_query("SELECT id FROM shop.main.no_such_table")
        .await
        .expect_err("invalid query must error");
    let db_error = error
        .as_db_error()
        .expect("error surfaced as a Postgres db error");
    assert!(
        !db_error.message().is_empty(),
        "the engine's error message must be carried through, got empty"
    );

    // The connection survived the error: a follow-up query still works.
    let messages = client
        .simple_query("SELECT count(*) AS n FROM shop.main.items")
        .await
        .expect("query after error");
    let counted = rows(&messages, 1);
    assert_eq!(counted, vec![vec!["4".to_owned()]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_serves_parameterless_prepared_statement() {
    let client = start_seeded_server().await;

    // tokio-postgres uses the extended protocol (Parse/Bind/Describe/Execute) for
    // prepared statements. A parameterless one describes its columns by planning
    // and returns typed rows.
    let statement = client
        .prepare("SELECT id, name, qty FROM shop.main.items ORDER BY id")
        .await
        .expect("prepare");
    let result = client.query(&statement, &[]).await.expect("execute");
    assert_eq!(result.len(), 4);
    let first = &result[0];
    let id: i32 = first.get("id");
    let name: &str = first.get("name");
    let qty: i32 = first.get("qty");
    assert_eq!((id, name, qty), (1, "apple", 5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_serves_integer_parameter() {
    let client = start_seeded_server().await;

    // A declared INT4 parameter is bound (binary-encoded by tokio-postgres) and
    // spliced into the SQL; only items with qty >= 5 come back.
    let statement = client
        .prepare_typed(
            "SELECT id, name FROM shop.main.items WHERE qty >= $1 ORDER BY id",
            &[Type::INT4],
        )
        .await
        .expect("prepare typed");
    let result = client.query(&statement, &[&5i32]).await.expect("execute");
    let ids: Vec<i32> = result.iter().map(|row| row.get::<_, i32>("id")).collect();
    assert_eq!(ids, vec![1, 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_serves_text_parameter() {
    let client = start_seeded_server().await;

    // A declared TEXT parameter is quoted into the SQL as a string literal.
    let statement = client
        .prepare_typed(
            "SELECT id FROM shop.main.items WHERE name = $1",
            &[Type::TEXT],
        )
        .await
        .expect("prepare typed");
    let result = client
        .query(&statement, &[&"apple"])
        .await
        .expect("execute");
    assert_eq!(result.len(), 1);
    let id: i32 = result[0].get("id");
    assert_eq!(id, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporal_and_decimal_columns_render_as_text() {
    let client = start_seeded_server().await;

    // Dates, timestamps, and decimals render to their canonical Postgres text;
    // the simple protocol returns every column as text, so the rendering shows
    // directly.
    let messages = client
        .simple_query("SELECT day, occurred, amount FROM shop.main.events ORDER BY id")
        .await
        .expect("temporal select");
    let result = rows(&messages, 3);
    assert_eq!(
        result,
        vec![
            vec![
                "2021-01-05".to_owned(),
                "2021-01-05 12:34:56".to_owned(),
                "19.95".to_owned(),
            ],
            vec![
                "2022-07-13".to_owned(),
                "2022-07-13 00:00:00".to_owned(),
                "100.00".to_owned(),
            ],
        ],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_refuses_unmapped_parameter_type_loudly() {
    let client = start_seeded_server().await;

    // A BYTEA parameter has no SQL-literal rendering, so binding one must fail
    // loudly at Execute rather than guess a value the engine cannot place. The
    // parameter is binary-encoded (tokio-postgres's default), so this also covers
    // that an unmappable binary parameter is refused, not silently accepted.
    let statement = client
        .prepare_typed(
            "SELECT id FROM shop.main.items WHERE name = $1",
            &[Type::BYTEA],
        )
        .await
        .expect("prepare typed");
    let bytes: Vec<u8> = vec![1, 2, 3];
    let error = client
        .query(&statement, &[&bytes])
        .await
        .expect_err("unmapped parameter type must be refused");
    assert!(
        error.as_db_error().is_some(),
        "refusal must be a Postgres error response, not a dropped connection"
    );

    // The connection survived the refusal: a follow-up query still works.
    let messages = client
        .simple_query("SELECT count(*) AS n FROM shop.main.items")
        .await
        .expect("query after refusal");
    assert_eq!(rows(&messages, 1), vec![vec!["4".to_owned()]]);
}
