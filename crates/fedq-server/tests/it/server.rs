//! End-to-end: start fedq-server on an ephemeral port over a tiny seeded DuckDB
//! config, connect with a real Postgres client (tokio-postgres), and assert the
//! simple and extended query protocols both work: plain and filtered SELECTs, a
//! bad query surfacing as a Postgres error without dropping the connection,
//! prepared statements with and without parameters, date/timestamp/decimal
//! columns rendering as text, and an unmapped parameter type being refused
//! loudly while the connection survives.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
    UserCredential,
};
use fq_connectors::DuckDbSource;
use serde_yaml::Value;
use tokio::net::TcpListener;
use tokio_postgres::error::SqlState;
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
    (2, DATE '2022-07-13', TIMESTAMP '2022-07-13 00:00:00', 100.00); \
    CREATE TABLE nums AS SELECT * FROM range(64000) t(x);";

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
/// `shop.main.items` table references resolve. `server` carries the authentication
/// config (an empty user list is trust auth).
fn shop_config(path: &str, server: ServerConfig) -> Config {
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
            change_keys: BTreeMap::new(),
        },
    );
    Config {
        datasources,
        optimizer: OptimizerConfig::default(),
        executor: ExecutorConfig::default(),
        cost: CostConfig::default(),
        server,
        accelerator: fq_common::AcceleratorConfig::default(),
        catalog: fq_common::CatalogConfig::default(),
        // A source path is required for the ACL/verifier store (it lives next to
        // the config); derive a unique sibling of the fixture's DuckDB file.
        source_path: Some(format!("{path}.fedq.yaml")),
    }
}

/// Seed a fresh DuckDB fixture and start the server for `config` on an ephemeral
/// port, returning the bound port. The server task runs for the test's duration.
async fn start_server(config: Config) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(fedq_server::serve(config, listener));
    addr.port()
}

/// Connect a Postgres client to `port` with the given connection string suffix
/// (user, and optionally password), spawning its connection driver task.
async fn connect(port: u16, credentials: &str) -> Result<Client, tokio_postgres::Error> {
    let conn_str = format!("host=127.0.0.1 port={port} dbname=fedq {credentials}");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// Seed a fresh DuckDB fixture, start a trust-auth server, and return a connected
/// Postgres client.
async fn start_seeded_server() -> Client {
    let path = temp_duck();
    seed_duck(&path, SEED_SQL);
    let port = start_server(shop_config(&path, ServerConfig::default())).await;
    connect(port, "user=postgres").await.expect("connect")
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

/// Seed a fresh fixture and start a server that requires SCRAM-SHA-256 auth for
/// the given users, returning the bound port.
async fn start_scram_server(users: Vec<UserCredential>) -> u16 {
    let path = temp_duck();
    seed_duck(&path, SEED_SQL);
    start_server(shop_config(
        &path,
        ServerConfig {
            users,
            ..ServerConfig::default()
        },
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_auth_accepts_the_correct_password() {
    // A user configured with a SCRAM salted hash authenticates with the matching
    // plaintext password and can then query.
    let user = fedq_server::hash_password("alice", "hunter2", true, 4096).expect("hash");
    let port = start_scram_server(vec![user]).await;

    let client = connect(port, "user=alice password=hunter2")
        .await
        .expect("correct password must connect");
    let messages = client
        .simple_query("SELECT count(*) AS n FROM shop.main.items")
        .await
        .expect("query after auth");
    assert_eq!(rows(&messages, 1), vec![vec!["4".to_owned()]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_auth_refuses_a_wrong_password() {
    // The same user with a wrong password is refused with a Postgres
    // invalid-password error, never allowed to connect.
    let user = fedq_server::hash_password("alice", "hunter2", true, 4096).expect("hash");
    let port = start_scram_server(vec![user]).await;

    let error = connect(port, "user=alice password=wrong")
        .await
        .expect_err("wrong password must be refused");
    let db_error = error
        .as_db_error()
        .expect("refusal is a Postgres error, not a dropped connection");
    assert_eq!(db_error.code(), &SqlState::INVALID_PASSWORD);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_auth_refuses_an_unknown_user() {
    // A user that is not configured is refused, even with a password that would be
    // correct for a configured user.
    let user = fedq_server::hash_password("alice", "hunter2", true, 4096).expect("hash");
    let port = start_scram_server(vec![user]).await;

    let error = connect(port, "user=bob password=hunter2")
        .await
        .expect_err("unknown user must be refused");
    let db_error = error
        .as_db_error()
        .expect("refusal is a Postgres error, not a dropped connection");
    assert_eq!(db_error.code(), &SqlState::INVALID_PASSWORD);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trust_auth_accepts_any_user_without_a_password() {
    // With no users configured, the server is trust auth: any user name connects
    // with no password (the backward-compatible default).
    let path = temp_duck();
    seed_duck(&path, SEED_SQL);
    let port = start_server(shop_config(&path, ServerConfig::default())).await;

    let client = connect(port, "user=whoever")
        .await
        .expect("trust auth must connect any user");
    let messages = client
        .simple_query("SELECT count(*) AS n FROM shop.main.items")
        .await
        .expect("query after trust connect");
    assert_eq!(rows(&messages, 1), vec![vec!["4".to_owned()]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_request_cancels_a_running_query() {
    // A long-running query is cancelled mid-flight by a Postgres CancelRequest. It
    // is a self cross join over 64k rows with a per-pair arithmetic filter, so
    // DuckDB must iterate all ~4e9 pairs (a plain count over a cross join it
    // computes analytically in no time); the whole thing pushes down to DuckDB as
    // one island and cannot finish before the cancel arrives on any machine. The
    // client gets SQLSTATE 57014 (query_canceled) immediately - pgwire abandons the
    // query rather than waiting for the worker - and the connection survives.
    let path = temp_duck();
    seed_duck(&path, SEED_SQL);
    let port = start_server(shop_config(&path, ServerConfig::default())).await;
    let client = connect(port, "user=postgres").await.expect("connect");
    let cancel_token = client.cancel_token();

    let query = tokio::spawn(async move {
        let result = client
            .simple_query(
                "SELECT count(*) AS n FROM shop.main.nums a CROSS JOIN shop.main.nums b \
                 WHERE (a.x * b.x) % 1000000007 = 0",
            )
            .await;
        (client, result)
    });

    // The query is dispatched to the worker within a few milliseconds on
    // localhost; wait past that so the cancel lands while it is executing.
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel_token
        .cancel_query(NoTls)
        .await
        .expect("send cancel request");

    let (client, result) = query.await.expect("query task joined");
    let error = result.expect_err("a cancelled query must return an error");
    let db_error = error
        .as_db_error()
        .expect("cancellation is a Postgres error, not a dropped connection");
    assert_eq!(db_error.code(), &SqlState::QUERY_CANCELED);

    // The cancelled connection stays open (a 57014 is a non-fatal error). A
    // follow-up on THIS connection is not issued: it would block behind the
    // cancelled query, which keeps running to completion on the worker thread (the
    // engine has no interruption seam; see the crate module doc).
    assert!(!client.is_closed(), "the connection survived the cancel");

    // The server keeps serving other connections while that query runs: a fresh
    // connection queries normally.
    let other = connect(port, "user=postgres")
        .await
        .expect("second connection");
    let messages = other
        .simple_query("SELECT count(*) AS n FROM shop.main.items")
        .await
        .expect("query on a second connection");
    assert_eq!(rows(&messages, 1), vec![vec!["4".to_owned()]]);
}
