//! End-to-end: start fedq-server on an ephemeral port over a tiny seeded DuckDB
//! config, connect with a real Postgres client (tokio-postgres), and assert that
//! plain and filtered SELECTs return the right rows, that a bad query surfaces as
//! a Postgres error without dropping the connection, and that the unsupported
//! extended protocol fails loudly.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};

use fq_common::{Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig};
use fq_connectors::DuckDbSource;
use serde_yaml::Value;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

/// The seed table for the server tests: four items with a quantity column the
/// filtered query selects on.
const ITEMS_SQL: &str = "CREATE TABLE items (id INTEGER, name VARCHAR, qty INTEGER); \
    INSERT INTO items VALUES \
    (1, 'apple', 5), (2, 'banana', 3), (3, 'cherry', 9), (4, 'date', 1);";

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
async fn serves_selects_and_surfaces_errors() {
    // Seed the fixture and start the server on an ephemeral port.
    let path = temp_duck();
    seed_duck(&path, ITEMS_SQL);
    let config = shop_config(&path);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(fedq_server::serve(config, listener));

    // Connect with a real Postgres client over trust auth.
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

    // The extended query protocol is refused loudly (not by dropping): a
    // parameterized query returns a db error and the connection stays usable.
    let extended = client
        .query("SELECT id FROM shop.main.items", &[])
        .await
        .expect_err("extended protocol must be refused");
    assert!(
        extended.as_db_error().is_some(),
        "extended-protocol refusal must be a Postgres error response"
    );
    let messages = client
        .simple_query("SELECT count(*) AS n FROM shop.main.items")
        .await
        .expect("simple query after extended refusal");
    assert_eq!(rows(&messages, 1), vec![vec!["4".to_owned()]]);
}
