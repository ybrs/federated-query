//! Real PostgreSQL startup and query protocols enforce the selected database and identity.

use fq_common::database::DatabaseId;
use fq_common::load_config;
use fq_common::security::AuthMode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

/// Start a source-free server with fresh storage and an optional SCRAM administrator.
async fn server(authenticated: bool) -> u16 {
    let root = std::env::temp_dir().join(format!("fq_wire_databases_{}", DatabaseId::new()));
    std::fs::create_dir(&root).expect("fixture directory");
    let path = root.join("engine.yaml");
    std::fs::write(&path, "datasources: {}\n").expect("fixture config");
    let mut config = load_config(path.to_str().expect("UTF-8 config")).expect("load config");
    if authenticated {
        config.server.auth = AuthMode::Scram;
        config
            .server
            .users
            .push(fedq_server::hash_password("root", "rootpw", true, 4096).expect("hash"));
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("address").port();
    tokio::spawn(fedq_server::serve(config, listener));
    port
}

/// Connect with an explicit database while retaining the driver's native startup errors.
async fn connect(
    port: u16,
    database: &str,
    credentials: &str,
) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname={database} {credentials}"),
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("test connection ended: {error}");
        }
    });
    Ok(client)
}

/// Extract a scalar text value from the simple query protocol's row messages.
async fn value(client: &Client, sql: &str) -> String {
    let messages = client.simple_query(sql).await.expect("query");
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            return row.get(0).expect("first column").to_string();
        }
    }
    panic!("scalar query returned no row");
}

/// Both protocols resolve identical object names within the connection's fixed database.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_database_controls_simple_and_extended_queries() {
    let port = server(false).await;
    let owner = connect(port, "main", "user=root").await.expect("main");
    owner
        .simple_query("CREATE DATABASE first")
        .await
        .expect("first");
    owner
        .simple_query("CREATE DATABASE second")
        .await
        .expect("second");
    let first = connect(port, "first", "user=root")
        .await
        .expect("first session");
    let second = connect(port, "second", "user=root")
        .await
        .expect("second session");
    first
        .simple_query("CREATE VIEW report AS SELECT CAST(11 AS BIGINT) AS value")
        .await
        .expect("first view");
    second
        .simple_query("CREATE VIEW report AS SELECT CAST(22 AS BIGINT) AS value")
        .await
        .expect("second view");
    assert_eq!(value(&first, "SELECT value FROM report").await, "11");
    let prepared = second
        .prepare("SELECT value FROM report")
        .await
        .expect("prepare");
    assert_eq!(
        second
            .query_one(&prepared, &[])
            .await
            .expect("execute")
            .get::<_, i64>(0),
        22
    );
    assert!(owner
        .simple_query("SELECT value FROM report")
        .await
        .is_err());
}

/// Unknown names fail the startup exchange rather than opening an arbitrary database.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_database_is_rejected_before_ready() {
    let port = server(false).await;
    let error = connect(port, "absent", "user=root")
        .await
        .expect_err("startup must fail");
    assert_eq!(
        error.as_db_error().expect("database error").code(),
        &SqlState::INVALID_CATALOG_NAME
    );
    assert_eq!(
        value(
            &connect(port, "main", "user=root").await.expect("main"),
            "SELECT 1"
        )
        .await,
        "1"
    );
}

/// A raw startup packet proves omission cannot be hidden by a client's default dbname.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn omitted_and_empty_database_are_rejected_before_ready() {
    let port = server(false).await;
    for parameters in [
        b"user\0root\0\0".as_slice(),
        b"user\0root\0database\0\0\0".as_slice(),
    ] {
        let mut socket = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("socket");
        let length = u32::try_from(parameters.len() + 8).expect("packet length");
        socket
            .write_all(&length.to_be_bytes())
            .await
            .expect("length");
        socket
            .write_all(&196_608_u32.to_be_bytes())
            .await
            .expect("protocol");
        socket.write_all(parameters).await.expect("parameters");
        expect_startup_error(&mut socket).await;
    }
}

/// Read complete backend frames until the expected error, rejecting ReadyForQuery.
async fn expect_startup_error(socket: &mut TcpStream) {
    loop {
        let tag = socket.read_u8().await.expect("backend tag");
        let length = socket.read_u32().await.expect("backend length");
        let mut payload = vec![0; usize::try_from(length - 4).expect("payload length")];
        socket.read_exact(&mut payload).await.expect("payload");
        assert_ne!(
            tag, b'Z',
            "server sent ReadyForQuery without a selected database"
        );
        if tag == b'E' {
            assert!(String::from_utf8_lossy(&payload).contains("3D000"));
            return;
        }
    }
}

/// Database ADMIN, admission, and live revocation use the identity established by SCRAM.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_connect_and_local_admin_are_enforced() {
    let port = server(true).await;
    let owner = connect(port, "main", "user=root password=rootpw")
        .await
        .expect("root");
    for sql in [
        "CREATE DATABASE first",
        "CREATE DATABASE second",
        "CREATE USER alice WITH PASSWORD 'pw'",
        "GRANT ADMIN ON DATABASE first TO alice",
    ] {
        owner.simple_query(sql).await.expect(sql);
    }
    let alice = connect(port, "first", "user=alice password=pw")
        .await
        .expect("local admin");
    alice
        .simple_query("CREATE VIEW report AS SELECT 17 AS value")
        .await
        .expect("local view");
    assert_eq!(value(&alice, "SELECT value FROM report").await, "17");
    let denied = connect(port, "second", "user=alice password=pw")
        .await
        .expect_err("no CONNECT");
    assert_eq!(
        denied.as_db_error().expect("db error").code(),
        &SqlState::INSUFFICIENT_PRIVILEGE
    );
    let denied = alice
        .simple_query("CREATE DATABASE forbidden")
        .await
        .expect_err("global admin denied");
    assert_eq!(
        denied.as_db_error().expect("db error").code(),
        &SqlState::INSUFFICIENT_PRIVILEGE
    );
    owner
        .simple_query("REVOKE ADMIN ON DATABASE first FROM alice")
        .await
        .expect("revoke");
    assert!(alice.simple_query("SELECT 1").await.is_err());
    assert!(alice.prepare("SELECT 1").await.is_err());
}
