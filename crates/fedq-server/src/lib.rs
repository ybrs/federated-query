//! fedq-server: a PostgreSQL wire-protocol server over `fq_runtime::Runtime`.
//!
//! Any Postgres client (psql, tokio-postgres, JDBC/ODBC drivers) can connect to
//! this server and issue SQL; the server runs it through the federated engine
//! and returns the Arrow result as Postgres rows. `serve` owns the accept loop;
//! the pgwire handlers live in `handler`; Arrow-to-Postgres conversion lives in
//! `encode`.
//!
//! # Design decisions
//!
//! ## Session model: one `Runtime` per connection, on its own OS thread
//!
//! `fq_runtime::Runtime` is not a thread-safe shared handle. Its execution path
//! populates and reads THREAD-LOCAL caches inside fq-exec (the pinned DuckDB
//! connections, the Postgres connection cache, the shipped-table tracker), so a
//! query is only coherent when the whole `execute` call runs on a single thread.
//! Rather than depend on the runtime's auto-traits (which the fq-exec data plane
//! could change under us), each connection gets a dedicated OS worker thread that
//! BUILDS its own `Runtime` from the shared config and services that connection's
//! queries in order. The async pgwire handler bridges to the thread over a
//! channel: only the SQL string and the Arrow result (both `Send`) cross the
//! boundary, and the `Runtime` never leaves its home thread. See `session`.
//!
//! The considered alternative was a single process-wide `Runtime` shared behind a
//! lock. That design is rejected: a lock would serialize every
//! connection onto one runtime AND still not fix the thread-local coherence
//! problem (the guarded critical section could run on any pool thread across
//! calls). Per-connection runtimes are the honest baseline. Because `Runtime`
//! holds its read-only `Catalog` behind an `Arc`, that catalog can be shared
//! across sessions to cut per-connection metadata loading without giving up
//! thread affinity; this server does not do that sharing.
//!
//! ## DuckDB file locking
//!
//! Each connection's `Runtime` opens its DuckDB handles READ-ONLY (both the
//! catalog's stats handle and the exec-plane read handle), which is what lets
//! many connections point at the same `.duckdb` file at once: DuckDB permits any
//! number of read-only openers but only a single writer per file per process.
//! This server is read-only over its sources, so the read-only open is the whole
//! story here. A future writable server would need a single shared write handle,
//! not one per connection.
//!
//! ## Startup and authentication: trust only
//!
//! Startup uses pgwire's no-auth handshake (`NoopStartupHandler`): any user name
//! is accepted with no password, like a `trust` line in `pg_hba.conf`. The
//! handshake is otherwise complete (parameter status, backend key, ReadyForQuery)
//! so real clients connect cleanly. Only trust auth is implemented; the handler
//! is a distinct type (`TrustStartup`) so an md5/SCRAM/password source can
//! replace it without touching the query path.
//!
//! ## Query protocol: simple only, extended fails loudly
//!
//! The simple query protocol (the `Query` message) is fully supported and is what
//! psql and `tokio_postgres::simple_query` use. The extended protocol
//! (Parse/Bind/Describe/Execute, used by parameterized `query`/`execute`) is NOT
//! implemented. Per the wire protocol it must fail LOUDLY, never by dropping
//! the connection: every extended entry point returns an `ErrorResponse` with
//! SQLSTATE `0A000` (feature_not_supported) at severity `ERROR`, so the client
//! sees a clean error and the connection stays open for the next statement.
//!
//! ## Result type mapping
//!
//! `encode` maps Arrow `DataType`s to Postgres type OIDs for the row description
//! and encodes each cell in the text format. Booleans, signed and unsigned
//! integers, 32/64-bit floats, and UTF-8 strings are supported. Any other Arrow
//! type (dates, timestamps, decimals, nested types) raises a proper Postgres
//! `ErrorResponse` naming the column and type rather than shipping a mislabeled
//! value. `EXPLAIN` needs no special handling: `Runtime::execute` already returns
//! it as a text column, which encodes as a normal string result.

mod encode;
mod handler;
mod session;

pub use handler::FedqHandlers;
pub use session::Session;

use std::sync::Arc;

use fq_common::Config;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

/// Accept connections on `listener` forever, serving each over the Postgres wire
/// protocol. Every accepted socket gets its own `Session` (a fresh `Runtime` on a
/// dedicated worker thread) and is driven on its own tokio task, so slow or idle
/// connections never block others. Returns only if `accept` itself fails.
pub async fn serve(config: Config, listener: TcpListener) -> std::io::Result<()> {
    loop {
        let (socket, _peer) = listener.accept().await?;
        let session = Session::spawn(config.clone());
        let handlers = Arc::new(FedqHandlers::new(session));
        // One task per connection: process_socket runs the whole startup +
        // query loop for this client. A protocol error inside it ends only this
        // connection, so the error is logged and the loop keeps accepting.
        tokio::spawn(async move {
            if let Err(error) = process_socket(socket, None, handlers).await {
                eprintln!("connection ended with error: {error}");
            }
        });
    }
}
