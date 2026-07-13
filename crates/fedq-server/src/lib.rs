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
//! ## Query protocol: simple and extended
//!
//! The simple query protocol (the `Query` message) runs the query string
//! directly and is what psql and `tokio_postgres::simple_query` use. The extended
//! protocol (Parse/Bind/Describe/Execute) is what most drivers use by default
//! (psycopg, JDBC, Npgsql, tokio-postgres), so it is fully served. Two aspects of
//! it need engine seams, because the engine takes a plain SQL string and has no
//! parameter placeholders of its own:
//!
//! ### Describe resolves the schema by planning, never by executing
//!
//! A Describe asks for a statement's or portal's result columns before (and
//! independently of) any Execute. Executing to learn the schema is rejected: it
//! would scan source data and pay the query's cost at Describe time. Instead
//! `Session::describe` calls `Runtime::describe`, which runs the parse -> bind ->
//! optimize -> physical-plan pipeline and reads the plan root's output types.
//! Planning is O(metadata) by the engine's design (catalog and statistics only,
//! under the planning budget), so Describe is cheap and reads no data. The one
//! looseness is a DECIMAL's scale, which is data-derived; the Postgres type both
//! the planned and the executed column map to (NUMERIC) is identical, so the row
//! description a client received from Describe always matches the rows Execute
//! sends. A statement Describe (before any value is bound) plans a form of the SQL
//! with each placeholder replaced by a typed `NULL`, which cannot change the
//! output column types.
//!
//! ### Bind is the parameter-substitution seam
//!
//! The engine has no `$n` parameters yet, so `params` splices each bound value in
//! as a SQL literal at Bind/Execute, producing an ordinary SQL string for
//! `Runtime::execute`. A value arrives text- or binary-encoded; pgwire's
//! `Portal::parameter` decodes either, and the server maps integers (2/4/8-byte),
//! floats (4/8-byte), booleans, and text/varchar (plus the unknown type Postgres
//! gives an undecorated literal). A parameter of any other type is refused loudly
//! by SQLSTATE, naming the type, rather than guessing a literal the engine cannot
//! place. This substitution is the seam real parameter support will replace; until
//! then a parameterized statement is planned and run as its substituted SQL.
//!
//! ## Result type mapping
//!
//! `encode` maps Arrow `DataType`s to Postgres type OIDs for the row description
//! and encodes each cell in the format the client requested (all-text for the
//! simple protocol; the client's per-column choice for the extended one).
//! Booleans, signed and unsigned integers, 32/64-bit floats, and UTF-8 strings
//! have a native Rust wire value and encode in either text or binary format.
//! Dates, timestamps (any time unit; UTC or unzoned), and decimals are rendered to
//! their canonical Postgres text; a client that requests one of these columns in
//! binary format is refused loudly, since a text rendering shipped under a binary
//! format code would be silently misread. Any other Arrow type (a non-UTC zoned
//! timestamp, nested types) raises a proper Postgres `ErrorResponse` naming the
//! column and type rather than shipping a mislabeled value. `EXPLAIN` needs no
//! special handling: `Runtime::execute` returns it as a text column and
//! `Runtime::describe` reports a single text `plan` column.

mod encode;
mod handler;
mod params;
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
