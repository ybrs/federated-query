// Imported fedqrs connectors: the lint allow below exempts this module from
// the workspace pedantic set (its house style predates the workspace's).
#![allow(clippy::all, clippy::pedantic)]
//! Native source connectors and the datasource registry.
//!
//! The runtime registers each datasource once at session init via `register`;
//! the engine then fetches from a source by name over a native driver. Fetched
//! data stays in Rust (as Arrow) for the rest of the query.
//!
//! Postgres reads go over ADBC, which decodes the wire straight into Arrow.
//! DuckDB reads go over the native duckdb driver, with connections cached
//! per thread and seeded for shipped temp tables.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::core::partition::{ctid_ranges, selectivity_from_stats};
use crate::core::types::DsKind;
use crate::error::{ExecError, ExecResult};

/// Connection parameters for one registered datasource. Parameters only, not a
/// live handle: connections are opened (and per-thread cached) at fetch time.
#[derive(Clone)]
pub struct DsSpec {
    pub kind: DsKind,
    /// Postgres: the connection URI. DuckDB: the database file path.
    pub uri: String,
    /// Path to the ADBC driver shared library (Postgres only).
    pub adbc_driver: Option<String>,
}

/// The identity of one engine session (one `Runtime`). Every process-wide data
/// plane map keys its entries on `(SessionId, name)`, so two runtimes that reuse
/// a datasource name never read each other's source, and a dropped runtime's
/// state can be pruned by its id alone. A plain incrementing counter: sessions
/// are minted rarely (once per runtime) and never need to be reconstructed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SessionId(u64);

/// Mint a fresh session id and record it as live. The caller (a `Runtime`)
/// registers its datasources under the returned id and prunes them with
/// `prune_session` when it drops.
pub fn open_session() -> SessionId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let id = SessionId(NEXT.fetch_add(1, Ordering::Relaxed));
    live_sessions().lock().unwrap().insert(id);
    id
}

/// The live-session set: the id of every runtime that has not yet dropped. Read
/// by `reap_dead_sessions` to drop the thread-local connections of a session
/// that dropped on another thread; written by `open_session` / `prune_session`.
fn live_sessions() -> &'static Mutex<HashSet<SessionId>> {
    static LIVE: OnceLock<Mutex<HashSet<SessionId>>> = OnceLock::new();
    LIVE.get_or_init(|| Mutex::new(HashSet::new()))
}

thread_local! {
    // The session whose datasources every data-plane call on THIS thread reads.
    // A `SessionScope` sets it for the duration of one execution (or one pool
    // job); the accessors below key their maps on it. `None` outside a scope is a
    // programming error - a fetch with no session installed cannot pick a source.
    static CURRENT_SESSION: Cell<Option<SessionId>> = const { Cell::new(None) };
}

/// Install `session` as the current data-plane session for the life of the
/// returned guard, restoring the previous session (nesting-safe) when it drops.
/// The execution entry point and every pool worker install one before touching a
/// source, so all session-keyed lookups on the thread resolve to `session`.
pub struct SessionScope {
    previous: Option<SessionId>,
}

impl SessionScope {
    /// Enter `session` on the current thread.
    pub fn enter(session: SessionId) -> SessionScope {
        let previous = CURRENT_SESSION.with(|current| current.replace(Some(session)));
        SessionScope { previous }
    }
}

impl Drop for SessionScope {
    fn drop(&mut self) {
        CURRENT_SESSION.with(|current| current.set(self.previous));
    }
}

/// The session installed on this thread. Panics when none is installed: every
/// data-plane read runs inside a `SessionScope`, so a missing session is a bug
/// (a source read with no session cannot be resolved without guessing).
pub fn current_session() -> SessionId {
    CURRENT_SESSION
        .with(Cell::get)
        .expect("no data-plane session installed on this thread; enter a SessionScope first")
}

/// Drop every trace of `session` from the data plane: its registry entries,
/// cached contexts and connections in the process-wide maps, this thread's
/// pooled connections for it, and the same in both worker pools. Called by a
/// `Runtime` as it drops. Only `session`'s entries are touched, so other live
/// sessions keep running.
pub fn prune_session(session: SessionId) {
    live_sessions().lock().unwrap().remove(&session);
    registry().lock().unwrap().retain(|key, _| key.0 != session);
    duck_base_cache()
        .lock()
        .unwrap()
        .retain(|key, _| key.0 != session);
    materialized_ctx_cache()
        .lock()
        .unwrap()
        .retain(|key, _| key.0 != session);
    parquet_ctx_cache()
        .lock()
        .unwrap()
        .retain(|key, _| key.0 != session);
    index_cache()
        .lock()
        .unwrap()
        .retain(|key, _| key.0 != session);
    prune_thread_local_caches(session);
    prune_parallel_pool(session);
    crate::engine::prune_step_pool(session);
}

/// Drop this thread's pooled Postgres/DuckDB/MySQL connections for a session
/// (shipped-table records with them). Runs on the thread that pruned or reaped
/// the session; only that thread's non-`Send` connection handles are touched.
pub(crate) fn prune_thread_local_caches(session: SessionId) {
    PG_CACHE.with(|cache| cache.borrow_mut().retain(|key, _| key.0 != session));
    PINNED_DUCK.with(|cache| cache.borrow_mut().retain(|key, _| key.0 != session));
    MYSQL_CACHE.with(|cache| cache.borrow_mut().retain(|key, _| key.0 != session));
    SHIPPED_PG.with(|shipped| shipped.borrow_mut().retain(|entry| entry.0 != session));
}

/// Drop this thread's pooled connections for every session that has since
/// dropped. The execution entry point calls it so a runtime dropped on another
/// thread still releases the connections it opened on THIS driving thread.
pub fn reap_dead_sessions() {
    let live = live_sessions().lock().unwrap().clone();
    PG_CACHE.with(|cache| cache.borrow_mut().retain(|key, _| live.contains(&key.0)));
    PINNED_DUCK.with(|cache| cache.borrow_mut().retain(|key, _| live.contains(&key.0)));
    MYSQL_CACHE.with(|cache| cache.borrow_mut().retain(|key, _| live.contains(&key.0)));
    SHIPPED_PG.with(|shipped| shipped.borrow_mut().retain(|entry| live.contains(&entry.0)));
}

static REGISTRY: OnceLock<Mutex<HashMap<(SessionId, String), DsSpec>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<(SessionId, String), DsSpec>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register (or replace) a datasource under `(session, name)`.
pub fn register(session: SessionId, name: String, spec: DsSpec) {
    registry().lock().unwrap().insert((session, name), spec);
}

/// Drop one datasource `(session, name)` from the data plane: its registry spec
/// plus every cached context and connection it seeded. A `DROP DATASOURCE` calls
/// this so the dropped name no longer resolves in the dropping session AND a
/// later create reusing the name (pointing elsewhere) never reads a stale cached
/// connection. The URI-keyed caches (DuckDB, Parquet, MySQL) are purged by the
/// spec's URI, so the spec is read before it is removed. Only this session's
/// entries for this name are touched; peers and other sources are untouched.
pub fn deregister(session: SessionId, name: &str) {
    let spec = registry()
        .lock()
        .unwrap()
        .remove(&(session, name.to_string()));
    let uri = spec.map(|spec| spec.uri);
    materialized_ctx_cache()
        .lock()
        .unwrap()
        .remove(&(session, name.to_string()));
    index_cache()
        .lock()
        .unwrap()
        .remove(&(session, name.to_string()));
    if let Some(uri) = uri {
        duck_base_cache()
            .lock()
            .unwrap()
            .remove(&(session, uri.clone()));
        parquet_ctx_cache().lock().unwrap().remove(&(session, uri));
    }
    PG_CACHE.with(|cache| {
        cache.borrow_mut().remove(&(session, name.to_string()));
    });
    SHIPPED_PG.with(|shipped| {
        shipped
            .borrow_mut()
            .retain(|entry| !(entry.0 == session && entry.1 == name));
    });
}

fn spec(name: &str) -> ExecResult<DsSpec> {
    let session = current_session();
    registry()
        .lock()
        .unwrap()
        .get(&(session, name.to_string()))
        .cloned()
        .ok_or_else(|| ExecError::runtime(format!("datasource '{name}' is not registered")))
}

/// The kind of a registered datasource (used to pick the SQL dialect).
pub fn kind(name: &str) -> ExecResult<DsKind> {
    Ok(spec(name)?.kind)
}

/// A cached, reused Postgres connection (driver + database kept alive with it).
struct PgConn {
    _driver: adbc_driver_manager::ManagedDriver,
    _database: adbc_driver_manager::ManagedDatabase,
    conn: adbc_driver_manager::ManagedConnection,
}

thread_local! {
    // One live connection per (session, datasource name), on the query-driving
    // thread. ADBC handles are not Send, and all fetches run on this one thread,
    // so a thread-local cache pools connections without any locking. The session
    // is part of the key so two runtimes reusing a name keep separate connections
    // and a dropped runtime's connections can be pruned by session.
    static PG_CACHE: RefCell<HashMap<(SessionId, String), PgConn>> = RefCell::new(HashMap::new());
    // DuckDB connections PINNED for the current query, keyed by (session, database
    // path). A pinned connection exists only after a `ship` step created a
    // temp table on it: temp objects are per-connection, so every later
    // fetch against that database must run on the SAME connection to see
    // the shipped relation. Cleared (dropping the temp tables with it) at
    // query end by `clear_shipments`.
    static PINNED_DUCK: RefCell<HashMap<(SessionId, String), duckdb::Connection>> =
        RefCell::new(HashMap::new());
    // (session, datasource name, table) triples shipped as Postgres session TEMP
    // TABLEs on the pooled PG_CACHE connection this query. Unlike DuckDB (where
    // dropping the pinned connection drops its temp tables), the PG connection is
    // reused across queries, so each shipped table is DROPped by name at query end.
    static SHIPPED_PG: RefCell<Vec<(SessionId, String, String)>> = RefCell::new(Vec::new());
}

/// A hard safety backstop on how many rows a single shipped relation may hold.
/// The optimizer only ships a dimension it ESTIMATES at <= ~200k rows
/// (SHIP_ROW_BUDGET in the planner), so this cap never trips a correct decision
/// - it exists solely to catch STALE STATS: a dimension estimated small but
/// actually huge, which would otherwise ingest for minutes-to-hours and hammer
/// the target (an ADBC binary-COPY of a giant relation into Postgres). Failing
/// loud here turns that runaway into an immediate, honest crash - a crash never
/// ships a lie. The value is a deliberately large multiple of the plan budget;
/// there is no query-derived "right" number for a safety valve, so it is a
/// fixed constant.
const SHIP_MAX_ROWS: usize = 50_000_000;

/// Materialize a relation as a TEMP TABLE on the target source (DuckDB or
/// Postgres), visible to that source's later reads this query. The optimizer
/// chooses the target by cost; the engine supports both writable sources.
pub fn ship_table(
    name: &str,
    table: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> ExecResult<()> {
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    if rows > SHIP_MAX_ROWS {
        return Err(ExecError::runtime(format!(
            "dim shipping refused: table '{table}' has {rows} rows, over the hard \
             cap of {SHIP_MAX_ROWS} - stale stats likely mis-planned this ship; \
             not ingesting"
        )));
    }
    let s = spec(name)?;
    match s.kind {
        DsKind::DuckDb => ship_table_duckdb(&s, table, schema, batches),
        DsKind::Postgres => ship_table_postgres(name, &s, table, schema, batches),
        DsKind::Parquet => Err(ExecError::runtime(format!(
            "ship target '{name}' is a read-only Parquet source; cannot ship into it"
        ))),
        DsKind::ClickHouse => Err(ExecError::runtime(format!(
            "ship target '{name}' is a ClickHouse source; the engine does not ship into it"
        ))),
        DsKind::MySql => Err(ExecError::runtime(format!(
            "ship target '{name}' is a MySQL source; the engine does not ship into it"
        ))),
        DsKind::Materialized => Err(ExecError::runtime(format!(
            "ship target '{name}' is the read-only materialized-view store; \
             cannot ship into it"
        ))),
    }
}

/// Ship into DuckDB: a TEMP TABLE on a connection pinned for the rest of the
/// query (temp objects are per-connection, so every later read must share it).
fn ship_table_duckdb(
    s: &DsSpec,
    table: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> ExecResult<()> {
    let key = (current_session(), s.uri.clone());
    PINNED_DUCK.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(&key) {
            map.insert(key.clone(), duck_cursor(&s.uri)?);
        }
        let conn = map.get(&key).unwrap();
        create_shipped_table(conn, table, &schema, batches)
    })
}

/// Ship into Postgres: a session TEMP TABLE (ADBC binary-COPY ingest) on the
/// pooled PG_CACHE connection, which every later fetch_postgres for this source
/// reuses, so the shipped table is visible to the island read. Recorded for an
/// explicit DROP at query end (the pooled connection outlives the query).
fn ship_table_postgres(
    name: &str,
    s: &DsSpec,
    table: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> ExecResult<()> {
    let session = current_session();
    let key = (session, name.to_string());
    let drop_sql = format!("DROP TABLE IF EXISTS \"{}\"", table.replace('"', "\"\""));
    PG_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(&key) {
            map.insert(key.clone(), open_pg(s).map_err(ExecError::runtime)?);
        }
        let pg = map.get_mut(&key).unwrap();
        exec_update(&mut pg.conn, &drop_sql).map_err(ExecError::runtime)?;
        ingest_temp(&mut pg.conn, table, schema, batches).map_err(ExecError::runtime)?;
        // ANALYZE the freshly-ingested temp table so Postgres plans the island
        // join over real row counts / NDVs instead of the default guess for an
        // unstatted relation. The ingest already succeeded, so a failure here is
        // real and propagates rather than being swallowed.
        let analyze_sql = format!("ANALYZE \"{}\"", table.replace('"', "\"\""));
        exec_update(&mut pg.conn, &analyze_sql).map_err(ExecError::runtime)
    })?;
    SHIPPED_PG.with(|s| {
        s.borrow_mut()
            .push((session, name.to_string(), table.to_string()))
    });
    Ok(())
}

/// Drop every shipped temp table: the pinned DuckDB connections (and their temp
/// tables with them), and each Postgres shipped table by name on its pooled
/// connection. Runs on every query exit path.
pub fn clear_shipments() {
    let session = current_session();
    PINNED_DUCK.with(|cache| cache.borrow_mut().retain(|key, _| key.0 != session));
    drop_shipped_pg(session);
}

/// DROP each of this session's Postgres shipped tables on its pooled connection,
/// then forget them (leaving any other session's shipments untouched).
fn drop_shipped_pg(session: SessionId) {
    let mut shipped = Vec::new();
    SHIPPED_PG.with(|s| {
        s.borrow_mut().retain(|entry| {
            if entry.0 == session {
                shipped.push(entry.clone());
                false
            } else {
                true
            }
        })
    });
    PG_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        for (_session, name, table) in shipped {
            if let Some(pg) = map.get_mut(&(session, name)) {
                let drop_sql = format!("DROP TABLE IF EXISTS \"{}\"", table.replace('"', "\"\""));
                let _ = exec_update(&mut pg.conn, &drop_sql);
            }
        }
    });
}

fn create_shipped_table(
    conn: &duckdb::Connection,
    table: &str,
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
) -> ExecResult<()> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let duck_type = duck_type_name(field.data_type()).map_err(ExecError::runtime)?;
        columns.push(format!(
            "\"{}\" {}",
            field.name().replace('"', "\"\""),
            duck_type
        ));
    }
    let quoted = table.replace('"', "\"\"");
    let create = format!(
        "DROP TABLE IF EXISTS \"{quoted}\"; CREATE TEMP TABLE \"{quoted}\" ({})",
        columns.join(", ")
    );
    conn.execute_batch(&create)
        .map_err(|e| ExecError::runtime(format!("duckdb ship table: {e}")))?;
    let mut appender = conn
        .appender(table)
        .map_err(|e| ExecError::runtime(format!("duckdb ship appender: {e}")))?;
    for batch in batches {
        appender
            .append_record_batch(batch)
            .map_err(|e| ExecError::runtime(format!("duckdb ship ingest: {e}")))?;
    }
    Ok(())
}

/// Run `sql` against the named source over its native driver and return the
/// full Arrow result held in Rust.
pub fn fetch(name: &str, sql: &str) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    let s = spec(name)?;
    match s.kind {
        DsKind::Postgres => fetch_postgres(name, &s, sql),
        DsKind::DuckDb => fetch_duckdb(&s, sql),
        DsKind::Parquet => fetch_parquet(&s, sql),
        DsKind::ClickHouse => fetch_clickhouse(name, &s, sql),
        DsKind::MySql => fetch_mysql(&s, sql),
        DsKind::Materialized => fetch_materialized(name, sql),
    }
}

/// A shared, cloneable ureq agent for ClickHouse HTTP reads (pools connections).
fn clickhouse_agent() -> &'static ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT.get_or_init(ureq::Agent::new_with_defaults)
}

/// Read a ClickHouse source by POSTing `sql` to its HTTP interface with
/// `FORMAT ArrowStream` and decoding the Arrow IPC stream straight into batches.
/// ClickHouse does the Arrow encoding, so the schema is authoritative; an IPC
/// decode failure names the failing frame and propagates rather than shipping a
/// partial result. The endpoint URI already carries any `?user=/&password=` auth
/// (built at registration), so this is a pure body POST.
fn fetch_clickhouse(
    name: &str,
    s: &DsSpec,
    sql: &str,
) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    trace_sql(name, sql);
    let query = format!("{sql}\nFORMAT ArrowStream");
    let response = clickhouse_agent()
        .post(&s.uri)
        .send(query.as_str())
        .map_err(|e| ExecError::runtime(format!("clickhouse http: {e}")))?;
    let reader = response.into_body().into_reader();
    let stream = arrow::ipc::reader::StreamReader::try_new(reader, None)
        .map_err(|e| ExecError::runtime(format!("clickhouse arrow stream: {e}")))?;
    let schema = stream.schema();
    let mut batches = Vec::new();
    for batch in stream {
        batches
            .push(batch.map_err(|e| ExecError::runtime(format!("clickhouse arrow batch: {e}")))?);
    }
    Ok((schema, batches))
}

// A DataFusion runtime for reading Parquet sources. Source reads run in the
// engine's sync step loop (not inside a block_on), so a dedicated runtime here
// is safe to block on.
fn parquet_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("parquet tokio runtime"))
}

// Read a Parquet source by running the pushed SQL through DataFusion over the
// directory's `<table>.parquet` files (registered under a `main` schema). This
// gives DataFusion's native projection / filter / row-group pushdown - the fair
// comparison point against DuckDB's read_parquet.
fn fetch_parquet(s: &DsSpec, sql: &str) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    let ctx = parquet_ctx(current_session(), &s.uri)
        .map_err(|e| ExecError::runtime(format!("parquet: {e}")))?;
    let sql = sql.to_string();
    let result = parquet_runtime().block_on(async move {
        let df = ctx.sql(&sql).await?;
        let schema = std::sync::Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await?;
        Ok::<(SchemaRef, Vec<RecordBatch>), datafusion::error::DataFusionError>((schema, batches))
    });
    result.map_err(|e| ExecError::runtime(format!("parquet query: {e}")))
}

// A DataFusion context per Parquet directory, built once (schema inference of
// every file is not repeated per query) and reused - SessionContext is Arc-cheap
// to clone.
fn parquet_ctx(
    session: SessionId,
    dir: &str,
) -> Result<datafusion::prelude::SessionContext, datafusion::error::DataFusionError> {
    let cache = parquet_ctx_cache();
    let key = (session, dir.to_string());
    let mut map = cache.lock().unwrap();
    if let Some(ctx) = map.get(&key) {
        return Ok(ctx.clone());
    }
    // Collect statistics from Parquet metadata so DataFusion's cost-based join
    // reordering has real cardinalities (otherwise multi-joins pick bad plans).
    let config = datafusion::execution::config::SessionConfig::new().with_collect_statistics(true);
    // Same shared 32 GB memory pool as the merge fragments - the Parquet path
    // executes DataFusion queries too and must be bounded by the same cap.
    let ctx = datafusion::prelude::SessionContext::new_with_config_rt(
        config,
        crate::engine::runtime_env(),
    );
    parquet_runtime().block_on(register_parquet_dir(&ctx, dir))?;
    map.insert(key, ctx.clone());
    Ok(ctx)
}

// A DataFusion context per (session, Parquet directory). Session-keyed so a
// dropped runtime's inferred-schema contexts prune with it and two runtimes over
// the same directory keep independent contexts.
fn parquet_ctx_cache(
) -> &'static Mutex<HashMap<(SessionId, String), datafusion::prelude::SessionContext>> {
    static CACHE: OnceLock<
        Mutex<HashMap<(SessionId, String), datafusion::prelude::SessionContext>>,
    > = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

// Register every `<dir>/<table>.parquet` as `main.<table>` in `ctx`.
async fn register_parquet_dir(
    ctx: &datafusion::prelude::SessionContext,
    dir: &str,
) -> Result<(), datafusion::error::DataFusionError> {
    use datafusion::catalog::SchemaProvider;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    let schema = std::sync::Arc::new(datafusion::catalog::MemorySchemaProvider::new());
    let entries = std::fs::read_dir(dir)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    for entry in entries {
        let path = entry
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
            .path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        let name = path.file_stem().unwrap().to_string_lossy().to_string();
        let url = ListingTableUrl::parse(path.to_string_lossy())?;
        // Infer string/binary columns as Utf8/Binary, NOT the Utf8View/BinaryView
        // DataFusion 54 defaults to: schema inference bakes the column types into
        // the ListingTable, and every other source yields plain Utf8, so a
        // view-typed parquet column would not line up when the coordinator merges
        // the two sides by Arrow type.
        let format = ParquetFormat::default().with_force_view_types(false);
        let options = ListingOptions::new(std::sync::Arc::new(format)).with_collect_stat(true);
        let file_schema = options.infer_schema(&ctx.state(), &url).await?;
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(file_schema);
        let table = std::sync::Arc::new(ListingTable::try_new(config)?);
        schema.register_table(name, table)?;
    }
    ctx.catalog("datafusion")
        .unwrap()
        .register_schema("main", schema)?;
    Ok(())
}

/// One materialized view's read shape for the data plane: its table name and
/// the ordered Arrow IPC chunk files that compose it. The registrar (the
/// runtime's DDL layer) passes the CATALOG's chunk list, never a directory
/// listing, so a half-published refresh generation is never read.
#[derive(Clone)]
pub struct MaterializedTable {
    pub table: String,
    pub chunks: Vec<String>,
}

// The DataFusion context per materialized-view datasource, rebuilt WHOLE by
// every `register_materialized` call (a DDL registers the full current table
// set, so replacement is the correct semantics - a dropped view disappears, a
// refreshed view points at its new chunk list).
fn materialized_ctx_cache(
) -> &'static Mutex<HashMap<(SessionId, String), datafusion::prelude::SessionContext>> {
    static CACHE: OnceLock<
        Mutex<HashMap<(SessionId, String), datafusion::prelude::SessionContext>>,
    > = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register (or replace) a materialized-view datasource: the `DsSpec` under
/// `name` plus a DataFusion context exposing every listed view as
/// `main.<table>` over its Arrow IPC chunk files. Called at runtime setup and
/// after every CREATE / REFRESH / DROP with the catalog's full current list.
pub fn register_materialized(
    session: SessionId,
    name: String,
    store_root: String,
    tables: Vec<MaterializedTable>,
) -> ExecResult<()> {
    let ctx = materialized_ctx(&tables)
        .map_err(|e| ExecError::runtime(format!("materialized store '{name}': {e}")))?;
    materialized_ctx_cache()
        .lock()
        .unwrap()
        .insert((session, name.clone()), ctx);
    register(
        session,
        name,
        DsSpec {
            kind: DsKind::Materialized,
            uri: store_root,
            adbc_driver: None,
        },
    );
    Ok(())
}

// Build a DataFusion context with each view registered as `main.<table>` over
// its chunk files. Shares the parquet path's runtime env (same memory pool).
fn materialized_ctx(
    tables: &[MaterializedTable],
) -> Result<datafusion::prelude::SessionContext, datafusion::error::DataFusionError> {
    let config = datafusion::execution::config::SessionConfig::new().with_collect_statistics(true);
    let ctx = datafusion::prelude::SessionContext::new_with_config_rt(
        config,
        crate::engine::runtime_env(),
    );
    parquet_runtime().block_on(register_materialized_tables(&ctx, tables))?;
    Ok(ctx)
}

// Register every view's chunk files as one listing table under a `public`
// schema - the schema the planner's rendered SQL reads views from (the parser
// defaults an unqualified reference's schema to `public`).
async fn register_materialized_tables(
    ctx: &datafusion::prelude::SessionContext,
    tables: &[MaterializedTable],
) -> Result<(), datafusion::error::DataFusionError> {
    use datafusion::catalog::SchemaProvider;
    use datafusion::datasource::file_format::arrow::ArrowFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    let schema = std::sync::Arc::new(datafusion::catalog::MemorySchemaProvider::new());
    for table in tables {
        // A view always has at least one chunk (an empty result still writes a
        // schema-bearing chunk); zero chunks is a registrar bug surfaced loudly.
        if table.chunks.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "materialized view '{}' has no chunk files",
                table.table
            )));
        }
        let mut urls = Vec::with_capacity(table.chunks.len());
        for chunk in &table.chunks {
            urls.push(ListingTableUrl::parse(chunk)?);
        }
        let options = ListingOptions::new(std::sync::Arc::new(ArrowFormat))
            .with_file_extension(".arrow")
            .with_collect_stat(true);
        // Every chunk of a view shares one schema by construction (a refresh
        // that would change it is refused), so the first chunk's schema is the
        // view's schema.
        let file_schema = options.infer_schema(&ctx.state(), &urls[0]).await?;
        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(file_schema);
        let listing = std::sync::Arc::new(ListingTable::try_new(config)?);
        schema.register_table(table.table.clone(), listing)?;
    }
    ctx.catalog("datafusion")
        .unwrap()
        .register_schema("public", schema)?;
    Ok(())
}

// Read the materialized-view store by running the pushed SQL through the
// datasource's registered DataFusion context (its views live under `main`).
fn fetch_materialized(name: &str, sql: &str) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    let ctx = materialized_ctx_cache()
        .lock()
        .unwrap()
        .get(&(current_session(), name.to_string()))
        .cloned()
        .ok_or_else(|| {
            ExecError::runtime(format!(
                "materialized store '{name}' has no registered view context"
            ))
        })?;
    let sql = sql.to_string();
    let result = parquet_runtime().block_on(async move {
        let df = ctx.sql(&sql).await?;
        let schema = std::sync::Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await?;
        Ok::<(SchemaRef, Vec<RecordBatch>), datafusion::error::DataFusionError>((schema, batches))
    });
    result.map_err(|e| ExecError::runtime(format!("materialized query: {e}")))
}

fn open_duckdb(path: &str) -> Result<duckdb::Connection, String> {
    // Read-WRITE open. DuckDB is single-instance-per-file per process: a
    // read-write instance takes an exclusive file lock, so a SECOND open on the
    // same file (even read-only) conflicts. The engine writes to DuckDB - dim
    // shipping and semi-join reduction CREATE TEMP TABLE and append Arrow via the
    // duckdb Appender, which REQUIRES a read-write base connection (a read-only
    // base rejects the append even though temp objects never touch the file).
    // The runtime therefore opens each file ONCE (read-write, in the catalog
    // DuckDbSource) and seeds that one instance here via `seed_duck_connection`,
    // so this standalone open is only reached by tests that drive DuckDB without
    // a catalog - where nothing else holds the file and read-write is safe.
    duckdb::Connection::open(path).map_err(|e| format!("duckdb open '{path}': {e}"))
}

/// The process-wide base DuckDB instance per file path. `duck_cursor` clones
/// cheap per-fetch cursors off it. Seeded by the runtime with the ONE read-write
/// instance the catalog opened (`seed_duck_connection`) so the whole process
/// shares a single DuckDB instance per file - no second file open, no lock
/// fight. A path absent here (tests) falls back to a standalone `open_duckdb`.
fn duck_base_cache() -> &'static Mutex<HashMap<(SessionId, String), duckdb::Connection>> {
    static CACHE: OnceLock<Mutex<HashMap<(SessionId, String), duckdb::Connection>>> =
        OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register the catalog's already-open read-write DuckDB connection as the
/// process-wide base for `path`, so the exec data plane clones cursors off the
/// SAME instance instead of opening the file a second time. Called once per
/// DuckDB datasource at runtime setup.
pub fn seed_duck_connection(session: SessionId, path: String, connection: duckdb::Connection) {
    duck_base_cache()
        .lock()
        .unwrap()
        .insert((session, path), connection);
}

// A per-fetch DuckDB cursor over the ONE process-wide instance for this file
// (`duck_base_cache`, seeded by the runtime or opened standalone in tests).
// `Connection::open` instantiates a whole DuckDB database (built-in function and
// type registration) - ~7-10ms regardless of file size - so re-opening per fetch
// was the dominant per-fetch cost. We hand out cheap cursors via `try_clone`: a
// `duckdb_connect` on the already-open database, microseconds. Each fetch gets
// its own cursor, so connection-scoped temp tables stay isolated and drop with
// the cursor. Mirrors the Postgres pool (PG_CACHE) and the Parquet context cache.
fn duck_cursor(path: &str) -> ExecResult<duckdb::Connection> {
    let key = (current_session(), path.to_string());
    let mut map = duck_base_cache().lock().unwrap();
    if !map.contains_key(&key) {
        let base = open_duckdb(path).map_err(ExecError::runtime)?;
        map.insert(key.clone(), base);
    }
    map.get(&key)
        .unwrap()
        .try_clone()
        .map_err(|e| ExecError::runtime(format!("duckdb cursor '{path}': {e}")))
}

// Print every SQL string the engine pushes to a source, gated on
// FEDQRS_TRACE_SQL=1. The single common artifact for diffing pushdown between
// the old and new planners: it shows directly whether a whole single-source
// subtree (aggregate/join) is pushed as one query or the raw table is pulled and
// processed in the coordinator.
fn trace_sql(source: &str, sql: &str) {
    static ON: OnceLock<bool> = OnceLock::new();
    let on = *ON.get_or_init(|| {
        std::env::var("FEDQRS_TRACE_SQL").map_or(false, |v| v != "0" && !v.is_empty())
    });
    if on {
        eprintln!("[fedqsql] source={source}\n{sql}\n");
    }
}

fn fetch_duckdb(s: &DsSpec, sql: &str) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    trace_sql(&s.uri, sql);
    // A query that shipped a temp table must keep reading on the pinned
    // connection - temp objects are invisible to other cursors.
    let key = (current_session(), s.uri.clone());
    let pinned = PINNED_DUCK.with(|cache| {
        let map = cache.borrow();
        match map.get(&key) {
            Some(conn) => Some(run_duckdb_query(conn, sql)),
            None => None,
        }
    });
    if let Some(result) = pinned {
        return result;
    }
    // A cursor on the process-wide cached instance (see `duck_cursor`): the
    // ~7-10ms DuckDB instance creation is paid once per file, not once per fetch.
    let conn = duck_cursor(&s.uri)?;
    run_duckdb_query(&conn, sql)
}

fn run_duckdb_query(
    conn: &duckdb::Connection,
    sql: &str,
) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| ExecError::runtime(format!("duckdb prepare: {e}")))?;
    let arrow = stmt
        .query_arrow([])
        .map_err(|e| ExecError::runtime(format!("duckdb query: {e}")))?;
    let schema = arrow.get_schema();
    let mut batches = Vec::new();
    for batch in arrow {
        batches.push(batch);
    }
    Ok((schema, batches))
}

// --- MySQL data plane ---------------------------------------------------------
//
// MySQL has no Arrow output format, so rows are decoded to Arrow here. Each
// result column's MySQL protocol type picks an Arrow builder; a value that does
// not fit its column's decoder (a non-UTF-8 byte string in a text column, an
// unsigned value past i64) fails loud, naming the column and its MySQL type,
// rather than shipping a corrupt or truncated batch.

thread_local! {
    // One live MySQL connection per (session, datasource URL) on the query-driving
    // thread (mirrors the DuckDB cursor cache; the sync client is not Sync).
    static MYSQL_CACHE: RefCell<HashMap<(SessionId, String), mysql::Conn>> =
        RefCell::new(HashMap::new());
}

/// The Arrow decoder chosen for one MySQL result column.
#[derive(Clone, Copy)]
enum MysqlDecoder {
    Int64,
    Float64,
    Utf8,
    Date32,
    TimestampMicros,
}

/// Pick the Arrow decoder for a MySQL protocol column type. An unmodeled type
/// (bit/geometry/vector) raises rather than guess a representation.
fn mysql_decoder(ty: mysql::consts::ColumnType) -> Result<MysqlDecoder, String> {
    use mysql::consts::ColumnType::*;
    match ty {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_LONG | MYSQL_TYPE_INT24
        | MYSQL_TYPE_LONGLONG | MYSQL_TYPE_YEAR => Ok(MysqlDecoder::Int64),
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE | MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => {
            Ok(MysqlDecoder::Float64)
        }
        MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_BLOB
        | MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_ENUM
        | MYSQL_TYPE_SET
        | MYSQL_TYPE_JSON => Ok(MysqlDecoder::Utf8),
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => Ok(MysqlDecoder::Date32),
        MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_TIMESTAMP2
        | MYSQL_TYPE_TIME
        | MYSQL_TYPE_TIME2 => Ok(MysqlDecoder::TimestampMicros),
        other => Err(format!("no Arrow decoder for MySQL column type {other:?}")),
    }
}

/// The Arrow field type a decoder produces.
fn mysql_arrow_type(decoder: MysqlDecoder) -> DataType {
    match decoder {
        MysqlDecoder::Int64 => DataType::Int64,
        MysqlDecoder::Float64 => DataType::Float64,
        MysqlDecoder::Utf8 => DataType::Utf8,
        MysqlDecoder::Date32 => DataType::Date32,
        MysqlDecoder::TimestampMicros => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
    }
}

/// Days from the civil date (proleptic Gregorian) to the Unix epoch. Hinnant's
/// algorithm; valid for the full MySQL date range.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Decode one MySQL result column into an Arrow array, or a String error naming
/// the column and MySQL type on the first value that does not fit the decoder.
fn decode_mysql_column(
    name: &str,
    ty: mysql::consts::ColumnType,
    decoder: MysqlDecoder,
    rows: &[mysql::Row],
    index: usize,
) -> Result<ArrayRef, String> {
    match decoder {
        MysqlDecoder::Int64 => decode_mysql_int64(name, ty, rows, index),
        MysqlDecoder::Float64 => decode_mysql_float64(name, ty, rows, index),
        MysqlDecoder::Utf8 => decode_mysql_utf8(name, ty, rows, index),
        MysqlDecoder::Date32 => decode_mysql_date32(name, ty, rows, index),
        MysqlDecoder::TimestampMicros => decode_mysql_timestamp(name, ty, rows, index),
    }
}

/// Report a value that does not fit its column's decoder, naming the column and
/// its MySQL type (the loud decode-error contract).
fn mysql_decode_err(name: &str, ty: mysql::consts::ColumnType, value: &mysql::Value) -> String {
    format!("MySQL column '{name}' ({ty:?}): cannot decode value {value:?}")
}

fn decode_mysql_int64(
    name: &str,
    ty: mysql::consts::ColumnType,
    rows: &[mysql::Row],
    index: usize,
) -> Result<ArrayRef, String> {
    let mut builder = arrow::array::Int64Builder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(index).unwrap_or(&mysql::Value::NULL) {
            mysql::Value::NULL => builder.append_null(),
            mysql::Value::Int(v) => builder.append_value(*v),
            mysql::Value::UInt(v) => builder.append_value(
                i64::try_from(*v)
                    .map_err(|_| mysql_decode_err(name, ty, &mysql::Value::UInt(*v)))?,
            ),
            mysql::Value::Bytes(bytes) => {
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| mysql_decode_err(name, ty, &mysql::Value::Bytes(bytes.clone())))?;
                builder.append_value(text.trim().parse::<i64>().map_err(|_| {
                    mysql_decode_err(name, ty, &mysql::Value::Bytes(bytes.clone()))
                })?);
            }
            other => return Err(mysql_decode_err(name, ty, other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn decode_mysql_float64(
    name: &str,
    ty: mysql::consts::ColumnType,
    rows: &[mysql::Row],
    index: usize,
) -> Result<ArrayRef, String> {
    let mut builder = arrow::array::Float64Builder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(index).unwrap_or(&mysql::Value::NULL) {
            mysql::Value::NULL => builder.append_null(),
            mysql::Value::Double(v) => builder.append_value(*v),
            mysql::Value::Float(v) => builder.append_value(f64::from(*v)),
            mysql::Value::Int(v) => builder.append_value(*v as f64),
            mysql::Value::UInt(v) => builder.append_value(*v as f64),
            mysql::Value::Bytes(bytes) => {
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| mysql_decode_err(name, ty, &mysql::Value::Bytes(bytes.clone())))?;
                builder.append_value(text.trim().parse::<f64>().map_err(|_| {
                    mysql_decode_err(name, ty, &mysql::Value::Bytes(bytes.clone()))
                })?);
            }
            other => return Err(mysql_decode_err(name, ty, other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn decode_mysql_utf8(
    name: &str,
    ty: mysql::consts::ColumnType,
    rows: &[mysql::Row],
    index: usize,
) -> Result<ArrayRef, String> {
    let mut builder = arrow::array::StringBuilder::new();
    for row in rows {
        match row.as_ref(index).unwrap_or(&mysql::Value::NULL) {
            mysql::Value::NULL => builder.append_null(),
            mysql::Value::Bytes(bytes) => {
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| mysql_decode_err(name, ty, &mysql::Value::Bytes(bytes.clone())))?;
                builder.append_value(text);
            }
            other => return Err(mysql_decode_err(name, ty, other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn decode_mysql_date32(
    name: &str,
    ty: mysql::consts::ColumnType,
    rows: &[mysql::Row],
    index: usize,
) -> Result<ArrayRef, String> {
    let mut builder = arrow::array::Date32Builder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(index).unwrap_or(&mysql::Value::NULL) {
            mysql::Value::NULL => builder.append_null(),
            mysql::Value::Date(y, mo, d, _, _, _, _) => {
                let days = days_from_civil(i64::from(*y), i64::from(*mo), i64::from(*d));
                builder.append_value(
                    i32::try_from(days)
                        .map_err(|_| mysql_decode_err(name, ty, row.as_ref(index).unwrap()))?,
                );
            }
            other => return Err(mysql_decode_err(name, ty, other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn decode_mysql_timestamp(
    name: &str,
    ty: mysql::consts::ColumnType,
    rows: &[mysql::Row],
    index: usize,
) -> Result<ArrayRef, String> {
    let mut builder = arrow::array::TimestampMicrosecondBuilder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(index).unwrap_or(&mysql::Value::NULL) {
            mysql::Value::NULL => builder.append_null(),
            mysql::Value::Date(y, mo, d, h, mi, s, us) => {
                let days = days_from_civil(i64::from(*y), i64::from(*mo), i64::from(*d));
                let micros = days * 86_400_000_000
                    + i64::from(*h) * 3_600_000_000
                    + i64::from(*mi) * 60_000_000
                    + i64::from(*s) * 1_000_000
                    + i64::from(*us);
                builder.append_value(micros);
            }
            other => return Err(mysql_decode_err(name, ty, other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Run `sql` against a MySQL source and decode the result set into one Arrow
/// batch. The whole set is buffered (the engine's other source reads do the
/// same) so column decoders see every row before a batch is built.
fn fetch_mysql(s: &DsSpec, sql: &str) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    trace_sql(&s.uri, sql);
    let key = (current_session(), s.uri.clone());
    MYSQL_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(&key) {
            map.insert(key.clone(), open_mysql(&s.uri).map_err(ExecError::runtime)?);
        }
        let conn = map.get_mut(&key).unwrap();
        run_mysql_query(conn, sql).map_err(ExecError::runtime)
    })
}

/// Open a MySQL connection from a `mysql://` URL.
fn open_mysql(url: &str) -> Result<mysql::Conn, String> {
    let opts = mysql::Opts::from_url(url).map_err(|e| format!("mysql url: {e}"))?;
    mysql::Conn::new(mysql::OptsBuilder::from_opts(opts)).map_err(|e| format!("mysql connect: {e}"))
}

fn run_mysql_query(
    conn: &mut mysql::Conn,
    sql: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    use mysql::prelude::Queryable;

    let mut result = conn
        .query_iter(sql)
        .map_err(|e| format!("mysql query: {e}"))?;
    let columns: Vec<mysql::Column> = result.columns().as_ref().to_vec();
    let mut rows: Vec<mysql::Row> = Vec::new();
    for row in result.by_ref() {
        rows.push(row.map_err(|e| format!("mysql row: {e}"))?);
    }

    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let name = column.name_str().to_string();
        let ty = column.column_type();
        let decoder = mysql_decoder(ty)?;
        fields.push(Field::new(&name, mysql_arrow_type(decoder), true));
        arrays.push(decode_mysql_column(&name, ty, decoder, &rows, index)?);
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    if arrays.is_empty() {
        return Ok((schema, Vec::new()));
    }
    let batch =
        RecordBatch::try_new(schema.clone(), arrays).map_err(|e| format!("mysql batch: {e}"))?;
    Ok((schema, vec![batch]))
}

// Returns a String error (not PyErr) so it can also run on a worker thread that
// does not hold the GIL (the parallel-scan path spawns such threads).
fn open_pg(s: &DsSpec) -> Result<PgConn, String> {
    use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver};

    let driver_path = s
        .adbc_driver
        .as_deref()
        .ok_or_else(|| "postgres datasource requires an 'adbc_driver' path".to_string())?;
    let mut driver = adbc_driver_manager::ManagedDriver::load_dynamic_from_filename(
        driver_path,
        None,
        AdbcVersion::V100,
    )
    .map_err(|e| format!("load adbc driver: {e}"))?;
    let opts = [(OptionDatabase::Uri, OptionValue::String(s.uri.clone()))];
    let database = driver
        .new_database_with_opts(opts)
        .map_err(|e| format!("adbc database: {e}"))?;
    let conn = database
        .new_connection()
        .map_err(|e| format!("adbc connection: {e}"))?;
    Ok(PgConn {
        _driver: driver,
        _database: database,
        conn,
    })
}

fn fetch_postgres(name: &str, s: &DsSpec, sql: &str) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    trace_sql(name, sql);
    let key = (current_session(), name.to_string());
    PG_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(&key) {
            map.insert(key.clone(), open_pg(s).map_err(ExecError::runtime)?);
        }
        let pg = map.get_mut(&key).unwrap();
        run_query(&mut pg.conn, sql).map_err(ExecError::runtime)
    })
}

type PgConnection = adbc_driver_manager::ManagedConnection;

/// Run a query on a connection and return its (numeric-normalized) Arrow result.
fn run_query(conn: &mut PgConnection, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    use adbc_core::{Connection, Statement};

    let mut stmt = conn
        .new_statement()
        .map_err(|e| format!("adbc statement: {e}"))?;
    stmt.set_sql_query(sql)
        .map_err(|e| format!("adbc set sql: {e}"))?;
    let reader = stmt.execute().map_err(|e| format!("adbc execute: {e}"))?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| format!("adbc batch: {e}"))?);
    }
    let (schema, batches) =
        normalize_numeric(schema, batches).map_err(|e| format!("numeric normalize: {e}"))?;
    reconcile_executed(schema, batches).map_err(|e| format!("schema reconcile: {e}"))
}

/// Make the reported schema agree with the executed batches.
///
/// ADBC can report a result column at one type while the batches arrive at
/// another: a temp-join result loses the NUMERIC typmod, so the stream schema
/// says Decimal128(38, 0) while the data-derived batches carry (38, 2) - and
/// without the typname metadata `normalize_numeric` never touches it (TPC-DS
/// q64 at SF1). A binding whose declared schema disagrees with its executed
/// batches poisons every downstream MemTable registration, so disagreeing
/// Decimal columns re-derive through the same string round-trip the metadata
/// path uses; any other disagreement is a driver contract violation and
/// fails loudly rather than shipping a lying schema.
fn reconcile_executed(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(SchemaRef, Vec<RecordBatch>), arrow::error::ArrowError> {
    let mut decimals: Vec<usize> = Vec::new();
    for batch in &batches {
        if batch.schema() == schema {
            continue;
        }
        collect_decimal_disagreements(&schema, &batch.schema(), &mut decimals)?;
    }
    if decimals.is_empty() {
        return Ok((schema, batches));
    }
    rebuild_decimals(schema, batches, &decimals)
}

/// Record the columns where two schemas disagree on a Decimal128 type; any
/// non-decimal disagreement is a driver contract violation and fails loudly.
fn collect_decimal_disagreements(
    schema: &SchemaRef,
    executed: &SchemaRef,
    decimals: &mut Vec<usize>,
) -> Result<(), arrow::error::ArrowError> {
    for (index, field) in schema.fields().iter().enumerate() {
        let executed_type = executed.field(index).data_type();
        if executed_type == field.data_type() {
            continue;
        }
        let both_decimal = matches!(field.data_type(), DataType::Decimal128(_, _))
            && matches!(executed_type, DataType::Decimal128(_, _));
        if !both_decimal {
            return Err(arrow::error::ArrowError::SchemaError(format!(
                "column '{}' reported as {:?} but executed as {:?}",
                field.name(),
                field.data_type(),
                executed_type
            )));
        }
        if !decimals.contains(&index) {
            decimals.push(index);
        }
    }
    Ok(())
}

/// Run a statement with no result set (DDL: DROP TABLE, etc.).
fn exec_update(conn: &mut PgConnection, sql: &str) -> Result<(), String> {
    use adbc_core::{Connection, Statement};

    let mut stmt = conn
        .new_statement()
        .map_err(|e| format!("adbc statement: {e}"))?;
    stmt.set_sql_query(sql)
        .map_err(|e| format!("adbc set sql: {e}"))?;
    stmt.execute_update()
        .map_err(|e| format!("adbc execute_update: {e}"))?;
    Ok(())
}

/// Postgres `numeric`/`decimal` columns arrive over ADBC as an opaque
/// string-backed extension type; DataFusion is strictly typed and will not do
/// arithmetic on them. Convert such columns to an exact `Decimal128(38, scale)`
/// at the boundary. The scale is derived from the values themselves (the widest
/// fractional part seen, over every batch) so nothing is rounded away - exact
/// decimal arithmetic then matches Postgres/DuckDB to the last digit.
fn normalize_numeric(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(SchemaRef, Vec<RecordBatch>), arrow::error::ArrowError> {
    let numeric: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            matches!(
                f.metadata()
                    .get("ADBC:postgresql:typname")
                    .map(String::as_str),
                Some("numeric") | Some("decimal")
            )
        })
        .map(|(i, _)| i)
        .collect();
    if numeric.is_empty() {
        return Ok((schema, batches));
    }
    rebuild_decimals(schema, batches, &numeric)
}

/// Re-derive the given columns as Decimal128 with a data-derived scale (via a
/// string round-trip) and rebuild every batch under the one resulting schema.
fn rebuild_decimals(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    columns: &[usize],
) -> Result<(SchemaRef, Vec<RecordBatch>), arrow::error::ArrowError> {
    let (strings, scales) = decimal_strings_and_scales(columns, &batches)?;
    let new_schema = decimal_schema(&schema, &scales);
    let mut out = Vec::with_capacity(batches.len());
    for (bi, batch) in batches.iter().enumerate() {
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        for (i, col) in batch.columns().iter().enumerate() {
            match scales.get(&i) {
                Some(scale) => {
                    let dtype = DataType::Decimal128(DECIMAL_PRECISION, *scale);
                    cols.push(arrow::compute::cast(&strings[bi][&i], &dtype)?);
                }
                None => cols.push(col.clone()),
            }
        }
        out.push(RecordBatch::try_new(new_schema.clone(), cols)?);
    }
    Ok((new_schema, out))
}

/// Max Decimal128 precision; the scale is derived per column, leaving the rest
/// for integer digits (ample for any real value).
const DECIMAL_PRECISION: u8 = 38;

/// For each numeric column, cast every batch's values to a Utf8 array (unwrapping
/// the opaque extension) and record the widest fractional-digit count seen.
fn decimal_strings_and_scales(
    numeric: &[usize],
    batches: &[RecordBatch],
) -> Result<(Vec<HashMap<usize, ArrayRef>>, HashMap<usize, i8>), arrow::error::ArrowError> {
    let mut scales: HashMap<usize, i8> = numeric.iter().map(|&i| (i, 0i8)).collect();
    let mut strings: Vec<HashMap<usize, ArrayRef>> = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut per_batch = HashMap::new();
        for &i in numeric {
            let utf8 = arrow::compute::cast(batch.column(i), &DataType::Utf8)?;
            let observed =
                max_fractional_digits(utf8.as_any().downcast_ref::<StringArray>().unwrap());
            let scale = scales.get_mut(&i).unwrap();
            *scale = (*scale).max(observed);
            per_batch.insert(i, utf8);
        }
        strings.push(per_batch);
    }
    Ok((strings, scales))
}

/// The widest number of digits after the decimal point in a string array.
fn max_fractional_digits(values: &StringArray) -> i8 {
    let mut widest = 0i8;
    for row in 0..values.len() {
        if values.is_null(row) {
            continue;
        }
        if let Some(dot) = values.value(row).find('.') {
            let frac = (values.value(row).len() - dot - 1).min(37) as i8;
            widest = widest.max(frac);
        }
    }
    widest
}

/// Rebuild the schema with each numeric column re-typed to its Decimal128 scale.
fn decimal_schema(schema: &SchemaRef, scales: &HashMap<usize, i8>) -> SchemaRef {
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| match scales.get(&i) {
            Some(scale) => Arc::new(Field::new(
                f.name(),
                DataType::Decimal128(DECIMAL_PRECISION, *scale),
                f.is_nullable(),
            )),
            None => f.clone(),
        })
        .collect();
    Arc::new(Schema::new(fields))
}

// --- parallel partitioned scan -----------------------------------------------
//
// A large whole-table read over one Postgres connection is bandwidth-bound. We
// match DuckDB's postgres scanner: split the table's heap into `ctid` page
// ranges and read them concurrently over N connections (each a binary COPY),
// then concatenate the Arrow. `ctid` is a row's physical (page, tuple) address,
// so page ranges partition the table with no partition column and no overlap.

/// Escape a single-quoted SQL string literal.
fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

/// Quote a (schema.)table reference.
fn qualified_table(schema: Option<&str>, table: &str) -> String {
    let t = table.replace('"', "\"\"");
    match schema {
        Some(s) => format!("\"{}\".\"{}\"", s.replace('"', "\"\""), t),
        None => format!("\"{t}\""),
    }
}

/// The table's heap page count (`pg_class.relpages`), for sizing the ranges.
fn relpages(name: &str, schema: Option<&str>, table: &str) -> ExecResult<u32> {
    let pred = match schema {
        Some(s) => format!("c.relname = '{}' AND n.nspname = '{}'", esc(table), esc(s)),
        None => format!("c.relname = '{}'", esc(table)),
    };
    let sql = format!(
        "SELECT c.relpages::bigint FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace WHERE {pred}"
    );
    let (_, batches) = fetch(name, &sql)?;
    for batch in &batches {
        if batch.num_rows() > 0 {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ExecError::runtime("relpages is not int8"))?;
            return Ok(col.value(0).max(0) as u32);
        }
    }
    Err(ExecError::runtime(format!(
        "table '{table}' not found for relpages"
    )))
}

const PARALLEL_WORKERS: usize = 8;

type QueryResult = Result<(SchemaRef, Vec<RecordBatch>), String>;

/// A partition read for a worker to run. The connection stays on the worker
/// (ADBC handles are not Send); only the job and its Arrow result cross channels.
/// `session` keys the worker's connection cache so two runtimes reusing a name
/// keep separate connections and a dropped runtime's are pruned.
struct Job {
    session: SessionId,
    name: String,
    spec: DsSpec,
    sql: String,
    reply: std::sync::mpsc::Sender<QueryResult>,
}

/// A message to a ctid reader worker: a partition read, or a prune dropping one
/// session's cached connections on that worker.
enum ParallelMsg {
    Run(Job),
    Prune(SessionId),
}

/// A fixed pool of long-lived reader threads, each keeping its own connections.
/// Persisting the threads pools connections across parallel scans, so repeated
/// reads pay no reconnect cost (the reason a spawn-per-call design was slower).
fn parallel_pool() -> &'static Vec<std::sync::mpsc::Sender<ParallelMsg>> {
    static POOL: OnceLock<Vec<std::sync::mpsc::Sender<ParallelMsg>>> = OnceLock::new();
    POOL.get_or_init(|| {
        let mut senders = Vec::new();
        for _ in 0..PARALLEL_WORKERS {
            let (tx, rx) = std::sync::mpsc::channel::<ParallelMsg>();
            std::thread::spawn(move || worker_loop(rx));
            senders.push(tx);
        }
        senders
    })
}

/// Drop `session`'s cached connections on every ctid reader worker (the pool
/// threads stay alive). Sent by `prune_session` as a runtime drops.
fn prune_parallel_pool(session: SessionId) {
    for sender in parallel_pool() {
        let _ = sender.send(ParallelMsg::Prune(session));
    }
}

/// One reader thread: keep a per-(session, datasource) connection and serve jobs
/// until the pool is dropped (process exit).
fn worker_loop(rx: std::sync::mpsc::Receiver<ParallelMsg>) {
    let mut conns: HashMap<(SessionId, String), PgConn> = HashMap::new();
    while let Ok(msg) = rx.recv() {
        match msg {
            ParallelMsg::Run(job) => {
                let _ = job.reply.send(run_job(&mut conns, &job));
            }
            ParallelMsg::Prune(session) => conns.retain(|key, _| key.0 != session),
        }
    }
}

fn run_job(conns: &mut HashMap<(SessionId, String), PgConn>, job: &Job) -> QueryResult {
    let key = (job.session, job.name.clone());
    if !conns.contains_key(&key) {
        conns.insert(key.clone(), open_pg(&job.spec)?);
    }
    let pg = conns.get_mut(&key).unwrap();
    run_query(&mut pg.conn, &job.sql)
}

/// Read `select_list` from a Postgres table with `partitions`-way parallel
/// ctid-partitioned binary COPY reads, concatenated into one Arrow result.
pub fn fetch_parallel(
    name: &str,
    schema: Option<&str>,
    table: &str,
    alias: Option<&str>,
    select_list: &str,
    partitions: usize,
    where_clause: Option<&str>,
) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    let s = spec(name)?;
    if s.kind != DsKind::Postgres {
        return Err(ExecError::runtime("parallel fetch is Postgres-only"));
    }
    let pages = relpages(name, schema, table)?;
    // Render the alias so a select-list or filter that qualifies columns by it
    // resolves (the unqualified `ctid` still binds to the single table).
    let table_ref = match alias {
        Some(a) => format!(
            "{} AS \"{}\"",
            qualified_table(schema, table),
            a.replace('"', "\"\"")
        ),
        None => qualified_table(schema, table),
    };
    let extra = match where_clause {
        Some(w) => format!(" AND ({w})"),
        None => String::new(),
    };

    let session = current_session();
    let pool = parallel_pool();
    let mut replies = Vec::new();
    for (i, (lo, hi)) in ctid_ranges(pages, partitions).into_iter().enumerate() {
        let sql = format!(
            "SELECT {select_list} FROM {table_ref} \
             WHERE ctid >= '({lo},0)'::tid AND ctid < '({hi},0)'::tid{extra}"
        );
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let job = Job {
            session,
            name: name.to_string(),
            spec: s.clone(),
            sql,
            reply: reply_tx,
        };
        pool[i % pool.len()]
            .send(ParallelMsg::Run(job))
            .map_err(|_| ExecError::runtime("parallel worker gone"))?;
        replies.push(reply_rx);
    }

    let mut result_schema: Option<SchemaRef> = None;
    let mut all = Vec::new();
    for reply_rx in replies {
        let joined = reply_rx
            .recv()
            .map_err(|_| ExecError::runtime("parallel worker dropped its reply"))?;
        let (schema, batches) = joined.map_err(ExecError::runtime)?;
        result_schema.get_or_insert(schema);
        all.extend(batches);
    }
    let result_schema =
        result_schema.ok_or_else(|| ExecError::runtime("parallel fetch produced no partitions"))?;
    // Partitions normalize their NUMERIC scales independently from their own
    // rows: an empty (or integral-only) FIRST partition reports scale 0 while
    // a later partition's batches carry the data-derived scale (TPC-DS q64 at
    // SF1: item filtered to 12 rows across 8 ctid ranges). Reconcile so the
    // one result schema agrees with EVERY batch.
    reconcile_executed(result_schema, all)
        .map_err(|e| ExecError::runtime(format!("parallel schema reconcile: {e}")))
}

// --- temp-table dynamic-filter pushdown --------------------------------------
//
// For a high-cardinality dynamic filter (too many keys for an IN list, but
// selective enough that a full scan wastes bandwidth), push the build keys into
// a session TEMP TABLE on the probe connection and let Postgres do the reduction
// server-side, transferring only matching rows. Ingest, join, and drop all run
// on the one pooled connection (temp tables are session-local).

/// Ingest key batches into a fresh TEMP TABLE via ADBC bulk ingest (binary COPY).
fn ingest_temp(
    conn: &mut PgConnection,
    table: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(), String> {
    use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
    use adbc_core::{Connection, Optionable, Statement};

    let mut stmt = conn
        .new_statement()
        .map_err(|e| format!("adbc statement: {e}"))?;
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table.to_string()),
    )
    .map_err(|e| format!("adbc target table: {e}"))?;
    stmt.set_option(
        OptionStatement::Temporary,
        OptionValue::String("true".to_string()),
    )
    .map_err(|e| format!("adbc temporary: {e}"))?;
    stmt.set_option(OptionStatement::IngestMode, IngestMode::Create.into())
        .map_err(|e| format!("adbc ingest mode: {e}"))?;

    let items: Vec<Result<RecordBatch, arrow::error::ArrowError>> =
        batches.into_iter().map(Ok).collect();
    let reader = arrow::array::RecordBatchIterator::new(items.into_iter(), schema);
    stmt.bind_stream(Box::new(reader))
        .map_err(|e| format!("adbc bind: {e}"))?;
    stmt.execute_update()
        .map_err(|e| format!("adbc ingest: {e}"))?;
    Ok(())
}

/// Push `keys` into a temp table, run `join_sql` against it, drop it, and
/// return the (reduced) Arrow result. All on the one pooled connection.
pub fn fetch_temp_join(
    name: &str,
    temp_table: &str,
    keys_schema: SchemaRef,
    keys_batches: Vec<RecordBatch>,
    join_sql: &str,
) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    let s = spec(name)?;
    match s.kind {
        DsKind::Postgres => {
            fetch_temp_join_pg(name, &s, temp_table, keys_schema, keys_batches, join_sql)
        }
        DsKind::DuckDb => {
            fetch_temp_join_duckdb(&s, temp_table, keys_schema, keys_batches, join_sql)
        }
        DsKind::Parquet => Err(ExecError::runtime(
            "temp-join pushdown does not apply to in-process Parquet",
        )),
        DsKind::ClickHouse => Err(ExecError::runtime(
            "temp-join pushdown is not implemented for ClickHouse",
        )),
        DsKind::MySql => Err(ExecError::runtime(
            "temp-join pushdown is not implemented for MySQL",
        )),
        DsKind::Materialized => Err(ExecError::runtime(
            "temp-join pushdown does not apply to the in-process materialized-view store",
        )),
    }
}

fn fetch_temp_join_pg(
    name: &str,
    s: &DsSpec,
    temp_table: &str,
    keys_schema: SchemaRef,
    keys_batches: Vec<RecordBatch>,
    join_sql: &str,
) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    let key = (current_session(), name.to_string());
    let drop_sql = format!(
        "DROP TABLE IF EXISTS \"{}\"",
        temp_table.replace('"', "\"\"")
    );
    PG_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(&key) {
            map.insert(key.clone(), open_pg(s).map_err(ExecError::runtime)?);
        }
        let pg = map.get_mut(&key).unwrap();

        exec_update(&mut pg.conn, &drop_sql).map_err(ExecError::runtime)?;
        ingest_temp(&mut pg.conn, temp_table, keys_schema, keys_batches)
            .map_err(ExecError::runtime)?;
        let result = run_query(&mut pg.conn, join_sql);
        // Best-effort cleanup; the ingest side already succeeded.
        let _ = exec_update(&mut pg.conn, &drop_sql);
        result.map_err(ExecError::runtime)
    })
}

/// The DuckDB arm of the temp-table pushdown. DuckDB temp tables are
/// connection-scoped, so the key ingest (Arrow appender) and the semi-join must
/// share one connection: a single cursor on the process-wide cached instance
/// (see `duck_cursor`). The temp table drops when this cursor drops.
fn fetch_temp_join_duckdb(
    s: &DsSpec,
    temp_table: &str,
    keys_schema: SchemaRef,
    keys_batches: Vec<RecordBatch>,
    join_sql: &str,
) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    let conn = duck_cursor(&s.uri)?;
    let key_field = keys_schema.field(0);
    let duck_type = duck_type_name(key_field.data_type()).map_err(ExecError::runtime)?;
    let create = format!(
        "CREATE TEMP TABLE \"{}\" (\"{}\" {})",
        temp_table.replace('"', "\"\""),
        key_field.name().replace('"', "\"\""),
        duck_type
    );
    conn.execute_batch(&create)
        .map_err(|e| ExecError::runtime(format!("duckdb temp table: {e}")))?;
    {
        let mut appender = conn
            .appender(temp_table)
            .map_err(|e| ExecError::runtime(format!("duckdb appender: {e}")))?;
        for batch in keys_batches {
            appender
                .append_record_batch(batch)
                .map_err(|e| ExecError::runtime(format!("duckdb key ingest: {e}")))?;
        }
    }
    let mut stmt = conn
        .prepare(join_sql)
        .map_err(|e| ExecError::runtime(format!("duckdb prepare: {e}")))?;
    let arrow = stmt
        .query_arrow([])
        .map_err(|e| ExecError::runtime(format!("duckdb temp-join: {e}")))?;
    let schema = arrow.get_schema();
    let mut batches = Vec::new();
    for batch in arrow {
        batches.push(batch);
    }
    Ok((schema, batches))
}

/// Whether the DuckDB temp-table ingest can represent this key column type.
pub fn duck_can_ingest(keys_schema: &SchemaRef) -> bool {
    duck_type_name(keys_schema.field(0).data_type()).is_ok()
}

/// The DuckDB column type for one Arrow key type; an error for a type the
/// ingest does not map (callers fall back to the full, unreduced fetch -
/// correct, just without the transfer saving).
fn duck_type_name(data_type: &DataType) -> Result<String, String> {
    let name = match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INTEGER".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "UTINYINT".to_string(),
        DataType::UInt16 => "USMALLINT".to_string(),
        DataType::UInt32 => "UINTEGER".to_string(),
        DataType::UInt64 => "UBIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({precision},{scale})"),
        other => return Err(format!("no DuckDB ingest type for Arrow {other}")),
    };
    Ok(name)
}

/// Estimate the fraction of the probe table a dynamic filter of `num_keys`
/// distinct values would select. Returns None when statistics are unavailable
/// (caller should then prefer the safe temp-table path). Used to choose
/// between the temp-table pushdown and a full scan.
///
/// Postgres: `pg_class.reltuples` and the column's `pg_stats.n_distinct`.
/// DuckDB: `num_keys / duckdb_tables().estimated_size` - a catalog read, no
/// scan. That equals the true selectivity for key columns (ndv = rows) and
/// UNDERestimates it for low-NDV columns, biasing toward the temp-table path;
/// bounded on DuckDB, where a semi-join costs a full vectorized scan anyway.
pub fn estimate_selectivity(
    name: &str,
    schema: Option<&str>,
    table: &str,
    column: &str,
    num_keys: usize,
) -> ExecResult<Option<f64>> {
    if kind(name)? == DsKind::DuckDb {
        return estimate_selectivity_duckdb(name, schema, table, num_keys);
    }
    let pred = match schema {
        Some(s) => format!("c.relname='{}' AND n.nspname='{}'", esc(table), esc(s)),
        None => format!("c.relname='{}'", esc(table)),
    };
    // n_distinct is `real` (float4) in pg_stats; cast both to float8 so the Arrow
    // result is Float64 (a float4 would silently fail the Float64 downcast).
    let sql = format!(
        "SELECT c.reltuples::float8, s.n_distinct::float8 FROM pg_class c \
         JOIN pg_namespace n ON n.oid=c.relnamespace \
         LEFT JOIN pg_stats s ON s.schemaname=n.nspname AND s.tablename=c.relname \
         AND s.attname='{}' WHERE {pred}",
        esc(column)
    );
    let (_, batches) = fetch(name, &sql)?;
    Ok(selectivity_from_stats(&batches, num_keys))
}

/// Whether a Postgres column has an index usable for an `col IN (...)` semi-join
/// - a btree/hash index whose LEADING key is that column, so the planner can
/// bitmap-index-scan the matches instead of sequentially scanning the table.
/// Cached per (datasource, schema, table, column): the schema is stable for the
/// session. Only meaningful for Postgres; the caller gates DuckDB on
/// selectivity alone (its scanner ignores indexes).
pub fn column_has_index(
    name: &str,
    schema: Option<&str>,
    table: &str,
    column: &str,
) -> ExecResult<bool> {
    let cache = index_cache();
    let key = (
        current_session(),
        format!(
            "{name}\u{1}{}\u{1}{table}\u{1}{column}",
            schema.unwrap_or("")
        ),
    );
    if let Some(cached) = cache.lock().unwrap().get(&key) {
        return Ok(*cached);
    }
    let indexed = pg_column_indexed(name, schema, table, column)?;
    cache.lock().unwrap().insert(key, indexed);
    Ok(indexed)
}

// The `column_has_index` result cache, keyed by (session, datasource/schema/
// table/column). Session-keyed so it prunes with the runtime that populated it.
fn index_cache() -> &'static Mutex<HashMap<(SessionId, String), bool>> {
    static CACHE: OnceLock<Mutex<HashMap<(SessionId, String), bool>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The catalog query behind `column_has_index`: an index on this table whose
/// first key column (`pg_index.indkey[0]`, an attnum) is `column`.
fn pg_column_indexed(
    name: &str,
    schema: Option<&str>,
    table: &str,
    column: &str,
) -> ExecResult<bool> {
    let pred = match schema {
        Some(s) => format!("c.relname='{}' AND n.nspname='{}'", esc(table), esc(s)),
        None => format!("c.relname='{}'", esc(table)),
    };
    let sql = format!(
        "SELECT 1 FROM pg_index i \
         JOIN pg_class c ON c.oid=i.indrelid \
         JOIN pg_namespace n ON n.oid=c.relnamespace \
         JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=i.indkey[0] \
         WHERE {pred} AND a.attname='{}' LIMIT 1",
        esc(column)
    );
    let (_, batches) = fetch(name, &sql)?;
    Ok(batches.iter().any(|b| b.num_rows() > 0))
}

/// The DuckDB selectivity upper bound: keys / catalog row count.
fn estimate_selectivity_duckdb(
    name: &str,
    schema: Option<&str>,
    table: &str,
    num_keys: usize,
) -> ExecResult<Option<f64>> {
    let pred = match schema {
        Some(s) => format!("table_name='{}' AND schema_name='{}'", esc(table), esc(s)),
        None => format!("table_name='{}'", esc(table)),
    };
    let sql = format!("SELECT estimated_size::DOUBLE FROM duckdb_tables() WHERE {pred}");
    let (_, batches) = fetch(name, &sql)?;
    Ok(duck_fraction(&batches, num_keys))
}

/// num_keys over the catalog row count, when the catalog knows the table.
fn duck_fraction(batches: &[RecordBatch], num_keys: usize) -> Option<f64> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let rows = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()?;
        if rows.is_null(0) || rows.value(0) <= 0.0 {
            return None;
        }
        return Some(num_keys as f64 / rows.value(0));
    }
    None
}

/// Build a `DsSpec` from a datasource kind and its already-extracted
/// parameters. `uri` is the source's location string (Postgres connection URI,
/// DuckDB file path, or Parquet directory); `adbc_driver` is the ADBC driver
/// path (Postgres only). Parameter extraction from the caller's config
/// happens at the FFI boundary; this keeps the kind -> `DsKind` mapping and the
/// loud validation in Rust.
pub fn spec_from_kind(
    kind: &str,
    uri: Option<String>,
    adbc_driver: Option<String>,
) -> ExecResult<DsSpec> {
    match kind {
        "postgres" | "postgresql" => {
            let uri = uri.ok_or_else(|| ExecError::value("postgres datasource needs 'uri'"))?;
            Ok(DsSpec {
                kind: DsKind::Postgres,
                uri,
                adbc_driver,
            })
        }
        "duckdb" => {
            let uri = uri.ok_or_else(|| ExecError::value("duckdb datasource needs 'path'"))?;
            Ok(DsSpec {
                kind: DsKind::DuckDb,
                uri,
                adbc_driver: None,
            })
        }
        "parquet" => {
            let uri = uri.ok_or_else(|| ExecError::value("parquet datasource needs 'dir'"))?;
            Ok(DsSpec {
                kind: DsKind::Parquet,
                uri,
                adbc_driver: None,
            })
        }
        "clickhouse" => {
            let uri =
                uri.ok_or_else(|| ExecError::value("clickhouse datasource needs an endpoint"))?;
            Ok(DsSpec {
                kind: DsKind::ClickHouse,
                uri,
                adbc_driver: None,
            })
        }
        "mysql" => {
            let uri = uri.ok_or_else(|| ExecError::value("mysql datasource needs a URL"))?;
            Ok(DsSpec {
                kind: DsKind::MySql,
                uri,
                adbc_driver: None,
            })
        }
        other => Err(ExecError::value(format!(
            "unknown datasource kind '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use std::sync::Arc;

    #[test]
    fn duck_type_names_cover_the_join_key_types() {
        assert_eq!(duck_type_name(&DataType::Int32).unwrap(), "INTEGER");
        assert_eq!(duck_type_name(&DataType::Int64).unwrap(), "BIGINT");
        assert_eq!(duck_type_name(&DataType::Utf8).unwrap(), "VARCHAR");
        assert_eq!(duck_type_name(&DataType::Date32).unwrap(), "DATE");
        assert_eq!(
            duck_type_name(&DataType::Decimal128(12, 2)).unwrap(),
            "DECIMAL(12,2)"
        );
        assert!(duck_type_name(&DataType::Binary).is_err());
    }

    #[test]
    fn duck_fraction_is_keys_over_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "estimated_size",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![Some(1000.0)])) as ArrayRef],
        )
        .unwrap();
        assert_eq!(duck_fraction(&[batch], 250), Some(0.25));
    }

    #[test]
    fn duck_fraction_none_without_catalog_row() {
        assert_eq!(duck_fraction(&[], 250), None);
    }

    #[test]
    fn duck_temp_join_roundtrip_in_memory() {
        // End-to-end on an in-memory database: create the probe, ingest keys
        // through the temp-table arm's own steps, and semi-join.
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE probe (k BIGINT, v VARCHAR);
             INSERT INTO probe SELECT g, 'v' || g FROM range(0, 100) t(g);
             CREATE TEMP TABLE fedq_dyn_keys (k BIGINT);",
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_i64, 3, 5])) as ArrayRef],
        )
        .unwrap();
        {
            let mut appender = conn.appender("fedq_dyn_keys").unwrap();
            appender.append_record_batch(batch).unwrap();
        }
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM probe WHERE k IN (SELECT k FROM fedq_dyn_keys)",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);
    }

    /// Seed an in-memory DuckDB instance holding one BIGINT value under `uri` for
    /// `session`, and register the datasource `name` pointing at it.
    fn seed_named_duck(session: SessionId, name: &str, uri: &str, value: i64) {
        register(
            session,
            name.to_string(),
            DsSpec {
                kind: DsKind::DuckDb,
                uri: uri.to_string(),
                adbc_driver: None,
            },
        );
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "CREATE TABLE t(v BIGINT); INSERT INTO t VALUES ({value})"
        ))
        .unwrap();
        seed_duck_connection(session, uri.to_string(), conn);
    }

    /// The single BIGINT value the current session reads from `name`.
    fn read_single_bigint(name: &str) -> i64 {
        let (_schema, batches) = fetch(name, "SELECT v FROM t").unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[test]
    fn two_sessions_reusing_a_name_read_their_own_source() {
        // Two sessions register the SAME datasource name pointing at different
        // DuckDB instances; the session-keyed registry must give each session
        // its own source. A name-only registry would let one registration
        // clobber the other and both sessions would read one source.
        let s1 = open_session();
        let s2 = open_session();
        seed_named_duck(s1, "shared", "bleed_uri_1", 111);
        seed_named_duck(s2, "shared", "bleed_uri_2", 222);

        let v1 = {
            let _scope = SessionScope::enter(s1);
            read_single_bigint("shared")
        };
        let v2 = {
            let _scope = SessionScope::enter(s2);
            read_single_bigint("shared")
        };
        assert_eq!(v1, 111, "session 1 must read its own source");
        assert_eq!(v2, 222, "session 2 must read its own source");

        prune_session(s1);
        prune_session(s2);
    }

    #[test]
    fn prune_session_drops_every_map_entry_for_it() {
        // A dropped runtime's state must leave the process-wide maps so its
        // connections/contexts are released and cannot be read again.
        let session = open_session();
        seed_named_duck(session, "d", "prune_uri", 7);
        let reg_key = (session, "d".to_string());
        let duck_key = (session, "prune_uri".to_string());
        assert!(registry().lock().unwrap().contains_key(&reg_key));
        assert!(duck_base_cache().lock().unwrap().contains_key(&duck_key));
        assert!(live_sessions().lock().unwrap().contains(&session));

        prune_session(session);

        assert!(!registry().lock().unwrap().contains_key(&reg_key));
        assert!(!duck_base_cache().lock().unwrap().contains_key(&duck_key));
        assert!(!live_sessions().lock().unwrap().contains(&session));
    }

    #[test]
    fn one_session_prune_leaves_another_running() {
        // Pruning one session must not touch a concurrently live session's state.
        let keep = open_session();
        let drop_it = open_session();
        seed_named_duck(keep, "src", "keep_uri", 55);
        seed_named_duck(drop_it, "src", "drop_uri", 99);

        prune_session(drop_it);

        let kept = {
            let _scope = SessionScope::enter(keep);
            read_single_bigint("src")
        };
        assert_eq!(kept, 55, "the surviving session still reads its source");
        assert!(!registry()
            .lock()
            .unwrap()
            .contains_key(&(drop_it, "src".to_string())));
        prune_session(keep);
    }
}
