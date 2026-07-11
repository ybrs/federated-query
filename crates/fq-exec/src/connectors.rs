// Imported fedqrs connectors - already validated by the TPC tallies; pedantic-
// cleaned incrementally, not on import.
#![allow(clippy::all, clippy::pedantic)]
//! Native source connectors and the datasource registry.
//!
//! Python registers each datasource once at session init via
//! `register_datasource`; the engine then fetches from a source by name, in
//! Rust, over a native driver. Fetched data stays in Rust (as Arrow) for the
//! rest of the query - it is never revived into Python objects.
//!
//! Postgres reads go over ADBC (the same C driver the Python path uses), which
//! decodes the wire straight into Arrow. DuckDB lands in a later phase.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::core::partition::{ctid_ranges, selectivity_from_stats};
use crate::core::types::DsKind;
use crate::error::{ExecError, ExecResult};

/// Connection parameters for one registered datasource. Parameters are stored
/// (not a live handle) for now; pooling live connections is a later step.
#[derive(Clone)]
pub struct DsSpec {
    pub kind: DsKind,
    /// Postgres: the connection URI. DuckDB: the database file path.
    pub uri: String,
    /// Path to the ADBC driver shared library (Postgres only).
    pub adbc_driver: Option<String>,
}

static REGISTRY: OnceLock<Mutex<HashMap<String, DsSpec>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, DsSpec>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register (or replace) a datasource under `name`.
pub fn register(name: String, spec: DsSpec) {
    registry().lock().unwrap().insert(name, spec);
}

fn spec(name: &str) -> ExecResult<DsSpec> {
    registry()
        .lock()
        .unwrap()
        .get(name)
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
    // One live connection per datasource name, on the query-driving thread.
    // ADBC handles are not Send, and all fetches run on this one thread, so a
    // thread-local cache pools connections without any locking.
    static PG_CACHE: RefCell<HashMap<String, PgConn>> = RefCell::new(HashMap::new());
    // DuckDB connections PINNED for the current query, keyed by database
    // path. A pinned connection exists only after a `ship` step created a
    // temp table on it: temp objects are per-connection, so every later
    // fetch against that database must run on the SAME connection to see
    // the shipped relation. Cleared (dropping the temp tables with it) at
    // query end by `clear_shipments`.
    static PINNED_DUCK: RefCell<HashMap<String, duckdb::Connection>> = RefCell::new(HashMap::new());
    // (datasource name, table) pairs shipped as Postgres session TEMP TABLEs on
    // the pooled PG_CACHE connection this query. Unlike DuckDB (where dropping
    // the pinned connection drops its temp tables), the PG connection is reused
    // across queries, so each shipped table is DROPped by name at query end.
    static SHIPPED_PG: RefCell<Vec<(String, String)>> = RefCell::new(Vec::new());
}

/// A hard safety backstop on how many rows a single shipped relation may hold.
/// The optimizer only ships a dimension it ESTIMATES at <= ~200k rows
/// (SHIP_ROW_BUDGET in the planner), so this cap never trips a correct decision
/// - it exists solely to catch STALE STATS: a dimension estimated small but
/// actually huge, which would otherwise ingest for minutes-to-hours and hammer
/// the target (an ADBC binary-COPY of a giant relation into Postgres). Failing
/// loud here turns that runaway into an immediate, honest crash - a crash never
/// ships a lie. The value is a deliberately large multiple of the plan budget;
/// there is no query-derived "right" number for a safety valve, so it is a fixed
/// constant for now and will move to user-overridable settings later.
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
    PINNED_DUCK.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(&s.uri) {
            map.insert(s.uri.clone(), duck_cursor(&s.uri)?);
        }
        let conn = map.get(&s.uri).unwrap();
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
    let drop_sql = format!("DROP TABLE IF EXISTS \"{}\"", table.replace('"', "\"\""));
    PG_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(name) {
            map.insert(name.to_string(), open_pg(s).map_err(ExecError::runtime)?);
        }
        let pg = map.get_mut(name).unwrap();
        exec_update(&mut pg.conn, &drop_sql).map_err(ExecError::runtime)?;
        ingest_temp(&mut pg.conn, table, schema, batches).map_err(ExecError::runtime)?;
        // ANALYZE the freshly-ingested temp table so Postgres plans the island
        // join over real row counts / NDVs instead of the default guess for an
        // unstatted relation. The ingest already succeeded, so a failure here is
        // real and propagates rather than being swallowed.
        let analyze_sql = format!("ANALYZE \"{}\"", table.replace('"', "\"\""));
        exec_update(&mut pg.conn, &analyze_sql).map_err(ExecError::runtime)
    })?;
    SHIPPED_PG.with(|s| s.borrow_mut().push((name.to_string(), table.to_string())));
    Ok(())
}

/// Drop every shipped temp table: the pinned DuckDB connections (and their temp
/// tables with them), and each Postgres shipped table by name on its pooled
/// connection. Runs on every query exit path.
pub fn clear_shipments() {
    PINNED_DUCK.with(|cache| cache.borrow_mut().clear());
    drop_shipped_pg();
}

/// DROP each Postgres shipped table on its pooled connection, then forget them.
fn drop_shipped_pg() {
    let shipped: Vec<(String, String)> = SHIPPED_PG.with(|s| s.borrow_mut().drain(..).collect());
    PG_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        for (name, table) in shipped {
            if let Some(pg) = map.get_mut(&name) {
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
    }
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
    let ctx = parquet_ctx(&s.uri).map_err(|e| ExecError::runtime(format!("parquet: {e}")))?;
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
    dir: &str,
) -> Result<datafusion::prelude::SessionContext, datafusion::error::DataFusionError> {
    static CACHE: OnceLock<Mutex<HashMap<String, datafusion::prelude::SessionContext>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = cache.lock().unwrap();
    if let Some(ctx) = map.get(dir) {
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
    map.insert(dir.to_string(), ctx.clone());
    Ok(ctx)
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
        let options = ListingOptions::new(std::sync::Arc::new(ParquetFormat::default()))
            .with_collect_stat(true);
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

fn open_duckdb(path: &str) -> Result<duckdb::Connection, String> {
    // Read-only open: DuckDB is single-writer per file per process, so the
    // runtime's metadata/stats handle (fq-connectors, also read-only) and this
    // execution handle must both be read-only to coexist on one file. The
    // federated benchmarks are read-only; the only writes this engine issues are
    // CREATE TEMP TABLE (dim shipping, semi-join reduction), which DuckDB allows
    // on a read-only database because temp objects never touch the file.
    let config = duckdb::Config::default()
        .access_mode(duckdb::AccessMode::ReadOnly)
        .map_err(|e| format!("duckdb config '{path}': {e}"))?;
    duckdb::Connection::open_with_flags(path, config)
        .map_err(|e| format!("duckdb open '{path}': {e}"))
}

// A per-fetch DuckDB cursor over ONE process-wide open database instance per
// file path. `Connection::open` instantiates a whole DuckDB database (built-in
// function and type registration) - measured at ~7-10ms regardless of file size
// - so re-opening per fetch was the dominant per-fetch cost. We open each file's
// instance once (cached here, never dropped, so it stays live for the process)
// and hand out cheap cursors via `try_clone`: a `duckdb_connect` on the
// already-open database, microseconds. Each fetch gets its own cursor, so
// connection-scoped temp tables stay isolated and drop with the cursor. The base
// keeps the same read-write open mode as before, so it shares configuration with
// any other DuckDB handle on the file in this process. Mirrors the Postgres pool
// (PG_CACHE) and the Parquet SessionContext cache.
fn duck_cursor(path: &str) -> ExecResult<duckdb::Connection> {
    static CACHE: OnceLock<Mutex<HashMap<String, duckdb::Connection>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = cache.lock().unwrap();
    if !map.contains_key(path) {
        let base = open_duckdb(path).map_err(ExecError::runtime)?;
        map.insert(path.to_string(), base);
    }
    map.get(path)
        .unwrap()
        .try_clone()
        .map_err(|e| ExecError::runtime(format!("duckdb cursor '{path}': {e}")))
}

fn fetch_duckdb(s: &DsSpec, sql: &str) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    // A query that shipped a temp table must keep reading on the pinned
    // connection - temp objects are invisible to other cursors.
    let pinned = PINNED_DUCK.with(|cache| {
        let map = cache.borrow();
        match map.get(&s.uri) {
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
    PG_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(name) {
            map.insert(name.to_string(), open_pg(s).map_err(ExecError::runtime)?);
        }
        let pg = map.get_mut(name).unwrap();
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
struct Job {
    name: String,
    spec: DsSpec,
    sql: String,
    reply: std::sync::mpsc::Sender<QueryResult>,
}

/// A fixed pool of long-lived reader threads, each keeping its own connections.
/// Persisting the threads pools connections across parallel scans, so repeated
/// reads pay no reconnect cost (the reason a spawn-per-call design was slower).
fn parallel_pool() -> &'static Vec<std::sync::mpsc::Sender<Job>> {
    static POOL: OnceLock<Vec<std::sync::mpsc::Sender<Job>>> = OnceLock::new();
    POOL.get_or_init(|| {
        let mut senders = Vec::new();
        for _ in 0..PARALLEL_WORKERS {
            let (tx, rx) = std::sync::mpsc::channel::<Job>();
            std::thread::spawn(move || worker_loop(rx));
            senders.push(tx);
        }
        senders
    })
}

/// One reader thread: keep a per-datasource connection and serve jobs until the
/// pool is dropped (process exit).
fn worker_loop(rx: std::sync::mpsc::Receiver<Job>) {
    let mut conns: HashMap<String, PgConn> = HashMap::new();
    while let Ok(job) = rx.recv() {
        let _ = job.reply.send(run_job(&mut conns, &job));
    }
}

fn run_job(conns: &mut HashMap<String, PgConn>, job: &Job) -> QueryResult {
    if !conns.contains_key(&job.name) {
        conns.insert(job.name.clone(), open_pg(&job.spec)?);
    }
    let pg = conns.get_mut(&job.name).unwrap();
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

    let pool = parallel_pool();
    let mut replies = Vec::new();
    for (i, (lo, hi)) in ctid_ranges(pages, partitions).into_iter().enumerate() {
        let sql = format!(
            "SELECT {select_list} FROM {table_ref} \
             WHERE ctid >= '({lo},0)'::tid AND ctid < '({hi},0)'::tid{extra}"
        );
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let job = Job {
            name: name.to_string(),
            spec: s.clone(),
            sql,
            reply: reply_tx,
        };
        pool[i % pool.len()]
            .send(job)
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
    let drop_sql = format!(
        "DROP TABLE IF EXISTS \"{}\"",
        temp_table.replace('"', "\"\"")
    );
    PG_CACHE.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(name) {
            map.insert(name.to_string(), open_pg(s).map_err(ExecError::runtime)?);
        }
        let pg = map.get_mut(name).unwrap();

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
    static CACHE: OnceLock<Mutex<HashMap<String, bool>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = format!(
        "{name}\u{1}{}\u{1}{table}\u{1}{column}",
        schema.unwrap_or("")
    );
    if let Some(cached) = cache.lock().unwrap().get(&key) {
        return Ok(*cached);
    }
    let indexed = pg_column_indexed(name, schema, table, column)?;
    cache.lock().unwrap().insert(key, indexed);
    Ok(indexed)
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
/// path (Postgres only). The Python-dict extraction that used to live here (the
/// pyo3 `register_datasource` entry) moves to the fedq-py crate; this keeps the
/// kind -> `DsKind` mapping and the loud validation in Rust.
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
}
