// Imported fedqrs engine: the lint allow below exempts this module from the
// workspace pedantic set (its house style predates the workspace's).
#![allow(clippy::all, clippy::pedantic)]
//! The IR interpreter: walks the orchestration steps, reads sources natively,
//! runs relational fragments on DataFusion, and returns the result as an
//! exportable Arrow stream.
//!
//! Everything stays in Rust. Source reads go over native drivers; the semi-join
//! reduction computes the build's distinct keys and injects them into the probe
//! SQL here, so key values are never handed to Python. Only the final result
//! crosses the boundary, once.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::view::ViewTable;
use datafusion::common::{Column, DataFusionError, JoinType, TableReference};
use datafusion::datasource::MemTable;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{col, lit, SessionConfig, SessionContext};
use futures::StreamExt;
use tokio::runtime::Runtime;

use crate::connectors;
use crate::core::expr::{literal_from_array, to_df_expr};
use crate::core::ir::{
    AggCall, AggSelectItem, Fragment, Ir, IrExpr, JoinKind, Projection, ScanSpec, Step,
};
use crate::core::sql::{base_filter_sql, render_expr, scan_sql, select_list_sql, temp_join_sql};
use crate::core::types::DsKind;
use crate::error::{ExecError, ExecResult};

/// Dynamic-filter strategy thresholds. Under `IN_CAP` distinct keys we inline an
/// IN list; above it we push a temp table unless the filter would select more
/// than the source's full-scan fraction, in which case a full scan of the
/// (bandwidth-bound) table wins. Partition count is tuned near the core count.
const IN_CAP: usize = 2000;
/// DuckDB's full-scan alternative is one single-stream read.
const FULL_SCAN_FRACTION: f64 = 0.40;
/// Postgres' full-scan alternative is the 8-way ctid-parallel read, whose
/// wire+decode runs ~3.6x a single stream, while the temp-table semi-join is
/// pinned to ONE connection (temp tables are connection-scoped). Measured on
/// the SF1 customer probe: 25% selectivity temp-join 75.6ms vs parallel full
/// 43ms - the break-even sits near 15%.
const PG_FULL_SCAN_FRACTION: f64 = 0.15;
/// DuckDB key-ingest ceiling: the point past which appending the keys into the
/// temp table and probing costs more than reading the probe whole, REGARDLESS
/// of selectivity. It is no longer a selectivity proxy - the near-superset case
/// that a small cap once guarded (150k keys covering most of the column - the
/// q18 regression) is now caught by `fetches_most_of_table`, which prices the
/// real keys/NDV from the planner's statistics. So this only bounds the ingest
/// work; a selective 100k-key set (q09: 108k green-part keys, 5.4% of lineitem)
/// now reduces instead of reading 60M rows whole.
const DUCK_TEMP_CAP: usize = 2_000_000;
const PARALLEL_PARTITIONS: usize = 8;
const DYN_KEYS_TEMP_TABLE: &str = "fedq_dyn_keys";
/// DataFusion memory cap. Every context draws from ONE shared pool, so the
/// engine as a whole is bounded; a fragment that would blow past it (a CROSS
/// join's cartesian product) fails with ResourcesExhausted instead of OOMing
/// the server. Sorts and grouped aggregates spill to disk via the default
/// DiskManager; hash and nested-loop joins do not spill and error instead.
const MEMORY_LIMIT_BYTES: usize = 32 * 1024 * 1024 * 1024;

/// The shared memory-capped RuntimeEnv (FairSpillPool + default DiskManager)
/// behind every DataFusion context the engine creates.
pub(crate) fn runtime_env() -> Arc<RuntimeEnv> {
    static ENV: OnceLock<Arc<RuntimeEnv>> = OnceLock::new();
    ENV.get_or_init(|| {
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(MEMORY_LIMIT_BYTES)))
            .build_arc()
            .expect("DataFusion runtime environment construction failed")
    })
    .clone()
}

/// A SessionContext wired to the shared memory-capped runtime.
fn memory_capped_context() -> SessionContext {
    SessionContext::new_with_config_rt(SessionConfig::new(), runtime_env())
}

/// Fully-read Arrow data with its schema. Clone is Arc-shallow (RecordBatch
/// columns are refcounted) - a prefetch job clones the keys it hands to a
/// worker so the driving thread's binding lifecycle stays untouched.
#[derive(Clone)]
struct Batches {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

/// A named intermediate produced by a step.
enum Binding {
    /// A relation held in memory (source result or fragment output).
    Materialized(Batches),
    /// The build side's distinct, NULL-free join-key values.
    Keys(Batches),
    /// A fragment's COMPOSED logical plan, not yet executed. Single-use
    /// chains of merge fragments fuse through this into ONE streaming
    /// DataFusion execution at the next pipeline breaker (a key collection,
    /// a multi-consumer, the return) instead of draining per step - which is
    /// what let SF10 intermediates blow the memory pool.
    Lazy(LazyRegion),
    /// A relation whose batches live in an Arrow IPC file on disk. A
    /// materialized binding past BINDING_SPILL_BYTES lands here instead of
    /// staying RAM-resident (memory the pool never accounts - the q64 RSS
    /// kill at SF10); consumers read it back as a STREAM, batch at a time,
    /// and the file is re-readable so multi-consumers need no copies.
    Spilled(SpilledBinding),
}

/// The on-disk form of a spilled relation. The temp file is reference
/// counted by DataFusion's DiskManager and deletes itself when the last
/// clone (binding or in-flight stream) drops.
struct SpilledBinding {
    schema: SchemaRef,
    file: RefCountedTempFile,
    rows: usize,
    /// In-memory Arrow size before spilling, so a consuming region can
    /// still price its total input volume.
    bytes: usize,
}

/// A composed-but-unexecuted region: the fused plan plus the MEASURED total
/// bytes of the relations feeding it (transitively through lazy inputs).
struct LazyRegion {
    plan: LogicalPlan,
    input_bytes: usize,
}

/// A region fed by more input data than this executes with SPILLABLE
/// sort-merge joins from the start: its hash-join builds (the one operator
/// with no spill path) would exhaust the pool anyway, and predicting from
/// MEASURED input bytes avoids paying a doomed first attempt (planner row
/// estimates are absent on aggregate intermediates, so this runtime signal
/// is the reliable one - TPC-DS q78's join inputs all estimate None).
const REGION_SPILL_JOIN_INPUT_BYTES: usize = 8 * 1024 * 1024 * 1024;

/// Resident (in-memory) bindings may collectively hold this much before the
/// LARGEST ones spill to disk. A per-binding threshold taxed every big-fact
/// query with disk I/O the box never needed (q05 went 3.8s -> 15.4s at SF10
/// re-reading a spilled store_sales that would have fit in RAM); what
/// actually kills a run is the SUM of resident bindings, so the budget
/// bounds the sum. 16GiB keeps typical multi-fact pipelines resident while
/// a q64-scale pile still spills, and leaves the 32GiB operator pool and
/// the harness RSS watchdog their headroom.
const RESIDENT_BINDINGS_BUDGET: usize = 16 * 1024 * 1024 * 1024;

/// Total Arrow buffer bytes held by a materialized relation.
fn batches_bytes(b: &Batches) -> usize {
    let mut total = 0;
    for batch in &b.batches {
        total += batch.get_array_memory_size();
    }
    total
}

/// Write a relation to an Arrow IPC temp file via the DiskManager.
fn spill_binding(b: &Batches) -> Result<SpilledBinding, DataFusionError> {
    let file = runtime_env().disk_manager.create_tmp_file("fedq_binding")?;
    let handle = std::fs::File::create(file.path())
        .map_err(|e| DataFusionError::Execution(format!("binding spill create: {e}")))?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(handle, &b.schema)?;
    let mut rows = 0;
    for batch in &b.batches {
        writer.write(batch)?;
        rows += batch.num_rows();
    }
    writer.finish()?;
    Ok(SpilledBinding {
        schema: b.schema.clone(),
        file,
        rows,
        bytes: batches_bytes(b),
    })
}

/// Store a materialized relation as a binding, keeping the SUM of resident
/// bindings under the budget: existing bindings spill largest-first to make
/// room, and a relation bigger than the whole budget can only live on disk.
fn store_relation(
    bindings: &mut HashMap<String, Binding>,
    name: &str,
    batches: Batches,
) -> Result<(), DataFusionError> {
    let incoming = batches_bytes(&batches);
    spill_for_headroom(bindings, incoming)?;
    if incoming > RESIDENT_BINDINGS_BUDGET {
        let spilled = spill_binding(&batches)?;
        bindings.insert(name.to_string(), Binding::Spilled(spilled));
        return Ok(());
    }
    bindings.insert(name.to_string(), Binding::Materialized(batches));
    Ok(())
}

/// Spill the LARGEST resident bindings until `incoming` fits in the budget.
/// Spilled bindings stay fully readable (every consumer streams the file),
/// so evicting an already-stored relation is always safe.
fn spill_for_headroom(
    bindings: &mut HashMap<String, Binding>,
    incoming: usize,
) -> Result<(), DataFusionError> {
    let mut sized: Vec<(String, usize)> = Vec::new();
    for (name, binding) in bindings.iter() {
        if let Binding::Materialized(b) = binding {
            sized.push((name.clone(), batches_bytes(b)));
        }
    }
    let mut resident: usize = sized.iter().map(|(_, bytes)| *bytes).sum();
    sized.sort_by(|a, b| b.1.cmp(&a.1));
    for (name, bytes) in sized {
        if resident.saturating_add(incoming) <= RESIDENT_BINDINGS_BUDGET {
            break;
        }
        let taken = match bindings.remove(&name) {
            Some(Binding::Materialized(b)) => b,
            _ => unreachable!("sized only holds materialized bindings"),
        };
        let spilled = spill_binding(&taken)?;
        bindings.insert(name, Binding::Spilled(spilled));
        resident = resident.saturating_sub(bytes);
    }
    Ok(())
}

/// One re-openable stream over a spilled relation's IPC file.
#[derive(Debug)]
struct SpillStream {
    schema: SchemaRef,
    file: RefCountedTempFile,
}

impl PartitionStream for SpillStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let schema = self.schema.clone();
        let opened = std::fs::File::open(self.file.path())
            .map_err(|e| DataFusionError::Execution(format!("binding spill open: {e}")))
            .and_then(|handle| {
                arrow::ipc::reader::FileReader::try_new(handle, None).map_err(DataFusionError::from)
            });
        match opened {
            Ok(reader) => {
                let batches = reader.map(|item| item.map_err(DataFusionError::from));
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::iter(batches),
                ))
            }
            Err(error) => Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(vec![Err(error)]),
            )),
        }
    }
}

/// A streaming table provider over a spilled relation.
fn spilled_provider(spilled: &SpilledBinding) -> Result<Arc<StreamingTable>, DataFusionError> {
    let stream = SpillStream {
        schema: spilled.schema.clone(),
        file: spilled.file.clone(),
    };
    Ok(Arc::new(StreamingTable::try_new(
        spilled.schema.clone(),
        vec![Arc::new(stream)],
    )?))
}

fn df_to_exec(e: DataFusionError) -> ExecError {
    ExecError::runtime(format!("{e}"))
}

/// Whether per-step timing lines go to stderr (FEDQRS_PROFILE=1). Read once.
fn profile_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("FEDQRS_PROFILE").map_or(false, |v| v != "0" && !v.is_empty()))
}

/// Total rows currently held by a binding (either variant), for profiling.
fn binding_rows(bindings: &HashMap<String, Binding>, name: &str) -> usize {
    match bindings.get(name) {
        Some(Binding::Materialized(b)) | Some(Binding::Keys(b)) => {
            b.batches.iter().map(|x| x.num_rows()).sum()
        }
        // A lazy binding has no rows yet; the forcing step reports them.
        Some(Binding::Lazy(_)) => 0,
        Some(Binding::Spilled(spilled)) => spilled.rows,
        None => 0,
    }
}

/// The fragment's operator name, for profiling labels.
fn fragment_kind(fragment: &Fragment) -> &'static str {
    match fragment {
        Fragment::HashJoin { .. } => "hash_join",
        Fragment::NestedLoopJoin { .. } => "nested_loop_join",
        Fragment::Project { .. } => "project",
        Fragment::Aggregate { .. } => "aggregate",
        Fragment::Sort { .. } => "sort",
        Fragment::Filter { .. } => "filter",
        Fragment::Limit { .. } => "limit",
        Fragment::SingleRowGuard { .. } => "single_row_guard",
        Fragment::RawSql { .. } => "raw_sql",
    }
}

/// One stderr line describing a finished step: time, output rows, what ran.
fn log_step(step: &Step, ir: &Ir, bindings: &HashMap<String, Binding>, elapsed_ms: f64) {
    let line = match step {
        Step::SourceScan {
            datasource,
            scan,
            binding,
            ..
        } => {
            let what = scan
                .raw_sql
                .as_deref()
                .unwrap_or(scan.table.as_deref().unwrap_or("?"));
            let short: String = what.chars().take(100).collect();
            format!(
                "source_scan   ds={datasource:<6} rows={:<9} {short}",
                binding_rows(bindings, binding)
            )
        }
        Step::CollectDistinct { key, binding, .. } => {
            format!(
                "collect_dist  key={key:<20} rows={}",
                binding_rows(bindings, binding)
            )
        }
        Step::InjectedScan {
            datasource,
            scan,
            inject_column,
            binding,
            ..
        } => {
            let what = scan
                .raw_sql
                .as_deref()
                .unwrap_or(scan.table.as_deref().unwrap_or("?"));
            let short: String = what.chars().take(80).collect();
            format!(
                "injected_scan ds={datasource:<6} rows={:<9} col={inject_column} {short}",
                binding_rows(bindings, binding)
            )
        }
        Step::Merge {
            fragment, binding, ..
        } => {
            let kind = ir.fragments.get(fragment).map_or("?", fragment_kind);
            format!(
                "merge         {kind:<16} rows={}",
                binding_rows(bindings, binding)
            )
        }
        Step::Ship {
            datasource, table, ..
        } => {
            format!("ship          ds={datasource:<6} table={table}")
        }
        Step::Return { .. } => "return".to_string(),
    };
    eprintln!("[fedqrs] {elapsed_ms:9.2}ms  {line}");
}

/// Drops the query's shipped temp tables (by dropping their pinned
/// connections) on EVERY exit path, error or success.
struct ShipmentGuard;

impl Drop for ShipmentGuard {
    fn drop(&mut self) {
        connectors::clear_shipments();
    }
}

/// Interpret `ir` and return the result schema, batches, and per-step
/// observations. Pure Rust with no Python state; the later `fedq-py` crate
/// wraps this behind the Arrow C-stream FFI and runs it with the GIL released
/// (holding the GIL would freeze every Python thread for the query's duration).
pub fn execute(ir: &Ir) -> ExecResult<(SchemaRef, Vec<RecordBatch>, Vec<(String, usize)>)> {
    // Shipped temp tables are strictly per-query; drop any pinned
    // connections a previous query left (also cleared on every exit path
    // by the guard below - this is the defensive backstop).
    connectors::clear_shipments();
    let _shipments = ShipmentGuard;
    let runtime = Runtime::new().map_err(|e| ExecError::runtime(format!("tokio runtime: {e}")))?;
    let mut bindings: HashMap<String, Binding> = HashMap::new();
    let mut remaining = binding_use_counts(ir);
    // (binding, measured rows) for every materialized step, returned to Python
    // as the learned-stats WRITE stream: the engine reports one number per step
    // and the caller decides what it means.
    let mut observations: Vec<(String, usize)> = Vec::new();
    // Fresh group-count harvest for this query (aggregates run lazily; their
    // counts are read from AggregateExec metrics after each region collects).
    AGG_HARVEST.with(|h| h.borrow_mut().clear());
    // Independent plain scans start fetching NOW on the prefetch workers; the
    // loop below consumes each result when it reaches that step, so remote
    // reads overlap while bindings still land in step order.
    let mut prefetched = dispatch_prefetches(ir);

    for (index, step) in ir.steps.iter().enumerate() {
        let started = std::time::Instant::now();
        // A prefetched scan reports the WORKER's fetch time on its profile
        // line (the driving thread's recv wait would under-report overlap).
        let mut profile_ms: Option<f64> = None;
        match step {
            Step::SourceScan {
                datasource,
                scan,
                binding,
                materialize: _,
            } => {
                let batches = match prefetched.take(index) {
                    Some(rx) => {
                        let (batches, fetch_ms) = recv_prefetch(rx)?;
                        profile_ms = Some(fetch_ms);
                        batches
                    }
                    None => fetch_source(datasource, scan)?,
                };
                store_relation(&mut bindings, binding, batches).map_err(df_to_exec)?;
            }

            Step::CollectDistinct {
                input,
                key,
                cap: _,
                binding,
            } => {
                runtime
                    .block_on(force_materialized(&mut bindings, input))
                    .map_err(df_to_exec)?;
                let keys = runtime
                    .block_on(collect_distinct_from(&bindings, input, key))
                    .map_err(df_to_exec)?;
                note_use(&mut remaining, input);
                bindings.insert(binding.clone(), Binding::Keys(keys));
                // Keys just landed: every later injection whose key sets are
                // now all bound starts fetching on the workers immediately,
                // overlapping whatever the loop does next.
                dispatch_ready_injections(ir, index, &bindings, &mut prefetched);
            }

            Step::InjectedScan {
                datasource,
                scan,
                inject_column,
                keys_from,
                binding,
                inject_column_ndv,
                extra_injections,
            } => {
                let batches = match prefetched.take(index) {
                    Some(rx) => {
                        let (batches, fetch_ms) = recv_prefetch(rx)?;
                        profile_ms = Some(fetch_ms);
                        batches
                    }
                    None => {
                        let keys = keys_binding(&bindings, keys_from)?;
                        let mut extras: Vec<(&str, &Batches)> = Vec::new();
                        for extra in extra_injections {
                            extras.push((
                                extra.column.as_str(),
                                keys_binding(&bindings, &extra.keys_from)?,
                            ));
                        }
                        run_injected_scan(
                            datasource,
                            scan,
                            inject_column,
                            keys,
                            *inject_column_ndv,
                            &extras,
                        )?
                    }
                };
                note_use(&mut remaining, keys_from);
                for extra in extra_injections {
                    note_use(&mut remaining, &extra.keys_from);
                }
                store_relation(&mut bindings, binding, batches).map_err(df_to_exec)?;
            }

            Step::Ship {
                datasource,
                input,
                table,
            } => {
                runtime
                    .block_on(force_materialized(&mut bindings, input))
                    .map_err(df_to_exec)?;
                let b = materialized(&bindings, input)?;
                connectors::ship_table(datasource, table, b.schema.clone(), b.batches.clone())?;
                note_use(&mut remaining, input);
            }

            Step::Merge {
                fragment,
                inputs,
                binding,
            } => {
                let frag = ir
                    .fragments
                    .get(fragment)
                    .ok_or_else(|| ExecError::runtime(format!("unknown fragment '{fragment}'")))?;
                let result = runtime
                    .block_on(run_fragment(&mut bindings, &mut remaining, frag, inputs))
                    .map_err(df_to_exec)?;
                match result {
                    // An eagerly materialized fragment result (the guard, a
                    // duplicate-name projection) spills past the threshold
                    // like any other relation.
                    Binding::Materialized(batches) => {
                        store_relation(&mut bindings, binding, batches).map_err(df_to_exec)?
                    }
                    other => {
                        bindings.insert(binding.clone(), other);
                    }
                }
            }

            Step::Return { input } => {
                runtime
                    .block_on(force_materialized(&mut bindings, input))
                    .map_err(df_to_exec)?;
                let (schema, batches) = export(&mut bindings, input)?;
                append_group_observations(ir, &mut observations);
                return Ok((schema, batches, observations));
            }
        }
        if let Some(name) = observable_binding(step) {
            observations.push((name.clone(), binding_rows(&bindings, name)));
        }
        if profile_enabled() {
            let elapsed = profile_ms.unwrap_or(started.elapsed().as_secs_f64() * 1000.0);
            log_step(step, ir, &bindings, elapsed);
        }
    }

    Err(ExecError::runtime("IR had no `return` step"))
}

/// The output binding of a step whose result is MATERIALIZED and row-counted
/// (a source read, a reduced scan, a distinct-key collection), or None for a
/// lazy merge fragment (fusion streams its rows past the boundary uncounted -
/// those become observations in v1.5 via DataFusion's output-row metrics).
fn observable_binding(step: &Step) -> Option<&String> {
    match step {
        Step::SourceScan { binding, .. } => Some(binding),
        Step::InjectedScan { binding, .. } => Some(binding),
        Step::CollectDistinct { binding, .. } => Some(binding),
        _ => None,
    }
}

/// How many times each binding is read across the whole plan (merge inputs,
/// distinct-key collection, injected-scan keys, the final return). A shared
/// binding - a multi-referenced CTE's materialized body - is CLONED
/// (Arc-shallow) by every consumer except its last, which takes it, so
/// memory is released exactly where single-use behavior released it.
fn binding_use_counts(ir: &Ir) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut bump = |name: &String| *counts.entry(name.clone()).or_insert(0) += 1;
    for step in &ir.steps {
        match step {
            Step::SourceScan { .. } => {}
            Step::Ship { input, .. } => bump(input),
            Step::CollectDistinct { input, .. } => bump(input),
            Step::InjectedScan {
                keys_from,
                extra_injections,
                ..
            } => {
                bump(keys_from);
                for extra in extra_injections {
                    bump(&extra.keys_from);
                }
            }
            Step::Merge { inputs, .. } => {
                for name in inputs.values() {
                    bump(name);
                }
            }
            Step::Return { input } => bump(input),
        }
    }
    counts
}

/// Record one non-consuming read of a binding, so a later consumer knows
/// whether it is the last reader (and may take instead of clone).
fn note_use(remaining: &mut HashMap<String, usize>, name: &str) {
    if let Some(uses) = remaining.get_mut(name) {
        *uses = uses.saturating_sub(1);
    }
}

/// A plain source read. A spec the planner marked `parallel` goes through the
/// ctid-partitioned parallel Postgres path (each partition on its own pooled
/// connection; NOTE: separate connections read separate snapshots, the same
/// trade-off the parallel probe scan already makes - a shared exported
/// snapshot is the follow-up for concurrent-write sources). Everything else
/// is a single-stream fetch.
fn fetch_source(datasource: &str, scan: &ScanSpec) -> ExecResult<Batches> {
    if !scan.parallel {
        return fetch_scan(datasource, scan, None);
    }
    require_parallelizable(datasource, scan)?;
    parallel_probe_scan(datasource, scan)
}

/// A `parallel` spec must be a plain structured Postgres table read; anything
/// else is a planner contract violation and refuses loudly rather than
/// guessing (a per-partition DISTINCT or LIMIT would return wrong rows).
fn require_parallelizable(datasource: &str, scan: &ScanSpec) -> ExecResult<()> {
    let postgres = connectors::kind(datasource)? == DsKind::Postgres;
    if postgres && scan.table.is_some() && !scan.distinct && scan.limit.is_none() {
        return Ok(());
    }
    Err(ExecError::runtime(
        "parallel scan spec is not a plain Postgres table read",
    ))
}

/// Render a scan to SQL and fetch it natively.
fn fetch_scan(
    datasource: &str,
    scan: &ScanSpec,
    extra_filter: Option<Expr>,
) -> ExecResult<Batches> {
    let kind = connectors::kind(datasource)?;
    let sql = scan_sql(kind, scan, extra_filter).map_err(df_to_exec)?;
    let (schema, batches) = connectors::fetch(datasource, &sql)?;
    Ok(Batches { schema, batches })
}

// ---------- cross-step parallel reads (plain source scans) --------------------

/// Workers serving whole-step scan prefetches. Distinct from the ctid
/// PARTITION pool (`connectors::fetch_parallel`, which splits ONE big scan):
/// each of these runs one COMPLETE plain scan, through the same
/// `connectors::fetch` the sequential path uses - which is thread-correct by
/// construction (per-THREAD PostgreSQL connection caches, a mutex-guarded
/// DuckDB base cloned per fetch, shared-safe Parquet contexts).
const STEP_WORKERS: usize = 6;

/// A prefetch's reply: the fetched batches plus the worker-measured fetch
/// time (the profile line reports the fetch cost, not the driving thread's
/// wait); errors cross the thread boundary as text.
type PrefetchReply = Result<(Batches, f64), String>;

/// The remote read a prefetch worker runs. Everything is OWNED (specs cloned,
/// keys Arc-shallow-cloned) so the job carries no borrow of the IR and the
/// pool can outlive every query.
enum PrefetchWork {
    /// A plain scan, rendered to SQL on the driving thread (rendering is pure).
    PlainScan { datasource: String, sql: String },
    /// A key-reduced scan. The worker runs the FULL delivery-strategy machinery
    /// (inline IN / temp table / unselective fallback); every path is worker-
    /// safe: temp tables are per-CONNECTION (each worker owns its own), the
    /// ctid fallback fans to a DIFFERENT pool (one-way, no cycle).
    InjectedScan {
        datasource: String,
        scan: ScanSpec,
        inject_column: String,
        keys: Batches,
        inject_column_ndv: Option<u64>,
        extras: Vec<(String, Batches)>,
    },
}

/// One prefetch job: the work plus its reply channel.
struct PrefetchJob {
    work: PrefetchWork,
    reply: std::sync::mpsc::Sender<PrefetchReply>,
}

/// The fixed pool of long-lived prefetch workers. Persisting the threads
/// pools their per-thread source connections across queries, so repeated
/// prefetches pay no reconnect cost (the same rationale as the ctid pool).
fn step_pool() -> &'static Vec<std::sync::mpsc::Sender<PrefetchJob>> {
    static POOL: OnceLock<Vec<std::sync::mpsc::Sender<PrefetchJob>>> = OnceLock::new();
    POOL.get_or_init(|| {
        let mut senders = Vec::new();
        for _ in 0..STEP_WORKERS {
            let (tx, rx) = std::sync::mpsc::channel::<PrefetchJob>();
            std::thread::spawn(move || prefetch_worker_loop(rx));
            senders.push(tx);
        }
        senders
    })
}

/// One prefetch worker: serve remote reads until process exit. A send
/// failure means the query abandoned its receiver (an error unwound the step
/// loop) - the fetch is discarded and the worker moves on.
fn prefetch_worker_loop(rx: std::sync::mpsc::Receiver<PrefetchJob>) {
    while let Ok(job) = rx.recv() {
        let started = std::time::Instant::now();
        let result = run_prefetch_work(&job.work)
            .map(|batches| (batches, started.elapsed().as_secs_f64() * 1000.0))
            .map_err(stringify_error);
        let _ = job.reply.send(result);
    }
}

/// Execute one prefetch work item on the worker's own connections.
fn run_prefetch_work(work: &PrefetchWork) -> ExecResult<Batches> {
    match work {
        PrefetchWork::PlainScan { datasource, sql } => {
            let (schema, batches) = connectors::fetch(datasource, sql)?;
            Ok(Batches { schema, batches })
        }
        PrefetchWork::InjectedScan {
            datasource,
            scan,
            inject_column,
            keys,
            inject_column_ndv,
            extras,
        } => {
            let mut extra_refs: Vec<(&str, &Batches)> = Vec::new();
            for (column, batches) in extras {
                extra_refs.push((column.as_str(), batches));
            }
            run_injected_scan(
                datasource,
                scan,
                inject_column,
                keys,
                *inject_column_ndv,
                &extra_refs,
            )
        }
    }
}

/// Render a worker-side error to text so it can cross the thread boundary
/// (worker replies are `Result<_, String>`); the driving thread rebuilds an
/// `ExecError` from the text.
fn stringify_error(e: ExecError) -> String {
    e.to_string()
}

/// Whether cross-step parallel reads are on. FEDQRS_PARALLEL_STEPS=0 is the
/// kill switch restoring the fully sequential step loop.
fn parallel_steps_enabled() -> bool {
    std::env::var("FEDQRS_PARALLEL_STEPS")
        .map(|v| v != "0")
        .unwrap_or(true)
}

/// The datasources any Ship step targets. A scan on such a datasource may
/// reference a shipped TEMP table that exists only on the driving thread's
/// PINNED connection, so it must never run on a prefetch worker (whose own
/// connection cannot see it). Conservative by datasource, not by table: the
/// IR does not yet carry per-scan ship-visibility edges.
fn ship_target_datasources(ir: &Ir) -> HashSet<String> {
    let mut targets = HashSet::new();
    for step in &ir.steps {
        if let Step::Ship { datasource, .. } = step {
            targets.insert(datasource.clone());
        }
    }
    targets
}

/// The (datasource, rendered SQL) of a scan this pass may prefetch: a plain
/// non-ctid-parallel SourceScan on a datasource no ship targets. A ctid-
/// parallel scan stays on the driving thread - it already fans out to the
/// partition pool, and running one pool inside another invites starvation
/// deadlocks. A datasource/render error returns None so the sequential arm
/// raises the IDENTICAL error in step order (deferred, never swallowed).
fn prefetch_sql(step: &Step, ship_targets: &HashSet<String>) -> Option<(String, String)> {
    let Step::SourceScan {
        datasource, scan, ..
    } = step
    else {
        return None;
    };
    if scan.parallel || ship_targets.contains(datasource) {
        return None;
    }
    let kind = connectors::kind(datasource).ok()?;
    let sql = scan_sql(kind, scan, None).ok()?;
    Some((datasource.clone(), sql))
}

/// Round-robin prefetch dispatch state: the per-step receivers the loop
/// consumes, plus the rotating pool cursor.
struct Prefetches {
    receivers: HashMap<usize, std::sync::mpsc::Receiver<PrefetchReply>>,
    next_worker: usize,
    ship_targets: HashSet<String>,
    enabled: bool,
}

impl Prefetches {
    /// Send one work item to the pool, keyed by its step index. A send
    /// failure (a worker died to a panic) simply leaves the step to the
    /// sequential arm.
    fn dispatch(&mut self, index: usize, work: PrefetchWork) {
        let pool = step_pool();
        let (tx, rx) = std::sync::mpsc::channel();
        let job = PrefetchJob { work, reply: tx };
        if pool[self.next_worker % pool.len()].send(job).is_ok() {
            self.receivers.insert(index, rx);
        }
        self.next_worker += 1;
    }

    /// Take the receiver for a step the loop has reached, if it was dispatched.
    fn take(&mut self, index: usize) -> Option<std::sync::mpsc::Receiver<PrefetchReply>> {
        self.receivers.remove(&index)
    }
}

/// Dispatch every DEPENDENCY-FREE prefetchable scan (plain source scans)
/// before the loop starts. The loop consumes each receiver when it reaches
/// that step, so results land in `bindings` in step order regardless of
/// completion order - nothing downstream can observe the concurrency.
fn dispatch_prefetches(ir: &Ir) -> Prefetches {
    let mut prefetches = Prefetches {
        receivers: HashMap::new(),
        next_worker: 0,
        ship_targets: ship_target_datasources(ir),
        enabled: parallel_steps_enabled(),
    };
    if !prefetches.enabled {
        return prefetches;
    }
    for (index, step) in ir.steps.iter().enumerate() {
        if let Some((datasource, sql)) = prefetch_sql(step, &prefetches.ship_targets) {
            prefetches.dispatch(index, PrefetchWork::PlainScan { datasource, sql });
        }
    }
    prefetches
}

/// Dispatch every LATER InjectedScan whose keys (primary + every extra) are
/// all bound - called after each CollectDistinct lands, the only step that
/// produces keys. The keys are Arc-shallow CLONED into the job, so the
/// driving thread's binding lifecycle (use counts, takes) stays untouched.
fn dispatch_ready_injections(
    ir: &Ir,
    after: usize,
    bindings: &HashMap<String, Binding>,
    prefetches: &mut Prefetches,
) {
    if !prefetches.enabled {
        return;
    }
    for (index, step) in ir.steps.iter().enumerate().skip(after + 1) {
        if prefetches.receivers.contains_key(&index) {
            continue;
        }
        if let Some(work) = injection_work(step, bindings, &prefetches.ship_targets) {
            prefetches.dispatch(index, work);
        }
    }
}

/// The owned work item for an InjectedScan whose every key binding is ready,
/// or None (not an injection, a ship-target datasource, or keys still
/// pending - a later CollectDistinct will retry it).
fn injection_work(
    step: &Step,
    bindings: &HashMap<String, Binding>,
    ship_targets: &HashSet<String>,
) -> Option<PrefetchWork> {
    let Step::InjectedScan {
        datasource,
        scan,
        inject_column,
        keys_from,
        inject_column_ndv,
        extra_injections,
        ..
    } = step
    else {
        return None;
    };
    if ship_targets.contains(datasource) {
        return None;
    }
    // Only NETWORK-bound injections overlap profitably. A DuckDB injected
    // scan is in-process and already saturates every core (measured at SF10:
    // concurrent duck semi-joins gained nothing and paid contention - q46
    // +82ms, q78 +60ms - while pg injections overlapped cleanly).
    if connectors::kind(datasource).ok()? != DsKind::Postgres {
        return None;
    }
    let keys = ready_keys(bindings, keys_from)?;
    let mut extras = Vec::new();
    for extra in extra_injections {
        extras.push((
            extra.column.clone(),
            ready_keys(bindings, &extra.keys_from)?,
        ));
    }
    Some(PrefetchWork::InjectedScan {
        datasource: datasource.clone(),
        scan: scan.clone(),
        inject_column: inject_column.clone(),
        keys,
        inject_column_ndv: *inject_column_ndv,
        extras,
    })
}

/// An Arc-shallow clone of a bound key set, or None while it is not (yet) a
/// Keys binding - a WRONG-typed binding also returns None so the sequential
/// arm raises the identical loud error at its step.
fn ready_keys(bindings: &HashMap<String, Binding>, name: &str) -> Option<Batches> {
    match bindings.get(name) {
        Some(Binding::Keys(batches)) => Some(batches.clone()),
        _ => None,
    }
}

/// Take a prefetched scan's result, converting a worker-side failure back
/// into the loud error the sequential path would have raised.
fn recv_prefetch(rx: std::sync::mpsc::Receiver<PrefetchReply>) -> ExecResult<(Batches, f64)> {
    match rx.recv() {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(message)) => Err(ExecError::runtime(message)),
        Err(_) => Err(ExecError::runtime(
            "prefetch worker died before replying (worker thread panic)",
        )),
    }
}

/// Borrow a binding as materialized batches.
fn materialized<'a>(bindings: &'a HashMap<String, Binding>, name: &str) -> ExecResult<&'a Batches> {
    match bindings.get(name) {
        Some(Binding::Materialized(b)) => Ok(b),
        Some(_) => Err(ExecError::runtime(format!(
            "binding '{name}' is not a relation"
        ))),
        None => Err(ExecError::runtime(format!("unknown binding '{name}'"))),
    }
}

/// Borrow the distinct build-key values a keys binding holds.
fn keys_binding<'a>(
    bindings: &'a HashMap<String, Binding>,
    keys_from: &str,
) -> ExecResult<&'a Batches> {
    match bindings.get(keys_from) {
        Some(Binding::Keys(b)) => Ok(b),
        _ => Err(ExecError::runtime(format!(
            "binding '{keys_from}' does not hold distinct keys"
        ))),
    }
}

/// Read the probe, reducing it by the build keys with the cheapest strategy for
/// the key cardinality and estimated selectivity:
///   - no keys  -> the probe matches nothing;
///   - < IN_CAP -> inline `col IN (v1, ..)` and fetch;
///   - selective -> push a TEMP TABLE of the keys and semi-join server-side;
///   - unselective (would fetch > FULL_SCAN_FRACTION) -> parallel full scan.
fn run_injected_scan(
    datasource: &str,
    scan: &ScanSpec,
    inject_column: &str,
    keys: &Batches,
    inject_column_ndv: Option<u64>,
    extras: &[(&str, &Batches)],
) -> ExecResult<Batches> {
    let num_keys: usize = keys.batches.iter().map(|b| b.num_rows()).sum();
    // Additional dimensions' keys ride along as bounded IN lists wherever a
    // filter slot exists (an over-cap extra is skipped - extras are a
    // bonus; the primary injection carries the reduction contract).
    let extra_filter = extras_in_filter(extras)?;

    if num_keys == 0 {
        return fetch_scan(datasource, scan, Some(lit(false)));
    }
    // A small key set delivers as an inline `col IN (v1, ..)` - a bounded
    // literal filter the source plans normally. (A raw-SQL / island probe with
    // injected_sql skips this: its filter is already baked into the SQL.)
    if num_keys < IN_CAP && scan.injected_sql.is_none() {
        let filter = in_list_filter(keys, inject_column)?;
        return fetch_scan(datasource, scan, Some(and_option(filter, extra_filter)));
    }
    let kind = connectors::kind(datasource)?;
    // Parquet and materialized-view reads are in-process (DataFusion); the
    // downstream join reduces without any transfer, so shipping keys anywhere
    // would be pointless.
    if matches!(kind, DsKind::Parquet | DsKind::Materialized) {
        return fetch_scan(datasource, scan, extra_filter);
    }
    // DuckDB: beyond the ingest ceiling, or a key type its temp-table ingest
    // cannot represent, the probe reads whole (correct, just unreduced).
    // Postgres ingests via ADBC and is bounded by the selectivity guard below.
    if kind == DsKind::DuckDb
        && (num_keys > DUCK_TEMP_CAP || !connectors::duck_can_ingest(&keys.schema))
    {
        return fetch_scan(datasource, scan, extra_filter);
    }
    // Postgres evaluates a key semi-join with an index scan ONLY when the probe
    // column is indexed; without an index it degrades to a full sequential scan
    // even at low cardinality (a worse disaster than shipping the rows), so an
    // unindexed Postgres probe reads whole and the coordinator join reduces.
    // DuckDB is columnar - its scanner ignores indexes, a semi-join is a full
    // vectorized scan regardless - so it is never gated on an index.
    if kind == DsKind::Postgres
        && scan.table.is_some()
        && !connectors::column_has_index(
            datasource,
            scan.schema.as_deref(),
            scan.table.as_deref().unwrap(),
            inject_column,
        )?
    {
        return unselective_probe_scan(kind, datasource, scan);
    }
    // A raw-SQL probe (a pushed remote subtree) has no catalog identity for
    // the selectivity guard; prefer the safe temp-table path directly.
    if scan.table.is_some()
        && fetches_most_of_table(datasource, scan, inject_column, num_keys, inject_column_ndv)?
    {
        return unselective_probe_scan(kind, datasource, scan);
    }
    // The temp-table and parallel paths render their own SQL; extras do not
    // thread through them yet - the primary reduction still applies, and the
    // downstream join keeps exactness.
    temp_table_probe(datasource, scan, keys, inject_column)
}

/// AND of every extra key set as an IN list, skipping over-cap sets.
fn extras_in_filter(extras: &[(&str, &Batches)]) -> ExecResult<Option<Expr>> {
    let mut combined: Option<Expr> = None;
    for (column, keys) in extras {
        let count: usize = keys.batches.iter().map(|b| b.num_rows()).sum();
        if count == 0 || count >= IN_CAP {
            continue;
        }
        let filter = in_list_filter(keys, column)?;
        combined = Some(and_option(filter, combined));
    }
    Ok(combined)
}

/// `left AND right?` - the left filter, tightened by an optional second.
fn and_option(left: Expr, right: Option<Expr>) -> Expr {
    match right {
        Some(other) => left.and(other),
        None => left,
    }
}

/// The probe read when the dynamic filter would select most of the table:
/// Postgres gets the ctid-partitioned parallel scan; DuckDB (columnar, no
/// secondary indexes - a semi-join costs a full scan anyway) just reads whole.
fn unselective_probe_scan(kind: DsKind, datasource: &str, scan: &ScanSpec) -> ExecResult<Batches> {
    if kind == DsKind::Postgres {
        return parallel_probe_scan(datasource, scan);
    }
    fetch_scan(datasource, scan, None)
}

/// `inject_column IN (v1, v2, ...)` from the collected key values.
fn in_list_filter(keys: &Batches, inject_column: &str) -> ExecResult<Expr> {
    let mut values = Vec::new();
    for batch in &keys.batches {
        let column = batch.column(0);
        for i in 0..batch.num_rows() {
            values.push(literal_from_array(column.as_ref(), i).map_err(df_to_exec)?);
        }
    }
    let column = Expr::Column(Column::new(
        None::<TableReference>,
        inject_column.to_string(),
    ));
    Ok(Expr::InList(InList::new(Box::new(column), values, false)))
}

/// True when the dynamic filter is estimated to select more of the probe
/// than the source's full-scan fraction (the point where the full read -
/// ctid-parallel on Postgres, single-stream on DuckDB - beats a semi-join).
/// Unknown statistics => false (prefer the safe temp-table path).
fn fetches_most_of_table(
    datasource: &str,
    scan: &ScanSpec,
    inject_column: &str,
    num_keys: usize,
    inject_column_ndv: Option<u64>,
) -> ExecResult<bool> {
    let table = scan
        .table
        .as_deref()
        .ok_or_else(|| ExecError::runtime("injected scan needs a structured table"))?;
    let threshold = match connectors::kind(datasource)? {
        DsKind::Postgres => PG_FULL_SCAN_FRACTION,
        _ => FULL_SCAN_FRACTION,
    };
    // The planner's probe-column NDV gives the real fraction (keys/NDV);
    // the source-side estimate is the fallback for unstatted columns.
    if let Some(ndv) = inject_column_ndv {
        if ndv > 0 {
            return Ok(num_keys as f64 / ndv as f64 > threshold);
        }
    }
    let fraction = connectors::estimate_selectivity(
        datasource,
        scan.schema.as_deref(),
        table,
        inject_column,
        num_keys,
    )?;
    Ok(fraction.map(|f| f > threshold).unwrap_or(false))
}

/// Parallel ctid-partitioned full scan of the probe (base predicate only); the
/// downstream DataFusion join does the reduction.
fn parallel_probe_scan(datasource: &str, scan: &ScanSpec) -> ExecResult<Batches> {
    let kind = connectors::kind(datasource)?;
    let table = scan
        .table
        .as_deref()
        .ok_or_else(|| ExecError::runtime("parallel scan needs a structured table"))?;
    let select = select_list_sql(scan);
    let where_clause = base_filter_sql(kind, scan).map_err(df_to_exec)?;
    let (schema, batches) = connectors::fetch_parallel(
        datasource,
        scan.schema.as_deref(),
        table,
        scan.alias.as_deref(),
        &select,
        PARALLEL_PARTITIONS,
        where_clause.as_deref(),
    )?;
    Ok(Batches { schema, batches })
}

/// Temp-table pushdown: ingest the keys and semi-join them server-side.
fn temp_table_probe(
    datasource: &str,
    scan: &ScanSpec,
    keys: &Batches,
    inject_column: &str,
) -> ExecResult<Batches> {
    let kind = connectors::kind(datasource)?;
    let key_col = keys.schema.field(0).name();
    // A planner-prerendered island with the key filter already placed on its
    // owning base relation beats the generic derived-table wrapper: sources
    // do not push a semi-join through the wrapper (q03 measured 3.1x). The
    // SQL references the same temp table this path is about to fill.
    let sql = match &scan.injected_sql {
        Some(prerendered) => prerendered.clone(),
        None => temp_join_sql(kind, scan, DYN_KEYS_TEMP_TABLE, key_col, inject_column)
            .map_err(df_to_exec)?,
    };
    let (schema, batches) = connectors::fetch_temp_join(
        datasource,
        DYN_KEYS_TEMP_TABLE,
        keys.schema.clone(),
        keys.batches.clone(),
        &sql,
    )?;
    Ok(Batches { schema, batches })
}

/// DISTINCT of one key column, NULL-free (the full set; the strategy chosen
/// in `run_injected_scan` decides how to apply it). Reads a materialized
/// relation from memory and a spilled one as a stream from its IPC file.
async fn collect_distinct_from(
    bindings: &HashMap<String, Binding>,
    name: &str,
    key: &str,
) -> Result<Batches, DataFusionError> {
    let ctx = memory_capped_context();
    match bindings.get(name) {
        Some(Binding::Materialized(src)) => {
            let table = MemTable::try_new(src.schema.clone(), vec![src.batches.clone()])?;
            ctx.register_table("build", Arc::new(table))?;
        }
        Some(Binding::Spilled(spilled)) => {
            ctx.register_table("build", spilled_provider(spilled)?)?;
        }
        Some(_) => {
            return Err(DataFusionError::Plan(format!(
                "binding '{name}' is not a relation (key collection needs one)"
            )))
        }
        None => return Err(DataFusionError::Plan(format!("unknown binding '{name}'"))),
    }

    let df = ctx
        .table("build")
        .await?
        .filter(col(key).is_not_null())?
        .select(vec![col(key)])?
        .distinct()?;

    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches = collect_tracked(df).await?;
    Ok(Batches { schema, batches })
}

/// Register the fragment's inputs and COMPOSE it on DataFusion.
///
/// The fragment is planned, not executed: the result is a Lazy binding so a
/// chain of single-use fragments fuses into one streaming execution at the
/// next pipeline breaker. The two exceptions materialize here: the
/// cardinality guard (its probe must see every input row) and a projection
/// with duplicate output names (see projected_binding).
async fn run_fragment(
    bindings: &mut HashMap<String, Binding>,
    remaining: &mut HashMap<String, usize>,
    fragment: &Fragment,
    inputs: &BTreeMap<String, String>,
) -> Result<Binding, DataFusionError> {
    let ctx = memory_capped_context();
    let mut input_bytes = 0usize;
    for (table_name, binding_name) in inputs {
        input_bytes = input_bytes.saturating_add(
            register_input(&ctx, bindings, remaining, table_name, binding_name).await?,
        );
    }

    match fragment {
        Fragment::HashJoin {
            join_type,
            left_keys,
            right_keys,
            project,
        } => {
            let joined = join_frame(&ctx, *join_type, left_keys, right_keys).await?;
            projected_binding(joined, project, false, input_bytes).await
        }
        Fragment::NestedLoopJoin {
            join_type,
            condition,
            project,
        } => {
            let joined = nested_loop_frame(&ctx, *join_type, condition).await?;
            projected_binding(joined, project, false, input_bytes).await
        }
        Fragment::Project { project, distinct } => {
            let frame = ctx.table("in_0").await?;
            projected_binding(frame, project, *distinct, input_bytes).await
        }
        Fragment::Aggregate {
            select,
            group_by,
            grouping_sets,
        } => {
            let frame = run_aggregate(&ctx, select, group_by, grouping_sets).await?;
            Ok(lazy(frame, input_bytes))
        }
        Fragment::Sort { keys } => Ok(lazy(run_sort(&ctx, keys).await?, input_bytes)),
        Fragment::Filter { predicate } => Ok(lazy(run_filter(&ctx, predicate).await?, input_bytes)),
        Fragment::Limit { limit, offset } => {
            Ok(lazy(run_limit(&ctx, *limit, *offset).await?, input_bytes))
        }
        Fragment::SingleRowGuard { keys } => {
            // The guard EXECUTES its cardinality probe over the whole input,
            // so its passthrough result is materialized, never lazy.
            Ok(Binding::Materialized(
                run_single_row_guard(&ctx, keys).await?,
            ))
        }
        Fragment::RawSql { sql } => Ok(lazy(run_raw_sql(&ctx, sql).await?, input_bytes)),
    }
}

/// Wrap a composed DataFrame as a lazy binding carrying the MEASURED total
/// bytes of the relations feeding its region.
fn lazy(frame: datafusion::dataframe::DataFrame, input_bytes: usize) -> Binding {
    Binding::Lazy(LazyRegion {
        plan: frame.into_unoptimized_plan(),
        input_bytes,
    })
}

/// Register one merge input on the fragment's scratch context.
///
/// A single-use LAZY binding registers as a VIEW over its composed plan, so
/// the region keeps streaming. Anything else materializes first - a
/// multi-consumer must not re-execute its plan once per reader (that would
/// be the CTE re-emission disease again) - and registers as a MemTable.
async fn register_input(
    ctx: &SessionContext,
    bindings: &mut HashMap<String, Binding>,
    remaining: &mut HashMap<String, usize>,
    table_name: &str,
    binding_name: &str,
) -> Result<usize, DataFusionError> {
    let uses = remaining.get(binding_name).copied().unwrap_or(0);
    let single_use_lazy = uses <= 1 && matches!(bindings.get(binding_name), Some(Binding::Lazy(_)));
    if single_use_lazy {
        note_use(remaining, binding_name);
        let region = take_lazy(bindings, binding_name)?;
        let input_bytes = region.input_bytes;
        ctx.register_table(table_name, Arc::new(ViewTable::new(region.plan, None)))?;
        return Ok(input_bytes);
    }
    if let Some(Binding::Spilled(spilled)) = bindings.get(binding_name) {
        // A spilled relation streams back from its IPC file; the file is
        // re-readable, so every consumer registers its own stream and the
        // last one drops the map entry (the provider keeps the temp file
        // alive until its execution finishes).
        let provider = spilled_provider(spilled)?;
        let input_bytes = spilled.bytes;
        ctx.register_table(table_name, provider)?;
        note_use(remaining, binding_name);
        if uses <= 1 {
            bindings.remove(binding_name);
        }
        return Ok(input_bytes);
    }
    force_materialized(bindings, binding_name).await?;
    if matches!(bindings.get(binding_name), Some(Binding::Spilled(_))) {
        // Forcing a big lazy region can itself spill; register the stream
        // through the same arm rather than pulling the file back into RAM.
        return Box::pin(register_input(
            ctx,
            bindings,
            remaining,
            table_name,
            binding_name,
        ))
        .await;
    }
    let b = consume_materialized(bindings, remaining, binding_name)?;
    let input_bytes = batches_bytes(&b);
    let table = MemTable::try_new(b.schema.clone(), vec![b.batches.clone()])
        .map_err(|e| binding_schema_error(binding_name, &b, e))?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(input_bytes)
}

/// Execute a lazy binding's composed plan and replace it with materialized
/// batches. A fused region runs HERE, as one streaming DataFusion execution
/// whose operators spill through the shared runtime while collect_tracked
/// accounts the accumulated result. A no-op for non-lazy bindings.
///
/// When the first attempt exhausts the memory pool inside an OPERATOR (a
/// hash-join build - the one operator with no spill path), the region
/// retries once with sort-merge joins, whose buffered side and sorts both
/// spill: slower, but it completes instead of erroring (TPC-DS q78 at SF10).
/// An exhaustion in the RESULT accumulation itself (fedq_collect) is not
/// retried - the accumulated output would be just as large either way.
async fn force_materialized(
    bindings: &mut HashMap<String, Binding>,
    name: &str,
) -> Result<(), DataFusionError> {
    if !matches!(bindings.get(name), Some(Binding::Lazy(_))) {
        return Ok(());
    }
    let region = take_lazy(bindings, name)?;
    if region.input_bytes > REGION_SPILL_JOIN_INPUT_BYTES {
        // The measured input volume says the hash builds cannot fit; go
        // straight to spillable sort-merge joins instead of paying a
        // doomed first attempt.
        let batches = execute_region(region.plan, true).await?;
        return store_relation(bindings, name, batches);
    }
    let batches = match execute_region(region.plan.clone(), false).await {
        Err(error) if retry_with_spillable_joins(&error) => {
            execute_region(region.plan, true).await?
        }
        other => other?,
    };
    store_relation(bindings, name, batches)
}

/// Run one composed region to completion. `spillable_joins` plans equi-joins
/// as sort-merge (spilling) instead of hash (non-spilling).
async fn execute_region(
    plan: LogicalPlan,
    spillable_joins: bool,
) -> Result<Batches, DataFusionError> {
    let mut config = SessionConfig::new();
    if spillable_joins {
        config = config.set_bool("datafusion.optimizer.prefer_hash_join", false);
    }
    let ctx = SessionContext::new_with_config_rt(config, runtime_env());
    let frame = ctx.execute_logical_plan(plan).await?;
    collect_batches(frame).await
}

/// Whether a region failure warrants the sort-merge retry: the pool was
/// exhausted by an operator, not by our own result accumulation (whose
/// reservation is named fedq_collect - re-running would accumulate the same
/// rows and fail the same way). The exhaustion arrives WRAPPED (streams
/// share errors via Context/Shared), so match on the root of the chain.
fn retry_with_spillable_joins(error: &DataFusionError) -> bool {
    match error.find_root() {
        DataFusionError::ResourcesExhausted(message) => !message.contains("fedq_collect"),
        _ => false,
    }
}

/// Take a binding out of the map as a lazy plan.
fn take_lazy(
    bindings: &mut HashMap<String, Binding>,
    name: &str,
) -> Result<LazyRegion, DataFusionError> {
    match bindings.remove(name) {
        Some(Binding::Lazy(region)) => Ok(region),
        Some(other) => {
            bindings.insert(name.to_string(), other);
            Err(DataFusionError::Plan(format!(
                "binding '{name}' is not lazy"
            )))
        }
        None => Err(DataFusionError::Plan(format!("unknown binding '{name}'"))),
    }
}

/// The scalar-subquery cardinality guard: error when `in_0` holds more than
/// one row (per distinct key tuple when keys are given, the decorrelated
/// per-outer-row rule), otherwise pass the input through unchanged. The
/// violation mirrors real engines' scalar-subquery error - returning the rows
/// would let the join above silently duplicate outer rows.
async fn run_single_row_guard(
    ctx: &SessionContext,
    keys: &[crate::core::ir::IrExpr],
) -> Result<Batches, DataFusionError> {
    let mut rendered_keys = Vec::with_capacity(keys.len());
    for key in keys {
        rendered_keys.push(render_expr(&to_df_expr(key)?)?);
    }
    // Violation iff the probe returns any row: a lone HAVING is the grand-
    // total count, GROUP BY + HAVING the per-key counts.
    let probe_sql = match rendered_keys.is_empty() {
        true => "SELECT 1 FROM \"in_0\" HAVING count(*) > 1".to_string(),
        false => format!(
            "SELECT 1 FROM \"in_0\" GROUP BY {} HAVING count(*) > 1 LIMIT 1",
            rendered_keys.join(", ")
        ),
    };
    let violations = ctx.sql(&probe_sql).await?.count().await?;
    if violations > 0 {
        return Err(DataFusionError::Execution(
            "Scalar subquery produced more than one row".to_string(),
        ));
    }
    collect_batches(ctx.table("in_0").await?).await
}

/// Plan pre-rendered SQL over the registered merge inputs (e.g. a whole CTE).
async fn run_raw_sql(
    ctx: &SessionContext,
    sql: &str,
) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
    ctx.sql(sql).await
}

/// Apply LIMIT/OFFSET over the single input `in_0`.
async fn run_limit(
    ctx: &SessionContext,
    limit: Option<usize>,
    offset: usize,
) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
    ctx.table("in_0").await?.limit(offset, limit)
}

/// Plan a boolean filter over the single input `in_0`.
async fn run_filter(
    ctx: &SessionContext,
    predicate: &crate::core::ir::IrExpr,
) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
    ctx.table("in_0").await?.filter(to_df_expr(predicate)?)
}

/// Plan an ORDER BY over the single input `in_0`.
async fn run_sort(
    ctx: &SessionContext,
    keys: &[crate::core::ir::SortKey],
) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
    let mut sort_exprs = Vec::with_capacity(keys.len());
    for k in keys {
        sort_exprs.push(to_df_expr(&k.expr)?.sort(k.ascending, k.nulls_first));
    }
    ctx.table("in_0").await?.sort(sort_exprs)
}

/// Run a GROUP BY over the single input `in_0` by building DataFusion SQL, so
/// every aggregate function DataFusion knows works without per-function wiring.
async fn run_aggregate(
    ctx: &SessionContext,
    select: &[AggSelectItem],
    group_by: &[crate::core::ir::IrExpr],
    grouping_sets: &[Vec<crate::core::ir::IrExpr>],
) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
    let mut items = Vec::with_capacity(select.len());
    for item in select {
        let rendered = match (&item.agg, &item.expr) {
            (Some(agg), _) => render_agg(agg)?,
            (None, Some(expr)) => render_expr(&to_df_expr(expr)?)?,
            (None, None) => {
                return Err(DataFusionError::Plan(
                    "aggregate select item has neither expr nor agg".into(),
                ))
            }
        };
        items.push(format!("{rendered} AS {}", quote_ident(&item.alias)));
    }

    let mut sql = format!("SELECT {} FROM \"in_0\"", items.join(", "));
    sql.push_str(&group_by_clause(group_by, grouping_sets)?);

    ctx.sql(&sql).await
}

/// Collect a DataFrame into Batches under ONE schema all batches conform to.
///
/// The Binding's schema must match its batches or registering it as a MemTable
/// rejects it ("Mismatch between schema and batches"). Two ways the logical
/// `df.schema()` and the executed batches disagree:
/// - TYPE: SUM widens decimal precision in execution (Decimal(17,2) ->
///   Decimal(27,2)) but not in the logical schema (q16/q94/q95).
/// - NULLABILITY: a UNION's branches can disagree on a column's nullability, so
///   the collected batches disagree with EACH OTHER (q77).
/// So take the executed types from the first batch, widen every field to
/// nullable (the safe superset), and re-schema every batch to it. Falls back to
/// the logical schema when empty.
async fn collect_batches(df: datafusion::dataframe::DataFrame) -> Result<Batches, DataFusionError> {
    let logical = Arc::new(df.schema().as_arrow().clone());
    let batches = collect_tracked(df).await?;
    let schema = match batches.first() {
        Some(batch) => nullable_schema(&batch.schema()),
        None => logical,
    };
    let batches = reschema(batches, &schema)?;
    Ok(Batches { schema, batches })
}

/// Materialize a DataFrame while charging every accumulated batch to the
/// shared memory pool. Operators only account their WORKING memory, so a
/// fragment whose OUTPUT explodes (a cross join's cartesian product) would
/// otherwise grow unseen past the cap and OOM the server; charging the
/// accumulation makes it fail with ResourcesExhausted instead. The
/// reservation is released once the batches are handed on as a binding.
async fn collect_tracked(
    df: datafusion::dataframe::DataFrame,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    // Build the plan explicitly (identical to what df.execute_stream does
    // internally) so the SAME plan is available AFTER the run for metric
    // harvesting - a fused region's interior AggregateExec carries the group
    // count the catalog wants, invisible from the streamed batches alone.
    // Measured free vs df.execute_stream (the walk touches only AggregateExec
    // metrics), so it runs unconditionally - no feature flag on the hot path.
    let plan = df.create_physical_plan().await?;
    let task_ctx = std::sync::Arc::new(df.task_ctx());
    let stream = datafusion::physical_plan::execute_stream(plan.clone(), task_ctx)?;
    let batches = collect_stream(stream).await?;
    harvest_aggregates(&plan);
    Ok(batches)
}

/// Drain a stream into a Vec, charging each batch to the shared memory pool.
async fn collect_stream(
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let reservation = MemoryConsumer::new("fedq_collect").register(&runtime_env().memory_pool);
    let mut batches = Vec::new();
    while let Some(item) = stream.next().await {
        let batch = item?;
        reservation.try_grow(batch.get_array_memory_size())?;
        batches.push(batch);
    }
    Ok(batches)
}

thread_local! {
    // (sorted group-column names, measured group count) harvested from every
    // final AggregateExec that ran during this query. execute() matches these
    // to the observed aggregate fragments (by group columns) after the step
    // loop, turning a lazy-fused cross-source GROUP BY's collapse - invisible
    // from the streamed batches - into a learned-stats observation.
    static AGG_HARVEST: std::cell::RefCell<Vec<(String, usize)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Walk a collected physical plan and record each FINAL aggregate's group
/// columns and output-row count (the group count) into the harvest buffer.
/// Partial aggregates are skipped - their per-partition counts are not the
/// group count.
fn harvest_aggregates(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
    let any = plan.as_ref() as &dyn std::any::Any;
    if let Some(agg) = any.downcast_ref::<AggregateExec>() {
        if !matches!(agg.mode(), AggregateMode::Partial) {
            if let Some(rows) = plan.metrics().and_then(|m| m.output_rows()) {
                let key = agg_group_key(agg);
                AGG_HARVEST.with(|h| h.borrow_mut().push((key, rows)));
            }
        }
    }
    for child in plan.children() {
        harvest_aggregates(child);
    }
}

/// The sorted, comma-joined output names of an aggregate's GROUP BY columns -
/// the key that matches a harvested count to its observed aggregate fragment.
fn agg_group_key(agg: &datafusion::physical_plan::aggregates::AggregateExec) -> String {
    let mut names: Vec<String> = agg
        .group_expr()
        .expr()
        .iter()
        .map(|(_, name)| name.clone())
        .collect();
    names.sort();
    names.join(",")
}

/// Append a (binding, group count) observation for every aggregate merge
/// fragment whose group columns match EXACTLY ONE harvested aggregate. Runs
/// after the whole plan executed, so every region has collected and harvested.
fn append_group_observations(ir: &Ir, observations: &mut Vec<(String, usize)>) {
    let harvest: Vec<(String, usize)> = AGG_HARVEST.with(|h| h.borrow().clone());
    for step in &ir.steps {
        if let Step::Merge {
            fragment, binding, ..
        } = step
        {
            if let Some(Fragment::Aggregate {
                group_by,
                grouping_sets,
                ..
            }) = ir.fragments.get(fragment)
            {
                if grouping_sets.is_empty() {
                    if let Some(rows) = match_group_count(&harvest, group_by) {
                        observations.push((binding.clone(), rows));
                    }
                }
            }
        }
    }
}

/// The group count for a group-by list when EXACTLY ONE harvested aggregate has
/// the same (sorted, plain-column) key, else None - the catalog would rather
/// miss a value than key an ambiguous one to the wrong subplan.
fn match_group_count(harvest: &[(String, usize)], group_by: &[IrExpr]) -> Option<usize> {
    let key = group_by_key(group_by)?;
    let mut found: Option<usize> = None;
    for (harvested_key, rows) in harvest {
        if *harvested_key == key {
            if found.is_some() {
                return None;
            }
            found = Some(*rows);
        }
    }
    found
}

/// The sorted comma-joined names of a group-by list when every key is a plain
/// column, else None (an expression key has no name to match a harvest on).
fn group_by_key(group_by: &[IrExpr]) -> Option<String> {
    if group_by.is_empty() {
        return None;
    }
    let mut names = Vec::with_capacity(group_by.len());
    for expr in group_by {
        match expr {
            IrExpr::Column { name, .. } => names.push(name.clone()),
            _ => return None,
        }
    }
    names.sort();
    Some(names.join(","))
}

/// A copy of `schema` with every field marked nullable, keeping each field's
/// executed data type. Lets batches that differ only on per-column nullability
/// (a UNION branch whose column is non-null vs one where it is) share one schema.
fn nullable_schema(schema: &Schema) -> SchemaRef {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        fields.push(Field::new(field.name(), field.data_type().clone(), true));
    }
    Arc::new(Schema::new(fields))
}

/// The GROUP BY clause: `GROUPING SETS (...)` when sets are given, else a plain
/// grouping key list, else empty.
fn group_by_clause(
    group_by: &[crate::core::ir::IrExpr],
    grouping_sets: &[Vec<crate::core::ir::IrExpr>],
) -> Result<String, DataFusionError> {
    if !grouping_sets.is_empty() {
        return grouping_sets_clause(grouping_sets);
    }
    if group_by.is_empty() {
        return Ok(String::new());
    }
    let mut groups = Vec::with_capacity(group_by.len());
    for g in group_by {
        groups.push(render_expr(&to_df_expr(g)?)?);
    }
    Ok(format!(" GROUP BY {}", groups.join(", ")))
}

/// Render `GROUP BY GROUPING SETS ((a, b), (a), ())`.
fn grouping_sets_clause(
    grouping_sets: &[Vec<crate::core::ir::IrExpr>],
) -> Result<String, DataFusionError> {
    let mut rendered = Vec::with_capacity(grouping_sets.len());
    for set in grouping_sets {
        let mut cols = Vec::with_capacity(set.len());
        for expr in set {
            cols.push(render_expr(&to_df_expr(expr)?)?);
        }
        rendered.push(format!("({})", cols.join(", ")));
    }
    Ok(format!(" GROUP BY GROUPING SETS ({})", rendered.join(", ")))
}

fn render_agg(agg: &AggCall) -> Result<String, DataFusionError> {
    let inner = if agg.star {
        "*".to_string()
    } else {
        let mut parts = Vec::with_capacity(agg.args.len());
        for a in &agg.args {
            parts.push(render_expr(&to_df_expr(a)?)?);
        }
        parts.join(", ")
    };
    let distinct = if agg.distinct { "DISTINCT " } else { "" };
    let mut sql = format!("{}({}{})", agg.func, distinct, inner);
    if let Some(wg) = &agg.within_group {
        let key = render_expr(&to_df_expr(&wg.key)?)?;
        let direction = if wg.desc { " DESC" } else { "" };
        sql.push_str(&format!(" WITHIN GROUP (ORDER BY {key}{direction})"));
    }
    Ok(sql)
}

/// Double-quote an identifier for a DataFusion SQL alias.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Project a composed frame to its output columns, lazily when possible.
///
/// Unique output names alias straight into the plan (the composed region
/// keeps streaming). Duplicate output names - legal SQL (`SELECT a, a`) that
/// a DataFusion projection cannot express - fall back to the eager path:
/// alias to unique internal names, execute, and rename the collected schema
/// (Arrow permits duplicate field names).
async fn projected_binding(
    df: datafusion::dataframe::DataFrame,
    project: &[Projection],
    distinct: bool,
    input_bytes: usize,
) -> Result<Binding, DataFusionError> {
    if !unique_aliases(project) {
        return Ok(Binding::Materialized(
            project_dataframe_dedup(df, project, distinct).await?,
        ));
    }
    let mut exprs = Vec::with_capacity(project.len());
    for p in project {
        exprs.push(to_df_expr(&p.expr)?.alias(&p.alias));
    }
    let mut projected = df.select(exprs)?;
    if distinct {
        projected = projected.distinct()?;
    }
    Ok(lazy(projected, input_bytes))
}

/// Whether every projection output name is unique.
fn unique_aliases(project: &[Projection]) -> bool {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for p in project {
        if !seen.insert(p.alias.as_str()) {
            return false;
        }
    }
    true
}

/// Eagerly project to output columns with duplicate names: alias each
/// expression to a unique internal name, execute, then rename the collected
/// schema to the intended (repeating) output names.
async fn project_dataframe_dedup(
    df: datafusion::dataframe::DataFrame,
    project: &[Projection],
    distinct: bool,
) -> Result<Batches, DataFusionError> {
    let mut exprs = Vec::with_capacity(project.len());
    for (i, p) in project.iter().enumerate() {
        exprs.push(to_df_expr(&p.expr)?.alias(format!("__c{i}")));
    }
    let mut projected = df.select(exprs)?;
    if distinct {
        projected = projected.distinct()?;
    }
    let internal = projected.schema().as_arrow().clone();
    let batches = collect_tracked(projected).await?;
    let schema = output_schema(&internal, project);
    let batches = reschema(batches, &schema)?;
    Ok(Batches { schema, batches })
}

/// The output schema: internal column types under the intended output names.
fn output_schema(internal: &Schema, project: &[Projection]) -> SchemaRef {
    let mut fields = Vec::with_capacity(project.len());
    for (field, p) in internal.fields().iter().zip(project) {
        fields.push(Field::new(
            &p.alias,
            field.data_type().clone(),
            field.is_nullable(),
        ));
    }
    Arc::new(Schema::new(fields))
}

/// Rebuild each batch under `schema` (same columns, renamed fields).
fn reschema(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        out.push(RecordBatch::try_new(
            schema.clone(),
            batch.columns().to_vec(),
        )?);
    }
    Ok(out)
}

/// Plan an equi-join of `in_left` and `in_right` on paired key columns.
async fn join_frame(
    ctx: &SessionContext,
    join_type: JoinKind,
    left_keys: &[String],
    right_keys: &[String],
) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
    let left = ctx.table("in_left").await?;
    let right = ctx.table("in_right").await?;
    let lk: Vec<&str> = left_keys.iter().map(|s| s.as_str()).collect();
    let rk: Vec<&str> = right_keys.iter().map(|s| s.as_str()).collect();
    left.join(right, datafusion_join_type(join_type), &lk, &rk, None)
}

/// Plan a non-equi (nested-loop) join on an arbitrary condition (`None` =
/// cross join) of `in_left` and `in_right`.
async fn nested_loop_frame(
    ctx: &SessionContext,
    join_type: JoinKind,
    condition: &Option<crate::core::ir::IrExpr>,
) -> Result<datafusion::dataframe::DataFrame, DataFusionError> {
    let left = ctx.table("in_left").await?;
    let right = ctx.table("in_right").await?;
    let on_exprs = match condition {
        Some(expr) => vec![to_df_expr(expr)?],
        None => Vec::new(),
    };
    left.join_on(right, datafusion_join_type(join_type), on_exprs)
}

/// Map the IR join kind to DataFusion's. A free function, not a `From` impl:
/// both types are external to this crate (JoinKind in fedqrs-core, JoinType in
/// datafusion), so the orphan rule forbids the trait impl here.
fn datafusion_join_type(k: JoinKind) -> JoinType {
    match k {
        JoinKind::Inner => JoinType::Inner,
        JoinKind::Left => JoinType::Left,
        JoinKind::Right => JoinType::Right,
        JoinKind::Full => JoinType::Full,
        JoinKind::Semi => JoinType::LeftSemi,
        JoinKind::Anti => JoinType::LeftAnti,
    }
}

/// Name the binding and the exact field difference when its batches do not
/// match its declared schema - "Mismatch between schema and batches" alone
/// says neither which input nor which column diverged.
fn binding_schema_error(name: &str, b: &Batches, source: DataFusionError) -> DataFusionError {
    let declared = b.schema.as_ref().clone();
    let executed = match b.batches.first() {
        Some(batch) => batch.schema().as_ref().clone(),
        None => return source,
    };
    let mut diffs = Vec::new();
    for (df, ef) in declared.fields().iter().zip(executed.fields()) {
        if !df.contains(ef) {
            diffs.push(format!("declared {df:?} vs executed {ef:?}"));
        }
    }
    DataFusionError::Plan(format!(
        "binding '{name}' schema/batches mismatch: {}",
        diffs.join("; ")
    ))
}

/// Consume a binding as materialized batches. The LAST reader takes it out
/// of the map; earlier readers of a shared binding (a multi-referenced CTE's
/// materialized body) get an Arc-shallow clone so the binding survives for
/// the next reader.
fn consume_materialized(
    bindings: &mut HashMap<String, Binding>,
    remaining: &mut HashMap<String, usize>,
    name: &str,
) -> Result<Batches, DataFusionError> {
    let uses = remaining.get(name).copied().unwrap_or(0);
    if uses > 1 {
        note_use(remaining, name);
        return match bindings.get(name) {
            Some(Binding::Materialized(b)) => Ok(Batches {
                schema: b.schema.clone(),
                batches: b.batches.clone(),
            }),
            Some(_) => Err(DataFusionError::Plan(format!(
                "binding '{name}' is not a relation"
            ))),
            None => Err(DataFusionError::Plan(format!("unknown binding '{name}'"))),
        };
    }
    note_use(remaining, name);
    take_materialized(bindings, name)
}

/// Take a binding out of the map as materialized batches.
fn take_materialized(
    bindings: &mut HashMap<String, Binding>,
    name: &str,
) -> Result<Batches, DataFusionError> {
    match bindings.remove(name) {
        Some(Binding::Materialized(b)) => Ok(b),
        Some(_) => Err(DataFusionError::Plan(format!(
            "binding '{name}' is not a relation"
        ))),
        None => Err(DataFusionError::Plan(format!("unknown binding '{name}'"))),
    }
}

/// Take the named binding as the result schema and batches.
fn export(
    bindings: &mut HashMap<String, Binding>,
    input: &str,
) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    match bindings.remove(input) {
        Some(Binding::Materialized(b)) => Ok((b.schema, b.batches)),
        Some(Binding::Spilled(spilled)) => read_spilled(&spilled),
        Some(_) => Err(ExecError::runtime(format!(
            "cannot return non-relation binding '{input}'"
        ))),
        None => Err(ExecError::runtime(format!(
            "return of unknown binding '{input}'"
        ))),
    }
}

/// Read a spilled relation back into memory for the final export: the result
/// crosses to Python as ONE in-memory Arrow stream, so a spilled final result
/// is materialized here (the spill file itself never crosses the boundary).
fn read_spilled(spilled: &SpilledBinding) -> ExecResult<(SchemaRef, Vec<RecordBatch>)> {
    let handle = std::fs::File::open(spilled.file.path())
        .map_err(|e| ExecError::runtime(format!("binding spill open: {e}")))?;
    let reader = arrow::ipc::reader::FileReader::try_new(handle, None)
        .map_err(|e| ExecError::runtime(format!("binding spill read: {e}")))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| ExecError::runtime(format!("binding spill batch: {e}")))?);
    }
    Ok((spilled.schema.clone(), batches))
}
