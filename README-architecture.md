# Architecture

The federated query engine executes SQL across heterogeneous data sources
(DuckDB, PostgreSQL, ClickHouse, MySQL, Parquet), pushing work down to each
source and stitching the results together over Apache Arrow. It is a single
Cargo workspace under `crates/`. This document describes how the crates fit
together, the query pipeline, the statement surface beyond plain queries, the
extension pattern the accelerator and event-analytics features follow, and the
three front doors that expose the engine.

All diagrams use plain ASCII. Arrows are written `->`.

## Crate layout

Every crate is one focused stage. Dependencies point strictly downward - a
crate never depends on a crate above it - so a change to a node type or a rule
recompiles only what is affected, and the boundaries are enforced by the build.

```
                         fedq-py   fedq-server   fedq (CLI)
                              \        |         /
                               \       |        /
                                    fq-runtime
        _____________________________/  |  \_____________________
       /            /           |       |        \       \       \
   fq-physical  fq-optimize  fq-decorrelate  fq-bind  fq-parse  fq-exec
       |    \        |            |            |        |         |
    fq-emit  \_______|____________|____________|________|    fq-connectors
       |             |            |            |        |         |
       \____________ fq-plan _____/____________/________/     fq-catalog
                        |                                         |
                     fq-common (leaf: config, errors, types) <---/

   fq-accel  (materialized-view store) --> object_store, stats SQLite
   fq-runtime composes fq-accel for the materialized-view statements
```

Per-crate responsibility (each crate's `src/lib.rs` module doc is the
authoritative statement):

- fq-common - the leaf crate: configuration (`Config`, loaded from YAML with
  serde `deny_unknown_fields` so an unknown or renamed field cannot be silently
  dropped), the error types, `DataType`, the planning budget, logging, and the
  event-statement value types. No engine logic.
- fq-plan - the node model: three enums, `Expr`, `LogicalPlan`, and
  `PhysicalPlan`, plus the shared expression-tree walkers. Every traversal is
  an exhaustive `match` with no `_` arm, so a new variant fails to compile every
  walker that has not handled it - the compiler enforces walker completeness.
  CTE producers are `Arc`-shared: one allocation per CTE, every CteScan points
  at it.
- fq-catalog - the metadata model and datasource registry: Schema / Table /
  Column, the `DataSource` trait (the catalog and statistics tier of a
  connector - no drivers), and the learned-statistics store `StatsCatalog` on
  rusqlite.
- fq-connectors - concrete connectors implementing `fq_catalog::DataSource`
  against real drivers: DuckDB, PostgreSQL, ClickHouse, MySQL, and Parquet
  (footer-only: schema, exact row counts, row-group statistics). This is the
  catalog / statistics tier ONLY; the data-plane fetch tier lives in fq-exec.
  Any unrecognized source kind is rejected loudly at registration.
- fq-parse - SQL text to `LogicalPlan`. polyglot-sql is the parser; a
  `Converter` walks its AST into the logical model and rejects unsupported SQL
  loudly (never silently dropped). It also classifies the non-query statement
  forms (materialized-view DDL, settings, event DDL and analyses) before the
  query pipeline sees them.
- fq-bind - name resolution and typing: resolves every table and column
  reference against the catalog, sets each column's qualifier and `DataType`,
  and RAISES on an invalid reference (a bogus qualifier or a typo'd column
  fails here, before any source is touched). One resolver, one path.
- fq-decorrelate - the always-decorrelate pass: rewrite every subquery
  expression into a set-based join so no correlated or bounded subquery
  survives into execution. Flattenable shapes (EXISTS / IN / op-ANY/ALL become
  SEMI/ANTI joins), the scalar LEFT-join path, the OR boolean-flag /
  domain-union path, the correlated sort-key rewrite, and the Neumann-Kemper
  dependent join with a LATERAL fallback. Synthetic columns carry a qualifier
  AND a `DataType`. Two post-passes run: an assertion that no subquery survives,
  and `scope::validate_scope`.
- fq-optimize - the logical optimizer and its estimation foundation: the
  `CostModel` (honest unknowns, a learned overlay, source `EXPLAIN` estimates),
  the `StatisticsCollector`, the fixpoint rule driver, and the rules -
  CTEUnionFilter, Predicate, SemiJoin, EagerAggregation, JoinOrdering (Selinger
  DP + greedy operator ordering + a locality term), Projection (including
  flag-union passthrough narrowing), Aggregate, OrderBy, Limit pushdown.
- fq-emit - canonical Postgres-form SQL text from `Expr` and clause nodes, plus
  the single transpile boundary (`to_source_sql`) that turns that canonical
  form into each source's own dialect via polyglot. Invariant: every canonical
  string it emits is valid Postgres that polyglot round-trips, because the
  transpile boundary re-parses it.
- fq-physical - optimized `LogicalPlan` to `PhysicalPlan`, then to executable
  steps: the `PhysicalPlanner` (lowering, join algorithm and orientation,
  dynamic-filter marking), `SingleSourcePushdown` (a subtree that lives on one
  source becomes one RemoteQuery), `DimShipping` (nine gates deciding when to
  ship a small dimension into a fact's source), and step building (PhysicalPlan
  to a `Vec<Step>` plus fragments, with CSE, reduction orientation, and the
  per-step observation stream).
- fq-exec - the execution engine over DataFusion: lazy fragment fusion into
  DataFusion regions, the fair spill pool, semi-join reductions (inline-IN,
  temp-table, Parquet delivery), ship execution, the prefetch pools,
  ctid-parallel Postgres reads, and the concrete connector data plane. The step
  bridge runs the whole pipeline's `Step` list end to end in-process (no JSON
  boundary).
- fq-runtime - the config-driven orchestration. `Runtime::from_config` wires
  every datasource into two tiers from one config (the shared
  `fq_catalog::Catalog` and the fq-exec data plane); `Runtime::execute` drives
  the pipeline and renames the engine's internal result schema onto the query's
  user-visible column names. It owns statement dispatch (MV DDL, settings,
  automatic substitution), textual and document EXPLAIN, the session-shared
  statistics collector and learned-stats persistence, and the StageLog
  planning-budget kill. This crate is pyo3-free.
- fq-accel - the materialized-view store (see Extension pattern).
- fedq-py, fedq-server, fedq - the three front doors (see Front doors).

## The query pipeline

A plain SQL query flows through fq-runtime as:

```
SQL text
  -> fq-parse       : polyglot-sql AST -> LogicalPlan (star-expanded, allowlist)
  -> fq-bind        : resolve every table/column, set qualifier + DataType
                      (an invalid reference RAISES here)
  -> fq-decorrelate : rewrite every subquery into a join (no correlation survives)
  -> fq-optimize    : cost-based rules + join ordering, to fixpoint
  -> fq-physical    : PhysicalPlanner -> PhysicalPlan (pushdown, dim-shipping),
                      then build_steps -> Vec<Step> + fragments
  -> fq-exec        : the step bridge fuses fragments into DataFusion regions
                      and executes; sources are read/pushed/shipped/reduced
  -> Arrow          : RecordBatches, renamed onto the user-visible output names
```

Two invariants govern the whole path:

- INVALID QUERIES RAISE. A bogus qualifier, a typo'd column, or SQL the engine
  cannot plan fails loudly (at binding, or at the parser's allowlist), never
  returns rows. Answering an invalid query is the engine's most severe failure
  mode.
- PLANNING IS O(METADATA). Every stage before execution reads only catalogs,
  cached / learned statistics, and source planner estimates (`EXPLAIN`) - never
  data. A hard wall-clock budget (`optimizer.planning_budget_ms`, default 100)
  kills a query whose planning touches data, with a per-stage report. There is
  no off switch.

Federation happens in fq-physical and fq-exec: SingleSourcePushdown collapses
any single-source subtree into one remote query in that source's dialect;
DimShipping ships a qualifying small dimension into a fact's source so a join
collapses to a single island; whatever cannot be pushed is executed by the
DataFusion coordinator over the Arrow results, with semi-join reductions and
dynamic filters shrinking each source read.

## Expression semantics

The engine's expression semantics are DUCKDB'S, on every source and on the
coordinator. A pushed expression must return what DuckDB returns for it;
placement is not part of a query, so where a table happens to sit must never
change an answer. Sources are each correct by their own rules and simply
disagree (most dialects truncate `int / int`; DuckDB divides as DOUBLE), so
rendering an expression verbatim into a source's dialect inherits that
source's meaning - the exact defect this section exists to forbid.

Enforcement is one construct at a time, at the narrowest choke point that
covers every execution path:

- Division: the binder rewrites a bound division over certainly-numeric
  operands into `CAST(left AS DOUBLE) / right` (`fq-bind/src/expr.rs`,
  `normalize_division`), a form with the same meaning in every dialect the
  engine renders to and in the coordinator. The cast is not skipped for a
  DOUBLE-typed dividend: a DECIMAL source column is modeled as DOUBLE, and
  only the explicit cast stops DataFusion from dividing the stored decimal
  at a fixed result scale that truncates the quotient. A division whose
  operand types the binder cannot see through keeps its written form.
- AVG: the binder wraps a certainly-double argument in the same DOUBLE cast
  (`normalize_avg`), so a decimal-backed column is averaged at double
  precision on every source instead of at DataFusion's decimal scale. The
  coordinator's own lowering applies the equivalent cast to the avg calls it
  executes itself (`fq-exec`), so both halves state one rule.
- round(): the binder rewrites `round(x)` and `round(x, n)` over a
  certainly-double `x` (n a literal in 0..=15) into guarded TRUNC arithmetic
  (`normalize_round`) that every dialect evaluates bit-identically to
  DuckDB's half-away-from-zero, because PostgreSQL rounds a double half to
  even. Uncertain shapes keep their written call.
- Divergences that are measured but not yet normalized are held STRICTLY in
  `tests/e2e_federated/known_fragment_divergences.py`: the fragment is still
  sent and still compared, and a source that starts agreeing fails the run so
  the entry dies with the fix. The registry is the work queue; it is
  currently empty. Result-TYPE divergences with agreeing values (a source
  declaring `decimal128(38,2)` where the oracle declares `decimal128(10,2)`,
  PostgreSQL answering an integer avg as numeric) are recorded by the
  fragment oracle's type-divergence count and are not yet normalized.
- The DuckDB oracle, the fragment oracle, and the plan-variant harness are
  the definition made executable: every placement and every plan of every
  corpus query is compared against DuckDB over the same data.

## The statement surface

`fq-parse` classifies each statement before the query pipeline. Anything it
does not recognize is a `Query` handed to the normal parser (which raises its
own loud error on SQL it cannot plan). The recognized non-query forms, all
dispatched by `fq-runtime`:

- Materialized views: `CREATE MATERIALIZED VIEW <name> AS <select>`, `REFRESH
  MATERIALIZED VIEW <name>`, `DROP MATERIALIZED VIEW <name>`, `SHOW
  MATERIALIZED VIEWS`.
- Settings: `SHOW SETTINGS`, `SHOW SETTING <name>`, `SET <name> = <value>`
  (or `SET <name> TO <value>`), `RESET <name>`, `RESET ALL`. The registry
  holds 33 tunables across the optimizer, cost model, executor, exec data
  plane, environment toggles, the accelerator, and the server. SET/RESET mutate
  the live session; a bad value or an unknown name raises, with a nearest-name
  suggestion.

## The materialized-view store

Materialized views (fq-accel) sit on top of the core engine. The design is
three parts:

1. A REGISTRY over a persisted result. The engine computes a view's result once
   and persists it; a lightweight registry beside the learned-stats SQLite
   (`<config>.stats.sqlite`) records the view name, its defining SQL, the
   ordered chunk list (the source of truth for which files compose the view -
   never a directory listing), the schema, timestamps, per-base-table source
   tokens, and the delta watermark.
2. A STORAGE CONTRACT. fq-accel's `ChunkStore` writes framed Arrow IPC chunks
   under `<config>.mv/` through the `object_store` abstraction, so the local
   filesystem backend can become an S3 bucket by configuration with no change
   to the chunk model.
3. USER-CONTROLLED FRESHNESS. Nothing on the query path checks the sources.
   Data advances only on an explicit REFRESH, which has three mechanics:
   WHOLE re-pull, DELTA APPEND past a monotonic change key's watermark, and
   PRIMARY-KEY MERGE (diff a fresh whole pull by key, rewrite only the chunks
   holding a changed or deleted key). A per-base-table source token lets REFRESH
   skip the pull entirely when nothing changed.

Automatic MV substitution (fq-runtime `substitute.rs`) closes the loop on the
query side: a plan that matches a view structurally, clears a name guard, and
clears a cost gate (estimated recompute cost must strictly exceed the read
cost) is rewritten to read the view. It is off-switchable per session
(`accelerator.enable_substitution`, read fresh per query) and tracks per-view
benefit counters.

## The event store

Event analytics (fq-events) is a second store beside the materialized views,
for the four product-analytics questions: funnels, retention, segmentation and
paths. `CREATE EVENT DATASET <name> FROM <source table> ACTOR <c> TIME <c>
EVENT <c> [TIEBREAK <c>] [PROPERTIES (...)] [WITH (...)]` reads a source table
once and materializes it into an actor-partitioned columnar store under
`<config>.events/`, registered in the same SQLite beside the learned stats.
`FUNNEL`, `RETENTION`, `SEGMENT` and `PATHS` are statements over that store,
each also usable as a table function in `FROM`; `REFRESH`, `REBUILD`, `DROP`
and `SHOW EVENT DATASETS` complete the surface. There is no ATTACH, LOAD or
WARM: a dataset is opened lazily on first use.

Two properties of the store decide how it behaves in practice, and both are
measured rather than assumed (numbers from an idle 12-core box, 2026-08-09):

- A BUILD IS ONE NUMBER, and about 40% of it is the paths index. 20,688,550
  events build in 8.7 s (index 3.5 s); 100,258,738 events in 41.2 s (index
  17.2 s). The index is a suffix array with an LCP array over every actor's
  duplicate-collapsed event stream, plus a second pair over the reversed text,
  which is what lets a PATHS statement answer at ANY depth from one interval
  sweep - depth changes neither the index's size nor the sweep's cost.
  Construction is linear (vendored libsais): 2.3 s at 20.1M positions, 11.3 s
  at 97.6M.

- REFRESH IS NOT INCREMENTAL, despite pulling only the delta. The source read
  is `WHERE <refresh_key> > <watermark>` and new generation files are appended
  rather than existing shards rewritten, but the paths index is then rebuilt
  over every event. Appending 1,000 rows to a 40M-event dataset costs 11.0 s -
  more than building the first 20M from scratch. Generations also never
  compact (`events.max_generations` is a setting nothing reads), and two
  generations already cost 3x on funnel and retention against one.

Without the index PATHS falls back to scanning the store, which is a slowdown
and not a different answer: on 100M events an anchored PATHS is 0.5 s against
0.013 s indexed, and both paths return byte-identical rows.

`event-analytics-design.md` section 6.2a carries the full measurement table and
the shapes a fix could take; taskq holds the open work.

## Front doors

Three binaries expose one `fq_runtime::Runtime`; all engine logic lives in the
runtime and the crates below it.

- fedq-py (`crates/fedq-py`) - the only pyo3 crate. `fedq.Runtime(config_path)`
  builds a runtime from a YAML config; `.execute(sql)` returns the Arrow result
  over the zero-copy Arrow C-stream boundary with the GIL released. This is what
  the benchmark harnesses drive.
- fedq-server (`crates/fedq-server`) - a PostgreSQL wire-protocol server
  (pgwire): any Postgres client can connect and issue SQL, and gets Arrow
  results back as Postgres rows. It speaks the simple and the extended query
  protocol (Parse/Bind/Describe/Execute), does SCRAM auth, and supports query
  cancellation. Each connection gets a dedicated OS thread that builds its own
  `Runtime` and services that connection in order (the fq-exec data plane keeps
  thread-local caches, so a query must run single-threaded); sources open
  read-only, which lets many connections share one DuckDB file.
- fedq (`crates/fedq`) - a clap-based CLI. `fedq --config <yaml> --command
  "<sql>"` (or `--file <path.sql>`) runs one statement and prints the result as
  table, csv, or json; with neither `--command` nor `--file`, the same config
  opens an interactive REPL.

## Deeper docs

- `event-analytics-design.md` - the event store's design: storage format,
  identity and dictionaries, indexes, the build pipeline, refresh mechanics
  (section 6.2a is the measured cost of refresh and the paths index), and the
  query algorithms behind the four analyses.
- `accelerator-plan.md` - the materialized-view / fragment-cache design: chunk
  model, keying, invalidation, the refresh mechanics, substitution, and the
  phased roadmap (S3 backend and benefit-based eviction are Phase D).
- `crates/fq-accel/README.md`, `crates/fedq-server/README.md`,
  `crates/fedq/README.md` - per-crate usage and design detail.
- `benchmarks/tpcds/README.md`, `benchmarks/tpch/README.md`,
  `benchmarks/perf_compare/README.md` - how to
  generate data, run each suite's single runner, and read the reports.
- taskq, project `federated-query` - the working state: what is built, the
  measured benchmark boards, the current perf tail with root-caused
  diagnoses, and the next steps.
- Each crate's `src/lib.rs` module doc is the authoritative description of that
  crate's responsibility and port notes.
