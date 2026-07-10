# Rewrite: everything Python -> Rust. Technical plan.

Convert the entire engine (30,183 lines under `federated_query/`) to Rust.
Conversion only - behavior-identical, no new features. TEST-FIRST: every
crate lands its translated test corpus before its implementation, and the
implementation is written to make those tests green. No serialization
boundary anywhere: the JSON IR and `rust_ir.py` cease to exist; the engine
executes `PhysicalPlan` structs it built itself, in-process.

Method: the Rust engine is a PARALLEL implementation inside this repo. The
Python engine keeps running untouched (it is the working product and the
behavioral reference) until the Rust engine passes the complete gate -
every translated test, the SQL-level integration corpus, and the tallies
(TPC-DS 99|0|0 at SF0.1/SF1/SF10, pg-dims AND adversarial; TPC-H 22/22;
cold and warm columns) - after which the Python package is DELETED. The
tallies compare against pure-DuckDB truth, so they are engine-independent
oracles; no cross-language plan comparison and no compatibility layer is
needed or built.

---

## 1. Workspace

`federated-query/` becomes a single Cargo workspace. `/workspace/fedqrs`
moves into it (git history imported via subtree merge). Crates are split by
MEANING - one crate per pipeline responsibility, dependencies forming a DAG
that mirrors the pipeline:

```
federated-query/
  Cargo.toml                 [workspace]
  crates/
    fq-common/               errors, config, logging, ASCII/type utilities
    fq-catalog/              metadata model + the learned-stats store
    fq-connectors/           PostgreSQL / DuckDB / Parquet: fetch, metadata,
                             statistics, probe, EXPLAIN estimates, shipping,
                             ctid-parallel + prefetch pools
    fq-plan/                 expression + logical + physical node model,
                             the shared walkers, EXPLAIN documents
    fq-parse/                polyglot AST -> logical plan, star expansion,
                             dialect options
    fq-bind/                 name resolution, typing, the Bound column model
    fq-decorrelate/          subquery removal (semi/anti/scalar/lateral)
    fq-optimize/             rule driver + every rule, join ordering,
                             cost model, statistics collection, signatures
    fq-physical/             lowering, single-source pushdown, dim shipping,
                             eager-agg interplay annotations, step building
    fq-emit/                 per-dialect SQL text from plan nodes
    fq-exec/                 the execution engine (today's fedqrs engine.rs:
                             steps, fragments, DataFusion regions, reductions,
                             spill, memory pool)
    fq-runtime/              pipeline orchestration, plan cache, EXPLAIN,
                             runtime assembly from config
    fedq/                    the CLI binary
    fedq-py/                 pyo3 bindings (execute(sql) -> Arrow) - exists
                             so the Python benchmark/tally harnesses drive
                             the engine; nothing else depends on it
  federated_query/           the Python engine - UNTOUCHED until the final
                             gate passes, then deleted
  benchmarks/  tests/        unchanged during the rewrite (see section 6)
```

Dependency DAG (arrows = depends on):

```
fq-parse -> fq-plan -> fq-common
fq-bind -> fq-plan, fq-catalog
fq-decorrelate -> fq-plan
fq-optimize -> fq-plan, fq-catalog, fq-connectors (stats round trips)
fq-physical -> fq-plan, fq-optimize (cost), fq-emit
fq-emit -> fq-plan
fq-exec -> fq-plan (consumes PhysicalPlan directly), fq-connectors
fq-runtime -> everything above
fedq, fedq-py -> fq-runtime
```

`fq-exec` and `fq-connectors` start as MOVES of today's fedqrs code
(`engine.rs`, `connectors.rs`, `core/`), then refactor per section 5. No
crate except `fedq-py` may depend on pyo3; `cargo test --workspace` runs
the entire engine GIL-free.

## 2. Core type decisions (made once, everything builds on them)

- `Expr` is ONE enum with a variant per expression node (the 23 Python
  classes): ColumnRef, Literal, BinaryOp, UnaryOp, FunctionCall, CaseExpr,
  InList, Between, Cast, WindowExpr, Extract, Interval, Tuple, and the
  subquery-bearing variants (Subquery, Exists, InSubquery,
  QuantifiedComparison) which exist ONLY pre-decorrelation. Every traversal
  is an exhaustive `match` with NO `_` arm - adding a variant breaks every
  walker at compile time. This replaces
  `test_expression_walker_exhaustiveness.py` (331 lines) with the compiler.
- `LogicalPlan` is one enum (the 23 node classes: Scan, Projection, Filter,
  Join, Aggregate, Sort, Limit, SetOperation, Union, Explain, CTE, CTERef,
  Values, SubqueryScan, SingleRowGuard, GroupedLimit, LateralJoin, ...).
  `PhysicalPlan` likewise (the 30 classes incl. PhysicalRemoteQuery,
  PhysicalShipment, PhysicalHashJoin, PhysicalWindow, Gather, ...).
  Children are `Box`/`Vec`; CTE bodies shared by reference use `Arc` plus a
  `NodeId` (a u64 stamped at construction) wherever Python used `id(node)`
  identity (scan-binding caches, injection dedup, CTE body sharing).
- TWO column reference types enforce the qualification invariant at compile
  time: `fq_parse::RawColumn { table: Option<Ident>, column: Ident }`
  exists only inside fq-parse; binding produces
  `BoundColumn { relation: RelId, column: Ident, ty: DataType }`.
  An unqualified post-bind reference is UNREPRESENTABLE;
  `scope_validator.py` (176 lines) is retired by the type system.
- Schema memoization: `OnceCell<Arc<[Ident]>>` per physical node, same
  invariant as Python (structural fields never mutate after construction) -
  the q59 lesson stays load-bearing.
- Estimates: `CardinalityEstimate { rows: Option<u64>, gaps: Vec<Gap> }`
  with the honest-unknowns semantics unchanged (None = unknown and
  propagates; gap-carrying value = upper bound; decision points define
  their unknown policy).
- Errors: one `FqError` per layer boundary (`ParseError`, `BindError`,
  `DecorrelationError`, `PlanError`, `ExecError`), `thiserror`-derived,
  message text preserved where tests assert on it. NO catch-and-rewrap;
  `?` propagates. The only "catch" sites are the ones Python deliberately
  has: the decorrelation->lateral fallback, the oracle/capability probes in
  connectors, and the SMJ retry in exec.
- House-rule enforcement in Rust: `#![deny(warnings)]`, clippy pedantic
  set agreed in fq-common, `match` exhaustiveness (no `_` on plan/expr
  enums, enforced by a workspace lint script like today's `lint/`), ASCII
  check unchanged, every fn commented (existing convention).

## 3. Per-crate specification: exactly what each one implements

Each subsection lists (a) the Python code it replaces, (b) the concrete
behaviors/steps to reproduce, (c) its TEST-FIRST corpus - the Python test
files translated into that crate's `#[cfg(test)]` / `tests/` BEFORE
implementation.

### 3.1 fq-common
Replaces: `model.py` (60), `config/config.py` (246), `utils/logging.py`
(151), `parser/errors.py`.
- Config structs (serde-YAML: DatasourceConfig, OptimizerConfig incl.
  enable flags + max_join_reorder_size, CostConfig, ExecutorConfig) - same
  YAML files parse unchanged.
- Error enums; `tracing` setup.
Tests first: `test_config.py` (243).

### 3.2 fq-catalog
Replaces: `catalog/catalog.py` (137), `catalog/schema.py` (143),
`catalog/stats_catalog.py` (330).
- Catalog: datasource registry, Schema/Table/Column metadata,
  `load_metadata` orchestration, case-insensitive table lookup.
- StatsCatalog on rusqlite against the SAME SQLite schema and files
  (table_stats / predicate_stats / group_stats / subplan_stats /
  materialized_fragments; WAL + synchronous=OFF; one commit per query;
  TTL reads; upsert self-heal).
Tests first: `test_catalog.py` (251), `test_stats_catalog.py` (419).

### 3.3 fq-connectors
Replaces: `datasources/base.py` (467), `postgresql.py` (849), `duckdb.py`
(218), `parquet.py` (49), `clickhouse.py` (215) + absorbs today's
`fedqrs/src/connectors.rs`.
- The existing Rust fetch machinery stays: per-thread pg ADBC connection
  caches, duck clone-per-cursor, parquet DataFusion contexts, ctid-parallel
  pool, prefetch pool, `ship_table` (duck pinned TEMP TABLE / pg pg_temp via
  binary COPY + ANALYZE + drop-at-end, SHIP_MAX_ROWS backstop).
- PORTS from Python: metadata loading (pg information_schema with the
  column-type mapping incl. uuid; duck duckdb_tables/columns); statistics
  (pg reltuples + pg_stats decode: negative n_distinct as fraction,
  histogram-bound min/max parsing to int/float/timestamp; duck catalog
  count + single approx aggregate scan per requested columns); the
  never-ANALYZEd PROBE (exact aggregate scan <= PROBE_EXACT_BYTES,
  TABLESAMPLE row estimate above, no sampled NDVs); `estimate_scan_rows`
  (pg EXPLAIN FORMAT JSON gated to ANALYZEd tables + rollback-on-failure;
  duck EXPLAIN "~N rows" parse); capability enums; uuid text decode.
- ClickHouse: same PARITY as Python today - metadata surface plus the loud
  "no native connector" refusal at registration.
Tests first: `test_datasources.py` (294), `test_statistics_layer.py` (325),
`test_postgres_streaming.py` (126), the probe/EXPLAIN tests inside
test_statistics_layer.

### 3.4 fq-plan
Replaces: `plan/logical.py` (1026), `plan/expressions.py` (1107),
`plan/physical.py` (3031), `plan/field_introspection.py`,
`plan/arrow_types.py`.
- The three node enums + constructors; `children`/`with_children` become
  struct field access; `transform_children`/`map_children` become one
  generic rebuild helper per enum.
- The shared expression walkers (`column_refs`, `split_conjuncts`,
  `combine_and`, `expression_children`, replace-refs) as free functions in
  one module - still the single home for traversal.
- EXPLAIN document construction (`build_document`, `_PlanFormatter`,
  `_DatasourceQueryCollector`, dynamic_filter_values EXPLAIN plumbing):
  the JSON document SHAPE is pinned by existing tests and must not change.
- Arrow DataType mapping.
Tests first: `test_expression_visitor.py` (248), field-preservation suites
(`test_node_field_preservation`, `test_field_preservation_round3`) where
they pin behavior (most retire - the type system covers construction), the
EXPLAIN-shape assertions extracted from e2e suites.

### 3.5 fq-parse
Replaces: `parser/parser.py` (2275), `parser/dialect.py` (88),
`processor/query_preprocessor.py` (937).
- polyglot-sql is the parser (spike: 121/121 corpus queries, 0.36ms avg).
  Canonical internal dialect remains Postgres-form.
- Star expansion FIRST, on the polyglot AST, catalog-driven: `*`/`alias.*`
  to explicit qualified columns; records the internal->visible
  ColumnMapping the runtime applies to result columns; static PIVOT ->
  conditional aggregation; named-window inlining; unresolvable star raises
  StarExpansionError. Same phase order as Python (expansion before parse-
  to-logical) so rename semantics are identical.
- AST -> LogicalPlan: multi-statement rejection; Select/Explain/set-op
  dispatch; clause builders in the same order (FROM incl. comma joins ->
  WHERE -> GROUP BY -> HAVING -> SELECT -> ORDER BY -> LIMIT/OFFSET);
  expression conversion incl.: simple CASE lowered to searched form
  (function-bearing operand refuses), positional ORDER BY resolved to
  outputs, window rejection in WHERE/GROUP BY/HAVING scoped at nested
  SELECT boundaries, BETWEEN [SYMMETRIC], IN list vs IN subquery,
  EXISTS/ANY/ALL, EXTRACT/INTERVAL, TABLESAMPLE, DISTINCT ON, VALUES,
  recursive CTEs, join read-set over-collection (`_columns_for_join_table`
  semantics: the parser over-collects, the binder prunes).
- The defensive posture ports EXACTLY: the SUPPORTED_*_ARGS allowlists
  become exhaustive matches over polyglot node fields - any field the
  builder does not consume raises UnsupportedSQLError. This is the
  crate where polyglot's AST shape differs from sqlglot's; the port maps
  the allowlists onto polyglot's actual field names.
Tests first: `test_parser.py` (165), `test_parser_subquery_support.py`
(249), `test_query_preprocessor.py` (287), `test_star_expansion_cte.py`
(168), `test_deferred_features.py` (169 - the shapes that must KEEP
raising), plus the parse-relevant halves of e2e suites.

### 3.6 fq-bind
Replaces: `parser/binder.py` (1633).
- The scope chain: a stack of alias->Table scopes plus in-scope CTEs; push/
  pop per relational node; ONE column resolver (`resolve_in_scopes`
  semantics): qualified -> nearest scope defining the table; bare ->
  nearest scope defining the column; intra-scope ambiguity = error;
  no-scope resolution = BindError (the invalid-query-must-raise rule).
  Case-insensitive qualifier matching.
- Subquery binding with enclosing scopes (correlated refs innermost-first);
  one shared expression dispatch; Cast target-type resolution; scan column
  pruning to real table columns; star read-set expansion from catalog;
  UNION branch arity checks; HAVING/ORDER-BY-over-aggregate resolution via
  the aggregate-alias map; the aggregate-output HOISTS
  (`_hoist_aggregate_calls`: HAVING/ORDER BY aggregate calls matching no
  output become hidden `__agg_N` outputs with a restore projection);
  ORDER BY sum(x)/substring(x) re-application fixes (bind to outputs, not
  recomputation).
- Output type: the plan generic over column representation flips from
  RawColumn to BoundColumn here.
Tests first: `test_binder.py` (615), `test_binder_subqueries.py` (176),
binding-error e2e cases (bogus qualifiers MUST raise - the guard tests).

### 3.7 fq-decorrelate
Replaces: `optimizer/decorrelation.py` (2744).
- EXISTS/NOT EXISTS -> SEMI/ANTI; IN/NOT IN (subquery) -> NULL-aware
  SEMI/ANTI; op ANY/ALL -> SEMI/ANTI with quantifier semantics; scalar
  subqueries -> joins on correlation keys wrapped in SingleRowGuard
  (keyless = at most one row total; keyed = per key), correlated LIMIT ->
  GroupedLimit; projection/OR subqueries -> boolean flag columns via union
  branches; the N-K dependent-join construction (domain derivation, DISTINCT
  peeling incl. `_peel_distinct_projection`, join-back conditions,
  `__subq_N` alias discipline - every synthetic output qualified);
  disjunctive rewrite (same-key OR of positive existentials -> one SEMI
  over a UNION ALL of key domains; mixed/negative shapes keep their path);
  non-flattenable correlation -> DecorrelationError caught ONCE ->
  LateralJoin fallback; the post-pass assertion that NO subquery expression
  survives.
Tests first: `test_decorrelator_rewrites.py` (328),
`test_nk_decorrelation.py` (278), the decorrelation e2e suites
(disjunctive, lateral), and a FIXTURE corpus: every (bound plan ->
decorrelated plan) pair the Python suite produces, dumped once as
readable Rust test fixtures (constructed in Rust test code, not JSON).

### 3.8 fq-optimize
Replaces: `optimizer/rules.py` (1818), `join_ordering.py` (1133),
`join_graph.py` (229), `cost.py` (1156), `estimate_defaults.py` (182),
`statistics.py` (238), `subplan_signature.py` (193), `pushdown.py` (206),
`cte_union_filter.py` (326), `eager_aggregation.py` (591), `factory.py`
(79).
- The fixpoint driver (max 10 iterations, rules in factory order) and each
  rule as `fn apply(&LogicalPlan) -> Option<LogicalPlan>`:
  1. CTEUnionFilterPushdown: OR of consumer filters into the shared body,
     translated onto grouping columns; constant tags substitute per branch;
     decline list (bare consumers, recursive, DISTINCT ON, grouping sets);
     idempotence by predicate-already-embedded check.
  2. PredicatePushdown: through projections (alias-rewritten), below joins
     split by side with outer-join safety, into set-operation branches
     (per-branch distributable conjuncts), into scans; transitive
     constants (a=lit + a=b derives b=lit, INNER routed, LEFT/SEMI/ANTI
     from preserved side); the filtered-join arm applies its own condition
     pushes; factor common conjuncts out of each conjunct's OR.
  3. SemiJoinPushdown: commute selective SEMI/ANTI below the INNER join
     when the condition references one side.
  4. EagerAggregation: all gates as landed (SUM/constant/passthrough
     outputs, INNER plain-equi tree, multi-source, ship-budget rescue gate,
     largest-base-scan collapse pricing, __eager alias discipline,
     idempotence via contains-eager-partial).
  5. JoinOrdering: region extraction (maximal INNER/CROSS+filter subtrees,
     outer/semi/anti/lateral boundaries); left-deep Selinger DP over
     CONNECTED subsets per component (max_join_reorder_size bound; GOO
     greedy above), locality term TRANSFER_WEIGHT x transfer with
     subset-determined island state; deterministic tie-breaks (sorted
     subsets, lexicographic sequences); declines on any unknown atom;
     re-emission in pushdown normal form; predicate-conservation guard
     RAISES on any unplaced conjunct; scan estimate + join-key NDV
     annotations stamped on emitted nodes.
  6. ProjectionPushdown (required-column collection, scan/projection
     trimming), AggregatePushdown (single-source GROUP BY folding),
     OrderByPushdown (key rewriting through projections/joins),
     LimitPushdown (safety conditions).
- CostModel: the tracked estimator for every logical node; join formula
  L x R / prod(max ndv) with composite cap at min-side rows; honest
  unknowns (Option rows, gap provenance, bounds semantics); selectivity
  pricing (eq 1/ndv, literal-literal exact, IN len/ndv, range min-max
  interpolation on the shared ordinal scale incl. temporal parsing,
  interval PAIRING for both-bounded ranges, NOT/OR/NULL-fraction
  complements with unknown propagation); filter VALUE CAPS on group-key
  NDVs (EQ -> 1, IN -> len); learned predicate-template fill BEFORE
  ask-the-source; CTE body registry (registered during rule descent, reset
  per walk, recursive stays unknown); aggregate estimates preferring
  measured group counts (subject = table name or subplan signature);
  SEMI/ANTI occupancy estimate (1 - e^-fanout).
- StatisticsCollector: session cache lazy per column, absence cached,
  learned overlay FILL-only, planner-estimate cache per rendered SQL.
- Subplan signatures + predicate templates (alias/constant-neutral,
  sorted shapes, sha1) - byte-identical signatures so existing learned
  catalogs keep matching.
Tests first: `test_logical_optimization.py` (1186),
`test_optimization_bugs*.py` (2113), `test_cost_model.py` (506),
`test_join_ordering_enumerator.py` (449), `test_join_ordering_rule.py`
(247), `test_join_graph.py` (194), `test_semi_join_pushdown.py` (199),
`test_cte_union_filter.py` (210), `test_eager_aggregation.py` (195),
`test_projection_pushdown_*.py` (548), `test_subplan_signature.py` (130),
`test_estimate_defaults.py`, `test_optimizer_factory.py` (152).

### 3.9 fq-emit
Replaces: `plan/emit/expressions.py`, `clauses.py`, `resolver.py` (~620)
AND the `to_source_sql` transpile boundary AND today's
`fedqrs core/src/sql.rs` (which already renders scan/temp-join SQL).
- ONE emitter: plan/expression nodes -> SQL text for a target dialect
  (Postgres, DuckDB, Parquet-DataFusion). No canonical-SQL-then-transpile
  round trip: each dialect renderer handles its divergences directly
  (function names incl. date/time mappings, TABLESAMPLE syntax,
  ordered-set aggregates, quoting, DISTINCT ON, grouping sets, window
  frames, interval literals, safe-division NULLIF rewrite).
- Renders: whole single-source islands (the SingleSourcePushdown output),
  scans with folded clauses, temp-join probes (`fedq_dyn_keys`), injected
  island SQL (key filter placed on the owning base relation INSIDE),
  correlated lateral SQL.
- polyglot is NOT on this path; it serves in TESTS as a cross-check oracle
  (transpile the Python emitter's canonical output and execution-compare).
Tests first: `test_dialect_rendering.py`, the emission assertions inside
`tests/e2e_pushdown/*` (aggregate_rendering, outer_join_condition_render,
ordered_set_aggregates, tablesample, named_windows, ...) - these assert on
the SQL the source RECEIVES and translate nearly 1:1; plus
execution-equality tests against real duck/pg for every emitted shape.

### 3.10 fq-physical
Replaces: `optimizer/physical_planner.py` (1198),
`single_source_pushdown.py` (1201), `dim_shipping.py` (445),
`executor/rust_ir.py` (2565 - the LOGIC, not the JSON).
- Lowering dispatch: try SingleSourcePushdown.try_build first (whole
  subtree one source + renderable -> PhysicalRemoteQuery with
  output_names, column_alias_map, estimated_rows = max-base root estimate,
  output_estimated_rows from the root annotation); then DimShipping
  .try_ship (all gates as landed: shippable shape, plain-aggregate
  collapse root, dimension-explosion with measured-group override,
  fact-large/foreign-small/ratio/budget, outputs-match, seeded schema from
  the pure plan, group_observation stamp under row-count-preserving
  wrappers); then per-node lowering (join algorithm selection: remote join
  -> equi-key extraction/orientation -> build-side by
  larger_estimated_side -> dynamic-filter marking -> hash join, else
  nested loop; set ops; CTE materialization; window projections; every
  scan annotated via the bounds-accepting `_scan_estimated_rows`).
- STEP BUILDING (rust_ir.py's semantics, now direct construction of
  fq-exec step values - no JSON): bottom-up emission of
  SourceScan/CollectDistinct/InjectedScan/Ship/Merge/Return steps and
  named fragments; identical-step CSE with alias-neutral scan share keys
  (fixed `__cse__` alias re-render), column-union widening, extras
  intersection narrowing; reduction orientation (`_orient_join` semantics:
  SEMI/LEFT fixed roles, cardinality probe through derived-table/union
  wrappers via `_derived_side_rows`, rank + urgency tie-breaks);
  injection-winner planning per traced base (score by donated keys,
  primary + up to two extras); probe base tracing through joins/renames/
  union branches incl. setop bases and identity pairs; useless-keys gate
  (USELESS_KEYS_NDV_FRACTION with containment fallback); delivery
  strategy inputs (inject_column_ndv threading); collect anchoring to
  originating base scans; binding use counts; observation provenance
  (table_rows / column_ndv / predicate templates / group signatures /
  island group stamps); parallel-scan spec eligibility; EXPLAIN dynamic
  filter values.
Tests first: `test_reduction_gate.py` (626), `test_dim_shipping_gate.py`
(245), `test_injected_scan_cse.py`, `test_parallel_scan_spec.py`,
`test_pushdown_real_e2e.py` (698), `test_functional_pushdown.py` (484),
`test_pushdown_cte.py` (386), the tests/e2e_pushdown suite (~50 files -
they drive SQL through the pipeline and assert pushed-SQL shapes and
results; they translate to Rust integration tests running against the same
seeded duck/pg fixtures).

### 3.11 fq-exec
Absorbs today's `fedqrs/src/engine.rs` + `core/` minus the IR:
- DELETE `core/src/ir.rs` and `execute_ir`/serde parsing. `execute(plan:
  &PhysicalPlan)` consumes the fq-plan tree; the step list is built by
  fq-physical in-process. `Step` stays as an internal exec type
  (constructed, never deserialized).
- Everything else stays as-is (it is already Rust and already tested by
  the tallies): lazy fragment fusion into DataFusion regions, the 32GB
  FairSpillPool + tracked collection, binding spill (16GiB resident budget,
  largest-first eviction), SMJ retry + 8GiB upfront region policy,
  reductions (inline-IN / temp-table / parquet delivery, unselective
  fallbacks), ship execution, prefetch pools (plain scans + pg injected),
  ctid-parallel reads, aggregate metric harvest, per-step observations,
  profile output.
- `expression_evaluator.py` (537): enumerate its remaining call sites
  during this port; each is either already covered by DataFusion
  evaluation (retire) or becomes a small kernel here (port with its tests,
  `test_expression_evaluator.py`, 230).
Tests first: `test_rust_engine.py` (368) translated to Rust integration
tests; existing Rust unit tests move with the code.

### 3.12 fq-runtime, fedq, fedq-py
Replaces: `processor/query_executor.py` (187), `query_context.py` (61),
`executor/executor.py` (126), `plan_cache.py` (97), `profiling.py` (138),
`cli/fedq.py` (599).
- The pipeline: expand -> parse -> bind -> decorrelate -> optimize ->
  physical -> execute -> rename-outputs; stage timers; EXPLAIN
  short-circuit (never executes); the plan cache (exact rewritten-SQL
  LRU+TTL keyed identically, EXPLAIN exempt, env controls); runtime
  assembly from Config (catalog build, connector registration, stats
  catalog next-to-config).
- `fedq` binary: the CLI (interactive + file modes as today's cli).
- `fedq-py`: `Runtime::execute(sql) -> Arrow C stream` + the same
  exception-class mapping tests assert on. Exists solely so
  `benchmarks/` and the SQL-level Python test suites keep running
  unchanged against the Rust engine.
Tests first: `test_plan_cache.py` (123), `test_critical_executor_bugs.py`
(1047, the executor-behavior halves), `test_e2e_*.py` (874) run through
fedq-py unchanged.

## 4. Build order

Strictly dependency-driven; each crate: translate its test corpus, then
implement to green, then the next crate. Integration milestones where the
whole engine is exercised:

1. fq-common, fq-catalog, fq-plan (pure data structures; fastest wins;
   everything depends on them).
2. fq-connectors (move existing Rust + port metadata/stats/probe; testable
   against the live pg/duck fixtures immediately).
3. fq-parse, fq-bind - MILESTONE A: SQL -> bound plan for the whole
   121-query corpus; corpus test = every query binds, every invalid-query
   fixture raises the right error.
4. fq-emit (needs fq-plan only) - MILESTONE B: every emitted shape
   execution-matches the Python emitter's output on live sources (the
   polyglot cross-check harness from the spike, promoted to a test).
5. fq-decorrelate, fq-optimize.
6. fq-physical + fq-exec refactor (IR deletion) - MILESTONE C: full
   pipeline runs single queries end to end; the SQL-level integration
   corpus starts passing.
7. fq-runtime + fedq + fedq-py - MILESTONE D (the gate): full Python
   e2e suites via fedq-py, tallies at all scales + adversarial + TPC-H +
   cold/warm, EXPLAIN document parity.
8. DELETE `federated_query/` (except nothing - the package goes), delete
   `/workspace/fedqrs` remnants, move `lint/` rules to the Rust lint
   script. HANDOFF/architecture docs rewritten against the crates.

## 5. What happens to today's fedqrs code, precisely

- `core/src/ir.rs` (Ir, Step deserialization, ScanSpec serde): Step and
  ScanSpec become plain structs in fq-exec constructed by fq-physical;
  ALL serde derives and the JSON entry point are deleted.
- `core/src/expr.rs` (IrExpr -> DataFusion Expr): replaced by direct
  fq-plan Expr -> DataFusion Expr conversion (one module in fq-exec).
- `core/src/sql.rs`: absorbed into fq-emit.
- `src/engine.rs`: splits - step execution/fragments/memory/prefetch into
  fq-exec; the scan-spec/delivery helpers stay with it; profile output
  stays.
- `src/connectors.rs`: becomes fq-connectors, plus the ported python
  connector surface (section 3.3).
- `src/lib.rs` pyo3 surface: replaced by fedq-py (execute(sql), not
  execute_ir(json)).

## 6. Test and oracle strategy (the test-first mechanics)

- UNIT: each crate's Python test files (mapped above; 80 files, 18,393
  lines, plus tests/e2e_pushdown) translate BEFORE implementation.
  Priority of translation within a file: behavior pins (guards, error
  cases, determinism, SQL shapes, results) first; Python-mechanics pins
  (pydantic field preservation, .create discipline, no-dataclass sweeps)
  are retired explicitly - the type system owns them now. Each retirement
  is listed in the crate's port notes, never silent.
- FIXTURE CORPORA: for decorrelate and optimize, the Python engine's
  stage outputs on the 121-query corpus are captured ONCE as Rust test
  fixtures (hand-checked plan constructions, not serialized dumps) for the
  gnarliest queries; the rest of the coverage comes from result-level
  tests.
- INTEGRATION: the SQL-level suites (tests/e2e_pushdown, test_e2e_*)
  keep their SQL + seeded-fixture form; they run against the Rust engine
  through fedq-py unchanged. Where they assert on pushed SQL, they assert
  through EXPLAIN exactly as today.
- SYSTEM ORACLES (never change during the rewrite): the TPC-DS tallies
  against pure-DuckDB truth at three scales x two placements, TPC-H
  against the federated oracle, cold_sources convergence, cold+warm
  columns. The Python engine additionally serves as a SECOND oracle the
  cheap way - run both runtimes on the same SQL and compare result
  tables - which needs no plan serialization and no interop.
- PERFORMANCE GATE at milestone D: cold and warm totals at least as good
  as the Python engine's current boards on the same box (the cold column
  is where the rewrite must show its floor win; the warm column must not
  regress the plan-cache behavior).

## 7. Assumptions taken (flag if wrong)

1. `benchmarks/` and the SQL-level Python test suites REMAIN Python and
   drive the engine through fedq-py; they are harnesses, not engine.
2. The Python engine is deleted wholesale at milestone D, not
   incrementally - it stays the shippable product until the Rust engine
   passes the full gate.
3. polyglot-sql is the parser; fq-emit renders dialects directly (polyglot
   is a test oracle only). If polyglot shows a parse gap on real input,
   the fix is contributed upstream or the construct is parsed by a local
   extension - not by reintroducing Python.
4. The learned-stats SQLite files and the YAML config format stay
   byte-compatible (they are data, not code).
