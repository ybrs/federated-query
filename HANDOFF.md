# HANDOFF - Python to Rust rewrite

This document is the working state of the Python->Rust rewrite of the engine.
The previous handoff (the Python engine's development history) is preserved in
`oldhandoff.md`.

## What this is

We are rewriting the entire engine (30,183 lines of Python under
`federated_query/`) into Rust, as a single Cargo workspace under
`federated-query/`. The governing technical plan is
`rewrite-python-to-rust-plan.md`.

Strategy (decided with the user):

- BIG-BANG, not incremental. The Rust engine is built as a parallel
  implementation. The Python engine stays the working product and the
  behavioral reference, untouched, until the Rust engine passes the full gate
  (all translated tests + the SQL-level corpus + the TPC-DS/TPC-H tallies vs
  pure-DuckDB truth), after which the Python package is deleted.
- TEST-FIRST per crate: each crate's Python test corpus is translated into Rust
  tests before/with the implementation. Every retirement (a Python-mechanics
  test the type system now owns - pydantic field preservation, `.create`
  discipline, no-dataclass sweeps) is stated explicitly in the crate's port
  notes, never silent.
- CLEAN RUST over Python parity. We do NOT need byte-compatibility with the
  Python engine, nor to read its on-disk artifacts (the learned-stats SQLite can
  be rebuilt). When faithful-to-Python and clean-Rust conflict, pick clean Rust;
  port BEHAVIOR, express it the natural Rust way.
- Python is FROZEN during the rewrite (stable reference; no new features).

## Branch, build, gates

- Branch: `rewrite-python-to-rust` (in this repo; `fedqrs` has a matching
  branch but has not been merged in yet).
- Toolchain: cargo 1.96.1 at `$HOME/.cargo/bin` (HOME=/tmp). clippy + rustfmt
  installed.
- Build env: `export CARGO_TARGET_DIR=/workspace/federated-query/target`. Deps
  come from crates.io.
- Before every commit run: `cargo fmt --all && cargo clippy --workspace
  --all-targets && cargo test --workspace`. A `cargo fmt` pre-commit hook is
  configured in `.claude/settings.json` (PreToolUse/Bash, gated on the command
  containing "git commit").
- Lint policy: `warnings = "deny"` ESCALATES clippy pedantic to errors. A
  curated allow-list lives in `Cargo.toml [workspace.lints.clippy]` (each entry
  justified): struct_excessive_bools, missing_errors_doc, doc_markdown,
  must_use_candidate, similar_names, missing_panics_doc, cast_precision_loss,
  cast_possible_truncation, implicit_hasher (per-fn), too_many_lines (per-fn).
  `clippy.toml` sets too-many-arguments-threshold=10.
- Test env: Postgres 17.4 is running on :5432 (binaries at
  `federated-query/postgres-17`, trust auth; started via
  `scripts/run-postgres.sh`). DuckDB fixtures live under `benchmarks/*/data/`.

## Crate DAG (target)

```
fq-parse -> fq-plan -> fq-common
fq-bind -> fq-plan, fq-catalog
fq-decorrelate -> fq-plan
fq-optimize -> fq-plan, fq-catalog, fq-connectors
fq-physical -> fq-plan, fq-optimize, fq-emit
fq-emit -> fq-plan
fq-exec -> fq-plan, fq-connectors
fq-runtime -> everything above
fedq, fedq-py -> fq-runtime
```

Key layering decision made during the port: the `DataSource` trait is the
CATALOG/STATISTICS tier only (metadata, statistics, capabilities,
map_native_type, render_dialect - no Arrow, no driver deps) and it lives in
fq-catalog; the concrete connectors implement it in fq-connectors. This keeps
fq-catalog (and thus the binder) free of the duckdb/postgres driver deps. The
DATA-PLANE fetch tier (Arrow streaming, ship, ctid-parallel = today's
pyo3-coupled `fedqrs` connectors.rs) is a separate concern that moves in with
fq-exec, de-pyo3-ified.

## Status: 534 tests green, clippy pedantic (deny) + rustfmt clean

Done through **fq-physical INCLUDING step building (build-order step 6)**. The whole
planning-and-lowering pipeline is complete in Rust: SQL string -> parse -> bind ->
decorrelate -> optimize -> optimized logical plan -> PhysicalPlan (single-source
pushdown + dim shipping + join-algorithm lowering) -> executable Step list +
Fragments (the rust_ir port, no JSON). What remains is EXECUTION: fq-exec (move the
fedqrs DataFusion + DuckDB engine in, delete the JSON IR, consume the Step list),
fq-runtime, the CLI/py bindings, then the final delete of `federated_query/`.

Per-crate test counts: fq-common 13, fq-catalog 30, fq-plan 47, fq-connectors 15,
fq-parse 34, fq-bind 25, fq-decorrelate 57, fq-optimize 185, fq-emit 34,
fq-physical 91. Total 534.

How the later crates were built (fq-decorrelate onward): each large crate/milestone
went ANALYSIS-SPEC (a subagent reads the Python module(s) in full and writes a
node-precise, implementation-ready spec) -> CODING SUBAGENT (implements test-first
against the spec, must end with cargo test + clippy + fmt green) -> ADVERSARIAL
REVIEW AGENT(s) (correctness vs the Python + project-rule compliance) -> apply the
real findings -> commit. The specs lived as `SPEC-*.md` inside the crate during the
work and were DELETED before commit (regenerable from the Python). This pipeline
scaled to ~8k LOC (fq-optimize) across four committed milestones.

Done crates (in build order):

### fq-common (13 tests) - COMPLETE
`config.rs` (load_config + Config/DataSourceConfig/OptimizerConfig/
ExecutorConfig/CostConfig; serde `deny_unknown_fields` + `default` replaces the
pydantic StateModel loudness), `error.rs` (ConfigError + UnsupportedSqlError),
`logging.rs` (tracing), `types.rs` (`DataType` enum + `is_renderable` - put here
so fq-catalog needn't depend on fq-plan). The `DataType -> Arrow` object mapping
(`arrow_type_for`) is deferred to fq-exec where the arrow crate lives.

### fq-catalog (30 tests) - COMPLETE (except stats-catalog cross-crate consumers)
- `schema.rs`: owned Schema/Table/Column tree (NO Arc/Weak - the Python parent
  back-references were engine-dead, only test-used, so retired; the FQN
  qualifier is denormalized onto children).
- `catalog.rs`: registry + `load_metadata` + `require_renderable` +
  `resolve_table_columns(ds?, schema?, table)` (used by star expansion).
- `datasource.rs`: the `DataSource` trait + value types (ColumnMetadata/
  TableMetadata/ColumnStatistics/TableStatistics), DataSourceCapability,
  RenderDialect, StatValue, the default `map_native_type` + shared builders.
- `stats_catalog.rs`: `StatsCatalog` on rusqlite (bundled). Direct surface
  ported; its cross-crate consumer tests (`_persist_observations`, the
  CostModel/StatisticsCollector overlay) translate with fq-physical/fq-optimize.
- `error.rs`: `CatalogError`.

### fq-plan (24 tests) - COMPLETE (except EXPLAIN doc + arrow_type_for, deferred)
- `expr.rs`: `Expr` enum + the shared walkers (`children`, `map_children`,
  `column_refs`, `split_conjuncts/disjuncts`, `combine_and/or`,
  `contains_aggregate`, `split_where_having`, `aggregate_output_map`, `get_type`)
  - every walker an exhaustive `match` with no `_` arm (the compiler replaces the
  Python walker-exhaustiveness tests). `LiteralValue` enum replaces `Any`;
  `NullsOrder` replaces the NULLS FIRST/LAST strings. The visitor ABC retires.
- `logical.rs`: `LogicalPlan` enum, struct-per-node, `children()`/`schema()`
  (Projection `*`-expansion, SEMI/ANTI left-only). `Scan` boxed inside its
  variant.
- `physical.rs`: `PhysicalPlan` enum over 25 node structs + `children()`.
  Runtime/emit artifacts made clean (datasource_connection dropped, query_ast/
  lateral_sql held as String, seeded_schema as name+DataType pairs, private
  caches + group_observation + dynamic_filter_values deferred).
- Deferred (tasks, not written blind): `arrow_type_for` -> fq-exec; the EXPLAIN
  document builder (build_document/_PlanFormatter) -> fq-runtime/fq-emit (needs
  estimated_cost, expression rendering, a physical-plan producer, e2e tests).
- Added later, with their first consumer (fq-decorrelate): `LogicalPlan::
  try_map_children` (the fallible recurse-and-rebuild, the port of Python
  `transform_children`) and `LogicalPlan::direct_expressions` (exhaustive per-node
  `&Expr` accessor). Both were deferred in the stage-A notes; un-deferred here.

### fq-connectors (15 tests) - DuckDB + Postgres done
- `duckdb_source.rs`: DuckDbSource against a real embedded DuckDB (metadata via
  information_schema, row count via duckdb_tables(), one approx aggregate scan
  for per-column NDV/nulls/min/max). Connection behind a Mutex (Connection is
  Send not Sync; the trait is Send+Sync; reads are optimize-time).
- `postgres_source.rs`: PostgresSource via the sync `postgres` crate.
  information_schema metadata + the uuid->VARCHAR map_native_type override;
  reltuples row count + pg_stats decode (signed n_distinct, histogram min/max);
  the statless PROBE (exact aggregate scan <=256MB, else TABLESAMPLE);
  estimate_scan_rows via EXPLAIN (FORMAT JSON), ANALYZEd-only. Tests use a
  schema-per-test (dropped on Drop) and early-return if pg is unreachable.
- Remaining (deferred, lower priority): parquet + clickhouse metadata surfaces;
  the data-plane Arrow fetch (moves in with fq-exec).

### fq-parse (34 tests) - SUBSTANTIALLY COMPLETE
On polyglot-sql 0.5.15 (its `Expression` is a ~1000-variant enum with a
dedicated variant per known function, so conversion is an ALLOWLIST: supported
constructs convert, every other variant raises `ParseError::Unsupported`).
Structured as a `Converter` struct (`convert.rs`) holding the catalog + an
in-scope CTE registry (RefCell); `expr.rs` and `functions.rs` are `impl
Converter` methods. Recursive descent: query -> plan, expr -> Expr, a
subquery-bearing expr recurses back into query.

Handled: single/multi-table SELECT with the full clause pipeline (FROM -> WHERE
-> GROUP BY -> HAVING -> SELECT -> ORDER BY -> LIMIT/OFFSET), left-deep JOINs
(JoinKind -> JoinType + natural flag), catalog-driven star expansion (`*` /
`alias.*`, no star survives), derived tables (-> SubqueryScan), CTE references
(-> CteRef), non-recursive WITH (-> Cte), DISTINCT / DISTINCT ON, binary set
operations (UNION/INTERSECT/EXCEPT), VALUES; expression nodes: columns,
literals, comparison/logical/arithmetic/concat binary ops, NOT/negate, IS
NULL/IS NOT NULL, parens, Cast, CASE (simple form lowered to searched),
BETWEEN/NOT BETWEEN, IN list/subquery, EXISTS, scalar subquery, op ANY/ALL, the
five aggregates, and scalar functions (a typed set + the generic
`Expression::Function{name,args}` node, so any function round-trips by name).

Still raises (minor tail, always loud): WITH RECURSIVE, comma joins, positioned
TRIM, window functions (Select.windows / QUALIFY), APPLY/ASOF joins, non-numeric
non-string literals (Date/Time/etc), BETWEEN SYMMETRIC, some IS shapes.

Entry points: `fq_parse::parse_with_catalog(sql, &Catalog)` (real, expands
stars) and `fq_parse::parse(sql)` (empty catalog convenience; a star raises).

### fq-bind (25 tests) - COMPLETE (Milestone A: SQL -> bound plan)

Ports `parser/binder.py`. `Binder { catalog, scopes: Vec<Scope> }` where a Scope
is a stack of `(alias, Table)` plus in-scope CTEs. ONE resolver
(`resolve_in_scopes`: qualified -> nearest scope defining the table; bare ->
nearest scope defining the column; intra-scope ambiguity = error; no resolution =
`BindError`). This is where "an invalid query MUST raise" is enforced - a bogus
qualifier or typo'd column raises before any source is touched. `bind_expr` is a
FALLIBLE recursive rebuild (NOT the infallible `map_children`), `&mut self` so it
can bind a subquery expr against the still-pushed outer scopes (correlated refs
resolve). Post-bind every ColumnRef carries its relation qualifier + DataType.
Covers: base/derived tables (SubqueryScan + rename), CTEs (Cte+CteRef registry),
set-op arity checks, Values, Explain, subquery exprs, HAVING/ORDER-BY output-alias
resolution (overlay stack, checked before resolve_in_scopes) + positional
ordinals, scan column pruning to real catalog columns + star read-set expansion
(guard_no_star). Deferred (binds without it, noted): the aggregate-call hoist for
ORDER BY/HAVING calls absent from SELECT; WITH RECURSIVE. Tests use HAND-BUILT
catalogs. A review agent pass fixed a soundness hole (the output-alias overlay
leaked into subqueries, binding an invalid query - bind_subplan now clears it),
an unmodeled-CAST-target silent pass, and a rejected GROUP BY output-alias.

### fq-decorrelate (57 tests) - COMPLETE

Ports `optimizer/decorrelation.py` (2744) + `scope_validator.py`. The engine
ALWAYS decorrelates: `decorrelate(plan)` removes every subquery EXPRESSION, then
asserts none survived, then runs `validate_scope`. Modules: `helpers.rs`
(is_subquery_node/expression_has_subquery/collect_inner_aliases[TRANSITIVE]/
references_outer|inner/is_correlated/replace_column_refs/and_join|or_join),
`prepare.rs` (SubqueryPreparer: strip/expose[{prefix}_k]/assemble/widen_aggregate
[{prefix}_g]/hoist_having[{prefix}_h]/guard_scalar[SingleRowGuard placement]/
build_order/apply_pending_limit[GroupedLimit]), `boolean.rs` (EXISTS->SEMI,
NOT->ANTI, IN NULL-aware, ANY->SEMI/ALL->ANTI violation, De Morgan negation,
constant-EXISTS), `filter.rs` (apply_conjunct dispatch), `value.rs` (scalar->LEFT
join ON TRUE vs condition, COUNT->COALESCE, join_flag Union with NO star -
passthrough expanded to qualified cols, LEFT->INNER tighten, rewrite_projection/
sort), `disjunctive.rs` (OR -> domain-union SEMI collapse else flag Union),
`dependent.rs` (Neumann-Kemper: build_domain[DISTINCT d0..]/per-domain aggregate
[domain cols as group_by AND passthrough, value nk_value]/ROW_NUMBER top-k[nk_rank]
/LEFT join-back/lateral_scalar fallback; DomMap = insertion-ordered Vec NOT
HashMap), `scope.rs` (validate_scope, qualifier-level well-scoped guard). Only
remaining seam: subquery-in-join-condition (raises Unsupported). Review fixes:
validate_scope was missing (ported+wired); a correlated HAVING-aggregate hoist left
an unqualified/unexposed ref in the join condition (a latent silent-wrong bug in
Python too) -> now RAISES loudly.

### fq-optimize (185 tests) - COMPLETE

Ports optimizer/{cost,statistics,estimate_defaults,subplan_signature,join_graph,
rules,pushdown,factory,join_ordering,eager_aggregation,cte_union_filter}.py
(~8k LOC / 11 modules), in four committed milestones:

- **M1 estimation foundation** (`cost/{mod,scan,join,aggregate,selectivity,ordinal}
  .rs`, `statistics.rs`, `estimate_defaults.rs`, `subplan_signature.rs`,
  `join_graph.rs`): `CostModel` = exhaustive per-node cardinality estimate; join
  formula L*R*sel / capped-denom, SEMI/ANTI occupancy 1-e^-fanout; the full
  selectivity tree + shared ordinal scale (temporal parsing) + interval pairing;
  HONEST UNKNOWNS (Option rows propagate, gaps recorded in defaults_used, NO
  fabricated constants). `StatisticsCollector` = session per-column cache (absence
  cached) + learned overlay (fill-only) + planner-estimate cache. `subplan_signature`
  / `scan_predicate_template` are BYTE-CRITICAL (sha1 canonical strings verified vs
  the Python module so existing learned catalogs keep matching). Public surface the
  rules/join-ordering call: estimate / join_tree_cost / column_ndv /
  conjunct_selectivity / group_key_dimension / register_cte / reset_cte_registry.
- **M2 rule driver + pushdown** (`rules/{driver,predicate,projection,aggregate,
  order_by,limit,semi_join}.rs`, `pushdown.rs`): fixpoint driver (max 10 iters,
  structural-change detection, Explain unwrap/rewrap, validate_scope after any
  changing rule). `OptimizationRule::apply(&self, plan) -> Result<plan>` (never
  Option; change = PartialEq). PredicatePushdown (through projections, below joins
  split by side with OUTER-JOIN SAFETY, into set-op branches + scans, TRANSITIVE
  CONSTANTS, OR factoring), ProjectionPushdown (the one raise site: PruneNoRule;
  plus a hardening - a subquery Expr surviving into the optimizer now RAISES
  SubquerySurvived, never silently prunes), Aggregate/OrderBy/Limit/SemiJoin
  pushdown. Every rule DECLINES rather than produce an incorrect plan.
- **M3 join ordering** (`rules/join_ordering.rs`): region -> connected components
  (equi-edges only) -> left-deep Selinger DP over a u64 bitmask of connected
  subsets (GOO greedy above max_join_reorder_size), objective = C_out (inline
  running sum, NOT join_tree_cost) + TRANSFER_WEIGHT * transfer with
  subset-determined island state; deterministic tie-breaks; re-emit in pushdown
  normal form; PREDICATE-CONSERVATION guard RAISES if any region conjunct is not
  placed exactly once; stamps Scan.estimated_rows/column_ndv + Join.estimated_rows/
  estimate_defaults. build_optimizer now threads a CostModel (the first stateful
  rule) held behind Rc<RefCell> since the trait is &self.
- **M4 perf rules** (`rules/{eager_aggregation,cte_union_filter}.rs`): both PRESERVE
  results, decline on any miss. EagerAggregation (always-on, cost-gated) pushes a
  Yan-Larson partial SUM below an INNER plain-equi dim-join tree when the collapse
  ratio <= 0.5 and a peeled dim exceeds the ship budget. CTEUnionFilterPushdown ORs
  the consumer filters, translates them onto the shared body's grouping columns
  (constant tags per branch), inserts under the aggregate of every union branch.

Full rule order: CTEUnionFilter, Predicate, SemiJoin, EagerAgg, JoinOrdering,
Projection, Aggregate, OrderBy, Limit. The M3 and M4 reviews found NO result-
changing defects; M1 review fixed the temporal parser + cross-type literal
equality; M2 review produced the ProjectionPushdown raise-hardening.

Deferred with clear seams: `scan_planner_estimate` returns `Ok(None)` until fq-emit
exists (it needs SQL rendering to build the EXPLAIN probe); the physical orientation
helpers land with fq-physical. NOT in fq-optimize: `single_source_pushdown.py` +
`dim_shipping.py` are federated-EXECUTION features that belong to a later crate.

### fq-emit (34 tests) - COMPLETE (SQL-emission toolkit)

Ports `plan/emit/{expressions,clauses,resolver}.py` (~620 LOC) + the
`plan/physical.py::to_source_sql` transpile boundary. KEY DESIGN (validated
empirically before building, resolving the plan-vs-old-HANDOFF fork): the crate
renders fq-plan `Expr`+clauses to canonical **Postgres-form** SQL TEXT directly
(hand-built strings, NOT polyglot's AST/generator), then `to_source_sql(pg_sql,
dialect)` transpiles via `polyglot_sql::transpile(sql, PostgreSQL, target)` -
exactly as the Python engine reused sqlglot's transpiler. Proven: polyglot
reproduces STRING_AGG->LISTAGG, TABLESAMPLE(10)->(10 PERCENT), PERCENTILE_CONT
WITHIN GROUP->QUANTILE_CONT for DuckDB, and PostgreSQL->PostgreSQL is identity. So
NO hand-rolled dialect-divergence table; polyglot owns the divergences. Because
transpile RE-PARSES our canonical text, every emitted string must be parseable
Postgres - the load-bearing invariant, tested by round-tripping representative
shapes through `to_source_sql(.., Postgres/DuckDb)`.

Modules: `dialect.rs` (Dialect enum {Postgres,DuckDb,ClickHouse,DataFusion} ->
polyglot DialectType; `to_source_sql` requires exactly one statement, surfaces
every transpile failure as `EmitError::Transpile`), `ident.rs` (quote_ident),
`resolver.rs` (ColumnResolver trait returning TEXT; SourceResolver = quoted
qualified / star; MergeResolver = physical-name via alias map, RAISES
`ColumnResolution` on a qualified ref the relation does not expose, bare name for
unqualified), `expr.rs` (render_expr: exhaustive match, every BinaryOp/UnaryOp/
InList/Between fully parenthesized [load-bearing - AND/OR terms are string-joined
downstream], the four subquery variants RAISE `SubqueryReachedEmit`, the shared
ordered-key/NULLS rule [LAST for ASC, FIRST for DESC, explicit override] used by
both ORDER BY and Window), `clauses.rs` (select_list, order_by, group_by/grouping
sets, set_op_keyword, assemble_select via a `SelectPieces` struct). Error variants
for closed-enum "unmapped operator/literal-type" are RETIRED (compiler is the
guard, no dead runtime variant). Review-fixed defensive holes: NEGATE of a
sign-leading operand (negative literal / nested negate) now parenthesizes so the
`-` tokens never fuse into `--` (a SQL comment) = unparseable text; a non-finite
float literal (NaN/Infinity) RAISES `UnrepresentableLiteral` instead of shipping
`NaN`/`inf`.

Deferred with clear seams (stated in the module docs): the fedqrs
`core/src/sql.rs` `ScanSpec`/DataFusion-`Expr` runtime dynamic-filter renderer
moves in with fq-exec (de-DataFusion-ified to reuse this crate's emitter); the
per-physical-node query assembly (`PhysicalRemoteQuery._build_query`, single-source
pushdown / injected / lateral island SQL, FROM/JOIN rendering) belongs to
fq-physical, which will COMPOSE these clause builders; the EXPLAIN document builder
stays with fq-runtime. `fq_optimize::statistics::scan_planner_estimate` stays
blocked until fq-physical produces a rendered scan to EXPLAIN.

### fq-physical (64 tests) - COMPLETE (LogicalPlan -> PhysicalPlan)

Ports `optimizer/physical_planner.py` + `single_source_pushdown.py` +
`dim_shipping.py` (the PhysicalPlan PRODUCER). Built in four milestones, each an
analysis-spec -> coding subagent -> commit, then three parallel adversarial reviews
(single-source SQL / planner+schema / dim-shipping gates) -> one fix.

- **M0 prerequisites** (5a1de0f): fq-plan `PhysicalLateralJoin.correlations` retyped
  to carry the `BinaryOpType` (the operator is load-bearing for the domain-filter
  reduction; the old `DataType` slot was wrong); `GroupObservation` + an
  `Option<GroupObservation>` field on `PhysicalHashAggregate`/`PhysicalRemoteQuery`;
  `has_window_output` + the `contains_window` walker. fq-optimize `cost::group_subject`
  made pub (same learned-stats subject both producers stamp and the cost model reads).
  fq-catalog `datasource_names()`.
- **M1 planner core** (49cd4bf): the `PhysicalPlanner` (cost_model behind
  `Rc<RefCell>`), per-node lowering, join-algorithm selection + key orientation
  (alias-then-name, RAISES on unorientable keys) + build-side (smaller side built) +
  dynamic-filter marking on the PROBE, cross-source LATERAL, CTE producer registry,
  scan estimate + coordinator-aggregate group_observation stamp. single_source and
  dim_shipping were STUBS here.
- **M2 single-source pushdown** (be8be98): `SingleSourcePushdown::try_build` absorbs a
  one-source renderable subtree into one `PhysicalRemoteQuery`. The AST-vs-text gap
  (Python accumulated sqlglot AST + rewrote it; fq-emit renders text) is resolved by a
  two-level accumulator - `Vec<Expr>`+`Vec<String>` for the SELECT list / DISTINCT-ON
  keys (the only pieces later rewrites touch), eagerly-rendered `String` for the rest.
  `relation_sql.rs` holds the FROM/JOIN/EXISTS/WITH text builders (SEMI/ANTI ->
  `WHERE [NOT] EXISTS(...)` built explicitly since sqlglot did it for free). Decline =
  `Ok(None)`; a subquery Expr reaching emit propagates `EmitError` loudly.
- **M3 dim shipping + typed schema** (c08018d): typed `PhysicalPlan::schema() ->
  Vec<(String, DataType)>` in fq-plan (the deferred prerequisite; clean-Rust - a bound
  ColumnRef carries its DataType so `Expr::get_type` needs no Arrow/kernels/column_
  aliases; source-typed nodes panic loudly rather than guess). `DimShipping::try_ship`:
  9 gates in order (kill switch, cost-model, shippable shape, plain aggregate,
  dimension explosion with measured-group override, local source, ship target, foreign
  scans, cost gate) -> build (synthetic scans -> replace -> single_source.try_build ->
  fallback-seed -> outputs_match/has_window -> wrap_shipments). Every gate miss
  DECLINES; estimate/group_key_dimension/plan_without_shipping/group_count errors
  PROPAGATE (never laundered into a decline). Thresholds ported verbatim (floor 100k,
  budget 200k, ratio 20, high-card NDV 10k, collapse fraction 0.1).
- **Review fix** (a13236b): `HashAggregate::schema()` now types aggregate outputs by
  the ARGUMENT-aware rule (`_infer_aggregate_type`: COUNT->BIGINT, MIN/MAX preserve arg
  type, AVG->DOUBLE, SUM widens integer keeps float/decimal) instead of name-only
  `get_type`, which had mis-declared SUM(float)->BIGINT and MIN/MAX->VARCHAR = a wrong
  seeded island schema. Other review findings were faithful-to-Python + unreachable
  through the current pipeline (documented, not changed): recursive-CTE Values-anchor
  column aliasing (reachable only once WITH RECURSIVE parses, which still raises);
  SEMI/ANTI over a filtered base-scan right (decorrelation wraps it in a SubqueryScan;
  the raw shape errors loudly at the source, never silent-wrong); RIGHT-JOIN
  non-preserved filter.

- **M4 step building** (f5aa7ca M4b + 6f1df53 M4c + ba6be89 M4d review fixes):
  ports `executor/rust_ir.py` (PhysicalPlan -> ordered `Step` list + named `Fragment`s,
  NO JSON). Two clean-Rust decisions: (a) IrExpr is ELIMINATED (plan section 5) - the
  `Step`/`Fragment`/`ScanSpec` plain structs (steps/types.rs, no serde, shapes from
  fedqrs core/src/ir.rs) hold `fq_plan::Expr` directly, retiring the whole `_serialize_*`
  layer; fragment col refs retagged via a `ColumnRef.table` rewrite; the Expr->DataFusion
  lowering moves to fq-exec. (b) step building lives in fq-physical (plan 3.10 assigns
  rust_ir semantics here; keeps the crate DataFusion-free), so fq-exec will CONSUME
  `build_steps(plan) -> Vec<Step>`, not build it. Modules steps/{types,expr_retag,
  scan_spec,render_sql,emit_nodes,cse,reduction,observe}.rs. Prerequisite M4a (00ee3bb):
  ported physical `column_aliases()` onto PhysicalPlan (23 node methods) +
  physical_column_name + contains_grouping. CORRECTNESS-CRITICAL bits reviewed: CSE share
  key canonicalizes ScanSpec.filter via fq-emit `render_canonical` (Debug/pointer would
  merge distinct scans = wrong rows); node identity = raw pointer of `&PhysicalPlan`
  (non-mutating walk); the reduced join STILL emits the coordinator HashJoin (injection is
  only a superset filter). Review fixed: RemoteSetOp had no renderer (same-source
  `A UNION ALL B` errored) -> added render_remote_set_op; silent alias-miss fallbacks ->
  raise (added `Expr::try_map_children`); self-join uniquify iterated sorted BTreeMap ->
  schema order. Deferred (result-correct): the `injected_sql` source-side key placement
  (RemoteQuery holds rendered SQL not an AST to splice; engine wraps the base with the IN
  filter) and window-over-GROUPING() split (raises loudly). build_steps needs no
  catalog/cost/stats - all stats pre-stamped on nodes; the sole external fact ("is source
  Postgres?" for the parallel gate) is a `datasource_kind` field stamped on PhysicalScan.

Test level: the crate is tested at the PhysicalPlan-TREE and STEP-LIST level (assert on
the produced tree / `PhysicalRemoteQuery.sql` / the `Vec<Step>` after
parse->bind->decorrelate->optimize->plan->build_steps), NOT via execution/EXPLAIN - the
Python e2e_pushdown suites assert through the runtime (EXPLAIN FORMAT JSON), which needs
fq-runtime + the EXPLAIN builder (later crates).

`scan_planner_estimate` stays blocked until the runtime can render a scan to EXPLAIN.

## NEXT: fq-exec (the execution engine; IR deletion) - MILESTONE C

Absorb today's `fedqrs/src/engine.rs` + `core/` MINUS the IR: delete `core/src/ir.rs`
and the serde/JSON entry point; `execute(plan: &PhysicalPlan)` runs the pipeline, and
the `Step` list is built by `fq_physical::build_steps` (ALREADY DONE - it is in
fq-physical, not here). fq-exec CONSUMES that `Vec<Step>` + fragments and adds the
`fq_plan::Expr -> DataFusion Expr` conversion (replacing `core/src/expr.rs`). Everything
else stays as-is (already Rust, already tallied):
lazy fragment fusion into DataFusion regions, the FairSpillPool + tracked collection,
binding spill, SMJ retry, reductions (inline-IN / temp-table / parquet delivery), ship
execution, prefetch pools, ctid-parallel reads, aggregate metric harvest, per-step
observations, profile output. Also absorbs the fedqrs `core/src/sql.rs`
ScanSpec/DataFusion-Expr runtime dynamic-filter renderer (de-DataFusion-ified to reuse
fq-emit). `expression_evaluator.py` call sites: each is either DataFusion-covered
(retire) or a small kernel here. `fedqrs` has a matching `rewrite-python-to-rust` branch
NOT yet merged into this workspace - the fq-exec step likely starts by importing it
(git subtree per plan section 1).

After fq-exec: fq-runtime + fedq + fedq-py, then the DELETE of `federated_query/` at the
final gate (tallies vs pure-DuckDB truth + the full SQL corpus).

## Commit log (rewrite so far, newest first)

```
ba6be89 fq-physical M4d - step-building review fixes (RemoteSetOp render, loud alias miss)
6f1df53 fq-physical M4c - semi-join reduction + observations
f5aa7ca fq-physical M4b - step building (PhysicalPlan -> Step list + Fragments)
00ee3bb fq-physical M4a - physical column_aliases() prerequisite
a13236b fq-physical review fix - argument-aware aggregate output types
c08018d fq-physical M3 - dim shipping + typed physical schema()
be8be98 fq-physical M2 - single-source pushdown (subtree -> one remote query)
49cd4bf fq-physical M1 - physical planner core (LogicalPlan -> PhysicalPlan)
5a1de0f fq-physical M0 - cross-crate prerequisites + crate scaffold
6e6a562 docs: record fq-emit commit hash in HANDOFF log
9138c75 fq-emit - canonical Postgres emitter + polyglot transpile boundary
ae91273 fq-optimize M4 - eager aggregation + CTE union filter pushdown
caf0ca8 fq-optimize M3 - cost-based join ordering (Selinger DP + GOO + locality)
6f45e1a fq-optimize M2 - fixpoint rule driver + pushdown rules
82e9616 fq-optimize M1 - cost model + statistics estimation foundation
e0f7af4 fq-decorrelate - decorrelation pass (EXISTS/IN/scalar/quantified, N-K dep join)
b096eb4 fix fq-parse + fq-bind review findings
1fbf7e2 fq-bind - derived tables, CTEs, set ops, subqueries, aliases (Milestone A)
ca1bfda fq-bind core (scope chain + resolver, base-table queries)
5500d1f docs: rewrite HANDOFF.md for the rewrite (old -> oldhandoff.md)
0a589fd fq-parse scalar functions (typed + generic Function node)
9a8cbf4 fq-parse CTEs (WITH -> Cte + CteRef)
00f613a fq-parse expression nodes, subqueries, set ops, derived tables
7272031 fq-parse star expansion (catalog-driven)
5cbefe0 fq-parse joins (left-deep fold, per-table column partition)
529e4f5 fq-parse structural core (single-table SELECT pipeline)
d38f862 Postgres connector (metadata, pg_stats, probe, EXPLAIN estimate)
9b4a98e fq-connectors + DuckDB connector (catalog/statistics tier)
b9b5bec fq-catalog DataSource trait + load_metadata (connector abstraction)
30a66c1 fq-plan stages B+C (physical enum, is_renderable, where/having split)
77e6847 scaffold workspace + fq-common, fq-catalog, fq-plan (stage A)
```
