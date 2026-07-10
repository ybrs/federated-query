# Rewrite: everything Python -> Rust

The COMPLETE conversion of the Python side (30,183 lines under
`federated_query/`) into the Rust engine (`/workspace/fedqrs`). Scope is
CONVERSION ONLY: behavior-identical, no new features, no caching/accelerator
work, no redesigns beyond what the language change itself forces. The
assessment that motivated this (measurements, the polyglot transpiler spike:
121/121 benchmark queries parse, 25/25 emitted renders execute identically)
is in `rust-rewrite-plan.md`; THIS document is the execution plan.

End state: one Rust engine that takes SQL text and a source configuration
and returns Arrow - parsing, binding, decorrelation, optimization, physical
planning, SQL emission, and execution all in-process, no JSON IR boundary,
no GIL on the query path. Python remains only as a thin pyo3 binding
(`import fedqrs; runtime.execute(sql) -> pyarrow.Table`) so existing
callers, tests and benchmark harnesses keep working unchanged.

---

## 1. Ground rules

1. BEHAVIOR-IDENTICAL. Every stage ports against a DIFFERENTIAL GATE: the
   Python implementation stays in-tree as the reference oracle until its
   Rust replacement produces identical stage output on the full corpus
   (121 benchmark queries x placements + targeted unit fixtures). The
   language-agnostic result tallies (99|0|0 at three TPC-DS scales +
   adversarial, TPC-H 22/22, cold/warm columns) are the final arbiter and
   never regress at any point of the migration.
2. NO new features. A behavior difference is a bug in the port, never an
   improvement to keep. Improvements found during porting are NOTED for
   after the migration.
3. The house invariants carry over with their enforcement STRENGTHENED by
   the type system (section 4).
4. Python is retired stage by stage, right-to-left along the pipeline; at
   every intermediate point the ENGINE SHIPS and all tallies pass.

## 2. Inventory: every module, its size, its Rust destination

Pipeline layers (the port order works right-to-left through these):

| Python module | lines | contents | Rust destination |
| --- | --- | --- | --- |
| executor/rust_ir.py | 2565 | 164 fns: physical plan -> IR JSON, CSE, reduction orientation/injection tracing, observation provenance | DISSOLVES into fedqrs planning (no IR boundary; the emit logic becomes direct step construction) |
| plan/physical.py | 3031 | 30 classes: PhysicalPlanNode tree, schema memoization, EXPLAIN formatting, dynamic-filter EXPLAIN values | fedqrs-plan::physical (structs + enum) |
| optimizer/physical_planner.py | 1198 | PhysicalPlanner: logical->physical lowering, scan annotation, join-side annotation | fedqrs-plan::lower |
| optimizer/single_source_pushdown.py | 1201 | collapse single-source subtrees into one remote query; SQL rendering context; correlated render | fedqrs-plan::pushdown + fedqrs-sql emission |
| optimizer/dim_shipping.py | 445 | DimShipping gates + island build + group-observation stamp | fedqrs-plan::shipping |
| plan/emit/* (expressions/clauses/resolver) | ~620 | canonical Postgres-form SQL assembly via sqlglot AST | fedqrs-sql: DIRECT per-dialect emission from plan nodes (replaces emit + to_source_sql transpile round trip; polyglot validates in the gate) |
| executor/executor.py, profiling.py, plan_cache.py | 361 | execute-to-table glue, stage timers, plan cache | fedqrs::runtime (plan cache keyed identically; TTL/LRU) |
| optimizer/rules.py | 1818 | RuleBasedOptimizer + Predicate/Projection/Limit/OrderBy/Aggregate/SemiJoin pushdown rules | fedqrs-optimize::rules |
| optimizer/join_ordering.py | 1133 | join-region DP/greedy enumerator + emission | fedqrs-optimize::join_order |
| optimizer/join_graph.py | 229 | region extraction (atoms + conjunct classification) | fedqrs-optimize::join_graph |
| optimizer/cte_union_filter.py | 326 | CTE union-filter pushdown rule | fedqrs-optimize::cte_union_filter |
| optimizer/eager_aggregation.py | 591 | partial-aggregate split rule | fedqrs-optimize::eager_agg |
| optimizer/cost.py | 1156 | CostModel: tracked estimates, honest unknowns/bounds, selectivities, CTE registry | fedqrs-optimize::cost |
| optimizer/estimate_defaults.py | 182 | CardinalityEstimate + combination helpers | fedqrs-optimize::estimate |
| optimizer/statistics.py | 238 | StatisticsCollector: session cache, learned overlay, planner-estimate cache | fedqrs-optimize::stats (connectors already in Rust) |
| optimizer/subplan_signature.py | 193 | alias/constant-neutral subplan + predicate signatures | fedqrs-optimize::signature |
| optimizer/pushdown.py | 206 | shared pushdown helpers (available_columns, bare_names) | fedqrs-optimize::util |
| optimizer/scope_validator.py | 176 | post-stage guard: every ColumnRef qualified | compile-time (section 4) + a debug-assert walker |
| optimizer/factory.py | 79 | the one rule-stack assembly | fedqrs-optimize::factory |
| optimizer/decorrelation.py | 2744 | 16 classes: N-K dependent-join decorrelation, disjunctive rewrite, laterals, single-row guards, grouped limits | fedqrs-decorrelate |
| parser/parser.py | 2275 | sqlglot AST -> LogicalPlanNode (one class, hundreds of converters) | fedqrs-parse (polyglot AST -> logical) |
| parser/binder.py | 1633 | name resolution, type stamping, star read-set expansion, aggregate-output hoists, positional ORDER BY | fedqrs-parse::bind |
| parser/dialect.py, errors.py | 102 | FedQPostgres dialect tweaks, BindingError | fedqrs-parse |
| processor/query_preprocessor.py | 937 | star expansion + column rename bookkeeping (sqlglot AST rewriting) | folded into fedqrs-parse::expand (same phase order, same rename contract for result columns) |
| processor/query_executor.py, query_context.py | 248 | pipeline orchestration + processor hooks | fedqrs::runtime::pipeline |
| plan/logical.py | 1026 | 23 classes: logical nodes | fedqrs-plan::logical (enum + structs) |
| plan/expressions.py | 1107 | 23 classes: expression nodes + THE shared walkers | fedqrs-plan::expr (enum; walkers become exhaustive matches) |
| plan/field_introspection.py, arrow_types.py | 148 | field preservation helpers, Arrow type mapping | fedqrs-plan (arrow types exist in engine) |
| catalog/catalog.py, schema.py | 280 | Catalog / Schema / Table / Column metadata | fedqrs::catalog; metadata FETCH moves into connectors (pg information_schema, duckdb_tables) |
| catalog/stats_catalog.py | 330 | learned-stats SQLite store | fedqrs::stats_catalog via rusqlite - SAME file format, drop-in compatible |
| datasources/base.py | 467 | DataSource ABC, TableStatistics/ColumnStatistics, capabilities | fedqrs-core::types + connectors (fetch/estimate_scan_rows/probe already exist in Rust for pg/duck/parquet) |
| datasources/postgresql.py | 849 | pg connector: metadata, stats, PROBE, EXPLAIN estimates, uuid decode, streaming | merge into fedqrs::connectors (fetch paths already there; port metadata/stats/probe/EXPLAIN pieces) |
| datasources/duckdb.py | 218 | duck connector: catalog stats, approx column scan, EXPLAIN estimates | merge into fedqrs::connectors |
| datasources/clickhouse.py | 215 | metadata-only python connector; engine registration RAISES today | KEEP PARITY: port the same metadata surface + the same loud refusal; a real clickhouse DsKind is future work, not this migration |
| datasources/parquet.py | 49 | parquet dir source | already in fedqrs::connectors |
| executor/expression_evaluator.py | 537 | coordinator-side pyarrow expression evaluation | VERIFY remaining call sites; where DataFusion evaluates the same expression the module retires, else ports as a small kernel set |
| config/config.py | 246 | YAML config models | fedqrs::config (serde) |
| cli/fedq.py | 599 | FedQRuntime assembly + CLI | fedqrs::runtime + a Rust `fedq` binary; a thin python FedQRuntime wrapper remains for API compatibility |
| model.py | 60 | StateModel (pydantic base) | superseded by the type mapping (section 4) |
| utils/logging.py | 151 | logging setup | tracing crate |

Out of scope entirely: benchmarks/ (harnesses stay Python - they drive the
public API), tests/ (strategy in section 6), lint/ (replaced by Rust
equivalents, section 4).

## 3. Target crate layout

Extend the existing fedqrs workspace (fedqrs-core already holds ir/expr/
sql/types):

```
fedqrs/
  core/            existing: DsKind, IrExpr, dialect SQL helpers
  plan/            logical + physical node model, lowering, pushdown,
                   shipping (fedqrs-plan)
  parse/           polyglot AST -> logical, star expansion, binder
                   (fedqrs-parse; depends on polyglot-sql)
  decorrelate/     the dependent-join machinery (fedqrs-decorrelate)
  optimize/        rules, join ordering, cost, stats, signatures
                   (fedqrs-optimize)
  sql/             per-dialect SQL EMISSION from plan nodes (fedqrs-sql;
                   grows out of core::sql; replaces plan/emit + transpile)
  src/             engine (execution), connectors, runtime (pipeline,
                   plan cache, catalog, stats catalog), pyo3 bindings
```

Dependency spine: parse -> plan <- optimize <- decorrelate; sql depends on
plan; runtime orchestrates all; NOTHING depends on pyo3 except the binding
layer (the engine must build and test GIL-free, `cargo test` end to end).

## 4. Type-system and invariant mapping

The Python house rules exist to make mistakes LOUD. Rust makes most of them
structural:

| Python invariant | Rust form |
| --- | --- |
| No silent fails; raise on unexpected case | `Result<T, PlanError>` everywhere; `match` with NO `_ =>` arm on plan/expression enums - adding a node variant breaks every walker at COMPILE time (the walker-descent lesson, enforced by the compiler instead of five perf bugs) |
| StateModel extra=forbid / model_copy rejects unknown keys | struct literals + `..prev` update syntax are compile-checked; serde `deny_unknown_fields` on every boundary type |
| .create with defended comments vs model_copy | constructors are ordinary `new`/struct literals; the copy-vs-fresh distinction disappears (moves/borrows make sharing explicit) |
| Columns must be qualified after bind | TWO TYPES: `RawColumn { table: Option<String>, column }` exists only in parse; binding produces `BoundColumn { relation: RelId, column }` - an unqualified reference after bind is UNREPRESENTABLE. scope_validator.py retires; a debug_assert walker remains for the parse layer |
| No star column lists | `Scan.columns: Vec<ColumnName>` built from catalog expansion; `*` never constructs |
| Invalid query must raise at bind | `BindError` variants (unknown table/column/ambiguous/etc.), returned not panicked; pyo3 maps them to the SAME Python exception types the tests pin |
| Expression walkers cover all node types | one `Expr` enum; every traversal is an exhaustive match |
| No dataclasses / mutable-by-default | plain structs, mutable by default (no interior-immutability ceremony) |
| ASCII only, comments on every fn | unchanged; clippy.toml + a small xtask lint replicating the ascii check |

Node model decisions (made once, up front):
- `LogicalPlan` and `PhysicalPlan` are ENUMS of structs (not trait objects):
  exhaustive matching is the point. Children are `Box<LogicalPlan>` /
  `Vec<LogicalPlan>`; shared subtrees (CTE bodies) use `Arc` where Python
  relied on object identity (`id(node)` caches in rust_ir/dim_shipping port
  to explicit ids stamped at construction).
- Schema memoization (the q59 lesson) becomes a `OnceCell<Vec<FieldName>>`
  per node - same invariant, structural fields never mutate after build.
- Estimates: `CardinalityEstimate { rows: Option<u64>, gaps: Vec<Gap> }`,
  same honest-unknowns semantics (None = unknown, gap-fed = upper bound).

## 5. Pipeline phases in Rust (what each stage must reproduce)

1. EXPAND (from query_preprocessor.py): star expansion against the catalog,
   producing the rewritten SQL and the internal->visible rename map the
   result layer applies at the end. Operates on the polyglot AST.
2. PARSE (parser.py): polyglot AST -> LogicalPlan. The 2275-line converter
   inventory ports 1:1 - every `_convert_*` (select/join/subquery/case/
   in/exists/window/interval/extract/values/set-ops/CTEs/positional ORDER
   BY/window-rejection scoping/simple-CASE lowering) becomes a match arm
   over polyglot nodes. FedQPostgres dialect tweaks (parser/dialect.py)
   become parse options.
3. BIND (binder.py): catalog resolution, type stamping, star read-set
   expansion, case-insensitive qualifiers, aggregate-output hoists
   (_hoist_aggregate_calls), ORDER BY output matching. Produces the
   BoundColumn representation (section 4).
4. DECORRELATE (decorrelation.py): the 16-class machinery - domain
   derivation, dependent-join construction, N-K rewrite, disjunctive
   existential split, scalar guards (SingleRowGuard), grouped limits,
   lateral fallback, DISTINCT peeling. Ports LAST of the logical stages
   (most subtle; largest fixture corpus).
5. OPTIMIZE (rules.py + the rule modules): the fixpoint driver and each
   rule as an object implementing `apply(&LogicalPlan) -> Option<LogicalPlan>`
   in the EXACT factory order (factory.py). Join ordering ports with its
   enumerator determinism intact (sorted subsets, lexicographic ties).
6. PHYSICAL (physical_planner.py + single_source_pushdown.py +
   dim_shipping.py): lowering, single-source collapse, ship construction,
   scan/join-side annotation.
7. EMIT (fedqrs-sql): render remote SQL PER DIALECT directly from plan
   nodes, replacing both plan/emit (canonical postgres assembly) and
   to_source_sql (sqlglot transpile). The differential gate for this stage
   is EXECUTION-based (run both renderings, compare results) exactly like
   the polyglot spike, plus polyglot-transpile as a cross-check oracle.
8. EXECUTE: rust_ir.py's semantics (CSE share keys, reduction orientation,
   injection tracing and winners, delivery strategy, observation
   provenance) become direct construction of engine steps - the JSON
   serialize/parse round trip disappears. The engine's execution layer
   (already Rust) is untouched.
9. RUNTIME: pipeline orchestration, plan cache (same keys/TTL semantics),
   learned-stats catalog via rusqlite AGAINST THE SAME SQLITE FILES,
   catalog metadata loading via connectors, config from the same YAML,
   EXPLAIN (text + JSON, same document shape - tests pin it).

## 6. Port order, gates, and the differential harness

Order = right-to-left pipeline cuts; each phase has a serialized CONTRACT
at its input boundary, produced by Python and consumed by Rust until the
next phase lands:

- P0 TOOLING (1-2 weeks): serde Serialize/Deserialize for the FULL logical
  and physical node models on the Rust side; a `to_plan_json()` test helper
  on the Python side (migration tooling, not a feature); the differential
  runner: for each corpus query, run Python stage vs Rust stage, diff
  canonical JSON; wire into CI alongside the tallies.
- P1 PHYSICAL + EMIT + EXECUTE-CONSTRUCTION (the largest single phase,
  ~8-9k dense lines: physical.py, physical_planner.py,
  single_source_pushdown.py, dim_shipping.py, plan/emit, rust_ir.py).
  Input contract: optimized logical plan JSON. Gates: (a) plan-shape diff
  is NOT required here (physical output feeds execution directly);
  (b) EXECUTION equivalence per query - identical results and identical
  emitted-SQL result sets against real sources; (c) full tallies + suite.
  Exit: python physical layer deleted; python optimizer emits logical JSON.
- P2 OPTIMIZE (~5.5k lines: rules, join_ordering, join_graph, cost, stats,
  signature, cte_union_filter, eager_aggregation, factory). Input contract:
  decorrelated plan JSON. Gate: OPTIMIZED-PLAN JSON diff (canonicalized)
  on the corpus + tallies. Stats round trips move to Rust connectors here
  (they already implement fetch/probe/EXPLAIN estimates).
- P3 DECORRELATE (~2.7k lines). Input contract: bound plan JSON. Gate:
  decorrelated-plan diff + the dedicated decorrelation fixture corpus
  (harvest: every plan the full Python suite ever decorrelates, dumped once
  as fixtures).
- P4 PARSE + BIND + EXPAND (~4.9k lines). Input contract: SQL text. Gate:
  bound-plan diff on the corpus + every parser/binder unit fixture
  (including the error cases: invalid queries must raise the SAME
  exception classes through pyo3).
- P5 RUNTIME + RETIREMENT: pipeline/plan-cache/catalog/config/CLI in Rust;
  python package shrinks to the pyo3 wrapper; delete the ported python
  modules and their now-redundant unit tests; the differential runner
  retires; the tallies and e2e suites (driving the public API) remain.

Test strategy per phase: (a) the tallies always run; (b) e2e_pushdown and
runtime-level Python tests keep running UNCHANGED throughout (they drive
the public API); (c) unit tests of ported internals are translated to Rust
`#[cfg(test)]` alongside their module - translation priority goes to tests
that pin BEHAVIOR (guards, error cases, determinism) over tests that pin
Python mechanics (pydantic field preservation etc., which the type system
now covers); (d) each phase's fixture corpus (stage-output JSON for all 121
queries x 2 placements) is committed so regressions diff readably.

## 7. Sizing and sequence summary

| Phase | scope | est. effort |
| --- | --- | --- |
| P0 | serde models + differential runner | 1-2 weeks |
| P1 | physical + emission + step construction | 4-6 weeks |
| P2 | optimizer + cost + stats | 3-5 weeks |
| P3 | decorrelation | 2-4 weeks |
| P4 | parse + bind + expand (polyglot) | 3-4 weeks |
| P5 | runtime + retirement | 1-2 weeks |

Single-stream estimate: ~3.5-5.5 months. Every phase ends with all tallies
green and the engine shippable; the migration can PAUSE indefinitely at any
phase boundary without debt beyond the remaining Python.

## 8. Risks

- polyglot maturity (0.5.x): mitigated by the spike results, per-release
  differential gates, and the fact that EMISSION does not depend on it
  (fedqrs-sql renders directly; polyglot is the input parser + a
  cross-check oracle). Fallback if a parse gap appears: sqlparser-rs for
  the affected construct, or contribute the fix upstream.
- Decorrelation subtlety: highest-risk port; owns the largest fixture
  corpus and goes second-to-last, when the differential tooling is mature.
- Velocity freeze: no new engine features land during a phase's port
  window in that phase's layer. Layers not yet under port accept changes,
  which then port with their layer (the corpus re-snapshots).
- Determinism drift (hash orders, float formatting in emitted SQL): the
  canonical-JSON differ normalizes; emitted SQL is compared by EXECUTION
  result, not text.
- The pyo3 exception contract: error types must map to the exact Python
  exception classes existing tests assert on; defined once in P0.
