# Federated Query Engine - Architecture

This document traces the complete path a SQL query string takes through the
engine, from text to returned Arrow rows. It names every class and method that
participates and the semantic role of each stage. It is derived from the code in
`federated_query/`; file paths and `file.py:line` citations point at the
definitions described.

All examples and diagrams use plain ASCII. Arrows are written `->`.

---

## 1. Overview

The engine runs a single SQL query across multiple heterogeneous data sources
(PostgreSQL, DuckDB, ClickHouse) and returns one Arrow table. The driving idea
is "push down as much as possible, merge the rest locally":

- SINGLE-SOURCE query: every table the query touches lives in one datasource.
  The whole (sub)plan is rendered back into one SQL statement and executed on
  that source in a single round trip. The engine does almost no local work; it
  just streams the source's Arrow result back to the caller. This is implemented
  by `SingleSourcePushdown` (`optimizer/single_source_pushdown.py`) which
  produces a `PhysicalRemoteQuery` (`plan/physical.py`).

- MULTI-SOURCE query: tables span datasources, so no single source can answer
  the whole query. The engine scans each source with its own pushed-down SQL
  (a `PhysicalScan` per source, `plan/physical.py`) and runs the cross-source
  operators (joins, aggregates, set ops, windows, sorts) in the embedded Rust
  engine `fedqrs` - a DataFusion-based executor loaded as a Python extension
  module (its sources live in `/workspace/fedqrs`). The physical plan is
  serialized to that engine's JSON IR (`executor/rust_ir.py`) and the whole plan
  - the source reads included - runs natively in Rust; only the final Arrow
  result crosses back to Python.

Every query, single- or multi-source, executes through `fedqrs`: a single-source
plan is one `source_scan` IR step the engine returns directly (one round trip to
the source), while a cross-source plan interleaves `source_scan` steps with
`merge` steps whose join / aggregate / sort fragments run on DataFusion. DuckDB
still appears - but only as a SOURCE connector (a native `duckdb` crate reading
`.duckdb` files), never as the local execution engine; the old in-memory-DuckDB
merge engine is gone. Both paths share the same logical plan, the same expression
nodes, and the same SQL emitter; they differ only in whether a subtree collapses
into one remote query or is split into per-source scans plus merge fragments.

Two correctness rules dominate the design and are referenced throughout (see
section 9): never fail silently, and an invalid query MUST raise rather than
return rows.

State-carrying types (plan nodes, expression nodes, catalog, config) are NOT
Python dataclasses. They subclass `StateModel` (`model.py`), a mutable
Pydantic `BaseModel` with `arbitrary_types_allowed=True`, `extra="forbid"`, and
a `model_copy` override that rejects unknown update keys. The point is loudness:
a mistyped or dropped field raises instead of silently defaulting. Copy-with-
change is always `node.model_copy(update={...})`, which preserves every field.

---

## 2. The pipeline at a glance

```
SQL string
  |
  v
[before processors] StarExpansionProcessor.before_execution (expand SELECT *)
  |
  v
[parse] Parser.parse_to_logical_plan sqlglot AST -> LogicalPlanNode tree
  |
  v
[bind] Binder.bind resolve tables/columns, set types, raise on invalid refs
  |
  v
[decorrelate] Decorrelator.decorrelate remove subquery expressions -> joins / lateral joins
  |
  v
[optimize] RuleBasedOptimizer.optimize predicate / projection / aggregate / order / limit pushdown
  |
  v
[plan] PhysicalPlanner.plan LogicalPlanNode -> PhysicalPlanNode
  | (single-source subtrees collapse to PhysicalRemoteQuery)
  v
[execute] Executor.execute_to_table serialize plan to fedqrs IR -> run natively in Rust -> pa.Table
  |
  v
[after processors] StarExpansionProcessor.after_execution rename internal -> visible names
  |
  v
pa.Table (or dict for EXPLAIN JSON) returned to caller
```

The orchestrator for the whole sequence is `QueryExecutor._plan_pipeline`
(`processor/query_executor.py`), which runs, in order: before-processors,
parse, bind, decorrelate, optimize, plan; then `_execute_pipeline`
(`processor/query_executor.py`) executes and runs after-processors. Note that
decorrelation runs BEFORE the rule-based optimizer, and single-source pushdown
runs DURING physical planning (it is not a logical rule).

---

## 3. Core data model

### 3.1 StateModel base (`model.py`)

`StateModel` is the common base for every state-carrying type. It is a mutable
Pydantic model. Its three guarantees: hold arbitrary values (sqlglot AST,
pyarrow), forbid unknown construction kwargs, and validate `model_copy` update
keys. There are no `@dataclass` types anywhere; this is enforced by tests.

### 3.2 Logical plan nodes (`plan/logical.py`)

Base class `LogicalPlanNode` (`plan/logical.py`) defines the node protocol:
`children`, `with_children` (a `model_copy`), `accept(visitor)`, and
`schema` (output column names). Enums: `JoinType`
(`plan/logical.py`: INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI),
`SetOpKind` (`plan/logical.py`: UNION, INTERSECT, EXCEPT), `ExplainFormat`
(`plan/logical.py`: TEXT, JSON), `AggregateFunction` (`plan/logical.py`).

Node classes:

- `Scan` - a base-table read. Carries datasource, schema_name,
  table_name, columns, alias, and the clauses pushdown may fold into it
  (filters, group_by, grouping_sets, aggregates, output_names, limit, offset,
  order_by_*, distinct, sample).
- `Projection` - SELECT list (expressions + aliases), with `distinct`
  and `distinct_on`.
- `Filter` - a boolean predicate over its input. A `Filter` whose input
  is an `Aggregate` is a HAVING clause.
- `Join` - left, right, join_type, condition, natural, using.
- `Aggregate` - group_by, aggregates, output_names, grouping_sets.
- `Sort` - sort_keys, ascending, nulls_order.
- `Limit` - limit (None for OFFSET-only) and offset.
- `SetOperation` - binary UNION/INTERSECT/EXCEPT (kind, distinct).
- `Union` - n-ary union variant.
- `Explain` - wraps a plan for EXPLAIN, with format.
- `CTE` / `CTERef` - WITH definition and reference.
- `Values` - constant rows (FROM-less SELECT).
- `SubqueryScan` - a derived table (subplan under an alias).
- `SingleRowGuard` - runtime cardinality guard for decorrelated scalar
  subqueries (at most one row, globally or per key).
- `GroupedLimit` - per-key LIMIT, produced when decorrelating a
  correlated LIMIT.
- `LateralJoin` - dependent join where the right may reference the
  left; the decorrelation fallback when a correlated subquery cannot flatten.

`LogicalPlanVisitor` (`plan/logical.py`) is the visitor protocol.

### 3.3 Expression nodes (`plan/expressions.py`)

Base class `Expression` (`plan/expressions.py`) with `get_type -> DataType`,
`accept(visitor)`, and `to_sql` (renders canonical Postgres-form SQL via the
single emitter). Enums: `DataType`, `BinaryOpType`,
`UnaryOpType`, `Quantifier`.

Nodes: `ColumnRef` (table + column + data_type set during binding),
`Literal`, `BinaryOp`, `UnaryOp`, `FunctionCall`
(scalar or aggregate, with `is_aggregate`, `distinct`,
`within_group_key`), `CaseExpr`, `InList`, `BetweenExpression`, `Cast`, `WindowExpr`, `Extract`, `Interval`, and the subquery-bearing nodes `SubqueryExpression`,
`ExistsExpression`, `InSubquery`, `QuantifiedComparison`, plus `TupleExpression`.

A shared set of expression-tree walkers lives at the bottom of the file and is
the single home for tree traversal (enforced by tests). Key ones:
`expression_children`, `map_children` (rebuilds via
`model_copy`), `column_refs`, `split_conjuncts`,
`combine_and`, and `split_where_having` (the single source of
truth for deciding which conjuncts are WHERE versus HAVING). `SUBQUERY_NODE_TYPES` marks the four subquery nodes that generic walkers do NOT descend into,
because their inner plans belong to a nested scope.

### 3.4 Physical plan nodes (`plan/physical.py`)

Base class `PhysicalPlanNode` (`plan/physical.py`). Each node implements
`children`, `schema -> pa.Schema`, and `column_aliases()` - the map from an
engine `(table, column)` to the physical Arrow column name that node's output
carries, which is the key to IR column resolution. Physical nodes are NOT
executed in Python: the tree is serialized to the fedqrs IR
(`executor/rust_ir.py`) and run in Rust. A residual
`execute -> Iterator[pa.RecordBatch]` survives only on `PhysicalScan` and
`PhysicalRemoteQuery`, used by EXPLAIN to sample a scan's build-side keys so it
can print the dynamic `IN` filter with real values; it is not the execution
path.

Single-source / remote-pushdown nodes:

- `PhysicalScan` - reads one table from one datasource, with filters,
  grouping, aggregates, ordering, limit folded in. When it is the probe of a
  semi-join reduction it is emitted as an `injected_scan` IR step and the Rust
  engine ANDs an `IN`-list of the build side's join keys into the scan.
- `PhysicalRemoteQuery` - an entire same-source subtree rendered as
  one sqlglot AST and executed in a single round trip. This is the output of
  single-source pushdown.
- `PhysicalRemoteJoin` and `PhysicalRemoteSetOp` - a join or
  set operation kept entirely on one source.

Cross-source operators (each emitted as one fedqrs merge fragment):

- `PhysicalProjection`, `PhysicalFilter`, `PhysicalWindow`, `PhysicalHashJoin`, `PhysicalNestedLoopJoin`,
  `PhysicalHashAggregate`, `PhysicalSort`, `PhysicalLimit`, `PhysicalValues`, `PhysicalUnion`,
  `PhysicalSetOperation`, `PhysicalSingleRowGuard`,
  `PhysicalGroupedLimit`, `PhysicalLateralJoin`, and `PhysicalAliasedRelation`
  (a column-passthrough wrapper whose IR emit just re-qualifies its input).
- CTE materialization: `PhysicalCTE`, `PhysicalCTEScan`,
  `PhysicalCTEMergeQuery`.
- `PhysicalShipment` - materializes a small dimension into the fact's source as
  a temp table so a cross-source fact-dimension subtree collapses into one
  island (dim shipping, section 4.7.1).

Which of these actually have an IR emitter is decided by `_NODE_EMITTERS` in
`executor/rust_ir.py` - the source of truth. Some nodes (e.g. `PhysicalUnion`,
`PhysicalSetOperation`, `PhysicalSingleRowGuard`, `PhysicalLateralJoin`,
`PhysicalCTE`) still exist in the physical layer but have no emitter yet, so a
plan that reaches Rust with one raises `UnsupportedIR` rather than running
wrong; this is the "missing operators" gap the engine roadmap tracks.

Utility: `PhysicalExplain` (builds the EXPLAIN document without
executing) and `Gather` (an unimplemented placeholder).

Module-level helpers worth knowing: `to_source_sql` (the one transpile
boundary - re-parses Postgres-form SQL and renders it in the source's dialect).
Column resolution goes through one rule, `_physical_column_name(col_ref,
alias_map)`, which honors a column's qualifier through the alias map; both the
schema-typing path (`_column_ref_type`) and the alias-map path
(`_resolve_column`) use it, and so does the IR serializer in
`executor/rust_ir.py`, so the declared schema and the Rust-side column names
resolve every column identically. A binary join's output is built by one set of helpers
(`_join_output_schema` / `_join_output_aliases` / `_merge_join_select_list`), all
taking the join type and returning LEFT columns only for SEMI/ANTI (the
`right_<name>` collision rule lives once in `_right_output_name`), so the schema,
the alias map, and the executed SELECT list always agree.

### 3.5 Catalog (`catalog/`)

`Catalog` (`catalog/catalog.py`) is the metadata registry. It holds
`datasources` (name -> DataSource) and `schemas` keyed by the tuple
`(datasource_name, schema_name)`. `register_datasource` adds a source;
`load_metadata` introspects all sources to discover schemas, tables, and
columns; `get_table(datasource, schema, table)` is the lookup the binder
uses; `resolve_table` resolves a partial reference in three forms
(`ds.schema.table`, `schema.table`, `table`).

The metadata tree types live in `catalog/schema.py`: `Schema` (a namespace
of tables), `Table` (a list of `Column`s with `get_column` (case-insensitive)), and `Column` (name + `DataType` + nullable). Tables
and columns carry read-only back-references to their parents, set by validators.

### 3.6 Config (`config/config.py`)

`Config` aggregates `DataSourceConfig`, `OptimizerConfig`, `ExecutorConfig`, and `CostConfig`. `load_config` reads YAML, rejects unknown top-level sections, and builds these models.
A `DataSourceConfig` holds the source `type` ("postgresql" / "duckdb" /
"clickhouse"), a `config` dict of connection params, and declared
`capabilities`.

---

## 4. Stage-by-stage detail

### 4.1 Wiring (how the pipeline is assembled)

The CLI (`cli/fedq.py`) builds everything. `cli` calls
`_prepare_runtime`, which loads config, builds the catalog via
`_build_catalog`, and constructs `FedQRuntime`. The runtime constructor
(`cli/fedq.py:82`) takes the whole `Config` and instantiates `Parser`,
`Binder(catalog)`, the optimizer via `build_optimizer(catalog,
config.optimizer, config.cost)` (the ONE assembly point, `optimizer/factory.py`,
so every `OptimizerConfig` flag and the `CostConfig` are honored everywhere),
`PhysicalPlanner(catalog)`, `Decorrelator`, and `Executor(config.executor)`,
then wraps them in a `QueryExecutor` with one processor:
`StarExpansionProcessor`. `_build_catalog` uses the factory `_create_datasource`
to map a config type to a connector class, calls `connect`, registers it, and
finally calls `catalog.load_metadata`.

`FedQRuntime.execute` simply delegates to `QueryExecutor.execute`.

### 4.2 Before-processors: SELECT * expansion

Entry point: `QueryExecutor._run_before_processors`
(`processor/query_executor.py`) runs each processor's `before_execution`.
The only processor is `StarExpansionProcessor` (`processor/query_preprocessor.py`),
which calls `QueryPreprocessor.preprocess`. Using catalog metadata, it
rewrites every `*` and `alias.*` into explicit, qualified column references and
records a `ColumnMapping` (`processor/query_context.py`) of internal name
(e.g. `pg.users.id`) to visible name (`id`) per output column on the
`QueryContext`. It also rewrites static PIVOT into conditional aggregation and
inlines named windows. Invariant: no star survives into the parser; an
unresolvable star raises `StarExpansionError` (`query_preprocessor.py`).
After execution, `StarExpansionProcessor.after_execution` renames the
result columns from internal to visible names.

### 4.3 Parse (`parser/parser.py`)

Entry point: `Parser.parse_to_logical_plan` (called from
`QueryExecutor._parse_query`, `query_executor.py`); the core is
`Parser.parse` (`parser/parser.py`) -> `ast_to_logical_plan`.
sqlglot parses the SQL using the custom `FedQPostgres` dialect (Postgres is the
canonical internal dialect). `_parse_one` rejects multi-statement
input. `ast_to_logical_plan` dispatches Select / Describe (EXPLAIN) / set
operations. `_convert_select` builds the plan clause by clause:
`_build_from_clause` -> `_build_where_clause` -> `_build_group_by_clause` ->
`_build_having_clause` -> `_build_select_clause` -> `_build_order_by_clause` ->
`_build_limit_clause`. Expressions convert through `_convert_expression`.

This stage is aggressively defensive about silent drops. Frozensets such as
`SUPPORTED_SELECT_ARGS`, `SUPPORTED_JOIN_ARGS`, `SUPPORTED_AGG_ARGS`, and
others list exactly which sqlglot args each builder consumes; anything else
raises `UnsupportedSQLError` via `_reject_unsupported_args`. Window
functions in WHERE/GROUP BY/HAVING, simple CASE, BETWEEN SYMMETRIC, FETCH WITH
TIES, and similar shapes are all rejected here rather than mis-planned.

Output: an unbound `LogicalPlanNode` tree whose `ColumnRef`s have no types yet.

### 4.4 Bind (`parser/binder.py`)

Entry point: `Binder.bind` (`parser/binder.py`), dispatched per node type.
Binding resolves every table and column reference against the catalog, attaches
`DataType`s, and validates structure (for example
`_check_set_branch_arity` raises when UNION branches differ in width).

The heart of resolution is a scope chain. The binder keeps `_scope_stack`, a list of dicts mapping alias -> `Table`, one entry per enclosing query
block, plus `_cte_tables` for in-scope CTEs. Each relational binder
pushes the scope visible to its children (`_push_scope_for`) and pops it
afterward. The single column resolver for the whole binder is
`resolve_in_scopes`: a qualified reference resolves against the nearest
scope that defines its table (`_resolve_qualified`), a bare one against
the nearest scope defining the column (`_resolve_unqualified`), with
intra-scope ambiguity an error (`_match_in_scope`). A reference that
resolves in NO scope raises `BindingError` - this is where an invalid
qualifier or a typo'd column name is caught (see section 9).

Subqueries bind through `SubqueryPlanBinder`, which carries the
enclosing scopes so correlated references resolve innermost-first. The single
expression dispatch `_bind_expr_dispatch` is shared by the top-level,
join, and subquery binders; only the column-leaf resolver and the
nested-subquery scopes differ. A compound expression rebuilds its children
through the one structural walker `plan/expressions.map_children` (which copies
every field via `model_copy`); only `Cast` (which also resolves its target
type) and the subquery nodes (which thread scopes) are handled specially - there
is no per-operator `_bind_binary_op`/`_bind_unary_op` ladder. `_bind_scan` prunes
a scan's over-collected column list down to columns that table actually has.

HAVING binds through one shared path - `_bind_having_with` /
`_resolve_having_column`, routed through `_bind_expr_dispatch` - used identically
by the top-level and subquery binders: a bare name matching an aggregate output
binds to that output, anything else (a grouping column, an outer-correlation
column, an unknown reference `resolve_in_scopes` rejects) falls through to the
base scope resolver. The `{output_name -> aggregate}` map is built once in
`_aggregate_alias_map` (HAVING and ORDER-BY-over-aggregate).

Output: a fully bound `LogicalPlanNode` tree.

### 4.5 Decorrelate (`optimizer/decorrelation.py`)

Entry point: `Decorrelator.decorrelate` (`optimizer/decorrelation.py`),
called from `QueryExecutor._decorrelate_plan` (`query_executor.py`). It
rewrites the bound plan so that NO subquery expression survives: EXISTS / NOT
EXISTS become SEMI / ANTI joins, IN / NOT IN (subquery) become NULL-aware SEMI /
ANTI joins, `op ANY/SOME/ALL` become SEMI / ANTI joins, and scalar subqueries
are hoisted into joins on the correlation keys (wrapped in `SingleRowGuard` for
cardinality safety, and `GroupedLimit` for a correlated LIMIT). Subqueries in a
projection or under an OR become boolean flag columns via a UNION of branches.
Classification is in `_semi_anti_parts`; scalar handling in
`_SubqueryPreparer.prepare_scalar`.

When a correlation cannot be flattened (non-equality correlation crossing an
aggregate or LIMIT), it raises `DecorrelationError` internally, which is
caught and turned into the `LateralJoin` fallback. A same-source
lateral can later be pushed as `LEFT JOIN LATERAL`; a cross-source lateral
becomes `PhysicalLateralJoin`. After rewriting, `decorrelate` asserts via
`_raise_if_subquery_expression` that no subquery node remains - a loud
guarantee that the rest of the pipeline never sees one.

### 4.6 Optimize (`optimizer/rules.py`)

Entry point: `RuleBasedOptimizer.optimize` (`optimizer/rules.py`), called
from `QueryExecutor._optimize_plan` (`query_executor.py`). It applies its
registered rules iteratively to a fixed point (max 10 iterations). The stack is
assembled in ONE place, `build_optimizer` (`optimizer/factory.py`), which honors
the `OptimizerConfig` flags. In order:

1. `PredicatePushdownRule` (`rules.py`) - pushes filters through projections,
  below joins (split by side, outer-join-safe), and into scans.
2. `SemiJoinPushdownRule` - commutes a selective SEMI/ANTI join below the INNER
  join it sits on when the semi condition references only one inner side, so the
  existential runs BEFORE the fact join. Runs before join ordering because it
  changes the region the reorderer then sees.
3. `JoinOrderingRule` (`optimizer/join_ordering.py`, gated by
  `enable_join_reordering`) - the cost-based reorderer (see below). Registered
  right after predicate pushdown so it reads the folded equi conditions and
  embedded scan filters, and before projection pushdown prunes columns.
4. `ProjectionPushdownRule` - column pruning: collects required columns and
  trims scans and intermediate projections.
5. `AggregatePushdownRule` - folds GROUP BY and aggregates into a single-source
  scan.
6. `OrderByPushdownRule` - pushes ORDER BY toward sources, rewriting keys
  through projections and joins.
7. `LimitPushdownRule` - pushes LIMIT/OFFSET toward sources when safe.

Every rule returns a new plan via `model_copy` and never mutates in place. Each
rule's pass-through arms (recurse into a node's children and rebuild only if one
changed) go through the single `transform_children(node, fn)` in
`plan/logical.py` - the plan-layer analogue of `map_children` - so the
recurse-and-rebuild mechanic is not hand-rolled per rule; only each rule's
genuinely special arm (e.g. predicate-into-scan folding) is written out.

Cost-based join ordering (the current optimizer's core). `CostModel`
(`optimizer/cost.py`) is LIVE - it is what `JoinOrderingRule` costs plans with -
not the dormant placeholder it once was. The pieces:

- `StatisticsCollector` (`optimizer/statistics.py`) fetches statistics from each
  source's CATALOG at optimize time, lazily per column (join keys and filter
  columns only) and session-cached, with a column's absence itself cached. A
  source that has no statistics returns None; nothing is ever fabricated.
- `CostModel.estimate` (`optimizer/cost.py`, combination helpers in
  `estimate_defaults.py`) produces a `CardinalityEstimate` for every logical node
  type. Joins use `card(L) * card(R) / prod max(ndv(l_key), ndv(r_key))`, the
  composite-key denominator capped at the smaller side's rows.
- HONEST UNKNOWNS, never fabricated constants: a statistic feeding an estimate
  is a MEASUREMENT with provenance or it is None. `CardinalityEstimate.rows`
  is None (unknown - propagates to every derived estimate), a point estimate
  (gap-free), or an UPPER BOUND by construction (a gap: an unknown selectivity
  ceilings at 1.0, an unknown NDV floors the join denominator at 1, an unknown
  group NDV clamps to input rows). Each gap is recorded in `defaults_used`
  provenance, which EXPLAIN surfaces. Decision points define their unknown
  policy: the join-order DP DECLINES a region containing an unknown atom
  (keeps the written order), gates decline, orientation may USE bounds (a
  side whose upper bound is under the other side's measured size is provably
  smaller). CTERef atoms estimate their registered BODY (the rule registers
  CTE bodies during descent, reset per plan walk); recursive CTEs stay
  unknown.
- Where the engine cannot price a PLAIN scan's predicate (LIKE patterns,
  column-vs-column ranges), it fills the gap from measurements, in order:
  the learned catalog's measured output for the same filter template
  (section 10), then the SOURCE PLANNER's estimate for the rendered scan
  (`DataSource.estimate_scan_rows` - pg `EXPLAIN (FORMAT JSON)`, ANALYZEd
  tables only so pg's internal default priors never leak in; DuckDB EXPLAIN
  text parse; capability-probe semantics, cached per rendered SQL per
  session), else the no-reduction bound with the gap recorded.
- `JoinOrderingRule` extracts join regions (`optimizer/join_graph.py`) - maximal
  INNER/CROSS-join-plus-filter subtrees, with outer / semi / anti / lateral joins
  as boundaries - and runs a left-deep Selinger DP per connected component
  (bounded by `max_join_reorder_size`, GOO greedy above the bound), considering
  only connected extensions so intra-component cross products are structurally
  impossible. It re-emits in predicate-pushdown's normal form and a
  predicate-conservation guard RAISES if any conjunct is not placed exactly once.
- A LOCALITY term charges rows crossing a source boundary
  (`TRANSFER_WEIGHT * transfer`) on top of `C_out`, so the DP prefers orders that
  keep same-source runs collapsible into one remote query and reduce cross-source
  transfer. The cost estimate also threads onto physical nodes (`estimated_rows`)
  so the semi-join reduction can orient itself by real cardinality (reduce the
  larger side) rather than a structural guess.

Output: an optimized but still logical `LogicalPlanNode` tree.

### 4.7 Physical planning (`optimizer/physical_planner.py`)

Entry point: `PhysicalPlanner.plan` (`physical_planner.py`) ->
`_plan_node`, called from `QueryExecutor._build_physical_plan`
(`query_executor.py`). The single most important step happens first:
`_plan_node` calls `SingleSourcePushdown.try_build(node)`
(`single_source_pushdown.py`) on the subtree. If the entire subtree lives in
one datasource and is renderable, `try_build` returns a `PhysicalRemoteQuery`
and planning of that subtree is done. This is the SINGLE-SOURCE path.

If the subtree spans sources (or cannot be rendered as one query), `_plan_node`
falls through to per-node dispatch, producing cross-source operators. Join
algorithm selection is in `_plan_join`: try a same-source
`PhysicalRemoteJoin` (`_try_plan_remote_join`); else extract equi-keys
(`_extract_join_keys`), orient them to sides (`_orient_join_keys`), choose a
build side (`_choose_build_side`, which reduces the LARGER estimated side via
`larger_estimated_side`), mark a dynamic filter on the probe-side scan
(`_mark_dynamic_filter` sets `dynamic_filter_keys`), and build a
`PhysicalHashJoin`; else fall back to `PhysicalNestedLoopJoin`. The semi-join
reduction itself (read the build, collect its distinct keys, inject them into
the probe) is not run here - it is emitted as `collect_distinct` +
`injected_scan` IR steps and executed in fedqrs. Set operations route through
`_plan_set_operation`; cross-source CTEs materialize through `_plan_cte`; a
projection with window functions becomes `PhysicalWindow` (`_plan_projection`).

Output: a `PhysicalPlanNode` tree.

### 4.7.1 Dim shipping (`optimizer/dim_shipping.py`)

Before the per-node cross-source dispatch, `_plan_node` tries ONE more
same-source collapse: `DimShipping.try_ship(node)`. When a large fact on one
source joins small dimensions on another and the query GROUPS BY dimension
columns, the fact cannot collapse into its own source and every fact row must
cross to the coordinator. Dim shipping instead SHIPS the small dimensions INTO
the fact's source as temp tables, so the whole join+aggregate runs as one island
there and only the aggregate OUTPUT crosses. It rewrites each foreign `Scan`
into a temp-table `Scan` on the fact's source, collapses the rewritten subtree
with `SingleSourcePushdown.try_build` into one island, and wraps it in a
`PhysicalShipment` per shipped dimension (`plan/physical.py`). The engine
materializes each shipment (`fedqrs` `connectors::ship_table`: a DuckDB pinned
TEMP TABLE, or a Postgres `pg_temp` table via ADBC binary-COPY, ANALYZEd after
ingest and dropped at query end) before the island reads it. The island carries
a SEEDED schema from the pure cross-source plan, because a temp table that does
not yet exist cannot be probed python-side.

Correctness rests on shipping only across deterministic, row-preserving-or-
reducing nodes and INNER equi joins (a foreign relation is evaluated and joined
identically to how the coordinator join would), so a shipped plan returns
exactly the rows the pure plan would. The GATE is a HEURISTIC, not a cost
decision - the cost model cannot estimate whether an aggregate COLLAPSES (the
NDV-independence product over-counts a correlated multi-column group; see the
`dim-shipping-*` docs). It ships only when: the root is a plain (non-rollup)
aggregate; the fact is genuinely large and each shipped dimension is small and
known (declining on any defaulted size); the island's outputs match the pure
plan's; and - the collapse guard - the aggregate's GROUP BY does NOT span two or
more independent high-cardinality SOURCE DIMENSIONS (`_dimension_explosion`,
`HIGH_CARD_NDV`). That last guard counts distinct high-card OWNER relations, not
high-card keys, so correlated keys from one dimension (an id and its description)
count once and still ship, while a genuinely explosive group (item x date) is
declined and keeps the pipelined no-ship plan. A hard runtime row cap in
`ship_table` (`SHIP_MAX_ROWS`) is the safety backstop against a stale-stats
mis-ship - it raises loudly rather than ingest a giant relation. Kill switch:
`FEDQ_DIM_SHIPPING=0`.

### 4.8 Execute (`executor/executor.py`)

Entry point: `Executor.execute_to_table` (`executor/executor.py`), called
from `QueryExecutor._run_physical_plan` (`query_executor.py`). It resolves the
plan's datasources and calls `execute_via_rust(plan, datasources)`
(`executor/rust_ir.py`), which (1) registers each datasource with the engine by
name (`register_datasources` -> the native Postgres / DuckDB / Parquet Rust
connectors), (2) serializes the physical plan into the JSON IR (`build_ir` walks
the tree bottom-up into `source_scan` / `injected_scan` / `collect_distinct` /
`merge` / `return` steps plus named `fragments`), (3) makes ONE
`fedqrs.execute_ir(...)` call that runs the whole plan natively, and (4) reads
the result back as a zero-copy Arrow C-stream
(`pa.RecordBatchReader.from_stream(...).read_all()`). A physical node or
expression the IR cannot represent raises `UnsupportedIR`, which propagates -
there is no fallback and no silent wrong answer.

EXPLAIN is plan introspection, not data, so it never reaches Rust:
`_run_physical_plan` short-circuits a `PhysicalExplain` through `_run_explain`,
returning the plan document dict from `build_document` for `FORMAT JSON` and a
rendered single-column table for `FORMAT TEXT`.

### 4.9 After-processors

`QueryExecutor._run_after_processors` (`query_executor.py`) runs processors
in reverse. `StarExpansionProcessor.after_execution` renames the Arrow columns
from internal names back to the user-visible names recorded during expansion, so
the caller never sees an internal name.

---

## 5. SQL emission (`plan/emit/`)

There is exactly ONE expression-to-SQL converter, and exactly one transpile
boundary per source. The emit package builds a sqlglot `exp.Expression` and
renders it once with `ast.sql(dialect=...)` (`plan/emit/__init__.py`).

- `SqlglotEmitter` (`plan/emit/expressions.py`) is an `ExpressionVisitor` that
  lowers engine `Expression` nodes to sqlglot AST. It dispatches per node type
  (`visit_column_ref`, `visit_binary_op` via the `_BINARY_BUILDERS` allowlist,
  `visit_function_call`, `visit_window_expr`, etc.). The exported entry is
  `expression_to_ast(expr, resolver)`. Correlated subquery nodes raise here,
  enforcing that decorrelation already removed them.
- The `resolver` argument is the ONLY thing that differs between the remote and
  merge paths. `ColumnResolver` (`plan/emit/resolver.py`) is the interface;
  `SourceResolver` emits quoted, table-qualified columns for a remote
  source; `MergeResolver` maps an engine `(table, column)` to the
  physical Arrow column name a merge relation actually carries.
  `CANONICAL_SOURCE_RESOLVER` is the singleton used by
  `Expression.to_sql`.
- Clause builders in `plan/emit/clauses.py` (`select_expressions`, `order_by`,
  `group_by`, `apply_limit_offset`, plus the fragment helpers and
  `aliased_select_fragment`) take a resolver and call `expression_to_ast`, so the
  SAME builders serve both paths. The set-operation kind -> sqlglot node map is
  one table, `clauses.SET_OP_EXP`. A scan's FROM table reference is built in one
  place, `build_table_ref` (quoted name, optional schema/alias/TABLESAMPLE), used
  by the remote scan and the single-source pushdown. Build-key arrays for hash
  joins are extracted in one place, `_key_column_arrays`, which raises on a
  non-column key.
- The SELECT skeleton (FROM / SELECT / WHERE / GROUP / HAVING / DISTINCT / ORDER
  / LIMIT) is composed in ONE place: `clauses.assemble_select`. A remote
  query is one SELECT over N relations; a single-table scan is the N=1 case and a
  same-source join/CTE/set-op subtree is the N>1 case, so both build their FROM,
  join, and clause pieces and assemble through the same function.
  `PhysicalScan._build_ast` (`plan/physical.py`) composes through it, and so
  does `SingleSourcePushdown._render` (`optimizer/single_source_pushdown.py`):
  the whole remote path is AST-first. `SingleSourcePushdown` builds its FROM as
  an exp.Table / exp.Subquery, its joins as exp.Join (SEMI/ANTI/LATERAL/NATURAL),
  set operations as exp.Union/Intersect/Except, and CTEs as exp.With - no SQL
  string is hand-crafted and `_finish` stores the built AST directly (no
  parse-back). Execution renders that AST through `to_source_sql`.
- `FedQPostgres` (`parser/dialect.py`) is the canonical internal dialect: a
  Postgres dialect that parses EXPLAIN into a native `exp.Describe` so the inner
  statement is never re-scanned from text. All plans render to Postgres form
  first; `to_source_sql` (`plan/physical.py`) re-parses that text through the
  source connection's `render_dialect` (postgres / duckdb / clickhouse) at
  execute time, the one place dialect divergence is resolved.

So: the remote path builds a Postgres-form AST with a `SourceResolver` and
transpiles to the source dialect. The merge path no longer runs SQL through a
local engine; instead `executor/rust_ir.py` serializes each cross-source
operator into the fedqrs IR, resolving every column to its physical Arrow name
via `_physical_column_name` (the same rule the remote path's schema uses). Most
merge fragments are STRUCTURED IR (`project` / `filter` / `aggregate` / `sort` /
`hash_join` / `limit`); the few easier to express as SQL - window, grouped-limit,
CTE-merge, VALUES - are rendered as DuckDB-dialect `raw_sql` fragments with a
`MergeResolver` and parsed by DataFusion's SQL front end in Rust. The
expression-to-IR conversion (`expr_to_ir`, `rust_ir.py`) mirrors the
expression-to-SQL emitter node for node.

---

## 6. End-to-end walkthrough: SINGLE-SOURCE query

Example (all tables in one PostgreSQL datasource named `pg`):

```sql
SELECT city, COUNT(*) AS n
FROM pg.public.users
WHERE age > 30
GROUP BY city
ORDER BY n DESC
LIMIT 10;
```

1. Before-processors. No star, so `StarExpansionProcessor` leaves the projection
  list as written and records no rename mappings.

2. Parse (`Parser`). `_convert_select` builds:
  `Limit(Sort(Aggregate(Filter(Scan(pg.public.users)))))`, roughly:
  - `Scan(datasource="pg", schema_name="public", table_name="users", ...)`
  - `Filter(predicate = BinaryOp(GT, ColumnRef(age), Literal(30)))`
  - `Aggregate(group_by=[ColumnRef(city)], aggregates=[ColumnRef(city),
  FunctionCall("COUNT", [*], is_aggregate=True)], output_names=["city","n"])`
  - `Sort(sort_keys=[ColumnRef("n")], ascending=[False])`
  - `Limit(limit=10, offset=0)`

3. Bind (`Binder`). The scan resolves `pg.public.users` to its `Table`. Each
  `ColumnRef` gets a `DataType` via `resolve_in_scopes`. `age`, `city` resolve
  against the users scope; the ORDER BY key `n` resolves against the aggregate
  output alias via `_bind_sort_keys_for_aggregate` (`binder.py`). An unknown
  column here would raise `BindingError`.

4. Decorrelate. No subqueries; the plan is returned unchanged (after the
  no-subquery assertion).

5. Optimize (`RuleBasedOptimizer`). `PredicatePushdownRule` pushes the `age > 30`
  filter into the `Scan` (`Scan.filters`). `AggregatePushdownRule` folds the
  GROUP BY and COUNT into the scan (`Scan.group_by`, `Scan.aggregates`,
  `Scan.output_names`). `OrderByPushdownRule` and `LimitPushdownRule` fold the
  ORDER BY and LIMIT into the scan as well. The result collapses toward a single
  `Scan` carrying every clause.

6. Physical planning (`PhysicalPlanner.plan`). `_plan_node` calls
  `SingleSourcePushdown.try_build`. Because every relation is in `pg`, it
  accumulates the clauses (`_PushContext`), renders one Postgres-form sqlglot
  AST, and returns a `PhysicalRemoteQuery` (`plan/physical.py`) whose
  `datasource_connection` is the `pg` `PostgreSQLDataSource`, `query_ast` is the
  built AST, and `output_names` is `["city", "n"]`.

7. Execute. `execute_via_rust` serializes the plan to a single `source_scan` IR
  step carrying the rendered source-dialect SQL, and `fedqrs.execute_ir` runs it.
  The SQL the engine sends to Postgres over its native ADBC connector is one
  statement, equivalent to:

  ```sql
  SELECT "city" AS "city", COUNT(*) AS "n"
  FROM "public"."users"
  WHERE "age" > 30
  GROUP BY "city"
  ORDER BY "n" DESC
  LIMIT 10
  ```

  The Rust engine's Postgres connector executes it (ADBC, Arrow-native) on a
  pooled connection and streams `pa.RecordBatch`es straight back to Python. The
  engine does no local joining, grouping, or sorting.

8. Return. The batches are assembled into a `pa.Table`; after-processors are a
  no-op (no star rename); the table is returned to the caller. The CLI prints it
  via `ResultPrinter` (`cli/fedq.py`).

The whole query ran on Postgres in one round trip - that is the single-source
pushdown win.

---

## 7. End-to-end walkthrough: MULTI-SOURCE query

Example (a Postgres table joined to a DuckDB table, with a filter):

```sql
SELECT u.name, o.total
FROM pg.public.users AS u
JOIN duck.main.orders AS o ON u.id = o.user_id
WHERE u.age > 30;
```

Here `users` is in datasource `pg` and `orders` is in datasource `duck`, so no
single source can answer the query.

1. Before-processors / Parse / Bind. As before, the plan binds to
  `Projection(Filter(Join(Scan(pg.users AS u), Scan(duck.orders AS o))))` with
  the join condition `BinaryOp(EQ, u.id, o.user_id)` and the filter
  `BinaryOp(GT, u.age, 30)`. Both scans resolve against their own catalog
  tables and aliases (`u`, `o`).

2. Decorrelate. No subqueries; unchanged.

3. Optimize. `PredicatePushdownRule` pushes `u.age > 30` below the join into the
  `pg.users` scan (it references only the left side, so it is outer-join-safe
  and lands in `Scan.filters`). `ProjectionPushdownRule` prunes each scan to the
  columns actually needed: `users` -> {id, name, age}, `orders` ->
  {user_id, total}. The join, projection, and per-source filters remain
  distinct because they straddle two sources.

4. Physical planning. `SingleSourcePushdown.try_build` is attempted on the whole
  subtree, but the two scans name different datasources, so it declines (returns
  None). `_plan_node` then plans each side independently: each `Scan` becomes a
  `PhysicalScan` bound to its own connection (`pg` and `duck`). For the join,
  `_plan_join` cannot use a remote join (different sources), so it extracts the
  equi-key pair (`u.id`, `o.user_id`), orients them to sides, chooses a build
  side via `_choose_build_side`, and produces a `PhysicalHashJoin`
  (`plan/physical.py`) over the two `PhysicalScan`s. The top
  `Projection` becomes a `PhysicalProjection`. Each `PhysicalScan` is rendered
  in its own source dialect: the Postgres scan as Postgres SQL, the DuckDB scan
  as DuckDB SQL (via `render_dialect` and `to_source_sql`).

5. Execute. `execute_via_rust` (`executor/rust_ir.py`) registers `pg` and `duck`
  with the engine, serializes the plan to IR, and hands it to
  `fedqrs.execute_ir`. Because the join is a single-key INNER join between two
  plain scans, the serializer emits the semi-join REDUCTION as explicit steps.
  Say the cost estimate makes `duck.orders` the larger side, so it is the probe
  and `pg.users` donates keys:
  - a `source_scan` step reads the build side `pg.users` with its pushed filter
  (`SELECT "id","name","age" FROM "public"."users" WHERE "age" > 30`) over the
  pooled Postgres connection, materialized into an Arrow binding.
  - a `collect_distinct` step collects that build side's distinct `id` values
  (capped at `_DYNAMIC_FILTER_MAX_KEYS`), in a DataFusion context.
  - an `injected_scan` step reads the probe `duck.orders` with the collected keys
  pushed in as `user_id IN (...)`, so DuckDB returns only matching `orders` rows.
  This is the dynamic-filter reduction that keeps the cross-source join cheap;
  the engine chooses the delivery (an inline `IN` list under the cap, else a
  temp-table semi-join).
  - a `merge` step runs the `hash_join` fragment: the two Arrow bindings are
  registered as in-memory tables (`in_left` / `in_right`) in a DataFusion
  `SessionContext` and joined on `id = user_id`, projecting the join's canonical
  output columns.
  - a second `merge` step runs the `project` fragment for `u.name, o.total`.
  The engine returns only the final result, as an Arrow C-stream.

6. Return. Python wraps that stream (`pa.RecordBatchReader.from_stream`) and
  materializes it into a `pa.Table`. Every source read, the reduction, the join,
  and the projection ran inside Rust; only the final rows crossed back.

So the multi-source path scans each source with its own pushed-down SQL (filters
and dynamic join-key filters included) and runs the cross-source join, aggregate,
sort, window, and set operations on DataFusion inside fedqrs - all driven by the
single IR the physical plan serializes to. There is no local DuckDB merge engine
and no per-operator Python execution.

---

## 8. Datasources (`datasources/`)

`DataSource` (`datasources/base.py`) is the abstract connector interface. Key
abstract methods: `connect`, `disconnect`, `get_capabilities`, `list_schemas` / `list_tables`, `get_table_metadata`, `get_table_statistics`, `execute_query` (returns
`Iterator[pa.RecordBatch]`), and `get_query_schema`. The class attribute
`render_dialect` (default "postgres") names the sqlglot dialect the engine
transpiles pushed SQL into for this source. Capabilities are enumerated by
`DataSourceCapability`. Metadata/statistics value types
(`ColumnMetadata`, `TableMetadata`, `ColumnStatistics`, `TableStatistics`) are
`StateModel`s defined in the same file.

The connector is the authority on its own types. `DataSource.map_native_type`
(native type name -> engine `DataType`) lives on the base with a generic SQL
default; a connector overrides it for types only it exposes (Postgres maps
`uuid`). The catalog does NOT map types itself - it delegates to the connector,
so the catalog and the execution path never disagree on what a column is. The
single `DataType -> Arrow` authority is `plan/arrow_types.py` (`arrow_type_for`,
which raises on an unmapped type; `is_renderable`), used by the executor's local
CAST and enforced at catalog load (a column mapped to a non-renderable `DataType`
fails loudly there). Shared connector helpers on the base remove per-connector
duplication: `_metadata_from_information_schema`, `_build_column_statistics`,
`_schema_probe_sql` (the one `LIMIT 0` schema probe), and the `_fetch_batch_size`
streaming chunk size.

Two-tier connectors. Since the Rust cutover there are two connector layers. The
Python `DataSource` classes below are the CATALOG and STATISTICS authority - they
introspect schemas, map native types, fetch `get_table_statistics` for the cost
model, and back EXPLAIN's dynamic-filter sampling. The DATA-PLANE reads happen in
Rust: `register_datasources` (`executor/rust_ir.py`) hands each source to the
engine by name with its connection params, and fedqrs reads it over a NATIVE Rust
connector - Postgres via ADBC (thread-local pooled connection), DuckDB via the
native `duckdb` crate (a file reopened per fetch, cheap), Parquet via a cached
DataFusion `SessionContext` per directory. A source type with no native Rust
connector (currently ClickHouse) raises `UnsupportedIR` at execute time rather
than running a plan it cannot read.

Connectors:

- `PostgreSQLDataSource` (`datasources/postgresql.py`) - uses a psycopg2
  `ThreadedConnectionPool`; `execute_query` routes to a psycopg2
  fetch-many path (`_execute_query_psycopg2`, building Arrow arrays from
  rows via an OID->Arrow type map kept consistent with `arrow_type_for`) or, when
  `driver: adbc` is configured, an Arrow-native ADBC streaming path
  (`_execute_query_adbc`). Inherits `render_dialect = "postgres"`.
- `DuckDBDataSource` (`datasources/duckdb.py`) - a single in-memory or
  file-backed connection; `execute_query` runs the query and yields
  Arrow batches from the materialized result. `render_dialect = "duckdb"`.
- `ParquetDataSource` (`datasources/parquet.py`) - a directory of Parquet files
  exposed as tables; the Rust engine reads them through a cached DataFusion
  context. `render_dialect = "duckdb"`.
- `ClickHouseDataSource` (`datasources/clickhouse.py`) - HTTP via
  clickhouse-connect; `execute_query` streams Arrow batches from
  `query_arrow_stream`. `render_dialect = "clickhouse"`. It is importable and
  usable for catalog/metadata, but has NO native Rust connector yet, so a plan
  that reads it raises `UnsupportedIR` at execute (see the two-tier note above).

All connectors return Arrow `RecordBatch`es, the common currency that lets fedqrs
combine results from heterogeneous sources.

---

## 9. Correctness posture

Two rules from the project doctrine are enforced structurally across the
pipeline. They are intentionally redundant - the same class of mistake is caught
at several stages.

Never fail silently. State types subclass `StateModel` (`model.py`) with
`extra="forbid"` and a key-validating `model_copy`, so a dropped or mistyped
field raises at construction or copy. Plan transformations always use
`model_copy(update=...)`, never field-by-field reconstruction, so a field can
never be silently lost (pinned by `tests/test_node_field_preservation.py`). The
parser uses allowlists (`SUPPORTED_*` frozensets) plus `_reject_unsupported_args`
(`parser/parser.py`) and raises `UnsupportedSQLError` (`parser/errors.py`)
on any SQL clause it does not consume, rather than ignoring it. The expression
emitter dispatches via allowlist tables and raises on an unknown operator. The
binder's dispatch (`bind`, `binder.py`) ends in
`raise BindingError(...)` for an unknown node type, and the decorrelator asserts
no subquery node survives (`_raise_if_subquery_expression`,
`decorrelation.py`). The IR serializer is the same kind of guard at the last
stage: `executor/rust_ir.py` raises `UnsupportedIR` on any physical node or
expression it does not know how to emit, rather than shipping the engine a plan
that could return wrong rows.

An invalid query MUST raise. This is the most severe failure mode: returning
rows for an invalid query manufactures a wrong answer. The single column
resolver `resolve_in_scopes` (`binder.py`) is the choke point: a qualified
reference to a table the query does not name raises `BindingError`
(`_resolve_qualified`), a bare column that resolves in no scope raises
(`_resolve_unqualified`), and a name ambiguous within a scope raises
(`_match_in_scope`). Because all binders (top-level, join, subquery)
funnel column resolution through this one method, there is no path by which a
bogus qualifier or typo'd column binds to a relation the query did not name -
it fails at bind time, before any source is queried. Set-operation arity
mismatches likewise raise at bind (`_check_set_branch_arity`).

Exceptions are caught in exactly one place: the CLI REPL (`FedQRepl`,
`cli/fedq.py`), which displays `BindingError`, `StarExpansionError`, sqlglot
parse errors, `ValueError` / `RuntimeError`, and `duckdb.Error` as `error: ...`,
with a broad fallback that surfaces anything else (an `UnsupportedIR`, a Rust
engine error) as `unexpected error: ...`. Everywhere else, exceptions propagate.
A crash never ships a lie.

---

## 10. Statistics: measure, learn, never fabricate

The optimizer's cardinality decisions - reduction orientation, the dim-shipping
gate, join order, build-side choice - consume exactly three DATA properties:
base row counts, column NDVs, and predicate selectivities. The governing
principle across every layer: each one is a MEASUREMENT with provenance
(source catalog, probe, learned) or it is honestly UNKNOWN - never a fabricated
constant. A fabricated row count next to a measured one inverts every
larger-side decision (the verified q39 cold-source regression); a real row
count paired with a fabricated NDV skews exactly the ratios that pick join
order (q23). Full history and decisions: `adaptive-catalog-plan.md`
("COURSE CORRECTION" section).

Measurements arrive through four channels:

1. SOURCE CATALOG (optimize time). reltuples / pg_stats on PostgreSQL,
   duckdb_tables() + one approx aggregate scan on DuckDB - what ANALYZE
   derived (section 4.6).
2. PROBE (first touch of a statless table). A never-ANALYZEd pg table
   (reltuples = -1) is MEASURED by the connector instead of reported unknown:
   at or below `PROBE_EXACT_BYTES` (256MB - every SF10 TPC-DS dimension fits)
   one exact aggregate scan returns the row count plus per-requested-column
   NDV / null fraction / min / max; above it a `TABLESAMPLE SYSTEM` block
   sample scales a row-count estimate only (a sampled NDV is biased low and
   would fabricate). Reads the real file size, so it works with relpages = 0.
   Session-cached by the collector; kill switch `FEDQ_STATS_PROBE=0`.
3. ASK-THE-SOURCE (optimize time, plain filtered scans the engine cannot
   price). The source planner's own EXPLAIN estimate, with the guards listed
   in section 4.6. Shares the probe's kill switch.
4. LEARNED CATALOG (execution time, persisted). `fedqrs.execute_ir` returns a
   per-step `(binding, measured rows)` list; `build_ir_with_observations`
   (`executor/rust_ir.py`) maps each binding to catalog provenance and
   `execute_via_rust` persists after the result is built, off the critical
   path, one commit per query (WAL, synchronous=OFF). ALWAYS ON: the catalog
   opens next to the loaded config (`<config>.stats.sqlite`), or the
   `FEDQ_STATS_CATALOG` env path. What each measured number MEANS:
   - an unfiltered plain base scan: the table's ROW COUNT;
   - a collect_distinct over an unfiltered base column: that column's NDV;
   - a FILTERED unreduced plain scan: the predicate's measured OUTPUT, keyed
     by its constant-neutral template (`scan_predicate_template`: sorted
     conjunct shapes, alias-neutral, constants dropped) - query N's
     measurement warms query N+1 whatever the literal values. An
     injection-reduced scan is excluded: its output is below what the
     predicate alone keeps, recording it would learn a lie;
   - a coordinator `PhysicalHashAggregate` (via the engine's AggregateExec
     metric harvest) or a SHIPPED ISLAND stamped with `group_observation`:
     the aggregate's measured GROUP COUNT, keyed by the input's subplan
     signature (`optimizer/subplan_signature.py` - alias- and
     constant-neutral). The island stamp applies only under
     row-count-preserving wrappers (Projection/Sort/SubqueryScan).

READ paths, all fill-not-override (a present source statistic is never
replaced; overriding destabilized decisions tuned against source estimates):
`StatisticsCollector._overlay_learned` fills missing row counts / NDVs;
`CostModel._scan_filter_rows` fills an unpriceable predicate from the learned
template measurement BEFORE asking the source planner; `CostModel.
_estimate_aggregate_tracked` uses a measured group count in place of the
NDV-independence product; the dim-shipping explosion gate PREFERS a measured
collapse (`SHIP_COLLAPSE_MAX_FRACTION` of estimated input rows) over its
dimension-width heuristic - self-correcting in both directions, since a
declined ship still measures its coordinator group count and a taken ship
measures its island output. A TTL bounds staleness; every write self-heals on
the next execution of the shape.

Correctness-neutral by construction. A learned or probed value only changes
WHICH plan the optimizer picks - never the answer - because every decision it
feeds is itself correctness-neutral. A stale or wrong value costs a slow
query, never a wrong one, so the system learns aggressively and invalidates
lazily.

The catalog is scoped one-per-configuration (keyed by the config's datasource
names, with a `source_fingerprint` slot for repoint detection). Tables:
`table_stats`, `predicate_stats`, `group_stats`, `subplan_stats`, and a
`materialized_fragments` registry reserved for the query accelerator.

### 10.1 The cold-source test harness

`benchmarks/tpcds/cold_sources.py` is the scoreboard for the statless-source
regime, and its SUCCESS CRITERION is CONVERGENCE: a never-ANALYZEd source's
time should approach the ANALYZED path's (the plan real statistics buy), not
merely beat learning-off. It runs the stat-sensitive subset three ways -
ANALYZED (real pg stats, the reference column), cold learning-OFF, cold
learning-ON warmed - and reports W-OFF and W-ANLZ deltas plus row-count
correctness across all three.

Simulating "never ANALYZEd" honestly requires that pg stats stay absent for
the WHOLE cold window, not just at its start:
- going cold DELETEs the tables' `pg_statistic` rows and sets
  `reltuples = -1, relpages = 0` - exactly a fresh load's state;
- it also `ALTER TABLE .. SET (autovacuum_enabled = false)` per table:
  autoanalyze triggers on WRITE counters (`n_mod_since_analyze`), which a
  read-only benchmark never advances, but the experiment must not depend on
  that;
- before restoring anything, `_verify_still_cold` asserts every table still
  has zero `pg_statistic` rows and `reltuples < 0`, and RAISES if statistics
  crept back - a benchmark that silently measured the analyzed path would
  report a lie;
- restore re-enables autovacuum and ANALYZEs, leaving the database as found.

The probe deliberately still works under this simulation (pg_relation_size
and TABLESAMPLE read the actual file, not relpages), while ask-the-source
deliberately refuses (pg's planner would only relay its own internal default
selectivities - the fabricated priors this engine removed).
`benchmarks/tpcds/diag_profile.py` applies the same guard for single-query
mechanism probes (`DIAG_SCALE` selects the scale).

## 11. Runtime performance layers (2026-07-10)

### 11.1 Plan cache

`executor/plan_cache.py`, wired into `QueryExecutor._plan_pipeline`. A
cross-query LRU+TTL map from the EXACT rewritten SQL to its physical plan: a
hit skips parse/bind/decorrelate/optimize/physical (11-26ms of a small
query) and goes straight to execution. It stores PLANS, never data - a hit
re-executes against the sources, so results always reflect current data;
the only staleness is plan QUALITY lagging newer learned statistics,
bounded by the TTL. Before-processors still run on every execution (star
expansion's after-hook renames columns from per-query state its before-hook
builds); EXPLAIN plans never cache (their execution mutates scan nodes and
must show current decisions). Keys are deliberately NOT constant-neutral
templates: the optimizer manufactures and copies literals (transitive
constants, CTE union-filter pushdown), so positional constant substitution
into a cached plan is unsound. `FEDQ_PLAN_CACHE=0` disables;
`FEDQ_PLAN_CACHE_TTL` / `_SIZE` override defaults.

### 11.2 Eager aggregation

`optimizer/eager_aggregation.py` (design: `eager-agg-plan.md`). Rewrites
`Aggregate(G, sums)` over a join tree into a FINAL aggregate over
`Join(decorating dims, SubqueryScan(PARTIAL aggregate over the fact side))`
so dim shipping collapses the partial - fact, its small dims, and the
pre-aggregation - into ONE island; only partial rows cross. No uniqueness
requirement on the decorating keys (Yan & Larson 1995: join multiplicity is
a function of the key alone, so the final merge counts each partial exactly
as the original counted each raw row). Gates, each declining safely: plain
single-level GROUP BY; SUM / constant / passthrough outputs; pure INNER
plain-equi join tree; MULTI-source only; at least one peeled dim OVER the
ship budget (eager RESCUES what full dim-shipping cannot collapse, never
pre-empts a full collapse that already wins - measured on q21); collapse
priced as partial groups vs the LARGEST base scan (join estimates
under-count through containment asymmetries). Kill switch FEDQ_EAGER_AGG=0.
Measured at SF10: q04 3.4x, q11 3.0x, q74 2.8x.

### 11.3 Cross-step parallel reads (engine)

`fedqrs engine.rs` (design + measurements: `parallel-reads-plan.md`). The
step loop prefetches every PLAIN source scan (non-ctid-parallel, not on a
ship-target datasource) onto a persistent worker pool before the loop, and
dispatches each InjectedScan the moment its key bindings land
(POSTGRES-only: a DuckDB injected scan is in-process and already saturates
every core, so concurrency there is pure contention). Results are consumed
at their step index - bindings, use counts, the memory budget and the spill
machinery stay single-threaded. Workers fetch through the same
`connectors::fetch` as the sequential path (per-THREAD pg connection
caches, mutex-guarded DuckDB clone base). Kill switch
FEDQRS_PARALLEL_STEPS=0. NOTE: on the loopback bench rig the overlap value
is mostly hidden (zero network latency); it scales with real source RTT.

### 11.4 Cold vs warm benchmarking

Every benchmark child times its FIRST execution (COLD: source connections,
statistics fetches and the statless probe, full planning) separately from
the warm run (plan-cache hit, cached stats); both TPC-DS and TPC-H reports
carry Warm and Cold columns, and `engine_timings.csv` both values. A change
that helps only warm runs can no longer silently tax first-sight queries.
