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
  by `SingleSourcePushdown` (`optimizer/single_source_pushdown.py:106`) which
  produces a `PhysicalRemoteQuery` (`plan/physical.py:1871`).

- MULTI-SOURCE query: tables span datasources, so no single source can answer
  the whole query. The engine scans each source with its own pushed-down SQL
  (a `PhysicalScan` per source, `plan/physical.py:481`), pulls those results as
  Arrow, and runs the cross-source operators (joins, aggregates, set ops,
  windows, sorts) locally in an in-memory DuckDB instance called the merge
  engine (`MergeEngine`, `executor/merge_engine.py:21`).

Both paths share the same logical plan, the same expression nodes, and the same
SQL emitter; they differ only in whether a subtree collapses into one remote
query or is split into per-source scans plus local merge operators.

Two correctness rules dominate the design and are referenced throughout (see
section 9): never fail silently, and an invalid query MUST raise rather than
return rows.

State-carrying types (plan nodes, expression nodes, catalog, config) are NOT
Python dataclasses. They subclass `StateModel` (`model.py:28`), a mutable
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
[before processors]   StarExpansionProcessor.before_execution   (expand SELECT *)
   |
   v
[parse]   Parser.parse_to_logical_plan   sqlglot AST -> LogicalPlanNode tree
   |
   v
[bind]    Binder.bind                    resolve tables/columns, set types, raise on invalid refs
   |
   v
[decorrelate]  Decorrelator.decorrelate  remove subquery expressions -> joins / lateral joins
   |
   v
[optimize]  RuleBasedOptimizer.optimize  predicate / projection / aggregate / order / limit pushdown
   |
   v
[plan]    PhysicalPlanner.plan           LogicalPlanNode -> PhysicalPlanNode
   |                                      (single-source subtrees collapse to PhysicalRemoteQuery)
   v
[execute]  Executor.execute_to_table     remote SQL per source + DuckDB merge -> pa.Table
   |
   v
[after processors]  StarExpansionProcessor.after_execution  rename internal -> visible names
   |
   v
pa.Table (or dict for EXPLAIN JSON) returned to caller
```

The orchestrator for the whole sequence is `QueryExecutor._plan_pipeline`
(`processor/query_executor.py:86`), which runs, in order: before-processors,
parse, bind, decorrelate, optimize, plan; then `_execute_pipeline`
(`processor/query_executor.py:75`) executes and runs after-processors. Note that
decorrelation runs BEFORE the rule-based optimizer, and single-source pushdown
runs DURING physical planning (it is not a logical rule).

---

## 3. Core data model

### 3.1 StateModel base (`model.py:28`)

`StateModel` is the common base for every state-carrying type. It is a mutable
Pydantic model. Its three guarantees: hold arbitrary values (sqlglot AST,
pyarrow), forbid unknown construction kwargs, and validate `model_copy` update
keys. There are no `@dataclass` types anywhere; this is enforced by tests.

### 3.2 Logical plan nodes (`plan/logical.py`)

Base class `LogicalPlanNode` (`plan/logical.py:49`) defines the node protocol:
`children()`, `with_children()` (a `model_copy`), `accept(visitor)`, and
`schema()` (output column names). Enums: `JoinType`
(`plan/logical.py:11`: INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI),
`SetOpKind` (`plan/logical.py:34`: UNION, INTERSECT, EXCEPT), `ExplainFormat`
(`plan/logical.py:42`: TEXT, JSON), `AggregateFunction` (`plan/logical.py:23`).

Node classes:

- `Scan` (`:87`) - a base-table read. Carries datasource, schema_name,
  table_name, columns, alias, and the clauses pushdown may fold into it
  (filters, group_by, grouping_sets, aggregates, output_names, limit, offset,
  order_by_*, distinct, sample).
- `Projection` (`:157`) - SELECT list (expressions + aliases), with `distinct`
  and `distinct_on`.
- `Filter` (`:197`) - a boolean predicate over its input. A `Filter` whose input
  is an `Aggregate` is a HAVING clause.
- `Join` (`:220`) - left, right, join_type, condition, natural, using.
- `Aggregate` (`:254`) - group_by, aggregates, output_names, grouping_sets.
- `Sort` (`:283`) - sort_keys, ascending, nulls_order.
- `Limit` (`:308`) - limit (None for OFFSET-only) and offset.
- `SetOperation` (`:360`) - binary UNION/INTERSECT/EXCEPT (kind, distinct).
- `Union` (`:336`) - n-ary union variant.
- `Explain` (`:392`) - wraps a plan for EXPLAIN, with format.
- `CTE` (`:415`) / `CTERef` (`:446`) - WITH definition and reference.
- `Values` (`:478`) - constant rows (FROM-less SELECT).
- `SubqueryScan` (`:505`) - a derived table (subplan under an alias).
- `SingleRowGuard` (`:532`) - runtime cardinality guard for decorrelated scalar
  subqueries (at most one row, globally or per key).
- `GroupedLimit` (`:561`) - per-key LIMIT, produced when decorrelating a
  correlated LIMIT.
- `LateralJoin` (`:595`) - dependent join where the right may reference the
  left; the decorrelation fallback when a correlated subquery cannot flatten.

`LogicalPlanVisitor` (`plan/logical.py:628`) is the visitor protocol.

### 3.3 Expression nodes (`plan/expressions.py`)

Base class `Expression` (`plan/expressions.py:27`) with `get_type() -> DataType`,
`accept(visitor)`, and `to_sql()` (renders canonical Postgres-form SQL via the
single emitter). Enums: `DataType` (`:10`), `BinaryOpType` (`:93`),
`UnaryOpType` (`:169`), `Quantifier` (`:449`).

Nodes: `ColumnRef` (`:56`, table + column + data_type set during binding),
`Literal` (`:77`), `BinaryOp` (`:127`), `UnaryOp` (`:178`), `FunctionCall`
(`:196`, scalar or aggregate, with `is_aggregate`, `distinct`,
`within_group_key`), `CaseExpr` (`:224`), `InList` (`:245`), `BetweenExpression`
(`:261`), `Cast` (`:278`), `WindowExpr` (`:303`), `Extract` (`:332`), `Interval`
(`:354`), and the subquery-bearing nodes `SubqueryExpression` (`:457`),
`ExistsExpression` (`:478`), `InSubquery` (`:502`), `QuantifiedComparison`
(`:547`), plus `TupleExpression` (`:527`).

A shared set of expression-tree walkers lives at the bottom of the file and is
the single home for tree traversal (enforced by tests). Key ones:
`expression_children` (`:598`), `map_children` (`:641`, rebuilds via
`model_copy`), `column_refs` (`:713`), `split_conjuncts` (`:723`),
`combine_and` (`:737`), and `split_where_having` (`:792`, the single source of
truth for deciding which conjuncts are WHERE versus HAVING). `SUBQUERY_NODE_TYPES`
(`:585`) marks the four subquery nodes that generic walkers do NOT descend into,
because their inner plans belong to a nested scope.

### 3.4 Physical plan nodes (`plan/physical.py`)

Base class `PhysicalPlanNode` (`plan/physical.py:78`). Each node implements
`children()`, `execute() -> Iterator[pa.RecordBatch]`, `schema() -> pa.Schema`,
and `estimated_cost()`. It also carries a per-query merge engine handle
(`set_merge_engine` / `merge_engine`) and `apply_dynamic_filter(...)` for
semi-join reduction (default returns False; only `PhysicalScan` overrides it).

Single-source / remote-pushdown nodes:

- `PhysicalScan` (`:481`) - reads one table from one datasource, with filters,
  grouping, aggregates, ordering, limit folded in. Overrides
  `apply_dynamic_filter` to AND an IN-list of join keys into its filters.
- `PhysicalRemoteQuery` (`:1871`) - an entire same-source subtree rendered as
  one sqlglot AST and executed in a single round trip. This is the output of
  single-source pushdown.
- `PhysicalRemoteJoin` (`:1256`) and `PhysicalRemoteSetOp` (`:1972`) - a join or
  set operation kept entirely on one source.

Local merge-engine operators (used on the cross-source path):

- `PhysicalProjection` (`:702`), `PhysicalFilter` (`:943`), `PhysicalWindow`
  (`:868`), `PhysicalHashJoin` (`:982`), `PhysicalNestedLoopJoin` (`:1463`),
  `PhysicalHashAggregate` (`:1550`), `PhysicalSort` (`:1711`), `PhysicalLimit`
  (`:1766`), `PhysicalValues` (`:1822`), `PhysicalUnion` (`:1922`),
  `PhysicalSetOperation` (`:2071`), `PhysicalSingleRowGuard` (`:2126`),
  `PhysicalGroupedLimit` (`:2197`), `PhysicalLateralJoin` (`:2306`).
- CTE materialization: `PhysicalCTE` (`:226`), `PhysicalCTEScan` (`:280`),
  `PhysicalCTEMergeQuery` (`:303`).

Utility: `PhysicalExplain` (`:2441`, builds the EXPLAIN document without
executing) and `Gather` (`:2539`, an unimplemented placeholder).

Module-level helpers worth knowing: `to_source_sql` (`:185`, the one transpile
boundary - re-parses Postgres-form SQL and renders it in the source's dialect),
`_MERGE_LEFT_RELATION` / `_MERGE_RIGHT_RELATION` (`:62`-`:63`, the names under
which merge inputs are registered in DuckDB), `_require_engine` (`:424`),
`_streaming_reader` (`:439`), and `_table_from_batches` (`:219`).

### 3.5 Catalog (`catalog/`)

`Catalog` (`catalog/catalog.py:9`) is the metadata registry. It holds
`datasources` (name -> DataSource) and `schemas` keyed by the tuple
`(datasource_name, schema_name)`. `register_datasource` (`:20`) adds a source;
`load_metadata` (`:28`) introspects all sources to discover schemas, tables, and
columns; `get_table(datasource, schema, table)` (`:93`) is the lookup the binder
uses; `resolve_table` (`:111`) resolves a partial reference in three forms
(`ds.schema.table`, `schema.table`, `table`).

The metadata tree types live in `catalog/schema.py`: `Schema` (`:80`, a namespace
of tables), `Table` (`:37`, a list of `Column`s with `get_column` (`:63`,
case-insensitive)), and `Column` (`:11`, name + `DataType` + nullable). Tables
and columns carry read-only back-references to their parents, set by validators.

### 3.6 Config (`config/config.py`)

`Config` (`:56`) aggregates `DataSourceConfig` (`:12`), `OptimizerConfig`
(`:21`), `ExecutorConfig` (`:31`), and `CostConfig` (`:47`). `load_config`
(`:65`) reads YAML, rejects unknown top-level sections, and builds these models.
A `DataSourceConfig` holds the source `type` ("postgresql" / "duckdb" /
"clickhouse"), a `config` dict of connection params, and declared
`capabilities`.

---

## 4. Stage-by-stage detail

### 4.1 Wiring (how the pipeline is assembled)

The CLI (`cli/fedq.py`) builds everything. `cli` (`:549`) calls
`_prepare_runtime` (`:445`), which loads config, builds the catalog via
`_build_catalog` (`:481`), and constructs `FedQRuntime` (`:83`). The runtime
constructor (`:86`) instantiates `Parser`, `Binder(catalog)`,
`RuleBasedOptimizer(catalog)` (rules registered by `_register_optimization_rules`
at `:111`), `PhysicalPlanner(catalog)`, `Decorrelator()`, and
`Executor(executor_config)` (warmed up immediately), then wraps them in a
`QueryExecutor` with one processor: `StarExpansionProcessor`. `_build_catalog`
uses the factory `_create_datasource` (`:494`) to map a config type to a
connector class, calls `connect()`, registers it, and finally calls
`catalog.load_metadata()`.

`FedQRuntime.execute` (`:119`) simply delegates to `QueryExecutor.execute`.

### 4.2 Before-processors: SELECT * expansion

Entry point: `QueryExecutor._run_before_processors`
(`processor/query_executor.py:103`) runs each processor's `before_execution`.
The only processor is `StarExpansionProcessor` (`processor/query_preprocessor.py:701`),
which calls `QueryPreprocessor.preprocess` (`:68`). Using catalog metadata, it
rewrites every `*` and `alias.*` into explicit, qualified column references and
records a `ColumnMapping` (`processor/query_context.py:8`) of internal name
(e.g. `pg.users.id`) to visible name (`id`) per output column on the
`QueryContext`. It also rewrites static PIVOT into conditional aggregation and
inlines named windows. Invariant: no star survives into the parser; an
unresolvable star raises `StarExpansionError` (`query_preprocessor.py:21`).
After execution, `StarExpansionProcessor.after_execution` (`:714`) renames the
result columns from internal to visible names.

### 4.3 Parse (`parser/parser.py`)

Entry point: `Parser.parse_to_logical_plan` (called from
`QueryExecutor._parse_query`, `query_executor.py:110`); the core is
`Parser.parse` (`parser/parser.py:124`) -> `ast_to_logical_plan` (`:157`).
sqlglot parses the SQL using the custom `FedQPostgres` dialect (Postgres is the
canonical internal dialect). `_parse_one` (`:140`) rejects multi-statement
input. `ast_to_logical_plan` dispatches Select / Describe (EXPLAIN) / set
operations. `_convert_select` (`:222`) builds the plan clause by clause:
`_build_from_clause` -> `_build_where_clause` -> `_build_group_by_clause` ->
`_build_having_clause` -> `_build_select_clause` -> `_build_order_by_clause` ->
`_build_limit_clause`. Expressions convert through `_convert_expression`
(`:1297`).

This stage is aggressively defensive about silent drops. Frozensets such as
`SUPPORTED_SELECT_ARGS` (`:65`), `SUPPORTED_JOIN_ARGS`, `SUPPORTED_AGG_ARGS`, and
others list exactly which sqlglot args each builder consumes; anything else
raises `UnsupportedSQLError` via `_reject_unsupported_args` (`:261`). Window
functions in WHERE/GROUP BY/HAVING, simple CASE, BETWEEN SYMMETRIC, FETCH WITH
TIES, and similar shapes are all rejected here rather than mis-planned.

Output: an unbound `LogicalPlanNode` tree whose `ColumnRef`s have no types yet.

### 4.4 Bind (`parser/binder.py`)

Entry point: `Binder.bind` (`parser/binder.py:115`), dispatched per node type.
Binding resolves every table and column reference against the catalog, attaches
`DataType`s, and validates structure (for example
`_check_set_branch_arity` (`:181`) raises when UNION branches differ in width).

The heart of resolution is a scope chain. The binder keeps `_scope_stack`
(`:109`), a list of dicts mapping alias -> `Table`, one entry per enclosing query
block, plus `_cte_tables` (`:113`) for in-scope CTEs. Each relational binder
pushes the scope visible to its children (`_push_scope_for`, `:658`) and pops it
afterward. The single column resolver for the whole binder is
`resolve_in_scopes` (`:807`): a qualified reference resolves against the nearest
scope that defines its table (`_resolve_qualified`, `:824`), a bare one against
the nearest scope defining the column (`_resolve_unqualified`, `:845`), with
intra-scope ambiguity an error (`_match_in_scope`, `:855`). A reference that
resolves in NO scope raises `BindingError` (`:49`) - this is where an invalid
qualifier or a typo'd column name is caught (see section 9).

Subqueries bind through `SubqueryPlanBinder` (`:1066`), which carries the
enclosing scopes so correlated references resolve innermost-first. The single
expression dispatch `_bind_expr_dispatch` (`:887`) is shared by the top-level,
join, and subquery binders; only the column-leaf resolver and the
nested-subquery scopes differ. `_bind_scan` (`:208`) prunes a scan's
over-collected column list down to columns that table actually has.

Output: a fully bound `LogicalPlanNode` tree.

### 4.5 Decorrelate (`optimizer/decorrelation.py`)

Entry point: `Decorrelator.decorrelate` (`optimizer/decorrelation.py:1057`),
called from `QueryExecutor._decorrelate_plan` (`query_executor.py:120`). It
rewrites the bound plan so that NO subquery expression survives: EXISTS / NOT
EXISTS become SEMI / ANTI joins, IN / NOT IN (subquery) become NULL-aware SEMI /
ANTI joins, `op ANY/SOME/ALL` become SEMI / ANTI joins, and scalar subqueries
are hoisted into joins on the correlation keys (wrapped in `SingleRowGuard` for
cardinality safety, and `GroupedLimit` for a correlated LIMIT). Subqueries in a
projection or under an OR become boolean flag columns via a UNION of branches.
Classification is in `_semi_anti_parts` (`:1282`); scalar handling in
`_SubqueryPreparer.prepare_scalar` (`:399`).

When a correlation cannot be flattened (non-equality correlation crossing an
aggregate or LIMIT), it raises `DecorrelationError` (`:179`) internally, which is
caught and turned into the `LateralJoin` fallback (`:1625`). A same-source
lateral can later be pushed as `LEFT JOIN LATERAL`; a cross-source lateral
becomes `PhysicalLateralJoin`. After rewriting, `decorrelate` asserts via
`_raise_if_subquery_expression` (`:1678`) that no subquery node remains - a loud
guarantee that the rest of the pipeline never sees one.

### 4.6 Optimize (`optimizer/rules.py`)

Entry point: `RuleBasedOptimizer.optimize` (`optimizer/rules.py:1290`), called
from `QueryExecutor._optimize_plan` (`query_executor.py:124`). It applies its
registered rules iteratively to a fixed point (max 10 iterations). The rules,
registered in `_register_optimization_rules` (`cli/fedq.py:111`) in this order:

1. `PredicatePushdownRule` (`rules.py:65`) - pushes filters through projections,
   below joins (split by side, outer-join-safe), and into scans.
2. `ProjectionPushdownRule` (`:381`) - column pruning: collects required columns
   and trims scans and intermediate projections.
3. `AggregatePushdownRule` (`:1165`) - folds GROUP BY and aggregates into a
   single-source scan.
4. `OrderByPushdownRule` (`:764`) - pushes ORDER BY toward sources, rewriting
   keys through projections and joins.
5. `LimitPushdownRule` (`:551`) - pushes LIMIT/OFFSET toward sources when safe.

Every rule returns a new plan via `model_copy` and never mutates in place.
`JoinReorderingRule` (`:750`) is a reserved placeholder (cost-based reordering is
future work). `CostModel` (`optimizer/cost.py:31`) and `StatisticsCollector`
(`optimizer/statistics.py:8`) exist and estimate cardinality / selectivity; they
are consulted during physical planning for build-side choice, not yet for join
reordering.

Output: an optimized but still logical `LogicalPlanNode` tree.

### 4.7 Physical planning (`optimizer/physical_planner.py`)

Entry point: `PhysicalPlanner.plan` (`physical_planner.py:111`) ->
`_plan_node` (`:125`), called from `QueryExecutor._build_physical_plan`
(`query_executor.py:128`). The single most important step happens first:
`_plan_node` calls `SingleSourcePushdown.try_build(node)`
(`single_source_pushdown.py:113`) on the subtree. If the entire subtree lives in
one datasource and is renderable, `try_build` returns a `PhysicalRemoteQuery`
and planning of that subtree is done. This is the SINGLE-SOURCE path.

If the subtree spans sources (or cannot be rendered as one query), `_plan_node`
falls through to per-node dispatch, producing local merge operators. Join
algorithm selection is in `_plan_join` (`:530`): try a same-source
`PhysicalRemoteJoin` (`_try_plan_remote_join`); else extract equi-keys
(`_extract_join_keys`, `:820`), orient them to sides (`_orient_join_keys`,
`:674`), choose a build side (`_choose_build_side`, `:584`), mark a dynamic
filter on the probe-side scan, and build a `PhysicalHashJoin`; else fall back to
`PhysicalNestedLoopJoin`. Set operations route through `_plan_set_operation`
(`:854`); cross-source CTEs materialize through `_plan_cte` (`:461`); a
projection with window functions becomes `PhysicalWindow` (`_plan_projection`,
`:332`).

Output: a `PhysicalPlanNode` tree.

### 4.8 Execute (`executor/executor.py`)

Entry point: `Executor.execute_to_table` (`executor/executor.py:82`), called
from `QueryExecutor._run_physical_plan` (`query_executor.py:132`). It calls
`execute` (`:38`), which attaches the reused `MergeEngine` to every node via
`_attach_merge_engine` (`:13`) and then pulls `plan.execute()`, collecting the
Arrow batches into a `pa.Table` (falling back to an empty typed table when there
are no rows). Execution is pull-based and streaming: each operator's `execute`
yields `pa.RecordBatch`es.

If the physical plan root is a `PhysicalExplain` with JSON format,
`_run_physical_plan` short-circuits and returns the plan document dict from
`build_document` instead of executing.

### 4.9 After-processors

`QueryExecutor._run_after_processors` (`query_executor.py:139`) runs processors
in reverse. `StarExpansionProcessor.after_execution` renames the Arrow columns
from internal names back to the user-visible names recorded during expansion, so
the caller never sees an internal name.

---

## 5. SQL emission (`plan/emit/`)

There is exactly ONE expression-to-SQL converter, and exactly one transpile
boundary per source. The emit package builds a sqlglot `exp.Expression` and
renders it once with `ast.sql(dialect=...)` (`plan/emit/__init__.py`).

- `SqlglotEmitter` (`plan/emit/expressions.py:94`) is an `ExpressionVisitor` that
  lowers engine `Expression` nodes to sqlglot AST. It dispatches per node type
  (`visit_column_ref`, `visit_binary_op` via the `_BINARY_BUILDERS` allowlist at
  `:15`, `visit_function_call`, `visit_window_expr`, etc.). The exported entry
  is `expression_to_ast(expr, resolver)` (`:305`). Correlated subquery nodes
  raise here (`:268` onward), enforcing that decorrelation already removed them.
- The `resolver` argument is the ONLY thing that differs between the remote and
  merge paths. `ColumnResolver` (`plan/emit/resolver.py:15`) is the interface;
  `SourceResolver` (`:23`) emits quoted, table-qualified columns for a remote
  source; `MergeResolver` (`:45`) maps an engine `(table, column)` to the
  physical Arrow column name a merge relation actually carries.
  `CANONICAL_SOURCE_RESOLVER` (`:68`) is the singleton used by
  `Expression.to_sql()`.
- Clause builders in `plan/emit/clauses.py` (`select_expressions` `:21`,
  `order_by` `:30`, `group_by` `:89`, `apply_limit_offset` `:112`, plus the
  fragment helpers) take a resolver and call `expression_to_ast`, so the SAME
  builders serve both paths.
- The SELECT skeleton (FROM / SELECT / WHERE / GROUP / HAVING / DISTINCT / ORDER
  / LIMIT) is composed in ONE place: `clauses.assemble_select` (`:136`). A remote
  query is one SELECT over N relations; a single-table scan is the N=1 case and a
  same-source join/CTE/set-op subtree is the N>1 case, so both build their FROM
  and clause pieces and assemble through the same function. `PhysicalScan._build_ast`
  (`plan/physical.py:728`) already composes through it. (`SingleSourcePushdown`
  still builds its larger N>1 skeleton as a string and parses it back in
  `_finish`; converging it onto `assemble_select` so the whole remote path is
  AST-first is in-progress cleanup, not two different designs - the expression
  conversion and the transpile boundary are already shared.)
- `FedQPostgres` (`parser/dialect.py:16`) is the canonical internal dialect: a
  Postgres dialect that parses EXPLAIN into a native `exp.Describe` so the inner
  statement is never re-scanned from text. All plans render to Postgres form
  first; `to_source_sql` (`plan/physical.py:185`) re-parses that text through the
  source connection's `render_dialect` (postgres / duckdb / clickhouse) at
  execute time, the one place dialect divergence is resolved.

So: the remote path builds a Postgres-form AST with a `SourceResolver` and
transpiles to the source dialect; the merge path builds DuckDB SQL with a
`MergeResolver` keyed to the physical column names of the Arrow relations
registered in the merge engine. The expression conversion is identical.

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
   output alias via `_bind_sort_keys_for_aggregate` (`binder.py:347`). An unknown
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
   AST, and returns a `PhysicalRemoteQuery` (`plan/physical.py:1871`) whose
   `datasource_connection` is the `pg` `PostgreSQLDataSource`, `query_ast` is the
   built AST, and `output_names` is `["city", "n"]`.

7. Execute. `Executor.execute_to_table` pulls `PhysicalRemoteQuery.execute`
   (`:1894`). That calls `_sql()` (`:1904`), which renders the AST to Postgres
   text and runs it through `to_source_sql` (here a Postgres -> Postgres pass).
   The SQL sent to the source is one statement, equivalent to:

   ```sql
   SELECT "city" AS "city", COUNT(*) AS "n"
   FROM "public"."users"
   WHERE "age" > 30
   GROUP BY "city"
   ORDER BY "n" DESC
   LIMIT 10
   ```

   `PostgreSQLDataSource.execute_query` (`datasources/postgresql.py:258`)
   executes it (psycopg2 batches, or ADBC for Arrow-native streaming) and yields
   `pa.RecordBatch`es. The engine does no local joining, grouping, or sorting.

8. Return. The batches are assembled into a `pa.Table`; after-processors are a
   no-op (no star rename); the table is returned to the caller. The CLI prints it
   via `ResultPrinter` (`cli/fedq.py:131`).

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
   (`plan/physical.py:982`) over the two `PhysicalScan`s. The top
   `Projection` becomes a `PhysicalProjection`. Each `PhysicalScan` is rendered
   in its own source dialect: the Postgres scan as Postgres SQL, the DuckDB scan
   as DuckDB SQL (via `render_dialect` and `to_source_sql`).

5. Execute. `Executor.execute` attaches the shared `MergeEngine` to every node,
   then pulls the root. The `PhysicalHashJoin` executes through the merge engine
   (`_execute_inner_merge`, `plan/physical.py:1047`):
   - It materializes the build side fully (`_materialize_build`). Say the build
     side is the smaller `pg.users` scan; that scan sends
     `SELECT "id","name","age" FROM "public"."users" WHERE "age" > 30` to
     Postgres and returns Arrow batches.
   - It performs semi-join reduction: `_reduce_probe_from_build` (`:1080`)
     collects the build side's distinct join keys and calls
     `probe_node.apply_dynamic_filter(...)`. The probe `PhysicalScan` ANDs an
     `id IN (...)` filter into its own SQL, so DuckDB only returns matching
     `orders` rows. This is the dynamic-filter pushdown the engine relies on to
     keep the cross-source join cheap.
   - It registers the two Arrow inputs in DuckDB under `_MERGE_LEFT_RELATION`
     ("in_left") and `_MERGE_RIGHT_RELATION` ("in_right"), builds the equi-join
     SQL with a `MergeResolver`, and calls `MergeEngine.run(sql, inputs)`
     (`executor/merge_engine.py:31`). DuckDB registers each Arrow table on a
     fresh cursor (`_stream`, `:47`), runs the join vectorized, and streams the
     result back as `pa.RecordBatch`es.
   - `PhysicalProjection` evaluates `u.name, o.total` over those batches.

6. Return. The streamed batches are collected into a `pa.Table` and returned.

So the multi-source path scans each source with its own pushed-down SQL (filters
and dynamic join-key filters included), then runs the join locally in DuckDB.
Other cross-source operators behave the same way: `PhysicalHashAggregate`,
`PhysicalSort`, `PhysicalUnion`, `PhysicalSetOperation`, and `PhysicalWindow`
each materialize or stream their inputs into the merge engine, register them
under stable relation names, and run a DuckDB SQL statement built by the shared
clause builders.

---

## 8. Datasources (`datasources/`)

`DataSource` (`datasources/base.py:63`) is the abstract connector interface. Key
abstract methods: `connect` (`:89`), `disconnect` (`:94`), `get_capabilities`
(`:99`), `list_schemas` / `list_tables` (`:104` / `:109`), `get_table_metadata`
(`:121`), `get_table_statistics` (`:134`), `execute_query` (`:149`, returns
`Iterator[pa.RecordBatch]`), and `get_query_schema` (`:161`). The class attribute
`render_dialect` (`:71`, default "postgres") names the sqlglot dialect the engine
transpiles pushed SQL into for this source. Capabilities are enumerated by
`DataSourceCapability` (`:12`). Metadata/statistics value types
(`ColumnMetadata`, `TableMetadata`, `ColumnStatistics`, `TableStatistics`) are
`StateModel`s defined in the same file.

Connectors:

- `PostgreSQLDataSource` (`datasources/postgresql.py:44`) - uses a psycopg2
  `ThreadedConnectionPool`; `execute_query` (`:258`) routes to a psycopg2
  fetch-many path (`_execute_query_psycopg2`, `:264`, building Arrow arrays from
  rows via an OID->Arrow type map) or, when `driver: adbc` is configured, an
  Arrow-native ADBC streaming path (`_execute_query_adbc`, `:302`). Inherits
  `render_dialect = "postgres"`.
- `DuckDBDataSource` (`datasources/duckdb.py:19`) - a single in-memory or
  file-backed connection; `execute_query` (`:146`) runs the query and yields
  Arrow batches from the materialized result. `render_dialect = "duckdb"`.
- `ClickHouseDataSource` (`datasources/clickhouse.py:68`) - HTTP via
  clickhouse-connect; `execute_query` (`:200`) streams Arrow batches from
  `query_arrow_stream`. `render_dialect = "clickhouse"`. (Note: it is importable
  from its module and usable via config, but not re-exported in
  `datasources/__init__.py`.)

All connectors return Arrow `RecordBatch`es, which is the common currency that
lets the merge engine combine results from heterogeneous sources.

---

## 9. Correctness posture

Two rules from the project doctrine are enforced structurally across the
pipeline. They are intentionally redundant - the same class of mistake is caught
at several stages.

Never fail silently. State types subclass `StateModel` (`model.py:28`) with
`extra="forbid"` and a key-validating `model_copy`, so a dropped or mistyped
field raises at construction or copy. Plan transformations always use
`model_copy(update=...)`, never field-by-field reconstruction, so a field can
never be silently lost (pinned by `tests/test_node_field_preservation.py`). The
parser uses allowlists (`SUPPORTED_*` frozensets) plus `_reject_unsupported_args`
(`parser/parser.py:261`) and raises `UnsupportedSQLError` (`parser/errors.py:4`)
on any SQL clause it does not consume, rather than ignoring it. The expression
emitter dispatches via allowlist tables and raises on an unknown operator. The
binder's dispatch (`bind`, `binder.py:115`) ends in
`raise BindingError(...)` for an unknown node type, and the decorrelator asserts
no subquery node survives (`_raise_if_subquery_expression`,
`decorrelation.py:1678`).

An invalid query MUST raise. This is the most severe failure mode: returning
rows for an invalid query manufactures a wrong answer. The single column
resolver `resolve_in_scopes` (`binder.py:807`) is the choke point: a qualified
reference to a table the query does not name raises `BindingError`
(`_resolve_qualified`, `:824`), a bare column that resolves in no scope raises
(`_resolve_unqualified`, `:845`), and a name ambiguous within a scope raises
(`_match_in_scope`, `:855`). Because all binders (top-level, join, subquery)
funnel column resolution through this one method, there is no path by which a
bogus qualifier or typo'd column binds to a relation the query did not name -
it fails at bind time, before any source is queried. Set-operation arity
mismatches likewise raise at bind (`_check_set_branch_arity`, `:181`).

Exceptions are caught in exactly one place: the CLI REPL (`FedQRepl`,
`cli/fedq.py:293`), which catches and displays `BindingError`,
`UnsupportedSQLError`, `StarExpansionError`, parse errors, and DuckDB errors so
the user sees them. Everywhere else, exceptions propagate. A crash never ships a
lie.
