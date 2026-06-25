# Pushdown Optimization Status

**Updated:** 2026-06 · **Branch:** `phase8` · **Suite:** 809 passed / 0 failed / 0 xfailed

> This supersedes the earlier 2025-11 snapshot (5 individual pushdown rules vs
> `test_pushdown_real_e2e.py`). The live, detailed handoff is `TODO-next.md`;
> the decorrelation north-star + remaining gaps are in `decorrelation-gaps.md`.

## How pushdown works now

The planner tries, top-down, to render the **largest same-source subtree** as a
single remote SQL query via `optimizer/single_source_pushdown.py`
(`SingleSourcePushdown.try_build` → `PhysicalRemoteQuery`). Anything that can't
be expressed as one source query (cross-source boundaries, unsupported nodes)
falls back to structural planning, where local set-based work runs in the
in-memory DuckDB **merge engine** (`executor/merge_engine.py`), never in
hand-rolled Python row loops.

## What pushes to a single source (one remote query)

- **Filters** (`WHERE`) and **projections** (column pruning), including computed
  projections (`UPPER`, `||`, `*`, `CASE`, `CAST`).
- **Aggregates** + `GROUP BY` + `HAVING`.
- **Joins** of every shape — N-way, `FULL OUTER`, self-joins, non-equi / `OR` /
  computed conditions, and the `SEMI`/`ANTI`/`LEFT` joins that decorrelation
  produces from `EXISTS`/`IN`/`ANY`/`ALL`/scalar subqueries. **No subquery
  expression is ever re-created** in the pushed SQL ("No Subqueries in the
  Physical Plan").
- **Derived tables** (`FROM (SELECT …) AS t`), **`LATERAL`** (same-source), and
  **set operations** (`UNION`/`INTERSECT`/`EXCEPT`) as a subquery body.
- **CTEs** (`WITH`, incl. `RECURSIVE`) — a same-source `WITH` is pushed whole as
  one remote `WITH` statement.
- `ORDER BY` / `LIMIT` (incl. Top-N folded onto a scan).

## What runs locally in the merge engine (cross-source)

Each operator materializes its inputs as Arrow once, registers them, and runs
SQL in DuckDB (see the per-operator audit in `TODO-next.md`):

- `PhysicalHashJoin`, `PhysicalNestedLoopJoin` (all join shapes), with
  **cross-source dynamic filtering / semi-join reduction** (build-side keys
  pushed into the probe as an `IN`/range filter).
- `PhysicalHashAggregate`, `PhysicalSort`, `PhysicalUnion`,
  `PhysicalSetOperation`, and `PhysicalGroupedLimit` (a per-key limit pushed as a
  `ROW_NUMBER() OVER (PARTITION BY …)` window).
- `PhysicalLateralJoin` (cross-source dependent join, with domain reduction) and
  `PhysicalCTE` / `PhysicalCTEScan` / `PhysicalCTEMergeQuery` (cross-source CTEs:
  materialize-once producer, or whole-`WITH` recursion in the merge engine).

Python paths remain only as a fallback when **no** merge engine is attached, or
for genuinely un-renderable expressions (e.g. `SUM(a*b)` in a `GROUP BY` key) —
sound degradations, not the default.

## Not yet pushed / out of scope (Phase 9+)

- General dependent-join decorrelation for the remaining fail-fast subquery
  shapes — see `decorrelation-gaps.md`.
- Cross-source correlated-subquery fallback (old "cluster D").
- Date/time function breadth, aggregate `FILTER`, `NATURAL`/`USING` joins.
- Cost-based plan selection / join reordering (`rules.py` has the stub).
