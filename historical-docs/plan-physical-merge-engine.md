# Plan: Physical Merge Engine (DuckDB as the local execution engine)

Status: **planned, not started.** Owner-requested; top priority for the
optimization phase.

## Problem

Profiled on a real 3-table cross-source `count(*)` (total ~24.5 ms): the
PostgreSQL fetch of 11,041 rows was ~5 ms (~20%); the **local execution was
~19.5 ms (~80%)**. Our local physical operators run **row-at-a-time in Python**:
`PhysicalHashJoin` builds a `dict` keyed by per-row `.as_py()` tuples, probes
row by row, and `_join_rows` allocates a fresh one-row `RecordBatch` per matched
row. The same shape is in `PhysicalHashAggregate`, `PhysicalUnion`,
`PhysicalSetOperation`, `PhysicalGroupedLimit`, `PhysicalSort`, etc. For any
query whose pushdown leaves a non-trivial number of rows to combine locally,
this Python row loop dominates.

## Goal

Replace the row-at-a-time local operators with a **vectorized local engine**:
an in-memory **DuckDB** "merge engine" that the local operators hand their Arrow
streams to. DuckDB is a world-class vectorized, multi-threaded, out-of-core
engine with correct SQL semantics for joins (all types), aggregates, sorts, set
streams lazily and returns Arrow streams.

Name: the local engine that *merges* the Arrow streams coming back from the
remote sources.

## What does NOT change (important)

Nothing upstream of execution:
- parser, binder, **decorrelation**, logical optimizer, physical planner,
- The logical and physical **plan trees stay exactly as they are.**

turns its child Arrow streams into its output Arrow stream. The decorrelation
already produced the join/semi/anti nodes and their conditions; we just *render
those existing conditions to SQL* and let DuckDB run them. No re-homing of
anything.

## Key facts (verified)

  1000-batch reader pulled only 7 batches. So we pass streams, we do **not**
  materialize inputs into `pa.Table`.
  to the algorithm (you must read all build rows to build the table); DuckDB
  buffers it internally on the smaller side and streams the probe. This is the
  floor for *any* hash join.
- In-memory DuckDB **spills to disk** under `memory_limit`, into
  `temp_directory` (default `.tmp`; set explicitly for us). Out-of-core works.
- DuckDB releases the GIL and runs multi-threaded.

## Architecture

```
```

- A **coordinator DuckDB** connection (in-memory), separate from the DuckDB
  *datasources*. Configured with `temp_directory` and `memory_limit`.
- A local operator's `execute()`:
  1. wrap each child operator's batch iterator as a `pa.RecordBatchReader`;
  2. `con.register("in0", reader0); con.register("in1", reader1); ...`;
  3. build a small SQL statement over those registered names from the node's
     own fields (join type + condition, group-by + aggregates, sort keys, ...);
  4. `for batch in con.execute(sql).fetch_record_batch(): yield batch`;
  5. unregister the inputs.
- Inputs stream in; the result streams out; DuckDB buffers only what the
  algorithm requires (build side / full aggregate state / full sort), spilling
  if it exceeds `memory_limit`.

## Approach: per-operator swap (surgical), not plan translation

Start by swapping individual operators' `execute()`. The plan tree is unchanged;
each node independently runs its piece in DuckDB. This is incremental and

(A later, optional optimization is *subtree fusion*: translate a maximal local
subtree into one DuckDB query to cut round-trips and intermediate copies. Bigger

## The genuinely fiddly bits (all execution-local, not plan changes)

   We control the `SELECT`, so alias columns explicitly to match what the parent
   joins with duplicate names.
2. **Rendering conditions to SQL.** Join keys/conditions, group-by/aggregate
   existing node fields via the same `Expression.to_sql()` path used for
   pushdown. The one careful case: an ANTI join whose condition carries the
   render *that exact condition*; DuckDB evaluates it correctly. No decorrelation
   change.
3. **The dynamic-filter / semi-join-reduction hook (G9).** Today
   `PhysicalHashJoin._maybe_reduce_probe` reads the build keys and injects
   `WHERE key IN (...)` into the probe-side *remote* scan *before* probing. That
   is a *remote pushdown* optimization (reduce what the source sends), separate
   Python, which means reading the build side once; then DuckDB reads build again
   for the join. Options: (a) accept the double-read of the (small) build side;
   (b) materialize the build once and feed the same Arrow to both the key
   extraction and DuckDB; (c) push v2.1 so the remote returns few rows and the
   local join is trivial regardless. Decide during the spike.
   keys keep native types and compare correctly. Until P3 lands, both sides must
   already agree on types (today they do, via the lossy-but-consistent coercion).
5. **Empty results / schema.** DuckDB must emit the right output schema even for
   zero rows; ensure the registered readers carry correct schemas and the
   `SELECT` projects deterministically.

## Coordinator lifecycle

- One coordinator DuckDB per query execution (created in the executor /
  `QueryExecutor`), or a small pool. Configure `temp_directory` and
  `memory_limit` from `ExecutorConfig`.
- Unique registration names per node (e.g. by node id) to avoid collisions in
  nested joins; unregister after use.
- Keep it strictly separate from DuckDB *datasource* connections.

## Sequencing

   child streams, run `SELECT <aliased cols> FROM in0 <type> JOIN in1 ON <cond>`,
   stream the result. Wire `temp_directory`/`memory_limit`. Validate correctness
   and speed against the suite.
2. Extend to all join shapes: LEFT/RIGHT/FULL/SEMI/ANTI, non-equi, the
   null-aware anti condition. Preserve the G9 dynamic-filter hook (bit #3).
3. Migrate `PhysicalHashAggregate` (group-by + global), `PhysicalSort` (incl.
   NULLS placement), `PhysicalUnion`/`PhysicalSetOperation`, distinct,
   `PhysicalGroupedLimit`.
4. Leave trivial vectorized ops as-is or use native Arrow (`Table.slice` for
5. Decide whether `PhysicalNestedLoopJoin` (non-equi / cross) also goes to

## Risks / open questions

  in `ExecutorConfig`; document.
- **Correctness guard:** the 116 decorrelation e2e tests encode exact SQL NULL
  migration; they are the safety net.
- **pyarrow.compute as an alternative kernel** for some operators (join/agg/sort
  all exist there too, ~0.4 ms for the join). DuckDB is preferred for full SQL
  semantics (esp. null-aware anti and set ops) and out-of-core; pyarrow is an
  option for the trivially-safe ops if we want to avoid a DuckDB round-trip.

## Expected payoff

For the profiled query: the ~19.5 ms local row-loop collapses to a vectorized
DuckDB join (~sub-ms to low-ms), so the query becomes fetch-bound (~5 ms) and
then, combined with **G9 v2.1** (push the probe keys so the remote returns ~1
row instead of 11k), effectively trivial. More broadly, every cross-source query
whose local step is non-trivial stops being Python-row-bound.
