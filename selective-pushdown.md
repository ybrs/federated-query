# Selective / Partial Join Pushdown (design note)

Status: **future work, not yet implemented.** This note explains why
`PhysicalRemoteJoin` (`federated_query/plan/physical.py`) is intentionally kept
even though the planner no longer constructs it, and what machinery a real
selective-pushdown capability needs.

## Where we are today (greedy whole-subtree pushdown)

The G1 single-source generator
(`federated_query/optimizer/single_source_pushdown.py`, surfaced as
`PhysicalRemoteQuery`) is invoked at the top of `PhysicalPlanner._plan_node`. It
one flat remote `SELECT`. Anything it cannot push (cross-source, a node it can't
render, a source without the `JOINS` capability) is declined and the planner
falls back to local operators.

This greedy "push everything you can" policy is the right default: pushing
computation down minimises data movement, which is the dominant cost in a
federated query. As a consequence, the old binary `PhysicalRemoteJoin` (two
does that and more. The planner produces zero `PhysicalRemoteJoin` nodes today
(verified by instrumenting both construction sites and running the full suite:
0 hits, failure count unchanged).

## Why "push everything" is a heuristic, not a law

The greedy policy assumes the only participants in a join are remote sources. It
breaks down the moment **part of the data lives on the coordinator**, or when
**cost** disagrees with pushing.


The data to join against is on our side, not the remote's:

- a small dimension/lookup table cached on the coordinator;
- an app-supplied `IN`-list or `VALUES` table, or an uploaded file;
- the materialized result of an earlier query, or a CTE we chose to compute once
  and reuse;
- a source that has no SQL engine at all (REST / CSV / KV / columnar file).

The remote cannot see those rows, so the join cannot be pushed wholesale. Two
strategies, both of which need pieces we don't have yet:

- materialize the local side, push the remote scan, hash-join locally; or
  ship the small side's join keys into the remote scan as
  `WHERE key IN (<keys>)`. This is a *dependent* join node, distinct from
  `PhysicalRemoteJoin`.

### 2. The query-accelerator case (the primary motivator)

When we run as a **query accelerator** we deliberately cache large tables on our
side. A query then mixes cached-local tables with remote ones, e.g. a 3-table
join where one (large) table is already cached locally. Here we want to push
*partial* pushdown of a single logical join, not all-or-nothing. This is exactly
the shape `PhysicalRemoteJoin`'s per-side SQL building was designed around, and
why it is retained.

### 3. Cost says pushing is a pessimization

Pushing down is usually the win, but not when:

- the remote join *fans out* cardinality and we would transfer a huge
  intermediate, whereas independent filtered/aggregated scans plus a local join
  move far less data;
- the remote is slow / rate-limited / overloaded and the coordinator has spare
  CPU (offload the work);
- the remote optimizer picks a bad plan (skew, missing stats) and we can do
  better locally.

Deciding to *decline* a pushable join is a cost-based decision the planner
cannot currently make.

### 4. The join needs coordinator-only logic

The join predicate uses a UDF / geo / ML-inference function only available
locally, or one input is produced by a window/recursive-CTE step the source
cannot run. The join must stay local.

### 5. Mixed-source N-way placement

adjacency, not by source. This needs join reordering driven by source placement
and cost.

## What's actually missing

None of the above wants "binary same-source remote join." They want three things
the engine does not have yet:

   cached-table / `VALUES` node that can sit under a join.
2. **A cost / placement decision layer** that can choose *not* to push a pushable
   join, emitting a local `PhysicalHashJoin` over `PhysicalRemoteQuery` inputs
   (the join operator already accepts arbitrary inputs; the gap is the decision).
3. **A bind / lookup (dependent) join node** for the dynamic-filtering strategy in

G9 v1 (dynamic filtering for INNER equi-joins, bare-scan probes, single-column
keys, filter-heuristic build-side selection) is done. The remaining work is
tracked as a **G9 v2 / v3 checklist** under the G9 entry in
`TODO-phase7-review.md`: v2 = coverage/correctness (inject into pushed-query
probes, composite keys, more literal types, see filters inside pushed
subqueries, config threshold, safe outer cases); v3 = the cost-based /
cost-based build-side selection, range pushdown for non-equi joins).

`PhysicalRemoteJoin` is retained as the reference for per-side remote SQL
generation that partial pushdown (case 2, the accelerator) will reuse. It is a
without sign-off.
