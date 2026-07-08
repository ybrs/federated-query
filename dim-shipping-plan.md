# Dim shipping - plan

Goal: stop transferring fact-scale rows when the blocker is a SMALL
dimension on the other source. q39 must move 26.5M inventory rows to the
coordinator because its aggregate groups by d_moy - a date_dim (pg) column
- so the duck island cannot swallow the join+aggregate. Shipping the
FILTERED dimension (366 rows) INTO DuckDB as a temp table makes the whole
join+aggregate a duck island; only the aggregate output (~200k rows)
returns. This generalizes: any fact-heavy subtree blocked from collapsing
by one or two small foreign dims (q39, parts of q70/q75, the remaining
q23/q14 costs).

ASCII only. Status: IN PROGRESS.

## Mechanism

1. IR: new step {op: "ship", datasource, input: <binding>, table: <name>}.
   The engine CREATEs a TEMP TABLE named <name> on the target source's
   connection from the binding's Arrow batches, mirroring the existing
   temp-table key delivery (connectors already ingest Arrow into duck temp
   tables for key joins; extend from single-column keys to full payloads).
   Temp tables are per-connection; the engine's duck instance is shared
   across the query (fedqrs 6b67235), so later scans in the SAME query see
   it. Names are per-query unique (__fedq_ship_N); the step DROPs first
   (defensive) and the table dies with the query's connection reset.
2. Planner: a new decision in the physical planner (where islands form).
   When a cross-source INNER equi join blocks a single-source collapse,
   and the FOREIGN side's output estimate is under a budget
   (SHIP_ROW_BUDGET, e.g. 100k rows), and the LOCAL side's subtree
   (including the join's other inputs and the aggregate above, if any) is
   otherwise single-source: replace the foreign subtree with a synthetic
   Scan(datasource=<local source>, table=__fedq_ship_N) whose schema is
   the foreign subtree's output, record (binding-producer subtree, name)
   on a plan-level shipping list, and let the EXISTING single-source
   pushdown collapse the now-local subtree.
3. Emitter: for each shipping entry, emit the foreign subtree normally
   (its binding), then the ship step, before the island's source_scan.
4. Cost gate: ship only when the local side's estimated rows exceed the
   foreign side's by a factor (say 50x) AND the foreign output estimate is
   known and under budget - unknown estimates DECLINE (a mis-shipped big
   dim floods the source); the reduction machinery remains for those.

## Correctness

- Only INNER equi joins with deterministic foreign subtrees ship (the
  foreign subtree is evaluated ONCE, exactly as the coordinator join
  would).
- The shipped table carries the foreign subtree's OUTPUT columns under
  their physical names; the synthetic scan exposes the same aliases, so
  every parent reference resolves unchanged.
- Snapshot semantics match the status quo: the foreign rows are read once
  and joined; the coordinator path reads them once and joins.
- Temp-table name collisions are impossible within a query (counter) and
  across queries (per-connection namespace + DROP IF EXISTS first).

## Phases

A. Engine: ship step (duck target first; pg ingest via ADBC later if a
   profile demands it) + tests in ir.rs parse + a pytest e2e reading a
   shipped dim from a duck island.
B. Planner: the replacement decision + cost gate, feature-complete for
   the q39 shape (dim under fact join under aggregate).
C. Measure q39/q75/q23/q14 at SF10; tighten the budget from data.

## Verification gates

pytest suite; staged SF10 engine+compare (cluster: 39, 75, 23, 14, plus
the full 99); SF0.1/SF1 sweeps; TPC-H fedpgduck (its dims are pg-side -
the gate must show no orientation damage).

## Rollback

Single commits per phase in both repos; the planner decision is one rule
behind the cost gate - reverting the planner commit restores today's
plans while the engine step stays inert.
