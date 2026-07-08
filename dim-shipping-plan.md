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

ASCII only. Status: Phase A DONE (fedqrs 082e4c7). Phase B DONE 2026-07-08
(federated-query, engine rebuilt for the Ship variant). Correct 99|0|0 at
SF0.1/SF1/SF10 and TPC-H pg-dims 22/22; net perf POSITIVE at SF10 (70.6s
vs 73.1s no-ship). q39 SF10 2.5s -> 1.6s, SF1 776ms -> 177ms; q59 -0.96s.

The blocker was resolved by SEEDING the island schema, not probing: the
island reads a temp table that lives only on the engine's pinned
connection, so the python-side LIMIT-0 probe cannot see it. Key facts that
made this safe: the IR ships NO column types to Rust (Rust reconciles real
types from executed batches), and python only needs the island's column
NAMES. So PhysicalRemoteQuery/PhysicalScan gained an optional
seeded_schema field; DimShippingRule seeds it from the pure cross-source
plan of the same subtree (plan_without_shipping), whose schema python has
already computed correctly.

WHAT LANDED:
- seeded_schema on PhysicalScan + PhysicalRemoteQuery (schema() returns it,
  skips the probe). tests/test_seeded_schema.py.
- PhysicalShipment plan node (mirrors CTE): table, datasource, body, child.
  Emitter _emit_shipment in rust_ir emits body binding -> ship step ->
  child island (order load-bearing). tests/test_ship_emit.py.
- optimizer/dim_shipping.py DimShipping, hooked in PhysicalPlanner._plan_node
  after try_build fails (guarded by self._shipping_enabled). Substitutes each
  foreign leaf scan with a synthetic duck temp Scan (same alias+columns,
  schema_name='temp'), re-runs single_source pushdown to collapse the now-
  local subtree into ONE island, seeds it, wraps in nested PhysicalShipments.
  tests/e2e_pushdown/test_dim_shipping.py.
- Kill switch: FEDQ_DIM_SHIPPING=0 disables.

GATES (all reliable signals; the cost model CANNOT estimate aggregate
collapse so no cardinality gate is possible):
1. Shape: subtree is only INNER joins + {Scan,Filter,Projection,Aggregate,
   Sort,Limit,SubqueryScan}. Anything else declines.
2. _collapses_via_aggregate: the subtree root (under row-preserving wrappers)
   must be a PLAIN Aggregate (grouping_sets=None). This is the key gate -
   shipping wins only when little crosses the boundary. A bare join/scan ships
   the whole joined fact for no gain (q22-below-rollup, q38/q87 DISTINCT
   branches); a ROLLUP/CUBE collapses poorly in one island (q14/q22/q67).
3. Local (ship target) must be DuckDB (engine ships temp tables into duck only).
4. Cost: local(fact) >= 100k (SHIP_LOCAL_FLOOR), sum(foreign) <= 200k
   (SHIP_ROW_BUDGET; excludes q38/q87's ~500k customer), local/foreign >= 20.
   Estimates come from scan annotation else a STATISTICS-BACKED cost estimate
   (a defaulted size = None = decline). Rule is self-sufficient (does not
   depend on join ordering having annotated the scans).
5. Fallback plan must NOT contain a window (collapsing window+grouping into
   one remote SELECT bypasses the two-stage split and won't bind - q86).
6. _outputs_match: the island's output_names must equal the pure plan's
   schema names exactly, else decline (an unprojected derived table collapses
   to a degenerate out=[] island - q34; declining there lets shipping fire one
   level down at the clean Aggregate).

RESIDUAL: q23 still ships (plain aggregate that does NOT collapse - aggOut
~= aggIn ~30M) and regresses ~0.8s; undetectable without aggregate-collapse
cardinality. Outweighed by the wins. To fix, teach the cost model NDV-based
group-count estimation, then gate on island output << fact.

ORIGINAL PLAN (historical) below.

CONTEXT SHIFT: with CSE + multi-injection landed, SF10 totals reached
1.00x vs DuckDB - parity. Dim shipping's remaining value is q39 (2.4s,
5.8x) and the long tail, not the totals.

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
