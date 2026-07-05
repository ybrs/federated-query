# Join-order experiment: findings

Question we set out to answer: **before building a cost-based optimizer, prove
that join reordering actually changes our timings.** The engine executes joins
strictly left-deep in the user's FROM order with no reordering
(`enable_join_reordering` is a dead config flag, read nowhere). We took the four
join-order-sensitive offenders from the fair PostgreSQL + DuckDB federated run
(`fedpgduck`) and ran each as written vs. hand-reordered, through the same
engine, checked against DuckDB.

Reproduce: `python benchmarks/tpch/experiments/joinorder_experiment.py --scale 1`
(reports land in `../reports/joinorder-<commit>-sf1.md`).

## SF1 result (6M lineitem)

| Query | Orig (ms) | Reordered (ms) | Speedup | vs DuckDB (orig -> reord) |
| --- | --- | --- | --- | --- |
| q08 | 5130 | 548 | **9.4x** | 82x -> 8.8x |
| q09 | 19439 | 3338 | **5.8x** | 180x -> 31x |
| q05 | 2763 | 2556 | 1.1x | 47x -> 44x |
| q07 | 2867 | 2635 | 1.1x | 74x -> 68x |

## What it tells us

1. **Join ordering is a large, real win where the FROM order forces a
   cross-product.** q08 and q09 begin with `part x supplier`, two tables that
   share no predicate - a 200k x 10k cartesian product (the exact shape that
   OOM-killed q09 at SF1 in the Parquet cells). Reordering so a filtered table
   drives into `lineitem` along predicate edges removes the cross-product and
   cuts 5.8-9.4x. Our worst queries go from 80-180x DuckDB to 9-31x.

2. **It must be COST-BASED, not a heuristic.** q05's join graph is cyclic
   (`customer-supplier` directly on `nationkey`, and also via
   `orders/lineitem`). A naive "small dimensions first" reorder joined customer
   to supplier early and built a ~60M-row same-nation intermediate: **10x SLOWER
   than the original** (2.7s -> 42.6s). Only joining `customer` last, carrying
   both its join conditions, avoids it. Picking the order needs cardinality
   estimation; a rule that just reshuffles tables can make things much worse.

3. **Join order is necessary but not sufficient.** q05 and q07 barely move
   (1.1x) once the cross-product cases are excluded - their FROM order is already
   connected. Their remaining 44-68x gap is elsewhere (projection pushdown does
   not prune the `lineitem` scan; cross-source data movement). A cost-based join
   optimizer fixes the catastrophic queries; the scan/pushdown work is a
   separate lever for the rest.

## Implication for the optimizer

Build a cardinality-driven join-ordering pass (statistics -> cost model ->
enumeration, e.g. DP up to `max_join_reorder_size`, greedy above it) whose first
job is to never produce a cross-product when a connected order exists. That
single property recovers most of the gap on the queries that currently blow up.
