# Performance exploration: why fedpgduck SF1 is 2.26x the DuckDB oracle

Date: 2026-07-06. Engine: fedqrs a6f052c (post duck-connection-reuse 6b67235).
Everything below is MEASURED on this machine; no estimates without a number
behind them. Benchmarks: `benchmarks/tpch`, fedpgduck cell (dims on PG,
facts on DuckDB), SF1 = 6M lineitem. Oracle = DuckDB with the same split via
its postgres connector.

Headline: ours 2456ms vs oracle 1086ms across the 22 queries (2.26x).
All 22 correct.

## Tools built for this

- `FEDQ_PROFILE=1` pipeline stage timers (parse/bind/decorrelate/optimize/
  plan/execute). NOTE: its operator instrumentation is broken since the
  Pydantic migration (`profiling.py:70` assigns `node.execute` on a
  StateModel and raises); only the stage timers were used.
- `FEDQRS_PROFILE=1` (fedqrs a6f052c): one stderr line per engine step -
  elapsed ms, output rows, datasource, fragment kind / scan SQL prefix.
- Scratchpad isolation scripts (profile_fedpgduck.py, pg_transfer_bench.py,
  pg_decode_decompose.py, island_tests.py) - see Repro at the end.

## Exonerated (measured, NOT the problem)

- tokio: `Runtime::new()` 0.18ms per query; `block_on` 0.17us.
- DataFusion fragments: q09's coordinator join shape (orders 1.5M x reduced
  lineitem 319k, int keys): duckdb 14.9ms, our fragment ~15ms. On par.
- Duck -> Arrow export: 1.5M rows x 2 int cols = 18ms. Row count and width
  are what matter, not export mechanics.
- DuckDB connection cost: fixed by 6b67235 (7-10ms/fetch -> 0.2ms cursor).

## Where the time goes (SF1, per-query decomposition)

Split of our 2456ms: Python planning 227ms (9%) + Python IR build 174ms (7%)
+ Rust execution 2050ms (83%).

Worst ratios and their step-trace attribution:

| query | ours | oracle | dominant steps (from FEDQRS_PROFILE) |
| ----- | ---- | ------ | ------------------------------------ |
| q07 | 222 | 43 | injected_scan duck 93ms shipping 1,828,450 rows (no-op supplier-key filter), joins on that bulk |
| q10 | 255 | 81 | source_scan pg customer 7 cols 155ms (single ADBC stream, whole table) |
| q13 | 218 | 101 | source_scan duck orders incl o_comment 98ms + nested_loop LEFT join 85ms |
| q05 | 169 | 45 | injected_scan duck island 58ms/910k rows (same no-op supplier keys) + pg customer 31-40ms |
| q09 | 246 | 97 | island broken: orders 1.5M + partsupp 800k pulled to coordinator, duck x duck joins there 45.8+42.9ms |
| q03 | 105 | 33 | injected duck island 67ms for 30k output rows (wrapper blocks pushdown; 21ms when pushed inside) |
| q02 | 113 | 30 | 25 serialized steps; ~5 pg round trips at 0.5-13.5ms each |

## F1. Python planning + IR build: 401ms, LINEAR in plan size

Question: does it grow exponentially or linearly with query complexity?
Answer: LINEAR (in relations + predicate size), with per-rule constants.
Measured pyplan+IR (ms) vs base relations:

  1 rel: q06 5.1, q01 7.8 | 2 rel: q13 7.1, q12 8.8, q14 8.6, q19 18.8
  3 rel: q03 13, q11 20, q18 17 | 4 rel: q10 16, q21 30.8
  6 rel: q05 19.5, q09 24.3, q07 52.7 | 8 rel: q08 33.7, q02 31.2

Roughly 4-5ms per relation, no blow-up. The Selinger DP is capped
(max_join_reorder_size=10; 2^8 subsets at worst here) and never dominates.
cProfile of q07's 41ms pipeline: optimize 17ms (640 transform_children
walks = fixpoint iterations x rules x nodes), star-expansion 7ms, parse
6ms, scope validation 6ms; sqlglot parse_one is called 20x PER QUERY
(~9ms) across preprocess/parse/render boundaries. q19/q07 outliers track
predicate size (big OR trees), still linear.

Levers: fewer fixpoint re-walks, cache sqlglot parses, but at 16% of total
this is lever #3, not #1. It IS the dominant share at SF0.1.

## F2. Key injection: no usefulness check, and orientation inputs are
## deliberately blunted (answers "why still wrong after 14 phases")

Two separate defects, with the exact decision inputs traced:

(a) NO-OP KEYS. q07 bottom join: probe = lineitem island (est 1,500,000),
build = supplier scan (est 10,000). Orientation "reduce the larger side"
is CORRECT here. The failure: supplier is UNFILTERED, so its 10k keys are
the entire l_suppkey FK domain - the injected filter passes every row.
NDV(l_suppkey) = 10,000 is IN the Python cost model's statistics cache at
emit time, but nothing compares keys-to-be-injected against probe-column
NDV. Isolated cost of the pointless temp-table semi-join: lineitem date
scan 108ms alone vs 184ms with the 10k-key IN (+75ms for zero filtering),
plus 1.83M rows shipped where ~146k (nation-filtered suppliers) suffice.
The runtime guard (engine.rs fetches_most_of_table) cannot catch it: for
duck it estimates selectivity as keys/table-rows (10k/6M = 0.17%), not
keys/column-NDV (10k/10k = 100%). Same disease q18's DUCK_TEMP_CAP note
documents, different symptom.

(b) ORIENTATION BLUNTED BY DESIGN. q10 top join, traced inputs:
  left  = PhysicalRemoteQuery est=2,000,405   (orders JOIN lineitem island)
  right = PhysicalScan        est=150,000     (pg customer)
"Reduce the larger side" -> inject customer's 150k keys into the island.
Actual island output: 114,705 rows (17x overestimate). The overestimate is
DELIBERATE: the max-base rule (64b7feb) sizes a remote by its LARGEST BASE
SCAN so a fact island never looks small (it fixed a real q09 regression,
1207ms, caused by composite-key underestimates). Consequence here: the
orientation can never prefer reducing a dim AGAINST a fact island, so q10
ships all 150k wide customers; the 150k keys then exceed DUCK_TEMP_CAP and
the "reduction" silently becomes a full fetch. Correct direction: collect
o_custkey from the filtered island (~38k keys, 25% of customer, under the
FULL_SCAN_FRACTION=0.40 guard) and reduce the PG read 155ms -> ~40ms.

Why 14 phases did not catch this: every phase made ORDERING cost-based;
the reduction DIRECTION only got the coarse larger-estimated-side rule in
phase 12, its inputs are deliberately conservative (max-base), the DP never
models reduction effects at all (the reduction-aware enumerator was
explicitly abandoned in the q21 phase as unnecessary THEN), and key
USEFULNESS (keys vs probe-column NDV) was never modeled anywhere. The
machinery is cost-based; this specific decision is not.

Note on "150k keys IN is slower than a full read": agreed, and the engine
already refuses it (IN_CAP=2000, DUCK_TEMP_CAP=50k, pg stats guard). The
fix is not bigger key sets; it is (a) skip emitting provably-useless
reductions (keys >= NDV of probe column), (b) let orientation use the real
island estimate for the DIM-vs-ISLAND decision, or reverse-reduce when the
island's own output estimate (not max-base) is far below the dim size.

## F3. The injected-IN wrapper defeats duckdb's optimizer

Ours wraps a pushed island as
  SELECT * FROM (<island join>) AS fedq_probe WHERE key IN (SELECT ... tmp)
assuming the source pushes the semi-join down. DuckDB DOES NOT push it
through the derived-table join. Measured on q03's island (orders JOIN
lineitem, 30,142 keys via temp table):
  wrapper form (ours):            65ms
  IN placed on orders inside:     21ms   (3.1x)
  island with no key filter:      67ms   (= wrapper form: filter did not
                                           reach the scan, join ran full)
Fix: when the inject column maps to exactly one base relation inside the
island, put the IN/temp-join predicate into THAT relation's WHERE in the
generated SQL instead of wrapping the whole island.

## F4. Islands exist, but only as the LEADING run of each join chain

Answers "we group joins into islands - why round trips?" Two cases:

- q09: the DP's chosen order interleaves sources because the part-key
  reduction of lineitem is genuinely the best first move. After lineitem
  is reduced AT THE COORDINATOR, the later duck relations (orders 1.5M,
  partsupp 800k) are no longer contiguous with it, so they arrive as
  separate full scans and the duck-x-duck joins run at the coordinator
  (45.8 + 42.9ms + 47ms transfer). The island mechanism only collapses the
  LEADING same-source run of a left-deep component (by design, HANDOFF
  locality phase); mid-chain same-source runs are never re-islanded.
- q02: the placement alternates pg-duck-pg (part -> partsupp -> supplier
  -> nation -> region), so after partsupp the pg run supplier/nation/region
  is mid-chain: each becomes its own injected pg scan (13.5 + 0.5 + 0.4ms
  round trips). Proof islands DO work where the rule allows: q02's
  correlated subquery is its own region, and its supplier x nation x
  region collapsed into ONE pg query returning 1,987 rows (5.98ms).
  A mid-chain same-source run COULD collapse into one remote query with
  the accumulated keys injected (needs F3's in-island injection first).

## F5. PG reads: we use one ADBC stream; the oracle uses parallel COPY.
## Our own parallel path already beats it - it is just not wired in.

The 2x read gap decomposed (customer SF1, 150k rows):

| path                                   | WIDE 7 cols | NARROW 2 cols |
| -------------------------------------- | ----------- | ------------- |
| COPY BINARY floor (zero decode)        | 76.6ms / 27.7MB | 38.3ms / 3.3MB |
| duckdb pg_scanner threads=1            | 91.2ms      | -             |
| duckdb pg_scanner threads=8            | 76.1ms      | 16.0ms        |
| ADBC single stream (our current path)  | 136.2ms     | 30.5ms        |
| connectorx                             | 177.0ms     | 72.9ms        |
| fedqrs fetch_parallel ADBC, 8 parts    | **37.5ms**  | **11.7ms**    |

Reading of the table:
- The floor for the wide read is the server+wire itself (77ms for 27.7MB);
  duckdb threads=1 sits 15ms above it (decode overlapped with receive);
  ADBC sits 60ms above it (decode sequential after receive, one thread).
  Batch-size hints (1-64MB) change nothing (121-125ms).
- For the narrow read the floor is the PG BACKEND's heap scan (38ms for
  3.3MB): one connection cannot go faster than one backend scans. duckdb
  beats the single-stream floor (16ms) by splitting the scan across
  backends with ctid-range COPY - server-side parallelism, not client.
- Our OWN ctid-partitioned parallel ADBC path (fetch_parallel, built for
  unselective probes, 8 workers with pooled connections) does the wide
  read in 37.5ms and the narrow in 11.7ms - FASTER THAN THE ORACLE'S
  SCANNER (76 / 16). Scaling 1->2->4->8 partitions: 160 -> 85 -> 51 -> 38ms
  (near-linear: server scan AND decode both parallelize).
So there is no driver mystery to solve: route large pg source scans
through fetch_parallel (page-count threshold via relpages, which it
already reads). q10's 155ms becomes ~40ms; every pg dim read shrinks.

## F6. q13: a single-sided predicate rides the LEFT-join condition

The query counts orders per customer, LEFT JOIN so zero-order customers
count, with `o_comment NOT LIKE '%special%requests%'` in the JOIN
CONDITION (per TPC-H spec). Our IR keeps the whole condition
(equi AND NOT LIKE) on a nested_loop_join fragment, so:
  - o_comment (the widest orders column) must ship: duck scan 98ms, and
  - the join fragment evaluates the LIKE coordinator-side: 85ms.
The NOT LIKE references ONLY orders columns. For a LEFT join, filtering
the NULLABLE side's INPUT by a condition conjunct that references only
that side is semantics-preserving (unmatched customers still null-extend);
it canNOT be moved to a WHERE (that would drop zero-order customers), but
it CAN move into the orders scan. Pushed: duck evaluates the LIKE in its
scan (it reads comments locally either way), ships 2 narrow columns, and
the remaining pure-equi condition becomes a hash_join fragment. Measured
duck-side: 3-col-with-comment export 123ms vs filtered 2-col export 118ms
(the LIKE costs ~100ms wherever it runs - the oracle pays it too, inside
its 101ms) - so the recoverable part is the coordinator join delta and
the comment transfer, ~60-70ms of our 218.

## F7. Serial step loop

Every step fully materializes, then the next starts. q02 = 25 sequential
steps; q05's pg customer read (31-40ms) and its duck island read (58ms)
are independent and could overlap. The oracle pipelines everything.
Biggest relative cost on small queries and SF0.1. Lever: run independent
steps (no binding dependency) concurrently; the step list is already a
DAG via binding names.

## F8. Fragment-input fragmentation (minor)

q09's in-query hash join ran 45.8ms where the same shape isolated is
~15ms; inputs arriving as hundreds of small batches from prior fragments
and 10-column projects explain most of the delta. Worth revisiting only
after F2-F5.

## Ranked levers (expected SF1 effect, from the numbers above)

1. F5 parallel pg source scans (machinery exists): ~-160ms and it
   de-risks every dim-heavy query; q10 alone -115ms. [DONE, round 2]
2. F2a skip useless reductions (keys >= NDV(probe col), stats already
   cached): q07 -150ms class, q05 -60ms class; also less coordinator work.
   [DONE for plain-scan builds, round 2; composite builds await item 7]
3. F2b orientation vs fact islands (use island output estimate for the
   dim-vs-island choice or allow reverse reduction): q10-class wins where
   F5 has not already absorbed them.
4. F3 in-island key injection (put IN on the owning base relation):
   q03 -45ms, prerequisite for mid-chain islands (F4).
5. F6 nullable-side condition pushdown for outer joins: q13 -60-70ms.
6. F1 planning/IR trims (sqlglot parse count, fixpoint re-walks): up to
   -200ms across the suite; dominant at SF0.1.
7. F7 overlap independent steps: tens of ms at SF1, more at SF0.1.

Rough floor if 1-5 land: ~1.4x. Below that needs F7 pipelining.

## The complete decision inventory (answer to "why not cost-based EVERYWHERE")

Every decision the engine makes between SQL text and rows, classified. This
is the full list - when the non-cost-based rows below are gone, there is no
"yes but we don't use it in..." left.

| # | decision | where | status |
| - | -------- | ----- | ------ |
| 1 | join ORDER within a region | JoinOrderingRule (Selinger DP, C_out + transfer) | COST-BASED |
| 2 | hash-join build side | _choose_build_side via larger_estimated_side | COST-BASED (phase 12) |
| 3 | predicate/projection/aggregate/orderby/limit pushdown | rules.py | rule-based, always-profitable rewrites - needs no cost |
| 4 | SEMI/ANTI commute below INNER | SemiJoinPushdownRule | structural gate (provable shape); cost-free by design |
| 5 | reduction USEFULNESS (do the keys filter at all?) | rust_ir _reduction_filters | COST-BASED as of this round (NDVs threaded from the cost model; abstains for composite builds) |
| 6 | parallel vs single-stream source read | rust_ir _parallel_scan_eligible | COST-BASED as of this round (estimated_rows threshold) |
| 7 | reduction ORIENTATION vs a fact island | rust_ir _orient_join | BLUNTED: max-base over-estimate by design; needs a real remote OUTPUT estimate carried separately |
| 8 | island formation (leading-run-only; break-for-reduction) | join ordering locality term | NOT modeled: the DP never sees reduction effects; mid-chain same-source runs never collapse |
| 9 | key delivery strategy (IN / temp table / full scan) | engine.rs IN_CAP, DUCK_TEMP_CAP, FULL_SCAN_FRACTION | CONSTANTS; duck guard uses keys/rows instead of keys/NDV |
| 10 | selectivity of DATE range filters | cost.py range interpolation | DEFAULTED (0.33) for non-numeric columns - q10's island est 2,000,405 = 1.5M x 0.33 x 4 vs actual 114k; TPC-H is full of date ranges, so this poisons many estimates |
| 11 | NDV/estimate coverage on physical nodes | only region atoms + remotes carry them | PARTIAL: LEFT/SEMI joins outside INNER regions carry no NDVs, so gates abstain there |
| 12 | nullable-side single-sided join conjunct pushdown (q13) | missing rewrite rule | needs no cost - it is semantics-preserving; just unimplemented |

So the honest answer to "how many steps": after this round, TWO estimate-
quality items (7+10+11 are one workstream: real per-node output estimates,
date interpolation, uniform threading) and ONE enumeration item (8, reduction
+transfer modeled in the DP, which subsumes mid-chain islands and makes 9's
thresholds costed choices). Plus the cost-free rewrite (12). That is the
whole remaining list; nothing else in the pipeline makes a size-sensitive
choice.

Why it kept coming back: the system has ONE cost number per node serving
THREE different questions - (a) ordering wants order-independent, never-
underestimate-facts comparability (hence max-base), (b) transfer/reduction
decisions want expected OUTPUT rows, (c) runtime strategy wants selectivity
of a concrete key set against a concrete column. Every phase tuned the shared
number for (a), and (b)/(c) kept inheriting a number that deliberately means
something else. The fix is not another patch to the shared number; it is
carrying the answers separately (output estimate + key NDVs per node), which
this round started.

## Round 2 results (parallel scans + usefulness gate, commit pending)

- fedqrs reads a big plain Postgres scan (>= 50k estimated rows, no
  DISTINCT/TABLESAMPLE/ORDER/LIMIT) through the existing ctid-parallel path:
  structured spec marked `parallel`, validated loudly in the engine.
- The reduction is refused when the build side's distinct keys cover >= 80%
  of the probe column's NDV (both from source statistics threaded onto the
  plan by join ordering / single-source pushdown). A composite build (island)
  abstains: its estimate is the max-base over-estimate, judging it killed
  q11's useful 400-key reduction in testing - measured, reverted, pinned by
  test_gate_abstains_for_a_composite_build_side.
- Gate (full 132-cell matrix, report-result-636b403.md): fedpgduck SF1
  2.26x -> 2.05-2.12x across three runs (q10 265 -> 156ms, q05 -21, q18 -21,
  q07 -10); SF0.1 3.27 -> 3.21x; single / fedparquet cells unchanged within
  noise; ALL cells correct; suite 1179 passed. q07's remaining 1.8M-row
  island transfer is item 8.

## Repro

  # per-query decomposition + engine step traces
  FEDQRS_PROFILE=1 PGUSER=postgres /workspace/venv-fedq/bin/python \
    <scratchpad>/profile_fedpgduck.py --scale 1 --queries q10
  # pg decode decomposition (floor / adbc / scanner / parallel)
  /workspace/venv-fedq/bin/python <scratchpad>/pg_decode_decompose.py
  # q03 wrapper + q13 shape isolation
  /workspace/venv-fedq/bin/python <scratchpad>/island_tests.py

  scratchpad = /tmp/claude-1000/-workspace/b72e0121-b604-4c82-8969-8f4aa297fcc3/scratchpad
  (session-local; copy into benchmarks/ if they should be kept)
