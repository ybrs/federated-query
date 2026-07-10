# Eager aggregation - plan

Push a PARTIAL aggregate below the joins that only decorate it, so the fact
collapses AT ITS SOURCE and partial rows - not raw fact rows - cross the
boundary. The q04 family (q04/q11/q74: the customer-year CTEs) is the
motivating case and the largest remaining single-query win.

## Measured motivation (SF10, q04 profile)

- store_sales crosses WHOLE at 811ms / 11.0M rows (both years), catalog_sales
  235ms / 5.7M, web_sales 103ms / 2.9M; the coordinator then aggregates
  ~19.5M rows into per-customer-year totals.
- En route it collects 426k customer keys and injects them into customer -
  fetching all 500k rows anyway (a useless reduction, ~115ms, repeated per
  channel).
- With a partial SUM GROUP BY (ss_customer_sk, d_year) evaluated inside
  DuckDB, ~850k partial rows cross instead of 11M, and the coordinator's
  aggregation shrinks proportionally. Estimate: q04 4.1s -> 1.5-2s; q11 and
  q74 share the shape (together ~8s of the 63s SF10 board).

## The design: COMPOSE, do not duplicate

One LOGICAL rule creates the partial; the EXISTING machinery does the rest.

Rewrite `Aggregate(G, aggs)` over a join tree as:

    Final:   Aggregate(G, merge(aggs))
               over Join(D_1 .. D_k,            -- the "decorating" dims
                         Partial)
    Partial: Aggregate((G n cols(S)) u K, partial(aggs))
               over S                            -- fact + its remaining joins

where S is the subtree supplying every aggregate input, D_i are dims joined
ABOVE the partial, and K are the fact-side join keys to those dims. After the
rewrite, dim shipping sees the PARTIAL as a plain aggregate root over
fact + small dims (q04: store_sales x date_dim) and collapses it into one
island through its EXISTING gates - no dim-shipping changes at all. The
useless customer injections disappear as a side effect: the coordinator join
probes 850k partial rows, not 11M.

## Correctness gates (each mandatory, each with a test)

1. DECOMPOSABLE aggregates only: SUM -> SUM(partial_sum);
   COUNT(x)/COUNT(*) -> SUM(partial_count); MIN/MAX -> MIN/MAX(partial).
   AVG is NOT rewritten in v1 (needs the sum/count pair split); DISTINCT
   aggregates, grouping sets, and window-bearing aggregates decline.
2. Aggregate INPUT expressions reference ONLY cols(S) - an expression mixing
   fact and decorating-dim columns cannot pre-aggregate.
3. Every decorating join D_i is INNER and equi on PLAIN COLUMNS, with the
   fact-side key column(s) added to the partial's group keys. NO uniqueness
   requirement - and none is needed (Yan & Larson 1995): the join
   multiplicity M is a function of the join key alone, every raw row inside
   one partial shares that key, so the final re-aggregation counts each
   partial M times exactly as the original counted each raw row M times.
   Duplication COMMUTES with the final SUM/COUNT merge and never affects
   MIN/MAX; INNER drops and NULL keys behave identically in both forms.
   (This is also why SQL Server's GbAggBeforeJoin, Trino's
   PushPartialAggregationThroughJoin and Spark's partial aggregation carry
   no constraint requirement. Declared-uniqueness machinery - RELY-style
   informational constraints in source configs - is a SEPARATE future
   feature for join elimination, not a prerequisite here; the earlier
   version of this gate required it out of an over-conservative reading,
   and the connector constraint-fetch + benchmark PK prerequisites it
   created are DROPPED from this plan's critical path.)
4. INNER-join drop semantics are preserved by construction: a partial row
   whose key misses D_i drops exactly as its raw rows would have.
5. Two customers sharing identical GROUP BY attribute tuples still merge
   correctly: the FINAL aggregate re-groups by the original G, so partials
   keyed by distinct c_customer_sk merge there - no functional-dependence
   assumption is needed.
6. NAMING: partial outputs are synthetic columns and MUST be qualified
   (columns-must-be-qualified rule) - the partial wraps in a SubqueryScan
   with a generated alias (__eager_N), mirroring decorrelation's __subq
   machinery, and the final aggregate's expressions rewrite onto that alias.

## Cost gate (decline is always safe)

Rewrite only when the partial COLLAPSES: estimated partial groups (NDV
product / learned group count for ((G n cols(S)) u K)) well under S's
estimated output rows (start at <= 0.5, tune against the tallies), and S's
rows are KNOWN (unknown declines - no bounds here, a non-collapsing partial
is pure overhead). The learned group_stats make this self-correcting the
same way the ship gate is: a run that measures the partial's true group
count corrects the next run's decision in either direction.

## Placement in the rule stack

After PredicatePushdown + SemiJoinPushdown, BEFORE JoinOrdering (the partial
changes the region the reorderer sees: the fact atom becomes the partial
subquery atom). The rewrite must preserve pushdown's normal form so the
fixpoint stays stable; the partial's subtree keeps its folded scan filters
untouched.

## Phases

- A. The rule for the single-fact shape (q04/q11/q74: one fact, one shipped
  dim inside S, one decorating dim outside): gates 1-6 + cost gate + unit
  tests over hand-built stats; verify q04's partial SHIPS at SF10 and the
  channel reads drop to ~850k/360k/190k partial rows.
- B. Multi-decorating-dim generality (several D_i) and COUNT/MIN/MAX
  decomposition tests.
- C. AVG via sum/count pair splitting, if any tally query needs it.

Gates per phase: full pytest; 99|0|0 at SF0.1/SF1/SF10 pg-dims + adversarial;
TPC-H 22/22; cold_sources convergence intact; the SF10 tally must show q04
materially down with NO other query regressing beyond noise (A/B per-query
diff, the outlier-sweep method).

## Explicitly out of scope

- Rewriting through OUTER joins (drop semantics differ).
- Aggregates over DISTINCT, grouping sets, windows.
- Cross-source partials that do NOT ship (a coordinator-side partial helps
  nothing - the fact already crossed).
