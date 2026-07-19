# Design history - digests of removed plan and design docs

Each section below distills a plan/design document that was removed from the
repo root after its work landed, was killed, or was superseded. The digest
keeps the problem, the key architecture decisions and why, the outcome, and
any hard-won lesson. The FULL text of every removed doc is in git history:
`git log --diff-filter=D --name-only` finds the deletion commit, then
`git show <commit>~1:<path>`.

Live design docs (NOT digested here, still authoritative):
`HANDOFF.md`, `rewrite-python-to-rust-plan.md`, `README-architecture.md`,
`accelerator-plan.md`, `events-plan.md`, `dim-shipping-open-problems.md`,
`doc/node-mutation-and-concurrency.md`.

## Decorrelation

### Pattern-based subquery decorrelation (decorrelation-plan.md)

Problem: correlated/uncorrelated subqueries must become join/CTE-based
relational algebra BEFORE cost optimization, preserving exact SQL semantics
(three-valued NULL, duplicate handling, outer-reference scoping), and must
FAIL FAST on unsupported shapes rather than silently skip.

Key decisions: run the pass after binding (scopes known) and before the rule
optimizer, which may then assume zero subquery expressions remain.
EXISTS/NOT EXISTS -> SEMI/ANTI join; IN/NOT IN and ANY/ALL -> null-safe
SEMI/ANTI (IS NOT DISTINCT FROM, never bare =); correlated scalar -> LEFT
join to an aggregate keyed by correlation columns with a single-row
cardinality guard; uncorrelated subqueries hoisted/inlined once. Multi-phase
walk: correlation analysis -> normalization -> rewrite -> validation (no
surviving subquery, all correlation keys covered by join conditions).
Rationale for null-safe equality and the cardinality guard: getting
NULL/multiplicity wrong manufactures wrong answers, the project's worst
failure mode.

Outcome: LANDED as the always-decorrelate pass in fq-decorrelate.

### Deliberate design deviations and the fail-fast contract (decorrelation-tasks.md)

Two deviations from the original sketch, both intentional and now in the
code: uncorrelated subqueries are INLINED as join inputs rather than hoisted
to named CTEs (CTE reuse left as a later optimization), and boolean flag
columns collapse UNKNOWN to FALSE. Non-aggregated scalar subqueries get a
runtime SingleRowGuard (raises a cardinality violation like real engines); a
correlated LIMIT n becomes a per-key GroupedLimit; disjunctive
OR-of-subqueries becomes per-disjunct boolean flags unioned by an exact-once
UNION ALL partition. HAVING aggregates referenced only in the predicate are
hoisted into the aggregate's outputs.

Contract: patterns that cannot be safely decorrelated RAISE (skip-level
correlation, non-equi correlation through aggregates/limits, subqueries in
GROUP BY / aggregate args / non-INNER join conditions, OFFSET in correlated
subqueries) rather than emit wrong rows.

### Neumann-Kemper general dependent-join unnesting (nk-decorrel-plan.md)

Problem: the pattern decorrelator raised NonFlattenableCorrelation on two
shapes - a non-equi correlation across an aggregate, and a top-k-per-outer
(LIMIT k with non-equi correlation) - and punted to a LATERAL fallback the
engine could not run cross-source.

Decision: add general dependent-join unnesting (Neumann & Kemper 2015) ONLY
at those raise sites, keeping the working pattern paths. It is a plan-time
rewrite that lowers a correlated subquery to ordinary algebra (distinct
domain -> join -> aggregate/row_number window -> join-back) using operators
the engine already runs; no new execution operators. The count bug (COUNT
must return 0, not NULL, on an empty correlation) is handled by a
domain-preserving LEFT join + COALESCE; MAX/MIN/SUM/AVG need no fix. A
user-written LATERAL is treated as the SAME dependent join and unnested
through the SAME single path, with no same-source vs cross-source
distinction at decorrelation time.

Outcome: LANDED. Every common correlated shape unnests to regular algebra
and runs cross-source. A genuinely non-unnestable dependent join (e.g.
lateral over a set-returning function) still emits a LateralJoin and fails
loud cross-source. LateralJoin was deliberately RETAINED as the last-resort
fallback (the plan's idea of retiring it entirely was not adopted).

Lesson: the equi domain-grouping the decorrelator already had for scalar
aggregates IS N-K's domain idea for the equi case; the general rewrite is
its generalization, not a rewrite from scratch.

### Disjunctive (OR-of-subqueries) decorrelation (disjunctive-decorrelation-plan.md)

Problem: OR-of-EXISTS/IN queries (TPC-DS q10/q35/q45) errored or exploded.
Root cause was NOT the flag rewrite but a PUSHDOWN gap: the other WHERE
conjuncts (join equalities) sat in a Filter above the decorrelation Union,
and predicate pushdown refused to descend through a set operation, stranding
conditionless cross joins per branch that the memory pool killed.

Decisions: (Phase 1) teach predicate pushdown to clone a deterministic
filter into every positionally-aligned Union branch and recurse, so the
joins get normal filtered-hash treatment - this alone closed the ERROR
column. (Phase 2) when every disjunct is a positive existential correlating
on the SAME outer key, collapse to ONE SEMI join over a DISTINCT
domain-union of the subquery keys (no input replication); mixed
positive/negative or different-key disjuncts fall through to the
boolean-flag Union path.

Outcome: LANDED (q10/q35/q45 pass and sped up). Phase 3 (LeftMark mark-join
to remove the flag path's 2x input replication) was NOT taken by its own
decision gate - the win was too small to justify the surface; revisit only
if flag-path disjunctions become hot at scale.

Lesson (recurring in this project): a rule that silently declines a node
type strands work above it - the same walker-descent-stop class as the perf
bugs.

### Decorrelation capability matrix (doc/decorrelation-capabilities.md)

Ground-truth inventory of the pattern decorrelator: EXISTS/NOT EXISTS ->
SEMI/ANTI, IN/tuple-IN/ANY/SOME -> null-aware SEMI, NOT IN/ALL -> null-aware
ANTI, scalar and derived-table subqueries peeled/inlined, OR-of-subqueries ->
per-disjunct flags unioned. Every unsupported pattern (Sort-topped body,
set-op body, OFFSET, GROUP-BY-position subquery, skip-level correlation,
multi-column/multi-row scalar) FAILS FAST with a clear error, never a silent
wrong answer - each pinned by an error-case test. The strategic fix named
there (general dependent join, Neumann & Kemper) was subsequently built,
removing most unsupported-shape failures wholesale.

## Optimizer and cost model

### Eager aggregation (eager-agg-plan.md)

Problem: the customer-year CTE family (q04/q11/q74) shipped whole facts
(11M+ rows) to the coordinator to aggregate, when a partial SUM/COUNT GROUP
BY could collapse the fact at its source so only ~850k partial rows cross.

Decisions: ONE logical rule creates the partial aggregate over the fact +
its in-source dims; the EXISTING dim-shipping machinery then sees a plain
aggregate over fact+small-dims and collapses it to one island - no
dim-shipping changes. Only decomposable aggregates (SUM->SUM,
COUNT->SUM(count), MIN/MAX->MIN/MAX); AVG excluded from v1. NO
uniqueness/PK requirement is needed (Yan & Larson 1995): join multiplicity
is a function of the join key alone, so duplication commutes with the final
SUM/COUNT merge - matching Trino/Spark/SQL-Server, and correcting an earlier
over-conservative gate that had wrongly required declared uniqueness. Cost
gate: rewrite only when the partial provably collapses, priced against the
LARGEST BASE SCAN in the subtree (the join estimate under-counts through
containment asymmetry and would veto the exact wins that matter).

Outcome: LANDED (q04 3.4x, q11 3.0x, q74 2.8x at SF10). Lesson: two
estimator refinements the gate forced in - literal-vs-literal equality is
exactly computable, and a filter's value-cap bounds a group key's NDV.

### Adaptive cardinality catalog: measure, do not fabricate (adaptive-catalog-plan.md)

Problem: the optimizer guessed cardinalities with fabricated default
constants and NDV-independence, and a fabricated 1000-row fact meeting a
measured 73k-row dim in the same arithmetic INVERTED every larger-side
decision - the warm-worse-than-cold regression.

Decisions: (HONEST-UNKNOWNS) delete every fabricated stat constant; a
statistic is a measurement with provenance (catalog/learned/probed) or it is
NULL, and unknown NULL-propagates through estimate arithmetic. Each decision
point declares an explicit unknown policy (conservative = keep the safe
orientation / decline the optimization), which is exactly the acceptable
cold-off behavior. A gap-fed estimate is an UPPER BOUND by construction
(unknown selectivity ceilings at 1.0, unknown NDV floors the denominator at
1), and decision points may USE bounds for two-way comparisons - refusing
bounds outright regressed queries whose whole-fact scan then shipped
unreduced. (PROBE) a never-ANALYZEd source table is probed for real stats
(pg exact aggregate <=256MB else block TABLESAMPLE; ask-the-source EXPLAIN
estimates for predicate shapes the engine cannot price) rather than reported
unknown. (REFINE) passive learning records base rows, predicate
selectivities keyed by a constant-neutral template, and dim-ship island
group counts, all self-healing on use with a TTL guard.

Safety property: a learned observation only changes WHICH plan is picked,
never the answer - so learn aggressively, invalidate lazily.

Outcome: HONEST-UNKNOWNS + PROBE + REFINE LANDED (inversion class dead; warm
converges to analyzed). Lesson: honest unknowns ALONE were insufficient -
loose bounds still mislead the join-order DP; the fix was better INPUTS
(probe), not a blunter gate (a "decline on any gap" gate would de-enumerate
59% of the healthy path).

### Same-source join pushdown, superseded design (join-pushdown-plan.md)

Original plan: a binary PhysicalRemoteJoin operator pushing a single
same-source equi-join to the source, guarded by a JOINS capability check
with fallback to local hash/nested-loop joins.

Superseded by: the greedy single-source generator (SingleSourcePushdown),
which pushes any same-source subtree - joins of every shape, computed
projections, derived tables - as ONE flat remote SELECT, declining only what
it cannot render or what crosses sources. This subsumes and beats binary
join pushdown. The lasting decision retained here is alias propagation:
parser/binder/optimizer must carry table aliases through every rewrite so
pushed SQL quotes them consistently.

### Selective / partial join pushdown: why greedy is a heuristic, not a law (selective-pushdown.md)

Greedy whole-subtree pushdown is the right DEFAULT (pushing minimises data
movement, the dominant federated cost). It breaks down in five cases where
it is a heuristic: (1) part of the data lives on the coordinator (cached
dim, IN-list/VALUES, an earlier materialized result, a source with no SQL
engine) - needs a local-table node under a join or a dependent bind/lookup
join; (2) the query-accelerator case (large tables deliberately cached
locally) - the primary motivator for PER-SIDE partial pushdown of a single
join; (3) cost says pushing is a pessimization (fan-out, slow/overloaded
remote, bad remote plan) - needs a cost/placement layer that can DECLINE a
pushable join; (4) the join needs coordinator-only logic (local UDF/geo/ML,
or a window/recursive input); (5) mixed-source N-way placement needs join
reordering driven by source placement. The three missing pieces are a
local-table/VALUES node, a cost-based decline-to-push decision layer, and a
dependent bind/lookup join.

### ORDER BY pushdown gaps, closed (plan-missing-pushdowns.md)

Decisions/guardrails that landed: alias-safe pushdown (ORDER BY on a
projection alias rewrites to the source column); join-side pushdown (sort
keys exclusive to one join input annotate that scan while a top-level Sort
is retained for correctness); only ColumnRef sort keys are pushable, complex
expressions stay local; sort keys aligned with group-by columns annotate the
pushed aggregate scan; UNION/INTERSECT/EXCEPT annotate children with sort
metadata but keep the top Sort.

### LIMIT / OFFSET / ORDER BY pushdown rules (doc/limit-pushdown.md)

Correctness contract: ORDER BY pushdown is treated as REQUIRED
(cursors/timeseries/merge inputs need deterministic order), while LIMIT
alone is unsafe to push in multi-node settings (non-deterministic). Push
ORDER BY and LIMIT/OFFSET together through row-preserving operators
(Projection, Filter) to the Scan - the critical ORDER BY + LIMIT pagination
case fetches N rows instead of the whole table. NEVER push either through
Join, Aggregate, or Union (they reorder/regroup rows). OFFSETs compose by
addition, LIMITs by minimum. Push only ColumnRef sort keys (computed
expressions and position refs resolve first); honor NULLS FIRST/LAST per
source dialect and capability; reject negative LIMIT/OFFSET at parse time.

### Source-catalog statistics, per-source reads (doc/statistics.md)

Where each stat comes from, per connector: Postgres uses pg_class.reltuples
and pg_stats (signed n_distinct decoded, null_frac, avg_width,
histogram-bound ends as min/max); DuckDB uses duckdb_tables().estimated_size
plus ONE aggregate scan (approx_count_distinct/count/min/max) for the
requested columns only; Parquet inherits DuckDB; ClickHouse uses count() +
uniqExact/countIf-null. A source NEVER invents a value: a never-ANALYZEd
Postgres table reports reltuples=-1 honestly as unknown (never coerced to
0). StatisticsCollector caches per (datasource, schema, table) for the
session, accumulating columns incrementally, caching a column's absence, and
raising on an unknown datasource name. The doc's other half ("named
defaults" like DEFAULT_ROW_COUNT=1000) was SUPERSEDED and deleted by the
adaptive-catalog honest-unknowns work above.

### Constant folding: why it was removed and the bar to reintroduce (doc/constant-folding.md)

ExpressionSimplificationRule (constant folding + algebraic simplification)
was removed. It predated the DuckDB merge engine and in the current
architecture was both redundant and WRONG: single-source expressions are
pushed to the source (which folds them) and local ones run in the merge
engine (which folds them); folding 0.1+0.2 in Python baked the float
0.30000000000000004 into pushed SQL where DuckDB would compute exact 0.3
with DECIMAL arithmetic; and algebraic simplification (quantity*0 -> 0, then
0=0 -> TRUE) was UNSOUND under NULL, collapsing a predicate to WHERE TRUE
and wrongly keeping NULL rows. Any reintroduction must clear decimal/string
type fidelity matching the source and apply only to provably non-null
inputs, keeping the exact-0.3 and constant-false regression tests green.

## Dimension shipping

### Dimension shipping: ship small foreign dims into the fact's source (dim-shipping-plan.md)

Problem: a fact-scale subtree (q39: 26.5M inventory rows) crossed to the
coordinator only because its aggregate grouped by a small foreign dimension
column. Shipping the FILTERED dim (366 rows) INTO DuckDB as a temp table
makes the whole join+aggregate one island; only the aggregate output
returns.

Key decisions: SEED the island schema rather than probe it - the temp table
lives only on the engine's pinned connection so a LIMIT-0 probe cannot see
it; this is safe because the IR ships no column types (the executor
reconciles real types from batches) and the planner only needs the island's
column NAMES, taken from the pure cross-source plan of the same subtree.
Gates are all reliable STRUCTURAL signals because the cost model cannot
estimate aggregate collapse: ship only a PLAIN (non-rollup) aggregate root,
INNER joins only, DuckDB target, fact large + foreign small + ratio, no
window in the fallback, island outputs must match the pure plan exactly.

Outcome: LANDED (Phase A+B), correct 99|0|0 all scales, net perf-positive at
SF10 (q39 2.5s -> 1.6s). The q23 non-collapse residual was later addressed
by the collapse gate below and is tracked in dim-shipping-open-problems.md.

### Dim-shipping collapse gate (dim-shipping-collapse-gate-plan.md)

See dim-shipping-open-problems.md (RESOLVED sections 1-3). Landed decision:
decline shipping a plain aggregate whose GROUP BY spans >=2 INDEPENDENT
high-cardinality SOURCE DIMENSIONS (distinct owner relations, NDV>=10k) -
correlated keys from ONE dimension (i_item_id+i_item_desc) still collapse
and still ship. Rests on ANALYZE-after-load + row-count-bounded NDV so a
small unstatted dim classifies low-card. Declined q23 only; every winner
(q39 etc.) preserved.

### Dim-shipping ship-size cap (dim-shipping-ship-cap-plan.md)

See dim-shipping-open-problems.md section 4. Landed: a hard row cap
(SHIP_MAX_ROWS = 50M) in ship_table sums the actual materialized rows and
RAISES loudly before ingesting an over-cap relation, converting a
stale-stats runaway ingest into an immediate honest crash ("a crash never
ships a lie"); a fixed constant because a safety valve has no query-derived
right value, the planner's SHIP_ROW_BUDGET keeps legitimate ships small. The
shipped Postgres temp table is ANALYZEd after ingest so the island join
plans over real stats (propagate ANALYZE failures, do not swallow).

## Execution

### Semi-join reduction through composite probes, injection base tracing (fact-injection-plan.md)

Problem: semi-join reduction only accepted a PLAIN SCAN as the probe, but in
the SF10 outlier cluster (q05/q39/q58/q06...) the fact sits inside a
composite subtree, so reduction flipped to the useless direction (collect
fact keys, inject into the small dim) or declined - shipping whole facts
(q39 133M inventory rows for 52 dates).

Decision: trace a probe key down to the base scan(s) that ORIGINATE it,
injecting the reducing keys into every such base, descending only where
filtering base rows by a key superset cannot change the join result above:
through projections/renames (bijective only), into a hash join's
non-null-extended side per join type (inner both, left->left,
semi/anti->left, full neither), and into every branch of a UNION ALL
(all-or-nothing; INTERSECT/EXCEPT do not descend). Orientation, then
emission into every traced base, then a cost gate priced with the TRACED
base's estimates (not the composite's unknown).

Outcome: DONE (q05 15.4s -> 0.6s, q58 2.8s -> 0.3s, q39 7.2s -> 2.4s at
SF10; 99|0|0 all scales; TPC-H 22/22).

### Fragment fusion, lazy bindings (fragment-fusion-plan.md)

Problem: the step interpreter drained every Arrow stream at every step and
re-registered it as a MemTable, keeping ~4 fact-sized intermediates resident
and denying DataFusion's spilling operators the chance to run - the SF10
memory wall (q23/q64/q67/q78).

Decisions: (Phase A) a Binding gains a Lazy(LogicalPlan) variant; a merge
step builds its fragment's plan in a scratch context and stores the plan
instead of executing, so a chain of merge fragments with no pipeline breaker
becomes ONE streaming DataFusion execution. Force a collect only at GENUINE
breakers - collect_distinct inputs (keys go into probe SQL text),
multi-consumer bindings (materialize once, else the CTE re-emission disease
returns), and the final return. (Phase B) re-anchor key collection to the
already-materialized base binding that carries the key column rather than
the previous join's output, so join cascades fuse (trading slightly fatter
dim reads for one streaming region).

Outcome: Phases A+B LANDED; the SF10 memory wall largely cleared.

### Parallel reads, overlapping independent remote fetches (parallel-reads-plan.md)

Problem: the step loop was strictly sequential, so independent fact reads
serialized even though nothing ordered them.

Decisions: keep BINDINGS single-threaded (only the driving thread touches
the map, use-counts, memory budget, spill); dispatch READ steps to a
persistent worker pool while preserving every real ordering edge
(dynamic-filter keys before the injected scan, ships before the island).
Ship visibility is handled conservatively: any scan on a datasource that is
the TARGET of any ship stays on the driving thread in step order (temp
tables live on a pinned connection).

Outcome: Phase A (parallel plain SourceScans, 6-worker prefetch pool) and
Phase B (parallel InjectedScans) LANDED. Two measured surprises: an
in-process DuckDB injected scan already saturates all cores, so concurrent
duck semi-joins are pure contention and stay sequential; and on a loopback
Postgres (zero network latency) overlap measures neutral - the mechanism's
value scales with real source latency. Lesson: on a single box competing for
one disk, parallel fetch was still net positive with no reproducible
per-query backfire.

## Perf rounds and the rewrite

### Federated perf diagnosis, the CBO completion round (perf-explore.md)

Decomposed the SF1 fedpgduck 2.26x gap into measured levers, most now
landed: parallel ctid Postgres source scans (the engine's own path beat the
DuckDB oracle's parallel COPY), skip provably-useless key reductions (keys
>= probe-column NDV, not keys/table-rows), in-island key injection placed on
the owning base relation's WHERE (DuckDB will not push a semi-join through a
derived-table join), nullable-side single-sided condition pushdown for outer
joins, and temporal range-selectivity interpolation.

Structural lesson: ONE cost number per node was serving THREE incompatible
questions - join ORDERING wants a never-underestimate-facts comparable
(hence max-base), transfer/reduction wants expected OUTPUT rows, runtime key
delivery wants selectivity of a concrete key set - so every ordering fix
kept mis-feeding the other two; the fix was carrying output estimates and
per-node key NDVs SEPARATELY.

Negative result (reverted): charging the DP's transfer term the reduced
expectation made totals worse, because cheaper transfer makes
island-breaking look free when the coordinator pays unpriced join-input
(materialize+hash) cost - reduction-aware ordering needs a paired
coordinator-input term first.

Process lesson: a plan-shape rule must gate on the full source-placement
matrix, not the unit suite (a LEFT-join filter-hoist bug passed the suite
and only the 132-cell differential caught it).

### Python -> Rust rewrite, the go decision (rust-rewrite-plan.md)

Assessment that chose to move the ~30k-line Python planning layer into Rust.
The deciding argument was the GIL (Python planning serializes concurrent
queries; the multi-query roadmap hits it structurally), above the 11-26ms
per-query planning floor. The sqlglot question was resolved by a spike:
polyglot-sql (Rust, 32 dialects) parsed all 121 benchmark queries with zero
failures ~10-20x faster and re-rendered the engine's emitted SQL with zero
transpile errors. Planned as a right-to-left cut (IR -> physical ->
optimizer -> decorrelation -> parser), each stage gated by a differential
harness with the results-level tallies as final arbiter. Fully superseded by
rewrite-python-to-rust-plan.md and HANDOFF.md (rewrite complete, 17 crates,
engine end-to-end; the big-bang approach was taken instead of the staged
cut).

### Original Python-engine architecture (oldplan.md)

The founding architecture doc: SQL -> parse -> bind -> pre-optimize ->
decorrelate -> logical optimize -> physical plan -> execute -> Arrow, with a
push-down-first philosophy, rule-based + cost-based (Volcano/Cascades)
optimization, and Arrow data transfer. It recorded the Phase-8 status and
named the papers the engine follows (Volcano/Cascades, Chaudhuri,
Neumann-Kemper). Its "immutable plan nodes" design point was later
explicitly reversed (the project's mutable-by-default and
node-mutation-idiom rules). Superseded by README-architecture.md.

## Removed trackers and reviews

### Python-engine phase roadmap (tasks.md)

The pre-rewrite Phase 0-14 roadmap and migration tracker. Phases 0-9 done;
the "not started" Phase 10+ rows were already stale (the Python tree contains
the CBO work they claim absent) and are moot: Python is frozen, the
equivalents were built in Rust instead. Lasting content: the decorrelation
north star was Neumann & Kemper "Unnesting Arbitrary Queries" (2015),
realized in fq-decorrelate; a structured-error-code scheme (FEDQ-NNNN) was
proposed but never implemented in either engine (see parked ideas below).

### jscpd duplicate-code cleanup status (status-report-cleanup.md)

A 2026-06-27 dedup session log. Lasting decision: the canonical
shared-expression-helper home in the Python tree is
`federated_query/plan/expressions.py` (expression_children, map_children,
split_where_having); dedup work routes through there. Its "not done" list
(two SQL emitters, Binder vs SubqueryPlanBinder fork) is carried in
review-tasks.md, which remains at the repo root as the live Python punch
list.

### Session-state snapshot (todo-current-state.md)

A "Phase 8 merged" session snapshot. Its SQL-audit open list (DISTINCT ON,
named WINDOW, TABLESAMPLE, ROLLUP/CUBE/GROUPING SETS) was fully resolved;
FETCH FIRST WITH TIES stays an intentional fail-fast. Nothing else lasting.

### Full Python code review 2026-06-28 (review-20260628.md)

Nearly every silent-failure and correctness finding was subsequently fixed
(ADBC error swallow, join-type INNER default, denylist walkers, MIN/MAX
aggregate typing, orientation fallback, alias-map resolution, magic-1000
cost defaults - all verified gone). Still-open carryovers were folded into
review-tasks.md (kept at the repo root) and the genuinely severe one - the
decorrelator's silent left-binding of an ambiguous right-side correlation -
is now a tracker ticket. Lesson worth keeping: the silent-fail sweep
methodology (parallel subsystem audits + adversarial verification) caught an
expression-walker exhaustiveness gap that keyword lints could not;
`tests/test_expression_walker_exhaustiveness.py` pins the pattern.

## Parked ideas swept from the removed docs (not tickets, grep-verified absent from crates/)

- Parallel-reads Phase C: parallel Ship uploads + exact per-scan
  ship-visibility edges in the IR to release the conservative
  ship-target-stays-sequential rule. Deferred to a multi-server phase;
  locally saves at most ~20ms.
- Bushy same-source emission: the left-deep join enumerator cannot collapse
  a mid-chain same-source dim run into one remote query (q09
  orders+partsupp, q02 supplier/nation/region still round-trip).
- Coordinator-input cost term + TRANSFER_WEIGHT recalibration, the
  prerequisite for reduction-aware join ordering (the reverted negative
  result above).
- Adaptive catalog Phase C (execution-time adaptivity: reduction
  orientation / build side decided from observed cardinalities of
  already-run inputs), Phase D (staleness hardening, source_fingerprint
  repoint detection), and plan caching keyed on normalized SQL.
- Eager aggregation for AVG via sum/count-pair split; only if a tally needs
  it.
- Fragment-fusion Phase C: streaming source TableProviders so single-use
  source reads feed the fused plan without an intermediate Vec (the q64
  resident-source case).
- Selective/partial pushdown machinery: a local-table/VALUES node, a
  cost-based decline-to-push layer, and a dependent bind/lookup join (the
  accelerator partial-pushdown case).
- Disjunctive decorrelation Phase 3: LeftMark mark-join to remove the flag
  path's 2x input replication; revisit only if flag-path disjunctions
  become hot at scale.
- Dim-shipping collapse-gate piece C: SubqueryScan NDV attribution
  (documented hardening, no measured query needs it).
- Structured error codes (FEDQ-NNNN) for engine errors; proposed for the
  Python engine, never implemented in either engine.
