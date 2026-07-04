# Neumann–Kemper general decorrelation — plan

## Goal

Replace the `LateralJoin` fallback (which the Rust engine cannot run — it's the
last capability gap, and forces the DuckDB-merge fallback) with **general
dependent-join unnesting** (Neumann & Kemper, *Unnesting Arbitrary Queries*,
2015). The unnesting is a **plan-time** rewrite that lowers a correlated subquery
to **ordinary relational algebra** (joins, aggregates, window) that the Rust
engine already executes. No new execution operators. It lives in Python (the
planning layer); it walks a tiny plan tree, touches zero rows.

## Approach: incremental (confirmed)

Keep the existing pattern-based decorrelator (`decorrelation.py`, ~1900 lines,
passes 1075 tests — EXISTS / IN / NOT IN / equi-correlated aggregates via
`_widen_aggregate`'s domain-grouping). Add N-K unnesting **only** where the
decorrelator currently raises `NonFlattenableCorrelation` and punts to
`_lateral_scalar`:

- **Non-equi correlation across an aggregate** — `decorrelation.py:811`
  (`_key_equality` requires `EQ`).
- **`LIMIT` with a non-equi correlation** (top-k per outer row) —
  `decorrelation.py:1106` (`_require_equi_correlation_for_limit`).

Both are caught at `decorrelation.py:1787-1788` (`_join_scalar` → `_lateral_scalar`).
We replace that fallback with the general unnesting.

## What we already have (substrate)

- **Correlation detection**: `_references_outer`, `_references_inner`,
  `_is_inner_column`, the `_SubqueryPreparer` that pulls correlation predicates.
- **Equi domain-grouping**: `_widen_aggregate` already adds the inner key to
  `GROUP BY` and relocates the equality above the aggregate as a join condition.
  This is exactly N-K's domain idea for the *equi* case; we generalize it.
- **The dependent-join representation**: `LateralJoin` (== `⋈ᴰ`).
- **Execution**: distinct/aggregate, non-equi (nested-loop) join, window
  (`row_number`), joins — all already run by the Rust engine.
- **Differential test harness** vs DuckDB (TPC-H comparison, parity tests).
- **Domain optimization already done**: semi-join reduction / dynamic-filter
  `IN (domain)` pushdown — N-K's key perf optimization is already in the engine.

## The rewrite (our two cases)

Notation: outer relation `O`, subquery references outer free vars `F` through a
correlation predicate `p(inner, F)`.

### Case A — non-equi correlated aggregate
`(SELECT MAX(x.a) FROM x WHERE p(x, O.f))`, `p` non-equi (e.g. `x.a < O.f`):

1. **Domain** `D = DISTINCT(π_F(O))` — the distinct outer correlation values.
2. **Dependent aggregate** — the non-equi correlation becomes a **join condition**,
   the domain becomes the grouping:
   `Aggregate(group_by = D.F, aggs) over ( D ⋈_{p(x, D.F)} x )` → columns `(D.F, __v)`.
3. **Count bug** — an aggregate that must return a value for an *empty* group
   (`COUNT → 0`) needs the domain preserved: `D LEFT JOIN <agg> ON D.F` then
   `COALESCE(count, 0)`. `MAX/MIN/SUM/AVG` return NULL on empty (which matches the
   correlated-scalar semantics), so they need no fix. Detect `COUNT` specifically.
4. **Stitch back**: `O LEFT JOIN <dependent-agg> ON O.F = D.F`; replace the
   subquery expression with `__v`.

### Case B — top-k per outer (`LIMIT k` with non-equi correlation)
`LATERAL (SELECT x.b FROM x WHERE p(x, O.f) ORDER BY x.c LIMIT k)`:

1. **Domain** `D = DISTINCT(π_F(O))`.
2. `D ⋈_{p(x, D.F)} x`.
3. `row_number() OVER (PARTITION BY D.F ORDER BY x.c) AS __rn`, then `WHERE __rn <= k`.
4. **Stitch back**: `O LEFT JOIN <top-k> ON O.F = D.F`.

Both lower to: distinct + (non-equi) join + aggregate/window + filter + join —
all operators the Rust engine runs.

## The subtle parts (what the tests must police)

- **Count bug**: `COUNT` over an empty correlation must be `0`, not NULL/absent →
  domain-preserving LEFT join + `COALESCE`.
- **NULL-aware NOT IN / anti**: NULL in the domain or the probe changes truth
  values. Mostly handled by existing anti-join paths; verify under N-K.
- **Empty domain / empty outer**: covered by the outer LEFT join.
- **Scalar cardinality**: the aggregate guarantees one row per domain value; the
  `SingleRowGuard` still applies for non-aggregate row subqueries.
- **Nested (multi-level) correlation**: unnest inside-out (fixpoint); the domain
  of an inner subquery may itself carry outer free vars.

## Implementation steps

Files: `federated_query/optimizer/decorrelation.py` (new N-K methods),
`tests/test_nk_decorrelation.py` (differential vs DuckDB).

1. **Correlation extraction** — given the subquery, return
   `(free_vars: list[ColumnRef], correlation_predicate, inner_body)`; reuse the
   preparer's outer/inner detection.
2. **Domain builder** — `Aggregate(distinct)` over `π_free_vars(O)`, fresh alias `__d`.
3. **Dependent-aggregate builder** (Case A) — `D ⋈_p inner_body`, aggregate
   grouped by `D.F`; count-bug LEFT join + `COALESCE`.
4. **Top-k builder** (Case B) — `D ⋈_p inner`, window `row_number`, filter.
5. **Stitch-back + replacement** — outer LEFT join on `F`, replace the subquery
   expression with the value column.
6. **Wire** — replace `_lateral_scalar` with `_unnest_dependent_scalar`; the two
   `NonFlattenableCorrelation` sites now route here instead of raising-to-lateral.
7. **Retire** the `LateralJoin` generation (keep the class for now; assert it is
   never produced).

## Testing — differential vs DuckDB (confirmed)

Harness: for each query, run through our engine (Rust default) and DuckDB
(oracle), diff exact results (order-insensitive, decimal-exact). DuckDB catches
the count bug and NULL semantics automatically.

Corpus (`tests/test_nk_decorrelation.py`):
- scalar correlated aggregate: equi and **non-equi** (`< , > , <=`)
- correlated `COUNT` (the count bug), and `COUNT` with a non-equi correlation
- `EXISTS` / `NOT EXISTS`, `IN` / `NOT IN` (including NULLs in the domain/probe)
- top-k per outer (`LIMIT k` with non-equi correlation)
- nested (2-level) correlation
- empty correlation domain, all-NULL correlation column
- **cross-source** variants of each (the actual point): correlation spanning two
  sources must lower to regular joins the Rust engine runs.

## Milestones

- **M1** — extraction + domain infra + Case A (non-equi aggregate) + count-bug +
  differential tests. Deliverable: cross-source non-equi correlated aggregate
  runs on Rust and matches DuckDB.
- **M2** — Case B (top-k per outer) + window handling + tests.
- **M3** — retire the `LateralJoin` fallback; assert no plan produces a lateral;
  full baseline + TPC-H green; the `e2e_pushdown/test_cross_source_lateral.py`
  tests now run on the Rust engine (no DuckDB fallback).

## User-written LATERAL (`LEFT JOIN LATERAL (...)`) — revisit

A user-written `LEFT JOIN LATERAL` is an explicit dependent join (not a subquery
we decorrelate), so it is *correctly* left as a `LateralJoin` today:

- **Same-source**: pushes to the owning source as one query and evaluates
  natively (Postgres/DuckDB do LATERAL). Keep this — it is the cheapest path.
- **Cross-source**: cannot push; currently fails fast.

Decision (to revisit after M3): a user LATERAL is structurally the *same*
dependent join the N-K machinery already unnests. So a **cross-source** user
LATERAL should route through the same domain -> join -> (aggregate | top-k window)
-> join-back unnesting and run on Rust, instead of failing. Same-source keeps the
cheap push. This unifies user laterals and subquery decorrelation under one
dependent-join-unnesting path. (Only genuinely non-unnestable dependent joins —
e.g. a lateral calling a set-returning function — would then remain, and those
fail loud.)

## Non-goals (for now)

- Uniform rewrite of the whole decorrelator (keep the working pattern paths).
- New execution operators (none needed — lowers to what we run).
- Join reordering / the cross-join pushdown gap (separate optimizer work).
