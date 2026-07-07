# Disjunctive decorrelation - plan

Goal: make the three remaining TPC-DS ERROR queries pass (q10, q35, q45),
and make OR-of-subqueries plans efficient in general. ASCII only.

Status: COMPLETE 2026-07-07. Phase 1 DONE (commit 1767f60): all three pass,
ERROR column closed. Phase 2 DONE (commit e68149e): q10 522 -> 143ms (1.49x),
q35 567 -> 136ms (1.72x). Phase 3 NOT TAKEN by its own decision gate: after
phases 1+2 the mixed-disjunction flag path (q45) sits at ~120ms / 3.1x,
below the next outlier family - a LeftMark join buys too little to justify
the Rust surface. Revisit only if a workload makes flag-path disjunctions
hot at scale.

## 1. The three queries and their shapes

- q10 / q35: `... AND (EXISTS (SELECT ... WHERE c.c_customer_sk = ws_bill_customer_sk ...)
  OR EXISTS (SELECT ... WHERE c.c_customer_sk = cs_ship_customer_sk ...))`.
  Two POSITIVE correlated EXISTS, both correlating on the SAME outer key
  expression (c.c_customer_sk), OR'd together.
- q45: `(substring(ca_zip,1,5) IN ('85669', ...)) OR (i_item_id IN (SELECT
  i_item_id FROM item WHERE ...))`. A plain predicate OR'd with an
  UNCORRELATED IN-subquery.

## 2. Diagnosis: where the cartesian product actually comes from

The decorrelator's OR handling (`_expand_or` / `_disjunct_term`,
optimizer/decorrelation.py) already rewrites each subquery disjunct into a
boolean flag via a SEMI/ANTI UNION SPLIT: the input subtree is duplicated into
a SEMI-join branch (flag TRUE) and an ANTI-join branch (flag FALSE), unioned,
with one filter above OR-ing the flags with the plain disjuncts. The SEMI/ANTI
joins themselves carry proper equality conditions (hash-joinable).

The blowup is NOT the flag rewrite. The original WHERE's OTHER conjuncts -
including every comma-join equality (`ws_bill_customer_sk = c_customer_sk`,
`ws_sold_date_sk = d_date_sk`, `d_qoy = 2`, ...) - sit in a Filter ABOVE the
Union, and `PredicatePushdownRule._rehome_filter_over_opaque`
(optimizer/rules.py) deliberately refuses to push any predicate below a
SetOperation/Union. Verified on q45 pg-dims: the physical plan is

```
PhysicalFilter (ALL join equalities AND the OR of flags)
  PhysicalUnion
    branch: web_sales CROSS customer CROSS customer_address CROSS date_dim CROSS RemoteQuery(item semi)
    branch: same input replicated, ANTI variant
```

Five-way conditionless cross joins per branch - the memory pool kills it.
So the first fix is a PUSHDOWN gap, not a new decorrelation technique.
(Same family as the walker-descent-stops lesson: a rule that silently
declines a node type strands work above it.)

Second-order problem (correct but wasteful): the union split replicates the
ENTIRE input subtree once per SEMI/ANTI pair, and NESTS when several
subquery disjuncts stack (q10's input appears 4x). Phases 2 and 3 remove the
replication for the common shapes.

## 3. Facts about the available machinery

- SEMI/ANTI joins with equality conditions plan as hash joins and take part
  in the semi-join reduction / dynamic key injection.
- Predicate pushdown, join ordering, projection pushdown all run AFTER
  decorrelation, so whatever shape decorrelation emits gets optimized -
  IF the rules descend into it.
- A deterministic filter commutes with every set operation when pushed into
  BOTH branches (same row values produce the same verdict on either side);
  our engine has no volatile predicate expressions.
- DataFusion 54 exposes `JoinType::LeftMark` (datafusion-common
  join_type.rs): left join emitting a boolean mark column - "does this left
  row have a match" - i.e. exactly a hash-computed EXISTS flag with NO input
  replication. fedqrs currently maps Inner/Left/Right/Full/Semi/Anti only.

## 4. Plan

### Phase 1 - predicate pushdown through set operations (enabler + standalone win)

Teach `PredicatePushdownRule` to push a Filter through Union / SetOperation
by cloning the predicate into every branch (rewriting column refs through
each branch's positionally-aligned output schema), instead of re-homing above.

- Gate: every branch schema matches the set-op output positionally (the
  OR-union branches are identical by construction; a mismatch declines and
  keeps today's behavior).
- Recurse afterward so the per-branch filter continues down into the joins
  (turning the cross joins into the normal filtered hash joins, which then
  get join ordering + semi-join reduction as usual).
- Expected effect: q10/q35/q45 all pass with ~2x input cost (the union split
  still replicates the input). This alone closes the ERROR column.
- Tests: rule-level (filter over UNION ALL/DISTINCT, INTERSECT, EXCEPT;
  name-remapping across differently-aliased branches; decline on arity or
  name mismatch) + e2e cross-source OR-of-EXISTS correctness + the three
  TPC-DS queries.
- Risk: touches a deliberately conservative arm; existing set-op suites and
  TPC-DS q02/q14/q77 (union-heavy, PASS today) are the regression canaries.

### Phase 2 - common-key OR of positive existentials -> one SEMI over a domain union (q10/q35 exact shape)

When EVERY disjunct is a positive existential (EXISTS / IN / = ANY) whose
correlation reduces to an equality on the SAME outer key expression:

```
EXISTS(S1: k = outer.k) OR EXISTS(S2: k = outer.k)
  ==  outer SEMI JOIN (SELECT k1 AS k FROM S1' UNION SELECT k2 AS k FROM S2') ON outer.k = k
```

- No input replication at all; ONE hash SEMI join; the build side (the
  unioned key domain) feeds the existing dynamic-filter injection into the
  probe scan - the "always decorrelate, make the join cheap" doctrine shape.
- UNION here is DISTINCT (a domain); duplicates in either subquery are
  irrelevant to EXISTS semantics, and SEMI join preserves outer multiplicity.
- Detection lives in `_expand_or`: if all disjuncts qualify, emit the domain
  union + one SEMI join; otherwise fall through to the flag path (which
  Phase 1 already made survivable). Mixed positive/negative disjuncts
  (NOT EXISTS / NOT IN) stay on the flag path: NULL-aware ANTI semantics do
  not distribute over a domain union.
- Tests: q10/q35 shapes single- and cross-source; duplicate outer rows keep
  multiplicity; disjuncts on DIFFERENT keys fall through to the flag path;
  NOT EXISTS in the OR falls through.

### Phase 3 (evaluate after 1+2) - mark-join flags instead of the union split (q45-class)

For disjunctions Phase 2 cannot take (mixed plain predicate + subquery, or
different correlation keys - q45), replace the SEMI/ANTI union split with a
MARK join per subquery disjunct: LEFT MARK join on the disjunct's equality,
flag column = mark, one filter ORs the marks with the plain disjuncts.
Removes the 2x-per-disjunct input replication and the nesting explosion.

- fedqrs: map a new IR join kind `mark` to `JoinType::LeftMark` (join
  fragment already generic over join_type); the mark column surfaces as a
  boolean output column named by the IR.
- Python: `PhysicalHashJoin` grows a mark variant (or a thin
  `PhysicalMarkJoin`); `_disjunct_term` emits flag = mark column instead of
  the union split; `column_aliases()` exposes the flag.
- Single-source pushdown: a mark join does not render to one SQL query; a
  same-source subtree under it still collapses per side (unchanged).
- Decide AFTER measuring Phase 1: if q45 at 2x input is fast enough at SF1,
  Phase 3 is a perf refinement, not a requirement.

## 5. Order and deliverables

1. Phase 1, with rule tests + the three queries + full 99-tally (expect
   PASS 98 | MISMATCH 1 | ERROR 0, timing TBD).
2. Phase 2, then re-tally and compare q10/q35 timings against Phase 1.
3. Phase 3 decision from the Phase 1/2 numbers (mark join is the only part
   needing Rust changes).

Out of scope: q18 (avg-of-decimal MISMATCH, separate open issue);
NOT-IN/NOT-EXISTS inside OR beyond the flag path; disjunctions of scalar
subquery comparisons (they already flatten through the value path).
