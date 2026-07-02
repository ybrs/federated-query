
**Status:** removed on 2026-06-18 (branch `phase8`). This note records what the
feature was, why it existed, why it was removed, and what must be true if it is
ever reintroduced.

## What it was

`ExpressionSimplificationRule` (in `optimizer/rules.py`) ran two rewriters over

- `ConstantFoldingRewriter`: evaluated operations on literal operands at compile

## Why it existed

It predates the **DuckDB merge engine**. When local execution was a
row-at-a-time Python evaluator, folding constants in the optimizer was a cheap
way to shrink work before it hit that slow path.

## Why it was removed

In the current architecture folding on fedq is both **redundant** and
**incorrect**:

1. **Redundant.** Single-source expressions are *pushed to the source*, which
   folds them itself; local/cross-source expressions run in the *DuckDB merge
   engine*, which also folds them. The Arrow evaluator already computes a
   pure-local `SELECT 1 + 2`. Nothing needed fedq to pre-fold.

2. **Wrong types.** Folding `0.1 + 0.2` in Python produced the float
   `0.30000000000000004` and baked it into the pushed SQL. Pushing the
   expression unevaluated lets DuckDB compute it with DECIMAL arithmetic and
   return an exact `0.3`. (See `test_float_addition_computed_by_source`.)

3. **Wrong NULL semantics (a correctness bug).** The algebraic simplifier
   `quantity * 0` to `0`, then `0 = 0` folded to `TRUE`, so
   `WHERE price - price = 0 AND quantity * 0 = 0` collapsed to `WHERE TRUE`.
   That is unsound: when `price`/`quantity` is NULL the real predicate is
   `UNKNOWN`, not `TRUE`, so NULL rows were wrongly **kept**. The note here is
   *algebraically replaced by 0*, and then the comparison `0 = 0` is genuinely

## What replaced it


  expression evaluator.

## If folding is ever reintroduced

It would be "engine territory" and must clear the bars this version failed:

- **Decimal/string fidelity.** Numeric folding must match the source's type
  system (e.g. `Decimal`, not Python `float`); string/other operators
  (`str || str`, etc.) raise the same fidelity question. Easiest correct rule:
  only fold when the result is provably identical across fedq and every target
  is non-nullable. Apply them only with proven non-null inputs.
- **Tests.** `test_float_addition_computed_by_source` (exact `0.3` from the
  source) and `test_constant_false_predicate_returns_no_rows`
  folding happens; any reintroduction must keep them green.
