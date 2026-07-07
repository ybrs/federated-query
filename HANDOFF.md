# Handoff: TPC-DS federated coverage + correctness

State of the work, facts only. Earlier phases (Rust cutover, N-K decorrelation,
cost-based optimizer, TPC-H fair benchmark) are in git history and in the
auto-memory; this document is the CURRENT TPC-DS federated push.

Test suite: **1221 passed, 3 skipped, 39 xfailed** (`POSTGRES_DB=duckpoc
/workspace/venv-fedq/bin/python -m pytest -q`).

---

## OPEN ISSUES (investigate next)

### q18: avg(decimal) differs by 0.01 cross-source - SUSPECTED TYPE CONVERSION

The one remaining MISMATCH. `q18` computes `avg(cast(<col> AS decimal(12,2)))`
under `GROUP BY ROLLUP(...)`; a ROLLUP subtotal row differs from pure DuckDB by
0.01 in the last decimal (e.g. agg2 `206.98` vs `206.99`, agg5 `-1022.42` vs
`-1022.41`).

NOT floating-point summation order: for float64 that effect is ~1e-10 on these
magnitudes, far below 0.01. The size points to a **precision/type-conversion
bug** - a decimal likely narrowed to float32 (or a scale truncated) somewhere in
the CROSS-SOURCE aggregation path. Evidence gathered:
- Single-source the engine matches DuckDB bit-for-bit: `avg(cast(cs_list_price
  AS decimal(12,2)))` over `catalog_sales` = `100.99702512344902` on both.
- The 0.01 gap appears only cross-source (fact on DuckDB, dims on Postgres,
  avg computed at the coordinator / DataFusion).
- Engine avg output type is `double`; DuckDB's `avg(cast(... as decimal))` is
  also `DOUBLE`. Result is deterministic run-to-run.
- Reproduce: `run_federated.py --scale-factor 0.1 --placements pg-dims --only 18`
  (correctness is vs pure DuckDB, so this is a real engine-vs-DuckDB diff).

Where to look: the type carried for the fetched decimal columns across the
Postgres/ADBC boundary and into the DataFusion aggregate (float32 vs float64 vs
decimal); the `cast(... AS decimal(12,2))` rendering in the pushed SQL vs at the
coordinator. Deferred by request.

---

## Current status (pg-dims, SF0.1)

`PASS 54 | MISMATCH 1 (q18, above) | ERROR 44` of 99. No OTHER wrong results:
correctness is compared against pure DuckDB, so a MISMATCH is a real engine diff.

Remaining ERROR clusters (by cause):
- **UnsupportedIR - 27**: WindowExpr at the coordinator (~11), `JoinType.CROSS`
  (~5), `PhysicalSingleRowGuard` (scalar-subquery guard, ~3), and other physical
  nodes. Window functions at the coordinator are the biggest sub-group - the
  highest-yield remaining target.
- **RuntimeError**: was 15; the aggregate-output-naming group (ORDER BY sum(x))
  and the self-join-of-union naming group are FIXED (below). Remaining: the
  "Mismatch between schema and batches" class (q16/q94/q95), `type_coercion`
  that is really a substring-output name (q79/q91), and a couple of others.
- UnsupportedSQLError - 2 (simple CASE, window in WHERE).

Set operations (UNION/INTERSECT/EXCEPT) are DONE - emitted via the raw_sql
escape hatch in `rust_ir` (`_emit_union` / `_emit_set_operation`); no Rust change.

## RuntimeError naming bugs fixed (this round)

- **ORDER BY sum(x) re-applied over the aggregate** (`binder.py`
  `_match_aggregate_output`): a bound sort key that equals an aggregate output
  expression now references that output column, not a FunctionCall recomputing
  sum over the aggregate's own output (which lacks raw x). Fixed q42/q85/q92/q96.
- **PhysicalProjection.column_aliases() leaked the input's columns**
  (`physical.py`): it returned `self.input.column_aliases()` even when the
  projection drops/renames columns, so a sort ABOVE a projection resolved a
  passthrough self-join-renamed column (right_customer_id) to a name the
  4-column projection had dropped. Now it returns the projection's OUTPUT
  contract (one entry per output, resolvable by output name or a passthrough's
  source column); the projection's own expressions are typed against
  self.input.column_aliases() directly. Fixed q04/q11/q74/q84. Debugged with a
  temporary FEDQRS_SCHEMA per-fragment schema trace in the Rust engine (removed).

## Benchmark setup (important)

- **Dedicated Postgres database per scale**: `pg_database_name(sf)` ->
  `tpcds_sf01` / `tpcds_sf1` / `tpcds_sf10` (generate.py). TPC-DS no longer
  shares `duckpoc` with TPC-H (they collided on the `customer` table; TPC-H's
  8-column customer shadowed TPC-DS's, breaking c_customer_sk binding). Reload
  after `make pg-start`: `load_postgres.py --scale-factor 0.1` (fills
  `tpcds_sf01`; the DB itself is created once via psycopg2 CREATE DATABASE).
- **Correctness vs pure DuckDB, timing vs the federated oracle** (split refs).
  The federated DuckDB oracle (Postgres attached) has scanner quirks that are
  NOT our bugs - q59 filter-pushdown dropped all rows (worked around with
  `SET pg_experimental_filter_pushdown=false`), q18 avg drift - so it is used
  ONLY for the timing baseline; correctness compares the engine against pure
  DuckDB over the same file (the canonical single-source answer).
- Each query runs in one isolated child (engine + both references together),
  memory-capped by an **RSS watchdog** (not RLIMIT_AS, which the Rust engine's
  virtual reservations tripped falsely - q72).

## Engine fixes made this session (all with tests, all committed)

- **Self-join output-name collision** (`physical.py _right_output_name`): a
  left-deep self-join of a CTE (q31 ss1..ss3) produced two `right_<name>`
  columns; now suffixed `_1, _2` until unique.
- **Positional ORDER BY** (`binder.py`): `ORDER BY <n>` resolves to the n-th
  output, not left as an integer literal (q62 vanished cross-source).
- **NULLIF / ROUND at the coordinator** (`expression_evaluator.py`): handlers
  added; the dialect's safe-division rewrite `a/b -> a/NULLIF(b,0)` no longer
  errors. NULLIF kept (not rewritten to CASE, which double-evaluates a).
- **Join read-set over-collection** (`parser.py _columns_for_join_table`): a
  join input's read-set now over-collects EVERY referenced column (the binder
  drops what a table lacks); an unqualified aggregate measure was being lost
  from the fact scan (q03 "No field named ss_ext_sales_price"). The parser does
  NOT attribute columns to tables - that is the binder's job once refs are
  qualified.
- **Reduction gate** (`rust_ir.py _probe_base_resolvable`): a join whose probe
  has no single injectable base emits a normal join instead of crashing on a
  None base (q96 AttributeError).
- **Case-insensitive table qualifiers** (`binder.py _resolve_qualified`): a
  `catalog` reference resolves against a `CATALOG` alias (q49).

## How to run

```
make pg-start                                        # Postgres in a container
cd benchmarks/tpcds
python generate.py --scale-factor 0.1                # DuckDB file (once)
python load_postgres.py --scale-factor 0.1           # into tpcds_sf01 (once)
PGUSER=postgres python run_federated.py --scale-factor 0.1 \
    --placements pg-dims --timeout 30 --report out.md
```

`run.py` is the single-source (pure DuckDB) variant. venv:
`/workspace/venv-fedq`. Engine (Rust) rebuild:
`PATH="$HOME/.cargo/bin:$PATH" VIRTUAL_ENV=/workspace/venv-fedq \
/workspace/venv-fedq/bin/maturin develop --release -m /workspace/fedqrs/Cargo.toml`.
