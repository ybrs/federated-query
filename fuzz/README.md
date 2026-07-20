# Federated query fuzzer

A differential and metamorphic fuzzer for the Rust federated SQL engine. It rides
the existing `tests/e2e_federated` harness (table library, placements, single-
DuckDB oracle, value-level compare, bounded env cache) rather than rebuilding any
of it: generated and mutated queries reference tables as `{table_name}`
placeholders exactly like a corpus case, so the harness qualifies them per
placement, and correctness is judged against the same single-DuckDB oracle.

## How to run

```bash
export POSTGRES_DB=test_db            # the shared test database
python -m fuzz.run_fuzz --minutes 5 --seed 1 --profile smoke   # ~5 min
python -m fuzz.run_fuzz --minutes 25 --seed 2 --profile deep   # longer
```

- `--seed S` makes the whole run deterministic: the same seed reproduces the same
  generation and mutation stream.
- `--profile smoke|deep` sets batch sizes and the per-query watchdog (smoke 15s,
  deep 30s).
- `--replay INDEX` re-runs the single deterministic work item at that index (with
  the deep oracles forced), reproducing a finding named in an artifact.
- `FEDQ_E2E_SKIP_PG=1` runs without PostgreSQL (every PostgreSQL placement is
  skipped, exactly as the e2e suite skips them).

PostgreSQL must be reachable unless `FEDQ_E2E_SKIP_PG=1`. A live stat line prints
every 30 seconds; a final report lists stats, the settings-variant classification,
and every finding.

## The oracle stack

Each generated or mutated query passes through, in order:

1. **Baseline.** Run on the all-tables single-DuckDB oracle. DuckDB rejects it ->
   discard (counted). DuckDB accepts -> its result is the ground truth.
2. **Placement invariance.** Run through the engine under a per-iteration sample
   of placements (always `oracle_single_duck` plus one PostgreSQL and one Parquet
   placement, plus a rotating pair) and compare each to ground truth with the
   harness's value-level `compare_tables`.
3. **Settings invariance.** For a sampled subset, re-run under each live optimizer
   variant; every variant must reproduce the ground truth. A mismatch names the
   guilty optimization.
4. **TLP.** For a plain SELECT with a splittable WHERE, check that
   `Q == Q[P] UNION ALL Q[NOT P] UNION ALL Q[P IS NULL]` as a multiset, with `P`
   biased toward a column on a different source than the FROM anchor.
5. **Contract.** The only acceptable engine outcomes are a value-match or a known
   designed raise. A mismatch, a panic, a hang (per-query watchdog), or an
   undocumented raise on oracle-accepted SQL is a finding.

Corpus-seeded **preserving** mutations additionally feed a metamorphic check: the
engine's result for the mutated query must equal its result for the original under
one cross-source placement.

## Settings variants (the optimizer fuzz)

The variants are verified at startup: each is applied and the engine's `EXPLAIN`
plan for a probe set is compared before/after. A variant that changes at least one
probe plan is `live` and swept by the settings oracle; a variant that never
changes a plan is a `dead` knob (logged, not counted as coverage); the planning-
budget raise is the `control` (never expected to change a plan or a result).

| Variant                   | Toggles                                                        | Role    |
|---------------------------|---------------------------------------------------------------|---------|
| `predicate_pushdown_off`  | `optimizer.enable_predicate_pushdown = false`                 | live    |
| `projection_pushdown_off` | `optimizer.enable_projection_pushdown = false`                | live    |
| `join_reorder_off`        | `optimizer.enable_join_reordering = false`                    | live    |
| `join_reorder_goo`        | `optimizer.max_join_reorder_size = 1` (forces GOO greedy)     | live    |
| `dim_shipping_aggressive` | `ship_local_floor=0`, `ship_row_budget=1e8`, `ship_min_ratio=1` | live  |
| `decorrelation_off`       | `optimizer.enable_decorrelation = false`                      | dead    |
| `accelerator_off`         | `accelerator.enable_substitution = false`                     | dead    |
| `planning_budget_high`    | `optimizer.planning_budget_ms = 60000`                        | control |

`decorrelation_off` is dead because the decorrelation pass runs unconditionally
(the gate has no effect, per its own setting description); `accelerator_off` is
dead because no materialized view exists to substitute. Both are kept in the list
so the dead-knob detector demonstrably catches them.

Variants apply via `SET <name> = <value>` on the live runtime (the engine reads
session-mutable settings fresh per query) and are always followed by `RESET ALL`.

## How findings flow to the corpus and tickets

Every deduped finding is written under `fuzz/artifacts/<timestamp>/` as a file
carrying a ready-to-park e2e corpus dict (`name`, `tables`, `query`, `finding`), a
greedily shrunk repro query, and the exact `--replay` command. Triage each:

- **Real engine bug** -> minimize, park the corpus dict in
  `fuzz/findings_corpus.py` (`SUSPECTED_ENGINE_BUGS` until the fix lands, then
  `CASES`), file a tracker ticket, and the e2e suite guards it thereafter.
- **Oracle/generator bug** -> fix the generator or add a discard rule.
- **Known class** -> the dedupe signature and the known-error / discard lists keep
  it from recurring.

`fuzz/findings_corpus.py` is registered in `tests/e2e_federated/cases.py` as the
`fuzz_findings` corpus, so `FEDQ_E2E_CORPUS=fuzz_findings pytest tests/e2e_federated`
runs exactly the parked findings.

## Known discard classes (kept out of the surface to avoid false findings)

The generator deliberately never emits, and the oracle stack discards, constructs
that legitimately diverge between the DuckDB baseline and the engine, so a report
is a real bug and not a dialect artifact:

- **Boolean in WHERE/GROUP BY/JOIN-ON.** A boolean column of a PostgreSQL-hosted
  table trips the engine's stats-probe bug (a documented `SUSPECTED_ENGINE_BUGS`
  entry). Booleans are projected but never predicated or grouped.
- **Integer division / modulo.** Never generated (DuckDB and PostgreSQL disagree).
- **Bare LIMIT without a total ORDER BY.** Never generated (nondeterministic which
  rows). LIMIT appears only with a unique total ordering.
- **Non-total-order window frames.** Window ORDER BY always ends in a unique key,
  so `ROW_NUMBER`/running sums are deterministic per row.
- **Oracle rejection.** A generated query DuckDB itself rejects is discarded, not
  reported.
- **Known designed raises.** Engine raises naming a designed restriction
  (cross-source `NATURAL`/`USING`, unsupported DML/DDL, ordered-set aggregate
  FILTER, tuple-IN, etc.) are discarded; an *undocumented* raise on oracle-accepted
  SQL is a lower-severity surface-gap finding.
- **Planning-budget kills.** Retried once; a deterministic kill is discarded (it is
  by-design under the O(metadata) budget), not reported.

## Files

- `generator.py` - typed SQL generator as hypothesis strategies over the derived
  table schema (joins, subqueries, set ops, aggregates, windows, CTEs, derived
  tables), federated-biased.
- `mutator.py` - corpus-seeded semantics-preserving and semantics-changing
  mutations.
- `oracles.py` - the baseline / placement / settings / TLP / metamorphic / contract
  check stack, environment and oracle caches, and the startup variant verifier.
- `reducer.py` - greedy structural minimizer for mutation-derived findings.
- `run_fuzz.py` - the deterministic, time-bounded driver.
- `findings_corpus.py` - the `fuzz_findings` e2e corpus module (parked findings).
```
