# Synthetic event-analytics benchmark

ONE benchmark entry point: `run_events.py`. Do not create another runner script
here - extend it. Every number this suite produces comes from it. It has three
subcommands; `run` is the default when none is named.

This suite times the engine's event analytics (FUNNEL / SEGMENT / PATHS over an
EVENT VIEW) against the honest pure-DuckDB SQL a user would otherwise write over
the same file. The DuckDB baseline is measured ONCE per dataset by `save-refs`
and read back by `run` - it is a function of the data, never re-measured per
invocation.

## One-time data setup (per scale)

```
/workspace/venv-fedq/bin/python run_events.py generate  --scale small
/workspace/venv-fedq/bin/python run_events.py save-refs  --scale small
```

- `generate` builds `data/events_<scale>.duckdb`: one `events` table of
  synthetic events. Every column is a DETERMINISTIC function of the row index
  (`hash(i, salt)`, not a thread-order-dependent RNG), so the same scale always
  produces the same bytes regardless of DuckDB's parallelism.
- `save-refs` measures every analysis's DuckDB baseline once (timing plus a
  result signature) into `data/references_<scale>.duckdb`. It REFUSES to
  overwrite a populated refs file; rebuild by deleting the file first (only when
  the DATA changed). A baseline that exceeds `--baseline-timeout` (default 300s)
  is interrupted and recorded as N/A with the reason - naive SQL not scaling is
  itself a finding, never silently dropped.

## Run the benchmark

```
/workspace/venv-fedq/bin/python run_events.py run --scale small --warm-runs 3
```

(equivalently, with no subcommand: `run_events.py --scale small ...`)

- Builds the config, then over one runtime: DROP any leftover view, CREATE EVENT
  VIEW over the generated table (timed), REFRESH it (timed), and time each
  analysis cold (first run) plus warm (median of `--warm-runs`), next to the
  CACHED DuckDB baseline for each.
- Cross-checks every engine result against the baseline's stored signature
  (AGREE = identical counts, DIFFER = a real divergence, N/A = the baseline was
  not measured). The baselines reproduce the engine's pinned semantics exactly,
  so AGREE is a genuine correctness gate.
- Prints, before running, how many engine executions the invocation performs
  (1 CREATE + 1 REFRESH + analyses x (1 + warm-runs)).
- Report: `reports/events-<scale>.md`.

The six analyses: FUNNEL common (3 frequent events) and FUNNEL selective (3 rare
events), both WITHIN 7 DAYS; SEGMENT MEASURE EVENTS and MEASURE ENTITIES, both
BY DAY; PATHS MAX DEPTH 5 TOP 20 with and without STARTING AT.

## Scales

| Scale  | Events        | Entities  | Approx file |
| ------ | ------------- | --------- | ----------- |
| small  | 1,000,000     | 50,000    | ~9 MB       |
| medium | 100,000,000   | 500,000   | ~7-8 GB     |
| large  | 1,000,000,000 | 5,000,000 | ~75 GB      |

`--events N` / `--entities E` override the defaults for a custom size. `large`
is defined but only builds where disk allows (`generate` prints the file size).

## Hard wall budgets (deterministic, not overridable)

`run` is killed if it exceeds its scale's budget: small 180s, medium 900s, large
3600s. A daemon watchdog `os._exit`s the process (exit code 124) so a hung
native call cannot outlive it. A run over budget is a regression to fix, not a
budget to raise.

## The dataset shape

- Entities are drawn from a bounded power law, `floor(E * u^2)` with
  `u = hash(...)/2^64`: density goes as `entity^(-1/2)`, so head entities carry
  many events and the tail is long, while the top entity stays a realistic
  ~0.4% of the log. (A pure Zipf s~=1, `floor(pow(E, u))`, would give one head
  entity ~5-6% of ALL events - unrealistic, and enough to make the per-entity
  funnel worst case pathological.)
- 20 event types, drawn Zipf over `pow(21, u)` so `page_view` dominates and
  `cancel_account` is the rare tail. The two funnels sit at deliberately
  different frequencies (a common one over head events, a selective one over
  rarer ones).
- Each draw hashes a DISTINCT integer (`i * 11 + salt`): DuckDB's multi-argument
  `hash(i, salt)` only shifts the base hash by the salt, correlating the streams
  (~0.6), which would tie an entity to a single event name. Distinct inputs
  decorrelate them (each entity then sees ~9 of the 20 event types).
- Timestamps are sessionized across 30 days: each entity's events cluster into
  sessions whose base instant is a hash of (entity, session), so a session is
  one burst; sessions scatter over the span.
- Property columns `device` and `country`, skew-drawn from small pools.
- A `seq` column (the global row index) is a real TIEBREAK: it gives
  equal-(entity, timestamp) events a deterministic order, and the paths baseline
  sorts by the same key.

## The baselines (what "honest" means per analysis)

- FUNNEL: a self-join + greedy-earliest aggregation - for each entity, the
  earliest step-2 strictly after step 1 within the anchored window, then the
  earliest step-3 after that; distinct-entity counts per depth give re-entry
  (max over attempts) for free. This is the standard single-source anchored
  funnel SQL.
- SEGMENT: a plain `GROUP BY date_trunc('day', ts)` with `count(*)` or
  `count(DISTINCT entity)` - the same UTC calendar-day bucket the engine uses.
- PATHS: a window-function reconstruction - order each entity's events by
  (timestamp, tiebreak), collapse consecutive duplicate names, keep the first
  MAX DEPTH steps from the anchor, join into a ` -> ` path, rank distinct paths
  by entity count then path.

All three reproduce the pinned semantics of `events-plan.md` section 5, so the
run-time cross-check is a real correctness gate, not decoration.

## Files

- `run_events.py` - THE runner and the only .py file here (generate / save-refs
  / run).
- `data/` - duck dataset files and references; `reports/` - runner output.
