# 9. Accelerator runtimes and the reproducible experiment

All values below come from the existing `benchmarks/perf_compare/compare.py`
runner's `studio-accelerator` scenario, through the release FedQ server and
actual Compose-backed sources. The two runs used identical engine, harness,
SQL, and fixture hashes. Both completed every lifecycle phase and restored
the original source data.

Each number is a median: **3 cold samples**, each in a fresh Python worker
and fresh release-server process/runtime, or **5 warm samples** after 2
untimed warmups on one live server connection. Cold does not mean that the
OS page cache or upstream database buffer pools were emptied.

The measured interval starts before issuing the statement and ends after its
result has been fully decoded by the client. It includes the engine's own
planning and execution, wire transport, and result decoding. Server startup,
fixture preparation, and the DROP before each CREATE are outside that
statement's interval. CREATE and REFRESH timings include all their internal
work. SQL tracing and fragment recording are enabled throughout.

| Statement/state | Run A cold ms | Run A warm ms | Run B cold ms | Run B warm ms |
| --- | ---: | ---: | ---: | ---: |
| Live join before materialization | 149.110 | 14.643 | 112.998 | 14.067 |
| CREATE MATERIALIZED VIEW | 307.539 | 201.033 | 300.804 | 226.065 |
| Same SELECT, substituted | 67.298 | 66.623 | 83.546 | 58.256 |
| Same SELECT after inserts, still stale | 83.225 | 58.262 | 75.133 | 58.308 |
| Live control, substitution disabled | 91.549 | 16.157 | 157.517 | 16.113 |
| REFRESH, whole re-pull | 236.242 | 75.180 | 209.543 | 149.966 |
| Same SELECT after refresh | 83.562 | 66.599 | 83.456 | 58.309 |

For a repeated warm dashboard read, the result is a regression: the live
join took 14.643 / 14.067 ms in runs A/B, while the first substituted read
phase took 66.623 / 58.256 ms. Cached reads avoided the three upstream data
fragments, but they did not deliver lower warm end-to-end latency on this
fixture. The cold cached medians were lower than the cold live medians in
both runs. Neither observation should be replaced by a blanket speedup claim.

The cost breakdown was not measured, so the cause of the warm regression
remains unverified. `cost_saved` in `SHOW MATERIALIZED VIEWS` is a model's
estimated work-saving counter, not measured milliseconds, and does not
contradict or override the actual latency measurements above.

Every sampled REFRESH reported a whole re-pull; these medians do not time a
no-op refresh and label it a rebuild. Repeated CREATE samples each start with
that view absent. Build and refresh also vary across runs, which is why both
cold and warm samples and both independent runs are retained.

Learned-statistics **reads** are disabled in the measured sessions so a cold
sample does not inherit learned estimates from a previous process's stored
catalog. Statistics can still be written. The planner's default wall-clock
budget is unchanged. The earlier direct functionality check with the normal
showcase configuration also demonstrated substitution and refresh; this
measurement control is not presented as a prerequisite for materialization.

The checked-in evidence is `tutorials/game-studio/evidence/accelerator.json`.
It contains all timed samples, warmup statuses, expected and actual result
states, plans, source fragments, counters, chunk hashes, and provenance for
runs `game-studio-accelerator-run3` and `game-studio-accelerator-run4`. The
complete generated reports retain server/worker logs and exact Arrow chunk
copies alongside their `results.json` files. The evidence keeps a SHA-256 of
each complete report.

## Reproduce the complete experiment against Docker Compose

Use the source databases and generated fixtures from [the setup chapter](setup.md). The harness
starts its own release FedQ servers and uses a private per-report catalog;
it does not require the earlier foreground FedQ server to remain running.
Run without another test suite or query workload contending with measurement.

```bash
docker compose -f tutorials/game-studio/compose.yaml up -d --wait
"$VIRTUAL_ENV/bin/python3" benchmarks/perf_compare/compare.py \
  studio-accelerator --label my-studio-accelerator-run
```

Use the same `GAME_STUDIO_DOCKER='sudo -n docker'` setting described in
[the setup chapter](setup.md) when your host requires it. Each label must be new; the runner
refuses to overwrite an existing report. It prints its full workload before
running: 175 FedQ client statements by default, including 56 timed targets,
14 untimed warmups, and the remaining setup/inspection/cleanup calls. All
seven phases run, plus the real MySQL insert and cleanup statements.

The harness checks the original and post-insert results with DuckDB once per
data state before measurement, then validates every sampled answer against
those references. It requires live plans to read all three sources and cached
plans/fragments to read the materialized result. It compares chunk hashes
before and after source changes, checks that refresh changes the result,
and verifies restoration after cleanup. SQL execution errors and planning-budget
kills are recorded as ERROR/KILLED and fail the run. Preflight, validation,
or cleanup failures also raise and prevent a completed report; no failed
sample is discarded to manufacture a successful median.

The two reserved ticket IDs must be absent before execution. On exit the
harness removes its view and only its own inserted ticket payloads, then
checks that the live answer and reserved-ID count have returned to the
original fixture state. It retains the evidence and chunk copies in the
report directory. A manually reproduced session can remove the view with:

```sql
DROP MATERIALIZED VIEW studio_accelerator_support;
```

The matching MySQL cleanup is:

```sql
DELETE FROM tickets
WHERE player_id = 1 AND category = 'gameplay' AND closed_at IS NULL
  AND (
      (ticket_id = 900000001 AND opened_at = '2026-01-30 12:00:00'
       AND handling_minutes = 17)
      OR
      (ticket_id = 900000002 AND opened_at = '2026-01-30 13:00:00'
       AND handling_minutes = 23)
  );
```

The accelerator chapters establish result reuse, explicit freshness, and a successful
full catch-up over three real sources. It also establishes a reproducible
warm-latency regression on the default fixture. Both belong in the account of
how the current accelerator behaves.

---

[Contents](README.md) | [Previous: 08-refresh-and-freshness.md](08-refresh-and-freshness.md) | [Next: 10-data-contracts.md](10-data-contracts.md)
