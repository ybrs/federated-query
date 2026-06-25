# fedq vs DuckDB — federated query comparison

**Date:** 2026-06-15 · What **fedq** and **DuckDB's `postgres_scanner`** push to
PostgreSQL on the same queries + data, with full timings and a correctness check.
Deep mechanics: [`duckdb-pushdowns.md`](duckdb-pushdowns.md).

**Two scenarios, two detailed docs** (this file is the overview):
- **[postgresql-duckdb-sources-compare.md](postgresql-duckdb-sources-compare.md)** —
  fact table in a *local DuckDB* (DuckDB's best case). Per-query: what each engine
  sends to each source, with timings.
- **[postgresql-clickhouse-sources-compare.md](postgresql-clickhouse-sources-compare.md)** —
  fact table in an *external ClickHouse* (the fair comparison). Per-query: what
  each engine sends to ClickHouse + Postgres, and the time spent on ClickHouse.

## Setup

| source | table | rows | columns |
|---|---|--:|---|
| **PostgreSQL** (`duckpoc`, PG 17) | `categories` | 50,000 | id, name, parent_id, is_active, created_at |
| **PostgreSQL** | `files` | 400,000 | id, category_id→categories.id, filename, size_bytes, owner_id, created_at |
| **DuckDB** (`analytics-poc.duckdb`) | `access_logs` | 10,000,000 | id, file_id→files.id (skewed), accessed_at, action, bytes_sent, user_id, day |

DuckDB tested at **stable v1.5.3** and **main** (built from source). DuckDB
transfers via binary `COPY … TO STDOUT` split into 4 `ctid` page ranges (shown as
`×4`); fedq sends ordinary `SELECT`s over ADBC/psycopg2. "What PG receives" is read
from the PG log (`log_statement=all`).

---

## Timing — every query, all engines

Median of 5 warm runs, milliseconds (localhost). fedq is shown for **both**
PostgreSQL drivers: `psyco` = psycopg2 (plain `SELECT` + tuple→Arrow),
`adbc` = ADBC (binary `COPY`). Reproduce: `make bench` / `./bench-all.sh`.

| Q | what it tests | result rows | stable | main | fedq-psyco | fedq-adbc |
|---|---|--:|--:|--:|--:|--:|
| Q0 | `count(*)` on remote | 1 | 27 | 27 | 11 | 11 |
| Q1 | headline: 3-table cross-source `GROUP BY` | 305,524 | 128 | 145 | 548 | **491** |
| Q2 | filter + projection (1 table) | 3 | 2 | 3 | 5.5 | **2.3** |
| Q3 | projection + filter `id<100` | 99 | 2 | 3 | 4.8 | **2.4** |
| Q4 | same-source join + aggregate | 5 | 52 | 59 | **112** | 125 |
| Q5 | remote aggregate `GROUP BY` | 5 | 48 | 47 | 110 | 110 |
| Q6 | cross-source, non-selective | 5 | 229 | 245 | 1148 | **1051** |
| Q7 | cross-source, selective (10 keys) | 5 | 45 | 45 | 21 | **16** |
| Q8 | comparison filters inside cross-source join | 5 | 59 | 62 | 553 | **459** |
| Q9 | `LIMIT` (no order) | 10 | 1 | 1 | **1.0** | 1.6 |
| Q11 | `ORDER BY` + `LIMIT` | 10 | 49 | 24 | 24 | 24 |
| Q12 | literal `IN (…)` | 5 | 3 | 4 | 5.7 | **2.4** |

*(Q10 is `EXPLAIN`, not timed.)*

**psycopg2 vs ADBC — as predicted, it depends on how much Postgres data moves:**
- **Big win where lots of rows are pulled** — Q1 −10%, Q6 −8%, Q8 −17%, Q7 −24%.
  These drag the full `files`/`categories` tables across; binary COPY pays off.
- **~2–2.4× on small single-table fetches** (Q2/Q3/Q12) — but only a few ms absolute.
- **Negligible or a hair slower where the result is tiny** — Q4/Q5 push the whole
  query to Postgres and get back 5 rows, so the transport is irrelevant and
  ADBC's slightly higher per-query setup makes it a touch slower (Q4 125 vs 112).
  Q9 (10 rows) likewise.

So `driver: adbc` is the right default for analytics that move real data; for
trivially-small results it's a wash. (Both configs are kept —
`fedq-poc-config.yaml` = psycopg2, `fedq-poc-config-adbc.yaml` = ADBC.)

Against DuckDB: fedq still loses on the big local joins (**Q1, Q6, Q8** — separate
DuckDB queries with Arrow between operators vs DuckDB's single C++ pipeline) and
wins where it pushes work down (**Q0** `COUNT(*)`, **Q7** selective `IN`, **Q5**
aggregate). fedq's edge is *data moved*, not local CPU.

> **Caveat — this table is apples-to-oranges in DuckDB's favour.** `access_logs`
> lives *inside* the DuckDB doing the join, so DuckDB pays nothing to "fetch" the
> 10M-row fact table; fedq moves it out of one DuckDB into another. The
> ClickHouse section below removes that home-field advantage.

---

## The fair comparison (fact table in ClickHouse) — summary

Full per-query breakdown in
[postgresql-clickhouse-sources-compare.md](postgresql-clickhouse-sources-compare.md).
With the fact table external (both engines pull it over the wire), **fedq wins 3
of 4** — the opposite of the local-DuckDB table above:

| Q | shape | fedq | DuckDB | why |
|---|---|--:|--:|---|
| Q7 | selective (10 keys) | **22 ms** | 55 ms | fedq reads CH in 5 ms (native Arrow) + pushes 10-key `IN` to PG; DuckDB's httpfs/Parquet read is 50 ms and it pulls all 400k files |
| Q1 | week (308k keys) | **444 ms** | 606 ms | DuckDB's CH read alone is 429 ms (httpfs+Parquet of 776k rows) |
| Q6 | non-selective (394k keys) | 1116 ms | **732 ms** | heavy join; DuckDB's single C++ pipeline beats fedq's operator-by-operator merge |
| Q8 | 10M scanned | **616 ms** | 723 ms | DuckDB's CH read is 709 ms |

**Key finding:** DuckDB has *no production-ready ClickHouse connector*. The
httpfs/`FORMAT Parquet` hatch works but is fragile (Range mismatch truncates big
results), and the `chsql_native` community extension lags 2 DuckDB releases and
**silently corrupts data** (Int64→0). fedq connects natively via the official
`clickhouse-connect` client — a ~150-line Python connector, correct and current.
The "DuckDB is 4× faster" impression comes only from the local-DuckDB table,
where DuckDB doesn't pay to fetch the fact table at all. **fedq's edge is
connector breadth — cheap in Python, expensive/fragile as a C++ extension.**

## Correctness — verified identical

fedq vs DuckDB, full (un-limited) aggregations, `diff`ed row-for-row
(`./verify-correctness.sh`):

| query | rows compared | result |
|---|--:|---|
| Q1 headline | 305,524 | identical |
| Q4 same-source | 42,858 | identical |
| Q7 selective | 10 | identical |

Q1 integrity: `sum(accesses) = 776,684` on both = the week's total `access_logs`
count. No row differs.

---

## Capability scorecard

| pushdown / behavior | fedq | DuckDB stable | DuckDB main |
|---|:--:|:--:|:--:|
| filter + projection (Q2, Q3) | | | |
| `LIMIT` (Q9) | | | |
| literal `IN (…)` (Q12) | | | |
| `ORDER BY` + `LIMIT` (Q11) | | | |
| remote aggregate / `count(*)` (Q0, Q5) | | | |
| same-source join pushdown (Q4) | | | |
| dynamic cross-source semi-join `IN` (Q7) | | | |
| cost-based: skip dynamic filter when non-selective (Q1) | | n/a | n/a |
| transport: binary COPY · ctid parallelism · shared snapshot | | | |
| raw local-CPU speed (embedded C++ engine) | — | | |

> DuckDB main already unwraps `OptionalFilter`/`SelectivityOptionalFilter` in its
> scanner, so join-derived dynamic pushdown is *plausibly* coming; today it drops
> the runtime `DynamicFilter` and the scan advertises no dynamic-filter support.
> See [`duckdb-pushdowns.md`](duckdb-pushdowns.md#dynamic--runtime-filter-pushdown-in-lists--current-status).

---

## Perf fix applied this session (Q1: 806 → 574 ms)

~⅓ of Q1 was a **Python row-loop in the G9 probe reduction**, not the join.
`_execute_inner_merge` extracted every build-side key via per-cell
`array[i].as_py()` to build the `IN`-filter — *before* the size check — so for a
400k-key build (> the 2,000 cap) it boxed 400k values then discarded the filter.

Fixed in `federated_query/plan/physical.py`: (1) distinct keys computed vectorized
(`concat_tables → drop_null → group_by → aggregate([])`); (2) cap enforced on the
vectorized count, so over-cap builds are skipped before any Python boxing. The
row-at-a-time path keeps its own reduction (its hash table is already built).
**Result:** Q1 806 → 574 ms, selective pushdown unchanged, full suite identical to
baseline (zero regressions), results still byte-identical to DuckDB.

## Transport: COPY vs direct SQL (PostgreSQL fetch driver)

DuckDB reads PostgreSQL with binary `COPY (SELECT …) TO STDOUT (FORMAT binary)`.
fedq has two drivers; **which one matters a lot:**

- **psycopg2** (the POC default, used for the timings above): runs a plain
  `SELECT`, gets Python row tuples, and converts them to Arrow chunk by chunk —
  per-cell Python work.
- **ADBC** (`driver: adbc`): already issues the **same binary `COPY`** DuckDB
  does, streaming Postgres binary straight into Arrow buffers in C.

So "use COPY instead of direct SQL" isn't something to hand-roll — it's already
behind `driver: adbc`, and it's **~2–3× faster** than the psycopg2 path
(`./bench-drivers.py`):

| fetch | psycopg2 `SELECT` | ADBC binary `COPY` | speedup |
|---|--:|--:|--:|
| files filtered (40k) | 61 ms | 27 ms | 2.2× |
| files full (400k) | 160 ms | 78 ms | 2.1× |
| categories (50k) | 66 ms | 21 ms | 3.1× |

**Implication:** the PostgreSQL fetch times in the tables above are the
*psycopg2* path; switching the config to `driver: adbc` roughly halves them. The
ADBC path also got the streaming fix below, so it no longer drains the whole
result into Python first.

## Streaming fix — ADBC must stream, not drain (`tests/test_postgres_streaming.py`)

The ADBC path used to call `cursor.fetch_arrow_table()`, which **buffers every
row into a Python Arrow table before DuckDB sees anything** — for a 10M-row scan
that's ~2 s of blocking and the whole result held in our process. It now hands
DuckDB a lazy `RecordBatchReader` (`fetch_record_batch`) with a 4 MB batch-size
cap, so DuckDB drives the pull (and can spill / stop early) while we hold ~one
batch. Pinned by `test_postgres_streaming.py` (first batch must arrive without
draining the result). Doesn't change wall-clock on build-heavy queries (the build
side is read fully regardless) — the win is memory and large/probe-side scans.

## Reproduce

Everything is wired into the **Makefile** (`make help` for the list):

```bash
cd investigate-duckdb/
make pg-data duck-data     # build the PostgreSQL + DuckDB fixtures
make binaries              # download stable CLI + build main from source
make bench                 # the full timing table (stable / main / fedq)
make bench-drivers         # psycopg2 SELECT vs ADBC binary COPY
make verify                # fedq-vs-DuckDB correctness
make perfdoc               # regenerate postgresql-duckdb-sources-compare.md
make ch-start ch-data      # bring up ClickHouse + load the fact table
make bench-clickhouse      # the fair CH+PG benchmark
make chdoc                 # regenerate postgresql-clickhouse-sources-compare.md
make test-streaming        # the streaming-contract test
```
