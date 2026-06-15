# How DuckDB pushes down to PostgreSQL — a POC for fedq phase 8

**Date:** 2026-06-15
**Why:** Phase 8 is the physical-plan / pushdown phase. Before we finalize how
fedq splits a federated query and what it pushes to each source, we wanted to
see exactly what a mature engine — DuckDB's `postgres_scanner` — actually sends
over the wire: which filters, projections, joins and aggregates it pushes, and
how it splits a scan. This is a black-box study: we ran real queries and read
PostgreSQL's `log_statement=all` log to see the literal SQL DuckDB emitted.

Everything here is reproducible from this folder — see
[Reproducing](#reproducing) at the bottom.

> Later sections answer the follow-ups: **[Dynamic / runtime filter pushdown
> (IN-lists)](#dynamic--runtime-filter-pushdown-in-lists--current-status)** — the
> status of DuckDB's recent runtime-filter work (and why it's likely to land
> soon) — and **[fedq on the same queries](#fedq-on-the-same-queries--head-to-head)**
> — a direct head-to-head of what each engine pushes, with **[timing
> benchmarks](#timing-warm-localhost-single-connection)** and a **[correctness
> check](#correctness--results-verified-identical)** (all results verified identical).

## TL;DR for fedq

DuckDB's postgres scanner is a **scan-level** pushdown engine, not a
sub-plan pushdown engine. It pushes down, per remote table:

| Pushdown | Pushed? | Notes |
|---|---|---|
| **Projection** (column pruning) | ✅ yes | only referenced columns; `count(*)` → `SELECT NULL` |
| **Filter** — constant comparisons (`=,<,>,<=,>=`, `col = true`) | ✅ yes | pushed even when the table is inside a join |
| **Filter** — bare boolean column (`WHERE is_active`) | ❌ no | must be written `is_active = true` to push |
| **LIMIT** (no ORDER BY) | ✅ yes | both stable and main |
| **ORDER BY (+ LIMIT)** | ⬆️ **main only** | stable pulls all + sorts locally; main pushes `ORDER BY … LIMIT` (`pg_order_pushdown`, default on) |
| **Aggregate** (`GROUP BY`, `count`, `sum`) | ❌ no | rows pulled, aggregated locally in DuckDB (optimizer exists in main's connector lib but **not wired** to PG) |
| **Join** (even same-source PG↔PG) | ❌ no | both tables pulled in full, joined in DuckDB |
| **Cross-source dynamic filter / semi-join / IN-list** | ❌ no | a selective local filter is **never** turned into a remote `IN (...)`; the full dimension table is pulled every time |

**The single most important result for us:** for the headline query *"most
accessed categories daily"* — a 10M-row local fact table joined to a 400k-row
remote `files` and a 50k-row remote `categories`, filtered to **one week** —
DuckDB still pulled **all 400,000 file rows and all 50,000 category rows**. It
does **no** sideways-information-passing to the remote. fedq's **G9 cross-source
dynamic filtering** is therefore a genuine differentiator: it is something even
DuckDB does not do.

The transport itself is good engineering and worth copying: **binary `COPY`**,
**ctid-range parallelism**, and a **shared snapshot** across the parallel readers.

---

## Setup

Two real sources, sizes per the brief:

**PostgreSQL** (`duckpoc` db, PG 17, the same bundled instance the fedq harness
uses, already running with `log_statement=all`):
- `categories` — 50,000 rows: `id, name, parent_id, is_active, created_at`
- `files` — 400,000 rows: `id, category_id→categories.id, filename, size_bytes, owner_id, created_at`

**DuckDB** (`analytics-poc.duckdb`):
- `access_logs` — 10,000,000 rows: `id, file_id→files.id (skewed), accessed_at, action, bytes_sent, user_id, day`
- 90 days of events (2026-01-01 .. 2026-04-01), `file_id` skewed so some files are hot.

DuckDB attaches PostgreSQL with the core extension:

```sql
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres'
  AS pg (TYPE postgres, READ_ONLY);
-- pg.files, pg.categories now queryable; access_logs is local.
```

Versions tested: **stable v1.5.3** and **main** (duckdb-postgres f77b0cb on
duckdb-core be587a70, built from source) — see [Stable vs main](#stable-vs-main).

> One filter caveat surfaces only at the literal level and is documented per
> query below: stable renders pushed constants as quoted strings (`= '42'`),
> main as native-typed literals (`= 42`). It does not change *which* predicates
> push, only how they are written.

---

## How DuckDB reads a PostgreSQL table

Every remote read is a **binary `COPY ... TO STDOUT`**, not a `SELECT` over the
wire and not row-by-row libpq fetches. For `SELECT count(*) FROM pg.files` the
log shows (catalog/handshake noise removed):

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT pg_is_in_recovery(), pg_export_snapshot(), ...;
COPY (SELECT NULL FROM "public"."files"
      WHERE ctid BETWEEN '(0,0)'::tid    AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
SET TRANSACTION SNAPSHOT '0000004F-000004B0-1';
COPY (SELECT NULL FROM "public"."files"
      WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT NULL FROM "public"."files"
      WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT NULL FROM "public"."files"
      WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```

Three mechanisms worth lifting:

1. **Binary `COPY` transport.** Lower per-row overhead than `SELECT`, and the
   result is a stream DuckDB decodes straight into vectors.
2. **`ctid`-range parallelism.** A big table is split into fixed **~1000-page
   chunks** by physical tuple id (`ctid`). Each chunk is a separate `COPY` run by
   a different thread. Small tables stay a single `COPY` — `categories` (50k)
   was never split; `files` (400k ≈ 3-4k pages) split into 4. This needs no
   indexes or numeric key — it works on any heap table.
3. **Shared snapshot.** The first reader calls `pg_export_snapshot()`; every
   parallel reader does `SET TRANSACTION SNAPSHOT` so all chunks see one
   consistent MVCC snapshot. (fedq's parallel-fetch path should do the same or
   accept cross-chunk skew.)

`count(*)` is the cleanest projection-pushdown demo: DuckDB needs **no columns**,
so it pushes `SELECT NULL` and counts the streamed rows locally. **The aggregate
is not pushed** — it pulls 400k empty rows and counts them in DuckDB.

---

## Pushdown, query by query

The literal SQL DuckDB emitted (ctid ranges collapsed to `<page>`):

### Filter + projection — both push (single table)
`SELECT id, category_id, size_bytes FROM pg.files WHERE category_id = 42 AND size_bytes > 5000000`
```sql
COPY (SELECT "category_id","size_bytes","id" FROM "public"."files"
      WHERE ctid BETWEEN <page>
        AND "category_id" = '42' AND "size_bytes" > '5000000') TO STDOUT (FORMAT "binary");
```
Both predicates land in the remote `WHERE`; only the 3 referenced columns are read.

### Aggregate — NOT pushed
`SELECT category_id, count(*), sum(size_bytes) FROM pg.files GROUP BY category_id ...`
```sql
COPY (SELECT "category_id","size_bytes" FROM "public"."files" WHERE ctid BETWEEN <page>) TO STDOUT (FORMAT "binary");
```
No `GROUP BY` / `count` / `sum` on the remote. All 400k rows of the two needed
columns are streamed and aggregated in DuckDB. Projection pushdown still trims
to the 2 columns.

### Same-source join — NOT pushed
`SELECT c.name, count(*) FROM pg.files f JOIN pg.categories c ON c.id=f.category_id WHERE c.is_active GROUP BY c.name`
```sql
COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN <page>) TO STDOUT (FORMAT "binary");
COPY (SELECT "id","is_active","name" FROM "public"."categories" WHERE ctid BETWEEN <page>) TO STDOUT (FORMAT "binary");
```
Even though both tables live in the *same* PostgreSQL, DuckDB does **not** send a
single joined query. It pulls each table independently and joins locally. Note
`is_active` is **pulled as a column**, not pushed as a filter — see the boolean
caveat below.

### Boolean filter caveat
`WHERE c.is_active` (bare column) → **not pushed** (the example above pulls the
column). `WHERE c.is_active = true` → **pushed**:
```sql
COPY (SELECT "id","is_active","name" FROM "public"."categories"
      WHERE ctid BETWEEN <page> AND "is_active" = 'true') TO STDOUT (FORMAT "binary");
```
Only comparison-shaped predicates are recognized as pushable table filters.
**Relevance to fedq:** when we generate remote SQL we should canonicalize bare
boolean refs to explicit `= true/false`, or we leave the same pushdown on the
table.

### Comparison filters push *even inside a cross-source join*
Headline-style 3-table join with `WHERE f.size_bytes > 9000000 AND c.is_active = true`:
```sql
COPY (SELECT "id","category_id","size_bytes" FROM "public"."files"
      WHERE ctid BETWEEN <page> AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "id","is_active","name" FROM "public"."categories"
      WHERE ctid BETWEEN <page> AND "is_active" = 'true') TO STDOUT (FORMAT "binary");
```
So filter pushdown is **per-base-table and join-independent**: a constant
predicate on a remote column is pushed regardless of joins above it. What is
*not* pushed is anything that depends on the *other* side of a join.

### LIMIT — pushed
`SELECT id, filename FROM pg.files LIMIT 10`
```sql
COPY (SELECT "id","filename" FROM "public"."files"  LIMIT 10) TO STDOUT (FORMAT "binary");
```
LIMIT goes to the remote, and the ctid parallel-split is disabled (single COPY).

---

## The headline query: "most accessed categories daily"

```sql
WITH daily AS (
  SELECT a.day, c.name AS category, count(*) AS accesses
  FROM access_logs a
  JOIN pg.files f      ON f.id = a.file_id
  JOIN pg.categories c ON c.id = f.category_id
  WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07'   -- one week
  GROUP BY a.day, c.name
)
SELECT day, category, accesses,
       rank() OVER (PARTITION BY day ORDER BY accesses DESC) AS rnk
FROM daily QUALIFY rnk <= 3 ORDER BY day, rnk;
```

What DuckDB sent to PostgreSQL — **the entire dimension tables**:
```sql
COPY (SELECT "id","category_id" FROM "public"."files"      WHERE ctid BETWEEN <page>) TO STDOUT (FORMAT "binary");  -- ×4, all 400k rows
COPY (SELECT "id","name"        FROM "public"."categories" WHERE ctid BETWEEN <page>) TO STDOUT (FORMAT "binary");  -- all 50k rows
```

The `WHERE a.day BETWEEN ...` filter prunes the **local** `access_logs` scan, and
after that filter only a small set of distinct `file_id`s survive — but **none of
that is communicated to PostgreSQL.** DuckDB pulls every file and every category,
builds hash tables, and joins locally. The plan (`EXPLAIN`, abbreviated) makes
the boundary explicit — `POSTGRES_SCAN` is always a leaf, the joins sit above it
in DuckDB:

```
HASH_GROUP_BY
└ HASH_JOIN  (file_id = id)
  ├ SEQ_SCAN  analytics-poc.access_logs        (~10,000,000 rows, Projections: file_id)
  └ HASH_JOIN  (category_id = id)
    ├ POSTGRES_SCAN  files       Projections: id, category_id   Filters: size_bytes>9000000
    └ POSTGRES_SCAN  categories  Projections: id, name
```

The `Filters:` / `Projections:` lines on `POSTGRES_SCAN` are precisely the two
pushdowns DuckDB does; there is no `Aggregate:` or `SemiJoinFilter:` line.

This is the gap fedq's **G9 dynamic cross-source filtering** targets: collect the
surviving `file_id`s (or a min/max / Bloom summary) from the filtered fact scan
and push `WHERE id IN (...)` / `WHERE id BETWEEN lo AND hi` into the `files` COPY,
then the surviving `category_id`s into the `categories` COPY. On this query that
turns a 400k-row + 50k-row transfer into a few-thousand-row transfer.

---

## Implications for fedq phase 8

1. **We already beat DuckDB on the thing that matters most here.** G9 dynamic
   filtering / semi-join pushdown is not in DuckDB's scanner. Keep it; it is the
   headline advantage on selective fact→dimension federated joins. This POC is a
   clean before/after benchmark for it.
2. **Adopt the transport tricks.** Binary `COPY (SELECT … WHERE ctid BETWEEN …)`
   with `pg_export_snapshot()` + `SET TRANSACTION SNAPSHOT` is a better remote
   scan than row `SELECT`s, and the ctid split parallelizes any heap table with
   no key or index. Worth comparing against our current ADBC fetch path (P2).
3. **Match its scan-level pushdowns at least.** Projection pruning, constant
   comparison filters (pushed independent of joins above), and LIMIT. Two easy
   wins it shows: prune to referenced columns aggressively (`count(*)`→no
   columns), and canonicalize bare boolean predicates to `= true/false` so they
   push.
4. **Aggregate and join pushdown are open ground.** DuckDB punts both to its
   local vectorized engine — which is reasonable *because that engine is
   DuckDB*. fedq's Physical Merge Engine (phase 7) gives us the same local
   vectorized fallback, so for an **all-remote-same-source** subtree we have a
   choice DuckDB doesn't take: push the whole join+aggregate as one SQL
   statement to that PostgreSQL (one round trip, PG does the work) instead of
   pulling both tables. That is a real win for the same-source case and we should
   decide it in the cost model, not hardcode "always pull".
5. **Snapshot consistency.** If we parallel-fetch a single source across chunks,
   export/serialize a snapshot like DuckDB does, or document that fedq chunks may
   see independent snapshots.

---

## Stable vs main

Two builds were compared on the identical harness and queries:

- **stable** — DuckDB **v1.5.3** CLI, postgres scanner as shipped.
- **main** — DuckDB core **be587a70** (2026-06-03, the commit `duckdb-postgres`
  main pins) + `duckdb-postgres` main **f77b0cb** (2026-06-09), built from source.
  (Note: `duckdb-postgres` main does **not** compile against duckdb-core HEAD
  `96b848d` — core moved `copy_to_bind_t` to `vector<Identifier>` and changed
  `KeyValueSecret::TryGetValue`; the extension lags core by a few days. Building
  the extension repo with its own pinned core submodule is the correct way to get
  a working "main", and is what these results use. One unrelated patch was needed:
  `postgres_oauth.cpp` uses the libpq-18 OAuth API, absent from the PG-17 libpq
  here, so it was stubbed — it has no bearing on pushdown.)

**Of the 11 probes, 9 produced byte-identical remote SQL. Two differ:**

### 1. ORDER BY pushdown — NEW in main (the meaningful change)
`SELECT id, size_bytes FROM pg.files ORDER BY size_bytes DESC LIMIT 10`

| | remote SQL |
|---|---|
| **stable** | `COPY (SELECT "id","size_bytes" FROM "public"."files" WHERE ctid BETWEEN <page>) …` — pulls **all 400k rows** in parallel, sorts + limits **locally** |
| **main** | `COPY (SELECT "id","size_bytes" FROM "public"."files" ORDER BY "size_bytes" DESC NULLS LAST LIMIT 10) …` — **PostgreSQL** sorts and limits; 10 rows come back |

main gates this with the new `pg_order_pushdown` option (**default `true`**). When
the ORDER BY/LIMIT is pushed, main also forces the scan to a **single task**
(`max_threads = 1`, no ctid split) since a global order can't be parallel-chunked.
Plain `LIMIT` without ORDER BY already pushed in stable (q9) and is unchanged.

### 2. Better-typed filter literals
Same predicates, different literal rendering:

| | filter SQL |
|---|---|
| **stable** | `… "category_id" = '42' AND "size_bytes" > '5000000'` … `"is_active" = 'true'` |
| **main** | `… "category_id" = 42 AND "size_bytes" > 5000000` … `"is_active" = true` |

stable sends every constant as a **quoted string** (PostgreSQL then implicitly
casts text→int/bool); main sends **native-typed literals**. The main form is
strictly better: a text literal compared to an indexed integer column can defeat
index usage / add per-row casts in PostgreSQL. This comes from the rewritten
`filter_pushdown.cpp` in main's shared `database-connector`.

### What is the same (architecturally unchanged)
Binary `COPY`, ctid-range parallelism + exported snapshot, projection pushdown,
filter-pushdown *semantics* (which predicates qualify — including the bare-boolean
gap), LIMIT pushdown, and — importantly — **still no join pushdown, no aggregate
pushdown, and no cross-source dynamic filter** (q1/q4/q5/q6/q7 identical on both).

### Groundwork visible in main (not yet active for PostgreSQL)
main's `duckdb-postgres` vendors a shared **`database-connector`** library (shared
with the mysql/sqlite/odbc scanners) that already contains an
**`AggregateOptimizer`** (`optimizer/aggregate_optimizer.cpp`) and an
**`OrderByAndLimitOptimizer`**. Only the order/limit optimizer is **wired** into
the postgres extension (`storage/postgres_optimizer.cpp`); `AggregateOptimizer`
is compiled but **not referenced** by the postgres scanner — i.e. aggregate
pushdown to PostgreSQL is built but not enabled. **Watch this:** the DuckDB DB
connectors are clearly heading toward aggregate (and eventually join) pushdown.
The new pushdown knobs in main: `pg_order_pushdown`, `pg_experimental_filter_pushdown`,
`pg_use_ctid_scan`, `pg_pages_per_task` (chunk size), and the handy
`pg_debug_show_queries` (prints every remote query to stdout — a cleaner capture
mechanism than scraping the PG log).

**Net for fedq:** main narrows the gap by one item (ORDER BY pushdown) and cleans
up literal typing, but the core differentiators we identified — **dynamic
cross-source filtering, aggregate pushdown, and join pushdown** — remain unbuilt
in DuckDB's postgres path even on main. Our G9 work is still ahead of it.

---

## Dynamic / runtime filter pushdown (IN-lists) — current status

> Asked specifically because DuckDB has been adding runtime-filter machinery
> recently. **Short answer: literal `IN (...)` pushdown works (and already did in
> stable); join-derived *dynamic* filter pushdown to PostgreSQL is NOT enabled,
> even on main.** Verified by source + captured SQL + EXPLAIN.

### What *is* recent (and real)
`duckdb-postgres` main rewrote filter pushdown into the shared `database-connector`
in commit **`53e5627` "Filter pushdown enhancements" (2026-06-07, 8 days ago)**,
wired into the postgres scanner by `6738a19` "Use filter pushdowns from dbconnector"
(2026-06-08). The new expression-based pushdown (`database-connector/src/table_scan/filter_pushdown.cpp`)
handles: comparisons, `AND`/`OR`, `IS [NOT] NULL`, **literal `IN (...)`**, struct-field
refs — with native-typed literals and `COLLATE "C"` for text. It also *recognizes*
DuckDB's runtime-filter wrapper functions:

```cpp
// filter_pushdown.cpp — BOUND_FUNCTION dispatch
if (func == OptionalFilterScalarFun::NAME)           return Transform(child_filter_expr); // unwrap & push
if (func == SelectivityOptionalFilterScalarFun::NAME) return Transform(child_filter_expr); // unwrap & push
if (func == DynamicFilterScalarFun::NAME)            return "";                            // <-- DROPPED
```

So `DynamicFilter` (the runtime min/max produced by a hash join) is **explicitly
dropped**; `OptionalFilter` / `SelectivityOptionalFilter` are unwrapped and pushed
*only if their child is a concrete expression* already materialized at scan-init.

### Literal IN-list — pushed by BOTH stable and main (not new)
`SELECT id, category_id FROM pg.files WHERE id IN (10, 250, 999, 40000, 123456)`:
```sql
-- main:    … AND "id" IN (10, 250, 999, 40000, 123456)     (native literals)
-- stable:  … AND "id" IN ('10', '250', '999', '40000', '123456')   (string literals)
```
Capability is identical; main only types the literals better.

### Join-derived dynamic IN/min-max — NOT pushed
A semi-join feeding `pg.files` from a **3-row** local set:
```sql
CREATE TEMP TABLE hot AS SELECT * FROM (VALUES (10),(250),(999)) t(fid);
SELECT id, category_id FROM pg.files WHERE id IN (SELECT fid FROM hot);
```
main still emits **four full-table COPYs** — `… WHERE ctid BETWEEN <page>` with **no
`IN` and no range** — i.e. it pulls all 400k rows and does the semi-join locally.
`EXPLAIN` confirms the `POSTGRES_SCAN` node carries **no `Filters:` line at all**:
DuckDB never attaches a runtime filter to the postgres scan.

### Why — and it's structural
The postgres table function opts into only two pushdowns:
```cpp
// postgres_scanner.cpp
projection_pushdown = true;
filter_pushdown     = true;     // static TableFilterSet only
// (no dynamic-filter / pushdown_complex_filter capability declared)
```
Because it never declares dynamic-filter support, DuckDB's optimizer doesn't
generate hash-join min/max/Bloom filters *for* the postgres scan. And even if a
`DynamicFilter` wrapper showed up in the static filter set, `TransformFilters`
**silently skips it** (it's an `__internal_tablefilter_…` function) rather than
pushing it. Architecturally this is unavoidable for the current design: the COPY
query is built **eagerly at scan-init** and split into fixed ctid page-ranges, but
a runtime min/max isn't known until the build side finishes — after the COPYs are
already issued. Real dynamic pushdown would need **deferred/lazy COPY generation**.

### Bottom line
- ✅ Literal `IN (...)` → pushed (stable + main).
- ✅ Recent rewrite *recognizes* `Optional`/`Selectivity` runtime-filter wrappers
  and can push their materialized child — groundwork, shared across the
  mysql/sqlite/odbc connectors too.
- ❌ True **join-derived dynamic filter** (sideways-information-passing IN-list /
  min-max) to PostgreSQL is **not enabled** on main; the selective cross-source
  join still transfers the whole dimension table.

The scaffolding (Optional/Selectivity/Dynamic recognition + `AggregateOptimizer`
in the shared connector) is a strong signal this is the direction of travel — but
as of today it is not wired for PostgreSQL. **fedq's G9 still does something DuckDB
does not.**

### Forward look — this is likely to land soon (watch it)
The pieces for dynamic IN/min-max pushdown are now *mostly in place*, which is why
we should expect DuckDB to close this gap in the near future and should track it:

1. The filter-pushdown path **already unwraps `OptionalFilter` and
   `SelectivityOptionalFilter`** and pushes their child expression. DuckDB's new
   "optional filter" framework (`SelectivityOptionalFilterType::{MIN_MAX, BF, PHJ,
   PRF}`) is precisely the mechanism for sideways-information-passing from a hash
   join — and unlike a raw `DynamicFilter`, an optional filter *carries a concrete
   child filter expression*. The moment a join emits its pruning info as an
   `OptionalFilter` wrapping a materialized `col IN (…)` or `col BETWEEN lo AND hi`
   (rather than a runtime-only `DynamicFilter`), **the scanner will push it with no
   further code change** — the `TransformExpression` recursion already handles it.
2. What's missing is two-fold and both are tractable: (a) the postgres table
   function must **advertise dynamic-filter support** (today it sets only
   `projection_pushdown`/`filter_pushdown`), and (b) the COPY query must be
   generated **after** the build side materializes (lazy/deferred scan) so the
   values are known. The shared `AggregateOptimizer` already compiled into the
   connector shows the team is actively extending pushdown in this exact layer.

So: **today** join-derived dynamic pushdown to PostgreSQL is off; **plausibly soon**
it arrives via the optional-filter path. For fedq this means G9 is a current
differentiator but not a permanent moat — the durable advantages are the *cost
model* deciding when to push (DuckDB will need one too) and same-source
join/aggregate subtree pushdown.

---

## fedq on the same queries — head-to-head

Ran the same workload through **fedq** itself (POC config `fedq-poc-config.yaml`
pointing at the identical `duckpoc` PG + `analytics-poc.duckdb`; captures in
`results/fedq_*.txt`). The remote SQL was read from the same PostgreSQL log.
fedq sends ordinary `SELECT`s (not binary `COPY`); the per-query `… LIMIT 0`
lines are its schema/type probes and are elided below.

### Basic filter + projection — parity
`… FROM pg.files WHERE category_id = 42 AND size_bytes > 5000000`
```sql
-- fedq:    SELECT "id","category_id","size_bytes" FROM "public"."files" AS files
--          WHERE ((category_id = 42) AND (size_bytes > 5000000))
-- duckdb:  COPY (SELECT "category_id","size_bytes","id" FROM … WHERE … category_id = 42 …) TO STDOUT
```
Both push the filter and prune columns. **Tie.**

### Same-source join + aggregate — fedq pushes the WHOLE subtree, DuckDB does not
`SELECT c.name, count(*) FROM pg.files f JOIN pg.categories c ON c.id=f.category_id WHERE c.is_active GROUP BY c.name ORDER BY n DESC LIMIT 5`

| engine | what PostgreSQL receives |
|---|---|
| **fedq** | **one query** — `SELECT c.name, COUNT(*) … FROM files f INNER JOIN categories c ON (c.id=f.category_id) WHERE (c.is_active = TRUE) GROUP BY c.name ORDER BY n DESC LIMIT 5` → PG returns **5 rows** |
| **duckdb** | two full-table scans (`category_id` from 400k files, `id,is_active,name` from 50k categories); join + filter + aggregate run **locally** |

fedq recognizes the join subtree is entirely one source and pushes
join + filter + aggregate + order + limit as a single statement
(`optimizer/single_source_pushdown.py`). **DuckDB's postgres scanner cannot push
a join or an aggregate at all** — this is the biggest gap.

### Selective cross-source join — fedq's G9 dynamic semi-join vs DuckDB's full pull
`… access_logs a JOIN pg.files f ON f.id=a.file_id WHERE a.day='2026-02-01' AND a.user_id=7 GROUP BY f.category_id …`
(local filter survives **10 distinct file_ids**)

| engine | what PostgreSQL receives | rows pulled from `files` |
|---|---|---|
| **fedq** | `SELECT "category_id","id" FROM "public"."files" AS f WHERE (f.id IN (145330, 339870, 127891, …10 ids…))` | **~10** |
| **duckdb** | 4× `COPY (SELECT "id","category_id" FROM files WHERE ctid BETWEEN <page>)` | **400,000** |

fedq scans the local fact table first, collects the surviving join keys, and
pushes them as a remote `IN (…)` — the **sideways-information-passing** DuckDB's
postgres path deliberately drops (see the dynamic-filter section above).
**40,000× less data moved on this query.**

### Non-selective cross-source join — fedq is cost-based, not naive
Same shape but filtered to a **whole week, all users** → **308,772 distinct
file_ids (77 % of the table)**. Here fedq does **not** build a 308k-element
IN-list; it pulls the full `files`/`categories` tables and joins locally — the
same thing DuckDB does, because for this selectivity it is the right plan. So
fedq's dynamic pushdown is a **cost decision** (push when selective, fall back
when not), not an unconditional rewrite.

### Scorecard

| query shape | fedq | DuckDB (stable & main) |
|---|---|---|
| filter + projection pushdown | ✅ | ✅ |
| LIMIT pushdown | ✅ | ✅ |
| ORDER BY pushdown | ✅ | ⬆️ main only |
| literal `IN (…)` pushdown | ✅ | ✅ |
| **same-source join pushdown** | ✅ one query | ❌ pulls both tables |
| **same-source aggregate pushdown** | ✅ | ❌ |
| **dynamic cross-source semi-join (`IN` of join keys)** | ✅ G9 | ❌ |
| **cost-based: skip dynamic filter when non-selective** | ✅ | n/a (never does it) |

**Takeaway:** on the basics fedq and DuckDB are even. On the two things that
dominate cost for federated analytics — *pushing a same-source join/aggregate as
one query*, and *pushing a selective join's keys to the remote* — fedq does both
and DuckDB's postgres scanner does neither (even on today's main). The transport
layer is where DuckDB is ahead (binary COPY, ctid parallelism, shared snapshot);
that's the part worth borrowing.

### Timing (warm, localhost, single connection)

Median of 5 warm runs each (ms). Same `duckpoc` PG + `analytics-poc.duckdb`, same
box. "Rows pulled from PG" is the qualitative driver behind the numbers.

| query (result size) | duckdb stable | duckdb main | **fedq** | rows pulled from PG: duckdb → fedq |
|---|--:|--:|--:|---|
| Q4 same-source join+agg (→5 rows) | 56 | 63 | **119** | ~450,000 → **5** (fedq pushes whole query) |
| Q7 selective cross-source (→5 rows) | 56 | 44 | **21** | 400,000 → **~10** (fedq G9 IN-list) |
| Q1 headline, non-selective (→305k rows) | 121 | 131 | **806** | ~450,000 → ~450,000 (both full scan) |

**Honest reading — do not over-claim fedq here:**
- **Q7 (selective): fedq wins outright** — ~2× faster *and* moves ~40,000× less
  data. When pushdown is selective the win shows up even on localhost.
- **Q4 (same-source): fedq moves ~90,000× less data yet is ~2× *slower* in
  wall-clock.** At this scale DuckDB's binary `COPY` of ~450k narrow rows is only
  tens of ms, so fedq's fixed per-query overhead (Python planning + ADBC round
  trips + the `LIMIT 0` schema probes, ~100 ms) dominates. The data-movement win
  would likely flip this **over a real network** (latency/bandwidth/egress), which
  localhost hides.
- **Q1 (non-selective): fedq ~6× slower** — both pull the same data, but fedq
  materializes 305k result rows through Python/Arrow + its merge engine, vs
  DuckDB's C++ vectorized engine.

**Net:** DuckDB's C++ engine + binary-COPY transport is faster in raw wall-clock
at localhost/small scale (2 of 3 here). fedq's structural advantage is **how much
data crosses the wire**, which dominates over a network, at larger scale, or with
egress costs — *not* local CPU. (Consistent with the engine being I/O-bound.)
Caveats: warm cache, single connection, localhost, ~0.5 GB data — a setup that
specifically *understates* the value of pushing less data.

### Correctness — results verified identical

Every query was cross-checked **fedq vs DuckDB** on the full (un-limited)
aggregation, normalized and `diff`ed row-for-row:

| query | rows compared | result |
|---|--:|---|
| Q7 selective (`GROUP BY category_id`) | 10 | ✅ identical |
| Q4 same-source (`GROUP BY name`, all active) | 42,858 | ✅ identical |
| Q1 headline (`GROUP BY day, name`, full week) | 305,524 | ✅ identical |

Integrity check on Q1: `sum(accesses) = 776,684` on both engines, which equals the
independently-counted number of `access_logs` rows in that week — every event maps
to exactly one `(day, category)` group, so the totals must (and do) match. DuckDB
stable and main also returned identical results to each other on all probes. So
the data-movement and timing differences above are **not** bought with wrong
answers: fedq and DuckDB agree on every row.

---

## Reproducing

From `investigate-duckdb/`:

```bash
# 1. PostgreSQL data (uses the bundled PG already running on :5432)
./pg.sh -d duckpoc -f schema.sql           # see commands in git / harness notes

# 2. DuckDB fact table (analytics-poc.duckdb, 10M rows) — already built

# 3. Run any probe and capture what PG received:
VERBOSE=1 ./capture.sh ./bin/duckdb-stable queries/q1_headline.sql Q1
# results/*.txt hold the saved runs; queries/*.sql are the probes.

# 4. Reproduce the benchmark table and the correctness check:
./bench.sh                 # median-of-5 warm timings: duckdb-stable / -main / fedq
./verify-correctness.sh    # diffs fedq vs duckdb results row-for-row (Q4/Q7/Q1)
```

- `capture.sh <duckdb> <sql> [label]` brackets `postgres-server.log` around the
  run and prints the statements PostgreSQL received (used for **stable**).
- `capture-main.sh <sql> [label]` does the same for the **main** build: it loads
  the locally-built `postgres_scanner.duckdb_extension` by path with `-unsigned`
  (the dev extension is unsigned and version-tagged `v0.0.1`, so plain
  `LOAD postgres` / `INSTALL` can't find it).
- `bin/duckdb-stable` = v1.5.3 CLI; `pg-scanner-src/build/release/duckdb` = main.
- `results/<q>-stable.txt` and `results/<q>-main.txt` hold every captured run.
- Probes: `q0` count, `q1` headline, `q2` filter, `q3` projection, `q4` PG-PG
  join, `q5` aggregate, `q6/q7` cross-source (full / selective), `q8` filters in
  join, `q9` LIMIT, `q10` EXPLAIN, `q11` ORDER BY + LIMIT (main-only pushdown),
  `q12` literal IN-list, `q13` IN via local subquery (dynamic-filter probe).
- **fedq** side: `capture-fedq.sh <sql> [label]` runs a query through the fedq
  REPL using `fedq-poc-config.yaml` (same `duckpoc` PG + `analytics-poc.duckdb`)
  and prints the SQL PostgreSQL received. fedq queries use 3-part names
  (`pg.public.files`, `analytics.main.access_logs`); see `queries/fedq_*.sql`
  and `results/fedq_*.txt`.

### Building the `main` binary (notes)
- `bin/duckdb-stable`: downloaded prebuilt v1.5.3 CLI.
- `pg-scanner-src/`: `git clone --recurse-submodules https://github.com/duckdb/duckdb-postgres`,
  then `GEN=ninja make release -j` (builds the pinned duckdb core + the scanner).
  Needs `cmake`/`ninja` (here: `pip install cmake ninja` into the venv).
- The only source change for this environment is the `postgres_oauth.cpp` stub
  (libpq-18 OAuth API absent on PG-17); irrelevant to pushdown behavior.
