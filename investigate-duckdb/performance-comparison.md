# fedq vs DuckDB — federated query comparison

**Date:** 2026-06-15 · What **fedq** and **DuckDB's `postgres_scanner`** push to
PostgreSQL on the same queries + data, with full timings and a correctness check.
Deep mechanics: [`duckdb-pushdowns.md`](duckdb-pushdowns.md).

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

---

## Per-query breakdown — incoming query, what each engine sends, and timings

For every query: a one-line description, the **incoming SQL**, **what fedq sends
to each source** (DuckDB `analytics` + PostgreSQL `pg`) with a per-source timing
split, **what DuckDB sends to PostgreSQL** plus its internal plan, and the
head-to-head total. All SQL is verbatim from the PG log / fedq EXPLAIN; timings
are warm (fedq = one measured run with `FEDQ_PROFILE`; DuckDB = median of 5).
Regenerate with `./gen_perf_doc.py`.

### Q0 — Count every file in PostgreSQL.

**Incoming query**
```sql
SELECT count(*) FROM files;
```

**fedq — what it sends to each source:**

- → PostgreSQL · `pg`
  ```sql
  SELECT COUNT(*) AS "COUNT(*)" FROM "public"."files" AS files
  ```

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **9 ms** · local combine **1 ms** · **total 10 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT NULL FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT NULL FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT NULL FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT NULL FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `UNGROUPED_AGGREGATE <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 23 ms**

**Head-to-head — fedq 10 ms · DuckDB 23 ms**

---

### Q1 — Daily access counts per category for one week (10M-row fact joined to both PG dimensions).

**Incoming query**
```sql
SELECT a.day, c.name, count(*) AS n
FROM access_logs a
JOIN files f ON f.id = a.file_id
JOIN categories c ON c.id = f.category_id
WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07'
GROUP BY a.day, c.name;
```

**fedq — what it sends to each source:**

- → DuckDB · `analytics`
  ```sql
  SELECT "day", "file_id" FROM "main"."access_logs" AS a WHERE (a.day BETWEEN CAST('2026-02-01' AS DATE) AND CAST('2026-02-07' AS DATE))
  ```
- → PostgreSQL · `pg`
  ```sql
  SELECT "id", "category_id" FROM "public"."files" AS f
  ```
- → PostgreSQL · `pg`
  ```sql
  SELECT "name", "id" FROM "public"."categories" AS c
  ```

**fedq timing:** DuckDB fetch **60 ms** · PostgreSQL fetch **197 ms** · local combine **291 ms** · **total 548 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "name" FROM "public"."categories" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- SEQ_SCAN <- HASH_JOIN <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 118 ms**

**Head-to-head — fedq 548 ms · DuckDB 118 ms**

---

### Q2 — Find big files in one category (two-predicate filter on a single table).

**Incoming query**
```sql
SELECT id, category_id, size_bytes FROM files
WHERE category_id = 42 AND size_bytes > 5000000;
```

**fedq — what it sends to each source:**

- → PostgreSQL · `pg`
  ```sql
  SELECT "id", "category_id", "size_bytes" FROM "public"."files" AS files WHERE ((category_id = 42) AND (size_bytes > 5000000))
  ```

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **1 ms** · local combine **5 ms** · **total 6 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "category_id", "size_bytes", "id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "category_id" = '42' AND "size_bytes" > '5000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id", "size_bytes", "id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid AND "category_id" = '42' AND "size_bytes" > '5000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id", "size_bytes", "id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid AND "category_id" = '42' AND "size_bytes" > '5000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id", "size_bytes", "id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid AND "category_id" = '42' AND "size_bytes" > '5000000') TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `PROJECTION <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 3 ms**

**Head-to-head — fedq 6 ms · DuckDB 3 ms**

---

### Q3 — Read one column for the first 99 files (projection + range filter).

**Incoming query**
```sql
SELECT category_id FROM files WHERE id < 100;
```

**fedq — what it sends to each source:**

- → PostgreSQL · `pg`
  ```sql
  SELECT "category_id", "id" FROM "public"."files" AS files WHERE (id < 100)
  ```

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **1 ms** · local combine **5 ms** · **total 5 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "id" < '100') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid AND "id" < '100') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid AND "id" < '100') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid AND "id" < '100') TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `PROJECTION <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 2 ms**

**Head-to-head — fedq 5 ms · DuckDB 2 ms**

---

### Q4 — Top-5 active categories by file count — both tables in PostgreSQL.

**Incoming query**
```sql
SELECT c.name, count(*) AS n
FROM files f JOIN categories c ON c.id = f.category_id
WHERE c.is_active = true
GROUP BY c.name ORDER BY n DESC LIMIT 5;
```

**fedq — what it sends to each source:**

- → PostgreSQL · `pg`
  ```sql
  SELECT c.name AS "name", COUNT(*) AS "n" FROM "public"."files" AS f INNER JOIN "public"."categories" AS c ON (c.id = f.category_id) WHERE (c.is_active = TRUE) GROUP BY c.name ORDER BY n DESC LIMIT 5
  ```

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **0 ms** · local combine **119 ms** · **total 119 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "is_active", "name" FROM "public"."categories" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid AND "is_active" = 'true') TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 52 ms**

**Head-to-head — fedq 119 ms · DuckDB 52 ms**

---

### Q5 — Top-5 categories by file count + total size — single-table aggregate.

**Incoming query**
```sql
SELECT category_id, count(*) AS n, sum(size_bytes) AS t
FROM files GROUP BY category_id ORDER BY n DESC LIMIT 5;
```

**fedq — what it sends to each source:**

- → PostgreSQL · `pg`
  ```sql
  SELECT category_id AS "category_id", COUNT(*) AS "n", SUM(size_bytes) AS "t" FROM "public"."files" AS files GROUP BY category_id ORDER BY n DESC NULLS FIRST LIMIT 5
  ```

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **127 ms** · local combine **2 ms** · **total 129 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 46 ms**

**Head-to-head — fedq 129 ms · DuckDB 46 ms**

---

### Q6 — Top-5 categories by download events — cross-source, but ~all files match.

**Incoming query**
```sql
SELECT c.name, count(*) AS n
FROM access_logs a JOIN files f ON f.id = a.file_id JOIN categories c ON c.id = f.category_id
WHERE a.action = 'download'
GROUP BY c.name ORDER BY n DESC LIMIT 5;
```

**fedq — what it sends to each source:**

- → DuckDB · `analytics`
  ```sql
  SELECT "file_id", "action" FROM "main"."access_logs" AS a WHERE (a.action = 'download')
  ```
- → PostgreSQL · `pg`
  ```sql
  SELECT "id", "category_id" FROM "public"."files" AS f
  ```
- → PostgreSQL · `pg`
  ```sql
  SELECT "name", "id" FROM "public"."categories" AS c
  ```

**fedq timing:** DuckDB fetch **212 ms** · PostgreSQL fetch **193 ms** · local combine **727 ms** · **total 1132 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "name" FROM "public"."categories" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- SEQ_SCAN <- HASH_JOIN <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 239 ms**

**Head-to-head — fedq 1132 ms · DuckDB 239 ms**

---

### Q7 — Top categories accessed by one user on one day — highly selective cross-source join.

**Incoming query**
```sql
SELECT f.category_id, count(*) AS n
FROM access_logs a JOIN files f ON f.id = a.file_id
WHERE a.day = DATE '2026-02-01' AND a.user_id = 7
GROUP BY f.category_id ORDER BY n DESC LIMIT 5;
```

**fedq — what it sends to each source:**

- → DuckDB · `analytics`
  ```sql
  SELECT "file_id", "day", "user_id" FROM "main"."access_logs" AS a WHERE ((a.day = CAST('2026-02-01' AS DATE)) AND (a.user_id = 7))
  ```
- → PostgreSQL · `pg`
  ```sql
  SELECT "category_id", "id" FROM "public"."files" AS f WHERE (f.id IN (145330, 339870, 127891, 150143, 21692, 68308, 37793, 353438, 318784, 338877))
  ```

**fedq timing:** DuckDB fetch **5 ms** · PostgreSQL fetch **1 ms** · local combine **19 ms** · **total 24 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- POSTGRES_SCAN <- SEQ_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 60 ms**

**Head-to-head — fedq 24 ms · DuckDB 60 ms**

---

### Q8 — Top categories for large downloaded files — cross-source with remote-column filters.

**Incoming query**
```sql
SELECT c.name, count(*) AS n
FROM access_logs a JOIN files f ON f.id = a.file_id JOIN categories c ON c.id = f.category_id
WHERE f.size_bytes > 9000000 AND c.is_active = true
GROUP BY c.name ORDER BY n DESC LIMIT 5;
```

**fedq — what it sends to each source:**

- → DuckDB · `analytics`
  ```sql
  SELECT "file_id" FROM "main"."access_logs" AS a
  ```
- → PostgreSQL · `pg`
  ```sql
  SELECT "id", "category_id", "size_bytes" FROM "public"."files" AS f WHERE (f.size_bytes > 9000000)
  ```
- → PostgreSQL · `pg`
  ```sql
  SELECT "name", "id", "is_active" FROM "public"."categories" AS c WHERE (c.is_active = True)
  ```

**fedq timing:** DuckDB fetch **43 ms** · PostgreSQL fetch **71 ms** · local combine **351 ms** · **total 466 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "is_active", "name" FROM "public"."categories" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid AND "is_active" = 'true') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- SEQ_SCAN <- HASH_JOIN <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 62 ms**

**Head-to-head — fedq 466 ms · DuckDB 62 ms**

---

### Q9 — Grab any 10 files (LIMIT, no order).

**Incoming query**
```sql
SELECT id, filename FROM files LIMIT 10;
```

**fedq — what it sends to each source:**

- → PostgreSQL · `pg`
  ```sql
  SELECT "id", "filename" FROM "public"."files" AS files LIMIT 10
  ```

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **0 ms** · local combine **1 ms** · **total 1 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "filename" FROM "public"."files"  LIMIT 10) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 1 ms**

**Head-to-head — fedq 1 ms · DuckDB 1 ms**

---

### Q11 — The 10 largest files (ORDER BY + LIMIT).

**Incoming query**
```sql
SELECT id, size_bytes FROM files ORDER BY size_bytes DESC LIMIT 10;
```

**fedq — what it sends to each source:**

- → PostgreSQL · `pg`
  ```sql
  SELECT "id", "size_bytes" FROM "public"."files" AS files ORDER BY size_bytes DESC NULLS FIRST LIMIT 10
  ```

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **20 ms** · local combine **1 ms** · **total 21 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 46 ms**

**Head-to-head — fedq 21 ms · DuckDB 46 ms**

---

### Q12 — Fetch five files by explicit id list (literal IN).

**Incoming query**
```sql
SELECT id, category_id FROM files WHERE id IN (10, 250, 999, 40000, 123456);
```

**fedq — what it sends to each source:**

- → PostgreSQL · `pg`
  ```sql
  SELECT "id", "category_id" FROM "public"."files" AS files WHERE (id IN (10, 250, 999, 40000, 123456))
  ```

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **1 ms** · local combine **5 ms** · **total 6 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "id" IN ('10', '250', '999', '40000', '123456')) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid AND "id" IN ('10', '250', '999', '40000', '123456')) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid AND "id" IN ('10', '250', '999', '40000', '123456')) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid AND "id" IN ('10', '250', '999', '40000', '123456')) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `PROJECTION <- FILTER <- HASH_JOIN <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 2 ms**

**Head-to-head — fedq 6 ms · DuckDB 2 ms**

---

## Correctness — verified identical

fedq vs DuckDB, full (un-limited) aggregations, `diff`ed row-for-row
(`./verify-correctness.sh`):

| query | rows compared | result |
|---|--:|---|
| Q1 headline | 305,524 | ✅ identical |
| Q4 same-source | 42,858 | ✅ identical |
| Q7 selective | 10 | ✅ identical |

Q1 integrity: `sum(accesses) = 776,684` on both = the week's total `access_logs`
count. No row differs.

---

## Capability scorecard

| pushdown / behavior | fedq | DuckDB stable | DuckDB main |
|---|:--:|:--:|:--:|
| filter + projection (Q2, Q3) | ✅ | ✅ | ✅ |
| `LIMIT` (Q9) | ✅ | ✅ | ✅ |
| literal `IN (…)` (Q12) | ✅ | ✅ | ✅ |
| `ORDER BY` + `LIMIT` (Q11) | ✅ | ❌ | ✅ |
| remote aggregate / `count(*)` (Q0, Q5) | ✅ | ❌ | ❌ |
| same-source join pushdown (Q4) | ✅ | ❌ | ❌ |
| dynamic cross-source semi-join `IN` (Q7) | ✅ | ❌ | ❌ |
| cost-based: skip dynamic filter when non-selective (Q1) | ✅ | n/a | n/a |
| transport: binary COPY · ctid parallelism · shared snapshot | ❌ | ✅ | ✅ |
| raw local-CPU speed (embedded C++ engine) | — | ✅ | ✅ |

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
make perfdoc               # regenerate the per-query breakdown
make test-streaming        # the streaming-contract test
```
