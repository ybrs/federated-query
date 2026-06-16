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

Median of 5 warm runs, milliseconds (localhost, single connection).

| Q | what it tests | result rows | stable | main | **fedq** |
|---|---|--:|--:|--:|--:|
| Q0 | `count(*)` on remote | 1 | 30 | 24 | **10** |
| Q1 | headline: 3-table cross-source `GROUP BY` | 305,524 | 126 | 147 | **574** |
| Q2 | filter + projection (1 table) | 3 | 2 | 3 | **5** |
| Q3 | projection + filter `id<100` | 99 | 2 | 2 | **5** |
| Q4 | same-source join + aggregate | 5 | 54 | 61 | **119** |
| Q5 | remote aggregate `GROUP BY` | 5 | 43 | 48 | **106** |
| Q6 | cross-source, non-selective | 5 | 222 | 262 | **1103** |
| Q7 | cross-source, selective (10 keys) | 5 | 43 | 42 | **20** |
| Q8 | comparison filters inside cross-source join | 5 | 60 | 61 | **549** |
| Q9 | `LIMIT` (no order) | 10 | 1 | 1 | **1** |
| Q11 | `ORDER BY` + `LIMIT` | 10 | 43 | 22 | **23** |
| Q12 | literal `IN (…)` | 5 | 3 | 3 | **5** |

*(Q10 is `EXPLAIN`, not timed. Reproduce: `./bench-all.sh`.)*

Where fedq wins: **Q0** (pushes `COUNT(*)`, DuckDB pulls 400k nulls), **Q7**
(selective — pushes 10-key `IN`, DuckDB pulls 400k). Where fedq loses: large local
joins (**Q1, Q6, Q8**) — fedq runs each operator as a separate DuckDB query with
Arrow between them; DuckDB is one fused C++ pipeline. Small queries (**Q2/Q3/Q9/Q12**)
are a wash (~ms either way). fedq's edge is *data moved*, not local CPU.

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

## Reproduce

```bash
cd investigate-duckdb/
./gen_perf_doc.py          # regenerate the whole per-query breakdown section
./bench-all.sh             # the summary timing table
./verify-correctness.sh    # the correctness table
```
