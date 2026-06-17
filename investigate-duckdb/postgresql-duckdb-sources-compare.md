# PostgreSQL + DuckDB sources — fedq vs DuckDB

**DuckDB's best case.** Here the 10M-row fact table `access_logs` lives in a
**local DuckDB file**, and the `files`/`categories` dimensions live in
**PostgreSQL**. For DuckDB this is home turf: the fact table is *inside the very
engine doing the join*, so it pays **nothing** to "fetch" it — only the
PostgreSQL dimensions cross the wire. fedq, by contrast, must move `access_logs`
out of its DuckDB source into the merge engine. So DuckDB's numbers here are
flattering; the apples-to-apples version (fact table in an external store both
must reach) is
[`postgresql-clickhouse-sources-compare.md`](postgresql-clickhouse-sources-compare.md).

For each query: the incoming SQL, what fedq sends to each source (DuckDB
`analytics` + PostgreSQL `pg`) with a per-source timing split, what DuckDB sends
to PostgreSQL plus its internal plan, and the head-to-head total. SQL is verbatim
from the PG log / fedq EXPLAIN; fedq timing is one profiled run, DuckDB is median
of 5. Regenerate: `./gen_perf_doc.py`.

> Note on fedq's split: it **streams** the remote sources lazily, so much of the
> real fetch time is charged to *local combine* rather than the per-source lines
> — trust the **total**, not the split, for fedq.

---


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

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **11 ms** · local combine **1 ms** · **total 12 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT NULL FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT NULL FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT NULL FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT NULL FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `UNGROUPED_AGGREGATE <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 27 ms**

**Head-to-head — fedq 12 ms · DuckDB 27 ms**

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

**fedq timing:** DuckDB fetch **71 ms** · PostgreSQL fetch **252 ms** · local combine **336 ms** · **total 659 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "name" FROM "public"."categories" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- SEQ_SCAN <- HASH_JOIN <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 149 ms**

**Head-to-head — fedq 659 ms · DuckDB 149 ms**

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

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **1 ms** · local combine **5 ms** · **total 6 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "id" < '100') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid AND "id" < '100') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid AND "id" < '100') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid AND "id" < '100') TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `PROJECTION <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 3 ms**

**Head-to-head — fedq 6 ms · DuckDB 3 ms**

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

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **0 ms** · local combine **164 ms** · **total 164 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "is_active", "name" FROM "public"."categories" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid AND "is_active" = 'true') TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 82 ms**

**Head-to-head — fedq 164 ms · DuckDB 82 ms**

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

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **141 ms** · local combine **1 ms** · **total 142 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 65 ms**

**Head-to-head — fedq 142 ms · DuckDB 65 ms**

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

**fedq timing:** DuckDB fetch **244 ms** · PostgreSQL fetch **238 ms** · local combine **995 ms** · **total 1477 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "name" FROM "public"."categories" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- SEQ_SCAN <- HASH_JOIN <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 282 ms**

**Head-to-head — fedq 1477 ms · DuckDB 282 ms**

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

**fedq timing:** DuckDB fetch **6 ms** · PostgreSQL fetch **1 ms** · local combine **17 ms** · **total 24 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- POSTGRES_SCAN <- SEQ_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 58 ms**

**Head-to-head — fedq 24 ms · DuckDB 58 ms**

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

**fedq timing:** DuckDB fetch **59 ms** · PostgreSQL fetch **106 ms** · local combine **477 ms** · **total 642 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "is_active", "name" FROM "public"."categories" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid AND "is_active" = 'true') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- HASH_GROUP_BY <- PROJECTION <- HASH_JOIN <- SEQ_SCAN <- HASH_JOIN <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 86 ms**

**Head-to-head — fedq 642 ms · DuckDB 86 ms**

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

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **1 ms** · local combine **1 ms** · **total 2 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "filename" FROM "public"."files"  LIMIT 10) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 1 ms**

**Head-to-head — fedq 2 ms · DuckDB 1 ms**

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

**fedq timing:** DuckDB fetch **0 ms** · PostgreSQL fetch **24 ms** · local combine **2 ms** · **total 25 ms**

**DuckDB — what it sends to PostgreSQL:**
```sql
COPY (SELECT "id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(1000,0)'::tid AND '(2000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(2000,0)'::tid AND '(3000,0)'::tid) TO STDOUT (FORMAT "binary");
COPY (SELECT "id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(3000,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
```
internal DuckDB plan (root ← leaves): `TOP_N <- POSTGRES_SCAN` — everything above the scans runs in DuckDB's own C++ engine.
**DuckDB total: 58 ms**

**Head-to-head — fedq 25 ms · DuckDB 58 ms**

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
**DuckDB total: 3 ms**

**Head-to-head — fedq 6 ms · DuckDB 3 ms**

---

## Summary — all queries

| Q | what it tests | fedq | DuckDB | winner |
|---|---|--:|--:|:--:|
| Q0 | Count every file in PostgreSQL | 12 ms | 27 ms | **fedq** |
| Q1 | Daily access counts per category for one week (10M-row fact joined to both PG dimensions) | 659 ms | 149 ms | DuckDB |
| Q2 | Find big files in one category (two-predicate filter on a single table) | 6 ms | 3 ms | DuckDB |
| Q3 | Read one column for the first 99 files (projection + range filter) | 6 ms | 3 ms | DuckDB |
| Q4 | Top-5 active categories by file count — both tables in PostgreSQL | 164 ms | 82 ms | DuckDB |
| Q5 | Top-5 categories by file count + total size — single-table aggregate | 142 ms | 65 ms | DuckDB |
| Q6 | Top-5 categories by download events — cross-source, but ~all files match | 1477 ms | 282 ms | DuckDB |
| Q7 | Top categories accessed by one user on one day — highly selective cross-source join | 24 ms | 58 ms | **fedq** |
| Q8 | Top categories for large downloaded files — cross-source with remote-column filters | 642 ms | 86 ms | DuckDB |
| Q9 | Grab any 10 files (LIMIT, no order) | 2 ms | 1 ms | DuckDB |
| Q11 | The 10 largest files (ORDER BY + LIMIT) | 25 ms | 58 ms | **fedq** |
| Q12 | Fetch five files by explicit id list (literal IN) | 6 ms | 3 ms | DuckDB |

DuckDB leads the cross-source queries here **because the fact table is inside its engine** — see the ClickHouse doc for the fair version.
