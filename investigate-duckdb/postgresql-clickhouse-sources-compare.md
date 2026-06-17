# PostgreSQL + ClickHouse sources — fedq vs DuckDB

**The fair comparison.** The 10M-row fact table `access_logs` lives in an
**external ClickHouse**; the `files`/`categories` dimensions live in
**PostgreSQL**. Neither engine gets the fact table for free — both pull it over
the wire. (The companion doc
[`postgresql-duckdb-sources-compare.md`](postgresql-duckdb-sources-compare.md)
keeps `access_logs` inside a local DuckDB — DuckDB's best case, where it pays
nothing to "fetch" the fact table.)

How each engine reaches ClickHouse:
- **fedq** — a native ClickHouse connector (`type: clickhouse`,
  `clickhouse-connect` Arrow stream) + ADBC Postgres, and it pushes the **G9
  dynamic `IN`-list from ClickHouse keys into Postgres**.
- **DuckDB** — has **no production-ready ClickHouse connector**; all paths are
  inadequate:
  - **httpfs + `FORMAT Parquet` over HTTP** (used below) — correct when it
    completes, but fragile: DuckDB Range-reads the response as a static file,
    while ClickHouse's `?query=` endpoint ignores Range and returns a *different
    byte layout each request*, so large results intermittently truncate
    (`No magic bytes`). Static filter embedded in the CH query, dims via
    `postgres_scanner`.
  - **`chsql_native` community extension** (native protocol) — lags 2 DuckDB
    releases (max v1.3.2 vs 1.5.3) and **silently corrupts data**: `Int64`→0,
    `Float64`→garbage, `Date`→wrong (verified — `file_id` comes back all zeros),
    so joins would be wrong. Unusable.
  - `mysql_scanner` against CH's MySQL port — fails (CH has no transactions).

  A *correct* DuckDB↔ClickHouse connector means a maintained C++ extension; fedq's
  is ~150 lines of Python on the official `clickhouse-connect` client — correct,
  current, easy. **That connector flexibility, not per-query speed, is the point.**

### How to read the timings
- **DuckDB → ClickHouse "X ms to read"** is the real HTTP transfer time (from
  `EXPLAIN ANALYZE`'s *HTTPFS HTTP Stats*) — DuckDB reads CH and PG **in
  parallel**, so it does NOT simply add up to the total.
- **fedq** per-source times come from `FEDQ_PROFILE` scan self-times. Caveat:
  fedq **streams** ClickHouse lazily, so much of the real CH read is charged to
  *local combine*, not to the CH line — trust the **total**, not the split, for
  fedq. (Totals are median of 3 warm runs.)

Regenerate: `./gen_ch_doc.py`.

---


### Q0 — count(*) on the remote files table (PostgreSQL only).

**Incoming query**
```sql
SELECT count(*) FROM files;
```

_(PostgreSQL-only — does not touch the ClickHouse fact table.)_

**fedq**

- → PostgreSQL `pg`  ·  **total 13 ms**
  ```sql
  SELECT COUNT(*) AS "COUNT(*)" FROM "public"."files" AS files
  ```

**DuckDB**

- → PostgreSQL (postgres_scanner)  ·  **total 28 ms**
  ```sql
  COPY (SELECT NULL FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
  ```

**Head-to-head — fedq 13 ms · DuckDB 28 ms**

---

### Q1 — Daily access counts per category for one week — cross-source (→308k file keys).

**Incoming query**
```sql
SELECT a.day, c.name, count(*) AS n
FROM access_logs a JOIN files f ON f.id=a.file_id JOIN categories c ON c.id=f.category_id
WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07'
GROUP BY a.day, c.name;
```

**fedq**

- → ClickHouse `ch`  ·  **28 ms**
  ```sql
  SELECT "day", "file_id" FROM "default"."access_logs" AS a WHERE (a.day BETWEEN CAST('2026-02-01' AS DATE) AND CAST('2026-02-07' AS DATE))
  ```
- → PostgreSQL `pg`  ·  **109 ms**
  ```sql
  COPY (SELECT "id", "category_id" FROM "public"."files" AS f) TO STDOUT (FORMAT binary)
  ```
- local combine **429 ms**  →  **total 565 ms**

**DuckDB**

- → ClickHouse (httpfs `FORMAT Parquet`)  ·  **429 ms** to read
  ```sql
  SELECT file_id, day FROM access_logs WHERE day BETWEEN '2026-02-01' AND '2026-02-07' FORMAT Parquet
  ```
- → PostgreSQL (postgres_scanner)
  ```sql
  COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
  ```
- **total 619 ms** (CH and PG read in parallel, then join locally)

**Head-to-head — fedq 565 ms · DuckDB 619 ms**

---

### Q2 — Filter + projection on one PostgreSQL table.

**Incoming query**
```sql
SELECT id, category_id, size_bytes FROM files WHERE category_id=42 AND size_bytes>5000000;
```

_(PostgreSQL-only — does not touch the ClickHouse fact table.)_

**fedq**

- → PostgreSQL `pg`  ·  **total 3 ms**
  ```sql
  SELECT "id", "category_id", "size_bytes" FROM "public"."files" AS files WHERE ((category_id = 42) AND (size_bytes > 5000000))
  ```

**DuckDB**

- → PostgreSQL (postgres_scanner)  ·  **total 2 ms**
  ```sql
  COPY (SELECT "category_id", "size_bytes", "id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "category_id" = '42' AND "size_bytes" > '5000000') TO STDOUT (FORMAT "binary");
  ```

**Head-to-head — fedq 3 ms · DuckDB 2 ms**

---

### Q3 — Projection + range filter on one PostgreSQL table.

**Incoming query**
```sql
SELECT category_id FROM files WHERE id<100;
```

_(PostgreSQL-only — does not touch the ClickHouse fact table.)_

**fedq**

- → PostgreSQL `pg`  ·  **total 3 ms**
  ```sql
  SELECT "category_id", "id" FROM "public"."files" AS files WHERE (id < 100)
  ```

**DuckDB**

- → PostgreSQL (postgres_scanner)  ·  **total 2 ms**
  ```sql
  COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "id" < '100') TO STDOUT (FORMAT "binary");
  ```

**Head-to-head — fedq 3 ms · DuckDB 2 ms**

---

### Q4 — Top-5 active categories — same-source join in PostgreSQL.

**Incoming query**
```sql
SELECT c.name, count(*) AS n FROM files f JOIN categories c ON c.id=f.category_id
WHERE c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;
```

_(PostgreSQL-only — does not touch the ClickHouse fact table.)_

**fedq**

- → PostgreSQL `pg`  ·  **total 170 ms**
  ```sql
  SELECT c.name AS "name", COUNT(*) AS "n" FROM "public"."files" AS f INNER JOIN "public"."categories" AS c ON (c.id = f.category_id) WHERE (c.is_active = TRUE) GROUP BY c.name ORDER BY n DESC LIMIT 5
  ```

**DuckDB**

- → PostgreSQL (postgres_scanner)  ·  **total 64 ms**
  ```sql
  COPY (SELECT "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
  ```

**Head-to-head — fedq 170 ms · DuckDB 64 ms**

---

### Q5 — Single-table aggregate in PostgreSQL.

**Incoming query**
```sql
SELECT category_id, count(*) AS n, sum(size_bytes) AS t FROM files GROUP BY category_id ORDER BY n DESC LIMIT 5;
```

_(PostgreSQL-only — does not touch the ClickHouse fact table.)_

**fedq**

- → PostgreSQL `pg`  ·  **total 115 ms**
  ```sql
  SELECT category_id AS "category_id", COUNT(*) AS "n", SUM(size_bytes) AS "t" FROM "public"."files" AS files GROUP BY category_id ORDER BY n DESC LIMIT 5
  ```

**DuckDB**

- → PostgreSQL (postgres_scanner)  ·  **total 72 ms**
  ```sql
  COPY (SELECT "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
  ```

**Head-to-head — fedq 115 ms · DuckDB 72 ms**

---

### Q6 — Top categories by download events — cross-source, non-selective (→394k file keys).

**Incoming query**
```sql
SELECT c.name, count(*) AS n
FROM access_logs a JOIN files f ON f.id=a.file_id JOIN categories c ON c.id=f.category_id
WHERE a.action = 'download'
GROUP BY c.name ORDER BY n DESC LIMIT 5;
```

**fedq**

- → ClickHouse `ch`  ·  **178 ms**
  ```sql
  SELECT "file_id", "action" FROM "default"."access_logs" AS a WHERE (a.action = 'download')
  ```
- → PostgreSQL `pg`  ·  **95 ms**
  ```sql
  COPY (SELECT "id", "category_id" FROM "public"."files" AS f) TO STDOUT (FORMAT binary)
  ```
- local combine **939 ms**  →  **total 1211 ms**

**DuckDB**

- → ClickHouse (httpfs `FORMAT Parquet`)  ·  **1250 ms** to read
  ```sql
  SELECT file_id FROM access_logs WHERE action='download' FORMAT Parquet
  ```
- → PostgreSQL (postgres_scanner)
  ```sql
  COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
  ```
- **total 890 ms** (CH and PG read in parallel, then join locally)

**Head-to-head — fedq 1211 ms · DuckDB 890 ms**

---

### Q7 — Top categories for one user on one day — cross-source, highly selective (→10 file keys).

**Incoming query**
```sql
SELECT f.category_id, count(*) AS n
FROM access_logs a JOIN files f ON f.id=a.file_id
WHERE a.day = DATE '2026-02-01' AND a.user_id = 7
GROUP BY f.category_id ORDER BY n DESC LIMIT 5;
```

**fedq**

- → ClickHouse `ch`  ·  **4 ms**
  ```sql
  SELECT "file_id", "day", "user_id" FROM "default"."access_logs" AS a WHERE ((a.day = CAST('2026-02-01' AS DATE)) AND (a.user_id = 7))
  ```
- → PostgreSQL `pg`  ·  **1 ms**
  ```sql
  COPY (SELECT "category_id", "id" FROM "public"."files" AS f WHERE (f.id IN (318784, 353438, 338877, …))) TO STDOUT (FORMAT binary)
  ```
- local combine **16 ms**  →  **total 21 ms**

**DuckDB**

- → ClickHouse (httpfs `FORMAT Parquet`)  ·  **54 ms** to read
  ```sql
  SELECT file_id FROM access_logs WHERE day='2026-02-01' AND user_id=7 FORMAT Parquet
  ```
- → PostgreSQL (postgres_scanner)
  ```sql
  COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
  ```
- **total 65 ms** (CH and PG read in parallel, then join locally)

**Head-to-head — fedq 21 ms · DuckDB 65 ms**

---

### Q8 — Large downloaded files — cross-source, no fact filter, dim filters (10M scanned).

**Incoming query**
```sql
SELECT c.name, count(*) AS n
FROM access_logs a JOIN files f ON f.id=a.file_id JOIN categories c ON c.id=f.category_id
WHERE f.size_bytes > 9000000 AND c.is_active = true
GROUP BY c.name ORDER BY n DESC LIMIT 5;
```

**fedq**

- → ClickHouse `ch`  ·  **178 ms**
  ```sql
  SELECT "file_id" FROM "default"."access_logs" AS a
  ```
- → PostgreSQL `pg`  ·  **53 ms**
  ```sql
  COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" AS f WHERE (f.size_bytes > 9000000)) TO STDOUT (FORMAT binary)
  ```
- local combine **420 ms**  →  **total 651 ms**

**DuckDB**

- → ClickHouse (httpfs `FORMAT Parquet`)  ·  **767 ms** to read
  ```sql
  SELECT file_id FROM access_logs FORMAT Parquet
  ```
- → PostgreSQL (postgres_scanner)
  ```sql
  COPY (SELECT "id", "category_id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "size_bytes" > '9000000') TO STDOUT (FORMAT "binary");
  ```
- **total 806 ms** (CH and PG read in parallel, then join locally)

**Head-to-head — fedq 651 ms · DuckDB 806 ms**

---

### Q9 — LIMIT (no order) on PostgreSQL.

**Incoming query**
```sql
SELECT id, filename FROM files LIMIT 10;
```

_(PostgreSQL-only — does not touch the ClickHouse fact table.)_

**fedq**

- → PostgreSQL `pg`  ·  **total 3 ms**
  ```sql
  SELECT "id", "filename" FROM "public"."files" AS files LIMIT 10
  ```

**DuckDB**

- → PostgreSQL (postgres_scanner)  ·  **total 1 ms**
  ```sql
  COPY (SELECT "id", "filename" FROM "public"."files"  LIMIT 10) TO STDOUT (FORMAT "binary");
  ```

**Head-to-head — fedq 3 ms · DuckDB 1 ms**

---

### Q11 — ORDER BY + LIMIT on PostgreSQL.

**Incoming query**
```sql
SELECT id, size_bytes FROM files ORDER BY size_bytes DESC LIMIT 10;
```

_(PostgreSQL-only — does not touch the ClickHouse fact table.)_

**fedq**

- → PostgreSQL `pg`  ·  **total 29 ms**
  ```sql
  SELECT "id", "size_bytes" FROM "public"."files" AS files ORDER BY size_bytes DESC LIMIT 10
  ```

**DuckDB**

- → PostgreSQL (postgres_scanner)  ·  **total 119 ms**
  ```sql
  COPY (SELECT "id", "size_bytes" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid) TO STDOUT (FORMAT "binary");
  ```

**Head-to-head — fedq 29 ms · DuckDB 119 ms**

---

### Q12 — Literal IN-list on PostgreSQL.

**Incoming query**
```sql
SELECT id, category_id FROM files WHERE id IN (10, 250, 999, 40000, 123456);
```

_(PostgreSQL-only — does not touch the ClickHouse fact table.)_

**fedq**

- → PostgreSQL `pg`  ·  **total 3 ms**
  ```sql
  SELECT "id", "category_id" FROM "public"."files" AS files WHERE (id IN (10, 250, 999, 40000, 123456))
  ```

**DuckDB**

- → PostgreSQL (postgres_scanner)  ·  **total 3 ms**
  ```sql
  COPY (SELECT "id", "category_id" FROM "public"."files" WHERE ctid BETWEEN '(0,0)'::tid AND '(1000,0)'::tid AND "id" IN ('10', '250', '999', '40000', '123456')) TO STDOUT (FORMAT "binary");
  ```

**Head-to-head — fedq 3 ms · DuckDB 3 ms**

---

## Summary — all queries

| Q | what it tests | fedq | DuckDB | winner |
|---|---|--:|--:|:--:|
| Q0 | count(*) on the remote files table (PostgreSQL only) | 13 ms | 28 ms | **fedq** |
| Q1 | Daily access counts per category for one week — cross-source (→308k file keys) | 565 ms | 619 ms | **fedq** |
| Q2 | Filter + projection on one PostgreSQL table | 3 ms | 2 ms | DuckDB |
| Q3 | Projection + range filter on one PostgreSQL table | 3 ms | 2 ms | DuckDB |
| Q4 | Top-5 active categories — same-source join in PostgreSQL | 170 ms | 64 ms | DuckDB |
| Q5 | Single-table aggregate in PostgreSQL | 115 ms | 72 ms | DuckDB |
| Q6 | Top categories by download events — cross-source, non-selective (→394k file keys) | 1211 ms | 890 ms | DuckDB |
| Q7 | Top categories for one user on one day — cross-source, highly selective (→10 file keys) | 21 ms | 65 ms | **fedq** |
| Q8 | Large downloaded files — cross-source, no fact filter, dim filters (10M scanned) | 651 ms | 806 ms | **fedq** |
| Q9 | LIMIT (no order) on PostgreSQL | 3 ms | 1 ms | DuckDB |
| Q11 | ORDER BY + LIMIT on PostgreSQL | 29 ms | 119 ms | **fedq** |
| Q12 | Literal IN-list on PostgreSQL | 3 ms | 3 ms | **fedq** |

The PostgreSQL-only rows (Q0/Q2/Q3/Q4/Q5/Q9/Q11/Q12) don't touch ClickHouse and
behave as in the DuckDB-local doc. The interesting rows are the **cross-source**
ones (Q1/Q6/Q7/Q8). There, with the fact table external, **fedq wins the
selective and full-scan queries** — the opposite of the DuckDB-local doc, where
DuckDB led every cross-source query. Two reasons:

1. **DuckDB's ClickHouse access is expensive.** Its only path is an
   httpfs/`FORMAT Parquet` HTTP round trip (tens of ms even for 10 rows,
   hundreds of ms to drag the big results across) — not a native columnar
   scanner. fedq's native Arrow connector is leaner on the fetch.
2. **fedq pushes the G9 dynamic `IN`-list to Postgres** (Q7: ~10 file rows vs
   DuckDB pulling all 400k). On the non-selective Q6, neither pushes a useful
   filter and DuckDB's single C++ join pipeline beats fedq's operator-by-operator
   merge.

**Why Q6 is the outlier (future work).** Q6 is the only cross-source query DuckDB
wins, because it forces the most rows through the *local* join with nothing to
prune them: `action='download'` keeps ~2.5M fact rows (25% of the table) and
every one matches a file and a category, so — unlike Q7 (selective → 10 rows), Q1
(smaller, 776k), or Q8 (reduced to ~960k by the dimension filter at the first
join) — all 2.5M flow through both joins and the aggregate, and the 394k distinct
keys are far over the dynamic-`IN` cap so nothing pushes down. fedq runs that
pipeline operator-by-operator (each join a separate merge-engine query with Arrow
handed between them — ~387ms + ~426ms in the two joins per the profile), whereas
DuckDB fuses scan→join→join→aggregate into one C++ pipeline. The fix is to render
a whole local same-source subtree as a *single* merge-engine query instead of
chaining per-operator; tracked for the optimization phase.

Numbers are median of warm runs; the big ClickHouse-over-HTTP reads have real
run-to-run variance, but the per-query winners are stable. Results verified
identical to the DuckDB-local runs (Q1 → 305,524 rows, Q7 → 10).
