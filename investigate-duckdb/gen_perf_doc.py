#!/usr/bin/env python
"""Generate the per-query performance section of performance-comparison.md.

For each query it records, in one consistent block: a one-line description, the
incoming SQL, what fedq sends to each source (DuckDB + PostgreSQL) with per-source
timing, what DuckDB sends to PostgreSQL plus its internal plan, total times, and
the head-to-head. Run from investigate-duckdb/ with PG up on :5432 (duckpoc)."""
import os, re, time, subprocess, statistics

os.environ["FEDQ_PROFILE"] = "1"  # populate last_profile_report for per-source timing

ROOT = "/workspace/federated-query/investigate-duckdb"
CFG = f"{ROOT}/fedq-poc-config.yaml"
PGLOG = "/workspace/federated-query/postgres-server.log"
DUCK = f"{ROOT}/bin/duckdb-stable"
EXT = f"{ROOT}/pg-scanner-src/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension"
ATTACH = ("ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' "
          "AS pg (TYPE postgres, READ_ONLY);")

from federated_query.cli.fedq import _prepare_runtime
RT, _, _ = _prepare_runtime(CFG)


def pg_log_tail(start):
    """Return PG statements logged since line `start` that touch our tables."""
    with open(PGLOG) as handle:
        lines = handle.readlines()[start:]
    out = []
    for line in lines:
        match = re.search(r"LOG:  (?:statement|execute [^:]*): (.*)", line)
        if not match:
            continue
        sql = match.group(1).rstrip()
        if re.search(r"files|categories", sql) and "LIMIT 0" not in sql:
            if not re.search(r"information_schema|pg_catalog|to_regclass", sql):
                out.append(sql)
    return out


def pg_log_len():
    """Current PG log length in lines."""
    with open(PGLOG) as handle:
        return len(handle.readlines())


def fedq_sources(sql):
    """Per-source queries fedq plans: list of (source_name, sql_text)."""
    doc = RT.explain(sql)
    rows = []
    for entry in doc["queries"]:
        query = entry["query"]
        text = query.sql(dialect="postgres") if hasattr(query, "sql") else str(query)
        rows.append((entry.get("datasource_name"), text))
    return rows


def parse_scan_times(report):
    """Map data-source prefix -> summed PhysicalScan self ms from a profile report."""
    pg_ms = duck_ms = 0.0
    for line in report.splitlines():
        match = re.search(r"([\d.]+) / +[\d.]+  PhysicalScan\(([^)]*)\)", line)
        if not match:
            continue
        self_ms, label = float(match.group(1)), match.group(2)
        if label.startswith("pg."):
            pg_ms += self_ms
        elif label.startswith("analytics."):
            duck_ms += self_ms
    return pg_ms, duck_ms


def fedq_run(sql):
    """Warm, then a measured run: returns (total_ms, pg_ms, duck_ms, full_pg_sql)."""
    RT.execute(sql)
    RT.execute(sql)
    start_log = pg_log_len()
    started = time.time()
    RT.execute(sql)
    total_ms = (time.time() - started) * 1000
    pg_ms, duck_ms = parse_scan_times(RT.query_executor.last_profile_report or "")
    return total_ms, pg_ms, duck_ms, pg_log_tail(start_log)


def duck_copies(sql):
    """What DuckDB (stable) sends to PostgreSQL for `sql`."""
    start_log = pg_log_len()
    script = f"LOAD postgres;\n{ATTACH}\n{sql}\n"
    subprocess.run([DUCK, "-readonly", f"{ROOT}/analytics-poc.duckdb"], input=script,
                   text=True, capture_output=True)
    return pg_log_tail(start_log)


def duck_time_ms(sql):
    """Median DuckDB wall time (ms) over 5 warm runs via the CLI .timer."""
    script = ".timer on\nLOAD postgres;\n" + ATTACH + "\n" + (sql + "\n") * 5
    res = subprocess.run([DUCK, "-readonly", f"{ROOT}/analytics-poc.duckdb"], input=script,
                         text=True, capture_output=True)
    times = re.findall(r"Run Time \(s\): real ([\d.]+)", res.stdout)
    nums = []
    for value in times[-5:]:
        nums.append(float(value) * 1000)
    return statistics.median(nums) if nums else float("nan")


def duck_plan(sql):
    """One-line DuckDB operator chain (leaf->root) from EXPLAIN."""
    script = f"LOAD postgres;\n{ATTACH}\nEXPLAIN {sql}\n"
    res = subprocess.run([DUCK, "-readonly", f"{ROOT}/analytics-poc.duckdb"], input=script,
                         text=True, capture_output=True)
    ops = []
    for token in re.findall(r"(POSTGRES_SCAN|SEQ_SCAN|HASH_JOIN|HASH_GROUP_BY|"
                            r"UNGROUPED_AGGREGATE|ORDER_BY|TOP_N|PROJECTION|FILTER|"
                            r"STREAMING_LIMIT|LIMIT)", res.stdout):
        if not ops or ops[-1] != token:
            ops.append(token)
    return " <- ".join(ops)


def _match_full_pg(explain_text, full_pg):
    """Pick the logged full PG statement for the table named in `explain_text`."""
    table = "categories" if "categories" in explain_text else "files"
    for statement in full_pg:
        if table in statement:
            return statement
    return explain_text


def block(qid, desc, incoming, duck_sql, fedq_sql):
    """Emit one fully-formatted per-query markdown block."""
    sources = fedq_sources(fedq_sql)
    total, pg_ms, duck_ms, full_pg = fedq_run(fedq_sql)
    local_ms = max(0.0, total - pg_ms - duck_ms)
    dcopies = duck_copies(duck_sql)
    dms = duck_time_ms(duck_sql)
    dplan = duck_plan(duck_sql)

    lines = [f"### {qid} — {desc}", "", "**Incoming query**", "```sql", incoming,
             "```", "", "**fedq — what it sends to each source:**", ""]
    # prefer the full (runtime) pg sql from the log, matched by table name; the
    # duckdb (analytics) sql comes from EXPLAIN (no runtime filter on that leaf)
    for source, text in sources:
        if source == "pg":
            text = _match_full_pg(text, full_pg)
        tag = "DuckDB · `analytics`" if source == "analytics" else "PostgreSQL · `pg`"
        lines += [f"- → {tag}", "  ```sql", f"  {text}", "  ```"]
    lines += ["",
              f"**fedq timing:** DuckDB fetch **{duck_ms:.0f} ms** · "
              f"PostgreSQL fetch **{pg_ms:.0f} ms** · local combine **{local_ms:.0f} ms** "
              f"· **total {total:.0f} ms**", "",
              "**DuckDB — what it sends to PostgreSQL:**", "```sql"]
    lines += dcopies
    lines += ["```",
              f"internal DuckDB plan (root ← leaves): `{dplan}` — everything above "
              f"the scans runs in DuckDB's own C++ engine.",
              f"**DuckDB total: {dms:.0f} ms**", "",
              f"**Head-to-head — fedq {total:.0f} ms · DuckDB {dms:.0f} ms**", "", "---", ""]
    return "\n".join(lines), total, dms


QUERIES = [
    ("Q0", "Count every file in PostgreSQL.",
     "SELECT count(*) FROM files;",
     "SELECT count(*) FROM pg.files;",
     "SELECT count(*) FROM pg.public.files;"),
    ("Q1", "Daily access counts per category for one week (10M-row fact joined to both PG dimensions).",
     "SELECT a.day, c.name, count(*) AS n\nFROM access_logs a\nJOIN files f ON f.id = a.file_id\nJOIN categories c ON c.id = f.category_id\nWHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07'\nGROUP BY a.day, c.name;",
     "SELECT a.day,c.name,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day,c.name;",
     "SELECT a.day,c.name,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day,c.name;"),
    ("Q2", "Find big files in one category (two-predicate filter on a single table).",
     "SELECT id, category_id, size_bytes FROM files\nWHERE category_id = 42 AND size_bytes > 5000000;",
     "SELECT id,category_id,size_bytes FROM pg.files WHERE category_id=42 AND size_bytes>5000000;",
     "SELECT id,category_id,size_bytes FROM pg.public.files WHERE category_id=42 AND size_bytes>5000000;"),
    ("Q3", "Read one column for the first 99 files (projection + range filter).",
     "SELECT category_id FROM files WHERE id < 100;",
     "SELECT category_id FROM pg.files WHERE id<100;",
     "SELECT category_id FROM pg.public.files WHERE id<100;"),
    ("Q4", "Top-5 active categories by file count — both tables in PostgreSQL.",
     "SELECT c.name, count(*) AS n\nFROM files f JOIN categories c ON c.id = f.category_id\nWHERE c.is_active = true\nGROUP BY c.name ORDER BY n DESC LIMIT 5;",
     "SELECT c.name,count(*) n FROM pg.files f JOIN pg.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;",
     "SELECT c.name,count(*) n FROM pg.public.files f JOIN pg.public.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;"),
    ("Q5", "Top-5 categories by file count + total size — single-table aggregate.",
     "SELECT category_id, count(*) AS n, sum(size_bytes) AS t\nFROM files GROUP BY category_id ORDER BY n DESC LIMIT 5;",
     "SELECT category_id,count(*) n,sum(size_bytes) t FROM pg.files GROUP BY category_id ORDER BY n DESC LIMIT 5;",
     "SELECT category_id,count(*) n,sum(size_bytes) t FROM pg.public.files GROUP BY category_id ORDER BY n DESC LIMIT 5;"),
    ("Q6", "Top-5 categories by download events — cross-source, but ~all files match.",
     "SELECT c.name, count(*) AS n\nFROM access_logs a JOIN files f ON f.id = a.file_id JOIN categories c ON c.id = f.category_id\nWHERE a.action = 'download'\nGROUP BY c.name ORDER BY n DESC LIMIT 5;",
     "SELECT c.name,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE a.action='download' GROUP BY c.name ORDER BY n DESC LIMIT 5;",
     "SELECT c.name,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE a.action='download' GROUP BY c.name ORDER BY n DESC LIMIT 5;"),
    ("Q7", "Top categories accessed by one user on one day — highly selective cross-source join.",
     "SELECT f.category_id, count(*) AS n\nFROM access_logs a JOIN files f ON f.id = a.file_id\nWHERE a.day = DATE '2026-02-01' AND a.user_id = 7\nGROUP BY f.category_id ORDER BY n DESC LIMIT 5;",
     "SELECT f.category_id,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id ORDER BY n DESC LIMIT 5;",
     "SELECT f.category_id,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id ORDER BY n DESC LIMIT 5;"),
    ("Q8", "Top categories for large downloaded files — cross-source with remote-column filters.",
     "SELECT c.name, count(*) AS n\nFROM access_logs a JOIN files f ON f.id = a.file_id JOIN categories c ON c.id = f.category_id\nWHERE f.size_bytes > 9000000 AND c.is_active = true\nGROUP BY c.name ORDER BY n DESC LIMIT 5;",
     "SELECT c.name,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE f.size_bytes>9000000 AND c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;",
     "SELECT c.name,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE f.size_bytes>9000000 AND c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;"),
    ("Q9", "Grab any 10 files (LIMIT, no order).",
     "SELECT id, filename FROM files LIMIT 10;",
     "SELECT id,filename FROM pg.files LIMIT 10;",
     "SELECT id,filename FROM pg.public.files LIMIT 10;"),
    ("Q11", "The 10 largest files (ORDER BY + LIMIT).",
     "SELECT id, size_bytes FROM files ORDER BY size_bytes DESC LIMIT 10;",
     "SELECT id,size_bytes FROM pg.files ORDER BY size_bytes DESC LIMIT 10;",
     "SELECT id,size_bytes FROM pg.public.files ORDER BY size_bytes DESC LIMIT 10;"),
    ("Q12", "Fetch five files by explicit id list (literal IN).",
     "SELECT id, category_id FROM files WHERE id IN (10, 250, 999, 40000, 123456);",
     "SELECT id,category_id FROM pg.files WHERE id IN (10,250,999,40000,123456);",
     "SELECT id,category_id FROM pg.public.files WHERE id IN (10,250,999,40000,123456);"),
]

HEADER = """# PostgreSQL + DuckDB sources — fedq vs DuckDB

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

"""

def _summary(rows):
    """Summary table over all queries, in order, from measured totals."""
    lines = ["## Summary — all queries", "",
             "| Q | what it tests | fedq | DuckDB | winner |",
             "|---|---|--:|--:|:--:|"]
    for qid, desc, ftotal, dtotal in rows:
        winner = "**fedq**" if ftotal < dtotal else "DuckDB"
        lines.append(f"| {qid} | {desc.rstrip('.')} | {ftotal:.0f} ms | {dtotal:.0f} ms | {winner} |")
    lines += ["", "DuckDB leads the cross-source queries here **because the fact table is "
              "inside its engine** — see the ClickHouse doc for the fair version.", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    parts = [HEADER]
    summary_rows = []
    for qid, desc, incoming, duck_sql, fedq_sql in QUERIES:
        markdown, ftotal, dtotal = block(qid, desc, incoming, duck_sql, fedq_sql)
        parts.append(markdown)
        summary_rows.append((qid, desc, ftotal, dtotal))
    parts.append(_summary(summary_rows))
    with open(f"{ROOT}/postgresql-duckdb-sources-compare.md", "w") as handle:
        handle.write("\n".join(parts))
    print("wrote postgresql-duckdb-sources-compare.md")
