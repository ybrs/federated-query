# Inspect plans, fragments, and verify answers

To obtain a plan for any full query in these chapters, put `EXPLAIN` before its SELECT or
WITH. `EXPLAIN (FORMAT JSON)` gives the same facts as a structured document.
EXPLAIN plans without executing the business query.

To capture the source fragments, enable recording and run a business query
in the same psql session:

```sql
SET diagnostics.record_pushdown_fragments = true;
```

After the query returns:

```sql
SHOW PUSHDOWN FRAGMENTS;
```

The chapters interpret these records as inputs to the execution graph:
`datasource` identifies the owner, `source_sql` is the source-dialect fragment,
`canonical_sql` is its PostgreSQL-form counterpart, and `output_names` names
what comes back to the coordinator. `has_dynamic_filter` distinguishes a
base query with an execution-time key restriction from an unrestricted read.
A failed query leaves the previous successful query's fragments in place;
a fresh connection has a separate journal.

Use psql's `\x on` for wide fragment output and `\x off` afterward. The
source SQL blocks in the chapters are inspection output with local upstream
table names; the business SQL blocks are the statements to submit to FedQ.

Keep FedQ server running. Leave psql with `\q`, then run the verifier from
the repository root with the virtualenv active:

```bash
"$VIRTUAL_ENV/bin/python3" tutorials/game-studio/verify.py \
  > /tmp/game-studio-verification.log 2>&1
cat /tmp/game-studio-verification.log
```

It connects to the server on port 56432, checks all five base-table row
counts, executes every scenario, compares its column names and values against
DuckDB, and captures its plan and source fragments. Declared outer ordering
is checked; unordered results are sorted for comparison without dropping
duplicates. An invalid
column must raise. A missing source/server or a mismatch raises; nothing is
skipped. Use `--dsn "$FEDQ_DSN"` for a different endpoint.

Answers are saved in `generated/results.json`; plans and fragments are in
`generated/inspections.json`. Integer counts and money are compared exactly;
floating expressions allow rounding differences. Wire-protocol decimals are
saved as exact JSON strings. The connection uses autocommit because the
server does not implement transaction-control statements.

The repository regression command is `./run_all_tests.sh`. The showcase
verifier checks this running installation and its data, rather than replacing
that complete suite.

## Scope of the timestamp examples

The gameplay examples use player-level associations. Timestamp-window variants
raised on coordinator interval expressions and MySQL DATETIME decoding; an
epoch-projection variant also failed on ClickHouse. Those paths are not
demonstrated by these chapters. The existence query's two conditions must not
be read as a claim of event-to-ticket causality or temporal attribution.

---

[Contents](README.md) | [Previous: setup.md](setup.md) | [Next: cli-and-notebooks.md](cli-and-notebooks.md)
