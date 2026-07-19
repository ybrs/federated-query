# Federated e2e correctness suite

A data-driven corpus of SQL cases run through the native engine under a matrix of
data-source placements, each result checked value-by-value against a single-DuckDB
oracle. The suite proves the engine returns correct answers when the same tables
live on DuckDB, Parquet, and PostgreSQL sources in every combination.

## How to run

```bash
cd /workspace/federated-query
/workspace/venv-fedq/bin/python -m pytest tests/e2e_federated -q
```

PostgreSQL must be reachable (see `README-test-harness-setup.md`; `make pg-start`).
The suite connects once; if it cannot connect it raises loudly rather than
skipping silently. To run without PostgreSQL, set `FEDQ_E2E_SKIP_PG=1`: every
placement that would use a PostgreSQL source is then skipped explicitly and the
skips appear in the pytest output.

`FEDQ_E2E_CORPUS` selects which corpus modules are collected (comma-separated
keys of `cases.CORPUS_MODULES`, e.g. `FEDQ_E2E_CORPUS=sanity,corpus_joins`);
unset collects all of them.

The reliable full pass is ONE PROCESS PER MODULE, run sequentially:

```bash
for m in sanity corpus_joins corpus_edges corpus_subqueries \
         corpus_aggregates corpus_setops_ctes corpus_windows_expressions; do
  FEDQ_E2E_CORPUS=$m /workspace/venv-fedq/bin/python -m pytest tests/e2e_federated -q
done
```

A single all-modules process accumulates every environment's PostgreSQL
connections for the process lifetime (the engine's connector registry is
process-global and name-keyed, so the suite must mint unique datasource names
per environment, which defeats connection reuse and prevents release) and
fails late pg placements with `catalog error: datasource error: db error`
once the server's connection ceiling is hit. Per-module processes release
connections at exit and stay comfortably under it.

Each corpus module may carry a `SUSPECTED_ENGINE_BUGS` list: verified
engine-vs-oracle mismatches parked out of `CASES` so the suite stays green,
each with a `finding` description and a tracker ticket. Re-enable a parked
case when its ticket is fixed.

Test ids read as `case_name[placement]`, e.g.
`test_corpus[left_join[pg_duck]]`.

## The pieces

- `tables.py` - the table library: ~15 canonical tiny tables (the
  orders/products/customers trio, the fact_sales/dim_day/dim_item star, and the
  `t_*` edge tables for null keys, duplicate keys, emptiness, every type, dates,
  and ASCII text edges). Each spec is portable DDL plus inserts that parse
  identically on DuckDB and PostgreSQL. All values are ASCII.
- `cases.py` - the case model and `validate_case` (raises on unknown or missing
  keys); `all_cases()` collects and validates the corpus.
- `sanity/corpus.py` - the sanity cases themselves.
- `placements.py` - the seven placement strategies.
- `runtime.py` - config building, seeding, and the engine adapter (`Environment`).
- `oracle.py` + `compare.py` - the single-DuckDB ground truth and the diff.
- `conftest.py` + `test_corpus.py` - parametrization, caching, and the test.

## Query qualification convention

A case query references each table as a `{table_name}` placeholder. The harness
fills each placeholder with the placement's fully qualified three-part engine
name (`<datasource>.<schema>.<table>`) via `str.format`. Placeholders (not bare
names) are used so a substring like `order_id` is never mistaken for the `orders`
table, and an undeclared placeholder raises loudly. The oracle fills the same
placeholders with bare names, since all its tables live in one DuckDB `main`
schema.

## Placements

A placement names a fixed list of source slots and a rule for assigning a case's
(sorted) tables to them:

| Placement            | Slots            | Rule           |
|----------------------|------------------|----------------|
| `oracle_single_duck` | one DuckDB       | all to one     |
| `duck_duck`          | two DuckDB       | round-robin    |
| `pg_duck`            | PostgreSQL, DuckDB | round-robin  |
| `duck_pg`            | DuckDB, PostgreSQL | round-robin  |
| `all_pg`             | two PostgreSQL schemas | round-robin |
| `parquet_duck`       | Parquet, DuckDB  | first isolated |
| `parquet_pg`         | Parquet, PostgreSQL | first isolated |

Round-robin deals tables cyclically across the slots; first-isolated pins the
first table to the (read-only) Parquet slot and sends the rest to the other slot.
Only slots that receive a table become datasources, so a case with fewer tables
than slots yields fewer distinct sources.

DuckDB slots seed a temp `.duckdb` file. Parquet slots export each table to
`<dir>/<table>.parquet` (exposed under schema `main`). PostgreSQL slots seed a
uniquely named `fed_*` schema in the shared test database. Datasource names and
PostgreSQL schema names both carry a per-environment hash, because the engine's
exec-plane connector registry is process-global and keyed by datasource name; a
reused name would let two environments collide.

## Caching

Seeded environments and their runtimes are cached at session scope keyed by
(frozenset-of-tables, placement name), and oracles by frozenset-of-tables. Cases
that share a table set and placement reuse one seeded environment and one runtime,
so the whole corpus runs in a few seconds.

## Adding a case

Add a dict to `sanity/corpus.py::CASES`:

- `name` (required): unique id.
- `tables` (required): list of library table names.
- `query` (required): SQL with `{table_name}` placeholders.
- `order_sensitive` (optional, default False): compare rows in exact order; set
  True only with a deterministic top-level ORDER BY.
- `expect_error` (optional): substring the engine's error must contain; the case
  must raise in every placement and no oracle is computed.
- `min_sources` (optional, default 1): skip placements that spread the case's
  tables over fewer than this many distinct sources.
- `extra_tables` (optional): inline `{name: {"ddl", "inserts"}}` specs seeded
  alongside the library tables.

`validate_case` runs at collection, so a malformed case fails fast.

## Comparison

The engine result and the oracle result must have the same column names
(compared case-insensitively, because both DuckDB and PostgreSQL fold unquoted
identifiers to lower case), the same row count, and equal rows - as a multiset by
default, or in exact order when `order_sensitive`. Values are normalized so equal
data from different sources compares equal: integer widths collapse to Python
`int`, floats compare within a 1e-9 relative tolerance, `Decimal` compares
exactly, dates and timestamps compare as Python `date`/`datetime`, and strings
compare exactly. A mismatch raises `AssertionError` naming the placement, both row
counts, and the first differing row pair.
