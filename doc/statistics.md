# Source-catalog statistics

The cost-based optimizer estimates cardinalities from statistics fetched from
the data sources' own catalogs at optimization time. This documents where each
statistic comes from, what happens when one is missing, and the caching rules.

## What is collected

Per table: `row_count`. Per column (only columns the optimizer actually needs -
join keys and filtered columns): `num_distinct` (NDV), `null_fraction`,
`avg_width`, `min_value` / `max_value`.

## Per-source catalog reads

| Source | Row count | Column stats |
| --- | --- | --- |
| PostgreSQL | `pg_class.reltuples` (schema-qualified via `pg_namespace`) | `pg_stats`: `n_distinct` (signed encoding decoded), `null_frac`, `avg_width`, `histogram_bounds` ends as min/max |
| DuckDB | `duckdb_tables().estimated_size` (catalog read; exact for native tables) | ONE aggregate scan per fetch: `approx_count_distinct`, `count`, `min`, `max` for the requested columns only |
| Parquet | inherits DuckDB (tables are loaded into an in-memory DuckDB) | inherits DuckDB |
| ClickHouse | `count()` | `uniqExact` / `countIf(... IS NULL)` for the requested columns, one scan |

PostgreSQL statistics exist only after `ANALYZE`: a never-analyzed table
reports `reltuples = -1` and has no `pg_stats` rows. The connector reports that
honestly (`row_count = None`, columns absent) - it never coerces -1 to 0 and
never fabricates an NDV. `benchmarks/tpch/load_postgres.py` runs `ANALYZE`
after loading for this reason.

## Missing statistics: named defaults with provenance

A source never invents a value. When a statistic is missing the ESTIMATOR
substitutes one of the named defaults in
`federated_query/optimizer/estimate_defaults.py`:

| Constant | Value | Applies when |
| --- | --- | --- |
| `DEFAULT_ROW_COUNT` | 1000 | source reports no row count |
| `DEFAULT_ATOM_ROWS` | 1000 | join input is not a plain scan (no catalog identity) |
| `DEFAULT_NDV_FRACTION` | 0.1 | column NDV unknown: `ndv = max(1, rows * 0.1)` |
| `DEFAULT_EQ_SELECTIVITY` | 0.1 | equality on a column with unknown NDV |
| `DEFAULT_RANGE_SELECTIVITY` | 0.33 | range predicate without usable min/max |
| `DEFAULT_LIKE_SELECTIVITY` | 0.1 | LIKE (not estimable from catalog stats) |
| `DEFAULT_NULL_FRACTION` | 0.05 | IS NULL with unknown null statistics |

Every substitution is recorded in the estimate's `defaults_used` provenance
list (`CardinalityEstimate`), naming the default's target (e.g.
`ndv(duck.main.lineitem.l_suppkey)`), and EXPLAIN prints the defaulted markers
alongside the estimated row counts - a plan costed from guesses is always
visibly different from one costed from real statistics.

## Caching

`StatisticsCollector` caches per `(datasource, schema, table)` for the life of
the session and accumulates COLUMNS incrementally: a query needing two join
keys fetches exactly those two columns; a later query needing one more fetches
only the missing one. A column the source has no statistics for is attempted
once and its absence is cached. `clear_cache()` starts a fresh view. An
unknown datasource name raises - a typo would otherwise silently disable
costing.
