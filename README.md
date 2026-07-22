# Federated Query Engine (fedq)

A federated SQL query engine in Rust that runs queries across heterogeneous data
sources (PostgreSQL, DuckDB, ClickHouse, MySQL, Parquet), pushing work down to
each source and joining across them. On top of the engine sits a materialized
event-analytics layer (Mixpanel/Amplitude-class: funnels, retention/cohorts,
segmentation, flows/paths) and a dynamic catalog for adding sources at runtime.

Three front doors over one runtime: a Python extension (`fedq`), a PostgreSQL
wire-protocol server (`fedq-server`), and a CLI (`fedq`).

## Quick start (build and run locally)

Prerequisites: a Rust toolchain (`cargo`), `curl` + `unzip` (to fetch the
prebuilt DuckDB library), and a working `python3` - the `fedq-py` extension is
built via pyo3. If a stale `VIRTUAL_ENV` points at a missing interpreter,
`make build` stops with a clear error and the fix (set `PYO3_PYTHON` or activate
a real venv) before compiling.

```
# Build the whole workspace in ONE command. The first run downloads the
# prebuilt libduckdb the engine links against (matching Cargo.lock, into
# .duckdb-lib/ - DuckDB is never compiled inside cargo); after that it just
# compiles. Needs internet access on the first run.
make build            # release build
# or:  make dev-build  # faster debug build

# Run the PostgreSQL-wire server over a config (no static sources needed -
# add them at runtime with the dynamic catalog).
printf 'datasources: {}\n' > fedq.yaml
target/release/fedq-server --config fedq.yaml --listen 127.0.0.1:5433

# Connect with any PostgreSQL client and query.
psql "host=127.0.0.1 port=5433 user=me dbname=fedq"
```

(`make build` runs `make setup` for you; run `make setup` alone if you only
want to fetch libduckdb without compiling.)

Then, in the psql session, register a source and run analytics:

```sql
CREATE DATASOURCE ev TYPE parquet WITH (dir = '/path/to/your/parquet_dir');
CREATE EVENT DATASET web FROM ev.main.events
  ACTOR user_id TIME ts EVENT event_name PROPERTIES (device, country);
FUNNEL ('page_view', 'view_item', 'add_to_cart') ON web WINDOW 7 DAY;
```

A full walkthrough (funnels, retention, segmentation, paths, refresh, auth) is
in `doc/event-analytics-tutorial.md`.

The CLI runs one statement without a server:
`target/release/fedq --config fedq.yaml --command "SHOW DATASOURCES"`.

## Features

- **Multi-source federation**: query across PostgreSQL, DuckDB, ClickHouse,
  MySQL, and Parquet, with same-source subtrees pushed down as one remote query.
- **Pushdown optimization**: predicate, projection, aggregate, order-by, limit,
  and same-source joins/set-ops pushed to the source.
- **Decorrelation**: correlated subqueries (EXISTS/IN/ANY/ALL/scalar) rewritten
  to joins with exact three-valued NULL semantics.
- **Dynamic catalog**: `CREATE / DROP / SHOW DATASOURCE` at runtime; the config
  is just bootstrap.
- **Event analytics**: materialized event datasets with funnel, retention,
  segmentation, and path analyses (see `crates/fq-events/README.md`).
- **Materialized views**, **users/ACLs (SCRAM)**, and native **EXPLAIN**.
- **Fail-fast**: errors surface; invalid queries raise instead of returning
  wrong rows.

## Architecture

The engine follows a classic database architecture with several optimization phases:

```
SQL Query
```

## Project Structure

```
federated-query/
```

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd federated-query

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

## Configuration

Create a YAML configuration file defining your data sources:

```yaml
datasources:
  postgres_prod:
    type: postgresql
    host: localhost
    port: 5432
    database: analytics
    user: postgres
    password: postgres
    schemas: [public]
    capabilities: [aggregations, joins, window_functions]

  local_duckdb:
    type: duckdb
    path: /data/analytics.duckdb
    read_only: true
    capabilities: [aggregations, joins]

optimizer:
  enable_predicate_pushdown: true
  enable_join_reordering: true

executor:
  max_memory_mb: 2048
  batch_size: 10000
```

See `config/example_config.yaml` for a complete example.

## Usage

```python
from federated_query.config import load_config
from federated_query.catalog import Catalog
from federated_query.parser import Parser, Binder
from federated_query.optimizer import RuleBasedOptimizer
from federated_query.executor import Executor

# Load configuration
config = load_config("config.yaml")

# Initialize catalog
catalog = Catalog()
# ... register data sources and load metadata ...

# Parse and execute query
parser = Parser()
binder = Binder(catalog)
optimizer = RuleBasedOptimizer(catalog)
executor = Executor(config.executor)

sql = """
    SELECT o.id, c.name, o.amount
    FROM postgres_prod.public.orders o
    JOIN local_duckdb.main.customers c ON o.customer_id = c.id
    WHERE o.amount > 1000
"""

logical_plan = parser.parse_to_logical_plan(sql, catalog)
bound_plan = binder.bind(logical_plan)
optimized_plan = optimizer.optimize(bound_plan)
# ... generate physical plan ...
results = executor.execute_to_table(physical_plan)

print(results)
```

### Explain Plans

Use `EXPLAIN` to inspect how a query will run without touching the underlying data sources:

```sql
EXPLAIN SELECT id, name FROM duckdb.main.users WHERE age > 30;
```


### Interactive CLI (`fedq`)

Install the package in editable mode to expose the `fedq` console script:

```bash
pip install -e .
fedq  # starts an interactive shell
```

By default the CLI starts an in-memory DuckDB data source populated with a `demo_users` table so you can experiment immediately:

```
fedq> SELECT * FROM duckdb_mem.main.demo_users WHERE age > 30;
```

Pass `--config path/to/config.yaml` to point the CLI at your own catalog configuration. The shell accepts multi-line statements terminated by `;` and supports `\q` to exit.

## Development Status

This project is under active development. See `HANDOFF.md` for the roadmap.

### Completed Phases

**Phase 0: Foundation** (done)
- Project structure and skeleton
- Core abstractions (plans, expressions, data sources)
- Configuration system
- Basic catalog structure
- Data source connectors (PostgreSQL, DuckDB)
- Test infrastructure

**Phase 1: Basic Query Execution** (done)
- Parser: AST to logical plan conversion
- Binder: Reference resolution with catalog integration
- Physical operators: Scan, Filter, Projection, Limit
- Basic executor: Single-table queries
- End-to-end pipeline for simple SELECT queries

**Query Example (Phase 1):**
```sql
SELECT col1, col2 FROM datasource.schema.table WHERE col1 > 10 LIMIT 100
```

**Phase 2: Joins and Multi-Table Queries** (done)
- Logical Join plan node with all join types
- Physical HashJoin implementation
- Physical NestedLoopJoin implementation
- Parser support for JOIN clauses
- Binder support for multi-table column resolution
- Physical planner join strategy selection
- All 70 tests passing (including 5 join tests)
- Federated join example working (DuckDB + PostgreSQL)

**Query Example (Phase 2):**
```sql
SELECT c.name, o.order_id, o.amount
FROM duckdb.customers c
JOIN postgres.orders o ON c.id = o.customer_id
WHERE o.amount > 1000
```

**Phase 3: Aggregations and Grouping** (done)
- Aggregations and grouping
- GROUP BY clause support (single and multiple columns)
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- HAVING clause support with expression rewriting
- Global aggregations (without GROUP BY)
- Federated aggregations across data sources

**Query Example (Phase 3):**
```sql
SELECT c.name, COUNT(*) as order_count, SUM(o.amount) as total_spent
FROM duckdb.customers c
JOIN postgres.orders o ON c.id = o.customer_id
GROUP BY c.name
HAVING SUM(o.amount) > 2000
```

**Phase 7: Subquery Decorrelation** (done)
- ANY / SOME / ALL quantified comparisons (incl. LIKE ALL)
  runtime cardinality guards, per-key limits for correlated LIMIT
- Boolean subqueries in SELECT lists (flag columns via SEMI/ANTI unions)
- OR-of-subqueries via union expansion; nested subqueries innermost-first
- Derived tables and subqueries in INNER join conditions
- Scoped subquery binding (correlated references resolved by the binder)
- All 116 decorrelation e2e tests passing against PostgreSQL

**Query Example (Phase 7):**
```sql
SELECT u.id, u.name,
       (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
FROM users u
WHERE u.country IN (SELECT code FROM countries WHERE enabled)
  AND EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100)
```

**Phase 7 Review + Phase 8 (in progress)**
- Native `EXPLAIN (FORMAT ...)` via a custom sqlglot dialect (no more
  Command-string fallback)
- Operator/cast correctness: `||`/`CONCAT`/`ILIKE`/`CAST`, string escaping,
  typed literals, OFFSET-without-LIMIT, NULL-aware comparisons
- Decorrelation correctness fixes (self-join aliasing, `NOT (subquery)`
  three-valued logic, EXISTS-over-global-aggregate, OR-expansion multiplicity,
  COUNT(DISTINCT), hash-join key orientation)
- Many pre-existing silent-fail / correctness fixes (DuckDB typed schemas,
  remote-join side filters, predicate-pushdown recursion, WHERE/HAVING split,
  NULLS FIRST/LAST, MIN/MAX type preservation)
  bind, single-source pushdown, and local multiset execution
- In progress (tracked in `TODO-phase7-review.md`, section G): broader
  same-source join pushdown (G1), computed-projection pushdown (G2), CTEs (G3),
  `CAST` target types (G5), date/time functions (G6), aggregate `FILTER` (G7)

### Known Limitations
  no dynamic filtering / semi-join reduction yet, so the probe side ships its
  entire table over the network. This is the top usability gap, tracked as
  **G9** in `TODO-phase7-review.md`.
- A comma/cross join with a two-sided equality in `WHERE` currently plans to a
  nested-loop (Cartesian) join + filter rather than a hash join (G9a).

### Next Phases
- **Phase 8**: Pushdown breadth (joins, projections), CTEs, date/time, and
  cross-source dynamic filtering (semi-join reduction)
- **Phase 9**: Parallel execution and memory management
- **Phase 10**: Window functions and remaining advanced SQL

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=federated_query

# Run specific test file
pytest tests/test_parser.py
```

## Documentation

- **[README-architecture.md](README-architecture.md)**: Detailed architecture and design decisions
- **[HANDOFF.md](HANDOFF.md)**: Current working state and roadmap

## Key Design Principles

1. **Immutable Plans**: All plan nodes are immutable for easier reasoning and thread safety
2. **Push-Down First**: Always try to push computation to data sources to minimize data transfer
3. **Arrow-Based**: Use Apache Arrow for efficient columnar data representation
4. **Cost-Based**: Use statistics and cost models to choose optimal execution strategies
5. **Extensible**: Easy to add new data sources, optimization rules, and operators

## Performance Considerations

Implemented today:
- **Predicate Pushdown**: Filters are pushed to data sources whenever possible
- **Projection Pushdown**: Only required columns are fetched from sources
- **Same-Source Pushdown**: Single-source joins and set operations are sent to the source as one remote query
- **Join Strategy Selection**: Chooses between hash and nested-loop joins (remote join when both sides share a source)

Not yet implemented (roadmap):

## Contributing

Contributions are welcome! Please see the roadmap in `HANDOFF.md` for areas that need work.

## License

MIT License

## References

### Papers
- "Volcano - An Extensible and Parallel Query Evaluation System" (Graefe, 1994)
- "The Cascades Framework for Query Optimization" (Graefe, 1995)
- "Unnesting Arbitrary Queries" (Neumann & Kemper, 2015)

### Systems
- PostgreSQL query planner
- Apache Calcite (federated query framework)
- DuckDB optimizer
- Presto/Trino distributed query engine
