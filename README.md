# Federated Query Engine

Federated Query Engine is a Rust SQL engine that queries heterogeneous data
sources and combines the results over Apache Arrow. It supports PostgreSQL,
DuckDB, ClickHouse, MySQL, and Parquet sources, with pushdown and federated
execution across sources.

The engine has three front doors over one runtime:

- fedq-server: a PostgreSQL wire-protocol server for psql and other
  PostgreSQL clients.
- fedq: a command-line client with one-shot and interactive modes.
- fedq-py: a Python extension used by the Python integration and benchmark
  harnesses.

## Quick start

Prerequisites are a Rust toolchain, Python 3, and curl plus unzip for the
prebuilt DuckDB library. A PostgreSQL client such as psql is useful for the
server example.

Build the workspace from the repository root:

~~~bash
make venv
make build
~~~

make build prepares the pinned DuckDB library and builds the release
artifacts. It does not compile DuckDB inside Cargo.

Start a server with an empty bootstrap catalog:

~~~bash
mkdir -p run
printf 'datasources: {}\n' > run/fedq.yaml
target/release/fedq-server --config run/fedq.yaml --listen 127.0.0.1:5433
~~~

Connect with any PostgreSQL client:

~~~bash
psql "host=127.0.0.1 port=5433 user=analyst dbname=fedq"
~~~

With no server.users section, the server uses trust authentication. See
crates/fedq-server/README.md for SCRAM configuration and credential
generation.

## Add sources at runtime

The YAML file can be empty. Sources can be registered, inspected, queried, and
removed while the server is running:

~~~sql
CREATE DATASOURCE events TYPE parquet
  WITH (dir = '/data/events');

SHOW DATASOURCES;
SHOW TABLES FROM events;

SELECT user_id, event_name, ts
FROM events.main.events
ORDER BY ts
LIMIT 20;

DROP DATASOURCE events;
~~~

CREATE DATASOURCE connects to the source and loads its metadata before the
source is installed. A failed connection or metadata load raises and does not
leave a partial catalog entry. Dynamic sources are persisted with the engine
state associated with the config path and are restored when the server starts
again. Bootstrap sources declared in YAML remain managed by the config file
and cannot be dropped with SQL.

The supported source kinds are postgres, postgresql, duckdb, clickhouse,
mysql, and parquet. Connection parameters are validated for the selected
source kind. See the event analytics tutorial for complete PostgreSQL and
Parquet examples:
doc/event-analytics-tutorial.md.

## Query features

- Federated queries across multiple sources.
- Predicate, projection, aggregate, ordering, limit, and same-source join
  pushdown.
- Hash and nested-loop joins, join ordering, dynamic filters, and semi-join
  reductions.
- Correlated subquery decorrelation with NULL-aware semantics.
- Materialized views with explicit refresh and query substitution.
- Event datasets with funnel, retention, segmentation, and path analyses.
- EXPLAIN, catalog inspection, and runtime settings.
- Fail-fast validation: invalid SQL, unknown tables, and unknown columns raise
  errors instead of producing rows.

Example federated query:

~~~sql
SELECT o.id, c.name, o.amount
FROM postgres_prod.public.orders AS o
JOIN local_duckdb.main.customers AS c ON o.customer_id = c.id
WHERE o.amount > 1000;
~~~

For event analytics, start with:

~~~sql
CREATE EVENT DATASET web FROM events.main.events
  ACTOR user_id TIME ts EVENT event_name
  PROPERTIES (device, country);

FUNNEL ('page_view', 'view_item', 'add_to_cart')
  ON web WINDOW 7 DAY;
~~~

## CLI

The CLI uses the same YAML config and runtime as the server:

~~~bash
# Run one statement.
target/release/fedq --config run/fedq.yaml \
  --command "SHOW DATASOURCES"

# Run a SQL file as one statement.
target/release/fedq --config run/fedq.yaml --file report.sql

# Start the interactive shell.
target/release/fedq --config run/fedq.yaml
~~~

Use --format table, --format csv, or --format json for one-shot output.
Statements in the interactive shell end with a semicolon; \q exits and
\timing toggles elapsed-time output. The full option reference is in
crates/fedq/README.md.

## Configuration

The YAML config bootstraps data sources and runtime settings. A minimal config
for runtime registration is:

~~~yaml
datasources: {}
~~~

Static sources can instead be declared under datasources:

~~~yaml
datasources:
  warehouse:
    type: postgresql
    host: localhost
    port: 5432
    database: analytics
    user: analyst
    password: change-me
    schemas: [public]

  local:
    type: duckdb
    path: /data/analytics.duckdb
~~~

The complete example is in config/example_config.yaml. Unknown config fields
are rejected. Secrets shown in examples should be replaced with real
credentials and should not be committed.

## Architecture

The workspace is organized as focused Rust crates:

~~~text
SQL text
  -> fq-parse       parse and classify statements
  -> fq-bind        resolve names and types
  -> fq-decorrelate rewrite subqueries into joins
  -> fq-optimize    optimize and estimate from metadata
  -> fq-physical    choose execution and pushdown steps
  -> fq-exec        execute source reads and coordinator work
  -> Arrow results
~~~

fq-runtime orchestrates the pipeline and owns statement dispatch, the live
catalog, persisted dynamic sources, materialized views, event datasets,
settings, and planning-budget enforcement. The connectors separate source
metadata and statistics from the execution data plane.

See README-architecture.md for crate boundaries, expression semantics,
planning constraints, and storage details.

## Repository layout

~~~text
crates/fq-parse/          SQL parsing and statement classification
crates/fq-bind/           Binding and type resolution
crates/fq-catalog/        Catalog and metadata model
crates/fq-optimize/       Logical optimization and cost estimation
crates/fq-physical/       Physical planning and pushdown
crates/fq-exec/           Federated execution
crates/fq-runtime/        Runtime orchestration and statement dispatch
crates/fq-events/         Event dataset storage and analytics
crates/fq-accel/          Materialized-view storage
crates/fedq-server/       PostgreSQL wire-protocol server
crates/fedq/              CLI
crates/fedq-py/           Python extension
tests/                    End-to-end suites
doc/                      Tutorials and design documentation
~~~

## Development

The repository has one complete test command:

~~~bash
./run_all_tests.sh
~~~

It runs the preflight checks, linters, Rust tests, Python tests, and end-to-end
tests. The complete output is written to run_all_tests.log.

Useful build commands are:

~~~bash
make setup       # fetch the pinned prebuilt DuckDB library
make dev-build   # debug build
make build       # release build
make hook-install
~~~

## License

MIT License


## References

### Papers
- "Volcano - An Extensible and Parallel Query Evaluation System" (Graefe, 1994)
- "The Cascades Framework for Query Optimization" (Graefe, 1995)
- "Unnesting Arbitrary Queries" (Neumann & Kemper, 2015)
-
