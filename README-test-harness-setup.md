# Test Harness Setup

Some test suites in this repository run against **real** data sources rather than
mocks, because the engine's whole job is to push computation down into live
databases. This document explains how to provision those data sources locally and
how the test fixtures wire them into the engine.

There are two kinds of data source used by the tests:

| Source     | Used by                                   | Provisioning            |
|------------|-------------------------------------------|-------------------------|
| DuckDB     | `tests/e2e_pushdown`, most `test_e2e_*`   | In-process, in-memory   |
| PostgreSQL | `tests/e2e_decorrelation`, `test_datasources` | Local server (below) |

DuckDB needs nothing — it runs in-memory inside the test process. PostgreSQL needs
a running server, which the scripts in `scripts/` provide without any system
install or Docker.

## 1. Provision PostgreSQL

The helper scripts download a self-contained PostgreSQL build (from
[theseus-rs/postgresql-binaries](https://github.com/theseus-rs/postgresql-binaries))
into `./postgres-17` and manage a data directory in `./postgres-data`. Both paths
are git-ignored.

```bash
# One-time: download the binaries for your OS/arch (auto-detected).
make download_postgres          # or: ./scripts/download_postgresql.sh

# Start PostgreSQL in the background (initializes the cluster on first run).
make pg-start                   # or: ./scripts/run-postgres.sh

# ... run tests ...

# Stop it when you are done (the cluster is preserved for next time).
make pg-stop                    # or: ./scripts/stop-postgres.sh
```

`run-postgres.sh` starts the server detached via `pg_ctl`, waits until it accepts
connections, and creates the `test_db` database. Server logs go to
`./postgres-server.log`.

### Defaults and overrides

The scripts and the test fixtures share the same defaults, which also match the
CI workflow, so a fresh `make pg-start` needs no configuration:

| Setting  | Default     | Env override        |
|----------|-------------|---------------------|
| host     | `localhost` | `POSTGRES_HOST`     |
| port     | `5432`      | `PGPORT` (scripts) / `POSTGRES_PORT` (tests) |
| database | `test_db`   | `PGDATABASE` / `POSTGRES_DB` |
| user     | `postgres`  | `PGUSER` / `POSTGRES_USER` |
| password | `postgres`  | `PGPASSWORD` / `POSTGRES_PASSWORD` |

The cluster is initialized with `trust` local authentication, so the password is
accepted but not enforced — convenient for a local-only test database.

## 2. Run the tests

```bash
make pg-start
make test            # full suite
make test-no-db      # skip the PostgreSQL-backed decorrelation suite
make pg-stop
```

In CI, PostgreSQL is provided by a service container on port 5432 with the same
credentials (see `.github/workflows/`), so the same fixtures work unchanged.

## 3. How the fixtures register data sources

Test fixtures build a `Catalog`, register one or more `DataSource` objects, and
call `load_metadata()` so the binder can resolve columns. The pattern lives in the
suite-level `conftest.py` files.

### DuckDB (in-memory) — see `tests/e2e_pushdown/conftest.py`

```python
ds = DuckDBDataSource(name="duckdb_primary", config={"database": ":memory:", "read_only": False})
ds.connect()
ds.connection.execute("CREATE TABLE orders (...)")   # seed data

catalog = Catalog()
catalog.register_datasource(ds)
catalog.load_metadata()
```

Queries then address the table with the engine's three-part name
`datasource.schema.table`, e.g. `duckdb_primary.main.orders`.

### PostgreSQL — see `tests/e2e_decorrelation/conftest.py`

The decorrelation tests reference tables with **two-part** names such as
`pg.users`. The parser resolves a two-part name as `schema.table` inside the
data source named `default` (see `Parser._extract_table_parts`):

| SQL name        | datasource | schema   | table |
|-----------------|------------|----------|-------|
| `users`         | `default`  | `public` | users |
| `pg.users`      | `default`  | `pg`     | users |
| `duckdb.main.t` | `duckdb`   | `main`   | t     |

So to make `pg.users` resolve, the fixtures:

1. Register the PostgreSQL source under the name **`default`** with
   `schemas=["pg"]`.
2. Create a PostgreSQL schema literally named **`pg`** and create the tables in it
   (the fixture sets `search_path TO pg`).
3. Call `catalog.load_metadata()` after the tables exist.

```python
ds = PostgreSQLDataSource(name="default", config={
    "host": "localhost", "port": 5432, "database": "test_db",
    "user": "postgres", "password": "postgres", "schemas": ["pg"],
})
ds.connect()

with ds.get_connection() as conn:          # pooled connection context manager
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA pg")
        cur.execute("SET search_path TO pg")
        cur.execute("CREATE TABLE users (...)")
        conn.commit()

catalog = Catalog()
catalog.register_datasource(ds)
catalog.load_metadata()
```

## 4. Adding a new test data source

1. Pick the datasource **name** to match the names your test SQL will use:
   - three-part SQL (`mydb.main.t`) → register the source as `mydb`;
   - two-part SQL (`s.t`) → register the source as `default` and put the table in
     a schema named `s`.
2. For PostgreSQL, create the schema/tables (the harness database is `test_db`);
   for DuckDB, create them on the in-memory connection.
3. `register_datasource(...)` then `load_metadata()` so the binder sees columns.
4. Read connection parameters from the `POSTGRES_*` environment variables (with
   the defaults above) so the suite runs both locally and in CI.
