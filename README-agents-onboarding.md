# Agent onboarding

Read this first. It is the operational map for working in this repo so you do
not rediscover the environment from scratch. For coding rules read `CLAUDE.md`;
for how the engine works read `README-architecture.md`. ASCII only, no line
numbers (they drift) - refer to files, classes, and methods by name.

Working directory for everything below: `/workspace/federated-query`.

## Environment

### Python virtualenv

`/workspace/venv-fedq` (Python 3.13). Always use its interpreter explicitly:

```
/workspace/venv-fedq/bin/python -m pytest ...
```

Do NOT rely on a bare `python`/`pytest` on PATH. Install/refresh deps with
`make install` (runs `pip install -r requirements.txt -e .` into the venv).

### Background services (already running in this environment)

Two real databases run in the background and are used by the end-to-end tests
and connectors. You normally do NOT need to start them - they are up.

- PostgreSQL: a self-contained bundled build under `./postgres-17` with its data
  in `./postgres-data`, listening on `localhost:5432`. Superuser `postgres` with
  trust auth (no password needed locally). Databases that exist: `postgres`,
  `test_db`, `duckpoc`. Managed by the Makefile:
  - `make pg-start` - start it in the background (idempotent; initializes the
    cluster and creates the DB on first run). Wraps `scripts/run-postgres.sh`.
  - `make pg-stop` - stop it. `scripts/download_postgresql.sh` fetches the
    binaries the first time. Server log: `./postgres-server.log`.
  - The pg-backed tests read `POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_DB`/
    `POSTGRES_USER`/`POSTGRES_PASSWORD` (defaults `localhost` / `5432` /
    `test_db` / `postgres` / `postgres`). The fixtures CREATE their own tables,
    so any existing, writable database works.
- ClickHouse: a running server on `localhost:8123` (HTTP) and `localhost:9000`
  (native). Tests read `CLICKHOUSE_HOST`/`CLICKHOUSE_PORT` (default
  `127.0.0.1` / 8123) and SKIP themselves if it is unreachable, so ClickHouse is
  optional for a green run.
- DuckDB: embedded (a library, not a server). No process to manage; the merge
  engine and the DuckDB connector open in-memory/file connections directly.

### Running the test suite

Full suite (Postgres must be up - it is):

```
POSTGRES_DB=duckpoc /workspace/venv-fedq/bin/python -m pytest -q
```

`POSTGRES_DB=duckpoc` selects an existing database; `make test` also works and
uses the `test_db` default. As of this writing a green run is ~1050+ passed, a
few skipped, ~21 xfailed. Useful variants:

- `make test-no-db` - skip the Postgres-backed tests (ignores
  `tests/e2e_decorrelation`), for when pg is not available.
- A single file: `POSTGRES_DB=duckpoc /workspace/venv-fedq/bin/python -m pytest tests/test_binder.py -q`.

Tests are the end-of-work verification, not the driver. Do not add special-case
branches just to turn a test green (see `CLAUDE.md`).

## Codebase map

Package root: `federated_query/`. The pipeline, in order (orchestrated by
`processor/query_executor.py::QueryExecutor`):

```
SQL text
  -> before-processors (SELECT * expansion)   processor/query_preprocessor.py
  -> parse    (sqlglot AST -> logical plan)    parser/parser.py
  -> bind     (resolve tables/columns, types)  parser/binder.py
  -> decorrelate (subqueries -> joins)         optimizer/decorrelation.py
  -> optimize (rule-based pushdown)            optimizer/rules.py
  -> physical plan (single-source vs merge)    optimizer/physical_planner.py
  -> execute  (pull-based Arrow batches)       executor/executor.py
  -> after-processors (rename to user names)   processor/query_preprocessor.py
```

Main areas:

- `parser/` - `parser.py` (sqlglot AST -> logical plan), `binder.py` (name/type
  resolution against the catalog), `dialect.py` (`FedQPostgres`, the canonical
  internal dialect), `errors.py`.
- `plan/` - the data model. `logical.py` (logical nodes + `transform_children`),
  `physical.py` (physical operators + execution), `expressions.py` (expression
  nodes + the tree walkers `map_children` / `expression_children` / `column_refs`),
  `arrow_types.py` (the one `DataType -> Arrow` authority), and `emit/` (the one
  SQL emitter: `expressions.py` = `SqlglotEmitter`, `clauses.py` = clause
  builders + `assemble_select`, `resolver.py` = `SourceResolver`/`MergeResolver`).
- `optimizer/` - `rules.py` (pushdown rules), `decorrelation.py` (subquery ->
  join), `single_source_pushdown.py` (render a same-source subtree as one remote
  query), `physical_planner.py` (logical -> physical), `pushdown.py` (side
  deciders), `scope_validator.py`. `cost.py`/`statistics.py` exist but are NOT
  wired into the pipeline yet.
- `executor/` - `executor.py` (drives the plan), `expression_evaluator.py` (the
  one local vectorized expression engine), `merge_engine.py` (the shared DuckDB
  instance that runs cross-source SQL).
- `datasources/` - `base.py` (`DataSource` abstract connector + shared helpers
  and `map_native_type`), `postgresql.py`, `duckdb.py`, `clickhouse.py`.
- `catalog/` - `catalog.py` (metadata registry; delegates type mapping to the
  connector), `schema.py` (`Schema`/`Table`/`Column`).
- `config/config.py`, `cli/fedq.py` (the REPL/CLI, the one place exceptions are
  caught for display), `model.py` (`StateModel`, the Pydantic base every state
  type derives from).

## Conventions worth knowing before you touch code

The codebase has been deliberately consolidated onto single paths - reuse the
existing helper instead of hand-rolling. Notable single homes:

- Column resolution at execution: `_physical_column_name` (in `physical.py`),
  backing both `_column_ref_type` and `_resolve_column`.
- Expression-tree rebuild: `plan/expressions.map_children` (allowlist + raise).
- Plan-tree recurse-and-rebuild: `plan/logical.transform_children`.
- SQL emission: `expression_to_ast` (the one emitter) + `clauses.assemble_select`
  + `build_table_ref`; rendered once via `to_source_sql`.
- Type mapping: connector `map_native_type` (native -> DataType) +
  `arrow_types.arrow_type_for` (DataType -> Arrow).

Guard tests enforce the doctrine - if you fight one, you are probably doing
something the project forbids:

- `tests/test_no_dataclasses.py` - no `@dataclass`; state types subclass
  `StateModel`.
- `tests/test_no_field_relisting.py` - never rebuild a node by re-listing its
  fields into the raw constructor (silently drops a defaulted field); use
  `model_copy(update=...)`.
- `tests/test_node_field_preservation.py` - `with_children` preserves every field.

The non-negotiables live in `CLAUDE.md`: never fail silently, an invalid query
MUST raise, allowlist + raise over denylist, ASCII only, no `@dataclass`, small
commented functions.
