"""Optional test-only routing of query execution through the Rust engine.

Set ``FEDQRS=1`` to make every ``Executor.execute_to_table`` call try the Rust
engine (``fedqrs``) first, falling back to the DuckDB merge engine when the plan
is not yet supported or a source cannot be read natively. This lets the whole
existing suite run against the Rust engine to measure coverage:

  - a plan the Rust engine runs and gets right -> the test passes on Rust;
  - a plan it runs and gets wrong -> the test FAILS (a real Rust bug, surfaced);
  - a plan it cannot represent / read -> it falls back, test passes on DuckDB,
    and the reason is tallied.

A session-end summary prints how many executions went through Rust vs fell back,
grouped by reason. Without ``FEDQRS`` set this file does nothing.
"""

import collections
import os
import tempfile

_ROUTING = {
    "rust": 0,
    "unsupported": 0,
    "error": 0,
    "reasons": collections.Counter(),
}

# id(datasource) -> a DuckDB file path Rust can open. In-memory test databases
# are snapshotted to a temp file once, since a separate Rust connection cannot
# see another connection's in-memory database.
_DUCK_FILES = {}


def _reason(exc) -> str:
    """A short, groupable label for why a plan fell back."""
    text = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
    return text[:70]


def pytest_configure(config):
    """Monkeypatch the executor to route through Rust when FEDQRS is set."""
    if not os.environ.get("FEDQRS"):
        return

    from federated_query.executor.executor import Executor
    from federated_query.plan import PhysicalExplain

    original = Executor.execute_to_table

    def routed(self, plan, query_executor=None):
        # EXPLAIN is plan introspection, not data execution: it runs no query
        # through any engine, so it is not a routing candidate. Run it normally.
        if isinstance(plan, PhysicalExplain):
            return original(self, plan, query_executor=query_executor)
        catalog = getattr(query_executor, "catalog", None)
        datasources = getattr(catalog, "datasources", None)
        if datasources:
            outcome = _run_via_rust(plan, list(datasources.values()))
            if outcome is not None:
                return outcome
        return original(self, plan, query_executor=query_executor)

    Executor.execute_to_table = routed


def _run_via_rust(plan, datasources):
    """Try the Rust engine; return its table, or None to fall back."""
    import json

    import fedqrs
    import pyarrow as pa
    from federated_query.executor.rust_ir import build_ir, UnsupportedIR

    try:
        for datasource in datasources:
            _register_for_rust(datasource)
        ir = build_ir(plan)
        table = pa.RecordBatchReader.from_stream(fedqrs.execute_ir(json.dumps(ir))).read_all()
        _ROUTING["rust"] += 1
        return table
    except UnsupportedIR as exc:
        _ROUTING["unsupported"] += 1
        _ROUTING["reasons"][_reason(exc)] += 1
        return None
    except Exception as exc:  # noqa: BLE001 - measurement fallback, intentional
        _ROUTING["error"] += 1
        _ROUTING["reasons"]["err: " + _reason(exc)] += 1
        return None


def _register_for_rust(datasource):
    """Register one datasource with the Rust engine; snapshot in-memory DuckDB."""
    import fedqrs
    from federated_query.datasources.duckdb import DuckDBDataSource
    from federated_query.datasources.postgresql import PostgreSQLDataSource
    from federated_query.executor.rust_ir import _pg_adbc_driver_path

    if isinstance(datasource, PostgreSQLDataSource):
        params = {"uri": datasource._adbc_uri(), "adbc_driver": _pg_adbc_driver_path()}
        fedqrs.register_datasource(datasource.name, "postgres", params)
        return
    if isinstance(datasource, DuckDBDataSource):
        fedqrs.register_datasource(datasource.name, "duckdb", {"path": _duckdb_file(datasource)})
        return
    raise RuntimeError(f"no Rust connector for {type(datasource).__name__}")


def _duckdb_file(datasource):
    """A DuckDB file path Rust can open. A real file is used as-is; an in-memory
    DB is snapshotted to a temp file, re-snapshotting whenever its table set has
    changed since last time (module-scoped fixtures add tables mid-run)."""
    path = getattr(datasource, "db_path", None)
    if path and path != ":memory:" and os.path.exists(path):
        return path
    connection = datasource.connection
    signature = _table_signature(connection)
    cached = _DUCK_FILES.get(id(datasource))
    if cached is not None and cached[0] == signature:
        return cached[1]
    fresh = _snapshot_memory_db(connection)
    _DUCK_FILES[id(datasource)] = (signature, fresh)
    return fresh


def _table_signature(connection):
    """The current set of (schema, table) pairs, so a change forces a re-snapshot."""
    rows = connection.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
    ).fetchall()
    return tuple(sorted(rows))


def _snapshot_memory_db(connection):
    """Copy every table of an in-memory DuckDB to a fresh file, return its path."""
    path = tempfile.mktemp(suffix=".duckdb")
    connection.execute(f"ATTACH '{path}' AS __rust")
    rows = connection.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
    ).fetchall()
    for schema, table in rows:
        connection.execute(
            f'CREATE TABLE __rust."{table}" AS SELECT * FROM "{schema}"."{table}"'
        )
    connection.execute("DETACH __rust")
    return path


def pytest_sessionfinish(session, exitstatus):
    """Print the Rust-routing coverage summary at the end of the run."""
    if not os.environ.get("FEDQRS"):
        return
    total = _ROUTING["rust"] + _ROUTING["unsupported"] + _ROUTING["error"]
    print("\n\n=== FEDQRS routing coverage ===")
    print(
        f"executions routed to Rust: {_ROUTING['rust']}  |  "
        f"fell back (unsupported): {_ROUTING['unsupported']}  |  "
        f"fell back (error): {_ROUTING['error']}  |  total: {total}"
    )
    print("top fallback reasons:")
    for reason, count in _ROUTING["reasons"].most_common(25):
        print(f"  {count:5}  {reason}")
    with open("/tmp/fedqrs_routing.txt", "w") as handle:
        handle.write(repr(dict(_ROUTING)))
