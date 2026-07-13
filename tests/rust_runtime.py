"""Drive the native Rust engine (`fedq`) from the Python test suites.

The Rust engine ships as `target/release/libfedq_py.so`, symlinked to
`tests/fedq.so`. This module puts the `tests/` directory on `sys.path` and
imports it, then adapts `fedq.Runtime` to the interface the pushdown E2E tests
expect: `execute(sql)` returns a `pyarrow.Table`, and the EXPLAIN helpers parse
the engine's textual physical-plan dump.
"""

import os
import tempfile

import pyarrow as pa
import sqlglot

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SO_PATH = os.path.join(_TESTS_DIR, "fedq.so")
_BUILD_HINT = (
    "cargo build --release -p fedq-py "
    "(then: ln -sf ../target/release/libfedq_py.so tests/fedq.so)"
)


def _import_fedq():
    """Import the native `fedq` module, raising a build hint if it is missing."""
    if not os.path.exists(_SO_PATH):
        raise ImportError(
            "tests/fedq.so is missing; build the engine with: " + _BUILD_HINT
        )
    import sys

    if _TESTS_DIR not in sys.path:
        sys.path.insert(0, _TESTS_DIR)
    import fedq

    return fedq


class RustRuntime:
    """Adapter over `fedq.Runtime` for the DuckDB pushdown E2E suites.

    Constructed from a list of ``(datasource_name, duckdb_file_path)`` pairs. It
    writes a temporary YAML config declaring each pair as a DuckDB source, then
    builds the native runtime. The datasource names must match the qualifiers the
    test SQL uses (e.g. ``duckdb_primary.main.orders`` needs a source named
    ``duckdb_primary``).
    """

    def __init__(self, sources):
        """Write the config for the given (name, path) pairs and open the engine."""
        fedq = _import_fedq()
        self._config_path = _write_config(sources)
        self._runtime = fedq.Runtime(self._config_path)

    def execute(self, sql):
        """Run one SQL statement and return its result as a pyarrow.Table."""
        stream = self._runtime.execute(sql)
        return pa.table(stream)

    def explain_text(self, sql):
        """Return the full textual physical-plan dump for ``sql``."""
        table = self.execute("EXPLAIN " + sql)
        column = table.column("plan").to_pylist()
        lines = []
        for line in column:
            lines.append(line)
        return "\n".join(lines)

    def explain_queries(self, sql):
        """Parse the plan dump into (datasource_name, sql_text) remote queries.

        Only nodes that carry a rendered SQL string appear here: ``RemoteQuery``
        and ``Gather`` lines look like ``RemoteQuery [<ds>] :: <SQL>``. Structured
        source-leaf ``Scan`` nodes carry no rendered SQL and are not returned.
        """
        text = self.explain_text(sql)
        queries = []
        for raw in text.splitlines():
            parsed = _parse_remote_line(raw)
            if parsed is not None:
                queries.append(parsed)
        return queries


def _parse_remote_line(raw):
    """Parse one ``RemoteQuery [ds] :: sql`` / ``Gather [ds] :: sql`` plan line.

    Returns ``(datasource_name, sql_text)`` or ``None`` when the line is not a
    rendered-SQL remote node.
    """
    line = raw.strip()
    prefix = None
    if line.startswith("RemoteQuery ["):
        prefix = len("RemoteQuery [")
    if line.startswith("Gather ["):
        prefix = len("Gather [")
    if prefix is None:
        return None
    close = line.index("]", prefix)
    datasource = line[prefix:close]
    marker = line.index("::", close)
    sql_text = line[marker + 2 :].strip()
    return (datasource, sql_text)


def _write_config(sources):
    """Write a temp YAML config with one DuckDB datasource per (name, path) pair."""
    lines = ["datasources:"]
    for name, path in sources:
        lines.append("  " + name + ":")
        lines.append("    type: duckdb")
        lines.append("    path: " + path)
    text = "\n".join(lines) + "\n"
    handle = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    handle.write(text)
    handle.close()
    return handle.name


def assert_raises_engine_error(match=None):
    """Assert the engine raises a RuntimeError, optionally matching a substring.

    The Rust engine surfaces every stage failure (parse/bind/plan/exec) as a
    Python ``RuntimeError`` carrying the engine's own message. A test that pins
    "an invalid query must raise" uses this so the intent stays pinned regardless
    of the Python-side exception class the old engine used.
    """
    import pytest

    return pytest.raises(RuntimeError, match=match)
