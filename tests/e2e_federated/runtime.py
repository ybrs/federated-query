"""Config building and the engine adapter for the federated suite.

An ``Environment`` is one placement of one case's tables. It OPENS the datasources
``build_test_harness.sh`` already created - it never creates them. Opening writes
the temp YAML config declaring those datasources and lazily builds the native
``fedq.Runtime``; it also holds the table -> fully-qualified-name map the harness
substitutes into the case query.

Nothing here seeds data, and there is no cache of seeded environments. A source
the tests need is either present, in which case every test reading it sees the
same bytes, or absent, in which case opening raises and names it. Data that a
test creates for itself is data whose cost and content nobody can audit.

The suite writes its own config builder here rather than extending the DuckDB-only
``tests/rust_runtime.py`` writer, because this suite mixes DuckDB, Parquet, and
PostgreSQL datasources in one config, which that writer cannot express.
"""

import os
import tempfile

import pyarrow as pa

from tests.e2e_federated import harness
from tests.e2e_federated import placements as placement_mod

_TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DUCK_SCHEMA = "main"
PARQUET_SCHEMA = "main"


def _import_fedq():
    """Import the native ``fedq`` module, putting ``tests/`` on ``sys.path``."""
    so_path = os.path.join(_TESTS_DIR, "fedq.so")
    if not os.path.exists(so_path):
        raise ImportError("tests/fedq.so is missing; build the engine first")
    import sys

    if _TESTS_DIR not in sys.path:
        sys.path.insert(0, _TESTS_DIR)
    import fedq

    return fedq


def adbc_driver_path():
    """Return the ADBC PostgreSQL driver shipped with the installed package.

    The location is derived from the imported ``adbc_driver_postgresql``, so it
    follows whichever interpreter is running the suite; no venv path or Python
    version is written down. A missing package raises ImportError and a package
    without its library raises here; neither is skipped.
    """
    import adbc_driver_postgresql

    package_dir = os.path.dirname(adbc_driver_postgresql.__file__)
    driver = os.path.join(package_dir, "libadbc_driver_postgresql.so")
    if not os.path.exists(driver):
        raise RuntimeError("adbc_driver_postgresql has no driver library at " + driver)
    return driver


class Environment:
    """One placement of a case's tables plus a lazily built engine runtime.

    ``sources`` are the ``RequiredSource``s this placement reads, ``qualified_map``
    maps each table to its three-part engine name, and ``source_count`` is the
    number of distinct datasources.
    """

    def __init__(self, sources, qualified_map, config_path):
        """Store the built sources, qualified-name map, and config file path."""
        self.sources = sources
        self.qualified_map = qualified_map
        self.config_path = config_path
        self.source_count = len(sources)
        self._runtime = None

    def render(self, query):
        """Fill a case query's ``{table}`` placeholders with qualified names."""
        return query.format(**self.qualified_map)

    def runtime(self):
        """Build (once) and return the native engine runtime for this config."""
        if self._runtime is None:
            fedq = _import_fedq()
            self._runtime = fedq.Runtime(self.config_path)
        return self._runtime

    def execute(self, query):
        """Run a rendered case query through the engine, returning a pyarrow.Table."""
        rendered = self.render(query)
        return pa.table(self.runtime().execute(rendered))

    def close(self):
        """Release the engine runtime so its session's connections are freed.

        Dropping the only reference to the native runtime runs its Rust Drop,
        which prunes the session from the exec-plane registry and closes the
        PostgreSQL connections it opened. The data outlives this: it was built
        once by build_test_harness.sh and is only read here.
        """
        self._runtime = None


def open_environment(placement, specs):
    """Open the pre-built datasources for one placement of a case's tables.

    Creates nothing. Every source this placement reads must already exist, built
    by ``build_test_harness.sh``; a missing one raises here naming the source and
    the command that builds it, because a test that quietly seeded its own data
    would be asserting against data no other test and no reviewer ever sees.
    """
    sources = harness.sources_for(placement, specs)
    _require_present(sources)
    qualified_map = _qualified_map(sources)
    config_path = _write_config(sources)
    return Environment(sources, qualified_map, config_path)


def _require_present(sources):
    """Raise naming every source of this environment that has not been built."""
    missing = []
    for source in sources:
        if not _source_exists(source):
            missing.append(source.name + " (" + source.kind + ") at " + source.location)
    if missing:
        raise RuntimeError(
            "test harness is incomplete, " + str(len(missing))
            + " datasource(s) missing: " + "; ".join(missing)
            + "  --  build them with ./build_test_harness.sh"
        )


def _source_exists(source):
    """Whether one datasource's data is present, checked per kind."""
    if source.kind == placement_mod.DUCK:
        return os.path.exists(source.location)
    if source.kind == placement_mod.PARQUET:
        return os.path.isdir(source.location)
    return _pg_schema_exists(source.schema)


def _pg_schema_exists(schema):
    """Whether a PostgreSQL schema exists in the suite's test database."""
    import psycopg2

    connection = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=harness.pg_database(),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )
    cursor = connection.cursor()
    cursor.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
        (schema,),
    )
    found = cursor.fetchone() is not None
    cursor.close()
    connection.close()
    return found


def _qualified_map(sources):
    """Return a table -> three-part-name map spanning every source read."""
    mapping = {}
    for source in sources:
        for table in source.table_names():
            mapping[table] = source.qualified(table)
    return mapping


def _write_config(sources):
    """Write the temp YAML config declaring every built datasource; return path."""
    lines = ["datasources:"]
    for source in sources:
        _append_source_block(lines, source)
    text = "\n".join(lines) + "\n"
    path = _temp_path(".yaml")
    handle = open(path, "w")
    handle.write(text)
    handle.close()
    return path


def _append_source_block(lines, source):
    """Append one datasource's YAML block for its kind to the config lines."""
    lines.append("  " + source.name + ":")
    if source.kind == placement_mod.DUCK:
        _append_duck_block(lines, source)
    elif source.kind == placement_mod.PARQUET:
        _append_parquet_block(lines, source)
    else:
        _append_pg_block(lines, source)


def _append_duck_block(lines, source):
    """Append a DuckDB datasource block (type + file path)."""
    lines.append("    type: duckdb")
    lines.append("    path: " + source.location)


def _append_parquet_block(lines, source):
    """Append a Parquet datasource block (type + directory)."""
    lines.append("    type: parquet")
    lines.append("    dir: " + source.location)


def _append_pg_block(lines, source):
    """Append a PostgreSQL datasource block (connection + schema + adbc driver)."""
    lines.append("    type: postgres")
    lines.append("    host: localhost")
    lines.append("    port: 5432")
    lines.append("    user: postgres")
    lines.append("    password: postgres")
    lines.append("    database: " + _pg_database())
    lines.append("    schemas:")
    lines.append("      - " + source.schema)
    lines.append("    adbc_driver: " + adbc_driver_path())


def _pg_database():
    """Return the PostgreSQL database name for the suite (env-overridable)."""
    return os.environ.get("POSTGRES_DB", "test_db")


def _temp_path(suffix):
    """Return a fresh, non-existent temp path (DuckDB rejects a pre-made file)."""
    directory = tempfile.mkdtemp(prefix="fedq_e2e_")
    return os.path.join(directory, "source" + suffix)
