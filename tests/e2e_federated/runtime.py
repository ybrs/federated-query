"""The engine adapter for the federated suite: open an environment and query it.

An ``Environment`` is one placement of one case's tables. It OPENS what
``build_test_harness.sh`` already created and creates nothing at all - not the
datasources, and not the engine config either. Both are on disk before a run
starts, so the exact YAML the engine read and the exact data it read are there to
open and inspect after a failure, and a run costs no file writes.

Opening resolves the pre-built config path, checks the sources it names are
present, and lazily builds the native ``fedq.Runtime`` over it. Anything missing
raises naming the file and the command that builds it.
"""

import os

import pyarrow as pa

from tests.e2e_federated import harness
from tests.e2e_federated import placements as placement_mod

_TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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
    config_path = harness.config_path(placement.name, specs.keys())
    if not os.path.exists(config_path):
        raise RuntimeError(
            "test harness is incomplete, engine config is missing at "
            + config_path + "  --  build it with ./build_test_harness.sh"
        )
    qualified_map = _qualified_map(sources)
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
