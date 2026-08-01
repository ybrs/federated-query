"""The engine adapter for the federated suite: one runtime, many placements.

An ``Environment`` is one placement of one case's tables. It builds NOTHING - not
data, not a config. Every datasource already exists and the single config already
declares them all, so a placement is only a choice of which three-part name each
table is read from.

That is why the whole suite shares ONE ``fedq.Runtime``. Every placement reads
the same datasources; only the names in the SQL differ. Opening an environment is
therefore free, and the engine's catalog is built once per session rather than
once per placement.
"""

import os

import pyarrow as pa

from tests.e2e_federated import harness

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


class Engine:
    """The one native runtime the suite queries, built lazily over the config."""

    def __init__(self, config_path):
        """Store the config path; the native runtime is built on first use."""
        self.config_path = config_path
        self._runtime = None

    def runtime(self):
        """Build (once) and return the native engine runtime for the config."""
        if self._runtime is None:
            fedq = _import_fedq()
            self._runtime = fedq.Runtime(self.config_path)
        return self._runtime

    def close(self):
        """Drop the native runtime so its PostgreSQL connections are released.

        The data is untouched: it was built by build_test_harness.sh and is only
        read here.
        """
        self._runtime = None


def open_engine():
    """Return the Engine over the pre-built config, raising if it is absent."""
    if not os.path.exists(harness.CONFIG_PATH):
        raise RuntimeError(
            "test harness is incomplete, engine config is missing at "
            + harness.CONFIG_PATH + "  --  build it with ./build_test_harness.sh"
        )
    return Engine(harness.CONFIG_PATH)


def _datasource_of(qualified_name):
    """Return the datasource part of a three-part engine name."""
    return qualified_name.split(".")[0]


class Environment:
    """One distribution of a case's tables, reading the shared engine.

    ``qualified_map`` maps each table to the three-part name of the instance this
    distribution reads it from; ``source_count`` is how many distinct datasources
    that spans.
    """

    def __init__(self, engine, qualified_map):
        """Store the shared engine and the table -> qualified-name map."""
        self.engine = engine
        self.qualified_map = qualified_map
        datasources = set()
        for name in qualified_map.values():
            datasources.add(_datasource_of(name))
        self.source_count = len(datasources)

    def render(self, query):
        """Fill a case query's ``{table}`` placeholders with qualified names."""
        return query.format(**self.qualified_map)

    def execute(self, query):
        """Run a rendered case query through the engine, returning a pyarrow.Table."""
        rendered = self.render(query)
        return pa.table(self.engine.runtime().execute(rendered))


def open_variation(engine, variation):
    """Return the Environment for one distribution of a case's tables.

    Creates nothing and opens nothing: the datasources already exist and the
    engine is already built, so this only decides which name each table is read
    from. That is why a distribution costs nothing to add.
    """
    return Environment(engine, variation.qualified_map())
