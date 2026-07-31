"""Pytest wiring for the federated e2e corpus: parametrization and caches.

The test is parametrized over every (case x placement) pair. The datasources
themselves are built once by ``build_test_harness.sh`` and only read here; what
this session holds is engine runtimes, keyed by (frozenset-of-tables, placement
name), so cases reading the same sources under the same placement share one
runtime instead of opening a second connection set for identical data.

PostgreSQL is required, unconditionally. There is no toggle that runs the suite
without it: a live connection is opened once per session and a failure to connect
raises loudly, because a run that quietly drops every PostgreSQL placement still
reports "passed" while having tested none of the federation this suite exists to
cover.
"""

import os

import psycopg2
import pytest

from tests.e2e_federated.cases import all_cases
from tests.e2e_federated.cases import case_table_specs
from tests.e2e_federated.placements import PLACEMENTS


def _connect_pg():
    """Open the shared autocommit PostgreSQL connection from the environment."""
    connection = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "test_db"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )
    connection.autocommit = True
    return connection


@pytest.fixture(scope="session")
def pg_connection():
    """Provide the shared PostgreSQL connection, raising if pg is unreachable.

    This connection READS the harness. It must never drop a ``fed_%`` schema:
    those hold the pre-built data every test in the suite depends on, so dropping
    one leaves every later run asserting against data that is not there. Only
    build_test_harness.sh removes harness data.
    """
    connection = _connect_pg()
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def open_environments():
    """Track every environment opened this session and close them at teardown.

    This holds engine runtimes, not data. Closing one drops its native runtime so
    the session's PostgreSQL connections are released; the datasources it read
    are untouched, because the suite never creates them.
    """
    opened = {}
    yield opened
    for environment in opened.values():
        environment.close()


@pytest.fixture(scope="session")
def oracle_registry():
    """A session cache: frozenset of table names -> Oracle."""
    return {}


def _case_placement_params():
    """Return (argvalues, ids) for the full case x placement parametrization.

    Ordered so that every test reading one (table set, placement) environment runs
    consecutively. That ordering is what keeps the suite's resource use flat: an
    environment is opened once, used by all its tests, and closed before the next
    one opens, so at most a couple of engine runtimes - and therefore a handful of
    PostgreSQL connections - are ever live. Case-major order would instead force
    every runtime to stay alive all run to be reused at all, which is hundreds of
    connections held for no reason.
    """
    ordered = []
    for case in all_cases():
        for placement in PLACEMENTS:
            ordered.append((_env_sort_key(case, placement), case, placement))
    ordered.sort(key=lambda entry: entry[0])
    argvalues = []
    ids = []
    for _key, case, placement in ordered:
        argvalues.append((case, placement))
        ids.append(case["name"] + "[" + placement.name + "]")
    return argvalues, ids


def _env_sort_key(case, placement):
    """Return the sort key grouping tests by the environment they read."""
    specs = case_table_specs(case)
    return (",".join(sorted(specs.keys())), placement.name, case["name"])


def pytest_generate_tests(metafunc):
    """Parametrize any test taking ``case`` and ``placement`` over the matrix."""
    if "case" not in metafunc.fixturenames:
        return
    if "placement" not in metafunc.fixturenames:
        return
    argvalues, ids = _case_placement_params()
    metafunc.parametrize("case,placement", argvalues, ids=ids)
