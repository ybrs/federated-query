"""Pytest wiring for the federated e2e corpus: parametrization and caches.

The test is parametrized over every (case x placement) pair. Seeded environments
and their engine runtimes are cached at session scope keyed by
(frozenset-of-tables, placement name), so cases sharing a table set and placement
reuse one seeded environment and one runtime.

PostgreSQL reachability is checked once. When ``FEDQ_E2E_SKIP_PG=1`` the suite
does not connect and marks every PostgreSQL placement as an explicit (visible)
skip. Otherwise a live connection is required and a failure to connect raises
loudly rather than silently skipping.
"""

import os

import psycopg2
import pytest

from tests.e2e_federated.cases import all_cases
from tests.e2e_federated.placements import PLACEMENTS


class PgState:
    """Session PostgreSQL state: whether pg is skipped and the shared handle."""

    def __init__(self, skip_pg, connection):
        """Store the skip flag and the shared psycopg2 connection (or None)."""
        self.skip_pg = skip_pg
        self.connection = connection


def _skip_pg_requested():
    """Whether the FEDQ_E2E_SKIP_PG toggle is set to 1."""
    return os.environ.get("FEDQ_E2E_SKIP_PG") == "1"


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


def _reset_fed_schemas(connection):
    """Drop every leftover ``fed_*`` schema so a run starts from a clean slate."""
    cursor = connection.cursor()
    cursor.execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'fed_%'")
    rows = cursor.fetchall()
    for row in rows:
        cursor.execute("DROP SCHEMA IF EXISTS " + row[0] + " CASCADE")
    cursor.close()


@pytest.fixture(scope="session")
def pg_state():
    """Provide the session PostgreSQL state, raising loudly if pg is required."""
    if _skip_pg_requested():
        yield PgState(skip_pg=True, connection=None)
        return
    connection = _connect_pg()
    _reset_fed_schemas(connection)
    yield PgState(skip_pg=False, connection=connection)
    _reset_fed_schemas(connection)
    connection.close()


@pytest.fixture(scope="session")
def env_registry():
    """A session cache: (frozenset tables, placement name) -> Environment."""
    return {}


@pytest.fixture(scope="session")
def oracle_registry():
    """A session cache: frozenset of table names -> Oracle."""
    return {}


def _case_placement_params():
    """Return (argvalues, ids) for the full case x placement parametrization."""
    argvalues = []
    ids = []
    for case in all_cases():
        _extend_params_for_case(case, argvalues, ids)
    return argvalues, ids


def _extend_params_for_case(case, argvalues, ids):
    """Append one (case, placement) param per placement for a single case."""
    for placement in PLACEMENTS:
        argvalues.append((case, placement))
        ids.append(case["name"] + "[" + placement.name + "]")


def pytest_generate_tests(metafunc):
    """Parametrize any test taking ``case`` and ``placement`` over the matrix."""
    if "case" not in metafunc.fixturenames:
        return
    if "placement" not in metafunc.fixturenames:
        return
    argvalues, ids = _case_placement_params()
    metafunc.parametrize("case,placement", argvalues, ids=ids)
