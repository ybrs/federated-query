"""Pytest wiring for the federated e2e corpus: parametrization and caches.

The test is parametrized over every (case x placement) pair. The datasources are
built once by ``build_test_harness.sh`` and only read here, and every placement
reads the same ones - a placement is a choice of names in the SQL - so the whole
session shares ONE engine runtime and one set of PostgreSQL connections.

PostgreSQL is required, unconditionally. There is no toggle that runs the suite
without it: a live connection is opened once per session and a failure to connect
raises loudly, because a run that quietly drops every PostgreSQL placement still
reports "passed" while having tested none of the federation this suite exists to
cover.
"""

import os
import shutil

import psycopg2
import pytest

from tests.e2e_federated import harness
from tests.e2e_federated import runtime as runtime_mod
from tests.e2e_federated.cases import all_cases
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


@pytest.fixture(scope="session", autouse=True)
def fresh_engine_state():
    """Clear the engine's learned state so every run starts from the same place.

    The engine writes its learned-statistics catalog and materialized-view chunks
    beside the config it was launched from, and the configs are pre-built at a
    stable location, so without this a run would plan using statistics learned by
    the previous one. That makes a result depend on how many times the suite has
    been run before, which is not a result. Testing the learning loop is a second
    pass over a warmed harness, not the default.

    This removes engine state only. The datasources and configs are untouched.
    """
    removed = 0
    for path in harness.engine_state_paths():
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        removed += 1
    return removed


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
def engine(fresh_engine_state):
    """The one native runtime the whole suite queries.

    Every placement reads the same datasources and differs only in the names its
    SQL uses, so one runtime serves them all. The engine's catalog is therefore
    built once per session instead of once per placement, and the suite holds one
    set of PostgreSQL connections rather than one per placement.
    """
    built = runtime_mod.open_engine()
    yield built
    built.close()


@pytest.fixture(scope="session")
def oracle_registry():
    """A session cache: frozenset of table names -> Oracle."""
    return {}


def _case_placement_params():
    """Return (argvalues, ids) for the full case x placement parametrization.

    Order carries no resource meaning: every test reads the same datasources
    through the same runtime, so no ordering opens or closes anything.
    """
    argvalues = []
    ids = []
    for case in all_cases():
        for placement in PLACEMENTS:
            argvalues.append((case, placement))
            ids.append(case["name"] + "[" + placement.name + "]")
    return argvalues, ids


def pytest_generate_tests(metafunc):
    """Parametrize any test taking ``case`` and ``placement`` over the matrix."""
    if "case" not in metafunc.fixturenames:
        return
    if "placement" not in metafunc.fixturenames:
        return
    argvalues, ids = _case_placement_params()
    metafunc.parametrize("case,placement", argvalues, ids=ids)
