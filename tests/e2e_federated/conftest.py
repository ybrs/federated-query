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
from tests.e2e_federated import known_failures
from tests.e2e_federated import runtime as runtime_mod
from tests.e2e_federated.cases import all_cases
from tests.e2e_federated.cases import case_table_specs


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
    """The one native runtime the whole suite queries, planning without learned stats.

    Every placement reads the same datasources and differs only in the names its
    SQL uses, so one runtime serves them all. The engine's catalog is therefore
    built once per session instead of once per placement, and the suite holds one
    set of PostgreSQL connections rather than one per placement.

    Learned statistics are turned OFF for this suite. The engine reads them on
    every plan, so with them on a query plans according to what the engine has
    executed before it and a verdict depends on the order tests happened to run
    in - six of this corpus's failures disappear that way. Off, a query plans the
    same however many queries preceded it, so a result is a property of the query.
    Exercising the learning loop is a separate, deliberate pass over a warmed
    catalog, not something this suite should inherit by accident.
    """
    built = runtime_mod.open_engine()
    built.runtime().execute("SET optimizer.enable_learned_statistics = false")
    yield built
    built.close()


@pytest.fixture(scope="session")
def oracle_registry():
    """A session cache: frozenset of table names -> Oracle."""
    return {}


def _case_variation_params():
    """Return (argvalues, ids) for every case under every distribution it has.

    Only distributions a case actually has are generated, so no test in this
    suite is ever skipped: an arrangement that does not exist is absent from the
    run rather than present and not executed.
    """
    argvalues = []
    ids = []
    for case in all_cases():
        specs = case_table_specs(case)
        for variation in harness.variations(specs, case.get("min_sources", 1)):
            argvalues.append(_param(case, variation))
            ids.append(case["name"] + "[" + variation.name + "]")
    return argvalues, ids


def _param(case, variation):
    """Return the parametrize entry, marked xfail when it is a known failure.

    A known failure is marked, never removed: the test still runs and its
    failure is still reported, so a defect stays exercised instead of vanishing
    from the suite.
    """
    reason = known_failures.lookup(case["name"], variation.name)
    if reason is None:
        return (case, variation)
    return pytest.param(
        case, variation, marks=pytest.mark.xfail(reason=reason, strict=True)
    )


def pytest_generate_tests(metafunc):
    """Parametrize any test taking ``case`` and ``variation`` over the matrix."""
    if "case" not in metafunc.fixturenames:
        return
    if "variation" not in metafunc.fixturenames:
        return
    argvalues, ids = _case_variation_params()
    metafunc.parametrize("case,variation", argvalues, ids=ids)


def pytest_sessionfinish(session, exitstatus):
    """Fail the run if any test was skipped.

    A skip in this suite means a check silently did not happen while the run
    still reported success. There is no legitimate skip here: a distribution a
    case does not have is not generated, and a missing dependency raises.
    """
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is None:
        return
    skipped = reporter.stats.get("skipped", [])
    if not skipped:
        return
    reporter.write_line("")
    reporter.write_line(
        "SKIPS ARE NOT ALLOWED: %d test(s) were skipped" % len(skipped), red=True
    )
    for report in skipped[:20]:
        reporter.write_line("  " + str(report.nodeid), red=True)
    session.exitstatus = 1
