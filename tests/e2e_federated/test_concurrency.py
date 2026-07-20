"""Concurrency tests for the session-scoped data plane, driven from Python.

Two properties are pinned:

- Many independent runtimes (each its own session) over ONE shared seeded
  environment, driven from their own threads at the same time, every session
  producing oracle-equal results. This exercises the process-wide session-keyed
  connector maps, the shared prefetch/ctid worker pools, and the concurrent
  learned-stats writes, all under real PostgreSQL + DuckDB sources.

- The single-Runtime-two-threads contract established in the concurrency review:
  ``fedq.Runtime.execute`` takes ``&self`` and releases the GIL, so two Python
  threads can drive ONE runtime at once. The engine keeps each driving thread's
  connection caches thread-local and guards every shared map with a mutex, so
  this is safe; the test pins that safety.

The runtimes are CONSTRUCTED sequentially (not concurrently): concurrent
construction over one config races on the shared learned-stats SQLite open (a
separate finding), while concurrent EXECUTION is safe. Sizes are tunable via
FEDQ_CONC_THREADS and FEDQ_CONC_ITERS with small defaults so the default suite
stays fast.
"""

import os
import threading

import pyarrow as pa
import pytest

from tests.e2e_federated import placements as placement_mod
from tests.e2e_federated import runtime as runtime_mod
from tests.e2e_federated.cases import all_cases, case_table_specs
from tests.e2e_federated.compare import compare_tables
from tests.e2e_federated.oracle import build_oracle

_MIX_CASE_NAMES = ("inner_join", "left_join", "right_join", "full_join")


def _threads():
    """Return the worker-thread count from FEDQ_CONC_THREADS (default 6)."""
    return int(os.environ.get("FEDQ_CONC_THREADS", "6"))


def _iterations():
    """Return the per-thread query-mix repeat count from FEDQ_CONC_ITERS."""
    return int(os.environ.get("FEDQ_CONC_ITERS", "3"))


def _pick_placement(pg_state):
    """Return a federated pg+duck placement, or duck+duck when pg is skipped."""
    name = "duck_duck" if pg_state.skip_pg else "pg_duck"
    for placement in placement_mod.PLACEMENTS:
        if placement.name == name:
            return placement
    raise RuntimeError("placement '" + name + "' not found")


def _mix_cases():
    """Return the join cases used as the concurrent query mix, in a fixed order."""
    by_name = {}
    for case in all_cases():
        if case["name"] in _MIX_CASE_NAMES:
            by_name[case["name"]] = case
    cases = []
    for name in _MIX_CASE_NAMES:
        cases.append(by_name[name])
    return cases


def _union_specs(cases):
    """Return the merged name -> TableSpec map spanning every case's tables."""
    specs = {}
    for case in cases:
        for name, spec in case_table_specs(case).items():
            specs[name] = spec
    return specs


class ConcurrencyFixture:
    """One seeded environment plus the rendered query mix and oracle answers.

    ``config_path`` builds a fresh runtime; ``engine_queries`` are the fully
    qualified engine SQL strings; ``oracle_tables`` are their reference results
    (computed once in the main thread) that every worker compares against.
    """

    def __init__(self, environment, engine_queries, oracle_tables):
        """Store the environment, the engine queries, and the oracle tables."""
        self.environment = environment
        self.engine_queries = engine_queries
        self.oracle_tables = oracle_tables

    def config_path(self):
        """Return the shared config path every worker builds its runtime from."""
        return self.environment.config_path


def _oracle_answers(oracle, cases):
    """Return the reference pyarrow table for each case's query, in order."""
    tables = []
    for case in cases:
        tables.append(oracle.run(case["query"]))
    return tables


def _engine_queries(environment, cases):
    """Return each case's engine SQL with the placement's qualified names filled."""
    queries = []
    for case in cases:
        queries.append(environment.render(case["query"]))
    return queries


@pytest.fixture(scope="module")
def concurrency_fixture(pg_state):
    """Seed one environment for the join mix and yield the concurrency fixture."""
    cases = _mix_cases()
    specs = _union_specs(cases)
    placement = _pick_placement(pg_state)
    environment = runtime_mod.build_environment(placement, specs, pg_state.connection)
    oracle = build_oracle(specs)
    fixture = ConcurrencyFixture(
        environment, _engine_queries(environment, cases), _oracle_answers(oracle, cases)
    )
    yield fixture
    environment.close()


def _run_query(runtime, sql):
    """Execute one SQL string on a runtime and return its pyarrow.Table."""
    return pa.table(runtime.execute(sql))


def _check_mix(fixture, runtime, label):
    """Run the whole query mix once on ``runtime`` and compare each to the oracle."""
    for index, sql in enumerate(fixture.engine_queries):
        engine_table = _run_query(runtime, sql)
        compare_tables(engine_table, fixture.oracle_tables[index], label, False)


def _spawn(worker, count):
    """Run ``worker(index)`` on ``count`` threads and return recorded failures.

    A worker's exception (an assertion mismatch or an engine error) is captured
    per thread and returned so the main thread can fail loudly with it; letting
    it escape the worker would otherwise be lost at the thread boundary.
    """
    failures = []
    lock = threading.Lock()
    threads = []
    for index in range(count):
        threads.append(threading.Thread(target=_guard, args=(worker, index, failures, lock)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return failures


def _guard(worker, index, failures, lock):
    """Run one worker, recording any exception it raises under the lock."""
    try:
        worker(index)
    except Exception as exc:  # surface cross-thread failures to the main thread
        with lock:
            failures.append("thread " + str(index) + ": " + repr(exc))


def test_concurrent_own_runtimes_stay_correct(concurrency_fixture):
    """Each thread drives its OWN runtime over the shared environment correctly."""
    count = _threads()
    iterations = _iterations()
    runtimes = _build_runtimes(concurrency_fixture.config_path(), count)

    def worker(index):
        """Repeatedly run the query mix on this thread's own runtime."""
        for _ in range(iterations):
            _check_mix(concurrency_fixture, runtimes[index], "own_runtime")

    failures = _spawn(worker, count)
    assert not failures, "concurrent own-runtime failures: " + "; ".join(failures)


def _build_runtimes(config_path, count):
    """Build ``count`` runtimes over one config sequentially (open is not concurrent)."""
    fedq = runtime_mod._import_fedq()
    runtimes = []
    for _ in range(count):
        runtimes.append(fedq.Runtime(config_path))
    return runtimes


def test_single_runtime_two_threads_is_safe(concurrency_fixture):
    """Pin the contract: two threads may drive ONE runtime (GIL-released execute).

    This is safe because per-thread connection caches are thread-local and every
    shared data-plane map is mutex-guarded, so both threads must produce
    oracle-equal results with no error or crash.
    """
    iterations = _iterations()
    fedq = runtime_mod._import_fedq()
    shared = fedq.Runtime(concurrency_fixture.config_path())

    def worker(_index):
        """Drive the shared runtime through the query mix repeatedly."""
        for _ in range(iterations):
            _check_mix(concurrency_fixture, shared, "shared_runtime")

    failures = _spawn(worker, 2)
    assert not failures, "single-runtime two-thread failures: " + "; ".join(failures)
