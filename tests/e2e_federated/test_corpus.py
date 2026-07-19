"""Run every corpus case under every placement and check it against the oracle.

For each (case, placement): skip explicitly when the placement uses PostgreSQL
and pg is toggled off, or when the placement spreads the case's tables over fewer
than the case's ``min_sources`` distinct sources. An ``expect_error`` case must
raise in every non-skipped placement. Every other case runs through the engine
and is compared, value by value, to the single-DuckDB oracle result.
"""

import pytest

from tests.e2e_federated import runtime as runtime_mod
from tests.e2e_federated.cases import case_table_specs
from tests.e2e_federated.compare import compare_tables
from tests.e2e_federated.oracle import build_oracle
from tests.e2e_federated.placements import placement_uses_postgres


def test_corpus(case, placement, pg_state, env_registry, oracle_registry):
    """Execute one corpus case under one placement and assert correctness."""
    specs = case_table_specs(case)
    _maybe_skip(case, placement, specs, pg_state)
    environment = _get_env(env_registry, placement, specs, pg_state)
    if "expect_error" in case:
        _run_expect_error(environment, case)
    else:
        _run_and_compare(environment, case, placement, oracle_registry, specs)


def _maybe_skip(case, placement, specs, pg_state):
    """Skip visibly for a toggled-off pg placement or an unmet min_sources."""
    used_slots, _mapping = placement.assign(specs.keys())
    if pg_state.skip_pg and placement_uses_postgres(placement, used_slots):
        pytest.skip("FEDQ_E2E_SKIP_PG=1: PostgreSQL placement skipped")
    min_sources = case.get("min_sources", 1)
    if len(used_slots) < min_sources:
        pytest.skip("placement yields " + str(len(used_slots)) + " source(s)")


def _get_env(env_registry, placement, specs, pg_state):
    """Return the cached Environment for (tables, placement), building it once."""
    key = (frozenset(specs.keys()), placement.name)
    if key not in env_registry:
        env_registry[key] = runtime_mod.build_environment(
            placement, specs, pg_state.connection
        )
    return env_registry[key]


def _get_oracle(oracle_registry, specs):
    """Return the cached single-DuckDB Oracle for a case's table set."""
    key = frozenset(specs.keys())
    if key not in oracle_registry:
        oracle_registry[key] = build_oracle(specs)
    return oracle_registry[key]


def _run_expect_error(environment, case):
    """Assert the engine raises a RuntimeError matching the case's substring."""
    with pytest.raises(RuntimeError, match=case["expect_error"]):
        environment.execute(case["query"])


def _run_and_compare(environment, case, placement, oracle_registry, specs):
    """Run the query through the engine and compare it to the oracle result."""
    oracle = _get_oracle(oracle_registry, specs)
    engine_table = environment.execute(case["query"])
    oracle_table = oracle.run(case["query"])
    order_sensitive = case.get("order_sensitive", False)
    compare_tables(engine_table, oracle_table, placement.name, order_sensitive)
