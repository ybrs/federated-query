"""Run every corpus case under every placement and check it against the oracle.

For each (case, placement): a cell is skipped only when the combination does not
exist - the placement needs more tables than the case has, or it spreads the
case's tables over fewer than the case's ``min_sources`` distinct sources. Those
are properties of the matrix, not switches; nothing a run can be configured with
removes a case. An ``expect_error`` case must raise in every placement it has.
Every other case runs through the engine and is compared, value by value, to the
single-DuckDB oracle result.
"""

import pytest

from tests.e2e_federated import runtime as runtime_mod
from tests.e2e_federated.cases import case_table_specs
from tests.e2e_federated.compare import compare_tables
from tests.e2e_federated.oracle import build_oracle


def test_corpus(case, placement, engine, oracle_registry):
    """Execute one corpus case under one placement and assert correctness."""
    specs = case_table_specs(case)
    _skip_absent_cell(case, placement, specs)
    environment = runtime_mod.open_environment(engine, placement, specs)
    if "expect_error" in case:
        _run_expect_error(environment, case)
    else:
        _run_and_compare(environment, case, placement, oracle_registry, specs)


def _skip_absent_cell(case, placement, specs):
    """Skip visibly when this (case, placement) cell does not exist.

    A placement that needs more tables than the case declares, or that lands the
    case's tables on fewer sources than it requires, has nothing to assert.
    """
    if len(specs) < placement.min_tables:
        pytest.skip(
            "placement needs >= " + str(placement.min_tables) + " tables"
        )
    used_slots, _mapping = placement.assign(specs.keys())
    min_sources = case.get("min_sources", 1)
    if len(used_slots) < min_sources:
        pytest.skip("placement yields " + str(len(used_slots)) + " source(s)")


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
