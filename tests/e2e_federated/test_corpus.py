"""Run every corpus case under every distribution and check it against the oracle.

Every case is run under EVERY way its tables can be distributed across sources -
every partition of its tables into co-located groups, each group on a DuckDB, a
PostgreSQL, or a Parquet source. Nothing is sampled, nothing is skipped, and
there is no fixed list of arrangements somebody has to remember to extend.

There is no skip path in this file. A distribution a case does not have is never
generated, so it never appears as a test that did not run.

An ``expect_error`` case must raise under every distribution. Every other case
runs through the engine and is compared, value by value, against the
single-DuckDB oracle result, which is the same for every distribution and is
therefore computed once per case.
"""

import pytest

from tests.e2e_federated import runtime as runtime_mod
from tests.e2e_federated.cases import case_table_specs
from tests.e2e_federated.compare import compare_tables
from tests.e2e_federated.oracle import build_oracle


def test_corpus(case, variation, engine, oracle_registry):
    """Execute one corpus case under one distribution and assert correctness."""
    environment = runtime_mod.open_variation(engine, variation)
    if "expect_error" in case:
        _run_expect_error(environment, case)
    else:
        _run_and_compare(environment, case, variation, oracle_registry)


def _reference(oracle_registry, case):
    """Return the oracle result for a case, computing it once.

    The reference answer does not depend on where the tables live - that is the
    property the suite exists to check - so it is computed once per case rather
    than once per distribution.
    """
    key = case["name"]
    if key not in oracle_registry:
        oracle = build_oracle(case_table_specs(case))
        oracle_registry[key] = oracle.run(case["query"])
    return oracle_registry[key]


def _run_expect_error(environment, case):
    """Assert the engine raises a RuntimeError matching the case's substring."""
    with pytest.raises(RuntimeError, match=case["expect_error"]):
        environment.execute(case["query"])


def _run_and_compare(environment, case, variation, oracle_registry):
    """Run the query through the engine and compare it to the oracle result."""
    oracle_table = _reference(oracle_registry, case)
    engine_table = environment.execute(case["query"])
    order_sensitive = case.get("order_sensitive", False)
    compare_tables(engine_table, oracle_table, variation.name, order_sensitive)
