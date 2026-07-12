"""Tests for the optimizer factory wiring and the EXPLAIN estimate surface.

The factory is the single place the rule stack is assembled, so the
OptimizerConfig flags are real everywhere; EXPLAIN prints each join's
estimated rows and marks estimates built on defaulted statistics.
"""

import pyarrow as pa
import pytest

from federated_query.catalog import Catalog
from federated_query.cli.fedq import FedQRuntime
from federated_query.config import Config
from federated_query.config.config import CostConfig, OptimizerConfig
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.optimizer import build_optimizer
from federated_query.plan.physical import (
    PhysicalNestedLoopJoin,
    PhysicalPlanNode,
    _PlanFormatter,
)
from federated_query.plan.logical import JoinType
from tests.duckdb_tmp import duckdb_path


def _rule_names(optimizer):
    """The registered rules' names in order."""
    names = []
    for rule in optimizer.rules:
        names.append(rule.name())
    return names


def test_factory_registers_join_ordering_after_pushdown():
    """PredicatePushdown, then SemiJoinPushdown (reduces the region join
    ordering will see), then JoinOrdering - in that order and before
    projection pruning."""
    optimizer = build_optimizer(Catalog(), OptimizerConfig(), CostConfig())
    names = _rule_names(optimizer)
    assert names.index("PredicatePushdown") < names.index("SemiJoinPushdown")
    assert names.index("SemiJoinPushdown") < names.index("JoinOrdering")
    assert names.index("JoinOrdering") < names.index("ProjectionPushdown")


def test_factory_flag_disables_join_ordering():
    """enable_join_reordering=False omits the rule - the flag is real."""
    config = OptimizerConfig(enable_join_reordering=False)
    optimizer = build_optimizer(Catalog(), config, CostConfig())
    assert "JoinOrdering" not in _rule_names(optimizer)


def test_factory_flag_disables_predicate_pushdown():
    """enable_predicate_pushdown=False omits that rule - no dead flags."""
    config = OptimizerConfig(enable_predicate_pushdown=False)
    optimizer = build_optimizer(Catalog(), config, CostConfig())
    assert "PredicatePushdown" not in _rule_names(optimizer)


class _Leaf(PhysicalPlanNode):
    """A minimal physical leaf for formatter tests."""

    def children(self):
        """A leaf has no children."""
        return []

    def schema(self):
        """A trivial one-column schema."""
        return pa.schema([pa.field("x", pa.int64())])

    def estimated_cost(self):
        """A fixed cost, irrelevant to the assertions."""
        return 1.0


def test_formatter_prints_estimated_rows_and_defaulted_stats():
    """A join carrying an estimate prints rows=<n>, and one whose estimate
    used defaults says so explicitly - never silently."""
    join = PhysicalNestedLoopJoin(
        left=_Leaf(), right=_Leaf(), join_type=JoinType.INNER, condition=None,
        estimated_rows=1234,
        estimate_defaults=["ndv(duck.main.t.k)"],
    )
    header = _PlanFormatter()._build_header(join)
    assert "rows=1234" in header
    assert "stats=defaulted[ndv(duck.main.t.k)]" in header


def test_formatter_unestimated_node_prints_minus_one():
    """A node with no estimate keeps the rows=-1 marker and no stats note."""
    join = PhysicalNestedLoopJoin(
        left=_Leaf(), right=_Leaf(), join_type=JoinType.INNER, condition=None,
    )
    header = _PlanFormatter()._build_header(join)
    assert "rows=-1" in header
    assert "stats=defaulted" not in header


def _two_source_runtime(config):
    """A runtime over two DuckDB sources: dims (part, supplier) and facts
    (lineitem), so their joins execute on the coordinator."""
    dims = DuckDBDataSource("dims", {"path": duckdb_path(), "read_only": False})
    dims.connect()
    dims.connection.execute("CREATE TABLE part (p_id INTEGER)")
    dims.connection.execute("INSERT INTO part VALUES (1), (2)")
    dims.connection.execute("CREATE TABLE supplier (s_id INTEGER)")
    dims.connection.execute("INSERT INTO supplier VALUES (1), (2)")
    facts = DuckDBDataSource("facts", {"path": duckdb_path(), "read_only": False})
    facts.connect()
    facts.connection.execute("CREATE TABLE lineitem (l_p INTEGER, l_s INTEGER)")
    facts.connection.execute("INSERT INTO lineitem VALUES (1, 1), (1, 2), (2, 1)")
    catalog = Catalog()
    catalog.register_datasource(dims)
    catalog.register_datasource(facts)
    catalog.load_metadata()
    return FedQRuntime(catalog, config)


_CROSS_PRODUCT_FROM_ORDER = (
    "SELECT count(*) AS n"
    " FROM dims.main.part AS p, dims.main.supplier AS s,"
    " facts.main.lineitem AS l"
    " WHERE p.p_id = l.l_p AND s.s_id = l.l_s"
)


def test_reordered_query_is_correct_end_to_end():
    """The q09-shape FROM order (part x supplier first) runs through the full
    pipeline with reordering on and returns the right answer."""
    runtime = _two_source_runtime(Config())
    result = runtime.execute(_CROSS_PRODUCT_FROM_ORDER)
    assert result.column("n").to_pylist() == [3]


def test_reordering_disabled_still_correct():
    """The same query with enable_join_reordering=false returns the same
    rows in the user's FROM order."""
    config = Config(optimizer=OptimizerConfig(enable_join_reordering=False))
    runtime = _two_source_runtime(config)
    result = runtime.execute(_CROSS_PRODUCT_FROM_ORDER)
    assert result.column("n").to_pylist() == [3]


def test_explain_shows_join_estimates():
    """EXPLAIN prints the reordered joins' estimated rows (not -1)."""
    runtime = _two_source_runtime(Config())
    document = runtime.explain(_CROSS_PRODUCT_FROM_ORDER)
    estimated = []
    for line in document["plan"]:
        if "Join" in line and "rows=" in line and "rows=-1" not in line:
            estimated.append(line)
    assert estimated, f"no estimated join line in plan: {document['plan']}"
