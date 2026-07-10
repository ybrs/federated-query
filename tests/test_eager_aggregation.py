"""Eager aggregation: the split's correctness-critical gates and the rewrite
shape, over hand-fed statistics (no live sources)."""

import pytest

from federated_query.catalog.catalog import Catalog
from federated_query.config.config import CostConfig
from federated_query.optimizer.cost import CostModel
from federated_query.optimizer.eager_aggregation import EagerAggregationRule
from federated_query.optimizer.statistics import StatisticsCollector
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
    FunctionCall,
    Literal,
)
from federated_query.plan.logical import (
    Aggregate,
    Join,
    JoinType,
    Scan,
    SubqueryScan,
)
from federated_query.datasources.base import ColumnStatistics, TableStatistics


class _StatsSource:
    """A fake datasource serving canned TableStatistics by (schema, table)."""

    def __init__(self, stats_by_table):
        """Hold the canned statistics."""
        self.stats_by_table = stats_by_table

    def get_table_statistics(self, schema, table, columns):
        """Serve the canned statistics regardless of requested columns."""
        return self.stats_by_table.get((schema, table))


def _stats(rows, ndvs):
    """TableStatistics with the given row count and per-column NDVs."""
    columns = {}
    for name, ndv in ndvs.items():
        columns[name] = ColumnStatistics(
            num_distinct=ndv, null_fraction=0.0, avg_width=8
        )
    return TableStatistics(
        row_count=rows, total_size_bytes=rows * 8, column_stats=columns
    )


def _rule(extra_sources=None):
    """An EagerAggregationRule over a two-source fake catalog: a big fact on
    'duck' and small dims on 'pg' (multi-source, so the gate can fire)."""
    catalog = Catalog()
    catalog.datasources["duck"] = _StatsSource({
        ("main", "fact"): _stats(1_000_000, {"f_cust": 50_000, "f_date": 400}),
    })
    catalog.datasources["pg"] = _StatsSource({
        ("public", "cust"): _stats(50_000, {"c_sk": 50_000, "c_name": 40_000}),
        ("public", "dates"): _stats(400, {"d_sk": 400, "d_year": 3}),
    })
    collector = StatisticsCollector(catalog)
    config = CostConfig(cpu_tuple_cost=0.01, io_page_cost=1.0,
                        network_byte_cost=0.0001, network_rtt_ms=10.0)
    return EagerAggregationRule(CostModel(config, collector))


def _col(table, column):
    """A qualified integer column reference."""
    return ColumnRef(table=table, column=column, data_type=DataType.INTEGER)


def _eq(left, right):
    """A plain column equality conjunct."""
    return BinaryOp(op=BinaryOpType.EQ, left=left, right=right)


def _plan():
    """cust JOIN (fact JOIN dates): SUM(f_amount) grouped by customer name
    and year - the q04 family's shape in miniature."""
    fact = Scan(datasource="duck", schema_name="main", table_name="fact",
                columns=["f_cust", "f_date", "f_amount"])
    dates = Scan(datasource="pg", schema_name="public", table_name="dates",
                 columns=["d_sk", "d_year"])
    cust = Scan(datasource="pg", schema_name="public", table_name="cust",
                columns=["c_sk", "c_name"])
    inner = Join(left=fact, right=dates, join_type=JoinType.INNER,
                 condition=_eq(_col("fact", "f_date"), _col("dates", "d_sk")))
    outer = Join(left=cust, right=inner, join_type=JoinType.INNER,
                 condition=_eq(_col("cust", "c_sk"), _col("fact", "f_cust")))
    total = FunctionCall(function_name="SUM", is_aggregate=True,
                         args=[_col("fact", "f_amount")])
    return Aggregate(
        input=outer,
        group_by=[_col("cust", "c_name"), _col("dates", "d_year")],
        aggregates=[_col("cust", "c_name"), _col("dates", "d_year"),
                    Literal(value="tag", data_type=DataType.VARCHAR), total],
        output_names=["c_name", "d_year", "tag", "total"],
    )


def _find(node, kind):
    """The first node of a type in the tree, or None."""
    if isinstance(node, kind):
        return node
    for child in node.children():
        found = _find(child, kind)
        if found is not None:
            return found
    return None


def test_rewrites_the_q04_shape():
    """The customer peels: the rewritten tree carries an __eager partial whose
    group keys are the fact join key + the surviving year key, a final
    aggregate with the ORIGINAL outputs, and the constant tag untouched."""
    rule = _rule()
    result = rule.apply(_plan())
    assert result is not None
    final = result
    assert isinstance(final, Aggregate)
    assert final.output_names == ["c_name", "d_year", "tag", "total"]
    partial_scan = _find(final, SubqueryScan)
    assert partial_scan is not None and partial_scan.alias.startswith("__eager_")
    partial = partial_scan.input
    key_names = sorted(ref.column for ref in partial.group_by)
    assert key_names == ["d_year", "f_cust"]
    # The merge SUM reads the partial's synthetic column via the alias.
    merged = final.aggregates[-1]
    assert merged.function_name == "SUM"
    assert merged.args[0].table == partial_scan.alias


def test_rewrite_is_idempotent():
    """Applying the rule to its own output changes nothing (the fixpoint
    terminates): the final aggregate's SUM reads an __eager column."""
    rule = _rule()
    once = rule.apply(_plan())
    assert once is not None
    assert rule.apply(once) is None


def test_single_source_declines():
    """A single-source tree pushes whole; the split would be pure overhead."""
    rule = _rule()
    plan = _plan()

    def _rehome(node):
        if isinstance(node, Scan):
            return node.model_copy(update={"datasource": "duck"})
        rebuilt = []
        for child in node.children():
            rebuilt.append(_rehome(child))
        return node.with_children(rebuilt)

    assert rule.apply(_rehome(plan)) is None


def test_distinct_aggregate_declines():
    """SUM(DISTINCT x) cannot pre-aggregate; the rule must decline."""
    rule = _rule()
    plan = _plan()
    distinct = plan.aggregates[-1].model_copy(update={"distinct": True})
    plan = plan.model_copy(
        update={"aggregates": plan.aggregates[:-1] + [distinct]}
    )
    assert rule.apply(plan) is None


def test_grouping_sets_decline():
    """ROLLUP/CUBE aggregates never split."""
    rule = _rule()
    plan = _plan()
    plan = plan.model_copy(update={"grouping_sets": [plan.group_by, []]})
    assert rule.apply(plan) is None


def test_outer_join_declines():
    """A LEFT join above the fact changes drop semantics; decline."""
    rule = _rule()
    plan = _plan()
    outer = plan.input.model_copy(update={"join_type": JoinType.LEFT})
    plan = plan.model_copy(update={"input": outer})
    assert rule.apply(plan) is None


def test_kill_switch(monkeypatch):
    """FEDQ_EAGER_AGG=0 disables the rule entirely."""
    monkeypatch.setenv("FEDQ_EAGER_AGG", "0")
    rule = _rule()
    assert rule.apply(_plan()) is None
