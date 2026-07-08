"""Tests for the dim-shipping dimension-explosion gate.

The gate declines shipping a plain aggregate whose GROUP BY spans two or more
INDEPENDENT high-cardinality dimensions (it will not collapse, so shipping moves
a large materialized result - the q23 regression). It must NOT decline an
aggregate that spans one high-card dimension, even with several correlated keys
from it (i_item_id + i_item_desc), which still collapses (the q17/q25/q39 wins).
"""

import types

from federated_query.optimizer.cost import CostModel
from federated_query.optimizer.statistics import StatisticsCollector
from federated_query.optimizer.dim_shipping import DimShipping
from federated_query.config.config import CostConfig
from federated_query.catalog.catalog import Catalog
from federated_query.plan.logical import Scan, Join, Aggregate, JoinType
from federated_query.plan.expressions import (
    ColumnRef,
    BinaryOp,
    BinaryOpType,
    DataType,
)
from federated_query.datasources.base import TableStatistics, ColumnStatistics


class _StatsSource:
    """A fake datasource serving canned TableStatistics by (schema, table)."""

    def __init__(self, stats_by_table):
        """Hold the canned per-table statistics the collector will serve."""
        self.stats_by_table = stats_by_table

    def get_table_statistics(self, schema, table, columns):
        """Serve the canned statistics for a table, or None when absent."""
        return self.stats_by_table.get((schema, table))


def _gate(stats_by_table):
    """A DimShipping whose planner exposes only a stats-backed cost model."""
    catalog = Catalog()
    catalog.datasources["ds"] = _StatsSource(stats_by_table)
    collector = StatisticsCollector(catalog)
    cost_config = CostConfig(
        cpu_tuple_cost=0.01, io_page_cost=1.0,
        network_byte_cost=0.0001, network_rtt_ms=10.0,
    )
    cost_model = CostModel(cost_config, collector)
    planner = types.SimpleNamespace(cost_model=cost_model)
    return DimShipping(planner)


def _table_stats(row_count, columns):
    """A TableStatistics with the given row count and per-column NDVs."""
    column_stats = {}
    for name, ndv in columns.items():
        column_stats[name] = ColumnStatistics(
            num_distinct=ndv, null_fraction=0.0, avg_width=8
        )
    return TableStatistics(
        row_count=row_count, total_size_bytes=row_count * 8,
        column_stats=column_stats,
    )


def _scan(table, columns):
    """A base Scan named after its table (its qualifier), reading columns."""
    return Scan(
        datasource="ds", schema_name="public", table_name=table,
        columns=columns,
    )


def _col(table, column):
    """A qualified column reference into a base scan."""
    return ColumnRef(table=table, column=column, data_type=DataType.INTEGER)


def _join(left, right):
    """An inner join of two scans on a trivial equi condition."""
    condition = BinaryOp(
        op=BinaryOpType.EQ,
        left=_col(left.table_name, left.columns[0]),
        right=_col(right.table_name, right.columns[0]),
    )
    return Join(
        left=left, right=right, join_type=JoinType.INNER, condition=condition
    )


def _aggregate(input_node, keys):
    """A plain GROUP BY aggregate over input_node keyed by the given columns."""
    return Aggregate(
        input=input_node, group_by=keys, aggregates=[],
        output_names=[key.column for key in keys],
    )


# item: two high-cardinality columns (id and description are correlated - both
# from item). date_dim: one high-cardinality column. store: one low-card column.
_ITEM = _table_stats(100000, {"i_item_sk": 100000, "i_item_desc": 60000})
_DATE = _table_stats(70000, {"d_date": 70000})
_STORE = _table_stats(100, {"s_state": 30})
# warehouse: a small dimension with NO column histograms - only a row count.
_WAREHOUSE_NO_COLS = TableStatistics(
    row_count=10, total_size_bytes=80, column_stats={}
)

_STATS = {
    ("public", "item"): _ITEM,
    ("public", "date_dim"): _DATE,
    ("public", "store"): _STORE,
    ("public", "warehouse"): _WAREHOUSE_NO_COLS,
}


def test_two_independent_high_card_dimensions_explode():
    """item x date_dim: two independent high-card dimensions => decline (q23)."""
    gate = _gate(_STATS)
    item = _scan("item", ["i_item_sk", "i_item_desc"])
    date = _scan("date_dim", ["d_date"])
    agg = _aggregate(
        _join(item, date),
        [_col("item", "i_item_sk"), _col("date_dim", "d_date")],
    )
    assert gate._dimension_explosion(agg) is True


def test_correlated_keys_from_one_dimension_do_not_explode():
    """i_item_sk + i_item_desc are both from item => one dimension => ship."""
    gate = _gate(_STATS)
    item = _scan("item", ["i_item_sk", "i_item_desc"])
    date = _scan("date_dim", ["d_date"])
    agg = _aggregate(
        _join(item, date),
        [_col("item", "i_item_sk"), _col("item", "i_item_desc")],
    )
    assert gate._dimension_explosion(agg) is False


def test_one_high_and_one_low_dimension_do_not_explode():
    """item (high) x store.s_state (low, NDV 30) => one high dimension => ship."""
    gate = _gate(_STATS)
    item = _scan("item", ["i_item_sk"])
    store = _scan("store", ["s_state"])
    agg = _aggregate(
        _join(item, store),
        [_col("item", "i_item_sk"), _col("store", "s_state")],
    )
    assert gate._dimension_explosion(agg) is False


def test_row_count_bounds_ndv_when_column_stats_absent():
    """warehouse has no column histogram but 10 rows, so its column is low-card
    (NDV <= row count) and item x warehouse spans one high dimension => ship."""
    gate = _gate(_STATS)
    item = _scan("item", ["i_item_sk"])
    warehouse = _scan("warehouse", ["w_warehouse_name"])
    agg = _aggregate(
        _join(item, warehouse),
        [_col("item", "i_item_sk"), _col("warehouse", "w_warehouse_name")],
    )
    assert gate._dimension_explosion(agg) is False


def test_unknown_ndv_without_row_count_counts_as_high_card():
    """With no stats at all for a table, its group key is unknown => high-card;
    two such independent dimensions => decline (the conservative default)."""
    gate = _gate({("public", "item"): _ITEM})
    item = _scan("item", ["i_item_sk"])
    mystery = _scan("mystery", ["m_key"])
    agg = _aggregate(
        _join(item, mystery),
        [_col("item", "i_item_sk"), _col("mystery", "m_key")],
    )
    assert gate._dimension_explosion(agg) is True
