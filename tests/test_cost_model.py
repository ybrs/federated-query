"""Tests for cost model and cardinality estimation."""

import pytest
from federated_query.optimizer.cost import CostModel
from federated_query.optimizer.statistics import StatisticsCollector
from federated_query.config.config import CostConfig
from federated_query.catalog.catalog import Catalog
from federated_query.plan.logical import (
    Scan,
    Filter,
    Projection,
    Join,
    Aggregate,
    Limit,
    JoinType,
)
from federated_query.plan.expressions import (
    BinaryOp,
    UnaryOp,
    ColumnRef,
    Literal,
    BinaryOpType,
    UnaryOpType,
    DataType,
)
from federated_query.datasources.base import (
    TableStatistics,
    ColumnStatistics,
)


@pytest.fixture
def cost_config():
    """Create cost configuration."""
    return CostConfig(
        cpu_tuple_cost=0.01,
        io_page_cost=1.0,
        network_byte_cost=0.0001,
        network_rtt_ms=10.0,
    )


@pytest.fixture
def table_stats():
    """Create sample table statistics."""
    return TableStatistics(
        row_count=1000,
        total_size_bytes=100000,
        column_stats={
            "id": ColumnStatistics(num_distinct=1000, null_fraction=0.0, avg_width=8),
            "name": ColumnStatistics(num_distinct=500, null_fraction=0.1, avg_width=20),
            "status": ColumnStatistics(num_distinct=5, null_fraction=0.0, avg_width=10),
        },
    )


@pytest.fixture
def cost_model(cost_config):
    """Create cost model without statistics."""
    return CostModel(cost_config)


class _StatsSource:
    """A fake datasource serving canned TableStatistics by (schema, table)."""

    def __init__(self, stats_by_table):
        self.stats_by_table = stats_by_table

    def get_table_statistics(self, schema, table, columns):
        """Serve the canned statistics regardless of the requested columns."""
        return self.stats_by_table[(schema, table)]


def _seeded_collector(datasource, stats_by_table):
    """A StatisticsCollector whose catalog holds one fake stats-serving source."""
    catalog = Catalog()
    catalog.datasources[datasource] = _StatsSource(stats_by_table)
    return StatisticsCollector(catalog)


class TestSelectivityEstimation:
    """Test selectivity estimation for various predicates."""

    def test_equality_selectivity_with_stats(self, cost_model, table_stats):
        """Test equality selectivity using statistics."""
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(table=None, column="status", data_type=DataType.VARCHAR),
            right=Literal(value="active", data_type=DataType.VARCHAR),
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        assert selectivity == pytest.approx(1.0 / 5, rel=0.01)

    def test_equality_selectivity_without_stats(self, cost_model):
        """Test equality selectivity without statistics."""
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(
                table=None, column="unknown_col", data_type=DataType.VARCHAR
            ),
            right=Literal(value="value", data_type=DataType.VARCHAR),
        )
        selectivity = cost_model.estimate_selectivity(predicate, None)
        assert selectivity == 0.1

    def test_inequality_selectivity(self, cost_model, table_stats):
        """Test inequality selectivity (LT, GT, etc)."""
        predicate = BinaryOp(
            op=BinaryOpType.LT,
            left=ColumnRef(table=None, column="id", data_type=DataType.INTEGER),
            right=Literal(value=100, data_type=DataType.INTEGER),
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        assert selectivity == 0.33

    def test_neq_selectivity(self, cost_model, table_stats):
        """Test not-equal selectivity."""
        predicate = BinaryOp(
            op=BinaryOpType.NEQ,
            left=ColumnRef(table=None, column="status", data_type=DataType.VARCHAR),
            right=Literal(value="active", data_type=DataType.VARCHAR),
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        expected = 1.0 - (1.0 / 5)
        assert selectivity == pytest.approx(expected, rel=0.01)

    def test_and_selectivity(self, cost_model, table_stats):
        """Test AND selectivity (product of operands)."""
        left = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(table=None, column="status", data_type=DataType.VARCHAR),
            right=Literal(value="active", data_type=DataType.VARCHAR),
        )
        right = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(table=None, column="id", data_type=DataType.INTEGER),
            right=Literal(value=100, data_type=DataType.INTEGER),
        )
        predicate = BinaryOp(op=BinaryOpType.AND, left=left, right=right)

        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        expected = (1.0 / 5) * 0.33
        assert selectivity == pytest.approx(expected, rel=0.01)

    def test_or_selectivity(self, cost_model, table_stats):
        """Test OR selectivity."""
        left = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(table=None, column="status", data_type=DataType.VARCHAR),
            right=Literal(value="active", data_type=DataType.VARCHAR),
        )
        right = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(table=None, column="status", data_type=DataType.VARCHAR),
            right=Literal(value="pending", data_type=DataType.VARCHAR),
        )
        predicate = BinaryOp(op=BinaryOpType.OR, left=left, right=right)

        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        sel_left = 1.0 / 5
        sel_right = 1.0 / 5
        expected = 1.0 - ((1.0 - sel_left) * (1.0 - sel_right))
        assert selectivity == pytest.approx(expected, rel=0.01)

    def test_not_selectivity(self, cost_model, table_stats):
        """Test NOT selectivity."""
        inner = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(table=None, column="status", data_type=DataType.VARCHAR),
            right=Literal(value="active", data_type=DataType.VARCHAR),
        )
        predicate = UnaryOp(op=UnaryOpType.NOT, operand=inner)

        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        expected = 1.0 - (1.0 / 5)
        assert selectivity == pytest.approx(expected, rel=0.01)

    def test_is_null_selectivity(self, cost_model, table_stats):
        """Test IS NULL selectivity using null_fraction."""
        predicate = UnaryOp(
            op=UnaryOpType.IS_NULL,
            operand=ColumnRef(table=None, column="name", data_type=DataType.VARCHAR),
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        assert selectivity == 0.1

    def test_is_not_null_selectivity(self, cost_model, table_stats):
        """Test IS NOT NULL selectivity."""
        predicate = UnaryOp(
            op=UnaryOpType.IS_NOT_NULL,
            operand=ColumnRef(table=None, column="name", data_type=DataType.VARCHAR),
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        assert selectivity == 0.9

    def test_like_selectivity(self, cost_model, table_stats):
        """Test LIKE selectivity."""
        predicate = BinaryOp(
            op=BinaryOpType.LIKE,
            left=ColumnRef(table=None, column="name", data_type=DataType.VARCHAR),
            right=Literal(value="%smith%", data_type=DataType.VARCHAR),
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        assert selectivity == 0.1


class TestCardinalityEstimation:
    """Test cardinality estimation for various plan nodes."""

    def test_scan_cardinality_without_stats(self, cost_model):
        """Test scan cardinality without statistics."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"],
        )
        cardinality = cost_model.estimate_cardinality(scan)
        assert cardinality == 1000

    def test_filter_cardinality(self, cost_model):
        """Test filter cardinality estimation."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "status"],
        )
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(table=None, column="status", data_type=DataType.VARCHAR),
            right=Literal(value="active", data_type=DataType.VARCHAR),
        )
        filter_node = Filter(input=scan, predicate=predicate)

        cardinality = cost_model.estimate_cardinality(filter_node)
        expected = 1000 * 0.1
        assert cardinality == int(expected)

    def test_project_cardinality(self, cost_model):
        """Test project cardinality (same as input)."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"],
        )
        project = Projection(
            input=scan,
            expressions=[
                ColumnRef(table=None, column="id", data_type=DataType.INTEGER)
            ],
            aliases=["id"],
        )

        cardinality = cost_model.estimate_cardinality(project)
        assert cardinality == cost_model.estimate_cardinality(scan)

    def test_join_cardinality_inner(self, cost_model):
        """Test inner join cardinality estimation."""
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id"],
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name"],
        )
        condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(
                table="orders", column="customer_id", data_type=DataType.INTEGER
            ),
            right=ColumnRef(table="customers", column="id", data_type=DataType.INTEGER),
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=condition,
        )

        cardinality = cost_model.estimate_cardinality(join)
        expected = 1000 * 1000 * 0.1
        assert cardinality == int(expected)

    def test_join_cardinality_cross(self, cost_model):
        """Test cross join cardinality (Cartesian product)."""
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id"],
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id"],
        )
        join = Join(
            left=left_scan, right=right_scan, join_type=JoinType.CROSS, condition=None
        )

        cardinality = cost_model.estimate_cardinality(join)
        assert cardinality == 1000 * 1000

    def test_join_cardinality_left_outer(self, cost_model):
        """Test left outer join cardinality."""
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id"],
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id"],
        )
        condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(
                table="orders", column="customer_id", data_type=DataType.INTEGER
            ),
            right=ColumnRef(table="customers", column="id", data_type=DataType.INTEGER),
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.LEFT,
            condition=condition,
        )

        cardinality = cost_model.estimate_cardinality(join)
        assert cardinality >= 1000

    def test_aggregate_cardinality_without_group_by(self, cost_model):
        """Test aggregate cardinality without GROUP BY."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "amount"],
        )
        agg = Aggregate(input=scan, group_by=[], aggregates=[], output_names=[])

        cardinality = cost_model.estimate_cardinality(agg)
        assert cardinality == 1

    def test_aggregate_cardinality_with_group_by(self, cost_model):
        """Test aggregate cardinality with GROUP BY."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["customer_id", "amount"],
        )
        agg = Aggregate(
            input=scan,
            group_by=[
                ColumnRef(table=None, column="customer_id", data_type=DataType.INTEGER)
            ],
            aggregates=[],
            output_names=["customer_id"],
        )

        cardinality = cost_model.estimate_cardinality(agg)
        assert cardinality <= 1000

    def test_limit_cardinality(self, cost_model):
        """Test limit cardinality estimation."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"],
        )
        limit = Limit(input=scan, limit=10)

        cardinality = cost_model.estimate_cardinality(limit)
        assert cardinality == 10

    def test_limit_with_offset(self, cost_model):
        """Test limit cardinality with offset."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"],
        )
        limit = Limit(input=scan, limit=10, offset=5)

        cardinality = cost_model.estimate_cardinality(limit)
        assert cardinality == 15


class TestCostModelWithStatistics:
    """Test cost model integrated with statistics collector."""

    def test_scan_with_statistics(self, cost_config, table_stats):
        """Test scan cardinality with statistics."""
        stats_collector = _seeded_collector(
            "test_ds", {("public", "users"): table_stats}
        )

        cost_model = CostModel(cost_config, stats_collector)
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"],
        )

        cardinality = cost_model.estimate_cardinality(scan)
        assert cardinality == 1000

    def test_scan_with_filter_pushdown(self, cost_config, table_stats):
        """Test scan with pushed-down filter."""
        stats_collector = _seeded_collector(
            "test_ds", {("public", "users"): table_stats}
        )

        cost_model = CostModel(cost_config, stats_collector)
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(table=None, column="status", data_type=DataType.VARCHAR),
            right=Literal(value="active", data_type=DataType.VARCHAR),
        )
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "status"],
            filters=predicate,
        )

        cardinality = cost_model.estimate_cardinality(scan)
        expected = 1000 * (1.0 / 5)
        assert cardinality == int(expected)


class TestOperatorCostEstimation:
    """Test cost estimation for various operators."""

    def test_scan_cost(self, cost_model):
        """Test scan cost estimation."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"],
        )
        cost = cost_model.estimate_logical_plan_cost(scan)
        assert cost > 0
        assert cost < 10000

    def test_filter_cost(self, cost_model):
        """Test filter cost includes input cost plus processing."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"],
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(table=None, column="id", data_type=DataType.INTEGER),
            right=Literal(value=100, data_type=DataType.INTEGER),
        )
        filter_node = Filter(input=scan, predicate=predicate)

        scan_cost = cost_model.estimate_logical_plan_cost(scan)
        filter_cost = cost_model.estimate_logical_plan_cost(filter_node)
        assert filter_cost > scan_cost

    def test_project_cost(self, cost_model):
        """Test projection cost estimation."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "email"],
        )
        project = Projection(
            input=scan,
            expressions=[
                ColumnRef(table=None, column="id", data_type=DataType.INTEGER),
                ColumnRef(table=None, column="name", data_type=DataType.VARCHAR),
            ],
            aliases=["id", "name"],
        )

        scan_cost = cost_model.estimate_logical_plan_cost(scan)
        project_cost = cost_model.estimate_logical_plan_cost(project)
        assert project_cost > scan_cost

    def test_join_cost(self, cost_model):
        """Test join cost estimation."""
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id"],
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name"],
        )
        condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(
                table="orders", column="customer_id", data_type=DataType.INTEGER
            ),
            right=ColumnRef(table="customers", column="id", data_type=DataType.INTEGER),
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=condition,
        )

        left_cost = cost_model.estimate_logical_plan_cost(left_scan)
        right_cost = cost_model.estimate_logical_plan_cost(right_scan)
        join_cost = cost_model.estimate_logical_plan_cost(join)
        assert join_cost > left_cost + right_cost

    def test_aggregate_cost(self, cost_model):
        """Test aggregation cost estimation."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["customer_id", "amount"],
        )
        agg = Aggregate(
            input=scan,
            group_by=[
                ColumnRef(table=None, column="customer_id", data_type=DataType.INTEGER)
            ],
            aggregates=[],
            output_names=["customer_id"],
        )

        scan_cost = cost_model.estimate_logical_plan_cost(scan)
        agg_cost = cost_model.estimate_logical_plan_cost(agg)
        assert agg_cost > scan_cost

    def test_limit_cost(self, cost_model):
        """Test limit cost is minimal."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"],
        )
        limit = Limit(input=scan, limit=10)

        limit_cost = cost_model.estimate_logical_plan_cost(limit)
        assert limit_cost > 0
        assert limit_cost < 100

    def test_complex_plan_cost(self, cost_model):
        """Test cost of complex plan with multiple operators."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "status"],
        )
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(table=None, column="status", data_type=DataType.VARCHAR),
            right=Literal(value="active", data_type=DataType.VARCHAR),
        )
        filter_node = Filter(input=scan, predicate=predicate)
        project = Projection(
            input=filter_node,
            expressions=[
                ColumnRef(table=None, column="id", data_type=DataType.INTEGER)
            ],
            aliases=["id"],
        )
        limit = Limit(input=project, limit=100)

        cost = cost_model.estimate_logical_plan_cost(limit)
        assert cost > 0

    def test_cost_increases_with_cardinality(self, cost_config, table_stats):
        """Test that cost increases with larger tables."""
        small_stats = TableStatistics(
            row_count=100, total_size_bytes=10000, column_stats=table_stats.column_stats
        )
        large_stats = TableStatistics(
            row_count=10000,
            total_size_bytes=1000000,
            column_stats=table_stats.column_stats,
        )

        stats_collector = _seeded_collector(
            "test_ds",
            {("public", "small"): small_stats, ("public", "large"): large_stats},
        )

        cost_model = CostModel(cost_config, stats_collector)

        small_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="small",
            columns=["id"],
        )
        large_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="large",
            columns=["id"],
        )

        small_cost = cost_model.estimate_logical_plan_cost(small_scan)
        large_cost = cost_model.estimate_logical_plan_cost(large_scan)
        assert large_cost > small_cost


def _tpch_like_stats():
    """SF1-shaped statistics: orders (1.5M, unique o_custkey domain 150k)
    joined to customer (150k, unique c_custkey), with a 5-value flag column."""
    orders = TableStatistics(
        row_count=1_500_000,
        total_size_bytes=0,
        column_stats={
            "o_custkey": ColumnStatistics(
                num_distinct=150_000, null_fraction=0.0, avg_width=8
            ),
            "o_flag": ColumnStatistics(
                num_distinct=5, null_fraction=0.0, avg_width=1
            ),
        },
    )
    customer = TableStatistics(
        row_count=150_000,
        total_size_bytes=0,
        column_stats={
            "c_custkey": ColumnStatistics(
                num_distinct=150_000, null_fraction=0.0, avg_width=8
            ),
            "c_age": ColumnStatistics(
                num_distinct=100, null_fraction=0.0, avg_width=4,
                min_value=0, max_value=100,
            ),
        },
    )
    return {("public", "orders"): orders, ("public", "customer"): customer}


def _provenance_model(cost_config):
    """A CostModel over the TPC-H-like seeded statistics."""
    collector = _seeded_collector("test_ds", _tpch_like_stats())
    return CostModel(cost_config, collector)


def _scan(table, columns, alias=None, filters=None):
    """A qualified scan of a seeded test table."""
    return Scan(
        datasource="test_ds", schema_name="public", table_name=table,
        columns=columns, alias=alias, filters=filters,
    )


def _custkey_join(join_type=JoinType.INNER, left_filters=None):
    """orders JOIN customer ON o.o_custkey = c.c_custkey (qualified refs)."""
    condition = BinaryOp(
        op=BinaryOpType.EQ,
        left=ColumnRef(table="o", column="o_custkey", data_type=DataType.INTEGER),
        right=ColumnRef(table="c", column="c_custkey", data_type=DataType.INTEGER),
    )
    return Join(
        left=_scan("orders", ["o_custkey", "o_flag"], alias="o",
                   filters=left_filters),
        right=_scan("customer", ["c_custkey", "c_age"], alias="c"),
        join_type=join_type,
        condition=condition,
    )


class TestProvenanceEstimate:
    """The provenance-carrying estimate() with the NDV join formula."""

    def test_inner_join_uses_ndv_formula(self, cost_config):
        """1.5M orders x 150k customer on custkey is ~1.5M rows - the NDV
        formula, not the old 0.1-of-cross-product guess (225 billion * 0.1)."""
        model = _provenance_model(cost_config)
        estimate = model.estimate(_custkey_join())
        assert estimate.rows == 1_500_000
        assert estimate.defaults_used == []

    def test_join_ndv_clamped_by_filtered_side(self, cost_config):
        """A filtered probe side cannot have more distinct keys than rows:
        orders filtered to 1/5 -> 300k rows; ndv(o_custkey) clamps 150k -> ...
        rows = 300k * 150k / max(150k, 150k) = 300k."""
        model = _provenance_model(cost_config)
        flag_filter = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(table="o", column="o_flag", data_type=DataType.VARCHAR),
            right=Literal(value="F", data_type=DataType.VARCHAR),
        )
        estimate = model.estimate(_custkey_join(left_filters=flag_filter))
        assert estimate.rows == 300_000

    def test_semi_join_bounded_by_left(self, cost_config):
        """SEMI keeps at most the left side's rows."""
        model = _provenance_model(cost_config)
        estimate = model.estimate(_custkey_join(join_type=JoinType.SEMI))
        assert estimate.rows <= 1_500_000

    def test_left_join_keeps_left_rows(self, cost_config):
        """LEFT output is at least the left side's rows."""
        model = _provenance_model(cost_config)
        estimate = model.estimate(_custkey_join(join_type=JoinType.LEFT))
        assert estimate.rows >= 1_500_000

    def test_cross_join_is_product(self, cost_config):
        """CROSS (no condition) is the plain product of the sides."""
        model = _provenance_model(cost_config)
        join = Join(
            left=_scan("orders", ["o_custkey"], alias="o"),
            right=_scan("customer", ["c_custkey"], alias="c"),
            join_type=JoinType.CROSS,
            condition=None,
        )
        estimate = model.estimate(join)
        assert estimate.rows == 1_500_000 * 150_000

    def test_missing_row_count_default_recorded(self, cost_config):
        """A table whose source honestly reports row_count=None (never
        analyzed) estimates from DEFAULT_ROW_COUNT and says so."""
        stats = _tpch_like_stats()
        stats[("public", "unknown_t")] = TableStatistics(
            row_count=None, total_size_bytes=0, column_stats={}
        )
        collector = _seeded_collector("test_ds", stats)
        model = CostModel(cost_config, collector)
        scan = Scan(
            datasource="test_ds", schema_name="public", table_name="unknown_t",
            columns=["x"],
        )
        estimate = model.estimate(scan)
        assert estimate.rows == 1000
        assert any("unknown_t" in entry for entry in estimate.defaults_used)

    def test_unknown_ndv_defaults_with_provenance(self, cost_config):
        """A join key without NDV statistics estimates from the NDV default
        and records exactly which column was defaulted."""
        stats = _tpch_like_stats()
        no_ndv = TableStatistics(
            row_count=150_000,
            total_size_bytes=0,
            column_stats={
                "c_custkey": ColumnStatistics(
                    num_distinct=None, null_fraction=0.0, avg_width=8
                ),
            },
        )
        stats[("public", "customer")] = no_ndv
        collector = _seeded_collector("test_ds", stats)
        model = CostModel(cost_config, collector)
        estimate = model.estimate(_custkey_join())
        joined = " ".join(estimate.defaults_used)
        assert "c_custkey" in joined
        assert estimate.rows > 0

    def test_range_interpolation_from_min_max(self, cost_config):
        """c_age <= 50 over min 0 / max 100 halves the scan, instead of the
        flat range default."""
        model = _provenance_model(cost_config)
        predicate = BinaryOp(
            op=BinaryOpType.LTE,
            left=ColumnRef(table="c", column="c_age", data_type=DataType.INTEGER),
            right=Literal(value=50, data_type=DataType.INTEGER),
        )
        scan = _scan("customer", ["c_custkey", "c_age"], alias="c",
                     filters=predicate)
        estimate = model.estimate(scan)
        assert estimate.rows == 75_000

    def test_pushed_aggregate_scan_estimates_groups(self, cost_config):
        """A scan with a pushed-down GROUP BY is costed as its group count
        (ndv of the key), not as the raw table."""
        model = _provenance_model(cost_config)
        scan = Scan(
            datasource="test_ds", schema_name="public", table_name="orders",
            columns=["o_flag"], alias="o",
            group_by=[ColumnRef(table="o", column="o_flag",
                                data_type=DataType.VARCHAR)],
            aggregates=[ColumnRef(table="o", column="o_flag",
                                  data_type=DataType.VARCHAR)],
        )
        estimate = model.estimate(scan)
        assert estimate.rows == 5

    def test_aggregate_groups_from_key_ndv(self, cost_config):
        """An Aggregate's output is the product of its group-key NDVs,
        clamped by its input size."""
        model = _provenance_model(cost_config)
        aggregate = Aggregate(
            input=_scan("orders", ["o_custkey", "o_flag"], alias="o"),
            group_by=[ColumnRef(table="o", column="o_flag",
                                data_type=DataType.VARCHAR)],
            aggregates=[],
            output_names=["o_flag"],
        )
        estimate = model.estimate(aggregate)
        assert estimate.rows == 5

    def test_estimate_raises_on_unknown_node(self, cost_config):
        """estimate() must raise for a plan node it has no rule for."""
        model = _provenance_model(cost_config)

        class _Strange:
            pass

        with pytest.raises(ValueError):
            model.estimate(_Strange())

    def test_join_tree_cost_sums_join_outputs(self, cost_config):
        """The C_out cost of a tree is the sum of its join output estimates."""
        model = _provenance_model(cost_config)
        join = _custkey_join()
        cost = model.join_tree_cost(join)
        assert cost == 1_500_000


def test_pushed_aggregate_scan_ignores_having_columns_for_stats(cost_config):
    """A scan carrying a pushed GROUP BY + HAVING must not request source
    stats for the HAVING's aggregate-output column (not a stored column) -
    only its real base columns. Regression for the q18 semi-atom estimate."""
    tables = {("public", "lineitem"): TableStatistics(
        row_count=6_000_000, total_size_bytes=0,
        column_stats={"l_orderkey": ColumnStatistics(
            num_distinct=1_500_000, null_fraction=0.0, avg_width=8)},
    )}
    model = CostModel(cost_config, _seeded_collector("ds", tables))
    having = BinaryOp(
        op=BinaryOpType.GT,
        left=ColumnRef(table="l", column="__subq_0_h1", data_type=DataType.INTEGER),
        right=Literal(value=300, data_type=DataType.INTEGER),
    )
    scan = Scan(
        datasource="ds", schema_name="public", table_name="lineitem",
        columns=["l_orderkey"], alias="l",
        group_by=[ColumnRef(table="l", column="l_orderkey",
                            data_type=DataType.INTEGER)],
        aggregates=[ColumnRef(table="l", column="l_orderkey",
                              data_type=DataType.INTEGER)],
        output_names=["l_orderkey"],
        filters=having,
    )
    # Would raise a KeyError inside the fake source if __subq_0_h1 were
    # requested; the group-count estimate returns cleanly instead.
    assert model.estimate(scan).rows > 0


class TestSemiAntiMatchedRows:
    """The SEMI/ANTI distinct-match estimate (occupancy formula).

    The bug: matched left rows were min(left, inner), which saturates to
    left for a many-to-many inner (inner >= left) and forces ANTI to 0 - a
    self-referential NOT-EXISTS fact atom then looked free to lead the join
    order (TPC-H q21). The occupancy estimate matched = left * (1 - e^-fanout)
    (fanout = inner/left) never saturates, so an ANTI is never spuriously
    empty, while a selective semi (inner << left) is unchanged.
    """

    def _model(self):
        """A cost model without statistics (the helper is stats-free)."""
        return CostModel(CostConfig(
            cpu_tuple_cost=0.01, io_page_cost=1.0,
            network_byte_cost=0.0001, network_rtt_ms=10.0,
        ))

    def test_fanout_one_keeps_a_real_anti_fraction(self):
        """inner == left (fanout 1): matched ~ 0.63*left, so ANTI ~ 0.37*left,
        never 0 - this is the q21 regime that used to collapse."""
        matched = self._model()._semi_matched_rows(1000, 1000)
        assert 600 <= matched <= 640
        assert 1000 - matched > 300

    def test_selective_inner_matches_the_inner_size(self):
        """inner << left: matched ~ inner (unchanged from the old min(left,
        inner) for a selective semi)."""
        matched = self._model()._semi_matched_rows(10, 1000)
        assert 9 <= matched <= 10

    def test_dense_fanout_saturates_to_left(self):
        """A genuinely dense many-to-many (every left key matched, huge fanout)
        still matches ~all of left, so ANTI is ~empty - the occupancy formula
        agrees with reality here, it only fixes the fanout~1 case."""
        matched = self._model()._semi_matched_rows(100_000, 1000)
        assert matched == 1000

    def test_bounds_never_exceed_left_or_go_negative(self):
        """matched is always within [0, left_rows]."""
        model = self._model()
        assert model._semi_matched_rows(0, 1000) == 0
        assert 0 <= model._semi_matched_rows(500, 1000) <= 1000
        assert model._semi_matched_rows(5, 0) == 0


def test_anti_join_estimate_is_not_zero_for_a_one_to_one_inner():
    """Integration: an ANTI join whose inner equals its left side (fanout 1)
    must estimate a real fraction of the left rows, not 0. Regression for the
    q21 fact atom that estimated 0 and mis-led the join order."""
    tables = {("public", "l1"): TableStatistics(
        row_count=1000, total_size_bytes=0,
        column_stats={"k": ColumnStatistics(
            num_distinct=1000, null_fraction=0.0, avg_width=8)},
    ), ("public", "l2"): TableStatistics(
        row_count=1000, total_size_bytes=0,
        column_stats={"k": ColumnStatistics(
            num_distinct=1000, null_fraction=0.0, avg_width=8)},
    )}
    model = CostModel(cost_config_fixture(), _seeded_collector("ds", tables))
    left = Scan(datasource="ds", schema_name="public", table_name="l1",
                columns=["k"], alias="l1")
    right = Scan(datasource="ds", schema_name="public", table_name="l2",
                 columns=["k"], alias="l2")
    condition = BinaryOp(
        op=BinaryOpType.EQ,
        left=ColumnRef(table="l1", column="k", data_type=DataType.INTEGER),
        right=ColumnRef(table="l2", column="k", data_type=DataType.INTEGER),
    )
    anti = Join(left=left, right=right, join_type=JoinType.ANTI, condition=condition)
    assert model.estimate(anti).rows > 200


def cost_config_fixture():
    """A plain cost config for direct construction in module-level tests."""
    return CostConfig(cpu_tuple_cost=0.01, io_page_cost=1.0,
                      network_byte_cost=0.0001, network_rtt_ms=10.0)
