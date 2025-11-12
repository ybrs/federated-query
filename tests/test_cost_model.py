"""Tests for cost model and cardinality estimation."""

import pytest
from federated_query.optimizer.cost import CostModel
from federated_query.optimizer.statistics import StatisticsCollector
from federated_query.config.config import CostConfig
from federated_query.catalog.catalog import Catalog
from federated_query.plan.logical import (
    Scan,
    Filter,
    Project,
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
        network_rtt_ms=10.0
    )


@pytest.fixture
def table_stats():
    """Create sample table statistics."""
    return TableStatistics(
        row_count=1000,
        total_size_bytes=100000,
        column_stats={
            "id": ColumnStatistics(
                num_distinct=1000,
                null_fraction=0.0,
                avg_width=8
            ),
            "name": ColumnStatistics(
                num_distinct=500,
                null_fraction=0.1,
                avg_width=20
            ),
            "status": ColumnStatistics(
                num_distinct=5,
                null_fraction=0.0,
                avg_width=10
            ),
        }
    )


@pytest.fixture
def cost_model(cost_config):
    """Create cost model without statistics."""
    return CostModel(cost_config)


class TestSelectivityEstimation:
    """Test selectivity estimation for various predicates."""

    def test_equality_selectivity_with_stats(self, cost_model, table_stats):
        """Test equality selectivity using statistics."""
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        assert selectivity == pytest.approx(1.0 / 5, rel=0.01)

    def test_equality_selectivity_without_stats(self, cost_model):
        """Test equality selectivity without statistics."""
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "unknown_col", DataType.VARCHAR),
            right=Literal("value", DataType.VARCHAR)
        )
        selectivity = cost_model.estimate_selectivity(predicate, None)
        assert selectivity == 0.1

    def test_inequality_selectivity(self, cost_model, table_stats):
        """Test inequality selectivity (LT, GT, etc)."""
        predicate = BinaryOp(
            op=BinaryOpType.LT,
            left=ColumnRef(None, "id", DataType.INTEGER),
            right=Literal(100, DataType.INTEGER)
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        assert selectivity == 0.33

    def test_neq_selectivity(self, cost_model, table_stats):
        """Test not-equal selectivity."""
        predicate = BinaryOp(
            op=BinaryOpType.NEQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        expected = 1.0 - (1.0 / 5)
        assert selectivity == pytest.approx(expected, rel=0.01)

    def test_and_selectivity(self, cost_model, table_stats):
        """Test AND selectivity (product of operands)."""
        left = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        right = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "id", DataType.INTEGER),
            right=Literal(100, DataType.INTEGER)
        )
        predicate = BinaryOp(op=BinaryOpType.AND, left=left, right=right)

        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        expected = (1.0 / 5) * 0.33
        assert selectivity == pytest.approx(expected, rel=0.01)

    def test_or_selectivity(self, cost_model, table_stats):
        """Test OR selectivity."""
        left = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        right = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("pending", DataType.VARCHAR)
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
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        predicate = UnaryOp(op=UnaryOpType.NOT, operand=inner)

        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        expected = 1.0 - (1.0 / 5)
        assert selectivity == pytest.approx(expected, rel=0.01)

    def test_is_null_selectivity(self, cost_model, table_stats):
        """Test IS NULL selectivity using null_fraction."""
        predicate = UnaryOp(
            op=UnaryOpType.IS_NULL,
            operand=ColumnRef(None, "name", DataType.VARCHAR)
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        assert selectivity == 0.1

    def test_is_not_null_selectivity(self, cost_model, table_stats):
        """Test IS NOT NULL selectivity."""
        predicate = UnaryOp(
            op=UnaryOpType.IS_NOT_NULL,
            operand=ColumnRef(None, "name", DataType.VARCHAR)
        )
        selectivity = cost_model.estimate_selectivity(predicate, table_stats)
        assert selectivity == 0.9

    def test_like_selectivity(self, cost_model, table_stats):
        """Test LIKE selectivity."""
        predicate = BinaryOp(
            op=BinaryOpType.LIKE,
            left=ColumnRef(None, "name", DataType.VARCHAR),
            right=Literal("%smith%", DataType.VARCHAR)
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
            columns=["id", "name"]
        )
        cardinality = cost_model.estimate_cardinality(scan)
        assert cardinality == 1000

    def test_filter_cardinality(self, cost_model):
        """Test filter cardinality estimation."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "status"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
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
            columns=["id", "name"]
        )
        project = Project(
            input=scan,
            expressions=[ColumnRef(None, "id", DataType.INTEGER)],
            aliases=["id"]
        )

        cardinality = cost_model.estimate_cardinality(project)
        assert cardinality == cost_model.estimate_cardinality(scan)

    def test_join_cardinality_inner(self, cost_model):
        """Test inner join cardinality estimation."""
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id"]
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name"]
        )
        condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("orders", "customer_id", DataType.INTEGER),
            right=ColumnRef("customers", "id", DataType.INTEGER)
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=condition
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
            columns=["id"]
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id"]
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.CROSS,
            condition=None
        )

        cardinality = cost_model.estimate_cardinality(join)
        assert cardinality == 1000 * 1000

    def test_join_cardinality_left_outer(self, cost_model):
        """Test left outer join cardinality."""
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id"]
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id"]
        )
        condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("orders", "customer_id", DataType.INTEGER),
            right=ColumnRef("customers", "id", DataType.INTEGER)
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.LEFT,
            condition=condition
        )

        cardinality = cost_model.estimate_cardinality(join)
        assert cardinality >= 1000

    def test_aggregate_cardinality_without_group_by(self, cost_model):
        """Test aggregate cardinality without GROUP BY."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "amount"]
        )
        agg = Aggregate(
            input=scan,
            group_by=[],
            aggregates=[],
            output_names=[]
        )

        cardinality = cost_model.estimate_cardinality(agg)
        assert cardinality == 1

    def test_aggregate_cardinality_with_group_by(self, cost_model):
        """Test aggregate cardinality with GROUP BY."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["customer_id", "amount"]
        )
        agg = Aggregate(
            input=scan,
            group_by=[ColumnRef(None, "customer_id", DataType.INTEGER)],
            aggregates=[],
            output_names=["customer_id"]
        )

        cardinality = cost_model.estimate_cardinality(agg)
        assert cardinality <= 1000

    def test_limit_cardinality(self, cost_model):
        """Test limit cardinality estimation."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"]
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
            columns=["id", "name"]
        )
        limit = Limit(input=scan, limit=10, offset=5)

        cardinality = cost_model.estimate_cardinality(limit)
        assert cardinality == 15


class TestCostModelWithStatistics:
    """Test cost model integrated with statistics collector."""

    def test_scan_with_statistics(self, cost_config, table_stats):
        """Test scan cardinality with statistics."""
        catalog = Catalog()
        stats_collector = StatisticsCollector(catalog)
        key = ("test_ds", "public", "users")
        stats_collector.cache[key] = table_stats

        cost_model = CostModel(cost_config, stats_collector)
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"]
        )

        cardinality = cost_model.estimate_cardinality(scan)
        assert cardinality == 1000

    def test_scan_with_filter_pushdown(self, cost_config, table_stats):
        """Test scan with pushed-down filter."""
        catalog = Catalog()
        stats_collector = StatisticsCollector(catalog)
        key = ("test_ds", "public", "users")
        stats_collector.cache[key] = table_stats

        cost_model = CostModel(cost_config, stats_collector)
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "status"],
            filters=predicate
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
            columns=["id", "name"]
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
            columns=["id", "name"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "id", DataType.INTEGER),
            right=Literal(100, DataType.INTEGER)
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
            columns=["id", "name", "email"]
        )
        project = Project(
            input=scan,
            expressions=[
                ColumnRef(None, "id", DataType.INTEGER),
                ColumnRef(None, "name", DataType.VARCHAR)
            ],
            aliases=["id", "name"]
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
            columns=["id", "customer_id"]
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name"]
        )
        condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("orders", "customer_id", DataType.INTEGER),
            right=ColumnRef("customers", "id", DataType.INTEGER)
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=condition
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
            columns=["customer_id", "amount"]
        )
        agg = Aggregate(
            input=scan,
            group_by=[ColumnRef(None, "customer_id", DataType.INTEGER)],
            aggregates=[],
            output_names=["customer_id"]
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
            columns=["id", "name"]
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
            columns=["id", "name", "status"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        filter_node = Filter(input=scan, predicate=predicate)
        project = Project(
            input=filter_node,
            expressions=[ColumnRef(None, "id", DataType.INTEGER)],
            aliases=["id"]
        )
        limit = Limit(input=project, limit=100)

        cost = cost_model.estimate_logical_plan_cost(limit)
        assert cost > 0

    def test_cost_increases_with_cardinality(self, cost_config, table_stats):
        """Test that cost increases with larger tables."""
        catalog = Catalog()
        stats_collector = StatisticsCollector(catalog)

        small_stats = TableStatistics(
            row_count=100,
            total_size_bytes=10000,
            column_stats=table_stats.column_stats
        )
        large_stats = TableStatistics(
            row_count=10000,
            total_size_bytes=1000000,
            column_stats=table_stats.column_stats
        )

        stats_collector.cache[("test_ds", "public", "small")] = small_stats
        stats_collector.cache[("test_ds", "public", "large")] = large_stats

        cost_model = CostModel(cost_config, stats_collector)

        small_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="small",
            columns=["id"]
        )
        large_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="large",
            columns=["id"]
        )

        small_cost = cost_model.estimate_logical_plan_cost(small_scan)
        large_cost = cost_model.estimate_logical_plan_cost(large_scan)
        assert large_cost > small_cost
