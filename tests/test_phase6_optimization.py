"""Tests for Phase 6 logical optimization rules."""

import pytest
from federated_query.optimizer.rules import (
    PredicatePushdownRule,
    ProjectionPushdownRule,
    LimitPushdownRule,
    RuleBasedOptimizer,
)
from federated_query.catalog.catalog import Catalog
from federated_query.plan.logical import (
    Scan,
    Filter,
    Project,
    Limit,
    Join,
    JoinType,
)
from federated_query.plan.expressions import (
    BinaryOp,
    ColumnRef,
    Literal,
    BinaryOpType,
    DataType,
)


@pytest.fixture
def catalog():
    """Create test catalog."""
    return Catalog()


class TestPredicatePushdown:
    """Test predicate pushdown optimization."""

    def test_push_filter_to_scan(self):
        """Test pushing filter into scan node."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(scan, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        assert isinstance(result, Scan)
        assert result.filters is not None
        assert result.filters == predicate

    def test_merge_adjacent_filters(self):
        """Test merging two adjacent filters."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        predicate1 = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        predicate2 = BinaryOp(
            op=BinaryOpType.LT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(65, DataType.INTEGER)
        )
        filter1 = Filter(scan, predicate1)
        filter2 = Filter(filter1, predicate2)

        rule = PredicatePushdownRule()
        result = rule.apply(filter2)

        assert isinstance(result, Scan)
        assert result.filters is not None

    def test_push_filter_through_project(self):
        """Test pushing filter through projection."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        project = Project(
            input=scan,
            expressions=[
                ColumnRef(None, "id", DataType.INTEGER),
                ColumnRef(None, "age", DataType.INTEGER)
            ],
            aliases=["id", "age"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(project, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        assert result is not None

    def test_no_pushdown_needed(self):
        """Test case where no pushdown is needed."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"]
        )

        rule = PredicatePushdownRule()
        result = rule.apply(scan)

        assert result == scan


class TestProjectionPushdown:
    """Test projection pushdown optimization."""

    def test_collect_required_columns_from_scan(self):
        """Test collecting columns from scan."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )

        rule = ProjectionPushdownRule()
        columns = rule._collect_required_columns(scan)

        assert "id" in columns
        assert "name" in columns
        assert "age" in columns

    def test_collect_required_columns_from_project(self):
        """Test collecting columns from projection."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        project = Project(
            input=scan,
            expressions=[
                ColumnRef(None, "id", DataType.INTEGER),
                ColumnRef(None, "name", DataType.VARCHAR)
            ],
            aliases=["id", "name"]
        )

        rule = ProjectionPushdownRule()
        columns = rule._collect_required_columns(project)

        assert "id" in columns
        assert "name" in columns

    def test_extract_columns_from_binary_op(self):
        """Test extracting columns from binary operation."""
        expr = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )

        rule = ProjectionPushdownRule()
        columns = rule._extract_columns(expr)

        assert "age" in columns
        assert len(columns) == 1

    def test_extract_columns_from_nested_expr(self):
        """Test extracting columns from nested expression."""
        expr = BinaryOp(
            op=BinaryOpType.AND,
            left=BinaryOp(
                op=BinaryOpType.GT,
                left=ColumnRef(None, "age", DataType.INTEGER),
                right=Literal(18, DataType.INTEGER)
            ),
            right=BinaryOp(
                op=BinaryOpType.EQ,
                left=ColumnRef(None, "status", DataType.VARCHAR),
                right=Literal("active", DataType.VARCHAR)
            )
        )

        rule = ProjectionPushdownRule()
        columns = rule._extract_columns(expr)

        assert "age" in columns
        assert "status" in columns


class TestLimitPushdown:
    """Test limit pushdown optimization."""

    def test_push_limit_through_project(self):
        """Test pushing limit through projection."""
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
        limit = Limit(project, limit=10)

        rule = LimitPushdownRule()
        result = rule.apply(limit)

        assert isinstance(result, Project)
        assert isinstance(result.input, Limit)
        assert result.input.limit == 10

    def test_push_limit_through_filter(self):
        """Test pushing limit through filter."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(scan, predicate)
        limit = Limit(filter_node, limit=10)

        rule = LimitPushdownRule()
        result = rule.apply(limit)

        assert isinstance(result, Filter)
        assert isinstance(result.input, Limit)
        assert result.input.limit == 10

    def test_limit_with_offset(self):
        """Test limit pushdown with offset."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"]
        )
        limit = Limit(scan, limit=10, offset=5)

        rule = LimitPushdownRule()
        result = rule.apply(limit)

        assert isinstance(result, Limit)
        assert result.limit == 10
        assert result.offset == 5


class TestRuleBasedOptimizer:
    """Test rule-based optimizer with multiple rules."""

    def test_optimizer_with_predicate_pushdown(self, catalog):
        """Test optimizer applies predicate pushdown."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(scan, predicate)

        optimizer = RuleBasedOptimizer(catalog)
        optimizer.add_rule(PredicatePushdownRule())

        result = optimizer.optimize(filter_node)

        assert isinstance(result, Scan)
        assert result.filters is not None

    def test_optimizer_with_multiple_rules(self, catalog):
        """Test optimizer with multiple rules."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        project = Project(
            input=scan,
            expressions=[ColumnRef(None, "id", DataType.INTEGER)],
            aliases=["id"]
        )
        limit = Limit(project, limit=10)

        optimizer = RuleBasedOptimizer(catalog)
        optimizer.add_rule(LimitPushdownRule())

        result = optimizer.optimize(limit)

        assert isinstance(result, Project)
        assert isinstance(result.input, Limit)

    def test_optimizer_reaches_fixed_point(self, catalog):
        """Test optimizer stops when no more changes occur."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name"]
        )

        optimizer = RuleBasedOptimizer(catalog)
        optimizer.add_rule(PredicatePushdownRule())

        result = optimizer.optimize(scan, max_iterations=10)

        assert result == scan


class TestComplexOptimizations:
    """Test complex optimization scenarios."""

    def test_predicate_and_limit_pushdown(self, catalog):
        """Test combined predicate and limit pushdown."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(scan, predicate)
        project = Project(
            input=filter_node,
            expressions=[ColumnRef(None, "id", DataType.INTEGER)],
            aliases=["id"]
        )
        limit = Limit(project, limit=10)

        optimizer = RuleBasedOptimizer(catalog)
        optimizer.add_rule(PredicatePushdownRule())
        optimizer.add_rule(LimitPushdownRule())

        result = optimizer.optimize(limit)

        assert result is not None

    def test_multiple_filter_merge(self, catalog):
        """Test merging multiple filters."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        pred1 = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        pred2 = BinaryOp(
            op=BinaryOpType.LT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(65, DataType.INTEGER)
        )
        pred3 = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )

        filter1 = Filter(scan, pred1)
        filter2 = Filter(filter1, pred2)
        filter3 = Filter(filter2, pred3)

        optimizer = RuleBasedOptimizer(catalog)
        optimizer.add_rule(PredicatePushdownRule())

        result = optimizer.optimize(filter3)

        assert isinstance(result, Scan)
        assert result.filters is not None
