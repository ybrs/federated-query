"""Tests for Phase 6 logical optimization rules."""

import pytest
from federated_query.optimizer.rules import (
    PredicatePushdownRule,
    ProjectionPushdownRule,
    OrderByPushdownRule,
    LimitPushdownRule,
    RuleBasedOptimizer,
)
from federated_query.catalog.catalog import Catalog
from federated_query.plan.logical import (
    Scan,
    Filter,
    Projection,
    Limit,
    Join,
    JoinType,
    Sort,
    Aggregate,
    Union,
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
        project = Projection(
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

    def test_push_filter_below_join_left_side(self):
        """Test pushing filter below join to left side."""
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "amount"]
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name"]
        )
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("orders", "customer_id", DataType.INTEGER),
            right=ColumnRef("customers", "id", DataType.INTEGER)
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "amount", DataType.DECIMAL),
            right=Literal(100, DataType.DECIMAL)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        assert isinstance(result, Join)
        assert isinstance(result.left, Scan)
        assert result.left.filters is not None

    def test_push_filter_below_join_right_side(self):
        """Test pushing filter below join to right side."""
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
            columns=["id", "name", "status"]
        )
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("orders", "customer_id", DataType.INTEGER),
            right=ColumnRef("customers", "id", DataType.INTEGER)
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        assert isinstance(result, Join)
        assert isinstance(result.right, Scan)
        assert result.right.filters is not None

    def test_filter_above_join_references_both_sides(self):
        """Test filter stays above join when referencing both sides."""
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "amount"]
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name", "credit_limit"]
        )
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("orders", "customer_id", DataType.INTEGER),
            right=ColumnRef("customers", "id", DataType.INTEGER)
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        predicate = BinaryOp(
            op=BinaryOpType.LT,
            left=ColumnRef(None, "amount", DataType.DECIMAL),
            right=ColumnRef(None, "credit_limit", DataType.DECIMAL)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        assert isinstance(result, Filter)
        assert isinstance(result.input, Join)


class TestProjectionPushdown:
    """Test projection pushdown optimization."""

    def test_collect_required_columns_from_scan_with_filter(self):
        """Test collecting columns from scan with filter."""
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"],
            filters=predicate
        )

        rule = ProjectionPushdownRule()
        columns = rule._collect_required_columns(scan)

        assert "age" in columns

    def test_prune_unused_columns_from_scan(self):
        """Test pruning unused columns from scan."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age", "email", "phone"]
        )
        project = Projection(
            input=scan,
            expressions=[
                ColumnRef(None, "id", DataType.INTEGER),
                ColumnRef(None, "name", DataType.VARCHAR)
            ],
            aliases=["id", "name"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(project)

        assert isinstance(result, Projection)
        assert isinstance(result.input, Scan)
        assert set(result.input.columns) == {"id", "name"}

    def test_keep_columns_needed_by_filter(self):
        """Test columns needed by filter are not pruned."""
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
        project = Projection(
            input=filter_node,
            expressions=[ColumnRef(None, "name", DataType.VARCHAR)],
            aliases=["name"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(project)

        assert isinstance(result, Projection)
        assert isinstance(result.input, Filter)
        assert isinstance(result.input.input, Scan)
        scan_cols = set(result.input.input.columns)
        assert "name" in scan_cols
        assert "age" in scan_cols

    def test_collect_required_columns_from_project(self):
        """Test collecting columns from projection."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
        )
        project = Projection(
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
        project = Projection(
            input=scan,
            expressions=[ColumnRef(None, "id", DataType.INTEGER)],
            aliases=["id"]
        )
        limit = Limit(project, limit=10)

        rule = LimitPushdownRule()
        result = rule.apply(limit)

        assert isinstance(result, Projection)
        assert isinstance(result.input, Limit)
        assert result.input.limit == 10

    def test_limit_does_not_push_through_filter(self):
        """Test that limit does NOT push through filter.

        This would change query semantics - filter must execute first.
        """
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

        # Limit should stay above filter
        assert isinstance(result, Limit)
        assert isinstance(result.input, Filter)
        assert result.limit == 10

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


class TestOrderByPushdown:
    """Test ORDER BY pushdown optimization."""

    def test_order_by_alias_pushdown(self):
        """Sort on alias should push to scan."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["order_id", "amount"]
        )
        project = Projection(
            input=scan,
            expressions=[ColumnRef(None, "order_id", DataType.INTEGER)],
            aliases=["oid"]
        )
        sort = Sort(
            input=project,
            sort_keys=[ColumnRef(None, "order_id", DataType.INTEGER)],
            ascending=[True],
            nulls_order=[None],
        )

        rule = OrderByPushdownRule()
        result = rule.apply(sort)

        assert isinstance(result, Projection)
        assert isinstance(result.input, Scan)
        assert result.input.order_by_keys is not None
        assert result.input.order_by_keys[0].column == "order_id"

    def test_order_by_join_side_pushdown(self):
        """Sort on one join side should annotate that side but keep top sort."""
        left = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "cid"]
        )
        right = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name"]
        )
        join = Join(left, right, JoinType.INNER, None)
        sort = Sort(
            input=join,
            sort_keys=[ColumnRef("orders", "id", DataType.INTEGER)],
            ascending=[True],
            nulls_order=[None],
        )

        rule = OrderByPushdownRule()
        result = rule.apply(sort)

        assert isinstance(result, Sort)
        assert isinstance(result.input, Join)
        assert isinstance(result.input.left, Scan)
        assert result.input.left.order_by_keys is not None
        assert result.input.left.order_by_keys[0].column == "id"
        assert result.input.right.order_by_keys is None

    def test_order_by_expression_not_pushed(self):
        """Non-column sort keys keep the top Sort but annotate scan metadata."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["amount"]
        )
        from federated_query.plan.expressions import BinaryOp, BinaryOpType
        expr = BinaryOp(
            op=BinaryOpType.ADD,
            left=ColumnRef(None, "amount", DataType.INTEGER),
            right=Literal(1, DataType.INTEGER),
        )
        sort = Sort(
            input=scan,
            sort_keys=[expr],
            ascending=[True],
            nulls_order=[None],
        )

        rule = OrderByPushdownRule()
        result = rule.apply(sort)

        assert isinstance(result, Sort)
        assert isinstance(result.input, Scan)
        assert result.input.order_by_keys is not None
        assert result.input.order_by_keys[0] == expr

    def test_order_by_group_by_pushdown(self):
        """Sort on group-by column should annotate scan when aggregate is pushed."""
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["cid", "amount"]
        )
        agg = Aggregate(
            input=scan,
            group_by=[ColumnRef(None, "cid", DataType.INTEGER)],
            aggregates=[
                ColumnRef(None, "cid", DataType.INTEGER),
            ],
            output_names=["cid"],
        )
        sort = Sort(
            input=agg,
            sort_keys=[ColumnRef(None, "cid", DataType.INTEGER)],
            ascending=[True],
            nulls_order=[None],
        )

        rule = OrderByPushdownRule()
        result = rule.apply(sort)

        assert isinstance(result, Sort)
        assert isinstance(result.input, Aggregate)
        assert isinstance(result.input.input, Scan)
        assert result.input.input.order_by_keys is not None
        assert result.input.input.order_by_keys[0].column == "cid"

    def test_order_by_union_pushdown(self):
        """Sort above UNION should propagate metadata into children."""
        left = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["cid"],
        )
        right = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders_backup",
            columns=["cid"],
        )
        union = Union([left, right], distinct=True)
        sort = Sort(
            input=union,
            sort_keys=[ColumnRef(None, "cid", DataType.INTEGER)],
            ascending=[True],
            nulls_order=[None],
        )

        rule = OrderByPushdownRule()
        result = rule.apply(sort)

        assert isinstance(result, Sort)
        assert isinstance(result.input, Union)
        for child in result.input.inputs:
            assert isinstance(child, Scan)
            if child.order_by_keys:
                assert child.order_by_keys[0].column == "cid"


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
        project = Projection(
            input=scan,
            expressions=[ColumnRef(None, "id", DataType.INTEGER)],
            aliases=["id"]
        )
        limit = Limit(project, limit=10)

        optimizer = RuleBasedOptimizer(catalog)
        optimizer.add_rule(LimitPushdownRule())

        result = optimizer.optimize(limit)

        assert isinstance(result, Projection)
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
        project = Projection(
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
