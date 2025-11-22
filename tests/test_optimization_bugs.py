"""Tests for optimization rule bugs and edge cases."""

import pytest
from federated_query.optimizer.rules import (
    PredicatePushdownRule,
    ProjectionPushdownRule,
    LimitPushdownRule,
)
from federated_query.plan.logical import (
    Scan,
    Filter,
    Projection,
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


class TestLimitPushdownSemantics:
    """Test that limit pushdown preserves query semantics."""

    def test_limit_should_not_push_through_filter(self):
        """Test that LIMIT does not push through filter.

        SELECT * FROM users WHERE age > 18 LIMIT 10

        Should be: Filter(Scan) -> Limit(Filter(Scan))
        NOT: Limit(Filter(Scan)) -> Filter(Limit(Scan))

        The second form would take 10 arbitrary rows then filter,
        potentially returning fewer than 10 rows.
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

    def test_limit_can_push_through_projection(self):
        """Test that LIMIT can safely push through projection.

        This is safe because projection doesn't change row count.
        """
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
        limit = Limit(project, limit=10)

        rule = LimitPushdownRule()
        result = rule.apply(limit)

        # Limit should push below projection
        assert isinstance(result, Projection)
        assert isinstance(result.input, Limit)
        assert result.input.limit == 10


class TestColumnPruningWithSelectStar:
    """Test that column pruning preserves SELECT * semantics."""

    def test_select_star_with_filter_keeps_all_columns(self):
        """Test that SELECT * with filter keeps all columns.

        SELECT * FROM users WHERE age > 18

        Should return all columns (id, name, age), not just age.
        Without an explicit projection, the scan should keep all columns.
        """
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age", "email"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(scan, predicate)

        rule = ProjectionPushdownRule()
        result = rule.apply(filter_node)

        # All columns should be kept (not just age)
        assert isinstance(result, Filter)
        assert isinstance(result.input, Scan)
        scan_cols = set(result.input.columns)
        # Should have all original columns, not just age
        assert scan_cols == {"id", "name", "age", "email"}

    def test_explicit_projection_prunes_correctly(self):
        """Test that explicit projection allows pruning.

        SELECT id, name FROM users WHERE age > 18

        Should only scan id, name, age (age needed for filter).
        """
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age", "email", "phone"]
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(scan, predicate)
        project = Projection(
            input=filter_node,
            expressions=[
                ColumnRef(None, "id", DataType.INTEGER),
                ColumnRef(None, "name", DataType.VARCHAR)
            ],
            aliases=["id", "name"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(project)

        # Should only keep id, name, age (not email, phone)
        assert isinstance(result, Projection)
        assert isinstance(result.input, Filter)
        assert isinstance(result.input.input, Scan)
        scan_cols = set(result.input.input.columns)
        assert scan_cols == {"id", "name", "age"}
        assert "email" not in scan_cols
        assert "phone" not in scan_cols


class TestOuterJoinFilterPushdown:
    """Test that filter pushdown respects outer join semantics."""

    def test_left_outer_join_right_filter_not_pushed(self):
        """Test that filters on right side of LEFT OUTER JOIN are not pushed.

        SELECT * FROM orders o LEFT OUTER JOIN customers c
          ON o.customer_id = c.id
        WHERE c.status = 'active'

        Pushing the filter below the join would change results:
        - Before: Join produces NULLs for non-matching orders, then filter removes them
        - After: Filter removes rows before join, changing which rows produce NULLs

        The filter must stay above the join for LEFT OUTER.
        """
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
            join_type=JoinType.LEFT,  # LEFT OUTER JOIN
            condition=join_condition
        )

        # Filter on right side column
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should NOT push below LEFT OUTER JOIN
        assert isinstance(result, Filter)
        assert isinstance(result.input, Join)
        assert result.input.join_type == JoinType.LEFT

    def test_inner_join_filter_can_push(self):
        """Test that filters can push below INNER JOIN.

        This is safe because INNER JOIN doesn't produce NULLs.
        """
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

        # Filter on right side column
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter SHOULD push below INNER JOIN
        assert isinstance(result, Join)
        assert isinstance(result.right, Scan)
        assert result.right.filters is not None

    def test_right_outer_join_left_filter_not_pushed(self):
        """Test that filters on left side of RIGHT OUTER JOIN are not pushed."""
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
            join_type=JoinType.RIGHT,  # RIGHT OUTER JOIN
            condition=join_condition
        )

        # Filter on left side column
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "amount", DataType.DECIMAL),
            right=Literal(100, DataType.DECIMAL)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should NOT push below RIGHT OUTER JOIN
        assert isinstance(result, Filter)
        assert isinstance(result.input, Join)
        assert result.input.join_type == JoinType.RIGHT
