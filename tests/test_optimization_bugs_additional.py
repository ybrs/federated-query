"""Tests for additional optimization bugs found in Phase 6."""

import pytest
from federated_query.optimizer.rules import (
    PredicatePushdownRule,
)
from federated_query.plan.logical import (
    Scan,
    Filter,
    Project,
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


class TestFilterThroughProjectionBug:
    """Test that filters actually push BELOW projections."""

    def test_filter_should_push_below_projection(self):
        """Test that filter on projected column pushes below projection.

        SELECT id FROM users WHERE id > 10

        Logical plan: Filter(Project(Scan(id, name, age), [id]), id > 10)

        Should optimize to: Project(Filter(Scan(id, name, age), id > 10), [id])

        This evaluates the filter BEFORE projection, which is more efficient.
        Currently BOTH branches of _push_filter_through_project return
        Filter(Project(...), predicate) so filter never moves below.
        """
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
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "id", DataType.INTEGER),
            right=Literal(10, DataType.INTEGER)
        )
        filter_node = Filter(project, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should be BELOW projection
        assert isinstance(result, Project), f"Expected Project, got {type(result).__name__}"

        # Filter should be pushed down (either as Filter node or merged into Scan)
        # Both Project(Filter(Scan)) and Project(Scan with filters) are valid
        is_pushed = (
            isinstance(result.input, Filter) or
            (isinstance(result.input, Scan) and result.input.filters is not None)
        )
        assert is_pushed, f"Filter should be pushed below projection, got {type(result.input).__name__}"

    def test_filter_on_unprojected_column_pushes_below(self):
        """Test that filter on column available in input pushes below projection.

        SELECT id FROM users WHERE age > 18

        Even though 'age' is not projected, it's available in the scan input,
        so filter CAN and SHOULD push below projection to evaluate earlier.

        Result: Project(Filter(Scan) or Scan with filters, [id])
        """
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
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(project, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should push below projection (age available in scan)
        assert isinstance(result, Project)
        # Either Filter(Scan) or Scan with filters
        is_pushed = (
            isinstance(result.input, Filter) or
            (isinstance(result.input, Scan) and result.input.filters is not None)
        )
        assert is_pushed, "Filter should push below projection"


class TestColumnDetectionForWrappedJoins:
    """Test that column detection works through Filter/Limit/etc wrapping."""

    def test_push_filter_with_filtered_join_inputs(self):
        """Test filter pushdown when join inputs are wrapped in filters.

        SELECT * FROM
          (SELECT * FROM orders WHERE amount > 50) o
          JOIN customers c ON o.customer_id = c.id
        WHERE c.status = 'active'

        The predicate `c.status = 'active'` references right side only.
        Currently _get_column_names returns empty set for Filter(Scan(...)),
        so pred_right_only is false and filter doesn't push.

        Should push the predicate to right side.
        """
        # Left side: Filter(Scan(...))
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "amount"]
        )
        left_predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "amount", DataType.DECIMAL),
            right=Literal(50, DataType.DECIMAL)
        )
        left_filtered = Filter(left_scan, left_predicate)

        # Right side: bare Scan
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
            left=left_filtered,  # Wrapped in Filter!
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Filter on right side
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should push to right side (even though left is wrapped)
        assert isinstance(result, Join)
        # Right side should now have a filter
        assert isinstance(result.right, Scan)
        assert result.right.filters is not None

    def test_push_filter_with_projected_join_inputs(self):
        """Test filter pushdown when join inputs are projections.

        SELECT * FROM
          (SELECT id, customer_id FROM orders) o
          JOIN (SELECT id, name FROM customers) c
          ON o.customer_id = c.id
        WHERE o.customer_id > 100

        Currently _get_column_names returns columns only for Project,
        but not for other wrappers. Need to handle this.
        """
        # Left side: Project(Scan(...))
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "amount"]
        )
        left_project = Project(
            input=left_scan,
            expressions=[
                ColumnRef(None, "id", DataType.INTEGER),
                ColumnRef(None, "customer_id", DataType.INTEGER)
            ],
            aliases=["id", "customer_id"]
        )

        # Right side: Project(Scan(...))
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name", "status"]
        )
        right_project = Project(
            input=right_scan,
            expressions=[
                ColumnRef(None, "id", DataType.INTEGER),
                ColumnRef(None, "name", DataType.VARCHAR)
            ],
            aliases=["id", "name"]
        )

        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("orders", "customer_id", DataType.INTEGER),
            right=ColumnRef("customers", "id", DataType.INTEGER)
        )
        join = Join(
            left=left_project,
            right=right_project,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Filter on left side column
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "customer_id", DataType.INTEGER),
            right=Literal(100, DataType.INTEGER)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should push to left side
        assert isinstance(result, Join)
        # Left side should be Project(Filter(Scan))
        assert isinstance(result.left, Project)
        # The filter should be below the project or in the scan
        # Either Project(Filter(Scan)) or Project(Scan with filters)
        has_filter = (
            isinstance(result.left.input, Filter) or
            (isinstance(result.left.input, Scan) and result.left.input.filters is not None)
        )
        assert has_filter, "Filter should have been pushed below projection"
