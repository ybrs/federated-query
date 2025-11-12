"""Tests for additional optimization bugs found in Phase 6."""

import pytest
from federated_query.optimizer.rules import (
    PredicatePushdownRule,
    LimitPushdownRule,
)
from federated_query.plan.logical import (
    Scan,
    Filter,
    Project,
    Join,
    JoinType,
    Limit,
)
from federated_query.plan.expressions import (
    BinaryOp,
    ColumnRef,
    Literal,
    BinaryOpType,
    DataType,
    FunctionCall,
)


class TestFunctionCallColumnExtraction:
    """Test that column extraction handles FunctionCall expressions."""

    def test_filter_with_function_call_should_not_push_incorrectly(self):
        """Test that filter with FunctionCall doesn't push incorrectly.

        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE ABS(o.amount - c.credit_limit) > 100

        The predicate references BOTH sides (o.amount and c.credit_limit).
        Currently _extract_column_refs returns empty set for FunctionCall,
        so pred_cols = {} and all(c in left_cols for c in {}) = True,
        causing the filter to push to left side incorrectly!

        The filter should stay ABOVE the join since it references both sides.
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

        # Filter with FunctionCall: ABS(o.amount - c.credit_limit) > 100
        abs_arg = BinaryOp(
            op=BinaryOpType.SUBTRACT,
            left=ColumnRef(None, "amount", DataType.DECIMAL),
            right=ColumnRef(None, "credit_limit", DataType.DECIMAL)
        )
        abs_call = FunctionCall(
            function_name="ABS",
            args=[abs_arg]
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=abs_call,
            right=Literal(100, DataType.DECIMAL)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should stay ABOVE join (references both sides)
        assert isinstance(result, Filter), f"Expected Filter above join, got {type(result).__name__}"
        assert isinstance(result.input, Join), f"Expected Join below filter, got {type(result.input).__name__}"

    def test_filter_with_coalesce_referencing_both_sides(self):
        """Test COALESCE with columns from both join sides.

        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE COALESCE(o.priority, c.default_priority) = 'high'

        COALESCE references both o.priority and c.default_priority.
        Should NOT push below join.
        """
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "priority"]
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name", "default_priority"]
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

        # COALESCE(o.priority, c.default_priority) = 'high'
        coalesce_call = FunctionCall(
            function_name="COALESCE",
            args=[
                ColumnRef(None, "priority", DataType.VARCHAR),
                ColumnRef(None, "default_priority", DataType.VARCHAR)
            ]
        )
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=coalesce_call,
            right=Literal("high", DataType.VARCHAR)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should stay ABOVE join
        assert isinstance(result, Filter)
        assert isinstance(result.input, Join)

    def test_filter_with_function_call_single_side_can_push(self):
        """Test that FunctionCall referencing only one side CAN push.

        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE UPPER(c.name) = 'ACME'

        UPPER(c.name) only references right side, so it CAN push to right.
        """
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

        # UPPER(c.name) = 'ACME'
        upper_call = FunctionCall(
            function_name="UPPER",
            args=[ColumnRef(None, "name", DataType.VARCHAR)]
        )
        predicate = BinaryOp(
            op=BinaryOpType.EQ,
            left=upper_call,
            right=Literal("ACME", DataType.VARCHAR)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should push to right side
        assert isinstance(result, Join)
        assert isinstance(result.right, Scan)
        assert result.right.filters is not None


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


class TestTableQualifierBug:
    """Test that table qualifiers are respected in predicate pushdown.

    Bug: When both join inputs have columns with the same name (e.g., orders.id
    and customers.id), filters with qualified references are incorrectly pushed
    to the wrong side because _extract_column_refs ignores table qualifiers.
    """

    def test_filter_with_qualified_column_pushes_to_correct_side(self):
        """Test filter with qualified column name pushes to correct side.

        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE customers.id > 100

        Both tables have "id" column. The filter explicitly references
        customers.id, so it MUST push to right side only.

        BUG: Currently _extract_column_refs returns {"id"} and _get_column_names
        returns {"id", "customer_id", ...} for left and {"id", "name", ...} for
        right. Since "id" is in both, pred_left_only = True (checked first),
        and filter incorrectly pushes to LEFT (orders.id > 100) instead of
        RIGHT (customers.id > 100).
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

        # Filter: customers.id > 100 (QUALIFIED reference to right side)
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef("customers", "id", DataType.INTEGER),
            right=Literal(100, DataType.INTEGER)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter MUST push to RIGHT side (customers), not left (orders)
        assert isinstance(result, Join), f"Expected Join, got {type(result).__name__}"

        # Right side should have the filter
        assert isinstance(result.right, Scan)
        assert result.right.filters is not None, "Filter should push to RIGHT side (customers)"

        # Left side should NOT have this filter
        assert isinstance(result.left, Scan)
        # Left side can have other filters, but not THIS one
        # We can't directly check this without inspecting filter content

    def test_filter_with_left_qualified_column_pushes_to_left(self):
        """Test filter with left-qualified column pushes to left side.

        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE orders.id > 100

        Both tables have "id". Filter references orders.id, so must push LEFT.
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

        # Filter: orders.id > 100 (QUALIFIED reference to left side)
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef("orders", "id", DataType.INTEGER),
            right=Literal(100, DataType.INTEGER)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter MUST push to LEFT side (orders), not right
        assert isinstance(result, Join)

        # Left side should have the filter
        assert isinstance(result.left, Scan)
        assert result.left.filters is not None, "Filter should push to LEFT side (orders)"

    def test_unqualified_column_with_collision_stays_above_join(self):
        """Test unqualified column reference with name collision stays above join.

        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE id > 100  -- Ambiguous! Could be orders.id or customers.id

        When column name is ambiguous and not qualified, safest behavior is to
        keep filter ABOVE join to avoid incorrect results.
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

        # Filter: id > 100 (UNQUALIFIED - ambiguous!)
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "id", DataType.INTEGER),  # No table qualifier!
            right=Literal(100, DataType.INTEGER)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should stay ABOVE join (ambiguous column reference)
        assert isinstance(result, Filter), "Ambiguous filter should stay above join"
        assert isinstance(result.input, Join)

    def test_qualified_column_in_complex_expression(self):
        """Test qualified columns in complex expression push correctly.

        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE orders.amount + customers.credit_limit > 1000

        Predicate references BOTH sides (orders.amount, customers.credit_limit),
        so must stay ABOVE join.
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

        # orders.amount + customers.credit_limit > 1000
        sum_expr = BinaryOp(
            op=BinaryOpType.ADD,
            left=ColumnRef("orders", "amount", DataType.DECIMAL),
            right=ColumnRef("customers", "credit_limit", DataType.DECIMAL)
        )
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=sum_expr,
            right=Literal(1000, DataType.DECIMAL)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter references BOTH sides, must stay above join
        assert isinstance(result, Filter)
        assert isinstance(result.input, Join)


class TestTableAliasBug:
    """Test that table aliases are handled correctly in predicate pushdown.

    Bug: _get_column_names qualifies scan columns with plan.table_name (physical name),
    but ColumnRef.table carries the alias used in the query. When a table is aliased
    (e.g., FROM users u WHERE u.age > 18), _can_evaluate_predicate compares the
    predicate column "u.age" against available names "users.age" and refuses to push
    the filter, leaving filters above scans and hurting performance.
    """

    def test_filter_with_aliased_table_should_push(self):
        """Test filter with aliased table reference pushes to scan.

        SELECT * FROM users u WHERE u.age > 18

        The table is aliased as "u", and the predicate uses the alias.
        Currently:
        - _get_column_names returns {"users.id", "users.name", "users.age"}
        - _extract_column_refs returns {"u.age"}
        - _can_evaluate_predicate compares "u.age" against "users.age" and fails
        - Filter stays above scan instead of pushing down

        The filter should push to the scan for better performance.
        """
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"],
            alias="u"  # Table is aliased as "u"
        )

        # Filter: u.age > 18 (uses alias "u")
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef("u", "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(scan, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should push to scan
        assert isinstance(result, Scan), f"Expected Scan with filter, got {type(result).__name__}"
        assert result.filters is not None, "Filter should push to scan"

    def test_aliased_join_filter_should_push(self):
        """Test filter on aliased join pushes correctly.

        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.amount > 100

        Both tables are aliased. The filter uses the alias "o".
        Currently fails to push because "o.amount" doesn't match "orders.amount".
        """
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "amount"],
            alias="o"  # Table is aliased as "o"
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name", "credit_limit"],
            alias="c"  # Table is aliased as "c"
        )
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("o", "customer_id", DataType.INTEGER),
            right=ColumnRef("c", "id", DataType.INTEGER)
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Filter: o.amount > 100 (uses alias "o")
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef("o", "amount", DataType.DECIMAL),
            right=Literal(100, DataType.DECIMAL)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should push to left side (orders)
        assert isinstance(result, Join), f"Expected Join, got {type(result).__name__}"
        assert isinstance(result.left, Scan)
        assert result.left.filters is not None, "Filter should push to left side (orders)"

    def test_mixed_alias_and_physical_name(self):
        """Test query mixing physical names and aliases.

        SELECT * FROM orders o
        JOIN customers ON o.customer_id = customers.id
        WHERE o.amount > 100 AND customers.status = 'active'

        Left table is aliased, right table is not.
        Both filters should push correctly.
        """
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "amount"],
            alias="o"  # Table is aliased as "o"
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name", "status"]
            # No alias - uses physical table name
        )
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("o", "customer_id", DataType.INTEGER),
            right=ColumnRef("customers", "id", DataType.INTEGER)
        )
        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Filter: o.amount > 100 AND customers.status = 'active'
        left_pred = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef("o", "amount", DataType.DECIMAL),
            right=Literal(100, DataType.DECIMAL)
        )
        right_pred = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("customers", "status", DataType.VARCHAR),
            right=Literal("active", DataType.VARCHAR)
        )
        predicate = BinaryOp(
            op=BinaryOpType.AND,
            left=left_pred,
            right=right_pred
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Both filters should push to their respective sides
        # (This is a complex case - ideally both predicates would split and push)
        # For now, at minimum, the rule should not fail
        assert result is not None


class TestColumnPruningJoinKeyBug:
    """Test that column pruning preserves columns needed by join conditions.

    Bug: _collect_required_columns stops at Project nodes and doesn't recurse
    into plan.input. This means columns needed by downstream operators (like
    join keys) are omitted from the required set. When _prune_scan_columns runs,
    it can drop join condition columns, breaking the join.
    """

    def test_projection_over_join_preserves_join_keys(self):
        """Test that join keys are preserved even when not in projection.

        SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id

        The projection only references u.name, but the join needs u.id and
        o.user_id. Currently _collect_required_columns stops at the Project
        and only records {name}, causing u.id to be pruned from the scan.

        This makes the join impossible to execute!
        """
        from federated_query.optimizer.rules import ProjectionPushdownRule

        # Left scan: users
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "email"],
            alias="u"
        )

        # Right scan: orders
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "user_id", "amount"],
            alias="o"
        )

        # Join condition: u.id = o.user_id
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("u", "id", DataType.INTEGER),
            right=ColumnRef("o", "user_id", DataType.INTEGER)
        )

        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Project: SELECT u.name (only projects name, not id!)
        project = Project(
            input=join,
            expressions=[ColumnRef("u", "name", DataType.VARCHAR)],
            aliases=["name"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(project)

        # Result should still be a projection
        assert isinstance(result, Project)

        # The join should still have both join key columns
        # Navigate: Project -> Join -> left/right scans
        assert isinstance(result.input, Join)
        resulting_join = result.input

        # Left side (users) must have 'id' column for join condition
        assert isinstance(resulting_join.left, Scan)
        assert "id" in resulting_join.left.columns, "users.id must be preserved for join key"
        assert "name" in resulting_join.left.columns, "users.name needed for projection"

        # Right side (orders) must have 'user_id' column for join condition
        assert isinstance(resulting_join.right, Scan)
        assert "user_id" in resulting_join.right.columns, "orders.user_id must be preserved for join key"

    def test_projection_over_filtered_join_preserves_filter_and_join_columns(self):
        """Test that both filter and join columns are preserved.

        SELECT u.name FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE o.amount > 100

        Projection needs: u.name
        Filter needs: o.amount
        Join needs: u.id, o.user_id

        All must be preserved even though only u.name is in the final projection.
        """
        from federated_query.optimizer.rules import ProjectionPushdownRule

        # Left scan: users
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "email"],
            alias="u"
        )

        # Right scan: orders
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "user_id", "amount"],
            alias="o"
        )

        # Join condition: u.id = o.user_id
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("u", "id", DataType.INTEGER),
            right=ColumnRef("o", "user_id", DataType.INTEGER)
        )

        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Filter: o.amount > 100
        filter_pred = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef("o", "amount", DataType.DECIMAL),
            right=Literal(100, DataType.DECIMAL)
        )
        filter_node = Filter(join, filter_pred)

        # Project: SELECT u.name
        project = Project(
            input=filter_node,
            expressions=[ColumnRef("u", "name", DataType.VARCHAR)],
            aliases=["name"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(project)

        # Find the scans (may be wrapped in filters)
        assert isinstance(result, Project)

        # Navigate down to find scans
        # Could be: Project -> Filter -> Join -> Scans
        # Or: Project -> Join -> Scans (if filter pushed down)
        current = result.input
        if isinstance(current, Filter):
            current = current.input

        assert isinstance(current, Join)
        join_node = current

        # Check left side (users) - may be wrapped in filter
        left_node = join_node.left
        if isinstance(left_node, Filter):
            left_node = left_node.input
        assert isinstance(left_node, Scan)
        assert "id" in left_node.columns, "users.id needed for join"
        assert "name" in left_node.columns, "users.name needed for projection"

        # Check right side (orders) - may be wrapped in filter
        right_node = join_node.right
        if isinstance(right_node, Filter):
            right_node = right_node.input
        assert isinstance(right_node, Scan)
        assert "user_id" in right_node.columns, "orders.user_id needed for join"
        assert "amount" in right_node.columns, "orders.amount needed for filter"

    def test_nested_projections_preserve_all_needed_columns(self):
        """Test nested projections preserve columns needed at each level.

        SELECT name FROM (
          SELECT u.name, u.id FROM users u
          JOIN orders o ON u.id = o.user_id
        )

        Inner projection needs: u.name, u.id (and join keys)
        Outer projection needs: name
        Join needs: u.id, o.user_id

        The u.id column is in the inner projection but not the outer one.
        It should still be preserved for the join.
        """
        from federated_query.optimizer.rules import ProjectionPushdownRule

        # Left scan: users
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "email"],
            alias="u"
        )

        # Right scan: orders
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "user_id", "amount"],
            alias="o"
        )

        # Join condition: u.id = o.user_id
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("u", "id", DataType.INTEGER),
            right=ColumnRef("o", "user_id", DataType.INTEGER)
        )

        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Inner projection: SELECT u.name, u.id
        inner_project = Project(
            input=join,
            expressions=[
                ColumnRef("u", "name", DataType.VARCHAR),
                ColumnRef("u", "id", DataType.INTEGER)
            ],
            aliases=["name", "id"]
        )

        # Outer projection: SELECT name
        outer_project = Project(
            input=inner_project,
            expressions=[ColumnRef(None, "name", DataType.VARCHAR)],
            aliases=["name"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(outer_project)

        # Navigate down to find the join and scans
        current = result
        while isinstance(current, Project):
            current = current.input

        assert isinstance(current, Join)
        join_node = current

        # Left side must have both id and name
        assert isinstance(join_node.left, Scan)
        assert "id" in join_node.left.columns, "users.id needed for join"
        assert "name" in join_node.left.columns, "users.name needed for projection"

        # Right side must have user_id
        assert isinstance(join_node.right, Scan)
        assert "user_id" in join_node.right.columns, "orders.user_id needed for join"


class TestAggregateColumnPruningBug:
    """Test that column pruning preserves columns needed by aggregate children.

    Bug: _collect_required_columns stops at Aggregate nodes and doesn't recurse
    into plan.input. This means columns needed by downstream operators (like
    join keys, filter columns) are omitted from the required set. When
    _prune_scan_columns runs, it can drop these columns, breaking the query.
    """

    def test_aggregate_over_join_preserves_join_keys(self):
        """Test that join keys are preserved for aggregates over joins.

        SELECT c.country, SUM(o.total)
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.country

        The aggregate collects c.country and o.total, but the join needs
        c.id and o.customer_id. Without recursion, these join keys are
        pruned and the join becomes impossible to execute!
        """
        from federated_query.optimizer.rules import ProjectionPushdownRule
        from federated_query.plan.logical import Aggregate

        # Left scan: customers
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name", "country"],
            alias="c"
        )

        # Right scan: orders
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "total"],
            alias="o"
        )

        # Join condition: c.id = o.customer_id
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("c", "id", DataType.INTEGER),
            right=ColumnRef("o", "customer_id", DataType.INTEGER)
        )

        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Aggregate: SELECT c.country, SUM(o.total) GROUP BY c.country
        # For testing purposes, just use column refs for aggregates
        group_by_expr = ColumnRef("c", "country", DataType.VARCHAR)
        sum_expr = ColumnRef("o", "total", DataType.DECIMAL)

        aggregate = Aggregate(
            input=join,
            group_by=[group_by_expr],
            aggregates=[sum_expr],
            output_names=["country", "sum_total"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(aggregate)

        # Result should still be an aggregate
        assert isinstance(result, Aggregate)

        # Navigate to the join
        assert isinstance(result.input, Join)
        join_node = result.input

        # Left side (customers) must have 'id' column for join
        assert isinstance(join_node.left, Scan)
        assert "id" in join_node.left.columns, "customers.id must be preserved for join key"
        assert "country" in join_node.left.columns, "customers.country needed for group by"

        # Right side (orders) must have 'customer_id' column for join
        assert isinstance(join_node.right, Scan)
        assert "customer_id" in join_node.right.columns, "orders.customer_id must be preserved for join key"
        assert "total" in join_node.right.columns, "orders.total needed for SUM"

    def test_aggregate_over_filter_preserves_filter_columns(self):
        """Test that filter columns are preserved for aggregates over filters.

        SELECT COUNT(*)
        FROM (SELECT * FROM orders WHERE status = 'pending') AS pending_orders

        The aggregate is just COUNT(*), contributing no columns. Without
        recursion into the filter, the 'status' column is not marked as
        required and gets pruned, making the filter impossible to evaluate!
        """
        from federated_query.optimizer.rules import ProjectionPushdownRule
        from federated_query.plan.logical import Aggregate

        # Scan: orders
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "status", "total"]
        )

        # Filter: WHERE status = 'pending'
        filter_pred = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "status", DataType.VARCHAR),
            right=Literal("pending", DataType.VARCHAR)
        )
        filter_node = Filter(scan, filter_pred)

        # Aggregate: SELECT COUNT(*)
        # For testing purposes, use a simple literal for COUNT(*)
        count_expr = Literal(1, DataType.INTEGER)

        aggregate = Aggregate(
            input=filter_node,
            group_by=[],
            aggregates=[count_expr],
            output_names=["count"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(aggregate)

        # Result should still be an aggregate
        assert isinstance(result, Aggregate)

        # Navigate to find the scan (may be wrapped in filter)
        current = result.input
        if isinstance(current, Filter):
            current = current.input

        assert isinstance(current, Scan)
        # The 'status' column MUST be preserved for the filter
        assert "status" in current.columns, "orders.status must be preserved for filter"

    def test_aggregate_over_filtered_join_preserves_all_columns(self):
        """Test that all columns are preserved for complex aggregate queries.

        SELECT c.country, COUNT(*)
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        WHERE o.status = 'completed'
        GROUP BY c.country

        Needs:
        - Group by: c.country
        - Join keys: c.id, o.customer_id
        - Filter: o.status

        All must be preserved!
        """
        from federated_query.optimizer.rules import ProjectionPushdownRule
        from federated_query.plan.logical import Aggregate

        # Left scan: customers
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name", "country"],
            alias="c"
        )

        # Right scan: orders
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "status", "total"],
            alias="o"
        )

        # Join condition: c.id = o.customer_id
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("c", "id", DataType.INTEGER),
            right=ColumnRef("o", "customer_id", DataType.INTEGER)
        )

        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Filter: WHERE o.status = 'completed'
        filter_pred = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("o", "status", DataType.VARCHAR),
            right=Literal("completed", DataType.VARCHAR)
        )
        filter_node = Filter(join, filter_pred)

        # Aggregate: SELECT c.country, COUNT(*) GROUP BY c.country
        group_by_expr = ColumnRef("c", "country", DataType.VARCHAR)
        count_expr = Literal(1, DataType.INTEGER)  # COUNT(*)

        aggregate = Aggregate(
            input=filter_node,
            group_by=[group_by_expr],
            aggregates=[count_expr],
            output_names=["country", "count"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(aggregate)

        # Result should still be an aggregate
        assert isinstance(result, Aggregate)

        # Navigate down to find the join (may have filter between)
        current = result.input
        if isinstance(current, Filter):
            current = current.input

        assert isinstance(current, Join)
        join_node = current

        # Check left side (customers)
        left_node = join_node.left
        if isinstance(left_node, Filter):
            left_node = left_node.input
        assert isinstance(left_node, Scan)
        assert "id" in left_node.columns, "customers.id needed for join"
        assert "country" in left_node.columns, "customers.country needed for group by"

        # Check right side (orders)
        right_node = join_node.right
        if isinstance(right_node, Filter):
            right_node = right_node.input
        assert isinstance(right_node, Scan)
        assert "customer_id" in right_node.columns, "orders.customer_id needed for join"
        assert "status" in right_node.columns, "orders.status needed for filter"


class TestParserAliasNotPopulatedBug:
    """Test that predicate pushdown works when parser doesn't populate Scan.alias.

    Bug: While we added Scan.alias field and updated _get_column_names to use it,
    the parser never actually populates this field when creating Scan nodes.
    When a query uses aliases (FROM users u WHERE u.age > 18), the parser creates:
    - Scan(table_name="users", alias=None)  # alias NOT populated!
    - ColumnRef("u", "age")  # but column refs USE the alias

    _get_column_names returns "users.age" (physical name) but the predicate has
    "u.age" (alias), causing mismatch and preventing pushdown even though it's safe.
    """

    def test_alias_in_columnref_but_not_in_scan(self):
        """Test filter pushdown when ColumnRef uses alias but Scan.alias is None.

        This simulates what the parser actually creates:
        SELECT * FROM users u WHERE u.age > 18

        Parser creates:
        - Scan(table_name="users")  # alias=None (default)
        - ColumnRef("u", "age")     # uses alias "u"

        The filter should still push to scan.
        """
        # Scan WITHOUT alias field populated (as parser would create)
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age"]
            # NO alias parameter - defaults to None
        )

        # Filter uses alias "u" (as parser would create)
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef("u", "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(scan, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should push to scan despite alias mismatch
        assert isinstance(result, Scan), f"Expected Scan with filter, got {type(result).__name__}"
        assert result.filters is not None, "Filter should push to scan even with alias in ColumnRef"

    def test_join_with_aliases_in_columnref_but_not_in_scan(self):
        """Test join filter pushdown when aliases are in ColumnRef but not Scan.

        SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id
        WHERE o.amount > 100

        Parser would create:
        - Scan(table_name="orders", alias=None)
        - Scan(table_name="customers", alias=None)
        - ColumnRef("o", "amount")
        - Join condition with ColumnRef("o", ...) and ColumnRef("c", ...)
        """
        # Scans WITHOUT alias populated
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "amount"]
            # NO alias - defaults to None
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name", "credit_limit"]
            # NO alias - defaults to None
        )

        # Join condition uses aliases "o" and "c"
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("o", "customer_id", DataType.INTEGER),
            right=ColumnRef("c", "id", DataType.INTEGER)
        )

        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Filter uses alias "o"
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef("o", "amount", DataType.DECIMAL),
            right=Literal(100, DataType.DECIMAL)
        )
        filter_node = Filter(join, predicate)

        rule = PredicatePushdownRule()
        result = rule.apply(filter_node)

        # Filter should push to left side despite alias mismatch
        assert isinstance(result, Join), f"Expected Join, got {type(result).__name__}"
        assert isinstance(result.left, Scan)
        assert result.left.filters is not None, "Filter should push to left side even with alias in ColumnRef"

    def test_aggregate_with_aliases_in_columnref_but_not_in_scan(self):
        """Test column pruning for aggregate when aliases in ColumnRef but not Scan.

        SELECT c.country, SUM(o.total)
        FROM customers c JOIN orders o ON c.id = o.customer_id
        GROUP BY c.country

        Parser would create Scan nodes with alias=None but ColumnRefs with aliases.
        Column pruning should still preserve join keys.
        """
        from federated_query.optimizer.rules import ProjectionPushdownRule
        from federated_query.plan.logical import Aggregate

        # Scans WITHOUT alias populated
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name", "country"]
            # NO alias
        )
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "total"]
            # NO alias
        )

        # Join condition uses aliases "c" and "o"
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef("c", "id", DataType.INTEGER),
            right=ColumnRef("o", "customer_id", DataType.INTEGER)
        )

        join = Join(
            left=left_scan,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Aggregate uses aliases
        group_by_expr = ColumnRef("c", "country", DataType.VARCHAR)
        sum_expr = ColumnRef("o", "total", DataType.DECIMAL)

        aggregate = Aggregate(
            input=join,
            group_by=[group_by_expr],
            aggregates=[sum_expr],
            output_names=["country", "sum_total"]
        )

        rule = ProjectionPushdownRule()
        result = rule.apply(aggregate)

        # Join keys should be preserved despite alias mismatch
        assert isinstance(result, Aggregate)
        assert isinstance(result.input, Join)
        join_node = result.input

        # Both scans should have join keys preserved
        assert isinstance(join_node.left, Scan)
        assert "id" in join_node.left.columns, "customers.id must be preserved for join key"

        assert isinstance(join_node.right, Scan)
        assert "customer_id" in join_node.right.columns, "orders.customer_id must be preserved for join key"


class TestLimitPushdownNotRecursing:
    """Test Bug #12: LimitPushdownRule doesn't recurse into non-Limit/Project nodes.

    The rule only descends into Limit and Project nodes. If the root plan is a Filter,
    Join, or other operator, _push_limit returns without visiting children. This means
    limits embedded inside subqueries (e.g., Filter(Limit(Project(Scan)))) are never
    considered for pushdown even though moving the limit below the projection is safe.
    """

    def test_limit_nested_under_filter_not_optimized(self):
        """Test that limit nested under filter is not being optimized.

        Plan shape: Filter(Limit(Project(Scan)))

        The Limit should be able to push below the Project to become:
        Filter(Project(Limit(Scan)))

        But currently the rule never visits the Limit because it stops at Filter.
        """
        scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="users",
            columns=["id", "name", "age", "email"]
        )

        # Project: SELECT id, name, age FROM ...
        project = Project(
            input=scan,
            expressions=[
                ColumnRef(None, "id", DataType.INTEGER),
                ColumnRef(None, "name", DataType.VARCHAR),
                ColumnRef(None, "age", DataType.INTEGER),
            ],
            aliases=["id", "name", "age"]
        )

        # Limit: ... LIMIT 10
        limit = Limit(input=project, limit=10, offset=0)

        # Filter: WHERE age > 18
        predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "age", DataType.INTEGER),
            right=Literal(18, DataType.INTEGER)
        )
        filter_node = Filter(input=limit, predicate=predicate)

        # Apply limit pushdown
        rule = LimitPushdownRule()
        result = rule.apply(filter_node)

        # Currently the rule doesn't recurse into Filter, so nothing changes
        # After fix: should have Filter(Project(Limit(Scan)))
        assert isinstance(result, Filter), "Should still be Filter at root"
        assert isinstance(result.input, Project), "Should have Project after pushing limit down"
        assert isinstance(result.input.input, Limit), "Limit should have pushed below Project"
        assert isinstance(result.input.input.input, Scan), "Limit should be above Scan"

    def test_limit_nested_under_join_not_optimized(self):
        """Test that limit nested in join child is not being optimized.

        Plan shape: Join(Limit(Project(Scan)), Scan)

        The Limit in the left child should be able to push below the Project,
        but currently the rule doesn't recurse into Join children.
        """
        # Left side: Limit(Project(Scan))
        left_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "customer_id", "amount"]
        )

        left_project = Project(
            input=left_scan,
            expressions=[
                ColumnRef(None, "customer_id", DataType.INTEGER),
                ColumnRef(None, "amount", DataType.DECIMAL),
            ],
            aliases=["customer_id", "amount"]
        )

        left_limit = Limit(input=left_project, limit=100, offset=0)

        # Right side: plain Scan
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="customers",
            columns=["id", "name"]
        )

        # Join
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "customer_id", DataType.INTEGER),
            right=ColumnRef(None, "id", DataType.INTEGER)
        )

        join = Join(
            left=left_limit,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Apply limit pushdown
        rule = LimitPushdownRule()
        result = rule.apply(join)

        # Currently the rule doesn't recurse into Join, so nothing changes
        # After fix: left child should be Project(Limit(Scan))
        assert isinstance(result, Join), "Should still be Join at root"
        assert isinstance(result.left, Project), "Left child should have Project after pushing limit"
        assert isinstance(result.left.input, Limit), "Limit should have pushed below Project"
        assert isinstance(result.left.input.input, Scan), "Limit should be above Scan"

    def test_deeply_nested_limit_not_optimized(self):
        """Test that deeply nested limit is not being optimized.

        Plan shape: Filter(Join(Filter(Limit(Project(Scan))), Scan))

        The deeply nested Limit should still be optimized even though it's
        nested under multiple layers of non-Limit/Project nodes.
        """
        # Deep left side: Filter(Limit(Project(Scan)))
        inner_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="products",
            columns=["id", "category", "price", "stock"]
        )

        inner_project = Project(
            input=inner_scan,
            expressions=[
                ColumnRef(None, "id", DataType.INTEGER),
                ColumnRef(None, "price", DataType.DECIMAL),
            ],
            aliases=["id", "price"]
        )

        inner_limit = Limit(input=inner_project, limit=50, offset=0)

        inner_predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "price", DataType.DECIMAL),
            right=Literal(10.0, DataType.DECIMAL)
        )
        inner_filter = Filter(input=inner_limit, predicate=inner_predicate)

        # Right side
        right_scan = Scan(
            datasource="test_ds",
            schema_name="public",
            table_name="orders",
            columns=["id", "product_id", "quantity"]
        )

        # Join
        join_condition = BinaryOp(
            op=BinaryOpType.EQ,
            left=ColumnRef(None, "id", DataType.INTEGER),
            right=ColumnRef(None, "product_id", DataType.INTEGER)
        )

        join = Join(
            left=inner_filter,
            right=right_scan,
            join_type=JoinType.INNER,
            condition=join_condition
        )

        # Outer filter
        outer_predicate = BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(None, "quantity", DataType.INTEGER),
            right=Literal(5, DataType.INTEGER)
        )
        outer_filter = Filter(input=join, predicate=outer_predicate)

        # Apply limit pushdown
        rule = LimitPushdownRule()
        result = rule.apply(outer_filter)

        # After fix: the deeply nested Limit should have pushed below Project
        assert isinstance(result, Filter), "Root should still be Filter"
        assert isinstance(result.input, Join), "Should have Join"
        assert isinstance(result.input.left, Filter), "Left should be Filter"
        assert isinstance(result.input.left.input, Project), "Should have Project after pushing"
        assert isinstance(result.input.left.input.input, Limit), "Limit should be below Project"
        assert isinstance(result.input.left.input.input.input, Scan), "Limit should be above Scan"
