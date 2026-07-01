"""Tests for SQL parser."""

import pytest
from federated_query.parser import Parser
from federated_query.plan.logical import (
    Projection,
    Scan,
    Join,
    Explain,
    ExplainFormat,
    Aggregate,
    Filter,
)
from federated_query.plan.expressions import FunctionCall, expression_children


def _find_nodes(plan, node_type):
    """Collect every node of a type in a logical plan tree."""
    found = []
    stack = [plan]
    while stack:
        node = stack.pop()
        if isinstance(node, node_type):
            found.append(node)
        stack.extend(node.children())
    return found


def _expr_has_aggregate(expr):
    """Whether an expression tree still contains an aggregate function call."""
    if isinstance(expr, FunctionCall) and expr.is_aggregate:
        return True
    for child in expression_children(expr):
        if _expr_has_aggregate(child):
            return True
    return False


def test_having_only_aggregate_builds_aggregate_node():
    """An aggregate that appears only in HAVING still builds an Aggregate node.

    Without scanning HAVING for aggregates the plan would aggregate nothing and
    evaluate COUNT(*) over a raw scan.
    """
    parser = Parser()
    plan = parser.parse("SELECT 1 AS one FROM products HAVING COUNT(*) > 0")
    assert _find_nodes(plan, Aggregate), "HAVING-only aggregate built no Aggregate"


def test_having_aggregate_inside_case_is_remapped():
    """An aggregate nested in a CASE in HAVING is remapped to its output column.

    The HAVING rewrite must recurse through every compound node, not just
    BinaryOp/UnaryOp, or the nested SUM stays an aggregate call referencing
    pre-aggregation input that the post-aggregate schema does not expose.
    """
    parser = Parser()
    plan = parser.parse(
        "SELECT category, SUM(price) AS total FROM products GROUP BY category "
        "HAVING CASE WHEN SUM(price) > 100 THEN 1 ELSE 0 END = 1"
    )
    for filter_node in _find_nodes(plan, Filter):
        assert not _expr_has_aggregate(
            filter_node.predicate
        ), "aggregate inside HAVING CASE was not remapped to an output column"


def test_parser_initialization():
    """Test parser initialization."""
    from federated_query.parser.dialect import FedQPostgres

    parser = Parser()
    assert parser.dialect is FedQPostgres


def test_parse_simple_select():
    """Test parsing a simple SELECT query."""
    parser = Parser()
    sql = "SELECT id, name FROM users WHERE age > 18"

    # Should parse without errors
    ast = parser.parse(sql)
    assert ast is not None


def test_parse_join():
    """Test parsing a JOIN query."""
    parser = Parser()
    sql = """
        SELECT o.id, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
    """

    ast = parser.parse(sql)
    assert ast is not None


def test_parse_aggregate():
    """Test parsing an aggregate query."""
    parser = Parser()
    sql = """
        SELECT region, COUNT(*), AVG(amount)
        FROM orders
        GROUP BY region
        HAVING COUNT(*) > 100
    """

    ast = parser.parse(sql)
    assert ast is not None


def test_scan_alias_set_for_from_clause():
    """Parser should populate Scan.alias for simple FROM."""
    parser = Parser()
    sql = "SELECT c.id FROM testdb.main.customers c"
    plan = parser.parse_to_logical_plan(sql)
    assert isinstance(plan, Projection)
    assert isinstance(plan.input, Scan)
    assert plan.input.alias == "c"


def test_scan_alias_set_for_join_tables():
    """Parser should set aliases on both sides of joins."""
    parser = Parser()
    sql = """
        SELECT c.id, o.order_id
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """
    plan = parser.parse_to_logical_plan(sql)
    assert isinstance(plan, Projection)
    join = plan.input
    assert isinstance(join, Join)
    assert isinstance(join.left, Scan)
    assert isinstance(join.right, Scan)
    assert join.left.alias == "c"
    assert join.right.alias == "o"


def test_explain_as_json_detected():
    """Parser should treat EXPLAIN (AS JSON) as JSON format."""
    parser = Parser()
    sql = "EXPLAIN (AS JSON) SELECT id FROM testdb.main.users"
    plan = parser.parse_to_logical_plan(sql)
    assert isinstance(plan, Explain)
    assert plan.format == ExplainFormat.JSON


def test_projection_distinct_flag_set():
    """Parser should mark Projection.distinct when SELECT DISTINCT is used."""
    parser = Parser()
    sql = "SELECT DISTINCT region FROM testdb.main.orders"
    plan = parser.parse_to_logical_plan(sql)
    assert isinstance(plan, Projection)
    assert plan.distinct is True


def test_projection_distinct_flag_not_set():
    """Parser should keep Projection.distinct False when DISTINCT is absent."""
    parser = Parser()
    sql = "SELECT region FROM testdb.main.orders"
    plan = parser.parse_to_logical_plan(sql)
    assert isinstance(plan, Projection)
    assert plan.distinct is False
