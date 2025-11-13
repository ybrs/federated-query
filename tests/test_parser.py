"""Tests for SQL parser."""

import pytest
from federated_query.parser import Parser
from federated_query.plan.logical import Project, Scan, Join


def test_parser_initialization():
    """Test parser initialization."""
    parser = Parser()
    assert parser.dialect == "postgres"


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
    assert isinstance(plan, Project)
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
    assert isinstance(plan, Project)
    join = plan.input
    assert isinstance(join, Join)
    assert isinstance(join.left, Scan)
    assert isinstance(join.right, Scan)
    assert join.left.alias == "c"
    assert join.right.alias == "o"
