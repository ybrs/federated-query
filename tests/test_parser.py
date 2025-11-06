"""Tests for SQL parser."""

import pytest
from federated_query.parser import Parser


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
