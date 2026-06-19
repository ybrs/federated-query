"""Tests for DuckDB optimized SQL reconstruction."""

import duckdb
import pytest

from duckdb_reconstruct import (
    PlanNode,
    PlanReconstructionError,
    read_logical_node_registry,
    reconstruct_sql,
    validate_tree,
)


def build_connection():
    """Create a DuckDB connection with non-empty test tables."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE orders(id INTEGER, product_id INTEGER, qty INTEGER)")
    connection.execute("CREATE TABLE products(id INTEGER, price INTEGER, name VARCHAR)")
    connection.execute("INSERT INTO orders VALUES (1, 10, 2), (2, 11, 3)")
    connection.execute("INSERT INTO products VALUES (10, 150, 'a'), (11, 50, 'b')")
    return connection


def normalize_sql(sql):
    """Normalize whitespace for stable SQL assertions."""
    return " ".join(sql.split())


def test_registry_classifies_every_source_logical_node():
    """Verify every DuckDB logical node has an explicit classification."""
    registry = read_logical_node_registry()
    registry.assert_classified()
    assert "DELIM_JOIN" in registry.supported_names
    assert "WINDOW" in registry.unsupported_names
    assert "SEQ_SCAN" not in registry.logical_names


def test_dynamic_scan_node_is_allowed_by_registry():
    """Verify dynamic LogicalGet scan names are handled separately."""
    registry = read_logical_node_registry()
    node = PlanNode(
        "SEQ_SCAN",
        tuple(),
        {"Table": "memory.main.orders", "Type": "Sequential Scan"},
    )
    validate_tree(node, registry)


def test_unknown_non_scan_node_fails():
    """Verify unknown non-scan nodes fail instead of being ignored."""
    registry = read_logical_node_registry()
    node = PlanNode("NOT_A_DUCKDB_NODE", tuple(), {})
    with pytest.raises(PlanReconstructionError):
        validate_tree(node, registry)


def test_exists_reconstructs_as_semi_join():
    """Verify EXISTS decorrelation is rendered as a semi join."""
    connection = build_connection()
    sql = """
        SELECT o.*
        FROM orders o
        WHERE EXISTS (
            SELECT 1
            FROM products p
            WHERE p.id = o.product_id
              AND p.price > 100
        )
    """
    result = reconstruct_sql(connection, sql, False, True)
    normalized = normalize_sql(result)
    assert "SELECT o.* FROM orders AS o SEMI JOIN products AS p" in normalized
    assert "ON p.id = o.product_id" in normalized
    assert "WHERE p.price>100" in normalized


def test_not_exists_reconstructs_as_anti_join():
    """Verify NOT EXISTS decorrelation is rendered as an anti join."""
    connection = build_connection()
    sql = """
        SELECT o.id
        FROM orders o
        WHERE NOT EXISTS (
            SELECT 1
            FROM products p
            WHERE p.id = o.product_id
        )
    """
    result = reconstruct_sql(connection, sql, False, True)
    normalized = normalize_sql(result)
    assert "SELECT o.id FROM orders AS o ANTI JOIN products AS p" in normalized
    assert "ON o.product_id IS NOT DISTINCT FROM p.product_id" in normalized


def test_in_subquery_reconstructs_as_semi_join():
    """Verify IN subquery decorrelation is rendered as a semi join."""
    connection = build_connection()
    sql = """
        SELECT *
        FROM orders
        WHERE product_id IN (
            SELECT id
            FROM products
            WHERE price > 100
        )
    """
    result = reconstruct_sql(connection, sql, False, True)
    normalized = normalize_sql(result)
    assert "SEMI JOIN products" in normalized
    assert "WHERE products.price>100" in normalized


def test_verbose_output_includes_mapping():
    """Verify default output includes node-to-SQL mapping details."""
    connection = build_connection()
    sql = """
        SELECT o.*
        FROM orders o
        WHERE EXISTS (
            SELECT 1
            FROM products p
            WHERE p.id = o.product_id
              AND p.price > 100
        )
    """
    result = reconstruct_sql(connection, sql, False, False)
    assert "Reconstruction mapping:" in result
    assert "DELIM_JOIN -> SEMI JOIN" in result
