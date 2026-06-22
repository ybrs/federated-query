"""Tests for DuckDB optimized SQL reconstruction."""

import duckdb
import pytest

from duckdb_reconstruct import (
    PlanNode,
    PlanReconstructionError,
    read_logical_node_registry,
    reconstruct_sql,
    run_repl,
    validate_tree,
)


class FakePromptSession:
    """Provides deterministic prompt input for REPL tests."""

    def __init__(self, lines):
        """Store prompt lines for sequential reads."""
        self.lines = list(lines)

    def prompt(self, prompt_text):
        """Return the next configured input line."""
        return self.lines.pop(0)


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
    assert "ON p.id = o.product_id" in normalized


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


def test_select_without_from_has_no_from_clause():
    """Verify dummy scans do not emit an empty FROM clause."""
    connection = duckdb.connect(":memory:")
    result = reconstruct_sql(connection, "SELECT 1;", False, True)
    assert result == "SELECT 1;"


def test_repl_executes_when_statement_ends_with_semicolon(capsys):
    """Verify the REPL submits on semicolon and keeps accepting commands."""
    connection = duckdb.connect(":memory:")
    session = FakePromptSession(["SELECT 1;", ":quit"])
    run_repl(session, connection, False, True)
    captured = capsys.readouterr()
    assert "SELECT 1;" in captured.out


def test_auto_schema_creates_missing_table_for_query():
    """Verify auto schema mode creates missing tables and columns."""
    connection = duckdb.connect(":memory:")
    sql = """
        SELECT order_id
        FROM orders
        WHERE region IN (
            SELECT region
            FROM orders
            GROUP BY region
            HAVING COUNT(*) > 10
        )
    """
    result = reconstruct_sql(connection, sql, False, True, True)
    normalized = normalize_sql(result)
    assert "SELECT order_id FROM orders" in normalized
    assert "SEMI JOIN (SELECT region, count_star()" in normalized
    assert "HAVING count_star() > 10" in normalized
    assert "ON orders.region = __duckdb_agg.region" in normalized


def test_repl_auto_schema_handles_missing_table(capsys):
    """Verify REPL auto schema mode handles missing catalog objects."""
    connection = duckdb.connect(":memory:")
    session = FakePromptSession(["SELECT order_id FROM orders;", ":quit"])
    run_repl(session, connection, False, True, True)
    captured = capsys.readouterr()
    assert "SELECT order_id" in captured.out
    assert "ERROR:" not in captured.out


def test_empty_rhs_semi_join_reconstructs_as_false_filter():
    """Verify empty SEMI join RHS does not produce invalid aliases."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE orders(id INTEGER, product_id INTEGER)")
    connection.execute("INSERT INTO orders VALUES (1, 10)")
    connection.execute("CREATE TABLE products(id INTEGER, price INTEGER)")
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
    assert result == "SELECT o.*\nFROM orders AS o\nWHERE FALSE;"


def test_correlated_cte_reference_is_not_loaded_as_base_table():
    """Verify CTE references are not treated as physical catalog tables."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE customers(id INTEGER)")
    connection.execute("INSERT INTO customers VALUES (1)")
    connection.execute("CREATE TABLE orders(customer_id INTEGER)")
    sql = """
        SELECT c.*
        FROM customers c
        WHERE EXISTS (
            WITH recent_orders AS (
                SELECT *
                FROM orders o
                WHERE o.customer_id = c.id
            )
            SELECT 1
            FROM recent_orders
        )
    """
    result = reconstruct_sql(connection, sql, False, True)
    normalized = normalize_sql(result)
    assert "FROM customers AS c SEMI JOIN orders AS o" in normalized
    assert "ON o.customer_id = c.id" in normalized


def test_auto_schema_cte_join_uses_derived_table():
    """Verify auto schema mode ignores CTE names as base tables."""
    connection = duckdb.connect(":memory:")
    sql = """
        WITH cte AS (
            SELECT customer_id, count(*) AS order_count
            FROM orders
            GROUP BY customer_id
        )
        SELECT c.*, cte.order_count
        FROM customers c
        LEFT JOIN cte
          ON cte.customer_id = c.id
    """
    result = reconstruct_sql(connection, sql, False, True, True)
    normalized = normalize_sql(result)
    assert "LEFT JOIN (SELECT customer_id, count_star() AS order_count" in normalized
    assert "FROM orders GROUP BY customer_id) AS cte" in normalized
    assert "ON cte.customer_id = c.id" in normalized


def test_inlined_star_cte_is_displayed_with_cte_alias():
    """Verify inlined CTE scans keep a usable CTE alias."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE customers(id INTEGER)")
    connection.execute("CREATE TABLE orders(customer_id INTEGER)")
    connection.execute("INSERT INTO customers VALUES (1)")
    connection.execute("INSERT INTO orders VALUES (1)")
    sql = """
        WITH cte AS (
            SELECT *
            FROM orders
        )
        SELECT *
        FROM customers c
        JOIN cte
          ON cte.customer_id = c.id
    """
    result = reconstruct_sql(connection, sql, False, True)
    normalized = normalize_sql(result)
    assert "FROM customers AS c JOIN orders AS cte" in normalized
    assert "ON cte.customer_id = c.id" in normalized


def test_auto_schema_propagates_star_cte_join_column():
    """Verify auto schema creates base columns referenced through star CTEs."""
    connection = duckdb.connect(":memory:")
    sql = """
        WITH cte AS (
            SELECT *
            FROM orders
        )
        SELECT *
        FROM customers c
        JOIN cte
          ON cte.customer_id = c.id
    """
    result = reconstruct_sql(connection, sql, False, True, True)
    normalized = normalize_sql(result)
    assert "JOIN orders AS cte" in normalized
    assert "ON cte.customer_id = c.id" in normalized
