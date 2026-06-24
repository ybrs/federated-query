"""Tests for DuckDB optimized SQL reconstruction."""

import json
import re
import subprocess
import sys

import duckdb
import pytest

from duckdb_node_manifest import (
    DEFAULT_DUCKDB_SOURCE_ROOT,
    read_duckdb_node_manifest,
)
from duckdb_reconstruct import (
    PlanNode,
    PlanReconstructionError,
    create_repl_session,
    default_repl_history_file,
    explain_optimized_json,
    is_scan_node,
    read_logical_node_registry,
    relation_handlers,
    reconstruct_sql,
    run_repl,
    validate_tree,
)


def duckdb_source_logical_node_names():
    """Return all rendered logical node names from DuckDB source."""
    manifest = read_duckdb_node_manifest(DEFAULT_DUCKDB_SOURCE_ROOT)
    return manifest.logical_plan_names()


def duckdb_source_logical_node_ids():
    """Return stable pytest ids for all DuckDB source logical nodes."""
    ids = []
    for name in duckdb_source_logical_node_names():
        ids.append(f"logical-node-{name}")
    return tuple(ids)


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
    connection.execute(
        "CREATE TABLE orders(id INTEGER, product_id INTEGER, qty INTEGER)"
    )
    connection.execute("CREATE TABLE products(id INTEGER, price INTEGER, name VARCHAR)")
    connection.execute("INSERT INTO orders VALUES (1, 10, 2), (2, 11, 3)")
    connection.execute("INSERT INTO products VALUES (10, 150, 'a'), (11, 50, 'b')")
    return connection


def normalize_sql(sql):
    """Normalize whitespace for stable SQL assertions."""
    return " ".join(sql.split())


def strip_ansi(text):
    """Remove ANSI escape sequences from colored REPL output."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def execute_sql_script(connection, sql):
    """Execute SQL text that may contain multiple statements and return rows."""
    return connection.execute(sql).fetchall()


def temp_view_names(sql):
    """Return CREATE TEMPORARY VIEW names from reconstructed SQL."""
    pattern = r"CREATE\s+TEMPORARY\s+VIEW\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS"
    return tuple(re.findall(pattern, sql, flags=re.IGNORECASE))


def assert_no_internal_placeholders(sql):
    """Assert reconstructed SQL does not expose DuckDB planner internals."""
    forbidden_tokens = (
        "SUBQUERY",
        "SINGLE JOIN",
        "sum_no_overflow",
        "arg_max_null",
        "arg_min_null",
    )
    for token in forbidden_tokens:
        assert token not in sql
    assert not re.search(r"#(?:\d+|\[[^\]]+\])", sql)


def assert_temp_views_are_unique(sql):
    """Assert reconstructed SQL does not overwrite temporary views."""
    names = temp_view_names(sql)
    assert len(names) == len(set(names))


def assert_reconstructed_sql_matches_original(setup_sql, query_sql, auto_schema=False):
    """Assert reconstructed SQL executes and returns the original query rows."""
    original_connection = duckdb.connect(":memory:")
    reconstructed_connection = duckdb.connect(":memory:")
    if setup_sql.strip():
        original_connection.execute(setup_sql)
        reconstructed_connection.execute(setup_sql)
    expected_rows = original_connection.execute(query_sql).fetchall()
    reconstructed_sql = reconstruct_sql(
        reconstructed_connection, query_sql, False, True, auto_schema
    )
    assert_no_internal_placeholders(reconstructed_sql)
    assert_temp_views_are_unique(reconstructed_sql)
    actual_rows = execute_sql_script(reconstructed_connection, reconstructed_sql)
    assert actual_rows == expected_rows


def plan_node_names(node):
    """Return all node names from a DuckDB optimized plan tree."""
    names = {node.name}
    for child in node.children:
        names.update(plan_node_names(child))
    return names


def plan_nodes(node):
    """Return all nodes from a DuckDB optimized plan tree."""
    nodes = [node]
    for child in node.children:
        nodes.extend(plan_nodes(child))
    return tuple(nodes)


def observed_contract_node_names():
    """Return node names observed from real representative SELECT plans."""
    names = set()
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        connection = duckdb.connect(":memory:")
        if setup_sql.strip():
            connection.execute(setup_sql)
        root = explain_optimized_json(connection, query_sql)
        names.update(plan_node_names(root))
        assert case_name
    return frozenset(names)


def observed_contract_nodes():
    """Return nodes observed from real representative SELECT plans."""
    nodes = []
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        connection = duckdb.connect(":memory:")
        if setup_sql.strip():
            connection.execute(setup_sql)
        root = explain_optimized_json(connection, query_sql)
        nodes.extend(plan_nodes(root))
        assert case_name
    return tuple(nodes)


def test_registry_classifies_every_source_logical_node():
    """Verify every DuckDB logical node comes from source."""
    registry = read_logical_node_registry()
    registry.assert_classified()
    assert "DELIM_JOIN" in registry.supported_names
    assert "WINDOW" in registry.supported_names
    assert "SEQ_SCAN" not in registry.logical_names


def test_node_manifest_script_outputs_logical_nodes_as_json():
    """Verify the source-node extractor CLI returns JSON node data."""
    result = subprocess.run(
        [sys.executable, "duckdb_node_manifest.py"],
        capture_output=True,
        check=True,
        text=True,
    )
    manifest = json.loads(result.stdout)
    assert "logical_nodes" in manifest
    assert len(manifest["logical_nodes"]) == len(duckdb_source_logical_node_names())


def test_registry_uses_exact_source_logical_node_names():
    """Verify registry logical names match the extractor source of truth."""
    registry = read_logical_node_registry()
    source_names = set()
    for name in duckdb_source_logical_node_names():
        source_names.add(name)
    assert registry.logical_names == frozenset(source_names)


@pytest.mark.parametrize(
    "node_name",
    duckdb_source_logical_node_names(),
    ids=duckdb_source_logical_node_ids(),
)
def test_each_duckdb_source_logical_node_is_classified(node_name):
    """Verify every source logical node is accepted by the registry."""
    registry = read_logical_node_registry()
    assert node_name in registry.supported_names


@pytest.mark.parametrize(
    "node_name",
    duckdb_source_logical_node_names(),
    ids=duckdb_source_logical_node_ids(),
)
def test_each_duckdb_source_logical_node_validation_decision(node_name):
    """Verify each source logical node passes registry validation."""
    registry = read_logical_node_registry()
    node = PlanNode(node_name, tuple(), {})
    registry.validate_node(node)


@pytest.mark.parametrize(
    "node_name",
    duckdb_source_logical_node_names(),
    ids=duckdb_source_logical_node_ids(),
)
def test_each_duckdb_source_logical_node_has_handler(node_name):
    """Verify every source logical node has a reconstruction handler."""
    handlers = relation_handlers()
    assert node_name in handlers


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
    assert "SELECT 1;" in strip_ansi(captured.out)


def test_default_repl_history_file_uses_home(monkeypatch, tmp_path):
    """Verify default REPL history path lives under the user's home."""
    monkeypatch.setenv("HOME", str(tmp_path))
    assert default_repl_history_file() == tmp_path / ".duckdb_reconstruct_history"


def test_create_repl_session_uses_file_history(tmp_path):
    """Verify REPL sessions persist history to the configured file."""
    history_file = tmp_path / "nested" / "history.txt"
    session = create_repl_session(history_file)
    assert session.history.filename == str(history_file)
    assert history_file.parent.exists()


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
    assert "CREATE TEMPORARY VIEW __duckdb_agg AS SELECT region, count_star()" in normalized
    assert "SELECT orders.order_id FROM orders" in normalized
    assert "HAVING count_star() > 10" in normalized
    assert "ON orders.region = __duckdb_agg.region" in normalized


def test_repl_auto_schema_handles_missing_table(capsys):
    """Verify REPL auto schema mode handles missing catalog objects."""
    connection = duckdb.connect(":memory:")
    session = FakePromptSession(["SELECT order_id FROM orders;", ":quit"])
    run_repl(session, connection, False, True, True)
    captured = capsys.readouterr()
    output = strip_ansi(captured.out)
    assert "SELECT order_id" in output
    assert "ERROR:" not in output


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
    assert (
        "CREATE TEMPORARY VIEW cte AS SELECT customer_id, count_star() AS order_count"
        in normalized
    )
    assert "FROM orders GROUP BY customer_id;" in normalized
    assert ";\n\nSELECT c.*" in result
    assert "LEFT JOIN cte" in normalized
    assert "ON cte.customer_id = c.id" in normalized


def test_chained_cte_aggregate_join_uses_final_join_alias():
    """Verify chained CTE aggregate joins preserve the final CTE alias."""
    connection = duckdb.connect(":memory:")
    sql = """
        WITH
        large_orders AS (
            SELECT id, customer_id, total_amount
            FROM orders
            WHERE total_amount > 100
        ),
        customer_totals AS (
            SELECT
                customer_id,
                COUNT(*) AS order_count,
                SUM(total_amount) AS total_spent
            FROM large_orders
            GROUP BY customer_id
        ),
        high_value_customers AS (
            SELECT customer_id, order_count, total_spent
            FROM customer_totals
            WHERE total_spent > 1000
        )
        SELECT c.id, c.name, h.order_count, h.total_spent
        FROM customers c
        JOIN high_value_customers h
          ON h.customer_id = c.id
    """
    result = reconstruct_sql(connection, sql, False, True, True)
    normalized = normalize_sql(result)
    assert "AS __duckdb_agg" not in normalized
    assert "CREATE TEMPORARY VIEW h AS SELECT customer_id" in normalized
    assert ";\n\nSELECT c.id" in result
    assert "JOIN h ON h.customer_id = c.id" in normalized
    assert "count_star() AS order_count" in normalized
    assert "sum_no_overflow(total_amount) AS total_spent" in normalized
    assert "HAVING total_spent > 1000" in normalized


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
    assert "CREATE TEMPORARY VIEW cte AS SELECT * FROM orders;" in normalized
    assert ";\n\nSELECT *" in result
    assert "FROM customers AS c JOIN cte" in normalized
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
    assert "CREATE TEMPORARY VIEW cte AS SELECT * FROM orders;" in normalized
    assert ";\n\nSELECT *" in result
    assert "JOIN cte" in normalized
    assert "ON cte.customer_id = c.id" in normalized


def test_root_derived_subquery_uses_extract_plan_projection():
    """Verify derived table projection details come from ExtractPlan."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE bar(x INTEGER)")
    connection.execute("INSERT INTO bar VALUES (1)")
    sql = "SELECT foo FROM (SELECT 1 AS foo FROM bar)"
    result = reconstruct_sql(connection, sql, False, True)
    assert result == "SELECT 1 AS foo\nFROM bar;"


def test_repl_root_derived_subquery_uses_live_connection_schema(capsys):
    """Verify REPL root subqueries use schema from the live connection."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE bar(x INTEGER)")
    session = FakePromptSession(
        ["select foo from (select 1 as foo from bar);", ":quit"]
    )
    run_repl(session, connection, False, False)
    captured = capsys.readouterr()
    output = strip_ansi(captured.out)
    assert "SELECT 1 AS foo" in output
    assert "FROM bar;" in output
    assert "ERROR:" not in output


def test_union_reconstructs_set_operation():
    """Verify UNION plans are reconstructed as set-operation SQL."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE foo(c INTEGER)")
    connection.execute("CREATE TABLE bar(d INTEGER)")
    result = reconstruct_sql(
        connection, "select c from foo union select d from bar;", False, True
    )
    assert result == "SELECT c\nFROM foo\nUNION\nSELECT d\nFROM bar;"


def test_repl_union_reconstructs_set_operation(capsys):
    """Verify REPL UNION queries do not fail on the UNION plan node."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE foo(c INTEGER)")
    connection.execute("CREATE TABLE bar(d INTEGER)")
    session = FakePromptSession(["select c from foo union select d from bar;", ":quit"])
    run_repl(session, connection, False, False)
    captured = capsys.readouterr()
    output = strip_ansi(captured.out)
    assert "SELECT c" in output
    assert "UNION" in output
    assert "SELECT d" in output
    assert "ERROR:" not in output


def broad_reconstruction_cases():
    """Return broad SQL cases that should reconstruct without invalid SQL."""
    cases = []
    add_projection_cases(cases)
    add_filter_cases(cases)
    add_join_cases(cases)
    add_subquery_cases(cases)
    add_aggregate_cases(cases)
    add_cte_cases(cases)
    add_set_operation_cases(cases)
    add_order_limit_cases(cases)
    add_root_derived_cases(cases)
    return tuple(cases)


def add_projection_cases(cases):
    """Add projection-only coverage cases."""
    for index in range(12):
        cases.append(f"SELECT col_{index} FROM table_{index}")
        cases.append(f"SELECT col_{index} AS alias_{index} FROM table_{index}")


def add_filter_cases(cases):
    """Add filter coverage cases."""
    for index in range(10):
        cases.append(f"SELECT a FROM filter_{index} WHERE b > {index}")
        cases.append(f"SELECT a FROM filter_{index} WHERE b = {index} AND c < 100")


def add_join_cases(cases):
    """Add join coverage cases."""
    for index in range(8):
        cases.append(
            f"SELECT l.a FROM left_{index} l JOIN right_{index} r ON r.id = l.id"
        )
        cases.append(
            f"SELECT l.a FROM left_{index} l LEFT JOIN right_{index} r ON r.id = l.id"
        )


def add_subquery_cases(cases):
    """Add EXISTS, NOT EXISTS, and IN subquery cases."""
    for index in range(8):
        cases.append(
            "SELECT a FROM outer_{0} o WHERE EXISTS "
            "(SELECT 1 FROM inner_{0} i WHERE i.id = o.id AND i.v > 1)".format(index)
        )
        cases.append(
            "SELECT a FROM outer_{0} o WHERE NOT EXISTS "
            "(SELECT 1 FROM inner_{0} i WHERE i.id = o.id)".format(index)
        )
        cases.append(
            "SELECT a FROM outer_{0} WHERE id IN "
            "(SELECT id FROM inner_{0} WHERE v > 1)".format(index)
        )


def add_aggregate_cases(cases):
    """Add aggregate and HAVING coverage cases."""
    for index in range(8):
        cases.append(f"SELECT id, count(*) FROM agg_{index} GROUP BY id")
        cases.append(
            f"SELECT id, count(*) FROM agg_{index} GROUP BY id HAVING count(*) > 1"
        )


def add_cte_cases(cases):
    """Add CTE coverage cases."""
    for index in range(5):
        cases.append(
            "WITH cte AS (SELECT * FROM cte_base_{0}) "
            "SELECT * FROM cte".format(index)
        )
        cases.append(
            "WITH cte AS (SELECT id, count(*) AS n FROM cte_base_{0} GROUP BY id) "
            "SELECT cte.id, cte.n FROM cte".format(index)
        )


def add_set_operation_cases(cases):
    """Add UNION, UNION ALL, EXCEPT, and INTERSECT coverage cases."""
    for index in range(4):
        cases.append(f"SELECT a FROM set_a_{index} UNION SELECT b FROM set_b_{index}")
        cases.append(
            f"SELECT a FROM set_a_{index} UNION ALL SELECT b FROM set_b_{index}"
        )
        cases.append(f"SELECT a FROM set_a_{index} EXCEPT SELECT b FROM set_b_{index}")
        cases.append(
            f"SELECT a FROM set_a_{index} INTERSECT SELECT b FROM set_b_{index}"
        )


def add_order_limit_cases(cases):
    """Add DISTINCT, ORDER BY, and LIMIT coverage cases."""
    for index in range(8):
        cases.append(f"SELECT DISTINCT a FROM ordered_{index}")
        cases.append(f"SELECT a FROM ordered_{index} ORDER BY a")
        cases.append(f"SELECT a FROM ordered_{index} LIMIT {index + 1}")


def add_root_derived_cases(cases):
    """Add root derived-table projection coverage cases."""
    for index in range(5):
        cases.append(
            "SELECT alias_{0} FROM "
            "(SELECT {0} AS alias_{0} FROM derived_{0})".format(index + 1)
        )


@pytest.mark.parametrize("sql", broad_reconstruction_cases())
def test_broad_query_reconstruction_cases_do_not_emit_invalid_empty_sql(sql):
    """Verify broad query forms reconstruct without known invalid placeholders."""
    connection = duckdb.connect(":memory:")
    result = reconstruct_sql(connection, sql, False, True, True)
    assert result.strip()
    assert result.endswith(";")
    assert "SELECT * WHERE FALSE" not in result
    assert "SINGLE JOIN (SELECT *" not in result


def test_scalar_subquery_filter_replaces_subquery_placeholder():
    """Verify scalar subquery filters do not emit SUBQUERY placeholders."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE x(id INTEGER, a INTEGER)")
    connection.execute("INSERT INTO x VALUES (1, 1)")
    connection.execute("CREATE TABLE y(id INTEGER, c INTEGER)")
    sql = "SELECT a FROM x WHERE (SELECT c FROM y WHERE y.id = x.id) > 1"
    result = reconstruct_sql(connection, sql, False, True)
    assert "WHERE y.c > 1" in result
    assert "SUBQUERY" not in result
    assert "SELECT * WHERE FALSE" not in result


def test_audit_exists_semijoin_reconstructed_sql_executes():
    """Audit EXISTS decorrelation does not reference hidden SEMI RHS aliases."""
    setup_sql = """
        CREATE TABLE orders(id INTEGER, product_id INTEGER, qty INTEGER);
        CREATE TABLE products(id INTEGER, price INTEGER, name VARCHAR);
        INSERT INTO orders VALUES (1, 10, 2), (2, 11, 3);
        INSERT INTO products VALUES (10, 150, 'a'), (11, 50, 'b');
    """
    query_sql = """
        SELECT o.*
        FROM orders o
        WHERE EXISTS (
            SELECT 1
            FROM products p
            WHERE p.id = o.product_id
              AND p.price > 100
        )
    """
    assert_reconstructed_sql_matches_original(setup_sql, query_sql)


def test_audit_correlated_delim_predicate_preserves_expression():
    """Audit correlated DELIM_GET joins preserve non-trivial predicates."""
    setup_sql = """
        CREATE TABLE orders(customer_id INTEGER, price INTEGER);
        INSERT INTO orders VALUES (1, 10), (11, 110), (2, 20);
    """
    query_sql = """
        SELECT customer_id,
               (
                   SELECT price
                   FROM orders X
                   WHERE X.customer_id = O.customer_id + 10
               ) AS p
        FROM orders O
        ORDER BY customer_id
    """
    assert_reconstructed_sql_matches_original(setup_sql, query_sql)


def test_ordered_limit_scalar_subquery_reconstructs_parseable_join():
    """Verify top-1 scalar subqueries render as parseable aggregate joins."""
    connection = duckdb.connect(":memory:")
    sql = """
        SELECT order_id
        FROM orders O
        WHERE price = (
            SELECT price
            FROM orders X
            WHERE X.region = O.region
            ORDER BY price DESC
            LIMIT 1
        )
    """
    result = reconstruct_sql(connection, sql, False, True, True)
    normalized = normalize_sql(result)
    assert "FROM orders AS O JOIN" in normalized
    assert "JOIN __duckdb_agg" in normalized
    assert "FROM orders AS X" in normalized
    assert "max(X.price) AS price" in result
    assert "ON O.region IS NOT DISTINCT FROM __duckdb_agg.region" in result
    assert "WHERE O.price = __duckdb_agg.price" in result
    assert "SINGLE JOIN" not in result
    assert "JOIN (SELECT" not in result
    assert "HAVING" not in result
    connection.execute(result)


def test_audit_ordered_limit_argmax_slots_are_resolved():
    """Audit arg_max scalar LIMIT rewrites do not leak bracketed slot refs."""
    setup_sql = """
        CREATE TABLE orders(order_id INTEGER, price INTEGER, region VARCHAR);
        INSERT INTO orders VALUES
            (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'a'),
            (4, 15, 'b'), (5, 25, 'b');
    """
    query_sql = """
        SELECT order_id
        FROM orders O
        WHERE price = (
            SELECT price
            FROM orders X
            WHERE X.region = O.region
            ORDER BY order_id DESC
            LIMIT 1
        )
        ORDER BY order_id
    """
    assert_reconstructed_sql_matches_original(setup_sql, query_sql)


def test_ordered_limit_offset_scalar_subquery_reconstructs_window_join():
    """Verify LIMIT OFFSET scalar subqueries render DuckDB's row-number window."""
    connection = duckdb.connect(":memory:")
    connection.execute(
        "CREATE TABLE orders(order_id BIGINT, price BIGINT, region VARCHAR)"
    )
    connection.execute(
        """
        INSERT INTO orders VALUES
            (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'a'), (4, 40, 'a'),
            (5, 10, 'b'), (6, 20, 'b'), (7, 30, 'b')
        """
    )
    sql = """
        SELECT order_id
        FROM orders O
        WHERE price = (
            SELECT price
            FROM orders X
            WHERE X.region = O.region
            ORDER BY price
            LIMIT 1 OFFSET 2
        )
    """
    result = reconstruct_sql(connection, sql, False, True)
    assert "CREATE TEMPORARY VIEW __duckdb_window AS" in result
    assert "ROW_NUMBER() OVER" in result
    assert "AS limit_rownum" in result
    assert "__duckdb_window.limit_rownum > 2" in result
    assert "__duckdb_window.limit_rownum <= 3" in result
    assert "WINDOW" not in result
    expected_rows = connection.execute(sql).fetchall()
    actual_rows = connection.execute(result).fetchall()
    assert actual_rows == expected_rows


def test_scalar_subquery_projection_reconstructs_joined_column():
    """Verify scalar subqueries in SELECT are rendered as joined columns."""
    connection = duckdb.connect(":memory:")
    sql = """
        SELECT customer_id,
               (
                   SELECT price
                   FROM orders X
                   WHERE X.customer_id = O.customer_id
               ) AS p
        FROM orders O
    """
    result = reconstruct_sql(connection, sql, False, True, True)
    normalized = normalize_sql(result)
    assert "SELECT O.customer_id, X.price AS p" in normalized
    assert "FROM orders AS O JOIN orders AS X" in normalized
    assert "ON O.customer_id IS NOT DISTINCT FROM X.customer_id" in result
    assert "(SELECT price" not in result
    assert "SINGLE JOIN" not in result
    connection.execute(result)


def test_audit_repeated_alias_join_preserves_where_predicate():
    """Audit repeated table aliases preserve filters above joins."""
    setup_sql = """
        CREATE TABLE orders(id INTEGER, product_id INTEGER, qty INTEGER);
        INSERT INTO orders VALUES (1, 10, 2), (2, 11, 3), (3, 12, 4);
    """
    query_sql = """
        SELECT o.id, x.id
        FROM orders o
        JOIN orders x ON o.product_id = x.product_id
        WHERE o.id < x.id
        ORDER BY o.id, x.id
    """
    assert_reconstructed_sql_matches_original(setup_sql, query_sql)


def test_group_by_scalar_subquery_reconstructs_materialized_plan():
    """Verify scalar subqueries in GROUP BY render as materialized plan nodes."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE orders(customer_id INTEGER, price INTEGER)")
    connection.execute("CREATE TABLE products(price INTEGER)")
    connection.execute("INSERT INTO orders VALUES (1, 10), (2, 20), (3, 30)")
    connection.execute("INSERT INTO products VALUES (10), (50)")
    sql = "SELECT COUNT(*) FROM orders GROUP BY (SELECT MAX(price) FROM products)"
    result = reconstruct_sql(connection, sql, False, True)
    assert "CREATE TEMPORARY VIEW __duckdb_agg AS" in result
    assert "CREATE TEMPORARY VIEW __duckdb_agg_2 AS" in result
    assert "CREATE TEMPORARY VIEW __duckdb_agg_3 AS" in result
    assert '"first"(__duckdb_agg.' in result
    assert "GROUP BY __duckdb_agg_2." in result
    assert "#0" not in result
    assert "SUBQUERY" not in result
    expected_rows = connection.execute(sql).fetchall()
    actual_rows = connection.execute(result).fetchall()
    assert actual_rows == expected_rows


def test_audit_aggregate_internal_functions_are_executable():
    """Audit aggregate reconstruction does not emit internal-only functions."""
    setup_sql = """
        CREATE TABLE orders(product_id INTEGER, qty INTEGER);
        INSERT INTO orders VALUES (10, 2), (10, 3), (11, 1);
    """
    query_sql = """
        SELECT product_id, sum(qty) AS s
        FROM orders
        GROUP BY product_id
        HAVING sum(qty) > 2
        ORDER BY product_id
    """
    assert_reconstructed_sql_matches_original(setup_sql, query_sql)


def test_audit_empty_result_preserves_projection_schema():
    """Audit EMPTY_RESULT placeholders expose projected output columns."""
    setup_sql = ""
    query_sql = "SELECT a FROM filter_0 WHERE b = 0 AND c < 100"
    connection = duckdb.connect(":memory:")
    reconstructed_sql = reconstruct_sql(connection, query_sql, False, True, True)
    assert_no_internal_placeholders(reconstructed_sql)
    execute_sql_script(connection, reconstructed_sql)


def representative_plan_shape_cases():
    """Return representative SQL cases for SELECT-reachable DuckDB nodes."""
    cases = []
    cases.append(
        (
            "unnest",
            "CREATE TABLE t(items INTEGER[]); INSERT INTO t VALUES ([1, 2]);",
            "SELECT value FROM t, UNNEST(items) AS u(value) ORDER BY value",
        )
    )
    cases.append(
        (
            "sample",
            "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1), (2), (3);",
            "SELECT id FROM t USING SAMPLE 100 PERCENT (bernoulli) ORDER BY id",
        )
    )
    cases.append(
        (
            "materialized_cte_scan",
            "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1), (2);",
            (
                "WITH cte AS MATERIALIZED (SELECT id FROM t) "
                "SELECT a.id, b.id FROM cte a JOIN cte b ON a.id = b.id"
            ),
        )
    )
    cases.append(
        (
            "positional_join",
            (
                "CREATE TABLE left_t(id INTEGER); "
                "CREATE TABLE right_t(id INTEGER); "
                "INSERT INTO left_t VALUES (1), (2); "
                "INSERT INTO right_t VALUES (10), (20);"
            ),
            "SELECT * FROM left_t POSITIONAL JOIN right_t",
        )
    )
    cases.append(
        (
            "asof_join",
            (
                "CREATE TABLE trades(symbol VARCHAR, ts INTEGER, price INTEGER); "
                "CREATE TABLE prices(symbol VARCHAR, ts INTEGER, bid INTEGER); "
                "INSERT INTO trades VALUES ('a', 10, 100); "
                "INSERT INTO prices VALUES ('a', 5, 90), ('a', 10, 95);"
            ),
            (
                "SELECT * FROM trades t ASOF JOIN prices p "
                "ON t.symbol = p.symbol AND t.ts >= p.ts"
            ),
        )
    )
    cases.append(
        (
            "recursive_cte",
            "",
            (
                "WITH RECURSIVE r(i) AS ("
                "SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < 3"
                ") SELECT i FROM r ORDER BY i"
            ),
        )
    )
    return tuple(cases)


def reconstruction_contract_cases():
    """Return real SQL cases that enforce reconstruction correctness."""
    cases = []
    add_basic_operator_contract_cases(cases)
    add_join_contract_cases(cases)
    add_correlated_contract_cases(cases)
    add_lineage_contract_cases(cases)
    add_set_contract_cases(cases)
    add_window_contract_cases(cases)
    add_node_shape_contract_cases(cases)
    add_empty_contract_cases(cases)
    return tuple(cases)


def add_basic_operator_contract_cases(cases):
    """Add basic SELECT operator contracts."""
    setup_sql = "CREATE TABLE t(a INTEGER); INSERT INTO t VALUES (1), (2), (2);"
    cases.append(("dummy_scan", "", "SELECT 1"))
    cases.append(("distinct", setup_sql, "SELECT DISTINCT a FROM t ORDER BY a"))
    cases.append(("limit", setup_sql, "SELECT a FROM t LIMIT 2"))
    cases.append(("order_by", setup_sql, "SELECT a FROM t ORDER BY a"))
    cases.append(("top_n", setup_sql, "SELECT a FROM t ORDER BY a LIMIT 2"))


def add_join_contract_cases(cases):
    """Add join and semi/anti join reconstruction contracts."""
    setup_sql = """
        CREATE TABLE orders(id INTEGER, product_id INTEGER, qty INTEGER);
        CREATE TABLE products(id INTEGER, price INTEGER, name VARCHAR);
        INSERT INTO orders VALUES (1, 10, 2), (2, 11, 3), (3, 12, 4);
        INSERT INTO products VALUES (10, 150, 'a'), (11, 50, 'b');
    """
    cases.append(("exists_semi_rhs_filter", setup_sql, exists_contract_sql()))
    cases.append(("not_exists_anti_join", setup_sql, not_exists_contract_sql()))
    cases.append(("in_subquery", setup_sql, in_contract_sql()))
    cases.append(("not_in_subquery", setup_sql, not_in_contract_sql()))
    cases.append(("repeated_alias_filter", setup_sql, repeated_alias_contract_sql()))


def exists_contract_sql():
    """Return an EXISTS query with an RHS-only predicate."""
    return """
        SELECT o.id
        FROM orders o
        WHERE EXISTS (
            SELECT 1 FROM products p
            WHERE p.id = o.product_id AND p.price > 100
        )
        ORDER BY o.id
    """


def not_exists_contract_sql():
    """Return a NOT EXISTS query for anti join reconstruction."""
    return """
        SELECT o.id
        FROM orders o
        WHERE NOT EXISTS (
            SELECT 1 FROM products p WHERE p.id = o.product_id
        )
        ORDER BY o.id
    """


def in_contract_sql():
    """Return an IN subquery contract query."""
    return """
        SELECT id FROM orders
        WHERE product_id IN (SELECT id FROM products WHERE price > 100)
        ORDER BY id
    """


def not_in_contract_sql():
    """Return a NOT IN subquery contract query."""
    return """
        SELECT id FROM orders
        WHERE product_id NOT IN (SELECT id FROM products WHERE price > 100)
        ORDER BY id
    """


def repeated_alias_contract_sql():
    """Return a self-join query that requires filter preservation."""
    return """
        SELECT o.id, x.id
        FROM orders o
        JOIN orders x ON o.product_id = x.product_id
        WHERE o.id < x.id
        ORDER BY o.id, x.id
    """


def add_correlated_contract_cases(cases):
    """Add correlated subquery position contracts."""
    setup_sql = """
        CREATE TABLE orders(customer_id INTEGER, price INTEGER, region VARCHAR);
        INSERT INTO orders VALUES
            (1, 10, 'a'), (11, 110, 'a'), (2, 20, 'b');
    """
    cases.append(("correlated_select", setup_sql, correlated_select_sql()))
    cases.append(("correlated_where", setup_sql, correlated_where_sql()))
    cases.append(("correlated_group_by", setup_sql, correlated_group_by_sql()))
    cases.append(("correlated_having", setup_sql, correlated_having_sql()))


def correlated_select_sql():
    """Return a scalar subquery in SELECT with non-trivial correlation."""
    return """
        SELECT customer_id,
               (
                   SELECT price FROM orders X
                   WHERE X.customer_id = O.customer_id + 10
               ) AS p
        FROM orders O
        ORDER BY customer_id
    """


def correlated_where_sql():
    """Return a scalar subquery in WHERE."""
    return """
        SELECT customer_id
        FROM orders O
        WHERE price = (
            SELECT price FROM orders X
            WHERE X.region = O.region
            ORDER BY price DESC
            LIMIT 1
        )
        ORDER BY customer_id
    """


def correlated_group_by_sql():
    """Return a scalar subquery in GROUP BY."""
    return """
        SELECT count(*)
        FROM orders O
        GROUP BY (SELECT max(price) FROM orders X WHERE X.region = O.region)
        ORDER BY count(*)
    """


def correlated_having_sql():
    """Return a scalar subquery in HAVING."""
    return """
        SELECT region, count(*)
        FROM orders O
        GROUP BY region
        HAVING count(*) >= (
            SELECT count(*) FROM orders X WHERE X.region = O.region
        )
        ORDER BY region
    """


def add_lineage_contract_cases(cases):
    """Add alias, CTE, aggregate, and temp-name lineage contracts."""
    cases.append(alias_collision_contract_case())
    cases.append(derived_cte_alias_contract_case())
    cases.append(non_materialized_cte_contract_case())
    cases.append(materialized_cte_contract_case())
    cases.append(aggregate_contract_case())


def alias_collision_contract_case():
    """Return a case where user aliases collide with generated names."""
    setup_sql = """
        CREATE TABLE orders(product_id INTEGER, qty INTEGER);
        INSERT INTO orders VALUES (10, 2), (10, 3), (11, 1);
    """
    query_sql = """
        SELECT product_id AS __duckdb_agg, sum(qty) AS __duckdb_agg_2
        FROM orders
        GROUP BY product_id
        ORDER BY product_id
    """
    return ("generated_alias_collision", setup_sql, query_sql)


def derived_cte_alias_contract_case():
    """Return a CTE alias lineage case with a derived aggregate RHS."""
    setup_sql = """
        CREATE TABLE customers(id INTEGER, name VARCHAR);
        CREATE TABLE orders(customer_id INTEGER, total_amount INTEGER);
        INSERT INTO customers VALUES (1, 'a'), (2, 'b');
        INSERT INTO orders VALUES (1, 200), (1, 300), (2, 10);
    """
    query_sql = """
        WITH totals AS (
            SELECT customer_id, sum(total_amount) AS spent
            FROM orders
            GROUP BY customer_id
        )
        SELECT c.id, h.spent
        FROM customers c
        JOIN totals h ON h.customer_id = c.id
        WHERE h.spent > 100
        ORDER BY c.id
    """
    return ("derived_cte_alias_lineage", setup_sql, query_sql)


def non_materialized_cte_contract_case():
    """Return a non-materialized CTE contract case."""
    setup_sql = "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1), (2);"
    query_sql = "WITH cte AS (SELECT id FROM t) SELECT id FROM cte ORDER BY id"
    return ("non_materialized_cte", setup_sql, query_sql)


def materialized_cte_contract_case():
    """Return a materialized CTE scan contract case."""
    setup_sql = "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1), (2);"
    query_sql = (
        "WITH cte AS MATERIALIZED (SELECT id FROM t) "
        "SELECT a.id, b.id FROM cte a JOIN cte b ON a.id = b.id ORDER BY a.id"
    )
    return ("materialized_cte_scan", setup_sql, query_sql)


def aggregate_contract_case():
    """Return an aggregate contract case that rejects internal functions."""
    setup_sql = """
        CREATE TABLE orders(product_id INTEGER, qty INTEGER);
        INSERT INTO orders VALUES (10, 2), (10, 3), (11, 1);
    """
    query_sql = """
        SELECT product_id, sum(qty) AS s
        FROM orders
        GROUP BY product_id
        HAVING sum(qty) > 2
        ORDER BY product_id
    """
    return ("aggregate_internal_function", setup_sql, query_sql)


def add_set_contract_cases(cases):
    """Add set-operation reconstruction contracts."""
    setup_sql = """
        CREATE TABLE left_t(a INTEGER);
        CREATE TABLE right_t(b INTEGER);
        INSERT INTO left_t VALUES (1), (2), (2);
        INSERT INTO right_t VALUES (2), (3);
    """
    cases.append(("union", setup_sql, set_query_sql("UNION")))
    cases.append(("union_all", setup_sql, set_query_sql("UNION ALL")))
    cases.append(("intersect", setup_sql, set_query_sql("INTERSECT")))
    cases.append(("except", setup_sql, set_query_sql("EXCEPT")))


def set_query_sql(operator):
    """Return a set-operation query for one operator."""
    return f"SELECT a FROM left_t {operator} SELECT b FROM right_t ORDER BY a"


def add_window_contract_cases(cases):
    """Add ordinary and decorrelation-generated window contracts."""
    setup_sql = """
        CREATE TABLE orders(order_id INTEGER, price INTEGER, region VARCHAR);
        INSERT INTO orders VALUES
            (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'a'),
            (4, 15, 'b'), (5, 25, 'b');
    """
    cases.append(("window_row_number", setup_sql, window_row_number_sql()))
    cases.append(("window_rank", setup_sql, window_rank_sql()))
    cases.append(("window_dense_rank", setup_sql, window_dense_rank_sql()))
    cases.append(("window_sum", setup_sql, window_sum_sql()))
    cases.append(("limit_offset_window", setup_sql, limit_offset_window_sql()))
    cases.append(("argmax_scalar_limit", setup_sql, argmax_scalar_limit_sql()))


def window_row_number_sql():
    """Return a ROW_NUMBER window query."""
    return """
        SELECT order_id,
               row_number() OVER (PARTITION BY region ORDER BY price) AS rn
        FROM orders
        ORDER BY order_id
    """


def window_rank_sql():
    """Return a RANK window query."""
    return """
        SELECT order_id, rank() OVER (PARTITION BY region ORDER BY price) AS r
        FROM orders
        ORDER BY order_id
    """


def window_dense_rank_sql():
    """Return a DENSE_RANK window query."""
    return """
        SELECT order_id,
               dense_rank() OVER (PARTITION BY region ORDER BY price) AS r
        FROM orders
        ORDER BY order_id
    """


def window_sum_sql():
    """Return an aggregate window query."""
    return """
        SELECT order_id,
               sum(price) OVER (PARTITION BY region ORDER BY order_id) AS running
        FROM orders
        ORDER BY order_id
    """


def limit_offset_window_sql():
    """Return a scalar LIMIT/OFFSET query that DuckDB rewrites with WINDOW."""
    return """
        SELECT order_id
        FROM orders O
        WHERE price = (
            SELECT price FROM orders X
            WHERE X.region = O.region
            ORDER BY price
            LIMIT 1 OFFSET 1
        )
        ORDER BY order_id
    """


def argmax_scalar_limit_sql():
    """Return a scalar top-1 query that can use arg_max slots."""
    return """
        SELECT order_id
        FROM orders O
        WHERE price = (
            SELECT price FROM orders X
            WHERE X.region = O.region
            ORDER BY order_id DESC
            LIMIT 1
        )
        ORDER BY order_id
    """


def add_node_shape_contract_cases(cases):
    """Add contracts for SELECT-reachable specialized plan nodes."""
    for case in representative_plan_shape_cases():
        cases.append(case)
    cases.append(sample_expression_contract_case())


def sample_expression_contract_case():
    """Return a SAMPLE expression variant contract."""
    setup_sql = "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1), (2), (3);"
    query_sql = "SELECT id FROM t USING SAMPLE reservoir(2 ROWS) REPEATABLE(1)"
    return ("sample_reservoir", setup_sql, query_sql)


def add_empty_contract_cases(cases):
    """Add empty-result projection lineage contracts."""
    setup_sql = "CREATE TABLE t(a INTEGER, b INTEGER); INSERT INTO t VALUES (1, 2);"
    query_sql = "SELECT a FROM t WHERE b = 0 AND a < 100 ORDER BY a"
    cases.append(("empty_result_projection_schema", setup_sql, query_sql))


@pytest.mark.parametrize("case_name,setup_sql,query_sql", representative_plan_shape_cases())
def test_audit_representative_plan_shapes_execute(case_name, setup_sql, query_sql):
    """Audit representative node shapes reconstruct and match original rows."""
    assert case_name
    assert_reconstructed_sql_matches_original(setup_sql, query_sql)


@pytest.mark.parametrize("case_name,setup_sql,query_sql", reconstruction_contract_cases())
def test_reconstruction_contract_cases(case_name, setup_sql, query_sql):
    """Enforce parse, execution, placeholder, lineage, and row contracts."""
    assert case_name
    assert_reconstructed_sql_matches_original(setup_sql, query_sql)


def test_observed_select_nodes_have_handlers():
    """Verify every observed SELECT plan node has a relation handler."""
    handlers = relation_handlers()
    missing_names = set()
    for node in observed_contract_nodes():
        if is_scan_node(node):
            continue
        if node.name not in handlers:
            missing_names.add(node.name)
    assert not missing_names


def test_observed_select_nodes_have_representative_contract_cases():
    """Verify every registered handler is reached by a contract case."""
    observed_names = observed_contract_node_names()
    missing_names = set()
    for handler_name in relation_handlers():
        if handler_name not in observed_names:
            missing_names.add(handler_name)
    assert not missing_names


def test_empty_rhs_single_join_fails_instead_of_emitting_invalid_placeholder():
    """Verify unsafe SINGLE JOIN EMPTY_RESULT plans fail instead of lying."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE x(id INTEGER, a INTEGER)")
    root = PlanNode(
        "COMPARISON_JOIN",
        (
            PlanNode(
                "SEQ_SCAN",
                tuple(),
                {"Table": "memory.main.x", "Type": "Sequential Scan"},
            ),
            PlanNode("EMPTY_RESULT", tuple(), {}),
        ),
        {"Join Type": "SINGLE", "Conditions": "id IS NOT DISTINCT FROM id"},
    )
    with pytest.raises(PlanReconstructionError, match="EMPTY_RESULT right side"):
        reconstruct_sql(connection, "SELECT a FROM x", False, True, False, root)
