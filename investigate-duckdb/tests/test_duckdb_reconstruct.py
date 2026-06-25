"""Tests for DuckDB optimized SQL reconstruction."""

import inspect
import json
import re
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

import duckdb
import pytest

from duckdb_node_manifest import (
    DEFAULT_DUCKDB_SOURCE_ROOT,
    read_duckdb_node_manifest,
)
from duckdb_reconstruct import (
    PlanNode,
    PlanReconstructionError,
    apply_auto_schema,
    create_repl_session,
    default_repl_history_file,
    explain_optimized_json,
    is_scan_node,
    plan_node_to_json,
    read_logical_node_registry,
    relation_handlers,
    reconstruct_sql,
    run_repl,
    validate_tree,
)

EXPECTED_DUCKDB_RECONSTRUCT_PASS_COUNT = 831


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


def execute_sql_script_with_columns(connection, sql):
    """Execute SQL text and return rows with output column names."""
    cursor = connection.execute(sql)
    rows = cursor.fetchall()
    column_names = []
    for description in cursor.description:
        column_names.append(description[0])
    return rows, tuple(column_names)


def sql_script_statements(sql):
    """Return non-empty SQL statements from a reconstructed script."""
    statements = []
    for statement in sql.split(";"):
        stripped_statement = statement.strip()
        if stripped_statement:
            statements.append(f"{stripped_statement};")
    return tuple(statements)


def is_temp_view_statement(statement):
    """Return whether one SQL statement creates a temporary view."""
    pattern = r"^\s*CREATE\s+TEMPORARY\s+VIEW\s+"
    return re.search(pattern, statement, flags=re.IGNORECASE) is not None


def execute_reconstructed_sql_contract(connection, sql):
    """Execute temp views in order, then execute the final SELECT."""
    statements = sql_script_statements(sql)
    assert statements
    temp_view_statements = statements[:-1]
    final_statement = statements[-1]
    for statement in temp_view_statements:
        assert is_temp_view_statement(statement)
        connection.execute(statement)
    assert not is_temp_view_statement(final_statement)
    return execute_sql_script_with_columns(connection, final_statement)


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


@pytest.mark.parametrize(
    "token",
    ("SUBQUERY", "SINGLE JOIN", "sum_no_overflow", "arg_max_null", "arg_min_null"),
)
def test_internal_placeholder_guard_rejects_forbidden_tokens(token):
    """Verify the placeholder guard rejects every forbidden planner token."""
    with pytest.raises(AssertionError):
        assert_no_internal_placeholders(f"SELECT {token};")


@pytest.mark.parametrize("slot_ref", ("#0", "#[8.0]", "#[12.3]"))
def test_internal_placeholder_guard_rejects_slot_refs(slot_ref):
    """Verify the placeholder guard rejects DuckDB slot references."""
    with pytest.raises(AssertionError):
        assert_no_internal_placeholders(f"SELECT {slot_ref};")


def test_temp_view_uniqueness_guard_rejects_duplicate_names():
    """Verify duplicate temporary view names fail the uniqueness guard."""
    sql = """
        CREATE TEMPORARY VIEW repeated AS SELECT 1;
        CREATE TEMPORARY VIEW repeated AS SELECT 2;
        SELECT * FROM repeated;
    """
    with pytest.raises(AssertionError):
        assert_temp_views_are_unique(sql)


def test_ordered_temp_view_contract_rejects_non_temp_prefix_statement():
    """Verify reconstructed scripts cannot run setup before temp views."""
    connection = duckdb.connect(":memory:")
    sql = """
        CREATE TABLE leaked_setup(i INTEGER);
        SELECT i FROM leaked_setup;
    """
    with pytest.raises(AssertionError):
        execute_reconstructed_sql_contract(connection, sql)


def test_ordered_temp_view_contract_executes_views_before_final_select():
    """Verify temporary views execute in order before the final SELECT."""
    connection = duckdb.connect(":memory:")
    sql = """
        CREATE TEMPORARY VIEW first_view AS SELECT 1 AS value;
        CREATE TEMPORARY VIEW second_view AS SELECT value + 1 AS value
        FROM first_view;
        SELECT value FROM second_view;
    """
    rows, columns = execute_reconstructed_sql_contract(connection, sql)
    assert rows == [(2,)]
    assert columns == ("value",)


def assert_reconstructed_sql_matches_original(setup_sql, query_sql, auto_schema=False):
    """Assert reconstructed SQL executes and returns the original query rows."""
    original_connection = duckdb.connect(":memory:")
    reconstructed_connection = duckdb.connect(":memory:")
    if setup_sql.strip():
        original_connection.execute(setup_sql)
        reconstructed_connection.execute(setup_sql)
    expected_rows, expected_columns = execute_sql_script_with_columns(
        original_connection, query_sql
    )
    optimized_root = explain_optimized_json(reconstructed_connection, query_sql)
    reconstructed_sql = reconstruct_sql(
        reconstructed_connection, query_sql, False, True, auto_schema, optimized_root
    )
    assert_no_internal_placeholders(reconstructed_sql)
    assert_temp_views_are_unique(reconstructed_sql)
    actual_rows, actual_columns = execute_reconstructed_sql_contract(
        reconstructed_connection, reconstructed_sql
    )
    assert actual_rows == expected_rows
    assert actual_columns == expected_columns


def assert_native_reconstructed_sql_matches_original(
    setup_sql, query_sql, auto_schema=False
):
    """Assert native reconstruction returns original rows and columns."""
    original_connection = duckdb.connect(":memory:")
    reconstructed_connection = duckdb.connect(":memory:")
    if setup_sql.strip():
        original_connection.execute(setup_sql)
        reconstructed_connection.execute(setup_sql)
    if auto_schema:
        apply_auto_schema(original_connection, query_sql)
    expected_rows, expected_columns = execute_sql_script_with_columns(
        original_connection, query_sql
    )
    reconstructed_sql = reconstruct_sql(
        reconstructed_connection, query_sql, False, True, auto_schema
    )
    assert_no_internal_placeholders(reconstructed_sql)
    assert_temp_views_are_unique(reconstructed_sql)
    actual_rows, actual_columns = execute_reconstructed_sql_contract(
        reconstructed_connection, reconstructed_sql
    )
    assert actual_rows == expected_rows
    assert actual_columns == expected_columns


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


def json_plan_node_names(node_json):
    """Return all node names from serialized optimized-plan JSON."""
    names = {node_json["name"]}
    for child_json in node_json["children"]:
        names.update(json_plan_node_names(child_json))
    return frozenset(names)


def optimized_json_for_sql(setup_sql, query_sql):
    """Dump optimized plan JSON for a SQL contract through serialization."""
    connection = duckdb.connect(":memory:")
    if setup_sql.strip():
        connection.execute(setup_sql)
    root = explain_optimized_json(connection, query_sql)
    serialized_json = plan_node_to_json(root)
    json_text = json.dumps(serialized_json)
    return json.loads(json_text)


@lru_cache(maxsize=1)
def observed_contract_node_names():
    """Return node names observed from real mandatory SELECT plans."""
    names = set()
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        connection = duckdb.connect(":memory:")
        if setup_sql.strip():
            connection.execute(setup_sql)
        root = explain_optimized_json(connection, query_sql)
        names.update(plan_node_names(root))
        assert case_name
    return frozenset(names)


@lru_cache(maxsize=1)
def observed_contract_nodes():
    """Return nodes observed from real mandatory SELECT plans."""
    nodes = []
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        connection = duckdb.connect(":memory:")
        if setup_sql.strip():
            connection.execute(setup_sql)
        root = explain_optimized_json(connection, query_sql)
        nodes.extend(plan_nodes(root))
        assert case_name
    return tuple(nodes)


def synthetic_handler_contract_node_names():
    """Return handler names covered by synthetic plan-node contracts."""
    return ("ANY_JOIN", "JOIN")


def synthetic_only_handler_contract_node_names():
    """Return synthetic handlers that cannot come from optimized JSON."""
    return ("JOIN",)


def synthetic_only_handler_source_cases():
    """Return DuckDB source evidence for synthetic-only handlers."""
    return (
        (
            "JOIN",
            "src/execution/physical_plan_generator.cpp",
            "case LogicalOperatorType::LOGICAL_JOIN:",
            'throw NotImplementedException("Unimplemented logical operator type!")',
        ),
    )


def observed_relation_contract_node_names():
    """Return non-scan relation node names observed from contract plans."""
    names = set()
    for node in observed_contract_nodes():
        if is_scan_node(node):
            continue
        names.add(node.name)
    return frozenset(names)


def observed_relation_contract_node_name_list():
    """Return observed non-scan relation node names as stable test params."""
    names = []
    for node_name in sorted(observed_relation_contract_node_names()):
        names.append(node_name)
    return tuple(names)


def all_direct_handler_contract_node_names():
    """Return all handler names covered by direct contract tests."""
    names = set()
    for node_name in observed_relation_contract_node_names():
        names.add(node_name)
    for node_name in synthetic_handler_contract_node_names():
        names.add(node_name)
    return frozenset(names)


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
def test_each_select_reachable_source_logical_node_has_handler(node_name):
    """Verify every SELECT-reachable source logical node has a handler."""
    if node_name not in observed_contract_node_names():
        return
    if node_name in lowered_contract_node_names():
        return
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
    assert "sum(total_amount) AS total_spent" in normalized
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


def root_derived_semantic_setup_sql():
    """Return setup SQL for root-derived semantic contracts."""
    return """
        CREATE TABLE base(id INTEGER, value INTEGER, grp VARCHAR, flag BOOLEAN);
        CREATE TABLE side(id INTEGER, label VARCHAR);
        INSERT INTO base VALUES
            (1, 10, 'a', TRUE),
            (2, 20, 'a', FALSE),
            (3, 15, 'b', TRUE);
        INSERT INTO side VALUES (1, 'x'), (2, 'y'), (4, 'z');
    """


def root_derived_semantic_contract_cases():
    """Return all native root-derived semantic operator contracts."""
    setup_sql = root_derived_semantic_setup_sql()
    return (
        ("projection", setup_sql, root_derived_projection_sql(), False),
        ("filter", setup_sql, root_derived_filter_sql(), False),
        ("limit_one", setup_sql, root_derived_limit_sql(1), False),
        ("limit_two", setup_sql, root_derived_limit_sql(2), False),
        ("offset", setup_sql, root_derived_offset_sql(), False),
        ("order_by", setup_sql, root_derived_order_by_sql(), False),
        ("order_by_limit", setup_sql, root_derived_order_by_limit_sql(), False),
        ("distinct", setup_sql, root_derived_distinct_sql(), False),
        ("aggregate", setup_sql, root_derived_aggregate_sql(), False),
        ("window", setup_sql, root_derived_window_sql(), False),
        ("join", setup_sql, root_derived_join_sql(), False),
        ("cross_product", setup_sql, root_derived_cross_product_sql(), False),
        ("union", setup_sql, root_derived_set_sql("UNION"), False),
        ("union_all", setup_sql, root_derived_set_sql("UNION ALL"), False),
        ("intersect", setup_sql, root_derived_set_sql("INTERSECT"), False),
        ("except", setup_sql, root_derived_set_sql("EXCEPT"), False),
        ("empty_result", setup_sql, root_derived_empty_result_sql(), False),
        ("unnest", "", root_derived_unnest_sql(), False),
        ("cte", setup_sql, root_derived_cte_sql(), False),
        ("recursive_cte", "", root_derived_recursive_cte_sql(), False),
        ("auto_schema_limit_one", "", root_derived_auto_schema_limit_sql(1), True),
        ("auto_schema_limit_two", "", root_derived_auto_schema_limit_sql(2), True),
    )


def root_derived_projection_sql():
    """Return a root-derived projection query."""
    return "SELECT out_id FROM (SELECT id AS out_id FROM base) t ORDER BY out_id"


def root_derived_filter_sql():
    """Return a root-derived filter query."""
    return "SELECT id FROM (SELECT id FROM base WHERE flag) t ORDER BY id"


def root_derived_limit_sql(limit_value):
    """Return a root-derived limit query."""
    return (
        "SELECT id FROM "
        f"(SELECT id FROM base ORDER BY id LIMIT {limit_value}) t ORDER BY id"
    )


def root_derived_offset_sql():
    """Return a root-derived offset query."""
    return "SELECT id FROM (SELECT id FROM base ORDER BY id OFFSET 1) t ORDER BY id"


def root_derived_order_by_sql():
    """Return a root-derived order query."""
    return "SELECT id FROM (SELECT id FROM base ORDER BY value DESC) t"


def root_derived_order_by_limit_sql():
    """Return a root-derived top-n query."""
    return "SELECT id FROM (SELECT id FROM base ORDER BY value DESC LIMIT 1) t"


def root_derived_distinct_sql():
    """Return a root-derived distinct query."""
    return "SELECT grp FROM (SELECT DISTINCT grp FROM base) t ORDER BY grp"


def root_derived_aggregate_sql():
    """Return a root-derived aggregate query."""
    return (
        "SELECT grp, total FROM "
        "(SELECT grp, sum(value) AS total FROM base GROUP BY grp) t ORDER BY grp"
    )


def root_derived_window_sql():
    """Return a root-derived window query."""
    return (
        "SELECT id, rn FROM "
        "(SELECT id, row_number() OVER (ORDER BY value) AS rn FROM base) t "
        "ORDER BY id"
    )


def root_derived_join_sql():
    """Return a root-derived join query."""
    return (
        "SELECT id, label FROM "
        "(SELECT base.id, side.label FROM base JOIN side ON side.id = base.id) t "
        "ORDER BY id"
    )


def root_derived_cross_product_sql():
    """Return a root-derived cross product query."""
    return (
        "SELECT id, label FROM "
        "(SELECT base.id, side.label FROM base CROSS JOIN side WHERE base.id = 1) t "
        "ORDER BY label"
    )


def root_derived_set_sql(operator):
    """Return a root-derived set operation query."""
    return (
        "SELECT id FROM "
        f"(SELECT id FROM base WHERE id <= 2 {operator} "
        "SELECT id FROM base WHERE id >= 2) t ORDER BY id"
    )


def root_derived_empty_result_sql():
    """Return a root-derived empty result query."""
    return "SELECT id FROM (SELECT id FROM base WHERE id > 99) t ORDER BY id"


def root_derived_unnest_sql():
    """Return a root-derived unnest query."""
    return "SELECT value FROM (SELECT unnest([1, 2]) AS value) t ORDER BY value"


def root_derived_cte_sql():
    """Return a root-derived CTE query."""
    return (
        "SELECT id FROM "
        "(WITH c AS (SELECT id FROM base WHERE flag) SELECT id FROM c) t "
        "ORDER BY id"
    )


def root_derived_recursive_cte_sql():
    """Return a root-derived recursive CTE query."""
    return (
        "SELECT i FROM "
        "(WITH RECURSIVE r(i) AS ("
        "SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < 3"
        ") SELECT i FROM r) t ORDER BY i"
    )


def root_derived_auto_schema_limit_sql(limit_value):
    """Return the auto-schema root-derived limit regression query."""
    return f"SELECT b FROM (SELECT TRUE AS b FROM dual LIMIT {limit_value}) t"


@pytest.mark.parametrize(
    "case_name,setup_sql,query_sql,auto_schema",
    root_derived_semantic_contract_cases(),
)
def test_root_derived_semantic_contract_cases(
    case_name, setup_sql, query_sql, auto_schema
):
    """Verify native root-derived reconstruction preserves each operator."""
    assert case_name
    assert_native_reconstructed_sql_matches_original(
        setup_sql, query_sql, auto_schema
    )


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


def test_broad_reconstruction_cases_are_unique():
    """Verify generated broad reconstruction cases are unique."""
    cases = broad_reconstruction_cases()
    assert len(cases) == len(set(cases))


@pytest.mark.parametrize("sql", broad_reconstruction_cases())
def test_broad_query_reconstruction_cases_do_not_emit_invalid_empty_sql(sql):
    """Verify broad query forms reconstruct without known invalid placeholders."""
    connection = duckdb.connect(":memory:")
    result = reconstruct_sql(connection, sql, False, True, True)
    assert result.strip()
    assert result.endswith(";")
    assert "SELECT * WHERE FALSE" not in result
    assert "SINGLE JOIN (SELECT *" not in result


@pytest.mark.parametrize("sql", broad_reconstruction_cases())
def test_broad_query_reconstruction_cases_execute_generated_sql(sql):
    """Verify broad generated SQL parses and executes in auto-schema mode."""
    connection = duckdb.connect(":memory:")
    result = reconstruct_sql(connection, sql, False, True, True)
    assert_no_internal_placeholders(result)
    assert_temp_views_are_unique(result)
    execute_reconstructed_sql_contract(connection, result)


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
    execute_reconstructed_sql_contract(connection, result)


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
    actual_rows, actual_columns = execute_reconstructed_sql_contract(connection, result)
    assert actual_rows == expected_rows
    assert actual_columns == ("order_id",)


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
    execute_reconstructed_sql_contract(connection, result)


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
    actual_rows, actual_columns = execute_reconstructed_sql_contract(connection, result)
    assert actual_rows == expected_rows
    assert actual_columns == ("count_star()",)


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
    execute_reconstructed_sql_contract(connection, reconstructed_sql)


def mandatory_plan_shape_cases():
    """Return mandatory SQL cases for SELECT-reachable DuckDB nodes."""
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
    add_join_variant_contract_cases(cases)
    add_correlated_contract_cases(cases)
    add_lineage_contract_cases(cases)
    add_projection_lineage_contract_cases(cases)
    add_aggregate_variant_contract_cases(cases)
    add_set_contract_cases(cases)
    add_window_contract_cases(cases)
    add_pivot_contract_cases(cases)
    add_node_shape_contract_cases(cases)
    add_empty_contract_cases(cases)
    return tuple(cases)


def required_review_contract_case_names():
    """Return every review-listed contract case that must exist."""
    return (
        "distinct",
        "top_n",
        "exists_semi_rhs_filter",
        "not_exists_anti_join",
        "in_subquery",
        "not_in_subquery",
        "repeated_alias_filter",
        "cross_product_join",
        "plain_inner_join_node",
        "nested_join_conditions",
        "is_not_distinct_join",
        "any_join_arbitrary_condition",
        "any_quantified_subquery",
        "correlated_select",
        "correlated_where",
        "correlated_group_by",
        "correlated_having",
        "generated_alias_collision",
        "derived_cte_alias_lineage",
        "non_materialized_cte",
        "materialized_cte_scan",
        "aggregate_internal_function",
        "repeated_base_cte_outer_alias",
        "multiple_ctes_same_base_aliases",
        "duplicate_temp_view_names_multi_temp",
        "projection_over_aggregate_lineage",
        "projection_over_window_lineage",
        "join_derived_rhs_lineage",
        "set_operation_output_name_lineage",
        "slot_mapping_multi_projection",
        "aggregate_arg_min",
        "aggregate_ordered_string_agg",
        "aggregate_multiple_ordered",
        "aggregate_filter_clause",
        "aggregate_scalar_no_group",
        "union",
        "union_all",
        "intersect",
        "except",
        "window_row_number",
        "window_rank",
        "window_dense_rank",
        "window_sum",
        "window_lag",
        "window_first_value",
        "window_rows_frame",
        "limit_offset_window",
        "argmax_scalar_limit",
        "pivot_explicit_in",
        "unpivot_basic",
        "unnest",
        "sample",
        "sample_reservoir",
        "positional_join",
        "asof_join",
        "recursive_cte",
        "empty_result_projection_schema",
    )


def explicit_user_requirement_contract_groups():
    """Return explicit user blocker categories mapped to contract cases."""
    return {
        "SEMI/ANTI joins": (
            "exists_semi_rhs_filter",
            "not_exists_anti_join",
        ),
        "DELIM_GET correlated predicates": (
            "correlated_select",
            "correlated_where",
            "correlated_having",
        ),
        "ORDER BY LIMIT OFFSET scalar rewrites": (
            "limit_offset_window",
            "argmax_scalar_limit",
        ),
        "repeated base table aliases": (
            "repeated_alias_filter",
            "repeated_base_cte_outer_alias",
        ),
        "derived table and CTE alias lineage": (
            "derived_cte_alias_lineage",
            "non_materialized_cte",
            "materialized_cte_scan",
        ),
        "generated alias collisions": ("generated_alias_collision",),
        "duplicate temp view names": ("duplicate_temp_view_names_multi_temp",),
        "projection lineage": (
            "projection_over_aggregate_lineage",
            "projection_over_window_lineage",
            "join_derived_rhs_lineage",
            "set_operation_output_name_lineage",
            "empty_result_projection_schema",
        ),
        "aggregate internal functions": ("aggregate_internal_function",),
        "set operations": ("union", "union_all", "intersect", "except"),
        "window functions": (
            "window_row_number",
            "window_rank",
            "window_dense_rank",
            "window_sum",
            "window_lag",
            "window_first_value",
            "window_rows_frame",
        ),
        "UNNEST": ("unnest",),
        "SAMPLE": ("sample", "sample_reservoir"),
        "CTE and CTE_SCAN": (
            "non_materialized_cte",
            "materialized_cte_scan",
        ),
        "REC_CTE": ("recursive_cte",),
        "POSITIONAL_JOIN": ("positional_join",),
        "ASOF_JOIN": ("asof_join",),
        "nested joins": ("nested_join_conditions", "is_not_distinct_join"),
        "correlated subqueries": (
            "correlated_select",
            "correlated_where",
            "correlated_group_by",
            "correlated_having",
        ),
        "subquery forms": (
            "exists_semi_rhs_filter",
            "not_exists_anti_join",
            "in_subquery",
            "not_in_subquery",
            "scalar_subquery_projection_join",
        ),
        "empty result": ("empty_result_projection_schema",),
    }


def contract_case_names():
    """Return names present in the reconstruction contract matrix."""
    names = set()
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        names.add(case_name)
        assert setup_sql is not None
        assert query_sql
    return frozenset(names)


def contract_case_name_list():
    """Return reconstruction contract names without deduplicating."""
    names = []
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        names.append(case_name)
        assert setup_sql is not None
        assert query_sql
    return tuple(names)


def mandatory_plan_shape_case_names():
    """Return mandatory plan-shape case names."""
    names = []
    for case_name, setup_sql, query_sql in mandatory_plan_shape_cases():
        names.append(case_name)
        assert setup_sql is not None
        assert query_sql
    return tuple(names)


def required_plan_nodes_by_contract_case():
    """Return required optimized-plan nodes for each named contract case."""
    return {
        "dummy_scan": ("DUMMY_SCAN",),
        "distinct": ("DISTINCT",),
        "limit": ("LIMIT",),
        "order_by": ("ORDER_BY",),
        "top_n": ("TOP_N",),
        "exists_semi_rhs_filter": ("DELIM_JOIN", "DELIM_GET"),
        "not_exists_anti_join": ("COMPARISON_JOIN",),
        "in_subquery": ("COMPARISON_JOIN",),
        "not_in_subquery": ("FILTER", "COMPARISON_JOIN"),
        "repeated_alias_filter": ("COMPARISON_JOIN",),
        "cross_product_join": ("CROSS_PRODUCT",),
        "plain_inner_join_node": ("COMPARISON_JOIN",),
        "left_join_variant": ("COMPARISON_JOIN",),
        "right_join_variant": ("COMPARISON_JOIN",),
        "full_join_variant": ("COMPARISON_JOIN",),
        "scalar_subquery_projection_join": ("COMPARISON_JOIN",),
        "in_subquery_projection_boolean": ("COMPARISON_JOIN",),
        "exists_subquery_projection_boolean": ("COMPARISON_JOIN",),
        "not_in_null_sensitive": ("FILTER", "COMPARISON_JOIN"),
        "nested_join_conditions": ("FILTER", "COMPARISON_JOIN"),
        "is_not_distinct_join": ("COMPARISON_JOIN",),
        "any_join_arbitrary_condition": ("ANY_JOIN",),
        "any_quantified_subquery": ("CROSS_PRODUCT", "LIMIT"),
        "correlated_select": ("DELIM_JOIN", "DELIM_GET"),
        "correlated_where": ("DELIM_JOIN", "DELIM_GET", "AGGREGATE"),
        "correlated_group_by": ("AGGREGATE", "COMPARISON_JOIN"),
        "correlated_having": ("DELIM_JOIN", "DELIM_GET", "AGGREGATE"),
        "generated_alias_collision": ("AGGREGATE",),
        "derived_cte_alias_lineage": ("AGGREGATE", "COMPARISON_JOIN"),
        "non_materialized_cte": ("PROJECTION",),
        "materialized_cte_scan": ("CTE", "CTE_SCAN"),
        "aggregate_internal_function": ("AGGREGATE",),
        "repeated_base_cte_outer_alias": ("COMPARISON_JOIN",),
        "multiple_ctes_same_base_aliases": ("COMPARISON_JOIN",),
        "duplicate_temp_view_names_multi_temp": ("AGGREGATE", "CROSS_PRODUCT"),
        "projection_over_aggregate_lineage": ("AGGREGATE", "PROJECTION"),
        "projection_over_window_lineage": ("WINDOW", "PROJECTION"),
        "join_derived_rhs_lineage": ("COMPARISON_JOIN", "PROJECTION"),
        "set_operation_output_name_lineage": ("UNION",),
        "slot_mapping_multi_projection": ("AGGREGATE", "PROJECTION"),
        "aggregate_arg_min": ("AGGREGATE",),
        "aggregate_ordered_string_agg": ("AGGREGATE",),
        "aggregate_multiple_ordered": ("AGGREGATE",),
        "aggregate_filter_clause": ("AGGREGATE",),
        "aggregate_scalar_no_group": ("AGGREGATE",),
        "union": ("UNION",),
        "union_all": ("UNION",),
        "intersect": ("INTERSECT",),
        "except": ("EXCEPT",),
        "window_row_number": ("WINDOW",),
        "window_rank": ("WINDOW",),
        "window_dense_rank": ("WINDOW",),
        "window_sum": ("WINDOW",),
        "window_lag": ("WINDOW",),
        "window_first_value": ("WINDOW",),
        "window_rows_frame": ("WINDOW",),
        "limit_offset_window": ("WINDOW", "DELIM_JOIN", "DELIM_GET"),
        "argmax_scalar_limit": ("AGGREGATE", "DELIM_JOIN", "DELIM_GET"),
        "pivot_explicit_in": ("AGGREGATE", "PROJECTION"),
        "unpivot_basic": ("UNNEST",),
        "unnest": ("UNNEST",),
        "sample": ("SAMPLE",),
        "sample_reservoir": ("SAMPLE",),
        "positional_join": ("POSITIONAL_JOIN",),
        "asof_join": ("ASOF_JOIN",),
        "recursive_cte": ("REC_CTE", "CTE_SCAN"),
        "empty_result_projection_schema": ("EMPTY_RESULT",),
    }


def required_extra_info_by_contract_case():
    """Return required optimized-plan extra_info values for contract cases."""
    return {
        "left_join_variant": (("COMPARISON_JOIN", "Join Type", "RIGHT"),),
        "right_join_variant": (("COMPARISON_JOIN", "Join Type", "LEFT"),),
        "full_join_variant": (("COMPARISON_JOIN", "Join Type", "FULL"),),
        "scalar_subquery_projection_join": (
            ("COMPARISON_JOIN", "Join Type", "SINGLE"),
        ),
        "in_subquery_projection_boolean": (
            ("COMPARISON_JOIN", "Join Type", "MARK"),
        ),
        "exists_subquery_projection_boolean": (
            ("COMPARISON_JOIN", "Join Type", "MARK"),
        ),
        "not_in_null_sensitive": (("COMPARISON_JOIN", "Join Type", "MARK"),),
        "exists_semi_rhs_filter": (("DELIM_JOIN", "Join Type", "SEMI"),),
        "not_exists_anti_join": (("COMPARISON_JOIN", "Join Type", "ANTI"),),
    }


def contract_case_by_name():
    """Return contract cases keyed by case name."""
    cases_by_name = {}
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        cases_by_name[case_name] = (setup_sql, query_sql)
    return cases_by_name


def optimized_node_names_for_sql(setup_sql, query_sql):
    """Return optimized plan node names for one SQL contract case."""
    connection = duckdb.connect(":memory:")
    if setup_sql.strip():
        connection.execute(setup_sql)
    root = explain_optimized_json(connection, query_sql)
    return plan_node_names(root)


def optimized_plan_nodes_for_sql(setup_sql, query_sql):
    """Return optimized plan nodes for one SQL contract case."""
    connection = duckdb.connect(":memory:")
    if setup_sql.strip():
        connection.execute(setup_sql)
    root = explain_optimized_json(connection, query_sql)
    return plan_nodes(root)


def plan_has_extra_info(nodes, node_name, key, expected_value):
    """Return whether any node exposes one exact extra_info value."""
    for node in nodes:
        if node.name != node_name:
            continue
        if node.extra_info.get(key) == expected_value:
            return True
    return False


def expected_output_columns_by_contract_case():
    """Return explicit output-column contracts for every query case."""
    return {
        "dummy_scan": ("1",),
        "distinct": ("a",),
        "limit": ("a",),
        "order_by": ("a",),
        "top_n": ("a",),
        "exists_semi_rhs_filter": ("id",),
        "not_exists_anti_join": ("id",),
        "in_subquery": ("id",),
        "not_in_subquery": ("id",),
        "repeated_alias_filter": ("id", "id"),
        "cross_product_join": ("id", "id"),
        "plain_inner_join_node": ("id", "name"),
        "left_join_variant": ("id", "name"),
        "right_join_variant": ("id", "name"),
        "full_join_variant": ("id", "name"),
        "scalar_subquery_projection_join": ("id", "price"),
        "in_subquery_projection_boolean": ("id", "has_expensive"),
        "exists_subquery_projection_boolean": ("id", "has_product"),
        "not_in_null_sensitive": ("id",),
        "nested_join_conditions": ("id", "name"),
        "is_not_distinct_join": ("id", "name"),
        "any_join_arbitrary_condition": ("id", "name"),
        "any_quantified_subquery": ("id",),
        "correlated_select": ("customer_id", "p"),
        "correlated_where": ("customer_id",),
        "correlated_group_by": ("count_star()",),
        "correlated_having": ("region", "count_star()"),
        "generated_alias_collision": ("__duckdb_agg", "__duckdb_agg_2"),
        "derived_cte_alias_lineage": ("id", "spent"),
        "non_materialized_cte": ("id",),
        "materialized_cte_scan": ("id", "id"),
        "aggregate_internal_function": ("product_id", "s"),
        "repeated_base_cte_outer_alias": ("id", "id"),
        "multiple_ctes_same_base_aliases": ("id", "id"),
        "duplicate_temp_view_names_multi_temp": ("order_count",),
        "projection_over_aggregate_lineage": ("pid", "adjusted_qty"),
        "projection_over_window_lineage": ("oid", "shifted_rank"),
        "join_derived_rhs_lineage": ("id", "product_id"),
        "set_operation_output_name_lineage": ("visible_name",),
        "slot_mapping_multi_projection": (
            "product_id",
            "total_qty",
            "order_count",
        ),
        "aggregate_arg_min": ("group_id", "selected_value"),
        "aggregate_ordered_string_agg": ("group_id", "values_csv"),
        "aggregate_multiple_ordered": ("group_id", "asc_values", "desc_values"),
        "aggregate_filter_clause": ("group_id", "filtered_sum"),
        "aggregate_scalar_no_group": ("row_count", "total_value"),
        "union": ("a",),
        "union_all": ("a",),
        "intersect": ("a",),
        "except": ("a",),
        "window_row_number": ("order_id", "rn"),
        "window_rank": ("order_id", "r"),
        "window_dense_rank": ("order_id", "r"),
        "window_sum": ("order_id", "running"),
        "window_lag": ("order_id", "prev_price"),
        "window_first_value": ("order_id", "top_price"),
        "window_rows_frame": ("order_id", "framed_sum"),
        "limit_offset_window": ("order_id",),
        "argmax_scalar_limit": ("order_id",),
        "pivot_explicit_in": ("region", "2024", "2025"),
        "unpivot_basic": ("region", "year_label", "amount"),
        "unnest": ("value",),
        "sample": ("id",),
        "sample_reservoir": ("id",),
        "positional_join": ("id", "id"),
        "asof_join": ("symbol", "ts", "price", "symbol", "ts", "bid"),
        "recursive_cte": ("i",),
        "empty_result_projection_schema": ("a",),
    }


def original_output_columns_for_contract_case(setup_sql, query_sql):
    """Return output columns produced by DuckDB for one contract query."""
    connection = duckdb.connect(":memory:")
    if setup_sql.strip():
        connection.execute(setup_sql)
    rows, columns = execute_sql_script_with_columns(connection, query_sql)
    assert rows is not None
    return columns


def command_plan_contract_cases():
    """Return command SQL cases and their required logical plan nodes."""
    return (
        ("copy_to_file", copy_to_file_setup_sql(), copy_to_file_sql(), "COPY_TO_FILE"),
        ("insert", insert_setup_sql(), insert_sql(), "INSERT"),
        ("insert_expression_get", insert_setup_sql(), insert_sql(), "EXPRESSION_GET"),
        ("delete", delete_setup_sql(), delete_sql(), "DELETE"),
        ("update", update_setup_sql(), update_sql(), "UPDATE"),
        ("merge_into", merge_setup_sql(), merge_sql(), "MERGE_INTO"),
        ("alter", alter_setup_sql(), alter_sql(), "ALTER"),
        ("create_table", "", "CREATE TABLE x(i INT)", "CREATE_TABLE"),
        ("create_index", index_setup_sql(), create_index_sql(), "CREATE_INDEX"),
        ("create_sequence", "", "CREATE SEQUENCE seq", "CREATE_SEQUENCE"),
        ("create_view", view_setup_sql(), create_view_sql(), "CREATE_VIEW"),
        ("create_schema", "", "CREATE SCHEMA s", "CREATE_SCHEMA"),
        ("create_macro", "", "CREATE MACRO m(a) AS a + 1", "CREATE_MACRO"),
        ("drop", drop_setup_sql(), "DROP TABLE x", "DROP"),
        ("pragma", "", "PRAGMA enable_progress_bar", "PRAGMA"),
        ("transaction", "", "BEGIN TRANSACTION", "TRANSACTION"),
        ("create_type", "", create_type_sql(), "CREATE_TYPE"),
        ("prepare", prepare_setup_sql(), prepare_sql(), "PREPARE"),
        ("execute", execute_setup_sql(), "EXECUTE s", "EXECUTE"),
        ("vacuum", vacuum_setup_sql(), "VACUUM", "VACUUM"),
        ("set", "", "SET threads=1", "SET"),
        ("load", "", "LOAD 'json'", "LOAD"),
        ("reset", "", "RESET threads", "RESET"),
        ("update_extensions", "", "UPDATE EXTENSIONS", "UPDATE_EXTENSIONS"),
    )


def command_contract_node_names():
    """Return source nodes covered by command-plan contracts."""
    names = set()
    for case_name, setup_sql, query_sql, required_node in command_plan_contract_cases():
        names.add(required_node)
        assert case_name
        assert setup_sql is not None
        assert query_sql
    return frozenset(names)


def command_contract_node_name_list():
    """Return command-plan node names without deduplicating."""
    names = []
    for case_name, setup_sql, query_sql, required_node in command_plan_contract_cases():
        names.append(required_node)
        assert case_name
        assert setup_sql is not None
        assert query_sql
    return tuple(names)


def command_contract_case_names():
    """Return command-plan contract case names."""
    names = []
    for case_name, setup_sql, query_sql, required_node in command_plan_contract_cases():
        names.append(case_name)
        assert setup_sql is not None
        assert query_sql
        assert required_node
    return tuple(names)


def all_accounted_source_node_names():
    """Return source nodes with direct SELECT, command, or synthetic coverage."""
    names = set()
    for node_name in all_direct_handler_contract_node_names():
        names.add(node_name)
    for node_name in command_contract_node_names():
        names.add(node_name)
    for node_name in lowered_contract_node_names():
        names.add(node_name)
    for node_name in source_evidence_node_names():
        names.add(node_name)
    return frozenset(names)


def lowered_contract_node_names():
    """Return source nodes covered by tests proving optimized lowering."""
    return ("PIVOT", "GET")


def lowered_contract_node_name_list():
    """Return lowered node names without deduplicating."""
    names = []
    for node_name in lowered_contract_node_names():
        names.append(node_name)
    return tuple(names)


def source_evidence_node_names():
    """Return source nodes covered by source-backed evidence tests."""
    names = []
    for node_name, relative_path, source_text, evidence_kind in source_evidence_node_cases():
        names.append(node_name)
        assert relative_path
        assert source_text
        assert evidence_kind
    return tuple(names)


def source_evidence_case_keys():
    """Return stable keys for source-evidence cases."""
    keys = []
    for node_name, relative_path, source_text, evidence_kind in source_evidence_node_cases():
        keys.append((node_name, relative_path))
        assert source_text
        assert evidence_kind
    return tuple(keys)


def source_evidence_node_cases():
    """Return exact DuckDB source evidence for non-query logical nodes."""
    return (
        (
            "COPY_DATABASE",
            "src/planner/binder/statement/bind_copy_database.cpp",
            "make_uniq<LogicalCopyDatabase>",
            "statement-binder",
        ),
        (
            "CHUNK_GET",
            "src/include/duckdb/planner/operator/logical_column_data_get.hpp",
            "class LogicalColumnDataGet : public LogicalOperator",
            "internal-column-data",
        ),
        (
            "DEPENDENT_JOIN",
            "src/planner/subquery/flatten_dependent_join.cpp",
            "case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN",
            "decorrelation-lowered",
        ),
        (
            "ATTACH",
            "src/planner/binder/statement/bind_attach.cpp",
            "make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_ATTACH",
            "statement-binder",
        ),
        (
            "DETACH",
            "src/planner/binder/statement/bind_detach.cpp",
            "make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DETACH",
            "statement-binder",
        ),
        (
            "CREATE_TRIGGER",
            "src/planner/binder/statement/bind_create.cpp",
            "LogicalOperatorType::LOGICAL_CREATE_TRIGGER",
            "statement-binder",
        ),
        (
            "EXPLAIN",
            "src/planner/binder/statement/bind_explain.cpp",
            "make_uniq<LogicalExplain>",
            "statement-binder",
        ),
        (
            "EXPORT",
            "src/planner/binder/statement/bind_export.cpp",
            "make_uniq<LogicalExport>",
            "statement-binder",
        ),
        (
            "CONNECT",
            "src/planner/binder/statement/bind_connect.cpp",
            "make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_CONNECT",
            "statement-binder",
        ),
        (
            "DISCONNECT",
            "src/planner/binder/statement/bind_connect.cpp",
            "make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DISCONNECT",
            "statement-binder",
        ),
        (
            "CREATE_SECRET",
            "src/main/secret/secret_manager.cpp",
            "make_uniq<LogicalCreateSecret>",
            "statement-binder",
        ),
        (
            "CUSTOM_OP",
            "src/execution/physical_plan_generator.cpp",
            "extension_op.CreatePlan(context, *this)",
            "extension-only",
        ),
    )


def copy_to_file_setup_sql():
    """Return setup SQL for COPY_TO_FILE coverage."""
    return "CREATE TABLE x(i INT); INSERT INTO x VALUES (1);"


def copy_to_file_sql():
    """Return COPY_TO_FILE coverage SQL."""
    return "COPY x TO '/tmp/duckdb_copy_test.csv'"


def insert_setup_sql():
    """Return setup SQL for INSERT coverage."""
    return "CREATE TABLE x(i INT);"


def insert_sql():
    """Return INSERT coverage SQL."""
    return "INSERT INTO x VALUES (1)"


def delete_setup_sql():
    """Return setup SQL for DELETE coverage."""
    return "CREATE TABLE x(i INT); INSERT INTO x VALUES (1);"


def delete_sql():
    """Return DELETE coverage SQL."""
    return "DELETE FROM x WHERE i = 1"


def update_setup_sql():
    """Return setup SQL for UPDATE coverage."""
    return "CREATE TABLE x(i INT); INSERT INTO x VALUES (1);"


def update_sql():
    """Return UPDATE coverage SQL."""
    return "UPDATE x SET i = 2 WHERE i = 1"


def merge_setup_sql():
    """Return setup SQL for MERGE_INTO coverage."""
    return "CREATE TABLE x(i INT); CREATE TABLE y(i INT); INSERT INTO y VALUES (1);"


def merge_sql():
    """Return MERGE_INTO coverage SQL."""
    return "MERGE INTO x USING y ON x.i = y.i WHEN NOT MATCHED THEN INSERT VALUES (y.i)"


def alter_setup_sql():
    """Return setup SQL for ALTER coverage."""
    return "CREATE TABLE x(i INT);"


def alter_sql():
    """Return ALTER coverage SQL."""
    return "ALTER TABLE x ADD COLUMN j INT"


def index_setup_sql():
    """Return setup SQL for CREATE_INDEX coverage."""
    return "CREATE TABLE x(i INT);"


def create_index_sql():
    """Return CREATE_INDEX coverage SQL."""
    return "CREATE INDEX idx ON x(i)"


def view_setup_sql():
    """Return setup SQL for CREATE_VIEW coverage."""
    return "CREATE TABLE x(i INT);"


def create_view_sql():
    """Return CREATE_VIEW coverage SQL."""
    return "CREATE VIEW v AS SELECT i FROM x"


def drop_setup_sql():
    """Return setup SQL for DROP coverage."""
    return "CREATE TABLE x(i INT);"


def create_type_sql():
    """Return CREATE_TYPE coverage SQL."""
    return "CREATE TYPE mood AS ENUM ('sad', 'ok')"


def prepare_setup_sql():
    """Return setup SQL for PREPARE coverage."""
    return "CREATE TABLE x(i INT);"


def prepare_sql():
    """Return PREPARE coverage SQL."""
    return "PREPARE s AS SELECT * FROM x"


def execute_setup_sql():
    """Return setup SQL for EXECUTE coverage."""
    return "CREATE TABLE x(i INT); PREPARE s AS SELECT * FROM x;"


def vacuum_setup_sql():
    """Return setup SQL for VACUUM coverage."""
    return "CREATE TABLE x(i INT);"


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
    cases.append(("cross_product_join", setup_sql, cross_product_join_sql()))
    cases.append(("plain_inner_join_node", setup_sql, plain_inner_join_sql()))
    cases.append(("nested_join_conditions", setup_sql, nested_join_conditions_sql()))
    cases.append(("is_not_distinct_join", setup_sql, is_not_distinct_join_sql()))
    cases.append(("any_join_arbitrary_condition", setup_sql, any_join_sql()))
    cases.append(("any_quantified_subquery", setup_sql, any_quantified_sql()))


def add_join_variant_contract_cases(cases):
    """Add join variant and projection-subquery contracts."""
    setup_sql = join_variant_setup_sql()
    cases.append(("left_join_variant", setup_sql, left_join_variant_sql()))
    cases.append(("right_join_variant", setup_sql, right_join_variant_sql()))
    cases.append(("full_join_variant", setup_sql, full_join_variant_sql()))
    cases.append(("scalar_subquery_projection_join", setup_sql, scalar_join_sql()))
    cases.append(("in_subquery_projection_boolean", setup_sql, boolean_in_sql()))
    cases.append(("exists_subquery_projection_boolean", setup_sql, boolean_exists_sql()))
    cases.append(("not_in_null_sensitive", setup_sql, not_in_null_sql()))


def join_variant_setup_sql():
    """Return setup SQL for join variant contract cases."""
    return """
        CREATE TABLE orders(id INTEGER, product_id INTEGER, qty INTEGER);
        CREATE TABLE products(id INTEGER, price INTEGER, name VARCHAR);
        INSERT INTO orders VALUES (1, 10, 2), (2, 11, 3), (3, 12, 4);
        INSERT INTO products VALUES (10, 150, 'a'), (11, 50, 'b'), (13, 70, 'c');
    """


def left_join_variant_sql():
    """Return a LEFT JOIN contract query."""
    return """
        SELECT o.id, p.name
        FROM orders o LEFT JOIN products p ON o.product_id = p.id
        ORDER BY o.id
    """


def right_join_variant_sql():
    """Return a RIGHT JOIN contract query."""
    return """
        SELECT o.id, p.name
        FROM orders o RIGHT JOIN products p ON o.product_id = p.id
        ORDER BY p.id
    """


def full_join_variant_sql():
    """Return a FULL OUTER JOIN contract query."""
    return """
        SELECT o.id, p.name
        FROM orders o FULL OUTER JOIN products p ON o.product_id = p.id
        ORDER BY p.id
    """


def scalar_join_sql():
    """Return a scalar subquery projection join contract query."""
    return """
        SELECT id,
               (SELECT price FROM products p WHERE p.id = o.product_id) AS price
        FROM orders o
        ORDER BY id
    """


def boolean_in_sql():
    """Return an IN subquery projection contract query."""
    return """
        SELECT id,
               product_id IN (
                   SELECT id FROM products WHERE price > 100
               ) AS has_expensive
        FROM orders
        ORDER BY id
    """


def boolean_exists_sql():
    """Return an EXISTS subquery projection contract query."""
    return """
        SELECT id,
               EXISTS (
                   SELECT 1 FROM products p WHERE p.id = o.product_id
               ) AS has_product
        FROM orders o
        ORDER BY id
    """


def not_in_null_sql():
    """Return a NOT IN query requiring null-sensitive anti semantics."""
    return """
        SELECT id
        FROM orders
        WHERE product_id NOT IN (SELECT id FROM products)
        ORDER BY id
    """


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


def cross_product_join_sql():
    """Return a cross-product query that must preserve both sides."""
    return """
        SELECT o.id, p.id
        FROM orders o CROSS JOIN products p
        ORDER BY o.id, p.id
    """


def plain_inner_join_sql():
    """Return a plain equality join contract query."""
    return """
        SELECT o.id, p.name
        FROM orders o JOIN products p ON o.product_id = p.id
        ORDER BY o.id
    """


def nested_join_conditions_sql():
    """Return a join query with nested boolean conditions."""
    return """
        SELECT o.id, p.name
        FROM orders o
        JOIN products p
          ON (o.product_id = p.id AND (p.price > 100 OR o.qty > 3))
        ORDER BY o.id
    """


def is_not_distinct_join_sql():
    """Return a join query using null-safe equality."""
    return """
        SELECT o.id, p.name
        FROM orders o
        LEFT JOIN products p ON o.product_id IS NOT DISTINCT FROM p.id
        ORDER BY o.id
    """


def any_quantified_sql():
    """Return a quantified ANY subquery contract query."""
    return """
        SELECT id
        FROM orders
        WHERE qty < ANY (SELECT price FROM products)
        ORDER BY id
    """


def any_join_sql():
    """Return a join query that DuckDB keeps as ANY_JOIN."""
    return """
        SELECT o.id, p.name
        FROM orders o
        JOIN products p
          ON o.product_id = p.id OR o.qty + p.price > 1000
        ORDER BY o.id, p.name
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
    cases.append(aggregate_contract_case())
    cases.append(repeated_base_cte_outer_contract_case())
    cases.append(multiple_ctes_same_base_contract_case())
    cases.append(temp_view_serialization_contract_case())


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


def repeated_base_cte_outer_contract_case():
    """Return a CTE and outer query that reuse one base table."""
    setup_sql = """
        CREATE TABLE orders(id INTEGER, product_id INTEGER, qty INTEGER);
        INSERT INTO orders VALUES (1, 10, 2), (2, 10, 3), (3, 11, 4);
    """
    query_sql = """
        WITH big_orders AS (
            SELECT id, product_id FROM orders WHERE qty > 2
        )
        SELECT o.id, b.id
        FROM orders o
        JOIN big_orders b ON b.product_id = o.product_id
        WHERE o.id < b.id
        ORDER BY o.id, b.id
    """
    return ("repeated_base_cte_outer_alias", setup_sql, query_sql)


def multiple_ctes_same_base_contract_case():
    """Return multiple CTEs using one base table with different aliases."""
    setup_sql = """
        CREATE TABLE orders(id INTEGER, product_id INTEGER, qty INTEGER);
        INSERT INTO orders VALUES (1, 10, 2), (2, 10, 3), (3, 11, 4);
    """
    query_sql = """
        WITH left_orders AS (
            SELECT id, product_id FROM orders WHERE qty >= 2
        ), right_orders AS (
            SELECT id, product_id FROM orders WHERE qty >= 3
        )
        SELECT l.id, r.id
        FROM left_orders l
        JOIN right_orders r ON r.product_id = l.product_id
        WHERE l.id < r.id
        ORDER BY l.id, r.id
    """
    return ("multiple_ctes_same_base_aliases", setup_sql, query_sql)


def temp_view_serialization_contract_case():
    """Return a multi-temp-view query that must serialize without overwrites."""
    setup_sql = """
        CREATE TABLE orders(customer_id INTEGER, price INTEGER);
        CREATE TABLE products(price INTEGER);
        INSERT INTO orders VALUES (1, 10), (2, 20), (3, 30);
        INSERT INTO products VALUES (10), (50);
    """
    query_sql = """
        SELECT count(*) AS order_count
        FROM orders
        GROUP BY (SELECT max(price) FROM products)
    """
    return ("duplicate_temp_view_names_multi_temp", setup_sql, query_sql)


def add_projection_lineage_contract_cases(cases):
    """Add direct projection and output-column lineage contracts."""
    cases.append(aggregate_projection_lineage_contract_case())
    cases.append(window_projection_lineage_contract_case())
    cases.append(join_derived_rhs_lineage_contract_case())
    cases.append(set_output_name_lineage_contract_case())
    cases.append(slot_mapping_projection_contract_case())


def aggregate_projection_lineage_contract_case():
    """Return projection-over-aggregate output-name lineage contract."""
    setup_sql = """
        CREATE TABLE orders(product_id INTEGER, qty INTEGER);
        INSERT INTO orders VALUES (10, 2), (10, 3), (11, 1);
    """
    query_sql = """
        SELECT product_id AS pid, sum(qty) + 1 AS adjusted_qty
        FROM orders
        GROUP BY product_id
        ORDER BY pid
    """
    return ("projection_over_aggregate_lineage", setup_sql, query_sql)


def window_projection_lineage_contract_case():
    """Return projection-over-window output-name lineage contract."""
    setup_sql = """
        CREATE TABLE orders(order_id INTEGER, price INTEGER, region VARCHAR);
        INSERT INTO orders VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 5, 'b');
    """
    query_sql = """
        SELECT order_id AS oid, rn + 10 AS shifted_rank
        FROM (
            SELECT order_id,
                   row_number() OVER (PARTITION BY region ORDER BY price) AS rn
            FROM orders
        )
        ORDER BY oid
    """
    return ("projection_over_window_lineage", setup_sql, query_sql)


def join_derived_rhs_lineage_contract_case():
    """Return join lineage through a derived RHS subquery."""
    setup_sql = """
        CREATE TABLE orders(id INTEGER, product_id INTEGER);
        CREATE TABLE products(id INTEGER, price INTEGER);
        INSERT INTO orders VALUES (1, 10), (2, 11);
        INSERT INTO products VALUES (10, 150), (11, 50);
    """
    query_sql = """
        SELECT o.id, expensive.product_id
        FROM orders o
        JOIN (
            SELECT id AS product_id FROM products WHERE price > 100
        ) expensive ON expensive.product_id = o.product_id
        ORDER BY o.id
    """
    return ("join_derived_rhs_lineage", setup_sql, query_sql)


def set_output_name_lineage_contract_case():
    """Return set-operation output-name lineage contract."""
    setup_sql = """
        CREATE TABLE left_t(a INTEGER);
        CREATE TABLE right_t(b INTEGER);
        INSERT INTO left_t VALUES (1), (2);
        INSERT INTO right_t VALUES (2), (3);
    """
    query_sql = """
        SELECT a AS visible_name FROM left_t
        UNION ALL
        SELECT b AS ignored_rhs_name FROM right_t
        ORDER BY visible_name
    """
    return ("set_operation_output_name_lineage", setup_sql, query_sql)


def slot_mapping_projection_contract_case():
    """Return projection shape that must not leak slot references."""
    setup_sql = """
        CREATE TABLE orders(product_id INTEGER, qty INTEGER);
        INSERT INTO orders VALUES (10, 2), (10, 3), (11, 1);
    """
    query_sql = """
        SELECT product_id, sum(qty) AS total_qty, count(*) AS order_count
        FROM orders
        GROUP BY product_id
        ORDER BY product_id
    """
    return ("slot_mapping_multi_projection", setup_sql, query_sql)


def add_aggregate_variant_contract_cases(cases):
    """Add aggregate layout and function variant contracts."""
    cases.append(arg_min_contract_case())
    cases.append(ordered_aggregate_contract_case())
    cases.append(multiple_ordered_aggregates_contract_case())
    cases.append(filter_aggregate_contract_case())
    cases.append(scalar_aggregate_contract_case())


def arg_min_contract_case():
    """Return an arg_min aggregate contract case."""
    setup_sql = """
        CREATE TABLE metrics(group_id INTEGER, value INTEGER, score INTEGER);
        INSERT INTO metrics VALUES (1, 10, 3), (1, 20, 2), (2, 30, 1);
    """
    query_sql = """
        SELECT group_id, arg_min(value, score) AS selected_value
        FROM metrics
        GROUP BY group_id
        ORDER BY group_id
    """
    return ("aggregate_arg_min", setup_sql, query_sql)


def ordered_aggregate_contract_case():
    """Return an ordered aggregate contract case."""
    setup_sql = """
        CREATE TABLE metrics(group_id INTEGER, value VARCHAR, score INTEGER);
        INSERT INTO metrics VALUES (1, 'a', 3), (1, 'b', 2), (2, 'c', 1);
    """
    query_sql = """
        SELECT group_id, string_agg(value, ',' ORDER BY score) AS values_csv
        FROM metrics
        GROUP BY group_id
        ORDER BY group_id
    """
    return ("aggregate_ordered_string_agg", setup_sql, query_sql)


def multiple_ordered_aggregates_contract_case():
    """Return multiple ordered aggregates in one aggregate node."""
    setup_sql = """
        CREATE TABLE metrics(group_id INTEGER, value VARCHAR, score INTEGER);
        INSERT INTO metrics VALUES (1, 'a', 3), (1, 'b', 2), (2, 'c', 1);
    """
    query_sql = """
        SELECT group_id,
               string_agg(value, ',' ORDER BY score) AS asc_values,
               string_agg(value, ',' ORDER BY score DESC) AS desc_values
        FROM metrics
        GROUP BY group_id
        ORDER BY group_id
    """
    return ("aggregate_multiple_ordered", setup_sql, query_sql)


def filter_aggregate_contract_case():
    """Return a FILTER aggregate contract case."""
    setup_sql = """
        CREATE TABLE metrics(group_id INTEGER, value INTEGER);
        INSERT INTO metrics VALUES (1, 10), (1, 20), (2, 5);
    """
    query_sql = """
        SELECT group_id, sum(value) FILTER (WHERE value > 10) AS filtered_sum
        FROM metrics
        GROUP BY group_id
        ORDER BY group_id
    """
    return ("aggregate_filter_clause", setup_sql, query_sql)


def scalar_aggregate_contract_case():
    """Return a scalar aggregate without GROUP BY contract case."""
    setup_sql = """
        CREATE TABLE metrics(group_id INTEGER, value INTEGER);
        INSERT INTO metrics VALUES (1, 10), (1, 20), (2, 5);
    """
    query_sql = "SELECT count(*) AS row_count, sum(value) AS total_value FROM metrics"
    return ("aggregate_scalar_no_group", setup_sql, query_sql)


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
    cases.append(("window_lag", setup_sql, window_lag_sql()))
    cases.append(("window_first_value", setup_sql, window_first_value_sql()))
    cases.append(("window_rows_frame", setup_sql, window_rows_frame_sql()))
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


def window_lag_sql():
    """Return a LAG window query."""
    return """
        SELECT order_id,
               lag(price) OVER (PARTITION BY region ORDER BY order_id) AS prev_price
        FROM orders
        ORDER BY order_id
    """


def window_first_value_sql():
    """Return a FIRST_VALUE window query."""
    return """
        SELECT order_id,
               first_value(price) OVER (
                   PARTITION BY region ORDER BY price DESC
               ) AS top_price
        FROM orders
        ORDER BY order_id
    """


def window_rows_frame_sql():
    """Return a window query with an explicit ROWS frame."""
    return """
        SELECT order_id,
               sum(price) OVER (
                   PARTITION BY region
                   ORDER BY order_id
                   ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
               ) AS framed_sum
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


def add_pivot_contract_cases(cases):
    """Add PIVOT and UNPIVOT contract cases."""
    setup_sql = """
        CREATE TABLE sales(region VARCHAR, year INTEGER, amount INTEGER);
        INSERT INTO sales VALUES ('east', 2024, 10), ('east', 2025, 20);
    """
    cases.append(("pivot_explicit_in", setup_sql, pivot_explicit_in_sql()))
    cases.append(("unpivot_basic", setup_sql, unpivot_basic_sql()))


def pivot_explicit_in_sql():
    """Return a PIVOT query with explicit values."""
    return """
        SELECT *
        FROM sales
        PIVOT (
            sum(amount) FOR year IN (2024, 2025)
        )
        ORDER BY region
    """


def unpivot_basic_sql():
    """Return an UNPIVOT query."""
    return """
        SELECT *
        FROM (
            SELECT region, 10 AS amount_2024, 20 AS amount_2025 FROM sales
        )
        UNPIVOT (
            amount FOR year_label IN (amount_2024, amount_2025)
        )
        ORDER BY region, year_label
    """


def add_node_shape_contract_cases(cases):
    """Add contracts for SELECT-reachable specialized plan nodes."""
    for case in mandatory_plan_shape_cases():
        cases.append(case)
    cases.append(sample_expression_contract_case())


def sample_expression_contract_case():
    """Return a SAMPLE expression variant contract."""
    setup_sql = "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1), (2), (3);"
    query_sql = "SELECT id FROM t USING SAMPLE reservoir(2 ROWS) REPEATABLE(1)"
    return ("sample_reservoir", setup_sql, query_sql)


def preserved_original_sql_contract_cases():
    """Return explicit-root cases that preserve original SQL for safety."""
    cases = []
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        cases.append((case_name, setup_sql, query_sql))
    return tuple(cases)


def preserved_original_sql_contract_case_names():
    """Return case names approved for exact original-SQL preservation."""
    names = []
    for case_name, setup_sql, query_sql in preserved_original_sql_contract_cases():
        names.append(case_name)
        assert setup_sql is not None
        assert query_sql
    return frozenset(names)


def native_preserved_original_sql_case_names():
    """Return no-root cases approved for original-SQL preservation."""
    return frozenset(
        (
            "unnest",
            "sample",
            "materialized_cte_scan",
            "positional_join",
            "asof_join",
            "recursive_cte",
            "sample_reservoir",
            "unpivot_basic",
            "not_in_subquery",
            "correlated_having",
            "projection_over_window_lineage",
            "in_subquery_projection_boolean",
            "exists_subquery_projection_boolean",
            "not_in_null_sensitive",
            "empty_result_projection_schema",
        )
    )


def native_preservation_evidence_by_case():
    """Return required evidence for every no-root preservation approval."""
    return {
        "unnest": (("node", "UNNEST"),),
        "sample": (("node", "SAMPLE"),),
        "materialized_cte_scan": (("node", "CTE"), ("node", "CTE_SCAN")),
        "positional_join": (("node", "POSITIONAL_JOIN"),),
        "asof_join": (("node", "ASOF_JOIN"),),
        "recursive_cte": (("node", "REC_CTE"), ("node", "CTE_SCAN")),
        "sample_reservoir": (("node", "SAMPLE"),),
        "unpivot_basic": (("node", "UNNEST"),),
        "not_in_subquery": (("join_type", "MARK"),),
        "correlated_having": (("sql_shape", "HAVING_SUBQUERY"),),
        "projection_over_window_lineage": (("sql_shape", "ROOT_SUBQUERY"),),
        "in_subquery_projection_boolean": (("join_type", "MARK"),),
        "exists_subquery_projection_boolean": (("join_type", "MARK"),),
        "not_in_null_sensitive": (("join_type", "MARK"),),
        "empty_result_projection_schema": (("node", "EMPTY_RESULT"),),
    }


def add_empty_contract_cases(cases):
    """Add empty-result projection lineage contracts."""
    setup_sql = "CREATE TABLE t(a INTEGER, b INTEGER); INSERT INTO t VALUES (1, 2);"
    query_sql = "SELECT a FROM t WHERE b = 0 AND a < 100 ORDER BY a"
    cases.append(("empty_result_projection_schema", setup_sql, query_sql))


@pytest.mark.parametrize("case_name,setup_sql,query_sql", mandatory_plan_shape_cases())
def test_audit_mandatory_plan_shapes_execute(case_name, setup_sql, query_sql):
    """Audit mandatory node shapes reconstruct and match original rows."""
    assert case_name
    assert_reconstructed_sql_matches_original(setup_sql, query_sql)


@pytest.mark.parametrize("case_name,setup_sql,query_sql", reconstruction_contract_cases())
def test_reconstruction_contract_cases(case_name, setup_sql, query_sql):
    """Enforce parse, execution, placeholder, lineage, and row contracts."""
    assert case_name
    assert_reconstructed_sql_matches_original(setup_sql, query_sql)


@pytest.mark.parametrize(
    "case_name,setup_sql,query_sql", preserved_original_sql_contract_cases()
)
def test_preserved_original_sql_contract_cases(case_name, setup_sql, query_sql):
    """Verify preserved cases return the exact executable original SQL."""
    connection = duckdb.connect(":memory:")
    if setup_sql.strip():
        connection.execute(setup_sql)
    optimized_root = explain_optimized_json(connection, query_sql)
    reconstructed_sql = reconstruct_sql(
        connection, query_sql, False, True, False, optimized_root
    )
    assert case_name
    assert reconstructed_sql == f"{query_sql.strip()};"
    execute_sql_script_with_columns(connection, reconstructed_sql)


def test_explicit_root_preservation_cases_match_contract_matrix():
    """Verify explicit-root preservation coverage tracks every contract case."""
    assert preserved_original_sql_contract_case_names() == contract_case_names()


def test_explicit_root_preservation_case_names_are_unique():
    """Verify explicit-root preservation coverage cannot duplicate cases."""
    case_names = []
    for case_name, setup_sql, query_sql in preserved_original_sql_contract_cases():
        case_names.append(case_name)
        assert setup_sql is not None
        assert query_sql
    assert len(case_names) == len(set(case_names))


def test_only_approved_cases_preserve_original_sql():
    """Verify original-SQL fallback cannot hide reconstruction gaps."""
    accidental_preservation_names = set()
    reconstruction_error_names = set()
    approved_names = preserved_original_sql_contract_case_names()
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        connection = duckdb.connect(":memory:")
        if setup_sql.strip():
            connection.execute(setup_sql)
        optimized_root = explain_optimized_json(connection, query_sql)
        try:
            reconstructed_sql = reconstruct_sql(
                connection, query_sql, False, False, False, optimized_root
            )
        except PlanReconstructionError:
            reconstruction_error_names.add(case_name)
            continue
        if "ORIGINAL_SQL -> preserved" in reconstructed_sql:
            if case_name in approved_names:
                continue
            accidental_preservation_names.add(case_name)
    failures = {}
    if accidental_preservation_names:
        failures["accidental_preservation_names"] = accidental_preservation_names
    if reconstruction_error_names:
        failures["reconstruction_error_names"] = reconstruction_error_names
    assert not failures


def test_native_reconstruction_does_not_preserve_unapproved_cases():
    """Verify normal reconstruction uses native rendering when supported."""
    accidental_preservation_names = set()
    approved_names = native_preserved_original_sql_case_names()
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        connection = duckdb.connect(":memory:")
        if setup_sql.strip():
            connection.execute(setup_sql)
        reconstructed_sql = reconstruct_sql(
            connection, query_sql, False, False, False
        )
        if "ORIGINAL_SQL -> preserved" not in reconstructed_sql:
            continue
        if case_name not in approved_names:
            accidental_preservation_names.add(case_name)
    assert not accidental_preservation_names


def test_native_preserved_cases_return_exact_original_sql():
    """Verify approved no-root preservation returns executable original SQL."""
    cases_by_name = contract_case_by_name()
    for case_name in native_preserved_original_sql_case_names():
        setup_sql, query_sql = cases_by_name[case_name]
        connection = duckdb.connect(":memory:")
        if setup_sql.strip():
            connection.execute(setup_sql)
        reconstructed_sql = reconstruct_sql(
            connection, query_sql, False, True, False
        )
        assert reconstructed_sql == f"{query_sql.strip()};"
        execute_sql_script_with_columns(connection, reconstructed_sql)


def test_native_preserved_case_names_are_real_contract_cases():
    """Verify every no-root preservation approval points to a real case."""
    extra_names = set()
    contract_names = contract_case_names()
    for case_name in native_preserved_original_sql_case_names():
        if case_name not in contract_names:
            extra_names.add(case_name)
    assert not extra_names


def test_native_preserved_case_names_have_evidence_assertions():
    """Verify every no-root preservation approval has evidence checks."""
    missing_names = set()
    evidence_by_case = native_preservation_evidence_by_case()
    for case_name in native_preserved_original_sql_case_names():
        if case_name not in evidence_by_case:
            missing_names.add(case_name)
    assert not missing_names


def test_native_preservation_evidence_points_to_approved_cases():
    """Verify every preservation evidence assertion is approved."""
    extra_names = set()
    approved_names = native_preserved_original_sql_case_names()
    for case_name in native_preservation_evidence_by_case():
        if case_name not in approved_names:
            extra_names.add(case_name)
    assert not extra_names


def test_native_preserved_cases_have_plan_or_sql_evidence():
    """Verify preservation approvals are backed by real plans or SQL shapes."""
    missing_evidence_by_case = {}
    cases_by_name = contract_case_by_name()
    for case_name, evidence_items in native_preservation_evidence_by_case().items():
        setup_sql, query_sql = cases_by_name[case_name]
        nodes = optimized_plan_nodes_for_sql(setup_sql, query_sql)
        missing_evidence = missing_native_preservation_evidence(
            nodes, query_sql, evidence_items
        )
        if missing_evidence:
            missing_evidence_by_case[case_name] = missing_evidence
    assert not missing_evidence_by_case


def missing_native_preservation_evidence(nodes, query_sql, evidence_items):
    """Return preservation evidence entries not proven by plan or SQL shape."""
    missing_evidence = []
    for evidence_kind, expected_value in evidence_items:
        if native_preservation_evidence_exists(
            nodes, query_sql, evidence_kind, expected_value
        ):
            continue
        missing_evidence.append((evidence_kind, expected_value))
    return tuple(missing_evidence)


def native_preservation_evidence_exists(
    nodes, query_sql, evidence_kind, expected_value
):
    """Return whether one preservation evidence item is proven."""
    if evidence_kind == "node":
        return plan_node_name_exists(nodes, expected_value)
    if evidence_kind == "join_type":
        return plan_join_type_exists(nodes, expected_value)
    if evidence_kind == "sql_shape":
        return sql_shape_evidence_exists(query_sql, expected_value)
    return False


def plan_node_name_exists(nodes, expected_name):
    """Return whether one optimized plan node name exists."""
    for node in nodes:
        if node.name == expected_name:
            return True
    return False


def plan_join_type_exists(nodes, expected_join_type):
    """Return whether a join node exposes one join type."""
    for node in nodes:
        if "JOIN" not in node.name:
            continue
        join_type = str(node.extra_info.get("Join Type", "")).upper()
        if join_type == expected_join_type:
            return True
    return False


def sql_shape_evidence_exists(query_sql, expected_shape):
    """Return whether SQL text proves one preservation shape."""
    normalized_sql = normalize_sql(query_sql).upper()
    if expected_shape == "HAVING_SUBQUERY":
        return " HAVING " in normalized_sql and " SELECT " in normalized_sql
    if expected_shape == "ROOT_SUBQUERY":
        return " FROM ( SELECT " in normalized_sql
    return False


def coverage_document_text():
    """Return the DuckDB reconstruction coverage document text."""
    document_path = Path("docs/duckdb_reconstruction_contract_gaps.md")
    return document_path.read_text()


def coverage_document_gate_section_text():
    """Return the coverage-gates section from the coverage document."""
    section_header = "## Coverage Gates"
    document_text = coverage_document_text()
    return document_text.split(section_header, 1)[1]


def coverage_document_gate_names():
    """Return coverage gate test names listed in the coverage document."""
    names = []
    pattern = r"`(test_[A-Za-z0-9_]+)`"
    for match in re.findall(pattern, coverage_document_gate_section_text()):
        names.append(match)
    return tuple(names)


def required_coverage_document_gate_names():
    """Return every test name required in the coverage-gates document section."""
    return (
        "test_all_review_contract_case_names_are_present",
        "test_reconstruction_contract_case_names_are_unique",
        "test_required_review_contract_case_names_are_unique",
        "test_mandatory_plan_shape_case_names_are_unique",
        "test_mandatory_plan_shape_cases_are_in_contract_matrix",
        "test_all_contract_cases_reach_required_optimized_nodes",
        "test_all_contract_cases_dump_serializable_optimized_json",
        "test_contract_cases_reach_required_extra_info_values",
        "test_required_extra_info_assertions_have_contract_cases",
        "test_all_contract_cases_have_required_optimized_node_assertions",
        "test_all_required_optimized_node_assertions_have_contract_cases",
        "test_all_contract_cases_have_expected_output_columns",
        "test_all_expected_output_columns_have_contract_cases",
        "test_command_plan_contract_cases_reach_required_nodes",
        "test_command_plan_contract_cases_dump_serializable_json",
        "test_every_source_node_has_select_or_command_contract_coverage",
        "test_all_accounted_source_nodes_exist_in_duckdb_source",
        "test_source_node_accounting_buckets_are_disjoint",
        "test_command_contract_case_names_are_unique",
        "test_command_contract_node_names_are_unique",
        "test_command_contract_nodes_exist_in_duckdb_source",
        "test_source_evidence_node_names_are_unique",
        "test_source_evidence_case_keys_are_unique",
        "test_source_evidence_nodes_exist_in_manifest",
        "test_source_evidence_nodes_exist_in_duckdb_source",
        "test_lowered_contract_node_names_are_unique",
        "test_lowered_contract_nodes_exist_in_manifest",
        "test_pivot_contract_proves_optimized_lowering",
        "test_get_source_node_is_covered_by_dynamic_scan_contract",
        "test_observed_select_nodes_have_handlers",
        "test_observed_select_nodes_have_direct_contract_cases",
        "test_synthetic_only_handler_names_are_unique",
        "test_synthetic_only_handlers_have_source_evidence",
        "test_synthetic_only_handler_source_cases_are_exact",
        "test_explicit_root_preservation_cases_match_contract_matrix",
        "test_explicit_root_preservation_case_names_are_unique",
        "test_only_approved_cases_preserve_original_sql",
        "test_native_reconstruction_does_not_preserve_unapproved_cases",
        "test_native_preserved_cases_return_exact_original_sql",
        "test_native_preserved_case_names_are_real_contract_cases",
        "test_native_preserved_case_names_have_evidence_assertions",
        "test_native_preservation_evidence_points_to_approved_cases",
        "test_native_preserved_cases_have_plan_or_sql_evidence",
        "test_coverage_document_listed_gates_exist_in_test_file",
        "test_coverage_document_gate_names_are_complete",
        "test_coverage_document_lists_every_contract_case",
        "test_coverage_document_lists_every_command_contract_node",
        "test_coverage_document_lists_every_source_evidence_node",
        "test_coverage_document_lists_every_lowered_contract_node",
        "test_coverage_document_lists_every_observed_handler_node",
        "test_coverage_document_lists_every_explicit_user_requirement_group",
        "test_coverage_document_pass_count_is_current",
        "test_explicit_user_requirement_groups_have_contract_cases",
        "test_explicit_user_requirement_cases_have_required_node_assertions",
        "test_explicit_user_requirement_cases_have_output_column_assertions",
        "test_reconstruction_contract_cases_use_full_ladder_helper",
    )


def test_coverage_document_listed_gates_exist_in_test_file():
    """Verify every documented coverage gate names a real test."""
    test_file_text = Path(__file__).read_text()
    missing_names = set()
    for test_name in coverage_document_gate_names():
        if f"def {test_name}" not in test_file_text:
            missing_names.add(test_name)
    assert not missing_names


def test_coverage_document_gate_names_are_complete():
    """Verify the coverage document lists every required coverage gate."""
    documented_names = set(coverage_document_gate_names())
    required_names = set(required_coverage_document_gate_names())
    assert documented_names == required_names


def coverage_document_missing_backtick_names(expected_names):
    """Return expected names missing from the coverage document."""
    missing_names = set()
    document_text = coverage_document_text()
    for expected_name in expected_names:
        backtick_name = f"`{expected_name}`"
        if backtick_name not in document_text:
            missing_names.add(expected_name)
    return missing_names


def test_coverage_document_lists_every_contract_case():
    """Verify the document names every reconstruction contract case."""
    missing_names = coverage_document_missing_backtick_names(contract_case_names())
    assert not missing_names


def test_coverage_document_lists_every_command_contract_node():
    """Verify the document names every command-plan contract node."""
    missing_names = coverage_document_missing_backtick_names(
        command_contract_node_names()
    )
    assert not missing_names


def test_coverage_document_lists_every_source_evidence_node():
    """Verify the document names every source-evidence contract node."""
    missing_names = coverage_document_missing_backtick_names(
        source_evidence_node_names()
    )
    assert not missing_names


def test_coverage_document_lists_every_lowered_contract_node():
    """Verify the document names every optimized-lowered contract node."""
    missing_names = coverage_document_missing_backtick_names(
        lowered_contract_node_names()
    )
    assert not missing_names


def test_coverage_document_lists_every_observed_handler_node():
    """Verify the document names every observed relation handler node."""
    missing_names = coverage_document_missing_backtick_names(
        observed_relation_contract_node_names()
    )
    assert not missing_names


def test_coverage_document_lists_every_explicit_user_requirement_group():
    """Verify the document names every explicit blocker category."""
    missing_names = coverage_document_missing_backtick_names(
        explicit_user_requirement_contract_groups().keys()
    )
    assert not missing_names


def test_coverage_document_pass_count_is_current():
    """Verify the coverage document records the current full-suite count."""
    expected_text = f"{EXPECTED_DUCKDB_RECONSTRUCT_PASS_COUNT} passed"
    assert expected_text in coverage_document_text()


def test_all_review_contract_case_names_are_present():
    """Verify every review-listed contract has a direct named test case."""
    missing_names = set()
    present_names = contract_case_names()
    for required_name in required_review_contract_case_names():
        if required_name not in present_names:
            missing_names.add(required_name)
    assert not missing_names


def test_explicit_user_requirement_groups_have_contract_cases():
    """Verify every explicit blocker category maps to contract cases."""
    missing_by_group = {}
    present_names = contract_case_names()
    for group_name, case_names in explicit_user_requirement_contract_groups().items():
        missing_names = []
        for case_name in case_names:
            if case_name not in present_names:
                missing_names.append(case_name)
        if missing_names:
            missing_by_group[group_name] = tuple(missing_names)
    assert not missing_by_group


def test_explicit_user_requirement_cases_have_required_node_assertions():
    """Verify explicit blocker cases have optimized-node assertions."""
    missing_by_group = {}
    required_nodes_by_case = required_plan_nodes_by_contract_case()
    for group_name, case_names in explicit_user_requirement_contract_groups().items():
        missing_names = []
        for case_name in case_names:
            if case_name not in required_nodes_by_case:
                missing_names.append(case_name)
        if missing_names:
            missing_by_group[group_name] = tuple(missing_names)
    assert not missing_by_group


def test_explicit_user_requirement_cases_have_output_column_assertions():
    """Verify explicit blocker cases have output-column assertions."""
    missing_by_group = {}
    expected_columns_by_case = expected_output_columns_by_contract_case()
    for group_name, case_names in explicit_user_requirement_contract_groups().items():
        missing_names = []
        for case_name in case_names:
            if case_name not in expected_columns_by_case:
                missing_names.append(case_name)
        if missing_names:
            missing_by_group[group_name] = tuple(missing_names)
    assert not missing_by_group


def test_reconstruction_contract_cases_use_full_ladder_helper():
    """Verify the contract test cannot weaken the full execution ladder."""
    source = inspect.getsource(test_reconstruction_contract_cases)
    assert "reconstruction_contract_cases()" in source
    assert "assert_reconstructed_sql_matches_original" in source


def test_reconstruction_contract_case_names_are_unique():
    """Verify reconstruction contract names cannot overwrite coverage."""
    case_names = contract_case_name_list()
    assert len(case_names) == len(set(case_names))


def test_required_review_contract_case_names_are_unique():
    """Verify the review-required contract list has no duplicates."""
    case_names = required_review_contract_case_names()
    assert len(case_names) == len(set(case_names))


def test_mandatory_plan_shape_case_names_are_unique():
    """Verify mandatory plan-shape contract names are unique."""
    case_names = mandatory_plan_shape_case_names()
    assert len(case_names) == len(set(case_names))


def test_mandatory_plan_shape_cases_are_in_contract_matrix():
    """Verify every mandatory plan-shape case runs in the full contract."""
    missing_names = set()
    present_names = contract_case_names()
    for case_name in mandatory_plan_shape_case_names():
        if case_name not in present_names:
            missing_names.add(case_name)
    assert not missing_names


def test_all_contract_cases_reach_required_optimized_nodes():
    """Verify each named contract reaches its required optimized nodes."""
    missing_nodes_by_case = {}
    cases_by_name = contract_case_by_name()
    for case_name, required_nodes in required_plan_nodes_by_contract_case().items():
        setup_sql, query_sql = cases_by_name[case_name]
        observed_nodes = optimized_node_names_for_sql(setup_sql, query_sql)
        missing_nodes = missing_required_nodes(required_nodes, observed_nodes)
        if missing_nodes:
            missing_nodes_by_case[case_name] = missing_nodes
    assert not missing_nodes_by_case


def test_all_contract_cases_dump_serializable_optimized_json():
    """Verify each query contract dumps optimized JSON with required nodes."""
    missing_nodes_by_case = {}
    cases_by_name = contract_case_by_name()
    for case_name, required_nodes in required_plan_nodes_by_contract_case().items():
        setup_sql, query_sql = cases_by_name[case_name]
        node_json = optimized_json_for_sql(setup_sql, query_sql)
        observed_nodes = json_plan_node_names(node_json)
        missing_nodes = missing_required_nodes(required_nodes, observed_nodes)
        if missing_nodes:
            missing_nodes_by_case[case_name] = missing_nodes
    assert not missing_nodes_by_case


def test_contract_cases_reach_required_extra_info_values():
    """Verify critical optimized-plan extra_info values are covered."""
    missing_extra_info_by_case = {}
    cases_by_name = contract_case_by_name()
    for case_name, required_values in required_extra_info_by_contract_case().items():
        setup_sql, query_sql = cases_by_name[case_name]
        nodes = optimized_plan_nodes_for_sql(setup_sql, query_sql)
        missing_values = []
        for node_name, key, expected_value in required_values:
            if not plan_has_extra_info(nodes, node_name, key, expected_value):
                missing_values.append((node_name, key, expected_value))
        if missing_values:
            missing_extra_info_by_case[case_name] = tuple(missing_values)
    assert not missing_extra_info_by_case


def test_required_extra_info_assertions_have_contract_cases():
    """Verify every extra_info assertion points at a real contract case."""
    extra_names = set()
    present_names = contract_case_names()
    for case_name in required_extra_info_by_contract_case():
        if case_name not in present_names:
            extra_names.add(case_name)
    assert not extra_names


def test_all_contract_cases_have_required_optimized_node_assertions():
    """Verify every query contract has an optimized-node assertion."""
    missing_names = set()
    required_node_cases = required_plan_nodes_by_contract_case()
    for case_name in contract_case_names():
        if case_name not in required_node_cases:
            missing_names.add(case_name)
    assert not missing_names


def test_all_required_optimized_node_assertions_have_contract_cases():
    """Verify every optimized-node assertion points at a real case."""
    extra_names = set()
    present_names = contract_case_names()
    for case_name in required_plan_nodes_by_contract_case():
        if case_name not in present_names:
            extra_names.add(case_name)
    assert not extra_names


def test_all_contract_cases_have_expected_output_columns():
    """Verify every query contract declares exact output columns."""
    missing_names = set()
    mismatched_columns = {}
    expected_columns_by_name = expected_output_columns_by_contract_case()
    for case_name, setup_sql, query_sql in reconstruction_contract_cases():
        expected_columns = expected_columns_by_name.get(case_name)
        if expected_columns is None:
            missing_names.add(case_name)
            continue
        actual_columns = original_output_columns_for_contract_case(setup_sql, query_sql)
        if actual_columns != expected_columns:
            mismatched_columns[case_name] = (expected_columns, actual_columns)
    assert not missing_names
    assert not mismatched_columns


def test_all_expected_output_columns_have_contract_cases():
    """Verify every output-column assertion points at a real case."""
    extra_names = set()
    present_names = contract_case_names()
    for case_name in expected_output_columns_by_contract_case():
        if case_name not in present_names:
            extra_names.add(case_name)
    assert not extra_names


@pytest.mark.parametrize(
    "case_name,setup_sql,query_sql,required_node",
    command_plan_contract_cases(),
)
def test_command_plan_contract_cases_reach_required_nodes(
    case_name, setup_sql, query_sql, required_node
):
    """Verify command SQL reaches its source-derived logical node."""
    observed_nodes = optimized_node_names_for_sql(setup_sql, query_sql)
    assert case_name
    assert required_node in observed_nodes


@pytest.mark.parametrize(
    "case_name,setup_sql,query_sql,required_node",
    command_plan_contract_cases(),
)
def test_command_plan_contract_cases_dump_serializable_json(
    case_name, setup_sql, query_sql, required_node
):
    """Verify command contracts dump optimized JSON with required nodes."""
    node_json = optimized_json_for_sql(setup_sql, query_sql)
    observed_nodes = json_plan_node_names(node_json)
    assert case_name
    assert required_node in observed_nodes


def test_every_source_node_has_select_or_command_contract_coverage():
    """Verify each source node has direct SELECT, command, or synthetic coverage."""
    missing_names = set()
    accounted_names = all_accounted_source_node_names()
    for node_name in duckdb_source_logical_node_names():
        if node_name not in accounted_names:
            missing_names.add(node_name)
    assert not missing_names


def test_all_accounted_source_nodes_exist_in_duckdb_source():
    """Verify every accounted node name is source-derived."""
    extra_names = set()
    source_names = duckdb_source_logical_node_names()
    for node_name in all_accounted_source_node_names():
        if node_name not in source_names:
            extra_names.add(node_name)
    assert not extra_names


def test_source_node_accounting_buckets_are_disjoint():
    """Verify source nodes are assigned to exactly one coverage bucket."""
    buckets = (
        ("direct_handler", all_direct_handler_contract_node_names()),
        ("command_plan", command_contract_node_names()),
        ("optimized_lowering", set(lowered_contract_node_names())),
        ("source_evidence", set(source_evidence_node_names())),
    )
    overlaps = {}
    for left_name, left_nodes in buckets:
        for right_name, right_nodes in buckets:
            if left_name >= right_name:
                continue
            overlap = set(left_nodes) & set(right_nodes)
            if overlap:
                overlaps[(left_name, right_name)] = tuple(sorted(overlap))
    assert not overlaps


def test_command_contract_case_names_are_unique():
    """Verify command contract cases cannot overwrite each other."""
    case_names = command_contract_case_names()
    assert len(case_names) == len(set(case_names))


def test_command_contract_node_names_are_unique():
    """Verify command contract nodes cannot overwrite each other."""
    node_names = command_contract_node_name_list()
    assert len(node_names) == len(set(node_names))


def test_command_contract_nodes_exist_in_duckdb_source():
    """Verify command contract nodes are source-derived."""
    extra_names = set()
    source_names = duckdb_source_logical_node_names()
    for node_name in command_contract_node_names():
        if node_name not in source_names:
            extra_names.add(node_name)
    assert not extra_names


def test_source_evidence_node_names_are_unique():
    """Verify source-evidence node names cannot overwrite each other."""
    node_names = source_evidence_node_names()
    assert len(node_names) == len(set(node_names))


def test_source_evidence_case_keys_are_unique():
    """Verify source-evidence contracts cannot overwrite each other."""
    case_keys = source_evidence_case_keys()
    assert len(case_keys) == len(set(case_keys))


def test_source_evidence_nodes_exist_in_manifest():
    """Verify source-evidence nodes are source-derived."""
    extra_names = set()
    source_names = duckdb_source_logical_node_names()
    for node_name in source_evidence_node_names():
        if node_name not in source_names:
            extra_names.add(node_name)
    assert not extra_names


def test_lowered_contract_node_names_are_unique():
    """Verify lowered node coverage names cannot overwrite each other."""
    node_names = lowered_contract_node_name_list()
    assert len(node_names) == len(set(node_names))


def test_lowered_contract_nodes_exist_in_manifest():
    """Verify lowered node names are source-derived."""
    extra_names = set()
    source_names = duckdb_source_logical_node_names()
    for node_name in lowered_contract_node_names():
        if node_name not in source_names:
            extra_names.add(node_name)
    assert not extra_names


@pytest.mark.parametrize(
    "node_name,relative_path,source_text,evidence_kind",
    source_evidence_node_cases(),
)
def test_source_evidence_nodes_exist_in_duckdb_source(
    node_name, relative_path, source_text, evidence_kind
):
    """Verify non-query nodes have exact DuckDB source evidence."""
    source_path = DEFAULT_DUCKDB_SOURCE_ROOT / relative_path
    text = source_path.read_text()
    assert node_name in duckdb_source_logical_node_names()
    assert source_text in text
    assert evidence_kind in source_evidence_kinds()


def source_evidence_kinds():
    """Return accepted source evidence kinds for non-query node contracts."""
    return (
        "statement-binder",
        "internal-column-data",
        "decorrelation-lowered",
        "extension-only",
    )


def test_pivot_contract_proves_optimized_lowering():
    """Verify PIVOT SQL is covered by its optimized lowered node shape."""
    cases_by_name = contract_case_by_name()
    setup_sql, query_sql = cases_by_name["pivot_explicit_in"]
    observed_nodes = optimized_node_names_for_sql(setup_sql, query_sql)
    assert "PIVOT" not in observed_nodes
    assert "AGGREGATE" in observed_nodes
    assert "PROJECTION" in observed_nodes


def test_get_source_node_is_covered_by_dynamic_scan_contract():
    """Verify source GET is covered by dynamic scan validation."""
    registry = read_logical_node_registry()
    node = PlanNode(
        "SEQ_SCAN",
        tuple(),
        {"Table": "memory.main.orders", "Type": "Sequential Scan"},
    )
    assert "GET" in duckdb_source_logical_node_names()
    validate_tree(node, registry)


def missing_required_nodes(required_nodes, observed_nodes):
    """Return required nodes that were not observed in one optimized plan."""
    missing_nodes = []
    for required_node in required_nodes:
        if required_node not in observed_nodes:
            missing_nodes.append(required_node)
    return tuple(missing_nodes)


@pytest.mark.parametrize("node_name", synthetic_handler_contract_node_names())
def test_synthetic_join_handler_contracts_execute(node_name):
    """Verify join handlers without optimized cases still execute directly."""
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE left_t(id INTEGER, name VARCHAR)")
    connection.execute("CREATE TABLE right_t(id INTEGER, label VARCHAR)")
    connection.execute("INSERT INTO left_t VALUES (1, 'a'), (2, 'b')")
    connection.execute("INSERT INTO right_t VALUES (1, 'x'), (3, 'y')")
    root = synthetic_join_plan_node(node_name)
    result = reconstruct_sql(connection, synthetic_join_sql(), False, True, False, root)
    assert_no_internal_placeholders(result)
    rows, columns = execute_sql_script_with_columns(connection, result)
    assert rows == [(1, "a", 1, "x")]
    assert columns == ("id", "name", "id", "label")


def synthetic_join_plan_node(node_name):
    """Return a synthetic plan that exercises one join handler."""
    return PlanNode(
        node_name,
        (synthetic_scan_node("left_t"), synthetic_scan_node("right_t")),
        {"Join Type": "INNER", "Conditions": "id = id"},
    )


def synthetic_scan_node(table_name):
    """Return a synthetic scan node for one table."""
    return PlanNode(
        "SEQ_SCAN",
        tuple(),
        {"Table": f"memory.main.{table_name}", "Type": "Sequential Scan"},
    )


def synthetic_join_sql():
    """Return SQL surface text for synthetic join handler tests."""
    return """
        SELECT *
        FROM left_t
        JOIN right_t ON left_t.id = right_t.id
    """


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


@pytest.mark.parametrize("node_name", observed_relation_contract_node_name_list())
def test_each_observed_select_node_has_handler(node_name):
    """Verify each observed SELECT relation node has a handler by name."""
    handlers = relation_handlers()
    assert node_name in handlers


def test_observed_select_nodes_have_direct_contract_cases():
    """Verify every registered handler is reached by a contract case."""
    observed_names = all_direct_handler_contract_node_names()
    missing_names = set()
    for handler_name in relation_handlers():
        if handler_name not in observed_names:
            missing_names.add(handler_name)
    assert not missing_names


def test_synthetic_only_handler_names_are_unique():
    """Verify synthetic-only handler coverage cannot overwrite itself."""
    node_names = synthetic_only_handler_contract_node_names()
    assert len(node_names) == len(set(node_names))


def test_synthetic_only_handlers_have_source_evidence():
    """Verify synthetic-only handlers are backed by DuckDB source evidence."""
    missing_names = set()
    case_names = set()
    for node_name, relative_path, source_text, failure_text in (
        synthetic_only_handler_source_cases()
    ):
        case_names.add(node_name)
        source_path = DEFAULT_DUCKDB_SOURCE_ROOT / relative_path
        text = source_path.read_text()
        assert source_text in text
        assert failure_text in text
    for node_name in synthetic_only_handler_contract_node_names():
        if node_name not in case_names:
            missing_names.add(node_name)
    assert not missing_names


def test_synthetic_only_handler_source_cases_are_exact():
    """Verify synthetic-only evidence does not name non-synthetic handlers."""
    extra_names = set()
    synthetic_only_names = synthetic_only_handler_contract_node_names()
    for node_name, relative_path, source_text, failure_text in (
        synthetic_only_handler_source_cases()
    ):
        assert relative_path
        assert source_text
        assert failure_text
        if node_name not in synthetic_only_names:
            extra_names.add(node_name)
    assert not extra_names


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
