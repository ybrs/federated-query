"""Three-table join pushdown and combined scenarios.

When every table lives on the same data source, the whole join (including
predicates, limit, and ordering) is pushed to that source as one remote query.
"""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    explain_document,
    from_table_name,
    join_table_names,
)


def _single_remote_query(document):
    """Assert the plan produced exactly one remote query and return its AST."""
    queries = document.get("queries", [])
    assert len(queries) == 1
    return queries[0]["query"]


def test_three_table_inner_chain(single_source_env):
    """Ensures a same-source three-table inner chain pushes as one query."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name, C.segment "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.customers C ON O.customer_id = C.customer_id"
    )
    ast = explain_datasource_query(runtime, sql)
    assert from_table_name(ast) == "orders"
    assert len(ast.args.get("joins") or []) == 2
    join_tables = join_table_names(ast)
    assert "products" in join_tables
    assert "customers" in join_tables


def test_mixed_inner_left_chain(single_source_env):
    """Validates mixed inner/left chains push together as one query."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name, C.segment "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "LEFT JOIN duckdb_primary.main.customers C ON O.customer_id = C.customer_id"
    )
    ast = explain_datasource_query(runtime, sql)
    assert from_table_name(ast) == "orders"
    assert len(ast.args.get("joins") or []) == 2


def test_three_table_with_predicates(single_source_env):
    """Confirms predicates on the joined tables push into the remote WHERE."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.customers C ON O.customer_id = C.customer_id "
        "WHERE P.category = 'clothing' AND C.segment = 'enterprise'"
    )
    ast = explain_datasource_query(runtime, sql)
    assert len(ast.args.get("joins") or []) == 2
    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_three_table_with_limit(single_source_env):
    """Ensures LIMIT pushes into the single remote query for three-table joins."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.customers C ON O.customer_id = C.customer_id "
        "LIMIT 5"
    )
    ast = explain_datasource_query(runtime, sql)
    assert len(ast.args.get("joins") or []) == 2
    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    assert int(limit_clause.expression.this) == 5
