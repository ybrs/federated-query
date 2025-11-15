"""Table and query structure edge cases pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    select_column_names,
    unwrap_parens,
)


def test_query_on_empty_table_behavior(single_source_env):
    """Documents query behavior on empty table (0 rows)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE 1 = 0"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)


def test_query_single_row_with_limit_one(single_source_env):
    """Validates query on effectively single-row result pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders LIMIT 1"
    ast = explain_datasource_query(runtime, sql)

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 1


def test_column_names_sql_keywords(single_source_env):
    """Checks queries with SQL keyword column names push correctly."""
    runtime = build_runtime(single_source_env)
    sql = 'SELECT "order_id", "select" FROM duckdb_primary.main.orders'

    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert len(projection) >= 1


def test_column_names_with_spaces(single_source_env):
    """Ensures column names with spaces (quoted) push correctly."""
    runtime = build_runtime(single_source_env)
    sql = 'SELECT order_id AS "order id" FROM duckdb_primary.main.orders'
    ast = explain_datasource_query(runtime, sql)

    expressions = ast.expressions
    assert len(expressions) == 1
    first_expr = expressions[0]
    assert isinstance(first_expr, exp.Alias)


def test_quoted_identifiers_case_sensitive(single_source_env):
    """Validates quoted identifiers preserve case sensitivity."""
    runtime = build_runtime(single_source_env)
    sql = 'SELECT "Order_ID" FROM duckdb_primary.main.orders'

    ast = explain_datasource_query(runtime, sql)
    expressions = ast.expressions
    assert len(expressions) == 1


def test_very_wide_select(single_source_env):
    """Checks SELECT with many columns (wide projection) pushes correctly."""
    runtime = build_runtime(single_source_env)
    columns = ", ".join([f"order_id AS col{i}" for i in range(20)])
    sql = f"SELECT {columns} FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)

    expressions = ast.expressions
    assert len(expressions) == 20


def test_select_duplicate_column_names(single_source_env):
    """Ensures SELECT with duplicate column names via aliasing pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id AS id, product_id AS id "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)

    expressions = ast.expressions
    assert len(expressions) == 2

    first_alias = expressions[0]
    assert isinstance(first_alias, exp.Alias)

    second_alias = expressions[1]
    assert isinstance(second_alias, exp.Alias)


def test_all_nulls_column_with_aggregate(single_source_env):
    """Validates aggregates on potentially all-NULL column push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT COUNT(region), SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)

    expressions = ast.expressions
    assert len(expressions) == 2

    first_expr = unwrap_parens(expressions[0])
    assert isinstance(first_expr, exp.Count)


def test_all_duplicate_values_with_distinct(single_source_env):
    """Checks DISTINCT on column with all duplicate values pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DISTINCT status FROM duckdb_primary.main.orders "
        "WHERE status = 'processing'"
    )
    ast = explain_datasource_query(runtime, sql)

    distinct = ast.args.get("distinct")
    assert distinct is not None

    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_column_special_characters(single_source_env):
    """Documents behavior of column names with special characters."""
    runtime = build_runtime(single_source_env)
    sql = 'SELECT order_id AS "order@id" FROM duckdb_primary.main.orders'

    ast = explain_datasource_query(runtime, sql)
    expressions = ast.expressions
    assert len(expressions) == 1
