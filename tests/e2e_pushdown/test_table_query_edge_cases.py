"""Table and query structure edge cases pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    select_column_names,
    unwrap_parens,
)


def test_query_on_empty_table_behavior(single_source_env):
    """A constant-false predicate is pushed to the source unevaluated."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders " "WHERE 1 = 0"
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    # fedq does not constant-fold: the source owns the data and evaluates
    # ``1 = 0`` itself, so the remote SQL carries the equality verbatim.
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)
    left = unwrap_parens(predicate.left)
    right = unwrap_parens(predicate.right)
    assert isinstance(left, exp.Literal) and int(left.this) == 1
    assert isinstance(right, exp.Literal) and int(right.this) == 0


def test_constant_false_predicate_returns_no_rows(single_source_env):
    """A ``WHERE 1 = 0`` query executes and returns an empty result.

    The source evaluates the constant-false predicate; whether or not fedq ever
    folds it, the end result must be a zero-row table. This guards that contract
    independently of the pushed predicate's shape.
    """
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders " "WHERE 1 = 0"
    table = runtime.execute(sql)
    assert table.num_rows == 0


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
    """A quoted space-alias pushes the internal column and renames locally.

    The remote query fetches the internal column name; the visible "order id"
    name is applied locally, so the pushed projection is the bare column.
    """
    runtime = build_runtime(single_source_env)
    sql = 'SELECT order_id AS "order id" FROM duckdb_primary.main.orders'
    ast = explain_datasource_query(runtime, sql)

    assert select_column_names(ast) == ["order_id"]
    result = runtime.execute(sql)
    assert result.schema.names == ["order id"]


def test_quoted_identifiers_case_sensitive(single_source_env):
    """Validates quoted identifiers preserve case sensitivity."""
    runtime = build_runtime(single_source_env)
    sql = 'SELECT "Order_ID" FROM duckdb_primary.main.orders'

    ast = explain_datasource_query(runtime, sql)
    expressions = ast.expressions
    assert len(expressions) == 1


def test_very_wide_select(single_source_env):
    """A wide projection of one repeated column dedups remotely, 20 cols locally.

    The remote query fetches ``order_id`` once; the 20 aliases are produced by
    the local projection, so the final result has all 20 columns.
    """
    runtime = build_runtime(single_source_env)
    columns = ", ".join([f"order_id AS col{i}" for i in range(20)])
    sql = f"SELECT {columns} FROM duckdb_primary.main.orders"

    result = runtime.execute(sql)
    assert len(result.schema.names) == 20


def test_select_duplicate_column_names(single_source_env):
    """Two columns aliased to one name push as their distinct internal columns.

    The shared visible name ``id`` is applied locally, so the remote query
    selects the two underlying columns and the result has two ``id`` columns.
    """
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id AS id, product_id AS id " "FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    assert select_column_names(ast) == ["order_id", "product_id"]

    result = runtime.execute(sql)
    assert result.schema.names == ["id", "id"]


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

    # The aggregate is pushed with its generated alias (COUNT(region) AS ...),
    # so unwrap the alias before checking the underlying call.
    first_expr = unwrap_parens(expressions[0])
    if isinstance(first_expr, exp.Alias):
        first_expr = first_expr.this
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
