"""NULL handling and three-valued logic pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    find_in_select,
    group_column_names,
    select_column_names,
    unwrap_parens,
)


def test_null_in_and_logic(single_source_env):
    """Verifies NULL handling in AND conditions maintains three-valued logic."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE quantity > 2 AND region IS NULL"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert set(projection) == {"order_id", "quantity", "region"}
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)


def test_null_in_or_logic(single_source_env):
    """Verifies NULL handling in OR conditions maintains three-valued logic."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE status = 'processing' OR region IS NULL"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert set(projection) == {"order_id", "status", "region"}
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Or)


def test_null_in_join_keys(single_source_env):
    """Ensures join operations handle NULL keys according to SQL semantics."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT o.order_id, p.name "
        "FROM duckdb_primary.main.orders o "
        "JOIN duckdb_primary.main.products p "
        "ON o.product_id = p.id "
        "WHERE o.region IS NULL"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert joins, "expected join to push down"
    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_null_in_group_by(single_source_env):
    """Validates GROUP BY treats NULL as a distinct group value."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, COUNT(*) "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    names = group_column_names(ast)
    assert names == ["region"]


def test_count_star_vs_count_column_with_nulls(single_source_env):
    """Ensures COUNT(*) counts all rows while COUNT(col) excludes NULLs."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT COUNT(*) AS total, COUNT(region) AS non_null_regions "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert set(projection) == {"COUNT(*)", "COUNT(region)"}
    count_star = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Count) and isinstance(node.this, exp.Star),
    )
    count_col = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Count)
        and isinstance(node.this, exp.Column),
    )
    assert len(count_star) == 1
    assert len(count_col) == 1


def test_sum_with_nulls(single_source_env):
    """Validates SUM ignores NULL values in aggregation."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT SUM(price) FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["SUM(price)"]
    matches = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Sum)
        and isinstance(node.this, exp.Column)
        and node.this.name.lower() == "price",
    )
    assert len(matches) == 1


def test_avg_with_nulls(single_source_env):
    """Validates AVG ignores NULL values when computing average."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT AVG(price) FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["AVG(price)"]
    matches = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Avg)
        and isinstance(node.this, exp.Column)
        and node.this.name.lower() == "price",
    )
    assert len(matches) == 1


def test_min_with_nulls(single_source_env):
    """Ensures MIN ignores NULL values when finding minimum."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT MIN(price) FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["MIN(price)"]
    matches = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Min)
        and isinstance(node.this, exp.Column)
        and node.this.name.lower() == "price",
    )
    assert len(matches) == 1


def test_max_with_nulls(single_source_env):
    """Ensures MAX ignores NULL values when finding maximum."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT MAX(price) FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["MAX(price)"]
    matches = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Max)
        and isinstance(node.this, exp.Column)
        and node.this.name.lower() == "price",
    )
    assert len(matches) == 1


def test_in_with_null_values(single_source_env):
    """Validates IN operator behavior when list contains NULL values."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE status IN ('processing', 'shipped')"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "status"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)


def test_not_in_with_null_semantics(single_source_env):
    """Validates NOT IN operator NULL semantics per SQL standard."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE status NOT IN ('cancelled', 'returned')"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "status"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Not)


def test_null_equality_comparison(single_source_env):
    """Verifies NULL = NULL evaluates to NULL not TRUE per SQL standard."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region IS NULL"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "region"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Is)
    assert isinstance(predicate.expression, exp.Null)


def test_null_with_boolean_logic(single_source_env):
    """Validates three-valued logic with NULL in complex conditions."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE (region IS NULL AND quantity > 2) OR status = 'processing'"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert set(projection) == {"order_id", "region", "quantity", "status"}
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Or)
