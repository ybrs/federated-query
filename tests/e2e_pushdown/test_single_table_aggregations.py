"""Single-table aggregation pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    find_in_select,
    group_column_names,
    select_column_names,
    unwrap_parens,
)


def test_count_star_pushdown(single_source_env):
    """Validates COUNT(*) is pushed once and operates over a star expression."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT COUNT(*) FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["COUNT(*)"]
    matches = find_in_select(ast, lambda node: isinstance(node, exp.Count))
    assert len(matches) == 1
    aggregate = matches[0]
    assert isinstance(aggregate.this, exp.Star)
    group_clause = ast.args.get("group")
    assert group_clause is None


def test_multiple_aggregates_with_aliases(single_source_env):
    """Ensures SUM(quantity) and AVG(price) push down exactly once each."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT SUM(quantity) AS total_qty, AVG(price) AS avg_price "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["SUM(quantity)", "AVG(price)"]
    sum_matches = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Sum)
        and isinstance(node.this, exp.Column)
        and node.this.name.lower() == "quantity",
    )
    assert len(sum_matches) == 1
    avg_matches = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Avg)
        and isinstance(node.this, exp.Column)
        and node.this.name.lower() == "price",
    )
    assert len(avg_matches) == 1


def test_group_by_single_column(single_source_env):
    """Checks single column GROUP BY emits only the region grouping."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, COUNT(*) AS cnt "
        "FROM duckdb_primary.main.orders GROUP BY region"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    names = group_column_names(ast)
    assert names == ["region"]
    projection = select_column_names(ast)
    assert projection == ["region", "COUNT(*)"]


def test_group_by_multiple_columns(single_source_env):
    """Checks multi-column GROUP BY emits region and status exactly once each."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, status, SUM(quantity) "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region, status"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    names = group_column_names(ast)
    assert set(names) == {"region", "status"}
    assert len(names) == 2
    projection = select_column_names(ast)
    assert projection[0] == "region"
    assert projection[1] == "status"


def test_having_clause_translated_to_remote_filter(single_source_env):
    """Verifies HAVING sum is converted into a remote filter predicate."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, SUM(quantity) AS total_qty "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region HAVING SUM(quantity) > 10"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["region", "SUM(quantity)"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)
    assert predicate.left.name.lower() == "total_qty"


def test_min_max_with_limit(single_source_env):
    """Ensures MIN/MAX push once each and limit/offset remain on datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT MIN(price) AS min_price, MAX(price) AS max_price "
        "FROM duckdb_primary.main.orders LIMIT 2 OFFSET 1"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["MIN(price)", "MAX(price)"]
    min_matches = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Min)
        and isinstance(node.this, exp.Column)
        and node.this.name.lower() == "price",
    )
    max_matches = find_in_select(
        ast,
        lambda node: isinstance(node, exp.Max)
        and isinstance(node.this, exp.Column)
        and node.this.name.lower() == "price",
    )
    assert len(min_matches) == 1
    assert len(max_matches) == 1
    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    offset_clause = ast.args.get("offset")
    assert offset_clause is not None
    offset_value = int(offset_clause.expression.this)
    assert offset_value == 1
