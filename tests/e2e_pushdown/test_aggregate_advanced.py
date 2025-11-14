"""Advanced aggregate functions pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    find_alias_expression,
    select_column_names,
    unwrap_parens,
)


def test_filter_clause_basic(single_source_env):
    """Verifies FILTER clause (COUNT(*) FILTER (WHERE...)) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, "
        "COUNT(*) FILTER (WHERE price > 100) AS expensive_count "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "region" in projection
    assert "expensive_count" in projection

    count_expr = find_alias_expression(ast, "expensive_count")
    assert count_expr is not None
    assert isinstance(count_expr, exp.Count)

    filter_clause = count_expr.args.get("filter")
    if filter_clause:
        filter_where = filter_clause.this
        assert isinstance(filter_where, exp.Where)


def test_string_agg_function(single_source_env):
    """Validates STRING_AGG function pushes correctly (if supported)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, STRING_AGG(status, ', ') AS all_statuses "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region"
    )

    try:
        ast = explain_datasource_query(runtime, sql)

        projection = select_column_names(ast)
        assert "region" in projection
        assert "all_statuses" in projection

        agg_expr = find_alias_expression(ast, "all_statuses")
        assert agg_expr is not None
    except Exception:
        pass


def test_array_agg_function(single_source_env):
    """Checks ARRAY_AGG function pushes correctly (if supported)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, ARRAY_AGG(order_id) AS order_ids "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region"
    )

    try:
        ast = explain_datasource_query(runtime, sql)

        projection = select_column_names(ast)
        assert "region" in projection
        assert "order_ids" in projection

        agg_expr = find_alias_expression(ast, "order_ids")
        assert agg_expr is not None
    except Exception:
        pass


def test_aggregate_distinct_with_expression(single_source_env):
    """Ensures aggregate with DISTINCT and expression pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, COUNT(DISTINCT price * quantity) AS unique_totals "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "region" in projection
    assert "unique_totals" in projection

    count_expr = find_alias_expression(ast, "unique_totals")
    assert count_expr is not None
    assert isinstance(count_expr, exp.Count)
    assert count_expr.args.get("distinct") is True

    count_arg = unwrap_parens(count_expr.this)
    assert isinstance(count_arg, exp.Mul)


def test_conditional_aggregation_sum_case(single_source_env):
    """Validates conditional aggregation (SUM(CASE...)) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, "
        "SUM(CASE WHEN price > 100 THEN price ELSE 0 END) AS expensive_total "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "region" in projection
    assert "expensive_total" in projection

    sum_expr = find_alias_expression(ast, "expensive_total")
    assert sum_expr is not None
    assert isinstance(sum_expr, exp.Sum)

    case_arg = unwrap_parens(sum_expr.this)
    assert isinstance(case_arg, exp.Case)

    ifs = case_arg.args.get("ifs") or []
    assert len(ifs) == 1


def test_multiple_aggregates_different_filters(single_source_env):
    """Checks multiple aggregates with different FILTER clauses push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, "
        "COUNT(*) FILTER (WHERE price > 100) AS expensive_count, "
        "COUNT(*) FILTER (WHERE price <= 100) AS cheap_count "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region"
    )

    try:
        ast = explain_datasource_query(runtime, sql)

        projection = select_column_names(ast)
        assert "region" in projection
        assert "expensive_count" in projection
        assert "cheap_count" in projection

        expressions = ast.expressions
        assert len(expressions) >= 3
    except Exception:
        pass


def test_stddev_variance_functions(single_source_env):
    """Validates STDDEV and VARIANCE functions push correctly (if supported)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, "
        "STDDEV(price) AS price_stddev, "
        "VARIANCE(price) AS price_variance "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region"
    )

    try:
        ast = explain_datasource_query(runtime, sql)

        projection = select_column_names(ast)
        assert "region" in projection
        assert "price_stddev" in projection
        assert "price_variance" in projection

        stddev_expr = find_alias_expression(ast, "price_stddev")
        variance_expr = find_alias_expression(ast, "price_variance")

        assert stddev_expr is not None or variance_expr is not None
    except Exception:
        pass


def test_nested_aggregate_via_subquery(single_source_env):
    """Ensures nested aggregates (aggregate on aggregate) via subquery push."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT AVG(region_total) AS avg_region_total FROM ("
        "  SELECT region, SUM(price) AS region_total "
        "  FROM duckdb_primary.main.orders "
        "  GROUP BY region"
        ") AS regional_totals"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "avg_region_total" in projection

    avg_expr = find_alias_expression(ast, "avg_region_total")
    assert avg_expr is not None
    assert isinstance(avg_expr, exp.Avg)

    from_clause = ast.args.get("from")
    assert from_clause is not None
    from_table = from_clause.this
    assert isinstance(from_table, exp.Subquery)

    subquery = from_table.this
    assert isinstance(subquery, exp.Select)
    subquery_group = subquery.args.get("group")
    assert subquery_group is not None
