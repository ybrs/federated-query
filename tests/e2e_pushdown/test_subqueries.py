"""Subquery pushdown tests (EXISTS, IN, scalar, correlated, derived tables).

NOTE: This file is intentionally separate because the query engine may implement
a decorrelation engine to normalize subqueries in the future. Keeping all subquery
tests in one place makes it easier to verify decorrelation behavior.
"""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    select_column_names,
    unwrap_parens,
)


# EXISTS Subqueries


def test_where_exists_basic(single_source_env):
    """Verifies WHERE EXISTS subquery pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders O "
        "WHERE EXISTS ("
        "  SELECT 1 FROM duckdb_primary.main.products P "
        "  WHERE P.id = O.product_id AND P.price > 100"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Exists)

    subquery = predicate.this
    assert isinstance(subquery, exp.Select)
    subquery_where = subquery.args.get("where")
    assert subquery_where is not None


def test_where_not_exists(single_source_env):
    """Validates WHERE NOT EXISTS subquery pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders O "
        "WHERE NOT EXISTS ("
        "  SELECT 1 FROM duckdb_primary.main.products P "
        "  WHERE P.id = O.product_id AND P.status = 'discontinued'"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Not)

    exists_expr = unwrap_parens(predicate.this)
    assert isinstance(exists_expr, exp.Exists)

    subquery = exists_expr.this
    assert isinstance(subquery, exp.Select)


# IN Subqueries


def test_where_in_subquery(single_source_env):
    """Checks WHERE col IN (SELECT...) pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "WHERE product_id IN ("
        "  SELECT id FROM duckdb_primary.main.products WHERE price > 100"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)

    subquery_expr = predicate.args.get("query")
    assert isinstance(subquery_expr, exp.Subquery)
    subquery = subquery_expr.this
    assert isinstance(subquery, exp.Select)

    subquery_where = subquery.args.get("where")
    assert subquery_where is not None


def test_where_not_in_subquery(single_source_env):
    """Validates WHERE col NOT IN (SELECT...) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE product_id NOT IN ("
        "  SELECT id FROM duckdb_primary.main.products WHERE status = 'discontinued'"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Not)

    in_expr = unwrap_parens(predicate.this)
    assert isinstance(in_expr, exp.In)

    subquery_expr = in_expr.args.get("query")
    assert isinstance(subquery_expr, exp.Subquery)


# Scalar Subqueries


def test_scalar_subquery_max(single_source_env):
    """Ensures scalar subquery with MAX pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "WHERE price = ("
        "  SELECT MAX(price) FROM duckdb_primary.main.orders"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Subquery)
    subquery = right.this
    assert isinstance(subquery, exp.Select)

    subquery_projection = subquery.expressions
    assert len(subquery_projection) == 1
    max_expr = unwrap_parens(subquery_projection[0])
    assert isinstance(max_expr, exp.Max)


def test_scalar_subquery_avg_comparison(single_source_env):
    """Validates scalar subquery with AVG in comparison pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "WHERE price > ("
        "  SELECT AVG(price) FROM duckdb_primary.main.orders"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Subquery)
    subquery = right.this
    assert isinstance(subquery, exp.Select)

    subquery_projection = subquery.expressions
    assert len(subquery_projection) == 1
    avg_expr = unwrap_parens(subquery_projection[0])
    assert isinstance(avg_expr, exp.Avg)


def test_scalar_subquery_in_having(single_source_env):
    """Checks scalar subquery in HAVING clause pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, AVG(price) AS avg_price "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region "
        "HAVING AVG(price) > ("
        "  SELECT AVG(price) FROM duckdb_primary.main.orders"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "region" in projection
    assert "avg_price" in projection

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Subquery)


# Correlated Subqueries


def test_correlated_subquery_in_where(single_source_env):
    """Validates correlated subquery (references outer query) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders O "
        "WHERE price > ("
        "  SELECT AVG(price) FROM duckdb_primary.main.orders "
        "  WHERE region = O.region"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Subquery)
    subquery = right.this
    assert isinstance(subquery, exp.Select)

    subquery_where = subquery.args.get("where")
    assert subquery_where is not None
    subquery_predicate = unwrap_parens(subquery_where.this)
    assert isinstance(subquery_predicate, exp.EQ)


def test_correlated_subquery_in_select(single_source_env):
    """Ensures correlated subquery in SELECT projection pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, "
        "  (SELECT COUNT(*) FROM duckdb_primary.main.products P "
        "   WHERE P.id = O.product_id) AS product_count "
        "FROM duckdb_primary.main.orders O"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "product_count" in projection

    expressions = ast.expressions
    assert len(expressions) == 2

    second_expr = expressions[1]
    if isinstance(second_expr, exp.Alias):
        subquery_expr = unwrap_parens(second_expr.this)
        assert isinstance(subquery_expr, exp.Subquery)


# ANY / ALL Operators


def test_where_any_operator(single_source_env):
    """Checks WHERE col = ANY (SELECT...) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price = ANY ("
        "  SELECT price FROM duckdb_primary.main.orders WHERE region = 'EU'"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Any)

    subquery_expr = predicate.this
    assert isinstance(subquery_expr, exp.Subquery)


def test_where_all_operator(single_source_env):
    """Validates WHERE col > ALL (SELECT...) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "WHERE price > ALL ("
        "  SELECT price FROM duckdb_primary.main.orders WHERE region = 'US'"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.All)

    subquery_expr = right.this
    assert isinstance(subquery_expr, exp.Subquery)


# Nested Subqueries


def test_nested_subqueries(single_source_env):
    """Ensures nested subqueries (subquery in subquery) push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE product_id IN ("
        "  SELECT id FROM duckdb_primary.main.products "
        "  WHERE price > ("
        "    SELECT AVG(price) FROM duckdb_primary.main.products"
        "  )"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)

    outer_subquery_expr = predicate.args.get("query")
    assert isinstance(outer_subquery_expr, exp.Subquery)
    outer_subquery = outer_subquery_expr.this
    assert isinstance(outer_subquery, exp.Select)

    inner_where = outer_subquery.args.get("where")
    assert inner_where is not None
    inner_predicate = unwrap_parens(inner_where.this)
    assert isinstance(inner_predicate, exp.GT)

    inner_subquery_expr = unwrap_parens(inner_predicate.right)
    assert isinstance(inner_subquery_expr, exp.Subquery)


# Derived Tables (Subquery in FROM)


def test_subquery_in_from_derived_table(single_source_env):
    """Validates subquery in FROM clause (derived table) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, high_price FROM ("
        "  SELECT order_id, price AS high_price "
        "  FROM duckdb_primary.main.orders "
        "  WHERE price > 100"
        ") AS high_orders "
        "WHERE high_price > 200"
    )
    ast = explain_datasource_query(runtime, sql)

    from_clause = ast.args.get("from")
    assert from_clause is not None
    from_table = from_clause.this
    assert isinstance(from_table, exp.Subquery)

    derived_query = from_table.this
    assert isinstance(derived_query, exp.Select)
    derived_where = derived_query.args.get("where")
    assert derived_where is not None

    outer_where = ast.args.get("where")
    assert outer_where is not None


def test_derived_table_with_join(single_source_env):
    """Checks derived table joined with regular table pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, H.avg_price FROM duckdb_primary.main.orders O "
        "JOIN ("
        "  SELECT region, AVG(price) AS avg_price "
        "  FROM duckdb_primary.main.orders "
        "  GROUP BY region"
        ") H ON O.region = H.region"
    )
    ast = explain_datasource_query(runtime, sql)

    joins = ast.args.get("joins") or []
    assert len(joins) == 1

    join = joins[0]
    join_table = join.this
    assert isinstance(join_table, exp.Subquery)

    derived_query = join_table.this
    assert isinstance(derived_query, exp.Select)
    derived_group = derived_query.args.get("group")
    assert derived_group is not None


# Multiple Subqueries


def test_multiple_subqueries_in_where(single_source_env):
    """Ensures multiple subqueries in WHERE clause push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE product_id IN ("
        "  SELECT id FROM duckdb_primary.main.products WHERE price > 100"
        ") "
        "AND region IN ("
        "  SELECT DISTINCT region FROM duckdb_primary.main.orders WHERE quantity > 10"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)

    left_pred = unwrap_parens(predicate.left)
    assert isinstance(left_pred, exp.In)
    left_subquery = left_pred.args.get("query")
    assert isinstance(left_subquery, exp.Subquery)

    right_pred = unwrap_parens(predicate.right)
    assert isinstance(right_pred, exp.In)
    right_subquery = right_pred.args.get("query")
    assert isinstance(right_subquery, exp.Subquery)


# Subqueries with Aggregation


def test_subquery_with_group_by(single_source_env):
    """Validates subquery containing GROUP BY pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region IN ("
        "  SELECT region FROM duckdb_primary.main.orders "
        "  GROUP BY region HAVING COUNT(*) > 10"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)

    subquery_expr = predicate.args.get("query")
    assert isinstance(subquery_expr, exp.Subquery)
    subquery = subquery_expr.this
    assert isinstance(subquery, exp.Select)

    subquery_group = subquery.args.get("group")
    assert subquery_group is not None
    group_expressions = subquery_group.expressions or []
    assert len(group_expressions) == 1

    subquery_having = subquery.args.get("where")
    assert subquery_having is not None


def test_subquery_with_order_by_limit(single_source_env):
    """Ensures subquery with ORDER BY LIMIT pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE product_id IN ("
        "  SELECT id FROM duckdb_primary.main.products "
        "  ORDER BY price DESC LIMIT 5"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)

    subquery_expr = predicate.args.get("query")
    assert isinstance(subquery_expr, exp.Subquery)
    subquery = subquery_expr.this
    assert isinstance(subquery, exp.Select)

    subquery_order = subquery.args.get("order")
    assert subquery_order is not None
    order_expressions = subquery_order.expressions
    assert len(order_expressions) == 1

    subquery_limit = subquery.args.get("limit")
    assert isinstance(subquery_limit, exp.Limit)
    limit_value = int(subquery_limit.expression.this)
    assert limit_value == 5


# Subquery with UNION


def test_subquery_with_union(single_source_env):
    """Validates subquery containing UNION pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region IN ("
        "  SELECT region FROM duckdb_primary.main.orders WHERE price > 100 "
        "  UNION "
        "  SELECT region FROM duckdb_primary.main.orders WHERE quantity > 10"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)

    subquery_expr = predicate.args.get("query")
    assert isinstance(subquery_expr, exp.Subquery)
    union_query = subquery_expr.this
    assert isinstance(union_query, exp.Union)


# Complex EXISTS with Multiple Conditions


def test_exists_with_complex_predicates(single_source_env):
    """Checks EXISTS with complex AND/OR predicates pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders O "
        "WHERE EXISTS ("
        "  SELECT 1 FROM duckdb_primary.main.products P "
        "  WHERE P.id = O.product_id "
        "    AND (P.price > 100 OR P.status = 'premium') "
        "    AND P.quantity > 0"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Exists)

    subquery = predicate.this
    assert isinstance(subquery, exp.Select)
    subquery_where = subquery.args.get("where")
    assert subquery_where is not None

    subquery_predicate = unwrap_parens(subquery_where.this)
    assert isinstance(subquery_predicate, exp.And)


# Cross-Datasource Subquery Fallback


def test_cross_datasource_subquery_fallback(multi_source_env):
    """Documents cross-datasource subquery behavior (fallback expected)."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE product_id IN ("
        "  SELECT user_id FROM postgres_secondary.public.users WHERE active = true"
        ")"
    )

    doc = runtime.explain(sql)
    datasources = doc.get("datasources") or {}

    duckdb_queries = datasources.get("duckdb_primary", [])
    postgres_queries = datasources.get("postgres_secondary", [])

    assert len(duckdb_queries) >= 1
    assert len(postgres_queries) >= 1
