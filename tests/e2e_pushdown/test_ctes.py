"""CTE (WITH clause) pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    select_column_names,
    unwrap_parens,
)


def test_simple_cte_referenced_once(single_source_env):
    """Verifies simple CTE referenced once pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH high_value AS ("
        "  SELECT order_id, price FROM duckdb_primary.main.orders WHERE price > 100"
        ") "
        "SELECT order_id FROM high_value WHERE price > 200"
    )
    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 1

    first_cte = ctes[0]
    assert isinstance(first_cte, exp.CTE)
    assert first_cte.alias.lower() == "high_value"

    cte_query = first_cte.this
    assert isinstance(cte_query, exp.Select)
    cte_where = cte_query.args.get("where")
    assert cte_where is not None


def test_cte_referenced_multiple_times(single_source_env):
    """Validates CTE referenced multiple times pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH high_value AS ("
        "  SELECT order_id, price FROM duckdb_primary.main.orders WHERE price > 100"
        ") "
        "SELECT h1.order_id FROM high_value h1 "
        "JOIN high_value h2 ON h1.price = h2.price "
        "WHERE h1.order_id != h2.order_id"
    )
    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 1

    joins = ast.args.get("joins") or []
    assert len(joins) == 1

    join = joins[0]
    join_table = join.this
    assert isinstance(join_table, (exp.Table, exp.Alias))


def test_multiple_ctes(single_source_env):
    """Ensures multiple CTEs push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH "
        "high_value AS ("
        "  SELECT order_id, price FROM duckdb_primary.main.orders WHERE price > 100"
        "), "
        "high_quantity AS ("
        "  SELECT order_id, quantity FROM duckdb_primary.main.orders WHERE quantity > 10"
        ") "
        "SELECT h.order_id FROM high_value h "
        "JOIN high_quantity q ON h.order_id = q.order_id"
    )
    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 2

    first_cte = ctes[0]
    assert isinstance(first_cte, exp.CTE)
    assert first_cte.alias.lower() == "high_value"

    second_cte = ctes[1]
    assert isinstance(second_cte, exp.CTE)
    assert second_cte.alias.lower() == "high_quantity"

    joins = ast.args.get("joins") or []
    assert len(joins) == 1


def test_cte_with_join(single_source_env):
    """Checks CTE containing JOIN pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH order_product AS ("
        "  SELECT O.order_id, O.price, P.name "
        "  FROM duckdb_primary.main.orders O "
        "  JOIN duckdb_primary.main.products P ON O.product_id = P.id"
        ") "
        "SELECT order_id, name FROM order_product WHERE price > 50"
    )
    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 1

    cte = ctes[0]
    assert isinstance(cte, exp.CTE)
    cte_query = cte.this
    assert isinstance(cte_query, exp.Select)

    cte_joins = cte_query.args.get("joins") or []
    assert len(cte_joins) == 1


def test_cte_with_aggregation(single_source_env):
    """Validates CTE with aggregation pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH region_stats AS ("
        "  SELECT region, COUNT(*) AS cnt, AVG(price) AS avg_price "
        "  FROM duckdb_primary.main.orders "
        "  GROUP BY region"
        ") "
        "SELECT region FROM region_stats WHERE cnt > 10"
    )
    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 1

    cte = ctes[0]
    assert isinstance(cte, exp.CTE)
    cte_query = cte.this
    assert isinstance(cte_query, exp.Select)

    cte_group = cte_query.args.get("group")
    assert cte_group is not None
    cte_group_expressions = cte_group.expressions or []
    assert len(cte_group_expressions) == 1


def test_cte_with_where(single_source_env):
    """Ensures CTE with WHERE clause pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH filtered_orders AS ("
        "  SELECT order_id, price, region "
        "  FROM duckdb_primary.main.orders "
        "  WHERE region = 'EU' AND price > 50"
        ") "
        "SELECT order_id FROM filtered_orders WHERE price > 100"
    )
    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 1

    cte = ctes[0]
    cte_query = cte.this
    assert isinstance(cte_query, exp.Select)

    cte_where = cte_query.args.get("where")
    assert cte_where is not None
    cte_predicate = unwrap_parens(cte_where.this)
    assert isinstance(cte_predicate, exp.And)

    main_where = ast.args.get("where")
    assert main_where is not None


def test_nested_cte(single_source_env):
    """Checks nested CTE (CTE referencing another CTE) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH "
        "all_orders AS ("
        "  SELECT order_id, price FROM duckdb_primary.main.orders"
        "), "
        "expensive_orders AS ("
        "  SELECT order_id FROM all_orders WHERE price > 100"
        ") "
        "SELECT order_id FROM expensive_orders"
    )
    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 2

    first_cte = ctes[0]
    assert isinstance(first_cte, exp.CTE)
    assert first_cte.alias.lower() == "all_orders"

    second_cte = ctes[1]
    assert isinstance(second_cte, exp.CTE)
    assert second_cte.alias.lower() == "expensive_orders"

    second_cte_query = second_cte.this
    assert isinstance(second_cte_query, exp.Select)
    second_from = second_cte_query.args.get("from")
    assert second_from is not None


def test_cte_with_union(single_source_env):
    """Validates CTE with UNION pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH combined AS ("
        "  SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'EU' "
        "  UNION ALL "
        "  SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'US'"
        ") "
        "SELECT order_id FROM combined"
    )
    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 1

    cte = ctes[0]
    assert isinstance(cte, exp.CTE)
    cte_query = cte.this
    assert isinstance(cte_query, exp.Union)


def test_cte_with_order_by_limit(single_source_env):
    """Ensures CTE with ORDER BY and LIMIT pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH top_orders AS ("
        "  SELECT order_id, price FROM duckdb_primary.main.orders "
        "  ORDER BY price DESC LIMIT 10"
        ") "
        "SELECT order_id FROM top_orders WHERE price > 100"
    )
    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 1

    cte = ctes[0]
    cte_query = cte.this
    assert isinstance(cte_query, exp.Select)

    cte_order = cte_query.args.get("order")
    assert cte_order is not None
    cte_order_expressions = cte_order.expressions
    assert len(cte_order_expressions) == 1

    cte_limit = cte_query.args.get("limit")
    assert isinstance(cte_limit, exp.Limit)
    limit_value = int(cte_limit.expression.this)
    assert limit_value == 10


def test_recursive_cte_behavior(single_source_env):
    """Documents recursive CTE behavior (fallback or support)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "WITH RECURSIVE counter(n) AS ("
        "  SELECT 1 "
        "  UNION ALL "
        "  SELECT n + 1 FROM counter WHERE n < 5"
        ") "
        "SELECT n FROM counter"
    )

    ast = explain_datasource_query(runtime, sql)

    with_clause = ast.args.get("with")
    assert with_clause is not None

    ctes = with_clause.expressions
    assert len(ctes) == 1

    cte = ctes[0]
    assert isinstance(cte, exp.CTE)
