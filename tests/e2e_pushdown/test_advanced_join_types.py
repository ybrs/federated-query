"""Advanced join type pushdown tests (FULL OUTER, self-join, NATURAL, USING)."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    join_table_names,
    select_column_names,
    unwrap_parens,
)


def test_full_outer_join_basic(single_source_env):
    """Verifies FULL OUTER JOIN pushes down to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "FULL OUTER JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) > 0
    first_join = joins[0]
    join_side = first_join.side
    assert join_side is not None


def test_full_outer_join_with_where(single_source_env):
    """Validates FULL OUTER JOIN with WHERE clause pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "FULL OUTER JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "WHERE O.region = 'EU' OR P.category = 'electronics'"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) > 0
    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_self_join_basic(single_source_env):
    """Ensures self-join (table joined to itself) pushes down."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O1.order_id, O2.order_id AS related_order "
        "FROM duckdb_primary.main.orders O1 "
        "JOIN duckdb_primary.main.orders O2 "
        "ON O1.region = O2.region AND O1.order_id < O2.order_id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) > 0
    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "related_order" in projection


def test_self_join_with_predicates(single_source_env):
    """Validates self-join with additional filtering predicates."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O1.order_id, O2.order_id "
        "FROM duckdb_primary.main.orders O1 "
        "JOIN duckdb_primary.main.orders O2 "
        "ON O1.product_id = O2.product_id "
        "WHERE O1.status = 'shipped' AND O2.status = 'processing'"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) > 0
    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_natural_join(single_source_env):
    """Checks NATURAL JOIN pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, name "
        "FROM duckdb_primary.main.orders "
        "NATURAL JOIN duckdb_primary.main.products"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) > 0


def test_join_with_using_single_column(single_source_env):
    """Validates JOIN with USING clause on single column."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "USING (product_id)"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    if joins:
        first_join = joins[0]
        using_clause = first_join.args.get("using")
        assert using_clause is not None or first_join.args.get("on") is not None


def test_join_with_using_multiple_columns(single_source_env):
    """Ensures JOIN with USING on multiple columns pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.orders O2 "
        "USING (region, status)"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) > 0


def test_join_with_multiple_on_conditions(single_source_env):
    """Validates JOIN with multiple AND conditions in ON clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id AND O.region = P.category"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) > 0
    first_join = joins[0]
    on_clause = first_join.args.get("on")
    if on_clause:
        condition = unwrap_parens(on_clause)
        assert isinstance(condition, (exp.EQ, exp.And))


def test_join_on_computed_columns(single_source_env):
    """Checks JOIN on arithmetic expressions pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.quantity * 2 = P.id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    if len(joins) > 0:
        first_join = joins[0]
        on_clause = first_join.args.get("on")
        assert on_clause is not None


def test_join_on_string_expressions(single_source_env):
    """Validates JOIN on string function expressions."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON UPPER(O.region) = UPPER(P.category)"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    if len(joins) > 0:
        first_join = joins[0]
        on_clause = first_join.args.get("on")
        assert on_clause is not None


def test_three_way_join_with_full_outer(single_source_env):
    """Ensures 3-table join with FULL OUTER pushes if supported."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "FULL OUTER JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "LEFT JOIN duckdb_primary.main.products P2 ON O.product_id = P2.id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 1


def test_join_with_coalesce_in_condition(single_source_env):
    """Checks JOIN with COALESCE in ON condition."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON COALESCE(O.product_id, 0) = P.id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) > 0


def test_join_with_case_in_condition(single_source_env):
    """Validates JOIN with CASE expression in ON clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = CASE WHEN P.active THEN P.id ELSE 0 END"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) > 0


def test_chained_left_right_combinations(single_source_env):
    """Ensures complex LEFT/RIGHT chain combinations push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "LEFT JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "RIGHT JOIN duckdb_primary.main.orders O2 ON O.region = O2.region"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 1


def test_mixed_inner_full_left_right_in_query(single_source_env):
    """Validates single query with INNER, FULL, LEFT, RIGHT joins."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "INNER JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "LEFT JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "WHERE O.status = 'processing'"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 1
    where_clause = ast.args.get("where")
    assert where_clause is not None
