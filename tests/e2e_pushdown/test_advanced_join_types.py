"""Advanced join type pushdown tests (FULL OUTER, self-join, NATURAL, USING)."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    join_table_names,
    select_column_names,
    unwrap_parens,
)


def _get_join_node(select_ast: exp.Select, index: int = 0) -> exp.Join:
    """Return the JOIN child at index from a SELECT, asserting it exists."""
    joins = select_ast.args.get("joins") or []
    assert len(joins) > index
    return joins[index]


def _normalize_join_kind(join_expr: exp.Join) -> str:
    """Normalize sqlglot join metadata into INNER/LEFT/RIGHT/FULL strings."""
    kind = join_expr.args.get("kind")
    if kind is not None:
        if hasattr(kind, "value"):
            return str(kind.value).upper()
        return str(kind).upper()
    side = join_expr.args.get("side")
    if side is not None:
        return str(side).upper()
    return "INNER"


def _assert_join_type(select_ast: exp.Select, expected: str, index: int = 0) -> None:
    """Assert the remote SQL join at index matches the required join type."""
    join = _get_join_node(select_ast, index)
    actual = _normalize_join_kind(join)
    assert actual == expected


def _assert_join_count(select_ast: exp.Select, expected: int) -> None:
    """Assert exact number of joins in the SELECT."""
    joins = select_ast.args.get("joins") or []
    assert len(joins) == expected


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "FULL")

    join_node = _get_join_node(ast)
    on_clause = join_node.args.get("on")
    assert on_clause is not None
    condition = unwrap_parens(on_clause)
    assert isinstance(condition, exp.EQ)

    left_col = unwrap_parens(condition.left)
    right_col = unwrap_parens(condition.right)
    assert isinstance(left_col, exp.Column)
    assert isinstance(right_col, exp.Column)
    assert left_col.table == "O"
    assert left_col.name.lower() == "product_id"
    assert right_col.table == "P"
    assert right_col.name.lower() == "id"


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "FULL")

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Or)

    left_pred = unwrap_parens(predicate.left)
    right_pred = unwrap_parens(predicate.right)
    assert isinstance(left_pred, exp.EQ)
    assert isinstance(right_pred, exp.EQ)

    left_col = unwrap_parens(left_pred.left)
    assert isinstance(left_col, exp.Column)
    assert left_col.table == "O"
    assert left_col.name.lower() == "region"

    right_col = unwrap_parens(right_pred.left)
    assert isinstance(right_col, exp.Column)
    assert right_col.table == "P"
    assert right_col.name.lower() == "category"


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "INNER")

    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "related_order" in projection

    join_node = _get_join_node(ast)
    on_clause = join_node.args.get("on")
    assert on_clause is not None
    condition = unwrap_parens(on_clause)
    assert isinstance(condition, exp.And)


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "INNER")

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)

    left_pred = unwrap_parens(predicate.left)
    right_pred = unwrap_parens(predicate.right)
    assert isinstance(left_pred, exp.EQ)
    assert isinstance(right_pred, exp.EQ)


def test_natural_join(single_source_env):
    """Checks NATURAL JOIN pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, name "
        "FROM duckdb_primary.main.orders "
        "NATURAL JOIN duckdb_primary.main.products"
    )
    ast = explain_datasource_query(runtime, sql)
    _assert_join_count(ast, 1)

    join_node = _get_join_node(ast)
    natural = join_node.args.get("natural")
    assert natural is True or natural is not None


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
    _assert_join_count(ast, 1)

    join_node = _get_join_node(ast)
    using_clause = join_node.args.get("using")
    on_clause = join_node.args.get("on")
    assert using_clause is not None or on_clause is not None


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "INNER")


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "INNER")

    join_node = _get_join_node(ast)
    on_clause = join_node.args.get("on")
    assert on_clause is not None
    condition = unwrap_parens(on_clause)
    assert isinstance(condition, exp.And)

    left_cond = unwrap_parens(condition.left)
    right_cond = unwrap_parens(condition.right)
    assert isinstance(left_cond, exp.EQ)
    assert isinstance(right_cond, exp.EQ)


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "INNER")

    join_node = _get_join_node(ast)
    on_clause = join_node.args.get("on")
    assert on_clause is not None
    condition = unwrap_parens(on_clause)
    assert isinstance(condition, exp.EQ)

    left_expr = unwrap_parens(condition.left)
    assert isinstance(left_expr, exp.Mul)


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "INNER")

    join_node = _get_join_node(ast)
    on_clause = join_node.args.get("on")
    assert on_clause is not None
    condition = unwrap_parens(on_clause)
    assert isinstance(condition, exp.EQ)

    left_expr = unwrap_parens(condition.left)
    right_expr = unwrap_parens(condition.right)
    assert isinstance(left_expr, exp.Upper)
    assert isinstance(right_expr, exp.Upper)


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
    assert len(joins) == 2

    _assert_join_type(ast, "FULL", index=0)
    _assert_join_type(ast, "LEFT", index=1)


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "INNER")

    join_node = _get_join_node(ast)
    on_clause = join_node.args.get("on")
    assert on_clause is not None
    condition = unwrap_parens(on_clause)
    assert isinstance(condition, exp.EQ)

    left_expr = unwrap_parens(condition.left)
    assert isinstance(left_expr, exp.Coalesce)


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
    _assert_join_count(ast, 1)
    _assert_join_type(ast, "INNER")

    join_node = _get_join_node(ast)
    on_clause = join_node.args.get("on")
    assert on_clause is not None
    condition = unwrap_parens(on_clause)
    assert isinstance(condition, exp.EQ)

    right_expr = unwrap_parens(condition.right)
    assert isinstance(right_expr, exp.Case)


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
    assert len(joins) == 2

    _assert_join_type(ast, "LEFT", index=0)
    _assert_join_type(ast, "RIGHT", index=1)


def test_mixed_inner_full_left_right_in_query(single_source_env):
    """Validates single query with INNER and LEFT joins."""
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
    assert len(joins) == 2

    _assert_join_type(ast, "INNER", index=0)
    _assert_join_type(ast, "LEFT", index=1)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    col = unwrap_parens(predicate.left)
    assert isinstance(col, exp.Column)
    assert col.table == "O"
    assert col.name.lower() == "status"

    lit = unwrap_parens(predicate.right)
    assert isinstance(lit, exp.Literal)
    assert lit.this == "processing"
