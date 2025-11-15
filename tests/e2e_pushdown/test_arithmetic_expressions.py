"""Arithmetic expression pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    find_alias_expression,
    select_column_names,
    unwrap_parens,
)


def test_multiplication_in_where_clause(single_source_env):
    """Verifies multiplication expression pushes down in WHERE predicate."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price * quantity > 100"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "price", "quantity"]

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Mul)
    left_col = unwrap_parens(left.left)
    right_col = unwrap_parens(left.right)
    assert isinstance(left_col, exp.Column)
    assert isinstance(right_col, exp.Column)
    assert left_col.name.lower() == "price"
    assert right_col.name.lower() == "quantity"

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert int(right.this) == 100


def test_complex_arithmetic_expression(single_source_env):
    """Validates complex arithmetic (price + 10) * 1.1 pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE (price + 10) * 1.1 > 50"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Mul)

    inner = unwrap_parens(left.left)
    assert isinstance(inner, exp.Add)
    add_left = unwrap_parens(inner.left)
    assert isinstance(add_left, exp.Column)
    assert add_left.name.lower() == "price"


def test_modulo_operator_in_predicate(single_source_env):
    """Ensures modulo operator pushes down for filtering even/odd values."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE quantity % 2 = 0"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Mod)
    mod_left = unwrap_parens(left.left)
    assert isinstance(mod_left, exp.Column)
    assert mod_left.name.lower() == "quantity"

    mod_right = unwrap_parens(left.right)
    assert isinstance(mod_right, exp.Literal)
    assert int(mod_right.this) == 2


def test_division_in_where_clause(single_source_env):
    """Validates division expression pushes down correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price / quantity < 10"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.LT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Div)
    div_left = unwrap_parens(left.left)
    div_right = unwrap_parens(left.right)
    assert isinstance(div_left, exp.Column)
    assert isinstance(div_right, exp.Column)
    assert div_left.name.lower() == "price"
    assert div_right.name.lower() == "quantity"


def test_unary_minus_operator(single_source_env):
    """Checks unary minus operator pushes down for negation."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE -price < -50"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.LT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Neg) or isinstance(left, exp.Mul)

    if isinstance(left, exp.Neg):
        inner = unwrap_parens(left.this)
        assert isinstance(inner, exp.Column)
        assert inner.name.lower() == "price"


def test_arithmetic_with_null_handling(single_source_env):
    """Validates arithmetic expressions with NULL produce expected behavior."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price * quantity AS total "
        "FROM duckdb_primary.main.orders "
        "WHERE price IS NOT NULL AND quantity IS NOT NULL"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "total" in projection

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)

    left_pred = unwrap_parens(predicate.left)
    right_pred = unwrap_parens(predicate.right)
    assert isinstance(left_pred, exp.Is)
    assert isinstance(right_pred, exp.Is)


def test_arithmetic_in_select_projection(single_source_env):
    """Ensures computed arithmetic columns push to datasource SELECT."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price * quantity AS total_cost "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "total_cost" in projection

    total_expr = find_alias_expression(ast, "total_cost")
    assert total_expr is not None
    assert isinstance(total_expr, exp.Mul)

    left_col = unwrap_parens(total_expr.left)
    right_col = unwrap_parens(total_expr.right)
    assert isinstance(left_col, exp.Column)
    assert isinstance(right_col, exp.Column)
    assert left_col.name.lower() == "price"
    assert right_col.name.lower() == "quantity"


def test_nested_arithmetic_expressions(single_source_env):
    """Validates deeply nested arithmetic ((a + b) * (c - d)) pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE (price + 5) * (quantity - 1) > 100"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Mul)

    left_operand = unwrap_parens(left.left)
    right_operand = unwrap_parens(left.right)
    assert isinstance(left_operand, exp.Add)
    assert isinstance(right_operand, exp.Sub)

    add_left = unwrap_parens(left_operand.left)
    assert isinstance(add_left, exp.Column)
    assert add_left.name.lower() == "price"

    sub_left = unwrap_parens(right_operand.left)
    assert isinstance(sub_left, exp.Column)
    assert sub_left.name.lower() == "quantity"


def test_arithmetic_with_multiple_columns(single_source_env):
    """Checks arithmetic involving 3+ columns pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, (price + 10) * quantity - 5 AS adjusted_total "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "adjusted_total" in projection

    adjusted_expr = find_alias_expression(ast, "adjusted_total")
    assert adjusted_expr is not None
    assert isinstance(adjusted_expr, exp.Sub)

    sub_left = unwrap_parens(adjusted_expr.left)
    assert isinstance(sub_left, exp.Mul)

    mul_left = unwrap_parens(sub_left.left)
    assert isinstance(mul_left, exp.Add)


def test_order_by_arithmetic_expression(single_source_env):
    """Ensures ORDER BY with arithmetic expression pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price, quantity "
        "FROM duckdb_primary.main.orders "
        "ORDER BY price * quantity DESC"
    )
    ast = explain_datasource_query(runtime, sql)

    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Mul)

    left_col = unwrap_parens(order_expr.left)
    right_col = unwrap_parens(order_expr.right)
    assert isinstance(left_col, exp.Column)
    assert isinstance(right_col, exp.Column)
    assert left_col.name.lower() == "price"
    assert right_col.name.lower() == "quantity"

    assert first_order.args.get("desc") is True
