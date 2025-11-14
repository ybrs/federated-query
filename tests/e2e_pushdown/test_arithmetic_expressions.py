"""Arithmetic expression pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
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
    assert "order_id" in projection
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)
    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Mul)
    assert isinstance(left.left, exp.Column)
    assert isinstance(left.right, exp.Column)


def test_complex_arithmetic_expression(single_source_env):
    """Validates complex arithmetic (price + tax) * factor pushes correctly."""
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
    assert isinstance(left.left, exp.Column)
    assert left.left.name.lower() == "quantity"


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
    assert isinstance(left.left, exp.Column)
    assert isinstance(left.right, exp.Column)


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
    assert isinstance(left, (exp.Neg, exp.Mul))


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
    found_mul = False
    for expression in ast.expressions:
        node = expression
        if isinstance(expression, exp.Alias):
            node = expression.this
        if isinstance(node, exp.Mul):
            found_mul = True
            break
    assert found_mul


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
    found_sub = False
    for expression in ast.expressions:
        node = expression
        if isinstance(expression, exp.Alias):
            node = expression.this
        if isinstance(node, exp.Sub):
            found_sub = True
            left = unwrap_parens(node.left)
            assert isinstance(left, exp.Mul)
            break
    assert found_sub


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
    assert len(expressions) > 0
    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Mul)
