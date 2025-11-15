"""Data type edge cases pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    unwrap_parens,
)


def test_integer_very_large_positive(single_source_env):
    """Verifies very large integer values push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price > 2147483647"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert int(right.this) == 2147483647


def test_integer_very_large_negative(single_source_env):
    """Validates very negative integer values push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price < -2147483648"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.LT)


def test_float_precision(single_source_env):
    """Checks float precision edge cases (0.1 + 0.2) push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price = 0.1 + 0.2"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Add)


def test_float_special_values_infinity(single_source_env):
    """Ensures handling of Infinity values pushes correctly (if supported)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price < 'Infinity'::FLOAT"
    )

    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_decimal_precision(single_source_env):
    """Validates DECIMAL precision handling pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price = CAST(123.456789 AS DECIMAL(10, 6))"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Cast)


def test_division_by_zero_behavior(single_source_env):
    """Checks division by zero behavior pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price / 0 AS division_result "
        "FROM duckdb_primary.main.orders"
    )

    ast = explain_datasource_query(runtime, sql)

    expressions = ast.expressions
    assert len(expressions) == 2


def test_negative_number_operations(single_source_env):
    """Ensures negative number arithmetic pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price * -1 < -100"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.LT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Mul)


def test_very_small_decimals(single_source_env):
    """Validates very small decimal values (0.000001) push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price > 0.000001"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)


def test_date_at_epoch(single_source_env):
    """Checks epoch date (1970-01-01) handling pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE created_at >= '1970-01-01'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GTE)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert "1970" in right.this


def test_far_future_date(single_source_env):
    """Validates far future date (9999-12-31) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE created_at < '9999-12-31'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.LT)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert "9999" in right.this


def test_boolean_null_handling(single_source_env):
    """Ensures boolean NULL handling pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE (price > 100) IS NULL"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Is)


def test_timestamp_timezone_comparison(single_source_env):
    """Validates timestamp with timezone comparisons push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE created_at > TIMESTAMP '2024-01-01 00:00:00'"
    )

    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_mixed_numeric_types(single_source_env):
    """Checks mixed numeric type operations (INT + FLOAT) push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE quantity + 1.5 > 10"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Add)

    add_right = unwrap_parens(left.right)
    assert isinstance(add_right, exp.Literal)


def test_scientific_notation(single_source_env):
    """Ensures scientific notation numbers push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price > 1.5e10"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)


def test_zero_in_numeric_operations(single_source_env):
    """Validates zero handling in various numeric operations pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price - price = 0 AND quantity * 0 = 0"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)
