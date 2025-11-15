"""String edge cases pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    unwrap_parens,
)


def test_empty_string_vs_null(single_source_env):
    """Verifies distinction between empty string and NULL pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = '' OR region IS NULL"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Or)

    left_pred = unwrap_parens(predicate.left)
    assert isinstance(left_pred, exp.EQ)

    right_pred = unwrap_parens(predicate.right)
    assert isinstance(right_pred, exp.Is)


def test_unicode_strings(single_source_env):
    """Validates Unicode strings (emojis, Chinese, Arabic) push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region IN ('🌍', '中国', 'العربية')"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)

    in_list = predicate.expressions
    assert len(in_list) == 3


def test_strings_with_single_quotes(single_source_env):
    """Checks strings with single quotes (O'Brien) push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'O''Brien'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)


def test_strings_with_backslashes(single_source_env):
    """Ensures strings with backslashes push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'C:\\\\Users\\\\test'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)


def test_strings_with_newlines(single_source_env):
    """Validates strings with newline characters push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'Line1\nLine2'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)


def test_strings_with_tabs(single_source_env):
    """Checks strings with tab characters push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'Col1\tCol2'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)


def test_very_long_strings(single_source_env):
    """Ensures very long strings (1KB+) push correctly."""
    runtime = build_runtime(single_source_env)
    long_string = "A" * 2000
    sql = (
        f"SELECT order_id FROM duckdb_primary.main.orders "
        f"WHERE region = '{long_string}'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert len(right.this) == 2000


def test_sql_injection_patterns_escaped(single_source_env):
    """Validates SQL injection patterns are properly escaped in pushdown."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = ''; DROP TABLE orders; --'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)


def test_case_sensitivity_in_comparisons(single_source_env):
    """Checks case-sensitive string comparisons push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' AND region != 'eu'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)


def test_like_wildcards_various_positions(single_source_env):
    """Validates LIKE with % and _ wildcards in various positions push."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region LIKE 'E%' OR region LIKE '%U' OR region LIKE 'E_'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Or)


def test_ilike_case_insensitive(single_source_env):
    """Ensures ILIKE (case-insensitive LIKE) pushes correctly if supported."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region ILIKE 'eu%'"
    )

    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.ILike)


def test_strings_with_double_quotes(single_source_env):
    """Validates strings with double quotes push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'Say \"Hello\"'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)
