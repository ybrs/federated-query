"""Predicate edge cases pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    unwrap_parens,
)


def test_empty_in_list(single_source_env):
    """Verifies empty IN list behavior (WHERE col IN ())."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders WHERE region IN ()"
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)

    in_list = predicate.expressions
    assert len(in_list) == 0


def test_single_item_in_list(single_source_env):
    """Validates single-item IN list pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders WHERE region IN ('EU')"
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)

    left = unwrap_parens(predicate.this)
    assert isinstance(left, exp.Column)
    assert left.name.lower() == "region"

    in_list = predicate.expressions
    assert len(in_list) == 1
    first_item = unwrap_parens(in_list[0])
    assert isinstance(first_item, exp.Literal)
    assert first_item.this == "EU"


def test_large_in_list(single_source_env):
    """Checks large IN list (100+ items) pushes correctly."""
    runtime = build_runtime(single_source_env)
    values = ", ".join([f"'{i}'" for i in range(150)])
    sql = f"SELECT order_id FROM duckdb_primary.main.orders WHERE region IN ({values})"
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)

    in_list = predicate.expressions
    assert len(in_list) == 150


def test_between_equal_bounds(single_source_env):
    """Ensures BETWEEN with equal bounds (BETWEEN 5 AND 5) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price BETWEEN 100 AND 100"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Between)

    column = unwrap_parens(predicate.this)
    assert isinstance(column, exp.Column)
    assert column.name.lower() == "price"

    low = unwrap_parens(predicate.args.get("low"))
    assert isinstance(low, exp.Literal)
    assert int(low.this) == 100

    high = unwrap_parens(predicate.args.get("high"))
    assert isinstance(high, exp.Literal)
    assert int(high.this) == 100


def test_between_inverted_bounds(single_source_env):
    """Validates BETWEEN with inverted bounds (BETWEEN 10 AND 5) pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price BETWEEN 200 AND 100"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Between)

    low = unwrap_parens(predicate.args.get("low"))
    assert isinstance(low, exp.Literal)
    assert int(low.this) == 200

    high = unwrap_parens(predicate.args.get("high"))
    assert isinstance(high, exp.Literal)
    assert int(high.this) == 100


def test_like_only_wildcards(single_source_env):
    """Checks LIKE with only wildcards (LIKE '%%') pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region LIKE '%%'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Like)

    column = unwrap_parens(predicate.this)
    assert isinstance(column, exp.Column)
    assert column.name.lower() == "region"

    pattern = predicate.expression
    assert isinstance(pattern, exp.Literal)
    assert pattern.this == "%%"


def test_like_escaped_wildcards(single_source_env):
    """Validates LIKE with escaped wildcards pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region LIKE 'test\\_%'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Like)

    column = unwrap_parens(predicate.this)
    assert isinstance(column, exp.Column)
    assert column.name.lower() == "region"

    pattern = predicate.expression
    assert isinstance(pattern, exp.Literal)
    assert "test" in pattern.this


def test_regex_operators_behavior(single_source_env):
    """Documents regex operators ~ and !~ behavior (if supported)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region ~ '^E[UW]$'"
    )

    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, (exp.RegexpLike, exp.Binary))
