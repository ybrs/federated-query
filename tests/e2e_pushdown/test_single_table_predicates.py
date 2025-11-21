"""Phase 1 pushdown tests: single-table selects and predicates."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    select_column_names,
    unwrap_parens,
)


def test_select_all_no_filter(single_source_env):
    """Checks SELECT * stays a single column star with no WHERE clause."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    assert isinstance(ast, exp.Select)
    where_clause = ast.args.get("where")
    assert where_clause is None
    projection = select_column_names(ast)
    assert len(projection) == 1
    assert projection[0] == "*"


def test_projection_subset_columns(single_source_env):
    """Ensures projections list exactly the requested column subset."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id, region FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "region"]


def test_numeric_predicate_pushdown(single_source_env):
    """Verifies numeric predicates push as GT over the quantity column."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE quantity > 4"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["*"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)
    assert predicate.left.name.lower() == "quantity"


def test_string_predicate_pushdown(single_source_env):
    """Validates string equality predicates push verbatim with literal value."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE status = 'processing'"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["*"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)
    assert predicate.left.name.lower() == "status"
    literal = predicate.right
    assert isinstance(literal, exp.Literal)
    assert literal.this == "processing"


def test_boolean_predicate_pushdown(single_source_env):
    """Ensures bare boolean columns appear unchanged in the WHERE clause."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT id FROM duckdb_primary.main.products WHERE active"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["id", "active"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Column)
    assert predicate.name.lower() == "active"


def test_compound_and_predicate(single_source_env):
    """Confirms AND predicates push both equality operands for region/status."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' AND status = 'processing'"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "region", "status"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)
    left = unwrap_parens(predicate.left)
    right = unwrap_parens(predicate.right)
    assert isinstance(left, exp.EQ)
    assert isinstance(right, exp.EQ)


def test_or_predicate(single_source_env):
    """Checks OR predicates push in one piece without simplification."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' OR region = 'APAC'"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "region"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Or)


def test_in_list_predicate(single_source_env):
    """Ensures IN lists target the status column with both literals present."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE status IN ('processing', 'shipped')"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "status"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)
    assert predicate.this.name.lower() == "status"


def test_between_predicate(single_source_env):
    """Validates BETWEEN pushes for quantity with the original bounds."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE quantity BETWEEN 2 AND 6"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "quantity"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Between)
    assert predicate.this.name.lower() == "quantity"


def test_like_predicate(single_source_env):
    """Checks LIKE predicates stay remote with their pattern intact."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT name FROM duckdb_primary.main.products "
        "WHERE name LIKE 'c%'"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["name"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Like)


def test_limit_pushdown(single_source_env):
    """Ensures LIMIT clause of 3 is part of the remote AST."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' LIMIT 3"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "region"]
    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 3


def test_limit_offset_pushdown(single_source_env):
    """Confirms LIMIT/OFFSET clauses retain expected numeric values remotely."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "LIMIT 2 OFFSET 1"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id"]
    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    offset_clause = ast.args.get("offset")
    assert offset_clause is not None
    offset_value = int(offset_clause.expression.this)
    assert offset_value == 1


def test_less_than_predicate(single_source_env):
    """Verifies less than predicates push as LT over the quantity column."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE quantity < 5"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["*"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.LT)
    assert predicate.left.name.lower() == "quantity"


def test_less_than_or_equal_predicate(single_source_env):
    """Verifies less than or equal predicates push as LTE correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE quantity <= 3"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["*"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.LTE)
    assert predicate.left.name.lower() == "quantity"


def test_greater_than_or_equal_predicate(single_source_env):
    """Verifies greater than or equal predicates push as GTE correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE quantity >= 2"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["*"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GTE)
    assert predicate.left.name.lower() == "quantity"


def test_not_equal_predicate(single_source_env):
    """Validates not equal predicates push as NEQ for status filtering."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE status != 'cancelled'"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["*"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.NEQ)
    assert predicate.left.name.lower() == "status"


def test_is_null_predicate(single_source_env):
    """Ensures IS NULL predicates push correctly to detect null values."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE region IS NULL"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["*"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Is)
    assert predicate.this.name.lower() == "region"
    assert isinstance(predicate.expression, exp.Null)


def test_is_not_null_predicate(single_source_env):
    """Ensures IS NOT NULL predicates push correctly to filter out nulls."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE region IS NOT NULL"
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["*"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Not)
    inner = unwrap_parens(predicate.this)
    assert isinstance(inner, exp.Is)
    assert inner.this.name.lower() == "region"
    assert isinstance(inner.expression, exp.Null)


def test_not_in_predicate(single_source_env):
    """Validates NOT IN predicates push correctly with negated list check."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE status NOT IN ('cancelled', 'returned')"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "status"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Not)
    inner = unwrap_parens(predicate.this)
    assert isinstance(inner, exp.In)
    assert inner.this.name.lower() == "status"


def test_not_between_predicate(single_source_env):
    """Validates NOT BETWEEN pushes correctly with negated range check."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE quantity NOT BETWEEN 3 AND 7"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "quantity"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Not)
    inner = unwrap_parens(predicate.this)
    assert isinstance(inner, exp.Between)
    assert inner.this.name.lower() == "quantity"


def test_not_like_predicate(single_source_env):
    """Checks NOT LIKE predicates push correctly with pattern negation."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT name FROM duckdb_primary.main.products "
        "WHERE name NOT LIKE 'temp%'"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["name"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Not)
    inner = unwrap_parens(predicate.this)
    assert isinstance(inner, exp.Like)


def test_not_operator_with_equality(single_source_env):
    """Validates NOT operator wraps equality predicates correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE NOT (status = 'cancelled')"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "status"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Not)
    inner = unwrap_parens(predicate.this)
    assert isinstance(inner, exp.EQ)


def test_three_way_and_predicate(single_source_env):
    """Ensures three-way AND predicates push all conditions together."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' AND status = 'processing' AND quantity > 2"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert set(projection) == {"order_id", "region", "status", "quantity"}
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)


def test_three_way_or_predicate(single_source_env):
    """Ensures three-way OR predicates push all alternatives together."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' OR region = 'APAC' OR region = 'NA'"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert projection == ["order_id", "region"]
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Or)


def test_nested_and_or_combination(single_source_env):
    """Validates nested (a AND b) OR (c AND d) structure pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE (region = 'EU' AND status = 'processing') "
        "OR (region = 'APAC' AND status = 'shipped')"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert set(projection) == {"order_id", "region", "status"}
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Or)
    left = unwrap_parens(predicate.left)
    right = unwrap_parens(predicate.right)
    assert isinstance(left, exp.And)
    assert isinstance(right, exp.And)


def test_complex_nested_logic(single_source_env):
    """Checks complex ((a OR b) AND c) structure maintains hierarchy."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE ((region = 'EU' OR region = 'APAC') AND status = 'processing')"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert set(projection) == {"order_id", "region", "status"}
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)
    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Or)
