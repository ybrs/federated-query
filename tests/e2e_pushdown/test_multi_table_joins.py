"""Three-table join pushdown and combined scenarios."""

from tests.e2e_pushdown.helpers import build_runtime


def _explain_document(runtime, sql):
    statement = f"EXPLAIN (FORMAT JSON) {sql}"
    document = runtime.execute(statement)
    assert isinstance(document, dict)
    return document


def _assert_dual_queries(document):
    queries = document.get("queries", [])
    assert len(queries) == 2
    first = queries[0]["query"]
    second = queries[1]["query"]
    assert len(first.args.get("joins") or []) == 1
    assert not second.args.get("joins")
    return first, second


def test_three_table_inner_chain(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name, C.segment "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.customers C ON O.customer_id = C.customer_id"
    )
    document = _explain_document(runtime, sql)
    first, second = _assert_dual_queries(document)
    assert '"orders"' in first.sql()
    assert '"customers"' in second.sql()
    assert any("PhysicalRemoteJoin" in line for line in document["plan"])


def test_mixed_inner_left_chain(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name, C.segment "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "LEFT JOIN duckdb_primary.main.customers C ON O.customer_id = C.customer_id"
    )
    document = _explain_document(runtime, sql)
    _assert_dual_queries(document)
    assert any("PhysicalHashJoin cost=-1.0 rows=-1 details=type=LEFT" in line for line in document["plan"])


def test_three_table_with_predicates(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.customers C ON O.customer_id = C.customer_id "
        "WHERE P.category = 'clothing' AND C.segment = 'enterprise'"
    )
    document = _explain_document(runtime, sql)
    _assert_dual_queries(document)
    assert any("PhysicalFilter" in line for line in document["plan"])


def test_three_table_with_limit(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.customers C ON O.customer_id = C.customer_id "
        "LIMIT 5"
    )
    document = _explain_document(runtime, sql)
    _assert_dual_queries(document)
    assert any("Limit" in line for line in document["plan"])
