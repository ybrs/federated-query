"""Cross-source dynamic filtering (semi-join reduction) tests.

A cross-source INNER join cannot be pushed to one engine, so it runs as a local
hash join. The build side's distinct join keys are pushed into the probe side's
remote query as a ``key IN (...)`` filter, so the probe does not ship rows that
cannot match.
"""

from sqlglot import exp

from tests.e2e_pushdown.helpers import build_runtime


def _find_in(ast) -> bool:
    """Whether a captured query AST contains an IN predicate."""
    return ast is not None and ast.find(exp.In) is not None


def test_cross_source_join_pushes_dynamic_filter(multi_source_env, duckdb_engine):
    """The probe-side remote query carries an IN filter from the build keys."""
    runtime = build_runtime(multi_source_env)
    multi_source_env.reset_datasources()
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_orders.main.orders O "
        "JOIN duckdb_products.main.products P ON O.product_id = P.id"
    )
    runtime.execute(sql)

    asts = multi_source_env.snapshot_asts()
    # build side is the right input (products); probe side is orders, which
    # should be constrained to product ids that exist in the build.
    assert _find_in(asts.get("duckdb_orders")), asts.get("duckdb_orders")


def test_cross_source_comma_join_pushes_dynamic_filter(multi_source_env, duckdb_engine):
    """The comma-join form (promoted to an equi-join) also reduces the probe."""
    runtime = build_runtime(multi_source_env)
    multi_source_env.reset_datasources()
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_orders.main.orders O, duckdb_products.main.products P "
        "WHERE O.product_id = P.id"
    )
    runtime.execute(sql)

    asts = multi_source_env.snapshot_asts()
    assert _find_in(asts.get("duckdb_orders")), asts.get("duckdb_orders")


def test_cross_source_join_results_correct(multi_source_env):
    """Dynamic filtering does not change results."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_orders.main.orders O "
        "JOIN duckdb_products.main.products P ON O.product_id = P.id"
    )
    table = runtime.execute(sql)
    # every order has a product_id in 101..104, all present in products
    assert table.num_rows == 10


def test_explain_shows_dynamic_filter_with_real_values(multi_source_env):
    """EXPLAIN renders the probe-side IN filter with the build's real keys."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "EXPLAIN SELECT O.order_id "
        "FROM duckdb_orders.main.orders O "
        "JOIN duckdb_products.main.products P ON O.product_id = P.id "
        "WHERE P.id = 101"
    )
    table = runtime.execute(sql)
    plan_text = "\n".join(row["plan"] for row in table.to_pylist())
    # the build (products) is filtered to id 101, so the probe IN shows it
    assert '"product_id" IN (101)' in plan_text


def test_build_side_chosen_by_filter_regardless_of_order(multi_source_env):
    """The filtered table is built from even when it is the left input, so the
    dynamic IN filter still targets the other (big, unfiltered) side."""
    runtime = build_runtime(multi_source_env)
    # filtered table (products) written FIRST (left input)
    sql = (
        "SELECT P.name, O.order_id "
        "FROM duckdb_products.main.products P, duckdb_orders.main.orders O "
        "WHERE O.product_id = P.id AND P.id = 101"
    )
    table = runtime.execute(sql)
    assert table.schema.names == ["name", "order_id"]

    plan = "\n".join(
        row["plan"] for row in runtime.execute("EXPLAIN " + sql).to_pylist()
    )
    # orders (the big, unfiltered side) is the one carrying the IN filter
    assert '"product_id" IN (101)' in plan
