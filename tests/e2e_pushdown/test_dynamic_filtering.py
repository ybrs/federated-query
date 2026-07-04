"""Cross-source dynamic filtering (semi-join reduction) tests.

A cross-source INNER join cannot be pushed to one engine, so it runs as a local
hash join. The build side's distinct join keys are pushed into the probe side's
remote query as a ``key IN (...)`` filter, so the probe does not ship rows that
cannot match.
"""

from federated_query.executor.rust_ir import build_ir

from tests.e2e_pushdown.helpers import build_runtime


def _injected_scan(multi_source_env, sql):
    """Return the Rust IR ``injected_scan`` step for the probe, if any.

    The Rust engine runs the whole cross-source join and injects the build's
    distinct keys into the probe's native query itself, so the Python proxy no
    longer sees the runtime SQL. The plan's serialized IR is where the semi-join
    reduction is now observable: an ``injected_scan`` step names the probe
    datasource and the column the ``key IN (...)`` filter constrains.
    """
    runtime = build_runtime(multi_source_env)
    plan = runtime.query_executor._plan_pipeline(sql, None)
    ir = build_ir(plan)
    for step in ir["steps"]:
        if step.get("op") == "injected_scan":
            return step
    return None


def test_cross_source_join_pushes_dynamic_filter(multi_source_env):
    """The probe scan carries an injected dynamic filter from the build keys."""
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_orders.main.orders O "
        "JOIN duckdb_products.main.products P ON O.product_id = P.id"
    )
    step = _injected_scan(multi_source_env, sql)
    # build side is products; the probe (orders) is constrained to product ids
    # that exist in the build, injected on its product_id column.
    assert step is not None
    assert step["datasource"] == "duckdb_orders"
    assert step["inject_column"] == "product_id"


def test_cross_source_comma_join_pushes_dynamic_filter(multi_source_env):
    """The comma-join form (promoted to an equi-join) also reduces the probe."""
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_orders.main.orders O, duckdb_products.main.products P "
        "WHERE O.product_id = P.id"
    )
    step = _injected_scan(multi_source_env, sql)
    assert step is not None
    assert step["datasource"] == "duckdb_orders"
    assert step["inject_column"] == "product_id"


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
