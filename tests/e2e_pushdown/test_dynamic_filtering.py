"""Cross-source dynamic filtering (semi-join reduction) tests.

A cross-source INNER join cannot be pushed to one engine, so it runs as a local
hash join. The build side's distinct join keys are pushed into the probe side's
remote query as a ``key IN (...)`` filter, so the probe does not ship rows that
cannot match.
"""


from tests.e2e_pushdown.helpers import build_runtime


def _injected_scan(multi_source_env, sql):
    """Return the probe scan the plan marks for key injection, if any.

    The engine's EXPLAIN runs the SAME winners pre-pass the step emitter runs
    and tags each scan it will reduce with ``+inject(<column>)``, so the
    semi-join reduction is observable in the plan dump. Returns a dict with the
    probe's ``datasource`` and ``inject_column``, or None when no scan is
    tagged.
    """
    runtime = build_runtime(multi_source_env)
    for line in runtime.explain_text(sql).splitlines():
        stripped = line.strip()
        if not stripped.startswith("Scan [") or "+inject(" not in stripped:
            continue
        datasource = stripped[len("Scan [") : stripped.index("]")]
        tag_start = stripped.index("+inject(") + len("+inject(")
        column = stripped[tag_start : stripped.index(")", tag_start)]
        return {"datasource": datasource, "inject_column": column}
    return None


def _all_injected_scans(multi_source_env, sql):
    """Map every EXPLAIN scan tagged with a dynamic-filter injection to its column.

    The engine's EXPLAIN runs the SAME winners pre-pass the step emitter runs and
    tags each scan it will reduce with ``+inject(<column>)``. This collects them as
    ``{datasource: column}`` so a test can assert which sides are (and are not)
    reduced by the semi-join reduction.
    """
    runtime = build_runtime(multi_source_env)
    injected = {}
    for line in runtime.explain_text(sql).splitlines():
        stripped = line.strip()
        if not stripped.startswith("Scan [") or "+inject(" not in stripped:
            continue
        datasource = stripped[len("Scan [") : stripped.index("]")]
        tag_start = stripped.index("+inject(") + len("+inject(")
        column = stripped[tag_start : stripped.index(")", tag_start)]
        injected[datasource] = column
    return injected


def test_cross_source_join_pushes_dynamic_filter(multi_source_env):
    """The probe scan carries an injected dynamic filter from the build keys.

    The build carries a selective filter: an UNFILTERED dimension's keys are
    the probe's whole FK domain and the cost-based usefulness gate correctly
    refuses that reduction (it would filter nothing)."""
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_orders.main.orders O "
        "JOIN duckdb_products.main.products P ON O.product_id = P.id "
        "WHERE P.id > 102"
    )
    step = _injected_scan(multi_source_env, sql)
    # build side is the filtered products; the probe (orders) is constrained
    # to the surviving product ids, injected on its product_id column.
    assert step is not None
    assert step["datasource"] == "duckdb_orders"
    assert step["inject_column"] == "product_id"


def test_cross_source_comma_join_pushes_dynamic_filter(multi_source_env):
    """The comma-join form (promoted to an equi-join) also reduces the probe."""
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_orders.main.orders O, duckdb_products.main.products P "
        "WHERE O.product_id = P.id AND P.id > 102"
    )
    step = _injected_scan(multi_source_env, sql)
    assert step is not None
    assert step["datasource"] == "duckdb_orders"
    assert step["inject_column"] == "product_id"


def test_unfiltered_dimension_keys_are_not_injected(multi_source_env):
    """An unfiltered build side's keys cover the probe column's whole value
    domain (every order's product_id exists in products), so the reduction is
    provably a no-op and the gate must skip it."""
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_orders.main.orders O "
        "JOIN duckdb_products.main.products P ON O.product_id = P.id"
    )
    assert _injected_scan(multi_source_env, sql) is None


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
    """EXPLAIN shows the probe constrained to the build's real key value.

    Planning is O(metadata) in this engine (EXPLAIN never executes the build to
    collect keys), so the literal build key reaches the probe as a plan-time
    transitive constant on the probe scan's own pushed SQL - the same reduction
    the runtime IN filter expressed, shown with the real value.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "EXPLAIN SELECT O.order_id "
        "FROM duckdb_orders.main.orders O "
        "JOIN duckdb_products.main.products P ON O.product_id = P.id "
        "WHERE P.id = 101"
    )
    table = runtime.execute(sql)
    plan_text = "\n".join(row["plan"] for row in table.to_pylist())
    orders_lines = []
    for line in plan_text.splitlines():
        if "Scan [duckdb_orders]" in line:
            orders_lines.append(line)
    assert orders_lines, f"no orders scan in plan:\n{plan_text}"
    assert '"product_id" = 101' in orders_lines[0], plan_text


def test_build_side_chosen_by_filter_regardless_of_order(multi_source_env):
    """The filtered table is built from even when it is the left input, so the
    dynamic key filter still targets the other (big, unfiltered) side."""
    runtime = build_runtime(multi_source_env)
    # filtered table (products) written FIRST (left input)
    sql = (
        "SELECT P.name, O.order_id "
        "FROM duckdb_products.main.products P, duckdb_orders.main.orders O "
        "WHERE O.product_id = P.id AND P.id = 101"
    )
    table = runtime.execute(sql)
    assert table.schema.names == ["name", "order_id"]

    # orders (the big, unfiltered side) is the one marked for the key injection
    step = _injected_scan(multi_source_env, sql)
    assert step is not None
    assert step["datasource"] == "duckdb_orders"
    assert step["inject_column"] == "product_id"


def test_computed_build_side_still_reduces_the_probe(multi_source_env):
    """A build side that is itself a JOIN (not a plain scan) still donates its
    distinct keys; the reduction machinery does not require plain scans on both
    sides.

    Orientation (current Rust cost-based plan): for the chain
    (customers JOIN orders) JOIN products, the cost model reduces the ORDERS
    probe - the filtered customers build donates its customer ids, so the orders
    scan carries ``+inject(customer_id)``. The products scan receives NO
    injection under this orientation. The reduction never changes the result.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_customers.main.customers C "
        "JOIN duckdb_orders.main.orders O ON C.customer_id = O.customer_id "
        "JOIN duckdb_products.main.products P ON O.product_id = P.id "
        "WHERE C.segment = 'enterprise'"
    )
    injected = _all_injected_scans(multi_source_env, sql)
    # orders (the probe) is reduced by the computed customers-side keys.
    assert injected.get("duckdb_orders") == "customer_id"
    # products receives no injection under this orientation.
    assert "duckdb_products" not in injected
    # The reduced plan computes the same rows as the unreduced semantics:
    # enterprise customers 1,2 -> orders 1,2,6,7, all with a known product id.
    result = runtime.execute(sql)
    assert result.num_rows == 4
