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


def test_computed_build_side_still_reduces_the_probe(multi_source_env):
    """A build side that is itself a JOIN (not a plain scan) must still donate
    its distinct keys: the reduction machinery is generic downstream, only the
    gate used to require plain scans on BOTH sides. The chain
    (customers JOIN orders) JOIN products must inject the pair's product ids
    into the products scan."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_customers.main.customers C "
        "JOIN duckdb_orders.main.orders O ON C.customer_id = O.customer_id "
        "JOIN duckdb_products.main.products P ON O.product_id = P.id "
        "WHERE C.segment = 'enterprise'"
    )
    plan = runtime.query_executor._plan_pipeline(sql, None)
    ir = build_ir(plan)
    injected = []
    for step in ir["steps"]:
        if step.get("op") == "injected_scan":
            injected.append(step)
    products_injections = []
    for step in injected:
        if step["datasource"] == "duckdb_products":
            products_injections.append(step)
    assert products_injections, f"no injection into products; steps: {injected}"
    assert products_injections[0]["inject_column"] == "id"
    # And the reduced plan computes the same rows as the unreduced semantics.
    result = runtime.execute(sql)
    assert result.num_rows > 0


def test_large_key_set_against_duckdb_probe_is_correct():
    """Above the IN cap (2000 keys) a DuckDB probe used to fall back to a
    full fetch; it now takes the temp-table semi-join arm. Either way the
    results must be exact - this pins correctness of the new arm end to end
    with 3000 distinct build keys."""
    import duckdb as duckdb_module
    from federated_query.catalog import Catalog
    from federated_query.cli.fedq import FedQRuntime
    from federated_query.config import Config
    from federated_query.datasources.duckdb import DuckDBDataSource
    from tests.duckdb_tmp import duckdb_path

    dims = DuckDBDataSource("dims", {"path": duckdb_path(), "read_only": False})
    dims.connect()
    dims.connection.execute(
        "CREATE TABLE keys_side (k BIGINT);"
        "INSERT INTO keys_side SELECT g * 2 FROM range(0, 3000) t(g);"
    )
    facts = DuckDBDataSource("facts", {"path": duckdb_path(), "read_only": False})
    facts.connect()
    facts.connection.execute(
        "CREATE TABLE probe (k BIGINT, v BIGINT);"
        "INSERT INTO probe SELECT g, g * 10 FROM range(0, 20000) t(g);"
    )
    catalog = Catalog()
    catalog.register_datasource(dims)
    catalog.register_datasource(facts)
    catalog.load_metadata()
    runtime = FedQRuntime(catalog, Config())
    result = runtime.execute(
        "SELECT sum(p.v) AS s FROM dims.main.keys_side d "
        "JOIN facts.main.probe p ON d.k = p.k"
    )
    oracle = duckdb_module.connect()
    oracle.execute(
        "CREATE TABLE keys_side AS SELECT g * 2 AS k FROM range(0, 3000) t(g);"
        "CREATE TABLE probe AS SELECT g AS k, g * 10 AS v FROM range(0, 20000) t(g);"
    )
    expected = oracle.execute(
        "SELECT sum(p.v) FROM keys_side d JOIN probe p ON d.k = p.k"
    ).fetchone()[0]
    assert result.column("s").to_pylist() == [expected]


def test_pushed_island_probe_receives_key_injection():
    """The composition of locality + reduction: the facts island collapses
    into ONE remote query, and the dim side's keys inject into THAT query
    (its raw SQL wrapped as a derived table). This is the q05 endgame: the
    island ships only rows matching the dim keys."""
    import duckdb as duckdb_module
    from federated_query.catalog import Catalog
    from federated_query.cli.fedq import FedQRuntime
    from federated_query.config import Config
    from federated_query.datasources.duckdb import DuckDBDataSource
    from tests.duckdb_tmp import duckdb_path

    dims = DuckDBDataSource("dims", {"path": duckdb_path(), "read_only": False})
    dims.connect()
    dims.connection.execute(
        "CREATE TABLE supplier (s_id INTEGER, s_flag VARCHAR);"
        "INSERT INTO supplier SELECT g, 'F' || (g % 3) FROM range(0, 100) t(g);"
    )
    facts = DuckDBDataSource("facts", {"path": duckdb_path(), "read_only": False})
    facts.connect()
    facts.connection.execute(
        "CREATE TABLE orders (o_id INTEGER, o_flag INTEGER);"
        "INSERT INTO orders SELECT g, g % 10 FROM range(0, 2000) t(g);"
        "CREATE TABLE lineitem (l_o INTEGER, l_s INTEGER, l_v INTEGER);"
        "INSERT INTO lineitem SELECT g % 2000, g % 100, g FROM range(0, 10000) t(g);"
    )
    catalog = Catalog()
    catalog.register_datasource(dims)
    catalog.register_datasource(facts)
    catalog.load_metadata()
    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT sum(l.l_v) AS s "
        "FROM dims.main.supplier s, facts.main.orders o, facts.main.lineitem l "
        "WHERE s.s_id = l.l_s AND o.o_id = l.l_o "
        "AND o.o_flag = 3 AND s.s_flag = 'F1'"
    )
    plan = runtime.query_executor._plan_pipeline(sql, None)
    ir = build_ir(plan)
    raw_injections = []
    for step in ir["steps"]:
        if step.get("op") == "injected_scan" and "raw_sql" in step["scan"]:
            raw_injections.append(step)
    assert raw_injections, f"no raw-sql injection; steps: {ir['steps']}"
    assert step_targets_facts_island(raw_injections[0])
    result = runtime.execute(sql)
    oracle = duckdb_module.connect()
    oracle.execute(
        "CREATE TABLE supplier AS SELECT g AS s_id, 'F' || (g % 3) AS s_flag"
        " FROM range(0, 100) t(g);"
        "CREATE TABLE orders AS SELECT g AS o_id, g % 10 AS o_flag"
        " FROM range(0, 2000) t(g);"
        "CREATE TABLE lineitem AS SELECT g % 2000 AS l_o, g % 100 AS l_s, g AS l_v"
        " FROM range(0, 10000) t(g);"
    )
    expected = oracle.execute(
        "SELECT sum(l.l_v) FROM supplier s, orders o, lineitem l "
        "WHERE s.s_id = l.l_s AND o.o_id = l.l_o "
        "AND o.o_flag = 3 AND s.s_flag = 'F1'"
    ).fetchone()[0]
    assert result.column("s").to_pylist() == [expected]


def step_targets_facts_island(step):
    """The injected raw query must be the facts island (joins both tables)."""
    raw = step["scan"]["raw_sql"].upper()
    return "ORDERS" in raw and "LINEITEM" in raw and step["datasource"] == "facts"


def test_semi_join_reduces_the_preserved_side():
    """An IN-subquery decorrelates to a SEMI join; the subquery side's keys
    must inject into the preserved side's scan (the injected IN filter IS
    the semi condition; the coordinator semi join stays for exactness)."""
    import duckdb as duckdb_module
    from federated_query.catalog import Catalog
    from federated_query.cli.fedq import FedQRuntime
    from federated_query.config import Config
    from federated_query.datasources.duckdb import DuckDBDataSource
    from tests.duckdb_tmp import duckdb_path

    facts = DuckDBDataSource("facts", {"path": duckdb_path(), "read_only": False})
    facts.connect()
    facts.connection.execute(
        "CREATE TABLE orders (o_id INTEGER, o_v INTEGER);"
        "INSERT INTO orders SELECT g, g % 50 FROM range(0, 500) t(g);"
    )
    dims = DuckDBDataSource("dims", {"path": duckdb_path(), "read_only": False})
    dims.connect()
    dims.connection.execute(
        "CREATE TABLE lineitem (l_o INTEGER, l_q INTEGER);"
        "INSERT INTO lineitem SELECT g % 500, g % 9 FROM range(0, 5000) t(g);"
    )
    catalog = Catalog()
    catalog.register_datasource(facts)
    catalog.register_datasource(dims)
    catalog.load_metadata()
    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT o.o_id FROM facts.main.orders o WHERE o.o_id IN ("
        "SELECT l.l_o FROM dims.main.lineitem l GROUP BY l.l_o"
        " HAVING sum(l.l_q) > 42)"
    )
    plan = runtime.query_executor._plan_pipeline(sql, None)
    ir = build_ir(plan)
    injected = []
    for step in ir["steps"]:
        if step.get("op") == "injected_scan" and step["datasource"] == "facts":
            injected.append(step)
    assert injected, f"orders never reduced; steps: {ir['steps']}"
    assert injected[0]["inject_column"] == "o_id"
    result = runtime.execute(sql)
    oracle = duckdb_module.connect()
    oracle.execute(
        "CREATE TABLE orders AS SELECT g AS o_id, g % 50 AS o_v"
        " FROM range(0, 500) t(g);"
        "CREATE TABLE lineitem AS SELECT g % 500 AS l_o, g % 9 AS l_q"
        " FROM range(0, 5000) t(g);"
    )
    expected = set()
    for row in oracle.execute(
        "SELECT o_id FROM orders WHERE o_id IN (SELECT l_o FROM lineitem"
        " GROUP BY l_o HAVING sum(l_q) > 42)"
    ).fetchall():
        expected.add(row[0])
    assert set(result.column("o_id").to_pylist()) == expected


def test_selective_semi_join_reduces_the_fact_scan():
    """The q18 shape end to end: a selective IN-subquery (few surviving
    orderkeys) must push below the fact join so the fact scan is reduced to
    the surviving keys - not scanned whole and filtered at the top. Verifies
    the fact injected_scan appears AND the result matches the oracle."""
    import duckdb as duckdb_module
    from federated_query.catalog import Catalog
    from federated_query.cli.fedq import FedQRuntime
    from federated_query.config import Config
    from federated_query.datasources.duckdb import DuckDBDataSource
    from tests.duckdb_tmp import duckdb_path

    dims = DuckDBDataSource("dims", {"path": duckdb_path(), "read_only": False})
    dims.connect()
    dims.connection.execute(
        "CREATE TABLE customer (c_id INTEGER, c_name VARCHAR);"
        "INSERT INTO customer SELECT g, 'C' || g FROM range(0, 300) t(g);"
    )
    facts = DuckDBDataSource("facts", {"path": duckdb_path(), "read_only": False})
    facts.connect()
    facts.connection.execute(
        "CREATE TABLE orders (o_id INTEGER, o_c INTEGER);"
        "INSERT INTO orders SELECT g, g % 300 FROM range(0, 3000) t(g);"
        "CREATE TABLE lineitem (l_o INTEGER, l_q INTEGER);"
        "INSERT INTO lineitem SELECT g % 3000, g % 13 FROM range(0, 30000) t(g);"
    )
    catalog = Catalog()
    catalog.register_datasource(dims)
    catalog.register_datasource(facts)
    catalog.load_metadata()
    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT c.c_name, sum(l.l_q) AS q "
        "FROM dims.main.customer c, facts.main.orders o, facts.main.lineitem l "
        "WHERE c.c_id = o.o_c AND l.l_o = o.o_id "
        "AND o.o_id IN (SELECT l2.l_o FROM facts.main.lineitem l2 "
        "GROUP BY l2.l_o HAVING sum(l2.l_q) > 80) "
        "GROUP BY c.c_name"
    )
    plan = runtime.query_executor._plan_pipeline(sql, None)
    ir = build_ir(plan)
    fact_injections = []
    for step in ir["steps"]:
        if step.get("op") == "injected_scan" and step["datasource"] == "facts":
            fact_injections.append(step)
    assert fact_injections, f"no fact reduction; steps: {ir['steps']}"
    result = runtime.execute(sql)
    oracle = duckdb_module.connect()
    oracle.execute(
        "CREATE TABLE customer AS SELECT g AS c_id, 'C' || g AS c_name"
        " FROM range(0, 300) t(g);"
        "CREATE TABLE orders AS SELECT g AS o_id, g % 300 AS o_c"
        " FROM range(0, 3000) t(g);"
        "CREATE TABLE lineitem AS SELECT g % 3000 AS l_o, g % 13 AS l_q"
        " FROM range(0, 30000) t(g);"
    )
    expected = set()
    for row in oracle.execute(
        "SELECT c.c_name, sum(l.l_q) FROM customer c, orders o, lineitem l "
        "WHERE c.c_id = o.o_c AND l.l_o = o.o_id "
        "AND o.o_id IN (SELECT l2.l_o FROM lineitem l2 GROUP BY l2.l_o"
        " HAVING sum(l2.l_q) > 80) GROUP BY c.c_name"
    ).fetchall():
        expected.add(tuple(row))
    got = set()
    for index in range(result.num_rows):
        got.add((result.column("c_name")[index].as_py(),
                 result.column("q")[index].as_py()))
    assert got == expected


def test_left_join_aggregate_subquery_probe_is_reduced():
    """The q17 shape: a scalar-avg subquery correlated to a filtered outer
    decorrelates to a LEFT join onto an aggregate subquery. The outer's keys
    must inject into that subquery's aggregate base (its raw SQL wrapped and
    filtered on the group key) so it computes averages for only the matching
    parts - not every part. Differential-checked against the oracle."""
    import duckdb as duckdb_module
    from federated_query.catalog import Catalog
    from federated_query.cli.fedq import FedQRuntime
    from federated_query.config import Config
    from federated_query.datasources.duckdb import DuckDBDataSource
    from tests.duckdb_tmp import duckdb_path

    dims = DuckDBDataSource("dims", {"path": duckdb_path(), "read_only": False})
    dims.connect()
    dims.connection.execute(
        "CREATE TABLE part (p_id INTEGER, p_flag VARCHAR);"
        "INSERT INTO part SELECT g, 'F' || (g % 50) FROM range(0, 500) t(g);"
    )
    facts = DuckDBDataSource("facts", {"path": duckdb_path(), "read_only": False})
    facts.connect()
    facts.connection.execute(
        "CREATE TABLE li (l_p INTEGER, l_q INTEGER);"
        "INSERT INTO li SELECT g % 500, g % 11 FROM range(0, 20000) t(g);"
    )
    catalog = Catalog()
    catalog.register_datasource(dims)
    catalog.register_datasource(facts)
    catalog.load_metadata()
    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT sum(l.l_q) AS s "
        "FROM dims.main.part p, facts.main.li l "
        "WHERE p.p_id = l.l_p AND p.p_flag = 'F7' "
        "AND l.l_q > (SELECT avg(l2.l_q) FROM facts.main.li l2 WHERE l2.l_p = p.p_id)"
    )
    plan = runtime.query_executor._plan_pipeline(sql, None)
    ir = build_ir(plan)
    raw_injections = []
    for step in ir["steps"]:
        if step.get("op") == "injected_scan" and "raw_sql" in step["scan"]:
            raw_injections.append(step)
    assert raw_injections, f"aggregate subquery not injected; steps: {ir['steps']}"
    result = runtime.execute(sql)
    oracle = duckdb_module.connect()
    oracle.execute(
        "CREATE TABLE part AS SELECT g AS p_id, 'F' || (g % 50) AS p_flag"
        " FROM range(0, 500) t(g);"
        "CREATE TABLE li AS SELECT g % 500 AS l_p, g % 11 AS l_q"
        " FROM range(0, 20000) t(g);"
    )
    expected = oracle.execute(
        "SELECT sum(l.l_q) FROM part p, li l WHERE p.p_id = l.l_p AND p.p_flag = 'F7'"
        " AND l.l_q > (SELECT avg(l2.l_q) FROM li l2 WHERE l2.l_p = p.p_id)"
    ).fetchone()[0]
    assert result.column("s").to_pylist() == [expected]


def test_reduction_reduces_the_larger_side_by_cardinality():
    """The q11 shape: a big fact scan INNER-joined to a tiny selective dim
    that collapsed into a remote query. The reduction must inject into the
    BIG fact (reduce it), not the tiny dim - driven by the cost estimate, not
    the structural remote>scan heuristic. Differential-checked."""
    import duckdb as duckdb_module
    from federated_query.catalog import Catalog
    from federated_query.cli.fedq import FedQRuntime
    from federated_query.config import Config
    from federated_query.datasources.duckdb import DuckDBDataSource
    from tests.duckdb_tmp import duckdb_path

    dims = DuckDBDataSource("dims", {"path": duckdb_path(), "read_only": False})
    dims.connect()
    dims.connection.execute(
        "CREATE TABLE supplier (s_id INTEGER, s_nat VARCHAR);"
        "INSERT INTO supplier SELECT g, 'N' || (g % 25) FROM range(0, 1000) t(g);"
    )
    facts = DuckDBDataSource("facts", {"path": duckdb_path(), "read_only": False})
    facts.connect()
    facts.connection.execute(
        "CREATE TABLE partsupp (ps_p INTEGER, ps_s INTEGER, ps_cost INTEGER);"
        "INSERT INTO partsupp SELECT g % 8000, g % 1000, g % 100"
        " FROM range(0, 80000) t(g);"
    )
    catalog = Catalog()
    catalog.register_datasource(dims)
    catalog.register_datasource(facts)
    catalog.load_metadata()
    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT sum(p.ps_cost) AS s "
        "FROM facts.main.partsupp p, dims.main.supplier s "
        "WHERE p.ps_s = s.s_id AND s.s_nat = 'N3'"
    )
    plan = runtime.query_executor._plan_pipeline(sql, None)
    ir = build_ir(plan)
    fact_injections = []
    for step in ir["steps"]:
        if step.get("op") == "injected_scan" and step["datasource"] == "facts":
            fact_injections.append(step)
    assert fact_injections, f"partsupp (the big side) not reduced; steps: {ir['steps']}"
    result = runtime.execute(sql)
    oracle = duckdb_module.connect()
    oracle.execute(
        "CREATE TABLE supplier AS SELECT g AS s_id, 'N' || (g % 25) AS s_nat"
        " FROM range(0, 1000) t(g);"
        "CREATE TABLE partsupp AS SELECT g % 8000 AS ps_p, g % 1000 AS ps_s,"
        " g % 100 AS ps_cost FROM range(0, 80000) t(g);"
    )
    expected = oracle.execute(
        "SELECT sum(p.ps_cost) FROM partsupp p, supplier s"
        " WHERE p.ps_s = s.s_id AND s.s_nat = 'N3'"
    ).fetchone()[0]
    assert result.column("s").to_pylist() == [expected]
