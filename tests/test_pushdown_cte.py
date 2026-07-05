"""Tests for pushdown-rule CTE descent and CTE-aware side classification
(the q15 fixes).

Problem A: none of the pushdown rules descended into a CTE, so a WITH body's
filter/aggregate never reached its scan - q15 shipped the entire lineitem
table across the wire twice, unfiltered and unaggregated.

Problem B: available_columns had no CTERef/SubqueryScan/Sort arms, so a
predicate over a CTE reference could not be classified to a join side - the
supplier x revenue join in q15 executed as a conditionless cross product.
"""

from federated_query.optimizer import pushdown
from federated_query.optimizer.rules import (
    AggregatePushdownRule,
    PredicatePushdownRule,
)
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
    FunctionCall,
    Literal,
)
from federated_query.plan.logical import (
    CTE,
    CTERef,
    Aggregate,
    Filter,
    Join,
    JoinType,
    Projection,
    Scan,
    Sort,
    SubqueryScan,
)


def _scan(table="lineitem", alias="lineitem", columns=("l_suppkey", "l_price", "l_ship")):
    """A scan with explicit alias and columns."""
    return Scan(
        datasource="duck", schema_name="main", table_name=table,
        columns=list(columns), alias=alias,
    )


def _col(table, column):
    """A qualified column reference."""
    return ColumnRef(table=table, column=column, data_type=DataType.INTEGER)


def _gt(ref, value):
    """ref > value."""
    return BinaryOp(
        op=BinaryOpType.GT, left=ref,
        right=Literal(value=value, data_type=DataType.INTEGER),
    )


def _revenue_cte(child):
    """CTE 'revenue': filtered aggregate over lineitem, like q15's body."""
    body = Aggregate(
        input=Filter(input=_scan(), predicate=_gt(_col("lineitem", "l_ship"), 10)),
        group_by=[_col("lineitem", "l_suppkey")],
        aggregates=[
            _col("lineitem", "l_suppkey"),
            FunctionCall(function_name="SUM", args=[_col("lineitem", "l_price")],
                         is_aggregate=True),
        ],
        output_names=["supplier_no", "total_revenue"],
    )
    return CTE(name="revenue", cte_plan=body, child=child,
               recursive=False, column_names=None)


def _cte_ref():
    """A bound reference to the revenue CTE."""
    return CTERef(
        name="revenue", alias="revenue",
        columns=["supplier_no", "total_revenue"],
        output_names=["supplier_no", "total_revenue"],
    )


def _scan_of(plan):
    """The single Scan leaf of a plan tree."""
    if isinstance(plan, Scan):
        return plan
    for child in plan.children():
        found = _scan_of(child)
        if found is not None:
            return found
    return None


def test_predicate_pushdown_descends_into_cte_body():
    """The WITH body's filter must reach its scan: q15 shipped 6M unfiltered
    rows because predicate pushdown never entered the CTE."""
    child = Projection(
        input=_cte_ref(),
        expressions=[_col("revenue", "total_revenue")],
        aliases=["total_revenue"],
    )
    tree = _revenue_cte(child)
    result = PredicatePushdownRule().apply(tree)
    assert result is not None
    scan = _scan_of(result.cte_plan)
    assert scan.filters is not None


def test_aggregate_pushdown_descends_into_cte_body():
    """The WITH body's aggregate must fold onto its (filtered) scan so the
    whole body renders as one remote GROUP BY query."""
    child = Projection(
        input=_cte_ref(),
        expressions=[_col("revenue", "total_revenue")],
        aliases=["total_revenue"],
    )
    pushed = PredicatePushdownRule().apply(_revenue_cte(child))
    result = AggregatePushdownRule().apply(pushed)
    assert result is not None
    scan = _scan_of(result.cte_plan)
    assert scan.group_by, "the CTE body's aggregate never folded onto the scan"


def test_available_columns_exposes_cte_ref_outputs():
    """A CTE reference exposes its output names bare and alias-qualified,
    so side classification can place predicates over it."""
    names = pushdown.available_columns(_cte_ref())
    assert "supplier_no" in names
    assert "revenue.supplier_no" in names
    assert "revenue.total_revenue" in names


def test_available_columns_exposes_subquery_scan_outputs():
    """A derived table exposes its subplan's outputs under its alias."""
    derived = SubqueryScan(
        input=Projection(
            input=_scan("t", "t", ("a", "b")),
            expressions=[_col("t", "a")],
            aliases=["a"],
        ),
        alias="dt",
    )
    names = pushdown.available_columns(derived)
    assert "a" in names
    assert "dt.a" in names


def test_available_columns_passes_through_sort():
    """A Sort exposes exactly what its input exposes."""
    sort = Sort(
        input=_scan("t", "t", ("a", "b")),
        sort_keys=[_col("t", "a")], ascending=[True], nulls_order=[None],
    )
    names = pushdown.available_columns(sort)
    assert "t.a" in names
    assert "b" in names


def test_preserved_side_predicate_descends_below_decorrelated_left_join():
    """The q15 shape: Filter(s_suppkey = supplier_no) above a LEFT join whose
    preserved side is supplier JOIN revenue(CTERef). The equality references
    only the preserved side, so it must fold into the INNER join's condition
    below - leaving it above makes supplier x revenue a cross product."""
    supplier = _scan("supplier", "supplier", ("s_suppkey", "s_name"))
    inner = Join(
        left=supplier, right=_cte_ref(), join_type=JoinType.INNER,
        condition=None,
    )
    scalar_side = SubqueryScan(
        input=Projection(
            input=_scan("x", "x", ("v",)),
            expressions=[_col("x", "v")],
            aliases=["__subq_0_v0"],
        ),
        alias="__subq_0",
    )
    left_join = Join(
        left=inner, right=scalar_side, join_type=JoinType.LEFT,
        condition=Literal(value=True, data_type=DataType.BOOLEAN),
    )
    equality = BinaryOp(
        op=BinaryOpType.EQ,
        left=_col("supplier", "s_suppkey"),
        right=_col("revenue", "supplier_no"),
    )
    tree = Filter(input=left_join, predicate=equality)
    result = PredicatePushdownRule().apply(tree)
    assert result is not None
    folded = _inner_join_of(result)
    assert folded is not None
    assert folded.condition is not None


def _inner_join_of(plan):
    """The INNER join node of a tree, if any."""
    if isinstance(plan, Join) and plan.join_type == JoinType.INNER:
        return plan
    for child in plan.children():
        found = _inner_join_of(child)
        if found is not None:
            return found
    return None


def test_q15_shape_explains_and_pushes_body_remote():
    """End to end on the q15 shape across two sources: EXPLAIN must not
    crash (the dynamic-filter mark used to accept a CTEScan build side the
    prefetch cannot execute), the CTE body's filter AND aggregate must render
    into the remote SQL, and the join must not be a conditionless cross."""
    import duckdb as duckdb_module
    from federated_query.catalog import Catalog
    from federated_query.cli.fedq import FedQRuntime
    from federated_query.config import Config
    from federated_query.datasources.duckdb import DuckDBDataSource
    from tests.duckdb_tmp import duckdb_path

    facts = DuckDBDataSource("facts", {"path": duckdb_path(), "read_only": False})
    facts.connect()
    facts.connection.execute(
        "CREATE TABLE lineitem (l_suppkey INTEGER, l_price DOUBLE, l_ship INTEGER);"
        "INSERT INTO lineitem SELECT g % 10, g * 1.5, g % 100 FROM range(0, 1000) t(g);"
    )
    dims = DuckDBDataSource("dims", {"path": duckdb_path(), "read_only": False})
    dims.connect()
    dims.connection.execute(
        "CREATE TABLE supplier (s_suppkey INTEGER, s_name VARCHAR);"
        "INSERT INTO supplier SELECT g, 'S' || g FROM range(0, 10) t(g);"
    )
    catalog = Catalog()
    catalog.register_datasource(facts)
    catalog.register_datasource(dims)
    catalog.load_metadata()
    runtime = FedQRuntime(catalog, Config())
    sql = (
        "WITH revenue AS ("
        " SELECT l_suppkey AS supplier_no, sum(l_price) AS total_revenue"
        " FROM facts.main.lineitem WHERE l_ship < 50 GROUP BY l_suppkey)"
        " SELECT s.s_suppkey, s.s_name, r.total_revenue"
        " FROM dims.main.supplier s, revenue r"
        " WHERE s.s_suppkey = r.supplier_no"
        " AND r.total_revenue = (SELECT max(total_revenue) FROM revenue)"
        " ORDER BY s.s_suppkey"
    )
    document = runtime.explain(sql)
    plan_text = "\n".join(document["plan"])
    # The supplier-revenue join must be a HASH join, not a nested loop.
    assert "PhysicalHashJoin" in plan_text
    facts_sql = []
    for entry in document["queries"]:
        if entry["datasource_name"] == "facts":
            facts_sql.append(str(entry["query"]))
    assert facts_sql, "no remote query reached the facts source"
    for remote in facts_sql:
        assert "WHERE" in remote.upper(), f"CTE body filter not pushed: {remote}"
        assert "GROUP BY" in remote.upper(), f"CTE body aggregate not pushed: {remote}"
    # And the whole thing computes the right answer.
    result = runtime.execute(sql)
    oracle = duckdb_module.connect()
    oracle.execute(
        "CREATE TABLE lineitem AS SELECT g % 10 AS l_suppkey, g * 1.5 AS l_price,"
        " g % 100 AS l_ship FROM range(0, 1000) t(g);"
        "CREATE TABLE supplier AS SELECT g AS s_suppkey, 'S' || g AS s_name"
        " FROM range(0, 10) t(g);"
    )
    expected = oracle.execute(
        "WITH revenue AS (SELECT l_suppkey AS supplier_no, sum(l_price) AS"
        " total_revenue FROM lineitem WHERE l_ship < 50 GROUP BY l_suppkey)"
        " SELECT s.s_suppkey, s.s_name, r.total_revenue FROM supplier s, revenue r"
        " WHERE s.s_suppkey = r.supplier_no AND r.total_revenue ="
        " (SELECT max(total_revenue) FROM revenue) ORDER BY s.s_suppkey"
    ).fetchall()
    got = []
    for index in range(result.num_rows):
        row = []
        for column in result.columns:
            row.append(column[index].as_py())
        got.append(tuple(row))
    assert got == expected


def test_aggregate_pushdown_descends_below_an_unfoldable_aggregate():
    """A HAVING subquery under the main query's aggregate: the OUTER
    aggregate cannot fold (its input is a join), but the INNER aggregate
    over a scan must still fold - the walker used to stop dead at the outer
    one, leaving q18's subquery to aggregate 6M rows on the coordinator."""
    inner_agg = Aggregate(
        input=_scan("lineitem", "l2", ("l_o", "l_q")),
        group_by=[_col("l2", "l_o")],
        aggregates=[
            _col("l2", "l_o"),
            FunctionCall(function_name="SUM", args=[_col("l2", "l_q")],
                         is_aggregate=True),
        ],
        output_names=["l_o", "total"],
    )
    subquery = SubqueryScan(
        input=Projection(
            input=Filter(input=inner_agg, predicate=_gt(_col("l2", "total"), 300)),
            expressions=[_col("l2", "l_o")],
            aliases=["k"],
        ),
        alias="__subq_0",
    )
    join = Join(
        left=_scan("orders", "o", ("o_id", "o_v")),
        right=subquery,
        join_type=JoinType.SEMI,
        condition=BinaryOp(op=BinaryOpType.EQ, left=_col("o", "o_id"),
                           right=_col("__subq_0", "k")),
    )
    outer = Aggregate(
        input=join,
        group_by=[_col("o", "o_v")],
        aggregates=[_col("o", "o_v")],
        output_names=["o_v"],
    )
    result = AggregatePushdownRule().apply(outer)
    assert result is not None
    folded = _scan_of(result.input.right)
    assert folded.group_by, "the inner aggregate never folded onto its scan"
