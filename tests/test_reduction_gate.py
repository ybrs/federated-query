"""Unit tests for the dynamic-filter reduction gate (rust_ir).

These pin the safety-critical decisions of _can_reduce / _orient_join /
_reducible_probe_base for LEFT joins, where the reduction machinery was
extended to reduce a NULLABLE right side (including an aggregate subquery
probe) by the PRESERVED left side's keys. The dangerous mistakes - reducing
the preserved side, or injecting on a key a projection renamed - must be
refused.
"""

import pyarrow as pa

from federated_query.executor.rust_ir import (
    _can_reduce,
    _orient_join,
    _probe_base_resolvable,
    _reducible_probe_base,
)
from federated_query.plan.expressions import ColumnRef, DataType
from federated_query.plan.logical import JoinType
from federated_query.plan.physical import (
    PhysicalAliasedRelation,
    PhysicalHashJoin,
    PhysicalProjection,
    PhysicalScan,
)


def _scan(table, alias, columns, **kw):
    """A qualified physical scan (optionally an aggregate scan) with its output
    schema seeded, so column_aliases() needs no live connection."""
    scan = PhysicalScan(
        datasource="duck",
        schema_name="main",
        table_name=table,
        columns=list(columns),
        alias=alias,
        **kw,
    )
    fields = []
    for name in columns:
        fields.append((name, pa.int32()))
    object.__setattr__(scan, "_schema", pa.schema(fields))
    return scan


def _col(table, column):
    """A qualified column reference."""
    return ColumnRef(table=table, column=column, data_type=DataType.INTEGER)


def _left_join(left, right, left_key, right_key):
    """A LEFT hash join on a single equi key, build side right (the nullable)."""
    return PhysicalHashJoin(
        left=left,
        right=right,
        join_type=JoinType.LEFT,
        left_keys=[left_key],
        right_keys=[right_key],
        build_side="right",
    )


def test_left_join_reduces_the_nullable_right_not_the_preserved_left():
    """_orient_join must make the preserved left the key donor and the nullable
    right the probe - never the reverse (that would drop preserved rows)."""
    left = _scan("part", "p", ["p_id"])
    right = _scan("li", "l", ["l_p", "l_q"])
    join = _left_join(left, right, _col("p", "p_id"), _col("l", "l_p"))
    build, probe, build_key, probe_key = _orient_join(join)
    assert build is left, "the preserved left side must donate keys"
    assert probe is right, "the nullable right side must be the reduced probe"
    assert build_key is join.left_keys[0] and probe_key is join.right_keys[0]


def test_left_join_over_a_plain_scan_probe_is_reducible():
    """A LEFT join whose right is a plain injectable scan reduces."""
    join = _left_join(
        _scan("part", "p", ["p_id"]),
        _scan("li", "l", ["l_p", "l_q"]),
        _col("p", "p_id"),
        _col("l", "l_p"),
    )
    assert _can_reduce(join) is True


def test_reducible_base_descends_through_alias_and_projection():
    """The base under alias+projection wrappers is found when the inject column
    survives unchanged to it (the q17 aggregate-subquery shape)."""
    agg = _scan(
        "li",
        "l",
        ["g0", "v0"],
        group_by=[_col("l", "g0")],
        aggregates=[_col("l", "v0")],
        output_names=["g0", "v0"],
    )
    proj = PhysicalProjection(
        input=agg,
        expressions=[_col("l", "g0"), _col("l", "v0")],
        output_names=["g0", "v0"],
    )
    aliased = PhysicalAliasedRelation(input=proj, alias="__subq_0")
    assert _reducible_probe_base(aliased, "g0") is agg


def test_reducible_base_refuses_a_renamed_key():
    """If a projection renames the key column, it does not survive to the base
    unchanged, so the probe is NOT reducible (injecting the old name would
    mis-target or fail) - the gate refuses rather than guess."""
    agg = _scan(
        "li",
        "l",
        ["gk", "v0"],
        group_by=[_col("l", "gk")],
        aggregates=[_col("l", "v0")],
        output_names=["gk", "v0"],
    )
    proj = PhysicalProjection(
        input=agg,
        expressions=[_col("l", "gk"), _col("l", "v0")],
        output_names=["renamed_key", "v0"],
    )
    assert _reducible_probe_base(proj, "renamed_key") is None


def test_scan_estimated_rows_survives_model_copy():
    """The cost estimate annotation must survive the model_copy derivations
    that later optimizer rules use (a rebuild via Scan.create would drop it -
    this pins that deriving with model_copy keeps it)."""
    from federated_query.plan.logical import Scan as LogicalScan

    scan = LogicalScan.create(
        datasource="duck",
        schema_name="main",
        table_name="partsupp",
        columns=["ps_partkey", "ps_suppkey"],
        alias="ps",
        estimated_rows=800000,
    )
    derived = scan.model_copy(update={"columns": ["ps_partkey"]})
    assert derived.estimated_rows == 800000


def _est_scan(table, alias, columns, rows, **kw):
    """A plain injectable scan carrying a cost estimate."""
    return _scan(table, alias, columns, estimated_rows=rows, **kw)


def _inner(left, right, left_key, right_key, build="right"):
    """An INNER hash join on a single equi key."""
    return PhysicalHashJoin(
        left=left,
        right=right,
        join_type=JoinType.INNER,
        left_keys=[left_key],
        right_keys=[right_key],
        build_side=build,
    )


def test_inner_reduces_the_larger_estimated_side():
    """The q11 fix: the bigger injectable side (partsupp 800k) becomes the
    probe (reduced), the smaller (supplier 400) donates keys - regardless of
    the structural _probe_preference that used to pick the small remote."""
    from federated_query.executor.rust_ir import _cardinality_probe

    big = _est_scan("partsupp", "ps", ["ps_partkey", "ps_suppkey"], 800000)
    small = _est_scan("supplier", "s", ["s_suppkey"], 400)
    # small on the left, big on the right (the shape _probe_preference got wrong)
    join = _inner(small, big, _col("s", "s_suppkey"), _col("ps", "ps_suppkey"))
    assert _cardinality_probe(join) is big
    build, probe, _, _ = _orient_join(join)
    assert probe is big and build is small


def test_inner_tie_falls_back_to_structural_choice():
    """Equal estimates (both defaulted) must NOT pick a side by cardinality -
    the cardinality path returns None so the existing heuristic runs."""
    from federated_query.executor.rust_ir import _cardinality_probe

    a = _est_scan("a", "a", ["k"], 1000)
    b = _est_scan("b", "b", ["k"], 1000)
    join = _inner(a, b, _col("a", "k"), _col("b", "k"))
    assert _cardinality_probe(join) is None


def test_inner_missing_estimate_falls_back():
    """When one side has no estimate the cardinality path declines."""
    from federated_query.executor.rust_ir import _cardinality_probe

    a = _est_scan("a", "a", ["k"], 1000)
    b = _scan("b", "b", ["k"])  # no estimated_rows
    join = _inner(a, b, _col("a", "k"), _col("b", "k"))
    assert _cardinality_probe(join) is None


def test_semi_and_left_orientation_unchanged():
    """The cardinality branch is INNER-only; SEMI/LEFT keep fixed roles."""
    big = _est_scan("li", "l", ["l_p", "l_q"], 6000000)
    small = _est_scan("part", "p", ["p_id"], 200)
    semi = PhysicalHashJoin(
        left=big,
        right=small,
        join_type=JoinType.SEMI,
        left_keys=[_col("l", "l_p")],
        right_keys=[_col("p", "p_id")],
        build_side="right",
    )
    build, probe, _, _ = _orient_join(semi)
    assert probe is big and build is small  # SEMI: preserved left is the probe


def test_remote_orientation_size_is_max_base_not_join_output():
    """A collapsed remote's orientation size must be its LARGEST base scan,
    not the join OUTPUT estimate - a multi-join output under-counts via
    composite-key correlation, which would make a fact island look tiny and
    wrongly lose the reduction to a dim (the q09 regression). Here a fact
    island (6M scan JOIN 800k scan, output annotated a bogus-small 3000) must
    orient-size as 6,000,000."""
    from federated_query.optimizer.single_source_pushdown import SingleSourcePushdown
    from federated_query.catalog.catalog import Catalog
    from federated_query.plan.logical import (
        Join as LJoin,
        Scan as LScan,
        JoinType as LJT,
    )
    from federated_query.plan.expressions import (
        BinaryOp as LBinOp,
        BinaryOpType as LBOp,
    )

    big = LScan.create(
        datasource="d",
        schema_name="m",
        table_name="lineitem",
        columns=["l_k"],
        alias="l",
        estimated_rows=6_000_000,
    )
    mid = LScan.create(
        datasource="d",
        schema_name="m",
        table_name="partsupp",
        columns=["ps_k"],
        alias="ps",
        estimated_rows=800_000,
    )
    join = LJoin.create(
        left=big,
        right=mid,
        join_type=LJT.INNER,
        condition=LBinOp(op=LBOp.EQ, left=_col("l", "l_k"), right=_col("ps", "ps_k")),
        estimated_rows=3000,  # the under-counted join OUTPUT - must NOT be used
    )
    sizer = SingleSourcePushdown(Catalog())
    assert sizer._root_estimate(join) == 6_000_000


def test_useless_key_reduction_predicate():
    """The pure predicate: expected keys covering >= 80% of the wider value
    domain are useless; a filtered build (few expected keys), a build domain
    much narrower than the probe's (dangling FKs), or an unknown build NDV
    are not. An unknown probe NDV falls back to build-domain coverage."""
    from federated_query.optimizer.estimate_defaults import useless_key_reduction

    assert useless_key_reduction(10000, 10000, 10000) is True
    assert useless_key_reduction(10000, 300, 10000) is False
    assert useless_key_reduction(None, 10000, 10000) is False
    assert useless_key_reduction(10000, None, 10000) is True
    assert useless_key_reduction(10000, 10000, None) is True
    # A full build domain that is much NARROWER than the probe domain still
    # filters (probe values outside it drop): not useless.
    assert useless_key_reduction(100, 100, 1000) is False
    # A filtered build donating 7 of its 10 domain values keeps ~70%: useful.
    assert useless_key_reduction(10, 7, 4) is False


def test_gate_skips_unfiltered_dimension_keys():
    """q07's shape: an unfiltered dimension's keys are the probe column's whole
    FK domain, so the reduction filters nothing and must be refused."""
    from federated_query.executor.rust_ir import _reduction_filters

    supplier = _est_scan(
        "supplier", "s", ["s_suppkey"], 10000, column_ndv={"s_suppkey": 10000}
    )
    lineitem = _est_scan(
        "lineitem",
        "l",
        ["l_suppkey", "l_qty"],
        6000000,
        column_ndv={"l_suppkey": 10000},
    )
    join = _inner(lineitem, supplier, _col("l", "l_suppkey"), _col("s", "s_suppkey"))
    assert _reduction_filters(join) is False


def test_gate_keeps_a_filtered_build_side():
    """q03's shape: a filtered dimension donates far fewer keys than the probe
    column's domain, so the reduction is useful and must be kept. The scan
    carries its pushed filter, as the planner produces it."""
    from federated_query.executor.rust_ir import _reduction_filters

    customer = _est_scan(
        "customer",
        "c",
        ["c_custkey"],
        30142,
        column_ndv={"c_custkey": 150000},
        filters=_col("c", "c_custkey"),
    )
    orders = _est_scan(
        "orders", "o", ["o_custkey", "o_key"], 1500000, column_ndv={"o_custkey": 100000}
    )
    join = _inner(orders, customer, _col("o", "o_custkey"), _col("c", "c_custkey"))
    assert _reduction_filters(join) is True


def test_gate_refuses_an_unfiltered_plain_build():
    """An UNFILTERED plain build scan donates its whole key domain - under FK
    containment the injection keeps every probe row - so the gate refuses
    STRUCTURALLY, no statistics needed (q39's whole-warehouse and q06's
    every-item injections)."""
    from federated_query.executor.rust_ir import _reduction_filters

    left = _est_scan("part", "p", ["p_id"], 200000)
    right = _est_scan("li", "l", ["l_p", "l_q"], 6000000)
    join = _inner(right, left, _col("l", "l_p"), _col("p", "p_id"))
    assert _reduction_filters(join) is False


def test_gate_abstains_for_a_composite_build_side():
    """q11's shape: a filtered ISLAND build (max-base over-estimate, base-domain
    NDVs) must not be judged - its useful reduction stays."""
    from federated_query.executor.rust_ir import _reduction_filters
    from federated_query.plan.physical import PhysicalRemoteQuery

    island = PhysicalRemoteQuery(
        datasource="pg",
        datasource_connection=None,
        query_ast=None,
        output_names=["s_suppkey"],
        column_alias_map={("supplier", "s_suppkey"): "s_suppkey"},
        estimated_rows=10000,
        column_ndv={"s_suppkey": 10000},
    )
    partsupp = _est_scan(
        "partsupp",
        "ps",
        ["ps_suppkey", "ps_partkey"],
        800000,
        column_ndv={"ps_suppkey": 10000},
    )
    join = _inner(
        partsupp, island, _col("ps", "ps_suppkey"), _col("supplier", "s_suppkey")
    )
    assert _reduction_filters(join) is True


def _island(output_est, est=10000):
    """A pg supplier island with base-domain NDV and an optional real output
    estimate (estimated_rows stays the max-base floor)."""
    from federated_query.plan.physical import PhysicalRemoteQuery

    return PhysicalRemoteQuery(
        datasource="pg",
        datasource_connection=None,
        query_ast=None,
        output_names=["s_suppkey"],
        column_alias_map={("supplier", "s_suppkey"): "s_suppkey"},
        estimated_rows=est,
        output_estimated_rows=output_est,
        column_ndv={"s_suppkey": 10000},
    )


def test_gate_judges_a_remote_build_by_its_output_estimate():
    """q11's shape resolved: a filtered island whose real output estimate is
    400 donates ~400 keys - useful against a 10k-NDV probe column. The same
    island estimated to return the whole domain is refused."""
    from federated_query.executor.rust_ir import _reduction_filters

    partsupp = _est_scan(
        "partsupp",
        "ps",
        ["ps_suppkey", "ps_partkey"],
        800000,
        column_ndv={"ps_suppkey": 10000},
    )

    def join_with(island):
        return _inner(
            partsupp, island, _col("ps", "ps_suppkey"), _col("supplier", "s_suppkey")
        )

    assert _reduction_filters(join_with(_island(output_est=400))) is True
    assert _reduction_filters(join_with(_island(output_est=10000))) is False


def test_orientation_prefers_the_remote_output_estimate():
    """larger_estimated_side sizes a remote by its real output estimate when
    present; the max-base estimated_rows is only the fallback floor."""
    from federated_query.optimizer.estimate_defaults import larger_estimated_side

    scan = _est_scan("customer", "c", ["c_custkey"], 150000)
    small_island = _island(output_est=400, est=6000000)
    assert larger_estimated_side(scan, small_island) is scan
    unestimated_island = _island(output_est=None, est=6000000)
    assert larger_estimated_side(scan, unestimated_island) is unestimated_island


def _remote_with_ast(sql, alias_map):
    """A remote island carrying a parsed AST and its column alias map."""
    import sqlglot
    from federated_query.plan.physical import PhysicalRemoteQuery
    from federated_query.datasources.duckdb import DuckDBDataSource

    return PhysicalRemoteQuery(
        datasource="duck",
        datasource_connection=DuckDBDataSource("duck", {"path": ":memory:"}),
        query_ast=sqlglot.parse_one(sql, dialect="postgres"),
        output_names=["o_custkey"],
        column_alias_map=alias_map,
    )


def test_island_injected_sql_lands_on_the_owning_relation():
    """The key filter goes INSIDE the island, qualified by the owning alias,
    reading the build key from the temp table (the wrapper form is not pushed
    down by sources - q03 measured 65ms wrapped vs 21ms inside)."""
    from federated_query.executor.rust_ir import _island_injected_sql

    island = _remote_with_ast(
        'SELECT "orders"."o_custkey" AS "o_custkey" FROM "main"."orders" AS "orders" '
        'JOIN "main"."lineitem" AS "lineitem" ON "lineitem"."l_orderkey" = "orders"."o_orderkey" '
        'WHERE "orders"."o_orderdate" < CAST(\'1995-03-15\' AS DATE)',
        {("orders", "o_custkey"): "o_custkey"},
    )
    sql = _island_injected_sql(island, "o_custkey", "c_custkey")
    assert sql is not None
    assert '"orders"."o_custkey" IN (SELECT "c_custkey" FROM fedq_dyn_keys)' in sql
    assert "o_orderdate" in sql


def test_island_injection_declines_unsafe_shapes():
    """No owner / ambiguous owner / LIMIT / window: fall back to the wrapper
    rather than change what the island returns."""
    from federated_query.executor.rust_ir import _island_injected_sql

    computed = _remote_with_ast(
        'SELECT "o_custkey" FROM "main"."orders" AS "orders"',
        {(None, "o_custkey"): "o_custkey"},
    )
    assert _island_injected_sql(computed, "o_custkey", "k") is None
    limited = _remote_with_ast(
        'SELECT "orders"."o_custkey" AS "o_custkey" FROM "main"."orders" AS "orders" LIMIT 5',
        {("orders", "o_custkey"): "o_custkey"},
    )
    assert _island_injected_sql(limited, "o_custkey", "k") is None
    windowed = _remote_with_ast(
        'SELECT "orders"."o_custkey" AS "o_custkey", '
        'ROW_NUMBER() OVER (ORDER BY "orders"."o_custkey") AS "rn" '
        'FROM "main"."orders" AS "orders"',
        {("orders", "o_custkey"): "o_custkey"},
    )
    assert _island_injected_sql(windowed, "o_custkey", "k") is None


def _agg_scan(table, alias, group_col, agg_out, group_out):
    """An aggregate PhysicalScan: AVG over `agg_out` GROUP BY `group_col`,
    the decorrelated correlated-average subquery shape (q17)."""
    from federated_query.plan.expressions import FunctionCall
    from federated_query.datasources.duckdb import DuckDBDataSource

    group_ref = _col(alias, group_col)
    agg = FunctionCall(
        function_name="AVG", args=[_col(alias, agg_out)], is_aggregate=True
    )
    scan = PhysicalScan(
        datasource="duck",
        schema_name="main",
        table_name=table,
        columns=[group_col, agg_out],
        alias=alias,
        group_by=[group_ref],
        aggregates=[agg, group_ref],
        output_names=["v0", group_out],
        datasource_connection=DuckDBDataSource("duck", {"path": ":memory:"}),
    )
    fields = [("v0", pa.float64()), (group_out, pa.int32())]
    object.__setattr__(scan, "_schema", pa.schema(fields))
    return scan


def test_aggregate_base_key_filter_goes_inside_the_group_by():
    """q17: the key filter lands in the aggregate scan's WHERE (before GROUP
    BY, on the base group column), not around its output - so the source
    scans only the reduced input (measured SF10: 1508ms wrapped vs 93ms)."""
    from federated_query.executor.rust_ir import _aggregate_injected_sql

    agg = _agg_scan("lineitem", "l", "l_partkey", "l_quantity", "__subq_0_g0")
    sql = _aggregate_injected_sql(agg, "__subq_0_g0", "p_partkey")
    assert sql is not None
    normalized = " ".join(sql.split())
    assert (
        'WHERE "l"."l_partkey" IN (SELECT "p_partkey" FROM fedq_dyn_keys)' in normalized
    )
    assert normalized.index("WHERE") < normalized.index("GROUP BY")


def test_aggregate_injection_declines_a_non_group_key_output():
    """The inject column must be a plain GROUP KEY; an aggregate output (the
    AVG) is not one, and filtering it would change the result - decline."""
    from federated_query.executor.rust_ir import _aggregate_injected_sql

    agg = _agg_scan("lineitem", "l", "l_partkey", "l_quantity", "__subq_0_g0")
    assert _aggregate_injected_sql(agg, "v0", "p_partkey") is None


def test_aggregate_injection_declines_grouping_sets():
    """GROUPING SETS: a filtered input changes which sets a row feeds - decline."""
    from federated_query.executor.rust_ir import _aggregate_injected_sql

    agg = _agg_scan("lineitem", "l", "l_partkey", "l_quantity", "__subq_0_g0")
    agg = agg.model_copy(update={"grouping_sets": [[_col("l", "l_partkey")], []]})
    assert _aggregate_injected_sql(agg, "__subq_0_g0", "p_partkey") is None


def test_join_subtree_is_not_an_injectable_base():
    """A join subtree has no single scan/remote base, so _reducible_probe_base
    returns None - there is nowhere to inject a dynamic filter (q96: a fact
    already joined to a dim)."""
    fact = _scan("store_sales", "ss", ["ss_hdemo_sk", "ss_store_sk"])
    dim = _scan("household_demographics", "hd", ["hd_demo_sk"])
    joined = _inner(fact, dim, _col("ss", "ss_hdemo_sk"), _col("hd", "hd_demo_sk"))
    assert _reducible_probe_base(joined, "ss_store_sk") is None


def test_probe_base_resolvable_traces_through_a_join_probe():
    """A join-subtree probe whose key column passes through a PRESERVED side
    now resolves: the keys inject into the base scan that originates the
    column (the SF10 outlier-cluster fix - previously the whole fact
    shipped because a composite probe was never injectable)."""
    left = _scan("store", "s", ["s_store_sk"])
    r1 = _scan("store_sales", "ss", ["ss_store_sk", "ss_hdemo_sk"])
    r2 = _scan("household_demographics", "hd", ["hd_demo_sk"])
    right_join = _inner(r1, r2, _col("ss", "ss_hdemo_sk"), _col("hd", "hd_demo_sk"))
    top = _left_join(
        left, right_join, _col("s", "s_store_sk"), _col("ss", "ss_store_sk")
    )
    assert _probe_base_resolvable(top) is True


def test_probe_base_resolvable_refuses_a_null_extended_key_path():
    """When the probe key only traces through a NULL-EXTENDED join side, the
    gate refuses: removing that side's rows would turn matches into NULL
    extensions, changing kept rows - so the emitter must never inject there
    (and never dereferences a None base, the original q96 crash)."""
    left = _scan("store", "s", ["s_store_sk"])
    r1 = _scan("household_demographics", "hd", ["hd_demo_sk"])
    r2 = _scan("store_sales", "ss", ["ss_store_sk", "ss_hdemo_sk"])
    # ss sits on the NULLABLE right of the inner left join, so its
    # ss_store_sk key column is unsafe to trace for injection.
    right_join = _left_join(r1, r2, _col("hd", "hd_demo_sk"), _col("ss", "ss_hdemo_sk"))
    top = _left_join(
        left, right_join, _col("s", "s_store_sk"), _col("ss", "ss_store_sk")
    )
    assert _probe_base_resolvable(top) is False


def test_probe_base_resolvable_allows_a_plain_scan_probe():
    """Two plain scans: the oriented probe is a scan whose key is at its base,
    so the gate allows the reduction."""
    dim = _scan("household_demographics", "hd", ["hd_demo_sk"])
    fact = _scan("store_sales", "ss", ["ss_hdemo_sk", "ss_store_sk"])
    join = _inner(fact, dim, _col("ss", "ss_hdemo_sk"), _col("hd", "hd_demo_sk"))
    assert _probe_base_resolvable(join) is True
