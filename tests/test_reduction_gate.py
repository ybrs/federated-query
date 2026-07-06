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
        datasource="duck", schema_name="main", table_name=table,
        columns=list(columns), alias=alias, **kw,
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
        left=left, right=right, join_type=JoinType.LEFT,
        left_keys=[left_key], right_keys=[right_key], build_side="right",
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
        _scan("part", "p", ["p_id"]), _scan("li", "l", ["l_p", "l_q"]),
        _col("p", "p_id"), _col("l", "l_p"),
    )
    assert _can_reduce(join) is True


def test_reducible_base_descends_through_alias_and_projection():
    """The base under alias+projection wrappers is found when the inject column
    survives unchanged to it (the q17 aggregate-subquery shape)."""
    agg = _scan("li", "l", ["g0", "v0"], group_by=[_col("l", "g0")],
                aggregates=[_col("l", "v0")], output_names=["g0", "v0"])
    proj = PhysicalProjection(
        input=agg, expressions=[_col("l", "g0"), _col("l", "v0")],
        output_names=["g0", "v0"],
    )
    aliased = PhysicalAliasedRelation(input=proj, alias="__subq_0")
    assert _reducible_probe_base(aliased, "g0") is agg


def test_reducible_base_refuses_a_renamed_key():
    """If a projection renames the key column, it does not survive to the base
    unchanged, so the probe is NOT reducible (injecting the old name would
    mis-target or fail) - the gate refuses rather than guess."""
    agg = _scan("li", "l", ["gk", "v0"], group_by=[_col("l", "gk")],
                aggregates=[_col("l", "v0")], output_names=["gk", "v0"])
    proj = PhysicalProjection(
        input=agg, expressions=[_col("l", "gk"), _col("l", "v0")],
        output_names=["renamed_key", "v0"],
    )
    assert _reducible_probe_base(proj, "renamed_key") is None


def test_scan_estimated_rows_survives_model_copy():
    """The cost estimate annotation must survive the model_copy derivations
    that later optimizer rules use (a rebuild via Scan.create would drop it -
    this pins that deriving with model_copy keeps it)."""
    from federated_query.plan.logical import Scan as LogicalScan
    scan = LogicalScan.create(
        datasource="duck", schema_name="main", table_name="partsupp",
        columns=["ps_partkey", "ps_suppkey"], alias="ps", estimated_rows=800000,
    )
    derived = scan.model_copy(update={"columns": ["ps_partkey"]})
    assert derived.estimated_rows == 800000


def _est_scan(table, alias, columns, rows, **kw):
    """A plain injectable scan carrying a cost estimate."""
    return _scan(table, alias, columns, estimated_rows=rows, **kw)


def _inner(left, right, left_key, right_key, build="right"):
    """An INNER hash join on a single equi key."""
    return PhysicalHashJoin(
        left=left, right=right, join_type=JoinType.INNER,
        left_keys=[left_key], right_keys=[right_key], build_side=build,
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
        left=big, right=small, join_type=JoinType.SEMI,
        left_keys=[_col("l", "l_p")], right_keys=[_col("p", "p_id")], build_side="right",
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
    from federated_query.plan.logical import Join as LJoin, Scan as LScan, JoinType as LJT
    from federated_query.plan.expressions import BinaryOp as LBinOp, BinaryOpType as LBOp
    big = LScan.create(datasource="d", schema_name="m", table_name="lineitem",
                       columns=["l_k"], alias="l", estimated_rows=6_000_000)
    mid = LScan.create(datasource="d", schema_name="m", table_name="partsupp",
                       columns=["ps_k"], alias="ps", estimated_rows=800_000)
    join = LJoin.create(
        left=big, right=mid, join_type=LJT.INNER,
        condition=LBinOp(op=LBOp.EQ, left=_col("l", "l_k"), right=_col("ps", "ps_k")),
        estimated_rows=3000,  # the under-counted join OUTPUT - must NOT be used
    )
    sizer = SingleSourcePushdown(Catalog())
    assert sizer._root_estimate(join) == 6_000_000


def test_useless_key_reduction_predicate():
    """The pure predicate: keys covering >= 80% of the probe column's domain
    are useless; a filtered build (few rows) or missing statistics are not."""
    from federated_query.optimizer.estimate_defaults import useless_key_reduction
    assert useless_key_reduction(10000, 10000, 10000) is True
    assert useless_key_reduction(10000, 300, 10000) is False
    assert useless_key_reduction(None, 10000, 10000) is False
    assert useless_key_reduction(10000, None, 10000) is True
    assert useless_key_reduction(10000, 10000, None) is False


def test_gate_skips_unfiltered_dimension_keys():
    """q07's shape: an unfiltered dimension's keys are the probe column's whole
    FK domain, so the reduction filters nothing and must be refused."""
    from federated_query.executor.rust_ir import _reduction_filters
    supplier = _est_scan("supplier", "s", ["s_suppkey"], 10000,
                         column_ndv={"s_suppkey": 10000})
    lineitem = _est_scan("lineitem", "l", ["l_suppkey", "l_qty"], 6000000,
                         column_ndv={"l_suppkey": 10000})
    join = _inner(lineitem, supplier, _col("l", "l_suppkey"), _col("s", "s_suppkey"))
    assert _reduction_filters(join) is False


def test_gate_keeps_a_filtered_build_side():
    """q03's shape: a filtered dimension donates far fewer keys than the probe
    column's domain, so the reduction is useful and must be kept."""
    from federated_query.executor.rust_ir import _reduction_filters
    customer = _est_scan("customer", "c", ["c_custkey"], 30142,
                         column_ndv={"c_custkey": 150000})
    orders = _est_scan("orders", "o", ["o_custkey", "o_key"], 1500000,
                       column_ndv={"o_custkey": 100000})
    join = _inner(orders, customer, _col("o", "o_custkey"), _col("c", "c_custkey"))
    assert _reduction_filters(join) is True


def test_gate_abstains_without_statistics():
    """No threaded NDVs on either side: keep today's reduce-by-default."""
    from federated_query.executor.rust_ir import _reduction_filters
    left = _est_scan("part", "p", ["p_id"], 200000)
    right = _est_scan("li", "l", ["l_p", "l_q"], 6000000)
    join = _inner(right, left, _col("l", "l_p"), _col("p", "p_id"))
    assert _reduction_filters(join) is True


def test_gate_abstains_for_a_composite_build_side():
    """q11's shape: a filtered ISLAND build (max-base over-estimate, base-domain
    NDVs) must not be judged - its useful reduction stays."""
    from federated_query.executor.rust_ir import _reduction_filters
    from federated_query.plan.physical import PhysicalRemoteQuery
    island = PhysicalRemoteQuery(
        datasource="pg", datasource_connection=None, query_ast=None,
        output_names=["s_suppkey"],
        column_alias_map={("supplier", "s_suppkey"): "s_suppkey"},
        estimated_rows=10000, column_ndv={"s_suppkey": 10000},
    )
    partsupp = _est_scan("partsupp", "ps", ["ps_suppkey", "ps_partkey"], 800000,
                         column_ndv={"ps_suppkey": 10000})
    join = _inner(partsupp, island,
                  _col("ps", "ps_suppkey"), _col("supplier", "s_suppkey"))
    assert _reduction_filters(join) is True
