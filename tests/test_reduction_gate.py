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
