"""Projection column resolution honors the qualifier through the alias map.

PhysicalProjection.schema() and execution resolve a column through the single
_physical_column_name rule, so a qualified reference over a renamed join reads
the intended column - never a bare-name match that, with two same-named columns,
would pick the wrong one.
"""

from typing import Any

import pyarrow as pa

from federated_query.plan.physical import (
    PhysicalPlanNode,
    PhysicalProjection,
    _physical_column_name,
    _column_ref_type,
)
from federated_query.plan.expressions import ColumnRef


class AliasNode(PhysicalPlanNode):
    """Leaf node exposing a fixed schema and (table, column) -> name alias map."""

    out_schema: Any
    aliases: Any

    def children(self):
        """Return no children for leaf node."""
        return []

    def execute(self):
        """Yield a single empty batch with the declared schema."""
        yield pa.RecordBatch.from_pylist([], schema=self.out_schema)

    def schema(self):
        """Return the declared schema."""
        return self.out_schema

    def estimated_cost(self):
        """Return zero cost for test node."""
        return 0.0

    def column_aliases(self):
        """Return the fixed alias map."""
        return self.aliases


def test_physical_column_name_honors_qualifier_over_duplicate():
    """A qualified ref resolves to its mapped physical name, not the bare match."""
    alias_map = {("t1", "id"): "id", ("t2", "id"): "right_id"}
    assert _physical_column_name(ColumnRef(table="t1", column="id"), alias_map) == "id"
    assert (
        _physical_column_name(ColumnRef(table="t2", column="id"), alias_map)
        == "right_id"
    )


def test_physical_column_name_unmapped_qualified_falls_back_to_bare_name():
    """A qualified ref the map does not carry (a flat scan column) uses its name.

    A rename only ever lives in the alias map (a join puts both (t1,id)->id and
    (t2,id)->right_id there), so the fallback is reached only for an unambiguous
    namespace where the bare name is correct.
    """
    assert _physical_column_name(ColumnRef(table="t", column="oid"), {}) == "oid"


def test_physical_column_name_unqualified_uses_bare_name():
    """An unqualified ref uses its own name when the map has no (None, name) entry."""
    assert _physical_column_name(ColumnRef(table=None, column="val"), {}) == "val"


def test_column_ref_type_picks_qualified_columns_type():
    """_column_ref_type types the qualified column, not a same-named sibling."""
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("right_id", pa.string())])
    alias_map = {("t1", "id"): "id", ("t2", "id"): "right_id"}
    typed = _column_ref_type(ColumnRef(table="t2", column="id"), schema, alias_map)
    assert typed == pa.string()


def test_projection_schema_types_qualified_column_through_alias_map():
    """Projecting a renamed right column declares the RIGHT side's type.

    Regression guard: schema() once resolved by bare name (get_field_index("id")
    -> the left ``id``), declaring the wrong type over a renamed join.
    """
    inp = AliasNode(
        out_schema=pa.schema(
            [pa.field("id", pa.int64()), pa.field("right_id", pa.string())]
        ),
        aliases={("t1", "id"): "id", ("t2", "id"): "right_id"},
    )
    projection = PhysicalProjection(
        input=inp,
        expressions=[ColumnRef(table="t2", column="id")],
        output_names=["id"],
    )
    assert projection.schema().field("id").type == pa.string()
