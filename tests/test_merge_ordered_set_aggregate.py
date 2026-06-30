"""Merge-engine ordered-set aggregates render and execute as DuckDB WITHIN GROUP.

The aggregate select list lowers through the one emitter, except an ordered-set
aggregate's WITHIN GROUP form: sqlglot's DuckDB dialect transpiles a typed
ordered-set call to an inline ``f(arg ORDER BY key)`` that DuckDB rejects, so the
literal ``WITHIN GROUP (ORDER BY key)`` is built explicitly. This pins that the
merge path keeps the literal form (and that DuckDB accepts it).
"""

import duckdb
import pyarrow as pa

from federated_query.plan.physical import PhysicalHashAggregate, PhysicalValues
from federated_query.plan.expressions import (
    FunctionCall,
    ColumnRef,
    Literal,
    DataType,
)


def _percentile_aggregate(desc: bool) -> PhysicalHashAggregate:
    """A PhysicalHashAggregate computing PERCENTILE_CONT WITHIN GROUP (ORDER BY x)."""
    dummy = PhysicalValues(
        rows=[[Literal(value=1, data_type=DataType.INTEGER)]], output_names=["x"]
    )
    func = FunctionCall(
        function_name="PERCENTILE_CONT",
        args=[Literal(value=0.5, data_type=DataType.DOUBLE)],
        is_aggregate=True,
        within_group_key=ColumnRef(table=None, column="x"),
        within_group_desc=desc,
    )
    return PhysicalHashAggregate(
        input=dummy, group_by=[], aggregates=[func], output_names=["p"]
    )


def test_ordered_set_aggregate_renders_within_group_form():
    """The rendered SQL keeps the literal WITHIN GROUP form, not the inline one."""
    rendered = _percentile_aggregate(desc=False)._render_agg_expr(
        _percentile_aggregate(desc=False).aggregates[0], {}
    )
    assert "WITHIN GROUP (ORDER BY" in rendered
    assert rendered == 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "x")'


def test_ordered_set_aggregate_desc_renders_direction():
    """A DESC ordered-set aggregate keeps DESC inside the WITHIN GROUP order."""
    aggregate = _percentile_aggregate(desc=True)
    rendered = aggregate._render_agg_expr(aggregate.aggregates[0], {})
    assert rendered == 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "x" DESC)'


def test_rendered_within_group_executes_in_duckdb():
    """DuckDB accepts the rendered WITHIN GROUP SQL (the inline form would not)."""
    aggregate = _percentile_aggregate(desc=False)
    rendered = aggregate._render_agg_expr(aggregate.aggregates[0], {})
    con = duckdb.connect()
    con.register("t", pa.table({"x": [1.0, 2.0, 3.0, 4.0]}))
    result = con.execute(f"SELECT {rendered} FROM t").fetchall()
    assert result == [(2.5,)]
