"""Column pruning descends through a Sort, keeping its sort-key columns.

ProjectionPushdownRule must handle Sort in both required-column collection and
pruning; handling it in only one makes them inconsistent. A scan beneath a Sort
is pruned to the columns actually used, and the sort keys are always retained so
the ORDER BY does not lose the column it sorts by.
"""

from federated_query.optimizer.rules import ProjectionPushdownRule
from federated_query.plan.logical import Scan, Projection, Sort
from federated_query.plan.expressions import ColumnRef, DataType


def _scan():
    """A scan exposing three columns a, b, c."""
    return Scan(
        datasource="ds",
        schema_name="public",
        table_name="t",
        columns=["a", "b", "c"],
    )


def test_prune_keeps_sort_key_and_projected_columns_below_sort():
    """Projection(a) over Sort(by b) over Scan(a,b,c) prunes the scan to {a, b}."""
    plan = Projection(
        input=Sort(
            input=_scan(),
            sort_keys=[ColumnRef(table="t", column="b", data_type=DataType.INTEGER)],
            ascending=[True],
            nulls_order=[None],
        ),
        expressions=[ColumnRef(table="t", column="a", data_type=DataType.INTEGER)],
        aliases=["a"],
    )

    result = ProjectionPushdownRule().apply(plan)

    scan = result.input.input
    assert isinstance(scan, Scan)
    # 'c' is unused and pruned; 'a' (projected) and 'b' (sort key) are kept.
    assert set(scan.columns) == {"a", "b"}
