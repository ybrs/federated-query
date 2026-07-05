"""Tests for the ProjectionPushdownRule gate (PP-M1).

The gate decides whether a plan carries an explicit output schema (so pruning
is safe) or is a programmatic plan with no projection (SELECT-everything
semantics; never prune). It must see through every pass-through node - the
Sort omission made the rule a no-op on all 22 TPC-H queries (every one has
ORDER BY at the root).
"""

from federated_query.optimizer.rules import ProjectionPushdownRule
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
    Literal,
)
from federated_query.plan.logical import (
    CTE,
    Filter,
    Limit,
    Projection,
    Scan,
    SetOperation,
    SetOpKind,
    Sort,
)


def _scan(columns=("a", "b", "c")):
    """A three-column scan to prune."""
    return Scan(
        datasource="ds", schema_name="s", table_name="t",
        columns=list(columns), alias="t",
    )


def _col(column):
    """A t-qualified column reference."""
    return ColumnRef(table="t", column=column, data_type=DataType.INTEGER)


def _projection(scan, columns=("a",)):
    """A projection of the named columns over a scan."""
    expressions = []
    for column in columns:
        expressions.append(_col(column))
    return Projection(input=scan, expressions=expressions, aliases=list(columns))


def _scan_of(plan):
    """The single Scan leaf of a plan tree."""
    if isinstance(plan, Scan):
        return plan
    for child in plan.children():
        found = _scan_of(child)
        if found is not None:
            return found
    return None


def test_limit_sort_projection_prunes():
    """Limit(Sort(Projection(Scan))) - the shape of every TPC-H query with
    ORDER BY + LIMIT - must prune the scan to the referenced columns."""
    tree = Limit(
        input=Sort(
            input=_projection(_scan(), ("a",)),
            sort_keys=[_col("a")], ascending=[True], nulls_order=[None],
        ),
        limit=10,
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    assert _scan_of(result).columns == ["a"]


def test_sort_over_projection_prunes():
    """Sort(Projection(Scan)) - plain ORDER BY at the root - prunes, keeping
    the sort key alongside the projected column."""
    tree = Sort(
        input=_projection(_scan(), ("a",)),
        sort_keys=[_col("b")], ascending=[True], nulls_order=[None],
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    assert _scan_of(result).columns == ["a", "b"]


def test_sort_filter_scan_without_projection_stays_full_width():
    """A programmatic plan with no projection anywhere keeps every column:
    the gate's SELECT-everything protection."""
    predicate = BinaryOp(
        op=BinaryOpType.GT, left=_col("a"),
        right=Literal(value=1, data_type=DataType.INTEGER),
    )
    tree = Sort(
        input=Filter(input=_scan(), predicate=predicate),
        sort_keys=[_col("b")], ascending=[True], nulls_order=[None],
    )
    result = ProjectionPushdownRule().apply(tree)
    pruned = _scan_of(result) if result is not None else _scan_of(tree)
    assert pruned.columns == ["a", "b", "c"]


def _named_scan(table, alias, columns):
    """A scan of a named table under an alias."""
    return Scan(
        datasource="ds", schema_name="s", table_name=table,
        columns=list(columns), alias=alias,
    )


def _named_projection(scan, alias, columns):
    """A projection of alias-qualified columns over a scan."""
    expressions = []
    for column in columns:
        expressions.append(
            ColumnRef(table=alias, column=column, data_type=DataType.INTEGER)
        )
    return Projection(input=scan, expressions=expressions, aliases=list(columns))


def test_cte_child_prunes():
    """A WITH query prunes through the CTE wrapper's main child."""
    cte_scan = _named_scan("w_src", "w", ("x", "y", "z"))
    tree = CTE(
        name="w",
        cte_plan=_named_projection(cte_scan, "w", ("x",)),
        child=Sort(
            input=_projection(_scan(), ("a",)),
            sort_keys=[_col("a")], ascending=[True], nulls_order=[None],
        ),
        recursive=False,
        column_names=None,
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    assert _scan_of(result.child).columns == ["a"]
    assert _scan_of(result.cte_plan).columns == ["x"]


def test_set_operation_of_projections_prunes_both_branches():
    """A root set operation whose branches carry explicit projections prunes
    inside both branches."""
    left_scan = _named_scan("t1", "l", ("a", "b"))
    right_scan = _named_scan("t2", "r", ("d", "e"))
    tree = SetOperation(
        left=_named_projection(left_scan, "l", ("a",)),
        right=_named_projection(right_scan, "r", ("d",)),
        kind=SetOpKind.UNION,
        distinct=True,
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    assert _scan_of(result.left).columns == ["a"]
    assert _scan_of(result.right).columns == ["d"]


def test_set_operation_with_bare_branch_stays_untouched():
    """A set operation with one bare-scan branch (its columns ARE the branch
    schema) must not prune anywhere - positional alignment is at stake."""
    tree = SetOperation(
        left=_projection(_scan(), ("a",)),
        right=_scan(),
        kind=SetOpKind.UNION,
        distinct=True,
    )
    result = ProjectionPushdownRule().apply(tree)
    pruned = result if result is not None else tree
    assert _scan_of(pruned.left).columns == ["a", "b", "c"]
    assert _scan_of(pruned.right).columns == ["a", "b", "c"]
