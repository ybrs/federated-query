"""Tests for qualified required-column collection and scan matching
(PP-M2 + PP-M3).

Collection is annotation-driven (direct_expressions) and returns QUALIFIED
names (`table.col`; bare only for unqualified refs), so same-named columns on
different relations prune independently. Scan matching honors alias and
physical table name; guards protect scans whose column list is semantic.
"""

from federated_query.optimizer.rules import ProjectionPushdownRule
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
    InSubquery,
    Literal,
)
from federated_query.plan.logical import (
    Filter,
    Join,
    JoinType,
    Projection,
    Scan,
)


def _scan(table="t", alias="t", columns=("a", "b", "c")):
    """A scan with explicit alias and columns."""
    return Scan(
        datasource="ds", schema_name="s", table_name=table,
        columns=list(columns), alias=alias,
    )


def _col(table, column):
    """A qualified column reference."""
    return ColumnRef(table=table, column=column, data_type=DataType.INTEGER)


def _scans_by_alias(plan, found=None):
    """Every scan in a tree keyed by alias."""
    if found is None:
        found = {}
    if isinstance(plan, Scan):
        found[plan.alias] = plan
    for child in plan.children():
        _scans_by_alias(child, found)
    return found


def test_self_join_prunes_each_side_independently():
    """Two scans of the SAME table under different aliases keep only their
    own referenced columns - bare-name union would keep the union on both."""
    n1 = _scan("nation", "n1", ("n_name", "n_nationkey", "n_comment"))
    n2 = _scan("nation", "n2", ("n_name", "n_nationkey", "n_comment"))
    join = Join(
        left=n1, right=n2, join_type=JoinType.INNER,
        condition=BinaryOp(
            op=BinaryOpType.EQ,
            left=_col("n1", "n_nationkey"), right=_col("n2", "n_nationkey"),
        ),
    )
    tree = Projection(
        input=join,
        expressions=[_col("n1", "n_name"), _col("n2", "n_comment")],
        aliases=["n_name", "n_comment"],
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    scans = _scans_by_alias(result)
    assert scans["n1"].columns == ["n_name", "n_nationkey"]
    assert scans["n2"].columns == ["n_nationkey", "n_comment"]


def test_unqualified_reference_still_matches():
    """A bare (unqualified) reference keeps the column on any scan exposing
    it - the safe fallback for hand-built plans."""
    tree = Projection(
        input=_scan(),
        expressions=[ColumnRef(table=None, column="b", data_type=DataType.INTEGER)],
        aliases=["b"],
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    assert _scans_by_alias(result)["t"].columns == ["b"]


def test_table_name_qualifier_matches_aliased_scan():
    """A reference qualified by the PHYSICAL table name still matches a scan
    that carries an alias (the documented alias-vs-physical tolerance)."""
    tree = Projection(
        input=_scan("orders", "o", ("o_id", "o_flag")),
        expressions=[_col("orders", "o_flag")],
        aliases=["o_flag"],
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    scans = _scans_by_alias(result)
    assert scans["o"].columns == ["o_flag"]


def test_in_subquery_value_and_correlated_refs_survive():
    """An IN-subquery predicate's probe column AND a correlated reference
    inside its subplan are both collected (they hide from the generic
    expression walk)."""
    inner = Projection(
        input=_scan("u", "u", ("k", "v")),
        expressions=[_col("u", "k"),
                     BinaryOp(op=BinaryOpType.EQ, left=_col("u", "v"),
                              right=_col("t", "c"))],
        aliases=["k", "match"],
    )
    predicate = InSubquery(
        value=_col("t", "b"),
        subquery=inner,
        negated=False,
    )
    tree = Projection(
        input=Filter(input=_scan(), predicate=predicate),
        expressions=[_col("t", "a")],
        aliases=["a"],
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    assert _scans_by_alias(result)["t"].columns == ["a", "b", "c"]


def test_scan_order_by_keys_survive_without_sort_node():
    """After OrderByPushdown dissolves the Sort into scan metadata, the sort
    column must still be collected from Scan.order_by_keys."""
    scan = Scan(
        datasource="ds", schema_name="s", table_name="t", columns=["a", "b", "c"],
        alias="t", order_by_keys=[_col("t", "b")], order_by_ascending=[True],
        order_by_nulls=[None],
    )
    tree = Projection(input=scan, expressions=[_col("t", "a")], aliases=["a"])
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    assert _scans_by_alias(result)["t"].columns == ["a", "b"]


def test_aggregate_folded_scan_untouched():
    """A scan carrying a pushed-down aggregate is never pruned: its column
    list is semantic (the remote query's shape), not a read set."""
    scan = Scan(
        datasource="ds", schema_name="s", table_name="t", columns=["a", "b", "c"],
        alias="t", group_by=[_col("t", "a")], aggregates=[_col("t", "a")],
        output_names=["a"],
    )
    tree = Projection(input=scan, expressions=[_col("t", "a")], aliases=["a"])
    result = ProjectionPushdownRule().apply(tree)
    pruned = result if result is not None else tree
    assert _scans_by_alias(pruned)["t"].columns == ["a", "b", "c"]


def test_distinct_scan_untouched():
    """Pruning a DISTINCT scan would change its deduplication set - wrong
    results. It must be left alone."""
    scan = Scan(
        datasource="ds", schema_name="s", table_name="t", columns=["a", "b"],
        alias="t", distinct=True,
    )
    tree = Projection(input=scan, expressions=[_col("t", "a")], aliases=["a"])
    result = ProjectionPushdownRule().apply(tree)
    pruned = result if result is not None else tree
    assert _scans_by_alias(pruned)["t"].columns == ["a", "b"]


def test_star_reference_keeps_all_columns():
    """A genuine star PROJECTION (not COUNT(*)) means keep all columns."""
    star = ColumnRef(table=None, column="*", data_type=DataType.INTEGER)
    tree = Projection(input=_scan(), expressions=[star], aliases=["star"])
    result = ProjectionPushdownRule().apply(tree)
    pruned = result if result is not None else tree
    assert _scans_by_alias(pruned)["t"].columns == ["a", "b", "c"]


def test_never_referenced_scan_keeps_all_columns():
    """A relation none of whose columns are referenced keeps everything
    (a zero-column scan is not representable)."""
    other = _scan("u", "u", ("x", "y"))
    join = Join(
        left=_scan(), right=other, join_type=JoinType.CROSS, condition=None,
    )
    tree = Projection(input=join, expressions=[_col("t", "a")], aliases=["a"])
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    scans = _scans_by_alias(result)
    assert scans["t"].columns == ["a"]
    assert scans["u"].columns == ["x", "y"]


def test_count_star_does_not_block_pruning():
    """COUNT(*) counts rows and needs no columns: its star argument must not
    poison the required set and disable pruning for the whole query (q21
    kept every scan full-width because of the count(*) in its SELECT list)."""
    from federated_query.plan.expressions import FunctionCall
    from federated_query.plan.logical import Aggregate

    count_star = FunctionCall(
        function_name="COUNT",
        args=[ColumnRef(table=None, column="*", data_type=DataType.INTEGER)],
        is_aggregate=True,
    )
    tree = Aggregate(
        input=_scan(),
        group_by=[_col("t", "a")],
        aggregates=[_col("t", "a"), count_star],
        output_names=["a", "n"],
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    assert _scans_by_alias(result)["t"].columns == ["a"]
