"""Push the union (OR) of every consumer's filter into a shared CTE body.

A multi-referenced CTE computes for ALL rows while each consumer keeps only
a slice: TPC-DS q31 reads three quarters of one year from CTEs that
aggregate every date, so the whole fact table crosses the wire six times.
Ordinary predicate pushdown must treat a shared CTE as opaque - pushing one
consumer's filter would starve the others - but the OR of ALL consumers'
filters is exact: every consumer's own filter stays in place, and any row
some consumer needs still passes the union.

The pushed filter may only reference GROUPING columns of the body's top
aggregate (translated through passthrough projections): a filter on group
columns is constant within a group, so it removes whole groups no consumer
wants and never partial rows of a kept group. It lands directly under the
aggregate, where the ordinary pushdown rules sink it to the base scans -
which is what lets the semi-join reduction inject dimension keys into the
facts.
"""

from typing import List, Optional, Tuple

from ..plan.expressions import (
    BetweenExpression,
    BinaryOp,
    ColumnRef,
    Expression,
    InList,
    Literal,
    UnaryOp,
    combine_and,
    combine_or,
    expression_children,
    split_conjuncts,
)
from ..plan.logical import (
    CTE,
    CTERef,
    Aggregate,
    Filter,
    LogicalPlanNode,
    Projection,
    Scan,
    SetOperation,
    SetOpKind,
    Union,
)
from .decorrelation import _replace_column_refs
from .rules import OptimizationRule

# Deterministic expression shapes safe to duplicate into the CTE body. An
# allowlist: anything else (subqueries, function calls) is left in place.
_PUSHABLE_EXPRESSIONS = (
    BinaryOp,
    UnaryOp,
    ColumnRef,
    Literal,
    InList,
    BetweenExpression,
)


class CTEUnionFilterPushdownRule(OptimizationRule):
    """Push the OR of all consumer filters into each shared CTE body."""

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        """Apply the union-filter push to every CTE wrapper in the plan."""
        return _apply(plan)

    def name(self) -> str:
        """Rule name for logging."""
        return "CTEUnionFilterPushdown"


def _apply(node: LogicalPlanNode) -> LogicalPlanNode:
    """Recurse over the plan, attempting the push at every CTE wrapper."""
    rebuilt_children = []
    for child in node.children():
        rebuilt_children.append(_apply(child))
    rebuilt = node
    if rebuilt_children:
        rebuilt = node.with_children(rebuilt_children)
    if isinstance(rebuilt, CTE):
        return _push_union_filter(rebuilt)
    return rebuilt


def _push_union_filter(cte: CTE) -> CTE:
    """The CTE with the consumers' union filter in its body, or unchanged
    when any guard declines (a recursive body, a bare unfiltered consumer,
    an untranslatable filter column, a non-aggregate body top). A UNION
    body (q04's year_total unions three channel aggregates) sinks the
    filter into EVERY branch, translated per branch."""
    if cte.recursive:
        return cte
    consumers = _consumer_filters(cte.child, cte.name)
    if consumers is None:
        return cte
    rebuilt = _pushed_body(cte.cte_plan, consumers)
    if rebuilt is None or rebuilt == cte.cte_plan:
        return cte
    return cte.model_copy(update={"cte_plan": rebuilt})


def _pushed_body(body: LogicalPlanNode, consumers) -> Optional[LogicalPlanNode]:
    """The body with the consumers' union filter inserted at every branch
    sink, or None when any branch declines (all-or-nothing keeps the plan
    unchanged rather than half-filtered). A row filter on output columns
    commutes with UNION - dedup included, since it drops whole duplicate
    groups - so union shapes recurse per branch, translated per branch
    (q04's year_total unions three channel aggregates)."""
    if isinstance(body, Union):
        rebuilt = []
        for branch in body.inputs:
            pushed = _pushed_body(branch, consumers)
            if pushed is None:
                return None
            rebuilt.append(pushed)
        return body.model_copy(update={"inputs": rebuilt})
    if isinstance(body, SetOperation) and body.kind == SetOpKind.UNION:
        left = _pushed_body(body.left, consumers)
        right = _pushed_body(body.right, consumers)
        if left is None or right is None:
            return None
        return body.model_copy(update={"left": left, "right": right})
    return _pushed_branch(body, consumers)


def _pushed_branch(branch: LogicalPlanNode, consumers) -> Optional[LogicalPlanNode]:
    """One leaf branch with the union filter at its aggregate sink."""
    sink = _aggregate_sink(branch)
    if sink is None:
        return None
    predicate = _union_predicate(consumers, sink)
    if predicate is None:
        return None
    if _already_pushed(branch, predicate):
        return branch
    return _insert_filter(sink, predicate)


def _consumer_filters(
    child: LogicalPlanNode, name: str
) -> Optional[List[Tuple[Filter, CTERef]]]:
    """Every (Filter, CTERef) consumer of the named CTE, or None when a
    consumer is unfiltered (it needs all rows) or a nested CTE shadows the
    name (references would be ambiguous)."""
    consumers = []
    for node, parent in _walk_with_parent(child, None):
        if isinstance(node, CTE) and node.name == name:
            return None
        if not isinstance(node, CTERef) or node.name != name:
            continue
        if not isinstance(parent, Filter):
            return None
        consumers.append((parent, node))
    return consumers or None


def _walk_with_parent(node: LogicalPlanNode, parent):
    """Yield (node, parent) pairs over the whole subtree."""
    yield node, parent
    for child in node.children():
        yield from _walk_with_parent(child, node)


def _aggregate_sink(body: LogicalPlanNode):
    """(projection chain, aggregate) forming the body's translatable top, or
    None. DISTINCT ON projections decline (they pick a survivor per group,
    so removing input rows changes kept rows); grouping-set aggregates
    decline (a filter under ROLLUP changes super-aggregate rows that a
    consumer may read through IS NULL)."""
    chain = []
    node = body
    while isinstance(node, Projection) and not node.distinct_on:
        chain.append(node)
        node = node.input
    if not isinstance(node, Aggregate) or node.grouping_sets:
        return None
    # `aggregates` is the aggregate's full SELECT list (grouping refs and
    # aggregate calls), parallel to output_names.
    if len(node.output_names) != len(node.aggregates):
        return None
    return chain, node


def _union_predicate(consumers, sink) -> Optional[Expression]:
    """OR of every consumer's translated filter, or None when any consumer
    contributes nothing pushable (its arm would be TRUE, making the whole
    union a no-op)."""
    translated = []
    for filter_node, ref in consumers:
        conjunct = _consumer_predicate(filter_node, ref, sink)
        if conjunct is None:
            return None
        translated.append(conjunct)
    return combine_or(translated)


def _consumer_predicate(filter_node: Filter, ref: CTERef, sink) -> Optional[Expression]:
    """AND of this consumer's pushable, TRANSLATED conjuncts, or None when
    none survive. A conjunct that does not translate (it references an
    aggregate output, e.g. q04's year_total > 0) is simply DROPPED - the
    consumer's arm gets weaker, which only lets more rows through the
    union; the consumer's own filter still applies exactly."""
    kept = []
    for conjunct in split_conjuncts(filter_node.predicate):
        if not _pushable(conjunct, ref.alias):
            continue
        rewritten = _translate_to_group_columns(conjunct, ref, sink)
        if rewritten is not None:
            kept.append(rewritten)
    if not kept:
        return None
    return combine_and(kept)


def _pushable(expr: Expression, alias: Optional[str]) -> bool:
    """Whether an expression is a deterministic shape over ONLY the given
    consumer alias, safe to duplicate into the CTE body."""
    if isinstance(expr, ColumnRef):
        return expr.table == alias
    if not isinstance(expr, _PUSHABLE_EXPRESSIONS):
        return False
    for child in expression_children(expr):
        if not _pushable(child, alias):
            return False
    return True


def _translate_to_group_columns(
    conjunct: Expression, ref: CTERef, sink
) -> Optional[Expression]:
    """The conjunct rewritten onto the aggregate's grouping columns, or None
    when any referenced output is not a plain grouping column."""
    mapping = {}
    for pair in _referenced_pairs(conjunct):
        replacement = _group_column_for(pair[1], ref, sink)
        if replacement is None:
            return None
        mapping[pair] = replacement
    return _replace_column_refs(conjunct, mapping)


def _referenced_pairs(expr: Expression) -> List[Tuple[Optional[str], str]]:
    """The distinct (qualifier, column) pairs referenced by an expression."""
    pairs = []
    if isinstance(expr, ColumnRef):
        pairs.append((expr.table, expr.column))
        return pairs
    for child in expression_children(expr):
        for pair in _referenced_pairs(child):
            if pair not in pairs:
                pairs.append(pair)
    return pairs


def _group_column_for(column: str, ref: CTERef, sink) -> Optional[Expression]:
    """The aggregate's grouping ColumnRef producing the CTE output named
    `column`, chased by POSITION through the passthrough projections (names
    survive CTE column renames that way), or None."""
    outputs = ref.output_names or []
    if column not in outputs:
        return None
    position = _chase_position(outputs.index(column), sink)
    if position is None:
        return None
    chain, aggregate = sink
    replacement = aggregate.aggregates[position]
    if isinstance(replacement, Literal):
        # A constant output column (q04's sale_type channel tag): the
        # literal holds for every row of this branch, so substituting it
        # lets the engine fold the comparison per branch.
        return replacement
    if not isinstance(replacement, ColumnRef):
        return None
    if replacement not in aggregate.group_by:
        return None
    return replacement


def _chase_position(position: int, sink) -> Optional[int]:
    """Follow an output position down the projection chain to the aggregate's
    output position, or None when a projection computes it. Each layer's
    reference resolves against the NAMES OF ITS OWN INPUT (the next chain
    element, or the aggregate at the end)."""
    chain, aggregate = sink
    for index, projection in enumerate(chain):
        if position >= len(projection.expressions):
            return None
        expr = projection.expressions[position]
        if not isinstance(expr, ColumnRef):
            return None
        below = (
            chain[index + 1].aliases
            if index + 1 < len(chain)
            else (aggregate.output_names)
        )
        if expr.column not in below:
            return None
        position = below.index(expr.column)
    return position


def _already_pushed(body: LogicalPlanNode, predicate: Expression) -> bool:
    """Whether an equal predicate already sits in the body (as a Filter
    conjunct or embedded in a scan) - the pushdown pass moves the inserted
    filter, so idempotence must look everywhere, not just the sink."""
    for node, _ in _walk_with_parent(body, None):
        if isinstance(node, Filter) and predicate in split_conjuncts(node.predicate):
            return True
        if isinstance(node, Scan) and node.filters is not None:
            if predicate in split_conjuncts(node.filters):
                return True
    return False


def _insert_filter(sink, predicate: Expression) -> LogicalPlanNode:
    """The body rebuilt with the union filter directly under the aggregate."""
    chain, aggregate = sink
    # Fresh node: this filter did not exist in the plan before, so there is
    # nothing to derive it from with model_copy.
    pushed = Filter.create(input=aggregate.input, predicate=predicate)
    rebuilt = aggregate.model_copy(update={"input": pushed})
    for projection in reversed(chain):
        rebuilt = projection.model_copy(update={"input": rebuilt})
    return rebuilt
