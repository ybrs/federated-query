"""Eager aggregation: push a PARTIAL aggregate below the joins that only
decorate it, so the fact collapses before those joins run.

    Aggregate(G, sums) over Join(D..., S)
        ->  Aggregate(G', merge-sums) over Join(D..., SubqueryScan(
                Aggregate((G n S) u K, partial-sums) over S, alias))

S is the subtree supplying every aggregate input; the D_i are "decorating"
dims (INNER plain-column equi joins whose columns feed no aggregate); K are
the S-side join keys to them. NO uniqueness requirement on the D_i keys:
join multiplicity is a function of the key alone and every raw row inside a
partial shares that key, so the final merge counts each partial exactly as
the original counted each raw row (Yan & Larson 1995). Full design and
gates: eager-agg-plan.md.

After the rewrite, dim shipping sees the partial as a plain aggregate root
over the fact and its remaining small dims and can collapse it into one
island; even unshipped, the coordinator aggregates the fact BEFORE the
decorating joins instead of after.

v1 scope (each miss DECLINES - declining is always safe):
- plain single-level GROUP BY, SUM aggregates only (no DISTINCT, no
  WITHIN GROUP);
- the aggregate input is a pure INNER-join tree of base scans whose every
  join conjunct is a plain column equality;
- join conjuncts POOL across the tree: survivor-survivor conjuncts rebuild
  the S tree, peeled-dim conjuncts lift above the partial;
- group keys and aggregate arguments on the S side are plain columns.
"""

import os
from itertools import combinations
from typing import Dict, List, Optional, Tuple

from ..plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    Expression,
    FunctionCall,
    Literal,
    column_refs,
    combine_and,
    split_conjuncts,
)
from ..plan.logical import (
    Aggregate,
    Join,
    JoinType,
    LogicalPlanNode,
    Scan,
    SubqueryScan,
    transform_children,
)
from .decorrelation import _replace_column_refs
from .rules import OptimizationRule
from .subplan_signature import scan_qualifier

# The partial must COLLAPSE: estimated groups at or below this fraction of
# the S subtree's estimated rows, both KNOWN (an unknown or bound-fed size
# declines - a non-collapsing partial is pure overhead). Self-corrects via
# learned group counts exactly like the dim-shipping gate.
EAGER_COLLAPSE_MAX_RATIO = 0.5

# Peel-subset enumeration cap: more eligible dims than this declines (the
# subset space doubles per dim; the q04 family has one or two).
MAX_PEEL_CANDIDATES = 4

# Alias prefix for the partial's derived table; also the idempotence guard
# (an aggregate whose inputs come from an eager alias never re-splits).
_EAGER_PREFIX = "__eager_"


class EagerAggregationRule(OptimizationRule):
    """Rewrite Aggregate-over-join into final-over-Join(dims, partial)."""

    def __init__(self, cost_model):
        """The rule prices collapse with the session's shared cost model."""
        self.cost_model = cost_model
        self._counter = 0

    def name(self) -> str:
        """Return this rule's identifier (used in logging and EXPLAIN)."""
        return "EagerAggregation"

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        """Rewrite every qualifying aggregate; None when nothing changed.
        FEDQ_EAGER_AGG=0 is the kill switch (mirrors FEDQ_DIM_SHIPPING)."""
        if os.environ.get("FEDQ_EAGER_AGG") == "0":
            return None
        self._counter = 0
        rewritten = self._rewrite(plan)
        if rewritten is plan:
            return None
        return rewritten

    def _rewrite(self, node: LogicalPlanNode) -> LogicalPlanNode:
        """Depth-first: children first (a CTE body's aggregate rewrites too),
        then attempt the split on this node."""
        node = transform_children(node, self._rewrite)
        if isinstance(node, Aggregate):
            split = self._try_split(node)
            if split is not None:
                return split
        return node

    # ------------------------------ gating --------------------------------

    def _try_split(self, agg: Aggregate) -> Optional[LogicalPlanNode]:
        """The rewritten final-over-partial tree, or None to keep `agg`."""
        shape = self._qualifying_shape(agg)
        if shape is None:
            return None
        scans, conjuncts = shape
        candidates = self._peel_candidates(agg, scans, conjuncts)
        if not candidates or len(candidates) > MAX_PEEL_CANDIDATES:
            return None
        return self._best_split(agg, scans, conjuncts, candidates)

    def _qualifying_shape(self, agg: Aggregate):
        """(scans, join conjuncts) when the aggregate and its input match the
        v1 shape, else None."""
        if agg.grouping_sets or not self._sum_only_outputs(agg):
            return None
        if not self._plain_column_keys(agg.group_by):
            return None
        scans: List[Scan] = []
        conjuncts: List[Expression] = []
        if not _collect_inner_tree(agg.input, scans, conjuncts):
            return None
        if not self._plain_equi_conjuncts(conjuncts):
            return None
        if self._single_source(scans):
            return None
        return scans, conjuncts

    def _single_source(self, scans) -> bool:
        """Whether every scan reads one datasource. A single-source subtree
        pushes whole and aggregates at the source in one pass - a partial
        there is pure overhead; the transform exists to shrink what CROSSES
        a source boundary."""
        sources = set()
        for scan in scans:
            sources.add(scan.datasource)
        return len(sources) <= 1

    def _sum_only_outputs(self, agg: Aggregate) -> bool:
        """Every output is a passthrough group column, a CONSTANT (a literal
        tag column like q04's sale_type), or a plain SUM call. An aggregate
        whose input already contains an eager partial (a SubqueryScan this
        rule aliased) also declines: the idempotence guard against
        re-splitting an already-split aggregate."""
        for entry in agg.aggregates:
            if isinstance(entry, (ColumnRef, Literal)):
                continue
            if not _plain_sum(entry):
                return False
        return not _contains_eager_partial(agg.input)

    def _plain_column_keys(self, keys: List[Expression]) -> bool:
        """Whether every group key is a qualified plain column reference."""
        for key in keys:
            if not isinstance(key, ColumnRef) or not key.table:
                return False
        return True

    def _plain_equi_conjuncts(self, conjuncts: List[Expression]) -> bool:
        """Whether every join conjunct is a column = column equality."""
        for conjunct in conjuncts:
            if not isinstance(conjunct, BinaryOp) or conjunct.op != BinaryOpType.EQ:
                return False
            if not isinstance(conjunct.left, ColumnRef):
                return False
            if not isinstance(conjunct.right, ColumnRef):
                return False
        return True

    def _peel_candidates(self, agg, scans, conjuncts) -> List[Scan]:
        """The dims eligible to move ABOVE the partial: scans owning no
        aggregate-argument column. The scans that DO feed aggregates form the
        mandatory core of S; no core means nothing anchors a partial."""
        core = self._agg_arg_owners(agg, scans)
        if not core:
            return []
        candidates = []
        for scan in scans:
            if scan_qualifier(scan) not in core:
                candidates.append(scan)
        return candidates

    def _agg_arg_owners(self, agg, scans) -> set:
        """The qualifiers of scans referenced by any SUM argument."""
        qualifiers = set()
        for scan in scans:
            qualifiers.add(scan_qualifier(scan))
        owners = set()
        for entry in agg.aggregates:
            if isinstance(entry, ColumnRef):
                continue
            for ref in column_refs(entry):
                if ref.table in qualifiers:
                    owners.add(ref.table)
        return owners

    # --------------------------- split choice ------------------------------

    def _best_split(self, agg, scans, conjuncts, candidates):
        """The rewrite for the best-collapsing peel subset passing the cost
        gate, or None when no subset passes."""
        best = None
        best_ratio = EAGER_COLLAPSE_MAX_RATIO
        for size in range(1, len(candidates) + 1):
            for subset in combinations(candidates, size):
                built = self._evaluate_subset(agg, scans, conjuncts, subset)
                if built is not None and built[0] <= best_ratio:
                    best_ratio = built[0]
                    best = built[1]
        return best

    def _evaluate_subset(self, agg, scans, conjuncts, peeled) -> Optional[Tuple]:
        """(collapse ratio, rewritten tree) for one peel subset, or None when
        the subset's shape or cost declines."""
        split = self._partition_conjuncts(conjuncts, peeled)
        if split is None:
            return None
        inner_conjuncts, lifted = split
        survivors = self._survivors(scans, peeled)
        s_tree = self._join_survivors(survivors, inner_conjuncts)
        if s_tree is None:
            return None
        partial = self._build_partial(agg, survivors, lifted, s_tree)
        if partial is None:
            return None
        ratio = self._collapse_ratio(partial, survivors)
        if ratio is None or ratio > EAGER_COLLAPSE_MAX_RATIO:
            return None
        return ratio, self._build_final(agg, partial, peeled, lifted)

    def _partition_conjuncts(self, conjuncts, peeled):
        """(conjuncts internal to S, conjuncts lifted above the partial).
        A conjunct is lifted when it references any peeled dim; every other
        conjunct must join two survivors. None when a lifted conjunct's
        OTHER side is neither a survivor column nor a peeled column (v1)."""
        peeled_names = set()
        for scan in peeled:
            peeled_names.add(scan_qualifier(scan))
        inner = []
        lifted = []
        for conjunct in conjuncts:
            touches = self._conjunct_touches(conjunct, peeled_names)
            if touches == 0:
                inner.append(conjunct)
            else:
                lifted.append(conjunct)
        return inner, lifted

    def _conjunct_touches(self, conjunct, peeled_names) -> int:
        """How many sides of a plain equality reference a peeled dim."""
        touches = 0
        for ref in (conjunct.left, conjunct.right):
            if ref.table in peeled_names:
                touches += 1
        return touches

    def _survivors(self, scans, peeled) -> List[Scan]:
        """The S-side scans, in original order."""
        survivors = []
        for scan in scans:
            if scan not in peeled:
                survivors.append(scan)
        return survivors

    def _join_survivors(self, survivors, inner_conjuncts):
        """A left-deep INNER join over the survivors placing every internal
        conjunct exactly once, or None when a survivor would join with no
        condition (a manufactured cross join declines)."""
        if not survivors:
            return None
        current = survivors[0]
        in_scope = {scan_qualifier(survivors[0])}
        pending = list(inner_conjuncts)
        for scan in survivors[1:]:
            in_scope.add(scan_qualifier(scan))
            covered, pending = _covered_conjuncts(pending, in_scope)
            if not covered:
                return None
            # One left-deep step of the rebuilt S tree; JoinOrdering re-costs
            # the region afterwards, so the rebuild order is not final.
            current = Join.create(
                left=current, right=scan, join_type=JoinType.INNER,
                condition=combine_and(covered),
            )
        if pending:
            return None
        return current

    # ---------------------------- construction -----------------------------

    def _build_partial(self, agg, survivors, lifted, s_tree):
        """The partial aggregate wrapped as an aliased derived table, plus its
        rewrite bookkeeping; None when a needed column is not a plain survivor
        column or output names would collide."""
        survivor_names = set()
        for scan in survivors:
            survivor_names.add(scan_qualifier(scan))
        keys = self._partial_keys(agg, lifted, survivor_names)
        if keys is None:
            return None
        select_list, names, sum_names = self._partial_outputs(agg, keys)
        if select_list is None:
            return None
        alias = f"{_EAGER_PREFIX}{self._counter}"
        self._counter += 1
        # The partial: group by the surviving group keys plus the lifted join
        # keys; aggregates IS the output select list (grouping columns pass
        # through), matching the Aggregate contract used across the engine.
        partial = Aggregate.create(
            input=s_tree, group_by=list(keys), aggregates=select_list,
            output_names=names,
        )
        # The derived-table wrapper gives every synthetic partial output a
        # REAL qualifier (columns-must-be-qualified), mirroring
        # decorrelation's __subq relations.
        scan = SubqueryScan.create(input=partial, alias=alias)
        return {"scan": scan, "alias": alias, "keys": keys, "sums": sum_names}

    def _partial_keys(self, agg, lifted, survivor_names):
        """The partial's group keys: surviving original keys plus each lifted
        conjunct's survivor-side column, deduplicated; None when a lifted
        conjunct has no survivor side to key on (a dim-dim link is fine - it
        contributes no key) or a survivor group key is missing."""
        keys: List[ColumnRef] = []
        seen = set()
        for key in agg.group_by:
            if key.table in survivor_names:
                _add_key(keys, seen, key)
        for conjunct in lifted:
            for ref in (conjunct.left, conjunct.right):
                if ref.table in survivor_names:
                    _add_key(keys, seen, ref)
        if not keys:
            return None
        return keys

    def _partial_outputs(self, agg, keys):
        """(select list, output names, per-SUM synthetic names) for the
        partial; None on an output-name collision (v1 declines rather than
        de-duplicating)."""
        select_list: List[Expression] = []
        names: List[str] = []
        for key in keys:
            select_list.append(key)
            names.append(key.column)
        sum_names: List[str] = []
        sum_index = 0
        for entry in agg.aggregates:
            # Group passthroughs are already emitted via `keys`; a constant
            # output needs nothing from the partial (it passes through the
            # FINAL aggregate unchanged); only SUM calls compute here.
            if not isinstance(entry, (ColumnRef, Literal)):
                name = f"{_EAGER_PREFIX}sum_{sum_index}"
                sum_index += 1
                select_list.append(entry)
                names.append(name)
                sum_names.append(name)
        if len(set(names)) != len(names):
            return None, None, None
        return select_list, names, sum_names

    def _collapse_ratio(self, built, survivors) -> Optional[float]:
        """Estimated partial groups over the LARGEST base scan in S; None
        when either is UNKNOWN or rests on gaps (a bound must not license the
        rewrite). The largest base is the denominator deliberately - the same
        safe over-estimate reasoning as single_source_pushdown._root_estimate:
        a JOIN estimate under-counts through composite/containment asymmetries
        (measured: q04's fact x filtered-date join estimated 304k of a true
        10.9M), and an under-counted denominator would veto exactly the
        rewrites that matter. The fact's rows floor the transfer any
        non-collapsing plan pays; the partial's groups replace it."""
        partial = built["scan"].input
        groups = self.cost_model.estimate(partial)
        if groups.rows is None or groups.defaults_used:
            return None
        largest = self._largest_scan_rows(survivors)
        if not largest:
            return None
        return groups.rows / largest

    def _largest_scan_rows(self, survivors) -> Optional[int]:
        """The largest survivor scan's estimated rows; None when any scan is
        UNKNOWN or gap-fed (the gate needs measurements, not bounds)."""
        largest = None
        for scan in survivors:
            estimate = self.cost_model.estimate(scan)
            if estimate.rows is None or estimate.defaults_used:
                return None
            if largest is None or estimate.rows > largest:
                largest = estimate.rows
        return largest

    def _build_final(self, agg, built, peeled, lifted):
        """The final tree: peeled dims joined back onto the partial's derived
        table, topped by the merge aggregate with the ORIGINAL outputs."""
        mapping = self._alias_mapping(built)
        # Rewrite ONCE up front: the ordering walk and the coverage check must
        # see the same qualifiers (the S side of every lifted conjunct now
        # reads from the partial's alias).
        pending = _rewrite_all(lifted, mapping)
        current = built["scan"]
        in_scope = {built["alias"]}
        for scan in _joinable_order(peeled, pending, in_scope):
            in_scope.add(scan_qualifier(scan))
            covered, pending = _covered_conjuncts(pending, in_scope)
            # One decorating dim joined back above the partial, its condition
            # rewritten onto the partial's alias.
            current = Join.create(
                left=current, right=scan, join_type=JoinType.INNER,
                condition=combine_and(covered),
            )
        _require_all_placed(pending)
        return self._final_aggregate(agg, built, current, mapping)

    def _alias_mapping(self, built) -> Dict:
        """Rewrite map: every partial group key, addressed by its ORIGINAL
        qualifier, now reads from the partial's alias under the same name."""
        mapping = {}
        for key in built["keys"]:
            mapping[(key.table, key.column)] = key.model_copy(
                update={"table": built["alias"]}
            )
        return mapping

    def _final_aggregate(self, agg, built, joined, mapping) -> Aggregate:
        """The merge aggregate: original group keys (S-side ones rewritten to
        the alias), each SUM merged as SUM of its partial column, original
        output names preserved."""
        group_by = _rewrite_all(agg.group_by, mapping)
        select_list: List[Expression] = []
        sum_iter = iter(built["sums"])
        for entry in agg.aggregates:
            if isinstance(entry, Literal):
                select_list.append(entry)
                continue
            if isinstance(entry, ColumnRef):
                select_list.append(_replace_column_refs(entry, mapping))
                continue
            # create, not model_copy: a FRESH reference to a column this
            # rule just synthesized on the partial - no prior node to copy.
            partial_ref = ColumnRef.create(
                table=built["alias"], column=next(sum_iter),
                data_type=entry.get_type(),
            )
            # The merge SUM keeps the original call's identity via model_copy
            # (distinct/within-group flags survive - they are gate-guaranteed
            # empty, and copying keeps that invariant visible), swapping only
            # the argument for the partial's synthetic column.
            select_list.append(entry.model_copy(update={"args": [partial_ref]}))
        # The final aggregate keeps the ORIGINAL output contract; only its
        # input and expression qualifiers changed.
        return agg.model_copy(update={
            "input": joined, "group_by": group_by, "aggregates": select_list,
        })


def _contains_eager_partial(node) -> bool:
    """Whether the subtree already holds one of this rule's partials (a
    SubqueryScan under the reserved alias prefix)."""
    if isinstance(node, SubqueryScan) and node.alias.startswith(_EAGER_PREFIX):
        return True
    for child in node.children():
        if _contains_eager_partial(child):
            return True
    return False


def _plain_sum(entry: Expression) -> bool:
    """Whether an output entry is a plain (non-DISTINCT, non-ordered-set)
    SUM aggregate call."""
    if not isinstance(entry, FunctionCall) or not entry.is_aggregate:
        return False
    if entry.distinct or entry.within_group_key is not None:
        return False
    return entry.function_name.upper() == "SUM"


def _collect_inner_tree(node, scans: List[Scan], conjuncts: List[Expression]) -> bool:
    """Collect the scans and join conjuncts of a pure INNER-join tree of base
    scans; False when the subtree contains anything else."""
    if isinstance(node, Scan):
        scans.append(node)
        return True
    if not isinstance(node, Join) or node.join_type != JoinType.INNER:
        return False
    if node.condition is None:
        return False
    conjuncts.extend(split_conjuncts(node.condition))
    return _collect_inner_tree(node.left, scans, conjuncts) and _collect_inner_tree(
        node.right, scans, conjuncts
    )


def _add_key(keys: List[ColumnRef], seen: set, ref: ColumnRef) -> None:
    """Append a group key once per (qualifier, column)."""
    identity = (ref.table, ref.column)
    if identity not in seen:
        seen.add(identity)
        keys.append(ref)


def _covered_conjuncts(pending, in_scope):
    """(conjuncts whose both sides are in scope, the rest), preserving order."""
    covered = []
    remaining = []
    for conjunct in pending:
        if conjunct.left.table in in_scope and conjunct.right.table in in_scope:
            covered.append(conjunct)
        else:
            remaining.append(conjunct)
    return covered, remaining


def _rewrite_all(expressions, mapping):
    """Every expression with the alias mapping applied."""
    rewritten = []
    for expression in expressions:
        rewritten.append(_replace_column_refs(expression, mapping))
    return rewritten


def _joinable_order(peeled, lifted, base_scope):
    """The peeled dims ordered so each joins something already in scope (a
    snowflake leaf waits for its parent dim); raises when no order exists -
    a peel subset with an unjoinable dim is a partitioning bug, since every
    lifted conjunct touched a peeled dim."""
    remaining = list(peeled)
    scope = set(base_scope)
    ordered = []
    while remaining:
        scan = _next_joinable(remaining, lifted, scope)
        scope.add(scan_qualifier(scan))
        remaining.remove(scan)
        ordered.append(scan)
    return ordered


def _next_joinable(remaining, lifted, scope):
    """The first peeled dim (original order) reachable from the scope."""
    for scan in remaining:
        if _reaches_scope(scan, lifted, scope):
            return scan
    raise ValueError(
        "eager aggregation: a peeled dim joins nothing in scope; the "
        "conjunct partition is inconsistent"
    )


def _reaches_scope(scan, lifted, scope) -> bool:
    """Whether any lifted conjunct links this dim to the current scope."""
    qualifier = scan_qualifier(scan)
    for conjunct in lifted:
        tables = {conjunct.left.table, conjunct.right.table}
        if qualifier in tables and tables & scope:
            return True
    return False


def _require_all_placed(pending) -> None:
    """Every lifted conjunct must be placed exactly once; a dropped predicate
    manufactures wrong results - crash instead."""
    if pending:
        raise ValueError(
            f"eager aggregation dropped {len(pending)} join conjunct(s); "
            "every lifted conjunct must be placed exactly once"
        )
