"""Prototype: a well-scoped-plan validator.

After decorrelation (and any plan rewrite) every qualified column reference in an
operator must resolve to a relation that operator's inputs actually expose. The
binder establishes this on the original tree; transforms can break it on the
rewritten tree, and nothing re-checks. This pass re-checks: it walks the plan
with a scope of visible relation qualifiers (mirroring SQL scoping, including
LATERAL correlation) and reports every qualified reference whose qualifier is not
in scope at the node that uses it.

Scope: qualifier-level (does the table/alias exist in scope?), not column-level
(the binder already checks columns on the original tree). Qualifier-level is the
strongest low-false-positive signal and is exactly the item-1 failure mode (a
SEMI join condition referencing a relation introduced only by its parent).
``find_scope_violations`` returns the list; ``validate_scope`` raises ScopeError
on the first non-empty result so a mis-scoped plan fails loudly at the layer that
built it, instead of crashing cryptically downstream or returning wrong rows.
"""

from typing import FrozenSet, List, Set, Tuple

from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    SubqueryScan,
    CTE,
    CTERef,
    Explain,
    Filter,
    Projection,
    Aggregate,
    Sort,
    Limit,
    GroupedLimit,
    SingleRowGuard,
    Join,
    JoinType,
    LateralJoin,
    SetOperation,
    Union,
    Values,
)
from ..plan.expressions import column_refs


class ScopeError(Exception):
    """A plan references a relation qualifier not in scope at the operator."""


# Single-input passthrough nodes: they expose the relation qualifiers of their
# input unchanged (Projection/Aggregate rename columns but, permissively, we let
# the input's qualifiers flow through to avoid false positives - a reference is
# only flagged when its qualifier appears in NO input subtree at all).
_PASSTHROUGH_INPUT = (
    Filter,
    Projection,
    Aggregate,
    Sort,
    Limit,
    GroupedLimit,
    SingleRowGuard,
    Explain,
)


def _output_qualifiers(node: LogicalPlanNode) -> Set[str]:
    """Relation qualifiers a node exposes to its parent."""
    if isinstance(node, Scan):
        return {node.alias or node.table_name}
    if isinstance(node, SubqueryScan):
        return {node.alias}
    if isinstance(node, CTERef):
        return {node.alias or node.name}
    if isinstance(node, Values):
        return set()
    return _output_qualifiers_compound(node)


def _output_qualifiers_compound(node: LogicalPlanNode) -> Set[str]:
    """Output qualifiers of multi-input / passthrough nodes; raise on unknown."""
    if isinstance(node, _PASSTHROUGH_INPUT):
        return _output_qualifiers(node.input)
    if isinstance(node, CTE):
        return _output_qualifiers(node.child)
    if isinstance(node, Join):
        return _join_output_qualifiers(node)
    if isinstance(node, LateralJoin):
        return _output_qualifiers(node.left) | _output_qualifiers(node.right)
    if isinstance(node, SetOperation):
        return _output_qualifiers(node.left)
    if isinstance(node, Union):
        return _union_output_qualifiers(node)
    raise ValueError(
        f"_output_qualifiers has no rule for plan node {type(node).__name__}"
    )


def _join_output_qualifiers(node: Join) -> Set[str]:
    """A join exposes both sides, except SEMI/ANTI which expose only the left."""
    if node.join_type in (JoinType.SEMI, JoinType.ANTI):
        return _output_qualifiers(node.left)
    return _output_qualifiers(node.left) | _output_qualifiers(node.right)


def _union_output_qualifiers(node: Union) -> Set[str]:
    """A union exposes the qualifiers of all its branches."""
    qualifiers: Set[str] = set()
    for branch in node.inputs:
        qualifiers |= _output_qualifiers(branch)
    return qualifiers


def _available_qualifiers(node: LogicalPlanNode) -> Set[str]:
    """Relation qualifiers visible to a node's OWN expressions (not its parent's).

    A join condition sees both sides; a scan's filters see the scan; every other
    expression-bearing node sees its single input.
    """
    if isinstance(node, Scan):
        return {node.alias or node.table_name}
    if isinstance(node, (Join, LateralJoin)):
        return _output_qualifiers(node.left) | _output_qualifiers(node.right)
    if isinstance(node, _PASSTHROUGH_INPUT):
        return _output_qualifiers(node.input)
    return set()


def _child_scopes(
    node: LogicalPlanNode, outer: FrozenSet[str]
) -> List[Tuple[LogicalPlanNode, FrozenSet[str]]]:
    """Each child paired with the outer scope visible inside it.

    A LATERAL join's right side may correlate to its left, so the left's output
    qualifiers are added to the right child's outer scope. Every other child sees
    the same outer scope as its parent (post-decorrelation, plain-join branches
    do not correlate to each other).
    """
    if isinstance(node, LateralJoin):
        right_outer = outer | frozenset(_output_qualifiers(node.left))
        return [(node.left, outer), (node.right, right_outer)]
    scopes: List[Tuple[LogicalPlanNode, FrozenSet[str]]] = []
    for child in node.children():
        scopes.append((child, outer))
    return scopes


def find_scope_violations(
    plan: LogicalPlanNode, outer: FrozenSet[str] = frozenset()
) -> List[str]:
    """Report every qualified reference whose qualifier is out of scope."""
    available = _available_qualifiers(plan) | set(outer)
    violations: List[str] = []
    for expr in plan.direct_expressions():
        for ref in column_refs(expr):
            if ref.table is not None and ref.table not in available:
                violations.append(
                    f"{type(plan).__name__} references {ref.table}.{ref.column}; "
                    f"in scope: {sorted(available)}"
                )
    for child, child_outer in _child_scopes(plan, outer):
        violations.extend(find_scope_violations(child, child_outer))
    return violations


def validate_scope(plan: LogicalPlanNode, stage: str = "") -> None:
    """Raise ScopeError if any qualified reference is out of scope.

    The stage label (e.g. "after decorrelation", "after PredicatePushdownRule")
    names where the mis-scoped plan was produced, so the failure points at the
    transform that broke the well-scoped invariant.
    """
    violations = find_scope_violations(plan)
    if not violations:
        return
    prefix = f"{stage}: " if stage else ""
    raise ScopeError(prefix + "; ".join(violations))
