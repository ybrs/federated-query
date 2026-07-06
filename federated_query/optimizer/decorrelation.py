"""Subquery decorrelation.

Rewrites subquery expressions in bound logical plans into join-based plans:

- EXISTS / NOT EXISTS            -> SEMI / ANTI join
- IN / NOT IN (subquery)         -> SEMI join / ANTI join with NULL-aware match
- op ANY / op SOME / op ALL      -> SEMI join / ANTI join over violations
- scalar subqueries              -> LEFT join (+ aggregation, runtime guards)
- boolean subqueries in a SELECT -> SEMI/ANTI branch pair unioned with flags

Uncorrelated subqueries are inlined as join inputs (executed per consuming
join) rather than hoisted to CTEs: the execution layer has no CTE support,
so an inlined subplan is the correct executable form. CTE reuse remains a
future optimization.

NULL semantics: WHERE-context rewrites are exact. NOT IN and ALL use
anti-join match conditions augmented with IS NULL terms, so UNKNOWN
outcomes behave like FALSE exactly as SQL requires in WHERE context.
Boolean flag columns built for SELECT-list subqueries collapse UNKNOWN to
FALSE; this is the one documented deviation from three-valued logic.

Correlated LIMIT subqueries become per-key GroupedLimit nodes; scalar
subqueries that cannot be proven single-row get a SingleRowGuard that
raises CardinalityViolationError at execution, mirroring real engines.

Unsupported patterns raise DecorrelationError. Decorrelation never
silently skips a subquery.
"""

from ..model import StateModel
from typing import Dict, List, Optional, Set, Tuple

from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Filter,
    Projection,
    Aggregate,
    Sort,
    Join,
    JoinType,
    Limit,
    Union,
    Explain,
    CTE,
    Values,
    SubqueryScan,
    CTERef,
    SingleRowGuard,
    GroupedLimit,
    LateralJoin,
    SetOperation,
    transform_children,
)
from ..plan.expressions import (
    Expression,
    ExistsExpression,
    InSubquery,
    QuantifiedComparison,
    Quantifier,
    SubqueryExpression,
    ColumnRef,
    Literal,
    DataType,
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
    FunctionCall,
    CaseExpr,
    InList,
    BetweenExpression,
    TupleExpression,
    WindowExpr,
    Cast,
    Extract,
    # Canonical shared expression utilities (single implementation lives in
    # plan/expressions.py); imported under the existing local names so call
    # sites are unchanged and there is exactly one implementation of each.
    expression_children as _expression_children,
    map_children as _rebuild_expression,
    column_refs as _expression_column_refs,
    split_conjuncts as _split_conjuncts,
    split_disjuncts as _split_disjuncts,
    combine_and,
    combine_or,
    SUBQUERY_NODE_TYPES as _SUBQUERY_NODE_TYPES,
)
from .scope_validator import validate_scope


class DecorrelationError(Exception):
    """Raised when decorrelation cannot be completed."""


class NonFlattenableCorrelation(DecorrelationError):
    """A correlation cannot become a set-based join (non-equality across an
    aggregate or LIMIT). The scalar-subquery path catches this specific type to
    fall back to a LATERAL join; other DecorrelationErrors propagate."""


# Logical negation of a comparison operator, used to push NOT through a
# quantified comparison by De Morgan (NOT (x op ANY S) == x neg_op ALL S).
_NEGATED_BINARY_OP = {
    BinaryOpType.EQ: BinaryOpType.NEQ,
    BinaryOpType.NEQ: BinaryOpType.EQ,
    BinaryOpType.LT: BinaryOpType.GTE,
    BinaryOpType.LTE: BinaryOpType.GT,
    BinaryOpType.GT: BinaryOpType.LTE,
    BinaryOpType.GTE: BinaryOpType.LT,
}


def _and_join(conjuncts: List[Expression]) -> Expression:
    """Combine conjuncts into a single AND expression; raise if empty."""
    result = combine_and(conjuncts)
    if result is None:
        raise DecorrelationError("Cannot build a predicate from no conjuncts")
    return result


def _is_unconditional_global_aggregate(plan: LogicalPlanNode) -> bool:
    """True when a subquery always yields exactly one row.

    A global (ungrouped) Aggregate emits one row even over empty input. A
    wrapping HAVING (Filter above the Aggregate) or any other top node could
    drop that row, so only a bare/projected ungrouped Aggregate qualifies.
    """
    node = plan
    if isinstance(node, Projection):
        node = node.input
    if not isinstance(node, Aggregate):
        return False
    return len(node.group_by) == 0


def _or_join(disjuncts: List[Expression]) -> Expression:
    """Combine disjuncts into a single OR expression; raise if empty."""
    result = combine_or(disjuncts)
    if result is None:
        raise DecorrelationError("Cannot build a predicate from no disjuncts")
    return result


def _expression_has_subquery(expr: Expression) -> bool:
    """Check whether an expression tree contains any subquery node."""
    if isinstance(expr, _SUBQUERY_NODE_TYPES):
        return True
    for child in _expression_children(expr):
        if _expression_has_subquery(child):
            return True
    return False


def _plan_expressions(plan: LogicalPlanNode) -> List[Expression]:
    """Every expression attached directly to a plan node.

    Delegates to the node's annotation-driven direct_expressions(), the single
    source of truth: every node type is covered without a per-node list here,
    and a field whose type cannot be classified raises rather than being
    silently skipped. The correlation guards (_is_correlated,
    _validate_no_outer_left, _raise_if_subquery_expression) walk this.
    """
    return plan.direct_expressions()


def _values_expressions(plan: Values) -> List[Expression]:
    """Flatten all row expressions of a Values node."""
    flattened: List[Expression] = []
    for row in plan.rows:
        flattened.extend(row)
    return flattened


def _values_has_subquery(plan: Values) -> bool:
    """Whether any Values row expression contains a subquery node."""
    for expr in _values_expressions(plan):
        if _expression_has_subquery(expr):
            return True
    return False


def _collect_inner_aliases(plan: LogicalPlanNode) -> Set[str]:
    """Collect relation aliases/names defined anywhere inside a plan.

    A distinct explicit alias hides the base table name (SQL scoping), so
    only the alias is added in that case; the base name is added only when
    the relation is unaliased.

    This is the TRANSITIVE collector: it recurses into a SubqueryScan's body to
    see every inner relation name (correlation analysis needs them). It is NOT
    the same as physical_planner._collect_relation_aliases, which stops at a
    SubqueryScan boundary because SQL scoping hides a subquery's internals from
    the outside; do not merge the two.
    """
    names: Set[str] = set()
    if isinstance(plan, Scan):
        if plan.alias and plan.alias != plan.table_name:
            names.add(plan.alias)
        else:
            names.add(plan.table_name)
    if isinstance(plan, SubqueryScan):
        names.add(plan.alias)
    if isinstance(plan, CTERef):
        names.add(plan.alias if plan.alias else plan.name)
    for child in plan.children():
        names.update(_collect_inner_aliases(child))
    return names


def _replace_column_refs(
    expr: Expression, mapping: Dict[Tuple[Optional[str], str], ColumnRef]
) -> Expression:
    """Rebuild an expression replacing mapped column references."""
    if isinstance(expr, ColumnRef):
        replacement = mapping.get((expr.table, expr.column))
        return replacement if replacement is not None else expr
    return _rebuild_expression(expr, lambda child: _replace_column_refs(child, mapping))


def _is_null_check(expr: Expression) -> UnaryOp:
    """Build an IS NULL check for an expression."""
    # The IS NULL predicate wrapping the operand, used to make NOT IN and ALL
    # anti-join conditions NULL-aware so an UNKNOWN comparison drops the row.
    return UnaryOp.create(op=UnaryOpType.IS_NULL, operand=expr)


class _PreparedSubquery(StateModel):
    """A subquery transformed into a join-ready right side.

    plan exposes deterministically renamed output columns; condition holds
    the rewritten correlation conjuncts (None when uncorrelated); values
    are the renamed output column names usable in comparisons.
    """

    plan: LogicalPlanNode
    condition: Optional[Expression]
    values: List[str]
    alias: str

    @classmethod
    def create(
        cls,
        *,
        plan: LogicalPlanNode,
        condition: Optional[Expression],
        values: List[str],
        alias: str,
    ) -> "_PreparedSubquery":
        """Sanctioned fresh-construction path for _PreparedSubquery.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here.
        alias is the subquery's derived-relation alias (its prefix), the
        qualifier its output columns carry once wrapped in a SubqueryScan."""
        return cls(
            plan=plan,
            condition=condition,
            values=values,
            alias=alias,
        )


class _PreparedScalar(StateModel):
    """A scalar subquery's join side plus its replacement expression."""

    plan: LogicalPlanNode
    condition: Optional[Expression]
    replacement: Expression

    @classmethod
    def create(
        cls,
        *,
        plan: LogicalPlanNode,
        condition: Optional[Expression],
        replacement: Expression,
    ) -> "_PreparedScalar":
        """Sanctioned fresh-construction path for _PreparedScalar.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            plan=plan,
            condition=condition,
            replacement=replacement,
        )


class _SubqueryPreparer:
    """Transforms one subquery plan into a join-ready right side.

    Correlated predicates are pulled out of the subquery's filters (and
    legally through aggregates and limits), inner columns they reference
    are exposed under deterministic ``<prefix>_*`` names, and the caller
    receives the rewritten plan plus the join condition.
    """

    def __init__(self, decorrelator: "Decorrelator", prefix: str):
        """Capture the owning decorrelator and this subquery's name prefix."""
        self.decorrelator = decorrelator
        self.prefix = prefix
        self.inner_aliases: Set[str] = set()
        self.pulled: List[Expression] = []
        self.pending_limit: Optional[int] = None
        # ORDER BY a recorded LIMIT caps, captured for the per-key limit so a
        # correlated "ORDER BY ... LIMIT n" keeps the first n rows per key.
        self.pending_order: Optional[Sort] = None

    def _wrap_boundary(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Expose the prepared subquery under its prefix alias as a derived relation."""
        # The assembled subquery plan exposed under its deterministic prefix
        # alias, so outer references to its output columns qualify to that
        # relation exactly like a FROM (SELECT ...) derived table's alias.
        return SubqueryScan.create(input=plan, alias=self.prefix)

    def _finalize(self, prepared: _PreparedSubquery) -> _PreparedSubquery:
        """Wrap a prepared subquery at its alias boundary.

        The single choke point where an IN/ANY/EXISTS subquery becomes a derived
        relation: its plan is wrapped in the prefix-aliased SubqueryScan. Its
        correlation condition already refers to the subquery's output columns
        with the prefix qualifier (set where those refs are constructed), so no
        unqualified subquery ref escapes into the outer plan.
        """
        return prepared.model_copy(
            update={"plan": self._wrap_boundary(prepared.plan)}
        )

    def _finalize_scalar(
        self,
        guarded: LogicalPlanNode,
        condition: Optional[Expression],
        replacement: Expression,
    ) -> "_PreparedScalar":
        """Wrap a scalar subquery at its alias boundary as a derived relation.

        The scalar value plan (already cardinality-guarded) becomes a derived
        relation under the prefix alias. The correlation condition and the outer
        replacement expression already carry the prefix qualifier on the
        subquery's own output columns (set at construction), so nothing
        unqualified escapes into the outer plan.
        """
        # The scalar subquery as a join-ready derived relation: its guarded value
        # plan wrapped at the prefix-aliased boundary, plus the already-qualified
        # correlation condition and outer replacement expression.
        return _PreparedScalar.create(
            plan=self._wrap_boundary(guarded),
            condition=condition,
            replacement=replacement,
        )

    def prepare_exists(self, subquery: LogicalPlanNode) -> _PreparedSubquery:
        """Prepare an EXISTS subquery: only correlation columns matter."""
        plan = self.decorrelator._rewrite_plan(subquery)
        self.inner_aliases = _collect_inner_aliases(plan)
        if not self._is_correlated(plan):
            # Uncorrelated EXISTS only asks whether any row exists, so the join
            # right side is the subplan capped at one row with no correlation
            # condition; LIMIT 1 keeps the SEMI join input minimal.
            capped = _PreparedSubquery.create(
                plan=Limit.create(input=plan, limit=1, offset=0),
                condition=None,
                values=[],
                alias=self.prefix,
            )
            return self._finalize(capped)
        core = self._peel_exists_top(plan)
        stripped = self._strip(core)
        return self._finalize(self._assemble(stripped, [], []))

    def _peel_exists_top(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Strip layers that do not affect existence (SELECT list, ORDER, LIMIT).

        EXISTS only asks whether any row qualifies, so a top projection, sort,
        or row-limit is a semantic no-op and is removed before the correlated
        filter spine is stripped.
        """
        if isinstance(plan, (Projection, Sort, Limit)):
            return self._peel_exists_top(plan.input)
        return plan

    def prepare_values(self, subquery: LogicalPlanNode) -> _PreparedSubquery:
        """Prepare an IN/ANY/ALL subquery: expose its value columns."""
        plan = self.decorrelator._rewrite_plan(subquery)
        self.inner_aliases = _collect_inner_aliases(plan)
        allow_wrappers = not self._is_correlated(plan)
        core, value_exprs = self._peel_values_top(plan, allow_wrappers)
        for value_expr in value_exprs:
            if self._references_outer(value_expr):
                raise DecorrelationError(
                    "Outer reference in a subquery's output column is " "not supported"
                )
        stripped = self._strip(core)
        value_names = []
        for index in range(len(value_exprs)):
            value_names.append(f"{self.prefix}_v{index}")
        return self._finalize(self._assemble(stripped, value_exprs, value_names))

    def prepare_scalar(self, subquery: LogicalPlanNode) -> _PreparedScalar:
        """Prepare a scalar subquery: single value plus cardinality rules."""
        plan = self.decorrelator._rewrite_plan(subquery)
        self.inner_aliases = _collect_inner_aliases(plan)
        plan = self._peel_scalar_limit_order(plan)
        if isinstance(plan, Aggregate):
            return self._prepare_scalar_aggregate(plan)
        return self._prepare_scalar_row(plan)

    def _peel_scalar_limit_order(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Peel a top LIMIT and the ORDER BY it caps into pending state.

        limit: the LIMIT count and the ORDER BY are recorded here and reattached
        as an order-aware GroupedLimit once the join side is assembled.
        """
        if isinstance(plan, Limit):
            self._record_limit(plan)
            plan = plan.input
        if self.pending_limit is not None and isinstance(plan, Sort):
            self.pending_order = plan
            plan = plan.input
        return plan

    def _record_limit(self, limit: Limit) -> None:
        """Record a subquery-level LIMIT for later placement."""
        if limit.offset:
            raise DecorrelationError("OFFSET in a correlated subquery is not supported")
        if self.pending_limit is not None:
            raise DecorrelationError("Nested LIMITs in a subquery are not supported")
        self.pending_limit = limit.limit

    def _prepare_scalar_aggregate(self, plan: Aggregate) -> _PreparedScalar:
        """Scalar over an aggregate: hoist aggregate calls, join on keys."""
        if len(plan.aggregates) != 1:
            raise DecorrelationError("Scalar subquery must return exactly one column")
        hoisted: List[Tuple[FunctionCall, str]] = []
        replacement = self._hoist_aggregate_calls(plan.aggregates[0], hoisted)
        if len(hoisted) == 0:
            raise DecorrelationError(
                "Scalar aggregate subquery has no aggregate function"
            )
        original_grouped = len(plan.group_by) > 0
        core = self._rebuild_hoisted_aggregate(plan, hoisted)
        stripped = self._strip(core)
        value_exprs, value_names = self._hoisted_value_refs(hoisted)
        prepared = self._assemble(stripped, value_exprs, value_names)
        guarded = self._guard_scalar(prepared, needs_guard=original_grouped)
        # The prepared scalar-aggregate side: the guarded per-key aggregate plan
        # wrapped at its alias boundary, its qualified correlation condition, and
        # the replacement expression (hoisted aggregate refs) that stands in for
        # the subquery in the outer row, qualified to the subquery alias.
        return self._finalize_scalar(
            guarded, prepared.condition, replacement
        )

    def _hoisted_value_refs(
        self, hoisted: List[Tuple[FunctionCall, str]]
    ) -> Tuple[List[Expression], List[str]]:
        """Build projection expressions/names for hoisted aggregates."""
        value_exprs: List[Expression] = []
        value_names: List[str] = []
        for _, name in hoisted:
            # A reference to one hoisted aggregate's renamed output column, so the
            # projection above the join can select the aggregate value by name.
            value_exprs.append(ColumnRef.create(table=None, column=name))
            value_names.append(name)
        return value_exprs, value_names

    def _hoist_aggregate_calls(
        self, expr: Expression, hoisted: List[Tuple[FunctionCall, str]]
    ) -> Expression:
        """Replace aggregate calls with renamed refs, collecting them.

        COUNT refs are wrapped in COALESCE(.., 0): after the LEFT join an
        absent group must read as zero, matching SQL COUNT semantics.
        """
        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            name = f"{self.prefix}_v{len(hoisted)}"
            hoisted.append((expr, name))
            # A reference to the renamed aggregate output that replaces the call
            # in place, so the outer expression reads the post-join column value.
            # Qualified to the subquery alias: this ref lands in the outer
            # expression, above the alias boundary the subquery is wrapped in.
            ref = ColumnRef.create(table=self.prefix, column=name)
            if expr.function_name.upper() == "COUNT":
                # A zero literal for the COALESCE default: after the LEFT join an
                # absent group has no COUNT row, and SQL COUNT must read as zero.
                zero = Literal.create(value=0, data_type=DataType.INTEGER)
                # COALESCE(count_ref, 0) so a missing (non-matching) group yields
                # zero rather than NULL, matching SQL COUNT semantics exactly.
                return FunctionCall.create(function_name="COALESCE", args=[ref, zero])
            return ref
        return _rebuild_expression(
            expr, lambda child: self._hoist_aggregate_calls(child, hoisted)
        )

    def _rebuild_hoisted_aggregate(
        self, plan: Aggregate, hoisted: List[Tuple[FunctionCall, str]]
    ) -> Aggregate:
        """Rebuild the aggregate so it outputs only pure aggregate calls."""
        aggregates: List[Expression] = []
        names: List[str] = []
        for call, name in hoisted:
            aggregates.append(call)
            names.append(name)
        return plan.model_copy(
            update={
                "group_by": list(plan.group_by),
                "aggregates": aggregates,
                "output_names": names,
            }
        )

    def _prepare_scalar_row(self, plan: LogicalPlanNode) -> _PreparedScalar:
        """Scalar over plain rows: project the value, guard cardinality."""
        core, value_exprs = self._peel_values_top(plan)
        if len(value_exprs) != 1:
            raise DecorrelationError("Scalar subquery must return exactly one column")
        self._reject_outer_refs_in_value(value_exprs[0])
        stripped = self._strip(core)
        value_name = f"{self.prefix}_v0"
        prepared = self._assemble(stripped, value_exprs, [value_name])
        guarded = self._guard_scalar(prepared, needs_guard=self.pending_limit != 1)
        # The prepared plain-row scalar side: the cardinality-guarded value plan
        # wrapped at its alias boundary, its qualified correlation condition, and
        # a reference to the single projected value column - qualified to the
        # subquery alias - that replaces the subquery in the outer expression.
        return self._finalize_scalar(
            guarded,
            prepared.condition,
            ColumnRef.create(table=self.prefix, column=value_name),
        )

    def _reject_outer_refs_in_value(self, expr: Expression) -> None:
        """Outer references in a non-aggregated scalar value are unsupported."""
        for ref in _expression_column_refs(expr):
            if ref.table is not None and ref.table not in self.inner_aliases:
                raise DecorrelationError(
                    "Outer reference in a non-aggregated scalar subquery "
                    "value is not supported"
                )

    def _guard_scalar(
        self, prepared: _PreparedSubquery, needs_guard: bool
    ) -> LogicalPlanNode:
        """Wrap the scalar side in a runtime cardinality guard if needed."""
        if not needs_guard:
            return prepared.plan
        keys: List[Expression] = []
        for name in self._key_names_of(prepared):
            # A reference to one correlation-key column that groups the guard, so
            # the runtime cardinality check is applied per outer-row key group.
            keys.append(ColumnRef.create(table=None, column=name))
        # The runtime cardinality guard: raises CardinalityViolationError if any
        # key group has more than one row, since a scalar subquery must be single
        # valued and this side could not be proven so statically.
        return SingleRowGuard.create(input=prepared.plan, keys=keys)

    def _key_names_of(self, prepared: _PreparedSubquery) -> List[str]:
        """The renamed correlation key columns exposed by the plan."""
        names = []
        for name in prepared.plan.schema():
            if name not in prepared.values:
                names.append(name)
        return names

    def _peel_values_top(
        self, plan: LogicalPlanNode, allow_wrappers: bool = False
    ) -> Tuple[LogicalPlanNode, List[Expression]]:
        """Split a subquery into its core and its output value expressions.

        ``allow_wrappers`` keeps a Sort or HAVING filter as part of the value
        relation; it is enabled only for uncorrelated value subqueries, where
        an ORDER BY/LIMIT or HAVING applies to the whole result set. A
        correlated subquery would need per-key handling the engine lacks, so it
        keeps failing fast rather than producing a wrong answer.
        """
        if isinstance(plan, Limit):
            self._record_limit(plan)
            return self._peel_values_top(plan.input, allow_wrappers)
        if isinstance(plan, Projection):
            self._reject_star_values(plan.expressions)
            return plan.input, list(plan.expressions)
        if isinstance(plan, Values):
            return self._peel_values_node(plan)
        if isinstance(plan, Aggregate):
            return plan, self._aggregate_value_refs(plan)
        if allow_wrappers and isinstance(plan, (Sort, Filter)):
            return self._peel_values_wrapper(plan)
        if allow_wrappers and isinstance(plan, SetOperation):
            return plan, self._relation_value_refs(plan)
        raise DecorrelationError(
            f"Unsupported subquery output shape: {type(plan).__name__}"
        )

    def _peel_values_wrapper(
        self, plan: LogicalPlanNode
    ) -> Tuple[LogicalPlanNode, List[Expression]]:
        """Peel a Sort or HAVING filter, keeping it wrapped around the core.

        An ORDER BY/LIMIT or a HAVING clause shapes the subquery's result set,
        of the value relation while the value columns are taken from below it.
        """
        inner_core, value_exprs = self._peel_values_top(plan.input, True)
        return plan.model_copy(update={"input": inner_core}), value_exprs

    def _reject_star_values(self, expressions: List[Expression]) -> None:
        """A star projection has no determinate value columns."""
        for expr in expressions:
            if isinstance(expr, ColumnRef) and expr.column == "*":
                raise DecorrelationError(
                    "SELECT * subqueries cannot be used as value subqueries"
                )

    def _peel_values_node(
        self, plan: Values
    ) -> Tuple[LogicalPlanNode, List[Expression]]:
        """A constant Values subquery is its own single-row core."""
        if len(plan.rows) != 1:
            raise DecorrelationError("Multi-row VALUES subqueries not supported")
        refs: List[Expression] = []
        for name in plan.output_names:
            # A reference to one Values output column, exposing the constant row's
            # value under its own name so it can be matched by the enclosing IN.
            refs.append(ColumnRef.create(table=None, column=name))
        return plan, refs

    def _aggregate_value_refs(self, plan: Aggregate) -> List[Expression]:
        """References to an aggregate subquery's output columns."""
        refs: List[Expression] = []
        for name in plan.output_names:
            # A reference to one aggregate output column, so the aggregate result
            # can serve as the comparison value of the enclosing IN/ANY/ALL.
            refs.append(ColumnRef.create(table=None, column=name))
        return refs

    def _relation_value_refs(self, plan: LogicalPlanNode) -> List[Expression]:
        """References to a relation's output columns (e.g. a set-operation body)."""
        refs: List[Expression] = []
        for name in plan.schema():
            # A reference to one output column of the relation (e.g. a set
            # operation), exposing its schema columns as comparison values.
            refs.append(ColumnRef.create(table=None, column=name))
        return refs

    def _is_correlated(self, plan: LogicalPlanNode) -> bool:
        """Check whether any expression references an outer relation."""
        for expr in _plan_expressions(plan):
            if self._references_outer(expr):
                return True
        for child in plan.children():
            if self._is_correlated(child):
                return True
        return False

    def _strip(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Remove correlated predicates from the subquery's filter spine."""
        if isinstance(plan, Filter):
            return self._strip_filter(plan)
        if isinstance(plan, Aggregate):
            return self._strip_aggregate(plan)
        if isinstance(plan, Limit):
            return self._strip_limit(plan)
        return self._strip_other(plan)

    def _strip_other(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Pass through sort nodes; stop at scans, joins, and projections."""
        if isinstance(plan, Sort):
            stripped = self._strip(plan.input)
            # The same ORDER BY re-seated on the correlation-stripped input; this
            # rebuilds an existing Sort, so model_copy preserves its keys and
            # direction and only swaps the input, never dropping a field.
            return plan.model_copy(update={"input": stripped})
        return plan

    def _strip_filter(self, plan: Filter) -> LogicalPlanNode:
        """Pull correlated conjuncts out of a filter."""
        stripped_input = self._strip(plan.input)
        kept: List[Expression] = []
        pulled_here: List[Expression] = []
        for conjunct in _split_conjuncts(plan.predicate):
            if self._references_outer(conjunct):
                pulled_here.append(conjunct)
            else:
                kept.append(conjunct)
        if isinstance(stripped_input, Aggregate):
            stripped_input, kept, pulled_here = self._hoist_having(
                stripped_input, kept, pulled_here
            )
        for conjunct in pulled_here:
            self.pulled.append(conjunct)
        if len(kept) == 0:
            return stripped_input
        # The residual filter holding only the non-correlated conjuncts; the
        # correlated ones were pulled out to become the join condition, so what
        # remains is the local predicate the subquery still enforces internally.
        return Filter.create(input=stripped_input, predicate=_and_join(kept))

    def _hoist_having(
        self,
        aggregate: Aggregate,
        kept: List[Expression],
        pulled: List[Expression],
    ) -> Tuple[Aggregate, List[Expression], List[Expression]]:
        """Materialize HAVING aggregate calls as aggregate outputs.

        A HAVING predicate above an aggregate may reference aggregate
        functions absent from the aggregate's output list. A filter (kept)
        or a pulled correlation condition cannot recompute them per row, so
        """
        aggregates = list(aggregate.aggregates)
        names = list(aggregate.output_names)
        rewritten_kept = self._hoist_each(kept, aggregates, names)
        rewritten_pulled = self._hoist_each(pulled, aggregates, names)
        widened = aggregate.model_copy(
            update={"aggregates": aggregates, "output_names": names}
        )
        return widened, rewritten_kept, rewritten_pulled

    def _hoist_each(
        self,
        conjuncts: List[Expression],
        aggregates: List[Expression],
        names: List[str],
    ) -> List[Expression]:
        """Hoist aggregate calls out of each conjunct into the output list."""
        rewritten: List[Expression] = []
        for conjunct in conjuncts:
            rewritten.append(self._hoist_having_expr(conjunct, aggregates, names))
        return rewritten

    def _hoist_having_expr(
        self,
        expr: Expression,
        aggregates: List[Expression],
        names: List[str],
    ) -> Expression:
        """Replace one aggregate call with a reference to a new output."""
        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            name = f"{self.prefix}_h{len(names)}"
            aggregates.append(expr)
            names.append(name)
            # A reference to the HAVING aggregate now materialized as an aggregate
            # output; the predicate reads it by name instead of recomputing the
            # aggregate call, which a post-aggregate filter cannot do per row.
            return ColumnRef.create(table=None, column=name)
        return _rebuild_expression(
            expr, lambda child: self._hoist_having_expr(child, aggregates, names)
        )

    def _references_outer(self, expr: Expression) -> bool:
        """Check whether an expression references an outer relation."""
        for ref in _expression_column_refs(expr):
            if ref.table is not None and ref.table not in self.inner_aliases:
                return True
        return False

    def _strip_aggregate(self, plan: Aggregate) -> LogicalPlanNode:
        """Pull correlated key equalities through an aggregate.

        Pulling a predicate above an aggregate is only legal when it is an
        equality on a column the aggregate groups by (or the aggregate is
        global, in which case the column becomes a new grouping key); the
        per-group partitioning then matches the per-outer-row evaluation.
        """
        before = len(self.pulled)
        stripped_input = self._strip(plan.input)
        crossing = self.pulled[before:]
        if len(crossing) == 0:
            return plan.model_copy(update={"input": stripped_input})
        return self._widen_aggregate(plan, stripped_input, crossing, before)

    def _widen_aggregate(
        self,
        plan: Aggregate,
        stripped_input: LogicalPlanNode,
        crossing: List[Expression],
        start: int,
    ) -> Aggregate:
        """Add correlation keys to an aggregate and rename predicates."""
        group_by = list(plan.group_by)
        had_group_by = len(plan.group_by) > 0
        aggregates = list(plan.aggregates)
        names = list(plan.output_names)
        for offset in range(len(crossing)):
            inner_ref, outer_side = self._key_equality(crossing[offset])
            self._add_group_key(group_by, inner_ref, had_group_by)
            key_name = f"{self.prefix}_g{start + offset}"
            aggregates.append(inner_ref)
            names.append(key_name)
            # A reference to the correlation key now emitted as a grouped output
            # of the widened aggregate, so the pulled predicate can join on it.
            # Qualified to the subquery alias: this ref lives in the correlation
            # condition, which is evaluated at the join above the alias boundary.
            renamed = ColumnRef.create(table=self.prefix, column=key_name)
            # The rewritten correlation equality (outer side = renamed inner key),
            # relocated above the aggregate to become part of the join condition
            # now that the key is exposed per group.
            self.pulled[start + offset] = BinaryOp.create(
                op=BinaryOpType.EQ, left=outer_side, right=renamed
            )
        return plan.model_copy(
            update={
                "input": stripped_input,
                "group_by": group_by,
                "aggregates": aggregates,
                "output_names": names,
            }
        )

    def _key_equality(self, predicate: Expression) -> Tuple[ColumnRef, Expression]:
        """Decompose a predicate into (inner key column, outer side)."""
        if not isinstance(predicate, BinaryOp) or predicate.op != BinaryOpType.EQ:
            raise NonFlattenableCorrelation(
                "Only equality correlation predicates can cross an "
                f"aggregate or limit, got: {predicate.to_sql()}"
            )
        sides = self._classify_equality_sides(predicate)
        if sides is None:
            raise DecorrelationError(
                "Correlation predicate must compare an inner column with an "
                f"outer expression: {predicate.to_sql()}"
            )
        return sides

    def _classify_equality_sides(
        self, predicate: BinaryOp
    ) -> Optional[Tuple[ColumnRef, Expression]]:
        """Find which equality side is the pure inner key column."""
        if self._is_inner_column(predicate.left) and not self._references_inner(
            predicate.right
        ):
            return predicate.left, predicate.right
        if self._is_inner_column(predicate.right) and not self._references_inner(
            predicate.left
        ):
            return predicate.right, predicate.left
        return None

    def _is_inner_column(self, expr: Expression) -> bool:
        """Check for a single column reference into the subquery."""
        if not isinstance(expr, ColumnRef):
            return False
        return expr.table is not None and expr.table in self.inner_aliases

    def _references_inner(self, expr: Expression) -> bool:
        """Check whether an expression touches any inner relation."""
        for ref in _expression_column_refs(expr):
            if ref.table is None or ref.table in self.inner_aliases:
                return True
        return False

    def _add_group_key(
        self, group_by: List[Expression], key: ColumnRef, had_group_by: bool
    ) -> None:
        """Add a correlation key to the grouping, validating legality.

        A scalar-aggregate subquery (no original GROUP BY) may correlate on
        several keys; each is added as a grouping key. But when the subquery
        already had its own GROUP BY, a correlation key not among those keys
        cannot be added - it would change the aggregation granularity - so it
        raises. ``had_group_by`` is the ORIGINAL state, not the growing list.
        """
        for existing in group_by:
            if (
                isinstance(existing, ColumnRef)
                and existing.table == key.table
                and existing.column == key.column
            ):
                return
        if had_group_by:
            raise DecorrelationError(
                f"Correlation key {key.to_sql()} is not part of the "
                "subquery's GROUP BY"
            )
        group_by.append(key)

    def _strip_limit(self, plan: Limit) -> LogicalPlanNode:
        """Convert a correlated inner LIMIT into a pending per-key limit."""
        before = len(self.pulled)
        stripped_input = self._strip(plan.input)
        if len(self.pulled) == before:
            # No correlation crossed this LIMIT, so it stays a plain row-limit on
            # the stripped input; nothing needs per-key handling here.
            return Limit.create(
                input=stripped_input, limit=plan.limit, offset=plan.offset
            )
        self._record_limit(plan)
        return stripped_input

    def _assemble(
        self,
        core: LogicalPlanNode,
        value_exprs: List[Expression],
        value_names: List[str],
    ) -> _PreparedSubquery:
        """Build the renamed right side and the join condition."""
        exposures, condition = self._expose_correlation_columns()
        self._validate_no_outer_left(core)
        value_exprs = self._partition_window_values(value_exprs, exposures)
        expressions: List[Expression] = []
        aliases: List[str] = []
        for expr, alias in exposures:
            expressions.append(expr)
            aliases.append(alias)
        expressions.extend(value_exprs)
        aliases.extend(value_names)
        if len(expressions) == 0:
            raise DecorrelationError("Subquery rewrite produced no output columns")
        non_keys = list(value_names)
        order = self._build_order(expressions, aliases, non_keys)
        # The renamed right side of the join: a projection exposing the exposed
        # correlation-key columns and the subquery value columns under their
        # deterministic aliases, so the outer join can reference them by name.
        plan: LogicalPlanNode = Projection.create(
            input=core, expressions=expressions, aliases=aliases
        )
        plan = self._apply_pending_limit(plan, aliases, non_keys, order)
        # The assembled join-ready subquery: its renamed plan, the pulled-out
        # correlation condition (None when uncorrelated), and the value column
        # names the caller compares against. plan/condition are still raw here;
        # the caller's _finalize wraps the plan at the alias boundary and
        # qualifies the condition (the scalar path guards the raw plan first).
        return _PreparedSubquery.create(
            plan=plan, condition=condition, values=value_names, alias=self.prefix
        )

    def _partition_window_values(self, value_exprs, exposures):
        """Partition any window value by the correlation key columns.

        A window inside a correlated subquery is implicitly partitioned by the
        correlation: once the equality filter is pulled out, the window must
        ``PARTITION BY`` those key columns so each outer row still sees only its
        own group. Without this the window would range over the whole scan.
        """
        keys = []
        for expr, _ in exposures:
            keys.append(expr)
        rewritten = []
        for expr in value_exprs:
            rewritten.append(self._partition_window_expr(expr, keys))
        return rewritten

    def _partition_window_expr(self, expr, keys):
        """Prepend correlation keys to every WindowExpr partition in the tree.

        A window nested inside an expression (``rank() OVER (...) + 1``) must be
        partitioned too, so the tree is rebuilt and every WindowExpr at any depth
        is lifted, not just a top-level one.
        """
        if len(keys) == 0:
            return expr
        rebuilt = _rebuild_expression(
            expr, lambda child: self._partition_window_expr(child, keys)
        )
        if not isinstance(rebuilt, WindowExpr):
            return rebuilt
        self._reject_non_equi_window()
        return rebuilt.model_copy(
            update={"partition_by": list(keys) + list(rebuilt.partition_by)}
        )

    def _reject_non_equi_window(self) -> None:
        """A correlated window only lifts to PARTITION BY under equi-correlation."""
        for predicate in self.pulled:
            if not isinstance(predicate, BinaryOp) or predicate.op != BinaryOpType.EQ:
                raise DecorrelationError(
                    "Only equality correlation can partition a window; "
                    f"got: {predicate.to_sql()}"
                )

    def _build_order(self, expressions, aliases, non_keys):
        """Map a pending ORDER BY onto projected aliases, projecting any missing.

        Returns ``(order_keys, ascending, nulls)`` referencing output aliases, or
        None. An ordering column that is not already selected is added as an
        extra projected output (and marked a non-key so it is not grouped on).
        """
        if self.pending_order is None:
            return None
        order_keys: List[Expression] = []
        for sort_key in self.pending_order.sort_keys:
            alias = self._order_alias(sort_key, expressions, aliases, non_keys)
            # A reference to the projected alias of one ORDER BY key, remapping the
            # pending sort onto the renamed outputs so the per-key limit can order
            # rows by columns that now live in the assembled projection.
            order_keys.append(ColumnRef.create(table=None, column=alias))
        return (
            order_keys,
            self.pending_order.ascending,
            self.pending_order.nulls_order,
        )

    def _order_alias(self, sort_key, expressions, aliases, non_keys) -> str:
        """Alias of a projected ORDER BY column, projecting it as an extra if absent."""
        target = sort_key.to_sql()
        for index in range(len(expressions)):
            if expressions[index].to_sql() == target:
                return aliases[index]
        alias = f"{self.prefix}_o{len(aliases)}"
        expressions.append(sort_key)
        aliases.append(alias)
        non_keys.append(alias)
        return alias

    def _expose_correlation_columns(
        self,
    ) -> Tuple[List[Tuple[Expression, str]], Optional[Expression]]:
        """Rename pulled predicates' inner columns and build the condition."""
        exposures: List[Tuple[Expression, str]] = []
        exposure_names: Dict[Tuple[Optional[str], str], ColumnRef] = {}
        rewritten: List[Expression] = []
        for predicate in self.pulled:
            self._expose_predicate_refs(predicate, exposures, exposure_names)
            rewritten.append(_replace_column_refs(predicate, exposure_names))
        if len(rewritten) == 0:
            return exposures, None
        return exposures, _and_join(rewritten)

    def _expose_predicate_refs(
        self,
        predicate: Expression,
        exposures: List[Tuple[Expression, str]],
        exposure_names: Dict[Tuple[Optional[str], str], ColumnRef],
    ) -> None:
        """Allocate renamed outputs for a predicate's inner columns."""
        for ref in _expression_column_refs(predicate):
            key = (ref.table, ref.column)
            if key in exposure_names:
                continue
            if self._is_renamed_ref(ref):
                # A key already widened into the aggregate below. Expose it by its
                # inner physical name (read from the aggregate output under the
                # boundary), aliased under that name; the condition keeps the
                # qualified ref, which resolves against this exposed column.
                inner = ColumnRef.create(table=None, column=ref.column)
                exposures.append((inner, ref.column))
                exposure_names[key] = ref
                continue
            if not self._is_inner_column(ref):
                continue
            name = f"{self.prefix}_k{len(exposures)}"
            exposures.append((ref, name))
            # The renamed reference that the pulled predicate's inner column maps
            # to; the correlation condition is rebuilt against this deterministic
            # name so it can be evaluated on the join's right side. Qualified to
            # the subquery alias, since the condition sits above the boundary.
            exposure_names[key] = ColumnRef.create(table=self.prefix, column=name)

    def _is_renamed_ref(self, ref: ColumnRef) -> bool:
        """Whether a ref is one this subquery already exposed as its own output.

        Identified by its qualifier: a ref carrying this subquery's alias is one
        we minted for the correlation condition (aggregate widening), not an
        inner column still to be exposed. Matched by qualifier, never by
        string-matching the column name.
        """
        return ref.table == self.prefix

    def _validate_no_outer_left(self, plan: LogicalPlanNode) -> None:
        """Fail if correlated references remain anywhere in the subplan."""
        for expr in _plan_expressions(plan):
            for ref in _expression_column_refs(expr):
                if self._is_leftover_outer(ref):
                    raise DecorrelationError(
                        f"Correlated reference {ref.to_sql()} is in an "
                        "unsupported position"
                    )
        for child in plan.children():
            self._validate_no_outer_left(child)

    def _is_leftover_outer(self, ref: ColumnRef) -> bool:
        """An unextracted outer reference left inside the subquery."""
        if ref.table is None:
            return False
        return ref.table not in self.inner_aliases

    def _apply_pending_limit(
        self,
        plan: LogicalPlanNode,
        aliases: List[str],
        non_keys: List[str],
        order,
    ) -> LogicalPlanNode:
        """Re-attach a recorded subquery LIMIT, per-key when correlated."""
        if self.pending_limit is None:
            return plan
        keys: List[Expression] = []
        for alias in aliases:
            if alias not in non_keys:
                # A reference to one correlation-key column that partitions the
                # per-key limit, so each outer row's group is limited on its own.
                keys.append(ColumnRef.create(table=None, column=alias))
        if len(keys) == 0:
            return self._unkeyed_limit(plan, order)
        self._require_equi_correlation_for_limit()
        return self._keyed_limit(plan, keys, order)

    def _require_equi_correlation_for_limit(self) -> None:
        """A per-key LIMIT is only correct under equi-correlation.

        Each outer row must map to exactly one key group; a non-equality
        correlation (``p.price < o.price``) matches many groups, so a per-key
        limit would not be a per-outer-row limit. Such a shape needs a general
        dependent join and is rejected rather than answered wrongly.
        """
        for predicate in self.pulled:
            if not isinstance(predicate, BinaryOp) or predicate.op != BinaryOpType.EQ:
                raise NonFlattenableCorrelation(
                    "Only equality correlation predicates can cross an "
                    f"aggregate or limit, got: {predicate.to_sql()}"
                )

    def _unkeyed_limit(self, plan: LogicalPlanNode, order) -> LogicalPlanNode:
        """A plain LIMIT, ordered when the subquery had ORDER BY ... LIMIT."""
        if order is not None:
            # The ORDER BY that the recorded LIMIT caps, applied before the row
            # limit so "ORDER BY ... LIMIT n" keeps the intended first n rows.
            plan = Sort.create(
                input=plan, sort_keys=order[0], ascending=order[1], nulls_order=order[2]
            )
        # The whole-result row limit for an uncorrelated subquery, where there is
        # no correlation key to partition on, so a plain LIMIT is correct.
        return Limit.create(input=plan, limit=self.pending_limit, offset=0)

    def _keyed_limit(self, plan: LogicalPlanNode, keys, order) -> LogicalPlanNode:
        """A per-key (grouped) LIMIT, ordered within each key when requested."""
        if order is None:
            # A per-key row limit with no ordering: each correlation-key group is
            # capped at the subquery's LIMIT independently of the others.
            return GroupedLimit.create(input=plan, keys=keys, limit=self.pending_limit)
        # A per-key ordered limit: within each correlation-key group the rows are
        # ordered as the subquery's ORDER BY asked, then capped, so each outer row
        # keeps its own first n rows.
        return GroupedLimit.create(
            input=plan,
            keys=keys,
            limit=self.pending_limit,
            order_by_keys=order[0],
            order_by_ascending=order[1],
            order_by_nulls=order[2],
        )


# Output names produced by Neumann-Kemper unnesting; always qualified by a fresh
# per-subquery relation alias, so the bare names are safe.
_UNNEST_VALUE = "nk_value"
_UNNEST_RANK = "nk_rank"


class Decorrelator:
    """Decorrelates subqueries in bound logical plans."""

    def __init__(self) -> None:
        """Initialize the deterministic subquery name counter."""
        self._counter = 0

    def decorrelate(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Remove all subquery expressions from a bound plan.

        Args:
            plan: Bound logical plan with potential subqueries

        Returns:
            Equivalent plan built from joins, unions, and guards

        Raises:
            DecorrelationError: If a subquery cannot be decorrelated
        """
        rewritten = self._rewrite_plan(plan)
        self._raise_if_subquery_expression(rewritten)
        validate_scope(rewritten, "after decorrelation")
        return rewritten

    def _next_prefix(self) -> str:
        """Allocate the next deterministic subquery name prefix."""
        prefix = f"__subq_{self._counter}"
        self._counter += 1
        return prefix

    def _rewrite_plan(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Rewrite one plan node, dispatching by type."""
        if isinstance(plan, Filter):
            return self._rewrite_filter(plan)
        if isinstance(plan, Projection):
            return self._rewrite_projection(plan)
        if isinstance(plan, Sort):
            return self._rewrite_sort(plan)
        if isinstance(plan, LateralJoin):
            return self._rewrite_lateral_join(plan)
        if isinstance(plan, Join):
            return self._rewrite_join(plan)
        if isinstance(plan, Values):
            return self._rewrite_values(plan)
        return self._rewrite_rest(plan)

    def _rewrite_values(self, node: Values) -> LogicalPlanNode:
        """Decorrelate subqueries inside a FROM-less SELECT's Values row.

        ``SELECT EXISTS (...)`` parses to a one-row Values whose expression
        is a subquery. The Values row acts as the single outer row: each
        subquery is threaded onto it as a join and the rewritten expressions
        are re-projected under the original output names.
        """
        if not _values_has_subquery(node):
            return node
        if len(node.rows) != 1:
            raise DecorrelationError(
                "Multi-row VALUES with subqueries is not supported"
            )
        # Thread joins onto a clean one-row placeholder, not the original
        # Values: the original still carries the subquery expressions and
        # would re-introduce them as a join input. The single constant row is
        # the outer row that each SELECT-list subquery join hangs off of.
        plan: LogicalPlanNode = Values.create(
            rows=[[Literal.create(value=1, data_type=DataType.INTEGER)]],
            output_names=["__exists_base"],
        )
        expressions: List[Expression] = []
        for expr in node.rows[0]:
            rewritten, plan = self._rewrite_value_expr(expr, plan)
            expressions.append(rewritten)
        # Re-project the rewritten SELECT-list expressions under the original
        # output names, over the plan that now carries the subquery joins, so the
        # FROM-less SELECT presents exactly the columns the user asked for.
        return Projection.create(
            input=plan, expressions=expressions, aliases=list(node.output_names)
        )

    def _rewrite_rest(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Rewrite remaining node types by recursing into children.

        These nodes carry no rewrite of their own; recurse so a subquery nested
        deeper still gets decorrelated, then rebuild via the shared
        transform_children. An Aggregate first rejects subqueries in grouping or
        aggregate positions, which are unsupported.
        """
        if isinstance(plan, Aggregate):
            self._reject_subqueries_in(plan.group_by, "GROUP BY")
            self._reject_subqueries_in(plan.aggregates, "aggregate expressions")
        return transform_children(plan, self._rewrite_plan)

    def _reject_subqueries_in(self, expressions: List[Expression], where: str) -> None:
        """Subqueries in grouping/aggregation positions are unsupported."""
        for expr in expressions:
            if _expression_has_subquery(expr):
                raise DecorrelationError(f"Subqueries in {where} are not supported")

    def _rewrite_filter(self, node: Filter) -> LogicalPlanNode:
        """Rewrite a filter: joins for conjuncts, unions for disjunctions.

        A filter does not change its input's schema, but decorrelating a scalar
        subquery in the predicate adds the subquery's value column through a
        join. A HAVING (a filter over an aggregate) has no SELECT projection
        above it to drop that extra column, so its result is re-projected back to
        the aggregate's columns. A WHERE filter is left alone: its SELECT
        projection already trims, and re-projecting would block the correlation
        pull-up when the filter sits inside another subquery.
        """
        input_plan = self._rewrite_plan(node.input)
        if not _expression_has_subquery(node.predicate):
            # No subquery in the predicate, so the filter is preserved as-is over
            # the (possibly rewritten) input; only the child needed decorrelating.
            return Filter.create(input=input_plan, predicate=node.predicate)
        original_schema = input_plan.schema()
        rewritten = self._rewrite_filter_predicate(input_plan, node.predicate)
        if isinstance(input_plan, Aggregate):
            return self._restore_filter_schema(rewritten, original_schema)
        return rewritten

    def _rewrite_filter_predicate(
        self, input_plan: LogicalPlanNode, predicate: Expression
    ) -> LogicalPlanNode:
        """Rewrite a subquery-bearing predicate into joins (AND) or unions (OR)."""
        disjuncts = _split_disjuncts(predicate)
        if len(disjuncts) > 1:
            return self._expand_or(input_plan, disjuncts)
        return self._rewrite_filter_conjuncts(input_plan, predicate)

    def _restore_filter_schema(
        self, plan: LogicalPlanNode, names: List[str]
    ) -> LogicalPlanNode:
        """Re-project a subquery-joined filter back to its pre-join columns."""
        if plan.schema() == names:
            return plan
        expressions: List[Expression] = []
        for name in names:
            # A reference to one original output column by its physical name; the
            # projection drops the columns the scalar subquery's join added.
            expressions.append(ColumnRef.create(table=None, column=name))
        # Restore the filter's original output schema over the join-threaded plan,
        # so a HAVING subquery's join does not leak its value column downstream.
        return Projection.create(
            input=plan, expressions=expressions, aliases=list(names)
        )

    def _expand_or(
        self, input_plan: LogicalPlanNode, disjuncts: List[Expression]
    ) -> LogicalPlanNode:
        """Rewrite OR-of-subqueries as one filter over per-disjunct flags.

        A distinct union of per-branch filters would merge full-duplicate
        source rows that have no key, losing their multiplicity. Instead
        each subquery disjunct becomes a boolean flag column (a SEMI/ANTI
        split that keeps every input row exactly once), plain disjuncts stay
        as predicates, and the terms are OR'd in a single filter. In WHERE
        context, collapsing a subquery flag's UNKNOWN to FALSE is safe: an
        OR term that is UNKNOWN and one that is FALSE both leave the row to
        be dropped unless another term is TRUE.
        """
        plan = input_plan
        terms: List[Expression] = []
        for disjunct in disjuncts:
            term, plan = self._disjunct_term(disjunct, plan)
            terms.append(term)
        # One filter over the OR of every per-disjunct term, on the plan grown
        # with each disjunct's SEMI/ANTI flag join; this keeps every input row
        # exactly once instead of a distinct union that would lose duplicates.
        return Filter.create(input=plan, predicate=_or_join(terms))

    def _disjunct_term(
        self, disjunct: Expression, plan: LogicalPlanNode
    ) -> Tuple[Expression, LogicalPlanNode]:
        """Turn one OR disjunct into a boolean term over the grown plan."""
        constant = self._constant_exists_value(disjunct)
        if constant is not None:
            # A constant boolean term for an EXISTS over a global aggregate that is
            # always TRUE/FALSE; no join is needed, so the plan passes through.
            return Literal.create(value=constant, data_type=DataType.BOOLEAN), plan
        if isinstance(disjunct, _SUBQUERY_NODE_TYPES):
            return self._join_flag(disjunct, plan)
        if not _expression_has_subquery(disjunct):
            return disjunct, plan
        return self._rewrite_value_expr(disjunct, plan)

    def _rewrite_filter_conjuncts(
        self, input_plan: LogicalPlanNode, predicate: Expression
    ) -> LogicalPlanNode:
        """Apply each conjunct as a join or a residual filter predicate."""
        plan = input_plan
        kept: List[Expression] = []
        for conjunct in _split_conjuncts(predicate):
            plan, residual = self._apply_conjunct(plan, conjunct)
            if residual is not None:
                kept.append(residual)
        if len(kept) == 0:
            return plan
        # The residual filter for conjuncts that did not become joins; subquery
        # conjuncts were consumed as SEMI/ANTI joins, and only the remaining plain
        # predicates stay as a filter over the join-threaded plan.
        return Filter.create(input=plan, predicate=_and_join(kept))

    def _apply_conjunct(
        self, plan: LogicalPlanNode, conjunct: Expression
    ) -> Tuple[LogicalPlanNode, Optional[Expression]]:
        """Turn one conjunct into a join (consumed) or keep it filtered."""
        constant = self._constant_exists_value(conjunct)
        if constant is not None:
            # A constant boolean residual for an always-true/false EXISTS over a
            # global aggregate; it stays as a filter predicate, no join required.
            return plan, Literal.create(value=constant, data_type=DataType.BOOLEAN)
        negated = self._negated_subquery_conjunct(conjunct)
        if negated is not None:
            return self._apply_conjunct(plan, negated)
        if isinstance(conjunct, _SUBQUERY_NODE_TYPES):
            right, condition, positive = self._semi_anti_parts(conjunct)
            join_type = JoinType.SEMI if positive else JoinType.ANTI
            # The SEMI/ANTI join that enforces a boolean subquery conjunct: SEMI
            # keeps outer rows that have a match, ANTI keeps those that do not, so
            # the conjunct is fully consumed and leaves no residual predicate.
            return (
                Join.create(
                    left=plan, right=right, join_type=join_type, condition=condition
                ),
                None,
            )
        if not _expression_has_subquery(conjunct):
            return plan, conjunct
        rewritten, plan = self._rewrite_value_expr(conjunct, plan)
        return plan, rewritten

    def _negated_subquery_conjunct(self, conjunct: Expression) -> Optional[Expression]:
        """Push a ``NOT`` around a subquery predicate into the node itself.

        Routing ``NOT (<subquery predicate>)`` through the flag path would
        collapse UNKNOWN to FALSE and keep rows SQL must drop. Negating the
        subquery node (swapping SEMI/ANTI with null-aware conditions) gives
        the exact WHERE semantics instead.
        """
        if not isinstance(conjunct, UnaryOp) or conjunct.op != UnaryOpType.NOT:
            return None
        operand = conjunct.operand
        if not isinstance(operand, _SUBQUERY_NODE_TYPES):
            return None
        return self._negate_subquery_predicate(operand)

    def _negate_subquery_predicate(self, node: Expression) -> Expression:
        """Return the logical negation of a boolean subquery predicate."""
        if isinstance(node, ExistsExpression):
            # The negation of an EXISTS is the same subquery with its negated flag
            # flipped, turning EXISTS into NOT EXISTS (and back) exactly.
            return ExistsExpression.create(
                subquery=node.subquery, negated=not node.negated
            )
        if isinstance(node, InSubquery):
            # The negation of an IN predicate is the same value/subquery with the
            # negated flag flipped, giving NOT IN with its NULL-aware anti-join.
            return InSubquery.create(
                value=node.value, subquery=node.subquery, negated=not node.negated
            )
        if isinstance(node, QuantifiedComparison):
            return self._negate_quantified(node)
        raise DecorrelationError(
            f"Cannot negate subquery predicate: {type(node).__name__}"
        )

    def _negate_quantified(self, node: QuantifiedComparison) -> QuantifiedComparison:
        """Negate ``x op ANY/ALL S`` by De Morgan (flip operator and quantifier)."""
        operator = _NEGATED_BINARY_OP.get(node.operator)
        if operator is None:
            raise DecorrelationError(
                f"Cannot negate quantified operator: {node.operator.value}"
            )
        if node.quantifier in (Quantifier.ANY, Quantifier.SOME):
            quantifier = Quantifier.ALL
        else:
            quantifier = Quantifier.ANY
        # The De Morgan dual of the quantified comparison: NOT (x op ANY S) becomes
        # x neg_op ALL S (and vice versa), so both the operator and the quantifier
        # are flipped while the left value and subquery are unchanged.
        return QuantifiedComparison.create(
            operator=operator,
            quantifier=quantifier,
            left=node.left,
            subquery=node.subquery,
        )

    def _constant_exists_value(self, expr: Expression) -> Optional[bool]:
        """Resolve an EXISTS over a global aggregate to a constant.

        A global (ungrouped) aggregate with no HAVING yields exactly one row
        for every outer row, even over empty input, so the EXISTS is always
        TRUE (and NOT EXISTS always FALSE). Key-widening would instead turn
        it into a grouped aggregate whose empty groups vanish, making the
        SEMI join drop non-matching outer rows.
        """
        if not isinstance(expr, ExistsExpression):
            return None
        if not _is_unconditional_global_aggregate(expr.subquery):
            return None
        return not expr.negated

    def _semi_anti_parts(
        self, expr: Expression
    ) -> Tuple[LogicalPlanNode, Optional[Expression], bool]:
        """Build (right side, condition, positive?) for a boolean subquery.

        positive=True means matching rows satisfy the predicate (SEMI join
        keeps them); positive=False means matches are violations (ANTI join
        keeps the non-matching rows).
        """
        if isinstance(expr, ExistsExpression):
            prepared = self._preparer().prepare_exists(expr.subquery)
            return prepared.plan, prepared.condition, not expr.negated
        if isinstance(expr, InSubquery):
            return self._in_parts(expr)
        if isinstance(expr, QuantifiedComparison):
            return self._quantified_parts(expr)
        raise DecorrelationError(
            f"Unsupported boolean subquery expression: {type(expr).__name__}"
        )

    def _preparer(self) -> _SubqueryPreparer:
        """Create a preparer with a fresh deterministic name prefix."""
        return _SubqueryPreparer(self, self._next_prefix())

    def _in_parts(
        self, expr: InSubquery
    ) -> Tuple[LogicalPlanNode, Optional[Expression], bool]:
        """Build the join parts for IN / NOT IN.

        IN matches with plain SQL equality (NULLs never match: UNKNOWN rows
        are filtered, as required). NOT IN treats NULL on either side as a
        match so the ANTI join drops rows whose result would be UNKNOWN.
        """
        prepared = self._preparer().prepare_values(expr.subquery)
        items = self._in_value_items(expr, prepared)
        comparisons: List[Expression] = []
        for index in range(len(items)):
            # A reference to one subquery value column that the matching outer
            # item is compared against; IN pairs each item with its value column.
            # Qualified to the subquery alias so it resolves against the boundary.
            value_ref = ColumnRef.create(
                table=prepared.alias, column=prepared.values[index]
            )
            comparisons.append(
                self._match_term(items[index], value_ref, null_aware=expr.negated)
            )
        condition = self._combine_condition(comparisons, prepared.condition)
        return prepared.plan, condition, not expr.negated

    def _in_value_items(
        self, expr: InSubquery, prepared: _PreparedSubquery
    ) -> List[Expression]:
        """The outer-side value items, validated against subquery arity."""
        if isinstance(expr.value, TupleExpression):
            items = list(expr.value.items)
        else:
            items = [expr.value]
        if len(items) != len(prepared.values):
            raise DecorrelationError(
                f"IN subquery returns {len(prepared.values)} columns but "
                f"{len(items)} values are compared"
            )
        return items

    def _match_term(
        self, outer_value: Expression, inner_ref: ColumnRef, null_aware: bool
    ) -> Expression:
        """Equality term; NULL-aware variants also match on NULL sides."""
        # The plain equality between the outer value and the subquery value column;
        # for IN this is the match, and NULL sides simply fail to match.
        equality = BinaryOp.create(
            op=BinaryOpType.EQ, left=outer_value, right=inner_ref
        )
        if not null_aware:
            return equality
        # For NOT IN the anti-join must also treat a NULL outer value as a match,
        # so the equality is OR'd with an IS NULL test on the outer side, making an
        # UNKNOWN comparison drop the outer row as SQL requires.
        with_outer_null = BinaryOp.create(
            op=BinaryOpType.OR, left=equality, right=_is_null_check(outer_value)
        )
        # Likewise a NULL on the subquery value side counts as a match, so the term
        # is OR'd with an IS NULL test on the inner ref to complete NULL-awareness.
        return BinaryOp.create(
            op=BinaryOpType.OR, left=with_outer_null, right=_is_null_check(inner_ref)
        )

    def _combine_condition(
        self, comparisons: List[Expression], correlation: Optional[Expression]
    ) -> Expression:
        """Combine value comparisons with correlation conjuncts."""
        conjuncts = list(comparisons)
        if correlation is not None:
            conjuncts.append(correlation)
        return _and_join(conjuncts)

    def _quantified_parts(
        self, expr: QuantifiedComparison
    ) -> Tuple[LogicalPlanNode, Optional[Expression], bool]:
        """Build the join parts for op ANY/SOME/ALL.

        ANY keeps rows with at least one satisfying comparison (SEMI). ALL
        keeps rows with no violation (ANTI); a NULL on either side makes
        the comparison UNKNOWN, which blocks ALL from being TRUE, so NULL
        sides count as violations.
        """
        prepared = self._preparer().prepare_values(expr.subquery)
        if len(prepared.values) != 1:
            raise DecorrelationError(
                "Quantified comparison subquery must return one column"
            )
        # A reference to the single subquery value column that the quantified
        # comparison ranges over (one column is required for op ANY/ALL).
        # Qualified to the subquery alias so it resolves against the boundary.
        value_ref = ColumnRef.create(table=prepared.alias, column=prepared.values[0])
        # The per-row comparison "left op value" applied against each subquery row;
        # ANY keeps rows where one holds, ALL keeps rows where none is violated.
        comparison = BinaryOp.create(op=expr.operator, left=expr.left, right=value_ref)
        if expr.quantifier in (Quantifier.ANY, Quantifier.SOME):
            condition = self._combine_condition([comparison], prepared.condition)
            return prepared.plan, condition, True
        violation = self._violation_term(comparison, expr.left, value_ref)
        condition = self._combine_condition([violation], prepared.condition)
        return prepared.plan, condition, False

    def _violation_term(
        self, comparison: BinaryOp, left: Expression, value_ref: ColumnRef
    ) -> Expression:
        """A row violating ALL: comparison fails or is UNKNOWN."""
        # The base violation of ALL: the comparison failing (NOT comparison), which
        # is the case where a subquery row disqualifies the outer row.
        negated = UnaryOp.create(op=UnaryOpType.NOT, operand=comparison)
        # A NULL left side makes the comparison UNKNOWN, which also blocks ALL from
        # being TRUE, so a NULL left counts as a violation and is OR'd in.
        with_left_null = BinaryOp.create(
            op=BinaryOpType.OR, left=negated, right=_is_null_check(left)
        )
        # A NULL subquery value likewise yields UNKNOWN and counts as a violation,
        # so the final term also OR's in an IS NULL test on the value column.
        return BinaryOp.create(
            op=BinaryOpType.OR, left=with_left_null, right=_is_null_check(value_ref)
        )

    def _rewrite_projection(self, node: Projection) -> Projection:
        """Rewrite projection expressions, joining subquery values in."""
        plan = self._rewrite_plan(node.input)
        expressions: List[Expression] = []
        for expr in node.expressions:
            rewritten, plan = self._rewrite_value_expr(expr, plan)
            expressions.append(rewritten)
        return node.model_copy(update={"input": plan, "expressions": expressions})

    def _rewrite_sort(self, node: Sort) -> LogicalPlanNode:
        """Rewrite sort keys; prune helper columns added for subqueries."""
        input_plan = self._rewrite_plan(node.input)
        if not self._any_has_subquery(node.sort_keys):
            # No subquery in the sort keys, so the same ORDER BY is re-seated over
            # the rewritten input; this rebuilds an existing Sort, so model_copy
            # preserves its keys and direction and only swaps the input.
            return node.model_copy(update={"input": input_plan})
        return self._rewrite_sort_with_subqueries(node, input_plan)

    def _any_has_subquery(self, expressions: List[Expression]) -> bool:
        """Check a list of expressions for subquery nodes."""
        for expr in expressions:
            if _expression_has_subquery(expr):
                return True
        return False

    def _rewrite_sort_with_subqueries(
        self, node: Sort, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Join subquery sort-key values below the sort, then re-project.

        A correlated sort-key subquery references columns of the relation
        below the SELECT projection (``ORDER BY (SELECT ... WHERE i.k =
        o.k)``). Those columns may be projected away, so the join is planted
        beneath the projection, the projection widened to carry the helper
        columns, and the result pruned back to the original output.
        """
        original_names = input_plan.schema()
        if isinstance(input_plan, Projection):
            return self._sort_subqueries_below_projection(
                node, input_plan, original_names
            )
        return self._sort_subqueries_inline(node, input_plan, original_names)

    def _sort_subqueries_inline(
        self, node: Sort, input_plan: LogicalPlanNode, original_names: List[str]
    ) -> LogicalPlanNode:
        """Plant sort-key subquery joins directly above the input."""
        plan = input_plan
        keys: List[Expression] = []
        for key in node.sort_keys:
            rewritten, plan = self._rewrite_value_expr(key, plan)
            keys.append(rewritten)
        # The same ORDER BY re-seated over the join-threaded input with its keys
        # rewritten to reference the joined subquery values; this transforms an
        # existing Sort, so model_copy keeps its direction and only swaps input
        # and sort keys.
        sorted_plan = node.model_copy(update={"input": plan, "sort_keys": keys})
        return self._prune_to_names(sorted_plan, original_names)

    def _sort_subqueries_below_projection(
        self, node: Sort, projection: Projection, original_names: List[str]
    ) -> LogicalPlanNode:
        """Plant sort-key joins beneath the projection where columns exist."""
        plan = self._rewrite_plan(projection.input)
        keys: List[Expression] = []
        for key in node.sort_keys:
            rewritten, plan = self._rewrite_value_expr(key, plan)
            keys.append(rewritten)
        widened = self._widen_projection_for_keys(projection, plan, keys)
        # The same ORDER BY re-seated above the widened projection (which now
        # carries the sort-key helper columns), with keys rewritten to the joined
        # values; model_copy rebuilds the existing Sort, keeping direction and
        # only swapping input and sort keys.
        sorted_plan = node.model_copy(update={"input": widened, "sort_keys": keys})
        return self._prune_to_names(sorted_plan, original_names)

    def _widen_projection_for_keys(
        self, projection: Projection, plan: LogicalPlanNode, keys: List[Expression]
    ) -> Projection:
        """Re-project over plan, passing through sort-key helper columns."""
        expressions = list(projection.expressions)
        aliases = list(projection.aliases)
        existing = set(aliases)
        for name in self._sort_helper_columns(keys, existing):
            # A pass-through reference to a sort-key column the projection did not
            # already output, so the ORDER BY above the projection can still see
            # the value before it is pruned back to the original columns.
            expressions.append(ColumnRef.create(table=None, column=name))
            aliases.append(name)
        return projection.model_copy(
            update={"input": plan, "expressions": expressions, "aliases": aliases}
        )

    def _sort_helper_columns(
        self, keys: List[Expression], existing: Set[str]
    ) -> List[str]:
        """Names referenced by sort keys that the projection does not output."""
        helpers: List[str] = []
        for key in keys:
            for ref in _expression_column_refs(key):
                name = ref.column
                if name not in existing and name not in helpers:
                    helpers.append(name)
        return helpers

    def _prune_to_names(self, plan: LogicalPlanNode, names: List[str]) -> Projection:
        """Project a plan back down to the given output columns."""
        expressions: List[Expression] = []
        for name in names:
            # A reference to one of the original output columns, so the pruning
            # projection selects exactly the pre-rewrite columns by name.
            expressions.append(ColumnRef.create(table=None, column=name))
        # The pruning projection that drops the helper columns added for subquery
        # sort keys, restoring the query's original output column set.
        return Projection.create(
            input=plan, expressions=expressions, aliases=list(names)
        )

    def _rewrite_join(self, node: Join) -> LogicalPlanNode:
        """Rewrite a join whose condition may contain subqueries."""
        left = self._rewrite_plan(node.left)
        right = self._rewrite_plan(node.right)
        if node.condition is None or not _expression_has_subquery(node.condition):
            # No subquery in the join condition, so the same join is kept with its
            # inputs replaced by their decorrelated forms; this rebuilds an
            # existing Join, so model_copy preserves type, condition, natural, and
            # using while only swapping the two child plans.
            return node.model_copy(update={"left": left, "right": right})
        if node.join_type != JoinType.INNER:
            raise DecorrelationError(
                "Subqueries in non-INNER join conditions are not supported"
            )
        return self._rewrite_inner_join_condition(node, left, right)

    def _rewrite_inner_join_condition(
        self, node: Join, left: LogicalPlanNode, right: LogicalPlanNode
    ) -> Join:
        """Decorrelate INNER join condition subqueries against the left side.

        Boolean subquery conjuncts become SEMI/ANTI joins on the left input;
        scalar subqueries join their value onto the left input. The rewrites
        assume the subqueries correlate (at most) with the left side. A
        reference to a right-side column whose name does not exist on the
        left fails at execution; one whose name DOES overlap a left column
        binds silently to the left (a known limitation, not yet rejected).
        """
        kept: List[Expression] = []
        for conjunct in _split_conjuncts(node.condition):
            left, residual = self._apply_conjunct(left, conjunct)
            if residual is not None:
                kept.append(residual)
        condition = _and_join(kept) if len(kept) > 0 else None
        return node.model_copy(
            update={"left": left, "right": right, "condition": condition}
        )

    def _rewrite_value_expr(
        self, expr: Expression, plan: LogicalPlanNode
    ) -> Tuple[Expression, LogicalPlanNode]:
        """Rewrite one expression, threading joins through the plan.

        Scalar subqueries become LEFT joins providing a value column;
        boolean subqueries become SEMI/ANTI branch pairs providing a flag
        column. The rewritten expression references those columns.
        """
        constant = self._constant_exists_value(expr)
        if constant is not None:
            # A constant boolean for an EXISTS over a global aggregate that is
            # always TRUE/FALSE; it replaces the subquery with no join added.
            return Literal.create(value=constant, data_type=DataType.BOOLEAN), plan
        if isinstance(expr, SubqueryExpression):
            return self._join_scalar(expr, plan)
        if isinstance(expr, _SUBQUERY_NODE_TYPES):
            return self._join_flag(expr, plan)
        return self._rewrite_value_children(expr, plan)

    def _rewrite_value_children(
        self, expr: Expression, plan: LogicalPlanNode
    ) -> Tuple[Expression, LogicalPlanNode]:
        """Recurse value rewriting through an expression's children."""
        state = {"plan": plan}

        def rebuild_child(child: Expression) -> Expression:
            """Rewrite one child, accumulating plan changes."""
            rewritten, state["plan"] = self._rewrite_value_expr(child, state["plan"])
            return rewritten

        rebuilt = _rebuild_expression(expr, rebuild_child)
        return rebuilt, state["plan"]

    def _join_scalar(
        self, expr: SubqueryExpression, plan: LogicalPlanNode
    ) -> Tuple[Expression, LogicalPlanNode]:
        """LEFT-join a scalar subquery's value onto the plan.

        Pattern-based flattening is tried first; a correlation that cannot be
        flattened to a set-based join (non-equality crossing an aggregate or
        LIMIT) falls back to a LATERAL join the executing engine decorrelates.
        """
        try:
            prepared = self._preparer().prepare_scalar(expr.subquery)
        except NonFlattenableCorrelation:
            return self._unnest_dependent_scalar(expr, plan)
        condition = prepared.condition
        if condition is None:
            # An uncorrelated scalar subquery has no correlation predicate, so the
            # LEFT join matches unconditionally; a TRUE literal is that condition.
            condition = Literal.create(value=True, data_type=DataType.BOOLEAN)
        # The LEFT join that attaches the scalar subquery's single value to every
        # outer row (NULL where no match), so the replacement expression can read
        # that value column in place of the subquery.
        joined = Join.create(
            left=plan,
            right=prepared.plan,
            join_type=JoinType.LEFT,
            condition=condition,
        )
        return prepared.replacement, joined

    def _lateral_scalar(
        self, expr: SubqueryExpression, plan: LogicalPlanNode
    ) -> Tuple[Expression, LogicalPlanNode]:
        """Build a LATERAL join for a correlated scalar that cannot flatten.

        The subquery's own inner subqueries are decorrelated, its single value
        is projected under a fresh name, and the outer correlation is left in
        place for the LATERAL to evaluate per left row.
        """
        body = self._rewrite_plan(expr.subquery)
        prefix = self._next_prefix()
        value_name = f"{prefix}_v0"
        # The lateral body exposed as a prefix-aliased derived relation so the
        # outer replacement ref qualifies to it, matching how every other
        # subquery relation carries an alias its output columns resolve through.
        right = SubqueryScan.create(
            input=self._project_lateral_value(body, value_name), alias=prefix
        )
        # The LATERAL join fallback for a correlation that cannot flatten to a
        # set-based join; the still-correlated body is evaluated per left row and
        # the executing engine decorrelates it.
        joined = LateralJoin.create(left=plan, right=right, join_type=JoinType.LEFT)
        # A reference to the single projected value of the lateral body, which
        # replaces the scalar subquery in the outer expression. Qualified to the
        # lateral relation's alias so it resolves against that derived relation.
        return ColumnRef.create(table=prefix, column=value_name), joined

    def _project_lateral_value(
        self, node: LogicalPlanNode, value_name: str
    ) -> LogicalPlanNode:
        """Rename a single-column subquery body's output to ``value_name``."""
        if isinstance(node, Projection):
            return node.model_copy(update={"aliases": [value_name]})
        if isinstance(node, Aggregate):
            return node.model_copy(update={"output_names": [value_name]})
        if isinstance(node, (Limit, Sort, Filter)):
            inner = self._project_lateral_value(node.input, value_name)
            return node.with_children([inner])
        raise DecorrelationError("Unsupported scalar subquery body for LATERAL")

    def _unnest_dependent_scalar(
        self, expr: SubqueryExpression, plan: LogicalPlanNode
    ) -> Tuple[Expression, LogicalPlanNode]:
        """Neumann-Kemper unnesting of a correlated scalar-aggregate subquery.

        Build the distinct domain of the outer correlation values, join it to the
        subquery's inner relation on the (possibly non-equi) correlation predicate,
        aggregate once per domain value, and LEFT-join the result back onto the
        outer - ordinary relational algebra with no residual correlation. Falls
        back to a LATERAL for a subquery shape not yet covered here.
        """
        body = self._rewrite_plan(expr.subquery)
        aggregate = self._dependent_shape(body)
        if aggregate is not None:
            return self._assemble_unnested(plan, body, aggregate)
        top_k = self._dependent_limit_shape(body)
        if top_k is not None:
            return self._assemble_unnested_limit(plan, top_k)
        return self._lateral_scalar(expr, plan)

    def _dependent_shape(self, body: LogicalPlanNode):
        """Recognize Aggregate([one agg], no GROUP BY, Filter(correlated, inner)).

        Returns (correlated_conjuncts, local_conjuncts, inner, free_vars), or None
        when the shape is outside this rewrite (the LATERAL fallback handles it).
        """
        if not self._is_scalar_aggregate_over_filter(body):
            return None
        inner_aliases = _collect_inner_aliases(body)
        correlated, local = self._split_by_outer(body.input.predicate, inner_aliases)
        # Free vars come from the correlation predicate AND the aggregate value
        # itself (e.g. MAX(x + o.f)); both are rewritten to the domain column.
        free_vars = self._distinct_outer_refs(
            correlated + list(body.aggregates), inner_aliases
        )
        if not correlated or not free_vars:
            return None
        return (correlated, local, body.input.input, free_vars)

    def _is_scalar_aggregate_over_filter(self, body: LogicalPlanNode) -> bool:
        """One aggregate, no GROUP BY, directly over a Filter carrying the correlation."""
        if not isinstance(body, Aggregate) or body.group_by:
            return False
        return len(body.aggregates) == 1 and isinstance(body.input, Filter)

    def _split_by_outer(self, predicate: Expression, inner_aliases):
        """Split a conjunctive predicate into (references-outer, inner-only) parts."""
        correlated: List[Expression] = []
        local: List[Expression] = []
        for conjunct in self._conjuncts(predicate):
            if self._refs_outside(conjunct, inner_aliases):
                correlated.append(conjunct)
            else:
                local.append(conjunct)
        return correlated, local

    def _conjuncts(self, predicate: Expression) -> List[Expression]:
        """Flatten a predicate's top-level AND tree into a list of conjuncts."""
        if isinstance(predicate, BinaryOp) and predicate.op == BinaryOpType.AND:
            return self._conjuncts(predicate.left) + self._conjuncts(predicate.right)
        return [predicate]

    def _refs_outside(self, expr: Expression, inner_aliases) -> bool:
        """Whether an expression references any relation not inside the subquery."""
        for ref in _expression_column_refs(expr):
            if self._is_outer_ref(ref, inner_aliases):
                return True
        return False

    def _is_outer_ref(self, ref: ColumnRef, inner_aliases) -> bool:
        """A qualified column belonging to an outer relation, not the subquery.

        The qualifier check excludes an unqualified ``*`` (from ``COUNT(*)``),
        which is not a correlation to any outer relation.
        """
        return ref.table is not None and ref.table not in inner_aliases

    def _distinct_outer_refs(self, predicates: List[Expression], inner_aliases):
        """The distinct outer column references (free vars) across the predicates."""
        seen: Set[Tuple[Optional[str], str]] = set()
        free_vars: List[ColumnRef] = []
        for predicate in predicates:
            self._collect_outer_refs(predicate, inner_aliases, seen, free_vars)
        return free_vars

    def _collect_outer_refs(self, predicate, inner_aliases, seen, free_vars) -> None:
        """Append each not-yet-seen outer column ref in a predicate to free_vars."""
        for ref in _expression_column_refs(predicate):
            key = (ref.table, ref.column)
            if self._is_outer_ref(ref, inner_aliases) and key not in seen:
                seen.add(key)
                free_vars.append(ref)

    def _assemble_unnested(self, plan, body, shape) -> Tuple[Expression, LogicalPlanNode]:
        """Build the domain -> dependent aggregate -> join-back rewrite for a shape."""
        correlated, local, inner, free_vars = shape
        dom_prefix = self._next_prefix()
        dep_prefix = self._next_prefix()
        domain, dom_map = self._build_domain(plan, free_vars, dom_prefix)
        dependent = self._build_dependent_relation(
            body.aggregates, [_UNNEST_VALUE], domain, inner,
            correlated, local, dom_map, dep_prefix,
        )
        # Join the aggregated per-domain values back to the outer plan on the
        # free variables; LEFT so outer rows with no matching group survive.
        joined = Join.create(
            left=plan, right=dependent, join_type=JoinType.LEFT,
            condition=self._join_back_condition(free_vars, dom_map, dep_prefix),
        )
        return self._scalar_value_ref(body, dep_prefix), joined

    def _build_domain(self, plan, free_vars, prefix):
        """Distinct outer correlation values, aliased so their columns are qualified.

        Returns the aliased domain relation and a map from each free var's
        (table, column) to its domain column reference (qualified to the alias).
        """
        names: List[str] = []
        dom_map: Dict[Tuple[Optional[str], str], ColumnRef] = {}
        for index, free_var in enumerate(free_vars):
            name = f"d{index}"
            names.append(name)
            # Each free variable resolves to its domain column d<i>, qualified
            # to the domain subquery's alias so every later ref is qualified.
            dom_map[(free_var.table, free_var.column)] = ColumnRef.create(
                table=prefix, column=name
            )
        # DISTINCT projection of the outer correlation values - the N-K domain
        # relation the dependent subquery is evaluated once per value of.
        distinct = Projection.create(
            input=plan, expressions=list(free_vars), aliases=names, distinct=True
        )
        # Wrap the domain under its own alias so its columns are addressable
        # (and qualified) from the dependent join and the join-back condition.
        return SubqueryScan.create(input=distinct, alias=prefix), dom_map

    def _build_dependent_relation(
        self, aggregates, agg_names, domain, inner, correlated, local, dom_map, prefix
    ):
        """Aggregate once per domain value over (domain JOIN inner ON correlation).

        aggregates / agg_names are lists so this serves both a scalar subquery
        (one value) and a user LATERAL (the body's whole aggregate projection).
        """
        condition = self._dependent_join_condition(correlated, local, dom_map)
        # The dependent join: inner body joined to the domain on the rewritten
        # correlation predicate - a plain INNER join, no correlation left.
        dependent_input = Join.create(
            left=domain, right=inner, join_type=JoinType.INNER, condition=condition
        )
        group_cols: List[Expression] = []
        select_list: List[Expression] = []
        names: List[str] = []
        for domain_ref in dom_map.values():
            group_cols.append(domain_ref)
            # aggregates IS the output SELECT list here, so the group key is
            # repeated as a passthrough output, not just a grouping key.
            select_list.append(domain_ref)
            names.append(domain_ref.column)
        # Rewrite any outer reference inside an aggregate value to its domain
        # column, so the aggregate no longer references the outer relation.
        for aggregate in aggregates:
            select_list.append(_replace_column_refs(aggregate, dom_map))
        names.extend(agg_names)
        # One aggregate row per domain value: group by the domain columns and
        # evaluate the (rewritten) aggregate values over the dependent join.
        aggregate = Aggregate.create(
            input=dependent_input, group_by=group_cols,
            aggregates=select_list, output_names=names,
        )
        # Alias the aggregated relation so the join-back can address its domain
        # columns and aggregate outputs with qualified references.
        return SubqueryScan.create(input=aggregate, alias=prefix)

    def _dependent_join_condition(self, correlated, local, dom_map) -> Expression:
        """The correlation (outer refs rewritten to domain columns) AND any locals."""
        rewritten: List[Expression] = []
        for conjunct in correlated:
            rewritten.append(_replace_column_refs(conjunct, dom_map))
        return _and_join(rewritten + local)

    def _join_back_condition(self, free_vars, dom_map, dep_prefix) -> Expression:
        """Equate each outer free var to the dependent relation's domain column."""
        equalities: List[Expression] = []
        for free_var in free_vars:
            domain_ref = dom_map[(free_var.table, free_var.column)]
            # The dependent relation re-exposes each domain column under its
            # own alias; the outer free var equates against that column.
            dependent_ref = ColumnRef.create(table=dep_prefix, column=domain_ref.column)
            # free_var = dependent.d<i>: one equality per free variable, ANDed
            # into the join-back condition by the caller below.
            equalities.append(
                BinaryOp.create(op=BinaryOpType.EQ, left=free_var, right=dependent_ref)
            )
        return _and_join(equalities)

    def _scalar_value_ref(self, body, dep_prefix) -> Expression:
        """The dependent aggregate value; COUNT of an empty group is 0, not NULL.

        This is the classic COUNT decorrelation bug (Kim 1982): a decorrelated
        COUNT joined back must yield 0 for outer rows with no match, whereas a
        plain join would drop them or produce NULL.
        """
        # The aggregate's output value as exposed by the dependent relation's
        # alias (qualified, like every post-binder column reference).
        value_ref = ColumnRef.create(table=dep_prefix, column=_UNNEST_VALUE)
        if not self._is_count_aggregate(body.aggregates[0]):
            return value_ref
        # COUNT over an empty group must be 0, not NULL (the classic count
        # bug); 0 is the substitute for the LEFT join's NULL below.
        zero = Literal.create(value=0, data_type=DataType.INTEGER)
        # COALESCE substitutes 0 for the NULL the LEFT join-back produces when
        # the dependent aggregate had no row for this outer value.
        return FunctionCall.create(
            function_name="COALESCE", args=[value_ref, zero], is_aggregate=False
        )

    def _is_count_aggregate(self, aggregate: Expression) -> bool:
        """Whether the aggregate is a COUNT (0 over an empty group, not NULL)."""
        return (
            isinstance(aggregate, FunctionCall)
            and aggregate.function_name.upper() == "COUNT"
        )

    def _dependent_limit_shape(self, body: LogicalPlanNode):
        """Recognize Limit[Sort?[Projection[Filter(correlated, inner)]]] - a
        top-k-per-outer row subquery.

        Returns (value, order, limit, correlated, local, inner, free_vars), or
        None when the shape is outside this rewrite (the LATERAL fallback covers it).
        """
        peeled = self._peel_limit_body(body)
        if peeled is None:
            return None
        value, order, limit, filter_node = peeled
        inner_aliases = _collect_inner_aliases(body)
        correlated, local = self._split_by_outer(filter_node.predicate, inner_aliases)
        free_vars = self._distinct_outer_refs(correlated, inner_aliases)
        if not correlated or not free_vars:
            return None
        return (value, order, limit, correlated, local, filter_node.input, free_vars)

    def _peel_limit_body(self, body: LogicalPlanNode):
        """Peel Limit / (optional Sort) / single-column Projection to its Filter.

        Returns (value_expr, order_or_None, limit, filter_node) or None.
        """
        if not isinstance(body, Limit):
            return None
        order, below_sort = self._peel_sort(body.input)
        return self._peel_value_filter(below_sort, order, body.limit)

    def _peel_sort(self, node: LogicalPlanNode):
        """Split a leading Sort into its (keys, ascending, nulls) order and input."""
        if isinstance(node, Sort):
            return (node.sort_keys, node.ascending, node.nulls_order), node.input
        return None, node

    def _peel_value_filter(self, node, order, limit):
        """A single-column Projection directly over a Filter carries the value."""
        if not self._is_value_projection(node):
            return None
        return (node.expressions[0], order, limit, node.input)

    def _is_value_projection(self, node: LogicalPlanNode) -> bool:
        """A single-column Projection directly over a Filter."""
        if not isinstance(node, Projection) or len(node.expressions) != 1:
            return False
        return isinstance(node.input, Filter)

    def _assemble_unnested_limit(self, plan, shape) -> Tuple[Expression, LogicalPlanNode]:
        """Build the domain -> top-k-per-domain -> join-back rewrite for a row subquery."""
        value, order, limit, correlated, local, inner, free_vars = shape
        dom_prefix = self._next_prefix()
        dep_prefix = self._next_prefix()
        domain, dom_map = self._build_domain(plan, free_vars, dom_prefix)
        top_k = self._build_top_k_relation(
            [value], [_UNNEST_VALUE], order, limit, domain, inner,
            correlated, local, dom_map, dep_prefix,
        )
        # Join the per-domain top-k rows back to the outer plan on the free
        # variables; LEFT so outer rows with no qualifying row survive.
        joined = Join.create(
            left=plan, right=top_k, join_type=JoinType.LEFT,
            condition=self._join_back_condition(free_vars, dom_map, dep_prefix),
        )
        # The subquery's value is the top-k relation's output column, qualified
        # to the dependent alias (row subquery: NULL when no row matched).
        return ColumnRef.create(table=dep_prefix, column=_UNNEST_VALUE), joined

    def _build_top_k_relation(
        self, value_exprs, value_names, order, limit, domain, inner,
        correlated, local, dom_map, prefix,
    ):
        """domain JOIN inner, ranked by ROW_NUMBER() per domain value, filtered to
        the top-k rows, projected to (domain columns, output columns).

        Output columns are a list so this serves both a scalar subquery (one
        value) and a user LATERAL (the body's whole projection). Uses a window
        (qualified columns, a real resolver) rather than a bare-key GroupedLimit,
        so column pruning keeps the domain column through the join.
        """
        condition = self._dependent_join_condition(correlated, local, dom_map)
        # The dependent join: inner body joined to the domain on the rewritten
        # correlation predicate - a plain INNER join, no correlation left.
        joined = Join.create(
            left=domain, right=inner, join_type=JoinType.INNER, condition=condition
        )
        if limit is None:
            projected = self._project_join_values(joined, value_exprs, value_names, dom_map)
            # No LIMIT: a multi-row (set) LATERAL keeps every matching row, so
            # no ranking - alias the projected join for the join-back.
            return SubqueryScan.create(input=projected, alias=prefix)
        win_prefix = self._next_prefix()
        ranked = self._rank_by_row_number(
            joined, value_exprs, value_names, order, dom_map, win_prefix
        )
        # Keep only rows ranked within the top-k for their domain value: the
        # rn <= limit cap over the ROW_NUMBER projection.
        capped = Filter.create(
            input=ranked, predicate=self._row_number_cap(win_prefix, limit)
        )
        projected = self._project_domain_values(capped, value_names, dom_map, win_prefix)
        # Alias the top-k relation so the join-back addresses its domain and
        # value columns with qualified references.
        return SubqueryScan.create(input=projected, alias=prefix)

    def _project_join_values(self, joined, value_exprs, value_names, dom_map):
        """Project (domain columns, output values) directly over the domain join,
        rewriting any outer reference in a value to its domain column."""
        names: List[str] = []
        for domain_ref in dom_map.values():
            names.append(domain_ref.column)
        names.extend(value_names)
        values: List[Expression] = []
        for value in value_exprs:
            values.append(_replace_column_refs(value, dom_map))
        expressions = list(dom_map.values()) + values
        # Project (domain columns, rewritten values) over the dependent join;
        # the aliases give each output its stable, addressable name.
        return Projection.create(input=joined, expressions=expressions, aliases=names)

    def _rank_by_row_number(self, joined, value_exprs, value_names, order, dom_map, win_prefix):
        """Project (domain cols, output values, ROW_NUMBER() per domain), aliased.

        Any outer reference inside an output value is rewritten to its domain
        column, so the projection over the join references no outer relation.
        """
        window = self._row_number_window(list(dom_map.values()), order)
        names: List[str] = []
        for domain_ref in dom_map.values():
            names.append(domain_ref.column)
        names.extend(value_names)
        names.append(_UNNEST_RANK)
        values: List[Expression] = []
        for value in value_exprs:
            values.append(_replace_column_refs(value, dom_map))
        expressions = list(dom_map.values()) + values + [window]
        # Project (domain cols, rewritten values, ROW_NUMBER) in one list; the
        # rank's alias is what the rn <= limit cap filters on.
        projection = Projection.create(input=joined, expressions=expressions, aliases=names)
        # Alias the ranked projection so the cap and the final projection can
        # reference the rank and the values with qualified names.
        return SubqueryScan.create(input=projection, alias=win_prefix)

    def _row_number_window(self, partition_keys, order):
        """A ROW_NUMBER() window partitioned by the domain, ordered as asked."""
        # ROW_NUMBER() itself takes no arguments; the ranking comes entirely
        # from the window's partition and order clauses below.
        row_number = FunctionCall.create(
            function_name="ROW_NUMBER", args=[], is_aggregate=False
        )
        keys, ascending, nulls = self._order_parts(order)
        # Partition by the domain columns (one ranking per outer value) and
        # order by the subquery's ORDER BY, preserving its nulls handling.
        return WindowExpr.create(
            function=row_number, partition_by=partition_keys,
            order_keys=keys, order_ascending=ascending, order_nulls=nulls,
        )

    def _order_parts(self, order):
        """Split an optional (keys, ascending, nulls) order tuple, empty when None."""
        if order is None:
            return [], [], []
        return order[0], order[1], order[2]

    def _row_number_cap(self, win_prefix, limit) -> Expression:
        """The rn <= limit predicate keeping the top-k rows per domain value."""
        # The rank column as exposed by the ranked subquery's alias; qualified
        # like every post-binder column reference.
        rank = ColumnRef.create(table=win_prefix, column=_UNNEST_RANK)
        # The subquery's LIMIT k becomes the inclusive bound on the rank
        # (ROW_NUMBER is 1-based, so rn <= k keeps exactly k rows per value).
        bound = Literal.create(value=limit, data_type=DataType.INTEGER)
        # rn <= k: the per-domain top-k predicate the caller applies above the
        # ranked projection.
        return BinaryOp.create(op=BinaryOpType.LTE, left=rank, right=bound)

    def _project_domain_values(self, capped, value_names, dom_map, win_prefix):
        """Project the ranked, capped relation down to (domain columns, values)."""
        expressions: List[Expression] = []
        names: List[str] = []
        for domain_ref in dom_map.values():
            # Re-expose each domain column from the ranked relation under the
            # window alias so the join-back can equate against it.
            expressions.append(ColumnRef.create(table=win_prefix, column=domain_ref.column))
            names.append(domain_ref.column)
        for value_name in value_names:
            # Re-expose each output value column from the ranked relation; the
            # rank column is deliberately not carried forward.
            expressions.append(ColumnRef.create(table=win_prefix, column=value_name))
            names.append(value_name)
        # The final top-k relation: (domain columns, values) only - the rank
        # was consumed by the cap and is projected away here.
        return Projection.create(input=capped, expressions=expressions, aliases=names)

    def _rewrite_lateral_join(self, node: LateralJoin) -> LogicalPlanNode:
        """Unnest a top-k user LATERAL to regular algebra, else recurse in place.

        A same-source LATERAL still pushes to its source unchanged; this only
        rewrites one we would otherwise run as a dependent join (cross-source).
        """
        outer = self._rewrite_plan(node.left)
        unnested = self._unnest_lateral(outer, node)
        if unnested is not None:
            return unnested
        return node.model_copy(
            update={"left": outer, "right": self._rewrite_plan(node.right)}
        )

    def _unnest_lateral(self, outer, node):
        """Domain -> unnested LATERAL body (top-k or aggregate) -> join back."""
        if not isinstance(node.right, SubqueryScan):
            return None
        alias = node.right.alias
        body = self._rewrite_plan(node.right.input)
        built = self._lateral_relation(outer, alias, body)
        if built is None:
            return None
        relation, free_vars, dom_map = built
        # Join the unnested LATERAL relation back to the outer plan on the free
        # variables, preserving the user's join type for the lateral.
        return Join.create(
            left=outer, right=relation, join_type=node.join_type,
            condition=self._join_back_condition(free_vars, dom_map, alias),
        )

    def _lateral_relation(self, outer, alias, body):
        """Unnest a LATERAL body to a relation - top-k, aggregate, or set - or None."""
        top_k = self._lateral_top_k_relation(outer, alias, body)
        if top_k is not None:
            return top_k
        aggregate = self._lateral_aggregate_relation(outer, alias, body)
        if aggregate is not None:
            return aggregate
        return self._lateral_set_relation(outer, alias, body)

    def _lateral_set_relation(self, outer, alias, body):
        """Build the relation for a plain multi-row LATERAL body: Projection[Filter].

        A dependent join with no aggregate or LIMIT keeps every matching inner row
        per outer, so it unnests to the domain join projected to the body columns.
        """
        if not isinstance(body, Projection) or not isinstance(body.input, Filter):
            return None
        inner_aliases = _collect_inner_aliases(body)
        correlated, local = self._split_by_outer(body.input.predicate, inner_aliases)
        free_vars = self._distinct_outer_refs(
            correlated + list(body.expressions), inner_aliases
        )
        if not correlated or not free_vars:
            return None
        domain, dom_map = self._build_domain(outer, free_vars, self._next_prefix())
        relation = self._build_top_k_relation(
            list(body.expressions), list(body.aliases), None, None, domain,
            body.input.input, correlated, local, dom_map, alias,
        )
        return (relation, free_vars, dom_map)

    def _lateral_top_k_relation(self, outer, alias, body):
        """Build the top-k relation for a Limit[...] LATERAL body, or None."""
        parts = self._peel_lateral_limit(body)
        if parts is None:
            return None
        shape = self._lateral_limit_shape(parts)
        if shape is None:
            return None
        return self._top_k_from_shape(outer, alias, shape)

    def _top_k_from_shape(self, outer, alias, shape):
        """Build (relation, free_vars, dom_map) for a recognized top-k body."""
        exprs, names, order, limit, correlated, local, inner, free_vars = shape
        domain, dom_map = self._build_domain(outer, free_vars, self._next_prefix())
        relation = self._build_top_k_relation(
            exprs, names, order, limit, domain, inner, correlated, local, dom_map, alias
        )
        return (relation, free_vars, dom_map)

    def _lateral_limit_shape(self, parts):
        """Correlation, free vars, and output for a peeled top-k LATERAL body."""
        projection, order, limit, filter_node, inner_aliases = parts
        correlated, local = self._split_by_outer(filter_node.predicate, inner_aliases)
        free_vars = self._distinct_outer_refs(
            correlated + list(projection.expressions), inner_aliases
        )
        if not correlated or not free_vars:
            return None
        return (list(projection.expressions), list(projection.aliases), order, limit,
                correlated, local, filter_node.input, free_vars)

    def _lateral_aggregate_relation(self, outer, alias, body):
        """Build the aggregate relation for an Aggregate[Filter] LATERAL body, or None."""
        if not self._is_aggregate_over_filter(body):
            return None
        inner_aliases = _collect_inner_aliases(body)
        correlated, local = self._split_by_outer(body.input.predicate, inner_aliases)
        free_vars = self._distinct_outer_refs(
            correlated + list(body.aggregates), inner_aliases
        )
        if not correlated or not free_vars:
            return None
        domain, dom_map = self._build_domain(outer, free_vars, self._next_prefix())
        relation = self._build_dependent_relation(
            body.aggregates, body.output_names, domain, body.input.input,
            correlated, local, dom_map, alias,
        )
        return (relation, free_vars, dom_map)

    def _is_aggregate_over_filter(self, body: LogicalPlanNode) -> bool:
        """An Aggregate with no GROUP BY directly over a Filter (the correlation)."""
        return (isinstance(body, Aggregate) and not body.group_by
                and isinstance(body.input, Filter))

    def _peel_lateral_limit(self, body):
        """Peel Limit / (Sort?) / Projection to its Filter for a LATERAL body.

        Returns (projection, order, limit, filter_node, inner_aliases) or None.
        """
        if not isinstance(body, Limit):
            return None
        order, below = self._peel_sort(body.input)
        if not isinstance(below, Projection) or not isinstance(below.input, Filter):
            return None
        return (below, order, body.limit, below.input, _collect_inner_aliases(body))

    def _join_flag(
        self, expr: Expression, plan: LogicalPlanNode
    ) -> Tuple[Expression, LogicalPlanNode]:
        """Produce a boolean flag column for a subquery predicate.

        The plan splits into a SEMI branch (matching rows) and an ANTI
        branch (non-matching rows); each branch tags its rows with a
        literal flag and the union restores the full row set exactly once.
        """
        right, condition, positive = self._semi_anti_parts(expr)
        flag_name = f"{self._next_prefix()}_flag"
        match_branch = self._flag_branch(
            plan, right, condition, JoinType.SEMI, positive, flag_name
        )
        miss_branch = self._flag_branch(
            plan, right, condition, JoinType.ANTI, not positive, flag_name
        )
        # The non-distinct union of the SEMI branch (matching rows tagged TRUE) and
        # the ANTI branch (non-matching rows tagged FALSE); together they restore
        # the full input row set exactly once, each carrying its boolean flag.
        union = Union.create(inputs=[match_branch, miss_branch], distinct=False)
        # A reference to that flag column, which stands in for the boolean subquery
        # in the outer expression.
        return ColumnRef.create(table=None, column=flag_name), union

    def _flag_branch(
        self,
        plan: LogicalPlanNode,
        right: LogicalPlanNode,
        condition: Optional[Expression],
        join_type: JoinType,
        flag_value: bool,
        flag_name: str,
    ) -> Projection:
        """One half of a flag pair: join, then append the literal flag."""
        # One half of the flag pair: a SEMI or ANTI join selecting either the
        # matching or the non-matching outer rows for this branch.
        joined = Join.create(
            left=plan, right=right, join_type=join_type, condition=condition
        )
        # A star reference so the branch carries through every original column
        # alongside the flag it is about to append.
        star = ColumnRef.create(table=None, column="*")
        # The constant boolean flag tagging this branch's rows (TRUE for the match
        # branch, FALSE for the miss branch) that the outer expression reads.
        flag = Literal.create(value=flag_value, data_type=DataType.BOOLEAN)
        # The projection that appends the flag to the branch's passed-through rows,
        # so the union of the two branches yields each row once with its flag.
        return Projection.create(
            input=joined, expressions=[star, flag], aliases=["*", flag_name]
        )

    def _raise_if_subquery_expression(self, plan: LogicalPlanNode) -> None:
        """Validate that no subquery expression survived decorrelation."""
        for expr in _plan_expressions(plan):
            if _expression_has_subquery(expr):
                raise DecorrelationError(
                    "Subquery expression survived decorrelation in "
                    f"{type(plan).__name__}: {expr!r}"
                )
        for child in plan.children():
            self._raise_if_subquery_expression(child)

    def __repr__(self) -> str:
        return "Decorrelator()"
