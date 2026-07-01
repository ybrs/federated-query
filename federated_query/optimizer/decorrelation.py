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

    @classmethod
    def create(
        cls,
        *,
        plan: LogicalPlanNode,
        condition: Optional[Expression],
        values: List[str],
    ) -> "_PreparedSubquery":
        """Sanctioned fresh-construction path for _PreparedSubquery.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            plan=plan,
            condition=condition,
            values=values,
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

    def prepare_exists(self, subquery: LogicalPlanNode) -> _PreparedSubquery:
        """Prepare an EXISTS subquery: only correlation columns matter."""
        plan = self.decorrelator._rewrite_plan(subquery)
        self.inner_aliases = _collect_inner_aliases(plan)
        if not self._is_correlated(plan):
            # Uncorrelated EXISTS only asks whether any row exists, so the join
            # right side is the subplan capped at one row with no correlation
            # condition; LIMIT 1 keeps the SEMI join input minimal.
            return _PreparedSubquery.create(
                plan=Limit.create(input=plan, limit=1, offset=0),
                condition=None,
                values=[],
            )
        core = self._peel_exists_top(plan)
        stripped = self._strip(core)
        return self._assemble(stripped, [], [])

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
        return self._assemble(stripped, value_exprs, value_names)

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
        # The prepared scalar-aggregate side: the guarded per-key aggregate plan,
        # its correlation condition, and the replacement expression (hoisted
        # aggregate refs) that stands in for the subquery in the outer row.
        return _PreparedScalar.create(
            plan=guarded, condition=prepared.condition, replacement=replacement
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
            ref = ColumnRef.create(table=None, column=name)
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
        # The prepared plain-row scalar side: the cardinality-guarded value plan,
        # its correlation condition, and a reference to the single projected value
        # column that replaces the subquery in the outer expression.
        return _PreparedScalar.create(
            plan=guarded,
            condition=prepared.condition,
            replacement=ColumnRef.create(table=None, column=value_name),
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
        aggregates = list(plan.aggregates)
        names = list(plan.output_names)
        for offset in range(len(crossing)):
            inner_ref, outer_side = self._key_equality(crossing[offset])
            self._add_group_key(group_by, inner_ref)
            key_name = f"{self.prefix}_g{start + offset}"
            aggregates.append(inner_ref)
            names.append(key_name)
            # A reference to the correlation key now emitted as a grouped output
            # of the widened aggregate, so the pulled predicate can join on it.
            renamed = ColumnRef.create(table=None, column=key_name)
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

    def _add_group_key(self, group_by: List[Expression], key: ColumnRef) -> None:
        """Add a correlation key to the grouping, validating legality."""
        for existing in group_by:
            if isinstance(existing, ColumnRef) and existing.column == key.column:
                return
        if len(group_by) > 0:
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
        # names the caller compares against.
        return _PreparedSubquery.create(
            plan=plan, condition=condition, values=value_names
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
                exposures.append((ref, ref.column))
                exposure_names[key] = ref
                continue
            if not self._is_inner_column(ref):
                continue
            name = f"{self.prefix}_k{len(exposures)}"
            exposures.append((ref, name))
            # The renamed reference that the pulled predicate's inner column maps
            # to; the correlation condition is rebuilt against this deterministic
            # name so it can be evaluated on the join's right side.
            exposure_names[key] = ColumnRef.create(table=None, column=name)

    def _is_renamed_ref(self, ref: ColumnRef) -> bool:
        """Check for a column already renamed by aggregate widening."""
        return ref.table is None and ref.column.startswith(self.prefix)

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
        """Rewrite a filter: joins for conjuncts, unions for disjunctions."""
        input_plan = self._rewrite_plan(node.input)
        if not _expression_has_subquery(node.predicate):
            # No subquery in the predicate, so the filter is preserved as-is over
            # the (possibly rewritten) input; only the child needed decorrelating.
            return Filter.create(input=input_plan, predicate=node.predicate)
        disjuncts = _split_disjuncts(node.predicate)
        if len(disjuncts) > 1:
            return self._expand_or(input_plan, disjuncts)
        return self._rewrite_filter_conjuncts(input_plan, node.predicate)

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
            value_ref = ColumnRef.create(table=None, column=prepared.values[index])
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
        value_ref = ColumnRef.create(table=None, column=prepared.values[0])
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
            return self._lateral_scalar(expr, plan)
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
        value_name = f"{self._next_prefix()}_v0"
        right = self._project_lateral_value(body, value_name)
        # The LATERAL join fallback for a correlation that cannot flatten to a
        # set-based join; the still-correlated body is evaluated per left row and
        # the executing engine decorrelates it.
        joined = LateralJoin.create(left=plan, right=right, join_type=JoinType.LEFT)
        # A reference to the single projected value of the lateral body, which
        # replaces the scalar subquery in the outer expression.
        return ColumnRef.create(table=None, column=value_name), joined

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
