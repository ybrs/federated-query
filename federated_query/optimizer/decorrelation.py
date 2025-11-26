"""Decorrelation transforms for subqueries."""

from dataclasses import dataclass
from typing import List, Set, Optional

from ..plan.logical import (
    LogicalPlanNode,
    Filter,
    Projection,
    Aggregate,
    Sort,
    Join,
    Limit,
    Union,
    Explain,
    CTE,
)
from ..plan.expressions import (
    Expression,
    ExistsExpression,
    InSubquery,
    QuantifiedComparison,
    SubqueryExpression,
    ColumnRef,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    CaseExpr,
    InList,
)


@dataclass
class CorrelationResult:
    """Correlation metadata for a subquery."""

    is_correlated: bool
    outer_references: List[ColumnRef]
    inner_tables: Set[str]


class CorrelationAnalyzer:
    """Detects outer references in subqueries."""

    def analyze(
        self,
        subquery: LogicalPlanNode,
        outer_scope: Set[str],
    ) -> CorrelationResult:
        inner_tables = self._collect_tables(subquery)
        refs = self._collect_outer_refs(subquery, inner_tables, outer_scope)
        is_correlated = len(refs) > 0
        return CorrelationResult(
            is_correlated=is_correlated,
            outer_references=refs,
            inner_tables=inner_tables,
        )

    def _collect_tables(self, plan: LogicalPlanNode) -> Set[str]:
        names: Set[str] = set()
        if hasattr(plan, "table_name") and hasattr(plan, "schema_name"):
            alias = getattr(plan, "alias", None)
            if alias:
                names.add(alias)
            names.add(plan.table_name)
        for child in plan.children():
            child_names = self._collect_tables(child)
            for name in child_names:
                names.add(name)
        return names

    def _collect_outer_refs(
        self,
        plan: LogicalPlanNode,
        inner: Set[str],
        outer: Set[str],
    ) -> List[ColumnRef]:
        refs: List[ColumnRef] = []
        expressions = self._collect_plan_expressions(plan)
        for expr in expressions:
            self._collect_expr_refs(expr, inner, outer, refs)
        for child in plan.children():
            child_refs = self._collect_outer_refs(child, inner, outer)
            for ref in child_refs:
                refs.append(ref)
        return refs

    def _collect_expr_refs(
        self,
        expr: Expression,
        inner: Set[str],
        outer: Set[str],
        refs: List[ColumnRef],
    ) -> None:
        if isinstance(expr, ColumnRef):
            if self._is_outer_ref(expr, inner, outer):
                refs.append(expr)
            return
        if isinstance(expr, BinaryOp):
            self._collect_expr_refs(expr.left, inner, outer, refs)
            self._collect_expr_refs(expr.right, inner, outer, refs)
            return
        if isinstance(expr, UnaryOp):
            self._collect_expr_refs(expr.operand, inner, outer, refs)
            return
        if isinstance(expr, FunctionCall):
            for arg in expr.args:
                self._collect_expr_refs(arg, inner, outer, refs)
            return
        if isinstance(expr, CaseExpr):
            for condition, result in expr.when_clauses:
                self._collect_expr_refs(condition, inner, outer, refs)
                self._collect_expr_refs(result, inner, outer, refs)
            if expr.else_result:
                self._collect_expr_refs(expr.else_result, inner, outer, refs)
            return
        if isinstance(expr, InList):
            self._collect_expr_refs(expr.value, inner, outer, refs)
            for option in expr.options:
                self._collect_expr_refs(option, inner, outer, refs)

    def _is_outer_ref(
        self,
        col_ref: ColumnRef,
        inner: Set[str],
        outer: Set[str],
    ) -> bool:
        if col_ref.table is None:
            return False
        if col_ref.table in inner:
            return False
        return True

    def _collect_plan_expressions(self, plan: LogicalPlanNode) -> List[Expression]:
        expressions: List[Expression] = []
        self._add_filter(plan, expressions)
        self._add_projection(plan, expressions)
        self._add_aggregate(plan, expressions)
        self._add_sort(plan, expressions)
        self._add_join(plan, expressions)
        return expressions

    def _add_filter(self, plan: LogicalPlanNode, expressions: List[Expression]) -> None:
        if isinstance(plan, Filter):
            expressions.append(plan.predicate)

    def _add_projection(
        self,
        plan: LogicalPlanNode,
        expressions: List[Expression],
    ) -> None:
        if isinstance(plan, Projection):
            for expr in plan.expressions:
                expressions.append(expr)

    def _add_aggregate(
        self,
        plan: LogicalPlanNode,
        expressions: List[Expression],
    ) -> None:
        if isinstance(plan, Aggregate):
            for expr in plan.group_by:
                expressions.append(expr)
            for expr in plan.aggregates:
                expressions.append(expr)

    def _add_sort(self, plan: LogicalPlanNode, expressions: List[Expression]) -> None:
        if isinstance(plan, Sort):
            for expr in plan.sort_keys:
                expressions.append(expr)

    def _add_join(self, plan: LogicalPlanNode, expressions: List[Expression]) -> None:
        if isinstance(plan, Join):
            if plan.condition:
                expressions.append(plan.condition)


class DecorrelationError(Exception):
    """Raised when decorrelation cannot be completed."""


class Decorrelator:
    """Decorrelates subqueries in logical plans."""

    def decorrelate(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """
        Remove correlated and uncorrelated subqueries from a plan.

        Current implementation fails fast when subquery expressions are present.

        Args:
            plan: Logical plan with potential subqueries

        Returns:
            Decorrelated logical plan

        Raises:
            DecorrelationError: If subquery expressions remain
        """
        decorrelated = self._rewrite_plan(plan, set())
        self._raise_if_subquery_expression(decorrelated)
        return decorrelated

    def _raise_if_subquery_expression(self, plan: LogicalPlanNode) -> None:
        """Raise if any subquery expression is found in the plan."""
        expressions = self._collect_plan_expressions(plan)
        for expr in expressions:
            if self._expression_contains_subquery(expr):
                raise DecorrelationError("Decorrelation not yet implemented")
        for child in plan.children():
            self._raise_if_subquery_expression(child)

    def _expression_contains_subquery(self, expr: Expression) -> bool:
        """Check expression tree for subquery nodes."""
        checkers = [
            self._is_direct_subquery,
            self._binary_has_subquery,
            self._unary_has_subquery,
            self._function_has_subquery,
            self._case_has_subquery,
            self._inlist_has_subquery,
        ]
        for checker in checkers:
            if checker(expr):
                return True
        return False

    def _is_direct_subquery(self, expr: Expression) -> bool:
        if isinstance(expr, SubqueryExpression):
            return True
        if isinstance(expr, ExistsExpression):
            return True
        if isinstance(expr, InSubquery):
            return True
        if isinstance(expr, QuantifiedComparison):
            return True
        return False

    def _binary_has_subquery(self, expr: Expression) -> bool:
        from ..plan.expressions import BinaryOp

        if not isinstance(expr, BinaryOp):
            return False
        if self._expression_contains_subquery(expr.left):
            return True
        return self._expression_contains_subquery(expr.right)

    def _unary_has_subquery(self, expr: Expression) -> bool:
        from ..plan.expressions import UnaryOp

        if not isinstance(expr, UnaryOp):
            return False
        return self._expression_contains_subquery(expr.operand)

    def _function_has_subquery(self, expr: Expression) -> bool:
        from ..plan.expressions import FunctionCall

        if not isinstance(expr, FunctionCall):
            return False
        for arg in expr.args:
            if self._expression_contains_subquery(arg):
                return True
        return False

    def _case_has_subquery(self, expr: Expression) -> bool:
        from ..plan.expressions import CaseExpr

        if not isinstance(expr, CaseExpr):
            return False
        if self._case_when_contains(expr.when_clauses):
            return True
        if expr.else_result and self._expression_contains_subquery(expr.else_result):
            return True
        return False

    def _case_when_contains(self, clauses) -> bool:
        for condition, result in clauses:
            if self._expression_contains_subquery(condition):
                return True
            if self._expression_contains_subquery(result):
                return True
        return False

    def _inlist_has_subquery(self, expr: Expression) -> bool:
        from ..plan.expressions import InList

        if not isinstance(expr, InList):
            return False
        if self._expression_contains_subquery(expr.value):
            return True
        for option in expr.options:
            if self._expression_contains_subquery(option):
                return True
        return False

    def _rewrite_plan(
        self,
        plan: LogicalPlanNode,
        outer_tables: Set[str],
    ) -> LogicalPlanNode:
        """Rewrite plan by decorrelating subquery expressions."""
        rewritten_children = []
        for child in plan.children():
            child_tables = self._collect_tables_from_scope(outer_tables, plan)
            rewritten_children.append(self._rewrite_plan(child, child_tables))

        if isinstance(plan, Filter):
            predicate = self._rewrite_expression(plan.predicate, outer_tables)
            return Filter(input=rewritten_children[0], predicate=predicate)

        if isinstance(plan, Projection):
            expressions = []
            for expr in plan.expressions:
                expressions.append(self._rewrite_expression(expr, outer_tables))
            return Projection(
                input=rewritten_children[0],
                expressions=expressions,
                aliases=plan.aliases,
                distinct=plan.distinct,
            )

        if isinstance(plan, Aggregate):
            group_by = []
            for expr in plan.group_by:
                group_by.append(self._rewrite_expression(expr, outer_tables))
            aggregates = []
            for expr in plan.aggregates:
                aggregates.append(self._rewrite_expression(expr, outer_tables))
            return Aggregate(
                input=rewritten_children[0],
                group_by=group_by,
                aggregates=aggregates,
                output_names=plan.output_names,
            )

        if isinstance(plan, Sort):
            sort_keys = []
            for expr in plan.sort_keys:
                sort_keys.append(self._rewrite_expression(expr, outer_tables))
            return Sort(
                input=rewritten_children[0],
                sort_keys=sort_keys,
                ascending=plan.ascending,
                nulls_order=plan.nulls_order,
            )

        if isinstance(plan, Join):
            condition = None
            if plan.condition:
                condition = self._rewrite_expression(plan.condition, outer_tables)
            return Join(
                left=rewritten_children[0],
                right=rewritten_children[1],
                join_type=plan.join_type,
                condition=condition,
            )

        if isinstance(plan, CTE):
            return CTE(
                name=plan.name,
                cte_plan=rewritten_children[0],
                child=rewritten_children[1],
            )

        if isinstance(plan, Limit):
            return Limit(
                input=rewritten_children[0],
                limit=plan.limit,
                offset=plan.offset,
            )

        if isinstance(plan, Union):
            return Union(inputs=rewritten_children, distinct=plan.distinct)

        if isinstance(plan, Explain):
            return Explain(input=rewritten_children[0], format=plan.format)

        if len(rewritten_children) == 0:
            return plan

        return plan.with_children(rewritten_children)

    def _rewrite_expression(
        self,
        expr: Expression,
        outer_tables: Set[str],
    ) -> Expression:
        """Recursively rewrite expressions, decorrelating subqueries."""
        if isinstance(expr, SubqueryExpression):
            return self._rewrite_scalar_subquery(expr, outer_tables)
        if isinstance(expr, ExistsExpression):
            return self._rewrite_exists(expr, outer_tables)
        if isinstance(expr, InSubquery):
            return self._rewrite_in_subquery(expr, outer_tables)
        if isinstance(expr, QuantifiedComparison):
            return self._rewrite_quantified(expr, outer_tables)

        from ..plan.expressions import BinaryOp, UnaryOp, FunctionCall, CaseExpr, InList

        if isinstance(expr, BinaryOp):
            left = self._rewrite_expression(expr.left, outer_tables)
            right = self._rewrite_expression(expr.right, outer_tables)
            return BinaryOp(op=expr.op, left=left, right=right)

        if isinstance(expr, UnaryOp):
            operand = self._rewrite_expression(expr.operand, outer_tables)
            return UnaryOp(op=expr.op, operand=operand)

        if isinstance(expr, FunctionCall):
            args = []
            for arg in expr.args:
                args.append(self._rewrite_expression(arg, outer_tables))
            return FunctionCall(
                function_name=expr.function_name,
                args=args,
                is_aggregate=expr.is_aggregate,
                distinct=expr.distinct,
            )

        if isinstance(expr, CaseExpr):
            rewritten_when = []
            for condition, result in expr.when_clauses:
                rewritten_condition = self._rewrite_expression(condition, outer_tables)
                rewritten_result = self._rewrite_expression(result, outer_tables)
                rewritten_when.append((rewritten_condition, rewritten_result))
            else_expr = None
            if expr.else_result:
                else_expr = self._rewrite_expression(expr.else_result, outer_tables)
            return CaseExpr(when_clauses=rewritten_when, else_result=else_expr)

        if isinstance(expr, InList):
            value_expr = self._rewrite_expression(expr.value, outer_tables)
            options = []
            for option in expr.options:
                options.append(self._rewrite_expression(option, outer_tables))
            return InList(value=value_expr, options=options)

        return expr

    def _rewrite_scalar_subquery(
        self,
        expr: SubqueryExpression,
        outer_tables: Set[str],
    ) -> Expression:
        raise DecorrelationError("Scalar subquery decorrelation not implemented")

    def _rewrite_exists(
        self,
        expr: ExistsExpression,
        outer_tables: Set[str],
    ) -> Expression:
        raise DecorrelationError("EXISTS decorrelation not implemented")

    def _rewrite_in_subquery(
        self,
        expr: InSubquery,
        outer_tables: Set[str],
    ) -> Expression:
        raise DecorrelationError("IN decorrelation not implemented")

    def _rewrite_quantified(
        self,
        expr: QuantifiedComparison,
        outer_tables: Set[str],
    ) -> Expression:
        raise DecorrelationError("Quantified comparison decorrelation not implemented")

    def _collect_tables_from_scope(
        self,
        outer_tables: Set[str],
        plan: LogicalPlanNode,
    ) -> Set[str]:
        """Collect table names visible to child plan."""
        names: Set[str] = set()
        for name in outer_tables:
            names.add(name)
        if hasattr(plan, "table_name") and hasattr(plan, "schema_name"):
            alias = getattr(plan, "alias", None)
            if alias:
                names.add(alias)
            names.add(plan.table_name)
        return names

    def _collect_plan_expressions(self, plan: LogicalPlanNode) -> List[Expression]:
        """Collect expressions attached to a plan node."""
        expressions: List[Expression] = []
        self._add_filter_expressions(plan, expressions)
        self._add_projection_expressions(plan, expressions)
        self._add_aggregate_expressions(plan, expressions)
        self._add_sort_expressions(plan, expressions)
        self._add_join_expression(plan, expressions)
        return expressions

    def _add_filter_expressions(
        self,
        plan: LogicalPlanNode,
        expressions: List[Expression],
    ) -> None:
        """Collect filter predicates."""
        if isinstance(plan, Filter):
            expressions.append(plan.predicate)

    def _add_projection_expressions(
        self,
        plan: LogicalPlanNode,
        expressions: List[Expression],
    ) -> None:
        """Collect projection expressions."""
        if isinstance(plan, Projection):
            for expr in plan.expressions:
                expressions.append(expr)

    def _add_aggregate_expressions(
        self,
        plan: LogicalPlanNode,
        expressions: List[Expression],
    ) -> None:
        """Collect aggregate and group by expressions."""
        if isinstance(plan, Aggregate):
            for expr in plan.group_by:
                expressions.append(expr)
            for expr in plan.aggregates:
                expressions.append(expr)

    def _add_sort_expressions(
        self,
        plan: LogicalPlanNode,
        expressions: List[Expression],
    ) -> None:
        """Collect sort keys."""
        if isinstance(plan, Sort):
            for expr in plan.sort_keys:
                expressions.append(expr)

    def _add_join_expression(
        self,
        plan: LogicalPlanNode,
        expressions: List[Expression],
    ) -> None:
        """Collect join conditions."""
        if isinstance(plan, Join):
            if plan.condition:
                expressions.append(plan.condition)


class NullSafeBuilder:
    """Builds null-safe comparison expressions."""

    def build_equality(self, left: Expression, right: Expression) -> BinaryOp:
        from ..plan.expressions import BinaryOpType

        return BinaryOp(op=BinaryOpType.EQ, left=left, right=right)


class CTEHoister:
    """
    Hoists uncorrelated subqueries into deterministic CTEs.

    This lets us evaluate an uncorrelated subquery once and reuse it. Example:
        input plan: Project(users, expr=(SELECT MAX(x) FROM t))
        hoist: CTE(name=cte_subq_0, cte_plan=Scan(t), child=Project(...cross cte...))

    Naming is stable (`cte_subq_<n>`) so tests can assert on plan shapes.
    """

    def __init__(self) -> None:
        self.counter = 0

    def hoist(self, subquery: LogicalPlanNode, parent: LogicalPlanNode) -> CTE:
        name = self._next_name()
        return CTE(name=name, cte_plan=subquery, child=parent)

    def _next_name(self) -> str:
        name = f"cte_subq_{self.counter}"
        self.counter += 1
        return name

    def __repr__(self) -> str:
        return "CTEHoister()"


    def _expression_contains_subquery(self, expr: Expression) -> bool:
        """Check if expression tree contains any subquery node."""
        checkers = [
            self._is_direct_subquery,
            self._binary_has_subquery,
            self._unary_has_subquery,
            self._function_has_subquery,
            self._case_has_subquery,
            self._inlist_has_subquery,
        ]
        for checker in checkers:
            if checker(expr):
                return True
        return False

    def _is_direct_subquery(self, expr: Expression) -> bool:
        """Direct subquery expression types."""
        if isinstance(expr, SubqueryExpression):
            return True
        if isinstance(expr, ExistsExpression):
            return True
        if isinstance(expr, InSubquery):
            return True
        if isinstance(expr, QuantifiedComparison):
            return True
        return False

    def _binary_has_subquery(self, expr: Expression) -> bool:
        """Check binary operands for subqueries."""
        from ..plan.expressions import BinaryOp

        if not isinstance(expr, BinaryOp):
            return False
        if self._expression_contains_subquery(expr.left):
            return True
        return self._expression_contains_subquery(expr.right)

    def _unary_has_subquery(self, expr: Expression) -> bool:
        """Check unary operand for subqueries."""
        from ..plan.expressions import UnaryOp

        if not isinstance(expr, UnaryOp):
            return False
        return self._expression_contains_subquery(expr.operand)

    def _function_has_subquery(self, expr: Expression) -> bool:
        """Check function arguments for subqueries."""
        from ..plan.expressions import FunctionCall

        if not isinstance(expr, FunctionCall):
            return False
        for arg in expr.args:
            if self._expression_contains_subquery(arg):
                return True
        return False

    def _case_has_subquery(self, expr: Expression) -> bool:
        """Check CASE branches for subqueries."""
        from ..plan.expressions import CaseExpr

        if not isinstance(expr, CaseExpr):
            return False
        if self._case_when_contains(expr.when_clauses):
            return True
        if expr.else_result and self._expression_contains_subquery(expr.else_result):
            return True
        return False

    def _case_when_contains(self, clauses) -> bool:
        """Check WHEN clauses for subqueries."""
        for condition, result in clauses:
            if self._expression_contains_subquery(condition):
                return True
            if self._expression_contains_subquery(result):
                return True
        return False

    def _inlist_has_subquery(self, expr: Expression) -> bool:
        """Check IN list options for subqueries."""
        from ..plan.expressions import InList

        if not isinstance(expr, InList):
            return False
        if self._expression_contains_subquery(expr.value):
            return True
        for option in expr.options:
            if self._expression_contains_subquery(option):
                return True
        return False

    def __repr__(self) -> str:
        return "Decorrelator()"
