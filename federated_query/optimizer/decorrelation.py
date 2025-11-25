"""Decorrelation transforms for subqueries."""

from typing import List

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
)
from ..plan.expressions import (
    Expression,
    ExistsExpression,
    InSubquery,
    QuantifiedComparison,
    SubqueryExpression,
)


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
        self._raise_if_subquery_expression(plan)
        return plan

    def _raise_if_subquery_expression(self, plan: LogicalPlanNode) -> None:
        """Raise if any subquery expression is found in the plan."""
        expressions = self._collect_plan_expressions(plan)
        for expr in expressions:
            if self._is_subquery_expression(expr):
                raise DecorrelationError("Decorrelation not yet implemented")
        for child in plan.children():
            self._raise_if_subquery_expression(child)

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

    def _is_subquery_expression(self, expr: Expression) -> bool:
        """Check if expression is a subquery or quantified predicate."""
        if isinstance(expr, SubqueryExpression):
            return True
        if isinstance(expr, ExistsExpression):
            return True
        if isinstance(expr, InSubquery):
            return True
        if isinstance(expr, QuantifiedComparison):
            return True
        return False

    def __repr__(self) -> str:
        return "Decorrelator()"
