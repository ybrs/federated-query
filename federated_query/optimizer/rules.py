"""Optimization rules for logical plans."""

from abc import ABC, abstractmethod
from typing import List, Optional
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Project,
    Filter,
    Join,
    Aggregate,
    Sort,
    Limit,
    Union,
)
from ..plan.expressions import Expression
from ..catalog.catalog import Catalog
from .expression_rewriter import (
    ExpressionRewriter,
    ConstantFoldingRewriter,
    ExpressionSimplificationRewriter,
    CompositeExpressionRewriter,
)


class OptimizationRule(ABC):
    """Base class for optimization rules."""

    @abstractmethod
    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        """Apply this rule to a plan.

        Args:
            plan: Input plan node

        Returns:
            Transformed plan if rule applies, None otherwise
        """
        pass

    @abstractmethod
    def name(self) -> str:
        """Return rule name for logging."""
        pass


class PredicatePushdownRule(OptimizationRule):
    """Push filters closer to data sources."""

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        # TODO: Implement predicate pushdown
        raise NotImplementedError()

    def name(self) -> str:
        return "PredicatePushdown"


class ProjectionPushdownRule(OptimizationRule):
    """Push projections to eliminate unused columns early."""

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        # TODO: Implement projection pushdown
        raise NotImplementedError()

    def name(self) -> str:
        return "ProjectionPushdown"


class JoinReorderingRule(OptimizationRule):
    """Reorder joins for better performance."""

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        # TODO: Implement join reordering
        raise NotImplementedError()

    def name(self) -> str:
        return "JoinReordering"


class ExpressionSimplificationRule(OptimizationRule):
    """Apply expression simplification and constant folding to plan."""

    def __init__(self):
        """Initialize expression simplification rule."""
        self.rewriter = CompositeExpressionRewriter()
        self.rewriter.add_rewriter(ConstantFoldingRewriter())
        self.rewriter.add_rewriter(ExpressionSimplificationRewriter())

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        """Apply expression rewriting to all expressions in the plan.

        Args:
            plan: Input plan node

        Returns:
            Transformed plan with simplified expressions
        """
        return self._rewrite_plan(plan)

    def _rewrite_plan(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recursively rewrite expressions in plan."""
        if isinstance(plan, Scan):
            if plan.filters:
                rewritten_filter = self.rewriter.rewrite(plan.filters)
                if rewritten_filter != plan.filters:
                    return Scan(
                        datasource=plan.datasource,
                        schema_name=plan.schema_name,
                        table_name=plan.table_name,
                        columns=plan.columns,
                        filters=rewritten_filter
                    )
            return plan

        if isinstance(plan, Project):
            rewritten_input = self._rewrite_plan(plan.input)
            rewritten_exprs = []
            changed = False

            for expr in plan.expressions:
                rewritten = self.rewriter.rewrite(expr)
                rewritten_exprs.append(rewritten)
                if rewritten != expr:
                    changed = True

            if changed or rewritten_input != plan.input:
                return Project(rewritten_input, rewritten_exprs, plan.aliases)
            return plan

        if isinstance(plan, Filter):
            rewritten_input = self._rewrite_plan(plan.input)
            rewritten_predicate = self.rewriter.rewrite(plan.predicate)

            if rewritten_predicate != plan.predicate or rewritten_input != plan.input:
                return Filter(rewritten_input, rewritten_predicate)
            return plan

        if isinstance(plan, Join):
            rewritten_left = self._rewrite_plan(plan.left)
            rewritten_right = self._rewrite_plan(plan.right)
            rewritten_condition = None

            if plan.condition:
                rewritten_condition = self.rewriter.rewrite(plan.condition)

            if (rewritten_left != plan.left or
                rewritten_right != plan.right or
                rewritten_condition != plan.condition):
                return Join(rewritten_left, rewritten_right, plan.join_type, rewritten_condition)
            return plan

        if isinstance(plan, Aggregate):
            rewritten_input = self._rewrite_plan(plan.input)
            rewritten_group_by = []
            group_by_changed = False

            for expr in plan.group_by:
                rewritten = self.rewriter.rewrite(expr)
                rewritten_group_by.append(rewritten)
                if rewritten != expr:
                    group_by_changed = True

            rewritten_aggs = []
            aggs_changed = False

            for expr in plan.aggregates:
                rewritten = self.rewriter.rewrite(expr)
                rewritten_aggs.append(rewritten)
                if rewritten != expr:
                    aggs_changed = True

            if (rewritten_input != plan.input or
                group_by_changed or
                aggs_changed):
                return Aggregate(rewritten_input, rewritten_group_by, rewritten_aggs, plan.output_names)
            return plan

        if isinstance(plan, Sort):
            rewritten_input = self._rewrite_plan(plan.input)
            rewritten_keys = []
            changed = False

            for key in plan.sort_keys:
                rewritten = self.rewriter.rewrite(key)
                rewritten_keys.append(rewritten)
                if rewritten != key:
                    changed = True

            if changed or rewritten_input != plan.input:
                return Sort(rewritten_input, rewritten_keys, plan.ascending)
            return plan

        if isinstance(plan, Limit):
            rewritten_input = self._rewrite_plan(plan.input)
            if rewritten_input != plan.input:
                return Limit(rewritten_input, plan.limit, plan.offset)
            return plan

        if isinstance(plan, Union):
            rewritten_inputs = []
            changed = False

            for input_plan in plan.inputs:
                rewritten = self._rewrite_plan(input_plan)
                rewritten_inputs.append(rewritten)
                if rewritten != input_plan:
                    changed = True

            if changed:
                return Union(rewritten_inputs, plan.distinct)
            return plan

        children = plan.children()
        if not children:
            return plan

        rewritten_children = []
        changed = False

        for child in children:
            rewritten = self._rewrite_plan(child)
            rewritten_children.append(rewritten)
            if rewritten != child:
                changed = True

        if changed:
            return plan.with_children(rewritten_children)
        return plan

    def name(self) -> str:
        return "ExpressionSimplification"


class RuleBasedOptimizer:
    """Rule-based query optimizer."""

    def __init__(self, catalog: Catalog):
        """Initialize optimizer.

        Args:
            catalog: Catalog for metadata access
        """
        self.catalog = catalog
        self.rules: List[OptimizationRule] = []

    def add_rule(self, rule: OptimizationRule) -> None:
        """Add an optimization rule.

        Args:
            rule: Optimization rule to add
        """
        self.rules.append(rule)

    def optimize(self, plan: LogicalPlanNode, max_iterations: int = 10) -> LogicalPlanNode:
        """Optimize a logical plan using registered rules.

        Applies rules iteratively until fixed point or max iterations.

        Args:
            plan: Input logical plan
            max_iterations: Maximum number of optimization passes

        Returns:
            Optimized logical plan
        """
        current_plan = plan
        iteration = 0

        while iteration < max_iterations:
            changed = False
            iteration += 1

            # Apply each rule
            for rule in self.rules:
                result = rule.apply(current_plan)
                if result is not None and result != current_plan:
                    current_plan = result
                    changed = True

            # If no rules made changes, we've reached fixed point
            if not changed:
                break

        return current_plan

    def __repr__(self) -> str:
        return f"RuleBasedOptimizer(rules={len(self.rules)})"
