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
        """Apply predicate pushdown optimization.

        Args:
            plan: Input plan node

        Returns:
            Transformed plan with pushed-down predicates
        """
        return self._push_down(plan)

    def _push_down(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recursively push predicates down the plan tree."""
        if isinstance(plan, Filter):
            return self._push_filter(plan)

        if isinstance(plan, Project):
            new_input = self._push_down(plan.input)
            if new_input != plan.input:
                return Project(new_input, plan.expressions, plan.aliases)
            return plan

        if isinstance(plan, Join):
            new_left = self._push_down(plan.left)
            new_right = self._push_down(plan.right)
            if new_left != plan.left or new_right != plan.right:
                return Join(new_left, new_right, plan.join_type, plan.condition)
            return plan

        if isinstance(plan, Aggregate):
            new_input = self._push_down(plan.input)
            if new_input != plan.input:
                return Aggregate(
                    new_input,
                    plan.group_by,
                    plan.aggregates,
                    plan.output_names
                )
            return plan

        if isinstance(plan, Limit):
            new_input = self._push_down(plan.input)
            if new_input != plan.input:
                return Limit(new_input, plan.limit, plan.offset)
            return plan

        return plan

    def _push_filter(self, filter_node: Filter) -> LogicalPlanNode:
        """Push a filter node down."""
        input_plan = filter_node.input
        predicate = filter_node.predicate

        if isinstance(input_plan, Filter):
            return self._merge_filters(filter_node, input_plan)

        if isinstance(input_plan, Project):
            return self._push_filter_through_project(
                filter_node,
                input_plan
            )

        if isinstance(input_plan, Join):
            return self._push_filter_below_join(filter_node, input_plan)

        if isinstance(input_plan, Scan):
            return self._push_filter_to_scan(filter_node, input_plan)

        new_input = self._push_down(input_plan)
        if new_input != input_plan:
            return Filter(new_input, predicate)

        return filter_node

    def _merge_filters(
        self,
        outer: Filter,
        inner: Filter
    ) -> LogicalPlanNode:
        """Merge two adjacent filters."""
        from ..plan.expressions import BinaryOp, BinaryOpType

        merged_predicate = BinaryOp(
            op=BinaryOpType.AND,
            left=outer.predicate,
            right=inner.predicate
        )

        new_filter = Filter(inner.input, merged_predicate)
        return self._push_down(new_filter)

    def _push_filter_through_project(
        self,
        filter_node: Filter,
        project: Project
    ) -> LogicalPlanNode:
        """Push filter through projection if possible.

        Push filter BELOW projection if the predicate can be evaluated
        using columns available in the projection's input.
        """
        predicate_cols = self._extract_column_refs(filter_node.predicate)
        input_cols = self._get_column_names(project.input)

        # Can we evaluate predicate using columns from input?
        can_push = predicate_cols.issubset(input_cols) if predicate_cols else True

        if can_push:
            # Push filter BELOW projection: Project(Filter(input, pred), ...)
            new_filter = Filter(project.input, filter_node.predicate)
            new_filter = self._push_down(new_filter)
            return Project(new_filter, project.expressions, project.aliases)

        # Filter references columns not available in input, keep above
        new_input = self._push_down(project.input)
        new_project = Project(new_input, project.expressions, project.aliases)
        return Filter(new_project, filter_node.predicate)

    def _push_filter_to_scan(
        self,
        filter_node: Filter,
        scan: Scan
    ) -> LogicalPlanNode:
        """Push filter into scan node."""
        from ..plan.expressions import BinaryOp, BinaryOpType

        if scan.filters:
            merged = BinaryOp(
                op=BinaryOpType.AND,
                left=scan.filters,
                right=filter_node.predicate
            )
            return Scan(
                datasource=scan.datasource,
                schema_name=scan.schema_name,
                table_name=scan.table_name,
                columns=scan.columns,
                filters=merged
            )

        return Scan(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=scan.columns,
            filters=filter_node.predicate
        )

    def _push_filter_below_join(
        self,
        filter_node: Filter,
        join: Join
    ) -> LogicalPlanNode:
        """Push filter below join when safe.

        Only safe for INNER joins. For outer joins, pushing filters
        below the join changes semantics:
        - LEFT OUTER: Can't push right-side filters (changes NULL rows)
        - RIGHT OUTER: Can't push left-side filters (changes NULL rows)
        - FULL OUTER: Can't push any filters
        """
        from ..plan.expressions import BinaryOp, BinaryOpType, ColumnRef
        from ..plan.logical import JoinType

        predicate = filter_node.predicate
        left_cols = self._get_column_names(join.left)
        right_cols = self._get_column_names(join.right)

        pred_cols = self._extract_column_refs(predicate)

        pred_left_only = all(c in left_cols for c in pred_cols)
        pred_right_only = all(c in right_cols for c in pred_cols)

        # Only push for INNER joins
        if join.join_type != JoinType.INNER:
            # For outer joins, keep filter above join
            new_join = Join(
                self._push_down(join.left),
                self._push_down(join.right),
                join.join_type,
                join.condition
            )
            return Filter(new_join, predicate)

        # INNER JOIN: safe to push
        if pred_left_only:
            new_left = Filter(join.left, predicate)
            new_left = self._push_down(new_left)
            new_join = Join(
                new_left,
                self._push_down(join.right),
                join.join_type,
                join.condition
            )
            return new_join

        if pred_right_only:
            new_right = Filter(join.right, predicate)
            new_right = self._push_down(new_right)
            new_join = Join(
                self._push_down(join.left),
                new_right,
                join.join_type,
                join.condition
            )
            return new_join

        new_join = Join(
            self._push_down(join.left),
            self._push_down(join.right),
            join.join_type,
            join.condition
        )
        return Filter(new_join, predicate)

    def _get_column_names(self, plan: LogicalPlanNode) -> set:
        """Get all column names available from a plan node.

        Recursively handles wrapped nodes (Filter, Limit, etc).
        """
        if isinstance(plan, Scan):
            return set(plan.columns)

        if isinstance(plan, Project):
            return set(plan.aliases)

        if isinstance(plan, Join):
            left_cols = self._get_column_names(plan.left)
            right_cols = self._get_column_names(plan.right)
            return left_cols.union(right_cols)

        if isinstance(plan, Filter):
            return self._get_column_names(plan.input)

        if isinstance(plan, Limit):
            return self._get_column_names(plan.input)

        if isinstance(plan, Aggregate):
            # Aggregate changes schema - return output columns
            return set(plan.output_names)

        return set()

    def _extract_column_refs(self, expr: Expression) -> set:
        """Extract column names from expression."""
        from ..plan.expressions import ColumnRef, BinaryOp, UnaryOp

        if isinstance(expr, ColumnRef):
            return {expr.column}

        if isinstance(expr, BinaryOp):
            left = self._extract_column_refs(expr.left)
            right = self._extract_column_refs(expr.right)
            return left.union(right)

        if isinstance(expr, UnaryOp):
            return self._extract_column_refs(expr.operand)

        return set()

    def name(self) -> str:
        return "PredicatePushdown"


class ProjectionPushdownRule(OptimizationRule):
    """Push projections to eliminate unused columns early."""

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        """Apply projection pushdown optimization.

        Args:
            plan: Input plan node

        Returns:
            Transformed plan with column pruning
        """
        has_explicit_projection = self._has_explicit_projection(plan)

        if not has_explicit_projection:
            # No explicit projection means SELECT *
            # Don't prune columns - user wants all of them
            return plan

        required_columns = self._collect_required_columns(plan)
        return self._prune_columns(plan, required_columns)

    def _has_explicit_projection(self, plan: LogicalPlanNode) -> bool:
        """Check if plan has explicit projection (not SELECT *)."""
        if isinstance(plan, Project):
            return True

        if isinstance(plan, Filter):
            return self._has_explicit_projection(plan.input)

        if isinstance(plan, Limit):
            return self._has_explicit_projection(plan.input)

        return False

    def _collect_required_columns(
        self,
        plan: LogicalPlanNode
    ) -> set:
        """Collect all required column names from plan."""
        columns = set()

        if isinstance(plan, Scan):
            if plan.filters:
                columns.update(self._extract_columns(plan.filters))
            return columns

        if isinstance(plan, Project):
            for expr in plan.expressions:
                columns.update(self._extract_columns(expr))
            return columns

        if isinstance(plan, Filter):
            columns.update(self._extract_columns(plan.predicate))
            columns.update(self._collect_required_columns(plan.input))
            return columns

        if isinstance(plan, Join):
            if plan.condition:
                columns.update(self._extract_columns(plan.condition))
            columns.update(self._collect_required_columns(plan.left))
            columns.update(self._collect_required_columns(plan.right))
            return columns

        if isinstance(plan, Aggregate):
            for expr in plan.group_by:
                columns.update(self._extract_columns(expr))
            for expr in plan.aggregates:
                columns.update(self._extract_columns(expr))
            return columns

        if isinstance(plan, Limit):
            columns.update(self._collect_required_columns(plan.input))
            return columns

        return columns

    def _extract_columns(self, expr: Expression) -> set:
        """Extract column names from expression."""
        from ..plan.expressions import ColumnRef, BinaryOp, UnaryOp, FunctionCall

        columns = set()

        if isinstance(expr, ColumnRef):
            columns.add(expr.column)
            return columns

        if isinstance(expr, BinaryOp):
            columns.update(self._extract_columns(expr.left))
            columns.update(self._extract_columns(expr.right))
            return columns

        if isinstance(expr, UnaryOp):
            columns.update(self._extract_columns(expr.operand))
            return columns

        if isinstance(expr, FunctionCall):
            for arg in expr.args:
                columns.update(self._extract_columns(arg))
            return columns

        return columns

    def _prune_columns(
        self,
        plan: LogicalPlanNode,
        required: set
    ) -> LogicalPlanNode:
        """Prune unused columns from plan."""
        if isinstance(plan, Scan):
            return self._prune_scan_columns(plan, required)

        if isinstance(plan, Project):
            new_input = self._prune_columns(plan.input, required)
            if new_input != plan.input:
                return Project(new_input, plan.expressions, plan.aliases)
            return plan

        if isinstance(plan, Filter):
            filter_cols = self._extract_columns(plan.predicate)
            combined_required = required.union(filter_cols)
            new_input = self._prune_columns(plan.input, combined_required)
            if new_input != plan.input:
                return Filter(new_input, plan.predicate)
            return plan

        if isinstance(plan, Join):
            left_req = self._get_required_for_subtree(plan.left, required)
            right_req = self._get_required_for_subtree(plan.right, required)
            new_left = self._prune_columns(plan.left, left_req)
            new_right = self._prune_columns(plan.right, right_req)
            if new_left != plan.left or new_right != plan.right:
                return Join(
                    new_left,
                    new_right,
                    plan.join_type,
                    plan.condition
                )
            return plan

        if isinstance(plan, Aggregate):
            new_input = self._prune_columns(plan.input, required)
            if new_input != plan.input:
                return Aggregate(
                    new_input,
                    plan.group_by,
                    plan.aggregates,
                    plan.output_names
                )
            return plan

        if isinstance(plan, Limit):
            new_input = self._prune_columns(plan.input, required)
            if new_input != plan.input:
                return Limit(new_input, plan.limit, plan.offset)
            return plan

        return plan

    def _prune_scan_columns(
        self,
        scan: Scan,
        required: set
    ) -> Scan:
        """Prune columns from scan node."""
        available = set(scan.columns)
        needed = available.intersection(required)

        if not needed:
            needed = available

        if needed != available:
            pruned_cols = []
            for col in scan.columns:
                if col in needed:
                    pruned_cols.append(col)

            return Scan(
                datasource=scan.datasource,
                schema_name=scan.schema_name,
                table_name=scan.table_name,
                columns=pruned_cols,
                filters=scan.filters
            )

        return scan

    def _get_required_for_subtree(
        self,
        plan: LogicalPlanNode,
        parent_required: set
    ) -> set:
        """Get required columns for a subtree."""
        local_required = set()

        if isinstance(plan, Scan):
            if plan.filters:
                local_required.update(self._extract_columns(plan.filters))

        if isinstance(plan, Filter):
            local_required.update(self._extract_columns(plan.predicate))

        return local_required.union(parent_required)

    def name(self) -> str:
        return "ProjectionPushdown"


class LimitPushdownRule(OptimizationRule):
    """Push LIMIT operators closer to data sources when safe."""

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        """Apply limit pushdown optimization.

        Args:
            plan: Input plan node

        Returns:
            Transformed plan with pushed-down limits
        """
        return self._push_limit(plan)

    def _push_limit(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recursively push limits down.

        Only pushes through projections (safe).
        Does not push through filters, joins, or aggregates.
        """
        if isinstance(plan, Limit):
            return self._try_push_limit(plan)

        if isinstance(plan, Project):
            new_input = self._push_limit(plan.input)
            if new_input != plan.input:
                return Project(new_input, plan.expressions, plan.aliases)
            return plan

        # Don't recurse through filters - limit shouldn't push below
        return plan

    def _try_push_limit(self, limit: Limit) -> LogicalPlanNode:
        """Try to push limit through input.

        Only push through operations that don't change row count.
        Safe: Projection (1:1 mapping)
        Unsafe: Filter (reduces rows), Join (changes row count)
        """
        input_plan = limit.input

        if isinstance(input_plan, Project):
            new_input = Limit(input_plan.input, limit.limit, limit.offset)
            return Project(new_input, input_plan.expressions, input_plan.aliases)

        # Don't push through filters - changes semantics
        # Don't push through joins - changes semantics
        # Don't push through aggregates - changes semantics

        return limit

    def name(self) -> str:
        return "LimitPushdown"


class JoinReorderingRule(OptimizationRule):
    """Reorder joins for better performance."""

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        # TODO: Implement cost-based join reordering
        # Requires integration with Phase 5 cost model
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
