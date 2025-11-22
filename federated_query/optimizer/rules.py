"""Optimization rules for logical plans."""

from abc import ABC, abstractmethod
from typing import List, Optional, TYPE_CHECKING
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Projection,
    Filter,
    Join,
    Aggregate,
    Sort,
    Limit,
    Union,
    Explain,
)
from ..plan.expressions import Expression, InList, BetweenExpression
from ..catalog.catalog import Catalog
from .expression_rewriter import (
    ExpressionRewriter,
    ConstantFoldingRewriter,
    ExpressionSimplificationRewriter,
    CompositeExpressionRewriter,
)

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


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

        if isinstance(plan, Projection):
            new_input = self._push_down(plan.input)
            if new_input != plan.input:
                return Projection(
                    new_input,
                    plan.expressions,
                    plan.aliases,
                    plan.distinct,
                )
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

        from ..plan.expressions import BinaryOp, BinaryOpType

        if isinstance(predicate, BinaryOp) and predicate.op == BinaryOpType.AND:
            left_result = self._push_filter(
                Filter(input_plan, predicate.left)
            )
            return self._push_filter(
                Filter(left_result, predicate.right)
            )

        if isinstance(input_plan, Filter):
            return self._merge_filters(filter_node, input_plan)

        if isinstance(input_plan, Projection):
            return self._push_filter_through_projection(
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

    def _can_evaluate_predicate(
        self,
        pred_cols: set,
        available_cols: set
    ) -> bool:
        """Check if predicate columns can be evaluated with available columns.

        Handles both qualified (table.column) and unqualified (column) references.
        An unqualified predicate column matches if there's a qualified column
        ending with that bare name.
        """
        for pred_col in pred_cols:
            if pred_col in available_cols:
                continue

            if "." not in pred_col:
                matched = False
                for avail_col in available_cols:
                    if avail_col.endswith(f".{pred_col}") or avail_col == pred_col:
                        matched = True
                        break
                if matched:
                    continue
                return False

            return False

        return True

    def _push_filter_through_projection(
        self,
        filter_node: Filter,
        projection: Projection
    ) -> LogicalPlanNode:
        """Push filter through projection if possible.

        Push filter BELOW projection if the predicate can be evaluated
        using columns available in the projection's input.
        """
        predicate_cols = self._extract_column_refs(filter_node.predicate)
        input_cols = self._get_column_names(projection.input)

        # Can we evaluate predicate using columns from input?
        can_push = self._can_evaluate_predicate(predicate_cols, input_cols)

        if can_push:
            # Push filter BELOW projection: Projection(Filter(input, pred), ...)
            new_filter = Filter(projection.input, filter_node.predicate)
            new_filter = self._push_down(new_filter)
            return Projection(
                new_filter,
                projection.expressions,
                projection.aliases,
                projection.distinct,
            )

        # Filter references columns not available in input, keep above
        new_input = self._push_down(projection.input)
        new_project = Projection(
            new_input,
            projection.expressions,
            projection.aliases,
            projection.distinct,
        )
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
                filters=merged,
                alias=scan.alias,
                group_by=scan.group_by,
                aggregates=scan.aggregates,
                output_names=scan.output_names,
                limit=scan.limit,
                offset=scan.offset,
                order_by_keys=scan.order_by_keys,
                order_by_ascending=scan.order_by_ascending,
                order_by_nulls=scan.order_by_nulls,
                distinct=scan.distinct,
            )

        return Scan(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=scan.columns,
            filters=filter_node.predicate,
            alias=scan.alias,
            group_by=scan.group_by,
            aggregates=scan.aggregates,
            output_names=scan.output_names,
            limit=scan.limit,
            offset=scan.offset,
            order_by_keys=scan.order_by_keys,
            order_by_ascending=scan.order_by_ascending,
            order_by_nulls=scan.order_by_nulls,
            distinct=scan.distinct,
        )

    def _predicate_matches_side(
        self,
        pred_cols: set,
        side_cols: set,
        other_cols: set
    ) -> bool:
        """Check if predicate columns match exactly one side of join.

        Handles both qualified and unqualified column references.
        Also handles alias vs physical name mismatches (e.g., "o.amount" vs "orders.amount").
        For both qualified and unqualified refs, checks if column uniquely belongs to this side.
        """
        for pred_col in pred_cols:
            # Exact match - pred_col is directly in side_cols
            if pred_col in side_cols:
                continue

            # Extract bare column name (part after last ".")
            if "." in pred_col:
                # Qualified reference like "o.amount" or "orders.amount"
                bare_col = pred_col.split(".")[-1]
            else:
                # Unqualified reference like "amount"
                bare_col = pred_col

            # Check if bare column name uniquely belongs to this side
            # This handles:
            # - Unqualified refs: "amount" matching "orders.amount"
            # - Alias mismatches: "o.amount" matching "orders.amount"
            side_has_col = False
            other_has_col = False

            for col in side_cols:
                if col.endswith(f".{bare_col}") or col == bare_col:
                    side_has_col = True
                    break

            for col in other_cols:
                if col.endswith(f".{bare_col}") or col == bare_col:
                    other_has_col = True
                    break

            # Column must be on this side only (not on other side)
            if side_has_col and not other_has_col:
                continue

            # Column is not uniquely on this side
            return False

        return True

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

        pred_left_only = self._predicate_matches_side(pred_cols, left_cols, right_cols)
        pred_right_only = self._predicate_matches_side(pred_cols, right_cols, left_cols)

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

        Returns qualified column names (e.g., "orders.id") for Scan nodes to
        enable correct filter pushdown when multiple tables have columns with
        the same name.

        Recursively handles wrapped nodes (Filter, Limit, etc).
        """
        if isinstance(plan, Scan):
            # Return qualified column names: "table.column" or "alias.column"
            # Use alias if present (e.g., "u" from "FROM users u")
            # Otherwise use physical table name
            # This prevents ambiguity when multiple tables have same column names
            table_ref = plan.alias if plan.alias else plan.table_name
            qualified = set()
            for col in plan.columns:
                qualified.add(f"{table_ref}.{col}")
            return qualified

        if isinstance(plan, Projection):
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
        """Extract qualified column names from expression.

        Returns column names with table qualifier when available (e.g., "orders.id"),
        or bare column names if no qualifier present (e.g., "id").

        This is critical for join filter pushdown: when both join inputs have
        columns with the same name, we must respect table qualifiers to avoid
        pushing filters to the wrong side.
        """
        from ..plan.expressions import ColumnRef, BinaryOp, UnaryOp, FunctionCall

        if isinstance(expr, ColumnRef):
            if expr.table:
                return {f"{expr.table}.{expr.column}"}
            return {expr.column}

        if isinstance(expr, BinaryOp):
            left = self._extract_column_refs(expr.left)
            right = self._extract_column_refs(expr.right)
            return left.union(right)

        if isinstance(expr, UnaryOp):
            return self._extract_column_refs(expr.operand)

        if isinstance(expr, FunctionCall):
            columns = set()
            for arg in expr.args:
                columns.update(self._extract_column_refs(arg))
            return columns

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
        if isinstance(plan, Projection):
            return True

        if isinstance(plan, Aggregate):
            # Aggregates have explicit output schema (group_by + aggregates)
            # so column pruning should be applied
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

        if isinstance(plan, Projection):
            # Collect columns from projection expressions
            for expr in plan.expressions:
                columns.update(self._extract_columns(expr))
            # CRITICAL: Must recurse into input to collect columns needed by
            # downstream operators (e.g., join keys, filter columns)
            # Otherwise those columns will be pruned and break the query!
            columns.update(self._collect_required_columns(plan.input))
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
            # Collect columns from group by expressions
            for expr in plan.group_by:
                columns.update(self._extract_columns(expr))
            # Collect columns from aggregate expressions
            for expr in plan.aggregates:
                columns.update(self._extract_columns(expr))
            # CRITICAL: Must recurse into input to collect columns needed by
            # downstream operators (e.g., join keys, filter columns)
            # Otherwise those columns will be pruned and break the query!
            columns.update(self._collect_required_columns(plan.input))
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

        if isinstance(expr, InList):
            columns.update(self._extract_columns(expr.value))
            for option in expr.options:
                columns.update(self._extract_columns(option))
            return columns

        if isinstance(expr, BetweenExpression):
            columns.update(self._extract_columns(expr.value))
            columns.update(self._extract_columns(expr.lower))
            columns.update(self._extract_columns(expr.upper))
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

        if isinstance(plan, Projection):
            new_input = self._prune_columns(plan.input, required)
            if new_input != plan.input:
                return Projection(
                    new_input,
                    plan.expressions,
                    plan.aliases,
                    plan.distinct,
                )
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
                filters=scan.filters,
                alias=scan.alias,
                group_by=scan.group_by,
                aggregates=scan.aggregates,
                output_names=scan.output_names,
                limit=scan.limit,
                offset=scan.offset,
                order_by_keys=scan.order_by_keys,
                order_by_ascending=scan.order_by_ascending,
                order_by_nulls=scan.order_by_nulls,
                distinct=scan.distinct,
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
        return self._rewrite_plan(plan)

    def _rewrite_plan(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recursively rewrite plan, pushing limits when safe."""
        handler = self._get_rewrite_handler(plan)
        if handler is None:
            return plan
        return handler(plan)

    def _get_rewrite_handler(self, plan: LogicalPlanNode):
        """Return rewrite handler for plan type."""
        handlers = {
            Limit: self._push_limit_node,
            Projection: self._rewrite_projection,
            Filter: self._rewrite_filter,
            Sort: self._rewrite_sort,
            Aggregate: self._rewrite_aggregate,
            Join: self._rewrite_join,
            Union: self._rewrite_union,
            Explain: self._rewrite_explain,
        }
        return handlers.get(type(plan))

    def _rewrite_projection(self, projection: Projection) -> LogicalPlanNode:
        """Rewrite projection child."""
        new_input = self._rewrite_plan(projection.input)
        if new_input != projection.input:
            return Projection(
                new_input,
                projection.expressions,
                projection.aliases,
                projection.distinct,
            )
        return projection

    def _rewrite_filter(self, filter_node: Filter) -> LogicalPlanNode:
        """Rewrite filter child."""
        new_input = self._rewrite_plan(filter_node.input)
        if new_input != filter_node.input:
            return Filter(new_input, filter_node.predicate)
        return filter_node

    def _rewrite_sort(self, sort: Sort) -> LogicalPlanNode:
        """Rewrite sort child."""
        new_input = self._rewrite_plan(sort.input)
        if new_input != sort.input:
            return Sort(new_input, sort.sort_keys, sort.ascending, sort.nulls_order)
        return sort

    def _rewrite_aggregate(self, aggregate: Aggregate) -> LogicalPlanNode:
        """Rewrite aggregate child."""
        new_input = self._rewrite_plan(aggregate.input)
        if new_input != aggregate.input:
            return Aggregate(
                input=new_input,
                group_by=aggregate.group_by,
                aggregates=aggregate.aggregates,
                output_names=aggregate.output_names
            )
        return aggregate

    def _rewrite_join(self, join: Join) -> LogicalPlanNode:
        """Rewrite join children."""
        new_left = self._rewrite_plan(join.left)
        new_right = self._rewrite_plan(join.right)
        if new_left != join.left or new_right != join.right:
            return Join(new_left, new_right, join.join_type, join.condition)
        return join

    def _rewrite_union(self, union: Union) -> LogicalPlanNode:
        """Rewrite union inputs."""
        new_inputs = []
        changed = False
        for child in union.inputs:
            rewritten = self._rewrite_plan(child)
            new_inputs.append(rewritten)
            if rewritten != child:
                changed = True
        if changed:
            return Union(new_inputs, union.distinct)
        return union

    def _rewrite_explain(self, explain: Explain) -> LogicalPlanNode:
        """Rewrite explain child."""
        new_input = self._rewrite_plan(explain.input)
        if new_input != explain.input:
            return Explain(new_input, explain.format)
        return explain

    def _push_limit_node(self, limit: Limit) -> LogicalPlanNode:
        """Push a single Limit node downward when safe."""
        input_node = self._rewrite_plan(limit.input)

        if isinstance(input_node, Projection):
            return self._push_through_projection(limit, input_node)

        if isinstance(input_node, Sort):
            return self._push_limit_with_sort(limit, input_node)

        if isinstance(input_node, Scan):
            scan_with_limit = self._apply_limit_metadata(input_node, limit.limit, limit.offset)
            return Limit(scan_with_limit, limit.limit, limit.offset)

        return Limit(input_node, limit.limit, limit.offset)

    def _push_through_projection(self, limit: Limit, projection: Projection) -> LogicalPlanNode:
        """Move limit below projection."""
        pushed_child = self._apply_limit_metadata(
            projection.input,
            limit.limit,
            limit.offset,
        )
        limited = Limit(pushed_child, limit.limit, limit.offset)
        return Projection(
            limited,
            projection.expressions,
            projection.aliases,
            projection.distinct,
        )

    def _push_limit_with_sort(self, limit: Limit, sort: Sort) -> LogicalPlanNode:
        """Move limit below sort and attach metadata to scan when possible."""
        sorted_input = self._rewrite_plan(sort.input)

        if isinstance(sorted_input, Scan):
            new_scan = Scan(
                datasource=sorted_input.datasource,
                schema_name=sorted_input.schema_name,
                table_name=sorted_input.table_name,
                columns=sorted_input.columns,
                filters=sorted_input.filters,
                alias=sorted_input.alias,
                group_by=sorted_input.group_by,
                aggregates=sorted_input.aggregates,
                output_names=sorted_input.output_names,
                order_by_keys=sort.sort_keys,
                order_by_ascending=sort.ascending,
                order_by_nulls=sort.nulls_order,
                limit=limit.limit,
                offset=limit.offset,
            )
            return Limit(new_scan, limit.limit, limit.offset)

        new_sort = Sort(
            input=sorted_input,
            sort_keys=sort.sort_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )
        return Limit(new_sort, limit.limit, limit.offset)

    def _apply_limit_metadata(
        self,
        node: LogicalPlanNode,
        limit_value: int,
        offset_value: int
    ) -> LogicalPlanNode:
        """Attach limit/offset metadata to scan while keeping plan shape."""
        if isinstance(node, Scan):
            return self._apply_limit_to_scan(node, limit_value, offset_value)
        return node

    def _apply_limit_to_scan(
        self,
        scan: Scan,
        limit_value: int,
        offset_value: int
    ) -> Scan:
        """Return new scan with updated limit metadata."""
        if scan.limit == limit_value and scan.offset == offset_value:
            return scan

        effective_limit = limit_value
        effective_offset = offset_value

        if scan.limit is not None and scan.limit < effective_limit:
            effective_limit = scan.limit
            effective_offset = scan.offset + offset_value
        elif scan.offset:
            effective_offset = scan.offset + offset_value

        return Scan(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=scan.columns,
            filters=scan.filters,
            alias=scan.alias,
            group_by=scan.group_by,
            aggregates=scan.aggregates,
            output_names=scan.output_names,
            limit=effective_limit,
            offset=effective_offset,
            order_by_keys=scan.order_by_keys,
            order_by_ascending=scan.order_by_ascending,
            order_by_nulls=scan.order_by_nulls,
            distinct=scan.distinct,
        )

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


class OrderByPushdownRule(OptimizationRule):
    """Push ORDER BY clauses to data sources when safe.

    Transforms:
        Sort(Projection?(Filter?(Scan))) → Projection?(Filter?(Scan(with order by)))

    This pushes ORDER BY to the data source for execution.
    ORDER BY pushdown is mandatory for correctness in many use cases:
    - Cursor processing over timeseries data
    - Streaming results in deterministic order
    - Sequential data processing
    """

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        """Apply ORDER BY pushdown optimization."""
        return self._push_order_by(plan)

    def _push_order_by(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recursively push ORDER BY down the plan tree."""
        if isinstance(plan, Sort):
            return self._try_push_sort(plan)

        return self._recurse_node(plan)

    def _try_push_sort(self, sort: Sort) -> LogicalPlanNode:
        """Try to push sort into its input."""
        if not self._sort_keys_are_columns(sort.sort_keys):
            pushed_child = self._attach_order_metadata(sort.input, sort)
            return Sort(
                input=pushed_child,
                sort_keys=sort.sort_keys,
                ascending=sort.ascending,
                nulls_order=sort.nulls_order,
            )

        input_node = sort.input

        if isinstance(input_node, Projection):
            return self._push_through_projection(sort, input_node)

        if isinstance(input_node, Filter):
            return self._push_through_filter(sort, input_node)

        if isinstance(input_node, Join):
            return self._push_through_join(sort, input_node)

        if isinstance(input_node, Aggregate):
            return self._push_through_aggregate(sort, input_node)

        if isinstance(input_node, Union):
            return self._push_through_union(sort, input_node)

        if isinstance(input_node, Scan):
            return self._push_to_scan(sort, input_node)

        return sort

    def _sort_keys_are_columns(self, sort_keys: List[Expression]) -> bool:
        """Return True only if all sort keys are simple column references."""
        from ..plan.expressions import ColumnRef

        for key in sort_keys:
            if not isinstance(key, ColumnRef):
                return False
        return True

    def _propagate_sort_metadata(self, node: LogicalPlanNode, sort: Sort) -> None:
        """Attach sort metadata to the lowest scan without removing top sort."""
        if isinstance(node, Scan):
            self._push_to_scan(sort, node)
            return
        child = getattr(node, "input", None)
        if isinstance(child, LogicalPlanNode):
            self._propagate_sort_metadata(child, sort)

    def _attach_order_metadata(
        self, node: LogicalPlanNode, sort: Sort
    ) -> LogicalPlanNode:
        """Return a new subtree with order metadata pushed into scans."""
        if isinstance(node, Scan):
            return self._push_to_scan(sort, node)

        if isinstance(node, Projection):
            new_input = self._attach_order_metadata(node.input, sort)
            return Projection(new_input, node.expressions, node.aliases, node.distinct)

        if isinstance(node, Filter):
            new_input = self._attach_order_metadata(node.input, sort)
            return Filter(new_input, node.predicate)

        if isinstance(node, Limit):
            new_input = self._attach_order_metadata(node.input, sort)
            return Limit(new_input, node.limit, node.offset)

        if isinstance(node, Aggregate):
            new_input = self._attach_order_metadata(node.input, sort)
            return Aggregate(new_input, node.group_by, node.aggregates, node.output_names)

        if isinstance(node, Sort):
            new_input = self._attach_order_metadata(node.input, sort)
            return Sort(new_input, node.sort_keys, node.ascending, node.nulls_order)

        return node

    def _push_to_scan(self, sort: Sort, scan: Scan) -> Scan:
        """Push ORDER BY into scan node."""
        return Scan(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=scan.columns,
            filters=scan.filters,
            alias=scan.alias,
            group_by=scan.group_by,
            aggregates=scan.aggregates,
            output_names=scan.output_names,
            limit=scan.limit,
            offset=scan.offset,
            order_by_keys=sort.sort_keys,
            order_by_ascending=sort.ascending,
            order_by_nulls=sort.nulls_order,
            distinct=scan.distinct,
        )

    def _push_through_projection(self, sort: Sort, projection: Projection) -> LogicalPlanNode:
        """Push ORDER BY through projection if all sort columns available."""
        rewritten_keys = self._rewrite_sort_keys_for_projection(
            sort.sort_keys,
            projection,
        )
        if rewritten_keys is None:
            return sort

        rewritten_sort = Sort(
            input=projection.input,
            sort_keys=rewritten_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )
        pushed_child = self._push_order_by(rewritten_sort)

        if isinstance(pushed_child, Sort):
            rebuilt_projection = Projection(
                pushed_child.input,
                projection.expressions,
                projection.aliases,
                projection.distinct,
            )
            return Sort(
                input=rebuilt_projection,
                sort_keys=sort.sort_keys,
                ascending=sort.ascending,
                nulls_order=sort.nulls_order,
            )

        return Projection(
            pushed_child,
            projection.expressions,
            projection.aliases,
            projection.distinct,
        )

    def _push_through_filter(self, sort: Sort, filter_node: Filter) -> LogicalPlanNode:
        """Push ORDER BY through filter (always safe)."""
        pushed_child = self._push_sort_into_child(sort, filter_node.input)
        return Filter(pushed_child, filter_node.predicate)

    def _push_through_join(self, sort: Sort, join: Join) -> LogicalPlanNode:
        """Push ORDER BY metadata into a single join side when safe."""
        if isinstance(join.left, Scan) and isinstance(join.right, Scan):
            if join.left.datasource != join.right.datasource:
                return sort
        sort_cols = self._extract_sort_columns(sort)
        left_cols = self._get_available_columns(join.left)
        right_cols = self._get_available_columns(join.right)

        left_only = self._columns_match_side(sort_cols, left_cols, right_cols)
        right_only = self._columns_match_side(sort_cols, right_cols, left_cols)

        new_left = self._push_sort_into_child(sort, join.left) if left_only else join.left
        new_right = self._push_sort_into_child(sort, join.right) if right_only else join.right

        rebuilt = Join(new_left, new_right, join.join_type, join.condition)
        return Sort(
            input=rebuilt,
            sort_keys=sort.sort_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )

    def _push_sort_into_child(
        self,
        sort: Sort,
        child: LogicalPlanNode,
    ) -> LogicalPlanNode:
        """Push sort keys into a specific child and return transformed child."""
        injected = Sort(
            input=child,
            sort_keys=sort.sort_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )
        pushed = self._push_order_by(injected)
        if isinstance(pushed, Sort):
            return pushed.input
        return pushed

    def _columns_match_side(
        self,
        sort_cols: set,
        side_cols: set,
        other_cols: set,
    ) -> bool:
        """Check if sort columns belong exclusively to one join side."""
        for col in sort_cols:
            if col in side_cols:
                continue

            bare = col.split(".")[-1]
            side_has = any(entry.endswith(f".{bare}") or entry == bare for entry in side_cols)
            other_has = any(entry.endswith(f".{bare}") or entry == bare for entry in other_cols)

            if side_has and not other_has:
                continue

            return False

        return True

    def _push_through_aggregate(self, sort: Sort, aggregate: Aggregate) -> LogicalPlanNode:
        """Push ORDER BY through aggregate when keys align with aggregate output."""
        sort_cols = self._extract_sort_columns(sort)
        output_cols = set(aggregate.output_names)
        if not sort_cols.issubset(output_cols):
            return sort

        group_cols = self._extract_group_columns(aggregate.group_by)
        if not sort_cols.issubset(group_cols):
            return sort

        pushed_child = self._push_sort_into_child(sort, aggregate.input)
        new_agg = Aggregate(
            input=pushed_child,
            group_by=aggregate.group_by,
            aggregates=aggregate.aggregates,
            output_names=aggregate.output_names,
        )
        return Sort(
            input=new_agg,
            sort_keys=sort.sort_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )

    def _extract_group_columns(self, group_by: List[Expression]) -> set:
        """Extract column names from group by expressions."""
        from ..plan.expressions import ColumnRef

        columns = set()
        for expr in group_by:
            if isinstance(expr, ColumnRef):
                if expr.table:
                    columns.add(f"{expr.table}.{expr.column}")
                columns.add(expr.column)
        return columns

    def _push_through_union(self, sort: Sort, union: Union) -> LogicalPlanNode:
        """Propagate sort metadata into union inputs while keeping top sort."""
        new_inputs = []
        for child in union.inputs:
            injected = Sort(
                input=child,
                sort_keys=sort.sort_keys,
                ascending=sort.ascending,
                nulls_order=sort.nulls_order,
            )
            pushed_child = self._push_order_by(injected)
            if isinstance(pushed_child, Sort):
                new_inputs.append(pushed_child.input)
            else:
                new_inputs.append(pushed_child)

        new_union = Union(new_inputs, union.distinct)
        return Sort(
            input=new_union,
            sort_keys=sort.sort_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )

    def _get_available_columns(self, plan: LogicalPlanNode) -> set:
        """Get all column names available from a plan node."""
        if isinstance(plan, Scan):
            columns = set()
            table_ref = plan.alias if plan.alias else plan.table_name
            for col in plan.columns:
                columns.add(col)
                columns.add(f"{table_ref}.{col}")
            return columns

        if isinstance(plan, Projection):
            columns = set()
            for alias in plan.aliases:
                columns.add(alias)

            if "*" in columns:
                return self._get_available_columns(plan.input)

            index = 0
            for expr in plan.expressions:
                from ..plan.expressions import ColumnRef

                if isinstance(expr, ColumnRef):
                    if expr.table:
                        columns.add(f"{expr.table}.{expr.column}")
                    columns.add(expr.column)
                index += 1

            return columns

        if isinstance(plan, Filter):
            return self._get_available_columns(plan.input)

        return set()

    def _extract_sort_columns(self, sort: Sort) -> set:
        """Extract column names from sort keys."""
        from ..plan.expressions import ColumnRef

        columns = set()
        for key in sort.sort_keys:
            if isinstance(key, ColumnRef):
                if key.table:
                    columns.add(f"{key.table}.{key.column}")
                columns.add(key.column)

        return columns

    def _rewrite_sort_keys_for_projection(
        self,
        sort_keys: List[Expression],
        projection: Projection,
    ) -> Optional[List[Expression]]:
        """Rewrite projection sort keys to input expressions when aliases are used."""
        alias_map = self._build_projection_alias_map(projection)
        available = self._get_available_columns(projection.input)
        rewritten: List[Expression] = []
        from ..plan.expressions import ColumnRef

        for key in sort_keys:
            if isinstance(key, ColumnRef):
                mapped = self._map_projection_sort_column(
                    key,
                    alias_map,
                    available,
                )
                if mapped is None:
                    return None
                rewritten.append(mapped)
                continue
            rewritten.append(key)

        return rewritten

    def _build_projection_alias_map(
        self,
        projection: Projection,
    ) -> dict:
        """Build a mapping from projection aliases to expressions."""
        alias_map = {}
        index = 0
        for alias in projection.aliases:
            expr = projection.expressions[index]
            alias_map[alias] = expr
            index += 1
        return alias_map

    def _map_projection_sort_column(
        self,
        key: Expression,
        alias_map: dict,
        available: set,
    ) -> Optional[Expression]:
        """Map a projection sort column to an input expression if possible."""
        from ..plan.expressions import ColumnRef

        if not isinstance(key, ColumnRef):
            return key

        if key.table is None and key.column in alias_map:
            return alias_map[key.column]

        if key.table:
            qualified = f"{key.table}.{key.column}"
            if qualified in available:
                return key

        if key.column in available:
            return key

        return None

    def _recurse_node(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recurse into node children."""
        if isinstance(plan, (Projection, Filter, Limit, Sort)):
            return self._recurse_unary(plan)

        if isinstance(plan, Join):
            return self._recurse_join(plan)

        if isinstance(plan, Aggregate):
            return self._recurse_aggregate(plan)

        return plan

    def _recurse_unary(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recurse into unary node."""
        new_input = self._push_order_by(plan.input)
        if new_input == plan.input:
            return plan

        if isinstance(plan, Projection):
            return Projection(
                new_input,
                plan.expressions,
                plan.aliases,
                plan.distinct,
            )

        if isinstance(plan, Filter):
            if isinstance(new_input, Scan) and new_input.aggregates:
                merged_filters = self._merge_filters(new_input.filters, plan.predicate)
                return Scan(
                    datasource=new_input.datasource,
                    schema_name=new_input.schema_name,
                    table_name=new_input.table_name,
                    columns=new_input.columns,
                    filters=merged_filters,
                    alias=new_input.alias,
                    group_by=new_input.group_by,
                    aggregates=new_input.aggregates,
                    output_names=new_input.output_names,
                    limit=new_input.limit,
                    offset=new_input.offset,
                    order_by_keys=new_input.order_by_keys,
                    order_by_ascending=new_input.order_by_ascending,
                    order_by_nulls=new_input.order_by_nulls,
                    distinct=new_input.distinct,
                )
            return Filter(new_input, plan.predicate)

        if isinstance(plan, Limit):
            return Limit(new_input, plan.limit, plan.offset)

        if isinstance(plan, Sort):
            return Sort(
                input=new_input,
                sort_keys=plan.sort_keys,
                ascending=plan.ascending,
                nulls_order=plan.nulls_order,
            )

        return plan

    def _recurse_join(self, join: Join) -> LogicalPlanNode:
        """Recurse into join node."""
        new_left = self._push_order_by(join.left)
        new_right = self._push_order_by(join.right)

        if new_left == join.left and new_right == join.right:
            return join

        return Join(new_left, new_right, join.join_type, join.condition)

    def _recurse_aggregate(self, agg: Aggregate) -> LogicalPlanNode:
        """Recurse into aggregate node."""
        new_input = self._push_order_by(agg.input)
        if new_input == agg.input:
            return agg

        return Aggregate(
            new_input,
            agg.group_by,
            agg.aggregates,
            agg.output_names
        )

    def name(self) -> str:
        return "OrderByPushdown"


class AggregatePushdownRule(OptimizationRule):
    """Push aggregates to data sources when possible.

    Transforms:
        Aggregate(Filter?(Scan)) → Scan(with aggregates)

    This pushes GROUP BY and aggregate functions (COUNT, SUM, etc.)
    to the data source for execution, reducing data transfer.
    """

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        """Apply aggregate pushdown optimization."""
        return self._push_aggregate(plan)

    def _push_aggregate(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recursively push aggregates down the plan tree."""
        if isinstance(plan, Aggregate):
            return self._try_push_aggregate(plan)

        return self._recurse_node(plan)

    def _try_push_aggregate(self, agg: Aggregate) -> LogicalPlanNode:
        """Try to push aggregate into its input."""
        input_node = agg.input

        if isinstance(input_node, Scan):
            return self._push_to_scan(agg, input_node, None)

        if isinstance(input_node, Filter):
            filter_input = input_node.input
            if isinstance(filter_input, Scan):
                return self._push_to_scan(agg, filter_input, input_node.predicate)

        return agg

    def _push_to_scan(
        self,
        agg: Aggregate,
        scan: Scan,
        filter_expr: Optional[Expression]
    ) -> Scan:
        """Push aggregate into scan node."""
        merged_filters = self._merge_filters(scan.filters, filter_expr)

        return Scan(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=scan.columns,
            filters=merged_filters,
            alias=scan.alias,
            group_by=agg.group_by,
            aggregates=agg.aggregates,
            output_names=agg.output_names,
            limit=scan.limit,
            offset=scan.offset,
            order_by_keys=scan.order_by_keys,
            order_by_ascending=scan.order_by_ascending,
            order_by_nulls=scan.order_by_nulls,
            distinct=scan.distinct,
        )

    def _merge_filters(
        self,
        scan_filter: Optional[Expression],
        filter_filter: Optional[Expression]
    ) -> Optional[Expression]:
        """Merge filters from scan and filter node."""
        if scan_filter is None:
            return filter_filter

        if filter_filter is None:
            return scan_filter

        from ..plan.expressions import BinaryOp, BinaryOpType
        return BinaryOp(
            op=BinaryOpType.AND,
            left=scan_filter,
            right=filter_filter
        )

    def _recurse_node(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recurse into node children."""
        if isinstance(plan, (Projection, Filter, Limit, Sort)):
            return self._recurse_unary(plan)

        if isinstance(plan, Join):
            return self._recurse_join(plan)

        return plan

    def _recurse_unary(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recurse into unary node."""
        new_input = self._push_aggregate(plan.input)
        if new_input == plan.input:
            return plan

        if isinstance(plan, Projection):
            return Projection(
                new_input,
                plan.expressions,
                plan.aliases,
                plan.distinct,
            )

        if isinstance(plan, Filter):
            return Filter(new_input, plan.predicate)

        if isinstance(plan, Limit):
            return Limit(new_input, plan.limit, plan.offset)

        if isinstance(plan, Sort):
            return Sort(new_input, plan.sort_keys, plan.ascending, plan.nulls_order)

        return plan

    def _recurse_join(self, join: Join) -> LogicalPlanNode:
        """Recurse into join node."""
        new_left = self._push_aggregate(join.left)
        new_right = self._push_aggregate(join.right)

        if new_left == join.left and new_right == join.right:
            return join

        return Join(new_left, new_right, join.join_type, join.condition)

    def name(self) -> str:
        return "AggregatePushdown"


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
                        filters=rewritten_filter,
                        alias=plan.alias,
                        group_by=plan.group_by,
                        aggregates=plan.aggregates,
                        output_names=plan.output_names,
                        limit=plan.limit,
                        offset=plan.offset,
                        order_by_keys=plan.order_by_keys,
                        order_by_ascending=plan.order_by_ascending,
                        order_by_nulls=plan.order_by_nulls,
                        distinct=getattr(plan, "distinct", False),
                    )
            return plan

        if isinstance(plan, Projection):
            rewritten_input = self._rewrite_plan(plan.input)
            rewritten_exprs = []
            changed = False

            for expr in plan.expressions:
                rewritten = self.rewriter.rewrite(expr)
                rewritten_exprs.append(rewritten)
                if rewritten != expr:
                    changed = True

            if changed or rewritten_input != plan.input:
                return Projection(
                    rewritten_input,
                    rewritten_exprs,
                    plan.aliases,
                    plan.distinct,
                )
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

    def optimize(
        self,
        plan: LogicalPlanNode,
        max_iterations: int = 10,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> LogicalPlanNode:
        """Optimize a logical plan using registered rules.

        Applies rules iteratively until fixed point or max iterations.

        Args:
            plan: Input logical plan
            max_iterations: Maximum number of optimization passes
            query_executor: Optional executor reference for context

        Returns:
            Optimized logical plan
        """
        if isinstance(plan, Explain):
            optimized_child = self.optimize(
                plan.input,
                max_iterations,
                query_executor=query_executor,
            )
            return Explain(optimized_child, plan.format)

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
