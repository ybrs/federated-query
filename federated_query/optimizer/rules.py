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
    SetOperation,
    Explain,
    transform_children,
)
from . import pushdown
from .scope_validator import validate_scope
from ..plan.expressions import (
    Expression,
    InList,
    BetweenExpression,
    and_expressions,
)
from ..catalog.catalog import Catalog

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


def _rewrite_set_operation_branches(set_op: SetOperation, rewrite) -> SetOperation:
    """Apply a recursive rewrite to both branches of a set operation.

    Pushdown rules descend the tree by node type; a set operation is opaque to
    them otherwise, so its branches (each a full subquery) would never be
    optimized. Rebuilding with the rewritten branches keeps every rule reaching
    inside UNION/INTERSECT/EXCEPT.
    """
    new_left = rewrite(set_op.left)
    new_right = rewrite(set_op.right)
    return set_op.model_copy(update={"left": new_left, "right": new_right})


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
        """Recursively push predicates down the plan tree.

        Filter is where pushdown happens; every other node here just recurses
        into its children and rebuilds if one changed (transform_children). A
        SetOperation rewrites its two branches; an unhandled node bottoms out.
        """
        if isinstance(plan, Filter):
            return self._push_filter(plan)
        if isinstance(plan, SetOperation):
            return _rewrite_set_operation_branches(plan, self._push_down)
        if isinstance(plan, (Projection, Join, Aggregate, Limit, Sort)):
            return transform_children(plan, self._push_down)
        return plan

    def _push_filter(self, filter_node: Filter) -> LogicalPlanNode:
        """Push a filter node down."""
        input_plan = filter_node.input
        predicate = filter_node.predicate

        from ..plan.expressions import BinaryOp, BinaryOpType

        is_conjunction = (
            isinstance(predicate, BinaryOp) and predicate.op == BinaryOpType.AND
        )
        if is_conjunction and self._can_absorb_split(input_plan):
            # Split a conjunctive predicate: wrap the child in a Filter carrying
            # only the left conjunct so each half can be pushed independently.
            left_result = self._push_filter(
                Filter.create(input=input_plan, predicate=predicate.left)
            )
            # Stack the right conjunct as its own Filter over the already-pushed
            # left result, so the remaining half descends on top of it.
            return self._push_filter(
                Filter.create(input=left_result, predicate=predicate.right)
            )

        if isinstance(input_plan, Filter):
            return self._merge_filters(filter_node, input_plan)

        if isinstance(input_plan, Projection):
            return self._push_filter_through_projection(filter_node, input_plan)

        if isinstance(input_plan, Join):
            return self._push_filter_below_join(filter_node, input_plan)

        if isinstance(input_plan, Scan):
            return self._push_filter_to_scan(filter_node, input_plan)

        new_input = self._push_down(input_plan)
        if new_input != input_plan:
            # The predicate could not descend further, but the child subtree was
            # rewritten; re-home the same filter over its optimized input.
            return filter_node.model_copy(update={"input": new_input})

        return filter_node

    def _can_absorb_split(self, input_plan: LogicalPlanNode) -> bool:
        """Whether splitting a conjunction helps for this input.

        Splitting distributes conjuncts into a join's sides or pushes them
        into a scan/projection. Over any other input (e.g. an Aggregate
        carrying HAVING) the parts cannot descend, and splitting then merging
        them back would recurse forever.
        """
        return isinstance(input_plan, (Join, Scan, Projection))

    def _merge_filters(self, outer: Filter, inner: Filter) -> LogicalPlanNode:
        """Merge two adjacent filters."""
        from ..plan.expressions import BinaryOp, BinaryOpType

        # Two stacked filters collapse into one: AND their predicates so a single
        # Filter can carry the combined condition of the outer and inner filters.
        merged_predicate = BinaryOp.create(
            op=BinaryOpType.AND, left=outer.predicate, right=inner.predicate
        )

        # A single Filter over the inner filter's input, carrying the merged
        # predicate, then re-run pushdown on the collapsed node.
        new_filter = Filter.create(input=inner.input, predicate=merged_predicate)
        return self._push_down(new_filter)

    def _can_evaluate_predicate(self, pred_cols: set, available_cols: set) -> bool:
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
        self, filter_node: Filter, projection: Projection
    ) -> LogicalPlanNode:
        """Push filter through projection if possible.

        Push filter BELOW projection if the predicate can be evaluated
        using columns available in the projection's input.
        """
        predicate_cols = pushdown.qualified_or_bare_names(filter_node.predicate)
        input_cols = pushdown.available_columns(projection.input)

        # Can we evaluate predicate using columns from input?
        can_push = self._can_evaluate_predicate(predicate_cols, input_cols)

        if can_push:
            # Push the filter BELOW the projection so it runs before column output.
            # Re-home the original filter over the projection's input, keeping its
            # predicate, since those input columns can evaluate the predicate.
            new_filter = filter_node.model_copy(update={"input": projection.input})
            new_filter = self._push_down(new_filter)
            return projection.model_copy(update={"input": new_filter})

        # Filter references columns not available in input, keep above
        new_input = self._push_down(projection.input)
        new_project = projection.model_copy(update={"input": new_input})
        # The predicate needs columns the projection does not expose, so keep the
        # filter above: re-home the original filter over the rebuilt projection.
        return filter_node.model_copy(update={"input": new_project})

    def _push_filter_to_scan(self, filter_node: Filter, scan: Scan) -> LogicalPlanNode:
        """Push filter into scan node."""
        from ..plan.expressions import BinaryOp, BinaryOpType

        if scan.filters:
            # The scan already carries a pushed predicate; AND the new filter's
            # predicate onto it so both conditions apply at the source.
            merged = BinaryOp.create(
                op=BinaryOpType.AND, left=scan.filters, right=filter_node.predicate
            )
            return scan.model_copy(update={"filters": merged})

        return scan.model_copy(update={"filters": filter_node.predicate})

    def _push_filter_below_join(
        self, filter_node: Filter, join: Join
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
        left_cols = pushdown.available_columns(join.left)
        right_cols = pushdown.available_columns(join.right)

        pred_cols = pushdown.qualified_or_bare_names(predicate)

        pred_left_only = pushdown.columns_belong_to_side(
            pred_cols, left_cols, right_cols
        )
        pred_right_only = pushdown.columns_belong_to_side(
            pred_cols, right_cols, left_cols
        )

        # Only push for INNER joins. model_copy preserves every other field
        # (notably NATURAL / USING), which a raw Join(...) rebuild would drop.
        if join.join_type != JoinType.INNER:
            # For outer joins, keep filter above join
            new_join = join.model_copy(
                update={
                    "left": self._push_down(join.left),
                    "right": self._push_down(join.right),
                }
            )
            # Outer-join semantics forbid pushing the predicate below the join, so
            # keep the original filter on top of the recursed join.
            return filter_node.model_copy(update={"input": new_join})

        # INNER JOIN: safe to push
        if pred_left_only:
            # The predicate touches only left-side columns; re-home the original
            # filter onto the join's left input so it runs before the join.
            new_left = filter_node.model_copy(update={"input": join.left})
            new_left = self._push_down(new_left)
            return join.model_copy(
                update={"left": new_left, "right": self._push_down(join.right)}
            )

        if pred_right_only:
            # The predicate touches only right-side columns; re-home the original
            # filter onto the join's right input so it runs before the join.
            new_right = filter_node.model_copy(update={"input": join.right})
            new_right = self._push_down(new_right)
            return join.model_copy(
                update={"left": self._push_down(join.left), "right": new_right}
            )

        # Predicate spans both sides of an INNER join. A cross-side equality is
        # a join key: fold it into the ON condition so a comma/cross join
        # becomes an equi-join (hash join) instead of a Cartesian product with a
        # filter on top. Non-equi cross-side predicates stay as a filter.
        if self._is_equi_predicate(predicate):
            merged_condition = self._merge_join_condition(join.condition, predicate)
            return join.model_copy(
                update={
                    "left": self._push_down(join.left),
                    "right": self._push_down(join.right),
                    "condition": merged_condition,
                }
            )

        new_join = join.model_copy(
            update={
                "left": self._push_down(join.left),
                "right": self._push_down(join.right),
            }
        )
        # A non-equi predicate spanning both sides cannot become a join key, so
        # keep the original filter above the recursed join.
        return filter_node.model_copy(update={"input": new_join})

    def _is_equi_predicate(self, predicate: Expression) -> bool:
        """Whether a predicate is a column-to-column equality (a join key)."""
        from ..plan.expressions import BinaryOp, BinaryOpType, ColumnRef

        return (
            isinstance(predicate, BinaryOp)
            and predicate.op == BinaryOpType.EQ
            and isinstance(predicate.left, ColumnRef)
            and isinstance(predicate.right, ColumnRef)
        )

    def _merge_join_condition(
        self, existing: Optional[Expression], predicate: Expression
    ) -> Expression:
        """AND a freshly inferred equi-condition into a join's ON clause."""
        from ..plan.expressions import BinaryOp, BinaryOpType

        if existing is None:
            return predicate
        # A join already has an ON condition; AND the inferred equi-predicate onto
        # it so the folded join key extends the existing condition.
        return BinaryOp.create(op=BinaryOpType.AND, left=existing, right=predicate)

    def name(self) -> str:
        """Return this rule's identifier (used in logging and EXPLAIN)."""
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

    def _collect_required_columns(self, plan: LogicalPlanNode) -> set:
        """Collect all required column names from plan."""
        columns = set()

        if isinstance(plan, Scan):
            if plan.filters:
                columns.update(pushdown.bare_names(plan.filters))
            return columns

        if isinstance(plan, Projection):
            # Collect columns from projection expressions
            for expr in plan.expressions:
                columns.update(pushdown.bare_names(expr))
            # CRITICAL: Must recurse into input to collect columns needed by
            # downstream operators (e.g., join keys, filter columns)
            # Otherwise those columns will be pruned and break the query!
            columns.update(self._collect_required_columns(plan.input))
            return columns

        if isinstance(plan, Filter):
            columns.update(pushdown.bare_names(plan.predicate))
            columns.update(self._collect_required_columns(plan.input))
            return columns

        if isinstance(plan, Join):
            if plan.condition:
                columns.update(pushdown.bare_names(plan.condition))
            columns.update(self._collect_required_columns(plan.left))
            columns.update(self._collect_required_columns(plan.right))
            return columns

        if isinstance(plan, Aggregate):
            # Collect columns from group by expressions
            for expr in plan.group_by:
                columns.update(pushdown.bare_names(expr))
            # Collect columns from aggregate expressions
            for expr in plan.aggregates:
                columns.update(pushdown.bare_names(expr))
            # CRITICAL: Must recurse into input to collect columns needed by
            # downstream operators (e.g., join keys, filter columns)
            # Otherwise those columns will be pruned and break the query!
            columns.update(self._collect_required_columns(plan.input))
            return columns

        if isinstance(plan, Limit):
            columns.update(self._collect_required_columns(plan.input))
            return columns

        if isinstance(plan, Sort):
            for key in plan.sort_keys:
                columns.update(pushdown.bare_names(key))
            columns.update(self._collect_required_columns(plan.input))
            return columns

        return columns

    def _prune_columns(self, plan: LogicalPlanNode, required: set) -> LogicalPlanNode:
        """Prune unused columns from plan."""
        if isinstance(plan, Scan):
            return self._prune_scan_columns(plan, required)

        if isinstance(plan, Projection):
            new_input = self._prune_columns(plan.input, required)
            if new_input != plan.input:
                return plan.model_copy(update={"input": new_input})
            return plan

        if isinstance(plan, Filter):
            filter_cols = pushdown.bare_names(plan.predicate)
            combined_required = required.union(filter_cols)
            new_input = self._prune_columns(plan.input, combined_required)
            if new_input != plan.input:
                return plan.model_copy(update={"input": new_input})
            return plan

        if isinstance(plan, Join):
            left_req = self._get_required_for_subtree(plan.left, required)
            right_req = self._get_required_for_subtree(plan.right, required)
            new_left = self._prune_columns(plan.left, left_req)
            new_right = self._prune_columns(plan.right, right_req)
            if new_left != plan.left or new_right != plan.right:
                return plan.model_copy(update={"left": new_left, "right": new_right})
            return plan

        if isinstance(plan, Aggregate):
            new_input = self._prune_columns(plan.input, required)
            if new_input != plan.input:
                return plan.model_copy(update={"input": new_input})
            return plan

        if isinstance(plan, Limit):
            new_input = self._prune_columns(plan.input, required)
            if new_input != plan.input:
                return plan.model_copy(update={"input": new_input})
            return plan

        if isinstance(plan, Sort):
            sort_cols = set()
            for key in plan.sort_keys:
                sort_cols.update(pushdown.bare_names(key))
            new_input = self._prune_columns(plan.input, required.union(sort_cols))
            if new_input != plan.input:
                return plan.model_copy(update={"input": new_input})
            return plan

        return plan

    def _prune_scan_columns(self, scan: Scan, required: set) -> Scan:
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

            return scan.model_copy(update={"columns": pruned_cols})

        return scan

    def _get_required_for_subtree(
        self, plan: LogicalPlanNode, parent_required: set
    ) -> set:
        """Get required columns for a subtree."""
        local_required = set()

        if isinstance(plan, Scan):
            if plan.filters:
                local_required.update(pushdown.bare_names(plan.filters))

        if isinstance(plan, Filter):
            local_required.update(pushdown.bare_names(plan.predicate))

        return local_required.union(parent_required)

    def name(self) -> str:
        """Return this rule's identifier (used in logging and EXPLAIN)."""
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
        """Recursively rewrite plan, pushing limits when safe.

        A Limit is pushed downward (_push_limit_node); every other handled node
        just recurses into its children (transform_children). An unhandled node
        type bottoms out unchanged.
        """
        if isinstance(plan, Limit):
            return self._push_limit_node(plan)
        if isinstance(plan, (Projection, Filter, Sort, Aggregate, Join, Union, Explain)):
            return transform_children(plan, self._rewrite_plan)
        return plan

    def _push_limit_node(self, limit: Limit) -> LogicalPlanNode:
        """Push a single Limit node downward when safe."""
        input_node = self._rewrite_plan(limit.input)

        if isinstance(input_node, Projection):
            return self._push_through_projection(limit, input_node)

        if isinstance(input_node, Sort):
            return self._push_limit_with_sort(limit, input_node)

        if isinstance(input_node, Scan):
            scan_with_limit = self._apply_limit_metadata(
                input_node, limit.limit, limit.offset
            )
            return limit.model_copy(update={"input": scan_with_limit, "offset": 0})

        return limit.model_copy(update={"input": input_node})

    def _distinct_blocks_pushdown(self, projection: Projection) -> bool:
        """Whether a DISTINCT projection blocks pushing a LIMIT below it.

        Pushing a LIMIT below DISTINCT over a Scan is safe: it renders as one
        ``SELECT DISTINCT ... LIMIT`` pushed to the single source. Over any
        other child (e.g. a cross-source Join) the DISTINCT runs locally, so a
        LIMIT pushed beneath it would cap rows before deduplication and return
        too few distinct rows - the LIMIT must stay above.
        """
        if not (projection.distinct or projection.distinct_on is not None):
            return False
        return not isinstance(projection.input, Scan)

    def _push_through_projection(
        self, limit: Limit, projection: Projection
    ) -> LogicalPlanNode:
        """Move limit below projection.

        The OFFSET is consumed by the scan only when the child is a scan; if
        it is not, the outer Limit must retain the offset rather than zero a
        value that was never pushed down (which would corrupt pagination).
        """
        if self._distinct_blocks_pushdown(projection):
            return limit.model_copy(update={"input": projection})
        pushed_child = self._apply_limit_metadata(
            projection.input,
            limit.limit,
            limit.offset,
        )
        retained_offset = 0 if isinstance(projection.input, Scan) else limit.offset
        limited = limit.model_copy(
            update={"input": pushed_child, "offset": retained_offset}
        )
        return projection.model_copy(update={"input": limited})

    def _push_limit_with_sort(self, limit: Limit, sort: Sort) -> LogicalPlanNode:
        """Move limit below sort and attach metadata to scan when possible."""
        sorted_input = self._rewrite_plan(sort.input)

        if isinstance(sorted_input, Scan):
            new_scan = sorted_input.model_copy(
                update={
                    "order_by_keys": sort.sort_keys,
                    "order_by_ascending": sort.ascending,
                    "order_by_nulls": sort.nulls_order,
                    "limit": limit.limit,
                    "offset": limit.offset,
                }
            )
            return limit.model_copy(update={"input": new_scan, "offset": 0})

        new_sort = sort.model_copy(update={"input": sorted_input})
        return limit.model_copy(update={"input": new_sort})

    def _apply_limit_metadata(
        self, node: LogicalPlanNode, limit_value: int, offset_value: int
    ) -> LogicalPlanNode:
        """Attach limit/offset metadata to scan while keeping plan shape."""
        if isinstance(node, Scan):
            return self._apply_limit_to_scan(node, limit_value, offset_value)
        return node

    def _apply_limit_to_scan(
        self, scan: Scan, limit_value: int, offset_value: int
    ) -> Scan:
        """Return new scan with updated limit metadata."""
        if scan.limit == limit_value and scan.offset == offset_value:
            return scan

        effective_limit = limit_value
        effective_offset = offset_value

        scan_limit_tighter = scan.limit is not None and (
            limit_value is None or scan.limit < limit_value
        )
        if scan_limit_tighter:
            effective_limit = scan.limit
            effective_offset = scan.offset + offset_value
        elif scan.offset:
            effective_offset = scan.offset + offset_value

        return scan.model_copy(
            update={"limit": effective_limit, "offset": effective_offset}
        )

    def name(self) -> str:
        """Return this rule's identifier (used in logging and EXPLAIN)."""
        return "LimitPushdown"


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
            return sort.model_copy(update={"input": pushed_child})

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

    def _attach_order_metadata(
        self, node: LogicalPlanNode, sort: Sort
    ) -> LogicalPlanNode:
        """Return a new subtree with order metadata pushed into scans."""
        if isinstance(node, Scan):
            return self._push_to_scan(sort, node)

        if isinstance(node, Projection):
            new_input = self._attach_order_metadata(node.input, sort)
            return node.model_copy(update={"input": new_input})

        if isinstance(node, Filter):
            new_input = self._attach_order_metadata(node.input, sort)
            # Order metadata was pushed into this filter's child; rebuild the
            # filter over that new input while preserving its predicate.
            return Filter.create(input=new_input, predicate=node.predicate)

        if isinstance(node, Limit):
            new_input = self._attach_order_metadata(node.input, sort)
            return node.model_copy(update={"input": new_input})

        if isinstance(node, Aggregate):
            new_input = self._attach_order_metadata(node.input, sort)
            return node.model_copy(update={"input": new_input})

        if isinstance(node, Sort):
            new_input = self._attach_order_metadata(node.input, sort)
            return node.model_copy(update={"input": new_input})

        return node

    def _push_to_scan(self, sort: Sort, scan: Scan) -> Scan:
        """Push ORDER BY into scan node."""
        return scan.model_copy(
            update={
                "order_by_keys": sort.sort_keys,
                "order_by_ascending": sort.ascending,
                "order_by_nulls": sort.nulls_order,
            }
        )

    def _push_through_projection(
        self, sort: Sort, projection: Projection
    ) -> LogicalPlanNode:
        """Push ORDER BY through projection if all sort columns available."""
        rewritten_keys = self._rewrite_sort_keys_for_projection(
            sort.sort_keys,
            projection,
        )
        if rewritten_keys is None:
            return sort

        rewritten_sort = sort.model_copy(
            update={"input": projection.input, "sort_keys": rewritten_keys}
        )
        pushed_child = self._push_order_by(rewritten_sort)

        if isinstance(pushed_child, Sort):
            rebuilt_projection = projection.model_copy(
                update={"input": pushed_child.input}
            )
            return sort.model_copy(update={"input": rebuilt_projection})

        return projection.model_copy(update={"input": pushed_child})

    def _push_through_filter(self, sort: Sort, filter_node: Filter) -> LogicalPlanNode:
        """Push ORDER BY through filter (always safe)."""
        pushed_child = self._push_sort_into_child(sort, filter_node.input)
        # Sort pushed into the filter's child; re-home the same filter over that
        # reordered input, keeping its predicate unchanged.
        return filter_node.model_copy(update={"input": pushed_child})

    def _push_through_join(self, sort: Sort, join: Join) -> LogicalPlanNode:
        """Push ORDER BY metadata into a single join side when safe."""
        left_scan = self._resolve_scan(join.left)
        right_scan = self._resolve_scan(join.right)
        if left_scan is not None and right_scan is not None:
            if left_scan.datasource != right_scan.datasource:
                return sort
        sort_cols = pushdown.column_key_set(sort.sort_keys)
        left_cols = pushdown.available_columns(join.left)
        right_cols = pushdown.available_columns(join.right)

        left_only = pushdown.columns_belong_to_side(sort_cols, left_cols, right_cols)
        right_only = pushdown.columns_belong_to_side(sort_cols, right_cols, left_cols)

        new_left = (
            self._push_sort_into_child(sort, join.left) if left_only else join.left
        )
        new_right = (
            self._push_sort_into_child(sort, join.right) if right_only else join.right
        )

        rebuilt = join.model_copy(update={"left": new_left, "right": new_right})
        return sort.model_copy(update={"input": rebuilt})

    def _push_sort_into_child(
        self,
        sort: Sort,
        child: LogicalPlanNode,
    ) -> LogicalPlanNode:
        """Push sort keys into a specific child and return transformed child."""
        injected = sort.model_copy(update={"input": child})
        pushed = self._push_order_by(injected)
        if isinstance(pushed, Sort):
            return pushed.input
        return pushed

    def _push_through_aggregate(
        self, sort: Sort, aggregate: Aggregate
    ) -> LogicalPlanNode:
        """Push ORDER BY through aggregate when keys align with aggregate output."""
        sort_cols = pushdown.column_key_set(sort.sort_keys)
        output_cols = set(aggregate.output_names)
        if not sort_cols.issubset(output_cols):
            return sort

        group_cols = pushdown.column_key_set(aggregate.group_by)
        if not sort_cols.issubset(group_cols):
            return sort

        pushed_child = self._push_sort_into_child(sort, aggregate.input)
        new_agg = aggregate.model_copy(update={"input": pushed_child})
        return sort.model_copy(update={"input": new_agg})

    def _push_through_union(self, sort: Sort, union: Union) -> LogicalPlanNode:
        """Propagate sort metadata into union inputs while keeping top sort."""
        new_inputs = []
        for child in union.inputs:
            injected = sort.model_copy(update={"input": child})
            pushed_child = self._push_order_by(injected)
            if isinstance(pushed_child, Sort):
                new_inputs.append(pushed_child.input)
            else:
                new_inputs.append(pushed_child)

        new_union = union.model_copy(update={"inputs": new_inputs})
        return sort.model_copy(update={"input": new_union})

    def _rewrite_sort_keys_for_projection(
        self,
        sort_keys: List[Expression],
        projection: Projection,
    ) -> Optional[List[Expression]]:
        """Rewrite projection sort keys to input expressions when aliases are used."""
        alias_map = self._build_projection_alias_map(projection)
        available = pushdown.available_columns(projection.input)
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
        """Recurse into node children.

        Filter has a special arm (a HAVING predicate folds onto an aggregate
        scan); every other handled node just recurses and rebuilds if a child
        changed (transform_children). A SetOperation rewrites its two branches.
        """
        if isinstance(plan, Filter):
            return self._recurse_filter(plan)
        if isinstance(plan, SetOperation):
            return _rewrite_set_operation_branches(plan, self._push_order_by)
        if isinstance(plan, (Projection, Limit, Sort, Join, Aggregate)):
            return transform_children(plan, self._push_order_by)
        return plan

    def _recurse_filter(self, plan: Filter) -> LogicalPlanNode:
        """Recurse into a Filter, folding HAVING onto an aggregate scan.

        When order-by pushdown turns the input into an aggregate-bearing Scan,
        the Filter is a HAVING clause and merges onto the scan's filters;
        otherwise the Filter is rebuilt over its new input.
        """
        new_input = self._push_order_by(plan.input)
        if new_input == plan.input:
            return plan
        if isinstance(new_input, Scan) and new_input.aggregates:
            merged_filters = self._merge_filters(new_input.filters, plan.predicate)
            return new_input.model_copy(update={"filters": merged_filters})
        return plan.model_copy(update={"input": new_input})

    def _resolve_scan(self, node: LogicalPlanNode) -> Optional[Scan]:
        """Return the first Scan found under wrappers (Projection/Filter/Limit/Sort/Aggregate)."""
        current = node
        while True:
            if isinstance(current, Scan):
                return current
            if isinstance(current, Projection):
                current = current.input
                continue
            if isinstance(current, Filter):
                current = current.input
                continue
            if isinstance(current, Limit):
                current = current.input
                continue
            if isinstance(current, Sort):
                current = current.input
                continue
            if isinstance(current, Aggregate):
                current = current.input
                continue
            return None

    def _merge_filters(
        self, left: Optional[Expression], right: Optional[Expression]
    ) -> Optional[Expression]:
        """None-safe AND of two predicate expressions (HAVING folded onto an
        aggregate-bearing scan after order-by pushdown)."""
        return and_expressions(left, right)

    def name(self) -> str:
        """Return this rule's identifier (used in logging and EXPLAIN)."""
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
        self, agg: Aggregate, scan: Scan, filter_expr: Optional[Expression]
    ) -> Scan:
        """Push aggregate into scan node."""
        merged_filters = self._merge_filters(scan.filters, filter_expr)

        return scan.model_copy(
            update={
                "filters": merged_filters,
                "group_by": agg.group_by,
                "grouping_sets": agg.grouping_sets,
                "aggregates": agg.aggregates,
                "output_names": agg.output_names,
            }
        )

    def _merge_filters(
        self, scan_filter: Optional[Expression], filter_filter: Optional[Expression]
    ) -> Optional[Expression]:
        """None-safe AND of a scan filter and a filter-node predicate."""
        return and_expressions(scan_filter, filter_filter)

    def _recurse_node(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Recurse into node children, rebuilding if a child changed.

        Every handled node just recurses (transform_children); a SetOperation
        rewrites its two branches. An unhandled node bottoms out unchanged.
        """
        if isinstance(plan, SetOperation):
            return _rewrite_set_operation_branches(plan, self._push_aggregate)
        if isinstance(plan, (Projection, Filter, Limit, Sort, Join)):
            return transform_children(plan, self._push_aggregate)
        return plan

    def name(self) -> str:
        """Return this rule's identifier (used in logging and EXPLAIN)."""
        return "AggregatePushdown"


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
            # Re-wrap the optimized child in an Explain, preserving the requested
            # output format so EXPLAIN reports the plan of the optimized query.
            return Explain.create(input=optimized_child, format=plan.format)

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
                    validate_scope(current_plan, f"after {type(rule).__name__}")

            # If no rules made changes, we've reached fixed point
            if not changed:
                break

        return current_plan

    def __repr__(self) -> str:
        return f"RuleBasedOptimizer(rules={len(self.rules)})"
