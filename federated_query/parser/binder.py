"""Binder resolves references and validates types."""

from dataclasses import replace
from typing import Callable, Dict, List, Optional, TYPE_CHECKING
from ..catalog.catalog import Catalog
from ..catalog.schema import Table, Column
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Projection,
    Filter,
    Limit,
    Sort,
    Join,
    Aggregate,
    Explain,
    CTE,
    CTERef,
    Values,
    SubqueryScan,
    SetOperation,
    LateralJoin,
)
from ..plan.expressions import (
    Expression,
    ColumnRef,
    Literal,
    BinaryOp,
    UnaryOp,
    DataType,
    FunctionCall,
    InList,
    BetweenExpression,
    Cast,
    Extract,
    Interval,
    CaseExpr,
    WindowExpr,
    SubqueryExpression,
    ExistsExpression,
    InSubquery,
    QuantifiedComparison,
    TupleExpression,
)

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


class BindingError(Exception):
    """Exception raised during binding."""

    pass


def _rebuild_function_call(expr, bound_args, bind_one) -> FunctionCall:
    """Rebuild a bound FunctionCall, preserving every field except the args.

    ``replace`` copies all other fields (is_aggregate, distinct, WITHIN GROUP
    info, and any field added later), so binding can never silently drop one.
    ``bind_one`` binds the WITHIN GROUP sort key of an ordered-set aggregate.
    """
    bound_key = None
    if expr.within_group_key is not None:
        bound_key = bind_one(expr.within_group_key)
    return replace(expr, args=bound_args, within_group_key=bound_key)


# Maps the leading keyword of a SQL CAST target type to the engine DataType.
_CAST_TYPE_KEYWORDS = {
    "INT": DataType.INTEGER,
    "INTEGER": DataType.INTEGER,
    "INT4": DataType.INTEGER,
    "SMALLINT": DataType.INTEGER,
    "INT2": DataType.INTEGER,
    "BIGINT": DataType.BIGINT,
    "INT8": DataType.BIGINT,
    "FLOAT": DataType.FLOAT,
    "FLOAT4": DataType.FLOAT,
    "REAL": DataType.FLOAT,
    "DOUBLE": DataType.DOUBLE,
    "FLOAT8": DataType.DOUBLE,
    "DECIMAL": DataType.DECIMAL,
    "NUMERIC": DataType.DECIMAL,
    "VARCHAR": DataType.VARCHAR,
    "CHAR": DataType.VARCHAR,
    "CHARACTER": DataType.VARCHAR,
    "TEXT": DataType.TEXT,
    "BOOLEAN": DataType.BOOLEAN,
    "BOOL": DataType.BOOLEAN,
    "DATE": DataType.DATE,
    "TIMESTAMP": DataType.TIMESTAMP,
    "DATETIME": DataType.TIMESTAMP,
}


class Binder:
    """Binder resolves table and column references."""

    def __init__(self, catalog: Catalog):
        """Initialize binder.

        Args:
            catalog: Catalog with metadata
        """
        self.catalog = catalog
        # Stack of visible relation scopes (alias -> Table), one entry per
        # enclosing query block. Subquery plans are bound with this stack
        # so correlated references resolve against outer relations.
        self._scope_stack: List[Dict[str, Table]] = []
        # CTE name -> synthetic Table describing its output columns, registered
        # while a WITH body and its children bind so a CTERef resolves like a
        # relation without a catalog lookup.
        self._cte_tables: Dict[str, Table] = {}

    def bind(
        self,
        plan: LogicalPlanNode,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> LogicalPlanNode:
        """Bind a logical plan.

        This resolves all table and column references, validates types,
        and adds metadata to the plan.

        Args:
            plan: Unbound logical plan

        Returns:
            Bound logical plan with resolved references

        Raises:
            BindingError: If binding fails
        """
        if isinstance(plan, Explain):
            return self._bind_explain(plan)
        if isinstance(plan, Scan):
            return self._bind_scan(plan)
        if isinstance(plan, Filter):
            return self._bind_filter(plan)
        if isinstance(plan, Projection):
            return self._bind_projection(plan)
        if isinstance(plan, Sort):
            return self._bind_sort(plan)
        if isinstance(plan, Limit):
            return self._bind_limit(plan)
        if isinstance(plan, Join):
            return self._bind_join(plan)
        if isinstance(plan, LateralJoin):
            return self._bind_lateral_join(plan)
        if isinstance(plan, Aggregate):
            return self._bind_aggregate(plan)
        if isinstance(plan, CTE):
            return self._bind_cte(plan)
        if isinstance(plan, CTERef):
            return self._bind_cte_ref(plan)
        if isinstance(plan, Values):
            return self._bind_values(plan)
        if isinstance(plan, SubqueryScan):
            return self._bind_subquery_scan(plan)
        if isinstance(plan, SetOperation):
            return self._bind_set_operation(plan)
        raise BindingError(f"Unsupported plan node type: {type(plan)}")

    def _bind_set_operation(self, set_op: SetOperation) -> SetOperation:
        """Bind both branches of a set operation and check their arity.

        SQL requires the branches of a UNION/INTERSECT/EXCEPT to have the same
        number of output columns; a mismatch is a binding error, not a runtime
        surprise.
        """
        bound_left = self.bind(set_op.left)
        bound_right = self.bind(set_op.right)
        self._check_set_branch_arity(bound_left, bound_right)
        return SetOperation(
            left=bound_left,
            right=bound_right,
            kind=set_op.kind,
            distinct=set_op.distinct,
        )

    def _check_set_branch_arity(
        self, left: LogicalPlanNode, right: LogicalPlanNode
    ) -> None:
        """Raise when set-operation branches expose differing column counts."""
        left_width = len(left.schema())
        right_width = len(right.schema())
        if left_width != right_width:
            raise BindingError(
                "Set-operation branches have different column counts: "
                f"{left_width} vs {right_width}"
            )

    def _bind_values(self, values: Values) -> Values:
        """Bind a constant Values node (no input columns to resolve)."""
        bound_rows = []
        for row in values.rows:
            bound_row = []
            for expr in row:
                bound_row.append(self._bind_expression(expr, None))
            bound_rows.append(bound_row)
        return Values(rows=bound_rows, output_names=values.output_names)

    def _bind_subquery_scan(self, node: SubqueryScan) -> SubqueryScan:
        """Bind a derived table by binding its inner plan."""
        bound_input = self.bind(node.input)
        return SubqueryScan(input=bound_input, alias=node.alias)

    def _bind_scan(self, scan: Scan) -> Scan:
        """Bind a Scan node, keeping only columns of the scanned table.

        The parser over-collects referenced names: a scan's column list may
        include names that belong to other relations, to enclosing queries
        (correlated references), or to nested subqueries. Names not present
        in this table are dropped here; references that resolve nowhere
        still fail loudly during expression binding.
        """
        from dataclasses import replace

        table = self._resolve_table(scan)
        kept = []
        for name in scan.columns:
            if name == "*" or table.get_column(name) is not None:
                kept.append(name)
        if len(kept) == 0:
            kept = ["*"]
        if kept == scan.columns:
            return scan
        return replace(scan, columns=kept)

    def _resolve_table(self, scan: Scan) -> Table:
        """Resolve table reference."""
        table = self.catalog.get_table(
            scan.datasource, scan.schema_name, scan.table_name
        )
        if table is None:
            raise BindingError(
                f"Table not found: {scan.datasource}.{scan.schema_name}.{scan.table_name}"
            )
        return table

    def _bind_filter(self, filter_node: Filter) -> Filter:
        """Bind a Filter node."""
        bound_input = self.bind(filter_node.input)

        self._push_scope_for(bound_input)
        try:
            bound_predicate = self._bind_filter_predicate(
                filter_node.predicate, bound_input
            )
        finally:
            self._pop_scope()

        return Filter(input=bound_input, predicate=bound_predicate)

    def _bind_filter_predicate(
        self, predicate: Expression, bound_input: LogicalPlanNode
    ) -> Expression:
        """Bind a filter predicate against its bound input plan."""
        if isinstance(bound_input, Aggregate):
            return self._bind_having_predicate(predicate, bound_input)
        if isinstance(bound_input, Join):
            tables = self._get_tables_from_join(bound_input.left, bound_input.right)
            return self._bind_join_condition(predicate, tables)
        table = self._get_table_from_plan(bound_input)
        return self._bind_expression(predicate, table)

    def _bind_projection(self, projection: Projection) -> Projection:
        """Bind a Projection node."""
        bound_input = self.bind(projection.input)

        self._push_scope_for(bound_input)
        try:
            bound_expressions = self._bind_projection_expressions(
                projection.expressions, bound_input
            )
            bound_distinct_on = self._bind_distinct_on(
                projection.distinct_on, bound_input
            )
        finally:
            self._pop_scope()

        return Projection(
            input=bound_input,
            expressions=bound_expressions,
            aliases=projection.aliases,
            distinct=projection.distinct,
            distinct_on=bound_distinct_on,
        )

    def _bind_distinct_on(self, keys, bound_input) -> Optional[List[Expression]]:
        """Bind the DISTINCT ON key expressions, or None for a plain projection."""
        if keys is None:
            return None
        return self._bind_projection_expressions(keys, bound_input)

    def _bind_projection_expressions(
        self, expressions: List[Expression], bound_input: LogicalPlanNode
    ) -> List[Expression]:
        """Bind projection expressions against the bound input plan."""
        if self._contains_join(bound_input):
            tables = self._extract_tables_from_tree(bound_input)
            bound_expressions = []
            for expr in expressions:
                bound_expressions.append(
                    self._bind_expression_multi_table(expr, tables)
                )
            return bound_expressions
        table = self._get_table_from_plan(bound_input)
        return self._bind_expressions(expressions, table)

    def _bind_sort(self, sort: Sort) -> Sort:
        """Bind a Sort node."""
        bound_input = self.bind(sort.input)
        self._push_scope_for(bound_input)
        try:
            return self._bind_sort_with_input(sort, bound_input)
        finally:
            self._pop_scope()

    def _bind_sort_with_input(self, sort: Sort, bound_input: LogicalPlanNode) -> Sort:
        """Bind sort keys against the already-bound input plan."""
        if isinstance(bound_input, Projection):
            bound_keys = self._bind_sort_keys_for_projection(
                sort.sort_keys, bound_input
            )
            return Sort(
                input=bound_input,
                sort_keys=bound_keys,
                ascending=sort.ascending,
                nulls_order=sort.nulls_order,
            )

        if isinstance(bound_input, Aggregate):
            bound_keys = self._bind_sort_keys_for_aggregate(sort.sort_keys, bound_input)
            return Sort(
                input=bound_input,
                sort_keys=bound_keys,
                ascending=sort.ascending,
                nulls_order=sort.nulls_order,
            )

        if isinstance(bound_input, Filter) and isinstance(bound_input.input, Aggregate):
            bound_keys = self._bind_sort_keys_for_aggregate(
                sort.sort_keys,
                bound_input.input,
            )
            return Sort(
                input=bound_input,
                sort_keys=bound_keys,
                ascending=sort.ascending,
                nulls_order=sort.nulls_order,
            )

        if self._contains_join(bound_input):
            tables = self._extract_tables_from_tree(bound_input)
            bound_keys = self._bind_sort_keys_multi(sort.sort_keys, tables)
            return Sort(
                input=bound_input,
                sort_keys=bound_keys,
                ascending=sort.ascending,
                nulls_order=sort.nulls_order,
            )

        table = self._get_table_from_plan(bound_input)
        bound_keys = self._bind_sort_keys(sort.sort_keys, table)
        return Sort(
            input=bound_input,
            sort_keys=bound_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )

    def _bind_sort_keys(
        self, sort_keys: List[Expression], table: Optional[Table]
    ) -> List[Expression]:
        """Bind ORDER BY expressions for a single table."""
        bound_keys: List[Expression] = []
        for key in sort_keys:
            bound_key = self._bind_expression(key, table)
            bound_keys.append(bound_key)
        return bound_keys

    def _bind_sort_keys_for_aggregate(
        self,
        sort_keys: List[Expression],
        aggregate: Aggregate,
    ) -> List[Expression]:
        """Bind ORDER BY keys when input is an Aggregate (supports output aliases)."""
        alias_map: Dict[str, Expression] = {}
        for index in range(len(aggregate.output_names)):
            alias = aggregate.output_names[index]
            expr = aggregate.aggregates[index]
            alias_map[alias] = expr

        bound_keys: List[Expression] = []
        table = self._get_table_from_plan(aggregate.input)
        for key in sort_keys:
            if isinstance(key, ColumnRef) and key.table is None:
                if key.column in alias_map:
                    expr = alias_map[key.column]
                    bound_keys.append(
                        ColumnRef(
                            table=None,
                            column=key.column,
                            data_type=expr.get_type(),
                        )
                    )
                    continue
            bound_keys.append(self._bind_expression(key, table))
        return bound_keys

    def _bind_sort_keys_multi(
        self,
        sort_keys: List[Expression],
        tables: Dict[Optional[str], Table],
    ) -> List[Expression]:
        """Bind ORDER BY expressions referencing multiple tables."""
        bound_keys: List[Expression] = []
        for key in sort_keys:
            bound_key = self._bind_expression_multi_table(key, tables)
            bound_keys.append(bound_key)
        return bound_keys

    def _bind_sort_keys_for_projection(
        self,
        sort_keys: List[Expression],
        projection: Projection,
    ) -> List[Expression]:
        """Bind ORDER BY keys when input is a Projection (supports aliases)."""
        alias_map = self._build_alias_expression_map(projection)
        tables = None
        if self._contains_join(projection.input):
            tables = self._extract_tables_from_tree(projection.input)
        table = self._get_table_from_plan(projection.input)

        bound_keys: List[Expression] = []
        for key in sort_keys:
            bound_key = self._bind_projection_sort_key(
                key=key,
                alias_map=alias_map,
                table=table,
                tables=tables,
            )
            bound_keys.append(bound_key)
        return bound_keys

    def _bind_projection_sort_key(
        self,
        key: Expression,
        alias_map: Dict[str, Expression],
        table: Optional[Table],
        tables: Optional[Dict[Optional[str], Table]],
    ) -> Expression:
        """Bind a single ORDER BY expression that may reference a select alias."""
        from ..plan.expressions import ColumnRef

        if isinstance(key, ColumnRef) and key.table is None:
            if key.column in alias_map:
                source_expr = alias_map[key.column]
                from ..plan.expressions import ColumnRef as BoundColumnRef

                if isinstance(source_expr, BoundColumnRef):
                    return ColumnRef(
                        table=source_expr.table,
                        column=source_expr.column,
                        data_type=source_expr.data_type,
                    )
                return ColumnRef(
                    table=None,
                    column=key.column,
                    data_type=source_expr.get_type(),
                )

        if tables is not None:
            return self._bind_expression_multi_table(key, tables)

        return self._bind_expression(key, table)

    def _build_alias_expression_map(
        self, projection: Projection
    ) -> Dict[str, Expression]:
        """Map output aliases to their bound expressions."""
        alias_map: Dict[str, Expression] = {}
        for index in range(len(projection.aliases)):
            alias = projection.aliases[index]
            expression = projection.expressions[index]
            alias_map[alias] = expression
        return alias_map

    def _contains_join(self, plan: LogicalPlanNode) -> bool:
        """Check if plan tree contains a Join node."""
        if isinstance(plan, Join):
            return True
        if hasattr(plan, "input"):
            return self._contains_join(plan.input)
        return False

    def _extract_tables_from_tree(
        self, plan: LogicalPlanNode
    ) -> Dict[Optional[str], Table]:
        """Extract all tables from plan tree."""
        if isinstance(plan, Join):
            return self._get_tables_from_join(plan.left, plan.right)
        if hasattr(plan, "input"):
            return self._extract_tables_from_tree(plan.input)
        return {}

    def _bind_expression_multi_table(
        self, expr: Expression, tables: Dict[Optional[str], Table]
    ) -> Expression:
        """Bind expression with multiple tables."""
        if isinstance(expr, ColumnRef):
            return self._bind_column_ref_multi_table(expr, tables)
        if isinstance(expr, Literal):
            return expr
        if isinstance(expr, BinaryOp):
            left = self._bind_expression_multi_table(expr.left, tables)
            right = self._bind_expression_multi_table(expr.right, tables)
            return BinaryOp(op=expr.op, left=left, right=right)
        if isinstance(expr, UnaryOp):
            operand = self._bind_expression_multi_table(expr.operand, tables)
            return UnaryOp(op=expr.op, operand=operand)
        if isinstance(expr, InList):
            return self._bind_in_list_multi(expr, tables)
        if isinstance(expr, BetweenExpression):
            return self._bind_between_multi(expr, tables)
        if isinstance(expr, CaseExpr):
            return self._bind_case_expr_multi(expr, tables)
        if isinstance(expr, Cast):
            return self._bind_cast(
                expr, lambda value: self._bind_expression_multi_table(value, tables)
            )
        if isinstance(expr, Extract):
            return self._bind_extract(
                expr, lambda value: self._bind_expression_multi_table(value, tables)
            )
        if isinstance(expr, Interval):
            return expr
        if isinstance(expr, FunctionCall):
            return self._bind_function_args(
                expr, lambda value: self._bind_expression_multi_table(value, tables)
            )
        if isinstance(expr, WindowExpr):
            return self._bind_window_expr(
                expr, lambda value: self._bind_expression_multi_table(value, tables)
            )
        if self._is_subquery_expression(expr):
            return self._bind_subquery_expr(
                expr, lambda value: self._bind_expression_multi_table(value, tables)
            )
        if isinstance(expr, TupleExpression):
            return self._bind_tuple(
                expr, lambda value: self._bind_expression_multi_table(value, tables)
            )

        return expr

    def _bind_limit(self, limit: Limit) -> Limit:
        """Bind a Limit node."""
        bound_input = self.bind(limit.input)
        return Limit(input=bound_input, limit=limit.limit, offset=limit.offset)

    def _bind_join(self, join: Join) -> Join:
        """Bind a Join node."""
        bound_left = self.bind(join.left)
        bound_right = self.bind(join.right)

        bound_condition = None
        if join.condition:
            self._push_scope_for(bound_left, bound_right)
            try:
                tables = self._get_tables_from_join(bound_left, bound_right)
                bound_condition = self._bind_join_condition(join.condition, tables)
            finally:
                self._pop_scope()

        return Join(
            left=bound_left,
            right=bound_right,
            join_type=join.join_type,
            condition=bound_condition,
            natural=join.natural,
            using=join.using,
        )

    def _bind_lateral_join(self, join: LateralJoin) -> LateralJoin:
        """Bind a LATERAL join, binding the right with the left in scope.

        Unlike a plain join, the right side may reference the left's columns
        (the dependent correlation), so the left's relation scope is pushed
        while the right is bound.
        """
        bound_left = self.bind(join.left)
        self._push_scope_for(bound_left)
        try:
            plan_binder = SubqueryPlanBinder(self, list(self._scope_stack))
            bound_right = plan_binder.bind(join.right)
        finally:
            self._pop_scope()
        return LateralJoin(left=bound_left, right=bound_right, join_type=join.join_type)

    def _bind_explain(self, explain: Explain) -> Explain:
        """Bind an Explain node."""
        bound_child = self.bind(explain.input)
        return Explain(input=bound_child, format=explain.format)

    def _bind_aggregate(self, aggregate: Aggregate) -> Aggregate:
        """Bind an Aggregate node."""
        bound_input = self.bind(aggregate.input)
        self._push_scope_for(bound_input)
        try:
            return self._bind_aggregate_with_input(aggregate, bound_input)
        finally:
            self._pop_scope()

    def _bind_aggregate_with_input(
        self, aggregate: Aggregate, bound_input: LogicalPlanNode
    ) -> Aggregate:
        """Bind aggregate expressions against the bound input plan."""
        if self._contains_join(bound_input):
            tables = self._extract_tables_from_tree(bound_input)
            bound_group_by = self._bind_group_by_multi_table(aggregate.group_by, tables)
            bound_aggregates = self._bind_aggregate_expressions_multi_table(
                aggregate.aggregates, tables
            )
            bind_set = lambda s: self._bind_group_by_multi_table(s, tables)
        else:
            table = self._get_table_from_plan(bound_input)
            bound_group_by = self._bind_group_by_expressions(aggregate.group_by, table)
            bound_aggregates = self._bind_aggregate_expressions(
                aggregate.aggregates, table
            )
            bind_set = lambda s: self._bind_group_by_expressions(s, table)

        return replace(
            aggregate,
            input=bound_input,
            group_by=bound_group_by,
            aggregates=bound_aggregates,
            grouping_sets=self._bind_grouping_sets(aggregate.grouping_sets, bind_set),
        )

    def _bind_grouping_sets(self, grouping_sets, bind_set):
        """Bind each grouping set's key expressions, or None for a flat GROUP BY."""
        if grouping_sets is None:
            return None
        bound = []
        for grouping_set in grouping_sets:
            bound.append(bind_set(grouping_set))
        return bound

    def _bind_cte(self, cte: CTE) -> CTE:
        """Bind a CTE: register its name as a relation, then bind body and child.

        The name is registered for the child (and any later CTE) to resolve a
        ``CTERef`` against the body's output columns; for ``WITH RECURSIVE`` the
        name is registered before the body binds so it can reference itself.
        """
        if cte.recursive:
            return self._bind_recursive_cte(cte)
        bound_plan = self.bind(cte.cte_plan)
        table = self._cte_table(cte, bound_plan)
        saved = self._cte_tables.get(cte.name)
        self._cte_tables[cte.name] = table
        bound_child = self.bind(cte.child)
        self._restore_cte(cte.name, saved)
        return CTE(
            name=cte.name,
            cte_plan=bound_plan,
            child=bound_child,
            column_names=cte.column_names,
        )

    def _bind_recursive_cte(self, cte: CTE) -> CTE:
        """Bind a recursive CTE; its name is in scope while its own body binds."""
        if not cte.column_names:
            raise BindingError(
                f"Recursive CTE '{cte.name}' requires an explicit column list"
            )
        table = Table(name=cte.name, columns=self._named_columns(cte.column_names))
        saved = self._cte_tables.get(cte.name)
        self._cte_tables[cte.name] = table
        bound_plan = self.bind(cte.cte_plan)
        bound_child = self.bind(cte.child)
        self._restore_cte(cte.name, saved)
        return CTE(
            name=cte.name,
            cte_plan=bound_plan,
            child=bound_child,
            recursive=True,
            column_names=cte.column_names,
        )

    def _bind_cte_ref(self, node: CTERef) -> CTERef:
        """Resolve a CTE reference to the registered CTE's output column names."""
        table = self._cte_tables.get(node.name)
        if table is None:
            raise BindingError(f"CTE not found: {node.name}")
        names = []
        for column in table.columns:
            names.append(column.name)
        return CTERef(
            name=node.name,
            alias=node.alias,
            columns=node.columns,
            output_names=names,
        )

    def _cte_table(self, cte: CTE, bound_plan: LogicalPlanNode) -> Table:
        """Build a Table describing a CTE's output columns."""
        columns = self._plan_output_columns(bound_plan)
        if cte.column_names:
            columns = self._rename_columns(cte.column_names, columns)
        return Table(name=cte.name, columns=columns)

    def _rename_columns(self, names: List[str], columns: List[Column]) -> List[Column]:
        """Re-label output columns with an explicit CTE column list."""
        renamed = []
        for index in range(len(names)):
            data_type = columns[index].data_type
            renamed.append(
                Column(name=names[index], data_type=data_type, nullable=True)
            )
        return renamed

    def _named_columns(self, names: List[str]) -> List[Column]:
        """Build placeholder columns for a name-only schema (recursive CTE)."""
        columns = []
        for name in names:
            columns.append(Column(name=name, data_type=DataType.NULL, nullable=True))
        return columns

    def _restore_cte(self, name: str, saved: Optional[Table]) -> None:
        """Restore the CTE registry entry after binding a WITH block."""
        if saved is None:
            del self._cte_tables[name]
        else:
            self._cte_tables[name] = saved

    def _bind_group_by_expressions(
        self, expressions: List[Expression], table: Optional[Table]
    ) -> List[Expression]:
        """Bind GROUP BY expressions."""
        bound = []
        for expr in expressions:
            bound_expr = self._bind_expression(expr, table)
            bound.append(bound_expr)
        return bound

    def _bind_aggregate_expressions(
        self, expressions: List[Expression], table: Optional[Table]
    ) -> List[Expression]:
        """Bind aggregate expressions."""
        bound = []
        for expr in expressions:
            bound_expr = self._bind_aggregate_expression(expr, table)
            bound.append(bound_expr)
        return bound

    def _bind_group_by_multi_table(
        self, expressions: List[Expression], tables: Dict[Optional[str], Table]
    ) -> List[Expression]:
        """Bind GROUP BY expressions referencing multiple tables."""
        bound: List[Expression] = []
        for expr in expressions:
            bound_expr = self._bind_expression_multi_table(expr, tables)
            bound.append(bound_expr)
        return bound

    def _bind_aggregate_expressions_multi_table(
        self, expressions: List[Expression], tables: Dict[Optional[str], Table]
    ) -> List[Expression]:
        """Bind aggregate expressions when multiple tables are available."""
        bound = []
        for expr in expressions:
            bound.append(self._bind_aggregate_expression_multi_table(expr, tables))
        return bound

    def _bind_aggregate_expression(
        self, expr: Expression, table: Optional[Table]
    ) -> Expression:
        """Bind an aggregate expression."""
        if isinstance(expr, FunctionCall):
            bound_args = self._bind_expressions(expr.args, table)
            return _rebuild_function_call(
                expr, bound_args, lambda e: self._bind_expression(e, table)
            )

        return self._bind_expression(expr, table)

    def _bind_aggregate_expression_multi_table(
        self, expr: Expression, tables: Dict[Optional[str], Table]
    ) -> Expression:
        """Bind aggregate expression referencing multiple tables."""
        if isinstance(expr, FunctionCall):
            bound_args = []
            for arg in expr.args:
                bound_arg = self._bind_expression_multi_table(arg, tables)
                bound_args.append(bound_arg)
            return _rebuild_function_call(
                expr, bound_args, lambda e: self._bind_expression_multi_table(e, tables)
            )

        return self._bind_expression_multi_table(expr, tables)

    def _bind_having_predicate(
        self, predicate: Expression, aggregate: Aggregate
    ) -> Expression:
        """Bind HAVING predicate against aggregate output schema."""
        alias_map: Dict[str, Expression] = {}
        for i in range(len(aggregate.output_names)):
            alias_map[aggregate.output_names[i]] = aggregate.aggregates[i]
        return self._bind_having_expression(predicate, alias_map)

    def _bind_having_expression(
        self, expr: Expression, alias_map: Dict[str, Expression]
    ) -> Expression:
        """Bind expression against aggregate output columns."""
        if isinstance(expr, ColumnRef):
            if expr.column in alias_map:
                source = alias_map[expr.column]
                return ColumnRef(
                    table=None,
                    column=expr.column,
                    data_type=source.get_type(),
                )
            raise BindingError(f"Column '{expr.column}' not found in aggregate output")

        if isinstance(expr, BinaryOp):
            left = self._bind_having_expression(expr.left, alias_map)
            right = self._bind_having_expression(expr.right, alias_map)
            return BinaryOp(op=expr.op, left=left, right=right)

        if isinstance(expr, UnaryOp):
            operand = self._bind_having_expression(expr.operand, alias_map)
            return UnaryOp(op=expr.op, operand=operand)

        if self._is_subquery_expression(expr):
            return self._bind_subquery_expr(
                expr, lambda value: self._bind_having_expression(value, alias_map)
            )

        return expr

    def _push_scope_for(self, *plans: LogicalPlanNode) -> None:
        """Push the relation scope visible inside the given subtree(s)."""
        scope: Dict[str, Table] = {}
        for plan in plans:
            self._add_plan_to_scope(plan, scope)
        self._scope_stack.append(scope)

    def _pop_scope(self) -> None:
        """Pop the innermost relation scope."""
        self._scope_stack.pop()

    def _add_plan_to_scope(
        self, plan: LogicalPlanNode, scope: Dict[str, Table]
    ) -> None:
        """Collect alias -> Table entries from a bound plan subtree."""
        if isinstance(plan, Scan):
            self._add_scan_to_scope(plan, scope)
            return
        if isinstance(plan, SubqueryScan):
            scope[plan.alias] = self._synthetic_table(plan)
            return
        if isinstance(plan, CTERef):
            self._add_cte_ref_to_scope(plan, scope)
            return
        for child in plan.children():
            self._add_plan_to_scope(child, scope)

    def _add_cte_ref_to_scope(self, node: CTERef, scope: Dict[str, Table]) -> None:
        """Register a CTE reference under its alias (or CTE name)."""
        table = self._cte_tables.get(node.name)
        if table is None:
            raise BindingError(f"CTE not found: {node.name}")
        name = node.alias if node.alias else node.name
        scope[name] = table

    def _add_scan_to_scope(self, scan: Scan, scope: Dict[str, Table]) -> None:
        """Register a scan under its alias (or table name)."""
        table = self.catalog.get_table(
            scan.datasource, scan.schema_name, scan.table_name
        )
        if table is None:
            raise BindingError(
                f"Table not found: {scan.datasource}.{scan.schema_name}.{scan.table_name}"
            )
        name = scan.alias if scan.alias else scan.table_name
        scope[name] = table

    def _synthetic_table(self, node: SubqueryScan) -> Table:
        """Build a Table describing a derived table's output columns."""
        columns = self._plan_output_columns(node.input)
        return Table(name=node.alias, columns=columns)

    def _plan_output_columns(self, plan: LogicalPlanNode) -> List[Column]:
        """Derive output Column metadata from a bound plan."""
        if isinstance(plan, Projection):
            return self._expression_columns(plan.aliases, plan.expressions)
        if isinstance(plan, Aggregate):
            return self._expression_columns(plan.output_names, plan.aggregates)
        if isinstance(plan, Values):
            return self._expression_columns(plan.output_names, plan.rows[0])
        if isinstance(plan, SetOperation):
            return self._plan_output_columns(plan.left)
        return self._plan_output_columns_from_children(plan)

    def _plan_output_columns_from_children(self, plan: LogicalPlanNode) -> List[Column]:
        """Derive output columns for pass-through and scan nodes."""
        if isinstance(plan, Scan):
            return self._scan_output_columns(plan)
        if isinstance(plan, Join):
            left_columns = self._plan_output_columns(plan.left)
            return left_columns + self._plan_output_columns(plan.right)
        if isinstance(plan, CTERef):
            table = self._cte_tables.get(plan.name)
            if table is None:
                raise BindingError(f"CTE not found: {plan.name}")
            return list(table.columns)
        children = plan.children()
        if len(children) == 1:
            return self._plan_output_columns(children[0])
        raise BindingError(
            f"Cannot derive output columns for plan node {type(plan).__name__}"
        )

    def _scan_output_columns(self, scan: Scan) -> List[Column]:
        """Output columns of a scan, respecting its column projection."""
        table = self.catalog.get_table(
            scan.datasource, scan.schema_name, scan.table_name
        )
        if table is None:
            raise BindingError(
                f"Table not found: {scan.datasource}.{scan.schema_name}.{scan.table_name}"
            )
        if "*" in scan.columns:
            return list(table.columns)
        columns = []
        for name in scan.schema():
            column = table.get_column(name)
            if column is None:
                raise BindingError(f"Column '{name}' not found in table {table.name}")
            columns.append(column)
        return columns

    def _expression_columns(
        self, names: List[str], expressions: List[Expression]
    ) -> List[Column]:
        """Pair output names with expression types as Column metadata."""
        columns = []
        for index in range(len(names)):
            data_type = expressions[index].get_type()
            columns.append(
                Column(name=names[index], data_type=data_type, nullable=True)
            )
        return columns

    def _bind_subquery_expr(
        self,
        expr: Expression,
        bind_value: Callable[[Expression], Expression],
        scopes: Optional[List[Dict[str, Table]]] = None,
    ) -> Expression:
        """Bind a subquery expression node.

        The subquery's plan is bound with the given scopes (the current
        scope stack by default) visible so correlated references resolve;
        outer-context parts (the IN value or the quantified comparison's
        left side) are bound with bind_value.
        """
        if scopes is None:
            scopes = list(self._scope_stack)
        plan_binder = SubqueryPlanBinder(self, scopes)
        if isinstance(expr, SubqueryExpression):
            return SubqueryExpression(subquery=plan_binder.bind(expr.subquery))
        if isinstance(expr, ExistsExpression):
            return ExistsExpression(
                subquery=plan_binder.bind(expr.subquery), negated=expr.negated
            )
        if isinstance(expr, InSubquery):
            return InSubquery(
                value=bind_value(expr.value),
                subquery=plan_binder.bind(expr.subquery),
                negated=expr.negated,
            )
        return QuantifiedComparison(
            operator=expr.operator,
            quantifier=expr.quantifier,
            left=bind_value(expr.left),
            subquery=plan_binder.bind(expr.subquery),
        )

    def _get_tables_from_join(
        self, left: LogicalPlanNode, right: LogicalPlanNode
    ) -> Dict[Optional[str], Table]:
        """Get tables from both sides of join.

        Returns:
            Dictionary mapping table names/aliases to Table objects
        """
        tables = {}

        left_tables = self._extract_tables(left)
        right_tables = self._extract_tables(right)

        tables.update(left_tables)
        tables.update(right_tables)

        return tables

    def _extract_tables(self, plan: LogicalPlanNode) -> Dict[Optional[str], Table]:
        """Extract all tables from a plan node."""
        tables = {}

        if isinstance(plan, Scan):
            table = self.catalog.get_table(
                plan.datasource, plan.schema_name, plan.table_name
            )
            if table:
                tables[plan.table_name] = table

        if isinstance(plan, SubqueryScan):
            tables[plan.alias] = self._synthetic_table(plan)
            return tables

        if isinstance(plan, CTERef):
            table = self._cte_tables.get(plan.name)
            if table is not None:
                tables[plan.alias if plan.alias else plan.name] = table
            return tables

        if isinstance(plan, Join):
            left_tables = self._extract_tables(plan.left)
            right_tables = self._extract_tables(plan.right)
            tables.update(left_tables)
            tables.update(right_tables)

        if hasattr(plan, "input"):
            return self._extract_tables(plan.input)

        return tables

    def _bind_join_condition(
        self, condition: Expression, tables: Dict[Optional[str], Table]
    ) -> Expression:
        """Bind join condition with multiple tables."""
        if isinstance(condition, ColumnRef):
            return self._bind_column_ref_multi_table(condition, tables)
        if isinstance(condition, Literal):
            return condition
        if isinstance(condition, BinaryOp):
            left = self._bind_join_condition(condition.left, tables)
            right = self._bind_join_condition(condition.right, tables)
            return BinaryOp(op=condition.op, left=left, right=right)
        if isinstance(condition, UnaryOp):
            operand = self._bind_join_condition(condition.operand, tables)
            return UnaryOp(op=condition.op, operand=operand)
        if self._is_subquery_expression(condition):
            return self._bind_subquery_expr(
                condition,
                lambda value: self._bind_join_condition(value, tables),
            )

        return condition

    def _bind_column_ref_multi_table(
        self, col_ref: ColumnRef, tables: Dict[Optional[str], Table]
    ) -> ColumnRef:
        """Bind column reference with multiple tables."""
        if col_ref.column == "*":
            return col_ref

        if col_ref.table:
            table = tables.get(col_ref.table)
            if table is None:
                for table_name, tbl in tables.items():
                    column = tbl.get_column(col_ref.column)
                    if column:
                        return ColumnRef(
                            table=col_ref.table,
                            column=col_ref.column,
                            data_type=column.data_type,
                        )
                return self._unresolved_or_raise(col_ref)
            column = table.get_column(col_ref.column)
            if column is None:
                return self._unresolved_or_raise(col_ref)
            return ColumnRef(
                table=col_ref.table,
                column=col_ref.column,
                data_type=column.data_type,
            )

        found_table = None
        found_column = None
        for table_name, table in tables.items():
            column = table.get_column(col_ref.column)
            if column:
                if found_column:
                    raise BindingError(
                        f"Column '{col_ref.column}' is ambiguous (found in multiple tables)"
                    )
                found_table = table_name
                found_column = column

        if found_column is None:
            return self._unresolved_or_raise(col_ref)

        return ColumnRef(
            table=found_table,
            column=col_ref.column,
            data_type=found_column.data_type,
        )

    def _unresolved_or_raise(self, col_ref: ColumnRef) -> ColumnRef:
        """Keep a column unbound only if an enclosing scope defines it.

        A reference that resolves in no in-scope or enclosing relation is a
        typo and must raise at bind time, not slip through to a KeyError or a
        remote UndefinedColumn at execution.
        """
        if self._resolves_in_scope_stack(col_ref):
            return col_ref
        raise BindingError(
            f"Column '{col_ref.to_sql()}' not found in any table in scope"
        )

    def _resolves_in_scope_stack(self, col_ref: ColumnRef) -> bool:
        """Whether a reference resolves against any enclosing query scope."""
        for scope in self._scope_stack:
            if self._scope_defines(scope, col_ref):
                return True
        return False

    def _scope_defines(self, scope: Dict[str, Table], col_ref: ColumnRef) -> bool:
        """Whether a relation scope defines the referenced column."""
        if col_ref.table is not None:
            table = scope.get(col_ref.table)
            return table is not None and table.get_column(col_ref.column) is not None
        for table in scope.values():
            if table.get_column(col_ref.column) is not None:
                return True
        return False

    def _get_table_from_plan(self, plan: LogicalPlanNode) -> Optional[Table]:
        """Extract table metadata from a plan node."""
        if isinstance(plan, Scan):
            return self.catalog.get_table(
                plan.datasource, plan.schema_name, plan.table_name
            )
        if isinstance(plan, SubqueryScan):
            return self._synthetic_table(plan)
        if isinstance(plan, CTERef):
            return self._cte_tables.get(plan.name)
        if hasattr(plan, "input"):
            return self._get_table_from_plan(plan.input)
        return None

    def _bind_expressions(
        self, expressions: List[Expression], table: Optional[Table]
    ) -> List[Expression]:
        """Bind a list of expressions."""
        bound = []
        for expr in expressions:
            bound_expr = self._bind_expression(expr, table)
            bound.append(bound_expr)
        return bound

    def _bind_expression(self, expr: Expression, table: Optional[Table]) -> Expression:
        """Bind an expression."""
        if isinstance(expr, ColumnRef):
            return self._bind_column_ref(expr, table)
        if isinstance(expr, Literal):
            return expr
        if isinstance(expr, BinaryOp):
            return self._bind_binary_op(expr, table)
        if isinstance(expr, UnaryOp):
            return self._bind_unary_op(expr, table)
        if isinstance(expr, InList):
            return self._bind_in_list(expr, table)
        if isinstance(expr, BetweenExpression):
            return self._bind_between(expr, table)
        if isinstance(expr, CaseExpr):
            return self._bind_case_expr(expr, table)
        if isinstance(expr, Cast):
            return self._bind_cast(
                expr, lambda value: self._bind_expression(value, table)
            )
        if isinstance(expr, Extract):
            return self._bind_extract(
                expr, lambda value: self._bind_expression(value, table)
            )
        if isinstance(expr, Interval):
            return expr
        if isinstance(expr, FunctionCall):
            return self._bind_function_args(
                expr, lambda value: self._bind_expression(value, table)
            )
        if isinstance(expr, WindowExpr):
            return self._bind_window_expr(
                expr, lambda value: self._bind_expression(value, table)
            )
        if self._is_subquery_expression(expr):
            return self._bind_subquery_expr(
                expr, lambda value: self._bind_expression(value, table)
            )
        if isinstance(expr, TupleExpression):
            return self._bind_tuple(
                expr, lambda value: self._bind_expression(value, table)
            )

        return expr

    def _bind_cast(
        self,
        expr: Cast,
        bind_value: Callable[[Expression], Expression],
    ) -> Cast:
        """Bind a CAST's inner expression and resolve its engine type."""
        bound_inner = bind_value(expr.expr)
        data_type = self._resolve_cast_type(expr.target_type)
        return Cast(
            expr=bound_inner,
            target_type=expr.target_type,
            data_type=data_type,
        )

    def _bind_extract(
        self,
        expr: Extract,
        bind_value: Callable[[Expression], Expression],
    ) -> Extract:
        """Bind the source of an EXTRACT while preserving its field keyword."""
        bound_source = bind_value(expr.source)
        return Extract(field=expr.field, source=bound_source)

    def _resolve_cast_type(self, target_type: str) -> DataType:
        """Map a SQL type text such as ``DECIMAL(10, 2)`` to a DataType."""
        keyword = target_type.split("(")[0].strip().upper()
        leading = keyword.split()[0] if keyword else ""
        if leading not in _CAST_TYPE_KEYWORDS:
            raise BindingError(f"Unsupported CAST target type: {target_type}")
        return _CAST_TYPE_KEYWORDS[leading]

    def _bind_function_args(
        self,
        expr: FunctionCall,
        bind_value: Callable[[Expression], Expression],
    ) -> FunctionCall:
        """Bind the arguments of a function call."""
        bound_args = []
        for arg in expr.args:
            bound_args.append(bind_value(arg))
        return _rebuild_function_call(expr, bound_args, bind_value)

    def _is_subquery_expression(self, expr: Expression) -> bool:
        """Check whether an expression node carries a subquery plan."""
        subquery_types = (
            SubqueryExpression,
            ExistsExpression,
            InSubquery,
            QuantifiedComparison,
        )
        return isinstance(expr, subquery_types)

    def _bind_tuple(
        self,
        expr: TupleExpression,
        bind_value: Callable[[Expression], Expression],
    ) -> TupleExpression:
        """Bind each item of a row value constructor."""
        bound_items = []
        for item in expr.items:
            bound_items.append(bind_value(item))
        return TupleExpression(items=tuple(bound_items))

    def _bind_column_ref(self, col_ref: ColumnRef, table: Optional[Table]) -> ColumnRef:
        """Bind a column reference."""
        if col_ref.column == "*":
            return col_ref

        if table is None:
            return col_ref

        column = table.get_column(col_ref.column)
        if column is None:
            raise BindingError(
                f"Column '{col_ref.column}' not found in table '{table.name}'"
            )

        return ColumnRef(
            table=col_ref.table,
            column=col_ref.column,
            data_type=column.data_type,
        )

    def _bind_binary_op(self, binary_op: BinaryOp, table: Optional[Table]) -> BinaryOp:
        """Bind a binary operation."""
        left = self._bind_expression(binary_op.left, table)
        right = self._bind_expression(binary_op.right, table)
        return BinaryOp(op=binary_op.op, left=left, right=right)

    def _bind_unary_op(self, unary_op: UnaryOp, table: Optional[Table]) -> UnaryOp:
        """Bind a unary operation."""
        operand = self._bind_expression(unary_op.operand, table)
        return UnaryOp(op=unary_op.op, operand=operand)

    def _bind_in_list(self, expr: InList, table: Optional[Table]) -> InList:
        """Bind an IN-list's value and each option against a single table."""
        bound_value = self._bind_expression(expr.value, table)
        bound_options: List[Expression] = []
        for option in expr.options:
            bound_option = self._bind_expression(option, table)
            bound_options.append(bound_option)
        return InList(value=bound_value, options=bound_options)

    def _bind_in_list_multi(
        self, expr: InList, tables: Dict[Optional[str], Table]
    ) -> InList:
        """Bind an IN-list across multiple tables (multi-relation scope)."""
        bound_value = self._bind_expression_multi_table(expr.value, tables)
        bound_options: List[Expression] = []
        for option in expr.options:
            bound_option = self._bind_expression_multi_table(option, tables)
            bound_options.append(bound_option)
        return InList(value=bound_value, options=bound_options)

    def _bind_between(
        self, expr: BetweenExpression, table: Optional[Table]
    ) -> BetweenExpression:
        """Bind a BETWEEN's value/lower/upper operands against a single table."""
        bound_value = self._bind_expression(expr.value, table)
        bound_lower = self._bind_expression(expr.lower, table)
        bound_upper = self._bind_expression(expr.upper, table)
        return BetweenExpression(
            value=bound_value,
            lower=bound_lower,
            upper=bound_upper,
        )

    def _bind_between_multi(
        self, expr: BetweenExpression, tables: Dict[Optional[str], Table]
    ) -> BetweenExpression:
        """Bind a BETWEEN expression across multiple tables (multi-relation scope)."""
        bound_value = self._bind_expression_multi_table(expr.value, tables)
        bound_lower = self._bind_expression_multi_table(expr.lower, tables)
        bound_upper = self._bind_expression_multi_table(expr.upper, tables)
        return BetweenExpression(
            value=bound_value,
            lower=bound_lower,
            upper=bound_upper,
        )

    def _bind_case_expr(self, expr: CaseExpr, table: Optional[Table]) -> CaseExpr:
        """Bind a CASE expression's WHEN conditions/results and ELSE on one table."""
        bound_when = []
        for condition, result in expr.when_clauses:
            bound_condition = self._bind_expression(condition, table)
            bound_result = self._bind_expression(result, table)
            bound_when.append((bound_condition, bound_result))
        bound_else = None
        if expr.else_result is not None:
            bound_else = self._bind_expression(expr.else_result, table)
        return CaseExpr(when_clauses=bound_when, else_result=bound_else)

    def _bind_case_expr_multi(
        self, expr: CaseExpr, tables: Dict[Optional[str], Table]
    ) -> CaseExpr:
        """Bind a CASE expression across multiple tables (multi-relation scope)."""
        bound_when = []
        for condition, result in expr.when_clauses:
            bound_condition = self._bind_expression_multi_table(condition, tables)
            bound_result = self._bind_expression_multi_table(result, tables)
            bound_when.append((bound_condition, bound_result))
        bound_else = None
        if expr.else_result is not None:
            bound_else = self._bind_expression_multi_table(expr.else_result, tables)
        return CaseExpr(when_clauses=bound_when, else_result=bound_else)

    def _bind_window_expr(
        self, expr: WindowExpr, bind: Callable[[Expression], Expression]
    ) -> WindowExpr:
        """Bind a window's inner function, partition keys, and order keys."""
        bound_partition = []
        for part in expr.partition_by:
            bound_partition.append(bind(part))
        bound_order = []
        for key in expr.order_keys:
            bound_order.append(bind(key))
        return replace(
            expr,
            function=bind(expr.function),
            partition_by=bound_partition,
            order_keys=bound_order,
        )

    def __repr__(self) -> str:
        return "Binder()"


class SubqueryPlanBinder:
    """Binds a subquery's plan with enclosing query scopes visible.

    Column references resolve innermost-first: the subquery's own relations
    win over enclosing query blocks, matching SQL scoping. Every reference
    must resolve somewhere; unknown tables or columns raise BindingError so
    nothing passes through unbound.
    """

    def __init__(self, host: Binder, outer_scopes: List[Dict[str, Table]]):
        """Capture the host binder (catalog access) and enclosing scopes."""
        self.host = host
        self.outer_scopes = outer_scopes

    def bind(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Bind one subquery plan node, dispatching by type."""
        if isinstance(plan, Scan):
            return self._bind_scan(plan)
        if isinstance(plan, Values):
            return self._bind_values(plan)
        if isinstance(plan, SubqueryScan):
            inner = SubqueryPlanBinder(self.host, self.outer_scopes)
            return SubqueryScan(input=inner.bind(plan.input), alias=plan.alias)
        return self._bind_relational(plan)

    def _bind_relational(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Bind relational operators that carry expressions."""
        if isinstance(plan, Filter):
            return self._bind_filter(plan)
        if isinstance(plan, Projection):
            return self._bind_projection(plan)
        if isinstance(plan, Aggregate):
            return self._bind_aggregate(plan)
        return self._bind_other(plan)

    def _bind_other(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Bind sort/limit/join nodes; reject anything unknown."""
        if isinstance(plan, Sort):
            return self._bind_sort(plan)
        if isinstance(plan, Limit):
            bound_input = self.bind(plan.input)
            return Limit(input=bound_input, limit=plan.limit, offset=plan.offset)
        if isinstance(plan, Join):
            return self._bind_join(plan)
        if isinstance(plan, SetOperation):
            return self._bind_set_operation(plan)
        raise BindingError(f"Unsupported plan node in subquery: {type(plan).__name__}")

    def _bind_set_operation(self, set_op: SetOperation) -> SetOperation:
        """Bind both branches of a set-operation subquery body.

        Each branch is bound with the enclosing scopes still visible (a branch
        may correlate to an outer relation); their output arity must match.
        """
        bound_left = self.bind(set_op.left)
        bound_right = self.bind(set_op.right)
        self.host._check_set_branch_arity(bound_left, bound_right)
        return SetOperation(
            left=bound_left,
            right=bound_right,
            kind=set_op.kind,
            distinct=set_op.distinct,
        )

    def _bind_scan(self, scan: Scan) -> Scan:
        """Bind a subquery scan via the host's lenient column filtering."""
        return self.host._bind_scan(scan)

    def _bind_values(self, values: Values) -> Values:
        """Bind constant rows; outer references are permitted."""
        bound_rows = []
        for row in values.rows:
            bound_row = []
            for expr in row:
                bound_row.append(self._bind_expr(expr, self.outer_scopes))
            bound_rows.append(bound_row)
        return Values(rows=bound_rows, output_names=values.output_names)

    def _bind_filter(self, filter_node: Filter) -> Filter:
        """Bind a filter, treating Filter-over-Aggregate as HAVING."""
        bound_input = self.bind(filter_node.input)
        if isinstance(bound_input, Aggregate):
            predicate = self._bind_having(filter_node.predicate, bound_input)
        else:
            predicate = self._bind_expr_for(filter_node.predicate, bound_input)
        return Filter(input=bound_input, predicate=predicate)

    def _bind_projection(self, projection: Projection) -> Projection:
        """Bind projection expressions against the subquery's relations."""
        bound_input = self.bind(projection.input)
        bound_expressions = []
        for expr in projection.expressions:
            bound_expressions.append(self._bind_expr_for(expr, bound_input))
        return Projection(
            input=bound_input,
            expressions=bound_expressions,
            aliases=projection.aliases,
            distinct=projection.distinct,
            distinct_on=self._bind_subquery_distinct_on(projection, bound_input),
        )

    def _bind_subquery_distinct_on(self, projection, bound_input):
        """Bind a subquery projection's DISTINCT ON keys, or None if absent."""
        if projection.distinct_on is None:
            return None
        bound = []
        for key in projection.distinct_on:
            bound.append(self._bind_expr_for(key, bound_input))
        return bound

    def _bind_aggregate(self, aggregate: Aggregate) -> Aggregate:
        """Bind group-by and aggregate expressions."""
        bound_input = self.bind(aggregate.input)
        bound_group_by = []
        for expr in aggregate.group_by:
            bound_group_by.append(self._bind_expr_for(expr, bound_input))
        bound_aggregates = []
        for expr in aggregate.aggregates:
            bound_aggregates.append(self._bind_expr_for(expr, bound_input))

        def bind_set(grouping_set):
            bound_keys = []
            for expr in grouping_set:
                bound_keys.append(self._bind_expr_for(expr, bound_input))
            return bound_keys

        return replace(
            aggregate,
            input=bound_input,
            group_by=bound_group_by,
            aggregates=bound_aggregates,
            grouping_sets=self._bind_grouping_sets(aggregate.grouping_sets, bind_set),
        )

    def _bind_grouping_sets(self, grouping_sets, bind_set):
        """Bind each grouping set's keys, or None for a flat GROUP BY."""
        if grouping_sets is None:
            return None
        bound = []
        for grouping_set in grouping_sets:
            bound.append(bind_set(grouping_set))
        return bound

    def _bind_sort(self, sort: Sort) -> Sort:
        """Bind sort keys against the subquery's relations."""
        bound_input = self.bind(sort.input)
        bound_keys = []
        for key in sort.sort_keys:
            bound_keys.append(self._bind_expr_for(key, bound_input))
        return Sort(
            input=bound_input,
            sort_keys=bound_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )

    def _bind_join(self, join: Join) -> Join:
        """Bind a join inside a subquery."""
        bound_left = self.bind(join.left)
        bound_right = self.bind(join.right)
        bound_condition = None
        if join.condition is not None:
            local: Dict[str, Table] = {}
            self.host._add_plan_to_scope(bound_left, local)
            self.host._add_plan_to_scope(bound_right, local)
            scopes = self.outer_scopes + [local]
            bound_condition = self._bind_expr(join.condition, scopes)
        return Join(
            left=bound_left,
            right=bound_right,
            join_type=join.join_type,
            condition=bound_condition,
        )

    def _bind_having(self, predicate: Expression, aggregate: Aggregate) -> Expression:
        """Bind a HAVING predicate over a subquery aggregate."""
        alias_map: Dict[str, Expression] = {}
        for index in range(len(aggregate.output_names)):
            alias_map[aggregate.output_names[index]] = aggregate.aggregates[index]
        local: Dict[str, Table] = {}
        self.host._add_plan_to_scope(aggregate.input, local)
        scopes = self.outer_scopes + [local]
        return self._bind_having_expr(predicate, alias_map, scopes)

    def _bind_having_expr(
        self,
        expr: Expression,
        alias_map: Dict[str, Expression],
        scopes: List[Dict[str, Table]],
    ) -> Expression:
        """Bind HAVING parts: aggregate output names first, then scopes."""
        output_ref = self._having_output_ref(expr, alias_map)
        if output_ref is not None:
            return output_ref
        if isinstance(expr, BinaryOp):
            left = self._bind_having_expr(expr.left, alias_map, scopes)
            right = self._bind_having_expr(expr.right, alias_map, scopes)
            return BinaryOp(op=expr.op, left=left, right=right)
        if isinstance(expr, UnaryOp):
            operand = self._bind_having_expr(expr.operand, alias_map, scopes)
            return UnaryOp(op=expr.op, operand=operand)
        return self._bind_expr(expr, scopes)

    def _having_output_ref(
        self, expr: Expression, alias_map: Dict[str, Expression]
    ) -> Optional[ColumnRef]:
        """Resolve a bare column against the aggregate's output names."""
        if not isinstance(expr, ColumnRef):
            return None
        if expr.table is not None:
            return None
        source = alias_map.get(expr.column)
        if source is None:
            return None
        return ColumnRef(table=None, column=expr.column, data_type=source.get_type())

    def _bind_expr_for(
        self, expr: Expression, bound_input: LogicalPlanNode
    ) -> Expression:
        """Bind an expression with the input's relations as local scope."""
        local: Dict[str, Table] = {}
        self.host._add_plan_to_scope(bound_input, local)
        return self._bind_expr(expr, self.outer_scopes + [local])

    def _bind_expr(
        self, expr: Expression, scopes: List[Dict[str, Table]]
    ) -> Expression:
        """Bind one expression with innermost-first scope resolution."""
        if isinstance(expr, ColumnRef):
            return self._resolve_column(expr, scopes)
        if isinstance(expr, Literal):
            return expr
        return self._bind_compound_expr(expr, scopes)

    def _bind_compound_expr(
        self, expr: Expression, scopes: List[Dict[str, Table]]
    ) -> Expression:
        """Bind binary/unary/function expressions."""
        if isinstance(expr, BinaryOp):
            left = self._bind_expr(expr.left, scopes)
            right = self._bind_expr(expr.right, scopes)
            return BinaryOp(op=expr.op, left=left, right=right)
        if isinstance(expr, UnaryOp):
            return UnaryOp(op=expr.op, operand=self._bind_expr(expr.operand, scopes))
        if isinstance(expr, FunctionCall):
            bound_args = []
            for arg in expr.args:
                bound_args.append(self._bind_expr(arg, scopes))
            return _rebuild_function_call(
                expr, bound_args, lambda e: self._bind_expr(e, scopes)
            )
        if isinstance(expr, WindowExpr):
            return self._bind_window(expr, scopes)
        return self._bind_special_expr(expr, scopes)

    def _bind_window(
        self, expr: WindowExpr, scopes: List[Dict[str, Table]]
    ) -> WindowExpr:
        """Bind a window's function, partition keys, and order keys in scope."""
        partition = []
        for part in expr.partition_by:
            partition.append(self._bind_expr(part, scopes))
        order = []
        for key in expr.order_keys:
            order.append(self._bind_expr(key, scopes))
        return replace(
            expr,
            function=self._bind_expr(expr.function, scopes),
            partition_by=partition,
            order_keys=order,
        )

    def _bind_special_expr(
        self, expr: Expression, scopes: List[Dict[str, Table]]
    ) -> Expression:
        """Bind CASE / IN-list / BETWEEN / tuple expressions."""
        if isinstance(expr, CaseExpr):
            return self._bind_case(expr, scopes)
        if isinstance(expr, InList):
            value = self._bind_expr(expr.value, scopes)
            options = []
            for option in expr.options:
                options.append(self._bind_expr(option, scopes))
            return InList(value=value, options=options)
        if isinstance(expr, BetweenExpression):
            return BetweenExpression(
                value=self._bind_expr(expr.value, scopes),
                lower=self._bind_expr(expr.lower, scopes),
                upper=self._bind_expr(expr.upper, scopes),
            )
        return self._bind_subquery_or_tuple(expr, scopes)

    def _bind_subquery_or_tuple(
        self, expr: Expression, scopes: List[Dict[str, Table]]
    ) -> Expression:
        """Bind nested subquery expressions and tuples; reject unknowns."""
        if isinstance(expr, TupleExpression):
            bound_items = []
            for item in expr.items:
                bound_items.append(self._bind_expr(item, scopes))
            return TupleExpression(items=tuple(bound_items))
        if self.host._is_subquery_expression(expr):
            return self.host._bind_subquery_expr(
                expr,
                lambda value: self._bind_expr(value, scopes),
                scopes=list(scopes),
            )
        raise BindingError(f"Unsupported expression in subquery: {type(expr).__name__}")

    def _bind_case(self, expr: CaseExpr, scopes: List[Dict[str, Table]]) -> CaseExpr:
        """Bind all branches of a CASE expression."""
        bound_when = []
        for condition, result in expr.when_clauses:
            bound_condition = self._bind_expr(condition, scopes)
            bound_result = self._bind_expr(result, scopes)
            bound_when.append((bound_condition, bound_result))
        bound_else = None
        if expr.else_result is not None:
            bound_else = self._bind_expr(expr.else_result, scopes)
        return CaseExpr(when_clauses=bound_when, else_result=bound_else)

    def _resolve_column(
        self, col_ref: ColumnRef, scopes: List[Dict[str, Table]]
    ) -> ColumnRef:
        """Resolve a column reference innermost-first across scopes."""
        if col_ref.column == "*":
            return col_ref
        if col_ref.table is not None:
            return self._resolve_qualified(col_ref, scopes)
        return self._resolve_unqualified(col_ref, scopes)

    def _resolve_qualified(
        self, col_ref: ColumnRef, scopes: List[Dict[str, Table]]
    ) -> ColumnRef:
        """Resolve a table-qualified reference to its nearest scope."""
        for scope in reversed(scopes):
            table = scope.get(col_ref.table)
            if table is None:
                continue
            column = table.get_column(col_ref.column)
            if column is None:
                raise BindingError(
                    f"Column '{col_ref.column}' not found in table '{col_ref.table}'"
                )
            return ColumnRef(
                table=col_ref.table,
                column=col_ref.column,
                data_type=column.data_type,
            )
        raise BindingError(f"Unknown table '{col_ref.table}' in subquery")

    def _resolve_unqualified(
        self, col_ref: ColumnRef, scopes: List[Dict[str, Table]]
    ) -> ColumnRef:
        """Resolve a bare column name to the nearest scope defining it."""
        for scope in reversed(scopes):
            match = self._match_in_scope(col_ref.column, scope)
            if match is not None:
                return match
        raise BindingError(f"Column '{col_ref.column}' not found in any visible scope")

    def _match_in_scope(
        self, column_name: str, scope: Dict[str, Table]
    ) -> Optional[ColumnRef]:
        """Find a column in one scope; ambiguity is an error."""
        found = None
        for alias, table in scope.items():
            column = table.get_column(column_name)
            if column is None:
                continue
            if found is not None:
                raise BindingError(
                    f"Column '{column_name}' is ambiguous in subquery scope"
                )
            found = ColumnRef(
                table=alias, column=column_name, data_type=column.data_type
            )
        return found

    def __repr__(self) -> str:
        return f"SubqueryPlanBinder(scopes={len(self.outer_scopes)})"
