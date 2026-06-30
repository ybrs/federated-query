"""Binder resolves references and validates types."""

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

    ``model_copy`` copies all other fields (is_aggregate, distinct, WITHIN GROUP
    info, and any field added later), so binding can never silently drop one.
    ``bind_one`` binds the WITHIN GROUP sort key of an ordered-set aggregate.
    """
    bound_key = None
    if expr.within_group_key is not None:
        bound_key = bind_one(expr.within_group_key)
    return expr.model_copy(update={"args": bound_args, "within_group_key": bound_key})


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
                bound_row.append(self._bind_expression(expr))
            bound_rows.append(bound_row)
        return values.model_copy(update={"rows": bound_rows})

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
        table = self._resolve_table(scan)
        kept = []
        for name in scan.columns:
            if name == "*" or table.get_column(name) is not None:
                kept.append(name)
        if len(kept) == 0:
            kept = ["*"]
        if kept == scan.columns:
            return scan
        return scan.model_copy(update={"columns": kept})

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
        """Bind a filter predicate against its bound input plan.

        A filter over an Aggregate is a HAVING clause (aggregate-output names
        in scope); everything else binds against the input's relation scope.
        """
        if isinstance(bound_input, Aggregate):
            return self._bind_having_predicate(predicate, bound_input)
        return self._bind_expression(predicate)

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

        return projection.model_copy(
            update={
                "input": bound_input,
                "expressions": bound_expressions,
                "distinct_on": bound_distinct_on,
            }
        )

    def _bind_distinct_on(self, keys, bound_input) -> Optional[List[Expression]]:
        """Bind the DISTINCT ON key expressions, or None for a plain projection."""
        if keys is None:
            return None
        return self._bind_projection_expressions(keys, bound_input)

    def _bind_projection_expressions(
        self, expressions: List[Expression], bound_input: LogicalPlanNode
    ) -> List[Expression]:
        """Bind projection expressions against the bound input's relation scope."""
        bound_expressions = []
        for expr in expressions:
            bound_expressions.append(self._bind_expression(expr))
        return bound_expressions

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
            return sort.model_copy(
                update={"input": bound_input, "sort_keys": bound_keys}
            )

        if isinstance(bound_input, Aggregate):
            bound_keys = self._bind_sort_keys_for_aggregate(sort.sort_keys, bound_input)
            return sort.model_copy(
                update={"input": bound_input, "sort_keys": bound_keys}
            )

        if isinstance(bound_input, Filter) and isinstance(bound_input.input, Aggregate):
            bound_keys = self._bind_sort_keys_for_aggregate(
                sort.sort_keys,
                bound_input.input,
            )
            return sort.model_copy(
                update={"input": bound_input, "sort_keys": bound_keys}
            )

        bound_keys = self._bind_sort_keys(sort.sort_keys)
        return sort.model_copy(update={"input": bound_input, "sort_keys": bound_keys})

    def _bind_sort_keys(self, sort_keys: List[Expression]) -> List[Expression]:
        """Bind ORDER BY expressions against the relation scope."""
        bound_keys: List[Expression] = []
        for key in sort_keys:
            bound_keys.append(self._bind_expression(key))
        return bound_keys

    def _bind_sort_keys_for_aggregate(
        self,
        sort_keys: List[Expression],
        aggregate: Aggregate,
    ) -> List[Expression]:
        """Bind ORDER BY keys when input is an Aggregate (supports output aliases)."""
        alias_map = self._aggregate_alias_map(aggregate)

        bound_keys: List[Expression] = []
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
            bound_keys.append(self._bind_expression(key))
        return bound_keys

    def _bind_sort_keys_for_projection(
        self,
        sort_keys: List[Expression],
        projection: Projection,
    ) -> List[Expression]:
        """Bind ORDER BY keys when input is a Projection (supports aliases)."""
        alias_map = self._build_alias_expression_map(projection)
        bound_keys: List[Expression] = []
        for key in sort_keys:
            bound_keys.append(self._bind_projection_sort_key(key, alias_map))
        return bound_keys

    def _bind_projection_sort_key(
        self,
        key: Expression,
        alias_map: Dict[str, Expression],
    ) -> Expression:
        """Bind a single ORDER BY expression that may reference a select alias."""
        from ..plan.expressions import ColumnRef

        if isinstance(key, ColumnRef) and key.table is None and key.column in alias_map:
            return self._sort_key_from_alias(key, alias_map[key.column])
        return self._bind_expression(key)

    def _sort_key_from_alias(
        self, key: Expression, source_expr: Expression
    ) -> Expression:
        """Resolve an ORDER BY reference to a SELECT alias's bound expression."""
        from ..plan.expressions import ColumnRef

        if isinstance(source_expr, ColumnRef):
            return ColumnRef(
                table=source_expr.table,
                column=source_expr.column,
                data_type=source_expr.data_type,
            )
        return ColumnRef(table=None, column=key.column, data_type=source_expr.get_type())

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

    def _bind_limit(self, limit: Limit) -> Limit:
        """Bind a Limit node."""
        bound_input = self.bind(limit.input)
        return limit.model_copy(update={"input": bound_input})

    def _bind_join(self, join: Join) -> Join:
        """Bind a Join node."""
        bound_left = self.bind(join.left)
        bound_right = self.bind(join.right)

        bound_condition = None
        if join.condition:
            self._push_scope_for(bound_left, bound_right)
            try:
                bound_condition = self._bind_expression(join.condition)
            finally:
                self._pop_scope()

        return join.model_copy(
            update={
                "left": bound_left,
                "right": bound_right,
                "condition": bound_condition,
            }
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
        return explain.model_copy(update={"input": bound_child})

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
        bound_group_by = self._bind_group_by_expressions(aggregate.group_by)
        bound_aggregates = self._bind_aggregate_expressions(aggregate.aggregates)
        return aggregate.model_copy(
            update={
                "input": bound_input,
                "group_by": bound_group_by,
                "aggregates": bound_aggregates,
                "grouping_sets": self._bind_grouping_sets(
                    aggregate.grouping_sets, self._bind_group_by_expressions
                ),
            }
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
        return cte.model_copy(update={"cte_plan": bound_plan, "child": bound_child})

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
        return cte.model_copy(
            update={
                "cte_plan": bound_plan,
                "child": bound_child,
                "recursive": True,
            }
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
        self, expressions: List[Expression]
    ) -> List[Expression]:
        """Bind GROUP BY expressions against the relation scope."""
        bound = []
        for expr in expressions:
            bound.append(self._bind_expression(expr))
        return bound

    def _bind_aggregate_expressions(
        self, expressions: List[Expression]
    ) -> List[Expression]:
        """Bind aggregate expressions against the relation scope."""
        bound = []
        for expr in expressions:
            bound.append(self._bind_aggregate_expression(expr))
        return bound

    def _bind_aggregate_expression(self, expr: Expression) -> Expression:
        """Bind an aggregate expression's arguments against the relation scope."""
        if isinstance(expr, FunctionCall):
            bound_args = []
            for arg in expr.args:
                bound_args.append(self._bind_expression(arg))
            return _rebuild_function_call(
                expr, bound_args, lambda e: self._bind_expression(e)
            )

        return self._bind_expression(expr)

    def _aggregate_alias_map(self, aggregate: Aggregate) -> Dict[str, Expression]:
        """Map each aggregate output name to its source expression."""
        alias_map: Dict[str, Expression] = {}
        for index in range(len(aggregate.output_names)):
            alias_map[aggregate.output_names[index]] = aggregate.aggregates[index]
        return alias_map

    def _bind_having_predicate(
        self, predicate: Expression, aggregate: Aggregate
    ) -> Expression:
        """Bind a HAVING predicate against the aggregate output and relation scope.

        The scope of the aggregate's input is already pushed (by _bind_filter),
        so a grouping column resolves through it; resolve_in_scopes raises on a
        reference the query does not name.
        """
        alias_map = self._aggregate_alias_map(aggregate)
        return self._bind_having_with(
            predicate,
            alias_map,
            lambda col_ref: self.resolve_in_scopes(self._scope_stack, col_ref),
        )

    def _bind_having_with(
        self,
        predicate: Expression,
        alias_map: Dict[str, Expression],
        base_resolve: Callable[[ColumnRef], Expression],
        subquery_scopes: Optional[List[Dict[str, Table]]] = None,
    ) -> Expression:
        """Bind a HAVING predicate via the shared expression dispatch.

        One HAVING binder for the top-level and subquery contexts; only
        base_resolve - the scope a non-output column resolves against - and the
        nested-subquery scopes differ. An aggregate-output name binds to the
        output; a grouping column or an outer-correlation column falls through to
        base_resolve, instead of being rejected or left unbound.
        """
        return self._bind_having_dispatch(
            predicate,
            lambda col_ref: self._resolve_having_column(
                col_ref, alias_map, base_resolve
            ),
            subquery_scopes,
        )

    def _bind_having_dispatch(
        self,
        expr: Expression,
        resolve: Callable[[ColumnRef], Expression],
        subquery_scopes: Optional[List[Dict[str, Table]]],
    ) -> Expression:
        """Walk a HAVING predicate through the shared dispatch with ``resolve``."""
        return self._bind_expr_dispatch(
            expr,
            lambda value: self._bind_having_dispatch(value, resolve, subquery_scopes),
            resolve,
            subquery_scopes=subquery_scopes,
        )

    def _resolve_having_column(
        self,
        col_ref: ColumnRef,
        alias_map: Dict[str, Expression],
        base_resolve: Callable[[ColumnRef], Expression],
    ) -> Expression:
        """Resolve a HAVING column: an aggregate output name first, else the scope.

        A bare column naming an aggregate output binds to that output; anything
        else (a grouping column, an outer-correlation column, or an unknown
        reference base_resolve will reject) resolves through base_resolve, so
        HAVING binds the same way at top level and inside a subquery.
        """
        if col_ref.table is None and col_ref.column in alias_map:
            source = alias_map[col_ref.column]
            return ColumnRef(
                table=None, column=col_ref.column, data_type=source.get_type()
            )
        return base_resolve(col_ref)

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

    def resolve_in_scopes(
        self, scopes: List[Dict[str, Table]], col_ref: ColumnRef
    ) -> ColumnRef:
        """Resolve a column reference innermost-first across a scope chain.

        This is the SINGLE column resolver for the whole binder (top-level and
        subquery): a qualified reference resolves against the nearest scope that
        defines its table, an unqualified one against the nearest scope that
        defines the column (ambiguity within a scope is an error). A reference
        that resolves in no scope raises, so a typo never slips through unbound.
        """
        if col_ref.column == "*":
            return col_ref
        if col_ref.table is not None:
            return self._resolve_qualified(scopes, col_ref)
        return self._resolve_unqualified(scopes, col_ref)

    def _resolve_qualified(
        self, scopes: List[Dict[str, Table]], col_ref: ColumnRef
    ) -> ColumnRef:
        """Resolve a table-qualified reference to the nearest scope with its table."""
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
                table=col_ref.table, column=col_ref.column, data_type=column.data_type
            )
        raise BindingError(
            f"Table '{col_ref.table}' not found in scope for column "
            f"'{col_ref.column}'"
        )

    def _resolve_unqualified(
        self, scopes: List[Dict[str, Table]], col_ref: ColumnRef
    ) -> ColumnRef:
        """Resolve a bare column name to the nearest scope that defines it."""
        for scope in reversed(scopes):
            match = self._match_in_scope(scope, col_ref.column)
            if match is not None:
                return match
        raise BindingError(f"Column '{col_ref.column}' not found in any table in scope")

    def _match_in_scope(
        self, scope: Dict[str, Table], column_name: str
    ) -> Optional[ColumnRef]:
        """Find a column in one scope; ambiguity across its tables is an error."""
        found = None
        for alias, table in scope.items():
            column = table.get_column(column_name)
            if column is None:
                continue
            if found is not None:
                raise BindingError(
                    f"Column '{column_name}' is ambiguous (found in multiple tables)"
                )
            found = ColumnRef(
                table=alias, column=column_name, data_type=column.data_type
            )
        return found

    def _bind_expression(self, expr: Expression) -> Expression:
        """Bind an expression, resolving every column against the scope chain.

        One expression binder serves top-level, join, and subquery contexts:
        columns resolve through resolve_in_scopes (the live scope stack), which
        validates the qualifier and raises on an unknown table - so an invalid
        reference can never bind to a relation the query does not name.
        """
        return self._bind_expr_dispatch(
            expr,
            lambda value: self._bind_expression(value),
            lambda col_ref: self.resolve_in_scopes(self._scope_stack, col_ref),
        )

    def _bind_expr_dispatch(
        self,
        expr: Expression,
        bind: Callable[[Expression], Expression],
        resolve_column: Callable[[ColumnRef], Expression],
        subquery_scopes: Optional[List[Dict[str, Table]]] = None,
    ) -> Expression:
        """Bind one expression: resolve a column leaf, rebuild a compound node.

        The ONLY things that differ between the single-table, multi-table, and
        subquery binders are ``resolve_column`` (the column leaf), ``bind`` (how
        children recurse), and ``subquery_scopes`` (the scopes a nested subquery
        sees, defaulting to the live scope stack); the dispatch is shared by all.
        """
        if isinstance(expr, ColumnRef):
            return resolve_column(expr)
        if isinstance(expr, (Literal, Interval)):
            return expr
        return self._bind_compound_expr(expr, bind, subquery_scopes)

    def _bind_compound_expr(
        self,
        expr: Expression,
        bind: Callable[[Expression], Expression],
        subquery_scopes: Optional[List[Dict[str, Table]]] = None,
    ) -> Expression:
        """Rebuild a compound expression by binding its children with ``bind``."""
        if isinstance(expr, BinaryOp):
            return self._bind_binary_op(expr, bind)
        if isinstance(expr, UnaryOp):
            return self._bind_unary_op(expr, bind)
        if isinstance(expr, InList):
            return self._bind_in_list(expr, bind)
        if isinstance(expr, BetweenExpression):
            return self._bind_between(expr, bind)
        if isinstance(expr, CaseExpr):
            return self._bind_case_expr(expr, bind)
        if isinstance(expr, Cast):
            return self._bind_cast(expr, bind)
        if isinstance(expr, Extract):
            return self._bind_extract(expr, bind)
        if isinstance(expr, FunctionCall):
            return self._bind_function_args(expr, bind)
        if isinstance(expr, WindowExpr):
            return self._bind_window_expr(expr, bind)
        if self._is_subquery_expression(expr):
            return self._bind_subquery_expr(expr, bind, scopes=subquery_scopes)
        if isinstance(expr, TupleExpression):
            return self._bind_tuple(expr, bind)
        raise BindingError(f"Unsupported expression type: {type(expr).__name__}")

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

    def _bind_binary_op(
        self, binary_op: BinaryOp, bind: Callable[[Expression], Expression]
    ) -> BinaryOp:
        """Bind a binary operation's operands with the given child binder."""
        return BinaryOp(
            op=binary_op.op, left=bind(binary_op.left), right=bind(binary_op.right)
        )

    def _bind_unary_op(
        self, unary_op: UnaryOp, bind: Callable[[Expression], Expression]
    ) -> UnaryOp:
        """Bind a unary operation's operand with the given child binder."""
        return UnaryOp(op=unary_op.op, operand=bind(unary_op.operand))

    def _bind_in_list(
        self, expr: InList, bind: Callable[[Expression], Expression]
    ) -> InList:
        """Bind an IN-list's value and each option with the given child binder."""
        bound_options: List[Expression] = []
        for option in expr.options:
            bound_options.append(bind(option))
        return InList(value=bind(expr.value), options=bound_options)

    def _bind_between(
        self, expr: BetweenExpression, bind: Callable[[Expression], Expression]
    ) -> BetweenExpression:
        """Bind a BETWEEN's value/lower/upper operands with the child binder."""
        return BetweenExpression(
            value=bind(expr.value), lower=bind(expr.lower), upper=bind(expr.upper)
        )

    def _bind_case_expr(
        self, expr: CaseExpr, bind: Callable[[Expression], Expression]
    ) -> CaseExpr:
        """Bind a CASE expression's WHEN/THEN branches and ELSE with the child binder."""
        bound_when = []
        for condition, result in expr.when_clauses:
            bound_when.append((bind(condition), bind(result)))
        bound_else = None
        if expr.else_result is not None:
            bound_else = bind(expr.else_result)
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
        return expr.model_copy(
            update={
                "function": bind(expr.function),
                "partition_by": bound_partition,
                "order_keys": bound_order,
            }
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
        return values.model_copy(update={"rows": bound_rows})

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
        return projection.model_copy(
            update={
                "input": bound_input,
                "expressions": bound_expressions,
                "distinct_on": self._bind_subquery_distinct_on(projection, bound_input),
            }
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

        return aggregate.model_copy(
            update={
                "input": bound_input,
                "group_by": bound_group_by,
                "aggregates": bound_aggregates,
                "grouping_sets": self._bind_grouping_sets(
                    aggregate.grouping_sets, bind_set
                ),
            }
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
        return sort.model_copy(update={"input": bound_input, "sort_keys": bound_keys})

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
        return join.model_copy(
            update={
                "left": bound_left,
                "right": bound_right,
                "condition": bound_condition,
            }
        )

    def _bind_having(self, predicate: Expression, aggregate: Aggregate) -> Expression:
        """Bind a HAVING predicate over a subquery aggregate.

        Delegates to the host's one HAVING binder; only the scope a non-output
        column resolves against differs - here the outer scopes plus the
        aggregate's input, so an outer-correlation column also binds.
        """
        alias_map = self.host._aggregate_alias_map(aggregate)
        local: Dict[str, Table] = {}
        self.host._add_plan_to_scope(aggregate.input, local)
        scopes = self.outer_scopes + [local]
        return self.host._bind_having_with(
            predicate,
            alias_map,
            lambda col_ref: self.host.resolve_in_scopes(scopes, col_ref),
            subquery_scopes=list(scopes),
        )

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
        """Bind one expression via the shared dispatch, resolving against scopes.

        The compound dispatch is the host binder's; only the column leaf (the
        scope chain) and the nested-subquery scopes differ here.
        """
        return self.host._bind_expr_dispatch(
            expr,
            lambda value: self._bind_expr(value, scopes),
            lambda col_ref: self.host.resolve_in_scopes(scopes, col_ref),
            subquery_scopes=list(scopes),
        )

    def __repr__(self) -> str:
        return f"SubqueryPlanBinder(scopes={len(self.outer_scopes)})"
