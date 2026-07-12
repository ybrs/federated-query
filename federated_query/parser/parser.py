"""SQL parser using sqlglot."""

from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

import sqlglot
from sqlglot import exp

from .dialect import FedQPostgres
from .errors import UnsupportedSQLError
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Projection,
    Filter,
    Limit,
    Join,
    JoinType,
    Aggregate,
    Explain,
    ExplainFormat,
    Values,
    SubqueryScan,
    SetOperation,
    SetOpKind,
    LateralJoin,
    CTE,
    CTERef,
)
from ..catalog.catalog import Catalog
from ..plan.expressions import (
    Expression,
    ColumnRef,
    Literal,
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
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
    Quantifier,
    TupleExpression,
    map_children,
)

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


# Parsed JOIN side / kind -> engine JoinType. SEMI/ANTI are produced by
# decorrelation, never parsed from SQL syntax, so they are intentionally absent:
# an unmapped side or kind raises instead of silently becoming INNER.
_JOIN_SIDES = {
    "LEFT": JoinType.LEFT,
    "RIGHT": JoinType.RIGHT,
    "FULL": JoinType.FULL,
}
_JOIN_KINDS = {
    "INNER": JoinType.INNER,
    "CROSS": JoinType.CROSS,
}


class Parser:
    """SQL parser that converts SQL to logical plan."""

    # SELECT clauses the converter actually consumes. Any other clause that
    # sqlglot attaches to a Select (windows, qualify, pivots, locks, into,
    # connect, sample, ...) is rejected so it can never be silently dropped.
    SUPPORTED_SELECT_ARGS = frozenset(
        {
            "expressions",
            "from_",
            "where",
            "group",
            "having",
            "joins",
            "order",
            "limit",
            "offset",
            "distinct",
            "with_",
        }
    )

    # A FROM-less SELECT (SELECT 1) has no input relation, so only a constant
    # projection list is meaningful; with_ is consumed upstream by _convert_with.
    # LIMIT/OFFSET/DISTINCT/WHERE/... have nothing to act on and must not be
    # silently dropped.
    SUPPORTED_VALUES_SELECT_ARGS = frozenset({"expressions", "with_"})

    # Set-operation node args the converter actually consumes. BY NAME (by_name)
    # and a WITH that binds the whole set operation (with_) are NOT handled, so
    # they fail fast rather than silently dropping the clause.
    SUPPORTED_SET_OP_ARGS = frozenset(
        {"this", "expression", "distinct", "order", "limit", "offset"}
    )

    # Per-JOIN clause args the converter consumes (right side, side/kind keyword,
    # ON condition, NATURAL method, USING columns).
    SUPPORTED_JOIN_ARGS = frozenset({"this", "side", "kind", "on", "method", "using"})

    # Aggregate args the converter consumes. big_int is sqlglot's COUNT-returns-
    # bigint marker; separator is the STRING_AGG separator. Any other modifier
    # (an inner ORDER BY / LIMIT carried as its own arg) fails fast.
    SUPPORTED_AGG_ARGS = frozenset({"this", "distinct", "big_int", "separator"})

    # CAST consumes only the value and the target type; safe (TRY_CAST) and
    # FORMAT change semantics and are rejected.
    SUPPORTED_CAST_ARGS = frozenset({"this", "to"})

    # BETWEEN consumes the value and its two bounds; SYMMETRIC is rejected.
    SUPPORTED_BETWEEN_ARGS = frozenset({"this", "low", "high"})

    # IN consumes a value list (expressions) or a subquery (query).
    SUPPORTED_IN_ARGS = frozenset({"this", "expressions", "query"})

    # Window consumes PARTITION BY, ORDER BY and the frame spec; over is the
    # OVER-keyword marker carried by sqlglot.
    SUPPORTED_WINDOW_ARGS = frozenset({"this", "partition_by", "order", "spec", "over"})

    def __init__(self):
        """Initialize parser."""
        self.dialect = FedQPostgres  # Custom Postgres dialect with native EXPLAIN
        # Names of CTEs visible at the current point of conversion; a bare table
        # reference matching one of these is a CTE reference, not a catalog table.
        self._cte_names: List[str] = []

    def parse(self, sql: str) -> LogicalPlanNode:
        """Parse SQL string to an unbound logical plan.

        Args:
            sql: SQL query string

        Returns:
            Logical plan root node
        """
        parsed_ast = self._parse_one(sql)
        return self.ast_to_logical_plan(parsed_ast)

    def parse_ast(self, sql: str) -> exp.Expression:
        """Parse SQL string to sqlglot AST without conversion."""
        return self._parse_one(sql)

    def _parse_one(self, sql: str) -> exp.Expression:
        """Parse exactly one statement, rejecting multi-statement input.

        sqlglot wraps semicolon-separated input in a ``Block``; silently
        running only the first statement would drop the rest, so a block with
        more than one statement is an error.
        """
        parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        if isinstance(parsed, exp.Block):
            if len(parsed.expressions) != 1:
                raise ValueError(
                    "Multi-statement input is not supported; "
                    "submit one statement at a time"
                )
            return parsed.expressions[0]
        return parsed

    def ast_to_logical_plan(self, ast: exp.Expression) -> LogicalPlanNode:
        """Convert sqlglot AST to logical plan.

        Args:
            ast: sqlglot expression tree

        Returns:
            Logical plan root node
        """
        if isinstance(ast, exp.Select):
            return self._convert_select(ast)
        if isinstance(ast, exp.Describe):
            return self._convert_explain(ast)
        if isinstance(ast, (exp.Union, exp.Intersect, exp.Except)):
            return self._convert_set_operation(ast)
        raise ValueError(f"Unsupported AST node type: {type(ast)}")

    _SET_OP_KINDS = {
        exp.Union: SetOpKind.UNION,
        exp.Intersect: SetOpKind.INTERSECT,
        exp.Except: SetOpKind.EXCEPT,
    }

    def _convert_set_operation(self, set_op: exp.SetOperation) -> LogicalPlanNode:
        """Convert a sqlglot set operation (UNION/INTERSECT/EXCEPT) to a plan.

        The two branches become a binary ``SetOperation``; any trailing
        ORDER BY / LIMIT that sqlglot attaches to the set-operation node wraps
        the whole result, so it is layered on top after the branches combine.
        """
        self._reject_unsupported_args(
            set_op, self.SUPPORTED_SET_OP_ARGS, "set operation"
        )
        kind = self._SET_OP_KINDS[type(set_op)]
        distinct = bool(set_op.args.get("distinct"))
        left = self._convert_set_branch(set_op.this)
        right = self._convert_set_branch(set_op.expression)
        # The binary UNION/INTERSECT/EXCEPT of two query blocks maps to one
        # SetOperation node, built from the two converted branches and the kind.
        plan: LogicalPlanNode = SetOperation.create(
            left=left, right=right, kind=kind, distinct=distinct
        )
        plan = self._build_order_by_clause(set_op, plan)
        plan = self._build_limit_clause(set_op, plan)
        return plan

    def _convert_set_branch(self, branch: exp.Expression) -> LogicalPlanNode:
        """Convert one branch of a set operation into a logical plan."""
        branch = self._unwrap_set_branch(branch)
        if isinstance(branch, exp.Select):
            return self._convert_select(branch)
        if isinstance(branch, (exp.Union, exp.Intersect, exp.Except)):
            return self._convert_set_operation(branch)
        raise ValueError(f"Unsupported set-operation branch: {type(branch)}")

    def _unwrap_set_branch(self, branch: exp.Expression) -> exp.Expression:
        """Strip anonymous parentheses/subquery wrappers around a branch.

        ``(SELECT ... UNION SELECT ...)`` parenthesizes a nested set operation
        for grouping; the wrapper carries no projection of its own and must be
        peeled to reach the inner query. An aliased derived table is a real
        relation and is left intact.
        """
        while isinstance(branch, exp.Subquery) and not branch.alias:
            branch = branch.this
        return branch

    def _convert_select(self, select: exp.Select) -> LogicalPlanNode:
        """Convert SELECT statement to logical plan.

        Args:
            select: sqlglot Select node

        Returns:
            Logical plan node
        """
        with_clause = select.args.get("with_")
        if with_clause is not None:
            return self._convert_with(select, with_clause)
        return self._convert_plain_select(select)

    def _reject_distinct_window(self, select: exp.Select) -> None:
        """Reject SELECT DISTINCT over a window function (silent-drop guard).

        A windowed aggregate routes through the aggregate path, which ignores
        DISTINCT, so DISTINCT would be silently dropped and return duplicate
        rows. Checked on the raw AST so every downstream path is covered.
        """
        if not select.args.get("distinct"):
            return
        for projection in select.expressions:
            if projection.find(exp.Window) is not None:
                raise UnsupportedSQLError(
                    "SELECT DISTINCT with window functions is not supported"
                )

    def _reject_unknown_select_args(self, select: exp.Select) -> None:
        """Fail fast on any SELECT clause the converter does not consume.

        Without this the parser would silently ignore clauses such as named
        WINDOW, pivots, locks, or sample and return a wrong answer or crash deep
        in the engine. Clauses we now support (DISTINCT ON, GROUP BY ROLLUP/CUBE/
        GROUPING SETS, FETCH FIRST) are handled by their own converters.
        """
        self._reject_unsupported_args(select, self.SUPPORTED_SELECT_ARGS, "SELECT")

    def _reject_unsupported_args(self, node, supported, context: str) -> None:
        """Fail fast on any arg of `node` outside `supported` (silent-drop guard).

        Shared by every builder that consumes only a subset of an AST node's
        clauses. `context` names the construct so a dropped clause crashes loudly
        with a readable message instead of being ignored and returning a wrong
        answer.
        """
        for name, value in node.args.items():
            if not value:
                continue
            if name not in supported:
                clause = name.upper().rstrip("_")
                raise UnsupportedSQLError(f"{context} does not support {clause}")

    def _convert_plain_select(self, select: exp.Select) -> LogicalPlanNode:
        """Convert a SELECT body (without its WITH clause) to a logical plan."""
        self._reject_unknown_select_args(select)
        self._reject_distinct_window(select)
        if select.args.get("from_") is None:
            return self._build_values_select(select)
        plan = self._build_from_clause(select)
        plan = self._build_where_clause(select, plan)
        plan = self._build_group_by_clause(select, plan)
        plan = self._build_having_clause(select, plan)
        plan = self._build_select_clause(select, plan)
        plan = self._build_order_by_clause(select, plan)
        plan = self._build_limit_clause(select, plan)
        return plan

    def _convert_with(
        self, select: exp.Select, with_clause: exp.With
    ) -> LogicalPlanNode:
        """Convert a ``WITH`` query into nested CTE nodes wrapping the body.

        Each CTE name is brought into scope before the body is converted so a
        later CTE (and, for ``WITH RECURSIVE``, the CTE's own body) resolves a
        bare reference to a ``CTERef`` rather than a catalog table. The scope is
        restored afterwards so sibling subqueries do not see these names.
        """
        recursive = bool(with_clause.args.get("recursive"))
        scope_mark = len(self._cte_names)
        definitions = self._convert_cte_definitions(with_clause, recursive)
        child = self._convert_plain_select(select)
        plan = self._wrap_ctes(definitions, child, recursive)
        del self._cte_names[scope_mark:]
        return plan

    def _convert_cte_definitions(self, with_clause, recursive):
        """Convert each CTE body in order, registering names as they bind."""
        definitions = []
        for cte in with_clause.expressions:
            name = cte.alias
            columns = self._cte_column_names(cte)
            if recursive:
                self._cte_names.append(name)
            cte_plan = self.ast_to_logical_plan(cte.this)
            if not recursive:
                self._cte_names.append(name)
            definitions.append((name, columns, cte_plan))
        return definitions

    def _wrap_ctes(self, definitions, child, recursive):
        """Nest CTE definitions around the body, innermost binding last."""
        plan = child
        for name, columns, cte_plan in reversed(definitions):
            # A WITH binding wraps the body in a CTE node naming the defined
            # relation, built from this definition's plan around the inner plan.
            plan = CTE.create(
                name=name,
                cte_plan=cte_plan,
                child=plan,
                recursive=recursive,
                column_names=columns,
            )
        return plan

    def _cte_column_names(self, cte: exp.CTE) -> Optional[List[str]]:
        """Return an explicit CTE output column list (``counter(n)``), if any."""
        alias_node = cte.args.get("alias")
        if alias_node is None:
            return None
        columns = alias_node.args.get("columns")
        if not columns:
            return None
        names = []
        for identifier in columns:
            names.append(identifier.name)
        return names

    def _maybe_cte_ref(
        self, table_expr: exp.Table, columns: List[str]
    ) -> Optional[CTERef]:
        """Build a CTERef when a bare table name matches an in-scope CTE."""
        if table_expr.catalog or table_expr.db:
            return None
        if table_expr.name not in self._cte_names:
            return None
        alias = self._extract_table_alias(table_expr)
        # A bare table name matching an in-scope WITH name is a CTE reference,
        # built from the sqlglot Table node's name, alias and needed columns.
        return CTERef.create(name=table_expr.name, alias=alias, columns=columns)

    def _build_values_select(self, select: exp.Select) -> LogicalPlanNode:
        """Build a single-row Values plan for a FROM-less SELECT.

        Supports constant projections such as ``SELECT 42`` or
        ``SELECT 'completed'``; clauses that require an input relation
        (WHERE/GROUP/HAVING/ORDER/JOIN as well as LIMIT/OFFSET/DISTINCT) have
        nothing to act on and are rejected so they are never silently dropped.
        """
        self._reject_unsupported_args(
            select, self.SUPPORTED_VALUES_SELECT_ARGS, "FROM-less SELECT"
        )
        row = []
        names = []
        for select_expr in select.expressions:
            row.append(self._convert_expression(select_expr))
            names.append(self._get_alias(select_expr))
        # A FROM-less SELECT (SELECT 1) is a single constant row, modelled as a
        # Values relation built from the converted projection expressions.
        return Values.create(rows=[row], output_names=names)

    def _convert_explain(self, describe: exp.Describe) -> LogicalPlanNode:
        """Convert a native ``exp.Describe`` (EXPLAIN) node to a logical plan.

        The wrapped statement is already a parsed AST, so no SQL text is
        re-scanned; the output format comes from the dialect's ``as_json``
        flag set while consuming the EXPLAIN option list.
        """
        inner_statement = describe.this
        if inner_statement is None:
            raise ValueError("EXPLAIN requires a statement to describe")
        child_plan = self.ast_to_logical_plan(inner_statement)
        explain_format = self._explain_format(describe)
        # EXPLAIN wraps the described statement in an Explain node, built from
        # the already-converted child plan plus the requested output format.
        return Explain.create(input=child_plan, format=explain_format)

    def _explain_format(self, describe: exp.Describe) -> ExplainFormat:
        """Map the dialect's JSON flag to an ExplainFormat."""
        if describe.args.get("as_json"):
            return ExplainFormat.JSON
        return ExplainFormat.TEXT

    def _build_from_clause(self, select: exp.Select) -> LogicalPlanNode:
        """Build scan node from FROM clause.

        Args:
            select: sqlglot Select node

        Returns:
            Scan node or Join node
        """
        from_clause = select.args.get("from_")
        if not from_clause:
            raise ValueError("SELECT must have FROM clause")

        table_expr = from_clause.this

        joins = select.args.get("joins")
        if joins:
            return self._build_join_plan(select, table_expr, joins)

        if isinstance(table_expr, exp.Subquery):
            return self._build_derived_table(table_expr)
        if isinstance(table_expr, exp.Values):
            return self._values_relation(table_expr)
        self._reject_unsupported_relation(table_expr)

        all_columns = self._collect_needed_columns(select)
        cte_ref = self._maybe_cte_ref(table_expr, all_columns)
        if cte_ref is not None:
            return cte_ref

        table_parts = self._extract_table_parts(table_expr)
        table_alias = self._extract_table_alias(table_expr)

        datasource = table_parts[0]
        schema_name = table_parts[1]
        table_name = table_parts[2]

        # A plain FROM table is a base-relation Scan, built from the sqlglot
        # Table node's datasource/schema/name plus the columns the query needs.
        return Scan.create(
            datasource=datasource,
            schema_name=schema_name,
            table_name=table_name,
            columns=all_columns,
            alias=table_alias,
            sample=self._table_sample_sql(table_expr),
        )

    def _table_sample_sql(self, table_expr: exp.Expression) -> Optional[str]:
        """Render a table's TABLESAMPLE clause as Postgres-form SQL, or None.

        The source dialect transpile at render time turns this into the form the
        target accepts (e.g. DuckDB's ``BERNOULLI (10 PERCENT)``).
        """
        sample = table_expr.args.get("sample")
        if sample is None:
            return None
        return sample.sql(dialect=self.dialect)

    def _build_join_input(
        self, table_expr: exp.Expression, column_usage: Dict[str, List[str]]
    ) -> LogicalPlanNode:
        """Build the plan for one input relation of a join."""
        if isinstance(table_expr, exp.Subquery):
            return self._build_derived_table(table_expr)
        if isinstance(table_expr, exp.Values):
            return self._values_relation(table_expr)
        self._reject_unsupported_relation(table_expr)
        table_alias = self._extract_table_alias(table_expr)
        columns = self._columns_for_join_table(column_usage)
        cte_ref = self._maybe_cte_ref(table_expr, columns)
        if cte_ref is not None:
            return cte_ref
        table_parts = self._extract_table_parts(table_expr)
        # One join input that is a base table becomes a Scan, built from the
        # sqlglot Table node with only the columns this side of the join uses.
        return Scan.create(
            datasource=table_parts[0],
            schema_name=table_parts[1],
            table_name=table_parts[2],
            columns=columns,
            alias=table_alias,
            sample=self._table_sample_sql(table_expr),
        )

    def _build_derived_table(self, subquery: exp.Subquery) -> SubqueryScan:
        """Build a SubqueryScan for a derived table in FROM."""
        alias = subquery.alias
        if not alias:
            raise ValueError("Derived tables require an alias")
        inner_plan = self.ast_to_logical_plan(subquery.this)
        column_names = self._derived_column_names(subquery)
        # A derived table (subquery in FROM) is a SubqueryScan wrapping the
        # converted inner plan under the required table alias, carrying any
        # ``AS alias(col, ...)`` column-rename list.
        return SubqueryScan.create(
            input=inner_plan, alias=alias, column_names=column_names
        )

    def _derived_column_names(self, subquery: exp.Subquery) -> Optional[List[str]]:
        """Extract a derived table's column-alias list, or None if it has none."""
        table_alias = subquery.args.get("alias")
        if table_alias is None or not table_alias.columns:
            return None
        names = []
        for identifier in table_alias.columns:
            names.append(identifier.name)
        return names

    def _values_relation(self, values: exp.Values) -> SubqueryScan:
        """Build a derived table from a VALUES list: ``(VALUES ...) AS v(a, b)``.

        The constant rows become a Values relation; the table alias and column
        aliases name it, so the rest of the engine treats it like any other
        derived table.
        """
        alias = values.alias
        if not alias:
            raise ValueError("VALUES in FROM requires an alias")
        rows = self._convert_values_rows(values)
        names = self._values_column_names(values, rows)
        # A VALUES list in FROM is a constant Values relation wrapped as a
        # SubqueryScan so its alias and column names name it like a derived table.
        return SubqueryScan.create(
            input=Values.create(rows=rows, output_names=names), alias=alias
        )

    def _convert_values_rows(self, values: exp.Values) -> List[List[Expression]]:
        """Convert each VALUES row (a tuple of constants) into expressions."""
        rows = []
        for row_expr in values.expressions:
            row = []
            for cell in row_expr.expressions:
                row.append(self._convert_expression(cell))
            if rows and len(row) != len(rows[0]):
                raise UnsupportedSQLError(
                    "VALUES rows must all have the same number of columns"
                )
            rows.append(row)
        return rows

    def _values_column_names(
        self, values: exp.Values, rows: List[List[Expression]]
    ) -> List[str]:
        """Return VALUES column names from the alias, or column1..N by default."""
        width = len(rows[0]) if rows else 0
        names = self._values_alias_columns(values)
        if names is not None:
            if rows and len(names) != width:
                raise UnsupportedSQLError(
                    "VALUES column alias count does not match the row width"
                )
            return names
        return self._default_value_columns(width)

    def _values_alias_columns(self, values: exp.Values) -> Optional[List[str]]:
        """Column names from the table alias's column list, or None if absent."""
        alias_node = values.args.get("alias")
        if alias_node is None:
            return None
        columns = alias_node.args.get("columns")
        if not columns:
            return None
        names = []
        for column in columns:
            names.append(column.name)
        return names

    def _default_value_columns(self, width: int) -> List[str]:
        """Default column1..columnN names when VALUES has no column aliases."""
        names = []
        for index in range(width):
            names.append(f"column{index + 1}")
        return names

    def _reject_unsupported_relation(self, table_expr: exp.Expression) -> None:
        """Fail fast on FROM items the engine cannot plan.

        LATERAL joins are handled before this point (see ``_build_lateral_join``).
        """
        if not isinstance(table_expr, exp.Table):
            raise ValueError(f"Unsupported FROM item: {type(table_expr).__name__}")
        if table_expr.args.get("pivots"):
            raise UnsupportedSQLError("PIVOT/UNPIVOT is not supported")

    def _build_join_plan(
        self, select: exp.Select, left_table: exp.Table, joins: List[exp.Join]
    ) -> LogicalPlanNode:
        """Build join plan from FROM and JOIN clauses.

        Args:
            select: sqlglot Select node
            left_table: Left table from FROM clause
            joins: List of JOIN clauses

        Returns:
            Join node
        """
        column_usage = self._collect_join_column_usage(select)

        left_plan = self._build_join_input(left_table, column_usage)

        current_plan = left_plan

        for join_clause in joins:
            self._reject_unsupported_args(join_clause, self.SUPPORTED_JOIN_ARGS, "JOIN")
            if isinstance(join_clause.this, exp.Lateral):
                current_plan = self._build_lateral_join(current_plan, join_clause)
                continue
            right_plan = self._build_join_input(join_clause.this, column_usage)

            join_type = self._extract_join_type(join_clause)
            join_condition = None
            if join_clause.args.get("on"):
                join_condition = self._convert_expression(join_clause.args["on"])

            # Each JOIN clause folds the running plan and the right input into a
            # binary Join node with the side/kind, ON condition and USING/NATURAL.
            current_plan = Join.create(
                left=current_plan,
                right=right_plan,
                join_type=join_type,
                condition=join_condition,
                natural=self._is_natural_join(join_clause),
                using=self._extract_using_columns(join_clause),
            )

        return current_plan

    def _build_lateral_join(
        self, left_plan: LogicalPlanNode, join_clause: exp.Join
    ) -> LateralJoin:
        """Build a LATERAL (dependent) join: the right may reference the left.

        The right is a derived table aliased like any other; a ``LEFT JOIN
        LATERAL`` keeps non-matching outer rows, while a comma/``CROSS`` lateral
        drops them (INNER). The binder later resolves the right's correlated
        references against the left's columns.
        """
        lateral = join_clause.this
        alias = lateral.alias
        if not alias:
            raise ValueError("LATERAL derived tables require an alias")
        # The LATERAL right side is a derived table that may reference the left;
        # built as a SubqueryScan over its converted inner plan under the alias.
        right = SubqueryScan.create(
            input=self.ast_to_logical_plan(lateral.this.this), alias=alias
        )
        side = join_clause.args.get("side")
        join_type = JoinType.LEFT if side else JoinType.INNER
        # A LATERAL join is a dependent join, built from the left plan and the
        # lateral right; LEFT when a side keyword is present, else INNER.
        return LateralJoin.create(left=left_plan, right=right, join_type=join_type)

    def _is_natural_join(self, join_clause: exp.Join) -> bool:
        """Whether the JOIN carries the NATURAL method keyword."""
        method = join_clause.args.get("method")
        return bool(method) and str(method).upper() == "NATURAL"

    def _extract_using_columns(self, join_clause: exp.Join) -> Optional[List[str]]:
        """Return the USING column names, or None when there is no USING."""
        using = join_clause.args.get("using")
        if not using:
            return None
        columns: List[str] = []
        for identifier in using:
            columns.append(identifier.name)
        return columns

    def _extract_table_alias(self, table_expr: exp.Table) -> Optional[str]:
        """Extract table alias if present.

        Args:
            table_expr: sqlglot Table node

        Returns:
            Table alias or table name if no alias
        """
        alias = table_expr.alias
        if alias:
            return str(alias)
        return str(table_expr.alias_or_name)

    def _filter_columns_for_table(
        self, columns: List[str], table_alias: Optional[str]
    ) -> List[str]:
        """Filter columns that belong to a specific table.

        Args:
            columns: All column names
            table_alias: Table alias to filter by

        Returns:
            List of columns for this table
        """
        if "*" in columns:
            return ["*"]

        filtered = []
        for col in columns:
            if "." in col:
                table_part, col_part = col.split(".", 1)
                if table_part == table_alias:
                    filtered.append(col_part)
            else:
                filtered.append(col)

        return filtered

    def _collect_join_column_usage(
        self, select: exp.Select
    ) -> Dict[Optional[str], List[str]]:
        """Collect column usage grouped by table alias for join planning."""
        usage: Dict[Optional[str], List[str]] = {}
        for column in select.find_all(exp.Column):
            table_name = column.table
            column_name = column.name
            if not column_name:
                continue
            if table_name is None:
                bucket = usage.setdefault(None, [])
                if column_name not in bucket:
                    bucket.append(column_name)
                continue
            bucket = usage.setdefault(table_name, [])
            if column_name not in bucket:
                bucket.append(column_name)
        return usage

    def _columns_for_join_table(
        self, usage: Dict[Optional[str], List[str]]
    ) -> List[str]:
        """The over-collected read-set for one join input: every column the
        query references, whatever table each was written against.

        The parser does not attribute a column to a table - resolving and
        qualifying every reference is the binder's job - so it over-collects and
        lets ``_bind_scan`` drop the names this table does not have (its
        docstring: "may include names of other relations ... names not present
        in this table are dropped"). Over-collecting is why an unqualified
        aggregate measure (q03's ``sum(ss_ext_sales_price)``) is not lost: it
        rides every input's read-set and survives on the one table that has it.
        A star anywhere means read everything.
        """
        columns: List[str] = []
        for names in usage.values():
            self._merge_referenced_names(names, columns)
        if "*" in columns:
            return ["*"]
        return columns

    def _merge_referenced_names(self, names: List[str], columns: List[str]) -> None:
        """Append the not-yet-seen names to the accumulator, preserving order."""
        for name in names:
            if name not in columns:
                columns.append(name)

    def _extract_join_type(self, join_clause: exp.Join) -> JoinType:
        """Map a parsed JOIN's side/kind to a JoinType, raising on anything else.

        A plain ``JOIN`` (no side, no kind) and an explicit ``INNER`` are INNER.
        Any side or kind the engine does not model (e.g. a dialect's SEMI/ANTI)
        raises rather than being silently downgraded to INNER.
        """
        kind = join_clause.args.get("kind")
        side = join_clause.args.get("side")
        if side:
            return self._sided_join_type(side, kind)
        if not kind:
            return JoinType.INNER
        if kind.upper() in _JOIN_KINDS:
            return _JOIN_KINDS[kind.upper()]
        raise UnsupportedSQLError(f"Unsupported JOIN kind: {kind}")

    def _sided_join_type(self, side: str, kind) -> JoinType:
        """An outer-join side (LEFT/RIGHT/FULL), optionally with an OUTER kind."""
        if side.upper() not in _JOIN_SIDES:
            raise UnsupportedSQLError(f"Unsupported JOIN side: {side}")
        if kind and kind.upper() != "OUTER":
            raise UnsupportedSQLError(
                f"Unsupported JOIN kind '{kind}' with side '{side}'"
            )
        return _JOIN_SIDES[side.upper()]

    def _extract_table_parts(self, table_expr: exp.Table) -> Tuple[str, str, str]:
        """Extract datasource, schema, and table name.

        Args:
            table_expr: sqlglot Table node

        Returns:
            Tuple of (datasource, schema, table_name)
        """
        parts = []
        if table_expr.catalog:
            parts.append(table_expr.catalog)
        if table_expr.db:
            parts.append(table_expr.db)
        parts.append(table_expr.name)

        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return "default", parts[0], parts[1]
        return "default", "public", parts[0]

    def _collect_needed_columns(self, select: exp.Select) -> List[str]:
        """Collect all columns needed from table.

        Args:
            select: sqlglot Select node

        Returns:
            List of column names
        """
        columns = []

        for expr in select.expressions:
            col_names = self._extract_column_names(expr)
            for col_name in col_names:
                if col_name not in columns:
                    columns.append(col_name)

        for clause in ("where", "group", "having", "order"):
            clause_expr = select.args.get(clause)
            if clause_expr is None:
                continue
            col_names = self._extract_column_names(clause_expr)
            for col_name in col_names:
                if col_name not in columns:
                    columns.append(col_name)

        if not columns:
            columns = ["*"]

        return columns

    def _extract_column_names(self, expr: exp.Expression) -> List[str]:
        """Extract column names from expression.

        Args:
            expr: sqlglot expression

        Returns:
            List of column names
        """
        columns = []

        if isinstance(expr, exp.Column):
            columns.append(expr.name)
        elif isinstance(expr, exp.Star):
            columns.append("*")
        else:
            for child in expr.iter_expressions():
                child_cols = self._extract_column_names(child)
                columns.extend(child_cols)

        return columns

    def _build_where_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build filter node from WHERE clause.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Logical plan with filter (or original if no WHERE)
        """
        where = select.args.get("where")
        if not where:
            return input_plan

        self._reject_window_in_clause(where.this, "WHERE")
        predicate = self._convert_expression(where.this)
        # A WHERE clause is a Filter over its input, built from the converted
        # boolean predicate of the sqlglot Where node.
        return Filter.create(input=input_plan, predicate=predicate)

    def _reject_window_in_clause(self, node: exp.Expression, clause: str) -> None:
        """Reject a window function used directly in WHERE / GROUP BY / HAVING.

        Window functions are only legal in SELECT and ORDER BY (they evaluate
        after WHERE/GROUP BY/HAVING). Fail fast with a clear error rather than
        push invalid SQL to a source or mis-evaluate it on the merge path.
        Only THIS clause's own scope is checked: the walk prunes at any nested
        SELECT, whose windows belong to that subquery's own SELECT list (a
        ranked derived table filtered by an outer IN is legal - TPC-DS q70).
        """
        if node is None:
            return
        for found in node.walk(prune=lambda n: isinstance(n, exp.Select)):
            if isinstance(found, exp.Window):
                raise UnsupportedSQLError(
                    f"window functions are not allowed in {clause}"
                )

    def _build_group_by_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build aggregate node from GROUP BY clause.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Logical plan with aggregate (or original if no GROUP BY)
        """
        group = select.args.get("group")
        has_aggregates = self._has_aggregate_functions(select)

        if group:
            self._reject_window_in_clause(group, "GROUP BY")
            return self._create_aggregate_node(select, input_plan, group)

        if has_aggregates:
            return self._create_aggregate_node_without_grouping(select, input_plan)

        return input_plan

    def _has_aggregate_functions(self, select: exp.Select) -> bool:
        """Whether the SELECT list, HAVING, or ORDER BY contains an aggregate.

        An aggregate may appear in HAVING or ORDER BY without being in the SELECT
        list (``... HAVING COUNT(*) > 1``); missing those would skip building the
        Aggregate node and mis-plan the aggregate over a raw scan.
        """
        for expr in select.expressions:
            if self._contains_aggregate_function(expr):
                return True
        return self._clause_has_aggregate(select, ("having", "order"))

    def _clause_has_aggregate(self, select: exp.Select, keys) -> bool:
        """Whether any of the named SELECT clauses carries an aggregate."""
        for key in keys:
            clause = select.args.get(key)
            if clause is not None and self._contains_aggregate_function(clause):
                return True
        return False

    def _contains_aggregate_function(self, expr: exp.Expression) -> bool:
        """Check if expression contains aggregate function.

        Aggregates inside a nested SELECT belong to that subquery, not to
        the current query block, so traversal stops at subquery boundaries.

        Args:
            expr: sqlglot expression

        Returns:
            True if expression contains aggregate function
        """
        if self._is_aggregate_function(expr):
            return True

        if isinstance(expr, (exp.Select, exp.Subquery)):
            return False

        for child in expr.iter_expressions():
            if self._contains_aggregate_function(child):
                return True

        return False

    def _is_aggregate_function(self, expr: exp.Expression) -> bool:
        """Check if an expression represents an aggregate function."""
        aggregate_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
        return isinstance(expr, exp.AggFunc) or isinstance(expr, aggregate_types)

    def _create_aggregate_node_without_grouping(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> Aggregate:
        """Create aggregate node without GROUP BY.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Aggregate node with empty group_by
        """
        aggregates = self._extract_aggregates_from_select(select)
        output_names = self._extract_output_names(select)

        # An aggregate SELECT with no GROUP BY collapses the input to one row;
        # built with an empty group_by from the converted SELECT-list aggregates.
        return Aggregate.create(
            input=input_plan,
            group_by=[],
            aggregates=aggregates,
            output_names=output_names,
        )

    def _create_aggregate_node(
        self, select: exp.Select, input_plan: LogicalPlanNode, group: exp.Group
    ) -> Aggregate:
        """Create aggregate node from SELECT and GROUP BY.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan
            group: sqlglot Group node

        Returns:
            Aggregate node
        """
        grouping_sets = self._extract_grouping_sets(group)
        group_by_exprs = self._extract_group_by_exprs(group, grouping_sets)
        aggregates = self._extract_aggregates_from_select(select)
        output_names = self._extract_output_names(select)

        # A GROUP BY SELECT is an Aggregate, built from the converted grouping
        # keys, SELECT-list aggregates and any expanded ROLLUP/CUBE grouping sets.
        return Aggregate.create(
            input=input_plan,
            group_by=group_by_exprs,
            aggregates=aggregates,
            output_names=output_names,
            grouping_sets=grouping_sets,
        )

    def _extract_group_by_exprs(
        self, group: exp.Group, grouping_sets: Optional[List[List[Expression]]] = None
    ) -> List[Expression]:
        """Extract the flat grouping expressions used for column resolution.

        For ROLLUP/CUBE/GROUPING SETS this is the distinct union of the keys
        across all grouping sets; for an ordinary GROUP BY it is the key list.
        """
        if grouping_sets is not None:
            return self._union_grouping_keys(grouping_sets)
        return self._convert_group_expressions(group.expressions)

    def _convert_group_expressions(
        self, expressions: List[exp.Expression]
    ) -> List[Expression]:
        """Convert a list of grouping key expressions."""
        converted = []
        for expr in expressions:
            converted.append(self._convert_expression(expr))
        return converted

    def _union_grouping_keys(
        self, grouping_sets: List[List[Expression]]
    ) -> List[Expression]:
        """Distinct grouping keys across every grouping set, preserving order."""
        seen = []
        keys = []
        for grouping_set in grouping_sets:
            for expr in grouping_set:
                token = expr.to_sql()
                if token not in seen:
                    seen.append(token)
                    keys.append(expr)
        return keys

    def _extract_grouping_sets(
        self, group: exp.Group
    ) -> Optional[List[List[Expression]]]:
        """Expand GROUP BY ROLLUP/CUBE/GROUPING SETS into explicit grouping sets.

        Returns None for an ordinary single-level GROUP BY. Supports leading
        normal keys plus exactly one ROLLUP/CUBE/GROUPING SETS construct; richer
        combinations fail fast rather than risk a wrong expansion.
        """
        construct = self._single_grouping_construct(group)
        if construct is None:
            return None
        normal_keys = self._convert_group_expressions(group.expressions)
        sets = []
        for construct_set in self._expand_grouping_construct(construct):
            sets.append(normal_keys + construct_set)
        return sets

    def _single_grouping_construct(self, group: exp.Group):
        """Return the one ROLLUP/CUBE/GROUPING SETS node, or None; reject mixes."""
        if group.args.get("totals"):
            raise UnsupportedSQLError("GROUP BY ... WITH TOTALS is not supported")
        present = self._grouping_constructs(group)
        if not present:
            return None
        if len(present) > 1:
            raise UnsupportedSQLError(
                "Combining multiple ROLLUP/CUBE/GROUPING SETS is not supported"
            )
        return present[0]

    def _grouping_constructs(self, group: exp.Group) -> List[exp.Expression]:
        """Collect the ROLLUP/CUBE/GROUPING SETS nodes attached to a GROUP BY."""
        present = []
        for kind in ("rollup", "cube", "grouping_sets"):
            present.extend(group.args.get(kind) or [])
        return present

    def _expand_grouping_construct(self, construct) -> List[List[Expression]]:
        """Expand one ROLLUP/CUBE/GROUPING SETS node into a list of grouping sets."""
        if isinstance(construct, exp.Rollup):
            return self._rollup_sets(
                self._convert_group_expressions(construct.expressions)
            )
        if isinstance(construct, exp.Cube):
            return self._cube_sets(
                self._convert_group_expressions(construct.expressions)
            )
        return self._explicit_grouping_sets(construct)

    def _rollup_sets(self, keys: List[Expression]) -> List[List[Expression]]:
        """ROLLUP(k1..kn): the n+1 prefixes [k1..kn], ..., [k1], []."""
        sets = []
        for size in range(len(keys), -1, -1):
            sets.append(list(keys[:size]))
        return sets

    def _cube_sets(self, keys: List[Expression]) -> List[List[Expression]]:
        """CUBE(k1..kn): every subset of the keys."""
        sets = []
        for mask in range(2 ** len(keys)):
            sets.append(self._subset_for_mask(keys, mask))
        return sets

    def _subset_for_mask(self, keys: List[Expression], mask: int) -> List[Expression]:
        """Pick the keys whose bit is set in the bitmask."""
        subset = []
        for index, key in enumerate(keys):
            if mask & (1 << index):
                subset.append(key)
        return subset

    def _explicit_grouping_sets(self, grouping_sets) -> List[List[Expression]]:
        """Convert an explicit GROUPING SETS node into lists of expressions."""
        sets = []
        for element in grouping_sets.expressions:
            sets.append(self._grouping_set_columns(element))
        return sets

    def _grouping_set_columns(self, element: exp.Expression) -> List[Expression]:
        """Return one grouping-set element's columns (handles (), (a), (a, b))."""
        if isinstance(element, exp.Tuple):
            return self._convert_group_expressions(element.expressions)
        if isinstance(element, exp.Paren):
            return [self._convert_expression(element.this)]
        return [self._convert_expression(element)]

    def _extract_aggregates_from_select(self, select: exp.Select) -> List[Expression]:
        """Extract all expressions from SELECT for aggregation.

        Args:
            select: sqlglot Select node

        Returns:
            List of expressions (both group keys and aggregates)
        """
        expressions = []
        for expr in select.expressions:
            converted = self._convert_expression(expr)
            expressions.append(converted)
        return expressions

    def _extract_output_names(self, select: exp.Select) -> List[str]:
        """Extract output column names from SELECT.

        Args:
            select: sqlglot Select node

        Returns:
            List of output names
        """
        names = []
        for expr in select.expressions:
            alias = self._get_alias(expr)
            names.append(alias)
        return names

    def _build_having_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build filter node from HAVING clause.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Logical plan with filter (or original if no HAVING)
        """
        having = select.args.get("having")
        if not having:
            return input_plan

        self._reject_window_in_clause(having.this, "HAVING")
        predicate = self._convert_expression(having.this)

        if isinstance(input_plan, Aggregate):
            predicate = self._rewrite_having_predicate(predicate, input_plan)

        # HAVING filters grouped rows, so it is a Filter above the Aggregate,
        # built from the predicate rewritten to reference aggregate outputs.
        return Filter.create(input=input_plan, predicate=predicate)

    def _is_aggregate_plan(self, plan: LogicalPlanNode) -> bool:
        """Whether the plan is an aggregate, optionally under a HAVING filter."""
        if isinstance(plan, Aggregate):
            return True
        return isinstance(plan, Filter) and isinstance(plan.input, Aggregate)

    def _build_select_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build projection node from SELECT clause.

        The aggregate (GROUP BY) result carries its own output shape, so the
        projection is the aggregate itself. DISTINCT has no representation on
        that path, so it fails fast instead of being silently dropped (which
        would let duplicate grouped rows through).
        """
        if self._is_aggregate_plan(input_plan):
            if select.args.get("distinct"):
                raise UnsupportedSQLError(
                    "SELECT DISTINCT over GROUP BY is not supported"
                )
            return input_plan

        expressions = []
        aliases = []

        for select_expr in select.expressions:
            expr = self._convert_expression(select_expr)
            expressions.append(expr)

            alias = self._get_alias(select_expr)
            aliases.append(alias)

        distinct_arg = select.args.get("distinct")
        has_distinct = bool(distinct_arg)
        # The SELECT list is a Projection over its input, built from the
        # converted output expressions, their aliases and any DISTINCT/DISTINCT ON.
        return Projection.create(
            input=input_plan,
            expressions=expressions,
            aliases=aliases,
            distinct=has_distinct,
            distinct_on=self._distinct_on_keys(distinct_arg),
        )

    def _distinct_on_keys(self, distinct_arg) -> Optional[List[Expression]]:
        """Convert the key expressions of a DISTINCT ON, or None for plain DISTINCT."""
        if distinct_arg is None:
            return None
        on = distinct_arg.args.get("on")
        if on is None:
            return None
        keys = []
        for key_expr in on.expressions:
            keys.append(self._convert_expression(key_expr))
        return keys

    def _get_alias(self, expr: exp.Expression) -> str:
        """Get alias for expression.

        Args:
            expr: sqlglot expression

        Returns:
            Alias name
        """
        if isinstance(expr, exp.Alias):
            return expr.alias
        if isinstance(expr, exp.Column):
            return expr.name
        return str(expr)

    def _build_order_by_clause(
        self, select: exp.Query, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build Sort node from ORDER BY clause.

        Args:
            select: sqlglot Select or set-operation node
            input_plan: Input logical plan

        Returns:
            Logical plan with Sort node (or original if no ORDER BY)
        """
        from ..plan.logical import Sort

        order = select.args.get("order")
        if not order:
            return input_plan

        sort_keys = []
        ascending = []
        nulls_order = []

        for ordered in order.expressions:
            expr = self._convert_expression(ordered.this)
            sort_keys.append(expr)

            is_desc = ordered.args.get("desc", False)
            ascending.append(not is_desc)

            nulls = ordered.args.get("nulls_first")
            if nulls is not None:
                nulls_order.append("FIRST" if nulls else "LAST")
            else:
                nulls_order.append(None)

        # ORDER BY becomes a Sort over its input, built from the converted sort
        # keys with their per-key ascending and NULLS FIRST/LAST directions.
        return Sort.create(
            input=input_plan,
            sort_keys=sort_keys,
            ascending=ascending,
            nulls_order=nulls_order,
        )

    def _build_limit_clause(
        self, select: exp.Query, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build limit node from LIMIT clause.

        Args:
            select: sqlglot Select or set-operation node
            input_plan: Input logical plan

        Returns:
            Logical plan with limit (or original if no LIMIT)
        """
        limit_expr = select.args.get("limit")
        offset_expr = select.args.get("offset")
        if not limit_expr and offset_expr is None:
            return input_plan

        limit_value = self._limit_count(limit_expr)
        offset_value = 0
        if offset_expr is not None:
            offset_value = int(offset_expr.expression.this)
        # LIMIT/OFFSET caps and skips rows, modelled as a Limit node built from
        # the parsed row count and offset of the sqlglot Limit/Offset clauses.
        return Limit.create(input=input_plan, limit=limit_value, offset=offset_value)

    def _limit_count(self, limit_expr) -> Optional[int]:
        """Return the row cap from a LIMIT or FETCH clause (None for OFFSET-only)."""
        if limit_expr is None:
            return None
        if isinstance(limit_expr, exp.Fetch):
            return self._fetch_count(limit_expr)
        return int(limit_expr.expression.this)

    def _fetch_count(self, fetch: exp.Fetch) -> int:
        """Return the row count of FETCH FIRST n ROWS ONLY.

        WITH TIES and PERCENT change the meaning of the count and are not
        supported, so they fail fast rather than be read as a plain row cap.
        """
        self._reject_fetch_options(fetch.args.get("limit_options"))
        return int(fetch.args["count"].this)

    def _reject_fetch_options(self, options) -> None:
        """Reject FETCH FIRST modifiers the engine does not implement."""
        if options is None:
            return
        if options.args.get("with_ties"):
            raise UnsupportedSQLError("FETCH FIRST ... WITH TIES is not supported")
        if options.args.get("percent"):
            raise UnsupportedSQLError("FETCH FIRST ... PERCENT is not supported")

    def _convert_expression(self, expr: exp.Expression) -> Expression:
        """Convert sqlglot expression to our Expression.

        Args:
            expr: sqlglot expression

        Returns:
            Our Expression object
        """
        if isinstance(expr, exp.In):
            return self._convert_in_expression(expr)
        if isinstance(expr, exp.Between):
            return self._convert_between_expression(expr)
        if isinstance(expr, exp.Column):
            return self._convert_column(expr)
        if isinstance(expr, exp.Is):
            return self._convert_is_expression(expr)
        if isinstance(expr, exp.Literal):
            return self._convert_literal(expr)
        if isinstance(expr, exp.Boolean):
            # A TRUE/FALSE keyword maps to a boolean Literal, built directly
            # from the sqlglot Boolean node's value.
            return Literal.create(value=bool(expr.this), data_type=DataType.BOOLEAN)
        if isinstance(expr, exp.Null):
            # A NULL keyword maps to a null-typed Literal carrying no value,
            # built directly from the sqlglot Null node.
            return Literal.create(value=None, data_type=DataType.NULL)
        if isinstance(expr, exp.Tuple):
            return self._convert_tuple_expression(expr)
        if isinstance(expr, exp.Exists):
            return self._convert_exists_expression(expr, False)
        if isinstance(expr, exp.Subquery):
            return self._convert_subquery_expression(expr)
        if isinstance(expr, exp.ILike):
            return self._convert_ilike_predicate(expr)
        if isinstance(expr, exp.Like):
            return self._convert_like_predicate(expr)
        if isinstance(expr, exp.Cast):
            return self._convert_cast(expr)
        if isinstance(expr, exp.Binary):
            return self._convert_binary(expr)
        if isinstance(expr, exp.Unary):
            return self._convert_unary(expr)
        if isinstance(expr, exp.Alias):
            return self._convert_expression(expr.this)
        if isinstance(expr, exp.Star):
            # A bare * projection maps to a wildcard ColumnRef with no table
            # qualifier, built from the sqlglot Star node.
            return ColumnRef.create(table=None, column="*")
        if isinstance(expr, exp.Case):
            return self._convert_case_expression(expr)
        if isinstance(expr, exp.Extract):
            return self._convert_extract(expr)
        if isinstance(expr, (exp.TimestampTrunc, exp.DateTrunc)):
            return self._convert_date_trunc(expr)
        if isinstance(expr, exp.Interval):
            return self._convert_interval(expr)
        if isinstance(expr, exp.Filter):
            return self._convert_filter_aggregate(expr)
        if isinstance(expr, exp.Window):
            return self._convert_window(expr)
        if isinstance(expr, exp.WithinGroup):
            return self._convert_within_group(expr)
        if isinstance(expr, exp.Grouping):
            return self._convert_grouping(expr)
        if self._is_aggregate_function(expr):
            return self._convert_aggregate_function(expr)
        if isinstance(expr, exp.Trim):
            return self._convert_trim(expr)
        if isinstance(expr, (exp.Anonymous, exp.Func, exp.Upper)):
            return self._convert_function_call(expr)

        raise ValueError(f"Unsupported expression type: {type(expr)}")

    def _convert_trim(self, trim: exp.Trim) -> FunctionCall:
        """Convert TRIM; an explicit LEADING/TRAILING/BOTH position is unsupported.

        The position keyword is a bare string slot the generic function-arg
        collector silently skips, which would turn TRIM(LEADING 'x' FROM col)
        into a both-sides trim. Reject it rather than drop it.
        """
        if trim.args.get("position"):
            raise UnsupportedSQLError(
                "TRIM with LEADING/TRAILING/BOTH is not supported"
            )
        return self._convert_function_call(trim)

    def _convert_in_expression(self, expr: exp.In) -> Expression:
        """Convert IN list (or IN subquery) to expression node.

        Only a value list and a subquery are handled; other forms such as
        ``IN UNNEST(...)`` (the ``field``/``unnest`` args) are rejected rather
        than silently treated as an empty or partial list.
        """
        self._reject_unsupported_args(expr, self.SUPPORTED_IN_ARGS, "IN")
        value_expr = self._convert_expression(expr.this)
        query = expr.args.get("query")
        if query is not None:
            subquery_plan = self._extract_subquery_plan(query)
            # ``x IN (SELECT ...)`` is a non-negated InSubquery, built from the
            # converted left value and the subquery's logical plan.
            return InSubquery.create(
                value=value_expr,
                subquery=subquery_plan,
                negated=False,
            )
        options: List[Expression] = []
        for option in expr.expressions:
            converted = self._convert_expression(option)
            options.append(converted)
        # ``x IN (a, b, ...)`` is an InList, built from the converted left value
        # and the converted list of candidate value expressions.
        return InList.create(value=value_expr, options=options)

    def _convert_between_expression(self, expr: exp.Between) -> Expression:
        """Convert BETWEEN to expression node.

        BETWEEN SYMMETRIC (bounds in either order) carries a ``symmetric`` flag
        with different semantics; it is rejected so it is not silently planned
        as a plain BETWEEN.
        """
        self._reject_unsupported_args(expr, self.SUPPORTED_BETWEEN_ARGS, "BETWEEN")
        value_expr = self._convert_expression(expr.this)
        low_expr = self._convert_expression(expr.args["low"])
        high_expr = self._convert_expression(expr.args["high"])
        # BETWEEN maps to a BetweenExpression, built from the converted value
        # and its converted low and high bound expressions.
        return BetweenExpression.create(
            value=value_expr, lower=low_expr, upper=high_expr
        )

    def _convert_is_expression(self, expr: exp.Is) -> Expression:
        """Convert IS and IS NOT comparisons into unary operations."""
        operand = self._convert_expression(expr.this)
        comparison = expr.expression
        if isinstance(comparison, exp.Null):
            # ``x IS NULL`` maps to an IS_NULL UnaryOp, built from the converted
            # left operand of the sqlglot Is node.
            return UnaryOp.create(op=UnaryOpType.IS_NULL, operand=operand)
        if isinstance(comparison, exp.Not) and isinstance(comparison.this, exp.Null):
            # ``x IS NOT NULL`` maps to an IS_NOT_NULL UnaryOp, built from the
            # converted left operand of the sqlglot Is node.
            return UnaryOp.create(op=UnaryOpType.IS_NOT_NULL, operand=operand)
        raise ValueError("IS comparison supports only NULL and NOT NULL")

    def _convert_case_expression(self, expr: exp.Case) -> Expression:
        """Convert a CASE expression to a searched CaseExpr.

        A simple CASE (CASE operand WHEN value ...) stores its operand under
        ``this`` and compares it to each WHEN value; it lowers to the exactly
        equivalent searched CASE with an ``operand = value`` condition per
        branch (plain equality, so a NULL operand matches no branch in both
        forms). A searched CASE converts its WHEN conditions as written.
        """
        operand = self._convert_case_operand(expr)
        when_clauses = []
        for if_clause in expr.args.get("ifs") or []:
            when_clauses.append(self._convert_when_clause(if_clause, operand))
        else_expr = None
        default_expr = expr.args.get("default")
        if default_expr is not None:
            else_expr = self._convert_expression(default_expr)
        # The searched CaseExpr, built from the converted (condition, result)
        # WHEN pairs and the optional ELSE result.
        return CaseExpr.create(when_clauses=when_clauses, else_result=else_expr)

    def _convert_case_operand(self, expr: exp.Case) -> Optional[Expression]:
        """The converted simple-CASE operand, or None for a searched CASE."""
        operand = expr.args.get("this")
        if operand is None:
            return None
        converted = self._convert_expression(operand)
        self._reject_case_operand_functions(converted)
        return converted

    def _reject_case_operand_functions(self, operand: Expression) -> None:
        """Reject a simple-CASE operand containing a function call.

        The lowering duplicates the operand into every WHEN condition; a
        volatile function (e.g. random()) would then re-evaluate per branch
        and could match the wrong arm. The engine carries no volatility
        metadata, so any function-bearing operand fails fast; columns,
        literals, and operator expressions are deterministic by construction.
        """
        from ..plan.expressions import FunctionCall, expression_children

        if isinstance(operand, FunctionCall):
            raise UnsupportedSQLError(
                "simple CASE with a function-call operand is not supported "
                "(the operand is duplicated into every WHEN condition)"
            )
        for child in expression_children(operand):
            self._reject_case_operand_functions(child)

    def _convert_when_clause(self, if_clause, operand: Optional[Expression]):
        """One (condition, result) WHEN pair of a CASE expression."""
        condition = self._convert_expression(if_clause.this)
        if operand is not None:
            # The searched-CASE equivalent of a simple-CASE branch: compare the
            # operand to this WHEN value with plain equality.
            condition = BinaryOp.create(
                op=BinaryOpType.EQ, left=operand, right=condition
            )
        result_expr = if_clause.args.get("true")
        if result_expr is not None:
            result = self._convert_expression(result_expr)
        else:
            # A WHEN with no THEN value yields SQL NULL, so build a
            # null-typed Literal as this branch's result.
            result = Literal.create(value=None, data_type=DataType.NULL)
        return (condition, result)

    def _convert_filter_aggregate(self, filter_expr: exp.Filter) -> FunctionCall:
        """Convert ``AGG(...) FILTER (WHERE p)`` into a CASE-guarded aggregate.

        Sources need not support the FILTER syntax: ``agg(x) FILTER (WHERE p)``
        equals ``agg(CASE WHEN p THEN x END)`` (``THEN 1`` for ``COUNT(*)``),
        which pushes down portably and keeps the aggregate node intact.
        """
        aggregate = self._convert_expression(filter_expr.this)
        predicate = self._convert_expression(filter_expr.expression.this)
        return self._guard_aggregate_args(aggregate, predicate)

    def _guard_aggregate_args(
        self, aggregate: FunctionCall, predicate: Expression
    ) -> FunctionCall:
        """Rebuild an aggregate with each argument gated by a filter predicate."""
        guarded_args: List[Expression] = []
        for arg in aggregate.args:
            guarded_args.append(self._guard_one_arg(arg, predicate))
        return aggregate.model_copy(update={"args": guarded_args})

    def _guard_one_arg(self, arg: Expression, predicate: Expression) -> CaseExpr:
        """Build ``CASE WHEN predicate THEN arg END``, using 1 for COUNT(*)."""
        result = arg
        if isinstance(arg, ColumnRef) and arg.column == "*":
            # COUNT(*) has no column to guard, so substitute the integer literal
            # 1 as the value counted inside the FILTER CASE guard.
            result = Literal.create(value=1, data_type=DataType.INTEGER)
        # ``agg(x) FILTER (WHERE p)`` becomes ``agg(CASE WHEN p THEN x END)``;
        # build that CASE guard with no ELSE so non-matching rows drop out.
        return CaseExpr.create(when_clauses=[(predicate, result)], else_result=None)

    def _convert_extract(self, extract: exp.Extract) -> Extract:
        """Convert EXTRACT(field FROM source), keeping the field keyword.

        sqlglot stores the field as a ``Var`` node (``YEAR``, ``MONTH`` ...) in
        ``this`` and the source date/time expression in ``expression``.
        """
        field = extract.this.name
        source = self._convert_expression(extract.expression)
        # EXTRACT(field FROM source) maps to an Extract node, built from the
        # field keyword and the converted source date/time expression.
        return Extract.create(field=field, source=source)

    def _convert_date_trunc(self, trunc: exp.Func) -> FunctionCall:
        """Convert DATE_TRUNC into a portable two-argument function call.

        The Postgres dialect normalises ``DATE_TRUNC('month', col)`` to a
        ``TimestampTrunc`` node holding the unit as a ``Var`` under ``unit`` and
        the source under ``this``; we re-materialise the standard call shape.
        """
        unit = trunc.args["unit"].name
        source = self._convert_expression(trunc.this)
        # The truncation unit ('month', ...) becomes a string Literal argument,
        # built from the unit keyword sqlglot stored on the trunc node.
        unit_literal = Literal.create(value=unit, data_type=DataType.VARCHAR)
        # Re-materialise the standard DATE_TRUNC(unit, source) call shape as a
        # FunctionCall from the unit literal and the converted source.
        return FunctionCall.create(
            function_name="DATE_TRUNC", args=[unit_literal, source]
        )

    def _convert_interval(self, interval: exp.Interval) -> Interval:
        """Convert an INTERVAL literal, preserving its magnitude and unit.

        sqlglot keeps the magnitude literal in ``this`` and the optional unit
        keyword as a ``Var`` under ``unit`` (absent for multi-field intervals).
        """
        value = interval.this.name
        unit_node = interval.args.get("unit")
        unit = unit_node.name if unit_node is not None else None
        # An INTERVAL literal maps to an Interval node, built from the magnitude
        # sqlglot kept in ``this`` and the optional unit keyword.
        return Interval.create(value=value, unit=unit)

    def _convert_column(self, col: exp.Column) -> ColumnRef:
        """Convert sqlglot Column to ColumnRef."""
        table = col.table if col.table else None
        # A column reference maps to a ColumnRef, built fresh from the sqlglot
        # Column node with the data type left unset for the binder to resolve.
        return ColumnRef.create(table=table, column=col.name)

    def _convert_literal(self, lit: exp.Literal) -> Literal:
        """Convert sqlglot Literal to our Literal.

        A quoted literal is always a string, even when it looks numeric
        (``'2'`` is the text ``2``, not the integer ``2``); honoring
        ``is_string`` keeps it from being coerced and mistyped in remote SQL.
        """
        value_str = lit.this
        if lit.is_string:
            # A quoted literal is always text even when it looks numeric, so map
            # it to a VARCHAR Literal built from the raw sqlglot literal string.
            return Literal.create(value=value_str, data_type=DataType.VARCHAR)
        data_type = self._infer_literal_type(value_str)
        converted_value = self._convert_literal_value(value_str, data_type)
        # An unquoted literal maps to a typed Literal, built from the value
        # coerced to the Python type of its inferred numeric/boolean data type.
        return Literal.create(value=converted_value, data_type=data_type)

    def _infer_literal_type(self, value: str) -> DataType:
        """Infer data type from literal value."""
        if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
            return DataType.INTEGER
        if value.replace(".", "", 1).replace("-", "", 1).isdigit():
            return DataType.DOUBLE
        if value.lower() in ("true", "false"):
            return DataType.BOOLEAN
        return DataType.VARCHAR

    def _convert_literal_value(self, value_str: str, data_type: DataType):
        """Convert literal string to proper Python type."""
        if data_type == DataType.INTEGER:
            return int(value_str)
        if data_type == DataType.DOUBLE:
            return float(value_str)
        if data_type == DataType.BOOLEAN:
            return value_str.lower() == "true"
        return value_str

    def _convert_tuple_expression(self, tuple_expr: exp.Tuple) -> Expression:
        """Convert a row value constructor such as ``(a, b)``."""
        items = []
        for item in tuple_expr.expressions:
            items.append(self._convert_expression(item))
        # A row value constructor ``(a, b)`` maps to a TupleExpression, built
        # from the converted item expressions of the sqlglot Tuple node.
        return TupleExpression.create(items=tuple(items))

    def _convert_like_predicate(self, like: exp.Like) -> Expression:
        """Convert LIKE, wrapping in NOT when sqlglot marks it negated.

        sqlglot represents ``NOT LIKE`` as a ``Like`` node carrying a
        ``negate`` flag rather than a wrapping ``Not`` node, so the flag
        must be honored here to preserve the negation.
        """
        quantified = self._convert_quantified_like(like)
        if quantified is not None:
            return quantified
        left = self._convert_expression(like.this)
        right = self._convert_expression(like.expression)
        # LIKE maps to a LIKE BinaryOp, built from the converted left value and
        # the converted pattern expression.
        comparison = BinaryOp.create(op=BinaryOpType.LIKE, left=left, right=right)
        if like.args.get("negate"):
            # sqlglot folds NOT LIKE into a negate flag, so wrap the comparison
            # in a NOT UnaryOp here to preserve the negation.
            return UnaryOp.create(op=UnaryOpType.NOT, operand=comparison)
        return comparison

    def _convert_ilike_predicate(self, ilike: exp.ILike) -> Expression:
        """Convert ILIKE / NOT ILIKE into a case-insensitive comparison.

        sqlglot models ``NOT ILIKE`` as an ``ILike`` node carrying a
        ``negate`` flag, so the negation is honored here.
        """
        left = self._convert_expression(ilike.this)
        right = self._convert_expression(ilike.expression)
        # ILIKE maps to a case-insensitive ILIKE BinaryOp, built from the
        # converted left value and the converted pattern expression.
        comparison = BinaryOp.create(op=BinaryOpType.ILIKE, left=left, right=right)
        if ilike.args.get("negate"):
            # sqlglot folds NOT ILIKE into a negate flag, so wrap the comparison
            # in a NOT UnaryOp here to preserve the negation.
            return UnaryOp.create(op=UnaryOpType.NOT, operand=comparison)
        return comparison

    def _convert_cast(self, cast: exp.Cast) -> Cast:
        """Convert CAST(expr AS type), preserving the target type text.

        A safe cast (TRY_CAST, ``safe`` flag) returns NULL instead of raising on
        a bad value, and a FORMAT clause changes parsing; both are rejected so
        the cast is not silently planned as a plain, error-raising CAST.
        """
        self._reject_unsupported_args(cast, self.SUPPORTED_CAST_ARGS, "CAST")
        inner = self._convert_expression(cast.this)
        target_type = cast.args["to"].sql(dialect=self.dialect)
        # CAST(expr AS type) maps to a Cast node, built from the converted inner
        # expression and the target type rendered as engine-dialect SQL text.
        return Cast.create(expr=inner, target_type=target_type)

    def _convert_quantified_like(self, like: exp.Like) -> Optional[Expression]:
        """Convert ``LIKE ANY/ALL (subquery)`` to a quantified comparison."""
        quantifiers = [
            (exp.Any, Quantifier.ANY),
            (exp.All, Quantifier.ALL),
        ]
        for node_type, quantifier in quantifiers:
            if isinstance(like.expression, node_type):
                left = self._convert_expression(like.this)
                subquery_plan = self._extract_subquery_plan(like.expression.this)
                # ``x LIKE ANY/ALL (subquery)`` maps to a QuantifiedComparison,
                # built from the left value, quantifier and the subquery plan.
                return QuantifiedComparison.create(
                    operator=BinaryOpType.LIKE,
                    quantifier=quantifier,
                    left=left,
                    subquery=subquery_plan,
                )
        return None

    def _convert_binary(self, binary: exp.Binary) -> BinaryOp:
        """Convert sqlglot binary operation."""
        if isinstance(binary.right, exp.Any):
            return self._build_quantified_comparison(binary, Quantifier.ANY)
        if hasattr(exp, "Some") and isinstance(binary.right, exp.Some):
            return self._build_quantified_comparison(binary, Quantifier.SOME)
        if isinstance(binary.right, exp.All):
            return self._build_quantified_comparison(binary, Quantifier.ALL)
        left = self._convert_expression(binary.left)
        right = self._convert_expression(binary.right)
        op = self._map_binary_op(binary)
        # A binary operator (arithmetic/comparison/logical) maps to a BinaryOp,
        # built from the converted left and right operands and the mapped op.
        return BinaryOp.create(op=op, left=left, right=right)

    def _map_binary_op(self, binary: exp.Binary) -> BinaryOpType:
        """Map sqlglot binary op to our BinaryOpType, failing on unknowns."""
        type_name = type(binary).__name__
        mapping = {
            "Add": BinaryOpType.ADD,
            "Sub": BinaryOpType.SUBTRACT,
            "Mul": BinaryOpType.MULTIPLY,
            "Div": BinaryOpType.DIVIDE,
            "Mod": BinaryOpType.MODULO,
            "EQ": BinaryOpType.EQ,
            "NEQ": BinaryOpType.NEQ,
            "LT": BinaryOpType.LT,
            "LTE": BinaryOpType.LTE,
            "GT": BinaryOpType.GT,
            "GTE": BinaryOpType.GTE,
            "And": BinaryOpType.AND,
            "Or": BinaryOpType.OR,
            "Like": BinaryOpType.LIKE,
            "ILike": BinaryOpType.ILIKE,
            "DPipe": BinaryOpType.CONCAT,
            "NullSafeEQ": BinaryOpType.NULL_SAFE_EQ,
            "NullSafeNEQ": BinaryOpType.NULL_SAFE_NEQ,
            "RegexpLike": BinaryOpType.REGEX_MATCH,
            "RegexpILike": BinaryOpType.REGEX_IMATCH,
        }
        if type_name not in mapping:
            raise ValueError(f"Unsupported binary operator: {type_name}")
        return mapping[type_name]

    def _convert_unary(self, unary: exp.Unary) -> UnaryOp:
        """Convert sqlglot unary operation."""
        if isinstance(unary, exp.Paren):
            return self._convert_expression(unary.this)

        if isinstance(unary, exp.Not):
            if isinstance(unary.this, exp.Exists):
                return self._convert_exists_expression(unary.this, True)
            if isinstance(unary.this, exp.In):
                query = unary.this.args.get("query")
                if query is not None:
                    value_expr = self._convert_expression(unary.this.this)
                    subquery_plan = self._extract_subquery_plan(query)
                    # ``x NOT IN (SELECT ...)`` is a negated InSubquery, built
                    # from the converted left value and the subquery's plan.
                    return InSubquery.create(
                        value=value_expr,
                        subquery=subquery_plan,
                        negated=True,
                    )

        operand = self._convert_expression(unary.this)
        op = self._map_unary_op(unary)
        # A unary operator (NOT / negation) maps to a UnaryOp, built from the
        # converted operand and the mapped unary op type.
        return UnaryOp.create(op=op, operand=operand)

    def _map_unary_op(self, unary: exp.Unary) -> UnaryOpType:
        """Map sqlglot unary op to our UnaryOpType, failing on unknowns."""
        type_name = type(unary).__name__
        mapping = {
            "Not": UnaryOpType.NOT,
            "Neg": UnaryOpType.NEGATE,
        }
        if type_name not in mapping:
            raise ValueError(f"Unsupported unary operator: {type_name}")
        return mapping[type_name]

    def _convert_exists_expression(
        self,
        exists_expr: exp.Exists,
        negated: bool,
    ) -> ExistsExpression:
        """Convert EXISTS/NOT EXISTS expression."""
        subquery_plan = self._extract_subquery_plan(exists_expr.this)
        # EXISTS / NOT EXISTS maps to an ExistsExpression, built from the
        # subquery's logical plan and the negation flag.
        return ExistsExpression.create(subquery=subquery_plan, negated=negated)

    def _convert_subquery_expression(
        self,
        subquery: exp.Subquery,
    ) -> SubqueryExpression:
        """Convert scalar subquery expression."""
        subquery_plan = self._extract_subquery_plan(subquery)
        # A scalar subquery ``(SELECT ...)`` used as a value maps to a
        # SubqueryExpression, built from the subquery's logical plan.
        return SubqueryExpression.create(subquery=subquery_plan)

    def _build_quantified_comparison(
        self,
        binary: exp.Binary,
        quantifier: Quantifier,
    ) -> QuantifiedComparison:
        """Convert quantified comparison such as > ANY or = ALL."""
        left_expr = self._convert_expression(binary.left)
        subquery_plan = self._extract_subquery_plan(binary.right.this)
        operator = self._map_binary_op(binary)
        # ``x op ANY/SOME/ALL (subquery)`` maps to a QuantifiedComparison, built
        # from the left value, the mapped operator, quantifier and subquery plan.
        return QuantifiedComparison.create(
            operator=operator,
            quantifier=quantifier,
            left=left_expr,
            subquery=subquery_plan,
        )

    def _extract_subquery_plan(
        self,
        subquery_expr: exp.Expression,
    ) -> "LogicalPlanNode":
        """Extract logical plan from sqlglot subquery expression."""
        if isinstance(subquery_expr, exp.Subquery):
            return self.ast_to_logical_plan(subquery_expr.this)
        if isinstance(subquery_expr, exp.Select):
            return self.ast_to_logical_plan(subquery_expr)
        raise ValueError(f"Unsupported subquery expression: {type(subquery_expr)}")

    def _convert_aggregate_function(self, func: exp.AggFunc) -> FunctionCall:
        """Convert sqlglot aggregate function.

        Args:
            func: sqlglot AggFunc node

        Returns:
            FunctionCall expression
        """
        self._reject_unsupported_args(func, self.SUPPORTED_AGG_ARGS, "aggregate")
        func_name = self._aggregate_sql_name(func)
        distinct = bool(func.args.get("distinct"))
        if isinstance(func.this, exp.Distinct):
            distinct = True
        args = self._extract_function_args(func, distinct)
        # An aggregate call (SUM/COUNT/...) maps to a FunctionCall marked
        # aggregate, built from its SQL name, converted args and DISTINCT flag.
        return FunctionCall.create(
            function_name=func_name,
            args=args,
            is_aggregate=True,
            distinct=distinct,
        )

    def _aggregate_sql_name(self, func: exp.AggFunc) -> str:
        """Return an aggregate's real SQL name in the engine dialect.

        ``type(func).__name__`` is a sqlglot class name (e.g. VariancePop),
        which is not a valid SQL function in any source. Rendering the node in
        the engine dialect yields the correct token (e.g. VAR_POP); the function
        head is everything before the first argument parenthesis.
        """
        rendered = func.sql(dialect=self.dialect)
        return rendered.split("(", 1)[0].strip().upper()

    def _convert_grouping(self, expr: exp.Grouping) -> FunctionCall:
        """Convert GROUPING(a, ...) into an aggregate-context FunctionCall.

        GROUPING reports whether each argument was rolled up in the current
        grouping set; it is only valid with GROUP BY, so it is marked aggregate.
        """
        # GROUPING(a, ...) maps to an aggregate-context FunctionCall, built from
        # its converted argument expressions and marked as an aggregate.
        return FunctionCall.create(
            function_name="GROUPING",
            args=self._convert_group_expressions(expr.expressions),
            is_aggregate=True,
        )

    def _convert_within_group(self, expr: exp.WithinGroup) -> FunctionCall:
        """Convert an ordered-set aggregate: f(args) WITHIN GROUP (ORDER BY key).

        Covers PERCENTILE_CONT/PERCENTILE_DISC/MODE (and MEDIAN, which the
        preprocessor re-renders to PERCENTILE_CONT(0.5) WITHIN GROUP).
        """
        aggregate = expr.this
        key, descending = self._within_group_order(expr.expression)
        # An ordered-set aggregate ``f(args) WITHIN GROUP (ORDER BY key)`` maps
        # to a FunctionCall carrying the sort key from the WITHIN GROUP order.
        return FunctionCall.create(
            function_name=self._aggregate_sql_name(aggregate),
            args=self._ordered_set_direct_args(aggregate),
            is_aggregate=True,
            within_group_key=key,
            within_group_desc=descending,
        )

    def _ordered_set_direct_args(self, aggregate: exp.AggFunc) -> List[Expression]:
        """Convert the direct arguments of an ordered-set aggregate.

        PERCENTILE_CONT/DISC carry a fraction in ``this``; MODE carries nothing.
        """
        if aggregate.this is None:
            return []
        return [self._convert_expression(aggregate.this)]

    def _within_group_order(self, order: exp.Order) -> Tuple[Expression, bool]:
        """Return the (key, descending) of a single-column WITHIN GROUP order.

        Only one ORDER BY key is supported; additional keys would be silently
        dropped, so they fail fast instead.
        """
        if len(order.expressions) != 1:
            raise UnsupportedSQLError("WITHIN GROUP supports a single ORDER BY key")
        ordered = order.expressions[0]
        key = self._convert_expression(ordered.this)
        descending = bool(ordered.args.get("desc"))
        return key, descending

    def _extract_function_args(
        self,
        func: exp.AggFunc,
        distinct: bool,
    ) -> List[Expression]:
        """Extract an aggregate's arguments, including a STRING_AGG separator.

        The separator (``STRING_AGG(x, ',')``) is a real second argument;
        dropping it would silently change the rendered aggregate. It is
        appended after the primary argument so the call round-trips faithfully.
        """
        args = self._extract_primary_args(func)
        separator = func.args.get("separator")
        if separator is not None:
            args.append(self._convert_expression(separator))
        return args

    def _extract_primary_args(self, func: exp.AggFunc) -> List[Expression]:
        """Convert the main argument(s): a DISTINCT list, one value, or * for COUNT."""
        value = getattr(func, "this", None)
        if isinstance(value, exp.Distinct):
            return self._convert_distinct_args(value)
        if value is not None:
            return [self._convert_expression(value)]
        if isinstance(func, exp.Count):
            # COUNT(*) has no argument node, so represent its ``*`` as a
            # wildcard ColumnRef with no table qualifier.
            return [ColumnRef.create(table=None, column="*")]
        return []

    def _convert_distinct_args(self, value: exp.Distinct) -> List[Expression]:
        """Convert the expressions inside a DISTINCT aggregate argument."""
        args = []
        for child in value.expressions or []:
            args.append(self._convert_expression(child))
        return args

    def _convert_function_call(self, func: exp.Expression) -> FunctionCall:
        """Convert generic function expressions, preserving every argument."""
        name = func.sql_name().upper()
        args: List[Expression] = []
        self._collect_function_args(func, args)
        # A generic scalar function call maps to a non-aggregate FunctionCall,
        # built from its SQL name and every collected argument expression.
        return FunctionCall.create(function_name=name, args=args, is_aggregate=False)

    def _convert_window(self, win: exp.Window) -> WindowExpr:
        """Convert a sqlglot Window (``func OVER (...)``) into a WindowExpr.

        Only PARTITION BY, ORDER BY and the frame spec are consumed; any other
        window clause is rejected rather than silently dropped from the frame.
        """
        self._reject_unsupported_args(win, self.SUPPORTED_WINDOW_ARGS, "window")
        function = self._convert_expression(win.this)
        partition_by = self._convert_partition_by(win)
        order_keys, ascending, nulls = self._convert_window_order(win)
        # ``func OVER (...)`` maps to a WindowExpr, built from the converted
        # function, PARTITION BY, ORDER BY keys/direction and frame spec.
        return WindowExpr.create(
            function=function,
            partition_by=partition_by,
            order_keys=order_keys,
            order_ascending=ascending,
            order_nulls=nulls,
            frame=self._render_window_frame(win),
        )

    def _convert_partition_by(self, win: exp.Window) -> List[Expression]:
        """Convert a window's PARTITION BY expressions."""
        result: List[Expression] = []
        for part in win.args.get("partition_by") or []:
            result.append(self._convert_expression(part))
        return result

    def _convert_window_order(self, win: exp.Window):
        """Convert a window's ORDER BY into (keys, ascending, nulls) lists."""
        keys: List[Expression] = []
        ascending: List[bool] = []
        nulls: List[Optional[str]] = []
        order = win.args.get("order")
        if order is None:
            return keys, ascending, nulls
        for ordered in order.expressions:
            keys.append(self._convert_expression(ordered.this))
            ascending.append(not ordered.args.get("desc", False))
            nulls.append(self._window_nulls(ordered))
        return keys, ascending, nulls

    def _window_nulls(self, ordered: exp.Ordered) -> Optional[str]:
        """Map an Ordered node's NULLS placement to 'FIRST'/'LAST'/None."""
        nulls = ordered.args.get("nulls_first")
        if nulls is None:
            return None
        return "FIRST" if nulls else "LAST"

    def _render_window_frame(self, win: exp.Window) -> Optional[str]:
        """Render the frame spec (``ROWS/RANGE ...``) verbatim, or None."""
        spec = win.args.get("spec")
        if spec is None:
            return None
        return spec.sql(dialect=self.dialect)

    def _collect_function_args(
        self, func: exp.Expression, args: List[Expression]
    ) -> None:
        """Gather a function's argument expressions in declaration order.

        Typed functions store arguments under named keys (e.g. NULLIF uses
        ``this`` and ``expression``), so iterate the node's ``arg_types``. An
        Anonymous call keeps the function name in ``this``, so only its
        ``expressions`` list holds real arguments.
        """
        if isinstance(func, exp.Anonymous):
            for child in func.expressions or []:
                args.append(self._convert_expression(child))
            return
        for key in func.arg_types:
            self._append_function_arg(func.args.get(key), args)

    def _append_function_arg(self, value, args: List[Expression]) -> None:
        """Convert one argument slot (a node, a list of nodes, or an absent slot).

        An absent slot (None) and a boolean modifier flag (e.g. CONCAT's ``safe``)
        are not positional arguments and are skipped. Any other non-expression
        slot - a string/keyword modifier such as TRIM's position - is NOT silently
        dropped: it raises, because dropping it loses semantics the generic
        FunctionCall cannot carry, so the function must be handled explicitly
        (as _convert_trim does) instead.
        """
        if value is None or isinstance(value, bool):
            return
        if isinstance(value, exp.Expression):
            args.append(self._convert_expression(value))
            return
        if isinstance(value, list):
            for item in value:
                self._append_function_arg(item, args)
            return
        raise UnsupportedSQLError(
            f"Unsupported function argument slot of type {type(value).__name__}: "
            f"{value!r}"
        )

    def _rewrite_having_predicate(
        self, predicate: Expression, aggregate: Aggregate
    ) -> Expression:
        """Rewrite HAVING predicate to reference output columns.

        Args:
            predicate: HAVING predicate expression
            aggregate: Aggregate node

        Returns:
            Rewritten predicate with column references
        """
        agg_map = self._build_aggregate_mapping(aggregate)
        return self._rewrite_expression(predicate, agg_map)

    def _build_aggregate_mapping(self, aggregate: Aggregate) -> Dict[str, str]:
        """Build mapping from aggregate expressions to output names.

        Args:
            aggregate: Aggregate node

        Returns:
            Dictionary mapping aggregate expression SQL to column name
        """
        mapping = {}

        for i in range(len(aggregate.aggregates)):
            agg_expr = aggregate.aggregates[i]
            output_name = aggregate.output_names[i]

            if isinstance(agg_expr, FunctionCall) and agg_expr.is_aggregate:
                agg_key = self._expr_to_key(agg_expr)
                mapping[agg_key] = output_name

        return mapping

    def _expr_to_key(self, expr: Expression) -> str:
        """Convert expression to string key for matching.

        Args:
            expr: Expression to convert

        Returns:
            String key for this expression
        """
        if isinstance(expr, FunctionCall):
            args_keys = [self._expr_to_key(arg) for arg in expr.args]
            args_str = ",".join(args_keys)
            return f"{expr.function_name.upper()}({args_str})"
        if isinstance(expr, ColumnRef):
            return expr.column
        if isinstance(expr, Literal):
            return str(expr.value)
        return expr.to_sql()

    def _rewrite_expression(
        self, expr: Expression, agg_map: Dict[str, str]
    ) -> Expression:
        """Rewrite expression replacing aggregates with column refs.

        Args:
            expr: Expression to rewrite
            agg_map: Mapping from aggregate keys to column names

        Returns:
            Rewritten expression
        """
        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            return self._rewrite_aggregate_ref(expr, agg_map)
        return map_children(
            expr, lambda child: self._rewrite_expression(child, agg_map)
        )

    def _rewrite_aggregate_ref(
        self, expr: FunctionCall, agg_map: Dict[str, str]
    ) -> Expression:
        """Replace an aggregate call with its output column reference, if mapped.

        An aggregate not present in the mapping is returned unchanged (its
        arguments reference input columns, not the aggregate's output).
        """
        expr_key = self._expr_to_key(expr)
        if expr_key in agg_map:
            # A HAVING aggregate is replaced by a reference to the aggregate's
            # output column, built as a ColumnRef carrying the aggregate's type.
            return ColumnRef.create(
                table=None, column=agg_map[expr_key], data_type=expr.get_type()
            )
        return expr

    def parse_to_logical_plan(
        self,
        sql: str,
        catalog: Optional[Catalog] = None,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> LogicalPlanNode:
        """Parse SQL directly to logical plan.

        Args:
            sql: SQL query string

        Returns:
            Logical plan root node
        """
        return self.parse(sql)

    def __repr__(self) -> str:
        return f"Parser(dialect={self.dialect})"
