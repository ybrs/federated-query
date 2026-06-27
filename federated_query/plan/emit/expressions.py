"""Convert engine expression nodes to a sqlglot AST.

This is the single expression renderer for the whole engine. ``to_sql`` on every
expression node delegates here, and the remote/merge SQL builders lower their
predicates through here too, so operator/quoting/function rendering lives in one
place. A node type with no mapping RAISES rather than emitting something wrong.
"""

from sqlglot import exp

from .. import expressions as E

# Allowlist: engine binary operator -> sqlglot node builder. A missing entry
# raises in visit_binary_op (never fall through to a default operator).
_BINARY_BUILDERS = {
    E.BinaryOpType.ADD: lambda l, r: exp.Add(this=l, expression=r),
    E.BinaryOpType.SUBTRACT: lambda l, r: exp.Sub(this=l, expression=r),
    E.BinaryOpType.MULTIPLY: lambda l, r: exp.Mul(this=l, expression=r),
    E.BinaryOpType.DIVIDE: lambda l, r: exp.Div(
        this=l, expression=r, typed=True, safe=False
    ),
    E.BinaryOpType.MODULO: lambda l, r: exp.Mod(this=l, expression=r),
    E.BinaryOpType.EQ: lambda l, r: exp.EQ(this=l, expression=r),
    E.BinaryOpType.NEQ: lambda l, r: exp.NEQ(this=l, expression=r),
    E.BinaryOpType.LT: lambda l, r: exp.LT(this=l, expression=r),
    E.BinaryOpType.LTE: lambda l, r: exp.LTE(this=l, expression=r),
    E.BinaryOpType.GT: lambda l, r: exp.GT(this=l, expression=r),
    E.BinaryOpType.GTE: lambda l, r: exp.GTE(this=l, expression=r),
    E.BinaryOpType.NULL_SAFE_EQ: lambda l, r: exp.NullSafeEQ(this=l, expression=r),
    E.BinaryOpType.NULL_SAFE_NEQ: lambda l, r: exp.NullSafeNEQ(this=l, expression=r),
    E.BinaryOpType.AND: lambda l, r: exp.And(this=l, expression=r),
    E.BinaryOpType.OR: lambda l, r: exp.Or(this=l, expression=r),
    E.BinaryOpType.CONCAT: lambda l, r: exp.DPipe(this=l, expression=r),
    E.BinaryOpType.LIKE: lambda l, r: exp.Like(this=l, expression=r),
    E.BinaryOpType.ILIKE: lambda l, r: exp.ILike(this=l, expression=r),
    E.BinaryOpType.REGEX_MATCH: lambda l, r: exp.RegexpLike(this=l, expression=r),
    E.BinaryOpType.REGEX_IMATCH: lambda l, r: exp.RegexpILike(this=l, expression=r),
}

# Allowlist: engine unary operator -> sqlglot node builder.
_UNARY_BUILDERS = {
    E.UnaryOpType.NOT: lambda o: exp.Not(this=o),
    E.UnaryOpType.NEGATE: lambda o: exp.Neg(this=o),
    E.UnaryOpType.IS_NULL: lambda o: exp.Is(this=o, expression=exp.Null()),
    E.UnaryOpType.IS_NOT_NULL: lambda o: exp.Not(
        this=exp.Is(this=o, expression=exp.Null())
    ),
}

# Engine data types whose literal renders as a numeric token.
_NUMERIC_TYPES = (
    E.DataType.INTEGER,
    E.DataType.BIGINT,
    E.DataType.FLOAT,
    E.DataType.DOUBLE,
    E.DataType.DECIMAL,
)

# Engine data types whose literal renders as a quoted string token.
_STRING_TYPES = (
    E.DataType.VARCHAR,
    E.DataType.TEXT,
    E.DataType.DATE,
    E.DataType.TIMESTAMP,
    E.DataType.INTERVAL,
)


def _bool_literal(value) -> exp.Expression:
    """Build a sqlglot boolean from a Python bool or a 'true'/'false' string."""
    if isinstance(value, str):
        return exp.Boolean(this=value.strip().lower() == "true")
    return exp.Boolean(this=bool(value))


def _literal_to_ast(literal: "E.Literal") -> exp.Expression:
    """Map a typed engine literal to its sqlglot literal node.

    NULL value and BOOLEAN are handled directly; numeric and string families
    come from the type tables above. An unmapped data type RAISES so a new
    DataType cannot silently render as raw text.
    """
    if literal.value is None or literal.data_type == E.DataType.NULL:
        return exp.Null()
    if literal.data_type == E.DataType.BOOLEAN:
        return _bool_literal(literal.value)
    if literal.data_type in _NUMERIC_TYPES:
        return exp.Literal.number(literal.value)
    if literal.data_type in _STRING_TYPES:
        return exp.Literal.string(str(literal.value))
    raise ValueError(f"no SQL literal mapping for data type {literal.data_type}")


class SqlglotEmitter(E.ExpressionVisitor):
    """Visitor that lowers an expression tree to a sqlglot AST.

    Holds the :class:`ColumnResolver` that decides how a column reference is
    rendered; everything else is path-independent.
    """

    def __init__(self, resolver):
        """Store the column resolver used for every ColumnRef in the tree."""
        self._resolver = resolver

    def _emit(self, expr: "E.Expression") -> exp.Expression:
        """Lower one child expression by dispatching it back through accept."""
        return expr.accept(self)

    def _emit_each(self, exprs) -> list:
        """Lower a list of child expressions, preserving order."""
        result = []
        for expr in exprs:
            result.append(self._emit(expr))
        return result

    def visit_column_ref(self, expr):
        """Defer column rendering to the resolver (source vs merge path)."""
        return self._resolver.resolve(expr.table, expr.column)

    def visit_literal(self, expr):
        """Render a typed literal via the literal type tables."""
        return _literal_to_ast(expr)

    def visit_binary_op(self, expr):
        """Render a binary operator from the allowlist; raise if unmapped.

        The result is parenthesized: sqlglot does not insert precedence parens,
        and predicate terms are AND-joined as strings by the SQL builders, so an
        un-parenthesized OR term would silently re-associate.
        """
        builder = _BINARY_BUILDERS.get(expr.op)
        if builder is None:
            raise ValueError(f"no SQL mapping for binary operator {expr.op}")
        return exp.Paren(this=builder(self._emit(expr.left), self._emit(expr.right)))

    def visit_unary_op(self, expr):
        """Render a unary operator from the allowlist; raise if unmapped."""
        builder = _UNARY_BUILDERS.get(expr.op)
        if builder is None:
            raise ValueError(f"no SQL mapping for unary operator {expr.op}")
        return exp.Paren(this=builder(self._emit(expr.operand)))

    def visit_function_call(self, expr):
        """Render a function/aggregate call, with DISTINCT and WITHIN GROUP."""
        call = self._build_call(
            expr.function_name, self._emit_each(expr.args), expr.distinct
        )
        if expr.within_group_key is None:
            return call
        return self._wrap_within_group(call, expr)

    def _build_call(self, name: str, args: list, distinct: bool) -> exp.Expression:
        """Build a verbatim ``name(args)`` / ``name(DISTINCT args)`` call.

        The function name is kept as written (a generic call). Dialect-specific
        function translation (e.g. PERCENTILE_CONT -> QUANTILE_CONT for DuckDB)
        happens at the single transpile boundary (to_source_sql), which parses
        this canonical Postgres form and renders the source dialect; a typed node
        here would mis-handle names like CONCAT (rendered as ``||``, which has
        different NULL semantics).
        """
        if distinct:
            return exp.Anonymous(
                this=name, expressions=[exp.Distinct(expressions=args)]
            )
        return exp.Anonymous(this=name, expressions=args)

    def _wrap_within_group(self, call: exp.Expression, expr) -> exp.Expression:
        """Wrap an ordered-set aggregate call in ``WITHIN GROUP (ORDER BY key)``.

        Direction is emitted only for DESC; an explicit ASC keyword inside
        WITHIN GROUP defeats sqlglot's ordered-set function transpilation.
        """
        key_ast = self._emit(expr.within_group_key)
        if expr.within_group_desc:
            ordered = exp.Ordered(this=key_ast, desc=True)
        else:
            ordered = exp.Ordered(this=key_ast)
        return exp.WithinGroup(this=call, expression=exp.Order(expressions=[ordered]))

    def visit_case_expr(self, expr):
        """Render a CASE as ``WHEN/THEN`` branches plus an optional ELSE."""
        ifs = []
        for condition, result in expr.when_clauses:
            ifs.append(exp.If(this=self._emit(condition), true=self._emit(result)))
        default = None
        if expr.else_result is not None:
            default = self._emit(expr.else_result)
        return exp.Case(ifs=ifs, default=default)

    def visit_in_list(self, expr):
        """Render ``(value IN (options...))``, parenthesized like a predicate."""
        in_node = exp.In(
            this=self._emit(expr.value), expressions=self._emit_each(expr.options)
        )
        return exp.Paren(this=in_node)

    def visit_between(self, expr):
        """Render ``(value BETWEEN lower AND upper)``, parenthesized."""
        between = exp.Between(
            this=self._emit(expr.value),
            low=self._emit(expr.lower),
            high=self._emit(expr.upper),
        )
        return exp.Paren(this=between)

    def visit_cast(self, expr):
        """Render ``CAST(expr AS target_type)``, parsing the type text."""
        return exp.Cast(
            this=self._emit(expr.expr), to=exp.DataType.build(expr.target_type)
        )

    def visit_extract(self, expr):
        """Render ``EXTRACT(field FROM source)``."""
        return exp.Extract(this=exp.var(expr.field), expression=self._emit(expr.source))

    def visit_interval(self, expr):
        """Render an ``INTERVAL 'value [unit]'`` literal."""
        literal = exp.Literal.string(str(expr.value))
        if expr.unit is None:
            return exp.Interval(this=literal)
        return exp.Interval(this=literal, unit=exp.var(expr.unit))

    def visit_window_expr(self, expr):
        """Render ``function OVER (PARTITION BY ... ORDER BY ... frame)``."""
        order = self._window_order(expr)
        spec = None
        if expr.frame:
            spec = _frame_spec(expr.frame)
        return exp.Window(
            this=self._emit(expr.function),
            partition_by=self._emit_each(expr.partition_by),
            order=order,
            spec=spec,
        )

    def _window_order(self, expr) -> "exp.Order":
        """Build the OVER clause ORDER BY, or None when there are no keys."""
        if not expr.order_keys:
            return None
        ordered = self._ordered_keys(
            expr.order_keys, expr.order_ascending, expr.order_nulls
        )
        return exp.Order(expressions=ordered)

    def _ordered_keys(self, keys, ascending, nulls) -> list:
        """Build one ``exp.Ordered`` per sort key with direction and NULLS."""
        items = []
        for index, key in enumerate(keys):
            items.append(self._ordered_key(index, key, ascending, nulls))
        return items

    def _ordered_key(self, index, key, ascending, nulls) -> "exp.Ordered":
        """Build a single ordered key honoring its ascending flag and NULLS."""
        desc = not ascending[index]
        nulls_spec = nulls[index]
        if nulls_spec is None:
            return exp.Ordered(this=self._emit(key), desc=desc)
        return exp.Ordered(
            this=self._emit(key), desc=desc, nulls_first=(nulls_spec.upper() == "FIRST")
        )

    def visit_subquery(self, expr):
        """A scalar subquery must be decorrelated before SQL emission."""
        raise ValueError(
            "SubqueryExpression reached SQL emission; decorrelate it first"
        )

    def visit_exists(self, expr):
        """An EXISTS predicate must be decorrelated before SQL emission."""
        raise ValueError("ExistsExpression reached SQL emission; decorrelate it first")

    def visit_in_subquery(self, expr):
        """An IN-subquery predicate must be decorrelated before SQL emission."""
        raise ValueError("InSubquery reached SQL emission; decorrelate it first")

    def visit_quantified_comparison(self, expr):
        """A quantified comparison must be decorrelated before SQL emission."""
        raise ValueError(
            "QuantifiedComparison reached SQL emission; decorrelate it first"
        )

    def visit_tuple(self, expr):
        """Render a row-value tuple ``(a, b, ...)``."""
        return exp.Tuple(expressions=self._emit_each(expr.items))


def _frame_spec(frame_text: str) -> exp.Expression:
    """Parse a stored raw window-frame fragment into a sqlglot WindowSpec.

    The frame is held as unstructured text (e.g. ``ROWS BETWEEN 1 PRECEDING AND
    CURRENT ROW``); parse it once here through an OVER clause to recover the spec.
    """
    import sqlglot

    window = sqlglot.parse_one(f"SELECT COUNT(*) OVER ({frame_text})", read="postgres")
    return window.expressions[0].args.get("spec")


def expression_to_ast(expr: "E.Expression", resolver) -> exp.Expression:
    """Lower an engine expression tree to a sqlglot AST using ``resolver``."""
    return expr.accept(SqlglotEmitter(resolver))
