"""Tests for the public ExpressionVisitor extension point.

The visitor lets callers walk an expression tree (to debug, monitor, or
rewrite queries). These tests exercise the pattern end to end — which nothing
else in the engine does — so the ``accept`` hooks and the ``ExpressionVisitor``
interface cannot silently drift out of sync as new expression types are added.
"""

from federated_query.plan.expressions import (
    Expression,
    ExpressionVisitor,
    ColumnRef,
    Literal,
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
    DataType,
    FunctionCall,
    Cast,
    CaseExpr,
    InList,
    BetweenExpression,
    SubqueryExpression,
    ExistsExpression,
    InSubquery,
    QuantifiedComparison,
    Quantifier,
    TupleExpression,
)
from federated_query.plan.logical import Values


def _dummy_subquery():
    """A minimal logical plan to stand in for a subquery's body."""
    return Values(
        rows=[[Literal(value=1, data_type=DataType.INTEGER)]],
        output_names=["x"],
    )


def _one_of_each_expression():
    """One instance of every expression type, paired with its hook label."""
    col = ColumnRef(table=None, column="a")
    lit = Literal(value=1, data_type=DataType.INTEGER)
    plan = _dummy_subquery()
    return [
        (col, "column_ref"),
        (lit, "literal"),
        (BinaryOp(op=BinaryOpType.ADD, left=col, right=lit), "binary_op"),
        (UnaryOp(op=UnaryOpType.NOT, operand=lit), "unary_op"),
        (FunctionCall(function_name="UPPER", args=[col]), "function_call"),
        (CaseExpr(when_clauses=[(col, lit)], else_result=lit), "case_expr"),
        (InList(value=col, options=[lit]), "in_list"),
        (BetweenExpression(value=col, lower=lit, upper=lit), "between"),
        (Cast(expr=col, target_type="VARCHAR"), "cast"),
        (SubqueryExpression(subquery=plan), "subquery"),
        (ExistsExpression(subquery=plan), "exists"),
        (InSubquery(value=col, subquery=plan), "in_subquery"),
        (
            QuantifiedComparison(
                operator=BinaryOpType.EQ,
                quantifier=Quantifier.ANY,
                left=col,
                subquery=plan,
            ),
            "quantified_comparison",
        ),
        (TupleExpression(items=(col, lit)), "tuple"),
    ]


class _LabelVisitor(ExpressionVisitor):
    """Returns the name of the hook each ``accept`` dispatched to."""

    def visit_column_ref(self, expr):
        return "column_ref"

    def visit_literal(self, expr):
        return "literal"

    def visit_binary_op(self, expr):
        return "binary_op"

    def visit_unary_op(self, expr):
        return "unary_op"

    def visit_function_call(self, expr):
        return "function_call"

    def visit_case_expr(self, expr):
        return "case_expr"

    def visit_in_list(self, expr):
        return "in_list"

    def visit_between(self, expr):
        return "between"

    def visit_cast(self, expr):
        return "cast"

    def visit_subquery(self, expr):
        return "subquery"

    def visit_exists(self, expr):
        return "exists"

    def visit_in_subquery(self, expr):
        return "in_subquery"

    def visit_quantified_comparison(self, expr):
        return "quantified_comparison"

    def visit_tuple(self, expr):
        return "tuple"


class _ColumnCollector(ExpressionVisitor):
    """Recursively collects every column name referenced in an expression."""

    def __init__(self):
        """Start with an empty list of collected column names."""
        self.columns = []

    def visit_column_ref(self, expr):
        self.columns.append(expr.column)

    def visit_literal(self, expr):
        return None

    def visit_binary_op(self, expr):
        expr.left.accept(self)
        expr.right.accept(self)

    def visit_unary_op(self, expr):
        expr.operand.accept(self)

    def visit_function_call(self, expr):
        for arg in expr.args:
            arg.accept(self)

    def visit_case_expr(self, expr):
        for condition, result in expr.when_clauses:
            condition.accept(self)
            result.accept(self)
        if expr.else_result is not None:
            expr.else_result.accept(self)

    def visit_in_list(self, expr):
        expr.value.accept(self)
        for option in expr.options:
            option.accept(self)

    def visit_between(self, expr):
        expr.value.accept(self)
        expr.lower.accept(self)
        expr.upper.accept(self)

    def visit_cast(self, expr):
        expr.expr.accept(self)

    def visit_tuple(self, expr):
        for item in expr.items:
            item.accept(self)

    def visit_subquery(self, expr):
        return None

    def visit_exists(self, expr):
        return None

    def visit_in_subquery(self, expr):
        expr.value.accept(self)

    def visit_quantified_comparison(self, expr):
        expr.left.accept(self)


def test_accept_dispatches_to_matching_hook():
    """Every expression type's accept() routes to its own visitor hook."""
    visitor = _LabelVisitor()
    for expr, expected_hook in _one_of_each_expression():
        assert expr.accept(visitor) == expected_hook


def test_visitor_interface_covers_every_expression_type():
    """The ABC declares exactly the hooks the accept() methods invoke.

    A new expression type added with an accept() hook but no abstract method
    (or vice versa) would leave a concrete visitor unable to handle the tree;
    this keeps the interface and the nodes in lockstep.
    """
    declared = set(ExpressionVisitor.__abstractmethods__)
    exercised = {"visit_" + label for _, label in _one_of_each_expression()}
    assert declared == exercised


def test_visitor_collects_nested_column_references():
    """A recursive visitor gathers columns from a compound expression."""
    # UPPER(a) BETWEEN b AND CAST(c AS INTEGER)
    expr = BetweenExpression(
        value=FunctionCall(
            function_name="UPPER", args=[ColumnRef(table=None, column="a")]
        ),
        lower=ColumnRef(table=None, column="b"),
        upper=Cast(expr=ColumnRef(table=None, column="c"), target_type="INTEGER"),
    )

    collector = _ColumnCollector()
    expr.accept(collector)

    assert collector.columns == ["a", "b", "c"]
