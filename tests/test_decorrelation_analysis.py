"""Unit tests for correlation analysis utilities."""

from federated_query.optimizer.decorrelation import (
    CorrelationAnalyzer,
    CorrelationResult,
)
from federated_query.plan.logical import Scan, Filter
from federated_query.plan.expressions import (
    ColumnRef,
    BinaryOp,
    BinaryOpType,
    Literal,
    DataType,
    Cast,
    Extract,
)


def _build_binary_comparison(left: ColumnRef, right: ColumnRef) -> BinaryOp:
    return BinaryOp(op=BinaryOpType.EQ, left=left, right=right)


def test_correlation_analyzer_detects_correlated_subquery():
    """
    Correlated subquery should report outer references and set is_correlated.
    """
    scan = Scan(
        datasource="pg",
        schema_name="public",
        table_name="orders",
        columns=["user_id"],
        alias="o",
    )
    predicate = _build_binary_comparison(
        ColumnRef(table="o", column="user_id"),
        ColumnRef(table="u", column="id"),
    )
    subquery = Filter(input=scan, predicate=predicate)

    analyzer = CorrelationAnalyzer()
    result = analyzer.analyze(subquery, {"u"})

    assert isinstance(result, CorrelationResult)
    assert result.is_correlated is True
    assert len(result.outer_references) == 1
    assert result.outer_references[0].table == "u"
    # An explicit alias hides the base table name in SQL scope, so the
    # analyzer tracks the relation by its alias only.
    assert "orders" not in result.inner_tables
    assert "o" in result.inner_tables


def test_correlation_analyzer_detects_outer_ref_inside_cast():
    """An outer reference wrapped in CAST/EXTRACT must still be seen.

    The correlation walker previously did not descend into Cast/Extract/Window,
    so such a subquery was treated as uncorrelated and executed once instead of
    per outer row - a silent wrong result.
    """
    scan = Scan(
        datasource="pg",
        schema_name="public",
        table_name="orders",
        columns=["amount"],
        alias="o",
    )
    predicate = BinaryOp(
        op=BinaryOpType.GT,
        left=ColumnRef(table="o", column="amount"),
        right=Cast(
            expr=ColumnRef(table="u", column="threshold"), target_type="DECIMAL"
        ),
    )
    result = CorrelationAnalyzer().analyze(Filter(input=scan, predicate=predicate), {"u"})

    assert result.is_correlated is True
    assert result.outer_references[0].table == "u"
    assert result.outer_references[0].column == "threshold"


def test_correlation_analyzer_detects_outer_ref_inside_extract():
    """An outer reference inside EXTRACT(... FROM outer.col) must be seen."""
    scan = Scan(
        datasource="pg",
        schema_name="public",
        table_name="orders",
        columns=["y"],
        alias="o",
    )
    predicate = BinaryOp(
        op=BinaryOpType.EQ,
        left=ColumnRef(table="o", column="y"),
        right=Extract(field="YEAR", source=ColumnRef(table="u", column="created")),
    )
    result = CorrelationAnalyzer().analyze(Filter(input=scan, predicate=predicate), {"u"})

    assert result.is_correlated is True
    assert result.outer_references[0].column == "created"


def test_correlation_analyzer_uncorrelated_subquery():
    """
    Uncorrelated subquery should not report outer references.
    """
    scan = Scan(
        datasource="pg",
        schema_name="public",
        table_name="cities",
        columns=["country"],
        alias="c",
    )
    predicate = _build_binary_comparison(
        ColumnRef(table="c", column="country"),
        ColumnRef(table="c", column="country"),
    )
    subquery = Filter(input=scan, predicate=predicate)

    analyzer = CorrelationAnalyzer()
    result = analyzer.analyze(subquery, set())

    assert isinstance(result, CorrelationResult)
    assert result.is_correlated is False
    assert len(result.outer_references) == 0
    # The alias hides the base table name in SQL scope.
    assert "cities" not in result.inner_tables
    assert "c" in result.inner_tables


def test_correlation_analyzer_handles_literals():
    """
    Literals should not be treated as outer references.
    """
    scan = Scan(
        datasource="pg",
        schema_name="public",
        table_name="products",
        columns=["id"],
        alias=None,
    )
    predicate = BinaryOp(
        op=BinaryOpType.GT,
        left=ColumnRef(table=None, column="id"),
        right=Literal(value=10, data_type=DataType.INTEGER),
    )
    subquery = Filter(input=scan, predicate=predicate)

    analyzer = CorrelationAnalyzer()
    result = analyzer.analyze(subquery, set())

    assert result.is_correlated is False
    assert len(result.outer_references) == 0
    assert "products" in result.inner_tables
