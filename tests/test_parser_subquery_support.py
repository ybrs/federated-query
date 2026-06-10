"""Unit tests for parser support needed by subquery decorrelation."""

import pytest

from federated_query.parser.parser import Parser
from federated_query.plan.logical import (
    Values,
    SubqueryScan,
    Filter,
    Projection,
    Scan,
)
from federated_query.plan.expressions import (
    Literal,
    InSubquery,
    TupleExpression,
    QuantifiedComparison,
    Quantifier,
    BinaryOpType,
)


def parse(sql):
    """Parse SQL into an unbound logical plan."""
    return Parser().parse(sql)


def test_fromless_select_builds_values():
    """SELECT 42 becomes a single-row Values node."""
    plan = parse("SELECT 42")
    assert isinstance(plan, Values)
    assert plan.output_names == ["42"]
    assert isinstance(plan.rows[0][0], Literal)
    assert plan.rows[0][0].value == 42


def test_fromless_select_string_literal():
    """SELECT 'completed' becomes Values with the string literal."""
    plan = parse("SELECT 'completed'")
    assert isinstance(plan, Values)
    assert plan.rows[0][0].value == "completed"


def test_boolean_literal_in_where():
    """WHERE FALSE parses to a boolean literal filter."""
    plan = parse("SELECT id FROM pg.users WHERE FALSE")
    assert isinstance(plan, Projection)
    filter_node = plan.input
    assert isinstance(filter_node, Filter)
    assert isinstance(filter_node.predicate, Literal)
    assert filter_node.predicate.value is False


def test_derived_table_builds_subquery_scan():
    """FROM (SELECT ...) dt becomes SubqueryScan with the alias."""
    plan = parse("SELECT dt.id FROM (SELECT id FROM pg.users) dt")
    assert isinstance(plan, Projection)
    scan = plan.input
    assert isinstance(scan, SubqueryScan)
    assert scan.alias == "dt"
    assert isinstance(scan.input, Projection)
    assert isinstance(scan.input.input, Scan)


def test_derived_table_without_alias_rejected():
    """Derived tables must carry an alias."""
    with pytest.raises(ValueError, match="alias"):
        parse("SELECT 1 FROM (SELECT id FROM pg.users)")


def test_tuple_in_subquery():
    """(a, b) IN (subquery) parses to InSubquery over a TupleExpression."""
    plan = parse(
        "SELECT * FROM pg.users u "
        "WHERE (u.city, u.country) IN (SELECT name, country FROM pg.cities)"
    )
    filter_node = plan.input
    assert isinstance(filter_node, Filter)
    predicate = filter_node.predicate
    assert isinstance(predicate, InSubquery)
    assert isinstance(predicate.value, TupleExpression)
    assert len(predicate.value.items) == 2


def test_like_all_parses_to_quantified_comparison():
    """LIKE ALL(subquery) becomes a quantified LIKE comparison."""
    plan = parse(
        "SELECT * FROM pg.products WHERE name LIKE ALL(SELECT name FROM pg.products)"
    )
    filter_node = plan.input
    predicate = filter_node.predicate
    assert isinstance(predicate, QuantifiedComparison)
    assert predicate.operator == BinaryOpType.LIKE
    assert predicate.quantifier == Quantifier.ALL


def test_lateral_rejected_with_clear_error():
    """LATERAL must fail with a clear message, not an AttributeError."""
    with pytest.raises(ValueError, match="LATERAL"):
        parse(
            "SELECT u.id, o.amount FROM pg.users u, "
            "LATERAL (SELECT amount FROM pg.orders WHERE user_id = u.id LIMIT 1) o"
        )


def test_with_clause_rejected_with_clear_error():
    """WITH (CTE) must fail fast instead of being silently dropped."""
    with pytest.raises(ValueError, match="WITH"):
        parse("WITH t AS (SELECT id FROM pg.users) SELECT * FROM t")


def test_recursive_cte_rejected_with_clear_error():
    """WITH RECURSIVE must fail fast."""
    with pytest.raises(ValueError, match="WITH"):
        parse(
            "WITH RECURSIVE tree AS ("
            " SELECT id, name FROM pg.users WHERE id = 1"
            " UNION ALL"
            " SELECT u.id, u.name FROM pg.users u, tree t WHERE u.id = t.id + 1)"
            " SELECT * FROM tree"
        )


def test_fromless_select_with_where_rejected():
    """A FROM-less SELECT cannot carry a WHERE clause."""
    with pytest.raises(ValueError, match="WHERE"):
        parse("SELECT 1 WHERE 1 = 1")
