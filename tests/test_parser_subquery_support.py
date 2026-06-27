"""Unit tests for parser support needed by subquery decorrelation."""

import pytest

from federated_query.parser.parser import Parser
from federated_query.parser.errors import UnsupportedSQLError
from federated_query.plan.logical import (
    Values,
    SubqueryScan,
    Filter,
    Projection,
    Scan,
    LateralJoin,
    CTE,
    CTERef,
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


def _plan_has(plan, node_type):
    """Whether a node of the given type appears anywhere in a plan."""
    if isinstance(plan, node_type):
        return True
    for child in plan.children():
        if _plan_has(child, node_type):
            return True
    return False


def test_lateral_parses_to_lateral_join():
    """LATERAL parses into a LateralJoin (dependent join) node."""
    plan = parse(
        "SELECT u.id, o.amount FROM pg.users u, "
        "LATERAL (SELECT amount FROM pg.orders WHERE user_id = u.id LIMIT 1) o"
    )
    assert _plan_has(plan, LateralJoin)


def test_with_clause_parsed_as_cte():
    """A WITH clause becomes a CTE node whose name reference is a CTERef."""
    plan = parse("WITH t AS (SELECT id FROM pg.users) SELECT id FROM t")
    assert isinstance(plan, CTE)
    assert plan.name == "t"
    child = plan.child
    while not isinstance(child, CTERef) and hasattr(child, "input"):
        child = child.input
    assert isinstance(child, CTERef)
    assert child.name == "t"


def test_recursive_cte_parsed_with_flag():
    """WITH RECURSIVE sets the recursive flag and the explicit column list."""
    plan = parse(
        "WITH RECURSIVE tree(id, name) AS ("
        " SELECT id, name FROM pg.users WHERE id = 1"
        " UNION ALL"
        " SELECT u.id, u.name FROM pg.users u, tree t WHERE u.id = t.id + 1)"
        " SELECT id FROM tree"
    )
    assert isinstance(plan, CTE)
    assert plan.recursive is True
    assert plan.column_names == ["id", "name"]


def test_fromless_select_with_where_rejected():
    """A FROM-less SELECT cannot carry a WHERE clause."""
    with pytest.raises(ValueError, match="WHERE"):
        parse("SELECT 1 WHERE 1 = 1")


def test_fromless_select_with_limit_rejected():
    """B2: a FROM-less SELECT must not silently drop LIMIT."""
    with pytest.raises(UnsupportedSQLError, match="LIMIT"):
        parse("SELECT 1 LIMIT 0")


def test_fromless_select_with_distinct_rejected():
    """B2: a FROM-less SELECT must not silently drop DISTINCT."""
    with pytest.raises(UnsupportedSQLError, match="DISTINCT"):
        parse("SELECT DISTINCT 1")


def test_distinct_over_group_by_rejected():
    """B1: SELECT DISTINCT over GROUP BY must fail, not return duplicates."""
    with pytest.raises(UnsupportedSQLError, match="DISTINCT over GROUP BY"):
        parse("SELECT DISTINCT count(*) FROM t GROUP BY a")


def test_union_by_name_rejected():
    """Sweep: UNION ... BY NAME is unhandled and must not be silently dropped."""
    with pytest.raises(UnsupportedSQLError, match="BY_NAME"):
        parse("SELECT a FROM t UNION ALL BY NAME SELECT b FROM u")


def test_with_over_set_operation_rejected():
    """Sweep: a WITH binding a top-level UNION drops the CTE today; fail fast."""
    with pytest.raises(UnsupportedSQLError, match="WITH"):
        parse("WITH x AS (SELECT 1 AS a) SELECT a FROM x UNION SELECT 2")


def test_simple_case_rejected():
    """A simple CASE (CASE operand WHEN ...) must not match the wrong branch."""
    with pytest.raises(UnsupportedSQLError, match="simple CASE"):
        parse("SELECT CASE x WHEN 1 THEN 'a' ELSE 'b' END FROM t")


def test_try_cast_rejected():
    """TRY_CAST must not be silently planned as an error-raising CAST."""
    with pytest.raises(UnsupportedSQLError, match="SAFE"):
        parse("SELECT TRY_CAST(x AS INT) FROM t")


def test_between_symmetric_rejected():
    """BETWEEN SYMMETRIC must not be silently planned as a plain BETWEEN."""
    with pytest.raises(UnsupportedSQLError, match="SYMMETRIC"):
        parse("SELECT a FROM t WHERE x BETWEEN SYMMETRIC 1 AND 9")


def test_string_agg_separator_preserved():
    """STRING_AGG separator is a real argument and must not be dropped."""
    plan = parse("SELECT STRING_AGG(s, ',') AS c FROM t GROUP BY r")
    aggregate = plan.aggregates[0]
    assert len(aggregate.args) == 2
    assert isinstance(aggregate.args[1], Literal)
    assert aggregate.args[1].value == ","


def test_within_group_multiple_keys_rejected():
    """WITHIN GROUP with multiple ORDER BY keys must not silently drop the rest."""
    with pytest.raises(UnsupportedSQLError, match="WITHIN GROUP"):
        parse(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a, b) FROM t"
        )


def test_values_ragged_rows_rejected():
    """VALUES rows of differing widths must fail, not build a ragged relation."""
    with pytest.raises(UnsupportedSQLError, match="same number of columns"):
        parse("SELECT * FROM (VALUES (1, 2), (3)) AS v(a, b)")


def test_values_alias_width_mismatch_rejected():
    """A VALUES column-alias count that does not match the row width must fail."""
    with pytest.raises(UnsupportedSQLError, match="alias count"):
        parse("SELECT * FROM (VALUES (1, 2)) AS v(a, b, c)")


def test_trim_position_keyword_rejected():
    """TRIM LEADING/TRAILING/BOTH must not be silently dropped to a both-trim."""
    with pytest.raises(UnsupportedSQLError, match="TRIM"):
        parse("SELECT trim(LEADING 'x' FROM col) FROM s.t")


def test_plain_trim_still_parses():
    """TRIM without a position keyword is unaffected."""
    assert parse("SELECT trim(col) FROM s.t") is not None
