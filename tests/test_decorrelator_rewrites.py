"""Unit tests for decorrelation rewrites: plan shapes, names, and errors.

These run against an in-memory DuckDB catalog (no external services) and
assert the logical plan structures the decorrelator must produce.
"""

import pytest

from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.parser import Parser, Binder
from federated_query.optimizer.decorrelation import Decorrelator, DecorrelationError
from federated_query.plan.logical import (
    Join,
    JoinType,
    Aggregate,
    Limit,
    Union,
    SingleRowGuard,
    GroupedLimit,
    LateralJoin,
    Projection,
)
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
    FunctionCall,
    ColumnRef,
)
from tests.duckdb_tmp import duckdb_path


@pytest.fixture(scope="module")
def catalog():
    """Catalog with users/orders tables in an in-memory DuckDB."""
    ds = DuckDBDataSource(
        name="default", config={"path": duckdb_path(), "read_only": False}
    )
    ds.connect()
    ds.connection.execute(
        "CREATE SCHEMA pg;"
        "CREATE TABLE pg.users (id INTEGER, name VARCHAR, country VARCHAR);"
        "CREATE TABLE pg.orders (id INTEGER, user_id INTEGER, amount DOUBLE);"
    )
    catalog = Catalog()
    catalog.register_datasource(ds)
    catalog.load_metadata()
    yield catalog
    ds.disconnect()


def decorrelate(catalog, sql):
    """Parse, bind, and decorrelate a query."""
    plan = Binder(catalog).bind(Parser().parse(sql))
    return Decorrelator().decorrelate(plan)


def find_all(plan, node_type):
    """Collect all nodes of a type from a plan tree."""
    found = []
    stack = [plan]
    while stack:
        node = stack.pop()
        if isinstance(node, node_type):
            found.append(node)
        for child in node.children():
            stack.append(child)
    return found


def the_join(plan, join_type):
    """The single join of the given type in the plan."""
    joins = []
    for join in find_all(plan, Join):
        if join.join_type == join_type:
            joins.append(join)
    assert len(joins) == 1, f"expected one {join_type} join, got {len(joins)}"
    return joins[0]


def condition_sql(join):
    """The join condition as SQL text for structural assertions."""
    assert join.condition is not None
    return join.condition.to_sql()


def test_correlated_exists_becomes_semi_join(catalog):
    """Correlated EXISTS turns into a SEMI join on the correlation key."""
    plan = decorrelate(
        catalog,
        "SELECT u.id FROM pg.users u "
        "WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id)",
    )
    join = the_join(plan, JoinType.SEMI)
    assert '"u"."id"' in condition_sql(join)
    assert "__subq_0_k0" in condition_sql(join)


def test_correlated_not_exists_becomes_anti_join(catalog):
    """Correlated NOT EXISTS turns into an ANTI join."""
    plan = decorrelate(
        catalog,
        "SELECT u.id FROM pg.users u "
        "WHERE NOT EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id)",
    )
    the_join(plan, JoinType.ANTI)


def test_uncorrelated_exists_uses_limit_one_probe(catalog):
    """Uncorrelated EXISTS probes a LIMIT 1 subplan with no condition."""
    plan = decorrelate(
        catalog,
        "SELECT id FROM pg.users "
        "WHERE EXISTS (SELECT 1 FROM pg.orders WHERE amount > 100)",
    )
    join = the_join(plan, JoinType.SEMI)
    assert join.condition is None
    limits = find_all(join.right, Limit)
    assert len(limits) == 1 and limits[0].limit == 1


def test_in_uses_plain_equality(catalog):
    """IN matches with SQL equality: no IS NULL terms in the condition."""
    plan = decorrelate(
        catalog,
        "SELECT id FROM pg.users "
        "WHERE country IN (SELECT name FROM pg.users WHERE id > 3)",
    )
    join = the_join(plan, JoinType.SEMI)
    assert "IS NULL" not in condition_sql(join)
    # The subquery value column is qualified to its derived-relation alias
    # (__subq_0), so the equality compares against "__subq_0"."__subq_0_v0".
    assert (
        '= "__subq_0"."__subq_0_v0"'
        in condition_sql(join).replace("(", "").replace(")", "")
    )


def test_not_in_uses_null_aware_match(catalog):
    """NOT IN matches NULL on either side so UNKNOWN rows are dropped."""
    plan = decorrelate(
        catalog,
        "SELECT id FROM pg.users "
        "WHERE country NOT IN (SELECT name FROM pg.users WHERE id > 3)",
    )
    join = the_join(plan, JoinType.ANTI)
    sql = condition_sql(join)
    assert sql.count("IS NULL") == 2


def test_any_becomes_semi_join_with_operator(catalog):
    """op ANY keeps the comparison operator in the SEMI condition."""
    plan = decorrelate(
        catalog,
        "SELECT id FROM pg.users " "WHERE id > ANY (SELECT user_id FROM pg.orders)",
    )
    join = the_join(plan, JoinType.SEMI)
    assert ">" in condition_sql(join)


def test_all_becomes_anti_join_over_violations(catalog):
    """op ALL anti-joins against violations with NULL guards."""
    plan = decorrelate(
        catalog,
        "SELECT id FROM pg.users " "WHERE id <= ALL (SELECT user_id FROM pg.orders)",
    )
    join = the_join(plan, JoinType.ANTI)
    sql = condition_sql(join)
    assert "NOT" in sql
    assert sql.count("IS NULL") == 2


def test_correlated_scalar_aggregate_left_join_with_group_key(catalog):
    """A correlated scalar SUM joins LEFT to an aggregate keyed by the
    correlation column."""
    plan = decorrelate(
        catalog,
        "SELECT u.id, (SELECT SUM(o.amount) FROM pg.orders o "
        "WHERE o.user_id = u.id) AS total FROM pg.users u",
    )
    join = the_join(plan, JoinType.LEFT)
    aggregates = find_all(join.right, Aggregate)
    assert len(aggregates) == 1
    assert len(aggregates[0].group_by) == 1
    assert aggregates[0].group_by[0].column == "user_id"


def test_correlated_scalar_count_wrapped_in_coalesce(catalog):
    """COUNT scalars read 0, not NULL, for outer rows without matches."""
    plan = decorrelate(
        catalog,
        "SELECT u.id, (SELECT COUNT(*) FROM pg.orders o "
        "WHERE o.user_id = u.id) AS cnt FROM pg.users u",
    )
    assert isinstance(plan, Projection)
    count_expr = plan.expressions[1]
    assert isinstance(count_expr, FunctionCall)
    assert count_expr.function_name.upper() == "COALESCE"


def test_non_aggregated_scalar_gets_cardinality_guard(catalog):
    """A non-aggregated scalar subquery is wrapped in a SingleRowGuard."""
    plan = decorrelate(
        catalog,
        "SELECT u.id, (SELECT amount FROM pg.orders WHERE user_id = u.id) "
        "AS amount FROM pg.users u",
    )
    guards = find_all(plan, SingleRowGuard)
    assert len(guards) == 1
    assert len(guards[0].keys) == 1


def test_correlated_limit_becomes_grouped_limit(catalog):
    """A correlated LIMIT 1 becomes a per-correlation-key limit."""
    plan = decorrelate(
        catalog,
        "SELECT u.id, (SELECT amount FROM pg.orders WHERE user_id = u.id "
        "LIMIT 1) AS first_amount FROM pg.users u",
    )
    grouped = find_all(plan, GroupedLimit)
    assert len(grouped) == 1
    assert grouped[0].limit == 1
    assert len(grouped[0].keys) == 1


def test_or_with_subquery_expands_to_flag_filter(catalog):
    """OR over a subquery branch expands into a per-disjunct flag filter.

    A distinct union would merge full-duplicate source rows, so the
    subquery disjunct is turned into a boolean flag (a SEMI/ANTI split that
    preserves row multiplicity) and OR'd with the plain predicate.
    """
    plan = decorrelate(
        catalog,
        "SELECT id FROM pg.users u WHERE u.country = 'FR' "
        "OR EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id)",
    )
    unions = find_all(plan, Union)
    assert len(unions) == 1
    assert unions[0].distinct is False
    the_join(plan, JoinType.SEMI)
    the_join(plan, JoinType.ANTI)


def test_exists_in_select_list_builds_flag_union(catalog):
    """EXISTS as a projection column becomes a SEMI/ANTI flag pair."""
    plan = decorrelate(
        catalog,
        "SELECT u.id, EXISTS (SELECT 1 FROM pg.orders o "
        "WHERE o.user_id = u.id) AS has_orders FROM pg.users u",
    )
    unions = find_all(plan, Union)
    assert len(unions) == 1
    assert unions[0].distinct is False
    the_join(plan, JoinType.SEMI)
    the_join(plan, JoinType.ANTI)
    assert isinstance(plan.expressions[1], ColumnRef)
    assert plan.expressions[1].column.endswith("_flag")


def test_subquery_names_are_deterministic(catalog):
    """Repeated decorrelation produces identical subquery names."""
    sql = (
        "SELECT u.id FROM pg.users u "
        "WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id)"
    )
    first = decorrelate(catalog, sql)
    second = decorrelate(catalog, sql)
    assert condition_sql(the_join(first, JoinType.SEMI)) == condition_sql(
        the_join(second, JoinType.SEMI)
    )
    assert "__subq_0" in condition_sql(the_join(first, JoinType.SEMI))


def test_multi_column_scalar_subquery_rejected(catalog):
    """Scalar subqueries returning two columns must raise."""
    with pytest.raises(DecorrelationError, match="one column"):
        decorrelate(
            catalog,
            "SELECT u.id, (SELECT user_id, amount FROM pg.orders WHERE id = 1) "
            "AS pair FROM pg.users u",
        )


def test_plan_without_subqueries_unchanged_in_shape(catalog):
    """Plans without subqueries keep their structure."""
    sql = "SELECT id FROM pg.users WHERE country = 'US'"
    bound = Binder(catalog).bind(Parser().parse(sql))
    rewritten = Decorrelator().decorrelate(bound)
    assert repr(rewritten) == repr(bound)
    assert repr(rewritten.input) == repr(bound.input)


def test_correlated_non_equality_through_aggregate_uses_lateral(catalog):
    """Non-equality correlation across an aggregate can't flatten to a set-based
    join, so it falls back to a LATERAL join (the engine evaluates it per row).
    """
    plan = decorrelate(
        catalog,
        "SELECT u.id, (SELECT SUM(o.amount) FROM pg.orders o "
        "WHERE o.user_id > u.id) AS total FROM pg.users u",
    )
    assert len(find_all(plan, LateralJoin)) == 1


def test_scalar_correlated_on_same_named_keys_groups_by_both(catalog):
    """Two correlation keys sharing a bare name (from different inner tables) must
    each become a grouping key, not be deduped by bare name into one."""
    plan = decorrelate(
        catalog,
        "SELECT o.amount, "
        "(SELECT count(*) FROM pg.users u, pg.orders o2 "
        " WHERE u.id = o.user_id AND o2.id = o.id) AS n "
        "FROM pg.orders o",
    )
    aggregates = find_all(plan, Aggregate)
    assert len(aggregates) == 1
    keyed = set()
    for key in aggregates[0].group_by:
        if isinstance(key, ColumnRef):
            keyed.add((key.table, key.column))
    # Both u.id and o2.id are grouping keys, distinguished by qualifier.
    assert ("u", "id") in keyed
    assert ("o2", "id") in keyed
