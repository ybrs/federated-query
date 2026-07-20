"""Subquery pushdown tests.

Design contract enforced here: the engine **fully decorrelates** every subquery
before planning, so no subquery *expression* (EXISTS / IN (SELECT ...) / ANY /
ALL / scalar ``(SELECT ...)``) ever survives into the physical plan. A
decorrelated EXISTS/IN becomes a ``SEMI JOIN`` (``ANTI JOIN`` for the negated
forms); a scalar subquery becomes a ``LEFT JOIN`` to a keyed aggregate. The
join's right side renders as a derived-table *relation* (``(SELECT ...) AS t``)
in FROM/JOIN position, which is a relation, not a subquery expression, and is
allowed. Every test below asserts both the expected join shape and the global
no-subquery-expression invariant.
"""

import duckdb
import pytest

from sqlglot import exp

from tests.e2e_pushdown.conftest import _seed_orders, _seed_products
from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    select_column_names,
    unwrap_parens,
)


def _marker_body_relation(exists_node):
    """Return the ``__subq_``-aliased derived relation of an EXISTS body, or None.

    The ``__subq_`` relation alias is synthesized ONLY by the decorrelation
    pass, so an EXISTS whose body reads from such a relation is provably the
    engine's rendering of a decorrelated SEMI/ANTI join, never a verbatim
    pass-through of the user's subquery expression.
    """
    body = exists_node.this
    if not isinstance(body, exp.Select):
        return None
    from_clause = body.args.get("from_")
    if from_clause is None:
        return None
    relation = from_clause.this
    if isinstance(relation, exp.Subquery) and relation.alias.startswith("__subq_"):
        return relation
    return None


def _semi_join_marker_kind(exists_node):
    """Classify an EXISTS node as the engine's SEMI/ANTI join rendering, or None.

    The physical plan carries a real SEMI/ANTI join; the SQL emitter renders it
    as ``[NOT ]EXISTS(SELECT 1 FROM (<body>) AS "__subq_N" WHERE <keys>)``
    because canonical Postgres form has no SEMI JOIN syntax. Returns "SEMI",
    "ANTI" (when the EXISTS sits under NOT), or None for any other EXISTS.
    """
    if _marker_body_relation(exists_node) is None:
        return None
    if isinstance(exists_node.parent, exp.Not):
        return "ANTI"
    return "SEMI"


def _join_kinds(ast):
    """Collect the join-shape tokens of an AST.

    sqlglot records SEMI/ANTI under a join's ``kind`` but LEFT/RIGHT/FULL under
    its ``side``, so both are gathered; ``["SEMI", "SEMI"]`` for two semi joins,
    ``["LEFT"]`` for one left join. The engine renders a physical SEMI/ANTI
    join as its EXISTS marker (see ``_semi_join_marker_kind``), so recognized
    markers count as SEMI/ANTI joins here.
    """
    kinds = []
    for join in ast.args.get("joins") or []:
        for key in ("side", "kind"):
            value = join.args.get(key)
            if value:
                kinds.append(str(value).upper())
    for exists_node in ast.find_all(exp.Exists):
        kind = _semi_join_marker_kind(exists_node)
        if kind is not None:
            kinds.append(kind)
    return kinds


def _assert_no_subquery_expressions(ast):
    """Assert no predicate/projection-position subquery survived decorrelation.

    A subquery in FROM/JOIN position is a derived-table *relation* and is
    decorrelation must leave the physical plan free of subquery expressions.
    An EXISTS that is the engine's semi-join marker is the RENDERING of the
    decorrelated join, not a surviving subquery expression; any other EXISTS
    is a failure.
    """
    for exists_node in ast.find_all(exp.Exists):
        assert (
            _semi_join_marker_kind(exists_node) is not None
        ), "EXISTS survived decorrelation"
    assert not list(ast.find_all(exp.Any)), "ANY survived decorrelation"
    assert not list(ast.find_all(exp.All)), "ALL survived decorrelation"
    for in_node in ast.find_all(exp.In):
        assert in_node.args.get("query") is None, "IN (SELECT ...) survived"
    _assert_only_relation_subqueries(ast)


def _assert_only_relation_subqueries(ast):
    """Assert every surviving Subquery sits in a relation position.

    FROM/JOIN derived tables and the derived table of a ``LATERAL`` (dependent)
    join are relations, not subquery expressions, and are allowed; a semi-join
    marker's ``__subq_`` relation sits under the EXISTS body's FROM, so it
    passes the same relation-position rule.
    """
    for subquery in ast.find_all(exp.Subquery):
        parent = subquery.parent
        assert isinstance(
            parent, (exp.From, exp.Join, exp.Lateral)
        ), "scalar subquery survived decorrelation"


def _assert_push_matches_source(single_source_env, sql):
    """Assert a query pushes as one remote query and matches the raw source.

    The engine result is compared against running the same SQL directly on the
    underlying DuckDB connection, so the pushed (decorrelated, join-form) query
    is verified for correctness, not just shape.
    """
    runtime = build_runtime(single_source_env)
    explain_datasource_query(runtime, sql)
    engine_rows = runtime.execute(sql).column(0).to_pylist()
    raw_sql = sql.replace("duckdb_primary.main.", "main.")
    source = single_source_env.datasources[0].connection
    expected = []
    for row in source.execute(raw_sql).fetchall():
        expected.append(row[0])
    assert sorted(engine_rows) == sorted(expected)


# EXISTS Subqueries -> SEMI / ANTI JOIN


def test_where_exists_basic(single_source_env):
    """A WHERE EXISTS pushes as a SEMI JOIN, no EXISTS in the pushed query."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders O "
        "WHERE EXISTS ("
        "  SELECT 1 FROM duckdb_primary.main.products P "
        "  WHERE P.id = O.product_id AND P.price > 100"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "SEMI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


def test_where_not_exists(single_source_env):
    """A WHERE NOT EXISTS pushes as an ANTI JOIN."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders O "
        "WHERE NOT EXISTS ("
        "  SELECT 1 FROM duckdb_primary.main.products P "
        "  WHERE P.id = O.product_id AND P.status = 'discontinued'"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "ANTI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


# IN Subqueries -> SEMI / ANTI JOIN


def test_where_in_subquery(single_source_env):
    """A WHERE col IN (SELECT ...) pushes as a SEMI JOIN."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "WHERE product_id IN ("
        "  SELECT id FROM duckdb_primary.main.products WHERE price > 100"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "SEMI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


def test_where_not_in_subquery(single_source_env):
    """A WHERE col NOT IN (SELECT ...) pushes as an ANTI JOIN."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE product_id NOT IN ("
        "  SELECT id FROM duckdb_primary.main.products WHERE status = 'discontinued'"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "ANTI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


# Scalar Subqueries -> LEFT JOIN to a keyed aggregate


def test_scalar_subquery_max(single_source_env):
    """A scalar ``= (SELECT MAX(...))`` pushes as an INNER equi join, no
    subquery expr: the guarded single row makes LEFT-ON-TRUE plus the
    equality filter identical to the equi join, and the equi join is what
    the semi-join reduction machinery can see (the q06 fix)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "WHERE price = ("
        "  SELECT MAX(price) FROM duckdb_primary.main.orders"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "INNER" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


def test_scalar_subquery_avg_comparison(single_source_env):
    """A scalar ``> (SELECT AVG(...))`` pushes as an INNER JOIN, no subquery expr.

    The decorrelated subquery output sits on the null-extended side of the join;
    the ``price > <subquery>`` comparison rejects a NULL there, so the outer join
    simplifies to INNER (a row with no subquery match is dropped either way).
    """
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "WHERE price > ("
        "  SELECT AVG(price) FROM duckdb_primary.main.orders"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "INNER" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


def test_scalar_subquery_in_having(single_source_env):
    """A scalar subquery in HAVING pushes as a LEFT JOIN over the aggregate."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, AVG(price) AS avg_price "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region "
        "HAVING AVG(price) > ("
        "  SELECT AVG(price) FROM duckdb_primary.main.orders"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "region" in projection
    assert "avg_price" in projection
    assert "LEFT" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


# Correlated Subqueries -> LEFT JOIN to a keyed aggregate


def test_correlated_subquery_in_where(single_source_env):
    """A correlated scalar in WHERE pushes as an INNER JOIN, no subquery expr.

    As with the uncorrelated case, the ``price > <subquery>`` comparison rejects
    a NULL subquery output, so the decorrelated outer join simplifies to INNER.
    """
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders O "
        "WHERE price > ("
        "  SELECT AVG(price) FROM duckdb_primary.main.orders "
        "  WHERE region = O.region"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "INNER" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


def test_correlated_subquery_in_select(single_source_env):
    """A correlated scalar in SELECT pushes as a LEFT JOIN, no subquery expr."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, "
        "  (SELECT COUNT(*) FROM duckdb_primary.main.products P "
        "   WHERE P.id = O.product_id) AS product_count "
        "FROM duckdb_primary.main.orders O"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "product_count" in projection
    assert "LEFT" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


# ANY / ALL Operators -> SEMI / ANTI JOIN


def test_where_any_operator(single_source_env):
    """A WHERE col = ANY (SELECT ...) pushes as a SEMI JOIN."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price = ANY ("
        "  SELECT price FROM duckdb_primary.main.orders WHERE region = 'EU'"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "SEMI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


def test_where_all_operator(single_source_env):
    """A WHERE col > ALL (SELECT ...) pushes as an ANTI JOIN."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "WHERE price > ALL ("
        "  SELECT price FROM duckdb_primary.main.orders WHERE region = 'US'"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "ANTI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


# Pushed-subquery execution correctness (decorrelated join form vs the source)


def test_exists_pushed_result_matches_source(single_source_env):
    """A pushed EXISTS (as a SEMI JOIN) returns the rows the source computes."""
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders o "
        "WHERE EXISTS (SELECT 1 FROM duckdb_primary.main.products p "
        "              WHERE p.id = o.product_id AND p.price > 50)"
    )
    _assert_push_matches_source(single_source_env, sql)


def test_correlated_scalar_pushed_result_matches_source(single_source_env):
    """A pushed correlated scalar (as a LEFT JOIN) matches the source's result."""
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders o "
        "WHERE price > (SELECT AVG(price) FROM duckdb_primary.main.products p "
        "               WHERE p.category = o.status)"
    )
    _assert_push_matches_source(single_source_env, sql)


def test_correlated_count_scalar_pushed_result_matches_source(single_source_env):
    """A pushed correlated COUNT scalar (COALESCE-wrapped) matches the source."""
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders o "
        "WHERE quantity >= (SELECT COUNT(*) FROM duckdb_primary.main.products p "
        "                   WHERE p.category = o.status)"
    )
    _assert_push_matches_source(single_source_env, sql)


def test_correlated_scalar_order_limit_matches_source(single_source_env):
    """A correlated scalar subquery with ORDER BY/LIMIT matches the raw source.

    Decorrelates to a LEFT JOIN with an order-aware per-key limit; verified by
    comparing engine output to the same SQL on the raw source.
    """
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT o.order_id, "
        "  (SELECT p.name FROM duckdb_primary.main.products p "
        "   WHERE p.id = o.product_id ORDER BY p.name LIMIT 1) AS pname "
        "FROM duckdb_primary.main.orders o"
    )
    result = runtime.execute(sql)
    engine_rows = list(zip(result.column(0).to_pylist(), result.column(1).to_pylist()))
    raw_sql = sql.replace("duckdb_primary.main.", "main.")
    source = single_source_env.datasources[0].connection
    raw_rows = source.execute(raw_sql).fetchall()
    assert sorted(engine_rows) == sorted(raw_rows)


def _assert_two_col_matches_source(single_source_env, sql):
    """Run a two-column query in the engine and compare to the raw source."""
    runtime = build_runtime(single_source_env)
    result = runtime.execute(sql)
    engine_rows = list(zip(result.column(0).to_pylist(), result.column(1).to_pylist()))
    source = single_source_env.datasources[0].connection
    raw_rows = source.execute(sql.replace("duckdb_primary.main.", "main.")).fetchall()
    assert sorted(engine_rows) == sorted(raw_rows)


# Non-equi correlated scalars -> LATERAL join (dependent join), same source


def test_non_equi_scalar_aggregate_in_where_unnests(single_source_env):
    """A non-equi correlated scalar aggregate in WHERE unnests to regular joins
    (Neumann-Kemper), never a LATERAL - no correlated subquery is pushed."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders o "
        "WHERE price < (SELECT MAX(p.price) FROM duckdb_primary.main.products p "
        "               WHERE p.price < o.price)"
    )
    ast = explain_datasource_query(runtime, sql)
    assert ast.find(exp.Lateral) is None
    _assert_no_subquery_expressions(ast)


def test_non_equi_scalar_aggregate_in_select_matches_source(single_source_env):
    """A non-equi correlated scalar SUM in SELECT matches the raw source."""
    sql = (
        "SELECT o.order_id, "
        "  (SELECT SUM(p.price) FROM duckdb_primary.main.products p "
        "   WHERE p.id > o.product_id) AS total "
        "FROM duckdb_primary.main.orders o"
    )
    _assert_two_col_matches_source(single_source_env, sql)


def test_non_equi_scalar_order_limit_matches_source(single_source_env):
    sql = (
        "SELECT o.order_id, "
        "  (SELECT p.name FROM duckdb_primary.main.products p "
        "   WHERE p.price < o.price ORDER BY p.name LIMIT 1) AS pname "
        "FROM duckdb_primary.main.orders o"
    )
    _assert_two_col_matches_source(single_source_env, sql)


# User-written LATERAL (dependent join), same source


def test_user_left_join_lateral_unnests_and_matches_source(single_source_env):
    """A user ``LEFT JOIN LATERAL`` unnests to regular joins (no LATERAL) and
    still pushes as one query to its single source, matching it exactly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT o.order_id, t.name FROM duckdb_primary.main.orders o "
        "LEFT JOIN LATERAL ("
        "  SELECT p.name FROM duckdb_primary.main.products p "
        "  WHERE p.id = o.product_id ORDER BY p.name LIMIT 1"
        ") t ON true"
    )
    ast = explain_datasource_query(runtime, sql)
    assert ast.find(exp.Lateral) is None
    _assert_two_col_matches_source(single_source_env, sql)


def test_user_comma_lateral_matches_source(single_source_env):
    """A user comma ``LATERAL`` (INNER) pushes as one query and matches source."""
    sql = (
        "SELECT o.order_id, t.name FROM duckdb_primary.main.orders o, "
        "LATERAL ("
        "  SELECT p.name FROM duckdb_primary.main.products p "
        "  WHERE p.id = o.product_id ORDER BY p.name LIMIT 1"
        ") t"
    )
    _assert_two_col_matches_source(single_source_env, sql)


# Nested Subqueries


def test_nested_subqueries(single_source_env):
    """A subquery nested inside a subquery decorrelates to nested joins."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE product_id IN ("
        "  SELECT id FROM duckdb_primary.main.products "
        "  WHERE price > ("
        "    SELECT AVG(price) FROM duckdb_primary.main.products"
        "  )"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "SEMI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


# Derived Tables (Subquery in FROM) -- relation subqueries, allowed


def test_subquery_in_from_derived_table(single_source_env):
    """A FROM derived table pushes as a relation subquery in FROM position."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, high_price FROM ("
        "  SELECT order_id, price AS high_price "
        "  FROM duckdb_primary.main.orders "
        "  WHERE price > 100"
        ") AS high_orders "
        "WHERE high_price > 200"
    )
    ast = explain_datasource_query(runtime, sql)
    from_clause = ast.find(exp.From)
    assert from_clause is not None
    from_table = from_clause.this
    assert isinstance(from_table, exp.Subquery)
    derived_query = from_table.this
    assert isinstance(derived_query, exp.Select)
    assert derived_query.args.get("where") is not None
    assert ast.args.get("where") is not None
    _assert_no_subquery_expressions(ast)


def test_derived_table_with_join(single_source_env):
    """A derived table joined to a base table pushes as a relation subquery."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, H.avg_price FROM duckdb_primary.main.orders O "
        "JOIN ("
        "  SELECT region, AVG(price) AS avg_price "
        "  FROM duckdb_primary.main.orders "
        "  GROUP BY region"
        ") H ON O.region = H.region"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) == 1
    join_table = joins[0].this
    assert isinstance(join_table, exp.Subquery)
    derived_query = join_table.this
    assert isinstance(derived_query, exp.Select)
    assert derived_query.args.get("group") is not None
    _assert_no_subquery_expressions(ast)


# Multiple Subqueries -> multiple SEMI JOINs


def test_multiple_subqueries_in_where(single_source_env):
    """Two IN subqueries in WHERE push as two SEMI JOINs."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE product_id IN ("
        "  SELECT id FROM duckdb_primary.main.products WHERE price > 100"
        ") "
        "AND region IN ("
        "  SELECT DISTINCT region FROM duckdb_primary.main.orders WHERE quantity > 10"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert _join_kinds(ast).count("SEMI") == 2
    _assert_no_subquery_expressions(ast)


# Subqueries whose body itself needs decorrelation work (known gaps)


def test_subquery_with_group_by(single_source_env):
    """An IN over a GROUP BY/HAVING body pushes as a SEMI JOIN to a derived agg."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region IN ("
        "  SELECT region FROM duckdb_primary.main.orders "
        "  GROUP BY region HAVING COUNT(*) > 10"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "SEMI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


def test_subquery_with_order_by_limit(single_source_env):
    """An IN over an ORDER BY/LIMIT body pushes as a SEMI JOIN to a derived rel."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE product_id IN ("
        "  SELECT id FROM duckdb_primary.main.products "
        "  ORDER BY price DESC LIMIT 5"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "SEMI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


def test_subquery_with_union(single_source_env):
    """An IN over a UNION body pushes as a SEMI JOIN to a derived relation."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region IN ("
        "  SELECT region FROM duckdb_primary.main.orders WHERE price > 100 "
        "  UNION "
        "  SELECT region FROM duckdb_primary.main.orders WHERE quantity > 10"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "SEMI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


# Complex EXISTS with Multiple Conditions


def test_exists_with_complex_predicates(single_source_env):
    """An EXISTS with complex AND/OR predicates pushes as a SEMI JOIN."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders O "
        "WHERE EXISTS ("
        "  SELECT 1 FROM duckdb_primary.main.products P "
        "  WHERE P.id = O.product_id "
        "    AND (P.price > 100 OR P.status = 'premium') "
        "    AND P.base_price > 0"
        ")"
    )
    ast = explain_datasource_query(runtime, sql)
    assert "SEMI" in _join_kinds(ast)
    _assert_no_subquery_expressions(ast)


# Cross-Datasource Subquery Fallback


def test_cross_datasource_subquery_fallback(multi_source_env):
    """A cross-source ``IN`` subquery decorrelates to a SEMI join run across
    sources: each side pushes to its own source and the join runs in the merge
    engine. Verified against a single combined DuckDB seeded identically.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT order_id FROM duckdb_orders.main.orders "
        "WHERE product_id IN ("
        "  SELECT id FROM duckdb_products.main.products WHERE price > 50"
        ")"
    )

    # both sources receive a query (the cross-source fallback)
    queried_sources = []
    for datasource, _sql_text in runtime.explain_queries(sql):
        queried_sources.append(datasource)
    assert queried_sources.count("duckdb_orders") >= 1
    assert queried_sources.count("duckdb_products") >= 1

    # and the result matches the same query on one combined database
    engine_rows = sorted(runtime.execute(sql).column(0).to_pylist())
    reference = duckdb.connect()
    _seed_orders(reference)
    _seed_products(reference)
    expected = sorted(
        row[0]
        for row in reference.execute(
            "SELECT order_id FROM orders "
            "WHERE product_id IN (SELECT id FROM products WHERE price > 50)"
        ).fetchall()
    )
    reference.close()
    assert engine_rows == expected


def test_cross_source_union_subquery_matches_source(multi_source_env):
    """A cross-source ``IN`` over a UNION body: the union pushes to its source
    and the SEMI join runs in the merge engine. Verified vs a combined DB."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT order_id FROM duckdb_orders.main.orders WHERE product_id IN ("
        "  SELECT id FROM duckdb_products.main.products WHERE price > 50 "
        "  UNION SELECT id FROM duckdb_products.main.products WHERE base_price > 30"
        ")"
    )
    engine_rows = sorted(runtime.execute(sql).column(0).to_pylist())
    reference = duckdb.connect()
    _seed_orders(reference)
    _seed_products(reference)
    expected = sorted(
        row[0]
        for row in reference.execute(
            "SELECT order_id FROM orders WHERE product_id IN ("
            "  SELECT id FROM products WHERE price > 50 "
            "  UNION SELECT id FROM products WHERE base_price > 30)"
        ).fetchall()
    )
    reference.close()
    assert engine_rows == expected


def test_cross_source_not_in_matches_source(multi_source_env):
    """A cross-source ``NOT IN`` (ANTI join) runs in the merge engine (DuckDB),
    not a Python re-scanning loop, and matches the same query on one database."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT order_id FROM duckdb_orders.main.orders "
        "WHERE product_id NOT IN ("
        "  SELECT id FROM duckdb_products.main.products WHERE price > 50"
        ")"
    )
    engine_rows = sorted(runtime.execute(sql).column(0).to_pylist())
    reference = duckdb.connect()
    _seed_orders(reference)
    _seed_products(reference)
    expected = sorted(
        row[0]
        for row in reference.execute(
            "SELECT order_id FROM orders "
            "WHERE product_id NOT IN (SELECT id FROM products WHERE price > 50)"
        ).fetchall()
    )
    reference.close()
    assert engine_rows == expected
