"""Backlog of intentionally-deferred, valid-SQL features.

Each item below is legal SQL the engine deliberately rejects (fail-fast) for now
rather than risk a silent wrong answer. These tests assert the DESIRED end state
(the form is accepted), marked ``xfail(strict=True)`` so they fail-as-expected
today and turn into a loud XPASS the moment a feature is implemented - the signal
to finish wiring it up and remove it from this list (and drop the matching
fail-fast guard + its raises-test).

This is the executable backlog: it makes "what is intentionally missing" visible
in the suite. Truly-invalid SQL (e.g. a window function in WHERE) and malformed
input (ragged VALUES) are NOT here - those are rejected by design and keep only
their passing raises-tests.

Cross-source NATURAL/USING lives in test_advanced_join_types.py (it needs the
two-source fixture); the same xfail(strict) convention is used there.
"""

import pytest

from federated_query.catalog import Catalog
from federated_query.catalog.schema import Column, Schema, Table
from federated_query.parser.parser import Parser
from federated_query.plan.expressions import DataType
from federated_query.processor import QueryContext, QueryPreprocessor


def _deferred(id_, sql, reason):
    """A parametrized case marked xfail(strict) with a backlog reason."""
    return pytest.param(
        sql, id=id_, marks=pytest.mark.xfail(strict=True, reason=reason)
    )


# Features rejected at parse time. Asserting parse() accepts them is the flip
# signal; full support (binding/planning/execution) is finished when the item is
# removed from this list.
_PARSER_DEFERRED = [
    _deferred(
        "simple_case",
        "SELECT CASE x WHEN 1 THEN 'a' ELSE 'b' END FROM s.t",
        "simple CASE (CASE operand WHEN ...)",
    ),
    _deferred(
        "try_cast", "SELECT TRY_CAST(x AS INT) FROM s.t", "TRY_CAST / SAFE_CAST"
    ),
    _deferred(
        "between_symmetric",
        "SELECT a FROM s.t WHERE x BETWEEN SYMMETRIC 1 AND 9",
        "BETWEEN SYMMETRIC",
    ),
    _deferred(
        "union_by_name",
        "SELECT a FROM s.t UNION ALL BY NAME SELECT a FROM s.u",
        "UNION [ALL] BY NAME",
    ),
    _deferred(
        "in_unnest",
        "SELECT a FROM s.t WHERE x IN UNNEST([1, 2, 3])",
        "IN UNNEST(array)",
    ),
    _deferred(
        "trim_position",
        "SELECT trim(LEADING 'x' FROM col) FROM s.t",
        "TRIM LEADING/TRAILING/BOTH",
    ),
    _deferred(
        "fetch_with_ties",
        "SELECT a FROM s.t ORDER BY a FETCH FIRST 2 ROWS WITH TIES",
        "FETCH FIRST ... WITH TIES",
    ),
    _deferred(
        "fetch_percent",
        "SELECT a FROM s.t ORDER BY a FETCH FIRST 10 PERCENT ROWS ONLY",
        "FETCH FIRST ... PERCENT",
    ),
    _deferred(
        "distinct_over_window",
        "SELECT DISTINCT rank() OVER (ORDER BY x) FROM s.t",
        "SELECT DISTINCT over a window function",
    ),
    _deferred(
        "distinct_over_group_by",
        "SELECT DISTINCT count(*) FROM s.t GROUP BY a",
        "SELECT DISTINCT over GROUP BY",
    ),
    _deferred(
        "combined_grouping_sets",
        "SELECT a, b FROM s.t GROUP BY ROLLUP (a), CUBE (b)",
        "combining multiple ROLLUP/CUBE/GROUPING SETS",
    ),
    _deferred(
        "within_group_multi_key",
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a, b) FROM s.t",
        "WITHIN GROUP with multiple ORDER BY keys",
    ),
    _deferred(
        "group_by_with_totals",
        "SELECT a, count(*) FROM s.t GROUP BY a WITH TOTALS",
        "GROUP BY ... WITH TOTALS",
    ),
]


@pytest.mark.parametrize("sql", _PARSER_DEFERRED)
def test_parser_accepts_deferred_feature(sql):
    """Valid SQL the parser deliberately rejects today; xfail until supported."""
    Parser().parse(sql)


def _pivot_catalog() -> Catalog:
    """A catalog with a table whose columns the PIVOT/window cases reference."""
    catalog = Catalog()
    schema = Schema(name="main", datasource="testdb")
    schema.add_table(
        Table(
            name="orders",
            columns=[
                Column(name="region", data_type=DataType.VARCHAR, nullable=False),
                Column(name="status", data_type=DataType.VARCHAR, nullable=False),
                Column(name="quantity", data_type=DataType.INTEGER, nullable=False),
                Column(name="order_id", data_type=DataType.INTEGER, nullable=False),
            ],
        )
    )
    catalog.schemas[("testdb", "main")] = schema
    return catalog


_T = "testdb.main.orders"

# Features rejected during preprocessing (PIVOT shapes, named-window chains).
_PREPROCESSOR_DEFERRED = [
    _deferred(
        "unpivot",
        f"SELECT * FROM {_T} UNPIVOT (val FOR col IN (quantity))",
        "UNPIVOT",
    ),
    _deferred(
        "multiple_pivot",
        f"SELECT * FROM {_T} "
        "PIVOT(SUM(quantity) FOR region IN ('us')) "
        "PIVOT(SUM(quantity) FOR status IN ('done'))",
        "multiple PIVOT clauses",
    ),
    _deferred(
        "pivot_multiple_aggregates",
        f"SELECT * FROM {_T} "
        "PIVOT(SUM(quantity), AVG(quantity) FOR region IN ('us', 'eu'))",
        "PIVOT with multiple aggregates",
    ),
    _deferred(
        "pivot_with_group_by",
        f"SELECT * FROM {_T} "
        "PIVOT(SUM(quantity) FOR region IN ('us', 'eu')) GROUP BY order_id",
        "GROUP BY combined with PIVOT",
    ),
    _deferred(
        "pivot_with_star_exclude",
        f"SELECT * EXCLUDE (quantity) FROM {_T} "
        "PIVOT(SUM(quantity) FOR region IN ('us', 'eu'))",
        "SELECT * EXCLUDE/REPLACE combined with PIVOT",
    ),
    _deferred(
        "chained_named_windows",
        f"SELECT row_number() OVER w2 FROM {_T} "
        "WINDOW w1 AS (PARTITION BY region), w2 AS (w1 ORDER BY order_id)",
        "chained named windows (w2 AS (w1 ...))",
    ),
]


@pytest.mark.parametrize("sql", _PREPROCESSOR_DEFERRED)
def test_preprocessor_accepts_deferred_feature(sql):
    """Valid SQL the preprocessor deliberately rejects today; xfail until supported."""
    QueryPreprocessor(_pivot_catalog()).preprocess(sql, QueryContext(sql))
