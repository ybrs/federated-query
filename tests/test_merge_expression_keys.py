"""Compound expressions in ORDER BY / GROUP BY / aggregates run in DuckDB.

DuckDB can order, group, and aggregate by any expression, so the engine renders
the expression to DuckDB SQL and runs it in the merge engine - there is no Python
sort/aggregate path. These tests pin both halves of that contract: the generated
SQL carries the expression (so it is sent to DuckDB, not evaluated in Python), and
the executed result is correct.
"""

import pyarrow as pa
import pytest

from federated_query.executor.merge_engine import MergeEngine
from federated_query.plan.expressions import (
    ColumnRef,
    BinaryOp,
    BinaryOpType,
    FunctionCall,
)
from federated_query.plan.physical import (
    PhysicalPlanNode,
    PhysicalSort,
    PhysicalHashAggregate,
    PhysicalGroupedLimit,
)


class _Node(PhysicalPlanNode):
    """Minimal physical node replaying a fixed table."""

    table: pa.Table

    def children(self):
        """A leaf node has no children."""
        return []

    def schema(self) -> pa.Schema:
        """Return the table schema."""
        return self.table.schema

    def column_aliases(self):
        """No qualified-name remapping."""
        return {}

    def execute(self):
        """Yield the table's batches."""
        for batch in self.table.to_batches():
            yield batch

    def estimated_cost(self) -> float:
        """Free; this is a test fixture."""
        return 0.0


@pytest.fixture
def engine():
    """A merge engine for the duration of one test."""
    eng = MergeEngine("256MB", None)
    yield eng
    eng.close()


def _col(name):
    """Shorthand for an unqualified column reference."""
    return ColumnRef(table=None, column=name)


def _mul(left, right):
    """Build the ``left * right`` arithmetic expression."""
    return BinaryOp(op=BinaryOpType.MULTIPLY, left=left, right=right)


def _rows(node, engine):
    """Run a node through the merge engine and collect rows as dicts."""
    node.set_merge_engine(engine)
    rows = []
    for batch in node.execute():
        rows.extend(batch.to_pylist())
    return rows


def test_order_by_expression_renders_and_runs_in_duckdb(engine):
    """``ORDER BY a * b`` renders to DuckDB SQL and orders by the product."""
    table = pa.table({"a": pa.array([3, 1, 2]), "b": pa.array([10, 40, 20])})
    node = PhysicalSort(
        input=_Node(table=table),
        sort_keys=[_mul(_col("a"), _col("b"))],
        ascending=[True],
        nulls_order=[None],
    )

    # The ORDER BY expression is rendered to DuckDB SQL (not evaluated in Python).
    order_sql = node._sort_order_clause(node.input.column_aliases())
    assert '"a" * "b"' in order_sql
    assert "ASC NULLS LAST" in order_sql

    rows = _rows(node, engine)
    # products: 3*10=30, 1*40=40, 2*20=40 -> ascending keeps 30 first.
    assert [(r["a"], r["b"]) for r in rows] == [(3, 10), (1, 40), (2, 20)]


def test_group_by_with_expression_aggregate_arg(engine):
    """``GROUP BY g, SUM(a * b)`` renders the product into DuckDB SQL and sums it."""
    table = pa.table(
        {
            "g": pa.array([1, 1, 2]),
            "a": pa.array([3, 1, 2]),
            "b": pa.array([10, 40, 20]),
        }
    )
    sum_ab = FunctionCall(
        function_name="SUM", args=[_mul(_col("a"), _col("b"))], is_aggregate=True
    )
    node = PhysicalHashAggregate(
        input=_Node(table=table),
        group_by=[_col("g")],
        aggregates=[_col("g"), sum_ab],
        output_names=["g", "s"],
    )

    # The aggregate argument expression is rendered to DuckDB SQL.
    agg_sql = node._aggregate_sql(node.input.column_aliases())
    assert 'SUM(' in agg_sql and '"a" * "b"' in agg_sql
    assert 'GROUP BY "g"' in agg_sql

    rows = sorted(_rows(node, engine), key=lambda r: r["g"])
    # g=1: 3*10 + 1*40 = 70 ; g=2: 2*20 = 40
    assert rows == [{"g": 1, "s": 70}, {"g": 2, "s": 40}]


def test_group_by_expression_key(engine):
    """``GROUP BY (a % 2)`` groups by the computed key in DuckDB."""
    table = pa.table({"a": pa.array([1, 2, 3, 4, 5])})
    parity = BinaryOp(op=BinaryOpType.MODULO, left=_col("a"), right=_lit(2))
    count_star = FunctionCall(function_name="COUNT", args=[], is_aggregate=True)
    node = PhysicalHashAggregate(
        input=_Node(table=table),
        group_by=[parity],
        aggregates=[parity, count_star],
        output_names=["parity", "n"],
    )

    agg_sql = node._aggregate_sql(node.input.column_aliases())
    assert '"a" % 2' in agg_sql

    rows = sorted(_rows(node, engine), key=lambda r: r["parity"])
    # evens: 2,4 -> 2 ; odds: 1,3,5 -> 3
    assert rows == [{"parity": 0, "n": 2}, {"parity": 1, "n": 3}]


def test_grouped_limit_expression_partition(engine):
    """A per-key limit partitioned by ``a % 2`` renders the expression to DuckDB."""
    table = pa.table({"a": pa.array([1, 3, 5, 2, 4]), "v": pa.array([10, 30, 50, 20, 40])})
    node = PhysicalGroupedLimit(
        input=_Node(table=table),
        keys=[BinaryOp(op=BinaryOpType.MODULO, left=_col("a"), right=_lit(2))],
        limit=1,
    )

    assert '"a" % 2' in node._partition_clause()

    rows = _rows(node, engine)
    # First row of each parity group in input order: odd -> a=1, even -> a=2.
    assert sorted(r["a"] for r in rows) == [1, 2]


def _lit(value):
    """An integer literal expression."""
    from federated_query.plan.expressions import Literal, DataType

    return Literal(value=value, data_type=DataType.INTEGER)
