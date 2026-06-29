"""Local DISTINCT (a cross-source projection) dedups via the merge engine.

PhysicalProjection.DISTINCT runs SELECT DISTINCT in DuckDB - the one local
set/dedup engine PhysicalUnion/Sort/aggregates also use - rather than a separate
pyarrow group-by path.
"""

from federated_query.executor.executor import Executor
from federated_query.plan.physical import PhysicalProjection, PhysicalValues
from federated_query.plan.expressions import Literal, ColumnRef, DataType


def _int_rows(values):
    """Build PhysicalValues rows (one integer column ``a``) from a list of ints."""
    rows = []
    for value in values:
        rows.append([Literal(value=value, data_type=DataType.INTEGER)])
    return PhysicalValues(rows=rows, output_names=["a"])


def test_local_distinct_dedups_duplicate_rows():
    """A DISTINCT projection over duplicate rows emits each value once."""
    source = _int_rows([1, 1, 2, 2, 2, 3])
    projection = PhysicalProjection(
        input=source,
        expressions=[ColumnRef(table=None, column="a")],
        output_names=["a"],
        distinct=True,
    )
    produced = []
    for batch in Executor().execute(projection):
        produced.extend(batch.to_pydict()["a"])
    assert sorted(produced) == [1, 2, 3]


def test_local_non_distinct_keeps_duplicates():
    """Without DISTINCT, every row is preserved (the merge path is not taken)."""
    source = _int_rows([1, 1, 2])
    projection = PhysicalProjection(
        input=source,
        expressions=[ColumnRef(table=None, column="a")],
        output_names=["a"],
        distinct=False,
    )
    produced = []
    for batch in Executor().execute(projection):
        produced.extend(batch.to_pydict()["a"])
    assert sorted(produced) == [1, 1, 2]
