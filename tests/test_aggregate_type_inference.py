"""PhysicalHashAggregate declares the type execution actually produces.

The merge aggregate force-casts DuckDB's output to its declared schema, so a
wrong declared type corrupts data. MIN/MAX must preserve the argument type (the
previous bug declared them float64, so MIN(name) over a varchar column cast
text to float64); COUNT is integer; AVG is double; SUM widens integers.
"""

import pyarrow as pa

from federated_query.plan.physical import PhysicalHashAggregate, PhysicalValues
from federated_query.plan.expressions import (
    Literal,
    ColumnRef,
    FunctionCall,
    DataType,
)


def _values(name, value, data_type):
    """A one-column, one-row constant relation exposing `name`."""
    return PhysicalValues(
        rows=[[Literal(value=value, data_type=data_type)]],
        output_names=[name],
    )


def _agg(function_name, column):
    """An aggregate FunctionCall over a single bare column reference."""
    return FunctionCall(
        function_name=function_name,
        args=[ColumnRef(table=None, column=column)],
        is_aggregate=True,
    )


def _declared_type(input_node, aggregate):
    """The Arrow type PhysicalHashAggregate declares for one aggregate."""
    node = PhysicalHashAggregate(
        input=input_node,
        group_by=[],
        aggregates=[aggregate],
        output_names=["result"],
    )
    return node.schema().field(0).type


def test_min_of_varchar_is_varchar():
    """MIN over a text column declares varchar, not float64."""
    values = _values("name", "alice", DataType.VARCHAR)
    assert _declared_type(values, _agg("MIN", "name")) == pa.string()


def test_max_of_varchar_is_varchar():
    """MAX over a text column declares varchar, not float64."""
    values = _values("name", "bob", DataType.VARCHAR)
    assert _declared_type(values, _agg("MAX", "name")) == pa.string()


def test_count_is_integer():
    """COUNT declares an integer type regardless of argument."""
    values = _values("name", "x", DataType.VARCHAR)
    assert _declared_type(values, _agg("COUNT", "name")) == pa.int64()


def test_avg_is_double():
    """AVG declares double."""
    values = _values("amount", 5, DataType.INTEGER)
    assert _declared_type(values, _agg("AVG", "amount")) == pa.float64()


def test_sum_of_integer_widens_to_int64():
    """SUM over an integer column declares a 64-bit integer, not float64."""
    values = _values("amount", 5, DataType.INTEGER)
    assert _declared_type(values, _agg("SUM", "amount")) == pa.int64()


def test_min_of_integer_preserves_integer():
    """MIN over an integer column preserves the integer type."""
    values = _values("amount", 5, DataType.INTEGER)
    assert _declared_type(values, _agg("MIN", "amount")) == pa.int64()
