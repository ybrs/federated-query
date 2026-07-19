"""Compare an engine result against the oracle result, value by value.

The comparison checks three things: the column names match (case-insensitively,
because both DuckDB and PostgreSQL fold unquoted identifiers to lower case and
the engine preserves the query's casing), the row counts match, and the rows
match either as a multiset (default) or in exact order (``order_sensitive``).

Values are normalized so equal data from different sources compares equal:
integer widths collapse to Python ``int``, floats compare within a relative
tolerance of 1e-9, ``Decimal`` compares exactly, ``date``/``timestamp`` compare
as Python ``date``/``datetime``, and strings compare exactly. A mismatch raises
``AssertionError`` naming the placement, both row counts, and the first differing
row pair.
"""

import datetime
from decimal import Decimal

_FLOAT_TOLERANCE = 1e-9


def compare_tables(engine_table, oracle_table, placement_name, order_sensitive):
    """Assert an engine pyarrow.Table equals the oracle table for a placement."""
    _compare_columns(engine_table, oracle_table, placement_name)
    engine_rows = _rows(engine_table)
    oracle_rows = _rows(oracle_table)
    _compare_counts(engine_rows, oracle_rows, placement_name)
    if order_sensitive:
        _compare_ordered(engine_rows, oracle_rows, placement_name)
    else:
        _compare_multiset(engine_rows, oracle_rows, placement_name)


def _compare_columns(engine_table, oracle_table, placement_name):
    """Raise unless both tables carry the same column names (case-insensitive)."""
    engine_cols = _lower_names(engine_table)
    oracle_cols = _lower_names(oracle_table)
    if engine_cols != oracle_cols:
        raise AssertionError(
            "[" + placement_name + "] column mismatch: engine "
            + str(engine_cols) + " vs oracle " + str(oracle_cols)
        )


def _lower_names(table):
    """Return a table's column names lower-cased, in order."""
    names = []
    for name in table.column_names:
        names.append(name.lower())
    return names


def _compare_counts(engine_rows, oracle_rows, placement_name):
    """Raise unless the two row lists have the same length."""
    if len(engine_rows) != len(oracle_rows):
        raise AssertionError(
            "[" + placement_name + "] row count mismatch: engine "
            + str(len(engine_rows)) + " vs oracle " + str(len(oracle_rows))
        )


def _rows(table):
    """Return a table's rows as a list of normalized value tuples."""
    columns = _column_lists(table)
    rows = []
    for index in range(table.num_rows):
        rows.append(_row_at(columns, index))
    return rows


def _column_lists(table):
    """Return each column as a Python list, in column order."""
    lists = []
    for column in table.columns:
        lists.append(column.to_pylist())
    return lists


def _row_at(columns, index):
    """Assemble one normalized row tuple across all column lists."""
    values = []
    for column in columns:
        values.append(_normalize(column[index]))
    return tuple(values)


def _normalize(value):
    """Return the canonical Python form of one cell value (mostly identity)."""
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def _compare_ordered(engine_rows, oracle_rows, placement_name):
    """Compare rows position by position (deterministic top-level ORDER BY)."""
    _raise_on_diff(engine_rows, oracle_rows, placement_name, "ordered")


def _compare_multiset(engine_rows, oracle_rows, placement_name):
    """Compare rows as multisets by sorting both under one total order."""
    engine_sorted = sorted(engine_rows, key=_row_sort_key)
    oracle_sorted = sorted(oracle_rows, key=_row_sort_key)
    _raise_on_diff(engine_sorted, oracle_sorted, placement_name, "multiset")


def _raise_on_diff(engine_rows, oracle_rows, placement_name, mode):
    """Find the first differing row pair and raise a detailed AssertionError."""
    for index in range(len(engine_rows)):
        if not _row_equal(engine_rows[index], oracle_rows[index]):
            raise AssertionError(
                "[" + placement_name + "] " + mode + " row " + str(index)
                + " differs\n  engine: " + repr(engine_rows[index])
                + "\n  oracle: " + repr(oracle_rows[index])
                + "\n  rows: engine=" + str(len(engine_rows))
                + " oracle=" + str(len(oracle_rows))
            )


def _row_equal(engine_row, oracle_row):
    """Whether two normalized rows are equal cell by cell."""
    for engine_value, oracle_value in zip(engine_row, oracle_row):
        if not _values_equal(engine_value, oracle_value):
            return False
    return True


def _values_equal(left, right):
    """Whether two normalized cell values are equal under the suite's rules."""
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    if isinstance(left, Decimal) and isinstance(right, Decimal):
        return left == right
    if _is_number(left) and _is_number(right):
        return _floats_close(float(left), float(right))
    return left == right


def _is_number(value):
    """Whether a value is a non-boolean numeric (int, float, or Decimal)."""
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _floats_close(left, right):
    """Whether two floats are within the suite's relative tolerance."""
    scale = max(1.0, abs(left), abs(right))
    return abs(left - right) <= _FLOAT_TOLERANCE * scale


def _row_sort_key(row):
    """Return a total-order sort key for a row (a tuple of per-value keys)."""
    keys = []
    for value in row:
        keys.append(_value_sort_key(value))
    return tuple(keys)


def _value_sort_key(value):
    """Return a None-safe (rank, number, text) key ordering any cell value."""
    if value is None:
        return (0, 0.0, "")
    if isinstance(value, bool):
        return (1, float(value), "")
    if _is_number(value):
        return (1, round(float(value), 6), "")
    if isinstance(value, (datetime.date, datetime.datetime)):
        return (3, 0.0, value.isoformat())
    return (2, 0.0, str(value))
