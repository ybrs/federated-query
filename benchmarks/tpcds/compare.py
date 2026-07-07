"""Compare two query result sets for TPC-DS correctness checking.

The engine result and the DuckDB oracle result are compared row by row, in
order: row i of the engine output must equal row i of DuckDB's output. Numbers
are rounded to a fixed number of decimals (TPC-DS aggregates are monetary) and
fixed-width CHAR padding is stripped, so only genuine value differences count.
Comparison is by column position; the two engines may name columns differently.

Row order is part of correctness here: a TPC-DS query with an ORDER BY must
return rows in that order. When the rows match as a set but not in order, the
mismatch is reported as an ordering difference so it is not confused with a
wrong value. Note that many TPC-DS queries end in ``ORDER BY ... LIMIT n`` over
columns with ties; when the tie-break differs between the two engines the
set-vs-order distinction in the reason makes that visible rather than hiding it.
"""

import datetime
from decimal import Decimal


def _normalize_value(value, decimals):
    """Turn one cell into a canonical, comparable string."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float, Decimal)):
        return "{0:.{1}f}".format(float(value), decimals)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace").rstrip()
    return str(value).rstrip()


def _normalize_row(row, decimals):
    """Normalize every cell of a row into a comparable tuple."""
    normalized = []
    for value in row:
        normalized.append(_normalize_value(value, decimals))
    return tuple(normalized)


def _normalize_rows(rows, decimals):
    """Normalize a list of rows into a list of comparable row tuples, in order."""
    normalized = []
    for row in rows:
        normalized.append(_normalize_row(row, decimals))
    return normalized


def _numeric_close(left, right):
    """Whether two numeric cells differ only by float summation noise.

    A float64 aggregate's last bits depend on summation ORDER (a distributed
    plan and a single-engine plan sum in different orders), and the
    fixed-decimal rounding AMPLIFIES a last-bit difference sitting exactly on
    a rounding boundary into a visible cent: q18's ROLLUP subtotal is engine
    206.98499999999999 vs oracle 206.985 (diff 1.4e-14), rounded to 206.98 vs
    206.99. Values within this TIGHT relative tolerance are the same number
    for correctness purposes; a real value bug (wrong rows, dropped rounding,
    a wrong scale) differs by far more than 1e-9 relative.
    """
    if not isinstance(left, (int, float, Decimal)) or isinstance(left, bool):
        return False
    if not isinstance(right, (int, float, Decimal)) or isinstance(right, bool):
        return False
    left_float = float(left)
    right_float = float(right)
    return abs(left_float - right_float) <= 1e-9 * max(
        1.0, abs(left_float), abs(right_float)
    )


def _cells_equal(engine_value, oracle_value, decimals):
    """Whether two cells match: by rounded form, or within numeric tolerance."""
    engine_norm = _normalize_value(engine_value, decimals)
    oracle_norm = _normalize_value(oracle_value, decimals)
    if engine_norm == oracle_norm:
        return True
    return _numeric_close(engine_value, oracle_value)


def _rows_equal(engine_row, oracle_row, decimals):
    """Whether two rows match cell-for-cell."""
    for engine_value, oracle_value in zip(engine_row, oracle_row):
        if not _cells_equal(engine_value, oracle_value, decimals):
            return False
    return True


def _first_differing_index(engine_rows, oracle_rows, decimals):
    """Return the index of the first row that differs, or -1 if all match."""
    for index in range(len(engine_rows)):
        if not _rows_equal(engine_rows[index], oracle_rows[index], decimals):
            return index
    return -1


def _describe_difference(index, engine_norm, oracle_norm):
    """Describe the first differing row, flagging an order-only difference."""
    if sorted(engine_norm) == sorted(oracle_norm):
        template = "rows match as a set but order differs at row {0}: engine={1} oracle={2}"
    else:
        template = "row {0} differs: engine={1} oracle={2}"
    return template.format(index, engine_norm[index], oracle_norm[index])


def compare_results(engine_rows, oracle_rows, decimals=2):
    """Compare two result sets row by row, returning (is_match, reason).

    ``reason`` is an empty string on a match and otherwise names the first
    discrepancy: a differing row count, or the first row position whose values
    (or ordering) differ.
    """
    if len(engine_rows) != len(oracle_rows):
        reason = "row count differs: engine={0} oracle={1}".format(
            len(engine_rows), len(oracle_rows)
        )
        return False, reason
    index = _first_differing_index(engine_rows, oracle_rows, decimals)
    if index == -1:
        return True, ""
    engine_norm = _normalize_rows(engine_rows, decimals)
    oracle_norm = _normalize_rows(oracle_rows, decimals)
    return False, _describe_difference(index, engine_norm, oracle_norm)
