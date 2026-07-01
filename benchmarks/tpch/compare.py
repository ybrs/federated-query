"""Compare two query result sets for TPC-H correctness checking.

The engine result and the DuckDB oracle result are compared as multisets of
normalized rows. Comparison is by column position (the two engines may name
columns differently), numbers are rounded to a fixed number of decimals (TPC-H
aggregates are monetary), and fixed-width CHAR padding is stripped. Row order is
not compared: TPC-H ties are order-ambiguous, so a multiset match is the
correctness signal we want.
"""

import datetime
from collections import Counter
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
    """Normalize a list of rows into a multiset (Counter) of row tuples."""
    counter = Counter()
    for row in rows:
        counter[_normalize_row(row, decimals)] += 1
    return counter


def _example_difference(engine_counter, oracle_counter):
    """Return a short description of one row present in only one side."""
    engine_only = engine_counter - oracle_counter
    oracle_only = oracle_counter - engine_counter
    if engine_only:
        row = next(iter(engine_only.elements()))
        return "engine produced unexpected row: {0}".format(row)
    if oracle_only:
        row = next(iter(oracle_only.elements()))
        return "engine missing expected row: {0}".format(row)
    return "row multiplicities differ"


def compare_results(engine_rows, oracle_rows, decimals=2):
    """Compare two result sets, returning (is_match, reason).

    ``reason`` is an empty string on a match and otherwise names the first
    discrepancy (row count, or an example row present on only one side).
    """
    if len(engine_rows) != len(oracle_rows):
        reason = "row count differs: engine={0} oracle={1}".format(
            len(engine_rows), len(oracle_rows)
        )
        return False, reason
    engine_counter = _normalize_rows(engine_rows, decimals)
    oracle_counter = _normalize_rows(oracle_rows, decimals)
    if engine_counter == oracle_counter:
        return True, ""
    return False, _example_difference(engine_counter, oracle_counter)
