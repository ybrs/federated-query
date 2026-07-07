"""Unit tests for the TPC-DS differential result comparator.

The comparator rounds numerics to a fixed number of decimals; a float64
aggregate whose last bits differ by summation order can sit exactly on a
rounding boundary, where the rounding AMPLIFIES a 1e-14 difference into a
visible cent (q18). The tolerance check must absorb that without masking
real value differences.
"""

import os
import sys

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "benchmarks", "tpcds"),
)

from compare import compare_results  # noqa: E402


def test_rounding_boundary_float_noise_matches():
    """A last-bit summation difference on a cent boundary is a match (q18)."""
    engine = [("AAAA", 206.98499999999999, -1022.4150000000002)]
    oracle = [("AAAA", 206.985, -1022.415)]
    match, reason = compare_results(engine, oracle, decimals=2)
    assert match, reason


def test_real_value_difference_still_fails():
    """A genuine cent difference (far beyond float noise) must still fail."""
    engine = [("AAAA", 206.99)]
    oracle = [("AAAA", 206.98)]
    match, reason = compare_results(engine, oracle, decimals=2)
    assert not match
    assert "row 0 differs" in reason


def test_rounded_equality_still_matches():
    """Values equal after rounding match exactly as before."""
    engine = [(1, 10.004)]
    oracle = [(1, 10.001)]
    match, _ = compare_results(engine, oracle, decimals=2)
    assert match


def test_string_and_null_cells_unaffected():
    """Non-numeric cells keep strict comparison; NULL matches only NULL."""
    match, _ = compare_results([("a", None)], [("a", None)], decimals=2)
    assert match
    match, _ = compare_results([("a", None)], [("b", None)], decimals=2)
    assert not match


def test_row_order_still_significant():
    """Out-of-order rows still fail, described as an ordering difference."""
    engine = [(1,), (2,)]
    oracle = [(2,), (1,)]
    match, reason = compare_results(engine, oracle, decimals=2)
    assert not match
    assert "order differs" in reason
