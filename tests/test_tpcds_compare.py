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


def test_pure_reorder_with_identical_multiset_matches():
    """An order-only difference with identical multisets is a match.

    Rows tied on their ORDER BY keys are legitimately unordered (TPC answer
    set convention); a genuine mis-sort under ORDER BY ... LIMIT changes
    WHICH rows survive and still fails via the multiset compare.
    """
    engine = [(1,), (2,)]
    oracle = [(2,), (1,)]
    match, _ = compare_results(engine, oracle, decimals=2)
    assert match


def test_tie_reordered_rows_match_as_multiset():
    """Rows tied on their sort keys may interleave differently (SF10 q18/q65/
    q71): an order-only difference with identical multisets is a match."""
    engine = [("able", None, 3.23), ("able", None, 8.25)]
    oracle = [("able", None, 8.25), ("able", None, 3.23)]
    match, _ = compare_results(engine, oracle, decimals=2)
    assert match


def test_reordered_rows_with_a_real_difference_still_fail():
    """Reordering must not mask a genuine value difference."""
    engine = [("able", 3.23), ("able", 8.25)]
    oracle = [("able", 8.25), ("able", 3.99)]
    match, reason = compare_results(engine, oracle, decimals=2)
    assert not match
    assert "differs" in reason
