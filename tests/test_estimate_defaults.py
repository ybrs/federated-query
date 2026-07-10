"""Tests for the provenance-carrying CardinalityEstimate model and its
combination helpers. The fabricated estimation constants are GONE: a statistic
is a measurement or it is UNKNOWN (None) - these tests pin that regime."""

import pytest
from pydantic import ValidationError

from federated_query.datasources.base import ColumnStatistics, TableStatistics
from federated_query.optimizer import estimate_defaults
from federated_query.optimizer.estimate_defaults import (
    CardinalityEstimate,
    apply_conjunct_term,
    combine_defaults,
    max_known_ndv,
)


def test_no_fabricated_stat_constants_exist():
    """The fabricated defaults must never come back: a made-up row count next
    to a measured one inverts every larger-side decision (the verified q39
    cold-source regression). Only cost WEIGHTS and decision THRESHOLDS remain."""
    banned = [
        "DEFAULT_ROW_COUNT",
        "DEFAULT_ATOM_ROWS",
        "DEFAULT_NDV_FRACTION",
        "DEFAULT_EQ_SELECTIVITY",
        "DEFAULT_RANGE_SELECTIVITY",
        "DEFAULT_LIKE_SELECTIVITY",
        "DEFAULT_NULL_FRACTION",
    ]
    for name in banned:
        assert not hasattr(estimate_defaults, name), name


def test_estimate_create_carries_rows_and_provenance():
    """An estimate names every statistics gap that fed it, tied to its target."""
    estimate = CardinalityEstimate.create(
        rows=42, defaults_used=["ndv(duck.main.lineitem.l_suppkey)"]
    )
    assert estimate.rows == 42
    assert estimate.defaults_used == ["ndv(duck.main.lineitem.l_suppkey)"]


def test_estimate_rows_can_be_unknown():
    """An UNKNOWN estimate is rows=None with its gap recorded - never a
    fabricated number."""
    estimate = CardinalityEstimate.create(
        rows=None, defaults_used=["row_count(pg.public.inventory)"]
    )
    assert estimate.rows is None
    assert estimate.defaults_used == ["row_count(pg.public.inventory)"]


def test_estimate_rejects_negative_rows():
    """A negative row estimate is a computation bug; construction must raise."""
    with pytest.raises(ValidationError):
        CardinalityEstimate.create(rows=-1, defaults_used=[])


def test_estimate_rejects_unknown_kwargs():
    """StateModel extra=forbid: a mistyped field name raises."""
    with pytest.raises(ValidationError):
        CardinalityEstimate(rows=1, defaults_used=[], row_cnt=2)


def test_combine_defaults_unions_in_order_without_duplicates():
    """Provenance from both join inputs merges, first occurrence wins."""
    left = CardinalityEstimate.create(rows=10, defaults_used=["a", "b"])
    right = CardinalityEstimate.create(rows=20, defaults_used=["b", "c"])
    merged = combine_defaults([left, right], ["d", "a"])
    assert merged == ["a", "b", "c", "d"]


def test_combine_defaults_empty_inputs():
    """No parents and no extras merge to an empty provenance list."""
    assert combine_defaults([], []) == []


def test_max_known_ndv_ignores_unknown_sides():
    """One known join-key NDV is a valid denominator on its own (containment);
    both unknown stays unknown."""
    assert max_known_ndv(100, 7) == 100
    assert max_known_ndv(None, 7) == 7
    assert max_known_ndv(100, None) == 100
    assert max_known_ndv(None, None) is None


def test_apply_conjunct_term_unknown_prices_at_its_bound():
    """An UNKNOWN equi NDV leaves the denominator alone (floor 1) but still
    counts toward the composite cap; an UNKNOWN selectivity reduces nothing
    (ceiling 1.0)."""
    denom, selectivity, equi = apply_conjunct_term(True, None, 1.0, 1.0, 0)
    assert (denom, selectivity, equi) == (1.0, 1.0, 1)
    denom, selectivity, equi = apply_conjunct_term(False, None, 1.0, 1.0, 0)
    assert (denom, selectivity, equi) == (1.0, 1.0, 0)


def test_apply_conjunct_term_known_values_fold_in():
    """A known equi NDV multiplies the denominator; a known selectivity
    multiplies the reduction factor."""
    denom, selectivity, equi = apply_conjunct_term(True, 50, 2.0, 1.0, 1)
    assert (denom, selectivity, equi) == (100.0, 1.0, 2)
    denom, selectivity, equi = apply_conjunct_term(False, 0.25, 2.0, 0.5, 1)
    assert (denom, selectivity, equi) == (2.0, 0.125, 1)


def test_table_statistics_row_count_can_be_unknown():
    """A source that does not know its row count reports None, never a guess."""
    stats = TableStatistics.create(
        row_count=None, total_size_bytes=0, column_stats={}
    )
    assert stats.row_count is None


def test_column_statistics_num_distinct_can_be_unknown():
    """A source that cannot provide NDV reports None, never a fabricated count."""
    stats = ColumnStatistics.create(
        num_distinct=None, null_fraction=0.0, avg_width=10
    )
    assert stats.num_distinct is None
