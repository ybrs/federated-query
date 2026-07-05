"""Tests for the named estimation defaults and the provenance-carrying
CardinalityEstimate model (M0 of the cost-based join-ordering optimizer)."""

import pytest
from pydantic import ValidationError

from federated_query.datasources.base import ColumnStatistics, TableStatistics
from federated_query.optimizer.estimate_defaults import (
    DEFAULT_ATOM_ROWS,
    DEFAULT_EQ_SELECTIVITY,
    DEFAULT_LIKE_SELECTIVITY,
    DEFAULT_NDV_FRACTION,
    DEFAULT_NULL_FRACTION,
    DEFAULT_RANGE_SELECTIVITY,
    DEFAULT_ROW_COUNT,
    CardinalityEstimate,
    combine_defaults,
)


def test_default_constants_are_the_documented_values():
    """The named defaults are pinned so a silent change shows up in review."""
    assert DEFAULT_ROW_COUNT == 1000
    assert DEFAULT_ATOM_ROWS == 1000
    assert DEFAULT_NDV_FRACTION == 0.1
    assert DEFAULT_EQ_SELECTIVITY == 0.1
    assert DEFAULT_RANGE_SELECTIVITY == 0.33
    assert DEFAULT_LIKE_SELECTIVITY == 0.1
    assert DEFAULT_NULL_FRACTION == 0.05


def test_estimate_create_carries_rows_and_provenance():
    """An estimate names every default that fed it, tied to its target."""
    estimate = CardinalityEstimate.create(
        rows=42, defaults_used=["ndv(duck.main.lineitem.l_suppkey)"]
    )
    assert estimate.rows == 42
    assert estimate.defaults_used == ["ndv(duck.main.lineitem.l_suppkey)"]


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
