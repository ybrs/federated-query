"""The single source-identity predicate used by every pushdown decision."""

from federated_query.optimizer.single_source_pushdown import same_source


def test_equal_names_are_same_source():
    """Two equal datasource names identify the same source."""
    assert same_source("pg", "pg")


def test_different_names_are_not_same_source():
    """Two different datasource names are different sources."""
    assert not same_source("pg", "duckdb")


def test_none_matches_nothing():
    """A None name (no source claimed yet) matches nothing, including None."""
    assert not same_source(None, "pg")
    assert not same_source("pg", None)
    assert not same_source(None, None)
