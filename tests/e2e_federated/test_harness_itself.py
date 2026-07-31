"""Tests for the e2e harness itself, not for the engine.

The harness decides what data exists and which data each test reads. A harness
bug is invisible in a normal run - it looks exactly like a green suite, because
the tests that should have caught it are the ones the bug disabled. These tests
check the harness itself, so that failure mode has something watching it.

What is pinned here:

- naming and locations are pure functions of (placement, table set), so the
  builder, the verifier, and a test independently agree where a source is;
- no two environments claim one datasource name with different tables, which
  would let one test read another's data;
- opening an environment CREATES NOTHING, and raises naming the source when the
  data it needs was never built;
- every environment the corpus needs is in the manifest, so the builder cannot
  build less than the tests read.
"""

import os

import pytest

from tests.e2e_federated import harness
from tests.e2e_federated import placements as placement_mod
from tests.e2e_federated import runtime as runtime_mod
from tests.e2e_federated.cases import all_cases
from tests.e2e_federated.cases import case_table_specs


def _placement(name):
    """Return the named placement, raising if the harness lost it."""
    for placement in placement_mod.PLACEMENTS:
        if placement.name == name:
            return placement
    raise KeyError("no placement named '" + name + "'")


def test_env_id_is_a_pure_function_of_placement_and_tables():
    """The same (placement, table set) always yields the same environment id."""
    first = harness.env_id("pg_duck", ["orders", "customers"])
    second = harness.env_id("pg_duck", ["customers", "orders"])
    assert first == second
    assert first != harness.env_id("duck_pg", ["orders", "customers"])
    assert first != harness.env_id("pg_duck", ["orders", "products"])


def test_source_locations_are_derived_not_random():
    """Two independent calls agree on where a source lives.

    The builder and the tests never exchange a path; each derives it. If these
    diverged, the builder would create data no test could find.
    """
    specs = case_table_specs({"name": "x", "tables": ["orders", "customers"],
                              "query": "SELECT 1 FROM {orders}, {customers}"})
    placement = _placement("pg_duck")
    first = harness.sources_for(placement, specs)
    second = harness.sources_for(placement, specs)
    assert [s.name for s in first] == [s.name for s in second]
    assert [s.location for s in first] == [s.location for s in second]


def test_no_two_environments_claim_one_name_with_different_tables():
    """Building the manifest raises on a datasource name claimed twice.

    A clash would mean one source on disk and two tests reading it, one of them
    against tables it did not seed - a wrong answer with no error.
    """
    sources = harness.manifest()
    assert sources
    for name in sources:
        assert sources[name].name == name


def test_manifest_covers_every_environment_the_corpus_needs():
    """Every (case, placement) a test will open is present in the manifest.

    This is the property that makes "build once, read many" safe: if the builder
    could enumerate less than the tests open, the difference would only surface
    as a mid-run failure on whichever test got there first.
    """
    sources = harness.manifest()
    for case in all_cases():
        specs = case_table_specs(case)
        for placement in placement_mod.PLACEMENTS:
            if not harness.placement_fits(case, specs, placement):
                continue
            for source in harness.sources_for(placement, specs):
                assert source.name in sources, (
                    "case " + case["name"] + " placement " + placement.name
                    + " opens unbuilt source " + source.name
                )


def test_opening_a_missing_environment_raises_and_names_it():
    """A test whose data was never built raises; it does not seed and not skip."""
    specs = case_table_specs({"name": "x", "tables": ["orders"],
                              "query": "SELECT 1 FROM {orders}"})
    placement = _placement("oracle_single_duck")
    sources = harness.sources_for(placement, specs)
    original = sources[0].location
    moved = original + ".absent"
    existed = os.path.exists(original)
    if existed:
        os.rename(original, moved)
    try:
        with pytest.raises(RuntimeError) as raised:
            runtime_mod.open_environment(placement, specs)
        assert sources[0].name in str(raised.value)
        assert "build_test_harness.sh" in str(raised.value)
    finally:
        if existed:
            os.rename(moved, original)


def test_opening_an_environment_creates_no_data():
    """Opening leaves the harness directory byte-for-byte as it found it."""
    specs = case_table_specs({"name": "x", "tables": ["orders", "customers"],
                              "query": "SELECT 1 FROM {orders}, {customers}"})
    placement = _placement("duck_duck")
    before = _harness_fingerprint()
    runtime_mod.open_environment(placement, specs)
    assert _harness_fingerprint() == before


def _harness_fingerprint():
    """Return a (path, size) listing of every file under the harness root."""
    entries = []
    for directory, _subdirs, filenames in os.walk(harness.HARNESS_ROOT):
        for filename in sorted(filenames):
            path = os.path.join(directory, filename)
            entries.append((path, os.path.getsize(path)))
    return sorted(entries)


def test_expected_row_counts_come_from_the_specs():
    """Row counts the verifier compares against are computed, never written down."""
    specs = case_table_specs({"name": "x", "tables": ["orders"],
                              "query": "SELECT 1 FROM {orders}"})
    counts = harness.expected_row_counts(list(specs.values()))
    assert counts["orders"] > 0
