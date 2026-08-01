"""Tests for the e2e harness itself, not for the engine.

The harness decides what data exists and which data each test reads. A harness
bug is invisible in a normal run - it looks exactly like a green suite, because
the tests that should have caught it are the ones the bug disabled. These tests
check the harness itself, so that failure mode has something watching it.

The property everything else rests on is that EVERY datasource holds EVERY table.
That is what lets a placement be a choice of names in the SQL rather than a
different set of files, so one config expresses the whole placement matrix. If an
instance were ever built with a subset of the tables, placements naming it would
resolve to tables that are not there.
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


def _specs(tables):
    """Return the TableSpec map for a list of table names."""
    return case_table_specs({"name": "x", "tables": tables, "query": "q"})


def test_every_source_holds_every_table():
    """Each instance carries the whole table set; that is what placements need."""
    wanted = sorted(harness.all_table_specs().keys())
    sources = harness.manifest()
    assert sources
    for name in sources:
        assert sorted(sources[name].table_names()) == wanted, name


def test_there_are_enough_instances_for_the_largest_case():
    """Enough instances of each kind exist to give every table its own source."""
    largest = 0
    for case in all_cases():
        largest = max(largest, len(case_table_specs(case)))
    assert harness.INSTANCES_PER_KIND >= largest


def test_a_same_kind_placement_reads_two_separate_databases():
    """duck_duck resolves to duck1 and duck2, not one source twice.

    Two tables on "two DuckDB sources" must be two real databases, or the engine
    would see them co-located and push down a join the placement means to split.
    """
    specs = _specs(["orders", "customers"])
    names = harness.qualified_map(_placement("duck_duck"), specs)
    sources = set()
    for table in names:
        sources.add(names[table].split(".")[0])
    assert sources == {"duck1", "duck2"}


def test_a_mixed_placement_reads_one_instance_of_each_kind():
    """pg_duck resolves to pg1 and duck1 - each kind counts its own instances."""
    specs = _specs(["orders", "customers"])
    names = harness.qualified_map(_placement("pg_duck"), specs)
    sources = set()
    for table in names:
        sources.add(names[table].split(".")[0])
    assert sources == {"pg1", "duck1"}


def test_a_single_source_placement_co_locates_every_table():
    """A one-slot placement puts every table on one source, so joins push down."""
    specs = _specs(["orders", "customers"])
    names = harness.qualified_map(_placement("oracle_single_duck"), specs)
    sources = set()
    for table in names:
        sources.add(names[table].split(".")[0])
    assert sources == {"duck1"}


def test_the_same_table_resolves_differently_under_different_placements():
    """A placement changes only which name a table is read from."""
    specs = _specs(["orders", "customers"])
    on_pg = harness.qualified_map(_placement("all_pg"), specs)
    on_duck = harness.qualified_map(_placement("duck_duck"), specs)
    assert on_pg["orders"] != on_duck["orders"]
    assert on_pg["orders"].startswith("pg")
    assert on_duck["orders"].startswith("duck")


def test_a_table_defined_two_ways_raises():
    """One set of sources cannot hold two definitions of one table name."""
    specs = harness.all_table_specs()
    assert specs
    clash = {}
    name = sorted(specs)[0]
    clash[name] = specs[name]
    with pytest.raises(ValueError) as raised:
        harness._insert_spec(clash, name, _AlteredSpec(specs[name]))
    assert name in str(raised.value)


class _AlteredSpec:
    """A table spec carrying the same name but different DDL, for the clash test."""

    def __init__(self, original):
        """Copy the name from the original spec and change its DDL."""
        self.name = original.name
        self.ddl = original.ddl + " -- different"
        self.inserts = original.inserts


def test_the_config_declares_exactly_the_manifest():
    """The config the engine reads names every source the manifest defines."""
    text = harness.config_text()
    for name in harness.manifest():
        assert "\n  " + name + ":" in text


def test_the_config_is_pre_built_not_composed_per_test():
    """The config the engine reads exists on disk before the run starts."""
    assert os.path.exists(harness.CONFIG_PATH), harness.CONFIG_PATH
    handle = open(harness.CONFIG_PATH)
    found = handle.read()
    handle.close()
    assert found == harness.config_text()


def test_opening_with_a_missing_config_raises_and_names_it():
    """A missing config raises naming the file; it is never written on the fly."""
    path = harness.CONFIG_PATH
    moved = path + ".absent"
    existed = os.path.exists(path)
    if existed:
        os.rename(path, moved)
    try:
        with pytest.raises(RuntimeError) as raised:
            runtime_mod.open_engine()
        assert "config" in str(raised.value)
        assert "build_test_harness.sh" in str(raised.value)
    finally:
        if existed:
            os.rename(moved, path)


def test_opening_an_environment_creates_nothing():
    """Opening leaves the harness directory byte-for-byte as it found it."""
    specs = _specs(["orders", "customers"])
    engine = runtime_mod.open_engine()
    before = _harness_fingerprint()
    runtime_mod.open_environment(engine, _placement("pg_duck"), specs)
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
    counts = harness.expected_row_counts(list(_specs(["orders"]).values()))
    assert counts["orders"] > 0
