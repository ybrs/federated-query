"""The pre-built data every federated e2e test reads, and how it is named.

This module is the single source of truth for what data the suite needs. Three
callers read it and none of them disagree, because there is only one list:

- ``build_test_harness.sh`` builds every source named here, and is the ONLY
  thing in the repository that creates test data.
- ``run_all_tests.sh`` verifies every source named here exists, and dies naming
  what is missing.
- the tests open these sources read-only. A test NEVER creates data. If a source
  is absent the test raises; it does not seed it and it does not skip.

Every name and location is a pure function of (placement, table set), so the
same source is found by the builder, the verifier, and the test without anything
being passed between them. DuckDB files and Parquet directories live under
``.test-harness/`` at the repository root; PostgreSQL sources are schemas in the
test database.
"""

import hashlib
import os

from tests.e2e_federated import placements as placement_mod
from tests.e2e_federated import tables as table_library
from tests.e2e_federated.cases import all_cases
from tests.e2e_federated.cases import case_table_specs

_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

HARNESS_ROOT = os.path.join(_REPO_ROOT, ".test-harness")
DUCK_SCHEMA = "main"
PARQUET_SCHEMA = "main"


class RequiredSource:
    """One datasource the suite needs: its name, kind, location, and tables.

    ``specs`` are the TableSpecs whose DDL and inserts define the data, kept so
    the builder can create it and the verifier can check what it should hold.
    """

    def __init__(self, name, kind, schema, location, specs):
        """Store the datasource name, kind, schema, location, and table specs."""
        self.name = name
        self.kind = kind
        self.schema = schema
        self.location = location
        self.specs = specs

    def table_names(self):
        """Return this source's table names, in the order they are seeded."""
        names = []
        for spec in self.specs:
            names.append(spec.name)
        return names

    def qualified(self, table):
        """Return the engine's three-part name for one of this source's tables."""
        return self.name + "." + self.schema + "." + table


def env_id(placement_name, table_names):
    """Return the stable id identifying one (placement, table-set) environment.

    Hashing the pair keeps each environment's PostgreSQL schema and datasource
    names globally unique, so two environments never share a schema and clobber
    each other's tables.
    """
    joined = placement_name + "|" + ",".join(sorted(table_names))
    return hashlib.md5(joined.encode("ascii")).hexdigest()[:8]


def source_name(kind, letter, identifier):
    """Return the datasource name for one slot of one environment."""
    return _NAME_PREFIX[kind] + letter + "_" + identifier


_NAME_PREFIX = {
    placement_mod.DUCK: "duck_",
    placement_mod.PARQUET: "pq_",
    placement_mod.PG: "pg_",
}


def duck_path(name):
    """Return the DuckDB file path for a datasource name."""
    return os.path.join(HARNESS_ROOT, "duck", name + ".duckdb")


def parquet_dir(name):
    """Return the Parquet directory path for a datasource name."""
    return os.path.join(HARNESS_ROOT, "parquet", name)


def pg_schema(letter, identifier):
    """Return the PostgreSQL schema name for one slot of one environment."""
    return "fed_" + letter + "_" + identifier


def config_path(placement_name, table_names):
    """Return the engine config file path for one (placement, table-set)."""
    name = placement_name + "_" + env_id(placement_name, table_names)
    return os.path.join(HARNESS_ROOT, "configs", name + ".yaml")


def engine_state_paths():
    """Return every path the engine derives from a config to store its own state.

    The engine keeps its learned-statistics catalog and materialized-view chunks
    beside the config it was launched from (``<stem>.stats.sqlite`` and
    ``<stem>.mv/``); the location is not configurable. These are engine state,
    not test data: a run that inherits statistics learned by an earlier run plans
    differently from one that does not, so the suite clears them and every run
    starts from the same state.
    """
    paths = []
    directory = os.path.join(HARNESS_ROOT, "configs")
    if not os.path.isdir(directory):
        return paths
    for entry in sorted(os.listdir(directory)):
        if entry.endswith(".stats.sqlite") or entry.endswith(".mv"):
            paths.append(os.path.join(directory, entry))
    return paths


def config_text(sources):
    """Return the YAML config declaring every datasource of one environment."""
    lines = ["datasources:"]
    for source in sources:
        _append_source_block(lines, source)
    return "\n".join(lines) + "\n"


def _append_source_block(lines, source):
    """Append one datasource's YAML block for its kind to the config lines."""
    lines.append("  " + source.name + ":")
    if source.kind == placement_mod.DUCK:
        lines.append("    type: duckdb")
        lines.append("    path: " + source.location)
        return
    if source.kind == placement_mod.PARQUET:
        lines.append("    type: parquet")
        lines.append("    dir: " + source.location)
        return
    _append_pg_block(lines, source)


def _append_pg_block(lines, source):
    """Append a PostgreSQL datasource block (connection + schema + adbc driver)."""
    lines.append("    type: postgres")
    lines.append("    host: " + os.environ.get("POSTGRES_HOST", "localhost"))
    lines.append("    port: " + os.environ.get("POSTGRES_PORT", "5432"))
    lines.append("    user: " + os.environ.get("POSTGRES_USER", "postgres"))
    lines.append("    password: " + os.environ.get("POSTGRES_PASSWORD", "postgres"))
    lines.append("    database: " + pg_database())
    lines.append("    schemas:")
    lines.append("      - " + source.schema)
    lines.append("    adbc_driver: " + adbc_driver_path())


def adbc_driver_path():
    """Return the ADBC PostgreSQL driver shipped with the installed package.

    The location is derived from the imported ``adbc_driver_postgresql``, so it
    follows whichever interpreter is running the suite; no venv path or Python
    version is written down. A missing package raises ImportError and a package
    without its library raises here; neither is skipped.
    """
    import adbc_driver_postgresql

    package_dir = os.path.dirname(adbc_driver_postgresql.__file__)
    driver = os.path.join(package_dir, "libadbc_driver_postgresql.so")
    if not os.path.exists(driver):
        raise RuntimeError("adbc_driver_postgresql has no driver library at " + driver)
    return driver


def pg_database():
    """Return the PostgreSQL database the suite's sources live in."""
    return os.environ.get("POSTGRES_DB", "test_db")


def sources_for(placement, specs):
    """Return the RequiredSources one (placement, table-set) environment reads.

    This is the function the tests call to find their data, and the same one the
    manifest calls to enumerate everything. A test and the builder therefore
    cannot disagree about where a source is.
    """
    used_slots, mapping = placement.assign(specs.keys())
    identifier = env_id(placement.name, specs.keys())
    sources = []
    for slot in used_slots:
        slot_specs = _specs_for_slot(slot.letter, mapping, specs)
        sources.append(_source_for_slot(slot, slot_specs, identifier))
    return sources


def _specs_for_slot(letter, mapping, specs):
    """Return the TableSpecs assigned to one slot, ordered by table name."""
    names = []
    for table, slot_letter in mapping.items():
        if slot_letter == letter:
            names.append(table)
    slot_specs = []
    for name in sorted(names):
        slot_specs.append(specs[name])
    return slot_specs


def _source_for_slot(slot, slot_specs, identifier):
    """Return the RequiredSource describing one slot's datasource."""
    name = source_name(slot.kind, slot.letter, identifier)
    if slot.kind == placement_mod.DUCK:
        return RequiredSource(
            name, slot.kind, DUCK_SCHEMA, duck_path(name), slot_specs
        )
    if slot.kind == placement_mod.PARQUET:
        return RequiredSource(
            name, slot.kind, PARQUET_SCHEMA, parquet_dir(name), slot_specs
        )
    schema = pg_schema(slot.letter, identifier)
    return RequiredSource(name, slot.kind, schema, schema, slot_specs)


def oracle_name(table_names):
    """Return the datasource name of the single-DuckDB oracle for a table set."""
    return "oracle_" + env_id("oracle", table_names)


def oracle_source(specs):
    """Return the RequiredSource holding a table set's ground-truth DuckDB.

    The oracle is one DuckDB carrying every table of a case, queried with bare
    names. It is built by build_test_harness.sh like every other source, so the
    reference answer a test compares against is data somebody can open and look
    at, not something conjured inside the test run.
    """
    name = oracle_name(specs.keys())
    ordered = []
    for table in sorted(specs.keys()):
        ordered.append(specs[table])
    return RequiredSource(name, placement_mod.DUCK, DUCK_SCHEMA,
                          duck_path(name), ordered)


def environment_keys():
    """Return every (case-tables, placement) environment the corpus needs.

    Yielded as (specs, placement) pairs, deduplicated by (table-set, placement)
    so an environment shared by many cases is described once.
    """
    seen = set()
    keys = []
    for case in all_cases():
        specs = case_table_specs(case)
        for placement in placement_mod.PLACEMENTS:
            if not placement_fits(case, specs, placement):
                continue
            key = (frozenset(specs.keys()), placement.name)
            if key in seen:
                continue
            seen.add(key)
            keys.append((specs, placement))
    return keys


def placement_fits(case, specs, placement):
    """Whether this placement produces a real environment for this case.

    A placement with more slots than the case has tables would leave slots
    empty, and a case declaring ``min_sources`` needs its tables to land on at
    least that many distinct sources.
    """
    if len(specs) < placement.min_tables:
        return False
    used_slots, _mapping = placement.assign(specs.keys())
    return len(used_slots) >= case.get("min_sources", 1)


def manifest():
    """Return every RequiredSource the whole suite needs, keyed by name.

    Raises when two environments claim the same datasource name with different
    table definitions: that would mean the builder created one of them and both
    tests read it, so one of the two would silently run against the wrong data.
    """
    sources = {}
    for specs, placement in environment_keys():
        for source in sources_for(placement, specs):
            _insert_unique(sources, source)
    for specs in _oracle_table_sets():
        _insert_unique(sources, oracle_source(specs))
    return sources


def _oracle_table_sets():
    """Return one specs map per distinct table set the corpus compares against."""
    seen = set()
    sets = []
    for case in all_cases():
        specs = case_table_specs(case)
        key = frozenset(specs.keys())
        if key in seen:
            continue
        seen.add(key)
        sets.append(specs)
    return sets


def _insert_unique(sources, source):
    """Add a source to the manifest, raising on a name clash with other tables."""
    existing = sources.get(source.name)
    if existing is None:
        sources[source.name] = source
        return
    if existing.table_names() != source.table_names():
        raise ValueError(
            "datasource '" + source.name + "' is claimed twice with different "
            "tables: " + str(existing.table_names()) + " vs "
            + str(source.table_names())
        )


def expected_row_counts(specs):
    """Return table name -> row count for the given specs, from their inserts.

    The count is derived by running each spec into an in-memory DuckDB, which is
    the same DDL and the same inserts the builder runs, so the verifier compares
    against what the data is defined to be rather than against a written-down
    number that can drift.
    """
    import duckdb

    connection = duckdb.connect()
    counts = {}
    for spec in specs:
        connection.execute(spec.ddl)
        for insert in spec.inserts:
            connection.execute(insert)
        rows = connection.execute("SELECT count(*) FROM " + spec.name).fetchone()
        counts[spec.name] = rows[0]
    connection.close()
    return counts


def library_table_names():
    """Return every table name the shared table library defines."""
    return sorted(table_library.TABLES.keys())
