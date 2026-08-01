"""The data every federated e2e test reads, and how it is named.

The whole suite reads ONE config declaring a fixed set of datasources: several
independent instances of each kind (DuckDB, PostgreSQL, Parquet), and every
instance holds EVERY table the corpus can reference.

That is what makes a placement a property of the QUERY rather than of the data.
Putting ``orders`` on PostgreSQL and ``customers`` on DuckDB is not a different
set of files, it is a different pair of names in the SQL::

    SELECT ... FROM pg1.<schema>.orders  JOIN duck1.main.customers ...
    SELECT ... FROM duck1.main.orders    JOIN pg1.<schema>.customers ...
    SELECT ... FROM pg1.<schema>.orders  JOIN pg1.<schema>.customers ...

The engine decides what it can push down from which datasource each REFERENCED
table belongs to; the other tables sitting in that source are not part of the
query and do not change the plan. So one config expresses every placement, and a
new placement costs a string rather than a build.

Several instances per kind exist because a placement may split tables across two
separate databases of the SAME kind (two DuckDB files, two PostgreSQL schemas).
``INSTANCES_PER_KIND`` is the largest number of tables any case declares, so
every table of any case can land on an instance of its own.

Three callers read this module and cannot disagree, because there is one list:
``build_test_harness.sh`` builds it, ``run_all_tests.sh`` verifies it, and the
tests open it read-only and raise if it is absent.
"""

import hashlib
import os

from tests.e2e_federated import placements as placement_mod
from tests.e2e_federated.cases import all_cases
from tests.e2e_federated.cases import case_table_specs

_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

HARNESS_ROOT = os.path.join(_REPO_ROOT, ".test-harness")
CONFIG_PATH = os.path.join(HARNESS_ROOT, "config.yaml")

DUCK_SCHEMA = "main"
PARQUET_SCHEMA = "main"

# One instance per table a case could split onto its own source. The corpus's
# largest case declares six tables, so six instances of each kind can express
# every way of scattering it, including all six onto separate sources.
INSTANCES_PER_KIND = 6

_NAME_PREFIX = {
    placement_mod.DUCK: "duck",
    placement_mod.PARQUET: "pq",
    placement_mod.PG: "pg",
}


class RequiredSource:
    """One datasource the suite reads: its name, kind, location, and tables.

    Every instance carries the same ``specs`` - every table the corpus can name -
    because which source a table is READ from is chosen per query, not baked in.
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


def all_table_specs():
    """Return name -> TableSpec for every table any case can reference.

    Cases draw from the shared table library and may also declare inline
    ``extra_tables``. Every instance holds all of them, so any case can be placed
    on any source. Two cases defining one name differently raises: the sources
    can only hold one of the two, so the other would silently read wrong data.
    """
    specs = {}
    for case in all_cases():
        for name, spec in case_table_specs(case).items():
            _insert_spec(specs, name, spec)
    return specs


def _insert_spec(specs, name, spec):
    """Add one table spec, raising when a name is defined two different ways."""
    existing = specs.get(name)
    if existing is None:
        specs[name] = spec
        return
    if existing.ddl != spec.ddl or existing.inserts != spec.inserts:
        raise ValueError(
            "table '" + name + "' is defined two different ways by different "
            "cases; one set of sources cannot hold both"
        )


def ordered_specs():
    """Return every table spec, ordered by name so builds are reproducible."""
    specs = all_table_specs()
    ordered = []
    for name in sorted(specs):
        ordered.append(specs[name])
    return ordered


def source_name(kind, index):
    """Return the datasource name of one instance of one kind (1-based)."""
    return _NAME_PREFIX[kind] + str(index)


def pg_schema(index):
    """Return the PostgreSQL schema name backing one PostgreSQL instance."""
    return "fed_pg" + str(index)


def duck_path(name):
    """Return the DuckDB file path for a datasource name."""
    return os.path.join(HARNESS_ROOT, "duck", name + ".duckdb")


def parquet_dir(name):
    """Return the Parquet directory path for a datasource name."""
    return os.path.join(HARNESS_ROOT, "parquet", name)


def pg_database():
    """Return the PostgreSQL database the suite's sources live in."""
    return os.environ.get("POSTGRES_DB", "test_db")


def source_for(kind, index):
    """Return the RequiredSource describing one instance of one kind."""
    name = source_name(kind, index)
    specs = ordered_specs()
    if kind == placement_mod.DUCK:
        return RequiredSource(name, kind, DUCK_SCHEMA, duck_path(name), specs)
    if kind == placement_mod.PARQUET:
        return RequiredSource(name, kind, PARQUET_SCHEMA, parquet_dir(name), specs)
    schema = pg_schema(index)
    return RequiredSource(name, kind, schema, schema, specs)


def manifest():
    """Return every RequiredSource the suite reads, keyed by datasource name."""
    sources = {}
    for kind in (placement_mod.DUCK, placement_mod.PG, placement_mod.PARQUET):
        for index in range(1, INSTANCES_PER_KIND + 1):
            source = source_for(kind, index)
            sources[source.name] = source
    return sources


def instance_indexes(placement):
    """Return slot letter -> instance index for one placement.

    A slot takes the next free instance of ITS OWN kind, so a placement holding
    two DuckDB slots reads duck1 and duck2 - two separate databases - while a
    placement holding one DuckDB and one PostgreSQL slot reads duck1 and pg1.
    """
    seen = {}
    indexes = {}
    for slot in placement.slots:
        seen[slot.kind] = seen.get(slot.kind, 0) + 1
        indexes[slot.letter] = seen[slot.kind]
    return indexes


def qualified_map(placement, specs):
    """Return table -> three-part engine name for one placement of a table set.

    This is where a placement becomes real: it picks, per table, which instance
    the query reads it from. Nothing is built and nothing is moved.
    """
    _used_slots, mapping = placement.assign(specs.keys())
    indexes = instance_indexes(placement)
    names = {}
    for table in sorted(specs.keys()):
        slot = placement.slot_by_letter(mapping[table])
        source = source_for(slot.kind, indexes[slot.letter])
        names[table] = source.qualified(table)
    return names


def placement_sources(placement, specs):
    """Return the RequiredSources a placement actually reads from."""
    used_slots, _mapping = placement.assign(specs.keys())
    indexes = instance_indexes(placement)
    sources = []
    for slot in used_slots:
        sources.append(source_for(slot.kind, indexes[slot.letter]))
    return sources


class Variation:
    """One distinct way to distribute a case's tables across datasources.

    ``assignment`` maps each table to the datasource name it is read from. Two
    tables sharing a name are co-located and the engine may push their join into
    that source; two tables on different names must be brought together by the
    coordinator. That distinction is the whole subject of this suite, so every
    distinct assignment is a distinct test.
    """

    def __init__(self, assignment):
        """Store the table -> datasource-name assignment."""
        self.assignment = assignment
        self.name = _variation_name(assignment)
        self.source_count = len(set(assignment.values()))

    def qualified_map(self):
        """Return table -> three-part engine name for this distribution."""
        names = {}
        for table in self.assignment:
            names[table] = _qualify(self.assignment[table], table)
        return names


def _variation_name(assignment):
    """Return the id of a variation: its source per table, in table order."""
    parts = []
    for table in sorted(assignment):
        parts.append(assignment[table])
    return "-".join(parts)


def _qualify(datasource, table):
    """Return the three-part engine name of a table in a named datasource."""
    if datasource.startswith("pg"):
        return datasource + "." + pg_schema(int(datasource[2:])) + "." + table
    return datasource + "." + DUCK_SCHEMA + "." + table


def variations(specs, min_sources):
    """Return every distinct distribution of a table set across datasources.

    A distribution is a partition of the tables into groups - the groups are the
    co-located sets - with each group placed on a source of one kind. Every
    partition and every kind labelling is generated; nothing is sampled and
    nothing is skipped, so a case is tested under every arrangement it has.

    ``min_sources`` drops arrangements with fewer distinct sources than the case
    requires. That is a property of the case, so those arrangements are never
    generated rather than generated and skipped.
    """
    tables = sorted(specs.keys())
    found = []
    for blocks in _partitions(tables):
        if len(blocks) < min_sources:
            continue
        _extend_with_labellings(found, blocks)
    return found


def _extend_with_labellings(found, blocks):
    """Append one Variation per kind labelling of an ordered set of blocks."""
    import itertools

    ordered = sorted(blocks, key=lambda block: sorted(block)[0])
    kinds = (placement_mod.DUCK, placement_mod.PG, placement_mod.PARQUET)
    for labelling in itertools.product(kinds, repeat=len(ordered)):
        found.append(Variation(_assign(ordered, labelling)))


def _assign(ordered_blocks, labelling):
    """Return table -> datasource name for one labelled partition.

    Instances are numbered per kind in block order, so two DuckDB groups read
    duck1 and duck2 - two separate databases - never one source twice.
    """
    counts = {}
    assignment = {}
    for index, block in enumerate(ordered_blocks):
        kind = labelling[index]
        counts[kind] = counts.get(kind, 0) + 1
        name = source_name(kind, counts[kind])
        for table in block:
            assignment[table] = name
    return assignment


def _partitions(items):
    """Yield every partition of ``items`` as a list of non-empty blocks."""
    if not items:
        yield []
        return
    first = items[0]
    for smaller in _partitions(items[1:]):
        for index in range(len(smaller)):
            grown = list(smaller)
            grown[index] = [first] + grown[index]
            yield grown
        yield [[first]] + smaller


def placement_fits(case, specs, placement):
    """Whether this placement produces a real environment for this case.

    A placement with more slots than the case has tables would leave slots empty,
    and a case declaring ``min_sources`` needs its tables to land on at least that
    many distinct sources.
    """
    if len(specs) < placement.min_tables:
        return False
    used_slots, _mapping = placement.assign(specs.keys())
    return len(used_slots) >= case.get("min_sources", 1)


def oracle_source():
    """Return the source holding the single-DuckDB ground truth.

    Every instance carries every table, so the first DuckDB instance already IS
    the all-tables oracle database; there is nothing extra to build.
    """
    return source_for(placement_mod.DUCK, 1)


def config_text():
    """Return the YAML config declaring every datasource the suite reads."""
    lines = ["datasources:"]
    sources = manifest()
    for name in sorted(sources):
        _append_source_block(lines, sources[name])
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


def engine_state_paths():
    """Return every path the engine derives from the config to store its state.

    The engine keeps its learned-statistics catalog and materialized-view chunks
    beside the config it was launched from (``<stem>.stats.sqlite`` and
    ``<stem>.mv/``); the location is not configurable. These are engine state,
    not test data: a run that inherits statistics learned by an earlier run plans
    differently from one that does not, so the suite clears them and every run
    starts from the same state.
    """
    stem = os.path.splitext(CONFIG_PATH)[0]
    paths = []
    for candidate in (stem + ".stats.sqlite", stem + ".mv"):
        if os.path.exists(candidate):
            paths.append(candidate)
    return paths


def expected_row_counts(specs):
    """Return table name -> row count for the given specs, from their inserts.

    The count is derived by running each spec into an in-memory DuckDB, which is
    the same DDL and the same inserts the builder runs, so the verifier compares
    against what the data is defined to be rather than a written-down number.
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


def table_set_digest(table_names):
    """Return a short stable digest of a table set, for cache keys and ids."""
    joined = ",".join(sorted(table_names))
    return hashlib.md5(joined.encode("ascii")).hexdigest()[:8]
