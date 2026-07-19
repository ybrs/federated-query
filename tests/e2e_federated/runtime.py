"""Config building, seeding, and the engine adapter for the federated suite.

An ``Environment`` is one placement of one case's tables: it seeds each used slot
(a DuckDB file, a Parquet directory, or a PostgreSQL schema), writes the temp
YAML config that declares those datasources, and lazily builds the native
``fedq.Runtime``. It also holds the table -> fully-qualified-name map the harness
substitutes into the case query.

The suite writes its own config builder here rather than extending the DuckDB-only
``tests/rust_runtime.py`` writer, because this suite mixes DuckDB, Parquet, and
PostgreSQL datasources in one config, which that writer cannot express.
"""

import hashlib
import os
import tempfile

import duckdb
import pyarrow as pa

from tests.e2e_federated import placements as placement_mod

_TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_ADBC_CANDIDATES = (
    "/workspace/venv-fedq/lib/python3.13/site-packages/"
    "adbc_driver_postgresql/libadbc_driver_postgresql.so",
)

DUCK_SCHEMA = "main"
PARQUET_SCHEMA = "main"


def _import_fedq():
    """Import the native ``fedq`` module, putting ``tests/`` on ``sys.path``."""
    so_path = os.path.join(_TESTS_DIR, "fedq.so")
    if not os.path.exists(so_path):
        raise ImportError("tests/fedq.so is missing; build the engine first")
    import sys

    if _TESTS_DIR not in sys.path:
        sys.path.insert(0, _TESTS_DIR)
    import fedq

    return fedq


def adbc_driver_path():
    """Return the first ADBC PostgreSQL driver library that exists, or raise."""
    for candidate in _ADBC_CANDIDATES:
        if os.path.exists(candidate):
            return candidate
    raise RuntimeError("ADBC PostgreSQL driver not found in: " + str(_ADBC_CANDIDATES))


def _env_id(placement_name, spec_names):
    """Return a short stable hash identifying a (placement, table-set) env.

    The hash makes each environment's PostgreSQL schema names globally unique, so
    two cached environments never share a schema and clobber each other's tables.
    """
    joined = placement_name + "|" + ",".join(sorted(spec_names))
    return hashlib.md5(joined.encode("ascii")).hexdigest()[:8]


class BuiltSource:
    """One seeded datasource of an environment: its name, kind, schema, tables."""

    def __init__(self, name, kind, schema, tables):
        """Store the datasource name, kind, schema, and the tables it holds."""
        self.name = name
        self.kind = kind
        self.schema = schema
        self.tables = tables
        self.location = None

    def qualified(self, table):
        """Return the engine's three-part name for one of this source's tables."""
        return self.name + "." + self.schema + "." + table


class Environment:
    """A seeded placement of a case's tables plus a lazily built engine runtime.

    ``sources`` are the used ``BuiltSource``s, ``qualified_map`` maps each table to
    its three-part engine name, and ``source_count`` is the number of distinct
    datasources (used to honor a case's ``min_sources``).
    """

    def __init__(self, sources, qualified_map, config_path):
        """Store the built sources, qualified-name map, and config file path."""
        self.sources = sources
        self.qualified_map = qualified_map
        self.config_path = config_path
        self.source_count = len(sources)
        self._runtime = None

    def render(self, query):
        """Fill a case query's ``{table}`` placeholders with qualified names."""
        return query.format(**self.qualified_map)

    def runtime(self):
        """Build (once) and return the native engine runtime for this config."""
        if self._runtime is None:
            fedq = _import_fedq()
            self._runtime = fedq.Runtime(self.config_path)
        return self._runtime

    def execute(self, query):
        """Run a rendered case query through the engine, returning a pyarrow.Table."""
        rendered = self.render(query)
        return pa.table(self.runtime().execute(rendered))


def build_environment(placement, specs, pg_connection):
    """Seed a placement of the given table specs and return its Environment.

    ``specs`` maps table name -> TableSpec. ``pg_connection`` is a shared psycopg2
    connection used to seed any PostgreSQL slot; it may be ``None`` only when the
    placement uses no PostgreSQL slot.
    """
    used_slots, mapping = placement.assign(specs.keys())
    env_id = _env_id(placement.name, specs.keys())
    sources = _build_sources(placement, used_slots, mapping, specs, env_id, pg_connection)
    qualified_map = _qualified_map(sources)
    config_path = _write_config(sources)
    return Environment(sources, qualified_map, config_path)


def _build_sources(placement, used_slots, mapping, specs, env_id, pg_connection):
    """Seed every used slot and return the ordered list of BuiltSources."""
    sources = []
    for slot in used_slots:
        slot_tables = _tables_for_slot(slot.letter, mapping)
        slot_specs = _slot_specs(slot_tables, specs)
        sources.append(_build_one_source(slot, slot_specs, env_id, pg_connection))
    return sources


def _tables_for_slot(letter, mapping):
    """Return the sorted table names assigned to one slot letter."""
    names = []
    for table, slot_letter in mapping.items():
        if slot_letter == letter:
            names.append(table)
    return sorted(names)


def _slot_specs(table_names, specs):
    """Return the ordered list of TableSpecs for a slot's table names."""
    slot_specs = []
    for name in table_names:
        slot_specs.append(specs[name])
    return slot_specs


def _build_one_source(slot, slot_specs, env_id, pg_connection):
    """Seed one slot according to its kind and return its BuiltSource.

    The datasource name carries ``env_id`` because the engine's exec-plane
    connector registry is keyed by datasource name and shared across every
    runtime in the process; a reused name would make two environments' sources
    collide, so each environment gets uniquely named datasources.
    """
    if slot.kind == placement_mod.DUCK:
        return _build_duck_source(slot, slot_specs, env_id)
    if slot.kind == placement_mod.PARQUET:
        return _build_parquet_source(slot, slot_specs, env_id)
    if slot.kind == placement_mod.PG:
        return _build_pg_source(slot, slot_specs, env_id, pg_connection)
    raise ValueError("unknown slot kind '" + str(slot.kind) + "'")


def _build_duck_source(slot, slot_specs, env_id):
    """Seed a fresh DuckDB file with the slot's tables and return the source."""
    name = "duck_" + slot.letter + "_" + env_id
    source = BuiltSource(name, slot.kind, DUCK_SCHEMA, _spec_names(slot_specs))
    source.location = seed_duckdb_file(slot_specs)
    return source


def _build_parquet_source(slot, slot_specs, env_id):
    """Export the slot's tables to a Parquet directory and return the source."""
    name = "pq_" + slot.letter + "_" + env_id
    source = BuiltSource(name, slot.kind, PARQUET_SCHEMA, _spec_names(slot_specs))
    source.location = seed_parquet_dir(slot_specs)
    return source


def _build_pg_source(slot, slot_specs, env_id, pg_connection):
    """Seed a unique PostgreSQL schema with the slot's tables, return the source."""
    if pg_connection is None:
        raise RuntimeError("a PostgreSQL slot needs a live pg connection")
    schema = "fed_" + slot.letter + "_" + env_id
    name = "pg_" + slot.letter + "_" + env_id
    source = BuiltSource(name, slot.kind, schema, _spec_names(slot_specs))
    seed_pg_schema(pg_connection, schema, slot_specs)
    source.location = schema
    return source


def _spec_names(slot_specs):
    """Return the list of table names carried by a list of TableSpecs."""
    names = []
    for spec in slot_specs:
        names.append(spec.name)
    return names


def seed_duckdb_file(slot_specs):
    """Create a temp DuckDB file, seed each spec, close it, and return its path."""
    path = _temp_path(".duckdb")
    writer = duckdb.connect(path)
    _run_specs(writer, slot_specs)
    writer.close()
    return path


def seed_parquet_dir(slot_specs):
    """Export each spec to ``<dir>/<table>.parquet`` and return the directory."""
    directory = tempfile.mkdtemp(prefix="fedq_pq_")
    writer = duckdb.connect()
    _run_specs(writer, slot_specs)
    _export_parquet(writer, directory, slot_specs)
    writer.close()
    return directory


def _export_parquet(writer, directory, slot_specs):
    """Write one Parquet file per seeded table into the directory."""
    for spec in slot_specs:
        target = os.path.join(directory, spec.name + ".parquet")
        writer.execute(
            "COPY " + spec.name + " TO '" + target + "' (FORMAT PARQUET)"
        )


def seed_pg_schema(pg_connection, schema, slot_specs):
    """Drop and recreate a PostgreSQL schema, then seed the slot's tables in it."""
    cursor = pg_connection.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
    cursor.execute("CREATE SCHEMA " + schema)
    cursor.execute("SET search_path TO " + schema)
    _run_specs(cursor, slot_specs)
    cursor.close()


def _run_specs(executor, slot_specs):
    """Run each spec's DDL and inserts on a DuckDB/psycopg2 cursor-like object."""
    for spec in slot_specs:
        executor.execute(spec.ddl)
        for insert in spec.inserts:
            executor.execute(insert)


def _qualified_map(sources):
    """Return a table -> three-part-name map spanning every built source."""
    mapping = {}
    for source in sources:
        for table in source.tables:
            mapping[table] = source.qualified(table)
    return mapping


def _write_config(sources):
    """Write the temp YAML config declaring every built datasource; return path."""
    lines = ["datasources:"]
    for source in sources:
        _append_source_block(lines, source)
    text = "\n".join(lines) + "\n"
    path = _temp_path(".yaml")
    handle = open(path, "w")
    handle.write(text)
    handle.close()
    return path


def _append_source_block(lines, source):
    """Append one datasource's YAML block for its kind to the config lines."""
    lines.append("  " + source.name + ":")
    if source.kind == placement_mod.DUCK:
        _append_duck_block(lines, source)
    elif source.kind == placement_mod.PARQUET:
        _append_parquet_block(lines, source)
    else:
        _append_pg_block(lines, source)


def _append_duck_block(lines, source):
    """Append a DuckDB datasource block (type + file path)."""
    lines.append("    type: duckdb")
    lines.append("    path: " + source.location)


def _append_parquet_block(lines, source):
    """Append a Parquet datasource block (type + directory)."""
    lines.append("    type: parquet")
    lines.append("    dir: " + source.location)


def _append_pg_block(lines, source):
    """Append a PostgreSQL datasource block (connection + schema + adbc driver)."""
    lines.append("    type: postgres")
    lines.append("    host: localhost")
    lines.append("    port: 5432")
    lines.append("    user: postgres")
    lines.append("    password: postgres")
    lines.append("    database: " + _pg_database())
    lines.append("    schemas:")
    lines.append("      - " + source.schema)
    lines.append("    adbc_driver: " + adbc_driver_path())


def _pg_database():
    """Return the PostgreSQL database name for the suite (env-overridable)."""
    return os.environ.get("POSTGRES_DB", "test_db")


def _temp_path(suffix):
    """Return a fresh, non-existent temp path (DuckDB rejects a pre-made file)."""
    directory = tempfile.mkdtemp(prefix="fedq_e2e_")
    return os.path.join(directory, "source" + suffix)
