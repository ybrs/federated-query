"""Dump the NEW (Rust) engine's pushed SQL per query, from the trace.

Runs each query through fedq.Runtime(cfg).execute(sql) with FEDQRS_TRACE_SQL=1
and captures the per-source SQL the Rust engine prints to stderr ([fedqsql]
blocks). Because the trace is written by native code to file descriptor 2, we
redirect fd 2 to a temp file around each execution (a sys.stderr swap would not
catch native writes). Per-query errors (including Rust panics) are caught and
recorded as a finding.

Usage: python dump_new.py <tpch|tpcds>
Writes benchmarks/plan_compare/new/<suite>/<q>.txt
"""

import glob
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))

# The duck source path and pg database for each suite's smallest loaded scale.
SUITE_SCALE = {"tpch": "0.01", "tpcds": "0.1"}
SUITE_PG_DB = {"tpch": "duckpoc", "tpcds": "tpcds_sf01"}


def _bench_dir(suite):
    """Absolute path to a suite's benchmark directory (has fedq.so + queries)."""
    return os.path.join(ROOT, "benchmarks", suite)


def _adbc_driver_path():
    """First existing ADBC Postgres driver shared library."""
    candidates = [
        "/workspace/venv-fedq/lib/python3.13/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
        "/workspace/venv/lib/python3.12/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    raise SystemExit("ADBC Postgres driver not found")


def _write_config(db_path, pg_database, adbc_driver):
    """Write a temp YAML config: a DuckDB source and a PostgreSQL source."""
    text = (
        "datasources:\n"
        "  duck:\n"
        "    type: duckdb\n"
        "    path: {db_path}\n"
        "    read_only: true\n"
        "  pg:\n"
        "    type: postgres\n"
        "    host: localhost\n"
        "    port: 5432\n"
        "    user: postgres\n"
        "    password: postgres\n"
        "    database: {database}\n"
        "    schemas:\n"
        "      - public\n"
        "    adbc_driver: {adbc_driver}\n"
    ).format(db_path=db_path, database=pg_database, adbc_driver=adbc_driver)
    handle = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    handle.write(text)
    handle.close()
    return handle.name


def _classify_source(uri):
    """Return 'duck' or 'pg' for a trace source uri."""
    if ".duckdb" in uri or uri.endswith(".db"):
        return "duck"
    return "pg"


# Postgres catalog / statistics probe queries the connector issues to fetch
# metadata (index keys, relpages). They are NOT query pushdowns, so they are
# excluded from the island comparison (the old engine walks the plan statically
# and never emits them).
CATALOG_PROBE_MARKERS = (
    "pg_index", "pg_class", "pg_namespace", "pg_attribute", "pg_statistic",
    "pg_stats", "relpages", "information_schema",
)


def _is_catalog_probe(sql):
    """Whether a traced SQL is a metadata/stats probe rather than a pushdown."""
    for marker in CATALOG_PROBE_MARKERS:
        if marker in sql:
            return True
    return False


def _parse_trace(text):
    """Split captured stderr into (source, kind, sql) island blocks.

    Each [fedqsql] block is 'source=<uri>' then the SQL. Catalog/stats probe
    queries are dropped; kind classifies the rest by content (join/agg/scan).
    """
    islands = []
    chunks = text.split("[fedqsql] source=")
    for chunk in chunks[1:]:
        lines = chunk.splitlines()
        uri = lines[0].strip()
        sql = "\n".join(lines[1:]).strip()
        if not sql or _is_catalog_probe(sql):
            continue
        islands.append((_classify_source(uri), _island_kind(sql), sql))
    return islands


def _island_kind(sql):
    """Classify a pushed SQL by CONTENT: 'join' > 'agg' > 'scan'."""
    upper = sql.upper()
    if " JOIN " in upper:
        return "join"
    aggregates = ("GROUP BY", "SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "STDDEV")
    for marker in aggregates:
        if marker in upper:
            return "agg"
    return "scan"


def _pushes_agg_join(islands):
    """Whether any island is a structural pushdown (join/aggregate)."""
    for _source, kind, _sql in islands:
        if kind in ("join", "agg"):
            return True
    return False


def _has_shipment(islands):
    """A shipment shows as an island whose SQL reads a __fedq_ship temp table
    (the shipment's CREATE/INSERT itself is not a traced [fedqsql] block)."""
    for _source, _kind, sql in islands:
        if "__fedq_ship" in sql:
            return True
    return False


def _capture_execute(runtime, engine_sql):
    """Execute a query with fd-2 redirected to a temp file; return (trace, err).

    The Rust engine writes [fedqsql] to fd 2 directly, so we dup2 a temp file
    over fd 2 for the duration of the call and read it back afterwards.
    """
    trace_file = tempfile.NamedTemporaryFile("w+", delete=False)
    trace_path = trace_file.name
    trace_file.close()
    saved = os.dup(2)
    target = os.open(trace_path, os.O_WRONLY | os.O_TRUNC)
    os.dup2(target, 2)
    os.close(target)
    error = None
    try:
        runtime.execute(engine_sql)
    except BaseException as caught:
        detail = str(caught).strip().splitlines()
        head = detail[0] if detail else type(caught).__name__
        error = "{0}: {1}".format(type(caught).__name__, head)
    finally:
        os.dup2(saved, 2)
        os.close(saved)
    with open(trace_path) as handle:
        trace = handle.read()
    os.remove(trace_path)
    return trace, error


def _write_report(out_path, name, islands, error):
    """Write one query's new-engine pushed-SQL report."""
    lines = ["QUERY: {0}".format(name), "ENGINE: new (rust)", ""]
    if error is not None:
        lines.append("ERROR: {0}".format(error))
    lines.append("ISLANDS: {0}".format(len(islands)))
    lines.append("PUSHES_AGG_OR_JOIN: {0}".format(_pushes_agg_join(islands)))
    lines.append("SHIPMENT_SEEN: {0}".format(_has_shipment(islands)))
    lines.append("")
    for index, (source, kind, sql) in enumerate(islands):
        lines.append("--- island {0} source={1} kind={2} ---".format(
            index, source, kind))
        lines.append(sql)
        lines.append("")
    with open(out_path, "w") as handle:
        handle.write("\n".join(lines))


def _dump_one(runtime, qualify, path, out_dir):
    """Dump one query's new-engine pushed SQL, recording errors as findings."""
    name = os.path.splitext(os.path.basename(path))[0]
    raw = open(path).read()
    engine_sql = qualify(raw)
    trace, error = _capture_execute(runtime, engine_sql)
    islands = _parse_trace(trace)
    _write_report(os.path.join(out_dir, name + ".txt"), name, islands, error)
    status = "ERROR" if error else "islands={0}".format(len(islands))
    print("{0:5} {1}".format(name, status), flush=True)


def _make_qualifier(suite):
    """Build the per-suite table->source qualifier used before execution.

    tpcds has a single runner (run_federated_rust.py) whose _qualify already
    binds the pg-dims placement, so it takes only (sql, source_map, dialect).
    tpch still carries the placement-parameterized run_federated._qualify.
    """
    if suite == "tpcds":
        import run_federated_rust as rfr

        def qualify(raw):
            return rfr._qualify(raw, rfr.FEDQ_SOURCES, "postgres")

        return qualify
    import run_federated as rf
    placement = rf.PLACEMENTS["pg-dims"]

    def qualify(raw):
        return rf._qualify(raw, placement, rf.FEDQ_SOURCES, "postgres")

    return qualify


def main():
    """Dump every query in a suite through the new engine's SQL trace."""
    suite = sys.argv[1]
    os.environ["FEDQRS_TRACE_SQL"] = "1"
    bench = _bench_dir(suite)
    sys.path.insert(0, bench)
    import fedq
    from generate import _db_path, DEFAULT_DATA_DIR
    scale = SUITE_SCALE[suite]
    db_path = _db_path(DEFAULT_DATA_DIR, scale)
    config_path = _write_config(db_path, SUITE_PG_DB[suite], _adbc_driver_path())
    runtime = fedq.Runtime(config_path)
    qualify = _make_qualifier(suite)
    out_dir = os.path.join(HERE, "new", suite)
    paths = sorted(glob.glob(os.path.join(bench, "queries", "q*.sql")))
    for path in paths:
        _dump_one(runtime, qualify, path, out_dir)


if __name__ == "__main__":
    main()
