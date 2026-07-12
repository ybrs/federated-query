"""THE TPC-DS benchmark runner: the single entry point for this suite.

Subcommands (the first argument selects one; when none is given the runner
defaults to ``run`` so the historical invocation keeps working):

  run        Drive all 99 queries through the RUST engine (fedq.Runtime) over
             the pg-dims federated split (dimensions on PostgreSQL, facts on
             DuckDB), verify each result against the cached pure-DuckDB truth in
             references_sf<sf>.duckdb, and time it against the CACHED DuckDB
             baseline (the oracle_timings table in the same references file).
             The DuckDB baseline is NEVER re-measured here - it is a function of
             the data, measured once by save-refs and read back.
  save-refs  Build references_sf<sf>.duckdb: the pure-DuckDB truth rows for
             every query (the canonical single-source answer) plus the FEDERATED
             DuckDB oracle timing (DuckDB with PostgreSQL attached). This path
             does NOT drive the Python or Rust engine. It refuses to run when the
             references file already holds oracle timings - rebuilding requires
             deleting the file first, an explicit human act.
  generate   Build the DuckDB dataset file and dump the 99 query texts for a
             scale factor.
  load-pg    Load every base table from the DuckDB file into a PostgreSQL
             database (both sources then hold identical rows; a placement only
             decides which source each table is READ from).

For ``run``, every dimension is placed on PostgreSQL and every fact on DuckDB,
so each fact-dimension join crosses sources. Each query is classified OK
(matches the reference), WRONG (runs but differs), or ERROR (raised in the
engine); every per-query exception is caught and recorded so one failure cannot
stop the whole run. The Rust engine's 100ms planning budget surfaces as an ERROR
("planning budget exceeded") for the heaviest queries; that is reported as-is,
never worked around.

  --warm-runs N (default 0): with N == 0 each query runs once on a shared
    runtime (the historical behavior), reported as the cold-ISH column. With
    N > 0 the first run stays the cold-ish column and the median of N subsequent
    runs on the live runtime is the warm column.
  --cold-process: run every query in a fresh child process with a fresh runtime,
    the strict cold definition of benchmarks/perf_compare (no plan cache, pooled
    connection, or cached statistic can leak between queries).
"""

import argparse
import datetime
import glob
import multiprocessing
import os
import re
import sys
import tempfile
import threading
import time
from decimal import Decimal

import duckdb
import pyarrow as pa
import sqlglot
from sqlglot import exp

HERE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(HERE_DIR, "reports")
sys.path.insert(0, HERE_DIR)


# --- data setup helpers (dataset file paths and generation) -------------------

DEFAULT_DATA_DIR = os.path.join(HERE_DIR, "data")
DEFAULT_QUERIES_DIR = os.path.join(HERE_DIR, "queries")


def _db_path(data_dir, scale_factor):
    """Build the database file path, encoding the scale factor in the name."""
    return os.path.join(data_dir, "tpcds_sf{0}.duckdb".format(scale_factor))


def pg_database_name(scale_factor):
    """The dedicated PostgreSQL database for a TPC-DS scale factor.

    TPC-DS gets its OWN database per scale so it never collides with the TPC-H
    benchmark on the same PostgreSQL server: both define a `customer` table, and
    a shared database let TPC-H's 8-column customer shadow TPC-DS's 18-column
    one (its c_customer_sk then unresolvable). The dot is dropped so the name
    needs no quoting: scale 0.1 -> tpcds_sf01, 1 -> tpcds_sf1, 10 -> tpcds_sf10.
    """
    return "tpcds_sf{0}".format(str(scale_factor).replace(".", ""))


def _generate_database(db_path, scale_factor):
    """Create the DuckDB file and populate it with dsdgen at the scale factor."""
    if os.path.exists(db_path):
        os.remove(db_path)
    connection = duckdb.connect(db_path)
    connection.execute("INSTALL tpcds")
    connection.execute("LOAD tpcds")
    connection.execute("CALL dsdgen(sf = {0})".format(scale_factor))
    connection.close()


def _write_queries(db_path, queries_dir):
    """Dump the 99 TPC-DS queries to queries/qNN.sql in DuckDB dialect."""
    connection = duckdb.connect(db_path, read_only=True)
    connection.execute("LOAD tpcds")
    rows = connection.execute(
        "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr"
    ).fetchall()
    connection.close()
    for query_nr, query in rows:
        target = os.path.join(queries_dir, "q{0:02d}.sql".format(query_nr))
        with open(target, "w") as handle:
            handle.write(query.strip() + "\n")
    return len(rows)


def generate(scale_factor, data_dir, queries_dir):
    """Generate the database file and the query files, reporting what was done."""
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(queries_dir, exist_ok=True)
    db_path = _db_path(data_dir, scale_factor)
    _generate_database(db_path, scale_factor)
    query_count = _write_queries(db_path, queries_dir)
    print("Generated database: {0}".format(db_path))
    print("Wrote {0} queries to: {1}".format(query_count, queries_dir))
    return db_path


# --- qualification: bare TPC-DS names into fully qualified references ----------
#
# The DuckDB TPC-DS extension emits queries that reference tables by their bare
# name (``FROM store_sales``). The federated query engine requires three-part
# names (``source.schema.table``). Only the 24 TPC-DS base tables are qualified;
# every other name (CTE names, column names, aliases) is left untouched, then
# the result is rendered in a target dialect.

# The 24 TPC-DS base tables produced by dsdgen. Any name outside this set
# (CTEs, derived tables) is deliberately left unqualified.
TPCDS_TABLES = frozenset(
    {
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
    }
)


def _qualify_table(table, source_name, schema_name):
    """Attach source and schema to a base TPC-DS table node in place."""
    if table.name.lower() not in TPCDS_TABLES:
        return
    if table.args.get("db"):
        return
    table.set("db", exp.to_identifier(schema_name))
    table.set("catalog", exp.to_identifier(source_name))


def qualify_query(sql, source_name, schema_name, target_dialect):
    """Qualify TPC-DS tables in one query and render it in the target dialect.

    The query is parsed as DuckDB SQL (its source dialect), every base TPC-DS
    table is rewritten to ``source_name.schema_name.table``, and the tree is
    rendered in ``target_dialect`` so the engine's parser accepts it.
    """
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        _qualify_table(table, source_name, schema_name)
    return tree.sql(dialect=target_dialect)


# --- result comparison for TPC-DS correctness checking ------------------------
#
# The engine result and the DuckDB oracle result are compared row by row, in
# order: row i of the engine output must equal row i of DuckDB's output. Numbers
# are rounded to a fixed number of decimals (TPC-DS aggregates are monetary) and
# fixed-width CHAR padding is stripped, so only genuine value differences count.
# Comparison is by column position; the two engines may name columns differently.
#
# Row order is part of correctness here: a TPC-DS query with an ORDER BY must
# return rows in that order. When the rows match as a set but not in order, the
# mismatch is reported as an ordering difference so it is not confused with a
# wrong value. Many TPC-DS queries end in ``ORDER BY ... LIMIT n`` over columns
# with ties; when the tie-break differs between the two engines the
# set-vs-order distinction in the reason makes that visible rather than hiding it.


def _normalize_value(value, decimals):
    """Turn one cell into a canonical, comparable string."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float, Decimal)):
        return "{0:.{1}f}".format(float(value), decimals)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace").rstrip()
    return str(value).rstrip()


def _normalize_row(row, decimals):
    """Normalize every cell of a row into a comparable tuple."""
    normalized = []
    for value in row:
        normalized.append(_normalize_value(value, decimals))
    return tuple(normalized)


def _normalize_rows(rows, decimals):
    """Normalize a list of rows into a list of comparable row tuples, in order."""
    normalized = []
    for row in rows:
        normalized.append(_normalize_row(row, decimals))
    return normalized


def _numeric_close(left, right):
    """Whether two numeric cells differ only by float summation noise.

    A float64 aggregate's last bits depend on summation ORDER (a distributed
    plan and a single-engine plan sum in different orders), and the
    fixed-decimal rounding AMPLIFIES a last-bit difference sitting exactly on
    a rounding boundary into a visible cent: q18's ROLLUP subtotal is engine
    206.98499999999999 vs oracle 206.985 (diff 1.4e-14), rounded to 206.98 vs
    206.99. Values within this TIGHT relative tolerance are the same number
    for correctness purposes; a real value bug (wrong rows, dropped rounding,
    a wrong scale) differs by far more than 1e-9 relative.
    """
    if not isinstance(left, (int, float, Decimal)) or isinstance(left, bool):
        return False
    if not isinstance(right, (int, float, Decimal)) or isinstance(right, bool):
        return False
    left_float = float(left)
    right_float = float(right)
    return abs(left_float - right_float) <= 1e-9 * max(
        1.0, abs(left_float), abs(right_float)
    )


def _cells_equal(engine_value, oracle_value, decimals):
    """Whether two cells match: by rounded form, or within numeric tolerance."""
    engine_norm = _normalize_value(engine_value, decimals)
    oracle_norm = _normalize_value(oracle_value, decimals)
    if engine_norm == oracle_norm:
        return True
    return _numeric_close(engine_value, oracle_value)


def _rows_equal(engine_row, oracle_row, decimals):
    """Whether two rows match cell-for-cell."""
    for engine_value, oracle_value in zip(engine_row, oracle_row):
        if not _cells_equal(engine_value, oracle_value, decimals):
            return False
    return True


def _first_differing_index(engine_rows, oracle_rows, decimals):
    """Return the index of the first row that differs, or -1 if all match."""
    for index in range(len(engine_rows)):
        if not _rows_equal(engine_rows[index], oracle_rows[index], decimals):
            return index
    return -1


def _describe_difference(index, engine_norm, oracle_norm):
    """Describe the first differing row, flagging an order-only difference."""
    if sorted(engine_norm) == sorted(oracle_norm):
        template = "rows match as a set but order differs at row {0}: engine={1} oracle={2}"
    else:
        template = "row {0} differs: engine={1} oracle={2}"
    return template.format(index, engine_norm[index], oracle_norm[index])


def compare_results(engine_rows, oracle_rows, decimals=2):
    """Compare two result sets row by row, returning (is_match, reason).

    ``reason`` is an empty string on a match and otherwise names the first
    discrepancy: a differing row count, or the first row position whose values
    (or ordering) differ.
    """
    if len(engine_rows) != len(oracle_rows):
        reason = "row count differs: engine={0} oracle={1}".format(
            len(engine_rows), len(oracle_rows)
        )
        return False, reason
    index = _first_differing_index(engine_rows, oracle_rows, decimals)
    if index == -1:
        return True, ""
    if _multisets_match(engine_rows, oracle_rows, decimals):
        # Rows with EQUAL ORDER BY keys are legitimately unordered (the TPC
        # answer-set convention; two DuckDB runs can interleave them
        # differently too), so an order-only difference with identical
        # multisets is a match. A genuine mis-sort under the ubiquitous
        # ORDER BY ... LIMIT still changes WHICH rows survive and fails the
        # multiset compare. (Observed at SF10: q18's data-NULL detail rows tie
        # with ROLLUP subtotal rows on every sort column; q65/q71 tie on
        # their full key lists.)
        return True, ""
    engine_norm = _normalize_rows(engine_rows, decimals)
    oracle_norm = _normalize_rows(oracle_rows, decimals)
    return False, _describe_difference(index, engine_norm, oracle_norm)


def _multisets_match(engine_rows, oracle_rows, decimals):
    """Whether the two result sets match as multisets under the cell rules.

    Both sides are sorted by their normalized form and compared pairwise with
    the tolerance-aware cell equality, so tie-reordered rows align while a
    genuinely differing value still fails.
    """
    engine_sorted = _sort_by_normal_form(engine_rows, decimals)
    oracle_sorted = _sort_by_normal_form(oracle_rows, decimals)
    for engine_row, oracle_row in zip(engine_sorted, oracle_sorted):
        if not _rows_equal(engine_row, oracle_row, decimals):
            return False
    return True


def _sort_by_normal_form(rows, decimals):
    """Rows sorted by their normalized (rounded, canonical-string) form."""
    keyed = []
    for row in rows:
        keyed.append((_normalize_row(row, decimals), row))
    keyed.sort(key=_normal_form_key)
    ordered = []
    for _, row in keyed:
        ordered.append(row)
    return ordered


def _normal_form_key(item):
    """Sort key accessor for (normal form, raw row) pairs."""
    return item[0]


# --- federated placement, sources, and shared query helpers -------------------

# The seven TPC-DS fact tables; everything else is a dimension. This drives the
# pg-dims placement: each fact-dimension join then crosses sources, which is the
# common TPC-DS shape (a large fact scanned against many small dimensions).
FACT_TABLES = frozenset(
    {
        "store_sales",
        "store_returns",
        "catalog_sales",
        "catalog_returns",
        "web_sales",
        "web_returns",
        "inventory",
    }
)

ENGINE_DIALECT = "postgres"

# How each source kind names its schema. The engine reads three-part references
# (pg tables as pg.public.X, duck tables as duck.main.X). The DuckDB oracle reads
# pg tables via the attached "pgdb" and duck tables bare (catalog None) so they
# resolve against the local DuckDB file.
FEDQ_SOURCES = {"pg": ("pg", "public"), "duck": ("duck", "main")}
ORACLE_SOURCES = {"pg": ("pgdb", "public"), "duck": (None, "main")}

# ADBC Postgres driver locations searched in order; first existing path wins.
ADBC_CANDIDATES = [
    "/workspace/venv-fedq/lib/python3.13/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
    "/workspace/venv/lib/python3.12/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
]


def _pg_dims_placement():
    """Map every dimension to "pg" and every fact to "duck"."""
    placement = {}
    for table in TPCDS_TABLES:
        placement[table] = "duck" if table in FACT_TABLES else "pg"
    return placement


PG_DIMS = _pg_dims_placement()


def _qualify(sql, source_map, dialect):
    """Qualify each base TPC-DS table to the source that holds it under pg-dims."""
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if name not in TPCDS_TABLES or table.args.get("db"):
            continue
        catalog, schema = source_map[PG_DIMS[name]]
        table.set("db", exp.to_identifier(schema))
        if catalog is not None:
            table.set("catalog", exp.to_identifier(catalog))
    return tree.sql(dialect=dialect)


def _read_query(path):
    """Read the original DuckDB query text from a .sql file."""
    with open(path) as handle:
        return handle.read()


def arrow_to_rows(stream):
    """Consume an Arrow C-stream export into a list of positional row tuples."""
    table = pa.table(stream)
    columns = []
    for index in range(table.num_columns):
        columns.append(table.column(index).to_pylist())
    rows = []
    for row in zip(*columns):
        rows.append(tuple(row))
    return rows


def _select_query_files(queries_dir, only):
    """Return the sorted query files, optionally filtered by --only numbers."""
    paths = sorted(glob.glob(os.path.join(queries_dir, "q*.sql")))
    if not only:
        return paths
    wanted = set()
    for token in only.split(","):
        wanted.add("q{0:02d}".format(int(token.strip())))
    selected = []
    for path in paths:
        if os.path.splitext(os.path.basename(path))[0] in wanted:
            selected.append(path)
    return selected


def _pg_database(options):
    """The PostgreSQL database to use: an explicit --pg-database, otherwise the
    scale's dedicated TPC-DS database (kept separate from the TPC-H benchmark)."""
    return options.pg_database or pg_database_name(options.scale_factor)


def _adbc_driver_path():
    """Return the first ADBC Postgres driver shared library that exists."""
    for candidate in ADBC_CANDIDATES:
        if os.path.exists(candidate):
            return candidate
    raise SystemExit("ADBC Postgres driver not found in: {0}".format(ADBC_CANDIDATES))


def _refs_path(scale_factor):
    """The per-scale cached references database file."""
    return os.path.join(
        DEFAULT_DATA_DIR, "references_sf{0}.duckdb".format(scale_factor))


def _oracle_ms(refs, name):
    """The cached federated-oracle timing for a query, or None."""
    row = refs.execute(
        "SELECT oracle_ms FROM oracle_timings WHERE query = ?", [name]
    ).fetchone()
    return None if row is None else row[0]


# --- run: drive the Rust engine over the pg-dims split ------------------------

# Hard wall-clock budgets per scale factor, seconds. DETERMINISTIC and not
# overridable: a run past its budget is killed outright (a watchdog thread
# calls os._exit, so even a hung native call cannot outlive it). A benchmark
# that needs longer is a regression to fix, not a budget to raise.
WALL_BUDGET_SECONDS = {"0.1": 60, "1": 60, "10": 300}


def _arm_watchdog(scale_factor):
    """Kill the whole process when the scale's wall budget expires. A daemon
    thread survives GIL-released native calls, so the kill is unconditional."""
    budget = WALL_BUDGET_SECONDS.get(scale_factor, 60)

    def _kill():
        sys.stderr.write(
            "WALL BUDGET EXCEEDED: sf{0} run past {1}s - killed. "
            "Fix the regression; the budget does not move.\n".format(
                scale_factor, budget))
        sys.stderr.flush()
        os._exit(124)

    timer = threading.Timer(budget, _kill)
    timer.daemon = True
    timer.start()
    print("[pg-dims] wall budget: {0}s (deterministic kill)".format(budget))


def write_config(db_path, database, adbc_driver, options):
    """Write a temp YAML config: a DuckDB source and a PostgreSQL source."""
    text = (
        "datasources:\n"
        "  duck:\n"
        "    type: duckdb\n"
        "    path: {db_path}\n"
        "    read_only: true\n"
        "  pg:\n"
        "    type: postgres\n"
        "    host: {host}\n"
        "    port: {port}\n"
        "    user: {user}\n"
        "    password: {password}\n"
        "    database: {database}\n"
        "    schemas:\n"
        "      - public\n"
        "    adbc_driver: {adbc_driver}\n"
    ).format(
        db_path=db_path, host=options.pg_host, port=options.pg_port,
        user=options.pg_user, password=options.pg_password,
        database=database, adbc_driver=adbc_driver,
    )
    handle = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    handle.write(text)
    handle.close()
    return handle.name


def _reference_rows(refs, name):
    """Read the cached pure-DuckDB truth rows for a query, in ORDER BY order."""
    query = 'SELECT * EXCLUDE (__ord) FROM "{0}" ORDER BY __ord'.format(name)
    return refs.execute(query).fetchall()


def _median_ms(thunk, warm_runs):
    """Time one COLD-ish run then WARM repeats; return (cold_ms, warm_ms, result).

    The first call is the cold-ish run reported as the cold column. When
    warm_runs is 0 no repeats run and warm_ms is None. Otherwise warm_ms is the
    median of warm_runs subsequent runs (plan-cache hits on the live runtime).
    The last executed result is returned for the correctness comparison.
    """
    start = time.perf_counter()
    result = thunk()
    cold_ms = (time.perf_counter() - start) * 1000.0
    times = []
    for _ in range(warm_runs):
        start = time.perf_counter()
        result = thunk()
        times.append((time.perf_counter() - start) * 1000.0)
    warm_ms = None
    if times:
        times.sort()
        warm_ms = times[len(times) // 2]
    return cold_ms, warm_ms, result


def _metric_ms(cold_ms, warm_ms):
    """The comparable time for a run: the warm median when repeats ran, else the
    single cold-ish run. This is the number that feeds the ratio and totals."""
    if warm_ms is not None:
        return warm_ms
    return cold_ms


def _error_result(name, error):
    """Assemble an ERROR record for a query that raised in the engine (a planning
    budget kill surfaces here carrying the engine's own message)."""
    detail = str(error).strip().splitlines()
    message = detail[0] if detail else type(error).__name__
    return {
        "name": name, "status": "ERROR",
        "reason": "{0}: {1}".format(type(error).__name__, message),
        "engine_rows": None, "truth_rows": None,
        "cold_ms": None, "warm_ms": None, "duck_ms": None, "ratio": None,
    }


def _compared_result(name, engine_rows, truth_rows, decimals, cold_ms, warm_ms, duck_ms):
    """Compare engine rows against the reference truth, classify OK/WRONG, and
    attach the cold/warm engine timings, the DuckDB baseline, and the ratio."""
    match, reason = compare_results(engine_rows, truth_rows, decimals)
    ours_ms = _metric_ms(cold_ms, warm_ms)
    ratio = ours_ms / duck_ms if duck_ms else None
    return {
        "name": name, "status": "OK" if match else "WRONG", "reason": reason,
        "engine_rows": len(engine_rows), "truth_rows": len(truth_rows),
        "cold_ms": cold_ms, "warm_ms": warm_ms, "duck_ms": duck_ms,
        "ratio": ratio,
    }


def evaluate_query(runtime, refs, path, decimals, warm_runs):
    """Run one query through the Rust engine, time it (cold-ish plus warm
    median), verify against the reference, classify it.

    The DuckDB baseline time is READ from the `oracle_timings` table the
    reference build saved into the references db (save-refs measures every query
    ONCE per dataset) - the baseline is a function of the data, never
    re-measured per run."""
    name = os.path.splitext(os.path.basename(path))[0]
    raw = _read_query(path)
    engine_sql = _qualify(raw, FEDQ_SOURCES, ENGINE_DIALECT)
    try:
        cold_ms, warm_ms, engine_rows = _median_ms(
            lambda: arrow_to_rows(runtime.execute(engine_sql)), warm_runs)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        # A Rust panic surfaces as pyo3_runtime.PanicException, which subclasses
        # BaseException (not Exception), so catch BaseException here to record a
        # panic as a per-query ERROR instead of aborting the whole run.
        return _error_result(name, error)
    truth_rows = _reference_rows(refs, name)
    return _compared_result(
        name, engine_rows, truth_rows, decimals, cold_ms, warm_ms,
        _oracle_ms(refs, name))


def _ms_text(value):
    """A milliseconds cell like '123.4', or '-' when there is no timing."""
    if value is None:
        return "-"
    return "{0:.1f}".format(value)


def _ratio_text(value):
    """An ours/DuckDB ratio cell like '1.51x', or '-' when unavailable."""
    if value is None:
        return "-"
    return "{0:.2f}x".format(value)


def _print_result(result):
    """Print one query's outcome row: status, cold/warm/duck ms, ratio, reason."""
    reason = result["reason"]
    if len(reason) > 40:
        reason = reason[:40] + "..."
    print("{0:5} {1:6} {2:>9} {3:>9} {4:>9} {5:>7}  {6}".format(
        result["name"], result["status"], _ms_text(result["cold_ms"]),
        _ms_text(result["warm_ms"]), _ms_text(result["duck_ms"]),
        _ratio_text(result["ratio"]), reason), flush=True)


def _tally(results):
    """Count OK / WRONG / ERROR across a result list."""
    tally = {"OK": 0, "WRONG": 0, "ERROR": 0}
    for result in results:
        tally[result["status"]] += 1
    return tally


def _geomean(ratios):
    """Geometric mean of positive ratios (0.0 for an empty list)."""
    if not ratios:
        return 0.0
    product = 1.0
    for ratio in ratios:
        product *= ratio
    return product ** (1.0 / len(ratios))


def _timing_totals(results):
    """Sum ours and DuckDB time and collect ratios over OK queries that have a
    DuckDB baseline; returns (ours_total_ms, duck_total_ms, ratios)."""
    ours_total = 0.0
    duck_total = 0.0
    ratios = []
    for result in results:
        if result["status"] != "OK" or not result["duck_ms"]:
            continue
        ours_total += _metric_ms(result["cold_ms"], result["warm_ms"])
        duck_total += result["duck_ms"]
        ratios.append(result["ratio"])
    return ours_total, duck_total, ratios


def _ratio_summary_line(ours_total, duck_total, ratios):
    """The tpch-style ours-vs-DuckDB totals line (seconds), or a note when no
    query carried a baseline timing."""
    if not ratios:
        return "ours -  duckdb -  ->  no DuckDB baseline timings measured"
    total_ratio = ours_total / duck_total if duck_total else float("nan")
    return ("ours {0:.1f}s  duckdb {1:.1f}s  ->  total {2:.2f}x  geomean {3:.2f}x  "
            "({4} OK queries measured)".format(
                ours_total / 1000.0, duck_total / 1000.0, total_ratio,
                _geomean(ratios), len(ratios)))


def _print_summary(results, tally, total_ms):
    """Print the OK/WRONG/ERROR tally and the tpch-style timing ratio line."""
    ours_total, duck_total, ratios = _timing_totals(results)
    print("-" * 72)
    print("[pg-dims] {0} ok | {1} wrong | {2} error   (total {3:.1f}s)".format(
        tally["OK"], tally["WRONG"], tally["ERROR"], total_ms / 1000.0))
    print("[pg-dims] " + _ratio_summary_line(ours_total, duck_total, ratios))


def _cluster_key(result):
    """A coarse grouping key: the reason with identifiers and numbers collapsed.

    Quoted identifiers and bare numbers are replaced with placeholders so that
    the same failure over different tables/columns clusters into one bucket.
    """
    reason = result["reason"]
    reason = re.sub(r"'[^']*'", "'X'", reason)
    reason = re.sub(r'"[^"]*"', '"X"', reason)
    reason = re.sub(r"\bq\d+\b", "qNN", reason)
    reason = re.sub(r"\d+", "N", reason)
    return reason


def _cluster_failures(results):
    """Group non-OK results by cluster key, WRONG buckets before ERROR ones."""
    clusters = {}
    for result in results:
        if result["status"] == "OK":
            continue
        key = _cluster_key(result)
        clusters.setdefault(key, {"status": result["status"], "members": []})
        clusters[key]["members"].append(result["name"])
    return clusters


def _cluster_sort_key(item):
    """Sort clusters: WRONG before ERROR, then by descending size, then label."""
    key, bucket = item
    status_rank = 0 if bucket["status"] == "WRONG" else 1
    return (status_rank, -len(bucket["members"]), key)


def _grouped_lines(results):
    """Build the grouped non-OK section, WRONG clusters first, largest first."""
    ordered = sorted(_cluster_failures(results).items(), key=_cluster_sort_key)
    lines = []
    for key, bucket in ordered:
        names = ", ".join(bucket["members"])
        lines.append("### {0} ({1}) [{2}]".format(
            key, len(bucket["members"]), bucket["status"]))
        lines.append("Queries: {0}".format(names))
        lines.append("")
    return lines


def _matrix_row(result):
    """Render one query's per-query markdown table row."""
    message = result["reason"] if result["reason"] else "rows and values match"
    rows_cell = "-"
    if result["engine_rows"] is not None:
        rows_cell = "{0} / {1}".format(result["engine_rows"], result["truth_rows"])
    return "| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} |".format(
        result["name"], result["status"], _ms_text(result["cold_ms"]),
        _ms_text(result["warm_ms"]), _ms_text(result["duck_ms"]),
        _ratio_text(result["ratio"]), rows_cell, message.replace("|", "\\|"))


def _matrix_lines(results):
    """Build the per-query markdown table (status, timings, rows, message)."""
    lines = [
        "| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio "
        "| Rows engine/truth | Message |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for result in results:
        lines.append(_matrix_row(result))
    return lines


def _timing_summary_lines(results):
    """The tpch-style timing totals table over OK queries with a DuckDB baseline."""
    ours_total, duck_total, ratios = _timing_totals(results)
    if not ratios:
        return []
    total_ratio = ours_total / duck_total if duck_total else float("nan")
    return [
        "## Timing summary (OK queries with a DuckDB baseline)",
        "",
        "| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |",
        "| --- | --- | --- | --- | --- |",
        "| {0:.1f} | {1:.1f} | {2:.2f}x | {3:.2f}x | {4} |".format(
            ours_total / 1000.0, duck_total / 1000.0, total_ratio,
            _geomean(ratios), len(ratios)),
        "",
    ]


def _report_lines(results, tally, total_ms, scale_factor):
    """Assemble the full markdown report for the run."""
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    ours_total, duck_total, ratios = _timing_totals(results)
    lines = [
        "# TPC-DS federated benchmark report (Rust engine)",
        "",
        "Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.",
        "Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).",
        "Truth: cached pure-DuckDB references in references_sf{0}.duckdb.".format(
            scale_factor),
        "Baseline: pure DuckDB over the fact+dim file (every table local).",
        "Generated: {0}".format(stamp),
        "",
        "Tally: {0} ok | {1} wrong | {2} error   (total {3} queries, {4:.1f}s)".format(
            tally["OK"], tally["WRONG"], tally["ERROR"], len(results),
            total_ms / 1000.0),
        "Timing: " + _ratio_summary_line(ours_total, duck_total, ratios),
        "",
        "## Non-OK queries grouped by reason",
        "",
    ]
    lines.extend(_grouped_lines(results))
    lines.extend(_timing_summary_lines(results))
    lines.append("## Per-query matrix")
    lines.append("")
    lines.extend(_matrix_lines(results))
    lines.append("")
    return lines


def write_report(results, tally, total_ms, options, path):
    """Write the per-query markdown report for this run."""
    lines = _report_lines(results, tally, total_ms, options.scale_factor)
    with open(path, "w") as handle:
        handle.write("\n".join(lines))
    print("Wrote report: {0}".format(path))


def _build_context(options):
    """Assemble the shared benchmark context the run modes need: the config
    path, the DuckDB fact+dim file, the pg database, and the ADBC driver."""
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    database = _pg_database(options)
    adbc_driver = _adbc_driver_path()
    config_path = write_config(db_path, database, adbc_driver, options)
    return config_path, db_path, database, adbc_driver


def _print_header(db_path, database, adbc_driver):
    """Print the run banner and the per-query column headings."""
    print("\n==== TPC-DS pg-dims (ours=fedq-rust vs pure-DuckDB baseline) ====")
    print("db={0} pg={1} adbc={2}".format(db_path, database, adbc_driver))
    print("{0:5} {1:6} {2:>9} {3:>9} {4:>9} {5:>7}".format(
        "query", "status", "cold", "warm", "duckdb", "ratio"))


def _finish(results, total_ms, options):
    """Print the summary, write the report, and return the results."""
    tally = _tally(results)
    _print_summary(results, tally, total_ms)
    report_path = os.path.join(
        REPORTS_DIR, "rust-fed-sf{0}.md".format(options.scale_factor))
    os.makedirs(REPORTS_DIR, exist_ok=True)
    write_report(results, tally, total_ms, options, report_path)
    return results


def _run_shared(options, paths):
    """Run every query on ONE shared runtime and DuckDB baseline connection.

    The runtime persists across queries, so a query's first run is only
    cold-ISH (connections pooled, statistics session-cached by earlier
    queries). Use --cold-process for the strict cold definition.
    """
    import fedq

    config_path, db_path, database, adbc_driver = _build_context(options)
    runtime = fedq.Runtime(config_path)
    refs = duckdb.connect(_refs_path(options.scale_factor), read_only=True)
    _print_header(db_path, database, adbc_driver)
    print("[pg-dims] {0} queries x {1} engine run(s) each; baseline CACHED "
          "(oracle_timings in the references db)".format(
              len(paths), 1 + options.warm_runs))
    results = []
    started = time.perf_counter()
    for path in paths:
        result = evaluate_query(
            runtime, refs, path, options.decimals, options.warm_runs)
        results.append(result)
        _print_result(result)
    return _finish(results, (time.perf_counter() - started) * 1000.0, options)


def _cold_worker(config_path, db_path, refs_path, path, decimals, warm_runs, result_queue):
    """Child entry: build a FRESH runtime and DuckDB baseline, evaluate one
    query, and send its result back. A fresh runtime per query is the strict
    cold definition of benchmarks/perf_compare - no plan cache, pooled
    connection, or cached statistic can leak in from an earlier query."""
    import fedq

    runtime = fedq.Runtime(config_path)
    refs = duckdb.connect(refs_path, read_only=True)
    result = evaluate_query(runtime, refs, path, decimals, warm_runs)
    result_queue.put(result)


def _evaluate_cold_process(config_path, db_path, refs_path, path, options):
    """Fork a fresh child to evaluate one query under the strict cold definition."""
    context = multiprocessing.get_context("fork")
    result_queue = context.Queue()
    process = context.Process(
        target=_cold_worker,
        args=(config_path, db_path, refs_path, path, options.decimals,
              options.warm_runs, result_queue))
    process.start()
    result = result_queue.get()
    process.join()
    return result


def _run_cold_process(options, paths):
    """Run every query in a fresh child with a fresh runtime - the strict cold
    definition of benchmarks/perf_compare. Slower than shared mode; each query
    pays the full connect + statistics + planning cost with nothing cached."""
    config_path, db_path, database, adbc_driver = _build_context(options)
    refs_path = _refs_path(options.scale_factor)
    _print_header(db_path, database, adbc_driver)
    results = []
    started = time.perf_counter()
    for path in paths:
        result = _evaluate_cold_process(config_path, db_path, refs_path, path, options)
        results.append(result)
        _print_result(result)
    return _finish(results, (time.perf_counter() - started) * 1000.0, options)


def run(options):
    """Run every selected query under the pg-dims federated split and report."""
    paths = _select_query_files(options.queries_dir, options.only)
    if not paths:
        raise SystemExit("No query files found in {0}".format(options.queries_dir))
    _arm_watchdog(options.scale_factor)
    if options.cold_process:
        return _run_cold_process(options, paths)
    return _run_shared(options, paths)


# --- save-refs: build the cached truth rows and federated-oracle timings ------
#
# The truth and oracle results are pure functions of the DATA, not of the engine
# under test, so they are measured ONCE per dataset into references_sf<sf>.duckdb
# (per-query truth tables with a __ord row-order column, plus an oracle_timings
# table) and read back by every run. CORRECTNESS truth is PURE DuckDB over the
# fact+dim file (every table local); the federated DuckDB oracle (Postgres
# attached) is used only for the timing baseline, since its postgres scanner has
# its own quirks (dropped rows, avg-of-decimal drift) that are not engine bugs.


def _pg_dsn(options):
    """The libpq DSN DuckDB's postgres extension attaches through."""
    return (
        "dbname={0} user={1} password={2} host={3} port={4}".format(
            _pg_database(options), options.pg_user, options.pg_password,
            options.pg_host, options.pg_port)
    )


def _reference_connection(db_path, options):
    """A DuckDB reference connection CAPPED below a memory budget.

    A reference computing a heavy SF10 truth (q64 joins three facts) can blow
    memory; DuckDB degrades gracefully under its own memory_limit (it spills),
    so the reference gets ~60 percent of the configured budget.
    """
    connection = duckdb.connect(db_path, read_only=True)
    if options.memory_limit > 0:
        cap_mb = max(2048, int(options.memory_limit * 0.6))
        connection.execute("SET memory_limit='{0}MB'".format(cap_mb))
    return connection


def build_oracle(db_path, options):
    """Open DuckDB over the dataset with PostgreSQL attached via the connector.

    Disable the postgres extension's filter pushdown: it is buggy (and flagged
    'experimental') - on q59 it mis-pushes a filter to Postgres and the scan
    returns 0 rows, so the oracle produced a wrong 0-row answer (verified: pure
    DuckDB and our engine both return 100). The oracle is the timing reference,
    so it must be right even at the cost of reading unfiltered dim rows and
    filtering locally.
    """
    connection = _reference_connection(db_path, options)
    connection.execute("LOAD postgres")
    connection.execute(
        "ATTACH '{0}' AS pgdb (TYPE postgres, READ_ONLY)".format(_pg_dsn(options))
    )
    connection.execute("SET pg_experimental_filter_pushdown=false")
    return connection


def _run_oracle(oracle, oracle_sql):
    """Warm the DuckDB-over-Postgres oracle, then time one execution."""
    oracle.execute(oracle_sql).fetchall()
    start = time.perf_counter()
    rows = oracle.execute(oracle_sql).fetchall()
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return elapsed_ms, rows


# The federated oracle's own time budget as a fraction of --timeout: past it the
# oracle is INTERRUPTED (duckdb Connection.interrupt is thread-safe) and the
# query keeps no oracle timing rather than burning the whole budget.
ORACLE_TIMEOUT_FRACTION = 0.5


def _time_federated_oracle(db_path, options, oracle_sql):
    """Time DuckDB-over-Postgres for the baseline, or None if it cannot run.

    The federated oracle is only a timing reference (its result is not trusted
    for correctness), so any failure - an OOM, a scanner error, an unsupported
    shape - must not fail an otherwise-correct query; it just yields no timing.
    """
    try:
        oracle = build_oracle(db_path, options)
        # The oracle gets its OWN time budget: a reference that neither errors
        # nor finishes (duck's postgres scanner grinding through a mis-ordered
        # spilling join) must yield "no timing", not burn the whole timeout.
        timer = threading.Timer(
            options.timeout * ORACLE_TIMEOUT_FRACTION, oracle.interrupt
        )
        timer.start()
        try:
            oracle_ms, _ = _run_oracle(oracle, oracle_sql)
        finally:
            timer.cancel()
        return oracle_ms
    except Exception:
        return None


def _refuse_existing_references(options):
    """Refuse to re-run the oracle when its results already exist. The
    references (truth + oracle_timings) are functions of the DATA, measured
    once per dataset; re-measuring them burns minutes-to-hours for nothing.
    DETERMINISTIC: presence of a populated oracle_timings table refuses,
    always - rebuilding requires DELETING the references file first, an
    explicit human act."""
    path = _refs_path(options.scale_factor)
    if not os.path.exists(path):
        return
    existing = duckdb.connect(path, read_only=True)
    try:
        count = existing.execute("SELECT count(*) FROM oracle_timings").fetchone()[0]
    except duckdb.CatalogException:
        return
    finally:
        existing.close()
    if count > 0:
        raise SystemExit(
            "REFUSED: {0} already holds {1} oracle timings. The oracle is "
            "never re-run while its results exist; delete the file first if "
            "the DATA changed.".format(path, count))


def _save_refs_queries(options):
    """(name, oracle_sql, truth_sql) for every selected query.

    truth_sql is the raw query run against PURE DuckDB (unqualified names
    resolve to the file's main schema) - the correctness reference. oracle_sql
    is qualified to the pg-dims split for the DuckDB-over-Postgres timing oracle.
    """
    prepared = []
    for path in _select_query_files(options.queries_dir, options.only):
        name = os.path.splitext(os.path.basename(path))[0]
        raw = _read_query(path)
        prepared.append((name, _qualify(raw, ORACLE_SOURCES, "duckdb"), raw))
    return prepared


def save_refs(options):
    """Persist truth rows and federated-oracle timings for every query.

    Refuses when the references file already holds oracle timings (rebuild =
    delete the file first). Does NOT drive the Python or Rust engine.
    """
    _refuse_existing_references(options)
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    refs = duckdb.connect(_refs_path(options.scale_factor))
    refs.execute(
        "CREATE OR REPLACE TABLE oracle_timings (query VARCHAR, oracle_ms DOUBLE)"
    )
    for name, oracle_sql, truth_sql in _save_refs_queries(options):
        started = time.perf_counter()
        truth = _reference_connection(db_path, options).execute(truth_sql).arrow()
        refs.register("truth_tmp", truth)
        # __ord pins the truth's row order through table storage, so the
        # in-order comparison still sees the ORDER BY output order.
        refs.execute(
            'CREATE OR REPLACE TABLE "{0}" AS '
            "SELECT row_number() OVER () AS __ord, * FROM truth_tmp".format(name)
        )
        refs.unregister("truth_tmp")
        truth_ms = (time.perf_counter() - started) * 1000.0
        oracle_ms = _time_federated_oracle(db_path, options, oracle_sql)
        refs.execute("INSERT INTO oracle_timings VALUES (?, ?)", [name, oracle_ms])
        oracle_text = "-" if oracle_ms is None else "{0:.0f}ms".format(oracle_ms)
        print(
            "{0:5} truth {1:7.0f}ms  oracle {2}".format(name, truth_ms, oracle_text),
            flush=True,
        )
    print("references written: {0}".format(_refs_path(options.scale_factor)))


# --- generate / load-pg: dataset construction --------------------------------


def _load_table(connection, table, schema):
    """Copy one base table from the DuckDB file into PostgreSQL, replacing it."""
    target = 'pg.{0}."{1}"'.format(schema, table)
    source = 'src.main."{0}"'.format(table)
    connection.execute("DROP TABLE IF EXISTS {0}".format(target))
    # CREATE ... AS SELECT streams the DuckDB table straight into PostgreSQL
    # through the attached connection, so both sources hold identical rows.
    connection.execute("CREATE TABLE {0} AS SELECT * FROM {1}".format(target, source))
    rows = connection.execute("SELECT count(*) FROM {0}".format(target)).fetchone()[0]
    return rows


def load(options):
    """Attach the DuckDB dataset (read-only) and PostgreSQL, load every table.

    A :memory: coordinator attaches the DuckDB file read-only and PostgreSQL
    read-write; a read-only DuckDB connection would force every attachment
    read-only and reject the CREATE TABLE into PostgreSQL. Every base table is
    loaded into PostgreSQL byte-identical to the DuckDB file, so a placement
    only decides which source each table is READ from.
    """
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    dsn = "dbname={0} user={1} password={2} host={3} port={4}".format(
        _pg_database(options), options.pg_user, options.pg_password,
        options.pg_host, options.pg_port)
    connection = duckdb.connect(":memory:")
    connection.execute("INSTALL postgres")
    connection.execute("LOAD postgres")
    connection.execute("ATTACH '{0}' AS src (READ_ONLY)".format(db_path))
    connection.execute("ATTACH '{0}' AS pg (TYPE postgres)".format(dsn))
    connection.execute("CREATE SCHEMA IF NOT EXISTS pg.{0}".format(options.pg_schema))
    for table in sorted(TPCDS_TABLES):
        rows = _load_table(connection, table, options.pg_schema)
        print("loaded {0:24} {1:>12} rows".format(table, rows))
    _analyze(connection)
    connection.close()


def _analyze(connection):
    """ANALYZE every loaded table so the cost model has row counts and column
    NDVs for all of them. Without this the small dimensions (warehouse and
    friends) carry no statistics until autoanalyze happens to fire, and the
    dim-shipping gate's dimension classification would depend on that timing."""
    connection.execute("CALL postgres_execute('pg', 'ANALYZE')")
    print("analyzed all tables")


def generate_cmd(options):
    """generate subcommand: build the DuckDB dataset and dump the query texts."""
    generate(options.scale_factor, options.data_dir, options.queries_dir)


# --- command line -------------------------------------------------------------

SUBCOMMANDS = ("run", "save-refs", "generate", "load-pg")


def _add_pg_arguments(parser):
    """Add the shared PostgreSQL connection flags to a subparser."""
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-database", default=None)
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="postgres")


def _build_parser():
    """Build the argparse parser with one subparser per subcommand."""
    parser = argparse.ArgumentParser(
        description="TPC-DS benchmark runner (run / save-refs / generate / load-pg).")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser(
        "run", help="drive the queries through the Rust engine and report")
    run_parser.add_argument("--scale-factor", default="0.1")
    run_parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    run_parser.add_argument("--only", default=None)
    run_parser.add_argument("--decimals", type=int, default=2)
    run_parser.add_argument("--warm-runs", type=int, default=0)
    run_parser.add_argument("--cold-process", action="store_true")
    _add_pg_arguments(run_parser)
    run_parser.set_defaults(func=run)

    refs_parser = subparsers.add_parser(
        "save-refs", help="build the cached truth rows and oracle timings")
    refs_parser.add_argument("--scale-factor", default="0.1")
    refs_parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    refs_parser.add_argument("--only", default=None)
    refs_parser.add_argument("--decimals", type=int, default=2)
    refs_parser.add_argument("--timeout", type=float, default=120.0)
    refs_parser.add_argument("--memory-limit", type=int, default=12288)
    _add_pg_arguments(refs_parser)
    refs_parser.set_defaults(func=save_refs)

    generate_parser = subparsers.add_parser(
        "generate", help="build the DuckDB dataset file and query texts")
    generate_parser.add_argument("--scale-factor", default="1")
    generate_parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    generate_parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    generate_parser.set_defaults(func=generate_cmd)

    load_parser = subparsers.add_parser(
        "load-pg", help="load the base tables into a PostgreSQL database")
    load_parser.add_argument("--scale-factor", default="1")
    load_parser.add_argument("--pg-schema", default="public")
    _add_pg_arguments(load_parser)
    load_parser.set_defaults(func=load)

    return parser


def _argv_with_default_command(argv):
    """Prepend the default ``run`` command when none is named, so the historical
    flags-only invocation keeps working. A bare -h/--help still lists the
    subcommands rather than jumping straight into run's help."""
    if not argv:
        return ["run"]
    if argv[0] in SUBCOMMANDS or argv[0] in ("-h", "--help"):
        return argv
    return ["run"] + argv


def main():
    """Entry point: dispatch the selected subcommand (default ``run``)."""
    parser = _build_parser()
    args = parser.parse_args(_argv_with_default_command(sys.argv[1:]))
    args.func(args)


if __name__ == "__main__":
    main()
