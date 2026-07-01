"""Generate the TPC-H dataset and query set used by the benchmark.

This writes a native DuckDB database file containing the eight TPC-H tables at
a chosen scale factor, and dumps the 22 standard TPC-H queries (exactly as the
DuckDB tpch extension emits them, in DuckDB dialect) to individual .sql files.
The benchmark runner (run.py) consumes both artifacts.
"""

import argparse
import os

import duckdb


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.join(HERE, "data")
DEFAULT_QUERIES_DIR = os.path.join(HERE, "queries")


def _db_path(data_dir, scale_factor):
    """Build the database file path, encoding the scale factor in the name."""
    return os.path.join(data_dir, "tpch_sf{0}.duckdb".format(scale_factor))


def _generate_database(db_path, scale_factor):
    """Create the DuckDB file and populate it with dbgen at the scale factor."""
    if os.path.exists(db_path):
        os.remove(db_path)
    connection = duckdb.connect(db_path)
    connection.execute("INSTALL tpch")
    connection.execute("LOAD tpch")
    connection.execute("CALL dbgen(sf = {0})".format(scale_factor))
    connection.close()


def _write_queries(db_path, queries_dir):
    """Dump the 22 TPC-H queries to queries/qNN.sql in DuckDB dialect."""
    connection = duckdb.connect(db_path, read_only=True)
    connection.execute("LOAD tpch")
    rows = connection.execute(
        "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr"
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


def _parse_args():
    """Parse command-line arguments for standalone generation."""
    parser = argparse.ArgumentParser(description="Generate TPC-H data and queries.")
    parser.add_argument("--scale-factor", default="0.01")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    return parser.parse_args()


def main():
    """Entry point for running generation directly."""
    args = _parse_args()
    generate(args.scale_factor, args.data_dir, args.queries_dir)


if __name__ == "__main__":
    main()
