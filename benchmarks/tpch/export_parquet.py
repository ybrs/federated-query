"""Export a generated TPC-H DuckDB database to per-table Parquet files.

The single-source and 2-Parquet federated benchmarks read the eight TPC-H
tables as Parquet. This dumps each table from ``data/tpch_sf<SF>.duckdb`` into a
target directory as ``<table>.parquet``, so the Parquet inputs are reproducible
from committed scripts rather than produced by hand.
"""

import argparse
import os

import duckdb

from generate import DEFAULT_DATA_DIR, _db_path
from qualify import TPCH_TABLES


def _export_table(connection, table, target_dir):
    """Copy one base table from the DuckDB file to a Parquet file."""
    path = os.path.join(target_dir, "{0}.parquet".format(table))
    connection.execute(
        "COPY (SELECT * FROM {0}) TO '{1}' (FORMAT parquet)".format(table, path)
    )
    return path


def export(scale_factor, target_dir):
    """Export every TPC-H base table from the SF database to Parquet files."""
    os.makedirs(target_dir, exist_ok=True)
    db_path = _db_path(DEFAULT_DATA_DIR, scale_factor)
    connection = duckdb.connect(db_path, read_only=True)
    for table in sorted(TPCH_TABLES):
        written = _export_table(connection, table, target_dir)
        print("wrote {0}".format(written))
    connection.close()


def _parse_args():
    """Parse the scale factor and target directory."""
    parser = argparse.ArgumentParser(description="Export TPC-H tables to Parquet.")
    parser.add_argument("--scale-factor", default="0.01")
    parser.add_argument("--target-dir", required=True)
    return parser.parse_args()


def main():
    """Entry point: export the SF database to a Parquet directory."""
    args = _parse_args()
    export(args.scale_factor, args.target_dir)


if __name__ == "__main__":
    main()
