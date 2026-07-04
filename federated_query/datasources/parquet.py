"""Parquet directory data source.

A directory of ``<table>.parquet`` files exposed as one source. Metadata and
planning use an in-memory DuckDB with a view per file (so the catalog, schema
inference, and SQL rendering all work unchanged); the native (Rust) engine reads
the Parquet directly through DataFusion. The render dialect is DuckDB's, which
DataFusion also parses.
"""

import glob
import os
from typing import Any, Dict

from .duckdb import DuckDBDataSource


class ParquetDataSource(DuckDBDataSource):
    """A directory of Parquet files, federated as one source."""

    render_dialect = "duckdb"

    def __init__(self, name: str, config: Dict[str, Any]):
        """Config needs ``dir``: the directory holding the ``<table>.parquet`` files."""
        self.parquet_dir = config["dir"]
        super().__init__(name, {"path": ":memory:", "read_only": False})

    def connect(self) -> None:
        """Open the metadata DuckDB and load each Parquet file as a base table.

        Base tables (not views) so the catalog's ``list_tables`` and statistics
        queries see them; this copy is only for planning. The native (Rust)
        engine reads the real Parquet files directly.
        """
        super().connect()
        for path in sorted(glob.glob(os.path.join(self.parquet_dir, "*.parquet"))):
            table = os.path.splitext(os.path.basename(path))[0]
            self.connection.execute(
                f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{path}')"
            )
