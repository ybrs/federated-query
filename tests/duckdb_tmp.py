"""A fresh DuckDB file path per call, for tests.

Tests use real DuckDB files, never in-memory: an in-memory database lives in a
single connection and cannot be read by the native (Rust) engine, and every data
source in the engine is a real file. Each call hands out a unique path; all of
them are removed at the end of the test session (see the root conftest).
"""

import os
import shutil
import tempfile

_DIRS = []


def duckdb_path():
    """Return a fresh, unique DuckDB database file path (temp dir per call)."""
    directory = tempfile.mkdtemp(prefix="fedq_test_duckdb_")
    _DIRS.append(directory)
    return os.path.join(directory, "db.duckdb")


def cleanup_duckdb_paths():
    """Remove every temp directory handed out by duckdb_path()."""
    for directory in _DIRS:
        shutil.rmtree(directory, ignore_errors=True)
    _DIRS.clear()
