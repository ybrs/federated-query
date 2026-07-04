"""Session-wide test fixtures.

The Rust engine is the one execution path (see Executor). This file just removes
the temp DuckDB files handed out to file-backed sources.
"""

import os


def pytest_sessionfinish(session, exitstatus):
    """Remove temp DuckDB files created for file-backed test sources."""
    from tests.duckdb_tmp import cleanup_duckdb_paths

    cleanup_duckdb_paths()
