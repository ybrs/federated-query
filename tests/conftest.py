"""Session-wide test fixtures.

The Rust engine is the default execution path (see Executor); the DuckDB merge
engine runs only under FEDQ_ENGINE=duckdb or as the fallback for a plan the Rust
engine does not yet cover. Tests that assert on Python-side pushdown SQL set
FEDQ_ENGINE=duckdb via the ``duckdb_engine`` fixture.

This file just removes the temp DuckDB files handed out to file-backed sources.
"""

import os


def pytest_sessionfinish(session, exitstatus):
    """Remove temp DuckDB files created for file-backed test sources."""
    from tests.duckdb_tmp import cleanup_duckdb_paths

    cleanup_duckdb_paths()
