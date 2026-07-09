"""Persistent SQLite store of learned cardinality observations.

The optimizer GUESSES cardinalities from source statistics, with the
NDV-independence and defaulting this project has repeatedly been bitten by
(dim-shipping collapse, q23/q39 gating, q54 reduction orientation). A federated
engine can instead MEASURE: it materializes every cross-source intermediate
anyway, so the exact size and distinct-key counts are free at runtime. This
catalog persists those measurements, keyed by logical identity, and serves them
back to warm future planning. One catalog per fedq configuration, keyed by the
config's datasource names.

Correctness-neutral by construction: a learned observation only changes WHICH
plan the optimizer picks (reduction orientation, gates, join order) - never the
answer. A stale or wrong value costs a slow query, never a wrong one. So writes
SELF-HEAL (every execution re-measures and upserts the truth) and reads apply a
TTL (older-than-TTL falls through to source stats); nothing here can produce an
incorrect result. See adaptive-catalog-plan.md.
"""

import json
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

# The table-level row count has no column, but SQLite treats NULL as distinct in
# a UNIQUE/PRIMARY KEY (NULLs never compare equal), which would defeat the
# upsert. So a table-level row uses this sentinel in column_name instead of NULL.
_TABLE_LEVEL = ""

# The measured fields of table_stats that _upsert_table_stat may write. A field
# outside this set is a programming error and raises rather than build bad SQL.
_TABLE_STAT_FIELDS = {"measured_rows", "measured_ndv"}

_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS source_identity (
        datasource TEXT PRIMARY KEY,
        source_fingerprint TEXT,
        first_seen TEXT,
        last_seen TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS table_stats (
        datasource TEXT NOT NULL,
        schema_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL DEFAULT '',
        measured_rows INTEGER,
        measured_ndv INTEGER,
        null_fraction REAL,
        min_val TEXT,
        max_val TEXT,
        observed_at TEXT,
        observation_count INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (datasource, schema_name, table_name, column_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS predicate_stats (
        datasource TEXT NOT NULL,
        schema_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
        predicate_template TEXT NOT NULL,
        param_bucket TEXT NOT NULL DEFAULT '',
        measured_input_rows INTEGER,
        measured_output_rows INTEGER,
        selectivity REAL,
        observed_at TEXT,
        observation_count INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (datasource, schema_name, table_name, predicate_template, param_bucket)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS group_stats (
        subject TEXT NOT NULL,
        group_key_set TEXT NOT NULL,
        measured_group_count INTEGER,
        measured_input_rows INTEGER,
        observed_at TEXT,
        observation_count INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (subject, group_key_set)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS subplan_stats (
        subplan_signature TEXT PRIMARY KEY,
        measured_output_rows INTEGER,
        output_key_ndv TEXT,
        observed_at TEXT,
        observation_count INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS materialized_fragments (
        subplan_signature TEXT PRIMARY KEY,
        location TEXT,
        measured_rows INTEGER,
        materialized_at TEXT,
        source_fingerprint TEXT
    )
    """,
)


def _utc_now() -> str:
    """The current UTC time as an ISO-8601 string (the observed_at stamp)."""
    return datetime.now(timezone.utc).isoformat()


def group_key(columns: List[str]) -> str:
    """The canonical group_key_set string for a list of GROUP BY column names.
    Sorted + JSON so the write side (a pushed aggregate scan) and the read side
    (the cost model's aggregate estimate) produce the IDENTICAL key regardless
    of column order."""
    return json.dumps(sorted(columns))


class StatsCatalog:
    """A SQLite-backed store of learned cardinality observations for one config.

    Writes come from execution measurements (self-healing upserts); reads warm
    the planner, applying an optional TTL. Single-process; a future move to
    PostgreSQL handles concurrency without changing this interface.
    """

    def __init__(self, path: str, clock=_utc_now):
        """Open (creating if absent) the catalog at `path` and ensure its schema.

        `clock` supplies observed_at stamps; it is injectable so tests control
        freshness without sleeping.
        """
        self._conn = sqlite3.connect(path)
        self._clock = clock
        self._tune()
        self._ensure_schema()

    def _tune(self) -> None:
        """WAL + synchronous=NORMAL: the write path runs during the timed query,
        so a per-commit fsync would tax every execution (measured +~50ms/query
        at SF10). WAL with NORMAL syncs far less while staying crash-safe enough
        for a stats cache whose values only steer plan choice."""
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")

    def _ensure_schema(self) -> None:
        """Create every catalog table if absent (idempotent on every open)."""
        for statement in _SCHEMA:
            self._conn.execute(statement)
        self._conn.commit()

    def commit(self) -> None:
        """Flush pending upserts. The write methods do NOT commit each - the
        query's whole batch of observations commits ONCE (per-upsert fsyncs were
        the +50ms/query tax)."""
        self._conn.commit()

    def close(self) -> None:
        """Commit any pending writes and close the connection."""
        self._conn.commit()
        self._conn.close()

    # --- write path (from execution measurements) -----------------------------

    def record_table_rows(
        self, datasource: str, schema: str, table: str, rows: int
    ) -> None:
        """Record a table's measured base (unfiltered) row count."""
        self._upsert_table_stat(datasource, schema, table, _TABLE_LEVEL,
                                 "measured_rows", rows)

    def record_column_ndv(
        self, datasource: str, schema: str, table: str, column: str, ndv: int
    ) -> None:
        """Record a column's measured distinct-value count (exact, from a
        collect_distinct the engine ran anyway)."""
        self._upsert_table_stat(datasource, schema, table, column,
                                 "measured_ndv", ndv)

    def _upsert_table_stat(self, datasource, schema, table, column, field, value):
        """Upsert one measured field of a table_stats row, refreshing observed_at
        and bumping observation_count. A field outside the allow-list raises."""
        if field not in _TABLE_STAT_FIELDS:
            raise ValueError(f"unknown table_stats field {field!r}")
        sql = (
            "INSERT INTO table_stats "
            "(datasource, schema_name, table_name, column_name, {field}, observed_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(datasource, schema_name, table_name, column_name) "
            "DO UPDATE SET {field}=excluded.{field}, observed_at=excluded.observed_at, "
            "observation_count=table_stats.observation_count+1"
        ).format(field=field)
        self._conn.execute(
            sql, (datasource, schema, table, column, value, self._clock())
        )

    def record_predicate(
        self, datasource: str, schema: str, table: str, template: str,
        input_rows: int, output_rows: int, param_bucket: str = "",
    ) -> None:
        """Record a filter template's measured input/output rows and selectivity."""
        selectivity = self._selectivity(input_rows, output_rows)
        self._conn.execute(
            "INSERT INTO predicate_stats (datasource, schema_name, table_name, "
            "predicate_template, param_bucket, measured_input_rows, "
            "measured_output_rows, selectivity, observed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(datasource, schema_name, table_name, predicate_template, "
            "param_bucket) DO UPDATE SET measured_input_rows=excluded.measured_input_rows, "
            "measured_output_rows=excluded.measured_output_rows, "
            "selectivity=excluded.selectivity, observed_at=excluded.observed_at, "
            "observation_count=predicate_stats.observation_count+1",
            (datasource, schema, table, template, param_bucket, input_rows,
             output_rows, selectivity, self._clock()),
        )

    def _selectivity(self, input_rows: int, output_rows: int) -> Optional[float]:
        """output/input as a fraction, or None when the input row count is zero
        (no selectivity is defined and dividing would raise)."""
        if not input_rows:
            return None
        return output_rows / input_rows

    def record_group(
        self, subject: str, group_columns: List[str], group_count: int,
        input_rows: Optional[int] = None,
    ) -> None:
        """Record a GROUP BY's MEASURED output-row count (the number of groups)
        for a subject (a table, or a subplan signature) and its group-key set.
        This is the collapse signal the cost model cannot estimate - the
        NDV-independence product over-counts - but the engine measures exactly."""
        self._conn.execute(
            "INSERT INTO group_stats (subject, group_key_set, measured_group_count, "
            "measured_input_rows, observed_at) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(subject, group_key_set) DO UPDATE SET "
            "measured_group_count=excluded.measured_group_count, "
            "measured_input_rows=excluded.measured_input_rows, "
            "observed_at=excluded.observed_at, "
            "observation_count=group_stats.observation_count+1",
            (subject, group_key(group_columns), group_count, input_rows, self._clock()),
        )

    # --- read path (warm the planner; TTL falls through to source stats) -------

    def group_count(
        self, subject: str, group_columns: List[str], max_age_seconds=None
    ) -> Optional[int]:
        """The learned number of groups for a subject's GROUP BY key set, or None
        (absent/stale). The measured collapse the cost model uses in place of its
        NDV-independence product."""
        row = self._conn.execute(
            "SELECT measured_group_count, observed_at FROM group_stats WHERE "
            "subject=? AND group_key_set=?",
            (subject, group_key(group_columns)),
        ).fetchone()
        return self._fresh_value(row, max_age_seconds)

    def table_rows(
        self, datasource: str, schema: str, table: str, max_age_seconds=None
    ) -> Optional[int]:
        """The learned base row count, or None when absent or older than the TTL."""
        row = self._conn.execute(
            "SELECT measured_rows, observed_at FROM table_stats WHERE "
            "datasource=? AND schema_name=? AND table_name=? AND column_name=?",
            (datasource, schema, table, _TABLE_LEVEL),
        ).fetchone()
        return self._fresh_value(row, max_age_seconds)

    def column_ndv(
        self, datasource: str, schema: str, table: str, column: str,
        max_age_seconds=None,
    ) -> Optional[int]:
        """The learned distinct-value count for a column, or None (absent/stale)."""
        row = self._conn.execute(
            "SELECT measured_ndv, observed_at FROM table_stats WHERE "
            "datasource=? AND schema_name=? AND table_name=? AND column_name=?",
            (datasource, schema, table, column),
        ).fetchone()
        return self._fresh_value(row, max_age_seconds)

    def predicate_selectivity(
        self, datasource: str, schema: str, table: str, template: str,
        param_bucket: str = "", max_age_seconds=None,
    ) -> Optional[float]:
        """The learned selectivity of a filter template, or None (absent/stale)."""
        row = self._conn.execute(
            "SELECT selectivity, observed_at FROM predicate_stats WHERE "
            "datasource=? AND schema_name=? AND table_name=? AND "
            "predicate_template=? AND param_bucket=?",
            (datasource, schema, table, template, param_bucket),
        ).fetchone()
        return self._fresh_value(row, max_age_seconds)

    def _fresh_value(self, row, max_age_seconds):
        """The stored value from a (value, observed_at) row, or None when the row
        is absent, its value is NULL, or it is older than the TTL."""
        if row is None or row[0] is None:
            return None
        if self._is_stale(row[1], max_age_seconds):
            return None
        return row[0]

    def _is_stale(self, observed_at, max_age_seconds) -> bool:
        """Whether an observed_at stamp is older than the TTL. No TTL (None) or a
        missing/unparseable stamp is treated as fresh (the value still self-heals
        on the next measurement)."""
        if max_age_seconds is None or not observed_at:
            return False
        age = self._now_dt() - datetime.fromisoformat(observed_at)
        return age.total_seconds() > max_age_seconds

    def _now_dt(self) -> datetime:
        """The clock's current instant as a datetime for TTL arithmetic."""
        return datetime.fromisoformat(self._clock())
