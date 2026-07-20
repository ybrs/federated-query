//! Persistent SQLite store of learned cardinality observations. Ports
//! `catalog/stats_catalog.py`.
//!
//! The optimizer GUESSES cardinalities from source statistics; a federated engine
//! can instead MEASURE (it materializes every cross-source intermediate anyway).
//! This catalog persists those measurements, keyed by logical identity, and
//! serves them back to warm future planning. Correctness-neutral by
//! construction: a learned value only changes WHICH plan is picked, never the
//! answer, so writes SELF-HEAL (upsert the truth) and reads apply a TTL.
//!
//! BYTE-COMPATIBLE: the SQLite schema and the `group_key_set` string format are
//! reproduced EXACTLY (including Python `json.dumps(sorted(...))`'s `", "`
//! element separator) so an existing learned catalog opens and matches unchanged.
//!
//! PORT NOTES:
//! - The `_upsert_table_stat` string field allow-list (and its
//!   `test_unknown_field_raises` guard) retire: the field is a two-variant enum
//!   (`TableStatField`), so an invalid column is unrepresentable at compile
//!   time (a stronger guarantee than the runtime check), and no dynamic SQL over
//!   an unvetted name is possible.
//! - `source_identity`, `subplan_stats`, and `materialized_fragments` tables are
//!   CREATED (schema parity) but have no read/write surface here - nothing in
//!   this workspace consumes them (repoint detection, subplan-stats reads, the
//!   accelerator all read the store elsewhere); no test here exercises them.
//! - The `_persist_observations` / `_plain_group_columns` tests and the
//!   CostModel/StatisticsCollector overlay tests in test_stats_catalog.py test
//!   OTHER crates (fq-physical/fq-runtime and fq-optimize); they translate with
//!   those crates, not here.

use chrono::DateTime;
use rusqlite::{params, Connection, ErrorCode, OptionalExtension, Params};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// The table-level row count has no column; SQLite treats NULL as distinct in a
/// PRIMARY KEY, which would defeat the upsert, so a table-level row uses this
/// empty-string sentinel in `column_name`.
const TABLE_LEVEL: &str = "";

/// Supplies `observed_at` stamps (RFC3339 / ISO-8601). Injectable so tests
/// control freshness without sleeping.
pub type Clock = Arc<dyn Fn() -> String + Send + Sync>;

/// Failure talking to the learned-stats catalog.
#[derive(Debug, Error)]
pub enum StatsError {
    /// Underlying SQLite error.
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

/// The measured table_stats field an upsert may write. An enum, so an invalid
/// column is unrepresentable (the retired Python string allow-list).
#[derive(Clone, Copy)]
enum TableStatField {
    MeasuredRows,
    MeasuredNdv,
}

impl TableStatField {
    // The SQLite column name for this field. Static strings only, so formatting
    // it into SQL cannot inject.
    fn column(self) -> &'static str {
        match self {
            TableStatField::MeasuredRows => "measured_rows",
            TableStatField::MeasuredNdv => "measured_ndv",
        }
    }
}

/// The six catalog tables, reproduced verbatim from `stats_catalog.py::_SCHEMA`
/// so an existing on-disk catalog is byte-compatible.
const SCHEMA: [&str; 6] = [
    "CREATE TABLE IF NOT EXISTS source_identity (
        datasource TEXT PRIMARY KEY,
        source_fingerprint TEXT,
        first_seen TEXT,
        last_seen TEXT
    )",
    "CREATE TABLE IF NOT EXISTS table_stats (
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
    )",
    "CREATE TABLE IF NOT EXISTS predicate_stats (
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
    )",
    "CREATE TABLE IF NOT EXISTS group_stats (
        subject TEXT NOT NULL,
        group_key_set TEXT NOT NULL,
        measured_group_count INTEGER,
        measured_input_rows INTEGER,
        observed_at TEXT,
        observation_count INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (subject, group_key_set)
    )",
    "CREATE TABLE IF NOT EXISTS subplan_stats (
        subplan_signature TEXT PRIMARY KEY,
        measured_output_rows INTEGER,
        output_key_ndv TEXT,
        observed_at TEXT,
        observation_count INTEGER NOT NULL DEFAULT 1
    )",
    "CREATE TABLE IF NOT EXISTS materialized_fragments (
        subplan_signature TEXT PRIMARY KEY,
        location TEXT,
        measured_rows INTEGER,
        materialized_at TEXT,
        source_fingerprint TEXT
    )",
];

/// The canonical `group_key_set` string for a list of GROUP BY column names.
///
/// Sorted + JSON so the write and read sides produce the IDENTICAL key
/// regardless of column order. Matches Python `json.dumps(sorted(columns))`
/// byte-for-byte, including the `", "` element separator, so a catalog written by
/// either engine keys the same.
pub fn group_key(columns: &[String]) -> String {
    let mut sorted: Vec<&String> = columns.iter().collect();
    sorted.sort();
    let mut parts = Vec::with_capacity(sorted.len());
    for column in sorted {
        // serde_json escapes the string exactly as Python's json module does.
        parts.push(serde_json::to_string(column).expect("string serializes"));
    }
    format!("[{}]", parts.join(", "))
}

/// How long a statement waits on a held write lock before SQLite gives up. The
/// busy handler covers ordinary contention; the open-time retry covers the
/// journal-mode transition the handler does not.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Retries granted to a busy open step before its real SQLite error surfaces.
/// The backoff doubles from 2ms, so the total wait is bounded (~0.5s) and the
/// open FAILS LOUDLY rather than hanging when the lock never clears.
const OPEN_BUSY_RETRIES: u32 = 8;

/// Whether a SQLite error is a transient lock contention (BUSY or LOCKED) that a
/// retry may clear, as opposed to a real failure.
fn is_busy(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(inner, _)
            if matches!(inner.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

/// Run an idempotent open step, retrying on BUSY/LOCKED with a doubling backoff.
/// Bounded: after `OPEN_BUSY_RETRIES` waits a final attempt runs and its result -
/// success or the genuine error - is returned, so a persistent lock never hangs.
fn with_busy_retry<T>(mut step: impl FnMut() -> Result<T, StatsError>) -> Result<T, StatsError> {
    let mut backoff = Duration::from_millis(2);
    for _ in 0..OPEN_BUSY_RETRIES {
        match step() {
            Err(StatsError::Sqlite(err)) if is_busy(&err) => {
                std::thread::sleep(backoff);
                backoff = backoff.saturating_mul(2);
            }
            result => return result,
        }
    }
    step()
}

/// A SQLite-backed store of learned cardinality observations for one config.
pub struct StatsCatalog {
    conn: std::sync::Mutex<Connection>,
    clock: Clock,
}

impl StatsCatalog {
    /// Open (creating if absent) the catalog at `path` with the wall-clock stamp.
    pub fn open(path: &str) -> Result<Self, StatsError> {
        Self::open_with_clock(path, default_clock())
    }

    /// Open the catalog with an injected clock (tests control freshness).
    pub fn open_with_clock(path: &str, clock: Clock) -> Result<Self, StatsError> {
        let conn = Connection::open(path)?;
        // Make ordinary statement contention WAIT for the write lock rather than
        // fail immediately; the write path relies on this handler.
        conn.busy_timeout(BUSY_TIMEOUT)?;
        let catalog = Self {
            conn: std::sync::Mutex::new(conn),
            clock,
        };
        // The WAL transition and CREATE TABLE DDL need a momentary exclusive lock
        // that concurrent opens of one fresh file contend for; SQLite can return
        // BUSY for the journal-mode change WITHOUT invoking the busy handler, so
        // these idempotent steps are retried explicitly.
        with_busy_retry(|| catalog.tune())?;
        with_busy_retry(|| catalog.ensure_schema())?;
        Ok(catalog)
    }

    /// WAL + synchronous=OFF: the write path runs during the timed query, so no
    /// fsync at all - the catalog only steers plan choice, never correctness, so
    /// losing the last few observations to an OS crash costs at most a cold
    /// estimate next run. This is what lets stat collection be ALWAYS ON.
    fn tune(&self) -> Result<(), StatsError> {
        self.conn
            .lock()
            .expect("stats catalog lock poisoned")
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF;")?;
        Ok(())
    }

    /// Create every catalog table if absent (idempotent on every open).
    fn ensure_schema(&self) -> Result<(), StatsError> {
        for statement in SCHEMA {
            self.conn
                .lock()
                .expect("stats catalog lock poisoned")
                .execute(statement, [])?;
        }
        Ok(())
    }

    // --- write path (from execution measurements) -----------------------------

    /// Record a table's measured base (unfiltered) row count.
    pub fn record_table_rows(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        rows: i64,
    ) -> Result<(), StatsError> {
        self.upsert_table_stat(
            datasource,
            schema,
            table,
            TABLE_LEVEL,
            TableStatField::MeasuredRows,
            rows,
        )
    }

    /// Record a column's measured distinct-value count (exact, from a
    /// collect_distinct the engine ran anyway).
    pub fn record_column_ndv(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        column: &str,
        ndv: i64,
    ) -> Result<(), StatsError> {
        self.upsert_table_stat(
            datasource,
            schema,
            table,
            column,
            TableStatField::MeasuredNdv,
            ndv,
        )
    }

    /// Upsert one measured field of a table_stats row, refreshing observed_at and
    /// bumping observation_count.
    fn upsert_table_stat(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        column: &str,
        field: TableStatField,
        value: i64,
    ) -> Result<(), StatsError> {
        let field = field.column();
        let sql = format!(
            "INSERT INTO table_stats \
             (datasource, schema_name, table_name, column_name, {field}, observed_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(datasource, schema_name, table_name, column_name) \
             DO UPDATE SET {field}=excluded.{field}, observed_at=excluded.observed_at, \
             observation_count=table_stats.observation_count+1"
        );
        self.conn
            .lock()
            .expect("stats catalog lock poisoned")
            .execute(
                &sql,
                params![datasource, schema, table, column, value, (self.clock)()],
            )?;
        Ok(())
    }

    /// Record a filter template's measured input/output rows and selectivity.
    pub fn record_predicate(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        template: &str,
        input_rows: Option<i64>,
        output_rows: i64,
        param_bucket: &str,
    ) -> Result<(), StatsError> {
        let selectivity = selectivity(input_rows, output_rows);
        self.conn
            .lock()
            .expect("stats catalog lock poisoned")
            .execute(
                "INSERT INTO predicate_stats (datasource, schema_name, table_name, \
             predicate_template, param_bucket, measured_input_rows, \
             measured_output_rows, selectivity, observed_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
             ON CONFLICT(datasource, schema_name, table_name, predicate_template, \
             param_bucket) DO UPDATE SET measured_input_rows=excluded.measured_input_rows, \
             measured_output_rows=excluded.measured_output_rows, \
             selectivity=excluded.selectivity, observed_at=excluded.observed_at, \
             observation_count=predicate_stats.observation_count+1",
                params![
                    datasource,
                    schema,
                    table,
                    template,
                    param_bucket,
                    input_rows,
                    output_rows,
                    selectivity,
                    (self.clock)()
                ],
            )?;
        Ok(())
    }

    /// Record a GROUP BY's MEASURED output-row count (number of groups) for a
    /// subject (a table or a subplan signature) and its group-key set.
    pub fn record_group(
        &self,
        subject: &str,
        group_columns: &[String],
        group_count: i64,
        input_rows: Option<i64>,
    ) -> Result<(), StatsError> {
        self.conn
            .lock()
            .expect("stats catalog lock poisoned")
            .execute(
                "INSERT INTO group_stats (subject, group_key_set, measured_group_count, \
             measured_input_rows, observed_at) VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(subject, group_key_set) DO UPDATE SET \
             measured_group_count=excluded.measured_group_count, \
             measured_input_rows=excluded.measured_input_rows, \
             observed_at=excluded.observed_at, \
             observation_count=group_stats.observation_count+1",
                params![
                    subject,
                    group_key(group_columns),
                    group_count,
                    input_rows,
                    (self.clock)()
                ],
            )?;
        Ok(())
    }

    /// Purge every learned row keyed on `datasource`: its `table_stats`,
    /// `predicate_stats`, and `source_identity` rows, and the `group_stats` rows
    /// whose subject is one of its tables (`datasource.schema.table`).
    /// Subplan-signature subjects carry no datasource prefix and are left to
    /// self-heal. Run on `DROP DATASOURCE` so a later create reusing the name
    /// (pointing elsewhere) does not inherit the old source's measurements.
    pub fn purge_datasource(&self, datasource: &str) -> Result<(), StatsError> {
        let conn = self.conn.lock().expect("stats catalog lock poisoned");
        conn.execute(
            "DELETE FROM table_stats WHERE datasource = ?1",
            params![datasource],
        )?;
        conn.execute(
            "DELETE FROM predicate_stats WHERE datasource = ?1",
            params![datasource],
        )?;
        conn.execute(
            "DELETE FROM source_identity WHERE datasource = ?1",
            params![datasource],
        )?;
        let prefix = format!("{}.", like_escape(datasource));
        conn.execute(
            "DELETE FROM group_stats WHERE subject = ?1 OR subject LIKE ?2 ESCAPE '\\'",
            params![datasource, format!("{prefix}%")],
        )?;
        Ok(())
    }

    // --- read path (warm the planner; TTL falls through to source stats) -------

    /// The learned base row count, or None when absent or older than the TTL.
    pub fn table_rows(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        max_age_seconds: Option<i64>,
    ) -> Result<Option<i64>, StatsError> {
        self.fresh(
            "SELECT measured_rows, observed_at FROM table_stats WHERE \
             datasource=?1 AND schema_name=?2 AND table_name=?3 AND column_name=?4",
            params![datasource, schema, table, TABLE_LEVEL],
            max_age_seconds,
        )
    }

    /// The learned distinct-value count for a column, or None (absent/stale).
    pub fn column_ndv(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        column: &str,
        max_age_seconds: Option<i64>,
    ) -> Result<Option<i64>, StatsError> {
        self.fresh(
            "SELECT measured_ndv, observed_at FROM table_stats WHERE \
             datasource=?1 AND schema_name=?2 AND table_name=?3 AND column_name=?4",
            params![datasource, schema, table, column],
            max_age_seconds,
        )
    }

    /// The learned number of groups for a subject's GROUP BY key set, or None.
    pub fn group_count(
        &self,
        subject: &str,
        group_columns: &[String],
        max_age_seconds: Option<i64>,
    ) -> Result<Option<i64>, StatsError> {
        self.fresh(
            "SELECT measured_group_count, observed_at FROM group_stats WHERE \
             subject=?1 AND group_key_set=?2",
            params![subject, group_key(group_columns)],
            max_age_seconds,
        )
    }

    /// The learned selectivity of a filter template, or None (absent/stale).
    pub fn predicate_selectivity(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        template: &str,
        param_bucket: &str,
        max_age_seconds: Option<i64>,
    ) -> Result<Option<f64>, StatsError> {
        self.fresh(
            "SELECT selectivity, observed_at FROM predicate_stats WHERE \
             datasource=?1 AND schema_name=?2 AND table_name=?3 AND \
             predicate_template=?4 AND param_bucket=?5",
            params![datasource, schema, table, template, param_bucket],
            max_age_seconds,
        )
    }

    /// The learned MEASURED output row count of a filter template, or None. The
    /// direct measurement, read in preference to the stored selectivity ratio.
    pub fn predicate_output_rows(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        template: &str,
        param_bucket: &str,
        max_age_seconds: Option<i64>,
    ) -> Result<Option<i64>, StatsError> {
        self.fresh(
            "SELECT measured_output_rows, observed_at FROM predicate_stats WHERE \
             datasource=?1 AND schema_name=?2 AND table_name=?3 AND \
             predicate_template=?4 AND param_bucket=?5",
            params![datasource, schema, table, template, param_bucket],
            max_age_seconds,
        )
    }

    /// The observation_count of a table-level row (how many times it self-healed).
    /// Used to verify an upsert bumps rather than duplicates.
    pub fn table_observation_count(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<i64>, StatsError> {
        let count = self
            .conn
            .lock()
            .expect("stats catalog lock poisoned")
            .query_row(
                "SELECT observation_count FROM table_stats WHERE \
                 datasource=?1 AND schema_name=?2 AND table_name=?3 AND column_name=?4",
                params![datasource, schema, table, TABLE_LEVEL],
                |row| row.get(0),
            )
            .optional()?;
        Ok(count)
    }

    /// Read a `(value, observed_at)` row and apply the freshness contract: absent
    /// row, NULL value, or a value older than the TTL all read as None.
    fn fresh<T, P>(
        &self,
        sql: &str,
        params: P,
        max_age_seconds: Option<i64>,
    ) -> Result<Option<T>, StatsError>
    where
        T: rusqlite::types::FromSql,
        P: Params,
    {
        let row = self
            .conn
            .lock()
            .expect("stats catalog lock poisoned")
            .query_row(sql, params, |row| {
                let value: Option<T> = row.get(0)?;
                let observed_at: Option<String> = row.get(1)?;
                Ok((value, observed_at))
            })
            .optional()?;
        match row {
            None | Some((None, _)) => Ok(None),
            Some((Some(value), observed_at)) => {
                if self.is_stale(observed_at.as_deref(), max_age_seconds) {
                    Ok(None)
                } else {
                    Ok(Some(value))
                }
            }
        }
    }

    /// Whether an observed_at stamp is older than the TTL. No TTL, a missing
    /// stamp, or an unparseable stamp is treated as fresh (the value still
    /// self-heals on the next measurement).
    fn is_stale(&self, observed_at: Option<&str>, max_age_seconds: Option<i64>) -> bool {
        let (Some(max_age), Some(observed_at)) = (max_age_seconds, observed_at) else {
            return false;
        };
        if observed_at.is_empty() {
            return false;
        }
        let now = (self.clock)();
        let (Ok(now_dt), Ok(observed_dt)) = (
            DateTime::parse_from_rfc3339(&now),
            DateTime::parse_from_rfc3339(observed_at),
        ) else {
            return false;
        };
        (now_dt - observed_dt).num_seconds() > max_age
    }
}

/// Escape the LIKE wildcards (`%`, `_`) and the escape char itself in a literal
/// so a datasource name containing one is matched as an exact prefix, not a
/// pattern (paired with `ESCAPE '\'`).
fn like_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// output/input as a fraction, or None when the input row count is unknown or
/// zero (no selectivity is defined). Mirrors Python's `if not input_rows`.
fn selectivity(input_rows: Option<i64>, output_rows: i64) -> Option<f64> {
    match input_rows {
        Some(input) if input != 0 => Some(output_rows as f64 / input as f64),
        _ => None,
    }
}

/// The wall-clock stamp: current UTC time as an RFC3339 / ISO-8601 string.
fn default_clock() -> Clock {
    Arc::new(|| chrono::Utc::now().to_rfc3339())
}
