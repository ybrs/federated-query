//! The materialized-view registry: the `materialized_views` table in the same
//! SQLite file as the learned-stats catalog (one store per config).
//!
//! The catalog row is the source of truth for a view's existence and for WHICH
//! chunk files compose it (`chunk_list`); the chunk directory itself is never
//! listed to answer a read. Rows are published/retired in single short
//! transactions so a reader that loads the row then opens the listed chunks
//! never observes a partial publication. Dropping is tombstone-safe: a
//! tombstoned row stops resolving immediately, its files are unlinked after,
//! and `sweep_tombstones` finishes any drop a crash interrupted.

use std::collections::BTreeMap;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::AccelError;
use crate::view::{ChangeKeyState, MaterializedView, ViewColumn};

/// Supplies timestamp strings (RFC3339); injectable so tests control time.
pub type Clock = fq_catalog::Clock;

/// The registry table. `source_tokens` is the JSON token map captured before
/// the last pull; `change_key` is the JSON delta-append state (NULL for a
/// view last pulled by merge or whole re-pull, which carry no state).
/// `use_count`/`cost_saved_ms` are the substitution benefit counters
/// (`record_substitution`); a table created before they existed gains them
/// through `add_benefit_columns`, with existing rows reading back as 0.
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS materialized_views (
    name TEXT PRIMARY KEY,
    definition_sql TEXT NOT NULL,
    location TEXT NOT NULL,
    chunk_list TEXT NOT NULL,
    output_schema TEXT NOT NULL,
    source_tokens TEXT,
    change_key TEXT,
    measured_rows INTEGER NOT NULL,
    byte_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    refreshed_at TEXT,
    deleted_at TEXT,
    use_count INTEGER NOT NULL DEFAULT 0,
    cost_saved_ms REAL NOT NULL DEFAULT 0
)";

/// The SELECT list every row read uses, in `row_to_view` order.
const ROW_COLUMNS: &str = "name, definition_sql, location, chunk_list, output_schema, \
     measured_rows, byte_size, created_at, refreshed_at, source_tokens, change_key, \
     use_count, cost_saved_ms";

/// A SQLite-backed registry of materialized views for one config.
pub struct ViewCatalog {
    conn: Mutex<Connection>,
    clock: Clock,
}

impl ViewCatalog {
    /// Open (creating the table if absent) the registry at `path` - the
    /// config's learned-stats SQLite file - with the wall-clock stamp.
    pub fn open(path: &str) -> Result<Self, AccelError> {
        Self::open_with_clock(path, default_clock())
    }

    /// Open the registry with an injected clock (tests control timestamps).
    pub fn open_with_clock(path: &str, clock: Clock) -> Result<Self, AccelError> {
        let conn = Connection::open(path)?;
        // WAL matches the learned-stats catalog sharing this file: many
        // readers plus one writer across the runtimes on one store. Unlike the
        // stats side, view rows are durable state, so synchronous stays ON.
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute(SCHEMA, [])?;
        add_benefit_columns(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            clock,
        })
    }

    /// Register a new view in one transaction. A name that already exists (live
    /// or tombstoned) raises `DuplicateView`: creation never silently replaces,
    /// and a tombstoned row still owns its directory until swept.
    pub fn register(&self, view: &MaterializedView) -> Result<(), AccelError> {
        let inserted = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .execute(
                "INSERT OR IGNORE INTO materialized_views \
             (name, definition_sql, location, chunk_list, output_schema, \
              measured_rows, byte_size, created_at, source_tokens, change_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    view.name,
                    view.definition_sql,
                    view.location,
                    serde_json::to_string(&view.chunk_list)?,
                    serde_json::to_string(&view.columns)?,
                    view.measured_rows,
                    view.byte_size,
                    (self.clock)(),
                    serde_json::to_string(&view.source_tokens)?,
                    encode_change_key(view.change_key.as_ref())?,
                ],
            )?;
        if inserted == 0 {
            return Err(AccelError::DuplicateView(view.name.clone()));
        }
        Ok(())
    }

    /// The live (non-tombstoned) view named `name`, or None.
    pub fn get(&self, name: &str) -> Result<Option<MaterializedView>, AccelError> {
        let sql = format!(
            "SELECT {ROW_COLUMNS} FROM materialized_views \
             WHERE name = ?1 AND deleted_at IS NULL"
        );
        let row = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .query_row(&sql, params![name], row_to_raw)
            .optional()?;
        match row {
            Some(raw) => Ok(Some(raw_to_view(raw)?)),
            None => Ok(None),
        }
    }

    /// Every live view, ordered by name.
    pub fn list_live(&self) -> Result<Vec<MaterializedView>, AccelError> {
        self.list_where("deleted_at IS NULL")
    }

    /// Every tombstoned view (a drop was started but its files may remain).
    pub fn list_tombstoned(&self) -> Result<Vec<MaterializedView>, AccelError> {
        self.list_where("deleted_at IS NOT NULL")
    }

    /// Rows matching a deleted_at predicate, ordered by name.
    fn list_where(&self, predicate: &str) -> Result<Vec<MaterializedView>, AccelError> {
        let sql =
            format!("SELECT {ROW_COLUMNS} FROM materialized_views WHERE {predicate} ORDER BY name");
        let conn = self.conn.lock().expect("view catalog lock poisoned");
        let mut statement = conn.prepare(&sql)?;
        let rows = statement.query_map([], row_to_raw)?;
        let mut views = Vec::new();
        for raw in rows {
            views.push(raw_to_view(raw?)?);
        }
        Ok(views)
    }

    /// Swap a live view's chunk list (a refresh publication) in one
    /// transaction: the new chunks, sizes, source tokens, change-key state,
    /// and `refreshed_at` land together, AFTER the new chunk files are already
    /// in place - a reader sees either the whole old row or the whole new one.
    /// Raises `UnknownView` when the name is absent or tombstoned.
    pub fn publish_refresh(
        &self,
        name: &str,
        chunk_list: &[String],
        measured_rows: i64,
        byte_size: i64,
        source_tokens: &BTreeMap<String, String>,
        change_key: Option<&ChangeKeyState>,
    ) -> Result<(), AccelError> {
        let updated = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .execute(
                "UPDATE materialized_views SET chunk_list = ?2, measured_rows = ?3, \
             byte_size = ?4, refreshed_at = ?5, source_tokens = ?6, change_key = ?7 \
             WHERE name = ?1 AND deleted_at IS NULL",
                params![
                    name,
                    serde_json::to_string(chunk_list)?,
                    measured_rows,
                    byte_size,
                    (self.clock)(),
                    serde_json::to_string(source_tokens)?,
                    encode_change_key(change_key)?,
                ],
            )?;
        if updated == 0 {
            return Err(AccelError::UnknownView(name.to_string()));
        }
        Ok(())
    }

    /// Tombstone a live view and return its row: the view stops resolving
    /// immediately; its chunk files are unlinked by the caller afterwards and
    /// the row is purged last. Raises `UnknownView` when absent/tombstoned.
    pub fn tombstone(&self, name: &str) -> Result<MaterializedView, AccelError> {
        let view = self
            .get(name)?
            .ok_or_else(|| AccelError::UnknownView(name.to_string()))?;
        let updated = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .execute(
                "UPDATE materialized_views SET deleted_at = ?2 \
             WHERE name = ?1 AND deleted_at IS NULL",
                params![name, (self.clock)()],
            )?;
        if updated == 0 {
            return Err(AccelError::UnknownView(name.to_string()));
        }
        Ok(view)
    }

    /// Record one automatic substitution against a live view: bump `use_count`
    /// and add `saved` (the cost model's estimated saving) to `cost_saved_ms`,
    /// in one short transaction. A tombstoned or absent view is a no-op (the
    /// benefit of a view being dropped need not be recorded); the row-level
    /// `use_count = use_count + 1` is atomic under SQLite's single writer, so
    /// concurrent reuses across runtimes never lose a count.
    pub fn record_substitution(&self, name: &str, saved: f64) -> Result<(), AccelError> {
        self.conn
            .lock()
            .expect("view catalog lock poisoned")
            .execute(
                "UPDATE materialized_views \
                 SET use_count = use_count + 1, cost_saved_ms = cost_saved_ms + ?2 \
                 WHERE name = ?1 AND deleted_at IS NULL",
                params![name, saved],
            )?;
        Ok(())
    }

    /// Delete a tombstoned row outright (the final step of a drop, after its
    /// chunk files are gone).
    pub fn purge(&self, name: &str) -> Result<(), AccelError> {
        self.conn
            .lock()
            .expect("view catalog lock poisoned")
            .execute(
                "DELETE FROM materialized_views WHERE name = ?1 AND deleted_at IS NOT NULL",
                params![name],
            )?;
        Ok(())
    }
}

/// The raw row tuple before JSON decoding (rusqlite's closure cannot return
/// our error type, so decoding happens outside it).
type RawRow = (
    String,
    String,
    String,
    String,
    String,
    i64,
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    i64,
    f64,
);

/// Read one row's columns in `ROW_COLUMNS` order.
fn row_to_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
        row.get(12)?,
    ))
}

/// Decode a raw row's JSON columns into the view record.
fn raw_to_view(raw: RawRow) -> Result<MaterializedView, AccelError> {
    let (
        name,
        definition_sql,
        location,
        chunk_list,
        output_schema,
        measured_rows,
        byte_size,
        created_at,
        refreshed_at,
        source_tokens,
        change_key,
        use_count,
        cost_saved,
    ) = raw;
    let chunk_list: Vec<String> = serde_json::from_str(&chunk_list)?;
    let columns: Vec<ViewColumn> = serde_json::from_str(&output_schema)?;
    Ok(MaterializedView {
        name,
        definition_sql,
        location,
        chunk_list,
        columns,
        measured_rows,
        byte_size,
        created_at,
        refreshed_at,
        source_tokens: decode_tokens(source_tokens.as_deref())?,
        change_key: decode_change_key(change_key.as_deref())?,
        use_count,
        cost_saved,
    })
}

/// Add `use_count` / `cost_saved_ms` to a `materialized_views` table created
/// before substitution benefit tracking existed. A no-op once both are present;
/// existing rows read back as 0 (no substitutions recorded yet).
fn add_benefit_columns(conn: &Connection) -> Result<(), AccelError> {
    let mut present = std::collections::BTreeSet::new();
    let mut statement = conn.prepare("SELECT name FROM pragma_table_info('materialized_views')")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        present.insert(row.get::<_, String>(0)?);
    }
    if !present.contains("use_count") {
        conn.execute(
            "ALTER TABLE materialized_views ADD COLUMN use_count INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !present.contains("cost_saved_ms") {
        conn.execute(
            "ALTER TABLE materialized_views ADD COLUMN cost_saved_ms REAL NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}

/// Encode the optional change-key state for its nullable TEXT column.
fn encode_change_key(state: Option<&ChangeKeyState>) -> Result<Option<String>, AccelError> {
    match state {
        Some(state) => Ok(Some(serde_json::to_string(state)?)),
        None => Ok(None),
    }
}

/// Decode the token map; a NULL column is an empty map.
fn decode_tokens(text: Option<&str>) -> Result<BTreeMap<String, String>, AccelError> {
    match text {
        Some(text) => Ok(serde_json::from_str(text)?),
        None => Ok(BTreeMap::new()),
    }
}

/// Decode the optional change-key state.
fn decode_change_key(text: Option<&str>) -> Result<Option<ChangeKeyState>, AccelError> {
    match text {
        Some(text) => Ok(Some(serde_json::from_str(text)?)),
        None => Ok(None),
    }
}

/// The wall-clock stamp: current UTC time as an RFC3339 string.
fn default_clock() -> Clock {
    std::sync::Arc::new(|| chrono::Utc::now().to_rfc3339())
}
