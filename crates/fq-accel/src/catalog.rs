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
use std::time::Duration;

use rusqlite::{params, Connection, ErrorCode, OptionalExtension};

use crate::error::AccelError;
use crate::view::{ChangeKeyState, MaterializedView, ViewColumn};

/// How long a statement waits on a held write lock before SQLite gives up. The
/// busy handler covers ordinary contention; the open-time retry covers the
/// journal-mode transition the handler does not.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Retries granted to a busy open step before its real SQLite error surfaces.
/// The backoff doubles from 2ms, so the total wait is bounded and the open
/// FAILS LOUDLY rather than hanging when the lock never clears.
const OPEN_BUSY_RETRIES: u32 = 8;

/// Whether a SQLite error is transient lock contention (BUSY or LOCKED) a retry
/// may clear, as opposed to a real failure.
fn is_busy(error: &AccelError) -> bool {
    matches!(
        error,
        AccelError::Sqlite(rusqlite::Error::SqliteFailure(inner, _))
            if matches!(inner.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

/// Run an idempotent open step, retrying on BUSY/LOCKED with a doubling backoff.
/// SQLite can return BUSY for the WAL journal-mode transition and the CREATE
/// TABLE DDL WITHOUT invoking the busy handler when concurrent opens of one
/// fresh file contend, so the step is retried explicitly; after the bounded
/// retries the genuine error surfaces.
fn with_busy_retry<T>(mut step: impl FnMut() -> Result<T, AccelError>) -> Result<T, AccelError> {
    let mut backoff = Duration::from_millis(2);
    for _ in 0..OPEN_BUSY_RETRIES {
        match step() {
            Err(error) if is_busy(&error) => {
                std::thread::sleep(backoff);
                backoff = backoff.saturating_mul(2);
            }
            result => return result,
        }
    }
    step()
}

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

/// The dynamic-datasource registry table, in the same SQLite file (one store per
/// config). `params` is the JSON object of connection params as they were
/// authored (scalars); `type` is the connector kind. `capabilities`,
/// `change_keys`, and `updated_by` carry NULL: the DDL surface sets none of them
/// and they are the storage hooks a later credential-reference / ownership layer
/// fills. A table created before those columns existed gains them through
/// `add_datasource_optional_columns` on open. `synchronous` stays ON (durable
/// operator state), and the name is the PRIMARY KEY so a concurrent create
/// races on `INSERT OR IGNORE`.
const DATASOURCE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS dynamic_datasources (
    name          TEXT PRIMARY KEY,
    type          TEXT NOT NULL,
    params        TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    capabilities  TEXT,
    change_keys   TEXT,
    updated_by    TEXT
)";

/// One persisted dynamic datasource: its name, connector kind, and the JSON
/// connection params as authored (all string scalars). The runtime rebuilds a
/// `DataSourceConfig` from these to connect the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicDatasource {
    pub name: String,
    pub kind: String,
    pub params: BTreeMap<String, String>,
    pub created_at: String,
}

/// The users table, in the same durable SQLite file (synchronous=ON). `verifier`
/// is the pg_authid SCRAM string - never a plaintext or the SaltedPassword; a
/// stolen verifier does not yield client-auth capability. `name` is the primary
/// key so a concurrent CREATE USER races on INSERT OR IGNORE (first-writer wins).
/// `created_by` is the creating principal, or 'bootstrap-import' for a config
/// seed. A dropped user leaves no dangling grant (DROP removes both in one txn).
const USERS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS users (
    name        TEXT PRIMARY KEY,
    verifier    TEXT NOT NULL,
    superuser   INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    created_by  TEXT
)";

/// The grants table. A grant authorizes `grantee` (a user name or the reserved
/// pseudo-grantee 'PUBLIC') to `privilege` ('SELECT' in v1) on an object at one
/// of three containment levels (`object_kind` datasource|schema|table, with
/// `object_path` d | d.s | d.s.t). The composite primary key makes GRANT
/// idempotent (a re-grant is a no-op success); the bind-time check queries the
/// three candidate paths of a table for {user, PUBLIC}.
const GRANTS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS grants (
    grantee     TEXT NOT NULL,
    privilege   TEXT NOT NULL,
    object_kind TEXT NOT NULL,
    object_path TEXT NOT NULL,
    granted_by  TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (grantee, privilege, object_kind, object_path)
)";

/// The index the bind-time grant lookup rides: it queries by `(grantee,
/// object_path)` for {user, PUBLIC} across a table's three candidate paths.
const GRANTS_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS grants_grantee_path ON grants (grantee, object_path)";

/// The ACL generation counter, one row. Every user/grant DDL write increments it
/// in the SAME transaction, so a live session detects a privilege change with one
/// O(1) read and reloads its cached grants only when it actually changed.
const ACL_META_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS acl_meta (
    key         TEXT PRIMARY KEY,
    value       INTEGER NOT NULL
)";

/// One persisted user: the SCRAM verifier, the superuser flag, and provenance.
#[derive(Clone, PartialEq, Eq)]
pub struct User {
    pub name: String,
    pub verifier: String,
    pub superuser: bool,
    pub created_at: String,
    pub created_by: Option<String>,
}

/// Redact the verifier under `Debug`: it is sensitive (offline attack + server
/// impersonation), so it is never rendered into a log or panic message.
impl std::fmt::Debug for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("User")
            .field("name", &self.name)
            .field("verifier", &"<redacted>")
            .field("superuser", &self.superuser)
            .field("created_at", &self.created_at)
            .field("created_by", &self.created_by)
            .finish()
    }
}

/// One persisted grant row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub grantee: String,
    pub privilege: String,
    pub object_kind: String,
    pub object_path: String,
    pub granted_by: String,
    pub created_at: String,
}

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
        // Make ordinary statement contention WAIT for the write lock rather than
        // fail immediately (the learned-stats catalog sharing this file relies
        // on the same handler).
        conn.busy_timeout(BUSY_TIMEOUT)?;
        // The WAL transition and CREATE TABLE DDL need a momentary exclusive lock
        // that concurrent opens of one fresh file contend for; SQLite can return
        // BUSY for these WITHOUT invoking the busy handler, so the idempotent
        // setup is retried explicitly.
        with_busy_retry(|| ensure_schema(&conn))?;
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

    /// Persist a dynamic datasource with `INSERT OR IGNORE` on the name primary
    /// key. Returns whether THIS call inserted the row: `false` means a
    /// concurrent create of the same name already won, and the caller raises
    /// `already exists` and discards its validated connection (first-writer
    /// wins, never a silent replace). `created_at` is stamped here.
    pub fn insert_datasource(&self, ds: &DynamicDatasource) -> Result<bool, AccelError> {
        let inserted = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .execute(
                "INSERT OR IGNORE INTO dynamic_datasources (name, type, params, created_at) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    ds.name,
                    ds.kind,
                    serde_json::to_string(&ds.params)?,
                    (self.clock)(),
                ],
            )?;
        Ok(inserted == 1)
    }

    /// The persisted dynamic datasource named `name`, or None.
    pub fn get_datasource(&self, name: &str) -> Result<Option<DynamicDatasource>, AccelError> {
        let row = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .query_row(
                "SELECT name, type, params, created_at FROM dynamic_datasources WHERE name = ?1",
                params![name],
                datasource_row,
            )
            .optional()?;
        match row {
            Some(raw) => Ok(Some(raw_to_datasource(raw)?)),
            None => Ok(None),
        }
    }

    /// Every persisted dynamic datasource, ordered by name.
    pub fn list_datasources(&self) -> Result<Vec<DynamicDatasource>, AccelError> {
        let conn = self.conn.lock().expect("view catalog lock poisoned");
        let mut statement = conn.prepare(
            "SELECT name, type, params, created_at FROM dynamic_datasources ORDER BY name",
        )?;
        let rows = statement.query_map([], datasource_row)?;
        let mut datasources = Vec::new();
        for raw in rows {
            datasources.push(raw_to_datasource(raw?)?);
        }
        Ok(datasources)
    }

    /// Delete a persisted dynamic datasource. Returns whether a row was removed
    /// (so a caller can distinguish a real drop from a missing name).
    pub fn delete_datasource(&self, name: &str) -> Result<bool, AccelError> {
        let deleted = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .execute(
                "DELETE FROM dynamic_datasources WHERE name = ?1",
                params![name],
            )?;
        Ok(deleted > 0)
    }

    /// Insert a user with `INSERT OR IGNORE` on the name primary key. Returns
    /// whether THIS call inserted the row: `false` means the name already exists,
    /// and the caller raises `already exists` (first-writer wins, never a silent
    /// replace). A successful insert bumps the ACL generation in the same
    /// transaction, so a live session's next query observes the new user.
    pub fn create_user(
        &self,
        name: &str,
        verifier: &str,
        superuser: bool,
        created_by: &str,
    ) -> Result<bool, AccelError> {
        let mut guard = self.conn.lock().expect("view catalog lock poisoned");
        let tx = guard.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO users (name, verifier, superuser, created_at, created_by) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                name,
                verifier,
                i64::from(superuser),
                (self.clock)(),
                created_by
            ],
        )?;
        if inserted == 1 {
            bump_generation(&tx)?;
        }
        tx.commit()?;
        Ok(inserted == 1)
    }

    /// Overwrite a user's verifier (`ALTER USER ... WITH PASSWORD`). Returns
    /// whether a row was updated (a missing name lets the caller raise). Bumps the
    /// generation so a live session refreshes, though the verifier is only
    /// consulted at a NEW connection's handshake.
    pub fn set_user_password(&self, name: &str, verifier: &str) -> Result<bool, AccelError> {
        self.update_user("verifier = ?2", params![name, verifier])
    }

    /// Set a user's superuser flag (`ALTER USER ... [NO]SUPERUSER`). Returns
    /// whether a row was updated; bumps the generation so an in-flight session
    /// cannot keep administering after its superuser bit is cleared.
    pub fn set_user_superuser(&self, name: &str, superuser: bool) -> Result<bool, AccelError> {
        self.update_user("superuser = ?2", params![name, i64::from(superuser)])
    }

    /// Run a single-column user UPDATE (keyed on `name` at `?1`) inside a
    /// generation-bumping transaction.
    fn update_user(
        &self,
        set_clause: &str,
        values: &[&dyn rusqlite::ToSql],
    ) -> Result<bool, AccelError> {
        let mut guard = self.conn.lock().expect("view catalog lock poisoned");
        let tx = guard.transaction()?;
        let sql = format!("UPDATE users SET {set_clause} WHERE name = ?1");
        let updated = tx.execute(&sql, values)?;
        if updated > 0 {
            bump_generation(&tx)?;
        }
        tx.commit()?;
        Ok(updated > 0)
    }

    /// Drop a user AND every grant it holds in one transaction (a dropped user
    /// leaves no dangling grant). Returns whether the user existed; a successful
    /// drop bumps the generation, so a live session of the dropped user is denied
    /// at its next query.
    pub fn drop_user(&self, name: &str) -> Result<bool, AccelError> {
        let mut guard = self.conn.lock().expect("view catalog lock poisoned");
        let tx = guard.transaction()?;
        tx.execute("DELETE FROM grants WHERE grantee = ?1", params![name])?;
        let deleted = tx.execute("DELETE FROM users WHERE name = ?1", params![name])?;
        if deleted > 0 {
            bump_generation(&tx)?;
        }
        tx.commit()?;
        Ok(deleted > 0)
    }

    /// The user named `name`, or None.
    pub fn get_user(&self, name: &str) -> Result<Option<User>, AccelError> {
        let user = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .query_row(
                "SELECT name, verifier, superuser, created_at, created_by \
                 FROM users WHERE name = ?1",
                params![name],
                user_row,
            )
            .optional()?;
        Ok(user)
    }

    /// Every user, ordered by name.
    pub fn list_users(&self) -> Result<Vec<User>, AccelError> {
        let conn = self.conn.lock().expect("view catalog lock poisoned");
        let mut statement = conn.prepare(
            "SELECT name, verifier, superuser, created_at, created_by FROM users ORDER BY name",
        )?;
        let rows = statement.query_map([], user_row)?;
        let mut users = Vec::new();
        for row in rows {
            users.push(row?);
        }
        Ok(users)
    }

    /// The number of superusers, for the refuse-start-without-superuser check.
    pub fn count_superusers(&self) -> Result<i64, AccelError> {
        let count = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .query_row(
                "SELECT COUNT(*) FROM users WHERE superuser <> 0",
                [],
                |row| row.get(0),
            )?;
        Ok(count)
    }

    /// Grant a privilege on an object to a grantee, idempotently: `INSERT OR
    /// IGNORE` on the composite key, so a re-grant is a no-op success (matching
    /// Postgres). Bumps the generation only when a new grant is actually added.
    pub fn grant(
        &self,
        grantee: &str,
        privilege: &str,
        object_kind: &str,
        object_path: &str,
        granted_by: &str,
    ) -> Result<(), AccelError> {
        let mut guard = self.conn.lock().expect("view catalog lock poisoned");
        let tx = guard.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO grants \
             (grantee, privilege, object_kind, object_path, granted_by, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                grantee,
                privilege,
                object_kind,
                object_path,
                granted_by,
                (self.clock)()
            ],
        )?;
        if inserted > 0 {
            bump_generation(&tx)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Revoke a grant. Returns whether a matching row existed; a revoke that
    /// matches nothing lets the caller RAISE (a silent no-op hides a typo). A
    /// successful revoke bumps the generation, so a peer loses access at its next
    /// query.
    pub fn revoke(
        &self,
        grantee: &str,
        privilege: &str,
        object_kind: &str,
        object_path: &str,
    ) -> Result<bool, AccelError> {
        let mut guard = self.conn.lock().expect("view catalog lock poisoned");
        let tx = guard.transaction()?;
        let deleted = tx.execute(
            "DELETE FROM grants WHERE grantee = ?1 AND privilege = ?2 \
             AND object_kind = ?3 AND object_path = ?4",
            params![grantee, privilege, object_kind, object_path],
        )?;
        if deleted > 0 {
            bump_generation(&tx)?;
        }
        tx.commit()?;
        Ok(deleted > 0)
    }

    /// Every grant, ordered for a stable `SHOW GRANTS`.
    pub fn list_grants(&self) -> Result<Vec<Grant>, AccelError> {
        self.query_grants(
            "SELECT grantee, privilege, object_kind, object_path, granted_by, created_at \
             FROM grants ORDER BY grantee, object_path, object_kind",
            params![],
        )
    }

    /// Every grant held by `grantee` (a user name, or 'PUBLIC'), for the bind-time
    /// cache reload and `SHOW GRANTS FOR`.
    pub fn grants_for(&self, grantee: &str) -> Result<Vec<Grant>, AccelError> {
        self.query_grants(
            "SELECT grantee, privilege, object_kind, object_path, granted_by, created_at \
             FROM grants WHERE grantee = ?1 ORDER BY object_path, object_kind",
            params![grantee],
        )
    }

    /// Run a grant SELECT and collect the rows.
    fn query_grants(
        &self,
        sql: &str,
        args: &[&dyn rusqlite::ToSql],
    ) -> Result<Vec<Grant>, AccelError> {
        let conn = self.conn.lock().expect("view catalog lock poisoned");
        let mut statement = conn.prepare(sql)?;
        let rows = statement.query_map(args, grant_row)?;
        let mut grants = Vec::new();
        for row in rows {
            grants.push(row?);
        }
        Ok(grants)
    }

    /// The current ACL generation counter (one indexed single-row read). A live
    /// session compares it to its cached value to decide whether to reload grants.
    pub fn acl_generation(&self) -> Result<i64, AccelError> {
        let generation = self
            .conn
            .lock()
            .expect("view catalog lock poisoned")
            .query_row(
                "SELECT value FROM acl_meta WHERE key = 'generation'",
                [],
                |row| row.get(0),
            )?;
        Ok(generation)
    }
}

/// Increment the ACL generation inside a write transaction, so a grant/user
/// change is never observed without the matching generation bump.
fn bump_generation(tx: &Connection) -> Result<(), AccelError> {
    tx.execute(
        "UPDATE acl_meta SET value = value + 1 WHERE key = 'generation'",
        [],
    )?;
    Ok(())
}

/// Read one user row (name, verifier, superuser, created_at, created_by).
fn user_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<User> {
    Ok(User {
        name: row.get(0)?,
        verifier: row.get(1)?,
        superuser: row.get::<_, i64>(2)? != 0,
        created_at: row.get(3)?,
        created_by: row.get(4)?,
    })
}

/// Read one grant row.
fn grant_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Grant> {
    Ok(Grant {
        grantee: row.get(0)?,
        privilege: row.get(1)?,
        object_kind: row.get(2)?,
        object_path: row.get(3)?,
        granted_by: row.get(4)?,
        created_at: row.get(5)?,
    })
}

/// The raw dynamic-datasource row before JSON decoding of `params`.
type DatasourceRow = (String, String, String, String);

/// Read one dynamic-datasource row (name, type, params JSON, created_at).
fn datasource_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DatasourceRow> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

/// Decode a raw row's `params` JSON into the datasource record.
fn raw_to_datasource(raw: DatasourceRow) -> Result<DynamicDatasource, AccelError> {
    let (name, kind, params, created_at) = raw;
    Ok(DynamicDatasource {
        name,
        kind,
        params: serde_json::from_str(&params)?,
        created_at,
    })
}

/// Set WAL and create/migrate both registry tables (idempotent on every open).
/// WAL matches the learned-stats catalog sharing this file: many readers plus
/// one writer across the runtimes on one store. Unlike the stats side, these
/// rows are durable operator state, so synchronous stays ON.
fn ensure_schema(conn: &Connection) -> Result<(), AccelError> {
    conn.execute_batch("PRAGMA journal_mode=WAL;")?;
    conn.execute(SCHEMA, [])?;
    add_benefit_columns(conn)?;
    conn.execute(DATASOURCE_SCHEMA, [])?;
    add_datasource_optional_columns(conn)?;
    conn.execute(USERS_SCHEMA, [])?;
    conn.execute(GRANTS_SCHEMA, [])?;
    conn.execute(GRANTS_INDEX, [])?;
    conn.execute(ACL_META_SCHEMA, [])?;
    // Seed the single generation row at 0 on a fresh store; a pre-existing row
    // keeps its counter (INSERT OR IGNORE never rewinds a live generation).
    conn.execute(
        "INSERT OR IGNORE INTO acl_meta (key, value) VALUES ('generation', 0)",
        [],
    )?;
    Ok(())
}

/// Add `capabilities`, `change_keys`, and `updated_by` to a
/// `dynamic_datasources` table created before they existed. The DDL surface
/// sets none of them today, so they read back NULL; the columns are the storage
/// hooks a later credential-reference / ownership layer writes into.
fn add_datasource_optional_columns(conn: &Connection) -> Result<(), AccelError> {
    let mut present = std::collections::BTreeSet::new();
    let mut statement =
        conn.prepare("SELECT name FROM pragma_table_info('dynamic_datasources')")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        present.insert(row.get::<_, String>(0)?);
    }
    for column in ["capabilities", "change_keys", "updated_by"] {
        if !present.contains(column) {
            conn.execute(
                &format!("ALTER TABLE dynamic_datasources ADD COLUMN {column} TEXT"),
                [],
            )?;
        }
    }
    Ok(())
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

#[cfg(test)]
mod datasource_tests {
    use super::*;

    /// A fresh temp SQLite path under a unique directory.
    fn temp_db(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!("fq_ds_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create dir");
        dir.join("store.sqlite").to_string_lossy().to_string()
    }

    /// One dynamic datasource with two params.
    fn sample() -> DynamicDatasource {
        let mut params = BTreeMap::new();
        params.insert("path".to_string(), "/data/a.db".to_string());
        params.insert("schemas".to_string(), "public,staging".to_string());
        DynamicDatasource {
            name: "sales".to_string(),
            kind: "duckdb".to_string(),
            params,
            created_at: String::new(),
        }
    }

    #[test]
    fn insert_and_read_back_round_trips() {
        let catalog = ViewCatalog::open(&temp_db("roundtrip")).expect("open");
        let ds = sample();
        assert!(catalog.insert_datasource(&ds).expect("insert"));
        let read = catalog
            .get_datasource("sales")
            .expect("get")
            .expect("present");
        assert_eq!(read.name, ds.name);
        assert_eq!(read.kind, ds.kind);
        assert_eq!(read.params, ds.params);
        assert!(
            !read.created_at.is_empty(),
            "created_at is stamped on insert"
        );
        let all = catalog.list_datasources().expect("list");
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn insert_or_ignore_makes_the_second_writer_lose() {
        let catalog = ViewCatalog::open(&temp_db("ignore")).expect("open");
        assert!(catalog.insert_datasource(&sample()).expect("first insert"));
        assert!(
            !catalog.insert_datasource(&sample()).expect("second insert"),
            "a duplicate name inserts nothing"
        );
    }

    #[test]
    fn delete_removes_the_row() {
        let catalog = ViewCatalog::open(&temp_db("delete")).expect("open");
        catalog.insert_datasource(&sample()).expect("insert");
        assert!(catalog.delete_datasource("sales").expect("delete"));
        assert!(catalog.get_datasource("sales").expect("get").is_none());
        assert!(
            !catalog.delete_datasource("sales").expect("delete again"),
            "deleting a missing name removes nothing"
        );
    }

    #[test]
    fn users_round_trip_and_first_writer_wins() {
        let catalog = ViewCatalog::open(&temp_db("users")).expect("open");
        assert!(catalog
            .create_user("alice", "verifier-a", true, "bootstrap-import")
            .expect("create"));
        // A duplicate name inserts nothing (first-writer-wins).
        assert!(!catalog
            .create_user("alice", "verifier-b", false, "alice")
            .expect("dup"));
        let alice = catalog.get_user("alice").expect("get").expect("present");
        assert_eq!(alice.verifier, "verifier-a");
        assert!(alice.superuser);
        assert_eq!(catalog.count_superusers().expect("count"), 1);
    }

    #[test]
    fn alter_and_drop_user_bump_the_generation() {
        let catalog = ViewCatalog::open(&temp_db("users_gen")).expect("open");
        let gen0 = catalog.acl_generation().expect("gen0");
        catalog
            .create_user("bob", "v", false, "root")
            .expect("create");
        assert!(catalog.acl_generation().expect("gen1") > gen0);
        let gen1 = catalog.acl_generation().expect("gen1");
        assert!(catalog.set_user_password("bob", "v2").expect("alter pw"));
        assert!(catalog.set_user_superuser("bob", true).expect("alter su"));
        assert!(catalog.acl_generation().expect("gen2") > gen1);
        assert!(
            catalog
                .get_user("bob")
                .expect("get")
                .expect("bob")
                .superuser
        );
        // A no-op alter of a missing user does not bump.
        let before = catalog.acl_generation().expect("before");
        assert!(!catalog.set_user_password("ghost", "x").expect("miss"));
        assert_eq!(catalog.acl_generation().expect("after"), before);
    }

    #[test]
    fn grants_are_idempotent_and_revoke_is_loud() {
        let catalog = ViewCatalog::open(&temp_db("grants")).expect("open");
        catalog
            .grant("alice", "SELECT", "table", "d.s.t", "root")
            .expect("grant");
        let gen1 = catalog.acl_generation().expect("gen1");
        // Re-granting the same row is a no-op success and does not bump.
        catalog
            .grant("alice", "SELECT", "table", "d.s.t", "root")
            .expect("regrant");
        assert_eq!(catalog.acl_generation().expect("gen2"), gen1);
        catalog
            .grant("PUBLIC", "SELECT", "datasource", "d", "root")
            .expect("public grant");
        assert_eq!(catalog.grants_for("alice").expect("for alice").len(), 1);
        assert_eq!(catalog.list_grants().expect("all").len(), 2);
        // A real revoke removes and bumps; a phantom revoke returns false.
        assert!(catalog
            .revoke("alice", "SELECT", "table", "d.s.t")
            .expect("revoke"));
        assert!(!catalog
            .revoke("alice", "SELECT", "table", "d.s.t")
            .expect("revoke again"));
    }

    #[test]
    fn drop_user_removes_its_grants() {
        let catalog = ViewCatalog::open(&temp_db("drop_user")).expect("open");
        catalog
            .create_user("carol", "v", false, "root")
            .expect("create");
        catalog
            .grant("carol", "SELECT", "table", "d.s.t", "root")
            .expect("grant");
        assert!(catalog.drop_user("carol").expect("drop"));
        assert!(catalog.get_user("carol").expect("get").is_none());
        assert!(catalog.grants_for("carol").expect("grants").is_empty());
        assert!(!catalog.drop_user("carol").expect("drop again"));
    }

    #[test]
    fn open_migrates_an_old_shape_table() {
        let path = temp_db("migrate");
        // An old-shape table lacking the optional columns.
        let conn = Connection::open(&path).expect("open raw");
        conn.execute(
            "CREATE TABLE dynamic_datasources (
                name TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                params TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
            [],
        )
        .expect("create old table");
        conn.execute(
            "INSERT INTO dynamic_datasources (name, type, params, created_at) \
             VALUES ('old', 'duckdb', '{\"path\":\"/x\"}', '2020-01-01T00:00:00Z')",
            [],
        )
        .expect("seed old row");
        drop(conn);
        // Reopening runs the migration (adding the optional columns) and reads
        // the pre-existing row back unchanged.
        let catalog = ViewCatalog::open(&path).expect("open migrates");
        let read = catalog
            .get_datasource("old")
            .expect("get")
            .expect("present");
        assert_eq!(read.kind, "duckdb");
        assert_eq!(read.params.get("path"), Some(&"/x".to_string()));
    }
}
