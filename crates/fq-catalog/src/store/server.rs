//! Durable server metadata independent of materialized-result storage.

use super::{open_connection, StoreError};
use crate::Clock;
use fq_common::database::PrincipalId;
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::Mutex;

const USERS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS users (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    verifier    TEXT NOT NULL,
    superuser   INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    created_by  TEXT
)";

const ACL_META_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS acl_meta (
    key         TEXT PRIMARY KEY,
    value       INTEGER NOT NULL
)";

/// One persisted user: the SCRAM verifier, the superuser flag, and provenance.
#[derive(Clone, PartialEq, Eq)]
pub struct User {
    pub id: PrincipalId,
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
            .field("id", &self.id)
            .field("name", &self.name)
            .field("verifier", &"<redacted>")
            .field("superuser", &self.superuser)
            .field("created_at", &self.created_at)
            .field("created_by", &self.created_by)
            .finish()
    }
}

/// A SQLite-backed server metadata catalog with transactional writes.
pub struct ServerCatalog {
    conn: Mutex<Connection>,
    clock: Clock,
}

impl ServerCatalog {
    /// Open the catalog at an explicit SQLite path.
    pub fn open(path: &str) -> Result<Self, StoreError> {
        let clock = std::sync::Arc::new(|| chrono::Utc::now().to_rfc3339());
        Self::open_with_clock(path, clock)
    }

    /// Open the catalog with an injectable timestamp source.
    pub fn open_with_clock(path: &str, clock: Clock) -> Result<Self, StoreError> {
        let conn = open_connection(path, ensure_schema)?;
        Ok(Self {
            conn: Mutex::new(conn),
            clock,
        })
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
    ) -> Result<bool, StoreError> {
        let changed = self.change(
            "INSERT INTO users (id,name,verifier,superuser,created_at,created_by) VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(name) DO NOTHING",
            params![PrincipalId::new().to_string(), name, verifier, i64::from(superuser), (self.clock)(), created_by],
        )?;
        Ok(changed == 1)
    }

    /// Overwrite a user's verifier (`ALTER USER ... WITH PASSWORD`). Returns
    /// whether a row was updated (a missing name lets the caller raise). Bumps the
    /// generation so a live session refreshes, though the verifier is only
    /// consulted at a NEW connection's handshake.
    pub fn set_user_password(&self, name: &str, verifier: &str) -> Result<bool, StoreError> {
        self.update_user("verifier = ?2", params![name, verifier])
    }

    /// Set a user's superuser flag (`ALTER USER ... [NO]SUPERUSER`). Returns
    /// whether a row was updated; bumps the generation so an in-flight session
    /// cannot keep administering after its superuser bit is cleared.
    pub fn set_user_superuser(&self, name: &str, superuser: bool) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().expect("server catalog lock poisoned");
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if !superuser {
            require_other_superuser(&tx, name)?;
        }
        let changed = tx.execute(
            "UPDATE users SET superuser=?2 WHERE name=?1",
            params![name, i64::from(superuser)],
        )?;
        if changed > 0 {
            bump_generation(&tx)?;
        }
        tx.commit()?;
        Ok(changed > 0)
    }

    /// Run a single-column user UPDATE (keyed on `name` at `?1`) inside a
    /// generation-bumping transaction.
    fn update_user(
        &self,
        set_clause: &str,
        values: &[&dyn rusqlite::ToSql],
    ) -> Result<bool, StoreError> {
        let sql = format!("UPDATE users SET {set_clause} WHERE name = ?1");
        Ok(self.change(&sql, values)? > 0)
    }

    /// Drop a user AND every grant it holds in one transaction (a dropped user
    /// leaves no dangling grant). Returns whether the user existed; a successful
    /// drop bumps the generation, so a live session of the dropped user is denied
    /// at its next query.
    pub fn drop_user(&self, name: &str) -> Result<bool, StoreError> {
        let mut guard = self.conn.lock().expect("server catalog lock poisoned");
        let tx = guard.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        require_other_superuser(&tx, name)?;
        let deleted = tx.execute("DELETE FROM users WHERE name = ?1", params![name])?;
        if deleted > 0 {
            bump_generation(&tx)?;
        }
        tx.commit()?;
        Ok(deleted > 0)
    }

    /// The user named `name`, or None.
    pub fn get_user(&self, name: &str) -> Result<Option<User>, StoreError> {
        let user = self
            .conn
            .lock()
            .expect("server catalog lock poisoned")
            .query_row(
                "SELECT id, name, verifier, superuser, created_at, created_by \
                 FROM users WHERE name = ?1",
                params![name],
                user_row,
            )
            .optional()?;
        Ok(user)
    }

    /// Every user, ordered by name.
    pub fn list_users(&self) -> Result<Vec<User>, StoreError> {
        let conn = self.conn.lock().expect("server catalog lock poisoned");
        let mut statement = conn.prepare(
            "SELECT id, name, verifier, superuser, created_at, created_by FROM users ORDER BY name",
        )?;
        let rows = statement.query_map([], user_row)?;
        let mut users = Vec::new();
        for row in rows {
            users.push(row?);
        }
        Ok(users)
    }

    /// The number of superusers, for the refuse-start-without-superuser check.
    pub fn count_superusers(&self) -> Result<i64, StoreError> {
        let count = self
            .conn
            .lock()
            .expect("server catalog lock poisoned")
            .query_row(
                "SELECT COUNT(*) FROM users WHERE superuser <> 0",
                [],
                |row| row.get(0),
            )?;
        Ok(count)
    }

    /// Commit one metadata mutation and its authorization generation together.
    fn change(&self, sql: &str, values: &[&dyn rusqlite::ToSql]) -> Result<usize, StoreError> {
        let mut guard = self.conn.lock().expect("server catalog lock poisoned");
        let tx = guard.transaction()?;
        let changed = tx.execute(sql, values)?;
        if changed > 0 {
            bump_generation(&tx)?;
        }
        tx.commit()?;
        Ok(changed)
    }

    /// The current ACL generation counter (one indexed single-row read). A live
    /// session compares it to its cached value to decide whether to reload grants.
    pub fn acl_generation(&self) -> Result<i64, StoreError> {
        let generation = self
            .conn
            .lock()
            .expect("server catalog lock poisoned")
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
fn bump_generation(tx: &Connection) -> Result<(), StoreError> {
    tx.execute(
        "UPDATE acl_meta SET value = value + 1 WHERE key = 'generation'",
        [],
    )?;
    Ok(())
}

/// Read one user row (name, verifier, superuser, created_at, created_by).
fn user_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<User> {
    let stored: String = row.get(0)?;
    let id = stored.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(User {
        id,
        name: row.get(1)?,
        verifier: row.get(2)?,
        superuser: row.get::<_, i64>(3)? != 0,
        created_at: row.get(4)?,
        created_by: row.get(5)?,
    })
}

/// Create this catalog's tables and initial generation metadata.
fn ensure_schema(conn: &Connection) -> Result<(), StoreError> {
    bootstrap::validate_schema(conn)?;
    databases::ensure_schema(conn)?;
    conn.execute(USERS_SCHEMA, [])?;
    bootstrap::ensure_schema(conn)?;
    access::ensure_schema(conn)?;
    conn.execute(ACL_META_SCHEMA, [])?;
    conn.execute(
        "INSERT OR IGNORE INTO acl_meta (key, value) VALUES ('generation', 0)",
        [],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;

#[path = "server_databases.rs"]
mod databases;

impl ServerCatalog {
    /// Resolve an authenticated identity without reassigning a recreated username.
    pub fn get_user_by_id(&self, id: PrincipalId) -> Result<Option<User>, StoreError> {
        let conn = self.conn.lock().expect("server catalog lock poisoned");
        Ok(conn
            .query_row(
                "SELECT id,name,verifier,superuser,created_at,created_by FROM users WHERE id=?1",
                [id.to_string()],
                user_row,
            )
            .optional()?)
    }
}

#[path = "server_access.rs"]
mod access;

#[path = "server_bootstrap.rs"]
mod bootstrap;

/// Refuse deleting or demoting the sole administrator inside the mutation transaction.
fn require_other_superuser(conn: &Connection, name: &str) -> Result<(), StoreError> {
    let last: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM users WHERE name=?1 AND superuser<>0)
         AND (SELECT count(*) FROM users WHERE superuser<>0)=1",
        [name],
        |row| row.get(0),
    )?;
    if last {
        return Err(StoreError::LastSuperuser);
    }
    Ok(())
}
