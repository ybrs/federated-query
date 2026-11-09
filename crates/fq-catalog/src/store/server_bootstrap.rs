//! Atomic bootstrap credentials and strict server metadata format ownership.

use super::{bump_generation, ServerCatalog, StoreError};
use fq_common::UserCredential;
use rusqlite::{params, Connection};

const BOOTSTRAP_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS bootstrap_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    complete INTEGER NOT NULL CHECK(complete IN (0,1))
)";

const STATE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS server_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version INTEGER NOT NULL CHECK(schema_version=1),
    users_seeded INTEGER NOT NULL CHECK(users_seeded IN (0,1))
)";

impl ServerCatalog {
    /// Seed credentials once, without resurrecting deleted or altered identities.
    pub fn seed_users(&self, users: &[UserCredential]) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().expect("server catalog lock poisoned");
        let tx = conn.transaction()?;
        let seeded: bool = tx.query_row(
            "SELECT users_seeded FROM server_state WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if seeded || users.is_empty() {
            return Ok(false);
        }
        for user in users {
            insert_seed_user(&tx, user, &(self.clock)())?;
        }
        bump_generation(&tx)?;
        tx.commit()?;
        Ok(true)
    }
}

impl ServerCatalog {
    /// Whether this root finished bootstrapping its main database.
    pub fn bootstrap_complete(&self) -> Result<bool, StoreError> {
        let conn = self.conn.lock().expect("server catalog lock poisoned");
        Ok(conn.query_row(
            "SELECT complete FROM bootstrap_state WHERE singleton=1",
            [],
            |row| row.get(0),
        )?)
    }

    /// Record that main is ready and this root has finished bootstrapping.
    pub fn mark_bootstrap_complete(&self) -> Result<(), StoreError> {
        if self.bootstrap_complete()? {
            return Ok(());
        }
        let changed = self.change(
            "UPDATE bootstrap_state SET complete=1 WHERE singleton=1",
            &[],
        )?;
        if changed != 1 {
            return Err(StoreError::IncompatibleSchema);
        }
        Ok(())
    }
}

/// Insert one validated bootstrap identity in the caller's all-or-nothing transaction.
fn insert_seed_user(
    conn: &Connection,
    user: &UserCredential,
    created_at: &str,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO users(id,name,verifier,superuser,created_at,created_by) VALUES (?1,?2,?3,?4,?5,'bootstrap')",
        params![fq_common::database::PrincipalId::new().to_string(), user.name, user.verifier, i64::from(user.superuser), created_at],
    )?;
    Ok(())
}

/// Reject unknown server layouts instead of upgrading or adopting their rows.
pub(super) fn validate_schema(conn: &Connection) -> Result<(), StoreError> {
    let users = table_exists(conn, "users")?;
    let state = table_exists(conn, "server_state")?;
    if users != state {
        return Err(StoreError::IncompatibleSchema);
    }
    if state {
        let version: i64 = conn.query_row(
            "SELECT schema_version FROM server_state WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if version != 1 {
            return Err(StoreError::IncompatibleSchema);
        }
    }
    Ok(())
}

/// Create the format/version marker and remember that inserted identities are authoritative.
pub(super) fn ensure_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute(BOOTSTRAP_SCHEMA, [])?;
    conn.execute(
        "INSERT INTO bootstrap_state VALUES (1,0) ON CONFLICT(singleton) DO NOTHING",
        [],
    )?;
    conn.execute(STATE_SCHEMA, [])?;
    conn.execute(
        "INSERT INTO server_state VALUES (1,1,0) ON CONFLICT(singleton) DO NOTHING",
        [],
    )?;
    conn.execute_batch(
        "CREATE TRIGGER IF NOT EXISTS users_are_authoritative AFTER INSERT ON users BEGIN
            UPDATE server_state SET users_seeded=1 WHERE singleton=1;
         END;",
    )?;
    Ok(())
}

/// Test for an owned schema table using metadata only.
fn table_exists(conn: &Connection, name: &str) -> Result<bool, StoreError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [name],
        |row| row.get(0),
    )?)
}
