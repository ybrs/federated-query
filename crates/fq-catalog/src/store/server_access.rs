//! Server-owned permissions to connect to or administer a database.

use super::{user_row, ServerCatalog, StoreError, User};
use fq_common::database::{DatabaseId, PrincipalId};
use fq_common::security::{DatabasePrivilege, GrantSubject};
use rusqlite::{params, Connection};

const ACCESS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS database_access (
    database_id TEXT NOT NULL REFERENCES databases(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    privilege TEXT NOT NULL CHECK(privilege IN ('CONNECT','ADMIN')),
    granted_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(database_id,subject,privilege),
    CHECK(subject <> 'PUBLIC' OR privilege='CONNECT')
)";

impl ServerCatalog {
    /// Grant database access to one stable identity or explicit PUBLIC CONNECT.
    pub fn grant_database(
        &self,
        database: DatabaseId,
        subject: GrantSubject,
        privilege: DatabasePrivilege,
        actor: &str,
    ) -> Result<(), StoreError> {
        if subject == GrantSubject::Public && privilege == DatabasePrivilege::Admin {
            return Err(StoreError::PublicDatabaseAdmin);
        }
        self.change(
            "INSERT INTO database_access VALUES (?1,?2,?3,?4,?5) ON CONFLICT(database_id,subject,privilege) DO NOTHING",
            params![database.to_string(), subject.storage_key(), privilege.as_str(), actor, (self.clock)()],
        )?;
        Ok(())
    }

    /// Revoke exactly one database permission and report a missing grant.
    pub fn revoke_database(
        &self,
        database: DatabaseId,
        subject: GrantSubject,
        privilege: DatabasePrivilege,
    ) -> Result<bool, StoreError> {
        Ok(self.change(
            "DELETE FROM database_access WHERE database_id=?1 AND subject=?2 AND privilege=?3",
            params![
                database.to_string(),
                subject.storage_key(),
                privilege.as_str()
            ],
        )? != 0)
    }

    /// Read the effective permission while rejecting unknown stored privilege values.
    pub fn database_access(
        &self,
        database: DatabaseId,
        principal: PrincipalId,
    ) -> Result<Option<DatabasePrivilege>, StoreError> {
        let conn = self.conn.lock().expect("server catalog lock poisoned");
        effective_access(&conn, database, principal)
    }

    /// Read identity, database rights and generation from the same root snapshot.
    pub fn authorization_snapshot(
        &self,
        database: DatabaseId,
        principal: PrincipalId,
    ) -> Result<(i64, Option<User>, Option<DatabasePrivilege>), StoreError> {
        use rusqlite::OptionalExtension;
        let mut conn = self.conn.lock().expect("server catalog lock poisoned");
        let tx = conn.transaction()?;
        let generation = tx.query_row(
            "SELECT value FROM acl_meta WHERE key='generation'",
            [],
            |row| row.get(0),
        )?;
        let user = tx
            .query_row(
                "SELECT id,name,verifier,superuser,created_at,created_by FROM users WHERE id=?1",
                [principal.to_string()],
                user_row,
            )
            .optional()?;
        let access = effective_access(&tx, database, principal)?;
        tx.commit()?;
        Ok((generation, user, access))
    }
}

/// Initialize root access metadata with cascaded database cleanup.
pub(super) fn ensure_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute(ACCESS_SCHEMA, [])?;
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS database_access_subject ON database_access(subject);
         CREATE TRIGGER IF NOT EXISTS database_access_subject_exists BEFORE INSERT ON database_access
         WHEN NEW.subject <> 'PUBLIC' AND NOT EXISTS(SELECT 1 FROM users WHERE id=NEW.subject)
         BEGIN SELECT RAISE(ABORT,'database grant references a nonexistent user'); END;
         CREATE TRIGGER IF NOT EXISTS delete_database_access AFTER DELETE ON users
         BEGIN DELETE FROM database_access WHERE subject=OLD.id; END;"
    )?;
    Ok(())
}

/// Decode only supported positive database privileges; every other value raises.
fn stored_privilege(value: &str) -> Result<DatabasePrivilege, StoreError> {
    match value {
        "CONNECT" => Ok(DatabasePrivilege::Connect),
        "ADMIN" => Ok(DatabasePrivilege::Admin),
        other => Err(StoreError::InvalidDatabasePrivilege(other.to_string())),
    }
}

/// Resolve a principal's strongest explicit or PUBLIC database permission.
fn effective_access(
    conn: &Connection,
    database: DatabaseId,
    principal: PrincipalId,
) -> Result<Option<DatabasePrivilege>, StoreError> {
    let mut statement = conn.prepare(
        "SELECT privilege FROM database_access WHERE database_id=?1 AND subject IN (?2,'PUBLIC')",
    )?;
    let rows = statement.query_map(
        params![database.to_string(), principal.to_string()],
        |row| row.get::<_, String>(0),
    )?;
    let mut permission = None;
    for row in rows {
        let privilege = stored_privilege(&row?)?;
        if permission.is_none() || privilege == DatabasePrivilege::Admin {
            permission = Some(privilege);
        }
    }
    Ok(permission)
}
