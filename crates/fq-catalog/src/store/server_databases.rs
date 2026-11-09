//! Server registration and state transitions for physically separate databases.

use super::{ServerCatalog, StoreError};
use fq_common::database::{DatabaseDescriptor, DatabaseId, DatabaseState};
use rusqlite::{params, Connection, OptionalExtension};

const DATABASES_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS databases (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK(length(name) > 0),
    state TEXT NOT NULL CHECK(state IN ('creating','ready','dropping')),
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL
)";

impl ServerCatalog {
    /// Reserve a fresh database identity and reject an already registered name.
    pub fn reserve_database(
        &self,
        id: DatabaseId,
        name: &str,
        actor: &str,
    ) -> Result<DatabaseDescriptor, StoreError> {
        let record = DatabaseDescriptor {
            id,
            name: name.to_string(),
            state: DatabaseState::Creating,
            created_at: (self.clock)(),
            created_by: actor.to_string(),
        };
        let inserted = self.change(
            "INSERT INTO databases(id,name,state,created_at,created_by) VALUES (?1,?2,'creating',?3,?4) ON CONFLICT(name) DO NOTHING",
            params![record.id.to_string(), name, record.created_at, actor],
        )?;
        if inserted == 0 {
            return Err(StoreError::DatabaseExists(name.to_string()));
        }
        Ok(record)
    }

    /// Find the exact registered name without substituting a default database.
    pub fn find_database(&self, name: &str) -> Result<Option<DatabaseDescriptor>, StoreError> {
        let conn = self.conn.lock().expect("server catalog lock poisoned");
        Ok(conn
            .query_row(
                "SELECT id,name,state,created_at,created_by FROM databases WHERE name=?1",
                [name],
                database_row,
            )
            .optional()?)
    }

    /// Require a registered database, regardless of its lifecycle state.
    pub fn get_database(&self, name: &str) -> Result<DatabaseDescriptor, StoreError> {
        self.find_database(name)?
            .ok_or_else(|| StoreError::DatabaseMissing(name.to_string()))
    }

    /// Enumerate lifecycle metadata for recovery and authorized database listings.
    pub fn list_databases(&self) -> Result<Vec<DatabaseDescriptor>, StoreError> {
        let conn = self.conn.lock().expect("server catalog lock poisoned");
        let mut statement = conn
            .prepare("SELECT id,name,state,created_at,created_by FROM databases ORDER BY name")?;
        let records = statement.query_map([], database_row)?;
        Ok(records.collect::<Result<Vec<_>, _>>()?)
    }

    /// Publish a fully initialized database so new sessions may acquire it.
    pub fn publish_database(&self, id: DatabaseId) -> Result<(), StoreError> {
        self.transition_database(id, DatabaseState::Creating, DatabaseState::Ready)
    }

    /// Retire a ready database before its files are removed.
    pub fn retire_database(&self, id: DatabaseId) -> Result<(), StoreError> {
        self.transition_database(id, DatabaseState::Ready, DatabaseState::Dropping)
    }

    /// Require the expected lifecycle state when publishing a transition.
    fn transition_database(
        &self,
        id: DatabaseId,
        previous: DatabaseState,
        next: DatabaseState,
    ) -> Result<(), StoreError> {
        let changed = self.change(
            "UPDATE databases SET state=?2 WHERE id=?1 AND state=?3",
            params![id.to_string(), next.as_str(), previous.as_str()],
        )?;
        if changed != 1 {
            return Err(StoreError::DatabaseState {
                database: id.to_string(),
                expected: previous.as_str().to_string(),
            });
        }
        Ok(())
    }

    /// Remove only an unpublished or retired registration after cleanup.
    pub fn purge_database(&self, id: DatabaseId) -> Result<(), StoreError> {
        let deleted = self.change(
            "DELETE FROM databases WHERE id=?1 AND state IN ('creating','dropping')",
            params![id.to_string()],
        )?;
        if deleted != 1 {
            return Err(StoreError::DatabaseState {
                database: id.to_string(),
                expected: "creating or dropping".to_string(),
            });
        }
        Ok(())
    }
}

/// Initialize the server's database registration table.
pub(super) fn ensure_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute(DATABASES_SCHEMA, [])?;
    Ok(())
}

/// Decode the exact database lifecycle tokens without defaulting malformed rows.
fn database_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DatabaseDescriptor> {
    let text: String = row.get(0)?;
    let id = text.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let state: String = row.get(2)?;
    Ok(DatabaseDescriptor {
        id,
        name: row.get(1)?,
        state: stored_state(&state)?,
        created_at: row.get(3)?,
        created_by: row.get(4)?,
    })
}

/// Reject any lifecycle state outside the registry's three defined states.
fn stored_state(state: &str) -> rusqlite::Result<DatabaseState> {
    match state {
        "creating" => Ok(DatabaseState::Creating),
        "ready" => Ok(DatabaseState::Ready),
        "dropping" => Ok(DatabaseState::Dropping),
        value => Err(rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid database state: {value}"),
            )),
        )),
    }
}
