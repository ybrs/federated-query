//! Durable database metadata independent of materialized-result storage.

use super::{open_connection, StoreError};
use crate::Clock;
pub use access::Grant;
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::Mutex;

const DATASOURCE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS dynamic_datasources (
    name          TEXT PRIMARY KEY,
    definition    TEXT NOT NULL,
    created_at    TEXT NOT NULL
)";

const VIEWS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS views (
    name           TEXT PRIMARY KEY,
    definition_sql TEXT NOT NULL,
    column_aliases TEXT,
    created_at     TEXT NOT NULL
)";

const SETTINGS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS engine_settings (
    name        TEXT PRIMARY KEY,
    value       TEXT NOT NULL
)";

/// One persisted non-materialized view: its name, defining SELECT, and the
/// optional positional output rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredView {
    pub name: String,
    pub definition_sql: String,
    pub column_aliases: Option<Vec<String>>,
    pub created_at: String,
    /// The view's identity in the shared local relation namespace.
    pub relation_id: fq_common::database::RelationId,
}

/// One persisted datasource definition: the complete typed source
/// configuration plus the time it was first stored.
#[derive(Debug, Clone, PartialEq)]
pub struct DatasourceDefinition {
    pub definition: fq_common::DataSourceConfig,
    pub created_at: String,
}

/// A SQLite-backed database metadata catalog with transactional writes.
pub struct DatabaseCatalog {
    pub(super) conn: Mutex<Connection>,
    pub(super) clock: Clock,
}

impl DatabaseCatalog {
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

    /// Persist a dynamic datasource with `INSERT OR IGNORE` on the name primary
    /// key. Returns whether THIS call inserted the row: `false` means a
    /// concurrent create of the same name already won, and the caller raises
    /// `already exists` and discards its validated connection (first-writer
    /// wins, never a silent replace). `created_at` is stamped here.
    pub fn insert_datasource(
        &self,
        definition: &fq_common::DataSourceConfig,
    ) -> Result<bool, StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO dynamic_datasources (name,definition,created_at) VALUES (?1,?2,?3)",
            params![definition.name, serde_json::to_string(definition)?, (self.clock)()],
        )?;
        Ok(inserted == 1)
    }

    /// The persisted datasource definition named `name`, or None.
    pub fn get_datasource(&self, name: &str) -> Result<Option<DatasourceDefinition>, StoreError> {
        let row = self
            .conn
            .lock()
            .expect("database catalog lock poisoned")
            .query_row(
                "SELECT definition, created_at FROM dynamic_datasources WHERE name = ?1",
                params![name],
                datasource_row,
            )
            .optional()?;
        match row {
            Some(raw) => Ok(Some(raw_to_datasource(raw)?)),
            None => Ok(None),
        }
    }

    /// Every persisted datasource definition, ordered by name.
    pub fn list_datasources(&self) -> Result<Vec<DatasourceDefinition>, StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        let mut statement =
            conn.prepare("SELECT definition, created_at FROM dynamic_datasources ORDER BY name")?;
        let rows = statement.query_map([], datasource_row)?;
        let mut datasources = Vec::new();
        for raw in rows {
            datasources.push(raw_to_datasource(raw?)?);
        }
        Ok(datasources)
    }

    /// Replace one stored datasource definition's declaration fields; the
    /// caller has already verified the connection identity is unchanged.
    pub fn update_datasource(
        &self,
        definition: &fq_common::DataSourceConfig,
    ) -> Result<(), StoreError> {
        let updated = self
            .conn
            .lock()
            .expect("database catalog lock poisoned")
            .execute(
                "UPDATE dynamic_datasources SET definition = ?2 WHERE name = ?1",
                params![definition.name, serde_json::to_string(definition)?],
            )?;
        if updated != 1 {
            return Err(StoreError::RelationMissing(definition.name.clone()));
        }
        Ok(())
    }

    /// Persist one setting override (`INSERT OR REPLACE`): the latest SET of
    /// a name wins.
    pub fn upsert_setting(&self, name: &str, value: &str) -> Result<(), StoreError> {
        self.conn
            .lock()
            .expect("database catalog lock poisoned")
            .execute(
                "INSERT OR REPLACE INTO engine_settings (name, value) VALUES (?1, ?2)",
                params![name, value],
            )?;
        Ok(())
    }

    /// Remove one persisted setting override; whether a row existed.
    pub fn delete_setting(&self, name: &str) -> Result<bool, StoreError> {
        let deleted = self
            .conn
            .lock()
            .expect("database catalog lock poisoned")
            .execute("DELETE FROM engine_settings WHERE name = ?1", params![name])?;
        Ok(deleted == 1)
    }

    /// Remove every persisted setting override (`RESET ALL`).
    pub fn clear_settings(&self) -> Result<(), StoreError> {
        self.conn
            .lock()
            .expect("database catalog lock poisoned")
            .execute("DELETE FROM engine_settings", [])?;
        Ok(())
    }

    /// Every persisted setting override, name-ordered.
    pub fn list_settings(&self) -> Result<Vec<(String, String)>, StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        let mut statement =
            conn.prepare("SELECT name, value FROM engine_settings ORDER BY name")?;
        let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut settings = Vec::new();
        for row in rows {
            settings.push(row?);
        }
        Ok(settings)
    }

    /// Every persisted non-materialized view, name-ordered.
    pub fn list_views(&self) -> Result<Vec<StoredView>, StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        let mut statement = conn.prepare(
            "SELECT v.name,v.definition_sql,v.column_aliases,v.created_at,r.id FROM views v \
             JOIN relations r ON r.name = v.name AND r.kind = 'view' ORDER BY v.name",
        )?;
        let rows = statement.query_map([], stored_view_row)?;
        let mut views = Vec::new();
        for row in rows {
            views.push(decode_stored_view(row?)?);
        }
        Ok(views)
    }

    /// Persist a view and its shared relation identity in one transaction,
    /// replacing any view of the same name (`CREATE OR REPLACE VIEW`) while
    /// retaining that view's identity and grants. The caller validates the
    /// definition BEFORE calling, so a stored row is always one that parsed
    /// and bound. Returns the view's relation identity.
    pub fn upsert_view(
        &self,
        name: &str,
        definition_sql: &str,
        column_aliases: Option<&[String]>,
        actor: &str,
    ) -> Result<fq_common::database::RelationRecord, StoreError> {
        let aliases = match column_aliases {
            Some(names) => Some(serde_json::to_string(names)?),
            None => None,
        };
        let mut conn = self.conn.lock().expect("database catalog lock poisoned");
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let relation = view_relation(&tx, name, actor, &(self.clock)())?;
        tx.execute(
            "INSERT OR REPLACE INTO views (name, definition_sql, column_aliases, created_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![name, definition_sql, aliases, (self.clock)()],
        )?;
        super::relations::bump_generation(&tx)?;
        tx.commit()?;
        Ok(relation)
    }

    /// Delete a persisted view and its relation identity. Returns whether a
    /// row was removed, so the caller can raise on a missing name instead of
    /// silently succeeding.
    pub fn delete_view(&self, name: &str) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().expect("database catalog lock poisoned");
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let deleted = tx.execute("DELETE FROM views WHERE name = ?1", params![name])?;
        if deleted == 0 {
            return Ok(false);
        }
        tx.execute(
            "DELETE FROM relations WHERE name = ?1 AND kind = 'view'",
            params![name],
        )?;
        super::relations::bump_generation(&tx)?;
        tx.commit()?;
        Ok(true)
    }

    /// Delete a persisted dynamic datasource. Returns whether a row was removed
    /// (so a caller can distinguish a real drop from a missing name).
    pub fn delete_datasource(&self, name: &str) -> Result<bool, StoreError> {
        let deleted = self
            .conn
            .lock()
            .expect("database catalog lock poisoned")
            .execute(
                "DELETE FROM dynamic_datasources WHERE name = ?1",
                params![name],
            )?;
        Ok(deleted > 0)
    }
}

/// The raw datasource row before JSON decoding of its definition.
type DatasourceRow = (String, String);

/// Read one datasource row (definition JSON, created_at).
fn datasource_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DatasourceRow> {
    Ok((row.get(0)?, row.get(1)?))
}

/// Decode a raw row's definition JSON into the typed record.
fn raw_to_datasource(raw: DatasourceRow) -> Result<DatasourceDefinition, StoreError> {
    let (definition, created_at) = raw;
    Ok(DatasourceDefinition {
        definition: serde_json::from_str(&definition)?,
        created_at,
    })
}

/// Create this catalog's tables and initial generation metadata.
fn ensure_schema(conn: &Connection) -> Result<(), StoreError> {
    super::relations::ensure_schema(conn)?;
    access::ensure_schema(conn)?;
    conn.execute(DATASOURCE_SCHEMA, [])?;
    conn.execute(VIEWS_SCHEMA, [])?;
    conn.execute(SETTINGS_SCHEMA, [])?;
    Ok(())
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;

/// Read one SQL view before decoding its optional positional column names.
fn stored_view_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<(String, String, Option<String>, String, String)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
    ))
}

/// Decode a view's optional column-rename JSON without defaulting corruption.
fn decode_stored_view(
    row: (String, String, Option<String>, String, String),
) -> Result<StoredView, StoreError> {
    let (name, definition_sql, aliases, created_at, relation_id) = row;
    let column_aliases = aliases
        .map(|text| serde_json::from_str(&text))
        .transpose()?;
    let relation_id = relation_id.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(StoredView {
        name,
        definition_sql,
        column_aliases,
        created_at,
        relation_id,
    })
}

/// Reuse a live view's relation identity, or allocate a ready one; a name
/// held by another relation kind raises instead of shadowing it.
fn view_relation(
    conn: &Connection,
    name: &str,
    actor: &str,
    created: &str,
) -> Result<fq_common::database::RelationRecord, StoreError> {
    use fq_common::database::{RelationKind, RelationState};
    if let Some(mut relation) = super::relations::find_relation(conn, name)? {
        if relation.kind != RelationKind::View || relation.state != RelationState::Ready {
            return Err(StoreError::RelationExists(name.to_string()));
        }
        relation.revision += 1;
        conn.execute(
            "UPDATE relations SET revision = ?2 WHERE id = ?1",
            params![relation.id.to_string(), relation.revision],
        )?;
        return Ok(relation);
    }
    let relation = fq_common::database::RelationRecord {
        id: fq_common::database::RelationId::new(),
        name: name.to_string(),
        kind: RelationKind::View,
        state: RelationState::Ready,
        revision: 0,
        created_by: actor.to_string(),
        created_at: created.to_string(),
    };
    conn.execute(
        "INSERT INTO relations VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            relation.id.to_string(),
            relation.name,
            relation.kind.as_str(),
            relation.state.as_str(),
            relation.revision,
            relation.created_by,
            relation.created_at
        ],
    )?;
    Ok(relation)
}

impl DatabaseCatalog {
    /// Stamp a newly created catalog with its durable database identity.
    pub fn initialize_identity(
        &self,
        id: fq_common::database::DatabaseId,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        conn.execute(
            "CREATE TABLE database_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), database_id TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK(schema_version=1))",
            [],
        )?;
        conn.execute(
            "INSERT INTO database_identity VALUES (1,?1,1)",
            [id.to_string()],
        )?;
        Ok(())
    }

    /// Reject a catalog file belonging to another database or schema version.
    pub fn verify_identity(
        &self,
        expected: fq_common::database::DatabaseId,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        let actual: String = conn.query_row(
            "SELECT database_id FROM database_identity WHERE singleton=1 AND schema_version=1",
            [],
            |row| row.get(0),
        )?;
        if actual != expected.to_string() {
            return Err(StoreError::DatabaseIdentity {
                expected: expected.to_string(),
                actual,
            });
        }
        Ok(())
    }
}

#[path = "database_access.rs"]
mod access;
