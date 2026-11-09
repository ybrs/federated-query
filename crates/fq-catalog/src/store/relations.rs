//! Database-local relation identity, publication, and namespace ownership.

use super::{DatabaseCatalog, StoreError};
use fq_common::database::{RelationId, RelationKind, RelationRecord, RelationState};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS relations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK(kind IN ('view','materialized_view')),
    state TEXT NOT NULL CHECK(state IN ('creating','ready','dropping')),
    revision INTEGER NOT NULL CHECK(revision>=0),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS catalog_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), generation INTEGER NOT NULL
);
INSERT INTO catalog_meta VALUES (1,0) ON CONFLICT(singleton) DO NOTHING;
";

impl DatabaseCatalog {
    /// Reserve the unique name before a build, leaving data work outside the transaction.
    pub fn reserve_relation(
        &self,
        name: &str,
        kind: RelationKind,
        actor: &str,
    ) -> Result<RelationRecord, StoreError> {
        let mut conn = self.conn.lock().expect("database catalog lock poisoned");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_available(&tx, name)?;
        let record = new_relation(name, kind, RelationState::Creating, actor, &(self.clock)());
        insert_relation(&tx, &record)?;
        tx.commit()?;
        Ok(record)
    }

    /// Publish a reserved relation and invalidate metadata snapshots in the same transaction.
    pub fn publish_relation(&self, id: RelationId) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().expect("database catalog lock poisoned");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute("UPDATE relations SET state='ready',revision=revision+1 WHERE id=?1 AND state='creating'", [id.to_string()])?;
        require_changed(changed, &id.to_string())?;
        bump_generation(&tx)?;
        tx.commit()?;
        Ok(())
    }

    /// Look up a local relation identity, including an unpublished name reservation.
    pub fn find_relation(&self, name: &str) -> Result<Option<RelationRecord>, StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        find_relation(&conn, name)
    }

    /// Remove one relation and its identity-scoped grants atomically.
    pub fn drop_relation(&self, id: RelationId) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().expect("database catalog lock poisoned");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_changed(
            tx.execute("DELETE FROM relations WHERE id=?1", [id.to_string()])?,
            &id.to_string(),
        )?;
        bump_generation(&tx)?;
        tx.commit()?;
        Ok(())
    }

    /// Read the generation advanced by local relation publication and deletion.
    pub fn catalog_generation(&self) -> Result<i64, StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        Ok(conn.query_row(
            "SELECT generation FROM catalog_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?)
    }
}

/// Create the relation namespace and its publication generation.
pub fn ensure_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(SCHEMA)?;
    Ok(())
}

/// Advance local metadata generation before committing a publication.
pub(super) fn bump_generation(conn: &Connection) -> Result<(), StoreError> {
    conn.execute(
        "UPDATE catalog_meta SET generation=generation+1 WHERE singleton=1",
        [],
    )?;
    Ok(())
}

/// Read one relation using its unique exact parser-normalized local name.
pub(super) fn find_relation(
    conn: &Connection,
    name: &str,
) -> Result<Option<RelationRecord>, StoreError> {
    let raw = conn
        .query_row(
            "SELECT id,name,kind,state,revision,created_by,created_at FROM relations WHERE name=?1",
            [name],
            raw_relation,
        )
        .optional()?;
    raw.map(decode_relation).transpose()
}

/// Reject any reserved or published relation that already owns the name.
fn require_available(conn: &Connection, name: &str) -> Result<(), StoreError> {
    if find_relation(conn, name)?.is_some() {
        return Err(StoreError::RelationExists(name.to_string()));
    }
    Ok(())
}

/// Allocate one durable identity and initialize its publication revision.
fn new_relation(
    name: &str,
    kind: RelationKind,
    state: RelationState,
    actor: &str,
    created: &str,
) -> RelationRecord {
    RelationRecord {
        id: RelationId::new(),
        name: name.to_string(),
        kind,
        state,
        revision: 0,
        created_by: actor.to_string(),
        created_at: created.to_string(),
    }
}

/// Persist the caller's reserved identity without replacing any existing name.
fn insert_relation(conn: &Connection, record: &RelationRecord) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO relations VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![
            record.id.to_string(),
            record.name,
            record.kind.as_str(),
            record.state.as_str(),
            record.revision,
            record.created_by,
            record.created_at
        ],
    )?;
    Ok(())
}

/// Refuse reporting a successful mutation when its required prior identity is absent.
fn require_changed(changed: usize, name: &str) -> Result<(), StoreError> {
    if changed != 1 {
        return Err(StoreError::RelationMissing(name.to_string()));
    }
    Ok(())
}

type RawRelation = (String, String, String, String, i64, String, String);

/// Extract a raw relation row before validating its stored enums and identity.
fn raw_relation(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRelation> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
    ))
}

/// Reject corrupt stored identities, kinds, states, or publication revisions.
fn decode_relation(raw: RawRelation) -> Result<RelationRecord, StoreError> {
    let (id, name, kind, state, revision, created_by, created_at) = raw;
    let id = id.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let kind =
        RelationKind::parse(&kind).ok_or_else(|| StoreError::InvalidRelation(kind.clone()))?;
    let state =
        RelationState::parse(&state).ok_or_else(|| StoreError::InvalidRelation(state.clone()))?;
    if revision < 0 {
        return Err(StoreError::InvalidRelation(revision.to_string()));
    }
    Ok(RelationRecord {
        id,
        name,
        kind,
        state,
        revision,
        created_by,
        created_at,
    })
}

#[cfg(test)]
#[path = "relations_tests.rs"]
mod tests;
