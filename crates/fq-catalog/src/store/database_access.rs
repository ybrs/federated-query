//! Database-owned SELECT permissions using stable principals and structured targets.

use super::{DatabaseCatalog, StoreError};
use fq_common::security::{GrantSubject, ObjectKey};
use rusqlite::{params, Connection};

/// One local SELECT grant and its audit provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub subject: GrantSubject,
    pub object: ObjectKey,
    pub granted_by: String,
    pub created_at: String,
}

impl DatabaseCatalog {
    /// Add one SELECT grant atomically with its cache invalidation generation.
    pub fn grant(
        &self,
        subject: GrantSubject,
        object: &ObjectKey,
        actor: &str,
    ) -> Result<(), StoreError> {
        self.access_change(
            "INSERT INTO object_grants(subject,object_key,granted_by,created_at,relation_id) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(subject,object_key) DO NOTHING",
            params![subject.storage_key(), serde_json::to_string(object)?, actor, (self.clock)(), object.relation().map(|(id, _)| id.to_string())],
        )?;
        Ok(())
    }

    /// Remove exactly the requested local grant and report whether it existed.
    pub fn revoke(&self, subject: GrantSubject, object: &ObjectKey) -> Result<bool, StoreError> {
        Ok(self.access_change(
            "DELETE FROM object_grants WHERE subject=?1 AND object_key=?2",
            params![subject.storage_key(), serde_json::to_string(object)?],
        )? > 0)
    }

    /// Read the database-local privilege generation in one indexed lookup.
    pub fn acl_generation(&self) -> Result<i64, StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        generation(&conn)
    }

    /// Capture one subject's and PUBLIC's grants and their exact snapshot generation.
    pub fn access_snapshot(&self, subject: GrantSubject) -> Result<(i64, Vec<Grant>), StoreError> {
        let mut conn = self.conn.lock().expect("database catalog lock poisoned");
        let tx = conn.transaction()?;
        let generation = generation(&tx)?;
        let grants = read_grants(&tx, Some(subject))?;
        tx.commit()?;
        Ok((generation, grants))
    }

    /// List local grants, optionally including only a principal and PUBLIC.
    pub fn list_grants(&self, subject: Option<GrantSubject>) -> Result<Vec<Grant>, StoreError> {
        let conn = self.conn.lock().expect("database catalog lock poisoned");
        read_grants(&conn, subject)
    }

    /// Write local grants and their generation in one transaction.
    fn access_change(&self, sql: &str, args: &[&dyn rusqlite::ToSql]) -> Result<usize, StoreError> {
        let mut conn = self.conn.lock().expect("database catalog lock poisoned");
        let tx = conn.transaction()?;
        let changed = tx.execute(sql, args)?;
        tx.commit()?;
        Ok(changed)
    }
}

/// Create the local grant tables without storing global identities or database rights.
pub(super) fn ensure_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS object_grants (
            subject TEXT NOT NULL, object_key TEXT NOT NULL,
            granted_by TEXT NOT NULL, created_at TEXT NOT NULL,
            relation_id TEXT REFERENCES relations(id) ON DELETE CASCADE,
            PRIMARY KEY(subject,object_key));
         CREATE TABLE IF NOT EXISTS database_acl (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), generation INTEGER NOT NULL);
         INSERT INTO database_acl VALUES (1,0) ON CONFLICT(singleton) DO NOTHING;
         CREATE INDEX IF NOT EXISTS object_grants_relation ON object_grants(relation_id);
         CREATE TRIGGER IF NOT EXISTS object_grant_insert AFTER INSERT ON object_grants
         BEGIN UPDATE database_acl SET generation=generation+1 WHERE singleton=1; END;
         CREATE TRIGGER IF NOT EXISTS object_grant_delete AFTER DELETE ON object_grants
         BEGIN UPDATE database_acl SET generation=generation+1 WHERE singleton=1; END;
         CREATE TRIGGER IF NOT EXISTS object_grant_relation BEFORE INSERT ON object_grants
         WHEN NEW.relation_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM relations WHERE id=NEW.relation_id AND state='ready'
            AND kind=json_extract(NEW.object_key,'$.kind'))
         BEGIN SELECT RAISE(ABORT,'SELECT grant references a nonexistent relation kind'); END;",
    )?;
    Ok(())
}

/// Read the exact generation associated with a SQLite snapshot.
fn generation(conn: &Connection) -> Result<i64, StoreError> {
    Ok(conn.query_row(
        "SELECT generation FROM database_acl WHERE singleton=1",
        [],
        |row| row.get(0),
    )?)
}

/// Decode one grant, rejecting malformed subjects and structured targets.
fn decode_grant(
    subject: &str,
    object: &str,
    actor: String,
    created: String,
) -> Result<Grant, StoreError> {
    let subject = match subject {
        "PUBLIC" => GrantSubject::Public,
        _ => GrantSubject::User(subject.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?),
    };
    Ok(Grant {
        subject,
        object: serde_json::from_str(object)?,
        granted_by: actor,
        created_at: created,
    })
}

/// Read grant rows while preserving identifier components exactly as stored.
fn read_grants(conn: &Connection, subject: Option<GrantSubject>) -> Result<Vec<Grant>, StoreError> {
    let mut statement = conn.prepare(
        "SELECT subject,object_key,granted_by,created_at FROM object_grants
         WHERE ?1 IS NULL OR subject IN (?1,'PUBLIC') ORDER BY subject,object_key",
    )?;
    let rows = statement.query_map([subject.map(GrantSubject::storage_key)], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut grants = Vec::new();
    for row in rows {
        let (subject, object, actor, created) = row?;
        grants.push(decode_grant(&subject, &object, actor, created)?);
    }
    Ok(grants)
}
