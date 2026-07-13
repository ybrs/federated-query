//! The event-view role registry: the `event_views` table in the same SQLite
//! file as the learned-stats catalog and the materialized-view registry.
//!
//! An event view is a materialized view (its chunks, schema, and lifecycle
//! live in `materialized_views`) PLUS a row here naming which of its columns
//! carry the entity / timestamp / event roles. Every intermediate state is a
//! valid plain materialized view: creation registers the roles AFTER the view
//! exists, and removal deletes the roles BEFORE the view goes, so a crash
//! between the two steps never leaves a role row pointing at nothing.

use std::sync::Mutex;

use fq_common::events::EventRoleColumns;
use rusqlite::{params, Connection, OptionalExtension};

use crate::error::EventError;

/// The role table beside `materialized_views`; `name` is the shared key.
/// `tiebreak_column` is NULL for a view with no declared tiebreak.
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS event_views (
    name TEXT PRIMARY KEY,
    entity_column TEXT NOT NULL,
    timestamp_column TEXT NOT NULL,
    event_column TEXT NOT NULL,
    tiebreak_column TEXT,
    created_at TEXT NOT NULL
)";

/// A SQLite-backed registry of event-view roles for one config.
pub struct EventViewRegistry {
    conn: Mutex<Connection>,
}

impl EventViewRegistry {
    /// Open (creating the table if absent) the registry at `path` - the
    /// config's learned-stats SQLite file. A table written before the
    /// tiebreak role existed gains the `tiebreak_column` column here; its
    /// existing rows read back as NULL, i.e. no tiebreak.
    pub fn open(path: &str) -> Result<Self, EventError> {
        let conn = Connection::open(path)?;
        // WAL matches the other users of this file (learned stats, the view
        // registry): many readers plus one writer across runtimes.
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute(SCHEMA, [])?;
        add_tiebreak_column(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Register the roles of a newly created event view. A name already
    /// registered raises `DuplicateEventView`; registration never replaces.
    pub fn register(&self, name: &str, roles: &EventRoleColumns) -> Result<(), EventError> {
        let inserted = self
            .conn
            .lock()
            .expect("event registry lock poisoned")
            .execute(
                "INSERT OR IGNORE INTO event_views \
                 (name, entity_column, timestamp_column, event_column, tiebreak_column, \
                  created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    name,
                    roles.entity,
                    roles.timestamp,
                    roles.event,
                    roles.tiebreak,
                    chrono::Utc::now().to_rfc3339(),
                ],
            )?;
        if inserted == 0 {
            return Err(EventError::DuplicateEventView(name.to_string()));
        }
        Ok(())
    }

    /// The roles of the event view named `name`, or None when the name is not
    /// an event view (it may still be a plain materialized view).
    pub fn get(&self, name: &str) -> Result<Option<EventRoleColumns>, EventError> {
        let row = self
            .conn
            .lock()
            .expect("event registry lock poisoned")
            .query_row(
                "SELECT entity_column, timestamp_column, event_column, tiebreak_column \
                 FROM event_views WHERE name = ?1",
                params![name],
                |row| {
                    Ok(EventRoleColumns {
                        entity: row.get(0)?,
                        timestamp: row.get(1)?,
                        event: row.get(2)?,
                        tiebreak: row.get(3)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    /// Remove an event view's role row (the first step of a drop). An unknown
    /// name raises `UnknownEventView`.
    pub fn remove(&self, name: &str) -> Result<(), EventError> {
        let removed = self
            .conn
            .lock()
            .expect("event registry lock poisoned")
            .execute("DELETE FROM event_views WHERE name = ?1", params![name])?;
        if removed == 0 {
            return Err(EventError::UnknownEventView(name.to_string()));
        }
        Ok(())
    }
}

/// Add `tiebreak_column` to an `event_views` table created before the
/// tiebreak role existed. A no-op when the column is already there.
fn add_tiebreak_column(conn: &Connection) -> Result<(), EventError> {
    let mut statement = conn.prepare("SELECT name FROM pragma_table_info('event_views')")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let column: String = row.get(0)?;
        if column == "tiebreak_column" {
            return Ok(());
        }
    }
    conn.execute(
        "ALTER TABLE event_views ADD COLUMN tiebreak_column TEXT",
        [],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh temp SQLite path unique to one test.
    fn temp_path() -> std::path::PathBuf {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fq_events_registry_{}_{id}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    /// A registry over a fresh temp SQLite file.
    fn temp_registry() -> EventViewRegistry {
        EventViewRegistry::open(&temp_path().to_string_lossy()).expect("open registry")
    }

    /// The role triple used across the registry tests (no tiebreak).
    fn roles() -> EventRoleColumns {
        EventRoleColumns {
            entity: "user_id".to_string(),
            timestamp: "ts".to_string(),
            event: "name".to_string(),
            tiebreak: None,
        }
    }

    #[test]
    fn register_get_remove_roundtrip() {
        let registry = temp_registry();
        registry.register("ev", &roles()).expect("register");
        assert_eq!(registry.get("ev").expect("get"), Some(roles()));
        registry.remove("ev").expect("remove");
        assert_eq!(registry.get("ev").expect("get after remove"), None);
    }

    #[test]
    fn duplicate_registration_raises() {
        let registry = temp_registry();
        registry.register("ev", &roles()).expect("register");
        let error = registry.register("ev", &roles()).unwrap_err();
        assert!(matches!(error, EventError::DuplicateEventView(ref n) if n == "ev"));
    }

    #[test]
    fn removing_an_unknown_view_raises() {
        let registry = temp_registry();
        let error = registry.remove("ghost").unwrap_err();
        assert!(matches!(error, EventError::UnknownEventView(ref n) if n == "ghost"));
    }

    #[test]
    fn a_tiebreak_role_round_trips() {
        let registry = temp_registry();
        let mut with_tiebreak = roles();
        with_tiebreak.tiebreak = Some("seq".to_string());
        registry.register("ev", &with_tiebreak).expect("register");
        assert_eq!(registry.get("ev").expect("get"), Some(with_tiebreak));
    }

    #[test]
    fn a_registry_written_before_the_tiebreak_role_migrates() {
        // A table in the pre-tiebreak shape, with one registered view.
        let path = temp_path();
        let conn = Connection::open(&path).expect("open raw sqlite");
        conn.execute(
            "CREATE TABLE event_views (
                name TEXT PRIMARY KEY,
                entity_column TEXT NOT NULL,
                timestamp_column TEXT NOT NULL,
                event_column TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
            [],
        )
        .expect("create old-shape table");
        conn.execute(
            "INSERT INTO event_views VALUES ('old_ev', 'user_id', 'ts', 'name', '2026-01-01')",
            [],
        )
        .expect("insert old row");
        drop(conn);

        // Opening migrates: the old view reads back with no tiebreak, and a
        // new view registers with one.
        let registry = EventViewRegistry::open(&path.to_string_lossy()).expect("open migrates");
        assert_eq!(registry.get("old_ev").expect("get old"), Some(roles()));
        let mut with_tiebreak = roles();
        with_tiebreak.tiebreak = Some("seq".to_string());
        registry
            .register("new_ev", &with_tiebreak)
            .expect("register new");
        assert_eq!(
            registry.get("new_ev").expect("get new"),
            Some(with_tiebreak)
        );
    }
}
