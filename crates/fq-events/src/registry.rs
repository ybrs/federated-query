//! The event-dataset registry: the `event_datasets` table in the engine's
//! stats SQLite (same connection discipline as the materialized-view
//! registry: WAL, busy retry, short single transactions, additive
//! migrations). The `file_list` JSON column is the ONLY source of truth for
//! which files compose a dataset; publish is a single-row transaction after
//! every file is written and fsynced, so a torn build is never visible.

use std::collections::BTreeMap;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};

use crate::error::EventStoreError;
use crate::model::{DatasetRecord, FileList, PropertyDef};

/// The registry over one stats SQLite file.
pub struct Registry {
    conn: Mutex<Connection>,
}

/// Map a rusqlite failure into the registry error.
fn db_err(error: &rusqlite::Error) -> EventStoreError {
    EventStoreError::Registry(error.to_string())
}

/// Map a serde failure into the registry error (a malformed stored JSON
/// column is registry corruption, never silently defaulted).
fn json_err(error: &serde_json::Error) -> EventStoreError {
    EventStoreError::Registry(format!("stored JSON column does not parse: {error}"))
}

impl Registry {
    /// Open (and migrate) the registry inside the stats SQLite at `path`.
    pub fn open(path: &std::path::Path) -> Result<Self, EventStoreError> {
        let conn = Connection::open(path).map_err(|error| db_err(&error))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|error| db_err(&error))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|error| db_err(&error))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS event_datasets (
                name             TEXT PRIMARY KEY,
                location         TEXT NOT NULL,
                source_kind      TEXT NOT NULL,
                source_ref       TEXT NOT NULL,
                actor_column     TEXT NOT NULL,
                time_column      TEXT NOT NULL,
                time_unit        TEXT NOT NULL,
                event_column     TEXT NOT NULL,
                tiebreak_column  TEXT,
                property_schema  TEXT NOT NULL,
                shards           INTEGER NOT NULL,
                file_list        TEXT NOT NULL,
                dict_state       TEXT NOT NULL,
                refresh_key      TEXT,
                watermark        TEXT,
                source_token     TEXT,
                measured_events  INTEGER NOT NULL,
                measured_actors  INTEGER NOT NULL,
                byte_size        INTEGER NOT NULL,
                build_millis     INTEGER NOT NULL,
                min_ts           INTEGER,
                max_ts           INTEGER,
                created_at       TEXT NOT NULL,
                refreshed_at     TEXT,
                deleted_at       TEXT
            );",
        )
        .map_err(|error| db_err(&error))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Insert a fresh dataset row (the CREATE publish step). A live row under
    /// the same name raises `NameTaken`.
    pub fn insert(&self, record: &DatasetRecord) -> Result<(), EventStoreError> {
        let conn = self.conn.lock().expect("registry lock poisoned");
        let existing: Option<String> = conn
            .query_row(
                "SELECT name FROM event_datasets WHERE name = ?1 AND deleted_at IS NULL",
                params![record.name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_err(&error))?;
        if existing.is_some() {
            return Err(EventStoreError::NameTaken {
                name: record.name.clone(),
            });
        }
        conn.execute(
            "INSERT OR REPLACE INTO event_datasets (
                name, location, source_kind, source_ref, actor_column, time_column,
                time_unit, event_column, tiebreak_column, property_schema, shards,
                file_list, dict_state, refresh_key, watermark, source_token,
                measured_events, measured_actors, byte_size, build_millis,
                min_ts, max_ts, created_at, refreshed_at, deleted_at
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,
                       ?19,?20,?21,?22,?23,?24,NULL)",
            params![
                record.name,
                record.location,
                record.source_kind,
                record.source_ref,
                record.actor_column,
                record.time_column,
                record.time_unit,
                record.event_column,
                record.tiebreak_column,
                serde_json::to_string(&record.properties).map_err(|error| json_err(&error))?,
                record.shards,
                serde_json::to_string(&record.file_list).map_err(|error| json_err(&error))?,
                serde_json::to_string(&record.dict_state).map_err(|error| json_err(&error))?,
                record.refresh_key,
                record.watermark,
                record.source_token,
                record.measured_events,
                record.measured_actors,
                record.byte_size,
                record.build_millis,
                record.min_ts,
                record.max_ts,
                record.created_at,
                record.refreshed_at,
            ],
        )
        .map_err(|error| db_err(&error))?;
        Ok(())
    }

    /// Publish a refresh/rebuild: swap the stored state columns in one
    /// transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_update(&self, record: &DatasetRecord) -> Result<(), EventStoreError> {
        let conn = self.conn.lock().expect("registry lock poisoned");
        let updated = conn
            .execute(
                "UPDATE event_datasets SET
                    property_schema = ?2, shards = ?3, file_list = ?4, dict_state = ?5,
                    watermark = ?6, source_token = ?7, measured_events = ?8,
                    measured_actors = ?9, byte_size = ?10, build_millis = ?11,
                    min_ts = ?12, max_ts = ?13, refreshed_at = ?14
                 WHERE name = ?1 AND deleted_at IS NULL",
                params![
                    record.name,
                    serde_json::to_string(&record.properties).map_err(|error| json_err(&error))?,
                    record.shards,
                    serde_json::to_string(&record.file_list).map_err(|error| json_err(&error))?,
                    serde_json::to_string(&record.dict_state).map_err(|error| json_err(&error))?,
                    record.watermark,
                    record.source_token,
                    record.measured_events,
                    record.measured_actors,
                    record.byte_size,
                    record.build_millis,
                    record.min_ts,
                    record.max_ts,
                    record.refreshed_at,
                ],
            )
            .map_err(|error| db_err(&error))?;
        if updated != 1 {
            return Err(EventStoreError::UnknownDataset {
                name: record.name.clone(),
            });
        }
        Ok(())
    }

    /// The live dataset row named `name`.
    pub fn get(&self, name: &str) -> Result<DatasetRecord, EventStoreError> {
        self.get_optional(name)?
            .ok_or_else(|| EventStoreError::UnknownDataset {
                name: name.to_string(),
            })
    }

    /// The live dataset row named `name`, or None.
    pub fn get_optional(&self, name: &str) -> Result<Option<DatasetRecord>, EventStoreError> {
        let conn = self.conn.lock().expect("registry lock poisoned");
        conn.query_row(
            &format!("{SELECT_COLUMNS} WHERE name = ?1 AND deleted_at IS NULL"),
            params![name],
            row_to_record,
        )
        .optional()
        .map_err(|error| db_err(&error))?
        .transpose()
    }

    /// Every live dataset, name-ordered.
    pub fn list(&self) -> Result<Vec<DatasetRecord>, EventStoreError> {
        let conn = self.conn.lock().expect("registry lock poisoned");
        let mut statement = conn
            .prepare(&format!(
                "{SELECT_COLUMNS} WHERE deleted_at IS NULL ORDER BY name"
            ))
            .map_err(|error| db_err(&error))?;
        let rows = statement
            .query_map([], row_to_record)
            .map_err(|error| db_err(&error))?
            .collect::<Result<Vec<_>, rusqlite::Error>>()
            .map_err(|error| db_err(&error))?;
        rows.into_iter().collect()
    }

    /// Every tombstoned dataset (an interrupted DROP to finish at open).
    pub fn tombstoned(&self) -> Result<Vec<(String, String)>, EventStoreError> {
        let conn = self.conn.lock().expect("registry lock poisoned");
        let mut statement = conn
            .prepare("SELECT name, location FROM event_datasets WHERE deleted_at IS NOT NULL")
            .map_err(|error| db_err(&error))?;
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|error| db_err(&error))?
            .collect::<Result<Vec<(String, String)>, rusqlite::Error>>()
            .map_err(|error| db_err(&error))?;
        Ok(rows)
    }

    /// Tombstone `name` (the first step of DROP; files are deleted next, then
    /// the row is purged).
    pub fn tombstone(&self, name: &str) -> Result<(), EventStoreError> {
        let conn = self.conn.lock().expect("registry lock poisoned");
        let updated = conn
            .execute(
                "UPDATE event_datasets SET deleted_at = datetime('now') \
                 WHERE name = ?1 AND deleted_at IS NULL",
                params![name],
            )
            .map_err(|error| db_err(&error))?;
        if updated != 1 {
            return Err(EventStoreError::UnknownDataset {
                name: name.to_string(),
            });
        }
        Ok(())
    }

    /// Purge a tombstoned row (the last step of DROP).
    pub fn purge(&self, name: &str) -> Result<(), EventStoreError> {
        let conn = self.conn.lock().expect("registry lock poisoned");
        conn.execute("DELETE FROM event_datasets WHERE name = ?1", params![name])
            .map_err(|error| db_err(&error))?;
        Ok(())
    }
}

/// The SELECT column list shared by `get` and `list`, in `row_to_record`
/// order.
const SELECT_COLUMNS: &str = "SELECT name, location, source_kind, source_ref, actor_column, \
     time_column, time_unit, event_column, tiebreak_column, property_schema, shards, \
     file_list, dict_state, refresh_key, watermark, source_token, measured_events, \
     measured_actors, byte_size, build_millis, min_ts, max_ts, created_at, refreshed_at \
     FROM event_datasets";

/// Deserialize one registry row.
fn row_to_record(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<DatasetRecord, EventStoreError>> {
    let property_schema: String = row.get(9)?;
    let file_list: String = row.get(11)?;
    let dict_state: String = row.get(12)?;
    let parse = || -> Result<DatasetRecord, EventStoreError> {
        let properties: Vec<PropertyDef> =
            serde_json::from_str(&property_schema).map_err(|error| json_err(&error))?;
        let file_list: FileList =
            serde_json::from_str(&file_list).map_err(|error| json_err(&error))?;
        let dict_state: BTreeMap<String, u64> =
            serde_json::from_str(&dict_state).map_err(|error| json_err(&error))?;
        Ok(DatasetRecord {
            name: row.get(0).map_err(|error| db_err(&error))?,
            location: row.get(1).map_err(|error| db_err(&error))?,
            source_kind: row.get(2).map_err(|error| db_err(&error))?,
            source_ref: row.get(3).map_err(|error| db_err(&error))?,
            actor_column: row.get(4).map_err(|error| db_err(&error))?,
            time_column: row.get(5).map_err(|error| db_err(&error))?,
            time_unit: row.get(6).map_err(|error| db_err(&error))?,
            event_column: row.get(7).map_err(|error| db_err(&error))?,
            tiebreak_column: row.get(8).map_err(|error| db_err(&error))?,
            properties,
            shards: row.get(10).map_err(|error| db_err(&error))?,
            file_list,
            dict_state,
            refresh_key: row.get(13).map_err(|error| db_err(&error))?,
            watermark: row.get(14).map_err(|error| db_err(&error))?,
            source_token: row.get(15).map_err(|error| db_err(&error))?,
            measured_events: row.get(16).map_err(|error| db_err(&error))?,
            measured_actors: row.get(17).map_err(|error| db_err(&error))?,
            byte_size: row.get(18).map_err(|error| db_err(&error))?,
            build_millis: row.get(19).map_err(|error| db_err(&error))?,
            min_ts: row.get(20).map_err(|error| db_err(&error))?,
            max_ts: row.get(21).map_err(|error| db_err(&error))?,
            created_at: row.get(22).map_err(|error| db_err(&error))?,
            refreshed_at: row.get(23).map_err(|error| db_err(&error))?,
        })
    };
    Ok(parse())
}

/// The dataset directory name under the store root: the name sanitized to a
/// filesystem-safe alphabet plus a 16-hex FNV-1a hash of the exact name (the
/// same convention the materialized-view store uses), so distinct names never
/// share a directory.
pub fn location_for(name: &str) -> String {
    let mut safe = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            safe.push(ch.to_ascii_lowercase());
        } else {
            safe.push('_');
        }
    }
    format!("{safe}-{:016x}", crate::model::fnv1a64(name.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{PropertyEncoding, ShardFileEntry};

    /// A minimal record for round-trip tests.
    fn record(name: &str) -> DatasetRecord {
        DatasetRecord {
            name: name.to_string(),
            location: location_for(name),
            source_kind: "table".to_string(),
            source_ref: "ev.main.events".to_string(),
            actor_column: "entity_id".to_string(),
            time_column: "ts".to_string(),
            time_unit: "us".to_string(),
            event_column: "event_name".to_string(),
            tiebreak_column: Some("seq".to_string()),
            properties: vec![PropertyDef {
                name: "device".to_string(),
                data_type: fq_common::DataType::Varchar,
                encoding: PropertyEncoding::Dict,
                nullable: true,
            }],
            shards: 64,
            file_list: FileList {
                generations: vec![crate::model::GenerationEntry {
                    generation: 0,
                    shards: vec![ShardFileEntry {
                        shard: 0,
                        seg: "shard-0000/gen-0.seg".to_string(),
                        act: "shard-0000/gen-0.act".to_string(),
                        events: 10,
                        new_actors: 2,
                        bytes: 1000,
                    }],
                    dicts: Vec::new(),
                }],
            },
            dict_state: BTreeMap::from([("__event__".to_string(), 20u64)]),
            refresh_key: Some("seq".to_string()),
            watermark: None,
            source_token: None,
            measured_events: 10,
            measured_actors: 2,
            byte_size: 1000,
            build_millis: 5,
            min_ts: Some(0),
            max_ts: Some(100),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            refreshed_at: None,
        }
    }

    #[test]
    fn insert_get_list_round_trip() {
        let dir = std::env::temp_dir().join(format!("fqev-registry-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.sqlite");
        let registry = Registry::open(&path).unwrap();
        let rec = record("web");
        registry.insert(&rec).unwrap();
        assert_eq!(registry.get("web").unwrap(), rec);
        assert_eq!(registry.list().unwrap(), vec![rec.clone()]);
        // A duplicate live name raises.
        assert!(matches!(
            registry.insert(&rec),
            Err(EventStoreError::NameTaken { .. })
        ));
        // Tombstone hides it; purge removes it.
        registry.tombstone("web").unwrap();
        assert!(registry.get_optional("web").unwrap().is_none());
        assert_eq!(registry.tombstoned().unwrap().len(), 1);
        registry.purge("web").unwrap();
        assert!(registry.tombstoned().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unknown_dataset_raises() {
        let dir = std::env::temp_dir().join(format!("fqev-registry2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let registry = Registry::open(&dir.join("t.sqlite")).unwrap();
        assert!(matches!(
            registry.get("nope"),
            Err(EventStoreError::UnknownDataset { .. })
        ));
        std::fs::remove_dir_all(&dir).ok();
    }
}
