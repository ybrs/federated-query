//! SQLite connection setup with bounded retries for schema lock contention.

use super::StoreError;
use rusqlite::{Connection, ErrorCode};
use std::time::Duration;

/// Whether a metadata operation failed because another writer holds a lock.
fn is_busy(error: &StoreError) -> bool {
    matches!(error, StoreError::Sqlite(rusqlite::Error::SqliteFailure(inner, _))
        if matches!(inner.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked))
}

/// Retry only idempotent schema operations after bounded lock contention.
fn with_busy_retry(
    mut operation: impl FnMut() -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    let mut backoff = Duration::from_millis(2);
    for _ in 0..8 {
        match operation() {
            Err(error) if is_busy(&error) => std::thread::sleep(backoff),
            result => return result,
        }
        backoff = backoff.saturating_mul(2);
    }
    operation()
}

/// Open durable metadata with WAL and initialize its own schema.
pub(super) fn open_connection(
    path: &str,
    initialize: impl Fn(&Connection) -> Result<(), StoreError>,
) -> Result<Connection, StoreError> {
    let mut conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    with_busy_retry(|| {
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        initialize(&tx)?;
        tx.commit()?;
        Ok(())
    })?;
    Ok(conn)
}
