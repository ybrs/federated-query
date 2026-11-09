//! Durable identity and database metadata separate from result materialization.

mod database;
pub mod relations;
mod server;
mod sqlite;

pub use database::Grant;
pub use database::{DatabaseCatalog, DatasourceDefinition, StoredView};
pub use server::{ServerCatalog, User};
use sqlite::open_connection;

/// A SQLite or stored-JSON metadata failure propagated to the caller.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A local relation already owns a reserved or visible name.
    #[error("relation '{0}' already exists")]
    RelationExists(String),
    /// A relation is absent, unpublished, or has a different category.
    #[error("relation '{0}' does not exist")]
    RelationMissing(String),
    /// Stored relation metadata contains an unknown enum, state, or revision.
    #[error("invalid stored relation metadata: {0}")]
    InvalidRelation(String),
    /// Removing the last server administrator would strand credential management.
    #[error("cannot remove the last superuser")]
    LastSuperuser,
    /// A non-current metadata layout requires explicit recreation.
    #[error("catalog schema is incompatible; recreate the catalog")]
    IncompatibleSchema,
    /// A stored database permission is outside the supported privilege enum.
    #[error("invalid stored database privilege: {0}")]
    InvalidDatabasePrivilege(String),
    /// PUBLIC cannot own database administration authority.
    #[error("PUBLIC cannot receive database ADMIN")]
    PublicDatabaseAdmin,
    /// A database file does not belong to the selected registered identity.
    #[error("database metadata identity mismatch: expected {expected}, found {actual}")]
    DatabaseIdentity { expected: String, actual: String },
    /// A database name is already reserved by a live lifecycle operation.
    #[error("database '{0}' already exists")]
    DatabaseExists(String),
    /// A requested database has no registration.
    #[error("database '{0}' does not exist")]
    DatabaseMissing(String),
    /// A lifecycle mutation did not find its required prior state.
    #[error("database '{database}' must be {expected}")]
    DatabaseState { database: String, expected: String },
    /// The underlying SQLite operation failed.
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    /// A persisted JSON value is malformed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
