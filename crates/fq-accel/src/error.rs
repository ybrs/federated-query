//! Errors of the materialized-view store.

use thiserror::Error;

/// A failure in the view catalog, the chunk store, or the lifecycle over them.
#[derive(Debug, Error)]
pub enum AccelError {
    /// Underlying SQLite error from the view registry.
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    /// Underlying object-store error from a chunk read/write/delete.
    #[error(transparent)]
    Store(#[from] object_store::Error),

    /// A chunk location that is not a valid object-store path.
    #[error(transparent)]
    StorePath(#[from] object_store::path::Error),

    /// Arrow (de)serialization error framing a chunk.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    /// A malformed stored JSON column (chunk list / output schema).
    #[error("materialized-view catalog JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// A view name that already exists; creation never silently replaces.
    #[error("materialized view '{0}' already exists")]
    DuplicateView(String),

    /// A view name the registry does not know (or that is tombstoned).
    #[error("materialized view '{0}' does not exist")]
    UnknownView(String),

    /// A result schema the store cannot hold: an Arrow type with no engine
    /// mapping, a duplicate or empty output column name, or a refresh whose
    /// re-executed SELECT no longer matches the stored schema.
    #[error("materialized view schema: {0}")]
    InvalidSchema(String),

    /// A malformed watermark operation: a column index outside the schema, or
    /// a change-key column whose type drifted between pulls.
    #[error("materialized view watermark: {0}")]
    Watermark(String),

    /// A merge refresh that cannot honor the primary-key declaration: a key
    /// value duplicated in the fresh pull or in the stored chunks. The
    /// declaration asserts uniqueness; merging on a duplicate would silently
    /// drop or misplace rows.
    #[error("materialized view merge: {0}")]
    MergeKey(String),
}
