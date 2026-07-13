//! Errors of the event-analytics extension.

use thiserror::Error;

/// A failure in the event-view registry, the materialization contract, or an
/// analysis kernel.
#[derive(Debug, Error)]
pub enum EventError {
    /// Underlying SQLite error from the role registry.
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    /// Arrow error sorting, casting, or framing batches.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    /// A chunk file that cannot be opened or read.
    #[error("event-view chunk io: {0}")]
    Io(#[from] std::io::Error),

    /// A role declaration the view cannot be built with: a role naming a
    /// column the SELECT does not produce, two roles naming one column, or a
    /// role column whose type has no event semantics.
    #[error("event view roles: {0}")]
    InvalidRoles(String),

    /// Event data that violates the materialization contract: a NULL in a
    /// role column, or a stored stream that is not (entity, timestamp)
    /// sorted. Raised at build before anything persists, and at scan before
    /// a kernel returns numbers computed over a broken ordering premise.
    #[error("event-view contract violated: {0}")]
    ContractViolation(String),

    /// An analysis spec the kernels refuse: a non-positive window, a step
    /// count outside 2..=16, a sub-day window over a DATE timestamp column,
    /// or window arithmetic that overflows the timestamp domain.
    #[error("event analysis: {0}")]
    Analysis(String),

    /// An event-view name that is already registered.
    #[error("event view '{0}' already exists")]
    DuplicateEventView(String),

    /// An event-view name the registry does not know.
    #[error("event view '{0}' does not exist")]
    UnknownEventView(String),
}
