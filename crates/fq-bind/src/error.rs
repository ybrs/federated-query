//! Binding errors.

use thiserror::Error;

/// A failure resolving names/types in a logical plan.
///
/// Ports `binder.py::BindingError`. This is the layer that enforces the "an
/// invalid query MUST raise" rule: a bogus qualifier, a typo'd column, an
/// ambiguous name, or a set-operation arity mismatch fails here, before any
/// source is touched - never returns rows for a query it could not resolve.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BindError {
    /// A table reference the catalog does not know.
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// A qualified reference to a table not in scope.
    #[error("Table '{table}' not found in scope for column '{column}'")]
    TableNotInScope { table: String, column: String },

    /// A qualified column absent from its (in-scope) table.
    #[error("Column '{column}' not found in table '{table}'")]
    ColumnNotInTable { column: String, table: String },

    /// A bare column that resolves in no scope.
    #[error("Column '{0}' not found in any table in scope")]
    ColumnNotInScope(String),

    /// A bare column found in more than one table of a scope.
    #[error("Column '{0}' is ambiguous (found in multiple tables)")]
    AmbiguousColumn(String),

    /// UNION/INTERSECT/EXCEPT branches expose differing column counts.
    #[error("set operation branches have different column counts: {left} vs {right}")]
    SetOpArity { left: usize, right: usize },

    /// A construct the binder does not handle; binding it raises.
    #[error("cannot bind: {0}")]
    Unsupported(String),

    /// A reference to a datasource that failed to connect/load at construction:
    /// carries the real connector error naming the source, deferred here so an
    /// unreferenced unavailable source blocks nothing.
    #[error("datasource '{name}' is unavailable: {error}")]
    DatasourceUnavailable { name: String, error: String },
}
