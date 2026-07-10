//! Errors for the catalog and its datasource abstraction.

use thiserror::Error;

/// A catalog-layer failure: metadata loading, type mapping, or a source error.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// A native column type the engine does not model. Ports the Python
    /// `ValueError("Unsupported column type for catalog mapping: ...")`: an
    /// unmodeled type must be added explicitly, never coerced to a string.
    #[error("Unsupported column type for catalog mapping: {0}")]
    UnsupportedColumnType(String),

    /// A column whose mapped `DataType` has no Arrow rendering - a connector bug,
    /// surfaced at load with the offending column rather than mid-query.
    #[error("Column {column:?} maps to DataType {data_type}, which has no Arrow rendering")]
    NonRenderableColumn { column: String, data_type: String },

    /// A failure reported by the underlying source driver (connection, query).
    /// Connectors map their driver-specific errors into this so the trait stays
    /// driver-agnostic.
    #[error("datasource error: {0}")]
    Source(String),
}
