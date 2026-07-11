//! Decorrelation errors.

use thiserror::Error;

/// A failure decorrelating a subquery expression. Ports `DecorrelationError` /
/// `NonFlattenableCorrelation`.
///
/// The engine ALWAYS decorrelates: every unsupported shape fails fast here rather
/// than leaving a subquery expression to be planned physically. `NonFlattenable`
/// is the one variant the scalar path CATCHES (to fall back to a lateral join);
/// every other variant propagates.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecorrelationError {
    /// A correlation cannot become a set-based join (non-equality across an
    /// aggregate or LIMIT). Caught by the scalar path -> LATERAL fallback.
    #[error("non-flattenable correlation: {0}")]
    NonFlattenable(String),

    /// A shape decorrelation refuses to guess at (multi-column scalar, star value
    /// subquery, OFFSET in a correlated subquery, skip-level correlation, ...).
    #[error("cannot decorrelate: {0}")]
    Unsupported(String),

    /// A post-condition failed - e.g. a subquery expression survived the pass, or
    /// an outer reference was left unresolved. A loud internal guard.
    #[error("decorrelation invariant violated: {0}")]
    Invariant(String),

    /// A qualified column reference resolves to a relation not in scope at the
    /// node that uses it - a mis-scoped plan a rewrite produced. The loud safety
    /// net that re-checks the well-scoped invariant after decorrelation, so a
    /// mis-scoped plan fails here rather than returning wrong rows downstream.
    #[error("scope violation: {0}")]
    Scope(String),
}
