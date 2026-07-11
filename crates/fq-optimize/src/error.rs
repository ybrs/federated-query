//! Estimation errors. Mirrors the `fq-decorrelate` thiserror conventions.
//!
//! NEVER SILENT: an unmodeled plan node, an ambiguous qualifier, a key that
//! resolves to no relation, or an unqualified join-key reference all RAISE. A
//! driver `Err` from the catalog/stats store PROPAGATES (only a catalog-returned
//! `None`/absence is the honest-unknown path).

use thiserror::Error;

/// A failure estimating a logical subplan's cardinality.
///
/// PartialEq is intentionally NOT derived: the wrapped `CatalogError`/`StatsError`
/// are not themselves `PartialEq`. Tests match on variants with `matches!`.
#[derive(Debug, Error)]
pub enum EstimateError {
    /// An unmodeled plan node reached the estimator (e.g. `Explain`). A
    /// silently-guessed cardinality for an unmodeled operator would corrupt
    /// join ordering, so this fails loud.
    #[error("estimate has no rule for plan node {0}")]
    NoRule(&'static str),

    /// A qualifier names more than one relation in the subtree, so a join/group
    /// key cannot be assigned to a single side without silently mis-estimating.
    #[error("qualifier {0:?} is ambiguous in the join subtree")]
    AmbiguousQualifier(String),

    /// A join/group key's owning relation is not present in its subtree.
    #[error("{relation}.{column} resolves to no relation")]
    NoOwningRelation { relation: String, column: String },

    /// An unqualified column reached a join condition; every post-binder
    /// reference must carry its relation qualifier.
    #[error("unqualified column {0:?} in a join condition; every post-binder reference must be qualified")]
    UnqualifiedInCondition(String),

    /// The catalog knows no datasource of this name - a typo would silently
    /// disable costing for every query, so it raises.
    #[error("catalog knows no datasource {0:?}")]
    UnknownDatasource(String),

    /// A metadata/type failure reported by the catalog layer.
    #[error(transparent)]
    Catalog(#[from] fq_catalog::CatalogError),

    /// A failure talking to the learned-stats catalog.
    #[error(transparent)]
    Stats(#[from] fq_catalog::StatsError),
}

/// Crate-internal result alias.
pub(crate) type Result<T> = std::result::Result<T, EstimateError>;
