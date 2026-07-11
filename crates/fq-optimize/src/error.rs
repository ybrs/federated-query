//! Estimation errors. Mirrors the `fq-decorrelate` thiserror conventions.
//!
//! NEVER SILENT: an unmodeled plan node, an ambiguous qualifier, a key that
//! resolves to no relation, or an unqualified join-key reference all RAISE. A
//! driver `Err` from the catalog/stats store PROPAGATES (only a catalog-returned
//! `None`/absence is the honest-unknown path).

use thiserror::Error;

use fq_decorrelate::DecorrelationError;

use crate::join_graph::JoinGraphError;

/// A failure applying an optimization rule to a logical plan.
///
/// `ProjectionPushdown`'s column pruner raises `PruneNoRule` on a plan node
/// outside its modeled set (a silently-skipped node could leave scans with stale
/// semantics). `JoinOrdering` raises `JoinOrder` on a violated enumerator/
/// placement invariant (a dropped or duplicated predicate is worse than a crash),
/// propagates a malformed-region `JoinGraph` error, and surfaces the estimator's
/// `Estimate` failures. Every other rule DECLINES (returns the plan unchanged).
/// The driver additionally raises `Scope` when a rule that changed the plan
/// produced a mis-scoped one - the loud safety net that fails at the rule that
/// broke it.
///
/// PartialEq is intentionally NOT derived: the wrapped `EstimateError` is not
/// itself `PartialEq`. Tests match on variants with `matches!`.
#[derive(Debug, Error)]
pub enum OptimizeError {
    /// `ProjectionPushdown`'s pruner reached a plan node it has no rule for.
    #[error("projection pushdown has no prune rule for plan node {0}")]
    PruneNoRule(&'static str),

    /// A subquery-bearing expression survived into the optimizer, where its
    /// columns are invisible to `Expr::children`; pruning around it could drop a
    /// needed column. Decorrelation must have removed it, so this is a violated
    /// invariant, not a valid plan.
    #[error("subquery expression {0} survived decorrelation into the optimizer")]
    SubquerySurvived(&'static str),

    /// A join-ordering invariant was violated (a bug, never a user error): a
    /// dropped/duplicated predicate, a misplaced equi key, or an enumerator
    /// state the connectivity guarantee makes impossible.
    #[error("join ordering invariant violated: {0}")]
    JoinOrder(String),

    /// An eager-aggregation invariant was violated (a bug, never a user error): a
    /// peeled dim that joins nothing already in scope, or a lifted join conjunct
    /// left unplaced. These fire only after every gate passed, so they can only
    /// mean the rewrite itself is inconsistent - a dropped or misplaced predicate
    /// manufactures wrong results, so it crashes rather than ships a lie.
    #[error("eager aggregation invariant violated: {0}")]
    EagerAggregation(String),

    /// A join region whose predicates cannot be soundly mapped to its atoms
    /// (unqualified or ambiguous reference); propagated, never swallowed.
    #[error(transparent)]
    JoinGraph(#[from] JoinGraphError),

    /// A cardinality estimate the join-ordering enumerator consulted failed.
    #[error(transparent)]
    Estimate(#[from] EstimateError),

    /// A rule produced a plan where a qualified reference escaped its relation's
    /// scope, caught by `validate_scope` after the rule changed the plan.
    #[error(transparent)]
    Scope(#[from] DecorrelationError),
}

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
