//! The physical planner's error type. Every unmodeled node/case is a typed
//! variant - the planner never returns a plausible-but-wrong plan (a crash never
//! ships a lie).

use fq_emit::EmitError;
use fq_optimize::EstimateError;

/// Errors the physical planner raises. Each maps to a Python `raise` site in
/// `physical_planner.py`; a cross-source shape the merge engine cannot resolve
/// fails loud rather than manufacturing a wrong plan.
// No `PartialEq`: the transparent `Estimate`/`SingleSource` payloads wrap error
// types that do not implement it. Tests match on the variant instead.
#[derive(Debug, thiserror::Error)]
pub enum PhysicalError {
    /// A scan names a source the catalog does not hold (validated at plan time).
    #[error("data source not found: {0}")]
    DatasourceNotFound(String),

    /// A cross-source LATERAL whose right side collects more than one base scan.
    #[error("cross-source LATERAL with multiple base relations is not supported yet")]
    LateralMultipleBaseRelations,

    /// A cross-source LATERAL right side that single-source pushdown cannot render.
    #[error("cross-source LATERAL right side is not renderable")]
    LateralNotRenderable,

    /// A cross-source NATURAL/USING join has no ON condition to key a merge-engine
    /// join on; a conditionless nested loop would be a silent Cartesian product.
    #[error("cross-source NATURAL/USING join is not supported; use an explicit ON condition")]
    CrossSourceNaturalUsingJoin,

    /// A CTE reference whose producer is not registered in the current scope.
    #[error("CTE '{name}' is not in scope")]
    CteNotInScope { name: String },

    /// A recursive cross-source CTE the merge engine cannot render.
    #[error("recursive CTE '{name}' is not renderable for the merge engine")]
    RecursiveCteNotRenderable { name: String },

    /// An equi-join key pair that resolves to neither side by qualifier or by bare
    /// column name; keeping the original order would key the join on mismatched
    /// columns (wrong or empty results), so the planner fails loud.
    #[error(
        "cannot orient join keys '{first}' / '{second}' to a join side; \
         neither resolves by qualifier or by column name"
    )]
    UnorientableJoinKeys { first: String, second: String },

    /// A cost-model estimate failed (propagated, never swallowed).
    #[error(transparent)]
    Estimate(#[from] EstimateError),

    /// A same-source render failed inside single-source pushdown (a surviving
    /// subquery reaching emit is a decorrelation bug), surfaced here verbatim.
    #[error(transparent)]
    SingleSource(#[from] EmitError),

    /// A learned-stats catalog read failed while a dim-shipping gate consulted a
    /// measured group count. Propagated, never mapped to a decline: a catalog
    /// FAULT is not the same as a catalog that recorded nothing (the latter is an
    /// `Ok(None)` decline; this is a real error the caller must see).
    #[error(transparent)]
    Stats(#[from] fq_catalog::StatsError),
}
