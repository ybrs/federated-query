//! The runtime's error type: one enum over every stage the orchestration drives.
//!
//! Each pipeline stage has its own error (parse, bind, decorrelate, optimize,
//! physical planning, execution) plus the connector/catalog setup errors from
//! `from_config`. `RuntimeError` wraps them via `#[from]` so `execute` and
//! `from_config` propagate the exact upstream cause with `?` - no message
//! rewriting, no laundering (a crash never ships a lie).

use fq_bind::BindError;
use fq_catalog::CatalogError;
use fq_decorrelate::DecorrelationError;
use fq_exec::ExecError;
use fq_optimize::OptimizeError;
use fq_parse::ParseError;
use fq_physical::{PhysicalError, StepError};
use thiserror::Error;

/// Every way `Runtime::from_config` or `Runtime::execute` can fail.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// A datasource block is missing a required parameter, or names a source
    /// kind the runtime does not build. Surfaced from `from_config`.
    #[error("configuration error: {0}")]
    Config(String),

    /// A catalog / connector setup failure (opening a source, loading metadata).
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),

    /// SQL that fq-parse cannot convert (an unsupported construct, or a syntax
    /// error). EXPLAIN currently lands here at parse; see the runtime's EXPLAIN
    /// handling.
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),

    /// An invalid reference the binder rejects (a bogus qualifier, a typo'd
    /// column, a table the query does not name). An invalid query MUST raise.
    #[error("bind error: {0}")]
    Bind(#[from] BindError),

    /// A subquery shape decorrelation cannot flatten.
    #[error("decorrelation error: {0}")]
    Decorrelate(#[from] DecorrelationError),

    /// An optimizer-rule failure (e.g. a predicate that cannot be placed).
    #[error("optimize error: {0}")]
    Optimize(#[from] OptimizeError),

    /// A physical-planning failure (an unorientable join key, an unlowerable
    /// node).
    #[error("physical planning error: {0}")]
    Physical(#[from] PhysicalError),

    /// An execution-engine failure (a source read, a fragment run, an unmapped
    /// expression).
    #[error("execution error: {0}")]
    Exec(#[from] ExecError),

    /// A step-render failure while describing a plan for EXPLAIN (a source-leaf
    /// scan whose effective SQL cannot be rendered). Surfaced so an EXPLAIN never
    /// emits a silently mislabeled scan row.
    #[error("step render error: {0}")]
    Step(#[from] StepError),

    /// A learned-stats catalog failure (opening the sqlite next to the config,
    /// or persisting an execution's observations). Never swallowed: a broken
    /// write path would silently starve every future plan of measurements.
    #[error("stats catalog error: {0}")]
    Stats(#[from] fq_catalog::StatsError),

    /// Planning blew its wall-clock budget (`optimizer.planning_budget_ms`).
    /// Planning is O(metadata) by design; the message reports every completed
    /// stage's timing so the offender is visible. Deep kills inside statistics
    /// collection surface as `Optimize`/`Physical` wrapping
    /// `EstimateError::PlanBudget` instead, naming the exact fetch.
    #[error("{0}")]
    PlanningBudget(String),

    /// The engine returned a result whose column count does not match the plan's
    /// user-visible output names. This means the rename would drop or invent a
    /// column, so it RAISES rather than ship a mislabeled result.
    #[error("result rename mismatch: {0}")]
    ResultShape(String),
}
