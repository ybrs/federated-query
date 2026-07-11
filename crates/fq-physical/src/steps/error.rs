//! Step-building errors. Every unmodeled node/case is a typed variant - a crash
//! never ships a lie, and an unhandled shape RAISES rather than emitting a plan
//! that could produce wrong rows. Ports the `UnsupportedIR` raise sites of
//! `executor/rust_ir.py`, split into precise variants.

use fq_emit::EmitError;

/// Errors `build_steps` raises. Each is a loud raise, never a silent default.
#[derive(Debug, thiserror::Error)]
pub enum StepError {
    /// A physical node the step builder does not model (a bare `Cte`, `RemoteJoin`,
    /// `LateralJoin`, `Explain`, `Gather` - none reach a well-formed step plan).
    #[error("physical node {0} is not supported by step-building")]
    UnsupportedNode(&'static str),

    /// A join type with no IR join-kind mapping.
    #[error("join type {0} is not yet supported by step-building")]
    UnsupportedJoinType(String),

    /// A cross-source DISTINCT ON: the project fragment cannot pick a survivor
    /// (no source ordering), so it fails loud rather than dropping the DISTINCT ON.
    #[error("DISTINCT ON is only supported when the query pushes to a single source")]
    DistinctOnCrossSource,

    /// A structured scan spec was asked of a scan that folds
    /// aggregation/ordering/limits (a capability probe, caught by the fallback).
    #[error("cannot build a structured (injectable) scan spec for a non-plain scan")]
    NonPlainScan,

    /// A source node that renders no source SQL (neither a scan nor a remote read).
    #[error("physical node {0} does not render source SQL")]
    NoSourceSql(&'static str),

    /// The reduced-join gate chose the reduced path but emission found no injection
    /// base (the gate and emission disagree - a step-builder bug, surfaced loudly).
    #[error("reduced join probe lost its injection base")]
    LostInjectionBase,

    /// A VALUES cell that is not a constant literal.
    #[error("non-literal VALUES cell is not supported")]
    NonLiteralValuesCell,

    /// A scan's SELECT list contains a `*` - a star must never survive into a bound
    /// scan (the binder expands it), so a star here is a loud raise.
    #[error("a '*' must never appear in a bound scan's columns")]
    StarInScanColumns,

    /// A two-stage window-over-GROUPING() aggregate the step builder does not yet
    /// render (the rare DataFusion planner-gap path); surfaced loudly, never
    /// mis-rendered as a single stage.
    #[error("window over GROUPING() (two-stage split) is not yet supported by step-building")]
    WindowSplitUnsupported,

    /// A SQL render/transpile failure, surfaced verbatim.
    #[error(transparent)]
    Emit(#[from] EmitError),
}
