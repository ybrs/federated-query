//! The engine's error type.
//!
//! The imported `fedqrs` engine reported failures as pyo3 `PyRuntimeError` /
//! `PyValueError`. De-pyo3-ifying it, every one of those becomes a variant of
//! `ExecError` carrying the same message text: a runtime failure (a source read,
//! a DataFusion execution, a contract violation the engine refuses loudly) maps
//! to `Runtime`, and a bad-input failure (an unknown datasource kind, a missing
//! parameter) maps to `Value`. The GIL-release wrapper that surrounded the
//! engine in pyo3 moves to the later `fedq-py` crate; nothing here touches
//! Python.

use thiserror::Error;

/// A failure raised by the execution engine.
#[derive(Debug, Error)]
pub enum ExecError {
    /// A runtime failure: a source read, a DataFusion execution, a spill, or a
    /// planner-contract violation the engine refuses to guess past. (Was
    /// `PyRuntimeError`.)
    #[error("{0}")]
    Runtime(String),

    /// An invalid-input failure: an unknown datasource kind or a missing
    /// required parameter. (Was `PyValueError`.)
    #[error("{0}")]
    Value(String),
}

impl ExecError {
    /// Build a `Runtime` error from anything string-like (the `PyRuntimeError::
    /// new_err` replacement).
    pub fn runtime(message: impl Into<String>) -> Self {
        ExecError::Runtime(message.into())
    }

    /// Build a `Value` error from anything string-like (the `PyValueError::
    /// new_err` replacement).
    pub fn value(message: impl Into<String>) -> Self {
        ExecError::Value(message.into())
    }
}

/// The engine's result alias (was pyo3's `PyResult`).
pub type ExecResult<T> = Result<T, ExecError>;
