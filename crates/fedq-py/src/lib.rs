//! `fedq` - the native extension module that lets Python drive the Rust engine.
//!
//! This is the ONLY crate that depends on pyo3. It is a thin wrapper over
//! `fq_runtime::Runtime`: Python builds a `Runtime` from a config file, calls
//! `execute(sql)`, and pulls the Arrow result back over the zero-copy Arrow
//! C-stream boundary (`ffi::ArrowStreamExport`). The benchmark harness writes a
//! temporary YAML config, constructs `fedq.Runtime(path)`, and consumes the
//! result with `pa.RecordBatchReader.from_stream(...)` / `pa.table(...)`.
//!
//! No engine logic lives here: every stage (parse, bind, decorrelate, optimize,
//! plan, execute) is fq-runtime's; this crate only marshals config in and Arrow
//! out and maps a `RuntimeError` onto a Python exception.

mod ffi;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use fq_common::load_config;
use fq_runtime::{Runtime as EngineRuntime, RuntimeError};

use ffi::{stream_from_batches, ArrowStreamExport};

/// A federated-query engine session, exposed to Python.
///
/// Wraps `fq_runtime::Runtime` (Send + Sync: it holds an `Arc<Catalog>` of
/// Send+Sync datasources plus plain config), so the pyclass needs no
/// `unsendable`; the GIL is released for the whole query run.
#[pyclass]
pub struct Runtime {
    inner: EngineRuntime,
}

#[pymethods]
impl Runtime {
    /// Build a runtime from a YAML config file PATH.
    ///
    /// A path (not a dict) is the whole constructor surface: it is the exact
    /// form `fq_common::load_config` consumes, it keeps the loudness of the
    /// config loader (unknown section / malformed datasource raise here), and
    /// the harness already writes a temporary YAML. Loading the config and
    /// wiring every datasource (metadata + the exec data plane) both happen
    /// now, so a bad config or an unreachable source fails at construction.
    #[new]
    fn new(config_path: &str) -> PyResult<Self> {
        let config = load_config(config_path).map_err(|error| {
            PyRuntimeError::new_err(format!("failed to load config '{config_path}': {error}"))
        })?;
        let inner =
            EngineRuntime::from_config(&config).map_err(|error| runtime_error_to_py(&error))?;
        Ok(Self { inner })
    }

    /// Run one SQL statement and return its Arrow result as an exportable
    /// C-stream. The engine spins its own runtime and touches no Python state,
    /// so the GIL is RELEASED for the run (`Python::detach`): holding it would
    /// freeze every other Python thread in the caller for the query's whole
    /// duration.
    fn execute(&self, py: Python<'_>, sql: &str) -> PyResult<ArrowStreamExport> {
        let (schema, batches) = py
            .detach(|| self.inner.execute(sql))
            .map_err(|error| runtime_error_to_py(&error))?;
        Ok(stream_from_batches(schema, batches))
    }
}

/// Map a `RuntimeError` onto a Python exception, preserving the exact upstream
/// cause message (no laundering - a crash never ships a lie). Every stage error,
/// including a bind rejection of an invalid query, surfaces to Python as a
/// `RuntimeError` carrying the engine's own message.
fn runtime_error_to_py(error: &RuntimeError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

/// The extension module's version (its Cargo package version). A cheap smoke
/// test that the module loaded.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The `fedq` module: the `Runtime` session class, the Arrow stream export type,
/// and a `version()` probe.
#[pymodule]
fn fedq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Runtime>()?;
    m.add_class::<ArrowStreamExport>()?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}
