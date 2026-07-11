//! The export half of the Arrow C-stream boundary between Rust and Python.
//!
//! Data crosses in Arrow's C stream ABI, wrapped in a PyCapsule named
//! `arrow_array_stream` (the Arrow PyCapsule interface). Nothing is copied at
//! the boundary: the engine hands over a `FFI_ArrowArrayStream` struct and the
//! Python consumer (pyarrow) pulls batches through it lazily via
//! `pa.RecordBatchReader.from_stream(...)` / `pa.table(...)`.
//!
//! This is the export direction only (Rust -> Python). The engine already
//! materializes the whole result, so `stream_from_batches` wraps the finished
//! batches; the import direction (`__arrow_c_stream__` on a Python producer)
//! is not needed here. Ported from `fedqrs/src/ffi.rs` (pyo3 + arrow only, no
//! engine coupling).

use std::ffi::CString;

use arrow::array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

const STREAM_CAPSULE_NAME: &str = "arrow_array_stream";

/// A Rust-owned Arrow stream, ready to be pulled by Python.
///
/// Holds the C stream struct until Python asks for it via `__arrow_c_stream__`,
/// at which point ownership moves into a PyCapsule. A stream is one-shot, so a
/// second export attempt raises rather than handing out a spent stream.
///
/// `unsendable`: the raw C stream struct is neither Send nor Sync, and it is
/// always created and consumed on the one Python thread driving the query.
#[pyclass(unsendable)]
pub struct ArrowStreamExport {
    inner: Option<FFI_ArrowArrayStream>,
}

impl ArrowStreamExport {
    /// Wrap any `Send` record-batch reader as an exportable stream.
    pub fn new(reader: Box<dyn RecordBatchReader + Send>) -> Self {
        ArrowStreamExport {
            inner: Some(FFI_ArrowArrayStream::new(reader)),
        }
    }
}

/// Build an exportable stream from fully-read batches and their schema.
pub fn stream_from_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> ArrowStreamExport {
    let items: Vec<Result<RecordBatch, ArrowError>> = batches.into_iter().map(Ok).collect();
    ArrowStreamExport::new(Box::new(RecordBatchIterator::new(
        items.into_iter(),
        schema,
    )))
}

#[pymethods]
impl ArrowStreamExport {
    /// Arrow PyCapsule interface: hand the C stream to the consumer.
    ///
    /// `requested_schema` (a schema-cast request) is accepted and ignored; the
    /// engine already produces the exact schema the result carries.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &mut self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let stream = self
            .inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Arrow stream already consumed"))?;
        let name = CString::new(STREAM_CAPSULE_NAME).unwrap();
        // The capsule owns the struct; when Python's consumer moves the stream
        // out it nulls the release, so the capsule's Drop sees an empty struct.
        PyCapsule::new(py, stream, Some(name))
    }
}
