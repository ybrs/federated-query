//! Reading an event view's persisted chunks back for the analysis kernels.
//! Chunks are the framed Arrow IPC files the materialized-view store wrote;
//! the registry's chunk list (not a directory listing) names them, and the
//! runtime hands their absolute paths here.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;

use crate::error::EventError;

/// Read the chunk files at `paths` into memory as one batch list, returning
/// the stored schema. Every chunk must carry the same schema (the store
/// writes one view with one schema); a mismatch raises rather than letting a
/// kernel mix incompatible columns. An empty path list raises: the store
/// always writes at least one schema-bearing chunk, so none means the caller
/// resolved the view wrongly.
pub fn read_chunks(paths: &[String]) -> Result<(SchemaRef, Vec<RecordBatch>), EventError> {
    let mut schema: Option<SchemaRef> = None;
    let mut batches = Vec::new();
    for path in paths {
        let reader = FileReader::try_new(std::fs::File::open(path)?, None)?;
        require_same_schema(&mut schema, &reader.schema(), path)?;
        for batch in reader {
            batches.push(batch?);
        }
    }
    let Some(schema) = schema else {
        return Err(EventError::ContractViolation(
            "event view has no chunk files; the store always writes at least one".to_string(),
        ));
    };
    Ok((schema, batches))
}

/// Hold the first chunk's schema and raise on any later chunk that differs.
fn require_same_schema(
    held: &mut Option<SchemaRef>,
    current: &SchemaRef,
    path: &str,
) -> Result<(), EventError> {
    match held {
        None => {
            *held = Some(current.clone());
            Ok(())
        }
        Some(first) if first.as_ref() == current.as_ref() => Ok(()),
        Some(first) => Err(EventError::ContractViolation(format!(
            "chunk '{path}' has schema {current} but the view's first chunk has {first}"
        ))),
    }
}
