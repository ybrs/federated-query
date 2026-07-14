//! Reading an event view's persisted chunks back for the analysis kernels.
//! Chunks are the framed Arrow IPC files the materialized-view store wrote;
//! the registry's chunk list (not a directory listing) names them, and the
//! runtime hands their absolute paths here.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;

use crate::error::EventError;

/// Read the chunk files at `paths`, materializing ONLY the named columns. The
/// row order is unchanged (projection drops columns, never rows), so row
/// ordinals from a row-index sidecar still address the same rows. A funnel
/// consults only the entity, timestamp, and event columns, so projecting to
/// those skips decoding the property columns (a 5 GB chunk at 100M rows is
/// dominated by that decode). A name absent from the stored schema raises. The
/// returned schema carries the projected columns in the requested order.
pub fn read_chunks_columns(
    paths: &[String],
    columns: &[&str],
) -> Result<(SchemaRef, Vec<RecordBatch>), EventError> {
    // The IPC file decoder projects the BATCHES but its `schema()` still reports
    // the file's full schema, so the projected schema is derived here from the
    // stored schema and the same projection - the schema the batches carry.
    let mut full_schema: Option<SchemaRef> = None;
    let mut batches = Vec::new();
    for path in paths {
        let stored = FileReader::try_new(std::fs::File::open(path)?, None)?.schema();
        require_same_schema(&mut full_schema, &stored, path)?;
        let projection = column_projection(&stored, columns)?;
        let reader = FileReader::try_new(std::fs::File::open(path)?, Some(projection))?;
        for batch in reader {
            batches.push(batch?);
        }
    }
    let Some(full_schema) = full_schema else {
        return Err(EventError::ContractViolation(
            "event view has no chunk files; the store always writes at least one".to_string(),
        ));
    };
    let projection = column_projection(&full_schema, columns)?;
    let projected = Arc::new(full_schema.project(&projection)?);
    Ok((projected, batches))
}

/// The stored-schema indices of the requested columns, in request order, or a
/// loud error naming a column the stored schema does not carry.
fn column_projection(stored: &SchemaRef, columns: &[&str]) -> Result<Vec<usize>, EventError> {
    let mut projection = Vec::with_capacity(columns.len());
    for column in columns {
        let index = stored
            .fields()
            .iter()
            .position(|field| field.name() == column)
            .ok_or_else(|| {
                EventError::ContractViolation(format!(
                    "event view chunk has no column '{column}' to project"
                ))
            })?;
        projection.push(index);
    }
    Ok(projection)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;

    /// Write a two-column (id Int32, name Utf8) IPC chunk to a temp file and
    /// return its path.
    fn write_chunk() -> String {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("batch");
        let path = std::env::temp_dir().join(format!(
            "fq_events_chunk_{}_{}.arrow",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let file = std::fs::File::create(&path).expect("create chunk");
        let mut writer = FileWriter::try_new(file, &schema).expect("writer");
        writer.write(&batch).expect("write");
        writer.finish().expect("finish");
        path.to_string_lossy().to_string()
    }

    #[test]
    fn projecting_keeps_only_the_named_columns_in_request_order() {
        let path = write_chunk();
        let (schema, batches) =
            read_chunks_columns(std::slice::from_ref(&path), &["name"]).expect("read");
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "name");
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8");
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(2), "c");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn projecting_an_absent_column_raises() {
        let path = write_chunk();
        let error = read_chunks_columns(std::slice::from_ref(&path), &["ghost"]).unwrap_err();
        assert!(matches!(error, EventError::ContractViolation(_)), "{error}");
        let _ = std::fs::remove_file(&path);
    }
}
