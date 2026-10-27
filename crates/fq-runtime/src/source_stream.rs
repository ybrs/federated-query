//! Rename source batches lazily while preserving upstream read errors.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fq_events::build::BatchSource;
use std::sync::Arc;

struct RenamedSource {
    source: Box<dyn BatchSource>,
    names: Vec<String>,
    schema: Option<SchemaRef>,
}

/// Wrap the source without pulling any rows or allocating a result collection.
pub(crate) fn rename(source: Box<dyn BatchSource>, names: Vec<String>) -> Box<dyn BatchSource> {
    Box::new(RenamedSource {
        source,
        names,
        schema: None,
    })
}

impl RenamedSource {
    /// Fix the visible schema on the first batch and reject mislabeled result shapes.
    fn output_schema(&mut self, batch: &RecordBatch) -> Result<SchemaRef, String> {
        if let Some(schema) = &self.schema {
            return Ok(Arc::clone(schema));
        }
        if batch.num_columns() != self.names.len() {
            return Err(format!(
                "source returned {} columns but the plan names {}",
                batch.num_columns(),
                self.names.len()
            ));
        }
        let schema = crate::renamed_schema(&batch.schema(), &self.names);
        self.schema = Some(Arc::clone(&schema));
        Ok(schema)
    }
}

impl BatchSource for RenamedSource {
    /// Pull exactly one upstream batch and preserve errors instead of reporting EOF.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        let Some(batch) = self.source.next_batch()? else {
            return Ok(None);
        };
        let schema = self.output_schema(&batch)?;
        RecordBatch::try_new(schema, batch.columns().to_vec())
            .map(Some)
            .map_err(|error| error.to_string())
    }

    /// Preserve the source's final refresh watermark unchanged.
    fn watermark(&self) -> Option<String> {
        self.source.watermark()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use fq_events::build::BatchSource;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;

    struct FailingSource {
        pulls: Rc<Cell<usize>>,
    }

    impl BatchSource for FailingSource {
        /// Produce one batch and then a real source failure whose identity must survive.
        fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
            self.pulls.set(self.pulls.get() + 1);
            if self.pulls.get() != 1 {
                return Err("source read failed".into());
            }
            let schema = Arc::new(Schema::new(vec![Field::new(
                "internal",
                DataType::Int64,
                false,
            )]));
            Ok(Some(
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7]))]).unwrap(),
            ))
        }
    }

    /// A rename must neither consume ahead nor turn a later error into successful EOF.
    #[test]
    fn renames_lazily_and_preserves_midstream_error() {
        let pulls = Rc::new(Cell::new(0));
        let source = Box::new(FailingSource {
            pulls: pulls.clone(),
        });
        let mut renamed = super::rename(source, vec!["visible".into()]);
        assert_eq!(pulls.get(), 0);
        let batch = renamed.next_batch().unwrap().unwrap();
        assert_eq!(pulls.get(), 1);
        assert_eq!(batch.schema().field(0).name(), "visible");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(renamed.next_batch().unwrap_err(), "source read failed");
        assert_eq!(pulls.get(), 2);
    }

    /// A mismatched output-name list must fail before emitting any mislabeled rows.
    #[test]
    fn rejects_rename_shape_mismatch() {
        let source = Box::new(FailingSource {
            pulls: Rc::new(Cell::new(0)),
        });
        let mut renamed = super::rename(source, Vec::new());
        assert!(renamed
            .next_batch()
            .unwrap_err()
            .contains("source returned 1 columns"));
    }
}
