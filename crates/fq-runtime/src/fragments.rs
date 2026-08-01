//! `SHOW PUSHDOWN FRAGMENTS`: the queries the last statement handed to sources.
//!
//! Each row is one round trip to one source. `source_sql` is the text that was
//! sent, in that source's dialect; `canonical_sql` is the same query in canonical
//! Postgres, which is dialect-neutral and can therefore be run on a reference
//! engine over the same data and compared. `output_names` is what the plan
//! expected back, so a source that returns a different set of columns is visible
//! without executing anything.
//!
//! Recording happens after execution, so this view describes a statement that
//! ran; a statement that raised leaves the previous statement's rows in place.

use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};

use fq_common::DataType;
use fq_physical::{collect_pushdown_fragments, PushdownFragment};
use fq_plan::physical::PhysicalPlan;

use crate::error::RuntimeError;
use crate::Runtime;

/// The ordered result columns of `SHOW PUSHDOWN FRAGMENTS`.
const FRAGMENT_COLUMNS: [&str; 5] = [
    "datasource",
    "source_sql",
    "canonical_sql",
    "output_names",
    "has_dynamic_filter",
];

impl Runtime {
    /// Replace the recorded fragments with the ones this plan pushes, when
    /// recording is on. Called after execution, so every fragment here is one the
    /// engine successfully rendered and sent.
    pub(crate) fn record_pushdown_fragments(
        &self,
        physical: &PhysicalPlan,
    ) -> Result<(), RuntimeError> {
        if !self.config_snapshot().diagnostics.record_pushdown_fragments {
            return Ok(());
        }
        let fragments = collect_pushdown_fragments(physical)?;
        *self
            .pushdown_fragments
            .write()
            .expect("pushdown fragment journal lock is not poisoned") = fragments;
        Ok(())
    }

    /// `SHOW PUSHDOWN FRAGMENTS`: one row per query the last statement sent to a
    /// source, in plan order.
    pub(crate) fn show_pushdown_fragments(
        &self,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let fragments = self
            .pushdown_fragments
            .read()
            .expect("pushdown fragment journal lock is not poisoned")
            .clone();
        build_fragment_result(&fragments)
    }
}

/// Build the `SHOW PUSHDOWN FRAGMENTS` result from the recorded fragments.
fn build_fragment_result(
    fragments: &[PushdownFragment],
) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let mut columns = FragmentColumns::with_capacity(fragments.len());
    for fragment in fragments {
        columns.push(fragment);
    }
    let schema = fragment_schema();
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns.finish())
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
    Ok((schema, vec![batch]))
}

/// The per-column builders for the `SHOW PUSHDOWN FRAGMENTS` result.
struct FragmentColumns {
    datasources: Vec<String>,
    source_sqls: Vec<String>,
    canonical_sqls: Vec<String>,
    output_names: Vec<String>,
    dynamic_filters: Vec<bool>,
}

impl FragmentColumns {
    /// Empty builders sized for the fragment count.
    fn with_capacity(rows: usize) -> Self {
        Self {
            datasources: Vec::with_capacity(rows),
            source_sqls: Vec::with_capacity(rows),
            canonical_sqls: Vec::with_capacity(rows),
            output_names: Vec::with_capacity(rows),
            dynamic_filters: Vec::with_capacity(rows),
        }
    }

    /// Append one fragment's cells.
    fn push(&mut self, fragment: &PushdownFragment) {
        self.datasources.push(fragment.datasource.clone());
        self.source_sqls.push(fragment.source_sql.clone());
        self.canonical_sqls.push(fragment.canonical_sql.clone());
        self.output_names.push(fragment.output_names.join(", "));
        self.dynamic_filters.push(fragment.has_dynamic_filter);
    }

    /// The finished Arrow arrays, in the order `FRAGMENT_COLUMNS` names them.
    fn finish(self) -> Vec<arrow::array::ArrayRef> {
        vec![
            Arc::new(StringArray::from(self.datasources)),
            Arc::new(StringArray::from(self.source_sqls)),
            Arc::new(StringArray::from(self.canonical_sqls)),
            Arc::new(StringArray::from(self.output_names)),
            Arc::new(BooleanArray::from(self.dynamic_filters)),
        ]
    }
}

/// The result columns of `SHOW PUSHDOWN FRAGMENTS`, for describing the statement
/// without running it.
pub(crate) fn describe_columns() -> Vec<(String, DataType)> {
    let mut columns = Vec::with_capacity(FRAGMENT_COLUMNS.len());
    for name in FRAGMENT_COLUMNS {
        let data_type = match name {
            "has_dynamic_filter" => DataType::Boolean,
            _ => DataType::Text,
        };
        columns.push((name.to_owned(), data_type));
    }
    columns
}

/// The Arrow schema of the `SHOW PUSHDOWN FRAGMENTS` result.
fn fragment_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(FRAGMENT_COLUMNS.len());
    for name in FRAGMENT_COLUMNS {
        let data_type = match name {
            "has_dynamic_filter" => ArrowDataType::Boolean,
            _ => ArrowDataType::Utf8,
        };
        fields.push(Field::new(name, data_type, false));
    }
    Arc::new(Schema::new(fields))
}
