//! The materialization contract: an event view's chunks hold rows globally
//! sorted by (entity ASC, timestamp ASC) - extended by (tiebreak ASC) when
//! the view declares a tiebreak role - with no NULL in a role column.
//! `build_event_view` establishes it - it validates the executed result and
//! returns it sorted, ready for the materialized-view store. The kernels'
//! cursor (`EventStream::rows`) re-verifies the same ordering at every scan.

use arrow::array::RecordBatch;
use arrow::compute::{concat_batches, lexsort_to_indices, take_record_batch, SortColumn};
use arrow::datatypes::SchemaRef;
use fq_common::events::EventRoleColumns;

use crate::error::EventError;
use crate::stream::EventStream;

/// Validate an executed defining-SELECT result against the role contract and
/// return its rows sorted by (entity, timestamp[, tiebreak]). Raises
/// `InvalidRoles` on a role that is missing, duplicated, or mistyped, and
/// `ContractViolation` on a NULL role value; nothing is meant to persist when
/// this raises.
pub fn build_event_view(
    view: &str,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    roles: &EventRoleColumns,
) -> Result<Vec<RecordBatch>, EventError> {
    // Opening a stream runs the full validation: role resolution and
    // distinctness, the type contract, and the NULL checks. The build path
    // only needs the checks, not the rows.
    EventStream::open(view, schema, batches, roles)?;
    let combined = concat_batches(schema, batches)?;
    let sorted = sort_by_contract_keys(&combined, roles)?;
    Ok(vec![sorted])
}

/// Sort one batch by (entity ASC, timestamp ASC), extended by (tiebreak ASC)
/// when the view declares a tiebreak role. Rows equal in every sort key keep
/// an UNSPECIFIED relative order; the analysis semantics are defined to be
/// independent of that residual order, so a rebuild never changes an answer.
fn sort_by_contract_keys(
    batch: &RecordBatch,
    roles: &EventRoleColumns,
) -> Result<RecordBatch, EventError> {
    let role_positions = crate::stream::role_indices(&batch.schema(), roles)?;
    let mut key_indices = vec![role_positions.entity, role_positions.time];
    if let Some(tiebreak_index) = role_positions.tiebreak {
        key_indices.push(tiebreak_index);
    }
    let mut sort_columns = Vec::with_capacity(key_indices.len());
    for key_index in key_indices {
        sort_columns.push(SortColumn {
            values: batch.column(key_index).clone(),
            options: None,
        });
    }
    let indices = lexsort_to_indices(&sort_columns, None)?;
    Ok(take_record_batch(batch, &indices)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType as ArrowType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    /// The standard test roles over (user_id, ts, name), no tiebreak.
    fn roles() -> EventRoleColumns {
        EventRoleColumns {
            entity: "user_id".to_string(),
            timestamp: "ts".to_string(),
            event: "name".to_string(),
            tiebreak: None,
        }
    }

    /// A (user_id Int32, ts Timestamp(us), name Utf8) batch from row tuples.
    fn batch(rows: &[(i32, i64, &str)]) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", ArrowType::Int32, true),
            Field::new(
                "ts",
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("name", ArrowType::Utf8, true),
        ]));
        let mut users = Vec::new();
        let mut times = Vec::new();
        let mut names = Vec::new();
        for (user, time, name) in rows {
            users.push(*user);
            times.push(*time);
            names.push(*name);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(users)),
                Arc::new(TimestampMicrosecondArray::from(times)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("batch");
        (schema, batch)
    }

    /// Flatten a built result back to (user, ts, name) tuples.
    fn tuples(batches: &[RecordBatch]) -> Vec<(i32, i64, String)> {
        let mut rows = Vec::new();
        for batch in batches {
            let users = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 users");
            let times = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("us timestamps");
            let names = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 names");
            for row in 0..batch.num_rows() {
                rows.push((
                    users.value(row),
                    times.value(row),
                    names.value(row).to_string(),
                ));
            }
        }
        rows
    }

    #[test]
    fn build_sorts_by_entity_then_time_across_batches() {
        let (schema, first) = batch(&[(2, 10, "b"), (1, 30, "c")]);
        let (_, second) = batch(&[(1, 10, "a"), (2, 5, "d")]);
        let sorted = build_event_view("ev", &schema, &[first, second], &roles()).expect("build");
        assert_eq!(
            tuples(&sorted),
            vec![
                (1, 10, "a".to_string()),
                (1, 30, "c".to_string()),
                (2, 5, "d".to_string()),
                (2, 10, "b".to_string()),
            ]
        );
    }

    #[test]
    fn build_of_an_empty_result_is_an_empty_sorted_batch() {
        let (schema, empty) = batch(&[]);
        let sorted = build_event_view("ev", &schema, &[empty], &roles()).expect("build");
        assert_eq!(tuples(&sorted).len(), 0);
    }

    #[test]
    fn a_declared_tiebreak_is_the_third_sort_key() {
        // Equal (entity, timestamp) rows land in tiebreak order regardless of
        // their input order.
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", ArrowType::Int32, true),
            Field::new(
                "ts",
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("name", ArrowType::Utf8, true),
            Field::new("seq", ArrowType::Int32, true),
        ]));
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(TimestampMicrosecondArray::from(vec![10, 10, 5])),
                Arc::new(StringArray::from(vec!["b", "a", "c"])),
                Arc::new(Int32Array::from(vec![2, 1, 9])),
            ],
        )
        .expect("batch");
        let mut with_tiebreak = roles();
        with_tiebreak.tiebreak = Some("seq".to_string());
        let sorted = build_event_view("ev", &schema, &[data], &with_tiebreak).expect("build");
        let names = sorted[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 names")
            .clone();
        let mut order = Vec::new();
        for row in 0..sorted[0].num_rows() {
            order.push(names.value(row).to_string());
        }
        assert_eq!(order, vec!["c", "a", "b"]);
    }

    #[test]
    fn duplicate_role_columns_raise() {
        let (schema, data) = batch(&[(1, 10, "a")]);
        let mut bad = roles();
        bad.event = "user_id".to_string();
        let error = build_event_view("ev", &schema, &[data], &bad).unwrap_err();
        assert!(matches!(error, EventError::InvalidRoles(_)), "{error}");
    }

    #[test]
    fn a_mistyped_role_raises() {
        // Swapped roles: the timestamp role points at the text column.
        let (schema, data) = batch(&[(1, 10, "a")]);
        let mut bad = roles();
        bad.timestamp = "name".to_string();
        bad.event = "ts".to_string();
        let error = build_event_view("ev", &schema, &[data], &bad).unwrap_err();
        assert!(matches!(error, EventError::InvalidRoles(_)), "{error}");
    }
}
