//! The materialization contract: an event view's chunks hold rows globally
//! sorted by (entity ASC, timestamp ASC) - extended by (tiebreak ASC) when
//! the view declares a tiebreak role - with no NULL in a role column. The sort
//! itself is the engine executor's job: the materialization runs the defining
//! SELECT with an ORDER BY on the contract keys, so `build_event_view` receives
//! rows already in contract order. It validates the roles and consolidates the
//! executor's output into the single stored chunk. The kernels' cursor
//! (`EventStream::rows`) re-verifies the ordering at every scan.

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use fq_common::events::EventRoleColumns;

use crate::error::EventError;
use crate::stream::validate_roles;

/// Validate an executor result (already ordered by the contract keys) against
/// the role contract and consolidate it into one stored chunk. Raises
/// `InvalidRoles` on a role that is missing, duplicated, or mistyped, and
/// `ContractViolation` on a NULL role value; nothing is meant to persist when
/// this raises. Consumes `batches` and drops them as soon as they are copied
/// into the single combined batch, so only one full copy of the rows is live.
pub fn build_event_view(
    view: &str,
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    roles: &EventRoleColumns,
) -> Result<Vec<RecordBatch>, EventError> {
    validate_roles(view, schema, &batches, roles)?;
    let combined = concat_batches(schema, &batches)?;
    drop(batches);
    Ok(vec![combined])
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
    fn build_consolidates_ordered_batches_preserving_their_order() {
        // The executor delivers rows already in contract order; build only
        // concatenates them into the one stored chunk, row order unchanged.
        let (schema, first) = batch(&[(1, 10, "a"), (1, 30, "c")]);
        let (_, second) = batch(&[(2, 5, "d"), (2, 10, "b")]);
        let built = build_event_view("ev", &schema, vec![first, second], &roles()).expect("build");
        assert_eq!(built.len(), 1);
        assert_eq!(
            tuples(&built),
            vec![
                (1, 10, "a".to_string()),
                (1, 30, "c".to_string()),
                (2, 5, "d".to_string()),
                (2, 10, "b".to_string()),
            ]
        );
    }

    #[test]
    fn build_of_an_empty_result_is_an_empty_batch() {
        let (schema, empty) = batch(&[]);
        let built = build_event_view("ev", &schema, vec![empty], &roles()).expect("build");
        assert_eq!(tuples(&built).len(), 0);
    }

    #[test]
    fn duplicate_role_columns_raise() {
        let (schema, data) = batch(&[(1, 10, "a")]);
        let mut bad = roles();
        bad.event = "user_id".to_string();
        let error = build_event_view("ev", &schema, vec![data], &bad).unwrap_err();
        assert!(matches!(error, EventError::InvalidRoles(_)), "{error}");
    }

    #[test]
    fn a_mistyped_role_raises() {
        // Swapped roles: the timestamp role points at the text column.
        let (schema, data) = batch(&[(1, 10, "a")]);
        let mut bad = roles();
        bad.timestamp = "name".to_string();
        bad.event = "ts".to_string();
        let error = build_event_view("ev", &schema, vec![data], &bad).unwrap_err();
        assert!(matches!(error, EventError::InvalidRoles(_)), "{error}");
    }

    #[test]
    fn a_null_role_value_raises() {
        // Consolidation still runs the full contract check; a NULL role raises.
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", ArrowType::Int32, true),
            Field::new(
                "ts",
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("name", ArrowType::Utf8, true),
        ]));
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(TimestampMicrosecondArray::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("batch");
        let error = build_event_view("ev", &schema, vec![data], &roles()).unwrap_err();
        assert!(matches!(error, EventError::ContractViolation(_)), "{error}");
    }
}
