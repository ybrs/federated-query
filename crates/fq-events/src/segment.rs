//! The segmentation kernel: a metric over events grouped into UTC calendar
//! time buckets, computed in one linear pass over the sorted stream.
//!
//! Distinct-entity counting is O(1) memory per bucket BECAUSE of the storage
//! contract: within any filtered substream entities arrive in non-decreasing
//! order, so "a new distinct entity for this bucket" is exactly "different
//! from this bucket's last contributor".

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Datelike, NaiveTime, Timelike, Utc};
use fq_common::events::{SegmentMeasure, SegmentSpec, TimeBucket};

use crate::error::EventError;
use crate::stream::{EntityKey, EventStream};

/// The segmentation result relation's fixed schema: one row per non-empty
/// bucket, ordered by bucket.
pub fn segment_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "bucket",
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", ArrowType::Int64, false),
    ]))
}

/// One bucket's accumulation: the running value plus, for the ENTITIES
/// measure, the bucket's last contributing entity (the sort contract makes
/// that single held key sufficient for exact distinct counting).
struct Cell {
    value: i64,
    last_entity: Option<EntityKey>,
}

/// Run a segmentation over a sorted event stream: filter by event name when
/// the spec carries one, truncate each event's instant to the spec's UTC
/// calendar bucket, and count events or distinct entities per bucket.
pub fn run_segment(
    stream: &EventStream,
    spec: &SegmentSpec,
) -> Result<(SchemaRef, Vec<RecordBatch>), EventError> {
    let mut cells: BTreeMap<i64, Cell> = BTreeMap::new();
    for row in stream.rows() {
        let row = row?;
        if let Some(only) = &spec.event {
            if row.event != only {
                continue;
            }
        }
        let instant = stream.scale().to_utc(row.time)?;
        let bucket = bucket_start_micros(instant, spec.bucket);
        let cell = cells.entry(bucket).or_insert(Cell {
            value: 0,
            last_entity: None,
        });
        accumulate(cell, spec.measure, &row);
    }
    Ok((segment_schema(), vec![result_batch(&cells)?]))
}

/// Fold one event into its bucket's cell under the spec's measure.
fn accumulate(cell: &mut Cell, measure: SegmentMeasure, row: &crate::stream::EventRow<'_>) {
    match measure {
        SegmentMeasure::Events => cell.value += 1,
        SegmentMeasure::Entities => {
            let already_counted = cell
                .last_entity
                .as_ref()
                .is_some_and(|held| held.matches(row.entity));
            if !already_counted {
                cell.value += 1;
                cell.last_entity = Some(row.entity.to_owned_key());
            }
        }
    }
}

/// The bucket start for an instant, as microseconds since the epoch: the UTC
/// calendar truncation the spec asks for (hour, day, ISO week's Monday, or
/// first of month), always at 00:00 except the hour bucket.
fn bucket_start_micros(instant: DateTime<Utc>, bucket: TimeBucket) -> i64 {
    let date = instant.date_naive();
    let start = match bucket {
        TimeBucket::Hour => date
            .and_time(NaiveTime::from_hms_opt(instant.hour(), 0, 0).expect("a valid hour of day")),
        TimeBucket::Day => date.and_time(NaiveTime::MIN),
        TimeBucket::Week => {
            let monday = date - chrono::Days::new(u64::from(date.weekday().num_days_from_monday()));
            monday.and_time(NaiveTime::MIN)
        }
        TimeBucket::Month => date
            .with_day(1)
            .expect("every month has a first day")
            .and_time(NaiveTime::MIN),
    };
    start.and_utc().timestamp_micros()
}

/// Assemble the one result batch from the accumulated cells, in bucket order
/// (the BTreeMap's iteration order).
fn result_batch(cells: &BTreeMap<i64, Cell>) -> Result<RecordBatch, EventError> {
    let mut buckets = Vec::with_capacity(cells.len());
    let mut values = Vec::with_capacity(cells.len());
    for (bucket, cell) in cells {
        buckets.push(*bucket);
        values.push(cell.value);
    }
    Ok(RecordBatch::try_new(
        segment_schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(buckets)),
            Arc::new(Int64Array::from(values)),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use fq_common::events::EventRoleColumns;

    /// The standard test roles over (user_id, ts, name), no tiebreak.
    fn roles() -> EventRoleColumns {
        EventRoleColumns {
            entity: "user_id".to_string(),
            timestamp: "ts".to_string(),
            event: "name".to_string(),
            tiebreak: None,
        }
    }

    /// A sorted stream over (user_id Int32, ts Timestamp(us), name Utf8) rows.
    fn stream(rows: &[(i32, i64, &str)]) -> EventStream {
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
        EventStream::open("ev", &schema, &[batch], &roles()).expect("open")
    }

    /// Flatten a segmentation result to (bucket_us, value) tuples.
    fn cells(result: &[RecordBatch]) -> Vec<(i64, i64)> {
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        let buckets = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("bucket timestamps");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 values");
        let mut rows = Vec::new();
        for row in 0..batch.num_rows() {
            rows.push((buckets.value(row), values.value(row)));
        }
        rows
    }

    /// One microsecond-scale day.
    const DAY: i64 = 86_400 * 1_000_000;

    /// A daily spec with the given measure and optional event filter.
    fn spec(measure: SegmentMeasure, event: Option<&str>) -> SegmentSpec {
        SegmentSpec {
            view: "ev".to_string(),
            measure,
            event: event.map(str::to_string),
            bucket: TimeBucket::Day,
        }
    }

    #[test]
    fn daily_event_counts_are_hand_computable() {
        // Day 0: three events; day 2: one. Day 1 has none and is not emitted.
        let stream = stream(&[
            (1, 0, "view"),
            (1, 100, "view"),
            (1, 2 * DAY, "view"),
            (2, 50, "click"),
        ]);
        let (_, result) =
            run_segment(&stream, &spec(SegmentMeasure::Events, None)).expect("segment");
        assert_eq!(cells(&result), vec![(0, 3), (2 * DAY, 1)]);
    }

    #[test]
    fn distinct_entities_count_each_user_once_per_bucket() {
        // Day 0 sees u1 (twice) and u2: two distinct entities. Day 1 sees u2
        // alone: an entity spanning buckets counts in each bucket it touches.
        let stream = stream(&[
            (1, 0, "view"),
            (1, 100, "view"),
            (2, 200, "view"),
            (2, DAY + 1, "view"),
        ]);
        let (_, result) =
            run_segment(&stream, &spec(SegmentMeasure::Entities, None)).expect("segment");
        assert_eq!(cells(&result), vec![(0, 2), (DAY, 1)]);
    }

    #[test]
    fn the_event_filter_measures_one_event_name() {
        let stream = stream(&[(1, 0, "view"), (1, 100, "click"), (2, 200, "view")]);
        let (_, result) =
            run_segment(&stream, &spec(SegmentMeasure::Events, Some("click"))).expect("segment");
        assert_eq!(cells(&result), vec![(0, 1)]);
    }

    #[test]
    fn calendar_buckets_truncate_in_utc() {
        // 2026-03-15 was a Sunday; its ISO week starts Monday 2026-03-09 and
        // its month bucket is 2026-03-01.
        let instant = DateTime::parse_from_rfc3339("2026-03-15T13:45:10Z")
            .expect("valid instant")
            .with_timezone(&Utc);
        let hour = DateTime::parse_from_rfc3339("2026-03-15T13:00:00Z").expect("hour");
        let day = DateTime::parse_from_rfc3339("2026-03-15T00:00:00Z").expect("day");
        let week = DateTime::parse_from_rfc3339("2026-03-09T00:00:00Z").expect("week");
        let month = DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z").expect("month");
        assert_eq!(
            bucket_start_micros(instant, TimeBucket::Hour),
            hour.timestamp_micros()
        );
        assert_eq!(
            bucket_start_micros(instant, TimeBucket::Day),
            day.timestamp_micros()
        );
        assert_eq!(
            bucket_start_micros(instant, TimeBucket::Week),
            week.timestamp_micros()
        );
        assert_eq!(
            bucket_start_micros(instant, TimeBucket::Month),
            month.timestamp_micros()
        );
    }
}
