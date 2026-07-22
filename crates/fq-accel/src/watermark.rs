//! The delta-append high-water mark: the largest change-key value a view has
//! pulled, extracted from Arrow batches and stored (as JSON) in the view's
//! registry row. A delta refresh pulls only rows ABOVE the watermark, so the
//! representation must be EXACT - any value the engine cannot represent or
//! render exactly is refused as `Unsupported` and the caller re-pulls whole
//! (a lossy watermark would silently skip or duplicate rows).

use arrow::array::{
    Array, ArrowPrimitiveType, AsArray, PrimitiveArray, RecordBatch, StringArrayType,
};
use arrow::datatypes::{DataType as ArrowType, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::error::AccelError;

/// One watermark value, typed to the change-key column's family. Ordering is
/// only defined within one variant: a view's column keeps its type across
/// refreshes, so a cross-variant comparison is a drifted store and raises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Watermark {
    /// Any integer-family column, widened to i64.
    Int(i64),
    /// A text column; SQL string comparison and Rust `str` comparison agree
    /// under the engine's binary collation.
    Text(String),
    /// A date column as days since the Unix epoch (arrow Date32; Date64
    /// normalizes exactly - it carries a date at millisecond grain, and only
    /// whole days are meaningful).
    DateDays(i32),
    /// A zone-less timestamp as microseconds since the Unix epoch. Second /
    /// millisecond / microsecond arrow units all widen exactly; nanosecond
    /// units are refused (truncation would re-pull boundary rows as
    /// duplicates).
    TimestampMicros(i64),
    /// A UTC-zoned timestamp as microseconds since the Unix epoch. Only a
    /// UTC-equivalent zone is accepted at scan (any other zone is refused);
    /// the value is an absolute instant, rendered with an explicit `+00:00`
    /// offset so every source dialect compares it exactly.
    TimestampTzMicros(i64),
}

impl Watermark {
    /// The larger of two watermarks of the SAME variant; a variant mismatch
    /// means the column changed type between pulls and raises.
    pub fn max_with(self, other: Watermark) -> Result<Watermark, AccelError> {
        match (self, other) {
            (Watermark::Int(a), Watermark::Int(b)) => Ok(Watermark::Int(a.max(b))),
            (Watermark::Text(a), Watermark::Text(b)) => Ok(Watermark::Text(a.max(b))),
            (Watermark::DateDays(a), Watermark::DateDays(b)) => Ok(Watermark::DateDays(a.max(b))),
            (Watermark::TimestampMicros(a), Watermark::TimestampMicros(b)) => {
                Ok(Watermark::TimestampMicros(a.max(b)))
            }
            (Watermark::TimestampTzMicros(a), Watermark::TimestampTzMicros(b)) => {
                Ok(Watermark::TimestampTzMicros(a.max(b)))
            }
            (a, b) => Err(AccelError::Watermark(format!(
                "watermark type changed between pulls ({a:?} vs {b:?}); the \
                 change-key column's type is not stable"
            ))),
        }
    }

    /// The watermark rendered as a SQL literal in the engine's own dialect,
    /// for the delta filter (`WHERE <column> > <literal>`) the runtime wraps
    /// around the view's stored SELECT. Every rendering is exact for its
    /// variant's precision.
    pub fn sql_literal(&self) -> String {
        match self {
            Watermark::Int(value) => value.to_string(),
            Watermark::Text(value) => format!("'{}'", value.replace('\'', "''")),
            Watermark::DateDays(days) => format!("DATE '{}'", date_from_epoch_days(*days)),
            Watermark::TimestampMicros(micros) => {
                let timestamp = chrono::DateTime::from_timestamp_micros(*micros)
                    .expect("stored watermark micros are in chrono range")
                    .naive_utc();
                format!("TIMESTAMP '{}'", timestamp.format("%Y-%m-%d %H:%M:%S%.6f"))
            }
            // A plain quoted string, not a TIMESTAMP keyword literal: the
            // TIMESTAMP keyword names the zone-less type in the source
            // dialects, which would drop the offset; a string with an explicit
            // offset coerces to the column's zoned type exactly everywhere the
            // engine pushes (DataFusion, DuckDB, Postgres).
            Watermark::TimestampTzMicros(micros) => {
                let timestamp = chrono::DateTime::from_timestamp_micros(*micros)
                    .expect("stored watermark micros are in chrono range")
                    .naive_utc();
                format!("'{}+00:00'", timestamp.format("%Y-%m-%d %H:%M:%S%.6f"))
            }
        }
    }
}

/// Render days-since-epoch as `YYYY-MM-DD`.
fn date_from_epoch_days(days: i32) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date");
    let date = epoch
        .checked_add_signed(chrono::Duration::days(i64::from(days)))
        .expect("stored watermark days are in chrono range");
    date.format("%Y-%m-%d").to_string()
}

/// The outcome of scanning batches for a watermark.
#[derive(Debug, PartialEq, Eq)]
pub enum WatermarkScan {
    /// The column can carry a watermark; the max over all rows, or None when
    /// there are no rows.
    Value(Option<Watermark>),
    /// The column cannot carry one (unsupported type flavor, or NULLs - a
    /// NULL never satisfies `> watermark`, so a delta over it would silently
    /// miss rows forever). The reason is surfaced in the refresh status; the
    /// caller re-pulls whole.
    Unsupported(String),
}

/// Scan `batches` and produce the max of column `column_index` as a watermark.
/// Raises only on a malformed input (an index outside the schema); every
/// data-shaped limitation is an `Unsupported` outcome, because the caller has
/// a correct fallback (a whole re-pull), and a limitation must steer there,
/// not abort the refresh.
pub fn scan_watermark(
    batches: &[RecordBatch],
    column_index: usize,
) -> Result<WatermarkScan, AccelError> {
    let mut max: Option<Watermark> = None;
    for batch in batches {
        if column_index >= batch.num_columns() {
            return Err(AccelError::Watermark(format!(
                "watermark column index {column_index} is outside the {}-column batch",
                batch.num_columns()
            )));
        }
        match batch_watermark(batch, column_index) {
            WatermarkScan::Unsupported(reason) => return Ok(WatermarkScan::Unsupported(reason)),
            WatermarkScan::Value(None) => {}
            WatermarkScan::Value(Some(value)) => {
                max = Some(match max {
                    None => value,
                    Some(current) => current.max_with(value)?,
                });
            }
        }
    }
    Ok(WatermarkScan::Value(max))
}

/// The watermark of one batch's column: NULL and type-flavor checks, then the
/// per-family max.
fn batch_watermark(batch: &RecordBatch, column_index: usize) -> WatermarkScan {
    let array = batch.column(column_index);
    let name = batch.schema().field(column_index).name().clone();
    if array.null_count() > 0 {
        return WatermarkScan::Unsupported(format!(
            "watermark column '{name}' holds NULL; a NULL never passes the \
             delta filter, so rows would be missed"
        ));
    }
    typed_max(array.as_ref(), &name)
}

/// Dispatch on the arrow type family; anything not listed is Unsupported with
/// the flavor named.
fn typed_max(array: &dyn Array, name: &str) -> WatermarkScan {
    use arrow::datatypes as adt;
    match array.data_type() {
        ArrowType::Int8 => int_max::<adt::Int8Type>(array),
        ArrowType::Int16 => int_max::<adt::Int16Type>(array),
        ArrowType::Int32 => int_max::<adt::Int32Type>(array),
        ArrowType::Int64 => int_max::<adt::Int64Type>(array),
        ArrowType::UInt8 => int_max::<adt::UInt8Type>(array),
        ArrowType::UInt16 => int_max::<adt::UInt16Type>(array),
        ArrowType::UInt32 => int_max::<adt::UInt32Type>(array),
        ArrowType::Utf8 => text_max(&array.as_string::<i32>()),
        ArrowType::LargeUtf8 => text_max(&array.as_string::<i64>()),
        ArrowType::Utf8View => text_max(&array.as_string_view()),
        ArrowType::Date32 => date32_max(array),
        ArrowType::Date64 => date64_max(array),
        ArrowType::Timestamp(unit, None) => timestamp_max(array, *unit, name, false),
        ArrowType::Timestamp(unit, Some(zone)) if is_utc_zone(zone) => {
            timestamp_max(array, *unit, name, true)
        }
        ArrowType::Timestamp(_, Some(zone)) => WatermarkScan::Unsupported(format!(
            "watermark column '{name}' carries zone '{zone}'; only a UTC zone \
             compares exactly across sources"
        )),
        other => WatermarkScan::Unsupported(format!(
            "watermark column '{name}' has type {other}, which cannot carry \
             an exact watermark"
        )),
    }
}

/// Whether an arrow timestamp zone string denotes UTC (the zero offset under
/// any of its spellings).
fn is_utc_zone(zone: &str) -> bool {
    matches!(
        zone.to_ascii_uppercase().as_str(),
        "UTC" | "Z" | "+00:00" | "+0000" | "+00"
    )
}

/// Max of an integer-family primitive array, widened to i64.
fn int_max<T>(array: &dyn Array) -> WatermarkScan
where
    T: ArrowPrimitiveType,
    T::Native: Into<i64>,
{
    let typed: &PrimitiveArray<T> = array.as_primitive();
    let max = arrow::compute::max(typed).map(|value| Watermark::Int(value.into()));
    WatermarkScan::Value(max)
}

/// Max of any string-array flavor.
fn text_max<'a>(array: &impl StringArrayType<'a>) -> WatermarkScan {
    let mut max: Option<&str> = None;
    for index in 0..array.len() {
        let value = array.value(index);
        if max.is_none_or(|current| value > current) {
            max = Some(value);
        }
    }
    WatermarkScan::Value(max.map(|value| Watermark::Text(value.to_string())))
}

/// Max of a Date32 array (already days since epoch).
fn date32_max(array: &dyn Array) -> WatermarkScan {
    let typed: &PrimitiveArray<arrow::datatypes::Date32Type> = array.as_primitive();
    let max = arrow::compute::max(typed).map(Watermark::DateDays);
    WatermarkScan::Value(max)
}

/// Max of a Date64 array (milliseconds since epoch carrying a date; the
/// division to whole days is exact by the Date64 contract - a mid-day
/// remainder means the column is not a date and is refused).
fn date64_max(array: &dyn Array) -> WatermarkScan {
    const MILLIS_PER_DAY: i64 = 86_400_000;
    let typed: &PrimitiveArray<arrow::datatypes::Date64Type> = array.as_primitive();
    match arrow::compute::max(typed) {
        None => WatermarkScan::Value(None),
        Some(millis) if millis % MILLIS_PER_DAY == 0 => WatermarkScan::Value(Some(
            Watermark::DateDays(i32::try_from(millis / MILLIS_PER_DAY).expect("date fits i32")),
        )),
        Some(millis) => WatermarkScan::Unsupported(format!(
            "Date64 value {millis}ms is not a whole day; the column is not a date"
        )),
    }
}

/// Max of a timestamp array, widened exactly to microseconds; `zoned` picks
/// the UTC-zoned variant (the caller already verified the zone is UTC).
/// Nanosecond timestamps are refused: truncating to microseconds would set the
/// watermark BELOW the true max and the next delta would re-pull (duplicate)
/// the boundary rows.
fn timestamp_max(array: &dyn Array, unit: TimeUnit, name: &str, zoned: bool) -> WatermarkScan {
    use arrow::datatypes as adt;
    let (raw, per_unit_micros) = match unit {
        TimeUnit::Second => (
            arrow::compute::max(array.as_primitive::<adt::TimestampSecondType>()),
            1_000_000_i64,
        ),
        TimeUnit::Millisecond => (
            arrow::compute::max(array.as_primitive::<adt::TimestampMillisecondType>()),
            1_000,
        ),
        TimeUnit::Microsecond => (
            arrow::compute::max(array.as_primitive::<adt::TimestampMicrosecondType>()),
            1,
        ),
        TimeUnit::Nanosecond => {
            return WatermarkScan::Unsupported(format!(
                "watermark column '{name}' is a nanosecond timestamp; a \
                 microsecond watermark would duplicate boundary rows"
            ));
        }
    };
    match raw {
        None => WatermarkScan::Value(None),
        Some(value) => match value.checked_mul(per_unit_micros) {
            Some(micros) if zoned => {
                WatermarkScan::Value(Some(Watermark::TimestampTzMicros(micros)))
            }
            Some(micros) => WatermarkScan::Value(Some(Watermark::TimestampMicros(micros))),
            None => WatermarkScan::Unsupported(format!(
                "watermark column '{name}' timestamp overflows microseconds"
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    };
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// A one-column batch named `v` over the given array.
    fn batch(array: ArrayRef) -> RecordBatch {
        let field = Field::new("v", array.data_type().clone(), true);
        let schema = Arc::new(Schema::new(vec![field]));
        RecordBatch::try_new(schema, vec![array]).expect("batch")
    }

    #[test]
    fn int_watermark_is_the_max_across_batches() {
        let first = batch(Arc::new(Int64Array::from(vec![3_i64, 9, 1])));
        let second = batch(Arc::new(Int64Array::from(vec![7_i64])));
        let scan = scan_watermark(&[first, second], 0).expect("scan");
        assert_eq!(scan, WatermarkScan::Value(Some(Watermark::Int(9))));
    }

    #[test]
    fn empty_batches_yield_no_watermark() {
        let empty = batch(Arc::new(Int64Array::from(Vec::<i64>::new())));
        let scan = scan_watermark(&[empty], 0).expect("scan");
        assert_eq!(scan, WatermarkScan::Value(None));
    }

    #[test]
    fn text_and_date_and_timestamp_watermarks() {
        let text = batch(Arc::new(StringArray::from(vec!["b", "a", "c"])));
        assert_eq!(
            scan_watermark(std::slice::from_ref(&text), 0).expect("scan"),
            WatermarkScan::Value(Some(Watermark::Text("c".to_string())))
        );
        let date = batch(Arc::new(Date32Array::from(vec![10, 20, 15])));
        assert_eq!(
            scan_watermark(std::slice::from_ref(&date), 0).expect("scan"),
            WatermarkScan::Value(Some(Watermark::DateDays(20)))
        );
        let ts = batch(Arc::new(TimestampMicrosecondArray::from(vec![
            5_000_000_i64,
            1_000_000,
        ])));
        assert_eq!(
            scan_watermark(std::slice::from_ref(&ts), 0).expect("scan"),
            WatermarkScan::Value(Some(Watermark::TimestampMicros(5_000_000)))
        );
    }

    #[test]
    fn nulls_are_unsupported_with_the_reason() {
        let with_null = batch(Arc::new(Int64Array::from(vec![Some(1_i64), None])));
        match scan_watermark(&[with_null], 0).expect("scan") {
            WatermarkScan::Unsupported(reason) => assert!(reason.contains("NULL"), "{reason}"),
            WatermarkScan::Value(other) => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn nanosecond_timestamps_and_floats_are_unsupported() {
        let nanos = batch(Arc::new(TimestampNanosecondArray::from(vec![1_i64])));
        assert!(matches!(
            scan_watermark(&[nanos], 0).expect("scan"),
            WatermarkScan::Unsupported(_)
        ));
        let floats = batch(Arc::new(arrow::array::Float64Array::from(vec![1.0])));
        assert!(matches!(
            scan_watermark(&[floats], 0).expect("scan"),
            WatermarkScan::Unsupported(_)
        ));
    }

    #[test]
    fn sql_literals_render_exactly() {
        assert_eq!(Watermark::Int(42).sql_literal(), "42");
        assert_eq!(
            Watermark::Text("o'brien".to_string()).sql_literal(),
            "'o''brien'"
        );
        // 2020-03-01 is 18322 days past the epoch.
        assert_eq!(
            Watermark::DateDays(18322).sql_literal(),
            "DATE '2020-03-01'"
        );
        assert_eq!(
            Watermark::TimestampMicros(1_583_020_800_123_456).sql_literal(),
            "TIMESTAMP '2020-03-01 00:00:00.123456'"
        );
    }

    #[test]
    fn utc_zoned_timestamps_carry_a_zoned_watermark() {
        let array = TimestampMicrosecondArray::from(vec![5_000_000_i64, 9_000_000])
            .with_timezone("UTC");
        let zoned = batch(Arc::new(array));
        assert_eq!(
            scan_watermark(std::slice::from_ref(&zoned), 0).expect("scan"),
            WatermarkScan::Value(Some(Watermark::TimestampTzMicros(9_000_000)))
        );
        // The literal is a plain quoted string carrying the explicit offset.
        assert_eq!(
            Watermark::TimestampTzMicros(1_583_020_800_123_456).sql_literal(),
            "'2020-03-01 00:00:00.123456+00:00'"
        );
    }

    #[test]
    fn a_non_utc_zone_is_unsupported_naming_the_zone() {
        let array = TimestampMicrosecondArray::from(vec![1_i64]).with_timezone("+02:00");
        let zoned = batch(Arc::new(array));
        match scan_watermark(&[zoned], 0).expect("scan") {
            WatermarkScan::Unsupported(reason) => {
                assert!(reason.contains("+02:00"), "{reason}");
            }
            WatermarkScan::Value(other) => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn max_with_rejects_a_type_change() {
        let error = Watermark::Int(1)
            .max_with(Watermark::Text("a".to_string()))
            .unwrap_err();
        assert!(matches!(error, AccelError::Watermark(_)));
    }

    #[test]
    fn watermark_roundtrips_through_json() {
        for watermark in [
            Watermark::Int(7),
            Watermark::Text("t".to_string()),
            Watermark::DateDays(1),
            Watermark::TimestampMicros(2),
            Watermark::TimestampTzMicros(3),
        ] {
            let json = serde_json::to_string(&watermark).expect("serialize");
            let back: Watermark = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, watermark);
        }
    }
}
