//! The shared ORDINAL SCALE, temporal parsing, and interval pairing for range
//! selectivity. Ports the `_range_ordinal`/`_temporal_days`/`_interval_*` family
//! of `optimizer/cost.py`. Clean-Rust-portable (selectivity is never persisted;
//! any consistent linear days count works - only the fraction `(p-low)/(high-low)`
//! matters).

use std::collections::BTreeMap;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};

use fq_catalog::datasource::{ColumnStatistics, StatValue, TableStatistics};
use fq_plan::expr::{BinaryOpType, ColumnRef, Expr, LiteralValue};

/// A range operand's comparable value. `None` at the call site means "not a
/// literal" (Python's `_NOT_A_LITERAL` sentinel).
#[derive(Debug, Clone, PartialEq)]
pub enum ComparableValue {
    /// A plain number (integer or float).
    Number(f64),
    /// An already-parsed temporal value on the day scale.
    Days(f64),
    /// An unparsed string, resolved to the day scale lazily.
    Text(String),
    /// A bool or NULL: has no ordinal position.
    Unusable,
}

/// The comparable value of a range operand: a plain `Literal`'s value, a temporal
/// `CAST` of one (`DATE 'x'`) parsed to the day scale, or `None` (not a literal).
pub fn comparable_literal(expr: &Expr) -> Option<ComparableValue> {
    match expr {
        Expr::Literal { value, .. } => Some(from_literal_value(value)),
        Expr::Cast {
            expr, data_type, ..
        } => {
            let Expr::Literal { value, .. } = expr.as_ref() else {
                return None;
            };
            if matches!(
                data_type,
                Some(fq_common::DataType::Date | fq_common::DataType::Timestamp)
            ) {
                Some(temporal_comparable(value))
            } else {
                Some(from_literal_value(value))
            }
        }
        _ => None,
    }
}

/// Convert a literal payload to a comparable value (no temporal parsing).
fn from_literal_value(value: &LiteralValue) -> ComparableValue {
    match value {
        LiteralValue::Integer(integer) => ComparableValue::Number(*integer as f64),
        LiteralValue::Float(float) => ComparableValue::Number(*float),
        LiteralValue::String(text) => ComparableValue::Text(text.clone()),
        LiteralValue::Boolean(_) | LiteralValue::Null => ComparableValue::Unusable,
    }
}

/// A temporal-CAST payload: a parseable ISO string lands on the day scale, an
/// unparseable one stays text (resolved to `None` downstream), a non-string keeps
/// its plain comparable value.
fn temporal_comparable(value: &LiteralValue) -> ComparableValue {
    let LiteralValue::String(text) = value else {
        return from_literal_value(value);
    };
    match parse_iso_days(text) {
        Some(days) => ComparableValue::Days(days),
        None => ComparableValue::Text(text.clone()),
    }
}

/// A statistic bound's position on the shared ordinal scale: numbers as-is,
/// ISO date/timestamp TEXT (Postgres histogram bounds) parsed to days, `None`
/// for a boolean or an unparseable string.
pub fn ordinal(value: &StatValue) -> Option<f64> {
    match value {
        StatValue::Integer(integer) => Some(*integer as f64),
        StatValue::Float(float) => Some(*float),
        StatValue::Boolean(_) => None,
        StatValue::Text(text) => parse_iso_days(text),
    }
}

/// A literal's position on the shared ordinal scale.
pub fn ordinal_of_literal(value: &ComparableValue) -> Option<f64> {
    match value {
        ComparableValue::Number(number) | ComparableValue::Days(number) => Some(*number),
        ComparableValue::Text(text) => parse_iso_days(text),
        ComparableValue::Unusable => None,
    }
}

/// Parse an ISO-8601 date or timestamp to (fractional) days since the proleptic
/// Gregorian epoch (Jan 1 of year 1 = day 1, matching Python `toordinal()`); the
/// absolute epoch is irrelevant since every use is a fraction on one scale.
fn parse_iso_days(text: &str) -> Option<f64> {
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Some(f64::from(date.num_days_from_ce()));
    }
    // Match Python `datetime.fromisoformat`: a `T` or space separator, OPTIONAL
    // fractional seconds (`%.f` matches both none and a fraction), and an optional
    // timezone offset (`Z`/`+HH:MM`). The fraction and offset are consumed but not
    // used - `datetime_days` keeps only the wall-clock h:m:s, matching Python's
    // `_temporal_days`, and the offset never shifts the ordinal (relative scale).
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(datetime) = NaiveDateTime::parse_from_str(text, format) {
            return Some(datetime_days(datetime));
        }
    }
    if let Ok(datetime) = DateTime::parse_from_rfc3339(text) {
        return Some(datetime_days(datetime.naive_local()));
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%dT%H:%M:%S%.f%:z"] {
        if let Ok(datetime) = DateTime::parse_from_str(text, format) {
            return Some(datetime_days(datetime.naive_local()));
        }
    }
    None
}

/// Days since the epoch for a datetime, with the time of day as a fraction.
fn datetime_days(datetime: NaiveDateTime) -> f64 {
    let time = datetime.time();
    let seconds = time.hour() * 3600 + time.minute() * 60 + time.second();
    f64::from(datetime.date().num_days_from_ce()) + f64::from(seconds) / 86_400.0
}

/// Linear position of `point` in `[low, high]` oriented by direction; `None` when
/// any bound is missing or the span is degenerate.
pub fn interpolate_fraction(
    low: Option<f64>,
    high: Option<f64>,
    point: Option<f64>,
    below: bool,
) -> Option<f64> {
    let (Some(low), Some(high), Some(point)) = (low, high, point) else {
        return None;
    };
    let span = high - low;
    if span <= 0.0 {
        return None;
    }
    let base = clamp01((point - low) / span);
    Some(if below { base } else { 1.0 - base })
}

/// Normalize a range comparison to `(column, literal, below)` where `below` means
/// the predicate keeps column values BELOW the literal. `(None, None, false)`
/// when it is not a column-vs-literal comparison.
pub fn range_parts<'a>(
    op: BinaryOpType,
    left: &'a Expr,
    right: &'a Expr,
) -> (Option<&'a ColumnRef>, Option<ComparableValue>, bool) {
    let below_ops = matches!(op, BinaryOpType::Lt | BinaryOpType::Lte);
    let left_value = comparable_literal(left);
    let right_value = comparable_literal(right);
    match (left, right) {
        (Expr::Column(column), _) if right_value.is_some() => {
            (Some(column), right_value, below_ops)
        }
        // literal < column reads as column > literal: the direction flips.
        (_, Expr::Column(column)) if left_value.is_some() => (Some(column), left_value, !below_ops),
        _ => (None, None, false),
    }
}

/// The column's statistics when both the ref and the stats exist.
pub fn column_stats_or_none<'a>(
    stats: Option<&'a TableStatistics>,
    col_ref: Option<&ColumnRef>,
) -> Option<&'a ColumnStatistics> {
    let (stats, col_ref) = (stats?, col_ref?);
    stats.column_stats.get(&col_ref.column)
}

/// Whether the column's min/max sit on a usable ordinal scale.
fn ordinal_stats(col_stats: &ColumnStatistics) -> Option<(f64, f64)> {
    let low = col_stats.min_value.as_ref().and_then(ordinal)?;
    let high = col_stats.max_value.as_ref().and_then(ordinal)?;
    Some((low, high))
}

/// One column's accumulated range bounds during interval pairing.
struct ColBounds<'a> {
    below: Vec<(f64, &'a Expr)>,
    above: Vec<(f64, &'a Expr)>,
    low: f64,
    high: f64,
}

/// Partition conjuncts into per-column both-bounded range pairs (returned as one
/// combined fraction) and everything else (per-conjunct pricing).
pub fn interval_terms<'a>(
    conjuncts: &[&'a Expr],
    stats: Option<&TableStatistics>,
) -> (Vec<&'a Expr>, f64) {
    let mut bounds: BTreeMap<String, ColBounds> = BTreeMap::new();
    let mut remaining = Vec::new();
    for &conjunct in conjuncts {
        if !bucket_range_bound(conjunct, stats, &mut bounds) {
            remaining.push(conjunct);
        }
    }
    let (fraction, unpaired) = paired_interval_fraction(&bounds);
    remaining.extend(unpaired);
    (remaining, fraction)
}

/// File one range conjunct under its column when the column's bounds and the
/// literal all sit on one ordinal scale; `false` to leave the conjunct for the
/// ordinary per-conjunct path.
fn bucket_range_bound<'a>(
    conjunct: &'a Expr,
    stats: Option<&TableStatistics>,
    bounds: &mut BTreeMap<String, ColBounds<'a>>,
) -> bool {
    let Expr::BinaryOp { op, left, right } = conjunct else {
        return false;
    };
    if !matches!(
        op,
        BinaryOpType::Lt | BinaryOpType::Lte | BinaryOpType::Gt | BinaryOpType::Gte
    ) {
        return false;
    }
    let (col_ref, value, below) = range_parts(*op, left, right);
    let Some(col_stats) = column_stats_or_none(stats, col_ref) else {
        return false;
    };
    let point = value.as_ref().and_then(ordinal_of_literal);
    let (Some(point), Some((low, high))) = (point, ordinal_stats(col_stats)) else {
        return false;
    };
    let column = col_ref.expect("column_stats_or_none returned Some, so col_ref is Some");
    let entry = bounds.entry(column.column.clone()).or_insert(ColBounds {
        below: Vec::new(),
        above: Vec::new(),
        low,
        high,
    });
    if below {
        entry.below.push((point, conjunct));
    } else {
        entry.above.push((point, conjunct));
    }
    true
}

/// One combined fraction over every column with BOTH a lower and an upper bound;
/// single-direction bounds are handed back for per-conjunct pricing.
fn paired_interval_fraction<'a>(bounds: &BTreeMap<String, ColBounds<'a>>) -> (f64, Vec<&'a Expr>) {
    let mut fraction = 1.0;
    let mut unpaired = Vec::new();
    for entry in bounds.values() {
        if !entry.below.is_empty() && !entry.above.is_empty() {
            fraction *= interval_fraction(entry);
        } else {
            for (_point, conjunct) in entry.below.iter().chain(entry.above.iter()) {
                unpaired.push(*conjunct);
            }
        }
    }
    (fraction, unpaired)
}

/// `F(tightest upper) - F(tightest lower)` on the column's ordinal scale, clamped
/// to `[0, 1]` (inverted bounds select nothing).
fn interval_fraction(entry: &ColBounds) -> f64 {
    let span = entry.high - entry.low;
    if span <= 0.0 {
        return 1.0;
    }
    let upper = entry
        .below
        .iter()
        .map(|(point, _)| *point)
        .fold(f64::INFINITY, f64::min);
    let lower = entry
        .above
        .iter()
        .map(|(point, _)| *point)
        .fold(f64::NEG_INFINITY, f64::max);
    let upper_fraction = clamp01((upper - entry.low) / span);
    let lower_fraction = clamp01((lower - entry.low) / span);
    (upper_fraction - lower_fraction).max(0.0)
}

/// Clamp a fraction to `[0, 1]`.
fn clamp01(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_ordinal_is_the_value() {
        assert_eq!(ordinal(&StatValue::Integer(5)), Some(5.0));
        assert_eq!(ordinal(&StatValue::Boolean(true)), None);
    }

    #[test]
    fn text_date_bounds_parse_to_days() {
        let a = ordinal(&StatValue::Text("1994-01-01".into())).unwrap();
        let b = ordinal(&StatValue::Text("1995-01-01".into())).unwrap();
        // 1994 was not a leap year: exactly 365 days apart.
        assert!((b - a - 365.0).abs() < 1e-9);
    }

    #[test]
    fn timestamp_carries_fractional_day() {
        let midnight = ordinal(&StatValue::Text("1994-01-01 00:00:00".into())).unwrap();
        let midday = ordinal(&StatValue::Text("1994-01-01 12:00:00".into())).unwrap();
        assert!((midday - midnight - 0.5).abs() < 1e-9);
    }

    #[test]
    fn timestamp_accepts_fractional_seconds_and_offsets() {
        // Python `datetime.fromisoformat` parses these; a narrower parser would
        // drop the bound to None and lose all range selectivity for timestamps.
        let base = ordinal(&StatValue::Text("1994-01-01 12:00:00".into())).unwrap();
        // Fractional seconds parse (and are ignored, keeping only h:m:s).
        let frac = ordinal(&StatValue::Text("1994-01-01 12:00:00.123456".into())).unwrap();
        assert!((frac - base).abs() < 1e-9);
        // `T` separator with fractional seconds.
        let tee = ordinal(&StatValue::Text("1994-01-01T12:00:00.5".into())).unwrap();
        assert!((tee - base).abs() < 1e-9);
        // A UTC offset parses (the wall-clock components set the ordinal).
        let offset = ordinal(&StatValue::Text("1994-01-01T12:00:00+00:00".into())).unwrap();
        assert!((offset - base).abs() < 1e-9);
        let zulu = ordinal(&StatValue::Text("1994-01-01T12:00:00Z".into())).unwrap();
        assert!((zulu - base).abs() < 1e-9);
    }

    #[test]
    fn interpolate_orients_by_direction() {
        // point at 25% of [0, 100]; below keeps 0.25, above keeps 0.75.
        assert_eq!(
            interpolate_fraction(Some(0.0), Some(100.0), Some(25.0), true),
            Some(0.25)
        );
        assert_eq!(
            interpolate_fraction(Some(0.0), Some(100.0), Some(25.0), false),
            Some(0.75)
        );
        // Degenerate span -> None.
        assert_eq!(
            interpolate_fraction(Some(5.0), Some(5.0), Some(5.0), true),
            None
        );
    }
}
