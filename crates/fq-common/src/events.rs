//! Event-analytics statement value types and shared calendar arithmetic.
//!
//! These are the typed forms `fq-parse` produces for the event statement
//! family (CREATE/REFRESH/REBUILD/DROP/SHOW EVENT DATASET and the four
//! analysis statements FUNNEL / RETENTION / SEGMENT / PATHS) and `fq-runtime`
//! dispatches on. They live in the leaf crate so the parser does not depend on
//! the event engine and the engine does not depend on the parser.
//!
//! The calendar helpers (civil date <-> day arithmetic, UTC bucketing) live
//! here too because both the statement parser (timestamp literals) and the
//! analysis kernels (bucket labels) need the same exact arithmetic.

use serde::{Deserialize, Serialize};

/// Where an event dataset's rows come from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventSource {
    /// `FROM <datasource>.<schema>.<table>`: a single source table.
    Table {
        datasource: String,
        schema: String,
        table: String,
    },
    /// `AS ( <select sql> )`: an arbitrary defining SELECT, stored verbatim.
    Select { sql: String },
}

/// The declared unit of the source time column. Nanoseconds are rejected at
/// build (truncating them silently would violate full precision).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeUnit {
    Micros,
    Millis,
}

/// `CREATE EVENT DATASET` in typed form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventDatasetDef {
    pub name: String,
    pub source: EventSource,
    pub actor_column: String,
    pub time_column: String,
    pub event_column: String,
    /// The declared tiebreak column; None means the synthetic ingest ordinal.
    pub tiebreak_column: Option<String>,
    /// The declared property columns. None means every source column not named
    /// as actor/time/event/tiebreak; Some(empty) means no properties.
    pub properties: Option<Vec<String>>,
    /// `WITH (shards = ...)`: overrides the derived shard count.
    pub shards: Option<u32>,
    /// `WITH (refresh_key = ...)`: the monotone-arrival column enabling
    /// incremental REFRESH.
    pub refresh_key: Option<String>,
    /// `WITH (time_unit = ...)`; defaults to microseconds.
    pub time_unit: TimeUnit,
}

/// A half-open `[from, to)` event-time interval in UTC microseconds. Absent
/// bounds default to the dataset's min/max event time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TimeRange {
    pub from: Option<i64>,
    pub to: Option<i64>,
}

/// A comparison operator of the event predicate grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

/// A literal of the event predicate grammar.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PredicateLiteral {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// The closed `WHERE` predicate language over declared properties.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EventPredicate {
    And(Vec<EventPredicate>),
    Or(Vec<EventPredicate>),
    Not(Box<EventPredicate>),
    Compare {
        property: String,
        op: CompareOp,
        literal: PredicateLiteral,
    },
    InList {
        property: String,
        literals: Vec<PredicateLiteral>,
    },
    IsNull {
        property: String,
        negated: bool,
    },
}

/// Funnel matching mode. `AnyOrder` is recognized by the grammar and rejected
/// by the engine (`EventUnsupported`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunnelMode {
    StrictOrder,
    AnyOrder,
}

/// `FUNNEL (...) ON <dataset>` in typed form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunnelSpec {
    pub dataset: String,
    /// The ordered step event names; the grammar requires at least two and the
    /// engine bounds the count at 32.
    pub steps: Vec<String>,
    pub window_micros: i64,
    pub mode: FunnelMode,
    pub range: TimeRange,
    pub filter: Option<EventPredicate>,
    pub breakdown: Option<String>,
}

/// The calendar grain of a retention cohort period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeriodGrain {
    Day,
    Week,
    Month,
}

/// Bounded = retained in EXACTLY period n; Unbounded = in period n or later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionMode {
    Bounded,
    Unbounded,
}

/// A birth/return event specification: any event, or one named event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventMatch {
    Any,
    Named(String),
}

/// `RETENTION ON <dataset>` in typed form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetentionSpec {
    pub dataset: String,
    pub birth: EventMatch,
    pub return_event: EventMatch,
    pub period: PeriodGrain,
    pub mode: RetentionMode,
    /// `PERIODS <n>`: the largest reported offset; None reports through the
    /// dataset's max event time.
    pub periods: Option<u32>,
    /// `AT <n>`: return only the rows at this one offset.
    pub at: Option<u32>,
    pub range: TimeRange,
    pub filter: Option<EventPredicate>,
    pub breakdown: Option<String>,
}

/// The segmentation measure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentMeasure {
    Count,
    Uniques,
    CountPerUnique,
}

/// The time-bucket grain of a segmentation query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketGrain {
    Hour,
    Day,
    Week,
    Month,
}

/// `SEGMENT <measure> ON <dataset>` in typed form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentSpec {
    pub dataset: String,
    pub measure: SegmentMeasure,
    /// `EVENT '<name>'`: restrict to one event name; None means all events.
    pub event: Option<String>,
    pub bucket: BucketGrain,
    pub range: TimeRange,
    pub filter: Option<EventPredicate>,
    pub breakdown: Option<String>,
}

/// The path anchor: forward from an event, backward into an event, or every
/// position (unanchored).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathAnchor {
    Unanchored,
    StartingAt(String),
    EndingAt(String),
}

/// `PATHS ON <dataset>` in typed form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathsSpec {
    pub dataset: String,
    pub anchor: PathAnchor,
    /// Window length in events; the engine bounds it to 2..=10.
    pub depth: u32,
    /// How many top paths to return; the engine bounds it to 1..=1000.
    pub top: u32,
    /// `MAXGAP <interval>`: a larger gap between consecutive path-stream
    /// events breaks the stream; None means no breaking.
    pub maxgap_micros: Option<i64>,
    pub range: TimeRange,
    pub filter: Option<EventPredicate>,
}

/// Engine settings for the event-analytics store (`events:` config section).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct EventsConfig {
    /// Peak build memory budget in bytes; the build's shard sizing and spill
    /// buffers derive from it, so peak RSS is independent of total row count.
    pub build_memory_bytes: u64,
    /// Build worker threads; 0 means all cores.
    pub build_threads: usize,
    /// Raw columnar bytes targeted per shard when deriving the shard count.
    pub target_shard_bytes: u64,
    /// Per-property build-dictionary budget; a property whose value map
    /// exceeds it is promoted to raw-string encoding.
    pub dict_max_bytes: u64,
    /// Decoded-block cache budget for warm queries.
    pub cache_bytes: u64,
    /// Per-query group/partial memory budget.
    pub query_memory_bytes: u64,
    /// Generation count per shard past which the next refresh compacts it.
    pub max_generations: u64,
    /// Whether an unknown event NAME in a statement matches nothing instead of
    /// raising `EventUnknownName`.
    pub allow_unknown_names: bool,
}

impl Default for EventsConfig {
    /// The measured defaults: a 6 GiB build ceiling, 256 MiB shard/dictionary
    /// grains, an 8 GiB warm cache, and a 4 GiB query budget.
    fn default() -> Self {
        Self {
            build_memory_bytes: 6 * 1024 * 1024 * 1024,
            build_threads: 0,
            target_shard_bytes: 256 * 1024 * 1024,
            dict_max_bytes: 256 * 1024 * 1024,
            cache_bytes: 8 * 1024 * 1024 * 1024,
            query_memory_bytes: 4 * 1024 * 1024 * 1024,
            max_generations: 8,
            allow_unknown_names: false,
        }
    }
}

/// Days since 1970-01-01 of a proleptic-Gregorian civil date (the standard
/// days-from-civil algorithm; exact over the full i64 timestamp range).
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let adjusted_year = if month <= 2 { year - 1 } else { year };
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let month_index = i64::from(if month > 2 { month - 3 } else { month + 9 });
    let day_of_year = (153 * month_index + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Civil (year, month, day) of a days-since-epoch value (inverse of
/// `days_from_civil`).
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    #[allow(clippy::cast_possible_truncation)]
    let month = month as u32;
    #[allow(clippy::cast_possible_truncation)]
    let day = day as u32;
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

/// Microseconds per UTC day.
pub const MICROS_PER_DAY: i64 = 86_400_000_000;

/// The UTC calendar day (days since epoch) containing a microsecond timestamp.
pub fn day_of_micros(micros: i64) -> i64 {
    micros.div_euclid(MICROS_PER_DAY)
}

/// The start (days since epoch) of the ISO Monday-based week containing a
/// day-since-epoch value. Day 0 (1970-01-01) was a Thursday, so day + 3 is
/// Monday-aligned.
pub fn week_start_of_day(day: i64) -> i64 {
    day - (day + 3).rem_euclid(7)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_round_trips_through_days() {
        for (year, month, day) in [
            (1970, 1, 1),
            (2025, 1, 1),
            (2025, 1, 31),
            (2000, 2, 29),
            (1969, 12, 31),
            (1900, 3, 1),
            (2400, 2, 29),
        ] {
            let days = days_from_civil(year, month, day);
            assert_eq!(civil_from_days(days), (year, month, day));
        }
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2025, 1, 1), 20_089);
    }

    #[test]
    fn week_start_is_monday_aligned() {
        // 2025-01-01 is a Wednesday; its ISO week starts Monday 2024-12-30.
        let wednesday = days_from_civil(2025, 1, 1);
        let monday = days_from_civil(2024, 12, 30);
        assert_eq!(week_start_of_day(wednesday), monday);
        assert_eq!(week_start_of_day(monday), monday);
        // A Sunday belongs to the week of the preceding Monday.
        let sunday = days_from_civil(2025, 1, 5);
        assert_eq!(week_start_of_day(sunday), monday);
    }

    #[test]
    fn negative_micros_floor_to_the_previous_day() {
        assert_eq!(day_of_micros(-1), -1);
        assert_eq!(day_of_micros(0), 0);
        assert_eq!(day_of_micros(MICROS_PER_DAY), 1);
    }
}
