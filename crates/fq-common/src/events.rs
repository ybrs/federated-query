//! Event-analytics spec types: the contract between the statement surface
//! (fq-parse, which produces them) and the analysis kernels (fq-events, which
//! interpret them). Pure data - this module keeps the two crates decoupled
//! (neither depends on the other; both depend here).

/// The role columns of an event view, each naming an output column of the
/// view's defining SELECT. Every output column not named by a role is a
/// property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRoleColumns {
    /// The entity id column (who did it): INTEGER / BIGINT / TEXT-like.
    pub entity: String,
    /// The event-time column (when): TIMESTAMP (any unit) / DATE.
    pub timestamp: String,
    /// The event-name column (what happened): TEXT-like.
    pub event: String,
    /// The optional tiebreak column: orders events that share an (entity,
    /// timestamp) pair. Orderable exact types only (integer / text / date /
    /// timestamp); None means ties order by event name in the paths kernel.
    pub tiebreak: Option<String>,
}

/// A parsed `FUNNEL OVER <view> STEPS (...) WITHIN <n> <unit>` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunnelSpec {
    /// The event view the funnel runs over.
    pub view: String,
    /// The ordered step event names, 2..=16 of them.
    pub steps: Vec<String>,
    /// The conversion window, anchored at each attempt's step-1 event.
    pub within: EventWindow,
}

/// A duration for funnel windows: a positive count of one calendar-free unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventWindow {
    pub count: i64,
    pub unit: WindowUnit,
}

/// The units a window duration can carry. All are fixed-length (no months),
/// so window arithmetic is exact integer math in the timestamp's native unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl WindowUnit {
    /// The unit's length in whole seconds.
    pub fn seconds(self) -> i64 {
        match self {
            WindowUnit::Seconds => 1,
            WindowUnit::Minutes => 60,
            WindowUnit::Hours => 3600,
            WindowUnit::Days => 86_400,
        }
    }
}

/// A parsed `SEGMENT OVER <view> MEASURE <m> [EVENT '<name>'] BY <bucket>`
/// statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSpec {
    /// The event view the segmentation runs over.
    pub view: String,
    /// What each bucket's value counts.
    pub measure: SegmentMeasure,
    /// An optional exact event-name filter applied before measuring.
    pub event: Option<String>,
    /// The UTC calendar bucket events are grouped into.
    pub bucket: TimeBucket,
}

/// A parsed `PATHS OVER <view> [STARTING AT '<name>'] MAX DEPTH <n> TOP <k>`
/// statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathsSpec {
    /// The event view the path analysis runs over.
    pub view: String,
    /// The optional anchor event: each entity's path starts at its first
    /// event with this name; None starts at the entity's first event.
    pub starting_at: Option<String>,
    /// The maximum number of steps a path keeps (collapsed steps, not raw
    /// events); always positive.
    pub max_depth: i64,
    /// How many of the most common paths the result returns; always positive.
    pub top: i64,
}

/// The segmentation measure: raw event count, or distinct entities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentMeasure {
    Events,
    Entities,
}

/// A UTC calendar truncation for segmentation buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeBucket {
    Hour,
    Day,
    /// ISO week: Monday 00:00 UTC.
    Week,
    /// First of the calendar month, 00:00 UTC.
    Month,
}
