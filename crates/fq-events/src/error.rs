//! The event-store error surface: one umbrella `EventError` over the typed
//! component errors every module raises. Nothing is stringly caught and
//! rewrapped; each variant carries the specifics its message needs, and a
//! contract violation always raises instead of degrading silently.

use thiserror::Error;

/// A source-schema violation detected before any row is processed: a missing
/// or mistyped actor/time/event/tiebreak/property column, or a source whose
/// shape drifted from the declared dataset schema.
#[derive(Debug, Error)]
pub enum EventSchemaError {
    #[error("source column '{column}' is missing from the source result")]
    MissingColumn { column: String },
    #[error(
        "source column '{column}' has type {found}, which cannot serve as {role}; \
         supported are {supported}"
    )]
    ColumnType {
        column: String,
        found: String,
        role: String,
        supported: String,
    },
    #[error(
        "source schema drifted: dataset '{dataset}' declares {declared} but the \
         source now produces {found}"
    )]
    Drift {
        dataset: String,
        declared: String,
        found: String,
    },
}

/// A build-time data contract violation. Every one names the offending row or
/// value; an event stream with a null subject, time, or name is invalid input,
/// never silently dropped.
#[derive(Debug, Error)]
pub enum EventBuildError {
    #[error("row {row} has a NULL actor id; an event without a subject is invalid")]
    NullActor { row: u64 },
    #[error("row {row} has a NULL event time")]
    NullTime { row: u64 },
    #[error("row {row} has a NULL event name")]
    NullEventName { row: u64 },
    #[error(
        "actor {actor_key} has two events at ts {ts} with equal tiebreak; ordering \
         between them is undefined - declare a discriminating TIEBREAK column"
    )]
    AmbiguousOrder { actor_key: String, ts: i64 },
    #[error(
        "the event-name dictionary exceeded its {budget} byte budget at {count} \
         distinct names; the event column cannot be promoted to raw strings"
    )]
    EventNameCardinality { budget: u64, count: u64 },
    #[error("shard {shard} holds more actors than this platform can index")]
    ShardOverflow { shard: u32 },
    #[error(
        "the paths index text holds {positions} positions, over the 32-bit \
         suffix-array bound of 2147483647"
    )]
    SuffixTextTooLong { positions: usize },
    #[error("libsais suffix construction failed with native code {code}")]
    SuffixSort { code: i32 },
    #[error("build io failure: {0}")]
    Io(String),
}

/// A store-format failure: a corrupt or torn file, a checksum mismatch, an
/// unknown format version, or a registry/file inconsistency. Never a partial
/// or silent read.
#[derive(Debug, Error)]
pub enum EventStoreError {
    #[error("event store corruption in '{file}': {detail}")]
    Corrupt { file: String, detail: String },
    #[error("event store io failure on '{file}': {detail}")]
    Io { file: String, detail: String },
    #[error("event dataset registry failure: {0}")]
    Registry(String),
    #[error("event dataset '{name}' does not exist")]
    UnknownDataset { name: String },
    #[error("relation or dataset '{name}' already exists")]
    NameTaken { name: String },
}

/// An invalid analysis statement: a reference the dataset's schema cannot
/// resolve, or a statement bound the grammar admits but the engine rejects.
/// An invalid query MUST raise; returning rows for it would manufacture a
/// wrong answer.
#[derive(Debug, Error)]
pub enum EventBindingError {
    #[error("dataset '{dataset}' has no property '{property}'; declared properties are {known}")]
    UnknownProperty {
        dataset: String,
        property: String,
        known: String,
    },
    #[error(
        "property '{property}' has type {data_type}, which cannot compare with a \
         {literal} literal"
    )]
    LiteralType {
        property: String,
        data_type: String,
        literal: String,
    },
    #[error("FUNNEL takes 2..=32 steps, got {got}")]
    StepCount { got: usize },
    #[error("PATHS DEPTH must be at least 2, got {got}")]
    PathDepth { got: u32 },
    #[error("PATHS TOP must be in 1..=1000, got {got}")]
    PathTop { got: u32 },
    #[error("FUNNEL WINDOW must be positive, got {got} microseconds")]
    Window { got: i64 },
    #[error("RETENTION AT {at} exceeds PERIODS {periods}")]
    AtBeyondPeriods { at: u32, periods: u32 },
    #[error("{clause} FROM/TO range is empty: from {from} is not before to {to}")]
    EmptyRange { clause: String, from: i64, to: i64 },
}

/// A statement names an event that does not exist in the dataset's dictionary.
/// A typo'd event name silently matching nothing is the classic silent wrong
/// funnel, so this raises by default; `events.allow_unknown_names` turns it
/// into an empty match for pre-registered pipelines.
#[derive(Debug, Error)]
#[error(
    "event name '{name}' does not exist in dataset '{dataset}'; nearest names are \
     {nearest}. Set events.allow_unknown_names = true to let unknown names match \
     nothing"
)]
pub struct EventUnknownName {
    pub dataset: String,
    pub name: String,
    pub nearest: String,
}

/// A refresh the dataset's declaration does not admit.
#[derive(Debug, Error)]
pub enum EventRefreshError {
    #[error(
        "dataset '{name}' has no refresh_key, so REFRESH cannot pull a delta; \
         use REBUILD EVENT DATASET {name}"
    )]
    NoRefreshKey { name: String },
    #[error(
        "dataset '{name}' is defined by a SELECT, so REFRESH cannot pull a delta; \
         use REBUILD EVENT DATASET {name}"
    )]
    SelectSource { name: String },
    #[error("refresh watermark failure for dataset '{name}': {detail}")]
    Watermark { name: String, detail: String },
}

/// A source value type the event store does not accept.
#[derive(Debug, Error)]
pub enum EventTypeError {
    #[error(
        "time column '{column}' carries nanosecond timestamps; the store holds \
         microseconds and will not truncate silently - cast the source to \
         microseconds or milliseconds"
    )]
    NanosecondTime { column: String },
    #[error("TIEBREAK column '{column}' has type {found}; a tiebreak must be an integer column")]
    TiebreakType { column: String, found: String },
    #[error("property '{column}' has type {found}, which the event store does not hold")]
    PropertyType { column: String, found: String },
}

/// A statement form the grammar admits but this engine build does not execute.
#[derive(Debug, Error)]
pub enum EventUnsupported {
    #[error("FUNNEL MODE ANY_ORDER is not supported; only STRICT_ORDER executes")]
    AnyOrderFunnel,
    #[error(
        "the {analysis} group state exceeded events.query_memory_bytes \
         ({budget} bytes); raise the setting to run this breakdown"
    )]
    QueryMemory { analysis: String, budget: u64 },
    #[error(
        "the funnel duration partials exceeded events.query_memory_bytes \
         ({budget} bytes); raise the setting to run this funnel"
    )]
    DurationMemory { budget: u64 },
}

/// The fq-events umbrella error.
#[derive(Debug, Error)]
pub enum EventError {
    #[error("event schema error: {0}")]
    Schema(#[from] EventSchemaError),
    #[error("event build error: {0}")]
    Build(#[from] EventBuildError),
    #[error("event store error: {0}")]
    Store(#[from] EventStoreError),
    #[error("event binding error: {0}")]
    Binding(#[from] EventBindingError),
    #[error("{0}")]
    UnknownName(#[from] EventUnknownName),
    #[error("event refresh error: {0}")]
    Refresh(#[from] EventRefreshError),
    #[error("event type error: {0}")]
    Type(#[from] EventTypeError),
    #[error("event unsupported: {0}")]
    Unsupported(#[from] EventUnsupported),
}
