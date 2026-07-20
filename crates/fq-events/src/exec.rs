//! The four analysis executors - funnel, retention, segmentation, paths -
//! plus predicate compilation, name binding, and partial merging.
//!
//! Execution is one worker per shard (bounded by cores) over a shared work
//! index; each worker streams its shard's actors through the generation-
//! merged cursor and partials merge at the end (sums, count maps, duration
//! arrays). Distinct-actor counts sum across shards because shards partition
//! actors. Every fast path here is an algebraic rewrite of the scan and is
//! oracle-checked by the events benchmark battery.

use std::collections::HashMap;

use crate::model::FastMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use fq_common::events::{
    civil_from_days, day_of_micros, days_from_civil, week_start_of_day, BucketGrain, CompareOp,
    EventMatch, EventPredicate, EventsConfig, FunnelMode, FunnelSpec, PathAnchor, PathsSpec,
    PeriodGrain, PredicateLiteral, RetentionMode, RetentionSpec, SegmentMeasure, SegmentSpec,
    TimeRange, MICROS_PER_DAY,
};

use crate::cursor::{prop_value, FileScan, PropRef, ShardScan};
use crate::error::{
    EventBindingError, EventError, EventStoreError, EventUnknownName, EventUnsupported,
};
use crate::model::PropertyEncoding;
use crate::store::{CachedItem, SegFile, StoreIo};
use crate::Dataset;

// ---------------------------------------------------------------------------
// Result relations (pinned shapes; the runtime maps them onto Arrow)
// ---------------------------------------------------------------------------

/// One funnel result row.
#[derive(Debug, Clone, PartialEq)]
pub struct FunnelRow {
    pub breakdown: Option<String>,
    pub step_index: u32,
    pub step_name: String,
    pub entered: u64,
    pub conversion_from_previous: f64,
    pub conversion_from_start: f64,
    pub median_seconds_from_previous: Option<f64>,
    pub p90_seconds_from_previous: Option<f64>,
}

/// The funnel result: rows ordered by (breakdown, step_index); the breakdown
/// column exists only when the statement has the clause.
#[derive(Debug, Clone, PartialEq)]
pub struct FunnelResult {
    pub has_breakdown: bool,
    pub rows: Vec<FunnelRow>,
}

/// One retention grid row.
#[derive(Debug, Clone, PartialEq)]
pub struct RetentionRow {
    pub breakdown: Option<String>,
    /// The cohort period's start instant, UTC microseconds.
    pub cohort_start: i64,
    pub cohort_size: u64,
    pub period_offset: u32,
    pub retained: u64,
    /// None when the cohort is empty.
    pub retention_rate: Option<f64>,
}

/// The retention result, ordered by (breakdown, cohort_start, period_offset).
#[derive(Debug, Clone, PartialEq)]
pub struct RetentionResult {
    pub has_breakdown: bool,
    pub rows: Vec<RetentionRow>,
}

/// One segmentation row.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentRow {
    /// The bucket's start instant, UTC microseconds.
    pub bucket: i64,
    pub breakdown: Option<String>,
    pub count: u64,
    pub uniques: u64,
}

/// The segmentation result, ordered by (breakdown, bucket).
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentResult {
    pub has_breakdown: bool,
    pub measure: SegmentMeasure,
    pub rows: Vec<SegmentRow>,
}

/// One paths row.
#[derive(Debug, Clone, PartialEq)]
pub struct PathsRow {
    pub rank: u32,
    /// Names joined with " -> ".
    pub path: String,
    pub occurrences: u64,
    pub share: f64,
}

/// The paths result, ordered by rank.
#[derive(Debug, Clone, PartialEq)]
pub struct PathsResult {
    pub rows: Vec<PathsRow>,
}

// ---------------------------------------------------------------------------
// Name binding
// ---------------------------------------------------------------------------

/// Resolve an event name to its dictionary code. An unknown name raises
/// `EventUnknownName` with the three nearest names by edit distance, unless
/// `events.allow_unknown_names` turns it into a never-matching sentinel.
fn resolve_event_code(
    dataset: &Dataset,
    name: &str,
    cfg: &EventsConfig,
) -> Result<u64, EventError> {
    if let Some(code) = dataset.event_dict.map.get(name) {
        return Ok(*code);
    }
    if cfg.allow_unknown_names {
        return Ok(NO_CODE);
    }
    let mut scored: Vec<(usize, &String)> = dataset
        .event_dict
        .values
        .iter()
        .map(|value| (levenshtein(name, value), value))
        .collect();
    scored.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    let nearest: Vec<String> = scored
        .iter()
        .take(3)
        .map(|(_, value)| format!("'{value}'"))
        .collect();
    Err(EventUnknownName {
        dataset: dataset.record.name.clone(),
        name: name.to_string(),
        nearest: if nearest.is_empty() {
            "(the dataset has no events)".to_string()
        } else {
            nearest.join(", ")
        },
    }
    .into())
}

/// A code no real event carries (dictionary codes are dense from 0).
const NO_CODE: u64 = u64::MAX;

/// Levenshtein edit distance (for the unknown-name suggestion list).
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let substitution = previous[j] + usize::from(ca != cb);
            current[j + 1] = substitution.min(previous[j + 1] + 1).min(current[j] + 1);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[b.len()]
}

/// Resolve a property name to its index, or raise listing the declared set.
fn resolve_property(dataset: &Dataset, property: &str) -> Result<usize, EventBindingError> {
    dataset
        .record
        .properties
        .iter()
        .position(|def| def.name == property)
        .ok_or_else(|| EventBindingError::UnknownProperty {
            dataset: dataset.record.name.clone(),
            property: property.to_string(),
            known: dataset
                .record
                .properties
                .iter()
                .map(|def| def.name.clone())
                .collect::<Vec<_>>()
                .join(", "),
        })
}

/// Validate a FROM/TO range: an empty declared range is an invalid query.
fn validate_range(clause: &str, range: TimeRange) -> Result<(), EventBindingError> {
    if let (Some(from), Some(to)) = (range.from, range.to) {
        if from >= to {
            return Err(EventBindingError::EmptyRange {
                clause: clause.to_string(),
                from,
                to,
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Predicate compilation
// ---------------------------------------------------------------------------

/// A compiled event predicate: property lookups resolved to indices, literal
/// values pre-typed against the property schema, dictionary equality
/// pre-resolved to codes. Null property values fail every comparison except
/// IS NULL.
enum Compiled {
    And(Vec<Compiled>),
    Or(Vec<Compiled>),
    Not(Box<Compiled>),
    IsNull {
        prop: usize,
        negated: bool,
    },
    /// Equality against a dict property whose value exists: code compare.
    CodeEq {
        prop: usize,
        code: u64,
        negated: bool,
    },
    /// Equality against a dict property value absent from the dictionary:
    /// matches nothing (or, negated, every non-null value).
    AbsentEq {
        prop: usize,
        negated: bool,
    },
    /// An ordering comparison on a string property (dict values decode to
    /// strings for the compare).
    StrCmp {
        prop: usize,
        op: CompareOp,
        value: String,
    },
    /// A membership test on a dict property: the sorted resolved code set.
    CodeIn {
        prop: usize,
        codes: Vec<u64>,
    },
    /// A membership test on a raw-string property.
    StrIn {
        prop: usize,
        values: Vec<String>,
    },
    NumCmp {
        prop: usize,
        op: CompareOp,
        value: f64,
    },
    NumIn {
        prop: usize,
        values: Vec<f64>,
    },
    BoolCmp {
        prop: usize,
        op: CompareOp,
        value: bool,
    },
}

/// Compile a parsed predicate against the dataset's property schema. Every
/// unknown property or mistyped literal raises before any data is touched.
fn compile_predicate(
    dataset: &Dataset,
    predicate: &EventPredicate,
) -> Result<Compiled, EventError> {
    match predicate {
        EventPredicate::And(terms) => Ok(Compiled::And(
            terms
                .iter()
                .map(|term| compile_predicate(dataset, term))
                .collect::<Result<_, _>>()?,
        )),
        EventPredicate::Or(terms) => Ok(Compiled::Or(
            terms
                .iter()
                .map(|term| compile_predicate(dataset, term))
                .collect::<Result<_, _>>()?,
        )),
        EventPredicate::Not(inner) => {
            Ok(Compiled::Not(Box::new(compile_predicate(dataset, inner)?)))
        }
        EventPredicate::IsNull { property, negated } => Ok(Compiled::IsNull {
            prop: resolve_property(dataset, property)?,
            negated: *negated,
        }),
        EventPredicate::Compare {
            property,
            op,
            literal,
        } => compile_compare(dataset, property, *op, literal),
        EventPredicate::InList { property, literals } => {
            compile_in_list(dataset, property, literals)
        }
    }
}

/// The literal kind name for a type-mismatch message.
fn literal_kind(literal: &PredicateLiteral) -> &'static str {
    match literal {
        PredicateLiteral::Str(_) => "string",
        PredicateLiteral::Int(_) | PredicateLiteral::Float(_) => "numeric",
        PredicateLiteral::Bool(_) => "boolean",
    }
}

/// Compile one comparison against the property's declared type.
fn compile_compare(
    dataset: &Dataset,
    property: &str,
    op: CompareOp,
    literal: &PredicateLiteral,
) -> Result<Compiled, EventError> {
    let prop = resolve_property(dataset, property)?;
    let def = &dataset.record.properties[prop];
    let mismatch = || {
        EventError::Binding(EventBindingError::LiteralType {
            property: property.to_string(),
            data_type: def.data_type.value().to_string(),
            literal: literal_kind(literal).to_string(),
        })
    };
    match (def.encoding, literal) {
        (PropertyEncoding::Dict, PredicateLiteral::Str(value)) => match op {
            CompareOp::Eq | CompareOp::Neq => {
                let negated = op == CompareOp::Neq;
                match dataset.prop_dicts[prop]
                    .as_ref()
                    .and_then(|d| d.map.get(value))
                {
                    Some(code) => Ok(Compiled::CodeEq {
                        prop,
                        code: *code,
                        negated,
                    }),
                    None => Ok(Compiled::AbsentEq { prop, negated }),
                }
            }
            CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
                Ok(Compiled::StrCmp {
                    prop,
                    op,
                    value: value.clone(),
                })
            }
        },
        (PropertyEncoding::RawString, PredicateLiteral::Str(value)) => Ok(Compiled::StrCmp {
            prop,
            op,
            value: value.clone(),
        }),
        (PropertyEncoding::I64 | PropertyEncoding::F64, PredicateLiteral::Int(value)) => {
            Ok(Compiled::NumCmp {
                prop,
                op,
                value: *value as f64,
            })
        }
        (PropertyEncoding::I64 | PropertyEncoding::F64, PredicateLiteral::Float(value)) => {
            Ok(Compiled::NumCmp {
                prop,
                op,
                value: *value,
            })
        }
        (PropertyEncoding::Bool, PredicateLiteral::Bool(value)) => match op {
            CompareOp::Eq | CompareOp::Neq => Ok(Compiled::BoolCmp {
                prop,
                op,
                value: *value,
            }),
            CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => Err(mismatch()),
        },
        (
            PropertyEncoding::Dict | PropertyEncoding::RawString,
            PredicateLiteral::Int(_) | PredicateLiteral::Float(_) | PredicateLiteral::Bool(_),
        )
        | (
            PropertyEncoding::I64 | PropertyEncoding::F64,
            PredicateLiteral::Str(_) | PredicateLiteral::Bool(_),
        )
        | (
            PropertyEncoding::Bool,
            PredicateLiteral::Str(_) | PredicateLiteral::Int(_) | PredicateLiteral::Float(_),
        ) => Err(mismatch()),
    }
}

/// Compile one IN list against the property's declared type.
fn compile_in_list(
    dataset: &Dataset,
    property: &str,
    literals: &[PredicateLiteral],
) -> Result<Compiled, EventError> {
    let prop = resolve_property(dataset, property)?;
    let def = &dataset.record.properties[prop];
    match def.encoding {
        PropertyEncoding::Dict => {
            let mut codes = Vec::new();
            for literal in literals {
                let PredicateLiteral::Str(value) = literal else {
                    return Err(EventBindingError::LiteralType {
                        property: property.to_string(),
                        data_type: def.data_type.value().to_string(),
                        literal: literal_kind(literal).to_string(),
                    }
                    .into());
                };
                // A value absent from the dictionary is data, not schema:
                // it just matches nothing.
                if let Some(code) = dataset.prop_dicts[prop]
                    .as_ref()
                    .and_then(|d| d.map.get(value))
                {
                    codes.push(*code);
                }
            }
            codes.sort_unstable();
            Ok(Compiled::CodeIn { prop, codes })
        }
        PropertyEncoding::RawString => {
            let mut values = Vec::new();
            for literal in literals {
                let PredicateLiteral::Str(value) = literal else {
                    return Err(EventBindingError::LiteralType {
                        property: property.to_string(),
                        data_type: def.data_type.value().to_string(),
                        literal: literal_kind(literal).to_string(),
                    }
                    .into());
                };
                values.push(value.clone());
            }
            Ok(Compiled::StrIn { prop, values })
        }
        PropertyEncoding::I64 | PropertyEncoding::F64 => {
            let mut values = Vec::new();
            for literal in literals {
                match literal {
                    PredicateLiteral::Int(value) => values.push(*value as f64),
                    PredicateLiteral::Float(value) => values.push(*value),
                    PredicateLiteral::Str(_) | PredicateLiteral::Bool(_) => {
                        return Err(EventBindingError::LiteralType {
                            property: property.to_string(),
                            data_type: def.data_type.value().to_string(),
                            literal: literal_kind(literal).to_string(),
                        }
                        .into())
                    }
                }
            }
            Ok(Compiled::NumIn { prop, values })
        }
        PropertyEncoding::Bool => Err(EventBindingError::LiteralType {
            property: property.to_string(),
            data_type: def.data_type.value().to_string(),
            literal: "list".to_string(),
        }
        .into()),
    }
}

/// Evaluate a compiled predicate for one event.
fn eval(
    compiled: &Compiled,
    dataset: &Dataset,
    file: &FileScan<'_>,
    row: usize,
) -> Result<bool, EventStoreError> {
    match compiled {
        Compiled::And(terms) => {
            for term in terms {
                if !eval(term, dataset, file, row)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Compiled::Or(terms) => {
            for term in terms {
                if eval(term, dataset, file, row)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Compiled::Not(inner) => Ok(!eval(inner, dataset, file, row)?),
        Compiled::IsNull { prop, negated } => {
            let (block, offset) = file.prop(*prop).prop_at(row)?;
            let is_null = matches!(prop_value(block, offset), PropRef::Null);
            Ok(is_null != *negated)
        }
        Compiled::CodeEq {
            prop,
            code,
            negated,
        } => {
            let (block, offset) = file.prop(*prop).prop_at(row)?;
            match prop_value(block, offset) {
                PropRef::Null => Ok(false),
                PropRef::Code(value) => Ok((value == *code) != *negated),
                other => unreachable!("dict property yielded {other:?}"),
            }
        }
        Compiled::AbsentEq { prop, negated } => {
            let (block, offset) = file.prop(*prop).prop_at(row)?;
            match prop_value(block, offset) {
                PropRef::Null => Ok(false),
                _ => Ok(*negated),
            }
        }
        Compiled::StrCmp { prop, op, value } => {
            let (block, offset) = file.prop(*prop).prop_at(row)?;
            let actual = match prop_value(block, offset) {
                PropRef::Null => return Ok(false),
                PropRef::Str(text) => text.to_string(),
                PropRef::Code(code) => dataset.dict_value(*prop, code).to_string(),
                other => unreachable!("string property yielded {other:?}"),
            };
            Ok(compare_ordered(&actual.as_str(), &value.as_str(), *op))
        }
        Compiled::CodeIn { prop, codes } => {
            let (block, offset) = file.prop(*prop).prop_at(row)?;
            match prop_value(block, offset) {
                PropRef::Null => Ok(false),
                PropRef::Code(value) => Ok(codes.binary_search(&value).is_ok()),
                other => unreachable!("dict property yielded {other:?}"),
            }
        }
        Compiled::StrIn { prop, values } => {
            let (block, offset) = file.prop(*prop).prop_at(row)?;
            match prop_value(block, offset) {
                PropRef::Null => Ok(false),
                PropRef::Str(text) => Ok(values.iter().any(|value| value == text)),
                PropRef::Code(code) => {
                    let text = dataset.dict_value(*prop, code);
                    Ok(values.iter().any(|value| value == text))
                }
                other => unreachable!("string property yielded {other:?}"),
            }
        }
        Compiled::NumCmp { prop, op, value } => {
            let Some(actual) = numeric_value(file, *prop, row)? else {
                return Ok(false);
            };
            Ok(compare_ordered(&actual, value, *op))
        }
        Compiled::NumIn { prop, values } => {
            let Some(actual) = numeric_value(file, *prop, row)? else {
                return Ok(false);
            };
            // Exact equality is the IN-list contract: the literal must equal
            // the stored value bit-for-bit as a number.
            #[allow(clippy::float_cmp)]
            let matched = values.contains(&actual);
            Ok(matched)
        }
        Compiled::BoolCmp { prop, op, value } => eval_bool_cmp(file, *prop, *op, *value, row),
    }
}

/// The numeric value of `prop` at `row` widened to f64, or None when null.
fn numeric_value(
    file: &FileScan<'_>,
    prop: usize,
    row: usize,
) -> Result<Option<f64>, EventStoreError> {
    let (block, offset) = file.prop(prop).prop_at(row)?;
    Ok(match prop_value(block, offset) {
        PropRef::Null => None,
        PropRef::I64(v) => Some(v as f64),
        PropRef::F64(v) => Some(v),
        other => unreachable!("numeric property yielded {other:?}"),
    })
}

/// Evaluate a bool comparison predicate for one event; a null value never
/// matches.
fn eval_bool_cmp(
    file: &FileScan<'_>,
    prop: usize,
    op: CompareOp,
    value: bool,
    row: usize,
) -> Result<bool, EventStoreError> {
    let (block, offset) = file.prop(prop).prop_at(row)?;
    let actual = match prop_value(block, offset) {
        PropRef::Null => return Ok(false),
        PropRef::Bool(v) => v,
        other => unreachable!("bool property yielded {other:?}"),
    };
    Ok(match op {
        CompareOp::Eq => actual == value,
        CompareOp::Neq => actual != value,
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
            unreachable!("bool ordering rejected at compile")
        }
    })
}

/// Apply an ordering comparison.
fn compare_ordered<T: PartialOrd>(actual: &T, target: &T, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => actual == target,
        CompareOp::Neq => actual != target,
        CompareOp::Lt => actual < target,
        CompareOp::Lte => actual <= target,
        CompareOp::Gt => actual > target,
        CompareOp::Gte => actual >= target,
    }
}

// ---------------------------------------------------------------------------
// Shared scan driving
// ---------------------------------------------------------------------------

/// The per-query scan context: the dataset, the compiled filter, and the
/// event-time range (half-open, defaulted from the dataset's measured
/// extremes).
struct ScanContext<'a> {
    dataset: &'a Dataset,
    filter: Option<Compiled>,
    from: i64,
    to: i64,
}

impl<'a> ScanContext<'a> {
    /// Build the context: compile the filter and default the range.
    fn new(
        dataset: &'a Dataset,
        filter: Option<&EventPredicate>,
        range: TimeRange,
    ) -> Result<Self, EventError> {
        let compiled = match filter {
            Some(predicate) => Some(compile_predicate(dataset, predicate)?),
            None => None,
        };
        let from = range.from.or(dataset.record.min_ts).unwrap_or(i64::MIN);
        let to = range
            .to
            .unwrap_or_else(|| dataset.record.max_ts.map_or(i64::MAX, |max| max + 1));
        Ok(Self {
            dataset,
            filter: compiled,
            from,
            to,
        })
    }

    /// Whether the event at `(file, row)` with time `ts` is visible to the
    /// analysis (range plus WHERE).
    fn visible(&self, file: &FileScan<'_>, row: usize, ts: i64) -> Result<bool, EventStoreError> {
        if ts < self.from || ts >= self.to {
            return Ok(false);
        }
        match &self.filter {
            None => Ok(true),
            Some(compiled) => eval(compiled, self.dataset, file, row),
        }
    }
}

/// Run `per_shard` for every shard on a shared work index, merging partials
/// with `merge`. Worker count is bounded by the machine's cores and the
/// shard count.
fn run_sharded<P: Send>(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &EventsConfig,
    per_shard: impl Fn(&ShardScan<'_>) -> Result<P, EventError> + Sync,
    mut merge: impl FnMut(P) -> Result<(), EventError>,
) -> Result<(), EventError> {
    let shard_files = dataset.shard_files();
    let work: Vec<(u32, &Vec<String>)> = shard_files
        .iter()
        .filter(|(_, files)| !files.is_empty())
        .map(|(shard, files)| (*shard, files))
        .collect();
    let threads = std::thread::available_parallelism()
        .map_or(4, std::num::NonZero::get)
        .min(work.len().max(1));
    let next = AtomicUsize::new(0);
    let partials: Mutex<Vec<P>> = Mutex::new(Vec::new());
    let failure: Mutex<Option<EventError>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= work.len() || failure.lock().expect("failure lock").is_some() {
                    return;
                }
                let (_, files) = work[index];
                let outcome = open_and_scan(io, dataset, cfg, files, &per_shard);
                match outcome {
                    Ok(partial) => partials.lock().expect("partials lock").push(partial),
                    Err(error) => {
                        let mut slot = failure.lock().expect("failure lock");
                        if slot.is_none() {
                            *slot = Some(error);
                        }
                    }
                }
            });
        }
    });
    if let Some(error) = failure.into_inner().expect("failure lock") {
        return Err(error);
    }
    for partial in partials.into_inner().expect("partials lock") {
        merge(partial)?;
    }
    Ok(())
}

/// Open one shard's files and run the per-shard body.
fn open_and_scan<P>(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &EventsConfig,
    files: &[String],
    per_shard: &(impl Fn(&ShardScan<'_>) -> Result<P, EventError> + Sync),
) -> Result<P, EventError> {
    let mut segs = Vec::with_capacity(files.len());
    for rel in files {
        segs.push(SegFile::open(
            io,
            rel,
            dataset.kinds.clone(),
            cfg.cache_bytes,
        )?);
    }
    let scan = ShardScan::open(io, segs)?;
    per_shard(&scan)
}

/// A group key of a breakdown value, comparable across shards (dictionary
/// codes are dataset-global).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum GroupKey {
    All,
    Null,
    Code(u64),
    Str(String),
    I64(i64),
    /// f64 bit pattern (total order over the bits; NaN never occurs in a
    /// stored property used as a group key with itself).
    F64(u64),
    Bool(bool),
}

/// The breakdown key of one event's property value.
fn group_key_of(value: PropRef<'_>) -> GroupKey {
    match value {
        PropRef::Null => GroupKey::Null,
        PropRef::Code(code) => GroupKey::Code(code),
        PropRef::Str(text) => GroupKey::Str(text.to_string()),
        PropRef::I64(value) => GroupKey::I64(value),
        PropRef::F64(value) => GroupKey::F64(value.to_bits()),
        PropRef::Bool(value) => GroupKey::Bool(value),
    }
}

/// Render a breakdown key as its result string (None = SQL NULL).
fn render_group_key(dataset: &Dataset, prop: Option<usize>, key: &GroupKey) -> Option<String> {
    match key {
        GroupKey::All | GroupKey::Null => None,
        GroupKey::Code(code) => {
            let prop = prop.expect("code keys come from a breakdown property");
            Some(dataset.dict_value(prop, *code).to_string())
        }
        GroupKey::Str(text) => Some(text.clone()),
        GroupKey::I64(value) => Some(value.to_string()),
        GroupKey::F64(bits) => Some(format!("{}", f64::from_bits(*bits))),
        GroupKey::Bool(value) => Some(if *value { "true" } else { "false" }.to_string()),
    }
}

/// The sort key of a rendered breakdown value: values ascending, NULL last.
fn breakdown_sort_key(rendered: Option<&String>) -> (bool, String) {
    match rendered {
        Some(text) => (false, text.clone()),
        None => (true, String::new()),
    }
}

// ---------------------------------------------------------------------------
// Funnel
// ---------------------------------------------------------------------------

/// One funnel partial: per breakdown group, per step, the entered count and
/// the witness step durations in microseconds.
struct FunnelPartial {
    entered: HashMap<GroupKey, Vec<u64>>,
    durations: HashMap<GroupKey, Vec<Vec<i64>>>,
}

/// Run a funnel per section 7.1: the STRICT_ORDER windowed-funnel DP per
/// actor (latest-anchor-dominates), then a greedy witness chase for actors
/// that converted past step one.
pub fn run_funnel(
    io: &StoreIo,
    dataset: &Dataset,
    spec: &FunnelSpec,
    cfg: &EventsConfig,
) -> Result<FunnelResult, EventError> {
    if spec.mode == FunnelMode::AnyOrder {
        return Err(EventUnsupported::AnyOrderFunnel.into());
    }
    if spec.steps.len() < 2 || spec.steps.len() > 32 {
        return Err(EventBindingError::StepCount {
            got: spec.steps.len(),
        }
        .into());
    }
    if spec.window_micros <= 0 {
        return Err(EventBindingError::Window {
            got: spec.window_micros,
        }
        .into());
    }
    validate_range("FUNNEL", spec.range)?;
    let breakdown_prop = match &spec.breakdown {
        Some(property) => Some(resolve_property(dataset, property)?),
        None => None,
    };
    let mut step_codes = Vec::with_capacity(spec.steps.len());
    for step in &spec.steps {
        step_codes.push(resolve_event_code(dataset, step, cfg)?);
    }
    let mut merged = FunnelPartial {
        entered: HashMap::new(),
        durations: HashMap::new(),
    };
    run_sharded(
        io,
        dataset,
        cfg,
        |scan| {
            let context = ScanContext::new(dataset, spec.filter.as_ref(), spec.range)?;
            funnel_shard(
                scan,
                &context,
                &step_codes,
                breakdown_prop,
                spec.window_micros,
            )
        },
        |partial| {
            for (key, counts) in partial.entered {
                let slot = merged
                    .entered
                    .entry(key)
                    .or_insert_with(|| vec![0; step_codes.len()]);
                for (index, count) in counts.iter().enumerate() {
                    slot[index] += count;
                }
            }
            for (key, steps) in partial.durations {
                let slot = merged
                    .durations
                    .entry(key)
                    .or_insert_with(|| vec![Vec::new(); step_codes.len()]);
                for (index, durations) in steps.into_iter().enumerate() {
                    slot[index].extend(durations);
                }
            }
            Ok(())
        },
    )?;
    Ok(render_funnel(dataset, spec, breakdown_prop, &merged))
}

/// The funnel scan of one shard.
fn funnel_shard(
    scan: &ShardScan<'_>,
    context: &ScanContext<'_>,
    step_codes: &[u64],
    breakdown_prop: Option<usize>,
    window: i64,
) -> Result<FunnelPartial, EventError> {
    let k = step_codes.len();
    let mut partial = FunnelPartial {
        entered: HashMap::new(),
        durations: HashMap::new(),
    };
    // Per-actor scratch, reused: per step, the visible matching events as
    // (sequence position, ts, file, row).
    let mut step_events: Vec<Vec<(u32, i64, u32, u32)>> = vec![Vec::new(); k];
    for run in scan.actor_runs() {
        for events in &mut step_events {
            events.clear();
        }
        let mut sequence = 0u32;
        // Pass one over the actor: collect visible step-matching events (the
        // DP itself is replayed by the anchor chase, which needs the lists
        // anyway).
        scan.for_each_event(&run, |file, row, code| {
            let mut matches_any = false;
            for step_code in step_codes {
                if code == *step_code {
                    matches_any = true;
                    break;
                }
            }
            if matches_any {
                let file_scan = &scan.files[file];
                let ts = file_scan.ts.i64_at(row)?;
                if context.visible(file_scan, row, ts)? {
                    for (step, step_code) in step_codes.iter().enumerate() {
                        if code == *step_code {
                            step_events[step].push((sequence, ts, file as u32, row as u32));
                        }
                    }
                    sequence += 1;
                }
            }
            Ok(true)
        })?;
        if step_events[0].is_empty() {
            continue;
        }
        let (depth, witness) = actor_depth_and_witness(&step_events, window);
        let (anchor_file, anchor_row) = witness.anchor;
        let key = match breakdown_prop {
            None => GroupKey::All,
            Some(prop) => {
                let file_scan = &scan.files[anchor_file as usize];
                let (block, offset) = file_scan.prop(prop).prop_at(anchor_row as usize)?;
                group_key_of(prop_value(block, offset))
            }
        };
        let entered = partial
            .entered
            .entry(key.clone())
            .or_insert_with(|| vec![0; k]);
        for count in entered.iter_mut().take(depth) {
            *count += 1;
        }
        if depth >= 2 {
            let durations = partial
                .durations
                .entry(key)
                .or_insert_with(|| vec![Vec::new(); k]);
            for (step, duration) in witness.durations.iter().enumerate() {
                durations[step + 1].push(*duration);
            }
        }
    }
    Ok(partial)
}

/// The witness chase result of one actor.
struct Witness {
    /// The witness anchor event's (file, row).
    anchor: (u32, u32),
    /// Per step 2..=depth, `ts(step) - ts(step-1)` in microseconds.
    durations: Vec<i64>,
}

/// The actor's maximum funnel depth (the exists-form) and its witness: the
/// earliest step-one event from which greedy forward matching reaches that
/// depth. Greedy-from-anchor is optimal for a fixed anchor, so enumerating
/// anchors in time order and chasing greedily computes both exactly.
fn actor_depth_and_witness(
    step_events: &[Vec<(u32, i64, u32, u32)>],
    window: i64,
) -> (usize, Witness) {
    let mut best_depth = 1usize;
    let mut best: Option<Witness> = None;
    for anchor in &step_events[0] {
        let (depth, chain) = greedy_chase(step_events, *anchor, window);
        if depth > best_depth || best.is_none() {
            best_depth = depth;
            let mut durations = Vec::with_capacity(chain.len());
            let mut previous_ts = anchor.1;
            for (_, ts) in &chain {
                durations.push(ts - previous_ts);
                previous_ts = *ts;
            }
            best = Some(Witness {
                anchor: (anchor.2, anchor.3),
                durations,
            });
            if best_depth == step_events.len() {
                break;
            }
        }
    }
    (
        best_depth,
        best.expect("step one non-empty implies a witness"),
    )
}

/// Greedily chase steps 2..k from one anchor: each step matched by the
/// earliest qualifying event after the previous match within the window of
/// the anchor. Returns the reached depth and the chain's (sequence, ts) per
/// matched step past the anchor.
fn greedy_chase(
    step_events: &[Vec<(u32, i64, u32, u32)>],
    anchor: (u32, i64, u32, u32),
    window: i64,
) -> (usize, Vec<(u32, i64)>) {
    let (mut sequence, anchor_ts, _, _) = anchor;
    let deadline = anchor_ts.saturating_add(window);
    let mut chain = Vec::new();
    for events in &step_events[1..] {
        // The first event of this step strictly after `sequence`.
        let position = events.partition_point(|(seq, _, _, _)| *seq <= sequence);
        let Some((seq, ts, _, _)) = events.get(position) else {
            break;
        };
        // Events are time-sorted, so if the earliest candidate misses the
        // window every later one does too.
        if *ts > deadline {
            break;
        }
        chain.push((*seq, *ts));
        sequence = *seq;
    }
    (1 + chain.len(), chain)
}

/// Render the merged funnel partials into the pinned result relation.
fn render_funnel(
    dataset: &Dataset,
    spec: &FunnelSpec,
    breakdown_prop: Option<usize>,
    merged: &FunnelPartial,
) -> FunnelResult {
    let mut groups: Vec<(Option<String>, GroupKey)> = merged
        .entered
        .keys()
        .map(|key| (render_group_key(dataset, breakdown_prop, key), key.clone()))
        .collect();
    groups.sort_by_key(|(rendered, _)| breakdown_sort_key(rendered.as_ref()));
    // A funnel nobody entered still reports its steps: without a breakdown
    // the relation is one all-zero row per step (a breakdown with no entrant
    // values has no rows to attribute).
    if groups.is_empty() && spec.breakdown.is_none() {
        groups.push((None, GroupKey::All));
    }
    let mut rows = Vec::new();
    let zero_entered = vec![0u64; spec.steps.len()];
    for (rendered, key) in groups {
        let entered = merged.entered.get(&key).unwrap_or(&zero_entered);
        let empty: Vec<Vec<i64>> = Vec::new();
        let durations = merged.durations.get(&key).unwrap_or(&empty);
        let start = entered[0];
        let mut previous = start;
        for (step, step_name) in spec.steps.iter().enumerate() {
            let count = entered[step];
            let (median, p90) = if step == 0 {
                (None, None)
            } else {
                let mut values: Vec<i64> = durations.get(step).map_or_else(Vec::new, Clone::clone);
                (
                    quantile_cont(&mut values, 0.5).map(|micros| micros / 1_000_000.0),
                    quantile_cont(&mut values, 0.9).map(|micros| micros / 1_000_000.0),
                )
            };
            rows.push(FunnelRow {
                breakdown: rendered.clone(),
                step_index: step as u32 + 1,
                step_name: step_name.clone(),
                entered: count,
                conversion_from_previous: ratio(count, if step == 0 { count } else { previous }),
                conversion_from_start: ratio(count, if step == 0 { count } else { start }),
                median_seconds_from_previous: median,
                p90_seconds_from_previous: p90,
            });
            previous = count;
        }
    }
    FunnelResult {
        has_breakdown: spec.breakdown.is_some(),
        rows,
    }
}

/// `numerator / denominator` with the 0/0 case pinned to 0.0 (an empty
/// funnel step has no conversion) and step one pinned to 1.0 by the callers.
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    numerator as f64 / denominator as f64
}

/// The linearly interpolated quantile over `values` (the `quantile_cont`
/// definition the oracle uses), exact via element selection.
fn quantile_cont(values: &mut [i64], q: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let position = (values.len() - 1) as f64 * q;
    let low_index = position.floor() as usize;
    let fraction = position - low_index as f64;
    let (_, low_value, rest) = values.select_nth_unstable(low_index);
    let low_value = *low_value;
    if fraction == 0.0 || rest.is_empty() {
        return Some(low_value as f64);
    }
    let high_value = *rest.iter().min().expect("rest non-empty");
    Some(low_value as f64 + (high_value - low_value) as f64 * fraction)
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

/// The calendar period arithmetic of one grain: timestamps map to a dense
/// period index; indices map back to the period's start instant.
#[derive(Clone, Copy)]
struct PeriodMath {
    grain: PeriodGrain,
}

impl PeriodMath {
    /// The period index containing `micros`.
    fn index(self, micros: i64) -> i64 {
        let day = day_of_micros(micros);
        match self.grain {
            PeriodGrain::Day => day,
            PeriodGrain::Week => week_start_of_day(day).div_euclid(7),
            PeriodGrain::Month => {
                let (year, month, _) = civil_from_days(day);
                year * 12 + i64::from(month) - 1
            }
        }
    }

    /// The start instant of period `index`, UTC microseconds.
    fn start(self, index: i64) -> i64 {
        match self.grain {
            PeriodGrain::Day => index * MICROS_PER_DAY,
            // Week indices divide the Monday-aligned day by 7; day 0 (a
            // Thursday) puts Mondays at 4 mod 7, so the start restores +4.
            PeriodGrain::Week => (index * 7 + 4) * MICROS_PER_DAY,
            PeriodGrain::Month => {
                let year = index.div_euclid(12);
                let month = index.rem_euclid(12) + 1;
                days_from_civil(year, u32::try_from(month).expect("month in 1..=12"), 1)
                    * MICROS_PER_DAY
            }
        }
    }
}

/// One retention partial.
struct RetentionPartial {
    /// (group, cohort index) -> cohort size.
    sizes: HashMap<(GroupKey, i64), u64>,
    /// BOUNDED: (group, cohort index, offset) -> retained count.
    /// UNBOUNDED: (group, cohort index, max offset) -> actor count.
    cells: HashMap<(GroupKey, i64, i64), u64>,
}

/// Run a retention grid per section 7.2.
pub fn run_retention(
    io: &StoreIo,
    dataset: &Dataset,
    spec: &RetentionSpec,
    cfg: &EventsConfig,
) -> Result<RetentionResult, EventError> {
    validate_range("RETENTION", spec.range)?;
    if let (Some(at), Some(periods)) = (spec.at, spec.periods) {
        if at > periods {
            return Err(EventBindingError::AtBeyondPeriods { at, periods }.into());
        }
    }
    let breakdown_prop = match &spec.breakdown {
        Some(property) => Some(resolve_property(dataset, property)?),
        None => None,
    };
    let birth_code = match &spec.birth {
        EventMatch::Any => None,
        EventMatch::Named(name) => Some(resolve_event_code(dataset, name, cfg)?),
    };
    let return_code = match &spec.return_event {
        EventMatch::Any => None,
        EventMatch::Named(name) => Some(resolve_event_code(dataset, name, cfg)?),
    };
    let math = PeriodMath { grain: spec.period };
    let mut merged = RetentionPartial {
        sizes: HashMap::new(),
        cells: HashMap::new(),
    };
    run_sharded(
        io,
        dataset,
        cfg,
        |scan| {
            let context = ScanContext::new(dataset, spec.filter.as_ref(), spec.range)?;
            retention_shard(
                scan,
                &context,
                birth_code,
                return_code,
                breakdown_prop,
                math,
                spec.mode,
            )
        },
        |partial| {
            for (key, count) in partial.sizes {
                *merged.sizes.entry(key).or_insert(0) += count;
            }
            for (key, count) in partial.cells {
                *merged.cells.entry(key).or_insert(0) += count;
            }
            Ok(())
        },
    )?;
    Ok(render_retention(
        dataset,
        spec,
        breakdown_prop,
        math,
        &merged,
    ))
}

/// The retention scan of one shard: one forward pass per actor; offsets
/// arrive non-decreasing (events are time-sorted), so per-actor dedup is a
/// last-counted comparison.
fn retention_shard(
    scan: &ShardScan<'_>,
    context: &ScanContext<'_>,
    birth_code: Option<u64>,
    return_code: Option<u64>,
    breakdown_prop: Option<usize>,
    math: PeriodMath,
    mode: RetentionMode,
) -> Result<RetentionPartial, EventError> {
    let mut partial = RetentionPartial {
        sizes: HashMap::new(),
        cells: HashMap::new(),
    };
    for run in scan.actor_runs() {
        let mut birth: Option<(GroupKey, i64)> = None;
        let mut last_counted: Option<i64> = None;
        let mut max_offset: Option<i64> = None;
        scan.for_each_event(&run, |file, row, code| {
            let file_scan = &scan.files[file];
            match &birth {
                None => {
                    if birth_code.is_none_or(|wanted| code == wanted) {
                        let ts = file_scan.ts.i64_at(row)?;
                        if context.visible(file_scan, row, ts)? {
                            let key = match breakdown_prop {
                                None => GroupKey::All,
                                Some(prop) => {
                                    let (block, offset) = file_scan.prop(prop).prop_at(row)?;
                                    group_key_of(prop_value(block, offset))
                                }
                            };
                            birth = Some((key, math.index(ts)));
                        }
                    }
                }
                Some((key, birth_index)) => {
                    if return_code.is_none_or(|wanted| code == wanted) {
                        let ts = file_scan.ts.i64_at(row)?;
                        if context.visible(file_scan, row, ts)? {
                            let offset = math.index(ts) - birth_index;
                            match mode {
                                RetentionMode::Bounded => {
                                    if last_counted != Some(offset) {
                                        last_counted = Some(offset);
                                        *partial
                                            .cells
                                            .entry((key.clone(), *birth_index, offset))
                                            .or_insert(0) += 1;
                                    }
                                }
                                RetentionMode::Unbounded => {
                                    max_offset = Some(offset);
                                }
                            }
                        }
                    }
                }
            }
            Ok(true)
        })?;
        if let Some((key, birth_index)) = birth {
            *partial.sizes.entry((key.clone(), birth_index)).or_insert(0) += 1;
            if let Some(offset) = max_offset {
                *partial.cells.entry((key, birth_index, offset)).or_insert(0) += 1;
            }
        }
    }
    Ok(partial)
}

/// Render the retention grid: every cohort period intersecting the range
/// appears (zero-birth periods with zero counts), offsets 0..=PERIODS
/// (default: through the dataset's max ts), UNBOUNDED via suffix sums of the
/// max-offset histogram, `AT n` filtering to one offset.
fn render_retention(
    dataset: &Dataset,
    spec: &RetentionSpec,
    breakdown_prop: Option<usize>,
    math: PeriodMath,
    merged: &RetentionPartial,
) -> RetentionResult {
    let range_from = spec.range.from.or(dataset.record.min_ts);
    let range_to_incl = spec.range.to.map(|to| to - 1).or(dataset.record.max_ts);
    let (Some(from), Some(to_incl)) = (range_from, range_to_incl) else {
        // An empty dataset has no calendar extent: the grid is empty.
        return RetentionResult {
            has_breakdown: spec.breakdown.is_some(),
            rows: Vec::new(),
        };
    };
    let first_index = math.index(from);
    let last_index = math.index(to_incl);
    let mut group_keys: Vec<GroupKey> = merged
        .sizes
        .keys()
        .map(|(key, _)| key.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if group_keys.is_empty() {
        group_keys.push(GroupKey::All);
    }
    let mut groups: Vec<(Option<String>, GroupKey)> = group_keys
        .into_iter()
        .map(|key| (render_group_key(dataset, breakdown_prop, &key), key))
        .collect();
    groups.sort_by_key(|(rendered, _)| breakdown_sort_key(rendered.as_ref()));
    let mut rows = Vec::new();
    for (rendered, key) in groups {
        for cohort in first_index..=last_index {
            let size = merged
                .sizes
                .get(&(key.clone(), cohort))
                .copied()
                .unwrap_or(0);
            let max_offset = spec.periods.map_or(last_index - cohort, i64::from).max(0);
            let unbounded_suffix = match spec.mode {
                RetentionMode::Bounded => Vec::new(),
                RetentionMode::Unbounded => suffix_counts(merged, &key, cohort, max_offset),
            };
            for offset in 0..=max_offset {
                if let Some(at) = spec.at {
                    if offset != i64::from(at) {
                        continue;
                    }
                }
                let retained = match spec.mode {
                    RetentionMode::Bounded => merged
                        .cells
                        .get(&(key.clone(), cohort, offset))
                        .copied()
                        .unwrap_or(0),
                    RetentionMode::Unbounded => unbounded_suffix[offset as usize],
                };
                rows.push(RetentionRow {
                    breakdown: rendered.clone(),
                    cohort_start: math.start(cohort),
                    cohort_size: size,
                    period_offset: u32::try_from(offset).expect("offset fits u32"),
                    retained,
                    retention_rate: if size == 0 {
                        None
                    } else {
                        Some(retained as f64 / size as f64)
                    },
                });
            }
        }
    }
    RetentionResult {
        has_breakdown: spec.breakdown.is_some(),
        rows,
    }
}

/// `retained_unbounded(n)` per offset 0..=max: the suffix sums of the
/// max-offset histogram (an actor whose latest return is period m counts as
/// retained at every n <= m).
fn suffix_counts(
    merged: &RetentionPartial,
    key: &GroupKey,
    cohort: i64,
    max_offset: i64,
) -> Vec<u64> {
    let mut suffix = vec![0u64; usize::try_from(max_offset).expect("offset fits") + 1];
    for ((cell_key, cell_cohort, cell_offset), count) in &merged.cells {
        if cell_key == key && *cell_cohort == cohort {
            // Offsets beyond the reported window still make the actor
            // retained at every reported offset.
            let capped = (*cell_offset).min(max_offset);
            if capped >= 0 {
                suffix[usize::try_from(capped).expect("offset fits")] += count;
            }
        }
    }
    for offset in (0..suffix.len().saturating_sub(1)).rev() {
        suffix[offset] += suffix[offset + 1];
    }
    suffix
}

// ---------------------------------------------------------------------------
// Segmentation
// ---------------------------------------------------------------------------

/// The bucket arithmetic of one grain.
#[derive(Clone, Copy)]
struct BucketMath {
    grain: BucketGrain,
}

impl BucketMath {
    /// The bucket start instant containing `micros`.
    fn bucket(self, micros: i64) -> i64 {
        match self.grain {
            BucketGrain::Hour => micros.div_euclid(3_600_000_000) * 3_600_000_000,
            BucketGrain::Day => day_of_micros(micros) * MICROS_PER_DAY,
            BucketGrain::Week => week_start_of_day(day_of_micros(micros)) * MICROS_PER_DAY,
            BucketGrain::Month => {
                let (year, month, _) = civil_from_days(day_of_micros(micros));
                days_from_civil(year, month, 1) * MICROS_PER_DAY
            }
        }
    }

    /// The bucket start of a whole UTC day (the pre-aggregate path).
    fn bucket_of_day(self, day: i64) -> i64 {
        match self.grain {
            BucketGrain::Hour => unreachable!("the pre-agg path never runs at hour grain"),
            BucketGrain::Day => day * MICROS_PER_DAY,
            BucketGrain::Week => week_start_of_day(day) * MICROS_PER_DAY,
            BucketGrain::Month => {
                let (year, month, _) = civil_from_days(day);
                days_from_civil(year, month, 1) * MICROS_PER_DAY
            }
        }
    }
}

/// Run a segmentation per section 7.3, taking the pre-aggregate fast path
/// when the query shape admits it.
pub fn run_segment(
    io: &StoreIo,
    dataset: &Dataset,
    spec: &SegmentSpec,
    cfg: &EventsConfig,
) -> Result<SegmentResult, EventError> {
    validate_range("SEGMENT", spec.range)?;
    let breakdown_prop = match &spec.breakdown {
        Some(property) => Some(resolve_property(dataset, property)?),
        None => None,
    };
    let event_code = match &spec.event {
        Some(name) => Some(resolve_event_code(dataset, name, cfg)?),
        None => None,
    };
    let math = BucketMath { grain: spec.bucket };
    if segment_fast_path_admissible(spec) {
        return segment_preagg(io, dataset, spec, event_code, math, cfg);
    }
    let mut merged: HashMap<(i64, GroupKey), (u64, u64)> = HashMap::new();
    run_sharded(
        io,
        dataset,
        cfg,
        |scan| {
            let context = ScanContext::new(dataset, spec.filter.as_ref(), spec.range)?;
            segment_shard(scan, &context, event_code, breakdown_prop, math, cfg)
        },
        |partial| {
            for (key, (count, uniques)) in partial {
                let slot = merged.entry(key).or_insert((0, 0));
                slot.0 += count;
                slot.1 += uniques;
            }
            Ok(())
        },
    )?;
    let mut rows: Vec<SegmentRow> = merged
        .into_iter()
        .map(|((bucket, key), (count, uniques))| SegmentRow {
            bucket,
            breakdown: render_group_key(dataset, breakdown_prop, &key),
            count,
            uniques,
        })
        .collect();
    rows.sort_by(|a, b| {
        (breakdown_sort_key(a.breakdown.as_ref()), a.bucket)
            .cmp(&(breakdown_sort_key(b.breakdown.as_ref()), b.bucket))
    });
    Ok(SegmentResult {
        has_breakdown: spec.breakdown.is_some(),
        measure: spec.measure,
        rows,
    })
}

/// Whether the pre-aggregate answers this query exactly: COUNT at a
/// day-or-coarser grain, no property filter, no breakdown, and day-aligned
/// (or absent) bounds.
fn segment_fast_path_admissible(spec: &SegmentSpec) -> bool {
    let aligned =
        |bound: Option<i64>| bound.is_none_or(|micros| micros.rem_euclid(MICROS_PER_DAY) == 0);
    spec.measure == SegmentMeasure::Count
        && spec.filter.is_none()
        && spec.breakdown.is_none()
        && spec.bucket != BucketGrain::Hour
        && aligned(spec.range.from)
        && aligned(spec.range.to)
}

/// The pre-aggregate fast path: merge the per-file event-by-day entries; no
/// event column is touched.
fn segment_preagg(
    io: &StoreIo,
    dataset: &Dataset,
    spec: &SegmentSpec,
    event_code: Option<u64>,
    math: BucketMath,
    cfg: &EventsConfig,
) -> Result<SegmentResult, EventError> {
    let from_day = spec.range.from.map(day_of_micros);
    let to_day = spec.range.to.map(day_of_micros);
    let mut buckets: HashMap<i64, u64> = HashMap::new();
    for (_, files) in dataset.shard_files() {
        for rel in files {
            let seg = SegFile::open(io, rel, dataset.kinds.clone(), cfg.cache_bytes)?;
            let preagg = seg.preagg(io)?;
            let CachedItem::Preagg(ref preagg) = *preagg else {
                unreachable!("preagg cache slot holds a preagg");
            };
            for (code, day, count) in &preagg.entries {
                if let Some(wanted) = event_code {
                    if *code != wanted {
                        continue;
                    }
                }
                if from_day.is_some_and(|from| *day < from) || to_day.is_some_and(|to| *day >= to) {
                    continue;
                }
                *buckets.entry(math.bucket_of_day(*day)).or_insert(0) += count;
            }
        }
    }
    let mut rows: Vec<SegmentRow> = buckets
        .into_iter()
        .map(|(bucket, count)| SegmentRow {
            bucket,
            breakdown: None,
            count,
            uniques: 0,
        })
        .collect();
    rows.sort_by_key(|row| row.bucket);
    Ok(SegmentResult {
        has_breakdown: false,
        measure: spec.measure,
        rows,
    })
}

/// Per (bucket, group key): the event count and the unique-actor count.
type SegmentGroups = HashMap<(i64, GroupKey), (u64, u64)>;

/// The segmentation scan of one shard: streaming dedup via the per-group
/// last-actor marker (actors are contiguous in the scan).
fn segment_shard(
    scan: &ShardScan<'_>,
    context: &ScanContext<'_>,
    event_code: Option<u64>,
    breakdown_prop: Option<usize>,
    math: BucketMath,
    cfg: &EventsConfig,
) -> Result<SegmentGroups, EventError> {
    let mut groups: FastMap<(i64, GroupKey), (u64, u64, u64)> = FastMap::default();
    let budget = cfg.query_memory_bytes / 16;
    for (run_index, run) in scan.actor_runs().into_iter().enumerate() {
        // Sequence numbers start at 1: a group slot's marker of 0 means the
        // group has not seen any actor yet.
        let actor_sequence = run_index as u64 + 1;
        scan.for_each_event(&run, |file, row, code| {
            if let Some(wanted) = event_code {
                if code != wanted {
                    return Ok(true);
                }
            }
            let file_scan = &scan.files[file];
            let ts = file_scan.ts.i64_at(row)?;
            if !context.visible(file_scan, row, ts)? {
                return Ok(true);
            }
            let key = match breakdown_prop {
                None => GroupKey::All,
                Some(prop) => {
                    let (block, offset) = file_scan.prop(prop).prop_at(row)?;
                    group_key_of(prop_value(block, offset))
                }
            };
            let slot = groups.entry((math.bucket(ts), key)).or_insert((0, 0, 0));
            slot.0 += 1;
            if slot.2 != actor_sequence {
                slot.1 += 1;
                slot.2 = actor_sequence;
            }
            Ok(true)
        })?;
        if approximate_group_bytes(groups.len()) > budget {
            return Err(EventUnsupported::QueryMemory {
                analysis: "SEGMENT".to_string(),
                budget: cfg.query_memory_bytes,
            }
            .into());
        }
    }
    Ok(groups
        .into_iter()
        .map(|(key, (count, uniques, _))| (key, (count, uniques)))
        .collect())
}

/// A coarse per-entry estimate of the group map's memory.
fn approximate_group_bytes(entries: usize) -> u64 {
    entries as u64 * 128
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// The bound on `DEPTH` (path explosion is exponential in depth).
const MAX_DEPTH: u32 = 10;

/// One path sequence key: the exact code sequence, inline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PathKey {
    codes: [u64; MAX_DEPTH as usize],
    len: u8,
}

/// Hash only the filled prefix: unfilled slots are always zero, so equal
/// keys hash equal while short paths skip the empty tail.
impl std::hash::Hash for PathKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u8(self.len);
        for code in &self.codes[..usize::from(self.len)] {
            state.write_u64(*code);
        }
    }
}

/// Run a paths analysis per section 7.4.
pub fn run_paths(
    io: &StoreIo,
    dataset: &Dataset,
    spec: &PathsSpec,
    cfg: &EventsConfig,
) -> Result<PathsResult, EventError> {
    if spec.depth < 2 || spec.depth > MAX_DEPTH {
        return Err(EventBindingError::PathDepth { got: spec.depth }.into());
    }
    if spec.top < 1 || spec.top > 1000 {
        return Err(EventBindingError::PathTop { got: spec.top }.into());
    }
    validate_range("PATHS", spec.range)?;
    let anchor = match &spec.anchor {
        PathAnchor::Unanchored => None,
        PathAnchor::StartingAt(name) => Some((resolve_event_code(dataset, name, cfg)?, true)),
        PathAnchor::EndingAt(name) => Some((resolve_event_code(dataset, name, cfg)?, false)),
    };
    let mut merged: FastMap<PathKey, u64> = FastMap::default();
    run_sharded(
        io,
        dataset,
        cfg,
        |scan| {
            let context = ScanContext::new(dataset, spec.filter.as_ref(), spec.range)?;
            paths_shard(scan, &context, spec, anchor, cfg)
        },
        |partial| {
            for (key, count) in partial {
                *merged.entry(key).or_insert(0) += count;
            }
            Ok(())
        },
    )?;
    Ok(render_paths(dataset, spec, &merged))
}

/// The paths scan of one shard: per actor, materialize the filtered,
/// gap-fragmented, duplicate-collapsed code stream into a reused scratch
/// buffer, then enumerate windows per the anchor variant.
fn paths_shard(
    scan: &ShardScan<'_>,
    context: &ScanContext<'_>,
    spec: &PathsSpec,
    anchor: Option<(u64, bool)>,
    cfg: &EventsConfig,
) -> Result<FastMap<PathKey, u64>, EventError> {
    let mut counts: FastMap<PathKey, u64> = FastMap::default();
    // The collapsed stream: (code, fragment id).
    let mut stream: Vec<(u64, u32)> = Vec::new();
    let budget = cfg.query_memory_bytes / 16;
    for run in scan.actor_runs() {
        stream.clear();
        let mut previous_ts: Option<i64> = None;
        let mut fragment = 0u32;
        scan.for_each_event(&run, |file, row, code| {
            let file_scan = &scan.files[file];
            let needs_ts = spec.maxgap_micros.is_some()
                || context.from != i64::MIN
                || context.to != i64::MAX
                || context.filter.is_some();
            let ts = if needs_ts || spec.maxgap_micros.is_some() {
                file_scan.ts.i64_at(row)?
            } else {
                0
            };
            if needs_ts && !context.visible(file_scan, row, ts)? {
                return Ok(true);
            }
            // A gap past MAXGAP between consecutive surviving events breaks
            // the stream BEFORE collapsing: duplicates in different fragments
            // stay distinct.
            if let Some(maxgap) = spec.maxgap_micros {
                let real_ts = if needs_ts {
                    ts
                } else {
                    file_scan.ts.i64_at(row)?
                };
                if let Some(previous) = previous_ts {
                    if real_ts - previous > maxgap {
                        fragment += 1;
                    }
                }
                previous_ts = Some(real_ts);
            }
            match stream.last() {
                Some((last_code, last_fragment))
                    if *last_code == code && *last_fragment == fragment => {}
                _ => stream.push((code, fragment)),
            }
            Ok(true)
        })?;
        enumerate_windows(&stream, spec, anchor, &mut counts);
        if approximate_group_bytes(counts.len()) > budget {
            return Err(EventUnsupported::QueryMemory {
                analysis: "PATHS".to_string(),
                budget: cfg.query_memory_bytes,
            }
            .into());
        }
    }
    Ok(counts)
}

/// Count one actor's windows into the map per the anchor variant.
fn enumerate_windows(
    stream: &[(u64, u32)],
    spec: &PathsSpec,
    anchor: Option<(u64, bool)>,
    counts: &mut FastMap<PathKey, u64>,
) {
    let depth = spec.depth as usize;
    let n = stream.len();
    for start in 0..n {
        let (code, fragment) = stream[start];
        match anchor {
            None => {
                // Unanchored: the full-length forward window only.
                let end = start + depth;
                if end <= n && stream[end - 1].1 == fragment {
                    count_window(stream, start, end, counts);
                }
            }
            Some((anchor_code, true)) => {
                // Forward flow: every anchor occurrence emits its forward
                // window, truncated at fragment end, length >= 2.
                if code == anchor_code {
                    let mut end = start + 1;
                    while end < n && end - start < depth && stream[end].1 == fragment {
                        end += 1;
                    }
                    if end - start >= 2 {
                        count_window(stream, start, end, counts);
                    }
                }
            }
            Some((anchor_code, false)) => {
                // Backward flow: the DEPTH-1 events before each anchor, in
                // forward order ending at it, truncated at fragment start.
                if code == anchor_code {
                    let mut begin = start;
                    while begin > 0 && start + 1 - begin < depth && stream[begin - 1].1 == fragment
                    {
                        begin -= 1;
                    }
                    if start + 1 - begin >= 2 {
                        count_window(stream, begin, start + 1, counts);
                    }
                }
            }
        }
    }
}

/// Record one window `[start, end)` of the stream.
fn count_window(
    stream: &[(u64, u32)],
    start: usize,
    end: usize,
    counts: &mut FastMap<PathKey, u64>,
) {
    let mut key = PathKey {
        codes: [0; MAX_DEPTH as usize],
        len: (end - start) as u8,
    };
    for (slot, (code, _)) in key.codes.iter_mut().zip(&stream[start..end]) {
        *slot = *code;
    }
    *counts.entry(key).or_insert(0) += 1;
}

/// Render the merged path counts: total-share, then TOP n by
/// (count desc, path string asc) - the tie order is pinned so results are
/// deterministic.
fn render_paths(
    dataset: &Dataset,
    spec: &PathsSpec,
    merged: &FastMap<PathKey, u64>,
) -> PathsResult {
    let total: u64 = merged.values().sum();
    let mut named: Vec<(u64, String)> = merged
        .iter()
        .map(|(key, count)| {
            let names: Vec<&str> = key.codes[..key.len as usize]
                .iter()
                .map(|code| dataset.event_name(*code))
                .collect();
            (*count, names.join(" -> "))
        })
        .collect();
    named.sort_by(|a, b| (std::cmp::Reverse(a.0), &a.1).cmp(&(std::cmp::Reverse(b.0), &b.1)));
    named.truncate(spec.top as usize);
    let rows = named
        .into_iter()
        .enumerate()
        .map(|(index, (count, path))| PathsRow {
            rank: index as u32 + 1,
            path,
            occurrences: count,
            share: if total == 0 {
                0.0
            } else {
                count as f64 / total as f64
            },
        })
        .collect();
    PathsResult { rows }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_cont_matches_linear_interpolation() {
        let mut values = vec![10i64, 20, 30, 40];
        assert_eq!(quantile_cont(&mut values, 0.5), Some(25.0));
        let mut values = vec![10i64, 20, 30];
        assert_eq!(quantile_cont(&mut values, 0.5), Some(20.0));
        let mut values = vec![10i64, 20, 30, 40];
        assert!((quantile_cont(&mut values, 0.9).unwrap() - 37.0).abs() < 1e-9);
        let mut empty: Vec<i64> = Vec::new();
        assert_eq!(quantile_cont(&mut empty, 0.5), None);
    }

    #[test]
    fn levenshtein_distances() {
        assert_eq!(levenshtein("signup", "signup"), 0);
        assert_eq!(levenshtein("signup", "sign_up"), 1);
        assert_eq!(levenshtein("abc", ""), 3);
    }

    #[test]
    fn period_math_maps_weeks_and_months() {
        let week = PeriodMath {
            grain: PeriodGrain::Week,
        };
        // 2025-01-01 (Wednesday) belongs to the week starting Monday
        // 2024-12-30.
        let jan1 = days_from_civil(2025, 1, 1) * MICROS_PER_DAY;
        let index = week.index(jan1);
        assert_eq!(
            week.start(index),
            days_from_civil(2024, 12, 30) * MICROS_PER_DAY
        );
        let month = PeriodMath {
            grain: PeriodGrain::Month,
        };
        let jan31 = days_from_civil(2025, 1, 31) * MICROS_PER_DAY + 5;
        let feb1 = days_from_civil(2025, 2, 1) * MICROS_PER_DAY;
        assert_eq!(month.index(feb1) - month.index(jan31), 1);
        assert_eq!(
            month.start(month.index(jan31)),
            days_from_civil(2025, 1, 1) * MICROS_PER_DAY
        );
    }
}
