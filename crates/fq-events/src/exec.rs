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

/// One ranked path.
#[derive(Debug, Clone, PartialEq)]
pub struct PathsRow {
    pub rank: u32,
    /// The path's event names in step order.
    pub steps: Vec<String>,
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
pub(crate) fn run_sharded<P: Send>(
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

/// Run a paths analysis per section 7.4. DEPTH is unbounded above: an
/// indexable statement (no WHERE, no MAXGAP) answers from the suffix-array
/// paths index at any depth, and the filtered/gapped scan below enumerates
/// one window per stream position, so depth never multiplies the work.
pub fn run_paths(
    io: &StoreIo,
    dataset: &Dataset,
    spec: &PathsSpec,
    cfg: &EventsConfig,
) -> Result<PathsResult, EventError> {
    if spec.depth < 2 {
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
    // The index fast path: an indexable spec over a dataset whose
    // current-generation index exists is answered from interval sweeps;
    // anything else (filter, MAXGAP, missing index) scans.
    if crate::paths_sa::spec_is_indexable(spec) {
        if let Some(result) = crate::paths_sa::run_from_index(io, dataset, spec, anchor, cfg)? {
            return Ok(result);
        }
    }
    run_paths_scan(io, dataset, spec, anchor, cfg)
}

/// One event of an actor's collapsed path stream: its code, its MAXGAP
/// fragment, and where its representative row lives (for anchor checks).
pub(crate) struct StreamEvent {
    pub(crate) code: u64,
    pub(crate) fragment: u32,
    pub(crate) file: usize,
    pub(crate) row: usize,
}

/// Stripe count of the fingerprint space: sorting and run-scans split by the
/// fingerprint's top bits and run one stripe per core.
const FP_STRIPES: usize = 16;

/// The stripe of a fingerprint.
fn stripe_of(fingerprint: u128) -> usize {
    (fingerprint >> 124) as usize & (FP_STRIPES - 1)
}

/// The 128-bit fingerprint of one window: xxh3 over the codes' little-endian
/// bytes, seeded by the window length so a sequence and its prefix stay
/// distinct. Distinct windows collide with probability about d^2 / 2^129 for
/// d distinct windows - below 1e-20 at a billion - and the top rows are
/// materialized from real code sequences, never decoded from the hash.
fn window_fingerprint(window: &[StreamEvent], scratch: &mut Vec<u8>) -> u128 {
    scratch.clear();
    for event in window {
        scratch.extend_from_slice(&event.code.to_le_bytes());
    }
    xxhash_rust::xxh3::xxh3_128_with_seed(scratch, window.len() as u64)
}

/// One shard's enumerated windows, buffered so the candidate pass re-walks
/// memory instead of re-scanning the dataset: the shard's collapsed streams
/// concatenated as one code buffer, one record per ADMITTED window
/// (fingerprint, buffer offset, length), and the fingerprints pre-split by
/// stripe for the global sort.
struct ShardWindows {
    codes: Vec<u32>,
    windows: Vec<(u128, u32, u32)>,
    stripes: Vec<Vec<u128>>,
}

/// Approximate resident bytes of the buffered windows of a scan, for the
/// query memory budget: the window records, their striped fingerprint
/// copies, and the code buffers.
fn scan_buffer_bytes(windows: usize, codes: usize) -> u64 {
    windows as u64 * (24 + 16) + codes as u64 * 4
}

/// The filtered/gapped paths scan: ONE sharded enumeration pass buffers every
/// admitted window, the fingerprints sort per stripe (counts are run
/// lengths), and the candidate pass walks the buffered records with cheap
/// rank compares - the TOP candidates are every fingerprint counted above
/// the TOP-th count plus the name-order best of those exactly at it.
fn run_paths_scan(
    io: &StoreIo,
    dataset: &Dataset,
    spec: &PathsSpec,
    anchor: Option<(u64, bool)>,
    cfg: &EventsConfig,
) -> Result<PathsResult, EventError> {
    let mut shards: Vec<ShardWindows> = Vec::new();
    let mut held = 0u64;
    run_sharded(
        io,
        dataset,
        cfg,
        |scan| paths_shard_windows(scan, dataset, spec, anchor),
        |shard| {
            held += scan_buffer_bytes(shard.windows.len(), shard.codes.len());
            if held > cfg.query_memory_bytes {
                return Err(EventUnsupported::QueryMemory {
                    analysis: "PATHS".to_string(),
                    budget: cfg.query_memory_bytes,
                }
                .into());
            }
            shards.push(shard);
            Ok(())
        },
    )?;
    let total: u64 = shards.iter().map(|shard| shard.windows.len() as u64).sum();
    let sorted = sort_stripes(&mut shards);
    let top = spec.top as usize;
    let (threshold, over_set, edge_fps) = threshold_and_over(&sorted, top);
    let ranks = code_ranks(&dataset.event_dict.values);
    let room = top - over_set.len();
    let mut over: FastMap<u128, Vec<u64>> = FastMap::default();
    let mut edge: Vec<(u128, Vec<u64>)> = Vec::new();
    let picks: Vec<ShardPicks> = std::thread::scope(|scope| {
        let handles: Vec<_> = shards
            .iter()
            .map(|shard| {
                let (over_set, ranks, edge_fps) = (&over_set, &ranks, &edge_fps);
                scope.spawn(move || {
                    pick_candidates(shard, over_set, ranks, edge_fps, threshold, room)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("candidate thread panicked"))
            .collect()
    });
    for pick in picks {
        for (fingerprint, codes) in pick.over {
            over.entry(fingerprint).or_insert(codes);
        }
        for (fingerprint, codes) in pick.edge {
            if !edge.iter().any(|(seen, _)| *seen == fingerprint) {
                edge.push((fingerprint, codes));
            }
        }
    }
    let mut candidates: Vec<(u64, Vec<u64>)> = Vec::new();
    for (fingerprint, codes) in over {
        candidates.push((fingerprint_count(&sorted, fingerprint), codes));
    }
    for (_fingerprint, codes) in edge {
        candidates.push((threshold, codes));
    }
    candidates.sort_unstable_by(|a, b| {
        b.0.cmp(&a.0).then_with(|| code_order(&ranks, &a.1, &b.1))
    });
    candidates.truncate(top);
    Ok(render_paths(dataset, candidates, total))
}

/// Drain every shard's stripe vectors into globally sorted stripes, one
/// stripe per thread; a fingerprint's count is its run length.
fn sort_stripes(shards: &mut [ShardWindows]) -> Vec<Vec<u128>> {
    // Move each shard's stripe vectors out first so the threads share an
    // immutable borrow.
    let donated: Vec<Vec<Vec<u128>>> = shards
        .iter_mut()
        .map(|shard| std::mem::take(&mut shard.stripes))
        .collect();
    let donated = &donated;
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..FP_STRIPES)
            .map(|stripe| {
                scope.spawn(move || {
                    let mut merged: Vec<u128> = Vec::with_capacity(
                        donated.iter().map(|maps| maps[stripe].len()).sum(),
                    );
                    for maps in donated {
                        merged.extend_from_slice(&maps[stripe]);
                    }
                    merged.sort_unstable();
                    merged
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("stripe sort thread panicked"))
            .collect()
    })
}

/// The threshold statistics of the sorted stripes: the TOP-th largest run
/// count (0 when fewer distinct fingerprints exist), the fingerprints
/// counted ABOVE it (always fewer than TOP), and - when the threshold is
/// above 1 - each stripe's fingerprints counted EXACTLY AT it, kept in
/// stripe-sorted order for the candidate pass's membership searches. Both
/// walks are sequential run scans; nothing is materialized per distinct
/// fingerprint.
fn threshold_and_over(sorted: &[Vec<u128>], top: usize) -> (u64, Vec<u128>, Vec<Vec<u128>>) {
    let per_stripe: Vec<(usize, Vec<u64>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = sorted
            .iter()
            .map(|stripe| {
                scope.spawn(move || {
                    let mut distinct = 0usize;
                    let mut smallest: std::collections::BinaryHeap<std::cmp::Reverse<u64>> =
                        std::collections::BinaryHeap::with_capacity(top + 1);
                    for_each_run(stripe, |_, count| {
                        distinct += 1;
                        smallest.push(std::cmp::Reverse(count));
                        if smallest.len() > top {
                            smallest.pop();
                        }
                    });
                    (distinct, smallest.into_iter().map(|r| r.0).collect::<Vec<u64>>())
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("threshold thread panicked"))
            .collect()
    });
    let mut distinct = 0usize;
    let mut smallest: std::collections::BinaryHeap<std::cmp::Reverse<u64>> =
        std::collections::BinaryHeap::with_capacity(top + 1);
    for (stripe_distinct, counts) in per_stripe {
        distinct += stripe_distinct;
        for count in counts {
            smallest.push(std::cmp::Reverse(count));
            if smallest.len() > top {
                smallest.pop();
            }
        }
    }
    let threshold = if distinct < top {
        0
    } else {
        smallest.peek().map_or(0, |reverse| reverse.0)
    };
    let picked: Vec<(Vec<u128>, Vec<u128>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = sorted
            .iter()
            .map(|stripe| {
                scope.spawn(move || {
                    let mut over = Vec::new();
                    let mut edge = Vec::new();
                    for_each_run(stripe, |fingerprint, count| {
                        if count > threshold {
                            over.push(fingerprint);
                        } else if threshold > 1 && count == threshold {
                            edge.push(fingerprint);
                        }
                    });
                    (over, edge)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("over thread panicked"))
            .collect()
    });
    let mut over = Vec::new();
    let mut edges = Vec::with_capacity(FP_STRIPES);
    for (stripe_over, stripe_edge) in picked {
        over.extend(stripe_over);
        edges.push(stripe_edge);
    }
    (threshold, over, edges)
}

/// Walk one sorted stripe's equal runs as (fingerprint, run length).
fn for_each_run(stripe: &[u128], mut visit: impl FnMut(u128, u64)) {
    let mut offset = 0usize;
    while offset < stripe.len() {
        let fingerprint = stripe[offset];
        let mut end = offset + 1;
        while end < stripe.len() && stripe[end] == fingerprint {
            end += 1;
        }
        visit(fingerprint, (end - offset) as u64);
        offset = end;
    }
}

/// The exact count of one fingerprint: the width of its equal range in its
/// sorted stripe.
fn fingerprint_count(sorted: &[Vec<u128>], fingerprint: u128) -> u64 {
    let stripe = &sorted[stripe_of(fingerprint)];
    let lo = stripe.partition_point(|probe| *probe < fingerprint);
    let hi = stripe.partition_point(|probe| *probe <= fingerprint);
    (hi - lo) as u64
}

/// One shard's candidate-pass result.
struct ShardPicks {
    over: Vec<(u128, Vec<u64>)>,
    edge: Vec<(u128, Vec<u64>)>,
}

/// Walk one shard's buffered windows: record the code sequence of every
/// over-threshold fingerprint on first sight, and keep the name-order best
/// `room` windows AT the threshold. At-threshold membership tests search the
/// precomputed edge fingerprints (nothing at threshold 1: every fingerprint
/// outside the over set then counts exactly 1), and once the best set fills,
/// a cheap name compare rejects almost every window first.
fn pick_candidates(
    shard: &ShardWindows,
    over_set: &[u128],
    ranks: &[u32],
    edge_fps: &[Vec<u128>],
    threshold: u64,
    room: usize,
) -> ShardPicks {
    let mut over: Vec<(u128, Vec<u64>)> = Vec::new();
    let mut edge: Vec<(u128, Vec<u64>)> = Vec::new();
    for (fingerprint, offset, length) in &shard.windows {
        let window = &shard.codes[*offset as usize..(*offset + *length) as usize];
        if over_set.contains(fingerprint) {
            if !over.iter().any(|(seen, _)| seen == fingerprint) {
                over.push((*fingerprint, window.iter().map(|code| u64::from(*code)).collect()));
            }
            continue;
        }
        if room == 0 {
            continue;
        }
        if edge.len() >= room {
            let worst = &edge[edge.len() - 1].1;
            if window_vs_codes(ranks, window, worst) != std::cmp::Ordering::Less {
                continue;
            }
        }
        if edge.iter().any(|(seen, _)| seen == fingerprint) {
            continue;
        }
        if threshold > 1
            && edge_fps[stripe_of(*fingerprint)]
                .binary_search(fingerprint)
                .is_err()
        {
            continue;
        }
        let codes: Vec<u64> = window.iter().map(|code| u64::from(*code)).collect();
        let at = edge.partition_point(|(_, seen)| {
            code_order(ranks, seen, &codes) == std::cmp::Ordering::Less
        });
        edge.insert(at, (*fingerprint, codes));
        edge.truncate(room);
    }
    ShardPicks { over, edge }
}

/// Window order of a raw u32-code window against a u64-code sequence:
/// elementwise event-name rank, shorter prefix first.
fn window_vs_codes(ranks: &[u32], window: &[u32], codes: &[u64]) -> std::cmp::Ordering {
    let shared = window.len().min(codes.len());
    for offset in 0..shared {
        let ra = ranks[window[offset] as usize];
        let rb = ranks[usize::try_from(codes[offset]).expect("code fits usize")];
        match ra.cmp(&rb) {
            std::cmp::Ordering::Equal => {}
            unequal => return unequal,
        }
    }
    window.len().cmp(&codes.len())
}

/// Event-name rank per code: the code's position in the dictionary's names
/// sorted ascending, so count ties order by integer compares.
fn code_ranks(names: &[String]) -> Vec<u32> {
    let mut order: Vec<u32> = (0..u32::try_from(names.len()).expect("dictionary fits u32"))
        .collect();
    order.sort_by(|a, b| names[*a as usize].cmp(&names[*b as usize]));
    let mut ranks = vec![0u32; names.len()];
    for (rank, code) in order.iter().enumerate() {
        ranks[*code as usize] = u32::try_from(rank).expect("rank fits u32");
    }
    ranks
}

/// Window order for count ties: elementwise event-name rank, shorter prefix
/// first - the order the rendered rows sort by.
fn code_order(ranks: &[u32], a: &[u64], b: &[u64]) -> std::cmp::Ordering {
    let shared = a.len().min(b.len());
    for offset in 0..shared {
        let ra = ranks[usize::try_from(a[offset]).expect("code fits usize")];
        let rb = ranks[usize::try_from(b[offset]).expect("code fits usize")];
        match ra.cmp(&rb) {
            std::cmp::Ordering::Equal => {}
            unequal => return unequal,
        }
    }
    a.len().cmp(&b.len())
}

/// Enumerate one shard's windows into a [`ShardWindows`] buffer: build each
/// actor's gap-fragmented, duplicate-collapsed stream, then walk it per the
/// anchor variant. The range and WHERE filter are tested on each window's
/// ANCHOR event only - its first event, or the ENDING AT anchor - so a window
/// whose anchor qualifies is followed past the range end.
fn paths_shard_windows(
    scan: &ShardScan<'_>,
    dataset: &Dataset,
    spec: &PathsSpec,
    anchor: Option<(u64, bool)>,
) -> Result<ShardWindows, EventError> {
    let context = ScanContext::new(dataset, spec.filter.as_ref(), spec.range)?;
    let mut buffered = ShardWindows {
        codes: Vec::new(),
        windows: Vec::new(),
        stripes: (0..FP_STRIPES).map(|_| Vec::new()).collect(),
    };
    let mut stream: Vec<StreamEvent> = Vec::new();
    let mut scratch: Vec<u8> = Vec::new();
    let constrained =
        context.from != i64::MIN || context.to != i64::MAX || context.filter.is_some();
    for run in scan.actor_runs() {
        stream.clear();
        let mut previous_ts: Option<i64> = None;
        let mut fragment = 0u32;
        scan.for_each_event(&run, |file, row, code| {
            // A gap past MAXGAP between consecutive events breaks the stream
            // BEFORE collapsing: duplicates in different fragments stay
            // distinct.
            if let Some(maxgap) = spec.maxgap_micros {
                let ts = scan.files[file].ts.i64_at(row)?;
                if let Some(previous) = previous_ts {
                    if ts - previous > maxgap {
                        fragment += 1;
                    }
                }
                previous_ts = Some(ts);
            }
            match stream.last() {
                Some(last) if last.code == code && last.fragment == fragment => {}
                _ => stream.push(StreamEvent {
                    code,
                    fragment,
                    file,
                    row,
                }),
            }
            Ok(true)
        })?;
        let mut anchor_ok = |index: usize| -> Result<bool, EventError> {
            if !constrained {
                return Ok(true);
            }
            let event = &stream[index];
            let file_scan = &scan.files[event.file];
            let ts = file_scan.ts.i64_at(event.row)?;
            Ok(context.visible(file_scan, event.row, ts)?)
        };
        let base = u32::try_from(buffered.codes.len()).expect("shard buffer fits u32");
        for event in &stream {
            buffered
                .codes
                .push(u32::try_from(event.code).expect("dictionary code fits u32"));
        }
        enumerate_windows(&stream, spec, anchor, &mut |window, start| {
            let fingerprint = window_fingerprint(window, &mut scratch);
            buffered.windows.push((
                fingerprint,
                base + u32::try_from(start).expect("stream offset fits u32"),
                u32::try_from(window.len()).expect("window length fits u32"),
            ));
            buffered.stripes[stripe_of(fingerprint)].push(fingerprint);
            Ok(())
        }, &mut anchor_ok)?;
    }
    Ok(buffered)
}

/// Emit one actor's windows per the anchor variant as (window slice, stream
/// start index), keeping only windows whose anchor event passes `anchor_ok`.
fn enumerate_windows(
    stream: &[StreamEvent],
    spec: &PathsSpec,
    anchor: Option<(u64, bool)>,
    emit: &mut impl FnMut(&[StreamEvent], usize) -> Result<(), EventError>,
    anchor_ok: &mut impl FnMut(usize) -> Result<bool, EventError>,
) -> Result<(), EventError> {
    let depth = spec.depth as usize;
    let n = stream.len();
    for start in 0..n {
        let (code, fragment) = (stream[start].code, stream[start].fragment);
        match anchor {
            None => {
                // Unanchored: the full-length forward window only, anchored on
                // its first event.
                let end = start + depth;
                if end <= n && stream[end - 1].fragment == fragment && anchor_ok(start)? {
                    emit(&stream[start..end], start)?;
                }
            }
            Some((anchor_code, true)) => {
                // Forward flow: every anchor occurrence emits its forward
                // window, truncated at fragment end, length >= 2.
                if code == anchor_code && anchor_ok(start)? {
                    let mut end = start + 1;
                    while end < n && end - start < depth && stream[end].fragment == fragment {
                        end += 1;
                    }
                    if end - start >= 2 {
                        emit(&stream[start..end], start)?;
                    }
                }
            }
            Some((anchor_code, false)) => {
                // Backward flow: the DEPTH-1 events before each anchor, in
                // forward order ending at it, truncated at fragment start.
                if code == anchor_code && anchor_ok(start)? {
                    let mut begin = start;
                    while begin > 0
                        && start + 1 - begin < depth
                        && stream[begin - 1].fragment == fragment
                    {
                        begin -= 1;
                    }
                    if start + 1 - begin >= 2 {
                        emit(&stream[begin..=start], begin)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Render ranked candidates: decode code sequences to names and compute
/// shares against the pre-limit window total. The candidates arrive already
/// sorted by (count desc, name order asc) and truncated to TOP.
fn render_paths(
    dataset: &Dataset,
    candidates: Vec<(u64, Vec<u64>)>,
    total: u64,
) -> PathsResult {
    let rows = candidates
        .into_iter()
        .enumerate()
        .map(|(index, (count, codes))| PathsRow {
            rank: u32::try_from(index).expect("rank fits u32") + 1,
            steps: codes
                .into_iter()
                .map(|code| dataset.event_name(code).to_string())
                .collect(),
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
