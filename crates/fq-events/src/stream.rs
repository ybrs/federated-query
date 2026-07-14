//! The sorted event stream the kernels scan: role columns normalized to one
//! representation per role, plus a row cursor that VERIFIES the storage
//! contract as it reads: (entity, timestamp) non-decreasing, the tiebreak
//! non-decreasing within equal (entity, timestamp) when the view declares
//! one, and no NULL roles. A kernel never returns numbers computed over a
//! stream whose ordering premise is broken - the cursor raises
//! `ContractViolation` first.

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType as ArrowType, SchemaRef, TimeUnit};
use fq_common::events::{EventRoleColumns, EventWindow, WindowUnit};

use crate::error::EventError;

/// The native unit of the view's timestamp column. Kernel time arithmetic
/// runs in this unit exactly (no lossy normalization across units).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeScale {
    Seconds,
    Millis,
    Micros,
    Nanos,
    /// A DATE column: whole days since the epoch.
    Days,
}

impl TimeScale {
    /// The scale of an Arrow timestamp-role type; any other type has no event
    /// time semantics and raises.
    pub fn of(arrow: &ArrowType) -> Result<Self, EventError> {
        match arrow {
            ArrowType::Timestamp(TimeUnit::Second, _) => Ok(TimeScale::Seconds),
            ArrowType::Timestamp(TimeUnit::Millisecond, _) | ArrowType::Date64 => {
                Ok(TimeScale::Millis)
            }
            ArrowType::Timestamp(TimeUnit::Microsecond, _) => Ok(TimeScale::Micros),
            ArrowType::Timestamp(TimeUnit::Nanosecond, _) => Ok(TimeScale::Nanos),
            ArrowType::Date32 => Ok(TimeScale::Days),
            other => Err(EventError::InvalidRoles(format!(
                "timestamp role has type {other}, which is not a TIMESTAMP or DATE"
            ))),
        }
    }

    /// Convert a window duration into this scale's native unit with exact
    /// integer arithmetic. Over a DATE column only whole-day units are
    /// meaningful, so a sub-day unit raises; an overflowing window raises.
    pub fn window_in_native(self, window: EventWindow) -> Result<i64, EventError> {
        if window.count <= 0 {
            return Err(EventError::Analysis(format!(
                "WITHIN needs a positive duration, got {}",
                window.count
            )));
        }
        if self == TimeScale::Days {
            if window.unit != WindowUnit::Days {
                return Err(EventError::Analysis(
                    "the view's timestamp column is a DATE; WITHIN must use DAYS".to_string(),
                ));
            }
            return Ok(window.count);
        }
        window
            .count
            .checked_mul(window.unit.seconds())
            .and_then(|seconds| seconds.checked_mul(self.per_second()))
            .ok_or_else(|| {
                EventError::Analysis(format!(
                    "WITHIN {} overflows the timestamp domain",
                    window.count
                ))
            })
    }

    /// Native units per second for the sub-day scales. `Days` never reaches
    /// here (`window_in_native` handles it before multiplying).
    fn per_second(self) -> i64 {
        match self {
            TimeScale::Seconds => 1,
            TimeScale::Millis => 1_000,
            TimeScale::Micros => 1_000_000,
            TimeScale::Nanos => 1_000_000_000,
            TimeScale::Days => unreachable!("day-scaled windows are whole day counts"),
        }
    }

    /// The UTC instant of a native timestamp value, for calendar bucketing.
    /// A value outside chrono's representable range raises.
    pub fn to_utc(self, native: i64) -> Result<chrono::DateTime<chrono::Utc>, EventError> {
        let out_of_range =
            || EventError::Analysis(format!("timestamp value {native} is out of range"));
        match self {
            TimeScale::Seconds => {
                chrono::DateTime::from_timestamp(native, 0).ok_or_else(out_of_range)
            }
            TimeScale::Millis => {
                chrono::DateTime::from_timestamp_millis(native).ok_or_else(out_of_range)
            }
            TimeScale::Micros => {
                chrono::DateTime::from_timestamp_micros(native).ok_or_else(out_of_range)
            }
            TimeScale::Nanos => Ok(chrono::DateTime::from_timestamp_nanos(native)),
            TimeScale::Days => {
                let seconds = native.checked_mul(86_400).ok_or_else(out_of_range)?;
                chrono::DateTime::from_timestamp(seconds, 0).ok_or_else(out_of_range)
            }
        }
    }
}

/// One row's entity value, borrowed from its batch. A view's entity column
/// has one type, so mixed variants never meet in one stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EntityRef<'a> {
    Int(i64),
    Text(&'a str),
}

impl EntityRef<'_> {
    /// An owned copy, taken only at entity boundaries (kernels that must
    /// remember an entity across rows clone here, never per row).
    pub fn to_owned_key(self) -> EntityKey {
        match self {
            EntityRef::Int(value) => EntityKey::Int(value),
            EntityRef::Text(text) => EntityKey::Text(text.to_string()),
        }
    }
}

/// An owned entity value a kernel holds across rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityKey {
    Int(i64),
    Text(String),
}

impl EntityKey {
    /// Whether this held key equals a row's borrowed entity value.
    pub fn matches(&self, entity: EntityRef<'_>) -> bool {
        match (self, entity) {
            (EntityKey::Int(held), EntityRef::Int(row)) => *held == row,
            (EntityKey::Text(held), EntityRef::Text(row)) => held == row,
            _ => false,
        }
    }
}

/// One row's tiebreak value, borrowed from its batch. A view's tiebreak
/// column has one type, so mixed variants never meet in one stream and the
/// derived cross-variant ordering is never consulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TiebreakRef<'a> {
    Int(i64),
    Text(&'a str),
}

/// One event row the cursor yields.
#[derive(Debug)]
pub struct EventRow<'a> {
    pub entity: EntityRef<'a>,
    /// The timestamp in the stream's native `TimeScale` unit.
    pub time: i64,
    pub event: &'a str,
    /// The tiebreak value; None on a view with no declared tiebreak.
    pub tiebreak: Option<TiebreakRef<'a>>,
    /// True on the first row of each entity's contiguous run.
    pub new_entity: bool,
}

/// A sorted set of candidate entity values a kernel restricts its scan to. The
/// variant matches the view's entity type; values are ascending - the order the
/// sorted stream stores entities in - so a kernel seeks forward through it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntitySet {
    Int(Vec<i64>),
    Text(Vec<String>),
}

impl EntitySet {
    /// The number of candidate entities.
    pub fn len(&self) -> usize {
        match self {
            EntitySet::Int(values) => values.len(),
            EntitySet::Text(values) => values.len(),
        }
    }

    /// Whether the candidate set is empty (no entity can contribute).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The entity column of one batch, normalized to a single width per kind.
enum EntityColumn {
    Int(Int64Array),
    Text(StringArray),
}

/// A tiebreak column normalized to a single representation per kind (dates
/// and timestamps keep their raw native-unit values as Int64).
enum TiebreakColumn {
    Int(Int64Array),
    Text(StringArray),
}

/// One batch's normalized role columns.
struct EventBatch {
    entity: EntityColumn,
    time: Int64Array,
    event: StringArray,
    tiebreak: Option<TiebreakColumn>,
    rows: usize,
}

/// A whole event view's rows, normalized for the kernels.
pub struct EventStream {
    batches: Vec<EventBatch>,
    scale: TimeScale,
}

impl EventStream {
    /// Normalize `batches` for scanning: resolve the role columns by name,
    /// enforce the role type contract, raise on any NULL role value, and cast
    /// each role column to its single kernel representation.
    pub fn open(
        view: &str,
        schema: &SchemaRef,
        batches: &[RecordBatch],
        roles: &EventRoleColumns,
    ) -> Result<Self, EventError> {
        validate_roles(view, schema, batches, roles)?;
        let indices = role_indices(schema, roles)?;
        let scale = TimeScale::of(schema.field(indices.time).data_type())?;
        let mut normalized = Vec::with_capacity(batches.len());
        for batch in batches {
            normalized.push(EventBatch {
                entity: normalize_entity(batch.column(indices.entity))?,
                time: cast_to_int64(batch.column(indices.time))?,
                event: cast_to_utf8(batch.column(indices.event))?,
                tiebreak: match indices.tiebreak {
                    Some(index) => Some(normalize_tiebreak(batch.column(index))?),
                    None => None,
                },
                rows: batch.num_rows(),
            });
        }
        Ok(Self {
            batches: normalized,
            scale,
        })
    }

    /// The native unit of the stream's timestamps.
    pub fn scale(&self) -> TimeScale {
        self.scale
    }

    /// A contract-verifying cursor over every row, in storage order.
    pub fn rows(&self) -> EventRows<'_> {
        EventRows {
            stream: self,
            batch: 0,
            row: 0,
            previous: None,
        }
    }

    /// Whether the whole view is one stored batch. The candidate scan seeks by
    /// binary search within a single sorted batch; a multi-batch stream (which
    /// a whole-rewrite event view never produces) has the kernel keep its plain
    /// full scan instead of pruning.
    pub fn is_single_batch(&self) -> bool {
        self.batches.len() == 1
    }

    /// A contract-verifying cursor over ONLY the rows of the candidate
    /// entities, in storage order. Each candidate's contiguous run is found by
    /// binary search over the single sorted batch, so non-candidate entities
    /// are never touched. The rows yielded are a subsequence of `rows()`, so a
    /// kernel folding them reaches the same numbers it would over the full scan
    /// restricted to those entities. Only valid on a single-batch stream.
    pub fn candidate_rows<'a>(&'a self, set: &EntitySet) -> CandidateRows<'a> {
        let ranges = if self.batches.len() == 1 {
            candidate_ranges(&self.batches[0], set)
        } else {
            Vec::new()
        };
        CandidateRows {
            stream: self,
            ranges,
            range_index: 0,
            row: 0,
            previous: None,
        }
    }

    /// The (entity, time, event, tiebreak) values at a batch row - the shared
    /// accessor of both cursors.
    fn row_values(
        &self,
        batch: usize,
        row: usize,
    ) -> (EntityRef<'_>, i64, &str, Option<TiebreakRef<'_>>) {
        let batch = &self.batches[batch];
        let entity = match &batch.entity {
            EntityColumn::Int(values) => EntityRef::Int(values.value(row)),
            EntityColumn::Text(values) => EntityRef::Text(values.value(row)),
        };
        let tiebreak = match &batch.tiebreak {
            Some(TiebreakColumn::Int(values)) => Some(TiebreakRef::Int(values.value(row))),
            Some(TiebreakColumn::Text(values)) => Some(TiebreakRef::Text(values.value(row))),
            None => None,
        };
        (
            entity,
            batch.time.value(row),
            batch.event.value(row),
            tiebreak,
        )
    }
}

/// The half-open row ranges of each candidate entity within one sorted batch,
/// in candidate order (ascending, the same order as the batch). A candidate the
/// view has no rows for contributes no range. A candidate set whose type does
/// not match the batch's entity column yields nothing.
fn candidate_ranges(batch: &EventBatch, set: &EntitySet) -> Vec<(usize, usize)> {
    match (&batch.entity, set) {
        (EntityColumn::Int(column), EntitySet::Int(values)) => int_ranges(column, values),
        (EntityColumn::Text(column), EntitySet::Text(values)) => text_ranges(column, values),
        _ => Vec::new(),
    }
}

/// The row ranges of each candidate integer entity, via `partition_point` over
/// the sorted values slice.
fn int_ranges(column: &Int64Array, values: &[i64]) -> Vec<(usize, usize)> {
    let slice = column.values();
    let mut ranges = Vec::with_capacity(values.len());
    for value in values {
        let low = slice.partition_point(|entry| entry < value);
        let high = slice.partition_point(|entry| entry <= value);
        if high > low {
            ranges.push((low, high));
        }
    }
    ranges
}

/// The row ranges of each candidate text entity, via binary search over the
/// sorted string column.
fn text_ranges(column: &StringArray, values: &[String]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::with_capacity(values.len());
    for value in values {
        let low = lower_bound(column, value);
        let high = upper_bound(column, value);
        if high > low {
            ranges.push((low, high));
        }
    }
    ranges
}

/// The first row whose entity is >= `value` in a sorted string column.
fn lower_bound(column: &StringArray, value: &str) -> usize {
    let (mut low, mut high) = (0usize, column.len());
    while low < high {
        let mid = low + (high - low) / 2;
        if column.value(mid) < value {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

/// The first row whose entity is > `value` in a sorted string column.
fn upper_bound(column: &StringArray, value: &str) -> usize {
    let (mut low, mut high) = (0usize, column.len());
    while low < high {
        let mid = low + (high - low) / 2;
        if column.value(mid) <= value {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

/// The cursor `EventStream::candidate_rows` returns: the rows of the candidate
/// entities only, each range a distinct entity's full contiguous run, verifying
/// ordering as it reads.
pub struct CandidateRows<'a> {
    stream: &'a EventStream,
    ranges: Vec<(usize, usize)>,
    range_index: usize,
    row: usize,
    previous: Option<(EntityRef<'a>, i64, Option<TiebreakRef<'a>>)>,
}

impl<'a> CandidateRows<'a> {
    /// Verify the visited subsequence stays ordered: a new entity must exceed
    /// the previous one, and within an entity timestamps (then tiebreaks at
    /// equal timestamps) must not regress.
    fn check_order(
        &self,
        entity: EntityRef<'a>,
        time: i64,
        tiebreak: Option<TiebreakRef<'a>>,
        new_entity: bool,
    ) -> Result<(), EventError> {
        let Some((previous_entity, previous_time, previous_tiebreak)) = self.previous else {
            return Ok(());
        };
        if new_entity {
            if entity <= previous_entity {
                return Err(EventError::ContractViolation(format!(
                    "candidate entities regress: {entity:?} after {previous_entity:?}"
                )));
            }
            return Ok(());
        }
        if time < previous_time {
            return Err(EventError::ContractViolation(format!(
                "timestamps regress within entity {entity:?}: {time} after {previous_time}"
            )));
        }
        if time == previous_time && tiebreak < previous_tiebreak {
            return Err(EventError::ContractViolation(format!(
                "tiebreak regresses within entity {entity:?} at timestamp {time}"
            )));
        }
        Ok(())
    }
}

impl<'a> Iterator for CandidateRows<'a> {
    type Item = Result<EventRow<'a>, EventError>;

    /// The next candidate row, or the contract violation that stops the scan.
    fn next(&mut self) -> Option<Self::Item> {
        while self.range_index < self.ranges.len() {
            let (start, end) = self.ranges[self.range_index];
            if self.row < start {
                self.row = start;
            }
            if self.row >= end {
                self.range_index += 1;
                self.row = 0;
                continue;
            }
            let new_entity = self.row == start;
            let (entity, time, event, tiebreak) = self.stream.row_values(0, self.row);
            self.row += 1;
            if let Err(error) = self.check_order(entity, time, tiebreak, new_entity) {
                return Some(Err(error));
            }
            self.previous = Some((entity, time, tiebreak));
            return Some(Ok(EventRow {
                entity,
                time,
                event,
                tiebreak,
                new_entity,
            }));
        }
        None
    }
}

/// The cursor `EventStream::rows` returns: yields each row and raises
/// `ContractViolation` the moment (entity, timestamp[, tiebreak]) ordering
/// regresses.
pub struct EventRows<'a> {
    stream: &'a EventStream,
    batch: usize,
    row: usize,
    previous: Option<(EntityRef<'a>, i64, Option<TiebreakRef<'a>>)>,
}

impl<'a> EventRows<'a> {
    /// The (entity, time, event, tiebreak) values at the cursor's current
    /// position.
    fn current(&self) -> (EntityRef<'a>, i64, &'a str, Option<TiebreakRef<'a>>) {
        let batch = &self.stream.batches[self.batch];
        let entity = match &batch.entity {
            EntityColumn::Int(values) => EntityRef::Int(values.value(self.row)),
            EntityColumn::Text(values) => EntityRef::Text(values.value(self.row)),
        };
        let tiebreak = match &batch.tiebreak {
            Some(TiebreakColumn::Int(values)) => Some(TiebreakRef::Int(values.value(self.row))),
            Some(TiebreakColumn::Text(values)) => Some(TiebreakRef::Text(values.value(self.row))),
            None => None,
        };
        (
            entity,
            batch.time.value(self.row),
            batch.event.value(self.row),
            tiebreak,
        )
    }

    /// Verify (entity, timestamp[, tiebreak]) did not regress from the
    /// previous row, and tell whether this row starts a new entity run.
    fn check_order(
        &self,
        entity: EntityRef<'a>,
        time: i64,
        tiebreak: Option<TiebreakRef<'a>>,
    ) -> Result<bool, EventError> {
        let Some((previous_entity, previous_time, previous_tiebreak)) = self.previous else {
            return Ok(true);
        };
        if entity == previous_entity {
            if time < previous_time {
                return Err(EventError::ContractViolation(format!(
                    "timestamps regress within entity {entity:?}: {time} after {previous_time}"
                )));
            }
            if time == previous_time && tiebreak < previous_tiebreak {
                return Err(EventError::ContractViolation(format!(
                    "tiebreak regresses within entity {entity:?} at timestamp {time}: \
                     {tiebreak:?} after {previous_tiebreak:?}"
                )));
            }
            return Ok(false);
        }
        if entity < previous_entity {
            return Err(EventError::ContractViolation(format!(
                "entities regress: {entity:?} after {previous_entity:?}; the stream must be \
                 sorted by (entity, timestamp)"
            )));
        }
        Ok(true)
    }
}

impl<'a> Iterator for EventRows<'a> {
    type Item = Result<EventRow<'a>, EventError>;

    /// The next row in storage order, or the contract violation that stops
    /// the scan.
    fn next(&mut self) -> Option<Self::Item> {
        while self.batch < self.stream.batches.len() {
            if self.row >= self.stream.batches[self.batch].rows {
                self.batch += 1;
                self.row = 0;
                continue;
            }
            let (entity, time, event, tiebreak) = self.current();
            self.row += 1;
            let new_entity = match self.check_order(entity, time, tiebreak) {
                Ok(new_entity) => new_entity,
                Err(error) => return Some(Err(error)),
            };
            self.previous = Some((entity, time, tiebreak));
            return Some(Ok(EventRow {
                entity,
                time,
                event,
                tiebreak,
                new_entity,
            }));
        }
        None
    }
}

/// The schema positions of an event view's role columns; `tiebreak` is None
/// on a view with no declared tiebreak.
pub struct RoleIndices {
    pub entity: usize,
    pub time: usize,
    pub event: usize,
    pub tiebreak: Option<usize>,
}

impl RoleIndices {
    /// Every declared role as (schema index, role keyword, column name), in
    /// declaration order - the shared walk for distinctness and NULL checks.
    fn declared<'a>(&self, roles: &'a EventRoleColumns) -> Vec<(usize, &'static str, &'a str)> {
        let mut declared = vec![
            (self.entity, "ENTITY", roles.entity.as_str()),
            (self.time, "TIMESTAMP", roles.timestamp.as_str()),
            (self.event, "EVENT", roles.event.as_str()),
        ];
        if let Some(index) = self.tiebreak {
            let column = roles
                .tiebreak
                .as_deref()
                .expect("a tiebreak index implies a declared tiebreak column");
            declared.push((index, "TIEBREAK", column));
        }
        declared
    }
}

/// Check batches against the role contract WITHOUT materializing the normalized
/// kernel columns: resolve the roles, enforce the type contract, and raise on
/// any NULL role value. This is the whole check `EventStream::open` runs before
/// it normalizes; the materialization path needs only the check, so it validates
/// through here and never clones a column of the result.
pub fn validate_roles(
    view: &str,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    roles: &EventRoleColumns,
) -> Result<(), EventError> {
    let indices = role_indices(schema, roles)?;
    TimeScale::of(schema.field(indices.time).data_type())?;
    require_entity_type(schema.field(indices.entity).data_type())?;
    require_text_type("event", schema.field(indices.event).data_type())?;
    if let Some(tiebreak_index) = indices.tiebreak {
        require_tiebreak_type(schema.field(tiebreak_index).data_type())?;
    }
    for batch in batches {
        require_no_role_nulls(view, batch, roles, &indices)?;
    }
    Ok(())
}

/// Resolve the role columns to schema indices, raising on a role that names
/// no output column or on two roles sharing one column.
pub fn role_indices(
    schema: &SchemaRef,
    roles: &EventRoleColumns,
) -> Result<RoleIndices, EventError> {
    let indices = RoleIndices {
        entity: index_of(schema, "ENTITY", &roles.entity)?,
        time: index_of(schema, "TIMESTAMP", &roles.timestamp)?,
        event: index_of(schema, "EVENT", &roles.event)?,
        tiebreak: match &roles.tiebreak {
            Some(column) => Some(index_of(schema, "TIEBREAK", column)?),
            None => None,
        },
    };
    require_distinct_roles(&indices, roles)?;
    Ok(indices)
}

/// Raise when two declared roles share one column: a shared column can carry
/// only one role's semantics (a tiebreak equal to a role column in particular
/// adds no ordering information and is a declaration mistake).
fn require_distinct_roles(
    indices: &RoleIndices,
    roles: &EventRoleColumns,
) -> Result<(), EventError> {
    let declared = indices.declared(roles);
    for (position, (index, _, _)) in declared.iter().enumerate() {
        for (later_index, _, _) in &declared[position + 1..] {
            if index != later_index {
                continue;
            }
            let mut rendered = Vec::with_capacity(declared.len());
            for (_, role, column) in &declared {
                rendered.push(format!("{role} '{column}'"));
            }
            return Err(EventError::InvalidRoles(format!(
                "the roles must name distinct columns, got {}",
                rendered.join(", ")
            )));
        }
    }
    Ok(())
}

/// The schema index of one role's column, or a loud error naming the role,
/// the missing column, and the columns that do exist.
fn index_of(schema: &SchemaRef, role: &str, column: &str) -> Result<usize, EventError> {
    let position = schema
        .fields()
        .iter()
        .position(|field| field.name() == column);
    position.ok_or_else(|| {
        let mut names = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            names.push(field.name().clone());
        }
        EventError::InvalidRoles(format!(
            "{role} role names column '{column}', which the defining SELECT does not \
             produce (its columns are: {})",
            names.join(", ")
        ))
    })
}

/// Enforce the entity role's type contract: integers or text. Anything else
/// has no defined identity semantics for sequence analysis.
pub fn require_entity_type(arrow: &ArrowType) -> Result<(), EventError> {
    if is_integer(arrow) || is_text(arrow) {
        return Ok(());
    }
    Err(EventError::InvalidRoles(format!(
        "entity role has type {arrow}; it must be an integer or text column"
    )))
}

/// Enforce a text-typed role (the event name).
pub fn require_text_type(role: &str, arrow: &ArrowType) -> Result<(), EventError> {
    if is_text(arrow) {
        return Ok(());
    }
    Err(EventError::InvalidRoles(format!(
        "{role} role has type {arrow}; it must be a text column"
    )))
}

/// Enforce the tiebreak role's type contract: an orderable EXACT type -
/// integer, text, date, or timestamp. Approximate or two-valued types (float,
/// bool) cannot carry a total per-row order and are refused, never coerced.
pub fn require_tiebreak_type(arrow: &ArrowType) -> Result<(), EventError> {
    let orderable_exact = is_integer(arrow)
        || is_text(arrow)
        || matches!(
            arrow,
            ArrowType::Date32 | ArrowType::Date64 | ArrowType::Timestamp(_, _)
        );
    if orderable_exact {
        return Ok(());
    }
    Err(EventError::InvalidRoles(format!(
        "tiebreak role has type {arrow}; it must be an integer, text, date, or \
         timestamp column"
    )))
}

/// Whether an Arrow type is an integer the entity role accepts (widths that
/// widen losslessly to Int64).
fn is_integer(arrow: &ArrowType) -> bool {
    matches!(
        arrow,
        ArrowType::Int8
            | ArrowType::Int16
            | ArrowType::Int32
            | ArrowType::Int64
            | ArrowType::UInt8
            | ArrowType::UInt16
            | ArrowType::UInt32
    )
}

/// Whether an Arrow type is a text type the entity/event roles accept.
fn is_text(arrow: &ArrowType) -> bool {
    matches!(
        arrow,
        ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View
    )
}

/// Raise `ContractViolation` if any role column of `batch` holds a NULL,
/// naming the view, the column, and the first offending row ordinal.
fn require_no_role_nulls(
    view: &str,
    batch: &RecordBatch,
    roles: &EventRoleColumns,
    indices: &RoleIndices,
) -> Result<(), EventError> {
    for (index, _, name) in indices.declared(roles) {
        let column = batch.column(index);
        if column.null_count() == 0 {
            continue;
        }
        let first_null = (0..column.len())
            .find(|row| column.is_null(*row))
            .expect("null_count > 0 implies a null row");
        return Err(EventError::ContractViolation(format!(
            "event view '{view}': role column '{name}' is NULL at row {first_null}; \
             every role value must be present on every event"
        )));
    }
    Ok(())
}

/// Normalize an entity column to Int64 or Utf8 (one representation per kind;
/// the widening casts are lossless under the role type contract).
fn normalize_entity(column: &ArrayRef) -> Result<EntityColumn, EventError> {
    if is_integer(column.data_type()) {
        return Ok(EntityColumn::Int(cast_to_int64(column)?));
    }
    Ok(EntityColumn::Text(cast_to_utf8(column)?))
}

/// Normalize a tiebreak column to Int64 or Utf8. Integers widen losslessly;
/// dates and timestamps keep their raw native-unit values, which preserve
/// their order exactly.
fn normalize_tiebreak(column: &ArrayRef) -> Result<TiebreakColumn, EventError> {
    if is_text(column.data_type()) {
        return Ok(TiebreakColumn::Text(cast_to_utf8(column)?));
    }
    Ok(TiebreakColumn::Int(cast_to_int64(column)?))
}

/// Cast a column to Int64 (timestamps keep their raw native-unit values).
fn cast_to_int64(column: &ArrayRef) -> Result<Int64Array, EventError> {
    let casted = cast(column, &ArrowType::Int64)?;
    Ok(casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cast to Int64 yields Int64Array")
        .clone())
}

/// Cast a column to Utf8.
fn cast_to_utf8(column: &ArrayRef) -> Result<StringArray, EventError> {
    let casted = cast(column, &ArrowType::Utf8)?;
    Ok(casted
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 yields StringArray")
        .clone())
}

/// The chunk-stored `Arc<Schema>` and rows the tests build streams from.
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema};
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

    /// The test roles with the Int32 `seq` column as the tiebreak.
    fn roles_with_tiebreak() -> EventRoleColumns {
        let mut roles = roles();
        roles.tiebreak = Some("seq".to_string());
        roles
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

    /// A batch like `batch` plus an Int32 `seq` tiebreak column.
    fn batch_with_seq(rows: &[(i32, i64, &str, i32)]) -> (SchemaRef, RecordBatch) {
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
        let mut users = Vec::new();
        let mut times = Vec::new();
        let mut names = Vec::new();
        let mut seqs = Vec::new();
        for (user, time, name, seq) in rows {
            users.push(*user);
            times.push(*time);
            names.push(*name);
            seqs.push(*seq);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(users)),
                Arc::new(TimestampMicrosecondArray::from(times)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(seqs)),
            ],
        )
        .expect("batch");
        (schema, batch)
    }

    #[test]
    fn cursor_yields_rows_with_entity_boundaries() {
        let (schema, data) = batch(&[(1, 10, "a"), (1, 20, "b"), (2, 5, "a")]);
        let stream = EventStream::open("ev", &schema, &[data], &roles()).expect("open");
        let mut rows = Vec::new();
        for row in stream.rows() {
            let row = row.expect("row");
            rows.push((row.time, row.new_entity));
        }
        assert_eq!(rows, vec![(10, true), (20, false), (5, true)]);
    }

    #[test]
    fn unsorted_timestamps_within_an_entity_raise() {
        let (schema, data) = batch(&[(1, 20, "a"), (1, 10, "b")]);
        let stream = EventStream::open("ev", &schema, &[data], &roles()).expect("open");
        let error = stream.rows().nth(1).expect("second row").unwrap_err();
        assert!(matches!(error, EventError::ContractViolation(_)), "{error}");
    }

    #[test]
    fn a_reappearing_entity_raises() {
        let (schema, data) = batch(&[(1, 10, "a"), (2, 10, "a"), (1, 30, "b")]);
        let stream = EventStream::open("ev", &schema, &[data], &roles()).expect("open");
        let error = stream.rows().nth(2).expect("third row").unwrap_err();
        assert!(matches!(error, EventError::ContractViolation(_)), "{error}");
    }

    #[test]
    fn a_null_role_value_raises_naming_column_and_row() {
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
        let Err(error) = EventStream::open("ev", &schema, &[data], &roles()) else {
            panic!("a NULL role value must raise");
        };
        let text = format!("{error}");
        assert!(text.contains("user_id") && text.contains("row 1"), "{text}");
    }

    #[test]
    fn a_missing_role_column_raises_listing_the_columns() {
        let (schema, data) = batch(&[(1, 10, "a")]);
        let mut bad = roles();
        bad.entity = "uzer_id".to_string();
        let Err(error) = EventStream::open("ev", &schema, &[data], &bad) else {
            panic!("a missing role column must raise");
        };
        let text = format!("{error}");
        assert!(
            text.contains("uzer_id") && text.contains("user_id"),
            "{text}"
        );
    }

    #[test]
    fn a_regressing_tiebreak_within_equal_timestamps_raises() {
        let (schema, data) = batch_with_seq(&[(1, 10, "a", 2), (1, 10, "b", 1)]);
        let stream =
            EventStream::open("ev", &schema, &[data], &roles_with_tiebreak()).expect("open");
        let error = stream.rows().nth(1).expect("second row").unwrap_err();
        let text = format!("{error}");
        assert!(
            matches!(error, EventError::ContractViolation(_)) && text.contains("tiebreak"),
            "{text}"
        );
    }

    #[test]
    fn the_tiebreak_orders_only_within_equal_timestamps() {
        // The tiebreak may regress freely when the timestamp advances or the
        // entity changes; only an equal-(entity, timestamp) pair constrains it.
        let (schema, data) = batch_with_seq(&[
            (1, 10, "a", 5),
            (1, 10, "b", 6),
            (1, 20, "c", 1),
            (2, 5, "d", 0),
        ]);
        let stream =
            EventStream::open("ev", &schema, &[data], &roles_with_tiebreak()).expect("open");
        let mut tiebreaks = Vec::new();
        for row in stream.rows() {
            let row = row.expect("row");
            tiebreaks.push(row.tiebreak.expect("tiebreak present"));
        }
        assert_eq!(
            tiebreaks,
            vec![
                TiebreakRef::Int(5),
                TiebreakRef::Int(6),
                TiebreakRef::Int(1),
                TiebreakRef::Int(0),
            ]
        );
    }

    #[test]
    fn a_null_tiebreak_value_raises_naming_column_and_row() {
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
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(TimestampMicrosecondArray::from(vec![10, 10])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![Some(1), None])),
            ],
        )
        .expect("batch");
        let Err(error) = EventStream::open("ev", &schema, &[data], &roles_with_tiebreak()) else {
            panic!("a NULL tiebreak value must raise");
        };
        let text = format!("{error}");
        assert!(text.contains("seq") && text.contains("row 1"), "{text}");
    }

    #[test]
    fn an_unorderable_tiebreak_type_raises() {
        // Float and boolean columns are not exact orderable tiebreaks.
        for unorderable in [ArrowType::Float64, ArrowType::Boolean] {
            let error = require_tiebreak_type(&unorderable).unwrap_err();
            assert!(matches!(error, EventError::InvalidRoles(_)), "{error}");
        }
        // The accepted exact types all pass.
        for orderable in [
            ArrowType::Int32,
            ArrowType::Int64,
            ArrowType::Utf8,
            ArrowType::Date32,
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
        ] {
            require_tiebreak_type(&orderable).expect("orderable exact type");
        }
    }

    #[test]
    fn a_tiebreak_sharing_a_role_column_raises() {
        let (schema, data) = batch(&[(1, 10, "a")]);
        let mut shared = roles();
        shared.tiebreak = Some("ts".to_string());
        let Err(error) = EventStream::open("ev", &schema, &[data], &shared) else {
            panic!("a tiebreak sharing a role column must raise");
        };
        let text = format!("{error}");
        assert!(
            text.contains("distinct") && text.contains("TIEBREAK"),
            "{text}"
        );
    }

    #[test]
    fn window_conversion_is_exact_per_scale() {
        let window = EventWindow {
            count: 2,
            unit: WindowUnit::Hours,
        };
        assert_eq!(
            TimeScale::Micros.window_in_native(window).expect("us"),
            2 * 3600 * 1_000_000
        );
        assert_eq!(
            TimeScale::Seconds.window_in_native(window).expect("s"),
            7200
        );
        // A DATE column accepts whole-day windows only.
        let days = EventWindow {
            count: 7,
            unit: WindowUnit::Days,
        };
        assert_eq!(TimeScale::Days.window_in_native(days).expect("d"), 7);
        assert!(TimeScale::Days.window_in_native(window).is_err());
        // Non-positive windows raise.
        let zero = EventWindow {
            count: 0,
            unit: WindowUnit::Days,
        };
        assert!(TimeScale::Micros.window_in_native(zero).is_err());
    }
}
