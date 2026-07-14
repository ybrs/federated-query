//! Derived structures built beside an event view's chunks, rebuilt with each
//! generation and consulted by the kernels to skip work. Two structures share
//! one build scan over the sorted stream:
//!
//! - [`EntityBitmaps`]: one roaring bitmap of entity ordinals per event name,
//!   over a dictionary of the view's distinct entities. FUNNEL reads step 1's
//!   bitmap popcount as its exact step-1 count and scans only the entities in
//!   `step1 & step2`; anchored PATHS scans only the anchor event's entities.
//! - [`SegmentAggregate`]: per-(calendar-bucket, event-name) event counts and
//!   distinct-entity counts, stored at day / week / month grain. SEGMENT BY
//!   DAY / WEEK / MONTH answers from it; BY HOUR falls back to the scan.
//!
//! Both are DERIVED: a caller that cannot load them (a missing file, a
//! generation mismatch, a read error) falls back to the plain scan, which
//! returns the identical answer. Nothing here is a source of truth; the chunks
//! are.

use std::collections::HashMap;

use arrow::array::{
    Array, Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use chrono::{DateTime, Utc};
use fq_common::events::{EventRoleColumns, SegmentMeasure, SegmentSpec, TimeBucket};
use roaring::RoaringBitmap;
use std::sync::Arc;

use crate::error::EventError;
use crate::segment::bucket_start_micros;
use crate::stream::{EntityRef, EntitySet, EventStream, TimeScale};

/// A magic word framing the bitmap sidecar; a file that does not open with it
/// is not one of ours and the caller falls back to the scan.
const BITMAP_MAGIC: u32 = 0x4657_4245; // "FWBE"

/// The bitmap sidecar format version; a file at another version is treated as
/// unloadable (fall back to the scan), never misread.
const BITMAP_VERSION: u8 = 1;

/// The distinct entity values of a view, in ascending order - the dense
/// ordinal space the bitmaps index. A view's entity column has one type, so a
/// dictionary is all-integer or all-text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityDictionary {
    Int(Vec<i64>),
    Text(Vec<String>),
}

impl EntityDictionary {
    /// The number of distinct entities.
    pub fn len(&self) -> usize {
        match self {
            EntityDictionary::Int(values) => values.len(),
            EntityDictionary::Text(values) => values.len(),
        }
    }

    /// Whether the view has no entities at all.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The entity values at the given ascending ordinals, as an [`EntitySet`]
    /// the kernels can seek with.
    fn take_ordinals(&self, ordinals: impl Iterator<Item = u32>) -> EntitySet {
        match self {
            EntityDictionary::Int(values) => {
                let mut out = Vec::new();
                for ordinal in ordinals {
                    out.push(values[ordinal as usize]);
                }
                EntitySet::Int(out)
            }
            EntityDictionary::Text(values) => {
                let mut out = Vec::new();
                for ordinal in ordinals {
                    out.push(values[ordinal as usize].clone());
                }
                EntitySet::Text(out)
            }
        }
    }
}

/// One roaring bitmap of entity ordinals per event name, over a shared entity
/// dictionary. Built at refresh from the sorted stream; the funnel and
/// anchored-paths kernels consult it to scan only candidate entities.
#[derive(Debug, Clone)]
pub struct EntityBitmaps {
    dictionary: EntityDictionary,
    per_event: HashMap<String, RoaringBitmap>,
}

impl EntityBitmaps {
    /// The distinct-entity count of the view (the dictionary size).
    pub fn entity_count(&self) -> usize {
        self.dictionary.len()
    }

    /// The number of distinct entities that emitted at least one event named
    /// `name` - a funnel's exact step-1 count, read without any scan.
    pub fn event_entities(&self, name: &str) -> u64 {
        self.per_event.get(name).map_or(0, RoaringBitmap::len)
    }

    /// The candidate entities a funnel must scan: those with BOTH a step-1 and
    /// a step-2 event, the only entities that can reach depth >= 2. Entities
    /// with step 1 but not step 2 have depth exactly 1 and are counted by
    /// `event_entities(step1)`, so omitting them from the scan is exact.
    pub fn funnel_candidates(&self, step1: &str, step2: &str) -> EntitySet {
        let empty = RoaringBitmap::new();
        let first = self.per_event.get(step1).unwrap_or(&empty);
        let second = self.per_event.get(step2).unwrap_or(&empty);
        let both = first & second;
        self.dictionary.take_ordinals(both.iter())
    }

    /// The candidate entities an anchored PATHS must scan: those with at least
    /// one anchor event. An entity without the anchor contributes no path, so
    /// omitting it is exact.
    pub fn anchor_candidates(&self, anchor: &str) -> EntitySet {
        match self.per_event.get(anchor) {
            Some(bitmap) => self.dictionary.take_ordinals(bitmap.iter()),
            None => match &self.dictionary {
                EntityDictionary::Int(_) => EntitySet::Int(Vec::new()),
                EntityDictionary::Text(_) => EntitySet::Text(Vec::new()),
            },
        }
    }

    /// Serialize to the framed bitmap format: magic and version, the entity
    /// dictionary, then each event name with its roaring bitmap blob.
    pub fn to_bytes(&self) -> Result<Vec<u8>, EventError> {
        let mut out = Vec::new();
        out.extend_from_slice(&BITMAP_MAGIC.to_le_bytes());
        out.push(BITMAP_VERSION);
        write_dictionary(&mut out, &self.dictionary);
        write_u32(
            &mut out,
            u32::try_from(self.per_event.len()).expect("event count fits u32"),
        );
        // A stable name order keeps the bytes a function of the data alone, so
        // a rebuild over identical rows yields identical bytes.
        let mut names: Vec<&String> = self.per_event.keys().collect();
        names.sort_unstable();
        for name in names {
            write_str(&mut out, name);
            let mut blob = Vec::new();
            self.per_event[name]
                .serialize_into(&mut blob)
                .expect("roaring serialize into a vec never fails");
            write_u32(
                &mut out,
                u32::try_from(blob.len()).expect("bitmap blob fits u32"),
            );
            out.extend_from_slice(&blob);
        }
        Ok(out)
    }

    /// Parse the framed bitmap format. A wrong magic or version raises so the
    /// caller falls back to the scan rather than trusting a foreign file.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, EventError> {
        let mut reader = ByteReader::new(bytes);
        if reader.read_u32()? != BITMAP_MAGIC {
            return Err(bitmap_format("not a bitmap sidecar (bad magic)"));
        }
        if reader.read_u8()? != BITMAP_VERSION {
            return Err(bitmap_format("bitmap sidecar version mismatch"));
        }
        let dictionary = read_dictionary(&mut reader)?;
        let event_count = reader.read_u32()?;
        let mut per_event = HashMap::with_capacity(event_count as usize);
        for _ in 0..event_count {
            let name = reader.read_str()?;
            let blob_len = reader.read_u32()? as usize;
            let blob = reader.read_bytes(blob_len)?;
            let bitmap = RoaringBitmap::deserialize_from(blob)
                .map_err(|error| bitmap_format(&format!("corrupt bitmap blob: {error}")))?;
            per_event.insert(name, bitmap);
        }
        Ok(Self {
            dictionary,
            per_event,
        })
    }

    /// Load a bitmap sidecar from `path`, or a format/io error the caller turns
    /// into a scan fallback.
    pub fn load(path: &str) -> Result<Self, EventError> {
        let bytes = std::fs::read(path)?;
        Self::from_bytes(&bytes)
    }
}

/// The distinct-entity dictionary and per-event bitmaps assembled during the
/// build scan. Entities are pushed in ascending ordinal order (the stream is
/// entity-sorted), so the dictionary is sorted by construction.
struct BitmapAccum {
    dictionary: Option<EntityDictionary>,
    per_event: HashMap<String, RoaringBitmap>,
}

impl BitmapAccum {
    /// An empty accumulator; the dictionary kind is fixed by the first entity.
    fn new() -> Self {
        Self {
            dictionary: None,
            per_event: HashMap::new(),
        }
    }

    /// Record a new entity's value at the next ordinal (called once per entity,
    /// at its run boundary, in ascending order).
    fn push_entity(&mut self, entity: EntityRef<'_>) {
        match (&mut self.dictionary, entity) {
            (Some(EntityDictionary::Int(values)), EntityRef::Int(value)) => values.push(value),
            (Some(EntityDictionary::Text(values)), EntityRef::Text(text)) => {
                values.push(text.to_string());
            }
            (None, EntityRef::Int(value)) => {
                self.dictionary = Some(EntityDictionary::Int(vec![value]));
            }
            (None, EntityRef::Text(text)) => {
                self.dictionary = Some(EntityDictionary::Text(vec![text.to_string()]));
            }
            _ => unreachable!("a view's entity column has one type for the whole stream"),
        }
    }

    /// Record that the entity at `ordinal` emitted an event named `event`.
    fn push_event(&mut self, ordinal: u32, event: &str) {
        self.per_event
            .entry(event.to_string())
            .or_default()
            .insert(ordinal);
    }

    /// The finished bitmaps (an empty view yields an empty dictionary).
    fn finish(self) -> EntityBitmaps {
        EntityBitmaps {
            dictionary: self.dictionary.unwrap_or(EntityDictionary::Int(Vec::new())),
            per_event: self.per_event,
        }
    }
}

/// The measure a pre-aggregate cell counts, matching [`SegmentMeasure`].
const MEASURE_EVENTS: i8 = 0;
const MEASURE_ENTITIES: i8 = 1;

/// The calendar grain a pre-aggregate cell is bucketed at. EVENTS are stored
/// only at day grain (they sum up); ENTITIES are stored at every grain (a
/// distinct count does not sum across days).
const GRAIN_DAY: i8 = 0;
const GRAIN_WEEK: i8 = 1;
const GRAIN_MONTH: i8 = 2;

/// One pre-aggregate cell: a measure over either one event name or all events
/// (`is_all`) in one calendar bucket at one grain. `is_all` is an explicit flag,
/// not an empty-name sentinel, so an event literally named "" never collides
/// with the all-events cell.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Cell {
    measure: i8,
    grain: i8,
    bucket_micros: i64,
    is_all: bool,
    event: String,
    value: i64,
}

impl Cell {
    /// Whether this cell answers a segment filter: the all-events cell for no
    /// filter, or exactly the named event's cell for a filter.
    fn matches(&self, filter: Option<&str>) -> bool {
        match filter {
            None => self.is_all,
            Some(name) => !self.is_all && self.event == name,
        }
    }
}

/// The time-bucketed pre-aggregate: the flat cell list SEGMENT answers from,
/// plus the Arrow schema it serializes under. Built at refresh in the same scan
/// as the bitmaps.
#[derive(Debug, Clone)]
pub struct SegmentAggregate {
    cells: Vec<Cell>,
}

impl SegmentAggregate {
    /// Answer a SEGMENT statement from the pre-aggregate, or None when it must
    /// fall back to the scan (a BY HOUR bucket, which the day-grain aggregate
    /// cannot answer without over-counting).
    pub fn serve(
        &self,
        spec: &SegmentSpec,
    ) -> Result<Option<(SchemaRef, Vec<RecordBatch>)>, EventError> {
        let Some(grain) = grain_of(spec.bucket) else {
            return Ok(None);
        };
        let filter = spec.event.as_deref();
        let mut buckets: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
        match spec.measure {
            SegmentMeasure::Events => self.gather_events(spec.bucket, filter, &mut buckets),
            SegmentMeasure::Entities => self.gather_entities(grain, filter, &mut buckets),
        }
        Ok(Some((
            crate::segment::segment_schema(),
            vec![segment_result(&buckets)?],
        )))
    }

    /// Sum the day-grain EVENTS cells matching the event filter into the target
    /// bucket (a day rolls up to its own week or month; a day is unchanged for
    /// BY DAY). EVENTS are additive across days, so summing is exact.
    fn gather_events(
        &self,
        bucket: TimeBucket,
        filter: Option<&str>,
        buckets: &mut std::collections::BTreeMap<i64, i64>,
    ) {
        for cell in &self.cells {
            if cell.measure != MEASURE_EVENTS || cell.grain != GRAIN_DAY || !cell.matches(filter) {
                continue;
            }
            let target = rollup_day(cell.bucket_micros, bucket);
            *buckets.entry(target).or_insert(0) += cell.value;
        }
    }

    /// Read the ENTITIES cells stored at the requested grain directly - a
    /// distinct count is stored per grain because it cannot be summed from a
    /// finer one.
    fn gather_entities(
        &self,
        grain: i8,
        filter: Option<&str>,
        buckets: &mut std::collections::BTreeMap<i64, i64>,
    ) {
        for cell in &self.cells {
            if cell.measure != MEASURE_ENTITIES || cell.grain != grain || !cell.matches(filter) {
                continue;
            }
            buckets.insert(cell.bucket_micros, cell.value);
        }
    }

    /// Serialize the cells as one Arrow IPC batch (the sidecar file).
    pub fn to_bytes(&self) -> Result<Vec<u8>, EventError> {
        let batch = self.to_batch()?;
        let schema = batch.schema();
        let mut buffer = Vec::new();
        let mut writer = FileWriter::try_new(&mut buffer, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);
        Ok(buffer)
    }

    /// Reconstruct the pre-aggregate from its Arrow IPC bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, EventError> {
        let reader = FileReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)?;
        let mut cells = Vec::new();
        for batch in reader {
            append_cells(&batch?, &mut cells)?;
        }
        Ok(Self { cells })
    }

    /// Load a pre-aggregate sidecar from `path`, or an error the caller turns
    /// into a scan fallback.
    pub fn load(path: &str) -> Result<Self, EventError> {
        let bytes = std::fs::read(path)?;
        Self::from_bytes(&bytes)
    }

    /// The cells as one Arrow batch under the fixed sidecar schema.
    fn to_batch(&self) -> Result<RecordBatch, EventError> {
        let mut measures = Vec::with_capacity(self.cells.len());
        let mut grains = Vec::with_capacity(self.cells.len());
        let mut micros = Vec::with_capacity(self.cells.len());
        let mut all_flags = Vec::with_capacity(self.cells.len());
        let mut events = Vec::with_capacity(self.cells.len());
        let mut values = Vec::with_capacity(self.cells.len());
        for cell in &self.cells {
            measures.push(cell.measure);
            grains.push(cell.grain);
            micros.push(cell.bucket_micros);
            all_flags.push(i8::from(cell.is_all));
            events.push(cell.event.clone());
            values.push(cell.value);
        }
        Ok(RecordBatch::try_new(
            segagg_schema(),
            vec![
                Arc::new(Int8Array::from(measures)),
                Arc::new(Int8Array::from(grains)),
                Arc::new(TimestampMicrosecondArray::from(micros)),
                Arc::new(Int8Array::from(all_flags)),
                Arc::new(StringArray::from(events)),
                Arc::new(Int64Array::from(values)),
            ],
        )?)
    }
}

/// The per-grain event-name interner and cell tables assembled during the build
/// scan. All distinct dedup uses the entity ORDINAL, which is monotonic across
/// the entity-sorted stream, so "a new distinct entity for this cell" is
/// "ordinal changed since this cell last counted one".
struct SegmentAccum {
    event_index: HashMap<String, usize>,
    event_names: Vec<String>,
    events_day: HashMap<i64, DayCounts>,
    entities: [HashMap<i64, DistinctCounts>; 3],
}

/// One day's EVENTS counts: the all-events total plus a per-event-index total.
struct DayCounts {
    all: i64,
    per_event: Vec<i64>,
}

/// One bucket's ENTITIES distinct counts at one grain: the all-events distinct
/// plus a per-event-index distinct, each carrying the last ordinal counted.
struct DistinctCounts {
    all: DistinctCell,
    per_event: Vec<DistinctCell>,
}

/// A running distinct count and the ordinal it last incremented on.
struct DistinctCell {
    count: i64,
    last_ordinal: i64,
}

impl DistinctCell {
    /// A fresh cell that has counted nothing (ordinal -1 never equals a real
    /// ordinal, so the first entity always counts).
    fn empty() -> Self {
        Self {
            count: 0,
            last_ordinal: -1,
        }
    }

    /// Count `ordinal` unless it is the one already counted here.
    fn observe(&mut self, ordinal: i64) {
        if self.last_ordinal != ordinal {
            self.count += 1;
            self.last_ordinal = ordinal;
        }
    }
}

impl SegmentAccum {
    /// An empty accumulator.
    fn new() -> Self {
        Self {
            event_index: HashMap::new(),
            event_names: Vec::new(),
            events_day: HashMap::new(),
            entities: [HashMap::new(), HashMap::new(), HashMap::new()],
        }
    }

    /// The dense index of an event name, interning a new one.
    fn intern(&mut self, event: &str) -> usize {
        if let Some(index) = self.event_index.get(event) {
            return *index;
        }
        let index = self.event_names.len();
        self.event_names.push(event.to_string());
        self.event_index.insert(event.to_string(), index);
        index
    }

    /// Fold one event into every cell it belongs to: the day EVENTS counts and
    /// the day / week / month ENTITIES distinct counts, for both the all-events
    /// cell and this event's cell.
    fn push(
        &mut self,
        scale: TimeScale,
        ordinal: i64,
        time: i64,
        event: &str,
    ) -> Result<(), EventError> {
        let instant = scale.to_utc(time)?;
        let event_index = self.intern(event);
        let day = bucket_start_micros(instant, TimeBucket::Day);
        self.count_day_events(day, event_index);
        let grain_buckets = [
            (0usize, day),
            (1, bucket_start_micros(instant, TimeBucket::Week)),
            (2, bucket_start_micros(instant, TimeBucket::Month)),
        ];
        for (grain, bucket) in grain_buckets {
            self.count_distinct(grain, bucket, event_index, ordinal);
        }
        Ok(())
    }

    /// Increment the day's all-events and per-event EVENTS counts.
    fn count_day_events(&mut self, day: i64, event_index: usize) {
        let counts = self.events_day.entry(day).or_insert_with(|| DayCounts {
            all: 0,
            per_event: Vec::new(),
        });
        counts.all += 1;
        ensure_len(&mut counts.per_event, event_index + 1, 0);
        counts.per_event[event_index] += 1;
    }

    /// Observe the entity ordinal in one grain bucket's all-events and
    /// per-event distinct cells.
    fn count_distinct(&mut self, grain: usize, bucket: i64, event_index: usize, ordinal: i64) {
        let counts = self.entities[grain]
            .entry(bucket)
            .or_insert_with(|| DistinctCounts {
                all: DistinctCell::empty(),
                per_event: Vec::new(),
            });
        counts.all.observe(ordinal);
        while counts.per_event.len() <= event_index {
            counts.per_event.push(DistinctCell::empty());
        }
        counts.per_event[event_index].observe(ordinal);
    }

    /// Flatten the tables into the sidecar's cell list.
    fn finish(self) -> SegmentAggregate {
        let mut cells = Vec::new();
        self.emit_events(&mut cells);
        self.emit_entities(&mut cells);
        SegmentAggregate { cells }
    }

    /// Emit the day-grain EVENTS cells (all-events and each event).
    fn emit_events(&self, cells: &mut Vec<Cell>) {
        for (day, counts) in &self.events_day {
            cells.push(all_cell(MEASURE_EVENTS, GRAIN_DAY, *day, counts.all));
            for (index, value) in counts.per_event.iter().enumerate() {
                if *value > 0 {
                    cells.push(event_cell(
                        MEASURE_EVENTS,
                        GRAIN_DAY,
                        *day,
                        &self.event_names[index],
                        *value,
                    ));
                }
            }
        }
    }

    /// Emit the ENTITIES cells at every grain (all-events and each event).
    fn emit_entities(&self, cells: &mut Vec<Cell>) {
        let grain_codes = [GRAIN_DAY, GRAIN_WEEK, GRAIN_MONTH];
        for (grain, table) in self.entities.iter().enumerate() {
            for (bucket, counts) in table {
                cells.push(all_cell(
                    MEASURE_ENTITIES,
                    grain_codes[grain],
                    *bucket,
                    counts.all.count,
                ));
                for (index, distinct) in counts.per_event.iter().enumerate() {
                    if distinct.count > 0 {
                        cells.push(event_cell(
                            MEASURE_ENTITIES,
                            grain_codes[grain],
                            *bucket,
                            &self.event_names[index],
                            distinct.count,
                        ));
                    }
                }
            }
        }
    }
}

/// Whether restricting a scan to `candidates` out of `entities` total is worth
/// the pruning overhead. The candidate scan reads only the candidates' rows but
/// pays a binary-search seek per candidate; when the candidates are most of the
/// entity space (an event-dense view where nearly every entity emits the step
/// events), the plain linear scan is cheaper. Measured on a 100M-event view
/// whose entities each emit ~200 events, `signup & begin_checkout` covers ~99.8%
/// of entities and pruning is a net loss, so pruning engages only below an 85%
/// share.
pub fn worth_pruning(candidates: usize, entities: usize) -> bool {
    entities > 0 && candidates * 20 < entities * 17
}

/// Both sidecars produced by one build scan over the sorted stream.
pub struct SidecarBuild {
    pub bitmaps: EntityBitmaps,
    pub segment: SegmentAggregate,
}

/// Build both sidecars in a single pass over the (entity, timestamp)-sorted
/// stream: assign each entity a dense ordinal at its run boundary, insert that
/// ordinal into each event name's bitmap, and fold each event into the
/// time-bucketed pre-aggregate. Opening the stream re-runs the full role and
/// null contract, so a build over a broken stream raises here.
pub fn build_sidecars(
    view: &str,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    roles: &EventRoleColumns,
) -> Result<SidecarBuild, EventError> {
    let stream = EventStream::open(view, schema, batches, roles)?;
    let scale = stream.scale();
    let mut bitmaps = BitmapAccum::new();
    let mut segment = SegmentAccum::new();
    let mut ordinal: i64 = -1;
    for row in stream.rows() {
        let row = row?;
        if row.new_entity {
            ordinal += 1;
            bitmaps.push_entity(row.entity);
        }
        let dense = u32::try_from(ordinal).expect("entity ordinal fits u32");
        bitmaps.push_event(dense, row.event);
        segment.push(scale, ordinal, row.time, row.event)?;
    }
    Ok(SidecarBuild {
        bitmaps: bitmaps.finish(),
        segment: segment.finish(),
    })
}

/// An all-events cell (the whole bucket's measure, over every event name).
fn all_cell(measure: i8, grain: i8, bucket_micros: i64, value: i64) -> Cell {
    Cell {
        measure,
        grain,
        bucket_micros,
        is_all: true,
        event: String::new(),
        value,
    }
}

/// A single-event cell (the bucket's measure over one event name).
fn event_cell(measure: i8, grain: i8, bucket_micros: i64, event: &str, value: i64) -> Cell {
    Cell {
        measure,
        grain,
        bucket_micros,
        is_all: false,
        event: event.to_string(),
        value,
    }
}

/// The grain code a bucket answers at, or None for BY HOUR (no stored grain).
fn grain_of(bucket: TimeBucket) -> Option<i8> {
    match bucket {
        TimeBucket::Hour => None,
        TimeBucket::Day => Some(GRAIN_DAY),
        TimeBucket::Week => Some(GRAIN_WEEK),
        TimeBucket::Month => Some(GRAIN_MONTH),
    }
}

/// Roll a stored day bucket up to the target bucket: a day is unchanged for BY
/// DAY and maps to its own week or month otherwise (a day never spans two, so
/// the mapping is exact). BY HOUR never reaches here.
fn rollup_day(day_micros: i64, target: TimeBucket) -> i64 {
    match target {
        TimeBucket::Day => day_micros,
        TimeBucket::Hour => unreachable!("BY HOUR falls back before rollup"),
        other => {
            let instant = DateTime::<Utc>::from_timestamp_micros(day_micros)
                .expect("a stored day bucket is a representable instant");
            bucket_start_micros(instant, other)
        }
    }
}

/// Assemble the SEGMENT result batch from the gathered buckets, in bucket order.
fn segment_result(
    buckets: &std::collections::BTreeMap<i64, i64>,
) -> Result<RecordBatch, EventError> {
    let mut micros = Vec::with_capacity(buckets.len());
    let mut values = Vec::with_capacity(buckets.len());
    for (bucket, value) in buckets {
        micros.push(*bucket);
        values.push(*value);
    }
    Ok(RecordBatch::try_new(
        crate::segment::segment_schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(micros)),
            Arc::new(Int64Array::from(values)),
        ],
    )?)
}

/// The Arrow schema the pre-aggregate serializes under.
fn segagg_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("measure", ArrowType::Int8, false),
        Field::new("grain", ArrowType::Int8, false),
        Field::new(
            "bucket",
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("is_all", ArrowType::Int8, false),
        Field::new("event", ArrowType::Utf8, false),
        Field::new("value", ArrowType::Int64, false),
    ]))
}

/// Append one loaded Arrow batch's rows to the cell list.
fn append_cells(batch: &RecordBatch, cells: &mut Vec<Cell>) -> Result<(), EventError> {
    let measures = column::<Int8Array>(batch, 0, "measure")?;
    let grains = column::<Int8Array>(batch, 1, "grain")?;
    let micros = column::<TimestampMicrosecondArray>(batch, 2, "bucket")?;
    let all_flags = column::<Int8Array>(batch, 3, "is_all")?;
    let events = column::<StringArray>(batch, 4, "event")?;
    let values = column::<Int64Array>(batch, 5, "value")?;
    for row in 0..batch.num_rows() {
        cells.push(Cell {
            measure: measures.value(row),
            grain: grains.value(row),
            bucket_micros: micros.value(row),
            is_all: all_flags.value(row) != 0,
            event: events.value(row).to_string(),
            value: values.value(row),
        });
    }
    Ok(())
}

/// Downcast a sidecar column, raising when the loaded file has the wrong layout
/// (a corrupt or foreign file, which the caller turns into a scan fallback).
fn column<'a, A: Array + 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a A, EventError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| {
            bitmap_format(&format!(
                "segment sidecar column '{name}' has the wrong type"
            ))
        })
}

/// Grow `values` to at least `len`, filling with `fill`.
fn ensure_len(values: &mut Vec<i64>, len: usize, fill: i64) {
    if values.len() < len {
        values.resize(len, fill);
    }
}

/// A segment/bitmap sidecar format error (a signal to fall back to the scan).
fn bitmap_format(message: &str) -> EventError {
    EventError::Analysis(format!("sidecar unusable: {message}"))
}

/// Write a length-prefixed UTF-8 string.
fn write_str(out: &mut Vec<u8>, value: &str) {
    write_u32(
        out,
        u32::try_from(value.len()).expect("string length fits u32"),
    );
    out.extend_from_slice(value.as_bytes());
}

/// Write a little-endian u32.
fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Write the entity dictionary: a kind byte, a count, then the values.
fn write_dictionary(out: &mut Vec<u8>, dictionary: &EntityDictionary) {
    match dictionary {
        EntityDictionary::Int(values) => {
            out.push(0);
            write_u32(
                out,
                u32::try_from(values.len()).expect("dictionary fits u32"),
            );
            for value in values {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        EntityDictionary::Text(values) => {
            out.push(1);
            write_u32(
                out,
                u32::try_from(values.len()).expect("dictionary fits u32"),
            );
            for value in values {
                write_str(out, value);
            }
        }
    }
}

/// Read the entity dictionary written by `write_dictionary`.
fn read_dictionary(reader: &mut ByteReader<'_>) -> Result<EntityDictionary, EventError> {
    let kind = reader.read_u8()?;
    let count = reader.read_u32()? as usize;
    match kind {
        0 => {
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(reader.read_i64()?);
            }
            Ok(EntityDictionary::Int(values))
        }
        1 => {
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(reader.read_str()?);
            }
            Ok(EntityDictionary::Text(values))
        }
        other => Err(bitmap_format(&format!("unknown dictionary kind {other}"))),
    }
}

/// A bounds-checked forward reader over the bitmap sidecar bytes; a short read
/// raises rather than panicking, so a truncated file becomes a scan fallback.
struct ByteReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> ByteReader<'a> {
    /// A reader positioned at the start of `bytes`.
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// Read `len` bytes, raising on a short buffer.
    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], EventError> {
        let end = self
            .position
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| bitmap_format("truncated sidecar"))?;
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }

    /// Read a little-endian u32.
    fn read_u32(&mut self) -> Result<u32, EventError> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("4 bytes")))
    }

    /// Read a little-endian i64.
    fn read_i64(&mut self) -> Result<i64, EventError> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }

    /// Read one byte.
    fn read_u8(&mut self) -> Result<u8, EventError> {
        Ok(self.read_bytes(1)?[0])
    }

    /// Read a length-prefixed UTF-8 string.
    fn read_str(&mut self) -> Result<String, EventError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|error| bitmap_format(&format!("invalid utf-8 in sidecar: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::build_event_view;
    use crate::funnel::{run_funnel, run_funnel_pruned};
    use crate::paths::{run_paths, run_paths_pruned};
    use crate::segment::run_segment;
    use arrow::array::{Int32Array, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use fq_common::events::{EventWindow, FunnelSpec, PathsSpec, WindowUnit};

    /// One microsecond-scale day.
    const DAY: i64 = 86_400 * 1_000_000;

    /// The standard test roles over (user_id, ts, name), no tiebreak.
    fn roles() -> EventRoleColumns {
        EventRoleColumns {
            entity: "user_id".to_string(),
            timestamp: "ts".to_string(),
            event: "name".to_string(),
            tiebreak: None,
        }
    }

    /// A (user_id Int32, ts Timestamp(us), name Utf8) schema and batch.
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

    /// Build (sort) the rows into a sorted stream, and build the sidecars over
    /// the same sorted batches - the runtime's establish-then-serve pair.
    fn built(rows: &[(i32, i64, &str)]) -> (EventStream, SidecarBuild) {
        let (schema, data) = batch(rows);
        let sorted = build_event_view("ev", &schema, &[data], &roles()).expect("build");
        let stream = EventStream::open("ev", &schema, &sorted, &roles()).expect("open");
        let sidecars = build_sidecars("ev", &schema, &sorted, &roles()).expect("sidecars");
        (stream, sidecars)
    }

    /// A segmentation spec over the test view.
    fn segment_spec(
        measure: SegmentMeasure,
        event: Option<&str>,
        bucket: TimeBucket,
    ) -> SegmentSpec {
        SegmentSpec {
            view: "ev".to_string(),
            measure,
            event: event.map(str::to_string),
            bucket,
        }
    }

    /// Flatten a SEGMENT result to (bucket, value) tuples.
    fn segment_tuples(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
        let mut out = Vec::new();
        for batch in batches {
            let buckets = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("bucket");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value");
            for row in 0..batch.num_rows() {
                out.push((buckets.value(row), values.value(row)));
            }
        }
        out
    }

    /// Assert the pre-aggregate answers a SEGMENT spec identically to the scan.
    fn assert_segment_identity(rows: &[(i32, i64, &str)], spec: &SegmentSpec) {
        let (stream, sidecars) = built(rows);
        let (_, scan) = run_segment(&stream, spec).expect("scan");
        let served = sidecars.segment.serve(spec).expect("serve");
        let (_, sidecar) = served.expect("a day/week/month bucket is served, not None");
        assert_eq!(
            segment_tuples(&scan),
            segment_tuples(&sidecar),
            "sidecar diverged from the scan for {spec:?}"
        );
    }

    /// The multi-day, multi-entity, multi-event fixture the segment identity
    /// tests share: entities that span days, weeks, and a month boundary.
    fn segment_fixture() -> Vec<(i32, i64, &'static str)> {
        vec![
            (1, 0, "view"),
            (1, 100, "view"),
            (1, 2 * DAY, "click"),
            (1, 9 * DAY, "view"),
            (2, DAY, "view"),
            (2, DAY + 50, "click"),
            (2, 33 * DAY, "view"),
            (3, 5 * DAY, "click"),
            (3, 40 * DAY, "click"),
            (4, 6 * DAY, "view"),
        ]
    }

    #[test]
    fn segment_events_and_entities_match_the_scan_at_every_grain() {
        let fixture = segment_fixture();
        for measure in [SegmentMeasure::Events, SegmentMeasure::Entities] {
            for event in [None, Some("view"), Some("click"), Some("absent")] {
                for bucket in [TimeBucket::Day, TimeBucket::Week, TimeBucket::Month] {
                    assert_segment_identity(&fixture, &segment_spec(measure, event, bucket));
                }
            }
        }
    }

    #[test]
    fn segment_hour_falls_back_to_the_scan() {
        let (_, sidecars) = built(&segment_fixture());
        let spec = segment_spec(SegmentMeasure::Events, None, TimeBucket::Hour);
        assert!(
            sidecars.segment.serve(&spec).expect("serve").is_none(),
            "BY HOUR must not be served from the day-grain aggregate"
        );
    }

    #[test]
    fn an_event_literally_named_empty_does_not_collide_with_all_events() {
        // The all-events cell is flagged, not sentinelled by an empty name, so
        // an event named "" is a normal single event: the scan and the sidecar
        // agree for both the no-filter and the EVENT '' cases.
        let rows = vec![(1, 0, ""), (1, 100, "view"), (2, 0, "")];
        for event in [None, Some("")] {
            assert_segment_identity(
                &rows,
                &segment_spec(SegmentMeasure::Entities, event, TimeBucket::Day),
            );
            assert_segment_identity(
                &rows,
                &segment_spec(SegmentMeasure::Events, event, TimeBucket::Day),
            );
        }
    }

    #[test]
    fn an_empty_view_serves_an_empty_segment() {
        let (stream, sidecars) = built(&[]);
        let spec = segment_spec(SegmentMeasure::Entities, None, TimeBucket::Week);
        let (_, scan) = run_segment(&stream, &spec).expect("scan");
        let (_, sidecar) = sidecars.segment.serve(&spec).expect("serve").expect("some");
        assert_eq!(segment_tuples(&scan), segment_tuples(&sidecar));
        assert!(segment_tuples(&sidecar).is_empty());
    }

    #[test]
    fn the_segment_sidecar_round_trips_through_its_bytes() {
        let (_, sidecars) = built(&segment_fixture());
        let bytes = sidecars.segment.to_bytes().expect("to_bytes");
        let loaded = SegmentAggregate::from_bytes(&bytes).expect("from_bytes");
        let spec = segment_spec(SegmentMeasure::Entities, Some("view"), TimeBucket::Week);
        let (_, before) = sidecars.segment.serve(&spec).expect("serve").expect("some");
        let (_, after) = loaded.serve(&spec).expect("serve").expect("some");
        assert_eq!(segment_tuples(&before), segment_tuples(&after));
    }

    /// One microsecond-scale hour, for funnel fixtures.
    const HOUR: i64 = 3600 * 1_000_000;

    /// A three-step funnel spec with a two-hour window.
    fn funnel_spec(steps: &[&str]) -> FunnelSpec {
        let mut owned = Vec::new();
        for step in steps {
            owned.push(step.to_string());
        }
        FunnelSpec {
            view: "ev".to_string(),
            steps: owned,
            within: EventWindow {
                count: 2,
                unit: WindowUnit::Hours,
            },
        }
    }

    /// The per-step entity counts of a funnel result.
    fn funnel_counts(batches: &[RecordBatch]) -> Vec<i64> {
        let mut out = Vec::new();
        for batch in batches {
            let entities = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("entities");
            for row in 0..batch.num_rows() {
                out.push(entities.value(row));
            }
        }
        out
    }

    /// The six-user fixture from the funnel kernel tests, exercising re-entry,
    /// window boundaries, ties, and a purchase-before-activate.
    fn funnel_fixture() -> Vec<(i32, i64, &'static str)> {
        vec![
            (1, 0, "signup"),
            (1, HOUR / 2, "activate"),
            (1, HOUR, "purchase"),
            (2, 0, "signup"),
            (2, 3 * HOUR, "activate"),
            (3, 0, "signup"),
            (3, HOUR / 3, "purchase"),
            (3, HOUR / 2, "activate"),
            (4, 0, "activate"),
            (4, HOUR / 2, "purchase"),
            (5, 0, "signup"),
            (5, 5 * HOUR / 2, "signup"),
            (5, 3 * HOUR, "activate"),
            (5, 7 * HOUR / 2, "purchase"),
            (6, 0, "signup"),
            (6, 0, "activate"),
        ]
    }

    #[test]
    fn pruned_funnel_matches_the_scan_on_the_six_user_fixture() {
        let (stream, sidecars) = built(&funnel_fixture());
        let spec = funnel_spec(&["signup", "activate", "purchase"]);
        let (_, scan) = run_funnel(&stream, &spec).expect("scan");
        let (_, pruned) = run_funnel_pruned(&stream, &spec, &sidecars.bitmaps).expect("pruned");
        assert_eq!(funnel_counts(&scan), funnel_counts(&pruned));
        // The bitmap popcount is the exact step-1 count.
        assert_eq!(
            i64::try_from(sidecars.bitmaps.event_entities("signup")).expect("count fits i64"),
            funnel_counts(&scan)[0]
        );
    }

    #[test]
    fn pruning_engages_only_below_the_selectivity_threshold() {
        // Empty entity space never prunes; a small candidate share prunes; a
        // near-full share (event-dense entities) does not.
        assert!(!worth_pruning(0, 0));
        assert!(worth_pruning(10, 100));
        assert!(worth_pruning(84, 100));
        assert!(!worth_pruning(85, 100));
        assert!(!worth_pruning(998, 1000));
    }

    #[test]
    fn a_dense_candidate_set_falls_back_to_the_scan_identically() {
        // Every entity emits both steps, so the candidate set is 100% and the
        // gate falls back to the plain scan - the answer must still match.
        let rows = vec![
            (1, 0, "a"),
            (1, HOUR, "b"),
            (2, 0, "a"),
            (2, HOUR, "b"),
            (3, 0, "a"),
            (3, HOUR, "b"),
        ];
        let (stream, sidecars) = built(&rows);
        let spec = funnel_spec(&["a", "b"]);
        let (_, scan) = run_funnel(&stream, &spec).expect("scan");
        let (_, gated) = run_funnel_pruned(&stream, &spec, &sidecars.bitmaps).expect("gated");
        assert_eq!(funnel_counts(&scan), funnel_counts(&gated));
        assert_eq!(funnel_counts(&gated), vec![3, 3]);
    }

    #[test]
    fn pruned_funnel_matches_the_scan_with_duplicate_and_rare_steps() {
        let (stream, sidecars) = built(&funnel_fixture());
        for steps in [
            vec!["signup", "signup"],
            vec!["purchase", "activate"],
            vec!["cancel_account", "signup"],
            vec!["signup", "activate", "purchase", "refund"],
        ] {
            let spec = funnel_spec(&steps);
            let (_, scan) = run_funnel(&stream, &spec).expect("scan");
            let (_, pruned) = run_funnel_pruned(&stream, &spec, &sidecars.bitmaps).expect("pruned");
            assert_eq!(
                funnel_counts(&scan),
                funnel_counts(&pruned),
                "steps {steps:?}"
            );
        }
    }

    #[test]
    fn the_bitmap_sidecar_round_trips_through_its_bytes() {
        let (stream, sidecars) = built(&funnel_fixture());
        let bytes = sidecars.bitmaps.to_bytes().expect("to_bytes");
        let loaded = EntityBitmaps::from_bytes(&bytes).expect("from_bytes");
        let spec = funnel_spec(&["signup", "activate", "purchase"]);
        let (_, before) = run_funnel_pruned(&stream, &spec, &sidecars.bitmaps).expect("before");
        let (_, after) = run_funnel_pruned(&stream, &spec, &loaded).expect("after");
        assert_eq!(funnel_counts(&before), funnel_counts(&after));
        assert_eq!(loaded.entity_count(), sidecars.bitmaps.entity_count());
    }

    #[test]
    fn from_bytes_rejects_a_foreign_bitmap_file() {
        assert!(EntityBitmaps::from_bytes(b"not a sidecar").is_err());
        assert!(EntityBitmaps::from_bytes(&[]).is_err());
    }

    /// The per-path (path, entities) tuples of a paths result.
    fn path_tuples(batches: &[RecordBatch]) -> Vec<(String, i64)> {
        let mut out = Vec::new();
        for batch in batches {
            let paths = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("path");
            let entities = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("entities");
            for row in 0..batch.num_rows() {
                out.push((paths.value(row).to_string(), entities.value(row)));
            }
        }
        out
    }

    #[test]
    fn pruned_anchored_paths_match_the_scan() {
        let rows = vec![
            (1, 10, "browse"),
            (1, 20, "signup"),
            (1, 30, "purchase"),
            (2, 10, "browse"),
            (2, 20, "purchase"),
            (3, 10, "signup"),
            (3, 20, "browse"),
            (3, 30, "purchase"),
        ];
        let (stream, sidecars) = built(&rows);
        let spec = PathsSpec {
            view: "ev".to_string(),
            starting_at: Some("signup".to_string()),
            max_depth: 5,
            top: 10,
        };
        let (_, scan) = run_paths(&stream, &spec).expect("scan");
        let (_, pruned) = run_paths_pruned(&stream, &spec, &sidecars.bitmaps).expect("pruned");
        assert_eq!(path_tuples(&scan), path_tuples(&pruned));
    }

    #[test]
    fn unanchored_paths_prune_falls_back_to_the_full_scan() {
        let rows = vec![(1, 10, "a"), (1, 20, "b"), (2, 10, "a"), (2, 20, "c")];
        let (stream, sidecars) = built(&rows);
        let spec = PathsSpec {
            view: "ev".to_string(),
            starting_at: None,
            max_depth: 5,
            top: 10,
        };
        let (_, scan) = run_paths(&stream, &spec).expect("scan");
        let (_, pruned) = run_paths_pruned(&stream, &spec, &sidecars.bitmaps).expect("pruned");
        assert_eq!(path_tuples(&scan), path_tuples(&pruned));
    }

    #[test]
    fn candidate_rows_yield_exactly_the_candidate_entities_in_order() {
        let (stream, sidecars) = built(&funnel_fixture());
        // Candidates = entities with both signup and activate: u1, u2, u3, u5,
        // u6 (u4 has activate but never signup).
        let candidates = sidecars.bitmaps.funnel_candidates("signup", "activate");
        let mut seen = Vec::new();
        for row in stream.candidate_rows(&candidates) {
            let row = row.expect("row");
            if row.new_entity {
                if let EntityRef::Int(value) = row.entity {
                    seen.push(value);
                }
            }
        }
        assert_eq!(seen, vec![1, 2, 3, 5, 6]);
    }
}
