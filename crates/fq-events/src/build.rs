//! The two-phase bounded-memory build pipeline.
//!
//! Phase B1 (partition) streams source record batches, canonicalizes and
//! hashes actor keys, dictionary-encodes the event name and every dict
//! property, and spills rows to per-shard lz4 frame files. Phase B2
//! (finalize) runs per shard: read the shard's spill, deduplicate actors and
//! assign dense local ids in deterministic `(hash64, key bytes)` order, sort
//! by `(local, ts, tiebreak)`, and stream the rows through the segment
//! encoder. Nothing ever requires the dataset, a full column, or a full
//! dictionary-encoded copy resident in memory: B1 memory is the shard
//! buffers plus the budgeted dictionaries, and B2 memory is per-shard, with
//! an external sort-merge fallback for a shard whose spill outgrows its
//! budget (extreme actor skew).

use std::io::{Read, Write};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array,
    Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType as ArrowType, SchemaRef, TimeUnit as ArrowTimeUnit};

use fq_common::events::TimeUnit;

use crate::dict::BuildDicts;
use crate::model::FastMap;
use crate::error::{
    EventBuildError, EventError, EventSchemaError, EventStoreError, EventTypeError,
};
use crate::format::{encode_act, ActorSidecar, PropCell, SegEncoder};
use crate::model::{
    hash_key, push_binary_key, push_int_key, push_utf8_key, PropertyDef, PropertyEncoding,
    ShardFileEntry,
};
use crate::store::StoreIo;

/// A pull source of Arrow record batches (the runtime wraps its streaming
/// source read behind this).
pub trait BatchSource {
    /// The next batch, or None at end of stream.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String>;

    /// The refresh watermark observed over the pulled rows (the maximum
    /// refresh-key value), available once the stream is exhausted. A source
    /// with no refresh key reports None.
    fn watermark(&self) -> Option<String> {
        None
    }
}

/// The declared column roles a build resolves against the source schema.
pub struct BuildSpec {
    pub actor_column: String,
    pub time_column: String,
    pub event_column: String,
    pub tiebreak_column: Option<String>,
    /// The declared property names; None means every remaining column.
    pub properties: Option<Vec<String>>,
    pub time_unit: TimeUnit,
    /// A refresh/rebuild passes the dataset's pinned property schema; the
    /// resolved source layout must match it (name and type) or the build
    /// raises schema drift. An initial build passes None and derives it.
    pub declared: Option<Vec<PropertyDef>>,
    /// The dataset name, for drift error text.
    pub dataset: String,
}

/// The per-build knobs (resolved from `EventsConfig` by the caller).
pub struct BuildParams {
    pub shards: u32,
    pub generation: u64,
    pub memory_bytes: u64,
    pub threads: usize,
    pub dict_max_bytes: u64,
    /// The ingest ordinal the synthetic tiebreak starts at (a refresh
    /// continues from the dataset's measured event count so ordinals stay
    /// unique dataset-wide).
    pub ordinal_start: u64,
}

/// Prior-generation dictionary values a refresh seeds the build with (empty
/// for an initial build). Property seeds key by property name.
#[derive(Default)]
pub struct DictSeeds {
    pub event: Vec<String>,
    pub props: std::collections::HashMap<String, Vec<String>>,
}

/// What one build produced (per-shard files are already written; the caller
/// publishes them in the registry transaction).
pub struct BuildOutcome {
    pub shard_entries: Vec<ShardFileEntry>,
    pub properties: Vec<PropertyDef>,
    pub dict_entries: Vec<crate::model::DictFileEntry>,
    pub dict_state: std::collections::BTreeMap<String, u64>,
    pub events: u64,
    pub new_actors: u64,
    pub byte_size: u64,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    pub spill_bytes: u64,
    pub partition_millis: u64,
    pub finalize_millis: u64,
}

/// How the actor column's values canonicalize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorKind {
    Int64,
    Int32,
    Utf8,
    LargeUtf8,
    Utf8View,
    Binary,
    FixedBinary,
}

/// How the time column's values convert to microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeKind {
    Micros,
    Millis,
    /// A raw integer column scaled by the declared `time_unit`.
    Int(TimeUnit),
}

/// How a string column is physically laid out in Arrow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrKind {
    Utf8,
    LargeUtf8,
    Utf8View,
}

/// How the tiebreak column's values read as i64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntKind {
    Int64,
    Int32,
}

/// The resolved source layout: column indices plus access kinds, validated
/// once against the first batch's schema.
struct SourceLayout {
    schema: SchemaRef,
    actor: (usize, ActorKind),
    time: (usize, TimeKind),
    event: (usize, StrKind),
    tiebreak: Option<(usize, IntKind)>,
    props: Vec<(usize, PropertyDef)>,
    /// Per property: its build-dictionary slot (dict-encoded props only).
    dict_slots: Vec<Option<usize>>,
}

/// Find `column` in `schema` (case-insensitive, the engine's identifier rule).
fn column_index(schema: &SchemaRef, column: &str) -> Result<usize, EventSchemaError> {
    schema
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(column))
        .ok_or_else(|| EventSchemaError::MissingColumn {
            column: column.to_string(),
        })
}

/// Resolve the actor column's access kind.
fn actor_kind(column: &str, ty: &ArrowType) -> Result<ActorKind, EventSchemaError> {
    match ty {
        ArrowType::Int64 => Ok(ActorKind::Int64),
        ArrowType::Int32 => Ok(ActorKind::Int32),
        ArrowType::Utf8 => Ok(ActorKind::Utf8),
        ArrowType::LargeUtf8 => Ok(ActorKind::LargeUtf8),
        ArrowType::Utf8View => Ok(ActorKind::Utf8View),
        ArrowType::Binary => Ok(ActorKind::Binary),
        ArrowType::FixedSizeBinary(_) => Ok(ActorKind::FixedBinary),
        other => Err(EventSchemaError::ColumnType {
            column: column.to_string(),
            found: other.to_string(),
            role: "the actor id".to_string(),
            supported: "Int32/Int64, Utf8, Binary, FixedSizeBinary".to_string(),
        }),
    }
}

/// Resolve the time column's access kind; nanosecond and second timestamps
/// raise (truncating or scaling them silently would misstate precision).
fn time_kind(column: &str, ty: &ArrowType, declared: TimeUnit) -> Result<TimeKind, EventError> {
    match ty {
        ArrowType::Timestamp(ArrowTimeUnit::Microsecond, _) => Ok(TimeKind::Micros),
        ArrowType::Timestamp(ArrowTimeUnit::Millisecond, _) => Ok(TimeKind::Millis),
        ArrowType::Timestamp(ArrowTimeUnit::Nanosecond, _) => Err(EventTypeError::NanosecondTime {
            column: column.to_string(),
        }
        .into()),
        ArrowType::Int64 => Ok(TimeKind::Int(declared)),
        other => Err(EventSchemaError::ColumnType {
            column: column.to_string(),
            found: other.to_string(),
            role: "the event time".to_string(),
            supported: "Timestamp(us), Timestamp(ms), Int64".to_string(),
        }
        .into()),
    }
}

/// Resolve a string column's physical layout.
fn str_kind(column: &str, ty: &ArrowType, role: &str) -> Result<StrKind, EventSchemaError> {
    match ty {
        ArrowType::Utf8 => Ok(StrKind::Utf8),
        ArrowType::LargeUtf8 => Ok(StrKind::LargeUtf8),
        ArrowType::Utf8View => Ok(StrKind::Utf8View),
        other => Err(EventSchemaError::ColumnType {
            column: column.to_string(),
            found: other.to_string(),
            role: role.to_string(),
            supported: "Utf8".to_string(),
        }),
    }
}

/// Resolve one property column's declaration from its Arrow type.
fn property_def(column: &str, ty: &ArrowType, nullable: bool) -> Result<PropertyDef, EventError> {
    let (data_type, encoding) = match ty {
        ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View => {
            (fq_common::DataType::Varchar, PropertyEncoding::Dict)
        }
        ArrowType::Int64 | ArrowType::Int32 => (fq_common::DataType::BigInt, PropertyEncoding::I64),
        ArrowType::Float64 | ArrowType::Float32 => {
            (fq_common::DataType::Double, PropertyEncoding::F64)
        }
        ArrowType::Boolean => (fq_common::DataType::Boolean, PropertyEncoding::Bool),
        other => {
            return Err(EventTypeError::PropertyType {
                column: column.to_string(),
                found: other.to_string(),
            }
            .into())
        }
    };
    Ok(PropertyDef {
        name: column.to_string(),
        data_type,
        encoding,
        nullable,
    })
}

/// Resolve the whole source layout against the first batch's schema.
fn resolve_layout(schema: &SchemaRef, spec: &BuildSpec) -> Result<SourceLayout, EventError> {
    let actor_index = column_index(schema, &spec.actor_column)?;
    let time_index = column_index(schema, &spec.time_column)?;
    let event_index = column_index(schema, &spec.event_column)?;
    let mut reserved = vec![actor_index, time_index, event_index];
    let tiebreak = match &spec.tiebreak_column {
        None => None,
        Some(column) => {
            let index = column_index(schema, column)?;
            reserved.push(index);
            let kind = match schema.field(index).data_type() {
                ArrowType::Int64 => IntKind::Int64,
                ArrowType::Int32 => IntKind::Int32,
                other => {
                    return Err(EventTypeError::TiebreakType {
                        column: column.clone(),
                        found: other.to_string(),
                    }
                    .into())
                }
            };
            Some((index, kind))
        }
    };
    let property_names: Vec<String> = match &spec.properties {
        Some(names) => names.clone(),
        None => schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| !reserved.contains(index))
            .map(|(_, field)| field.name().clone())
            .collect(),
    };
    let mut props = Vec::with_capacity(property_names.len());
    for name in &property_names {
        let index = column_index(schema, name)?;
        let field = schema.field(index);
        let mut def = property_def(name, field.data_type(), field.is_nullable())?;
        if let Some(declared) = &spec.declared {
            let pinned = declared.iter().find(|d| d.name == *name).ok_or_else(|| {
                EventSchemaError::Drift {
                    dataset: spec.dataset.clone(),
                    declared: "no such property".to_string(),
                    found: name.clone(),
                }
            })?;
            if pinned.data_type != def.data_type {
                return Err(EventSchemaError::Drift {
                    dataset: spec.dataset.clone(),
                    declared: format!("{} {}", pinned.name, pinned.data_type),
                    found: format!("{} {}", def.name, def.data_type),
                }
                .into());
            }
            // The pinned encoding wins: a property promoted to raw strings in
            // an earlier build stays raw.
            def.encoding = pinned.encoding;
            def.nullable = pinned.nullable || def.nullable;
        }
        props.push((index, def));
    }
    let mut dict_slots = Vec::with_capacity(props.len());
    let mut next_slot = 0usize;
    for (_, def) in &props {
        if def.encoding == PropertyEncoding::Dict {
            dict_slots.push(Some(next_slot));
            next_slot += 1;
        } else {
            dict_slots.push(None);
        }
    }
    Ok(SourceLayout {
        schema: Arc::clone(schema),
        actor: (
            actor_index,
            actor_kind(&spec.actor_column, schema.field(actor_index).data_type())?,
        ),
        time: (
            time_index,
            time_kind(
                &spec.time_column,
                schema.field(time_index).data_type(),
                spec.time_unit,
            )?,
        ),
        event: (
            event_index,
            str_kind(
                &spec.event_column,
                schema.field(event_index).data_type(),
                "the event name",
            )?,
        ),
        tiebreak,
        props,
        dict_slots,
    })
}

// ---------------------------------------------------------------------------
// Spill frame format
// ---------------------------------------------------------------------------

/// One shard's in-memory spill buffer: columnar per flush, carrying the raw
/// string alongside every dict code so a mid-build promotion never re-reads
/// the source.
struct ShardBuffer {
    actors: Vec<u32>,
    ts: Vec<i64>,
    tiebreak: Vec<i64>,
    event: Vec<u64>,
    props: Vec<PropSpill>,
    bytes: usize,
}

/// One property's spill buffer.
enum PropSpill {
    Dict {
        validity: Vec<u8>,
        codes: Vec<u64>,
        raw_lens: Vec<u32>,
        raw_bytes: Vec<u8>,
    },
    I64 {
        validity: Vec<u8>,
        values: Vec<i64>,
    },
    F64 {
        validity: Vec<u8>,
        values: Vec<f64>,
    },
    Bool {
        validity: Vec<u8>,
        values: Vec<u8>,
    },
}

impl PropSpill {
    /// An empty buffer for a property encoding. A raw-string declaration
    /// never appears at build time (promotion happens during the build), so
    /// dict covers strings.
    fn new(encoding: PropertyEncoding) -> Self {
        match encoding {
            PropertyEncoding::Dict | PropertyEncoding::RawString => PropSpill::Dict {
                validity: Vec::new(),
                codes: Vec::new(),
                raw_lens: Vec::new(),
                raw_bytes: Vec::new(),
            },
            PropertyEncoding::I64 => PropSpill::I64 {
                validity: Vec::new(),
                values: Vec::new(),
            },
            PropertyEncoding::F64 => PropSpill::F64 {
                validity: Vec::new(),
                values: Vec::new(),
            },
            PropertyEncoding::Bool => PropSpill::Bool {
                validity: Vec::new(),
                values: Vec::new(),
            },
        }
    }
}

impl ShardBuffer {
    /// An empty buffer over the given property encodings.
    fn new(props: &[(usize, PropertyDef)]) -> Self {
        Self {
            actors: Vec::new(),
            ts: Vec::new(),
            tiebreak: Vec::new(),
            event: Vec::new(),
            props: props
                .iter()
                .map(|(_, def)| PropSpill::new(def.encoding))
                .collect(),
            bytes: 0,
        }
    }

    /// Rows buffered.
    fn rows(&self) -> usize {
        self.actors.len()
    }

    /// Serialize the buffer as one frame payload and clear it.
    fn drain_frame(&mut self) -> Vec<u8> {
        let rows = self.rows();
        let mut payload = Vec::with_capacity(self.bytes + 64);
        payload.extend_from_slice(&(rows as u32).to_le_bytes());
        for actor in &self.actors {
            crate::format::put_varint(&mut payload, u64::from(*actor));
        }
        for value in &self.ts {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in &self.tiebreak {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for code in &self.event {
            crate::format::put_varint(&mut payload, *code);
        }
        for prop in &self.props {
            drain_prop(prop, &mut payload);
        }
        self.actors.clear();
        self.ts.clear();
        self.tiebreak.clear();
        self.event.clear();
        for prop in &mut self.props {
            clear_prop(prop);
        }
        self.bytes = 0;
        payload
    }
}

/// Serialize one property buffer into a frame payload.
fn drain_prop(prop: &PropSpill, payload: &mut Vec<u8>) {
    match prop {
        PropSpill::Dict {
            validity,
            codes,
            raw_lens,
            raw_bytes,
        } => {
            payload.extend_from_slice(validity);
            for code in codes {
                crate::format::put_varint(payload, *code);
            }
            for len in raw_lens {
                crate::format::put_varint(payload, u64::from(*len));
            }
            payload.extend_from_slice(raw_bytes);
        }
        PropSpill::I64 { validity, values } => {
            payload.extend_from_slice(validity);
            for value in values {
                payload.extend_from_slice(&value.to_le_bytes());
            }
        }
        PropSpill::F64 { validity, values } => {
            payload.extend_from_slice(validity);
            for value in values {
                payload.extend_from_slice(&value.to_le_bytes());
            }
        }
        PropSpill::Bool { validity, values } => {
            payload.extend_from_slice(validity);
            payload.extend_from_slice(values);
        }
    }
}

/// Clear one property buffer after a flush.
fn clear_prop(prop: &mut PropSpill) {
    match prop {
        PropSpill::Dict {
            validity,
            codes,
            raw_lens,
            raw_bytes,
        } => {
            validity.clear();
            codes.clear();
            raw_lens.clear();
            raw_bytes.clear();
        }
        PropSpill::I64 { validity, values } => {
            validity.clear();
            values.clear();
        }
        PropSpill::F64 { validity, values } => {
            validity.clear();
            values.clear();
        }
        PropSpill::Bool { validity, values } => {
            validity.clear();
            values.clear();
        }
    }
}

/// The decoded columnar contents of one shard's whole spill.
#[derive(Default)]
struct SpillColumns {
    actors: Vec<u32>,
    ts: Vec<i64>,
    tiebreak: Vec<i64>,
    event: Vec<u64>,
    props: Vec<PropColumns>,
}

/// One property's decoded spill columns.
enum PropColumns {
    Dict {
        validity: Vec<u8>,
        codes: Vec<u64>,
        /// Whether row `i` carries raw bytes (rows spilled before a
        /// promotion carry codes only; their strings come from the dict).
        raw_flags: Vec<u8>,
        raw_offsets: Vec<u64>,
        raw_bytes: Vec<u8>,
    },
    I64 {
        validity: Vec<u8>,
        values: Vec<i64>,
    },
    F64 {
        validity: Vec<u8>,
        values: Vec<f64>,
    },
    Bool {
        validity: Vec<u8>,
        values: Vec<u8>,
    },
}

impl SpillColumns {
    /// Rows held.
    fn rows(&self) -> usize {
        self.actors.len()
    }

    /// The property cell of row `row` for property `prop` under the final
    /// (possibly promoted) encoding.
    fn cell(&self, prop: usize, promoted: bool, row: usize) -> PropCell<'_> {
        match &self.props[prop] {
            PropColumns::Dict {
                validity,
                codes,
                raw_flags,
                raw_offsets,
                raw_bytes,
            } => {
                if validity[row] == 0 {
                    PropCell::Null
                } else if promoted && raw_flags[row] == 1 {
                    PropCell::Str(
                        &raw_bytes[raw_offsets[row] as usize..raw_offsets[row + 1] as usize],
                    )
                } else {
                    // A promoted property's code rows (spilled before the
                    // promotion) resolve to strings through the dict snapshot
                    // at the encoder.
                    PropCell::Code(codes[row])
                }
            }
            PropColumns::I64 { validity, values } => {
                if validity[row] == 0 {
                    PropCell::Null
                } else {
                    PropCell::I64(values[row])
                }
            }
            PropColumns::F64 { validity, values } => {
                if validity[row] == 0 {
                    PropCell::Null
                } else {
                    PropCell::F64(values[row])
                }
            }
            PropColumns::Bool { validity, values } => {
                if validity[row] == 0 {
                    PropCell::Null
                } else {
                    PropCell::Bool(values[row] != 0)
                }
            }
        }
    }
}

/// Append one lz4 frame to a spill file: `[compressed_len u32][lz4 bytes]`.
/// The compression pays for itself: the spill's resident set shrinks about
/// 2x, which keeps the read-back and every concurrent phase page-cache-hot.
fn write_frame(file: &mut std::fs::File, payload: &[u8]) -> Result<(), EventBuildError> {
    let compressed = lz4_flex::compress_prepend_size(payload);
    let mut out = Vec::with_capacity(4 + compressed.len());
    out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    out.extend_from_slice(&compressed);
    file.write_all(&out)
        .map_err(|error| EventBuildError::Io(format!("spill write: {error}")))?;
    Ok(())
}

/// Read every frame of a spill file, appending each decoded payload into
/// `columns` via `append_frame`.
fn read_frames(
    path: &std::path::Path,
    mut on_payload: impl FnMut(&[u8]) -> Result<(), EventError>,
) -> Result<(), EventError> {
    let file = std::fs::File::open(path)
        .map_err(|error| EventBuildError::Io(format!("spill open: {error}")))?;
    let mut reader = std::io::BufReader::with_capacity(1 << 20, file);
    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(EventBuildError::Io(format!("spill read: {error}")).into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut compressed = vec![0u8; len];
        reader
            .read_exact(&mut compressed)
            .map_err(|error| EventBuildError::Io(format!("spill read: {error}")))?;
        let payload = lz4_flex::decompress_size_prepended(&compressed)
            .map_err(|error| EventBuildError::Io(format!("spill lz4: {error}")))?;
        on_payload(&payload)?;
    }
}

/// Decode one frame payload into shard columns.
fn append_frame(
    payload: &[u8],
    props: &[(usize, PropertyDef)],
    columns: &mut SpillColumns,
) -> Result<(), EventError> {
    let file = "spill";
    let mut reader = crate::format::ByteReader::new(payload, file);
    let map_err = |error: EventStoreError| EventBuildError::Io(error.to_string());
    let rows = reader.u32().map_err(map_err)? as usize;
    if columns.props.is_empty() {
        for (_, def) in props {
            columns.props.push(match def.encoding {
                PropertyEncoding::Dict | PropertyEncoding::RawString => PropColumns::Dict {
                    validity: Vec::new(),
                    codes: Vec::new(),
                    raw_flags: Vec::new(),
                    raw_offsets: vec![0],
                    raw_bytes: Vec::new(),
                },
                PropertyEncoding::I64 => PropColumns::I64 {
                    validity: Vec::new(),
                    values: Vec::new(),
                },
                PropertyEncoding::F64 => PropColumns::F64 {
                    validity: Vec::new(),
                    values: Vec::new(),
                },
                PropertyEncoding::Bool => PropColumns::Bool {
                    validity: Vec::new(),
                    values: Vec::new(),
                },
            });
        }
    }
    for _ in 0..rows {
        let actor = reader.varint().map_err(map_err)?;
        columns
            .actors
            .push(u32::try_from(actor).map_err(|_| {
                EventBuildError::Io("spill actor code exceeds u32".to_string())
            })?);
    }
    for chunk in reader.take(rows * 8).map_err(map_err)?.chunks_exact(8) {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(chunk);
        columns.ts.push(i64::from_le_bytes(raw));
    }
    for chunk in reader.take(rows * 8).map_err(map_err)?.chunks_exact(8) {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(chunk);
        columns.tiebreak.push(i64::from_le_bytes(raw));
    }
    for _ in 0..rows {
        columns.event.push(reader.varint().map_err(map_err)?);
    }
    for slot in &mut columns.props {
        append_frame_prop(&mut reader, rows, slot).map_err(map_err)?;
    }
    Ok(())
}

/// Decode one property's frame section.
fn append_frame_prop(
    reader: &mut crate::format::ByteReader<'_>,
    rows: usize,
    slot: &mut PropColumns,
) -> Result<(), EventStoreError> {
    match slot {
        PropColumns::Dict {
            validity,
            codes,
            raw_flags,
            raw_offsets,
            raw_bytes,
        } => {
            validity.extend_from_slice(reader.take(rows)?);
            for _ in 0..rows {
                codes.push(reader.varint()?);
            }
            // Raw lengths are 0 = absent (a code-only row), n + 1 = n bytes.
            let mut total = *raw_offsets.last().expect("offsets seeded");
            let mut byte_total = 0u64;
            for _ in 0..rows {
                let shifted = reader.varint()?;
                if shifted == 0 {
                    raw_flags.push(0);
                } else {
                    raw_flags.push(1);
                    total += shifted - 1;
                    byte_total += shifted - 1;
                }
                raw_offsets.push(total);
            }
            raw_bytes.extend_from_slice(reader.take(byte_total as usize)?);
        }
        PropColumns::I64 { validity, values } => {
            validity.extend_from_slice(reader.take(rows)?);
            for chunk in reader.take(rows * 8)?.chunks_exact(8) {
                let mut raw = [0u8; 8];
                raw.copy_from_slice(chunk);
                values.push(i64::from_le_bytes(raw));
            }
        }
        PropColumns::F64 { validity, values } => {
            validity.extend_from_slice(reader.take(rows)?);
            for chunk in reader.take(rows * 8)?.chunks_exact(8) {
                let mut raw = [0u8; 8];
                raw.copy_from_slice(chunk);
                values.push(f64::from_bits(u64::from_le_bytes(raw)));
            }
        }
        PropColumns::Bool { validity, values } => {
            validity.extend_from_slice(reader.take(rows)?);
            values.extend_from_slice(reader.take(rows)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// B1: partition
// ---------------------------------------------------------------------------

/// The shared B1 state across workers.
/// The build-wide actor dictionary: every distinct canonical actor key gets
/// one dense u32 code at first sight, so a spill row carries a small code and
/// the finalize prices identity once per DISTINCT actor. Striped locks bound
/// contention. Codes are minted in ARRIVAL order, which is nondeterministic
/// across workers - nothing downstream may order anything by code value
/// (local ids re-sort by (hash, key) per shard).
/// One dictionary stripe: actor-key hash to the (key bytes, code) entries
/// sharing it.
type DictStripe = FastMap<u64, Vec<(Vec<u8>, u32)>>;

struct ActorDict {
    stripes: Vec<Mutex<DictStripe>>,
    next_code: AtomicU32,
}

impl ActorDict {
    /// An empty dictionary with enough stripes to keep worker contention low.
    fn new() -> Self {
        Self {
            stripes: (0..256).map(|_| Mutex::new(FastMap::default())).collect(),
            next_code: AtomicU32::new(0),
        }
    }

    /// The code of `(hash, key)`, minted on first sight. Equal hashes with
    /// different key bytes stay distinct (collision safety).
    fn code_of(&self, hash: u64, key: &[u8]) -> Result<u32, EventBuildError> {
        let stripe = &self.stripes[(hash % 256) as usize];
        let mut map = stripe.lock().expect("actor dict stripe poisoned");
        let bucket = map.entry(hash).or_default();
        for (existing, code) in bucket.iter() {
            if existing == key {
                return Ok(*code);
            }
        }
        let code = self.next_code.fetch_add(1, Ordering::Relaxed);
        if code == u32::MAX {
            return Err(EventBuildError::Io(
                "actor dictionary exhausted the u32 code space".to_string(),
            ));
        }
        bucket.push((key.to_vec(), code));
        Ok(code)
    }

    /// Freeze into the code-indexed table the finalize reads.
    fn freeze(&self) -> ActorTable {
        let count = self.next_code.load(Ordering::Relaxed) as usize;
        let mut hashes = vec![0u64; count];
        let mut keys: Vec<Vec<u8>> = vec![Vec::new(); count];
        for stripe in &self.stripes {
            let map = stripe.lock().expect("actor dict stripe poisoned");
            for (hash, bucket) in map.iter() {
                for (key, code) in bucket {
                    hashes[*code as usize] = *hash;
                    keys[*code as usize].clone_from(key);
                }
            }
        }
        ActorTable { hashes, keys }
    }
}

/// The frozen actor dictionary: code -> (hash, canonical key bytes).
pub(crate) struct ActorTable {
    hashes: Vec<u64>,
    keys: Vec<Vec<u8>>,
}

impl ActorTable {
    /// The key bytes of `code`.
    pub(crate) fn key(&self, code: u32) -> &[u8] {
        &self.keys[code as usize]
    }

    /// The precomputed hash of `code`.
    pub(crate) fn hash_of(&self, code: u32) -> u64 {
        self.hashes[code as usize]
    }

    /// The number of minted codes.
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }
}

struct Partition<'a> {
    layout: &'a SourceLayout,
    dicts: &'a BuildDicts,
    actors: ActorDict,
    shards: u32,
    buffer_bytes: usize,
    spill_files: Vec<Mutex<std::fs::File>>,
    /// Uncompressed spilled bytes per shard (sizes B2 concurrency and the
    /// whale fallback).
    shard_bytes: Vec<AtomicU64>,
    spill_total: AtomicU64,
}

/// One dispatched unit: a batch plus the global ordinal of its first row.
struct Job {
    batch: RecordBatch,
    start_ordinal: u64,
}

/// Run phase B1: stream the source through the workers into per-shard spill
/// files. Returns total rows partitioned.
fn run_partition(
    partition: &Partition<'_>,
    source: &mut dyn BatchSource,
    threads: usize,
    ordinal_start: u64,
) -> Result<u64, EventError> {
    let (sender, receiver) = mpsc::sync_channel::<Job>(threads * 2);
    let receiver = Mutex::new(receiver);
    let failure: Mutex<Option<EventError>> = Mutex::new(None);
    let total = AtomicU64::new(0);
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            handles.push(scope.spawn(|| worker_loop(partition, &receiver, &failure)));
        }
        let mut ordinal = ordinal_start;
        loop {
            if failure.lock().expect("failure lock").is_some() {
                break;
            }
            let batch = match source.next_batch() {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(message) => {
                    *failure.lock().expect("failure lock") =
                        Some(EventBuildError::Io(format!("source read: {message}")).into());
                    break;
                }
            };
            if batch.schema() != partition.layout.schema {
                *failure.lock().expect("failure lock") = Some(
                    EventSchemaError::Drift {
                        dataset: String::new(),
                        declared: partition.layout.schema.to_string(),
                        found: batch.schema().to_string(),
                    }
                    .into(),
                );
                break;
            }
            let rows = batch.num_rows() as u64;
            total.fetch_add(rows, Ordering::Relaxed);
            if sender
                .send(Job {
                    batch,
                    start_ordinal: ordinal,
                })
                .is_err()
            {
                break;
            }
            ordinal += rows;
        }
        drop(sender);
        for handle in handles {
            handle.join().expect("build worker panicked");
        }
    });
    if let Some(error) = failure.into_inner().expect("failure lock") {
        return Err(error);
    }
    Ok(total.into_inner())
}

/// One B1 worker: pull jobs, partition each batch into thread-local shard
/// buffers, and spill full buffers as lz4 frames.
fn worker_loop(
    partition: &Partition<'_>,
    receiver: &Mutex<mpsc::Receiver<Job>>,
    failure: &Mutex<Option<EventError>>,
) {
    let mut buffers: Vec<ShardBuffer> = (0..partition.shards)
        .map(|_| ShardBuffer::new(&partition.layout.props))
        .collect();
    loop {
        let job = {
            let guard = receiver.lock().expect("receiver lock poisoned");
            guard.recv()
        };
        let Ok(job) = job else {
            break;
        };
        if let Err(error) = process_batch(partition, &job, &mut buffers) {
            let mut slot = failure.lock().expect("failure lock");
            if slot.is_none() {
                *slot = Some(error);
            }
            // Keep draining so the producer never blocks on a full channel.
        }
    }
    // Flush every remaining non-empty buffer.
    for (shard, buffer) in buffers.iter_mut().enumerate() {
        if buffer.rows() > 0 {
            if let Err(error) = flush_shard(partition, shard, buffer) {
                let mut slot = failure.lock().expect("failure lock");
                if slot.is_none() {
                    *slot = Some(error.into());
                }
            }
        }
    }
}

/// Spill one shard buffer as a frame.
fn flush_shard(
    partition: &Partition<'_>,
    shard: usize,
    buffer: &mut ShardBuffer,
) -> Result<(), EventBuildError> {
    let payload = buffer.drain_frame();
    partition.shard_bytes[shard].fetch_add(payload.len() as u64, Ordering::Relaxed);
    partition
        .spill_total
        .fetch_add(payload.len() as u64, Ordering::Relaxed);
    let mut file = partition.spill_files[shard]
        .lock()
        .expect("spill file lock poisoned");
    write_frame(&mut file, &payload)
}

/// Partition one batch into the worker's shard buffers.
fn process_batch(
    partition: &Partition<'_>,
    job: &Job,
    buffers: &mut [ShardBuffer],
) -> Result<(), EventError> {
    let layout = partition.layout;
    let batch = &job.batch;
    let rows = batch.num_rows();
    // Encode the event-name column for the whole batch under one dictionary
    // acquisition; a null name raises with its global row position.
    let event_array = batch.column(layout.event.0);
    if event_array.null_count() > 0 {
        let row = (0..rows)
            .find(|row| event_array.is_null(*row))
            .expect("null_count > 0");
        return Err(EventBuildError::NullEventName {
            row: job.start_ordinal + row as u64,
        }
        .into());
    }
    let mut event_codes = Vec::with_capacity(rows);
    encode_event_column(
        partition.dicts,
        event_array,
        layout.event.1,
        &mut event_codes,
    )?;
    // Encode every dict property for the batch; promoted properties fall back
    // to the raw string carried in the spill.
    let mut prop_codes: Vec<Option<(Vec<u64>, bool)>> = Vec::with_capacity(layout.props.len());
    for (prop_index, (column, def)) in layout.props.iter().enumerate() {
        if let (PropertyEncoding::Dict, Some(slot)) = (def.encoding, layout.dict_slots[prop_index])
        {
            let mut codes = Vec::with_capacity(rows);
            let promoted =
                encode_prop_column(partition.dicts, slot, batch.column(*column), &mut codes)?;
            prop_codes.push(Some((codes, promoted)));
        } else {
            prop_codes.push(None);
        }
    }
    push_rows(partition, job, buffers, &event_codes, &prop_codes)
}

/// Append every row of a batch to its shard buffer and flush full buffers.
/// The time column and the actor keys materialize batch-wide first (one
/// variant-matched pass each, keys written contiguously and hashed in
/// place), so the row loop reads flat arrays.
fn push_rows(
    partition: &Partition<'_>,
    job: &Job,
    buffers: &mut [ShardBuffer],
    event_codes: &[u64],
    prop_codes: &[Option<(Vec<u64>, bool)>],
) -> Result<(), EventError> {
    let layout = partition.layout;
    let batch = &job.batch;
    let rows = batch.num_rows();
    let actor = ActorAccessor::new(batch.column(layout.actor.0), layout.actor.1);
    let time = TimeAccessor::new(batch.column(layout.time.0), layout.time.1);
    let tiebreak = layout
        .tiebreak
        .map(|(index, kind)| TiebreakAccessor::new(batch.column(index), kind));
    let times = time.fill(rows, job.start_ordinal)?;
    let mut key_lens: Vec<u32> = Vec::with_capacity(rows);
    let mut key_bytes: Vec<u8> = Vec::with_capacity(rows * 16);
    let mut hashes: Vec<u64> = Vec::with_capacity(rows);
    actor.fill_keys(
        rows,
        job.start_ordinal,
        &mut key_lens,
        &mut key_bytes,
        &mut hashes,
    )?;
    let mut key_offset = 0usize;
    for (row, event_code) in event_codes.iter().enumerate().take(rows) {
        let global_row = job.start_ordinal + row as u64;
        let len = key_lens[row] as usize;
        let key = &key_bytes[key_offset..key_offset + len];
        key_offset += len;
        let hash = hashes[row];
        let shard = (hash & u64::from(partition.shards - 1)) as usize;
        let actor = partition.actors.code_of(hash, key)?;
        let tb = match &tiebreak {
            Some(accessor) => accessor.value(row, global_row)?,
            None => i64::try_from(global_row).expect("ordinal fits i64"),
        };
        let buffer = &mut buffers[shard];
        buffer.actors.push(actor);
        buffer.ts.push(times[row]);
        buffer.tiebreak.push(tb);
        buffer.event.push(*event_code);
        buffer.bytes += 25;
        push_prop_row(batch, layout, prop_codes, row, buffer);
        if buffer.bytes >= partition.buffer_bytes {
            flush_shard(partition, shard, &mut buffers[shard])?;
        }
    }
    Ok(())
}

/// Append one row's property cells to a shard buffer.
fn push_prop_row(
    batch: &RecordBatch,
    layout: &SourceLayout,
    prop_codes: &[Option<(Vec<u64>, bool)>],
    row: usize,
    buffer: &mut ShardBuffer,
) {
    for (prop_index, (column, def)) in layout.props.iter().enumerate() {
        let array = batch.column(*column);
        let valid = u8::from(!array.is_null(row));
        match (&mut buffer.props[prop_index], def.encoding) {
            (
                PropSpill::Dict {
                    validity,
                    codes,
                    raw_lens,
                    raw_bytes,
                },
                PropertyEncoding::Dict | PropertyEncoding::RawString,
            ) => {
                validity.push(valid);
                // A dict-encoded row spills its CODE only (the dict can
                // materialize the string later even if the property promotes
                // mid-build); a raw-string or already-promoted row carries
                // the raw bytes, with the code as a dead placeholder. Raw
                // lengths spill shifted: 0 = absent, n + 1 = n bytes.
                let carry_raw = if let Some((batch_codes, promoted)) =
                    prop_codes[prop_index].as_ref()
                {
                    codes.push(batch_codes[row]);
                    *promoted
                } else {
                    codes.push(0);
                    true
                };
                if valid == 1 && carry_raw {
                    let value = string_value(array, row);
                    raw_lens.push(value.len() as u32 + 1);
                    raw_bytes.extend_from_slice(value.as_bytes());
                    buffer.bytes += 12 + value.len();
                } else {
                    raw_lens.push(0);
                    buffer.bytes += 12;
                }
            }
            (PropSpill::I64 { validity, values }, PropertyEncoding::I64) => {
                validity.push(valid);
                values.push(if valid == 1 { int_value(array, row) } else { 0 });
                buffer.bytes += 9;
            }
            (PropSpill::F64 { validity, values }, PropertyEncoding::F64) => {
                validity.push(valid);
                values.push(if valid == 1 {
                    float_value(array, row)
                } else {
                    0.0
                });
                buffer.bytes += 9;
            }
            (PropSpill::Bool { validity, values }, PropertyEncoding::Bool) => {
                validity.push(valid);
                let value = if valid == 1 {
                    let array = array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("bool property downcasts");
                    u8::from(array.value(row))
                } else {
                    0
                };
                values.push(value);
                buffer.bytes += 2;
            }
            (_, encoding) => unreachable!("spill buffer mismatches encoding {encoding:?}"),
        }
    }
}

/// The string at `row` of a string-typed array (validated at layout time).
fn string_value(array: &dyn Array, row: usize) -> &str {
    let any = array.as_any();
    if let Some(strings) = any.downcast_ref::<StringArray>() {
        return strings.value(row);
    }
    if let Some(strings) = any.downcast_ref::<LargeStringArray>() {
        return strings.value(row);
    }
    if let Some(strings) = any.downcast_ref::<StringViewArray>() {
        return strings.value(row);
    }
    unreachable!("string column downcasts to a string array")
}

/// The integer at `row` of an int-typed array (validated at layout time).
fn int_value(array: &dyn Array, row: usize) -> i64 {
    let any = array.as_any();
    if let Some(values) = any.downcast_ref::<Int64Array>() {
        return values.value(row);
    }
    if let Some(values) = any.downcast_ref::<Int32Array>() {
        return i64::from(values.value(row));
    }
    unreachable!("integer column downcasts to an integer array")
}

/// The float at `row` of a float-typed array (validated at layout time).
fn float_value(array: &dyn Array, row: usize) -> f64 {
    let any = array.as_any();
    if let Some(values) = any.downcast_ref::<Float64Array>() {
        return values.value(row);
    }
    if let Some(values) = any.downcast_ref::<Float32Array>() {
        return f64::from(values.value(row));
    }
    unreachable!("float column downcasts to a float array")
}

/// Encode a batch's event names.
fn encode_event_column(
    dicts: &BuildDicts,
    array: &Arc<dyn Array>,
    kind: StrKind,
    codes: &mut Vec<u64>,
) -> Result<(), EventBuildError> {
    match kind {
        StrKind::Utf8 => {
            let strings = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 downcasts");
            dicts.encode_events(strings.iter().map(|v| v.expect("nulls pre-checked")), codes)
        }
        StrKind::LargeUtf8 => {
            let strings = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8 downcasts");
            dicts.encode_events(strings.iter().map(|v| v.expect("nulls pre-checked")), codes)
        }
        StrKind::Utf8View => {
            let strings = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("utf8 view downcasts");
            dicts.encode_events(strings.iter().map(|v| v.expect("nulls pre-checked")), codes)
        }
    }
}

/// Encode a batch's values for one dict property; returns the promotion flag.
fn encode_prop_column(
    dicts: &BuildDicts,
    prop_index: usize,
    array: &Arc<dyn Array>,
    codes: &mut Vec<u64>,
) -> Result<bool, EventError> {
    let any = array.as_any();
    if let Some(strings) = any.downcast_ref::<StringArray>() {
        return Ok(dicts.encode_prop(prop_index, strings.iter(), codes));
    }
    if let Some(strings) = any.downcast_ref::<LargeStringArray>() {
        return Ok(dicts.encode_prop(prop_index, strings.iter(), codes));
    }
    if let Some(strings) = any.downcast_ref::<StringViewArray>() {
        return Ok(dicts.encode_prop(prop_index, strings.iter(), codes));
    }
    Err(EventTypeError::PropertyType {
        column: format!("property #{prop_index}"),
        found: array.data_type().to_string(),
    }
    .into())
}

/// The actor column accessor: canonical key bytes per row.
enum ActorAccessor<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Binary(&'a BinaryArray),
    FixedBinary(&'a FixedSizeBinaryArray),
}

impl<'a> ActorAccessor<'a> {
    /// Downcast the actor array per its resolved kind.
    fn new(array: &'a Arc<dyn Array>, kind: ActorKind) -> Self {
        let any = array.as_any();
        match kind {
            ActorKind::Int64 => ActorAccessor::Int64(any.downcast_ref().expect("int64 actor")),
            ActorKind::Int32 => ActorAccessor::Int32(any.downcast_ref().expect("int32 actor")),
            ActorKind::Utf8 => ActorAccessor::Utf8(any.downcast_ref().expect("utf8 actor")),
            ActorKind::LargeUtf8 => {
                ActorAccessor::LargeUtf8(any.downcast_ref().expect("large utf8 actor"))
            }
            ActorKind::Utf8View => {
                ActorAccessor::Utf8View(any.downcast_ref().expect("utf8 view actor"))
            }
            ActorKind::Binary => ActorAccessor::Binary(any.downcast_ref().expect("binary actor")),
            ActorKind::FixedBinary => {
                ActorAccessor::FixedBinary(any.downcast_ref().expect("fixed binary actor"))
            }
        }
    }

    /// Every row's canonical key bytes appended contiguously (lengths and
    /// hashes recorded per row), in one variant-matched pass; a null actor
    /// raises with its global row.
    fn fill_keys(
        &self,
        rows: usize,
        start_ordinal: u64,
        lens: &mut Vec<u32>,
        bytes: &mut Vec<u8>,
        hashes: &mut Vec<u64>,
    ) -> Result<(), EventError> {
        for row in 0..rows {
            let start = bytes.len();
            self.push_key(row, start_ordinal + row as u64, bytes)?;
            lens.push((bytes.len() - start) as u32);
            hashes.push(hash_key(&bytes[start..]));
        }
        Ok(())
    }

    /// Append row `row`'s canonical key bytes; a null actor raises.
    fn push_key(&self, row: usize, global_row: u64, out: &mut Vec<u8>) -> Result<(), EventError> {
        let null = match self {
            ActorAccessor::Int64(array) => array.is_null(row),
            ActorAccessor::Int32(array) => array.is_null(row),
            ActorAccessor::Utf8(array) => array.is_null(row),
            ActorAccessor::LargeUtf8(array) => array.is_null(row),
            ActorAccessor::Utf8View(array) => array.is_null(row),
            ActorAccessor::Binary(array) => array.is_null(row),
            ActorAccessor::FixedBinary(array) => array.is_null(row),
        };
        if null {
            return Err(EventBuildError::NullActor { row: global_row }.into());
        }
        match self {
            ActorAccessor::Int64(array) => push_int_key(out, array.value(row)),
            ActorAccessor::Int32(array) => push_int_key(out, i64::from(array.value(row))),
            ActorAccessor::Utf8(array) => push_utf8_key(out, array.value(row)),
            ActorAccessor::LargeUtf8(array) => push_utf8_key(out, array.value(row)),
            ActorAccessor::Utf8View(array) => push_utf8_key(out, array.value(row)),
            ActorAccessor::Binary(array) => push_binary_key(out, array.value(row)),
            ActorAccessor::FixedBinary(array) => push_binary_key(out, array.value(row)),
        }
        Ok(())
    }
}

/// The time column accessor: microseconds per row.
enum TimeAccessor<'a> {
    Micros(&'a TimestampMicrosecondArray),
    Millis(&'a TimestampMillisecondArray),
    Int(&'a Int64Array, TimeUnit),
}

impl<'a> TimeAccessor<'a> {
    /// Downcast the time array per its resolved kind.
    fn new(array: &'a Arc<dyn Array>, kind: TimeKind) -> Self {
        let any = array.as_any();
        match kind {
            TimeKind::Micros => TimeAccessor::Micros(any.downcast_ref().expect("us time")),
            TimeKind::Millis => TimeAccessor::Millis(any.downcast_ref().expect("ms time")),
            TimeKind::Int(unit) => TimeAccessor::Int(any.downcast_ref().expect("int time"), unit),
        }
    }

    /// Every row's time in microseconds, in one variant-matched pass; a null
    /// time raises with its global row.
    fn fill(&self, rows: usize, start_ordinal: u64) -> Result<Vec<i64>, EventError> {
        let mut out = Vec::with_capacity(rows);
        match self {
            TimeAccessor::Micros(array) => {
                for row in 0..rows {
                    if array.is_null(row) {
                        return Err(EventBuildError::NullTime {
                            row: start_ordinal + row as u64,
                        }
                        .into());
                    }
                    out.push(array.value(row));
                }
            }
            TimeAccessor::Millis(array) => {
                for row in 0..rows {
                    if array.is_null(row) {
                        return Err(EventBuildError::NullTime {
                            row: start_ordinal + row as u64,
                        }
                        .into());
                    }
                    out.push(array.value(row) * 1000);
                }
            }
            TimeAccessor::Int(array, unit) => {
                let scale = match unit {
                    TimeUnit::Micros => 1,
                    TimeUnit::Millis => 1000,
                };
                for row in 0..rows {
                    if array.is_null(row) {
                        return Err(EventBuildError::NullTime {
                            row: start_ordinal + row as u64,
                        }
                        .into());
                    }
                    out.push(array.value(row) * scale);
                }
            }
        }
        Ok(out)
    }

}

/// The declared tiebreak column accessor.
enum TiebreakAccessor<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
}

impl<'a> TiebreakAccessor<'a> {
    /// Downcast the tiebreak array per its resolved kind.
    fn new(array: &'a Arc<dyn Array>, kind: IntKind) -> Self {
        let any = array.as_any();
        match kind {
            IntKind::Int64 => TiebreakAccessor::Int64(any.downcast_ref().expect("i64 tiebreak")),
            IntKind::Int32 => TiebreakAccessor::Int32(any.downcast_ref().expect("i32 tiebreak")),
        }
    }

    /// Row `row`'s tiebreak value; a null tiebreak raises (it could not order
    /// the row).
    fn value(&self, row: usize, global_row: u64) -> Result<i64, EventError> {
        let (null, value) = match self {
            TiebreakAccessor::Int64(array) => (array.is_null(row), array.value(row)),
            TiebreakAccessor::Int32(array) => (array.is_null(row), i64::from(array.value(row))),
        };
        if null {
            return Err(EventBuildError::NullTime { row: global_row }.into());
        }
        Ok(value)
    }
}

// ---------------------------------------------------------------------------
// B2: finalize
// ---------------------------------------------------------------------------

/// One finalized shard's report.
pub(crate) struct ShardResult {
    pub(crate) entry: Option<ShardFileEntry>,
    pub(crate) min_ts: Option<i64>,
    pub(crate) max_ts: Option<i64>,
}

/// Run the whole build over the source, writing generation files under
/// `location` and returning the outcome for the caller to publish.
#[allow(clippy::too_many_lines)]
pub fn run_build(
    io: &StoreIo,
    location: &str,
    spec: &BuildSpec,
    params: &BuildParams,
    seeds: DictSeeds,
    prior_sidecars: &[Vec<Arc<ActorSidecar>>],
    source: &mut dyn BatchSource,
) -> Result<BuildOutcome, EventError> {
    let build_dir = io.root().join(location).join("build");
    std::fs::create_dir_all(&build_dir)
        .map_err(|error| EventBuildError::Io(format!("build dir: {error}")))?;
    let started = std::time::Instant::now();
    let outcome = run_build_inner(
        io,
        location,
        spec,
        params,
        seeds,
        prior_sidecars,
        source,
        &build_dir,
    );
    // The spill directory is transient scratch in every outcome; on failure
    // the caller also unlinks any generation files it saw published.
    std::fs::remove_dir_all(&build_dir).ok();
    let mut outcome = outcome?;
    let total_millis = started.elapsed().as_millis() as u64;
    outcome.finalize_millis = total_millis - outcome.partition_millis;
    Ok(outcome)
}

/// The outcome of building from a source that yielded no batches: an empty
/// (but live) dataset whose dictionary state carries the seeds forward
/// unchanged.
fn empty_source_outcome(spec: &BuildSpec, seeds: &DictSeeds) -> BuildOutcome {
    let mut dict_state = std::collections::BTreeMap::new();
    dict_state.insert(
        crate::model::EVENT_DICT.to_string(),
        seeds.event.len() as u64,
    );
    for (name, values) in &seeds.props {
        dict_state.insert(name.clone(), values.len() as u64);
    }
    BuildOutcome {
        shard_entries: Vec::new(),
        properties: spec.declared.clone().unwrap_or_default(),
        dict_entries: Vec::new(),
        dict_state,
        events: 0,
        new_actors: 0,
        byte_size: 0,
        min_ts: None,
        max_ts: None,
        spill_bytes: 0,
        partition_millis: 0,
        finalize_millis: 0,
    }
}

/// One spill file per shard under the build directory.
fn create_spill_files(
    build_dir: &std::path::Path,
    shards: usize,
) -> Result<Vec<Mutex<std::fs::File>>, EventError> {
    let mut spill_files = Vec::with_capacity(shards);
    for shard in 0..shards {
        let path = build_dir.join(format!("spill-{shard:04}.tmp"));
        let file = std::fs::File::create(&path)
            .map_err(|error| EventBuildError::Io(format!("spill create: {error}")))?;
        spill_files.push(Mutex::new(file));
    }
    Ok(spill_files)
}

/// The build body (separated so the spill directory cleanup wraps it).
#[allow(clippy::too_many_arguments)]
fn run_build_inner(
    io: &StoreIo,
    location: &str,
    spec: &BuildSpec,
    params: &BuildParams,
    seeds: DictSeeds,
    prior_sidecars: &[Vec<Arc<ActorSidecar>>],
    source: &mut dyn BatchSource,
    build_dir: &std::path::Path,
) -> Result<BuildOutcome, EventError> {
    let first = source
        .next_batch()
        .map_err(|message| EventBuildError::Io(format!("source read: {message}")))?;
    let Some(first_batch) = first else {
        return Ok(empty_source_outcome(spec, &seeds));
    };
    let layout = resolve_layout(&first_batch.schema(), spec)?;
    let mut prop_seed_values: Vec<Vec<String>> = Vec::new();
    let mut seeds = seeds;
    for (prop_index, (_, def)) in layout.props.iter().enumerate() {
        if layout.dict_slots[prop_index].is_some() {
            prop_seed_values.push(seeds.props.remove(&def.name).unwrap_or_default());
        }
    }
    let dicts = BuildDicts::new(seeds.event, prop_seed_values, params.dict_max_bytes);
    let dicts = &dicts;
    let threads = if params.threads == 0 {
        std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
    } else {
        params.threads
    };
    let shards = params.shards as usize;
    let buffer_bytes = (params.memory_bytes / (4 * u64::from(params.shards)))
        .clamp(256 * 1024, 4 * 1024 * 1024) as usize;
    let spill_files = create_spill_files(build_dir, shards)?;
    let partition = Partition {
        layout: &layout,
        dicts,
        actors: ActorDict::new(),
        shards: params.shards,
        buffer_bytes,
        spill_files,
        shard_bytes: (0..shards).map(|_| AtomicU64::new(0)).collect(),
        spill_total: AtomicU64::new(0),
    };
    let partition_started = std::time::Instant::now();
    let mut prefixed = PrefixedSource {
        first: Some(first_batch),
        rest: source,
    };
    let events = run_partition(&partition, &mut prefixed, threads, params.ordinal_start)?;
    let partition_millis = partition_started.elapsed().as_millis() as u64;
    drop(partition.spill_files);
    // B2 concurrency: bounded by the memory budget over the largest shard's
    // working set (raw columns + permutation + gather, ~3x its spill bytes).
    let largest = partition
        .shard_bytes
        .iter()
        .map(|bytes| bytes.load(Ordering::Relaxed))
        .max()
        .unwrap_or(0)
        .max(1);
    let by_memory = (params.memory_bytes / (largest * 3)).max(1) as usize;
    let workers = threads.min(by_memory).max(1);
    let peak_shard_budget = (params.memory_bytes / threads as u64).max(1024 * 1024);
    // Promoted dict properties resolve their code-only spill rows through a
    // values snapshot; the dictionaries are final once the partition ends,
    // and so is the actor dictionary the finalize reads identities from.
    let promoted_values = promoted_value_snapshots(&layout, dicts);
    let actor_table = partition.actors.freeze();
    let shard_results = finalize_shards(
        io,
        location,
        params,
        &layout,
        dicts,
        &actor_table,
        &promoted_values,
        prior_sidecars,
        build_dir,
        &partition.shard_bytes,
        peak_shard_budget,
        workers,
    )?;
    let (dict_entries, dict_state) =
        write_dict_files(io, location, params.generation, &layout, dicts)?;
    let mut outcome = BuildOutcome {
        shard_entries: Vec::new(),
        properties: final_properties(&layout, dicts),
        dict_entries,
        dict_state,
        events,
        new_actors: 0,
        byte_size: 0,
        min_ts: None,
        max_ts: None,
        spill_bytes: partition.spill_total.load(Ordering::Relaxed),
        partition_millis,
        finalize_millis: 0,
    };
    for result in shard_results {
        if let Some(entry) = result.entry {
            outcome.new_actors += entry.new_actors;
            outcome.byte_size += entry.bytes;
            outcome.shard_entries.push(entry);
        }
        outcome.min_ts = merge_min(outcome.min_ts, result.min_ts);
        outcome.max_ts = merge_max(outcome.max_ts, result.max_ts);
    }
    Ok(outcome)
}

/// Write this generation's dictionary files (the event dictionary plus every
/// unpromoted dict property with appended values) and pin the resulting
/// per-column code counts. A promoted property's partial dictionary is
/// discarded and it leaves the dictionary state.
fn write_dict_files(
    io: &StoreIo,
    location: &str,
    generation: u64,
    layout: &SourceLayout,
    dicts: &BuildDicts,
) -> Result<
    (
        Vec<crate::model::DictFileEntry>,
        std::collections::BTreeMap<String, u64>,
    ),
    EventError,
> {
    let mut entries = Vec::new();
    let mut state = std::collections::BTreeMap::new();
    let event = dicts.event.lock().expect("event dict lock poisoned");
    state.insert(crate::model::EVENT_DICT.to_string(), event.code_count());
    if !event.appended().is_empty() {
        let rel = format!(
            "{location}/dict/{}.gen-{generation}.dict",
            crate::model::EVENT_DICT
        );
        let bytes = crate::format::encode_dict(
            crate::model::EVENT_DICT,
            event.first_new_code(),
            event.appended(),
        );
        io.put_verified(&rel, bytes)?;
        entries.push(crate::model::DictFileEntry {
            column: crate::model::EVENT_DICT.to_string(),
            file: rel,
            codes_after: event.code_count(),
        });
    }
    drop(event);
    for (prop_index, (_, def)) in layout.props.iter().enumerate() {
        let Some(slot) = layout.dict_slots[prop_index] else {
            continue;
        };
        if dicts.props[slot].promoted.load(Ordering::Relaxed) {
            continue;
        }
        let dict = dicts.props[slot]
            .dict
            .lock()
            .expect("prop dict lock poisoned");
        state.insert(def.name.clone(), dict.code_count());
        if dict.appended().is_empty() {
            continue;
        }
        let rel = format!("{location}/dict/{}.gen-{generation}.dict", def.name);
        let bytes = crate::format::encode_dict(&def.name, dict.first_new_code(), dict.appended());
        io.put_verified(&rel, bytes)?;
        entries.push(crate::model::DictFileEntry {
            column: def.name.clone(),
            file: rel,
            codes_after: dict.code_count(),
        });
    }
    Ok((entries, state))
}

/// Min-merge two optional bounds.
pub(crate) fn merge_min(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(v), None) | (None, Some(v)) => Some(v),
        (None, None) => None,
    }
}

/// Max-merge two optional bounds.
pub(crate) fn merge_max(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(v), None) | (None, Some(v)) => Some(v),
        (None, None) => None,
    }
}

/// The final property declarations after promotion.
fn final_properties(layout: &SourceLayout, dicts: &BuildDicts) -> Vec<PropertyDef> {
    let mut dict_index = 0usize;
    let mut properties = Vec::with_capacity(layout.props.len());
    for (_, def) in &layout.props {
        let mut def = def.clone();
        if def.encoding == PropertyEncoding::Dict {
            if dicts.props[dict_index].promoted.load(Ordering::Relaxed) {
                def.encoding = PropertyEncoding::RawString;
            }
            dict_index += 1;
        }
        properties.push(def);
    }
    properties
}

/// A source that replays one already-pulled batch before the remainder.
struct PrefixedSource<'a> {
    first: Option<RecordBatch>,
    rest: &'a mut dyn BatchSource,
}

impl BatchSource for PrefixedSource<'_> {
    /// The replayed first batch, then the underlying stream.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        if let Some(batch) = self.first.take() {
            return Ok(Some(batch));
        }
        self.rest.next_batch()
    }

    /// The underlying stream's watermark (the replayed batch was pulled from
    /// the same stream, whose tracker already saw it).
    fn watermark(&self) -> Option<String> {
        self.rest.watermark()
    }
}

/// Run B2 across shards on a shared work index.
#[allow(clippy::too_many_arguments)]
fn finalize_shards(
    io: &StoreIo,
    location: &str,
    params: &BuildParams,
    layout: &SourceLayout,
    dicts: &BuildDicts,
    actor_table: &ActorTable,
    promoted_values: &[Option<Vec<String>>],
    prior_sidecars: &[Vec<Arc<ActorSidecar>>],
    build_dir: &std::path::Path,
    shard_bytes: &[AtomicU64],
    peak_shard_budget: u64,
    workers: usize,
) -> Result<Vec<ShardResult>, EventError> {
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<(usize, ShardResult)>> = Mutex::new(Vec::new());
    let failure: Mutex<Option<EventError>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let shard = next.fetch_add(1, Ordering::Relaxed);
                if shard >= params.shards as usize
                    || failure.lock().expect("failure lock").is_some()
                {
                    return;
                }
                let outcome = finalize_one_shard(
                    io,
                    location,
                    params,
                    layout,
                    dicts,
                    actor_table,
                    promoted_values,
                    prior_sidecars,
                    build_dir,
                    shard,
                    shard_bytes[shard].load(Ordering::Relaxed),
                    peak_shard_budget,
                );
                match outcome {
                    Ok(result) => results.lock().expect("results lock").push((shard, result)),
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
    let mut collected = results.into_inner().expect("results lock");
    collected.sort_by_key(|(shard, _)| *shard);
    Ok(collected.into_iter().map(|(_, result)| result).collect())
}

/// Finalize one shard: the in-memory path, or the external merge fallback
/// when the shard's spill outgrew twice its budget (extreme actor skew).
#[allow(clippy::too_many_arguments)]
fn finalize_one_shard(
    io: &StoreIo,
    location: &str,
    params: &BuildParams,
    layout: &SourceLayout,
    dicts: &BuildDicts,
    actor_table: &ActorTable,
    promoted_values: &[Option<Vec<String>>],
    prior_sidecars: &[Vec<Arc<ActorSidecar>>],
    build_dir: &std::path::Path,
    shard: usize,
    spilled_bytes: u64,
    peak_shard_budget: u64,
) -> Result<ShardResult, EventError> {
    let spill_path = build_dir.join(format!("spill-{shard:04}.tmp"));
    if spilled_bytes == 0 {
        std::fs::remove_file(&spill_path).ok();
        return Ok(ShardResult {
            entry: None,
            min_ts: None,
            max_ts: None,
        });
    }
    let priors = prior_sidecars.get(shard).map_or(&[][..], Vec::as_slice);
    let result = if spilled_bytes > peak_shard_budget * 2 {
        crate::build_external::finalize_external(
            io,
            location,
            params,
            &layout_props(layout),
            dicts,
            actor_table,
            promoted_values,
            priors,
            &spill_path,
            shard,
            peak_shard_budget,
        )?
    } else {
        finalize_in_memory(
            io,
            location,
            params,
            layout,
            dicts,
            actor_table,
            promoted_values,
            priors,
            &spill_path,
            shard,
        )?
    };
    std::fs::remove_file(&spill_path)
        .map_err(|error| EventBuildError::Io(format!("spill unlink: {error}")))?;
    Ok(result)
}

/// The property declarations of a layout (for the external path).
fn layout_props(layout: &SourceLayout) -> Vec<PropertyDef> {
    layout.props.iter().map(|(_, def)| def.clone()).collect()
}

/// The in-memory finalize path: load the shard's spill whole, assign ids,
/// sort a permutation, and stream rows into the segment encoder.
#[allow(clippy::too_many_arguments)]
fn finalize_in_memory(
    io: &StoreIo,
    location: &str,
    params: &BuildParams,
    layout: &SourceLayout,
    dicts: &BuildDicts,
    actor_table: &ActorTable,
    promoted_values: &[Option<Vec<String>>],
    priors: &[Arc<ActorSidecar>],
    spill_path: &std::path::Path,
    shard: usize,
) -> Result<ShardResult, EventError> {
    let mut columns = SpillColumns::default();
    read_frames(spill_path, |payload| {
        append_frame(payload, &layout.props, &mut columns)
    })?;
    let rows = columns.rows();
    let assignment = assign_local_ids(&columns, actor_table, priors, shard as u32)?;
    // Sort materialized (local, ts, tiebreak, row) records: one contiguous
    // comparison per element instead of three random array loads per key
    // evaluation (the closure-keyed sort was the finalize hot spot).
    let mut keyed: Vec<(u64, i64, i64, u32)> = Vec::with_capacity(rows);
    for row in 0..rows {
        keyed.push((
            assignment.row_locals[row],
            columns.ts[row],
            columns.tiebreak[row],
            row as u32,
        ));
    }
    keyed.sort_unstable();
    let promoted = promotion_flags(layout, dicts);
    let encodings = final_encodings(layout, &promoted);
    let mut encoder = SegEncoder::new(shard as u32, params.generation, &encodings);
    let mut cells: Vec<PropCell<'_>> = Vec::with_capacity(layout.props.len());
    let mut min_ts = None;
    let mut max_ts = None;
    // The sorted key tuples already hold (local, ts, tiebreak) in output
    // order, so the encoder reads them sequentially; only the event code,
    // property cells, and key bytes gather through the permutation.
    for (local, ts, tiebreak, row) in &keyed {
        let row = *row as usize;
        cells.clear();
        for (prop, is_promoted) in promoted.iter().enumerate() {
            cells.push(resolve_cell(
                columns.cell(prop, *is_promoted, row),
                promoted_values[prop].as_deref(),
            ));
        }
        min_ts = merge_min(min_ts, Some(*ts));
        max_ts = merge_max(max_ts, Some(*ts));
        encoder.push_row(
            *local,
            *ts,
            *tiebreak,
            columns.event[row],
            &cells,
            actor_table.key(columns.actors[row]),
        )?;
    }
    let entry = write_shard_files(
        io,
        location,
        params.generation,
        shard as u32,
        encoder,
        &assignment,
    )?;
    Ok(ShardResult {
        entry: Some(entry),
        min_ts,
        max_ts,
    })
}

/// Which dict properties ended the build promoted, positionally per property.
/// Per property: the dictionary values (code order) of a PROMOTED dict
/// property, `None` for everything else. Code-only spill rows of a promoted
/// property read their strings here at the encoder.
fn promoted_value_snapshots(layout: &SourceLayout, dicts: &BuildDicts) -> Vec<Option<Vec<String>>> {
    let flags = promotion_flags(layout, dicts);
    let mut snapshots = Vec::with_capacity(layout.props.len());
    for (prop_index, promoted) in flags.iter().enumerate() {
        let snapshot = match (promoted, layout.dict_slots[prop_index]) {
            (true, Some(slot)) => Some(
                dicts.props[slot]
                    .dict
                    .lock()
                    .expect("dict lock poisoned")
                    .values()
                    .to_vec(),
            ),
            _ => None,
        };
        snapshots.push(snapshot);
    }
    snapshots
}

/// A promoted property's code-only cell (spilled before the promotion)
/// resolved to its dictionary string; every other cell passes through.
fn resolve_cell<'a>(cell: PropCell<'a>, values: Option<&'a [String]>) -> PropCell<'a> {
    match (cell, values) {
        (PropCell::Code(code), Some(values)) => {
            PropCell::Str(values[usize::try_from(code).expect("code fits usize")].as_bytes())
        }
        (cell, _) => cell,
    }
}

fn promotion_flags(layout: &SourceLayout, dicts: &BuildDicts) -> Vec<bool> {
    let mut dict_index = 0usize;
    let mut flags = Vec::with_capacity(layout.props.len());
    for (_, def) in &layout.props {
        if def.encoding == PropertyEncoding::Dict {
            flags.push(dicts.props[dict_index].promoted.load(Ordering::Relaxed));
            dict_index += 1;
        } else {
            flags.push(false);
        }
    }
    flags
}

/// The final per-property encodings after promotion.
fn final_encodings(layout: &SourceLayout, promoted: &[bool]) -> Vec<PropertyEncoding> {
    layout
        .props
        .iter()
        .zip(promoted)
        .map(|((_, def), is_promoted)| {
            if *is_promoted {
                PropertyEncoding::RawString
            } else {
                def.encoding
            }
        })
        .collect()
}

/// The id assignment of one shard's spill: per-row local ids plus the new
/// actors in local order.
pub(crate) struct IdAssignment {
    pub row_locals: Vec<u64>,
    /// New locals in ascending order (their keys are in `new_keys`).
    pub new_locals: Vec<u64>,
    pub new_keys: Vec<Vec<u8>>,
    pub new_hashes: Vec<u64>,
    pub first_new_local: u64,
}

/// Deduplicate actors and assign dense local ids from the shard's DISTINCT
/// actor codes: the distinct set (bounded by the build's actors, never its
/// rows) sorts by `(hash, key bytes)` - the deterministic order the sidecars
/// pin - resolves returning actors through the priors, and assigns new ids
/// continuing from the priors' maximum; every row then maps through its code.
fn assign_local_ids(
    columns: &SpillColumns,
    actor_table: &ActorTable,
    priors: &[Arc<ActorSidecar>],
    shard: u32,
) -> Result<IdAssignment, EventError> {
    let mut present = vec![false; actor_table.keys.len()];
    for code in &columns.actors {
        present[*code as usize] = true;
    }
    let mut distinct: Vec<u32> = Vec::new();
    for (code, seen) in present.iter().enumerate() {
        if *seen {
            distinct.push(code as u32);
        }
    }
    distinct.sort_by(|a, b| {
        (actor_table.hash_of(*a), actor_table.key(*a))
            .cmp(&(actor_table.hash_of(*b), actor_table.key(*b)))
    });
    let mut next_local: u64 = priors
        .iter()
        .map(|sidecar| sidecar.first_local_id + sidecar.index.len() as u64)
        .max()
        .unwrap_or(0);
    let first_new_local = next_local;
    let mut new_locals = Vec::new();
    let mut new_keys: Vec<Vec<u8>> = Vec::new();
    let mut new_hashes = Vec::new();
    let mut code_local = vec![0u64; actor_table.keys.len()];
    for code in &distinct {
        let local = resolve_or_assign(
            priors,
            actor_table.hash_of(*code),
            actor_table.key(*code),
            &mut next_local,
            &mut new_locals,
            &mut new_keys,
            &mut new_hashes,
            shard,
        )?;
        code_local[*code as usize] = local;
    }
    let mut row_locals = vec![0u64; columns.rows()];
    for (row, code) in columns.actors.iter().enumerate() {
        row_locals[row] = code_local[*code as usize];
    }
    Ok(IdAssignment {
        row_locals,
        new_locals,
        new_keys,
        new_hashes,
        first_new_local,
    })
}

/// Resolve one `(hash, key)` to its local id: a prior-generation actor keeps
/// its id; an unseen actor takes the next dense id and is recorded for the
/// sidecar.
#[allow(clippy::too_many_arguments)]
fn resolve_or_assign(
    priors: &[Arc<ActorSidecar>],
    hash: u64,
    key: &[u8],
    next_local: &mut u64,
    new_locals: &mut Vec<u64>,
    new_keys: &mut Vec<Vec<u8>>,
    new_hashes: &mut Vec<u64>,
    shard: u32,
) -> Result<u64, EventError> {
    if let Some(local) = lookup_prior(priors, hash, key) {
        return Ok(local);
    }
    let local = *next_local;
    *next_local = next_local
        .checked_add(1)
        .ok_or(EventBuildError::ShardOverflow { shard })?;
    new_locals.push(local);
    new_keys.push(key.to_vec());
    new_hashes.push(hash);
    Ok(local)
}

/// Resolve `(hash, key)` against prior generations' sidecars.
pub(crate) fn lookup_prior(priors: &[Arc<ActorSidecar>], hash: u64, key: &[u8]) -> Option<u64> {
    for sidecar in priors {
        if let Some(local) = sidecar.lookup(hash, key) {
            return Some(local);
        }
    }
    None
}

/// Write one shard's `.seg` and `.act` files and build its file-list entry.
pub(crate) fn write_shard_files(
    io: &StoreIo,
    location: &str,
    generation: u64,
    shard: u32,
    encoder: SegEncoder,
    assignment: &IdAssignment,
) -> Result<ShardFileEntry, EventError> {
    let events = encoder.events();
    let seg_bytes = encoder.finish();
    let seg_rel = format!("{location}/shard-{shard:04}/gen-{generation}.seg");
    let act_rel = format!("{location}/shard-{shard:04}/gen-{generation}.act");
    let byte_size = seg_bytes.len() as u64;
    io.put_verified(&seg_rel, seg_bytes)?;
    let mut new_actors: Vec<(u64, u64, &[u8])> = Vec::with_capacity(assignment.new_locals.len());
    for (index, local) in assignment.new_locals.iter().enumerate() {
        new_actors.push((
            assignment.new_hashes[index],
            *local,
            assignment.new_keys[index].as_slice(),
        ));
    }
    let act_bytes = encode_act(shard, generation, assignment.first_new_local, &new_actors);
    io.put_verified(&act_rel, act_bytes)?;
    Ok(ShardFileEntry {
        shard,
        seg: seg_rel,
        act: act_rel,
        events,
        new_actors: assignment.new_locals.len() as u64,
        bytes: byte_size,
    })
}

/// The peak resident set size of this process in bytes (`VmHWM` from
/// `/proc/self/status`), reported with build metrics so the memory ceiling is
/// measured, not claimed.
pub fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// One row's owned property cell in the external merge path.
#[derive(Debug, Clone)]
pub(crate) enum PropRowCell {
    Null,
    Code(u64),
    Str(Vec<u8>),
    I64(i64),
    F64(f64),
    Bool(bool),
}

impl PropRowCell {
    /// The borrowed encoder cell of this owned cell.
    pub(crate) fn as_cell(&self) -> PropCell<'_> {
        match self {
            PropRowCell::Null => PropCell::Null,
            PropRowCell::Code(code) => PropCell::Code(*code),
            PropRowCell::Str(bytes) => PropCell::Str(bytes),
            PropRowCell::I64(value) => PropCell::I64(*value),
            PropRowCell::F64(value) => PropCell::F64(*value),
            PropRowCell::Bool(value) => PropCell::Bool(*value),
        }
    }
}

/// Stream a spill file's frames into per-frame `SpillColumns` chunks (the
/// external path processes one frame-chunk at a time instead of the whole
/// file).
pub(crate) fn append_frame_reader(
    path: &std::path::Path,
    props: &[PropertyDef],
    mut on_chunk: impl FnMut(SpillChunk) -> Result<(), EventError>,
) -> Result<(), EventError> {
    let indexed: Vec<(usize, PropertyDef)> = props.iter().cloned().enumerate().collect();
    read_frames(path, |payload| {
        let mut columns = SpillColumns::default();
        append_frame(payload, &indexed, &mut columns)?;
        on_chunk(SpillChunk { columns })
    })
}

/// One decoded spill frame for the external path.
pub(crate) struct SpillChunk {
    columns: SpillColumns,
}

impl SpillChunk {
    /// Rows in this chunk.
    pub(crate) fn rows(&self) -> usize {
        self.columns.rows()
    }

    /// The actor code of row `row`.
    pub(crate) fn actor(&self, row: usize) -> u32 {
        self.columns.actors[row]
    }

    /// The `(ts, tiebreak, event)` of row `row`.
    pub(crate) fn order(&self, row: usize) -> (i64, i64, u64) {
        (
            self.columns.ts[row],
            self.columns.tiebreak[row],
            self.columns.event[row],
        )
    }

    /// The owned property cells of row `row` under final encodings; a
    /// promoted property's code-only rows resolve through `promoted_values`.
    pub(crate) fn cells(
        &self,
        promoted: &[bool],
        promoted_values: &[Option<Vec<String>>],
        row: usize,
    ) -> Vec<PropRowCell> {
        let mut cells = Vec::with_capacity(self.columns.props.len());
        for (prop, is_promoted) in promoted.iter().enumerate() {
            let cell = resolve_cell(
                self.columns.cell(prop, *is_promoted, row),
                promoted_values[prop].as_deref(),
            );
            cells.push(match cell {
                PropCell::Null => PropRowCell::Null,
                PropCell::Code(code) => PropRowCell::Code(code),
                PropCell::Str(bytes) => PropRowCell::Str(bytes.to_vec()),
                PropCell::I64(value) => PropRowCell::I64(value),
                PropCell::F64(value) => PropRowCell::F64(value),
                PropCell::Bool(value) => PropRowCell::Bool(value),
            });
        }
        cells
    }
}
