//! The event store's core value model: canonical actor keys and their routing
//! hash, the width-adaptive dictionary code vector, the property schema, and
//! the dataset descriptor the registry persists.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::error::EventBuildError;

/// The canonical actor-key tag bytes. The tag is part of identity: integer
/// `42` and string `"42"` are different actors.
pub const KEY_TAG_INT: u8 = 0x01;
pub const KEY_TAG_UTF8: u8 = 0x02;
pub const KEY_TAG_BINARY: u8 = 0x03;

/// Append the canonical key bytes of a signed-integer actor id.
pub fn push_int_key(out: &mut Vec<u8>, value: i64) {
    out.push(KEY_TAG_INT);
    out.extend_from_slice(&value.to_le_bytes());
}

/// Append the canonical key bytes of a UTF-8 string actor id.
pub fn push_utf8_key(out: &mut Vec<u8>, value: &str) {
    out.push(KEY_TAG_UTF8);
    out.extend_from_slice(value.as_bytes());
}

/// Append the canonical key bytes of a binary actor id (a 16-byte UUID or
/// arbitrary bytes).
pub fn push_binary_key(out: &mut Vec<u8>, value: &[u8]) {
    out.push(KEY_TAG_BINARY);
    out.extend_from_slice(value);
}

/// A human-readable rendering of canonical key bytes for error messages:
/// integers print numerically, strings verbatim, binary as hex.
pub fn describe_key(key: &[u8]) -> String {
    match key.split_first() {
        Some((&KEY_TAG_INT, payload)) if payload.len() == 8 => {
            let mut raw = [0u8; 8];
            raw.copy_from_slice(payload);
            i64::from_le_bytes(raw).to_string()
        }
        Some((&KEY_TAG_UTF8, payload)) => String::from_utf8_lossy(payload).into_owned(),
        Some((&KEY_TAG_BINARY | _, payload)) => {
            let mut text = String::with_capacity(payload.len() * 2);
            for byte in payload {
                write!(text, "{byte:02x}").expect("formatting into a String does not fail");
            }
            format!("0x{text}")
        }
        None => "<empty key>".to_string(),
    }
}

/// 64-bit FNV-1a over arbitrary bytes: the store's dependency-free hash
/// primitive, also used for region checksums and location hashing.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The splitmix64 finalizer: fixes FNV-1a's weak avalanche on short keys so
/// shard routing does not skew.
pub fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    value
}

/// The actor routing hash over canonical key bytes. Correctness never depends
/// on its uniqueness: equal hashes disambiguate by comparing raw key bytes.
pub fn hash_key(key_bytes: &[u8]) -> u64 {
    mix64(fnv1a64(key_bytes))
}

/// A fast non-cryptographic hasher for the engine's hot count maps (the
/// FxHash fold: rotate, xor, multiply). Never used for identity or on-disk
/// state - routing and checksums stay on the keyed FNV-1a/splitmix pair.
#[derive(Default, Clone, Copy)]
pub struct FxHasher(u64);

impl std::hash::Hasher for FxHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.add(u64::from(*byte));
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.add(u64::from(value));
    }

    fn write_u32(&mut self, value: u32) {
        self.add(u64::from(value));
    }

    fn write_u64(&mut self, value: u64) {
        self.add(value);
    }

    fn write_i64(&mut self, value: i64) {
        self.add(value as u64);
    }

    fn write_usize(&mut self, value: usize) {
        self.add(value as u64);
    }
}

impl FxHasher {
    /// Fold one word into the state.
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}

/// The build-hasher of `FastMap`.
#[derive(Default, Clone, Copy)]
pub struct FxBuild;

impl std::hash::BuildHasher for FxBuild {
    type Hasher = FxHasher;

    fn build_hasher(&self) -> FxHasher {
        FxHasher::default()
    }
}

/// A hash map over the fast hasher, for per-event count maps.
pub type FastMap<K, V> = std::collections::HashMap<K, V, FxBuild>;

/// A width-adaptive vector of dictionary codes: the in-memory decoded form of
/// a code column block. The width is chosen from the data's maximum code;
/// nothing caps the code space (codes are u64 logically).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeVec {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
}

impl CodeVec {
    /// Build the narrowest vector holding `codes`.
    pub fn from_codes(codes: &[u64]) -> Self {
        let max = codes.iter().copied().max().unwrap_or(0);
        if u8::try_from(max).is_ok() {
            CodeVec::U8(codes.iter().map(|&c| c as u8).collect())
        } else if u16::try_from(max).is_ok() {
            CodeVec::U16(codes.iter().map(|&c| c as u16).collect())
        } else if u32::try_from(max).is_ok() {
            CodeVec::U32(codes.iter().map(|&c| c as u32).collect())
        } else {
            CodeVec::U64(codes.to_vec())
        }
    }

    /// The number of codes held.
    pub fn len(&self) -> usize {
        match self {
            CodeVec::U8(v) => v.len(),
            CodeVec::U16(v) => v.len(),
            CodeVec::U32(v) => v.len(),
            CodeVec::U64(v) => v.len(),
        }
    }

    /// Whether the vector is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The code at `index`, widened to u64.
    pub fn get(&self, index: usize) -> u64 {
        match self {
            CodeVec::U8(v) => u64::from(v[index]),
            CodeVec::U16(v) => u64::from(v[index]),
            CodeVec::U32(v) => u64::from(v[index]),
            CodeVec::U64(v) => v[index],
        }
    }

    /// The heap bytes held (for cache accounting).
    pub fn heap_bytes(&self) -> usize {
        match self {
            CodeVec::U8(v) => v.len(),
            CodeVec::U16(v) => v.len() * 2,
            CodeVec::U32(v) => v.len() * 4,
            CodeVec::U64(v) => v.len() * 8,
        }
    }
}

/// How one property column is physically encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyEncoding {
    /// Dictionary-coded string.
    Dict,
    /// Verbatim string blocks (the unbounded-cardinality fallback a promotion
    /// records).
    RawString,
    /// Raw 64-bit signed integers.
    I64,
    /// Raw 64-bit floats.
    F64,
    /// One bit per event.
    Bool,
}

impl PropertyEncoding {
    /// The persisted encoding token.
    pub fn token(self) -> &'static str {
        match self {
            PropertyEncoding::Dict => "dict",
            PropertyEncoding::RawString => "raw_string",
            PropertyEncoding::I64 => "i64",
            PropertyEncoding::F64 => "f64",
            PropertyEncoding::Bool => "bool",
        }
    }
}

/// One declared property of a dataset (a `property_schema` JSON entry).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyDef {
    pub name: String,
    /// The engine type name (`fq_common::DataType` serialized form).
    pub data_type: fq_common::DataType,
    pub encoding: PropertyEncoding,
    pub nullable: bool,
}

/// The reserved dictionary name of the event-name column.
pub const EVENT_DICT: &str = "__event__";

/// One shard file entry of the registry `file_list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardFileEntry {
    pub shard: u32,
    pub seg: String,
    pub act: String,
    pub events: u64,
    pub new_actors: u64,
    pub bytes: u64,
}

/// One dictionary file entry of the registry `file_list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DictFileEntry {
    pub column: String,
    pub file: String,
    pub codes_after: u64,
}

/// One generation of the registry `file_list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationEntry {
    pub generation: u64,
    pub shards: Vec<ShardFileEntry>,
    pub dicts: Vec<DictFileEntry>,
}

/// The registry `file_list` JSON: the ONLY source of truth for which files
/// compose a dataset; readers never trust a directory listing.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FileList {
    pub generations: Vec<GenerationEntry>,
}

/// One registered event dataset: the registry row, deserialized.
#[derive(Debug, Clone, PartialEq)]
pub struct DatasetRecord {
    pub name: String,
    pub location: String,
    /// `table` or `select`.
    pub source_kind: String,
    /// `datasource.schema.table` or the defining SELECT sql.
    pub source_ref: String,
    pub actor_column: String,
    pub time_column: String,
    pub time_unit: String,
    pub event_column: String,
    /// The declared tiebreak column, or None for the synthetic ingest ordinal.
    pub tiebreak_column: Option<String>,
    pub properties: Vec<PropertyDef>,
    pub shards: u32,
    pub file_list: FileList,
    /// Per dictionary column, the pinned total code count.
    pub dict_state: std::collections::BTreeMap<String, u64>,
    pub refresh_key: Option<String>,
    /// The stored watermark JSON (opaque to the store; the runtime renders it
    /// into the delta predicate).
    pub watermark: Option<String>,
    pub source_token: Option<String>,
    pub measured_events: i64,
    pub measured_actors: i64,
    pub byte_size: i64,
    pub build_millis: i64,
    /// The dataset's measured event-time extremes, present once at least one
    /// event is stored; they anchor default FROM/TO ranges and retention grid
    /// extents without any data scan.
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    pub created_at: String,
    pub refreshed_at: Option<String>,
}

/// Narrow a u64 (an on-disk count) to usize, raising `ShardOverflow` on a
/// platform that cannot address it.
pub fn usize_of(value: u64, shard: u32) -> Result<usize, EventBuildError> {
    usize::try_from(value).map_err(|_| EventBuildError::ShardOverflow { shard })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_tags_separate_types() {
        let mut int_key = Vec::new();
        push_int_key(&mut int_key, 42);
        let mut str_key = Vec::new();
        push_utf8_key(&mut str_key, "42");
        assert_ne!(int_key, str_key);
        assert_ne!(hash_key(&int_key), hash_key(&str_key));
        assert_eq!(describe_key(&int_key), "42");
        assert_eq!(describe_key(&str_key), "42");
    }

    #[test]
    fn hash_spreads_dense_ids_across_shards() {
        // Dense integer ids must not collapse onto few shards; with 64 shards
        // and 64k dense ids every shard must be populated.
        let mut seen = vec![0u32; 64];
        let mut key = Vec::new();
        for id in 0..65_536_i64 {
            key.clear();
            push_int_key(&mut key, id);
            seen[(hash_key(&key) & 63) as usize] += 1;
        }
        for (shard, count) in seen.iter().enumerate() {
            assert!(*count > 512, "shard {shard} starved: {count}");
        }
    }

    #[test]
    fn code_vec_adapts_width_to_the_data() {
        let small = CodeVec::from_codes(&[0, 1, 255]);
        assert!(matches!(small, CodeVec::U8(_)));
        let wide = CodeVec::from_codes(&[0, 70_000]);
        assert!(!matches!(wide, CodeVec::U16(_)));
        assert!(matches!(wide, CodeVec::U32(_)));
        let huge = CodeVec::from_codes(&[u64::from(u32::MAX) + 10]);
        assert!(matches!(huge, CodeVec::U64(_)));
        assert_eq!(wide.get(1), 70_000);
    }
}
