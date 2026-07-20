//! The on-disk byte formats of the event store: the shard segment file
//! (`.seg`), the actor sidecar (`.act`), and the dictionary generation file
//! (`.dict`), plus the block codecs they share.
//!
//! All integers are little-endian. `varint` is LEB128; signed deltas are
//! zigzag-coded. Checksums are FNV-1a 64 over the exact stored region bytes.
//! Every decode validates lengths and checksums and raises
//! `EventStoreError::Corrupt` on any inconsistency - never a partial or
//! silent read.
//!
//! A `.seg` file is: header, one region per column (ts, tiebreak, event, then
//! properties in declared order), the actor directory region, the block stats
//! region, the pre-aggregate region, then a footer holding a region directory
//! with per-region checksums. Every column is a sequence of independently
//! encoded and zstd-compressed blocks of `BLOCK_ROWS` events, so decode is
//! block-granular and cacheable.

use crate::model::FastMap;

use crate::error::{EventBuildError, EventStoreError};
use crate::model::{describe_key, CodeVec, PropertyEncoding};

/// Events per column block; the last block of a file is short.
pub const BLOCK_ROWS: usize = 65_536;

/// The `.seg` leading magic.
pub const SEG_MAGIC: &[u8; 8] = b"FQEVSEG1";
/// The `.act` leading magic.
pub const ACT_MAGIC: &[u8; 8] = b"FQEVACT1";
/// The shared trailing magic of `.seg` and `.act`.
pub const END_MAGIC: &[u8; 8] = b"FQEVEND1";
/// The `.dict` leading magic.
pub const DICT_MAGIC: &[u8; 8] = b"FQEVDCT1";
/// The one format version this build reads and writes.
pub const FORMAT_VERSION: u32 = 1;

/// Region kinds of the `.seg` footer directory.
pub const REGION_COLUMN: u8 = 0;
pub const REGION_ACTOR_DIR: u8 = 1;
pub const REGION_BLOCK_STATS: u8 = 2;
pub const REGION_PREAGG: u8 = 3;
/// Region kinds of the `.act` footer directory.
pub const REGION_HASH_INDEX: u8 = 4;
pub const REGION_KEY_HEAP: u8 = 5;

/// 64-bit FNV-1a (re-exported from the model for the format's checksums).
pub use crate::model::fnv1a64;

// ---------------------------------------------------------------------------
// varint / zigzag primitives
// ---------------------------------------------------------------------------

/// Append a LEB128 varint.
pub fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Zigzag-encode a signed value.
pub fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// Decode a zigzag-encoded value.
pub fn unzigzag(value: u64) -> i64 {
    (value >> 1).cast_signed() ^ -(value & 1).cast_signed()
}

/// A bounds-checked little-endian reader over a byte slice; every read
/// failure names the file it came from.
pub struct ByteReader<'a> {
    bytes: &'a [u8],
    pos: usize,
    file: &'a str,
}

impl<'a> ByteReader<'a> {
    /// Read from the start of `bytes`, attributing errors to `file`.
    pub fn new(bytes: &'a [u8], file: &'a str) -> Self {
        Self {
            bytes,
            pos: 0,
            file,
        }
    }

    /// The corruption error for a short or malformed read.
    fn corrupt(&self, detail: &str) -> EventStoreError {
        EventStoreError::Corrupt {
            file: self.file.to_string(),
            detail: format!("{detail} at byte {}", self.pos),
        }
    }

    /// Take `count` raw bytes.
    pub fn take(&mut self, count: usize) -> Result<&'a [u8], EventStoreError> {
        let end = self
            .pos
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| self.corrupt("truncated bytes"))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    /// Read a u32.
    pub fn u32(&mut self) -> Result<u32, EventStoreError> {
        let raw = self.take(4)?;
        Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    /// Read a u64.
    pub fn u64(&mut self) -> Result<u64, EventStoreError> {
        let raw = self.take(8)?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(raw);
        Ok(u64::from_le_bytes(buf))
    }

    /// Read an i64.
    pub fn i64(&mut self) -> Result<i64, EventStoreError> {
        Ok(self.u64()?.cast_signed())
    }

    /// Read one byte.
    pub fn u8(&mut self) -> Result<u8, EventStoreError> {
        Ok(self.take(1)?[0])
    }

    /// Read a LEB128 varint.
    pub fn varint(&mut self) -> Result<u64, EventStoreError> {
        let mut value: u64 = 0;
        let mut shift = 0u32;
        loop {
            let byte = self.u8()?;
            if shift >= 64 {
                return Err(self.corrupt("varint overflows 64 bits"));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    /// Read a zigzag varint.
    pub fn signed_varint(&mut self) -> Result<i64, EventStoreError> {
        Ok(unzigzag(self.varint()?))
    }

    /// Whether every byte has been consumed.
    pub fn at_end(&self) -> bool {
        self.pos == self.bytes.len()
    }

    /// The byte position of the next unread byte.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// The unread remainder.
    pub fn rest(&mut self) -> &'a [u8] {
        let slice = &self.bytes[self.pos..];
        self.pos = self.bytes.len();
        slice
    }
}

// ---------------------------------------------------------------------------
// zstd helpers
// ---------------------------------------------------------------------------

/// zstd-compress `payload` at `level`.
fn compress(payload: &[u8], level: i32) -> Vec<u8> {
    zstd::bulk::compress(payload, level).expect("zstd compression is infallible on memory buffers")
}

/// zstd-decompress `bytes` into exactly `uncompressed_len` bytes.
fn decompress(
    bytes: &[u8],
    uncompressed_len: usize,
    file: &str,
) -> Result<Vec<u8>, EventStoreError> {
    let out = zstd::bulk::decompress(bytes, uncompressed_len).map_err(|error| {
        EventStoreError::Corrupt {
            file: file.to_string(),
            detail: format!("zstd decompression failed: {error}"),
        }
    })?;
    if out.len() != uncompressed_len {
        return Err(EventStoreError::Corrupt {
            file: file.to_string(),
            detail: format!(
                "zstd payload decompressed to {} bytes, expected {uncompressed_len}",
                out.len()
            ),
        });
    }
    Ok(out)
}

/// Wrap a region payload as `[uncompressed_len u64][zstd bytes]` (the
/// container used by the actor-directory, stats, pre-agg, and sidecar
/// regions; column regions carry per-block compression instead).
fn compress_region(payload: &[u8], level: i32) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + payload.len() / 2);
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(&compress(payload, level));
    out
}

/// Unwrap a `[uncompressed_len u64][zstd bytes]` region container.
fn decompress_region(bytes: &[u8], file: &str) -> Result<Vec<u8>, EventStoreError> {
    let mut reader = ByteReader::new(bytes, file);
    let uncompressed_len = reader.u64()?;
    let uncompressed_len =
        usize::try_from(uncompressed_len).map_err(|_| EventStoreError::Corrupt {
            file: file.to_string(),
            detail: "region uncompressed length is not addressable".to_string(),
        })?;
    decompress(reader.rest(), uncompressed_len, file)
}

// ---------------------------------------------------------------------------
// Column model
// ---------------------------------------------------------------------------

/// The physical kind of one `.seg` column, driving its block codec and stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    /// Event time, i64 microseconds, delta-coded.
    Time,
    /// The order tiebreak, i64, delta-coded.
    Tiebreak,
    /// The event-name dictionary code.
    Event,
    /// A property with the given encoding (nullable, validity-carrying).
    Prop(PropertyEncoding),
}

/// One row's value for one property, as fed to the segment encoder.
#[derive(Debug, Clone, Copy)]
pub enum PropCell<'a> {
    Null,
    Code(u64),
    Str(&'a [u8]),
    I64(i64),
    F64(f64),
    Bool(bool),
}

/// One decoded property block: per-row validity (byte per row, 1 = valid;
/// None when the block has no nulls) plus the typed values.
#[derive(Debug, Clone, PartialEq)]
pub struct PropBlock {
    pub validity: Option<Vec<u8>>,
    pub values: PropValues,
}

/// The typed values of a decoded property block.
#[derive(Debug, Clone, PartialEq)]
pub enum PropValues {
    Codes(CodeVec),
    I64(Vec<i64>),
    F64(Vec<f64>),
    /// Byte per row, 1 = true.
    Bool(Vec<u8>),
    /// `offsets` has row_count + 1 entries into `bytes`.
    Str {
        offsets: Vec<u32>,
        bytes: Vec<u8>,
    },
}

/// One decoded column block.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedBlock {
    /// A ts or tiebreak block.
    I64(Vec<i64>),
    /// An event-code block.
    Codes(CodeVec),
    /// A property block.
    Prop(PropBlock),
}

impl DecodedBlock {
    /// Approximate heap bytes, for cache accounting.
    pub fn heap_bytes(&self) -> usize {
        match self {
            DecodedBlock::I64(values) => values.len() * 8,
            DecodedBlock::Codes(codes) => codes.heap_bytes(),
            DecodedBlock::Prop(prop) => {
                let validity = prop.validity.as_ref().map_or(0, Vec::len);
                validity
                    + match &prop.values {
                        PropValues::Codes(codes) => codes.heap_bytes(),
                        PropValues::I64(values) => values.len() * 8,
                        PropValues::F64(values) => values.len() * 8,
                        PropValues::Bool(values) => values.len(),
                        PropValues::Str { offsets, bytes } => offsets.len() * 4 + bytes.len(),
                    }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Block encoding
// ---------------------------------------------------------------------------

/// Encode a delta-coded i64 block: first value raw, then zigzag varint deltas
/// versus the previous value in the block.
fn encode_i64_block(values: &[i64], out: &mut Vec<u8>) {
    let mut previous = 0i64;
    for (index, value) in values.iter().enumerate() {
        if index == 0 {
            out.extend_from_slice(&value.to_le_bytes());
        } else {
            put_varint(out, zigzag(value.wrapping_sub(previous)));
        }
        previous = *value;
    }
}

/// Decode a delta-coded i64 block of `rows` values.
fn decode_i64_block(reader: &mut ByteReader<'_>, rows: usize) -> Result<Vec<i64>, EventStoreError> {
    let mut values = Vec::with_capacity(rows);
    if rows == 0 {
        return Ok(values);
    }
    let mut previous = reader.i64()?;
    values.push(previous);
    for _ in 1..rows {
        previous = previous.wrapping_add(reader.signed_varint()?);
        values.push(previous);
    }
    Ok(values)
}

/// Encode a code block: `width u8` in {1,2,4,8} chosen from the block's
/// maximum code, then the fixed-width array. The width adapts to the DATA and
/// never caps the code space.
fn encode_code_block(codes: &[u64], out: &mut Vec<u8>) {
    let max = codes.iter().copied().max().unwrap_or(0);
    if u8::try_from(max).is_ok() {
        out.push(1);
        for code in codes {
            out.push(*code as u8);
        }
    } else if u16::try_from(max).is_ok() {
        out.push(2);
        for code in codes {
            out.extend_from_slice(&(*code as u16).to_le_bytes());
        }
    } else if u32::try_from(max).is_ok() {
        out.push(4);
        for code in codes {
            out.extend_from_slice(&(*code as u32).to_le_bytes());
        }
    } else {
        out.push(8);
        for code in codes {
            out.extend_from_slice(&code.to_le_bytes());
        }
    }
}

/// Decode a code block of `rows` values into the matching-width vector.
fn decode_code_block(reader: &mut ByteReader<'_>, rows: usize) -> Result<CodeVec, EventStoreError> {
    let width = reader.u8()?;
    match width {
        1 => Ok(CodeVec::U8(reader.take(rows)?.to_vec())),
        2 => {
            let raw = reader.take(rows * 2)?;
            let mut values = Vec::with_capacity(rows);
            for chunk in raw.chunks_exact(2) {
                values.push(u16::from_le_bytes([chunk[0], chunk[1]]));
            }
            Ok(CodeVec::U16(values))
        }
        4 => {
            let raw = reader.take(rows * 4)?;
            let mut values = Vec::with_capacity(rows);
            for chunk in raw.chunks_exact(4) {
                values.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            Ok(CodeVec::U32(values))
        }
        8 => {
            let raw = reader.take(rows * 8)?;
            let mut values = Vec::with_capacity(rows);
            for chunk in raw.chunks_exact(8) {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(chunk);
                values.push(u64::from_le_bytes(buf));
            }
            Ok(CodeVec::U64(values))
        }
        other => Err(EventStoreError::Corrupt {
            file: reader.file.to_string(),
            detail: format!("code block has invalid width {other}"),
        }),
    }
}

/// Pack a byte-per-row 0/1 vector into an LSB-first bitmap.
fn pack_bits(values: &[u8], out: &mut Vec<u8>) {
    let mut current = 0u8;
    for (index, value) in values.iter().enumerate() {
        if *value != 0 {
            current |= 1 << (index % 8);
        }
        if index % 8 == 7 {
            out.push(current);
            current = 0;
        }
    }
    if !values.len().is_multiple_of(8) {
        out.push(current);
    }
}

/// Unpack an LSB-first bitmap into a byte-per-row 0/1 vector of `rows`.
fn unpack_bits(raw: &[u8], rows: usize) -> Vec<u8> {
    let mut values = Vec::with_capacity(rows);
    for index in 0..rows {
        values.push((raw[index / 8] >> (index % 8)) & 1);
    }
    values
}

/// Encode one property block: `has_nulls u8`, the validity bitmap when nulls
/// are present, then the encoding-specific payload for ALL rows (null rows
/// carry a zero/empty placeholder governed by validity - null is never a
/// sentinel code).
fn encode_prop_block(
    encoding: PropertyEncoding,
    validity: Option<&[u8]>,
    payload: PropSlice<'_>,
    out: &mut Vec<u8>,
) {
    match validity {
        Some(bits) if bits.contains(&0) => {
            out.push(1);
            pack_bits(bits, out);
        }
        Some(_) | None => out.push(0),
    }
    match (encoding, payload) {
        (PropertyEncoding::Dict, PropSlice::Codes(codes)) => encode_code_block(codes, out),
        (PropertyEncoding::I64, PropSlice::I64(values)) => {
            for value in values {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        (PropertyEncoding::F64, PropSlice::F64(values)) => {
            for value in values {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        (PropertyEncoding::Bool, PropSlice::Bool(values)) => pack_bits(values, out),
        (PropertyEncoding::RawString, PropSlice::Str { lens, bytes }) => {
            for len in lens {
                put_varint(out, u64::from(*len));
            }
            out.extend_from_slice(bytes);
        }
        (encoding, payload) => {
            unreachable!("property encoder fed a mismatched payload: {encoding:?} vs {payload:?}")
        }
    }
}

/// A borrowed property block payload fed to the encoder.
#[derive(Debug, Clone, Copy)]
enum PropSlice<'a> {
    Codes(&'a [u64]),
    I64(&'a [i64]),
    F64(&'a [f64]),
    Bool(&'a [u8]),
    Str { lens: &'a [u32], bytes: &'a [u8] },
}

/// Decode one property block of `rows` values.
fn decode_prop_block(
    encoding: PropertyEncoding,
    reader: &mut ByteReader<'_>,
    rows: usize,
) -> Result<PropBlock, EventStoreError> {
    let has_nulls = reader.u8()?;
    let validity = match has_nulls {
        0 => None,
        1 => Some(unpack_bits(reader.take(rows.div_ceil(8))?, rows)),
        other => {
            return Err(EventStoreError::Corrupt {
                file: reader.file.to_string(),
                detail: format!("property block has invalid null flag {other}"),
            })
        }
    };
    let values = match encoding {
        PropertyEncoding::Dict => PropValues::Codes(decode_code_block(reader, rows)?),
        PropertyEncoding::I64 => {
            let mut values = Vec::with_capacity(rows);
            for _ in 0..rows {
                values.push(reader.i64()?);
            }
            PropValues::I64(values)
        }
        PropertyEncoding::F64 => {
            let mut values = Vec::with_capacity(rows);
            for _ in 0..rows {
                values.push(f64::from_bits(reader.u64()?));
            }
            PropValues::F64(values)
        }
        PropertyEncoding::Bool => {
            PropValues::Bool(unpack_bits(reader.take(rows.div_ceil(8))?, rows))
        }
        PropertyEncoding::RawString => {
            let mut offsets = Vec::with_capacity(rows + 1);
            offsets.push(0u32);
            let mut total = 0u64;
            for _ in 0..rows {
                total += reader.varint()?;
                let offset = u32::try_from(total).map_err(|_| EventStoreError::Corrupt {
                    file: reader.file.to_string(),
                    detail: "string block exceeds 4 GiB".to_string(),
                })?;
                offsets.push(offset);
            }
            let bytes = reader.take(total as usize)?.to_vec();
            PropValues::Str { offsets, bytes }
        }
    };
    Ok(PropBlock { validity, values })
}

/// Decode one column block by kind. `zstd_level` selection at encode does not
/// affect decode (zstd frames are self-describing).
pub fn decode_block(
    kind: ColumnKind,
    compressed: &[u8],
    uncompressed_len: usize,
    rows: usize,
    file: &str,
) -> Result<DecodedBlock, EventStoreError> {
    let payload = decompress(compressed, uncompressed_len, file)?;
    let mut reader = ByteReader::new(&payload, file);
    let decoded = match kind {
        ColumnKind::Time | ColumnKind::Tiebreak => {
            DecodedBlock::I64(decode_i64_block(&mut reader, rows)?)
        }
        ColumnKind::Event => DecodedBlock::Codes(decode_code_block(&mut reader, rows)?),
        ColumnKind::Prop(encoding) => {
            DecodedBlock::Prop(decode_prop_block(encoding, &mut reader, rows)?)
        }
    };
    if !reader.at_end() {
        return Err(EventStoreError::Corrupt {
            file: file.to_string(),
            detail: "column block has trailing bytes".to_string(),
        });
    }
    Ok(decoded)
}

// ---------------------------------------------------------------------------
// Block statistics
// ---------------------------------------------------------------------------

/// The per-block skip statistic of one code-column block: the exact code set
/// when it is small, else the code range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockCodes {
    List(Vec<u64>),
    Range(u64, u64),
}

impl BlockCodes {
    /// Whether the block may contain `code`.
    pub fn may_contain(&self, code: u64) -> bool {
        match self {
            BlockCodes::List(codes) => codes.binary_search(&code).is_ok(),
            BlockCodes::Range(min, max) => *min <= code && code <= *max,
        }
    }
}

/// The block statistics of one column.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnStats {
    /// No stats recorded (tiebreak, bool, raw-string columns).
    None,
    /// Per block (min_ts, max_ts).
    TsMinMax(Vec<(i64, i64)>),
    /// Per block code set / range.
    Codes(Vec<BlockCodes>),
    /// Per block (min bits, max bits, null count).
    Numeric(Vec<(u64, u64, u64)>),
}

/// The statistics family a column kind records.
fn stats_family(kind: ColumnKind) -> u8 {
    match kind {
        ColumnKind::Time => 1,
        ColumnKind::Tiebreak => 0,
        ColumnKind::Event => 2,
        ColumnKind::Prop(encoding) => match encoding {
            PropertyEncoding::Dict => 2,
            PropertyEncoding::I64 | PropertyEncoding::F64 => 3,
            PropertyEncoding::Bool | PropertyEncoding::RawString => 0,
        },
    }
}

/// Serialize the per-column block statistics into the stats region payload.
fn encode_stats(columns: &[ColumnStats], out: &mut Vec<u8>) {
    for stats in columns {
        match stats {
            ColumnStats::None => out.push(0),
            ColumnStats::TsMinMax(blocks) => {
                out.push(1);
                for (min, max) in blocks {
                    out.extend_from_slice(&min.to_le_bytes());
                    out.extend_from_slice(&max.to_le_bytes());
                }
            }
            ColumnStats::Codes(blocks) => {
                out.push(2);
                for block in blocks {
                    match block {
                        BlockCodes::List(codes) => {
                            out.push(codes.len() as u8);
                            for code in codes {
                                put_varint(out, *code);
                            }
                        }
                        BlockCodes::Range(min, max) => {
                            out.push(0xff);
                            put_varint(out, *min);
                            put_varint(out, *max);
                        }
                    }
                }
            }
            ColumnStats::Numeric(blocks) => {
                out.push(3);
                for (min_bits, max_bits, nulls) in blocks {
                    out.extend_from_slice(&min_bits.to_le_bytes());
                    out.extend_from_slice(&max_bits.to_le_bytes());
                    put_varint(out, *nulls);
                }
            }
        }
    }
}

/// Parse the stats region payload back into per-column statistics; the block
/// count of every column is `blocks` (derived from the header event count).
pub fn decode_stats(
    payload: &[u8],
    column_count: usize,
    blocks: usize,
    file: &str,
) -> Result<Vec<ColumnStats>, EventStoreError> {
    let mut reader = ByteReader::new(payload, file);
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let family = reader.u8()?;
        let stats = match family {
            0 => ColumnStats::None,
            1 => {
                let mut entries = Vec::with_capacity(blocks);
                for _ in 0..blocks {
                    entries.push((reader.i64()?, reader.i64()?));
                }
                ColumnStats::TsMinMax(entries)
            }
            2 => {
                let mut entries = Vec::with_capacity(blocks);
                for _ in 0..blocks {
                    let count = reader.u8()?;
                    if count == 0xff {
                        entries.push(BlockCodes::Range(reader.varint()?, reader.varint()?));
                    } else {
                        let mut codes = Vec::with_capacity(usize::from(count));
                        for _ in 0..count {
                            codes.push(reader.varint()?);
                        }
                        entries.push(BlockCodes::List(codes));
                    }
                }
                ColumnStats::Codes(entries)
            }
            3 => {
                let mut entries = Vec::with_capacity(blocks);
                for _ in 0..blocks {
                    entries.push((reader.u64()?, reader.u64()?, reader.varint()?));
                }
                ColumnStats::Numeric(entries)
            }
            other => {
                return Err(EventStoreError::Corrupt {
                    file: file.to_string(),
                    detail: format!("stats region has invalid family {other}"),
                })
            }
        };
        columns.push(stats);
    }
    if !reader.at_end() {
        return Err(EventStoreError::Corrupt {
            file: file.to_string(),
            detail: "stats region has trailing bytes".to_string(),
        });
    }
    Ok(columns)
}

// ---------------------------------------------------------------------------
// Actor directory
// ---------------------------------------------------------------------------

/// The decoded actor directory of one shard-generation file: the sorted local
/// ids present, the CSR event-range prefix (`offsets[i]..offsets[i+1]` is
/// actor i's contiguous event range in every column), and each actor's first
/// event ts in THIS file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorDirectory {
    pub locals: Vec<u64>,
    pub offsets: Vec<u64>,
    pub first_ts: Vec<i64>,
}

impl ActorDirectory {
    /// The event range of the actor at directory position `index`.
    pub fn range(&self, index: usize) -> (u64, u64) {
        (self.offsets[index], self.offsets[index + 1])
    }

    /// The directory position of `local`, if that actor has events here.
    pub fn position_of(&self, local: u64) -> Option<usize> {
        self.locals.binary_search(&local).ok()
    }

    /// Approximate heap bytes, for cache accounting.
    pub fn heap_bytes(&self) -> usize {
        self.locals.len() * 8 + self.offsets.len() * 8 + self.first_ts.len() * 8
    }
}

/// Serialize the actor directory into its region payload.
fn encode_actor_directory(dir: &ActorDirectory, out: &mut Vec<u8>) {
    let mut previous_local = 0u64;
    for (index, local) in dir.locals.iter().enumerate() {
        put_varint(out, local - previous_local);
        previous_local = *local;
        put_varint(out, dir.offsets[index + 1] - dir.offsets[index]);
    }
    let mut previous_ts = 0i64;
    for ts in &dir.first_ts {
        put_varint(out, zigzag(ts.wrapping_sub(previous_ts)));
        previous_ts = *ts;
    }
}

/// Parse an actor-directory region payload of `actor_count` entries.
pub fn decode_actor_directory(
    payload: &[u8],
    actor_count: usize,
    file: &str,
) -> Result<ActorDirectory, EventStoreError> {
    let mut reader = ByteReader::new(payload, file);
    let mut locals = Vec::with_capacity(actor_count);
    let mut offsets = Vec::with_capacity(actor_count + 1);
    offsets.push(0u64);
    let mut local = 0u64;
    let mut offset = 0u64;
    for index in 0..actor_count {
        let delta = reader.varint()?;
        local = if index == 0 { delta } else { local + delta };
        locals.push(local);
        offset += reader.varint()?;
        offsets.push(offset);
    }
    let mut first_ts = Vec::with_capacity(actor_count);
    let mut ts = 0i64;
    for _ in 0..actor_count {
        ts = ts.wrapping_add(reader.signed_varint()?);
        first_ts.push(ts);
    }
    if !reader.at_end() {
        return Err(EventStoreError::Corrupt {
            file: file.to_string(),
            detail: "actor directory has trailing bytes".to_string(),
        });
    }
    Ok(ActorDirectory {
        locals,
        offsets,
        first_ts,
    })
}

// ---------------------------------------------------------------------------
// Pre-aggregate region
// ---------------------------------------------------------------------------

/// The per-file event-by-day pre-aggregate: sorted `(event code, UTC day,
/// count)` entries plus one per-code total. Counts are additive across files,
/// so day/week/month COUNT segmentation merges these without touching event
/// columns.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Preagg {
    pub entries: Vec<(u64, i64, u64)>,
    pub totals: Vec<(u64, u64)>,
}

impl Preagg {
    /// Approximate heap bytes, for cache accounting.
    pub fn heap_bytes(&self) -> usize {
        self.entries.len() * 24 + self.totals.len() * 16
    }
}

/// Serialize the pre-aggregate region payload.
fn encode_preagg(preagg: &Preagg, out: &mut Vec<u8>) {
    out.extend_from_slice(&(preagg.entries.len() as u32).to_le_bytes());
    let mut previous_code = 0u64;
    let mut previous_day = 0i64;
    for (code, day, count) in &preagg.entries {
        put_varint(out, code - previous_code);
        if *code != previous_code {
            previous_day = 0;
        }
        put_varint(out, zigzag(day.wrapping_sub(previous_day)));
        put_varint(out, *count);
        previous_code = *code;
        previous_day = *day;
    }
    out.extend_from_slice(&(preagg.totals.len() as u32).to_le_bytes());
    let mut previous_code = 0u64;
    for (code, total) in &preagg.totals {
        put_varint(out, code - previous_code);
        put_varint(out, *total);
        previous_code = *code;
    }
}

/// Parse a pre-aggregate region payload.
pub fn decode_preagg(payload: &[u8], file: &str) -> Result<Preagg, EventStoreError> {
    let mut reader = ByteReader::new(payload, file);
    let entry_count = reader.u32()? as usize;
    let mut entries = Vec::with_capacity(entry_count);
    let mut code = 0u64;
    let mut day = 0i64;
    for _ in 0..entry_count {
        let code_delta = reader.varint()?;
        if code_delta != 0 {
            day = 0;
        }
        code += code_delta;
        day = day.wrapping_add(reader.signed_varint()?);
        entries.push((code, day, reader.varint()?));
    }
    let total_count = reader.u32()? as usize;
    let mut totals = Vec::with_capacity(total_count);
    let mut code = 0u64;
    for _ in 0..total_count {
        code += reader.varint()?;
        totals.push((code, reader.varint()?));
    }
    if !reader.at_end() {
        return Err(EventStoreError::Corrupt {
            file: file.to_string(),
            detail: "preagg region has trailing bytes".to_string(),
        });
    }
    Ok(Preagg { entries, totals })
}

// ---------------------------------------------------------------------------
// Segment header / footer
// ---------------------------------------------------------------------------

/// The `.seg` fixed header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegHeader {
    pub shard: u32,
    pub generation: u64,
    pub event_count: u64,
    pub actor_count: u64,
    pub column_count: u32,
}

/// The `.seg` header byte length.
pub const SEG_HEADER_LEN: usize = 8 + 4 + 4 + 8 + 8 + 8 + 4;

/// Serialize the segment header.
fn encode_seg_header(header: &SegHeader, out: &mut Vec<u8>) {
    out.extend_from_slice(SEG_MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&header.shard.to_le_bytes());
    out.extend_from_slice(&header.generation.to_le_bytes());
    out.extend_from_slice(&header.event_count.to_le_bytes());
    out.extend_from_slice(&header.actor_count.to_le_bytes());
    out.extend_from_slice(&header.column_count.to_le_bytes());
}

/// Parse and validate a segment header.
pub fn decode_seg_header(bytes: &[u8], file: &str) -> Result<SegHeader, EventStoreError> {
    let mut reader = ByteReader::new(bytes, file);
    if reader.take(8)? != SEG_MAGIC {
        return Err(EventStoreError::Corrupt {
            file: file.to_string(),
            detail: "bad segment magic".to_string(),
        });
    }
    let version = reader.u32()?;
    if version != FORMAT_VERSION {
        return Err(EventStoreError::Corrupt {
            file: file.to_string(),
            detail: format!("unknown segment format version {version}"),
        });
    }
    Ok(SegHeader {
        shard: reader.u32()?,
        generation: reader.u64()?,
        event_count: reader.u64()?,
        actor_count: reader.u64()?,
        column_count: reader.u32()?,
    })
}

/// One footer region-directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionEntry {
    pub kind: u8,
    /// The column index for `REGION_COLUMN` entries, else `u32::MAX`.
    pub column: u32,
    pub offset: u64,
    pub length: u64,
    pub checksum: u64,
}

/// Serialize the footer: region entries, footer length, trailing magic.
fn encode_footer(entries: &[RegionEntry], out: &mut Vec<u8>) {
    let start = out.len();
    for entry in entries {
        out.push(entry.kind);
        out.extend_from_slice(&entry.column.to_le_bytes());
        out.extend_from_slice(&entry.offset.to_le_bytes());
        out.extend_from_slice(&entry.length.to_le_bytes());
        out.extend_from_slice(&entry.checksum.to_le_bytes());
    }
    let footer_len = (out.len() - start) as u32;
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(END_MAGIC);
}

/// One directory entry's serialized length.
const REGION_ENTRY_LEN: usize = 1 + 4 + 8 + 8 + 8;

/// Parse a footer's region entries from its entry area bytes.
pub fn decode_footer_entries(
    bytes: &[u8],
    file: &str,
) -> Result<Vec<RegionEntry>, EventStoreError> {
    if !bytes.len().is_multiple_of(REGION_ENTRY_LEN) {
        return Err(EventStoreError::Corrupt {
            file: file.to_string(),
            detail: "footer entry area has a partial entry".to_string(),
        });
    }
    let mut reader = ByteReader::new(bytes, file);
    let mut entries = Vec::with_capacity(bytes.len() / REGION_ENTRY_LEN);
    while !reader.at_end() {
        entries.push(RegionEntry {
            kind: reader.u8()?,
            column: reader.u32()?,
            offset: reader.u64()?,
            length: reader.u64()?,
            checksum: reader.u64()?,
        });
    }
    Ok(entries)
}

/// Parse the trailing `[footer_len u32][magic 8]` of a `.seg`/`.act` file.
pub fn decode_footer_tail(tail: &[u8], file: &str) -> Result<u32, EventStoreError> {
    if tail.len() != 12 || &tail[4..] != END_MAGIC {
        return Err(EventStoreError::Corrupt {
            file: file.to_string(),
            detail: "bad trailing magic".to_string(),
        });
    }
    Ok(u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]))
}

// ---------------------------------------------------------------------------
// Column region assembly (encoder side)
// ---------------------------------------------------------------------------

/// One column being encoded: its finished compressed blocks plus the current
/// open block's buffered values.
struct ColumnEncoder {
    kind: ColumnKind,
    /// Per finished block: (compressed bytes, uncompressed length).
    blocks: Vec<(Vec<u8>, u32)>,
    /// The open block's buffered values by kind.
    i64_buf: Vec<i64>,
    code_buf: Vec<u64>,
    f64_buf: Vec<f64>,
    bool_buf: Vec<u8>,
    str_lens: Vec<u32>,
    str_bytes: Vec<u8>,
    validity: Vec<u8>,
    /// Block statistics accumulated as blocks close.
    stats: ColumnStats,
}

impl ColumnEncoder {
    /// A fresh encoder for one column.
    fn new(kind: ColumnKind) -> Self {
        let stats = match stats_family(kind) {
            0 => ColumnStats::None,
            1 => ColumnStats::TsMinMax(Vec::new()),
            2 => ColumnStats::Codes(Vec::new()),
            3 => ColumnStats::Numeric(Vec::new()),
            family => unreachable!("stats_family returned unknown family {family}"),
        };
        Self {
            kind,
            blocks: Vec::new(),
            i64_buf: Vec::new(),
            code_buf: Vec::new(),
            f64_buf: Vec::new(),
            bool_buf: Vec::new(),
            str_lens: Vec::new(),
            str_bytes: Vec::new(),
            validity: Vec::new(),
            stats,
        }
    }

    /// Rows buffered in the open block.
    fn buffered(&self) -> usize {
        match self.kind {
            ColumnKind::Time | ColumnKind::Tiebreak => self.i64_buf.len(),
            ColumnKind::Event => self.code_buf.len(),
            ColumnKind::Prop(_) => self.validity.len(),
        }
    }

    /// Close the open block: encode, compress, record stats, reset buffers.
    fn close_block(&mut self) {
        let rows = self.buffered();
        if rows == 0 {
            return;
        }
        let mut payload = Vec::with_capacity(rows * 4);
        let mut level = 1;
        match self.kind {
            ColumnKind::Time | ColumnKind::Tiebreak => {
                encode_i64_block(&self.i64_buf, &mut payload);
                if let ColumnStats::TsMinMax(entries) = &mut self.stats {
                    let min = self.i64_buf.iter().copied().min().expect("non-empty block");
                    let max = self.i64_buf.iter().copied().max().expect("non-empty block");
                    entries.push((min, max));
                }
                self.i64_buf.clear();
            }
            ColumnKind::Event => {
                encode_code_block(&self.code_buf, &mut payload);
                push_code_stats(&mut self.stats, &self.code_buf, None);
                self.code_buf.clear();
            }
            ColumnKind::Prop(encoding) => {
                self.close_prop_block(encoding, &mut payload, &mut level);
            }
        }
        let uncompressed =
            u32::try_from(payload.len()).expect("a 65536-row block payload is far below 4 GiB");
        self.blocks.push((compress(&payload, level), uncompressed));
    }

    /// Close the open block of a property column.
    fn close_prop_block(
        &mut self,
        encoding: PropertyEncoding,
        payload: &mut Vec<u8>,
        level: &mut i32,
    ) {
        let validity = std::mem::take(&mut self.validity);
        match encoding {
            PropertyEncoding::Dict => {
                encode_prop_block(
                    encoding,
                    Some(&validity),
                    PropSlice::Codes(&self.code_buf),
                    payload,
                );
                push_code_stats(&mut self.stats, &self.code_buf, Some(&validity));
                self.code_buf.clear();
            }
            PropertyEncoding::I64 => {
                encode_prop_block(
                    encoding,
                    Some(&validity),
                    PropSlice::I64(&self.i64_buf),
                    payload,
                );
                push_numeric_stats(
                    &mut self.stats,
                    self.i64_buf.iter().map(|v| *v as u64),
                    &validity,
                );
                self.i64_buf.clear();
            }
            PropertyEncoding::F64 => {
                encode_prop_block(
                    encoding,
                    Some(&validity),
                    PropSlice::F64(&self.f64_buf),
                    payload,
                );
                push_numeric_stats(
                    &mut self.stats,
                    self.f64_buf.iter().map(|v| v.to_bits()),
                    &validity,
                );
                self.f64_buf.clear();
            }
            PropertyEncoding::Bool => {
                encode_prop_block(
                    encoding,
                    Some(&validity),
                    PropSlice::Bool(&self.bool_buf),
                    payload,
                );
                self.bool_buf.clear();
            }
            PropertyEncoding::RawString => {
                *level = 3;
                encode_prop_block(
                    encoding,
                    Some(&validity),
                    PropSlice::Str {
                        lens: &self.str_lens,
                        bytes: &self.str_bytes,
                    },
                    payload,
                );
                self.str_lens.clear();
                self.str_bytes.clear();
            }
        }
    }

    /// Assemble the column region: block index then concatenated block bytes.
    fn into_region(mut self) -> (Vec<u8>, ColumnStats) {
        self.close_block();
        let mut region = Vec::new();
        region.extend_from_slice(&(self.blocks.len() as u32).to_le_bytes());
        let mut offset = 0u64;
        for (compressed, uncompressed) in &self.blocks {
            region.extend_from_slice(&offset.to_le_bytes());
            region.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
            region.extend_from_slice(&uncompressed.to_le_bytes());
            offset += compressed.len() as u64;
        }
        for (compressed, _) in &self.blocks {
            region.extend_from_slice(compressed);
        }
        (region, self.stats)
    }
}

/// Record a closed code block's skip statistic: the sorted distinct code list
/// when it is at most 16 codes, else the code range. Null rows do not
/// contribute codes.
fn push_code_stats(stats: &mut ColumnStats, codes: &[u64], validity: Option<&[u8]>) {
    let ColumnStats::Codes(entries) = stats else {
        unreachable!("code stats pushed onto a non-code column");
    };
    let mut distinct: Vec<u64> = Vec::new();
    for (index, code) in codes.iter().enumerate() {
        if let Some(bits) = validity {
            if bits[index] == 0 {
                continue;
            }
        }
        if let Err(position) = distinct.binary_search(code) {
            if distinct.len() >= 17 {
                continue;
            }
            distinct.insert(position, *code);
        }
    }
    if distinct.len() <= 16 {
        entries.push(BlockCodes::List(distinct));
    } else {
        let mut min = u64::MAX;
        let mut max = 0u64;
        for (index, code) in codes.iter().enumerate() {
            if let Some(bits) = validity {
                if bits[index] == 0 {
                    continue;
                }
            }
            min = min.min(*code);
            max = max.max(*code);
        }
        entries.push(BlockCodes::Range(min, max));
    }
}

/// Record a closed numeric block's zone map (min/max bit patterns + nulls).
fn push_numeric_stats(stats: &mut ColumnStats, values: impl Iterator<Item = u64>, validity: &[u8]) {
    let ColumnStats::Numeric(entries) = stats else {
        unreachable!("numeric stats pushed onto a non-numeric column");
    };
    let mut min = u64::MAX;
    let mut max = 0u64;
    let mut nulls = 0u64;
    for (index, bits) in values.enumerate() {
        if validity[index] == 0 {
            nulls += 1;
            continue;
        }
        min = min.min(bits);
        max = max.max(bits);
    }
    entries.push((min, max, nulls));
}

// ---------------------------------------------------------------------------
// The streaming segment encoder
// ---------------------------------------------------------------------------

/// The streaming `.seg` encoder: rows are pushed in `(local, ts, tiebreak)`
/// order and blocks close as they fill, so peak memory is the compressed
/// output plus one open block per column. Both the in-memory and the external
/// merge finalize paths feed this same encoder.
pub struct SegEncoder {
    shard: u32,
    generation: u64,
    columns: Vec<ColumnEncoder>,
    dir: ActorDirectory,
    preagg_map: FastMap<u64, u64>,
    totals_map: FastMap<u64, u64>,
    event_count: u64,
    /// The previous row's full sort key, for the ambiguous-order guard.
    previous: Option<(u64, i64, i64)>,
    /// The previous row's canonical key bytes (reused storage; rendered only
    /// when the guard fires).
    previous_key: Vec<u8>,
}

impl SegEncoder {
    /// A fresh encoder for one shard-generation file over the given property
    /// encodings.
    pub fn new(shard: u32, generation: u64, properties: &[PropertyEncoding]) -> Self {
        let mut columns = vec![
            ColumnEncoder::new(ColumnKind::Time),
            ColumnEncoder::new(ColumnKind::Tiebreak),
            ColumnEncoder::new(ColumnKind::Event),
        ];
        for encoding in properties {
            columns.push(ColumnEncoder::new(ColumnKind::Prop(*encoding)));
        }
        Self {
            shard,
            generation,
            columns,
            dir: ActorDirectory {
                locals: Vec::new(),
                offsets: vec![0],
                first_ts: Vec::new(),
            },
            preagg_map: FastMap::default(),
            totals_map: FastMap::default(),
            event_count: 0,
            previous: None,
            previous_key: Vec::new(),
        }
    }

    /// Push one event row. Rows MUST arrive in `(local, ts, tiebreak)` order;
    /// an adjacent duplicate full key raises `AmbiguousOrder` (ordering
    /// between the two events would be undefined). `key_bytes` is the actor's
    /// canonical key, used only for that error's message.
    pub fn push_row(
        &mut self,
        local: u64,
        ts: i64,
        tiebreak: i64,
        event_code: u64,
        props: &[PropCell<'_>],
        key_bytes: &[u8],
    ) -> Result<(), EventBuildError> {
        if let Some((previous_local, previous_ts, previous_tb)) = self.previous {
            debug_assert!(
                (previous_local, previous_ts, previous_tb) <= (local, ts, tiebreak),
                "segment rows pushed out of order"
            );
            if (previous_local, previous_ts, previous_tb) == (local, ts, tiebreak) {
                return Err(EventBuildError::AmbiguousOrder {
                    actor_key: describe_key(&self.previous_key),
                    ts,
                });
            }
        }
        if self.previous.is_none_or(|(l, _, _)| l != local) {
            self.dir.locals.push(local);
            self.dir.offsets.push(self.event_count);
            self.dir.first_ts.push(ts);
        }
        self.previous = Some((local, ts, tiebreak));
        self.previous_key.clear();
        self.previous_key.extend_from_slice(key_bytes);
        self.push_columns(ts, tiebreak, event_code, props);
        let day = fq_common::events::day_of_micros(ts);
        *self
            .preagg_map
            .entry(preagg_key(event_code, day))
            .or_insert(0) += 1;
        *self.totals_map.entry(event_code).or_insert(0) += 1;
        self.event_count += 1;
        *self.dir.offsets.last_mut().expect("offsets never empty") = self.event_count;
        Ok(())
    }

    /// Append one row to every column buffer, closing full blocks.
    fn push_columns(&mut self, ts: i64, tiebreak: i64, event_code: u64, props: &[PropCell<'_>]) {
        self.columns[0].i64_buf.push(ts);
        self.columns[1].i64_buf.push(tiebreak);
        self.columns[2].code_buf.push(event_code);
        for (index, cell) in props.iter().enumerate() {
            push_prop_cell(&mut self.columns[3 + index], *cell);
        }
        for column in &mut self.columns {
            if column.buffered() >= BLOCK_ROWS {
                column.close_block();
            }
        }
    }

    /// Finish the file: close open blocks, assemble every region, and return
    /// the complete `.seg` bytes.
    pub fn finish(mut self) -> Vec<u8> {
        // The offsets vector carries a trailing running total only while rows
        // stream in; freeze it to actor_count + 1 entries.
        let header = SegHeader {
            shard: self.shard,
            generation: self.generation,
            event_count: self.event_count,
            actor_count: self.dir.locals.len() as u64,
            column_count: self.columns.len() as u32,
        };
        let mut out = Vec::new();
        encode_seg_header(&header, &mut out);
        let mut entries = Vec::new();
        let mut stats = Vec::with_capacity(self.columns.len());
        for (index, column) in std::mem::take(&mut self.columns).into_iter().enumerate() {
            let (region, column_stats) = column.into_region();
            stats.push(column_stats);
            push_region(&mut out, &mut entries, REGION_COLUMN, index as u32, &region);
        }
        let mut dir_payload = Vec::new();
        encode_actor_directory(&self.dir, &mut dir_payload);
        let dir_region = compress_region(&dir_payload, 1);
        push_region(
            &mut out,
            &mut entries,
            REGION_ACTOR_DIR,
            u32::MAX,
            &dir_region,
        );
        let mut stats_payload = Vec::new();
        encode_stats(&stats, &mut stats_payload);
        let stats_region = compress_region(&stats_payload, 1);
        push_region(
            &mut out,
            &mut entries,
            REGION_BLOCK_STATS,
            u32::MAX,
            &stats_region,
        );
        let mut preagg_payload = Vec::new();
        encode_preagg(&self.preagg(), &mut preagg_payload);
        let preagg_region = compress_region(&preagg_payload, 1);
        push_region(
            &mut out,
            &mut entries,
            REGION_PREAGG,
            u32::MAX,
            &preagg_region,
        );
        encode_footer(&entries, &mut out);
        out
    }

    /// The accumulated pre-aggregate in its sorted persisted shape.
    fn preagg(&self) -> Preagg {
        let mut entries: Vec<(u64, i64, u64)> = self
            .preagg_map
            .iter()
            .map(|(key, count)| {
                let (code, day) = split_preagg_key(*key);
                (code, day, *count)
            })
            .collect();
        entries.sort_unstable();
        let mut totals: Vec<(u64, u64)> = self
            .totals_map
            .iter()
            .map(|(code, total)| (*code, *total))
            .collect();
        totals.sort_unstable();
        Preagg { entries, totals }
    }

    /// Events pushed so far.
    pub fn events(&self) -> u64 {
        self.event_count
    }

    /// Actors seen so far.
    pub fn actors(&self) -> u64 {
        self.dir.locals.len() as u64
    }
}

/// Pack a (code, day) pre-agg key. Days since epoch fit comfortably in 32
/// bits for any representable microsecond timestamp.
fn preagg_key(code: u64, day: i64) -> u64 {
    (code << 32) | u64::from((day as i32) as u32)
}

/// Unpack a pre-agg key.
fn split_preagg_key(key: u64) -> (u64, i64) {
    (
        key >> 32,
        i64::from(((key & 0xffff_ffff) as u32).cast_signed()),
    )
}

/// Append one property cell to its column's open block buffers.
fn push_prop_cell(column: &mut ColumnEncoder, cell: PropCell<'_>) {
    let ColumnKind::Prop(encoding) = column.kind else {
        unreachable!("property cell pushed onto a core column");
    };
    let valid = !matches!(cell, PropCell::Null);
    column.validity.push(u8::from(valid));
    match (encoding, cell) {
        (PropertyEncoding::Dict, PropCell::Code(code)) => column.code_buf.push(code),
        (PropertyEncoding::Dict, PropCell::Null) => column.code_buf.push(0),
        (PropertyEncoding::I64, PropCell::I64(value)) => column.i64_buf.push(value),
        (PropertyEncoding::I64, PropCell::Null) => column.i64_buf.push(0),
        (PropertyEncoding::F64, PropCell::F64(value)) => column.f64_buf.push(value),
        (PropertyEncoding::F64, PropCell::Null) => column.f64_buf.push(0.0),
        (PropertyEncoding::Bool, PropCell::Bool(value)) => column.bool_buf.push(u8::from(value)),
        (PropertyEncoding::Bool, PropCell::Null) => column.bool_buf.push(0),
        (PropertyEncoding::RawString, PropCell::Str(bytes)) => {
            column
                .str_lens
                .push(u32::try_from(bytes.len()).expect("string cell below 4 GiB"));
            column.str_bytes.extend_from_slice(bytes);
        }
        (PropertyEncoding::RawString, PropCell::Null) => column.str_lens.push(0),
        (encoding, cell) => {
            unreachable!("property cell {cell:?} does not match declared encoding {encoding:?}")
        }
    }
}

/// Append a region's bytes to the file image and record its directory entry.
fn push_region(
    out: &mut Vec<u8>,
    entries: &mut Vec<RegionEntry>,
    kind: u8,
    column: u32,
    bytes: &[u8],
) {
    entries.push(RegionEntry {
        kind,
        column,
        offset: out.len() as u64,
        length: bytes.len() as u64,
        checksum: fnv1a64(bytes),
    });
    out.extend_from_slice(bytes);
}

// ---------------------------------------------------------------------------
// Column region reading
// ---------------------------------------------------------------------------

/// A verified, loaded column region: the block index plus the concatenated
/// compressed block bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRegion {
    /// Per block: (offset into `bytes`, compressed length, uncompressed length).
    pub blocks: Vec<(u64, u32, u32)>,
    pub bytes: Vec<u8>,
}

impl ColumnRegion {
    /// Approximate heap bytes, for cache accounting.
    pub fn heap_bytes(&self) -> usize {
        self.blocks.len() * 16 + self.bytes.len()
    }

    /// Decode block `index`, whose row count is `rows`.
    pub fn decode(
        &self,
        kind: ColumnKind,
        index: usize,
        rows: usize,
        file: &str,
    ) -> Result<DecodedBlock, EventStoreError> {
        let (offset, compressed_len, uncompressed_len) = self.blocks[index];
        let start = offset as usize;
        let end = start + compressed_len as usize;
        if end > self.bytes.len() {
            return Err(EventStoreError::Corrupt {
                file: file.to_string(),
                detail: format!("block {index} overruns its column region"),
            });
        }
        decode_block(
            kind,
            &self.bytes[start..end],
            uncompressed_len as usize,
            rows,
            file,
        )
    }
}

/// Parse a raw column-region byte area (already checksum-verified).
pub fn decode_column_region(bytes: &[u8], file: &str) -> Result<ColumnRegion, EventStoreError> {
    let mut reader = ByteReader::new(bytes, file);
    let block_count = reader.u32()? as usize;
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        blocks.push((reader.u64()?, reader.u32()?, reader.u32()?));
    }
    Ok(ColumnRegion {
        blocks,
        bytes: reader.rest().to_vec(),
    })
}

/// Unwrap and verify a compressed non-column region (actor dir, stats,
/// preagg, sidecar regions) into its payload.
pub fn open_region(bytes: &[u8], file: &str) -> Result<Vec<u8>, EventStoreError> {
    decompress_region(bytes, file)
}

/// The row count of block `index` in a file of `event_count` rows.
pub fn block_rows(event_count: u64, index: usize) -> usize {
    let start = index as u64 * BLOCK_ROWS as u64;
    usize::try_from((event_count - start).min(BLOCK_ROWS as u64)).expect("block fits usize")
}

/// The block count of a file of `event_count` rows.
pub fn block_count(event_count: u64) -> usize {
    usize::try_from(event_count.div_ceil(BLOCK_ROWS as u64)).expect("block count fits usize")
}

// ---------------------------------------------------------------------------
// Actor sidecar (.act)
// ---------------------------------------------------------------------------

/// The decoded actor sidecar of one shard-generation: identity data for the
/// actors FIRST SEEN in it. `index` maps `(hash64, key bytes)` to local id;
/// `keys` is the reverse map in local-id order starting at `first_local_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorSidecar {
    pub first_local_id: u64,
    /// Sorted by (hash64, key bytes).
    pub index: Vec<(u64, u64)>,
    /// Per new actor (local order): canonical key bytes.
    pub key_offsets: Vec<u64>,
    pub key_bytes: Vec<u8>,
}

impl ActorSidecar {
    /// The canonical key bytes of `local` (which must be in this sidecar).
    pub fn key_of(&self, local: u64) -> &[u8] {
        let index = (local - self.first_local_id) as usize;
        let start = self.key_offsets[index] as usize;
        let end = self.key_offsets[index + 1] as usize;
        &self.key_bytes[start..end]
    }

    /// The local id of `(hash, key)` when present: binary search by hash,
    /// then raw key-byte comparison on hash equality (collision safety).
    pub fn lookup(&self, hash: u64, key: &[u8]) -> Option<u64> {
        let mut position = self.index.partition_point(|(h, _)| *h < hash);
        while position < self.index.len() && self.index[position].0 == hash {
            let local = self.index[position].1;
            if self.key_of(local) == key {
                return Some(local);
            }
            position += 1;
        }
        None
    }

    /// Approximate heap bytes, for cache accounting.
    pub fn heap_bytes(&self) -> usize {
        self.index.len() * 16 + self.key_offsets.len() * 8 + self.key_bytes.len()
    }
}

/// Serialize an actor sidecar file. `new_actors` holds `(hash, local, key)`
/// and must be sorted by local id; the hash index is written sorted by
/// `(hash, key bytes)`.
pub fn encode_act(
    shard: u32,
    generation: u64,
    first_local_id: u64,
    new_actors: &[(u64, u64, &[u8])],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(ACT_MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&shard.to_le_bytes());
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(&(new_actors.len() as u64).to_le_bytes());
    let mut sorted: Vec<&(u64, u64, &[u8])> = new_actors.iter().collect();
    sorted.sort_unstable_by(|a, b| (a.0, a.2).cmp(&(b.0, b.2)));
    let mut index_payload = Vec::with_capacity(new_actors.len() * 16);
    for (hash, local, _) in &sorted {
        index_payload.extend_from_slice(&hash.to_le_bytes());
        index_payload.extend_from_slice(&local.to_le_bytes());
    }
    let mut heap_payload = Vec::new();
    for (_, _, key) in new_actors {
        // The tag byte leads each key; the length covers tag + payload.
        put_varint(&mut heap_payload, key.len() as u64);
        heap_payload.extend_from_slice(key);
    }
    let mut entries = Vec::new();
    let index_region = compress_region(&index_payload, 1);
    push_region(
        &mut out,
        &mut entries,
        REGION_HASH_INDEX,
        u32::MAX,
        &index_region,
    );
    let heap_region = compress_region(&heap_payload, 1);
    push_region(
        &mut out,
        &mut entries,
        REGION_KEY_HEAP,
        u32::MAX,
        &heap_region,
    );
    // The sidecar footer leads with first_local_id, then the shared region
    // directory shape.
    let footer_start = out.len();
    out.extend_from_slice(&first_local_id.to_le_bytes());
    for entry in &entries {
        out.push(entry.kind);
        out.extend_from_slice(&entry.column.to_le_bytes());
        out.extend_from_slice(&entry.offset.to_le_bytes());
        out.extend_from_slice(&entry.length.to_le_bytes());
        out.extend_from_slice(&entry.checksum.to_le_bytes());
    }
    let footer_len = (out.len() - footer_start) as u32;
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(END_MAGIC);
    out
}

/// Parse and fully verify an actor sidecar file from its complete bytes.
pub fn decode_act(bytes: &[u8], file: &str) -> Result<ActorSidecar, EventStoreError> {
    let corrupt = |detail: &str| EventStoreError::Corrupt {
        file: file.to_string(),
        detail: detail.to_string(),
    };
    let mut header = ByteReader::new(bytes, file);
    if header.take(8)? != ACT_MAGIC {
        return Err(corrupt("bad sidecar magic"));
    }
    let version = header.u32()?;
    if version != FORMAT_VERSION {
        return Err(corrupt(&format!(
            "unknown sidecar format version {version}"
        )));
    }
    let _shard = header.u32()?;
    let _generation = header.u64()?;
    let new_actor_count = usize::try_from(header.u64()?).map_err(|_| corrupt("actor count"))?;
    if bytes.len() < 12 {
        return Err(corrupt("sidecar shorter than its trailer"));
    }
    let footer_len = decode_footer_tail(&bytes[bytes.len() - 12..], file)? as usize;
    let footer_start = bytes
        .len()
        .checked_sub(12 + footer_len)
        .ok_or_else(|| corrupt("footer length overruns the file"))?;
    let mut footer = ByteReader::new(&bytes[footer_start..bytes.len() - 12], file);
    let first_local_id = footer.u64()?;
    let entries = decode_footer_entries(footer.rest(), file)?;
    let mut index = Vec::with_capacity(new_actor_count);
    let mut key_offsets = Vec::with_capacity(new_actor_count + 1);
    let mut key_bytes = Vec::new();
    for entry in entries {
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end > bytes.len() {
            return Err(corrupt("sidecar region overruns the file"));
        }
        let region_bytes = &bytes[start..end];
        if fnv1a64(region_bytes) != entry.checksum {
            return Err(corrupt("sidecar region checksum mismatch"));
        }
        let payload = open_region(region_bytes, file)?;
        match entry.kind {
            REGION_HASH_INDEX => {
                let mut reader = ByteReader::new(&payload, file);
                for _ in 0..new_actor_count {
                    index.push((reader.u64()?, reader.u64()?));
                }
            }
            REGION_KEY_HEAP => {
                let mut reader = ByteReader::new(&payload, file);
                key_offsets.push(0u64);
                let mut total = 0u64;
                for _ in 0..new_actor_count {
                    let len = reader.varint()?;
                    total += len;
                    key_offsets.push(total);
                    key_bytes.extend_from_slice(reader.take(len as usize)?);
                }
            }
            other => return Err(corrupt(&format!("unknown sidecar region kind {other}"))),
        }
    }
    if index.len() != new_actor_count || key_offsets.len() != new_actor_count + 1 {
        return Err(corrupt("sidecar regions are incomplete"));
    }
    Ok(ActorSidecar {
        first_local_id,
        index,
        key_offsets,
        key_bytes,
    })
}

// ---------------------------------------------------------------------------
// Dictionary generation files (.dict)
// ---------------------------------------------------------------------------

/// Serialize one dictionary generation file: the values APPENDED in this
/// generation, in code order starting at `first_code`.
pub fn encode_dict(column: &str, first_code: u64, values: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(DICT_MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    put_varint(&mut out, column.len() as u64);
    out.extend_from_slice(column.as_bytes());
    out.extend_from_slice(&first_code.to_le_bytes());
    out.extend_from_slice(&(values.len() as u64).to_le_bytes());
    let mut offset = 0u32;
    out.extend_from_slice(&offset.to_le_bytes());
    for value in values {
        offset += u32::try_from(value.len()).expect("dictionary value below 4 GiB");
        out.extend_from_slice(&offset.to_le_bytes());
    }
    for value in values {
        out.extend_from_slice(value.as_bytes());
    }
    let checksum = fnv1a64(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

/// Parse and verify one dictionary generation file: `(column, first_code,
/// values)`.
pub fn decode_dict(
    bytes: &[u8],
    file: &str,
) -> Result<(String, u64, Vec<String>), EventStoreError> {
    let corrupt = |detail: &str| EventStoreError::Corrupt {
        file: file.to_string(),
        detail: detail.to_string(),
    };
    if bytes.len() < 8 {
        return Err(corrupt("dictionary file shorter than its checksum"));
    }
    let body = &bytes[..bytes.len() - 8];
    let stored = u64::from_le_bytes(bytes[bytes.len() - 8..].try_into().expect("8 bytes"));
    if fnv1a64(body) != stored {
        return Err(corrupt("dictionary checksum mismatch"));
    }
    let mut reader = ByteReader::new(body, file);
    if reader.take(8)? != DICT_MAGIC {
        return Err(corrupt("bad dictionary magic"));
    }
    let version = reader.u32()?;
    if version != FORMAT_VERSION {
        return Err(corrupt(&format!(
            "unknown dictionary format version {version}"
        )));
    }
    let name_len = reader.varint()? as usize;
    let column = String::from_utf8(reader.take(name_len)?.to_vec())
        .map_err(|_| corrupt("dictionary column name is not UTF-8"))?;
    let first_code = reader.u64()?;
    let count = usize::try_from(reader.u64()?).map_err(|_| corrupt("dictionary count"))?;
    let mut offsets = Vec::with_capacity(count + 1);
    for _ in 0..=count {
        offsets.push(reader.u32()?);
    }
    let heap = reader.rest();
    let mut values = Vec::with_capacity(count);
    for window in offsets.windows(2) {
        let start = window[0] as usize;
        let end = window[1] as usize;
        if end < start || end > heap.len() {
            return Err(corrupt("dictionary offsets overrun the heap"));
        }
        values.push(
            std::str::from_utf8(&heap[start..end])
                .map_err(|_| corrupt("dictionary value is not UTF-8"))?
                .to_string(),
        );
    }
    Ok((column, first_code, values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip() {
        let mut buf = Vec::new();
        let values = [0u64, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX];
        for value in values {
            put_varint(&mut buf, value);
        }
        let mut reader = ByteReader::new(&buf, "test");
        for value in values {
            assert_eq!(reader.varint().unwrap(), value);
        }
        assert!(reader.at_end());
        for value in [0i64, 1, -1, i64::MAX, i64::MIN, -123_456] {
            assert_eq!(unzigzag(zigzag(value)), value);
        }
    }

    #[test]
    fn seg_round_trips_through_encode_and_decode() {
        let props = [PropertyEncoding::Dict, PropertyEncoding::I64];
        let mut encoder = SegEncoder::new(3, 0, &props);
        let key = b"\x01k";
        // Two actors; the second has a null property and a same-ts pair
        // separated by tiebreak.
        encoder
            .push_row(0, 1_000, 0, 2, &[PropCell::Code(1), PropCell::I64(7)], key)
            .unwrap();
        encoder
            .push_row(0, 2_000, 1, 5, &[PropCell::Code(0), PropCell::Null], key)
            .unwrap();
        encoder
            .push_row(9, 1_500, 2, 2, &[PropCell::Null, PropCell::I64(-3)], key)
            .unwrap();
        encoder
            .push_row(9, 1_500, 3, 2, &[PropCell::Code(1), PropCell::I64(0)], key)
            .unwrap();
        let bytes = encoder.finish();

        let header = decode_seg_header(&bytes[..SEG_HEADER_LEN], "t.seg").unwrap();
        assert_eq!(header.event_count, 4);
        assert_eq!(header.actor_count, 2);
        assert_eq!(header.column_count, 5);
        let footer_len = decode_footer_tail(&bytes[bytes.len() - 12..], "t.seg").unwrap() as usize;
        let entries = decode_footer_entries(
            &bytes[bytes.len() - 12 - footer_len..bytes.len() - 12],
            "t.seg",
        )
        .unwrap();
        assert_eq!(entries.len(), 5 + 3);
        // Every region's checksum verifies.
        for entry in &entries {
            let region = &bytes[entry.offset as usize..(entry.offset + entry.length) as usize];
            assert_eq!(fnv1a64(region), entry.checksum, "kind {}", entry.kind);
        }
        // The ts column decodes back.
        let ts_entry = entries[0];
        let region = decode_column_region(
            &bytes[ts_entry.offset as usize..(ts_entry.offset + ts_entry.length) as usize],
            "t.seg",
        )
        .unwrap();
        let DecodedBlock::I64(ts) = region.decode(ColumnKind::Time, 0, 4, "t.seg").unwrap() else {
            panic!("expected an i64 block");
        };
        assert_eq!(ts, vec![1_000, 2_000, 1_500, 1_500]);
        // The dict property decodes with its validity.
        let prop_entry = entries[3];
        let region = decode_column_region(
            &bytes[prop_entry.offset as usize..(prop_entry.offset + prop_entry.length) as usize],
            "t.seg",
        )
        .unwrap();
        let DecodedBlock::Prop(prop) = region
            .decode(ColumnKind::Prop(PropertyEncoding::Dict), 0, 4, "t.seg")
            .unwrap()
        else {
            panic!("expected a property block");
        };
        assert_eq!(prop.validity, Some(vec![1, 1, 0, 1]));
        let PropValues::Codes(codes) = prop.values else {
            panic!("expected codes");
        };
        assert_eq!(
            (0..4).map(|i| codes.get(i)).collect::<Vec<_>>(),
            vec![1, 0, 0, 1]
        );
        // The actor directory decodes back.
        let dir_entry = entries[5];
        let payload = open_region(
            &bytes[dir_entry.offset as usize..(dir_entry.offset + dir_entry.length) as usize],
            "t.seg",
        )
        .unwrap();
        let dir = decode_actor_directory(&payload, 2, "t.seg").unwrap();
        assert_eq!(dir.locals, vec![0, 9]);
        assert_eq!(dir.offsets, vec![0, 2, 4]);
        assert_eq!(dir.first_ts, vec![1_000, 1_500]);
        // The preagg decodes back.
        let preagg_entry = entries[7];
        let payload = open_region(
            &bytes[preagg_entry.offset as usize
                ..(preagg_entry.offset + preagg_entry.length) as usize],
            "t.seg",
        )
        .unwrap();
        let preagg = decode_preagg(&payload, "t.seg").unwrap();
        assert_eq!(preagg.totals, vec![(2, 3), (5, 1)]);
        assert_eq!(preagg.entries, vec![(2, 0, 3), (5, 0, 1)]);
        // The stats region decodes back.
        let stats_entry = entries[6];
        let payload = open_region(
            &bytes[stats_entry.offset as usize..(stats_entry.offset + stats_entry.length) as usize],
            "t.seg",
        )
        .unwrap();
        let stats = decode_stats(&payload, 5, 1, "t.seg").unwrap();
        assert_eq!(stats[0], ColumnStats::TsMinMax(vec![(1_000, 2_000)]));
        assert_eq!(
            stats[2],
            ColumnStats::Codes(vec![BlockCodes::List(vec![2, 5])])
        );
    }

    #[test]
    fn ambiguous_order_raises() {
        let mut encoder = SegEncoder::new(0, 0, &[]);
        let mut key = Vec::new();
        crate::model::push_int_key(&mut key, 42);
        encoder.push_row(1, 5, 7, 0, &[], &key).unwrap();
        let error = encoder.push_row(1, 5, 7, 1, &[], &key).unwrap_err();
        assert!(
            matches!(error, EventBuildError::AmbiguousOrder { ref actor_key, ts: 5 } if actor_key == "42")
        );
    }

    #[test]
    fn corrupted_region_bytes_raise() {
        let mut encoder = SegEncoder::new(0, 0, &[]);
        encoder.push_row(0, 1, 0, 0, &[], b"\x01k").unwrap();
        let bytes = encoder.finish();
        let footer_len = decode_footer_tail(&bytes[bytes.len() - 12..], "t.seg").unwrap() as usize;
        let entries = decode_footer_entries(
            &bytes[bytes.len() - 12 - footer_len..bytes.len() - 12],
            "t.seg",
        )
        .unwrap();
        let entry = entries[0];
        let mut region =
            bytes[entry.offset as usize..(entry.offset + entry.length) as usize].to_vec();
        region[0] ^= 0xff;
        assert_ne!(fnv1a64(&region), entry.checksum);
    }

    #[test]
    fn act_round_trips_and_disambiguates_equal_hashes() {
        let key_a: &[u8] = b"\x02alpha";
        let key_b: &[u8] = b"\x02beta";
        // Force equal hashes in the index by writing the same hash for two
        // distinct keys: lookup must compare raw key bytes.
        let actors: Vec<(u64, u64, &[u8])> = vec![(77, 0, key_a), (77, 1, key_b)];
        let bytes = encode_act(4, 0, 0, &actors);
        let sidecar = decode_act(&bytes, "t.act").unwrap();
        assert_eq!(sidecar.first_local_id, 0);
        assert_eq!(sidecar.lookup(77, key_a), Some(0));
        assert_eq!(sidecar.lookup(77, key_b), Some(1));
        assert_eq!(sidecar.lookup(77, b"\x02gamma"), None);
        assert_eq!(sidecar.key_of(1), key_b);
    }

    #[test]
    fn dict_round_trips_and_checksums() {
        let values = vec!["page_view".to_string(), "signup".to_string()];
        let bytes = encode_dict("__event__", 0, &values);
        let (column, first, decoded) = decode_dict(&bytes, "t.dict").unwrap();
        assert_eq!(column, "__event__");
        assert_eq!(first, 0);
        assert_eq!(decoded, values);
        let mut torn = bytes.clone();
        torn[10] ^= 1;
        assert!(decode_dict(&torn, "t.dict").is_err());
    }

    #[test]
    fn wide_code_blocks_use_wider_widths() {
        // Codes above 255 must byte-pack at width 2, above 65535 at width 4;
        // nothing caps the code space.
        let mut payload = Vec::new();
        encode_code_block(&[0, 300, 70_000], &mut payload);
        assert_eq!(payload[0], 4);
        let mut reader = ByteReader::new(&payload, "t");
        let codes = decode_code_block(&mut reader, 3).unwrap();
        assert_eq!(codes.get(2), 70_000);
    }
}
