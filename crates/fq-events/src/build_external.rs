//! The external-merge finalize fallback for a shard whose spill outgrew its
//! memory budget (extreme actor skew concentrating on single-actor whales).
//!
//! Two extra spill passes replace the in-memory sort: pass one streams the
//! shard's frames collecting only distinct `(hash, key)` pairs (bounded by
//! distinct actors, tiny for a whale shard) and assigns local ids in the same
//! deterministic `(hash, key)` order the in-memory path uses; pass two
//! re-streams the frames into budget-sized runs, sorts each run by
//! `(local, ts, tiebreak)`, and spills it row-major; a k-way heap merge then
//! feeds the same streaming segment encoder. Output bytes are identical to
//! the in-memory path - same total order, same block boundaries - only
//! slower.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{Read, Write};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::build::{
    append_frame_reader, lookup_prior, merge_max, merge_min, write_shard_files, BuildParams,
    IdAssignment, PropRowCell, ShardResult,
};
use crate::dict::BuildDicts;
use crate::error::{EventBuildError, EventError};
use crate::format::{ActorSidecar, ByteReader, PropCell, SegEncoder};
use crate::model::{PropertyDef, PropertyEncoding};
use crate::store::StoreIo;

/// One row of a sorted run.
struct RunRow {
    local: u64,
    ts: i64,
    tiebreak: i64,
    event: u64,
    cells: Vec<PropRowCell>,
    key: Vec<u8>,
}

impl RunRow {
    /// The total sort key.
    fn sort_key(&self) -> (u64, i64, i64) {
        (self.local, self.ts, self.tiebreak)
    }

    /// Approximate held bytes, for run sizing.
    fn bytes(&self) -> usize {
        let mut total = 48 + self.key.len();
        for cell in &self.cells {
            total += match cell {
                PropRowCell::Str(bytes) => 24 + bytes.len(),
                PropRowCell::Null
                | PropRowCell::Code(_)
                | PropRowCell::I64(_)
                | PropRowCell::F64(_)
                | PropRowCell::Bool(_) => 16,
            };
        }
        total
    }
}

/// Finalize one whale shard externally.
#[allow(clippy::too_many_arguments)]
pub(crate) fn finalize_external(
    io: &StoreIo,
    location: &str,
    params: &BuildParams,
    props: &[PropertyDef],
    dicts: &BuildDicts,
    priors: &[Arc<ActorSidecar>],
    spill_path: &std::path::Path,
    shard: usize,
    peak_shard_budget: u64,
) -> Result<ShardResult, EventError> {
    let assignment = assign_ids_streaming(props, priors, spill_path, shard as u32)?;
    let promoted = promotion_flags(props, dicts);
    let run_paths = write_sorted_runs(
        props,
        &promoted,
        &assignment,
        priors,
        spill_path,
        peak_shard_budget,
    )?;
    let encodings = final_encodings(props, &promoted);
    let mut encoder = SegEncoder::new(shard as u32, params.generation, &encodings);
    let mut min_ts = None;
    let mut max_ts = None;
    merge_runs(&run_paths, props, |row| {
        min_ts = merge_min(min_ts, Some(row.ts));
        max_ts = merge_max(max_ts, Some(row.ts));
        let cells: Vec<PropCell<'_>> = row.cells.iter().map(PropRowCell::as_cell).collect();
        encoder.push_row(row.local, row.ts, row.tiebreak, row.event, &cells, &row.key)?;
        Ok(())
    })?;
    for path in &run_paths {
        std::fs::remove_file(path).ok();
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

/// Which dict properties are promoted, positionally.
fn promotion_flags(props: &[PropertyDef], dicts: &BuildDicts) -> Vec<bool> {
    let mut dict_index = 0usize;
    let mut flags = Vec::with_capacity(props.len());
    for def in props {
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
fn final_encodings(props: &[PropertyDef], promoted: &[bool]) -> Vec<PropertyEncoding> {
    props
        .iter()
        .zip(promoted)
        .map(|(def, is_promoted)| {
            if *is_promoted {
                PropertyEncoding::RawString
            } else {
                def.encoding
            }
        })
        .collect()
}

/// Pass one: collect distinct `(hash, key)` pairs and assign local ids in
/// deterministic `(hash, key)` order, resolving returning actors through
/// prior sidecars.
fn assign_ids_streaming(
    props: &[PropertyDef],
    priors: &[Arc<ActorSidecar>],
    spill_path: &std::path::Path,
    shard: u32,
) -> Result<IdAssignment, EventError> {
    let mut pairs: Vec<(u64, Vec<u8>)> = Vec::new();
    append_frame_reader(spill_path, props, |chunk| {
        for row in 0..chunk.rows() {
            pairs.push((chunk.hash(row), chunk.key(row).to_vec()));
        }
        pairs.sort_unstable();
        pairs.dedup();
        Ok(())
    })?;
    pairs.sort_unstable();
    pairs.dedup();
    let mut next_local: u64 = priors
        .iter()
        .map(|sidecar| sidecar.first_local_id + sidecar.index.len() as u64)
        .max()
        .unwrap_or(0);
    let first_new_local = next_local;
    let mut new_locals = Vec::new();
    let mut new_keys = Vec::new();
    let mut new_hashes = Vec::new();
    for (hash, key) in &pairs {
        if lookup_prior(priors, *hash, key).is_none() {
            new_locals.push(next_local);
            new_keys.push(key.clone());
            new_hashes.push(*hash);
            next_local = next_local
                .checked_add(1)
                .ok_or(EventBuildError::ShardOverflow { shard })?;
        }
    }
    Ok(IdAssignment {
        // The external path resolves per-row locals during run writing, not
        // here; the field stays empty.
        row_locals: Vec::new(),
        new_locals,
        new_keys,
        new_hashes,
        first_new_local,
    })
}

/// Resolve `(hash, key)` to its local id through the assignment or priors.
fn resolve_local(
    assignment: &IdAssignment,
    priors: &[Arc<ActorSidecar>],
    hash: u64,
    key: &[u8],
) -> u64 {
    if let Some(local) = lookup_prior(priors, hash, key) {
        return local;
    }
    // New actors were assigned in (hash, key) order; binary search the
    // parallel arrays directly.
    let mut low = 0usize;
    let mut high = assignment.new_locals.len();
    while low < high {
        let mid = usize::midpoint(low, high);
        let mid_key = (
            assignment.new_hashes[mid],
            assignment.new_keys[mid].as_slice(),
        );
        if mid_key < (hash, key) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let found = low < assignment.new_locals.len()
        && assignment.new_hashes[low] == hash
        && assignment.new_keys[low] == key;
    assert!(found, "pass one saw every (hash, key) pair");
    assignment.new_locals[low]
}

/// Pass two: stream frames into budget-sized runs, sort each, spill it
/// row-major lz4; returns the run paths.
fn write_sorted_runs(
    props: &[PropertyDef],
    promoted: &[bool],
    assignment: &IdAssignment,
    priors: &[Arc<ActorSidecar>],
    spill_path: &std::path::Path,
    peak_shard_budget: u64,
) -> Result<Vec<std::path::PathBuf>, EventError> {
    let mut runs: Vec<std::path::PathBuf> = Vec::new();
    let mut buffer: Vec<RunRow> = Vec::new();
    let mut buffered_bytes = 0usize;
    append_frame_reader(spill_path, props, |chunk| {
        for row in 0..chunk.rows() {
            let (ts, tiebreak, event) = chunk.order(row);
            let key = chunk.key(row).to_vec();
            let local = resolve_local(assignment, priors, chunk.hash(row), &key);
            let run_row = RunRow {
                local,
                ts,
                tiebreak,
                event,
                cells: chunk.cells(promoted, row),
                key,
            };
            buffered_bytes += run_row.bytes();
            buffer.push(run_row);
        }
        if buffered_bytes as u64 >= peak_shard_budget {
            runs.push(flush_run(spill_path, runs.len(), &mut buffer)?);
            buffered_bytes = 0;
        }
        Ok(())
    })?;
    if !buffer.is_empty() {
        runs.push(flush_run(spill_path, runs.len(), &mut buffer)?);
    }
    Ok(runs)
}

/// Sort and spill one run.
fn flush_run(
    spill_path: &std::path::Path,
    index: usize,
    buffer: &mut Vec<RunRow>,
) -> Result<std::path::PathBuf, EventError> {
    buffer.sort_by_key(RunRow::sort_key);
    let path = spill_path.with_extension(format!("run{index}"));
    let mut file = std::io::BufWriter::new(
        std::fs::File::create(&path)
            .map_err(|error| EventBuildError::Io(format!("run create: {error}")))?,
    );
    let mut payload = Vec::new();
    for row in buffer.drain(..) {
        encode_run_row(&row, &mut payload);
        if payload.len() >= 4 * 1024 * 1024 {
            write_run_frame(&mut file, &payload)?;
            payload.clear();
        }
    }
    if !payload.is_empty() {
        write_run_frame(&mut file, &payload)?;
    }
    file.flush()
        .map_err(|error| EventBuildError::Io(format!("run flush: {error}")))?;
    Ok(path)
}

/// Append one lz4 frame to a run file.
fn write_run_frame(file: &mut impl Write, payload: &[u8]) -> Result<(), EventError> {
    let compressed = lz4_flex::compress_prepend_size(payload);
    file.write_all(&(compressed.len() as u32).to_le_bytes())
        .and_then(|()| file.write_all(&compressed))
        .map_err(|error| EventBuildError::Io(format!("run write: {error}")).into())
}

/// Serialize one run row.
fn encode_run_row(row: &RunRow, out: &mut Vec<u8>) {
    out.extend_from_slice(&row.local.to_le_bytes());
    out.extend_from_slice(&row.ts.to_le_bytes());
    out.extend_from_slice(&row.tiebreak.to_le_bytes());
    crate::format::put_varint(out, row.event);
    crate::format::put_varint(out, row.key.len() as u64);
    out.extend_from_slice(&row.key);
    for cell in &row.cells {
        match cell {
            PropRowCell::Null => out.push(0),
            PropRowCell::Code(code) => {
                out.push(1);
                crate::format::put_varint(out, *code);
            }
            PropRowCell::Str(bytes) => {
                out.push(2);
                crate::format::put_varint(out, bytes.len() as u64);
                out.extend_from_slice(bytes);
            }
            PropRowCell::I64(value) => {
                out.push(3);
                out.extend_from_slice(&value.to_le_bytes());
            }
            PropRowCell::F64(value) => {
                out.push(4);
                out.extend_from_slice(&value.to_le_bytes());
            }
            PropRowCell::Bool(value) => {
                out.push(5);
                out.push(u8::from(*value));
            }
        }
    }
}

/// A streaming reader over one sorted run.
struct RunReader {
    reader: std::io::BufReader<std::fs::File>,
    frame: Vec<u8>,
    position: usize,
    props: usize,
    exhausted: bool,
}

impl RunReader {
    /// Open a run file.
    fn open(path: &std::path::Path, props: usize) -> Result<Self, EventError> {
        let file = std::fs::File::open(path)
            .map_err(|error| EventBuildError::Io(format!("run open: {error}")))?;
        Ok(Self {
            reader: std::io::BufReader::with_capacity(1 << 20, file),
            frame: Vec::new(),
            position: 0,
            props,
            exhausted: false,
        })
    }

    /// The next row, or None at end.
    fn next_row(&mut self) -> Result<Option<RunRow>, EventError> {
        if self.position >= self.frame.len() && !self.load_frame()? {
            return Ok(None);
        }
        let frame = std::mem::take(&mut self.frame);
        let mut reader = ByteReader::new(&frame[self.position..], "run");
        let row = decode_run_row(&mut reader, self.props)
            .map_err(|error| EventBuildError::Io(error.to_string()))?;
        self.position += reader.position();
        self.frame = frame;
        Ok(Some(row))
    }

    /// Load the next frame; false at end of file.
    fn load_frame(&mut self) -> Result<bool, EventError> {
        if self.exhausted {
            return Ok(false);
        }
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.exhausted = true;
                return Ok(false);
            }
            Err(error) => return Err(EventBuildError::Io(format!("run read: {error}")).into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut compressed = vec![0u8; len];
        self.reader
            .read_exact(&mut compressed)
            .map_err(|error| EventBuildError::Io(format!("run read: {error}")))?;
        self.frame = lz4_flex::decompress_size_prepended(&compressed)
            .map_err(|error| EventBuildError::Io(format!("run lz4: {error}")))?;
        self.position = 0;
        Ok(true)
    }
}

/// Deserialize one run row.
fn decode_run_row(
    reader: &mut ByteReader<'_>,
    props: usize,
) -> Result<RunRow, crate::error::EventStoreError> {
    let local = reader.u64()?;
    let ts = reader.i64()?;
    let tiebreak = reader.i64()?;
    let event = reader.varint()?;
    let key_len = reader.varint()? as usize;
    let key = reader.take(key_len)?.to_vec();
    let mut cells = Vec::with_capacity(props);
    for _ in 0..props {
        let tag = reader.u8()?;
        cells.push(match tag {
            0 => PropRowCell::Null,
            1 => PropRowCell::Code(reader.varint()?),
            2 => {
                let len = reader.varint()? as usize;
                PropRowCell::Str(reader.take(len)?.to_vec())
            }
            3 => PropRowCell::I64(reader.i64()?),
            4 => PropRowCell::F64(f64::from_bits(reader.u64()?)),
            5 => PropRowCell::Bool(reader.u8()? != 0),
            other => {
                return Err(crate::error::EventStoreError::Corrupt {
                    file: "run".to_string(),
                    detail: format!("unknown run cell tag {other}"),
                })
            }
        });
    }
    Ok(RunRow {
        local,
        ts,
        tiebreak,
        event,
        cells,
        key,
    })
}

/// Merge-heap entry set: each entry pairs a run head's `(local, ts, tiebreak)`
/// sort key (reversed for min-first order) with the index of its run.
type MergeHeap = BinaryHeap<(Reverse<(u64, i64, i64)>, usize)>;

/// K-way merge the sorted runs, feeding each row in `(local, ts, tiebreak)`
/// order to `emit`.
fn merge_runs(
    run_paths: &[std::path::PathBuf],
    props: &[PropertyDef],
    mut emit: impl FnMut(RunRow) -> Result<(), EventBuildError>,
) -> Result<(), EventError> {
    let mut readers = Vec::with_capacity(run_paths.len());
    let mut heap: MergeHeap = BinaryHeap::new();
    let mut heads: Vec<Option<RunRow>> = Vec::with_capacity(run_paths.len());
    for (index, path) in run_paths.iter().enumerate() {
        let mut reader = RunReader::open(path, props.len())?;
        let head = reader.next_row()?;
        if let Some(row) = &head {
            heap.push((Reverse(row.sort_key()), index));
        }
        readers.push(reader);
        heads.push(head);
    }
    while let Some((_, index)) = heap.pop() {
        let row = heads[index].take().expect("heap entries have heads");
        emit(row)?;
        let next = readers[index].next_row()?;
        if let Some(row) = &next {
            heap.push((Reverse(row.sort_key()), index));
        }
        heads[index] = next;
    }
    Ok(())
}
