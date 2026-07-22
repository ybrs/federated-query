//! The paths index: a suffix array with an LCP array over every actor's
//! duplicate-collapsed event stream, plus a second pair over the reversed
//! text. A PATHS statement with no WHERE filter and no MAXGAP is answered at
//! ANY depth from one sweep over a suffix-array interval: consecutive
//! suffixes whose common prefix covers the window length form one distinct
//! path, a window's count is its run length, and a FROM/TO range admits a
//! window by its anchor event's timestamp, read per position. Depth changes
//! neither the index's size nor the sweep's cost.
//!
//! The three statement variants map to interval sweeps:
//!
//! - unanchored: the whole forward array; a position is a window when a full
//!   DEPTH of events remains inside its actor stream.
//! - `STARTING AT`: the forward-array bucket of the anchor's symbol; windows
//!   truncate at stream end (length >= 2).
//! - `ENDING AT`: the same bucket of the BACKWARD array (suffixes of the
//!   reversed text group windows by their content read backward from the
//!   anchor); windows truncate at stream start.
//!
//! The index is DATASET-LEVEL and rebuilt after every create/refresh/rebuild:
//! streams span generation boundaries inside an actor, so per-generation
//! increments cannot be summed. The file name carries a fingerprint of the
//! dataset's generation file list; a file whose fingerprint does not match
//! the live dataset is never loaded (a crash between publish and index write
//! leaves a correct dataset that scans until the next refresh rebuilds it).

use std::cmp::Ordering;

use fq_common::events::{PathsSpec, TimeRange};

use crate::error::{EventBuildError, EventError, EventStoreError};
use crate::exec::{run_sharded, PathsResult, PathsRow};
use crate::format::fnv1a64;
use crate::sais;
use crate::store::{cached_paths_sa, CachedItem, StoreIo};
use crate::Dataset;

// The serialized arrays are raw little-endian; the sweep reads them back with
// plain memory copies, so a big-endian target cannot share store files.
#[cfg(target_endian = "big")]
compile_error!("the paths index stores little-endian arrays; big-endian is unsupported");

/// The file magic of a serialized paths index.
const MAGIC: &[u8; 4] = b"FQPS";

/// The serialization version this build writes and reads.
const VERSION: u8 = 4;

/// The concatenated collapsed text: symbol 0 separates actor streams, and an
/// event with code `c` is the symbol `c + 1`. Byte symbols serve every
/// dictionary of at most 254 events; anything larger widens to i32.
enum Text {
    Small(Vec<u8>),
    Wide(Vec<i32>),
}

impl Text {
    /// The symbol at `pos`.
    fn symbol(&self, pos: usize) -> u32 {
        match self {
            Text::Small(text) => u32::from(text[pos]),
            Text::Wide(text) => u32::try_from(text[pos]).expect("symbols are non-negative"),
        }
    }

    /// Position count.
    fn len(&self) -> usize {
        match self {
            Text::Small(text) => text.len(),
            Text::Wide(text) => text.len(),
        }
    }

    /// Bytes per symbol in the serialized form.
    fn width(&self) -> u8 {
        match self {
            Text::Small(_) => 1,
            Text::Wide(_) => 4,
        }
    }
}

/// The loaded index.
pub struct PathsSa {
    /// Distinct event codes of the dataset dictionary; text symbols run
    /// `0..=alphabet` (0 is the separator).
    alphabet: u32,
    text: Text,
    /// Per direction: the suffix array, its LCP array, and two SA-ORDER
    /// companions - each suffix's window availability (how many events remain
    /// inside its actor stream; 0 or 1 at separators) and its anchor
    /// timestamp. The sweep walks all four sequentially; nothing in its hot
    /// loop is a random access.
    sa_fwd: Vec<i32>,
    lcp_fwd: Vec<i32>,
    avail_fwd: Vec<u32>,
    ts_fwd: Vec<i64>,
    sa_bwd: Vec<i32>,
    lcp_bwd: Vec<i32>,
    avail_bwd: Vec<u32>,
    ts_bwd: Vec<i64>,
    /// Suffix-array bucket offsets per first symbol (length `alphabet + 2`):
    /// suffixes whose first symbol is `s` occupy `buckets[s]..buckets[s + 1]`
    /// in BOTH arrays (the reversed text is a permutation of the text).
    buckets: Vec<u32>,
}

/// Which suffix array a sweep walks.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    Forward,
    Backward,
}

impl PathsSa {
    /// Approximate resident bytes, for the block cache's budget accounting.
    pub fn heap_bytes(&self) -> usize {
        let n = self.text.len();
        n * (usize::from(self.text.width()) + 2 * (4 + 4 + 4 + 8)) + self.buckets.len() * 4
    }

    /// The sweep arrays of a direction: (sa, lcp, avail, anchor ts), all in
    /// suffix-array order.
    fn arrays(&self, dir: Direction) -> (&[i32], &[i32], &[u32], &[i64]) {
        match dir {
            Direction::Forward => (&self.sa_fwd, &self.lcp_fwd, &self.avail_fwd, &self.ts_fwd),
            Direction::Backward => (&self.sa_bwd, &self.lcp_bwd, &self.avail_bwd, &self.ts_bwd),
        }
    }

    /// The forward-order symbol at `offset` of the window represented by
    /// suffix-array position `rep` with length `w`.
    fn window_symbol(&self, dir: Direction, rep: u32, w: u32, offset: u32) -> u32 {
        match dir {
            Direction::Forward => self.text.symbol(rep as usize + offset as usize),
            Direction::Backward => {
                let origin = self.text.len() - 1 - rep as usize;
                self.text.symbol(origin + 1 - w as usize + offset as usize)
            }
        }
    }

    /// Whether two same-length windows hold the same symbols.
    fn windows_equal(&self, dir: Direction, a: u32, b: u32, w: u32) -> bool {
        (0..w).all(|offset| {
            self.window_symbol(dir, a, w, offset) == self.window_symbol(dir, b, w, offset)
        })
    }

    /// Window order for count ties: elementwise event-name rank, shorter
    /// prefix first - the order the rendered rows sort by. The groups carry
    /// their first symbol, so the common case (different first events)
    /// resolves without touching the text.
    fn window_order(&self, ranks: &[u32], dir: Direction, a: &Group, b: &Group) -> Ordering {
        if a.sym != b.sym {
            return ranks[a.sym as usize].cmp(&ranks[b.sym as usize]);
        }
        let shared = a.w.min(b.w);
        for offset in 0..shared {
            let ra = ranks[self.window_symbol(dir, a.rep, a.w, offset) as usize];
            let rb = ranks[self.window_symbol(dir, b.rep, b.w, offset) as usize];
            match ra.cmp(&rb) {
                Ordering::Equal => {}
                unequal => return unequal,
            }
        }
        a.w.cmp(&b.w)
    }

    /// Selection order: count descending, then window order ascending. Total
    /// over distinct windows, so top-N pruning is exact.
    fn selection_order(&self, ranks: &[u32], dir: Direction, a: &Group, b: &Group) -> Ordering {
        b.count
            .cmp(&a.count)
            .then_with(|| self.window_order(ranks, dir, a, b))
    }

    /// The event codes of a group's window, in forward order.
    fn window_codes(&self, dir: Direction, group: &Group) -> Vec<u64> {
        (0..group.w)
            .map(|offset| u64::from(self.window_symbol(dir, group.rep, group.w, offset) - 1))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

/// The dataset's index fingerprint: a hash of the ordered generation file
/// list, so the index file is bound to the exact set of segments it was
/// built over.
fn fingerprint(dataset: &Dataset) -> u64 {
    use std::fmt::Write;
    let mut names = String::new();
    for (shard, files) in dataset.shard_files() {
        for file in files {
            writeln!(names, "{shard}:{file}").expect("string write is infallible");
        }
    }
    fnv1a64(names.as_bytes())
}

/// The forward index file's relative path for the current generation set
/// (text and forward arrays).
fn fwd_rel(dataset: &Dataset) -> String {
    format!(
        "{}/paths_f_{:016x}.psax",
        dataset.record.location,
        fingerprint(dataset)
    )
}

/// The backward index file's relative path (backward arrays only); split from
/// the forward file so the two encode, write, and load on concurrent threads.
fn bwd_rel(dataset: &Dataset) -> String {
    format!(
        "{}/paths_b_{:016x}.psax",
        dataset.record.location,
        fingerprint(dataset)
    )
}

/// The files sitting DIRECTLY under the dataset location (not inside a
/// generation subdirectory) whose name starts with `prefix`. The object-store
/// list API prefixes by whole path segments, so a partial file name must be
/// filtered here, over the location listing.
fn location_files_with_prefix(
    io: &StoreIo,
    location: &str,
    prefix: &str,
) -> Result<Vec<String>, EventError> {
    let mut matched = Vec::new();
    for rel in io.list(location)? {
        let Some(name) = rel
            .strip_prefix(location)
            .and_then(|rest| rest.strip_prefix('/'))
        else {
            continue;
        };
        if !name.contains('/') && name.starts_with(prefix) {
            matched.push(rel);
        }
    }
    Ok(matched)
}

/// One shard's duplicate-collapsed streams: `codes[bounds[i]..bounds[i + 1]]`
/// (and the aligned timestamps) is actor i's sequence.
struct ShardStream {
    codes: Vec<u32>,
    ts: Vec<i64>,
    bounds: Vec<u32>,
}

/// Read every shard's collapsed stream in one sharded dataset pass.
fn collect_streams(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &fq_common::events::EventsConfig,
) -> Result<Vec<ShardStream>, EventError> {
    let mut streams = Vec::new();
    run_sharded(
        io,
        dataset,
        cfg,
        |scan| {
            let mut codes: Vec<u32> = Vec::new();
            let mut ts: Vec<i64> = Vec::new();
            let mut bounds: Vec<u32> = vec![0];
            for run in scan.actor_runs() {
                let start = codes.len();
                scan.for_each_event(&run, |file, row, code| {
                    if codes.len() == start || u64::from(codes[codes.len() - 1]) != code {
                        codes.push(u32::try_from(code).expect("dictionary code fits u32"));
                        ts.push(scan.files[file].ts.i64_at(row)?);
                    }
                    Ok(true)
                })?;
                bounds.push(u32::try_from(codes.len()).expect("shard stream fits u32"));
            }
            Ok(ShardStream { codes, ts, bounds })
        },
        |stream| {
            streams.push(stream);
            Ok(())
        },
    )?;
    Ok(streams)
}

/// Assemble the index from collapsed streams: lay out the separator-joined
/// text with its per-position timestamp and actor-bound arrays, suffix-sort
/// the text and its reversal concurrently, and derive both LCP arrays and the
/// first-symbol buckets.
fn assemble(streams: &[ShardStream], alphabet: u32) -> Result<PathsSa, EventBuildError> {
    let events: usize = streams.iter().map(|stream| stream.codes.len()).sum();
    let actors: usize = streams.iter().map(|stream| stream.bounds.len() - 1).sum();
    let n = events + actors;
    if i32::try_from(n).is_err() {
        return Err(EventBuildError::SuffixTextTooLong { positions: n });
    }
    let small = alphabet <= 254;
    let mut text8: Vec<u8> = Vec::with_capacity(if small { n } else { 0 });
    let mut text32: Vec<i32> = Vec::with_capacity(if small { 0 } else { n });
    let mut ts: Vec<i64> = Vec::with_capacity(n);
    let mut start: Vec<u32> = Vec::with_capacity(n);
    let mut end: Vec<u32> = Vec::with_capacity(n);
    for stream in streams {
        for actor in 0..stream.bounds.len() - 1 {
            let lo = stream.bounds[actor] as usize;
            let hi = stream.bounds[actor + 1] as usize;
            let text_lo = u32::try_from(ts.len()).expect("text fits u32");
            let text_hi = text_lo + u32::try_from(hi - lo).expect("stream fits u32");
            for offset in lo..hi {
                if small {
                    text8.push(u8::try_from(stream.codes[offset] + 1).expect("code fits u8"));
                } else {
                    text32.push(
                        i32::try_from(stream.codes[offset] + 1).expect("code fits i32"),
                    );
                }
                ts.push(stream.ts[offset]);
                start.push(text_lo);
                end.push(text_hi);
            }
            // The separator after the stream: an out-of-alphabet symbol whose
            // start == end == its own position keeps every window off it.
            if small {
                text8.push(0);
            } else {
                text32.push(0);
            }
            ts.push(0);
            start.push(text_hi);
            end.push(text_hi);
        }
    }
    let text = if small {
        Text::Small(text8)
    } else {
        Text::Wide(text32)
    };
    let (sa_fwd, lcp_fwd, sa_bwd, lcp_bwd) = sort_both_directions(&text, alphabet)?;
    let buckets = bucket_offsets(&text, alphabet);
    // Scatter availability and anchor timestamps into SUFFIX-ARRAY order, so
    // the sweep never takes a random read: forward availability runs to the
    // actor stream's end; a backward suffix (of the reversed text) maps onto
    // its origin position, whose availability runs back to the stream start.
    let last = ts.len().saturating_sub(1);
    let (avail_fwd, ts_fwd) = sa_companions(&sa_fwd, |pos| {
        (end[pos] - u32::try_from(pos).expect("position fits u32"), ts[pos])
    });
    let (avail_bwd, ts_bwd) = sa_companions(&sa_bwd, |pos| {
        let origin = last - pos;
        (
            u32::try_from(origin).expect("position fits u32") - start[origin] + 1,
            ts[origin],
        )
    });
    Ok(PathsSa {
        alphabet,
        text,
        sa_fwd,
        lcp_fwd,
        avail_fwd,
        ts_fwd,
        sa_bwd,
        lcp_bwd,
        avail_bwd,
        ts_bwd,
        buckets,
    })
}

/// Gather one direction's (availability, anchor ts) companions in suffix-array
/// order, in parallel over output chunks.
fn sa_companions(
    sa: &[i32],
    shape: impl Fn(usize) -> (u32, i64) + Sync,
) -> (Vec<u32>, Vec<i64>) {
    let mut avail = vec![0u32; sa.len()];
    let mut ts = vec![0i64; sa.len()];
    let workers = std::thread::available_parallelism()
        .map_or(4, std::num::NonZero::get)
        .min(sa.len() / 65_536 + 1);
    let chunk = sa.len().div_ceil(workers.max(1));
    std::thread::scope(|scope| {
        let mut avail_rest: &mut [u32] = &mut avail;
        let mut ts_rest: &mut [i64] = &mut ts;
        let mut offset = 0usize;
        while offset < sa.len() {
            let take = chunk.min(sa.len() - offset);
            let (avail_out, avail_tail) = avail_rest.split_at_mut(take);
            avail_rest = avail_tail;
            let (ts_out, ts_tail) = ts_rest.split_at_mut(take);
            ts_rest = ts_tail;
            let source = &sa[offset..offset + take];
            let shape = &shape;
            scope.spawn(move || {
                for (slot, suffix) in source.iter().enumerate() {
                    let pos = usize::try_from(*suffix).expect("suffix position fits usize");
                    let (a, t) = shape(pos);
                    avail_out[slot] = a;
                    ts_out[slot] = t;
                }
            });
            offset += take;
        }
    });
    (avail, ts)
}

/// Suffix-sort the text and its reversal on concurrent thread teams, each
/// with half the machine's cores, returning both (SA, LCP) pairs.
#[allow(clippy::type_complexity)]
fn sort_both_directions(
    text: &Text,
    alphabet: u32,
) -> Result<(Vec<i32>, Vec<i32>, Vec<i32>, Vec<i32>), EventBuildError> {
    let cores = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    let team = i32::try_from((cores / 2).max(1)).expect("core count fits i32");
    let k = i32::try_from(alphabet + 1).expect("alphabet fits i32");
    let (forward, backward) = std::thread::scope(|scope| {
        let fwd = scope.spawn(move || match text {
            Text::Small(bytes) => {
                let sa = sais::suffix_array_u8(bytes, team)?;
                let lcp = sais::lcp_array_u8(bytes, &sa, team)?;
                Ok::<_, EventBuildError>((sa, lcp))
            }
            Text::Wide(symbols) => {
                let sa = sais::suffix_array_i32(symbols, k, team)?;
                let lcp = sais::lcp_array_i32(symbols, &sa, team)?;
                Ok((sa, lcp))
            }
        });
        let bwd = scope.spawn(move || match text {
            Text::Small(bytes) => {
                let reversed: Vec<u8> = bytes.iter().rev().copied().collect();
                let sa = sais::suffix_array_u8(&reversed, team)?;
                let lcp = sais::lcp_array_u8(&reversed, &sa, team)?;
                Ok::<_, EventBuildError>((sa, lcp))
            }
            Text::Wide(symbols) => {
                let reversed: Vec<i32> = symbols.iter().rev().copied().collect();
                let sa = sais::suffix_array_i32(&reversed, k, team)?;
                let lcp = sais::lcp_array_i32(&reversed, &sa, team)?;
                Ok((sa, lcp))
            }
        });
        (
            fwd.join().expect("forward sort thread panicked"),
            bwd.join().expect("backward sort thread panicked"),
        )
    });
    let (sa_fwd, lcp_fwd) = forward?;
    let (sa_bwd, lcp_bwd) = backward?;
    Ok((sa_fwd, lcp_fwd, sa_bwd, lcp_bwd))
}

/// First-symbol bucket offsets from the text histogram: the suffix array is
/// first-symbol-major, so exclusive prefix sums locate every symbol's
/// interval - `buckets[s]..buckets[s + 1]` holds the suffixes starting with
/// symbol `s`, in both arrays.
fn bucket_offsets(text: &Text, alphabet: u32) -> Vec<u32> {
    let mut buckets = vec![0u32; alphabet as usize + 2];
    for pos in 0..text.len() {
        buckets[text.symbol(pos) as usize + 1] += 1;
    }
    for symbol in 1..buckets.len() {
        buckets[symbol] += buckets[symbol - 1];
    }
    buckets
}

/// Build the paths index over the dataset's current generation set, replacing
/// any stale index files, and return the DDL status summary.
pub(crate) fn rebuild(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &fq_common::events::EventsConfig,
) -> Result<String, EventError> {
    for stale in location_files_with_prefix(io, &dataset.record.location, "paths_")? {
        io.delete(&stale)?;
    }
    let started = std::time::Instant::now();
    let streams = collect_streams(io, dataset, cfg)?;
    let collected = started.elapsed().as_secs_f64();
    let alphabet = u32::try_from(dataset.event_dict.values.len()).expect("dictionary fits u32");
    let index = assemble(&streams, alphabet)?;
    drop(streams);
    let sorted = started.elapsed().as_secs_f64();
    let positions = index.text.len();
    let stamp = fingerprint(dataset);
    // The two files encode and write concurrently; each is roughly half the
    // index's bytes.
    let written = std::thread::scope(|scope| {
        let fwd = scope.spawn(|| {
            let bytes = encode_fwd(&index, stamp);
            let len = bytes.len();
            io.put_verified(&fwd_rel(dataset), bytes).map(|()| len)
        });
        let bwd = scope.spawn(|| {
            let bytes = encode_bwd(&index, stamp);
            let len = bytes.len();
            io.put_verified(&bwd_rel(dataset), bytes).map(|()| len)
        });
        let fwd_len = fwd.join().expect("forward write thread panicked")?;
        let bwd_len = bwd.join().expect("backward write thread panicked")?;
        Ok::<usize, crate::error::EventStoreError>(fwd_len + bwd_len)
    })?;
    let total = started.elapsed().as_secs_f64();
    Ok(format!(
        "paths index: {positions} positions, {written} bytes, {total:.3}s \
         (collect {collected:.3}s, sort {:.3}s, write {:.3}s)",
        sorted - collected,
        total - sorted
    ))
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

/// The little-endian byte view of a plain numeric slice.
fn raw_bytes<T>(values: &[T]) -> &[u8] {
    // A plain numeric slice is always viewable as its raw bytes.
    unsafe {
        std::slice::from_raw_parts(
            values.as_ptr().cast::<u8>(),
            std::mem::size_of_val(values),
        )
    }
}

/// The shared file header: magic, version, fingerprint, position count,
/// alphabet, and symbol width.
fn encode_header(index: &PathsSa, fingerprint: u64, capacity: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&fingerprint.to_le_bytes());
    out.extend_from_slice(&(index.text.len() as u64).to_le_bytes());
    out.extend_from_slice(&index.alphabet.to_le_bytes());
    out.push(index.text.width());
    out
}

/// Encode the forward file: header, text, then the forward arrays.
fn encode_fwd(index: &PathsSa, fingerprint: u64) -> Vec<u8> {
    let n = index.text.len();
    let width = usize::from(index.text.width());
    let mut out = encode_header(index, fingerprint, 32 + n * (width + 20));
    match &index.text {
        Text::Small(text) => out.extend_from_slice(text),
        Text::Wide(text) => out.extend_from_slice(raw_bytes(text)),
    }
    out.extend_from_slice(raw_bytes(&index.sa_fwd));
    out.extend_from_slice(raw_bytes(&index.lcp_fwd));
    out.extend_from_slice(raw_bytes(&index.avail_fwd));
    out.extend_from_slice(raw_bytes(&index.ts_fwd));
    out
}

/// Encode the backward file: header, then the backward arrays.
fn encode_bwd(index: &PathsSa, fingerprint: u64) -> Vec<u8> {
    let n = index.text.len();
    let mut out = encode_header(index, fingerprint, 32 + n * 20);
    out.extend_from_slice(raw_bytes(&index.sa_bwd));
    out.extend_from_slice(raw_bytes(&index.lcp_bwd));
    out.extend_from_slice(raw_bytes(&index.avail_bwd));
    out.extend_from_slice(raw_bytes(&index.ts_bwd));
    out
}

/// A section cursor over the serialized bytes with explicit bounds checks.
struct Sections<'a> {
    bytes: &'a [u8],
    offset: usize,
    rel: &'a str,
}

impl<'a> Sections<'a> {
    /// Take `len` bytes or raise store corruption.
    fn take(&mut self, len: usize) -> Result<&'a [u8], EventStoreError> {
        let end = self.offset.checked_add(len).ok_or_else(|| self.corrupt())?;
        if end > self.bytes.len() {
            return Err(self.corrupt());
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    /// The truncated-file error.
    fn corrupt(&self) -> EventStoreError {
        EventStoreError::Corrupt {
            file: self.rel.to_string(),
            detail: "paths index file is shorter than its header promises".to_string(),
        }
    }
}

/// Copy one raw little-endian section into a typed vector.
fn copy_typed<T: Copy + Default>(bytes: &[u8]) -> Vec<T> {
    let count = bytes.len() / std::mem::size_of::<T>();
    let mut values = vec![T::default(); count];
    // The destination is freshly allocated at exactly the section's length.
    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ptr(),
            values.as_mut_ptr().cast::<u8>(),
            bytes.len(),
        );
    }
    values
}

/// Parse and verify one file's header, returning its section cursor and the
/// (position count, alphabet, symbol width) triple.
fn open_sections<'a>(
    bytes: &'a [u8],
    rel: &'a str,
    expected: u64,
) -> Result<(Sections<'a>, usize, u32, u8), EventStoreError> {
    let corrupt = |detail: String| EventStoreError::Corrupt {
        file: rel.to_string(),
        detail,
    };
    let mut sections = Sections {
        bytes,
        offset: 0,
        rel,
    };
    if sections.take(4)? != MAGIC.as_slice() {
        return Err(corrupt("paths index magic mismatch".to_string()));
    }
    let version = sections.take(1)?[0];
    if version != VERSION {
        return Err(corrupt(format!("paths index version {version}, expected {VERSION}")));
    }
    let stored = u64::from_le_bytes(sections.take(8)?.try_into().expect("8 bytes"));
    if stored != expected {
        return Err(corrupt(format!(
            "paths index fingerprint {stored:016x} does not match dataset {expected:016x}"
        )));
    }
    let n = usize::try_from(u64::from_le_bytes(
        sections.take(8)?.try_into().expect("8 bytes"),
    ))
    .map_err(|_| corrupt("paths index position count overflows usize".to_string()))?;
    let alphabet = u32::from_le_bytes(sections.take(4)?.try_into().expect("4 bytes"));
    let width = sections.take(1)?[0];
    Ok((sections, n, alphabet, width))
}

/// Decode the two index files, verifying magic, version, and fingerprint on
/// each and that their headers agree. A mismatched fingerprint is store
/// corruption (the file names embed the same fingerprint that named them).
fn decode(
    fwd_bytes: &[u8],
    bwd_bytes: &[u8],
    rel: &str,
    expected: u64,
) -> Result<PathsSa, EventStoreError> {
    let corrupt = |detail: String| EventStoreError::Corrupt {
        file: rel.to_string(),
        detail,
    };
    let (mut sections, n, alphabet, width) = open_sections(fwd_bytes, rel, expected)?;
    let (mut sections_b, n_b, alphabet_b, width_b) = open_sections(bwd_bytes, rel, expected)?;
    if n != n_b || alphabet != alphabet_b || width != width_b {
        return Err(corrupt("paths index halves disagree on their header".to_string()));
    }
    // Every section's bytes are sliced up front, then all nine copy into
    // their typed vectors on concurrent threads - the copies dominate a cold
    // load, and they are independent.
    let text_bytes = sections.take(n * usize::from(width))?;
    let sa_fwd_bytes = sections.take(n * 4)?;
    let lcp_fwd_bytes = sections.take(n * 4)?;
    let avail_fwd_bytes = sections.take(n * 4)?;
    let ts_fwd_bytes = sections.take(n * 8)?;
    let sa_bwd_bytes = sections_b.take(n * 4)?;
    let lcp_bwd_bytes = sections_b.take(n * 4)?;
    let avail_bwd_bytes = sections_b.take(n * 4)?;
    let ts_bwd_bytes = sections_b.take(n * 8)?;
    let (text, sa_fwd, lcp_fwd, avail_fwd, ts_fwd, sa_bwd, lcp_bwd, avail_bwd, ts_bwd) =
        std::thread::scope(|scope| {
            let text = scope.spawn(move || match width {
                1 => Ok(Text::Small(copy_typed::<u8>(text_bytes))),
                4 => Ok(Text::Wide(copy_typed::<i32>(text_bytes))),
                other => Err(format!("paths index symbol width {other}")),
            });
            let sa_f = scope.spawn(move || copy_typed::<i32>(sa_fwd_bytes));
            let lcp_f = scope.spawn(move || copy_typed::<i32>(lcp_fwd_bytes));
            let avail_f = scope.spawn(move || copy_typed::<u32>(avail_fwd_bytes));
            let ts_f = scope.spawn(move || copy_typed::<i64>(ts_fwd_bytes));
            let sa_b = scope.spawn(move || copy_typed::<i32>(sa_bwd_bytes));
            let lcp_b = scope.spawn(move || copy_typed::<i32>(lcp_bwd_bytes));
            let avail_b = scope.spawn(move || copy_typed::<u32>(avail_bwd_bytes));
            let ts_b = scope.spawn(move || copy_typed::<i64>(ts_bwd_bytes));
            (
                text.join().expect("text copy thread panicked"),
                sa_f.join().expect("copy thread panicked"),
                lcp_f.join().expect("copy thread panicked"),
                avail_f.join().expect("copy thread panicked"),
                ts_f.join().expect("copy thread panicked"),
                sa_b.join().expect("copy thread panicked"),
                lcp_b.join().expect("copy thread panicked"),
                avail_b.join().expect("copy thread panicked"),
                ts_b.join().expect("copy thread panicked"),
            )
        });
    let text = text.map_err(corrupt)?;
    let buckets = bucket_offsets(&text, alphabet);
    Ok(PathsSa {
        alphabet,
        text,
        sa_fwd,
        lcp_fwd,
        avail_fwd,
        ts_fwd,
        sa_bwd,
        lcp_bwd,
        avail_bwd,
        ts_bwd,
        buckets,
    })
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

/// Whether the spec's shape is answerable from the index: no property filter
/// (anchor-property serving is not built) and no MAXGAP (fragmenting changes
/// the counted stream). A time range IS indexable, at any precision, as is
/// any DEPTH.
pub(crate) fn spec_is_indexable(spec: &PathsSpec) -> bool {
    spec.filter.is_none() && spec.maxgap_micros.is_none()
}

/// Load the dataset's current index into the warm cache without answering
/// anything; a missing index is a no-op. The first PATHS statement then pays
/// a cache hit instead of the multi-gigabyte cold read.
pub(crate) fn prewarm(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &fq_common::events::EventsConfig,
) -> Result<(), EventError> {
    let rel = fwd_rel(dataset);
    let rel_b = bwd_rel(dataset);
    if !io.exists(&rel)? || !io.exists(&rel_b)? {
        return Ok(());
    }
    let expected = fingerprint(dataset);
    cached_paths_sa(io, &rel, cfg.cache_bytes, || {
        let (fwd_bytes, bwd_bytes) = std::thread::scope(|scope| {
            let bwd = scope.spawn(|| io.get(&rel_b));
            (io.get(&rel), bwd.join().expect("backward read thread panicked"))
        });
        Ok(CachedItem::PathsSa(decode(
            &fwd_bytes?,
            &bwd_bytes?,
            &rel,
            expected,
        )?))
    })?;
    Ok(())
}

/// Answer an indexable PATHS spec from the dataset's index, or `None` when no
/// current-fingerprint index file exists (never built, or the dataset changed
/// since). The caller scans on `None`.
pub(crate) fn run_from_index(
    io: &StoreIo,
    dataset: &Dataset,
    spec: &PathsSpec,
    anchor: Option<(u64, bool)>,
    cfg: &fq_common::events::EventsConfig,
) -> Result<Option<PathsResult>, EventError> {
    let rel = fwd_rel(dataset);
    let rel_b = bwd_rel(dataset);
    if !io.exists(&rel)? || !io.exists(&rel_b)? {
        return Ok(None);
    }
    let expected = fingerprint(dataset);
    let cached = cached_paths_sa(io, &rel, cfg.cache_bytes, || {
        // Both halves read concurrently; each is roughly half the bytes.
        let (fwd_bytes, bwd_bytes) = std::thread::scope(|scope| {
            let bwd = scope.spawn(|| io.get(&rel_b));
            (io.get(&rel), bwd.join().expect("backward read thread panicked"))
        });
        Ok(CachedItem::PathsSa(decode(
            &fwd_bytes?,
            &bwd_bytes?,
            &rel,
            expected,
        )?))
    })?;
    let CachedItem::PathsSa(ref index) = *cached else {
        unreachable!("paths index cache slot holds a paths index");
    };
    Ok(Some(answer(
        index,
        &dataset.event_dict.values,
        spec,
        anchor,
    )))
}

/// One distinct window met during a sweep: its representative suffix-array
/// text position, its length, its in-range occurrence count, and its
/// forward-order first symbol (the count-tie prefilter).
#[derive(Clone, Copy)]
struct Group {
    rep: u32,
    w: u32,
    count: u64,
    sym: u32,
}

/// A chunk's sweep result: the boundary groups (which may continue into the
/// neighboring chunks), the pruned interior candidates, and the chunk's
/// in-range window total.
struct ChunkSweep {
    total: u64,
    head: Option<Group>,
    tail: Option<Group>,
    /// Whether `head` and `tail` are the SAME group (at most one group met).
    single: bool,
    interior: Vec<Group>,
}

/// The sweep parameters shared by every chunk of one statement.
struct Sweep<'a> {
    index: &'a PathsSa,
    dir: Direction,
    /// Anchored sweeps truncate windows at the stream boundary (length >= 2);
    /// unanchored sweeps admit full-length windows only.
    truncate: bool,
    depth: u32,
    from: i64,
    to: i64,
    ranks: &'a [u32],
    top: usize,
}

/// Keep the best `cap` groups by selection order, exactly.
struct TopSet<'a, 'b> {
    sweep: &'a Sweep<'b>,
    groups: Vec<Group>,
}

impl<'a, 'b> TopSet<'a, 'b> {
    /// An empty set.
    fn new(sweep: &'a Sweep<'b>) -> Self {
        Self {
            sweep,
            groups: Vec::new(),
        }
    }

    /// Offer one closed group.
    fn offer(&mut self, group: Group) {
        let order = |a: &Group, b: &Group| {
            self.sweep
                .index
                .selection_order(self.sweep.ranks, self.sweep.dir, a, b)
        };
        if self.groups.len() >= self.sweep.top {
            let worst = self.groups[self.groups.len() - 1];
            if order(&worst, &group) != Ordering::Greater {
                return;
            }
        }
        let at = self
            .groups
            .binary_search_by(|probe| order(probe, &group))
            .unwrap_or_else(|insert| insert);
        self.groups.insert(at, group);
        self.groups.truncate(self.sweep.top);
    }
}

/// Sweep one suffix-array index range: walk positions in order, group
/// consecutive suffixes into distinct windows (same length, common prefix
/// covering that length), and count each group's in-range members. Boundary
/// groups are returned open for the cross-chunk merge.
fn sweep_chunk(sweep: &Sweep<'_>, lo: usize, hi: usize) -> ChunkSweep {
    let (sa, lcp, avail_sa, ts_sa) = sweep.index.arrays(sweep.dir);
    let buckets = &sweep.index.buckets;
    let mut total = 0u64;
    let mut head: Option<Group> = None;
    let mut pending: Option<Group> = None;
    let mut interior = TopSet::new(sweep);
    let mut open: Option<Group> = None;
    let mut min_lcp = u32::MAX;
    // The forward sweep's first-symbol cursor: suffixes are first-symbol-major,
    // so tracking the bucket boundary makes each group's first symbol free.
    let mut sym_cursor = match buckets.partition_point(|offset| *offset as usize <= lo) {
        0 => 0u32,
        above => u32::try_from(above - 1).expect("symbol fits u32"),
    };
    let close = |group: Group, head: &mut Option<Group>, interior: &mut TopSet<'_, '_>,
                     pending: &mut Option<Group>| {
        if head.is_none() {
            *head = Some(group);
        } else if let Some(previous) = pending.replace(group) {
            interior.offer(previous);
        }
    };
    for at in lo..hi {
        if open.is_some() {
            min_lcp = min_lcp.min(u32::try_from(lcp[at]).expect("lcp is non-negative"));
        }
        let avail = avail_sa[at];
        let (valid, w) = if sweep.truncate {
            (avail >= 2, sweep.depth.min(avail))
        } else {
            (avail >= sweep.depth, sweep.depth)
        };
        if !valid {
            continue;
        }
        let anchor_ts = ts_sa[at];
        let in_range = anchor_ts >= sweep.from && anchor_ts < sweep.to;
        match open.as_mut() {
            Some(group) if group.w == w && min_lcp >= group.w => {
                if in_range {
                    group.count += 1;
                }
            }
            _ => {
                if let Some(done) = open.take() {
                    close(done, &mut head, &mut interior, &mut pending);
                }
                let rep = u32::try_from(sa[at]).expect("suffix positions are non-negative");
                let sym = match sweep.dir {
                    Direction::Forward => {
                        while (at as u32) >= buckets[sym_cursor as usize + 1] {
                            sym_cursor += 1;
                        }
                        sym_cursor
                    }
                    // Backward groups read their forward first symbol from the
                    // text; backward sweeps are anchored buckets, so this stays
                    // off the unanchored whole-array path.
                    Direction::Backward => sweep.index.window_symbol(sweep.dir, rep, w, 0),
                };
                open = Some(Group {
                    rep,
                    w,
                    count: u64::from(in_range),
                    sym,
                });
                min_lcp = u32::MAX;
            }
        }
        if in_range {
            total += 1;
        }
    }
    if let Some(previous) = pending.take() {
        interior.offer(previous);
    }
    // A met group only ever closes when its successor opens, so at the end of
    // the range the LAST group is still open: `open` is None exactly when the
    // chunk met no group at all, and a lone `open` with no closed head is the
    // chunk's single group, continuable at both edges.
    let (head, tail, single) = match (head, open) {
        (None, None) => (None, None, false),
        (None, Some(only)) => (Some(only), Some(only), true),
        (Some(_), None) => unreachable!("a met group stays open to the chunk edge"),
        (Some(first), Some(last)) => (Some(first), Some(last), false),
    };
    ChunkSweep {
        total,
        head,
        tail,
        single,
        interior: interior.groups,
    }
}

/// Run one sweep across the whole `lo..hi` interval on all cores and merge
/// the chunk results into the final candidate list and in-range total.
fn sweep_interval(sweep: &Sweep<'_>, lo: usize, hi: usize) -> (Vec<Group>, u64) {
    let span = hi - lo;
    let workers = std::thread::available_parallelism()
        .map_or(4, std::num::NonZero::get)
        .min(span / 65_536 + 1);
    let mut chunks: Vec<(usize, usize)> = Vec::with_capacity(workers);
    for worker in 0..workers {
        let begin = lo + span * worker / workers;
        let finish = lo + span * (worker + 1) / workers;
        if begin < finish {
            chunks.push((begin, finish));
        }
    }
    let results: Vec<ChunkSweep> = std::thread::scope(|scope| {
        let handles: Vec<_> = chunks
            .iter()
            .map(|(begin, finish)| {
                let (begin, finish) = (*begin, *finish);
                scope.spawn(move || sweep_chunk(sweep, begin, finish))
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("sweep thread panicked"))
            .collect()
    });
    let mut finals: Vec<Group> = Vec::new();
    let mut open: Option<Group> = None;
    let mut total = 0u64;
    for chunk in results {
        total += chunk.total;
        finals.extend(chunk.interior);
        let mut head = chunk.head;
        if let Some(carried) = open.take() {
            if let Some(h) = head.as_mut() {
                if carried.w == h.w
                    && sweep
                        .index
                        .windows_equal(sweep.dir, carried.rep, h.rep, carried.w)
                {
                    h.count += carried.count;
                } else {
                    finals.push(carried);
                }
            } else {
                // An all-invalid chunk: the carried group may continue past it.
                open = Some(carried);
                continue;
            }
        }
        match (head, chunk.tail, chunk.single) {
            (Some(merged), Some(_), true) => open = Some(merged),
            (Some(merged), Some(tail), false) => {
                finals.push(merged);
                open = Some(tail);
            }
            (None, None, _) => {}
            _ => unreachable!("chunk boundary groups are both set or both empty"),
        }
    }
    if let Some(last) = open {
        finals.push(last);
    }
    (finals, total)
}

/// Event-name ranks per text symbol: the rank of a code's name among the
/// dictionary's names sorted ascending, so count ties order without string
/// comparisons in the sweep.
fn name_ranks(names: &[String], alphabet: u32) -> Vec<u32> {
    let mut order: Vec<u32> = (0..alphabet).collect();
    order.sort_by(|a, b| names[*a as usize].cmp(&names[*b as usize]));
    let mut ranks = vec![0u32; alphabet as usize + 1];
    for (rank, code) in order.iter().enumerate() {
        ranks[*code as usize + 1] = u32::try_from(rank).expect("rank fits u32");
    }
    ranks
}

/// Assemble the result: sweep the variant's interval, rank candidates by
/// (count desc, names asc), and compute shares against the in-range window
/// total - the same semantics the scan's enumeration yields.
fn answer(
    index: &PathsSa,
    names: &[String],
    spec: &PathsSpec,
    anchor: Option<(u64, bool)>,
) -> PathsResult {
    let (from, to) = range_bounds(spec.range);
    let ranks = name_ranks(names, index.alphabet);
    let (dir, truncate, lo, hi) = match anchor {
        None => (
            Direction::Forward,
            false,
            index.buckets[1] as usize,
            index.text.len(),
        ),
        Some((code, forward)) => {
            if code >= u64::from(index.alphabet) {
                return PathsResult { rows: Vec::new() };
            }
            let symbol = usize::try_from(code + 1).expect("symbol fits usize");
            let dir = if forward {
                Direction::Forward
            } else {
                Direction::Backward
            };
            (
                dir,
                true,
                index.buckets[symbol] as usize,
                index.buckets[symbol + 1] as usize,
            )
        }
    };
    let sweep = Sweep {
        index,
        dir,
        truncate,
        depth: spec.depth,
        from,
        to,
        ranks: &ranks,
        top: spec.top as usize,
    };
    let (mut finals, total) = sweep_interval(&sweep, lo, hi);
    finals.retain(|group| group.count > 0);
    finals.sort_unstable_by(|a, b| index.selection_order(&ranks, dir, a, b));
    finals.truncate(spec.top as usize);
    let rows = finals
        .into_iter()
        .enumerate()
        .map(|(at, group)| {
            let steps: Vec<String> = index
                .window_codes(dir, &group)
                .into_iter()
                .map(|code| names[usize::try_from(code).expect("code fits usize")].clone())
                .collect();
            PathsRow {
                rank: u32::try_from(at).expect("rank fits u32") + 1,
                steps,
                occurrences: group.count,
                share: if total == 0 {
                    0.0
                } else {
                    group.count as f64 / total as f64
                },
            }
        })
        .collect();
    PathsResult { rows }
}

/// The half-open admission bounds of a time range (unbounded sides admit
/// everything on that side).
fn range_bounds(range: TimeRange) -> (i64, i64) {
    (
        range.from.unwrap_or(i64::MIN),
        range.to.unwrap_or(i64::MAX),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::events::PathAnchor;

    /// A deterministic linear-congruential sequence for stream synthesis.
    struct Lcg(u64);

    impl Lcg {
        /// The next value below `bound`.
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            (self.0 >> 33) % bound
        }
    }

    /// Synthesize collapsed actor streams: no consecutive duplicate codes,
    /// strictly increasing timestamps, varied lengths including empty.
    fn synth_streams(actors: usize, alphabet: u64, seed: u64) -> Vec<(Vec<u32>, Vec<i64>)> {
        let mut rng = Lcg(seed);
        let mut streams = Vec::new();
        for _ in 0..actors {
            let length = rng.next(120) as usize;
            let mut codes: Vec<u32> = Vec::new();
            let mut ts: Vec<i64> = Vec::new();
            let mut clock = 1_000 + rng.next(50) as i64;
            for _ in 0..length {
                let mut code = u32::try_from(rng.next(alphabet)).expect("code fits");
                if codes.last() == Some(&code) {
                    code = (code + 1) % u32::try_from(alphabet).expect("alphabet fits");
                }
                codes.push(code);
                ts.push(clock);
                clock += 1 + rng.next(40) as i64;
            }
            streams.push((codes, ts));
        }
        streams
    }

    /// Pack synthetic streams into shard form (three shards, round-robin).
    fn shard_streams(streams: &[(Vec<u32>, Vec<i64>)]) -> Vec<ShardStream> {
        let mut shards: Vec<ShardStream> = (0..3)
            .map(|_| ShardStream {
                codes: Vec::new(),
                ts: Vec::new(),
                bounds: vec![0],
            })
            .collect();
        for (at, (codes, ts)) in streams.iter().enumerate() {
            let shard = &mut shards[at % 3];
            shard.codes.extend_from_slice(codes);
            shard.ts.extend_from_slice(ts);
            shard
                .bounds
                .push(u32::try_from(shard.codes.len()).expect("stream fits u32"));
        }
        shards
    }

    /// The brute-force answer: enumerate every window per the anchor variant
    /// directly over the streams, count by code sequence, rank by
    /// (count desc, names asc), share against the in-range total.
    fn brute(
        streams: &[(Vec<u32>, Vec<i64>)],
        names: &[String],
        spec: &PathsSpec,
        anchor: Option<(u64, bool)>,
    ) -> PathsResult {
        let (from, to) = range_bounds(spec.range);
        let depth = spec.depth as usize;
        let mut counts: std::collections::BTreeMap<Vec<u32>, u64> = std::collections::BTreeMap::new();
        let mut total = 0u64;
        for (codes, ts) in streams {
            let n = codes.len();
            for at in 0..n {
                let window: Option<(Vec<u32>, i64)> = match anchor {
                    None => {
                        if at + depth <= n {
                            Some((codes[at..at + depth].to_vec(), ts[at]))
                        } else {
                            None
                        }
                    }
                    Some((code, true)) => {
                        let hi = (at + depth).min(n);
                        if u64::from(codes[at]) == code && hi - at >= 2 {
                            Some((codes[at..hi].to_vec(), ts[at]))
                        } else {
                            None
                        }
                    }
                    Some((code, false)) => {
                        let lo = (at + 1).saturating_sub(depth);
                        if u64::from(codes[at]) == code && at + 1 - lo >= 2 {
                            Some((codes[lo..=at].to_vec(), ts[at]))
                        } else {
                            None
                        }
                    }
                };
                if let Some((window, anchor_ts)) = window {
                    if anchor_ts >= from && anchor_ts < to {
                        *counts.entry(window).or_insert(0) += 1;
                        total += 1;
                    }
                }
            }
        }
        let mut ranked: Vec<(Vec<String>, u64)> = counts
            .into_iter()
            .map(|(window, count)| {
                let steps: Vec<String> = window
                    .iter()
                    .map(|code| names[*code as usize].clone())
                    .collect();
                (steps, count)
            })
            .collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        ranked.truncate(spec.top as usize);
        let rows = ranked
            .into_iter()
            .enumerate()
            .map(|(at, (steps, count))| PathsRow {
                rank: u32::try_from(at).expect("rank fits") + 1,
                steps,
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

    /// Assert two results agree row for row.
    fn assert_same(index_result: &PathsResult, brute_result: &PathsResult, what: &str) {
        assert_eq!(
            index_result.rows.len(),
            brute_result.rows.len(),
            "row count differs: {what}"
        );
        for (left, right) in index_result.rows.iter().zip(&brute_result.rows) {
            assert_eq!(left.rank, right.rank, "rank differs: {what}");
            assert_eq!(left.steps, right.steps, "steps differ: {what}");
            assert_eq!(left.occurrences, right.occurrences, "count differs: {what}");
            assert!(
                (left.share - right.share).abs() < 1e-12,
                "share differs: {what}"
            );
        }
    }

    /// A spec with the given shape (the dataset/anchor fields are unused by
    /// the sweep, which receives the resolved anchor directly).
    fn spec_of(depth: u32, top: u32, range: TimeRange) -> PathsSpec {
        PathsSpec {
            dataset: "t".to_string(),
            anchor: PathAnchor::Unanchored,
            depth,
            top,
            maxgap_micros: None,
            range,
            filter: None,
        }
    }

    /// Every variant, depth, and range against brute force, on both symbol
    /// widths and through a serialization round trip.
    #[test]
    fn sweeps_match_brute_force() {
        let names: Vec<String> = ["zeta", "alpha", "mid", "beta", "omega"]
            .iter()
            .map(|name| (*name).to_string())
            .collect();
        let alphabet = u32::try_from(names.len()).expect("alphabet fits");
        for seed in [7u64, 99, 1234] {
            let streams = synth_streams(40, u64::from(alphabet), seed);
            let shards = shard_streams(&streams);
            let built = assemble(&shards, alphabet).expect("assemble");
            let fwd = encode_fwd(&built, 42);
            let bwd = encode_bwd(&built, 42);
            let index = decode(&fwd, &bwd, "test.psax", 42).expect("decode");
            let ranges = [
                TimeRange::default(),
                TimeRange {
                    from: Some(1_100),
                    to: Some(1_400),
                },
                TimeRange {
                    from: Some(2_000),
                    to: None,
                },
            ];
            let mut anchors: Vec<Option<(u64, bool)>> = vec![None];
            for code in 0..u64::from(alphabet) {
                anchors.push(Some((code, true)));
                anchors.push(Some((code, false)));
            }
            for depth in [2u32, 3, 5, 7, 29, 50] {
                for range in ranges {
                    for anchor in &anchors {
                        for top in [3u32, 10, 1000] {
                            let spec = spec_of(depth, top, range);
                            let got = answer(&index, &names, &spec, *anchor);
                            let want = brute(&streams, &names, &spec, *anchor);
                            let what = format!(
                                "seed {seed} depth {depth} top {top} range {range:?} anchor {anchor:?}"
                            );
                            assert_same(&got, &want, &what);
                        }
                    }
                }
            }
        }
    }

    /// An unknown anchor code (allow_unknown_names) yields an empty result.
    #[test]
    fn unknown_anchor_is_empty() {
        let names = vec!["a".to_string(), "b".to_string()];
        let streams = synth_streams(5, 2, 3);
        let shards = shard_streams(&streams);
        let index = assemble(&shards, 2).expect("assemble");
        let spec = spec_of(3, 10, TimeRange::default());
        let result = answer(&index, &names, &spec, Some((u64::MAX, true)));
        assert!(result.rows.is_empty());
    }

    /// A dataset with no events yields an empty index and empty results.
    #[test]
    fn empty_dataset_is_empty() {
        let index = assemble(&[], 4).expect("assemble empty");
        let names = vec!["a".to_string(); 4];
        let spec = spec_of(2, 10, TimeRange::default());
        let result = answer(&index, &names, &spec, None);
        assert!(result.rows.is_empty());
    }
}
