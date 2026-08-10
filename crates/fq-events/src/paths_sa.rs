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
use crate::stitch;
use crate::store::{cached_paths_sa, CachedItem, StoreIo};
use crate::Dataset;

// The serialized arrays are raw little-endian; the sweep reads them back with
// plain memory copies, so a big-endian target cannot share store files.
#[cfg(target_endian = "big")]
compile_error!("the paths index stores little-endian arrays; big-endian is unsupported");

/// The file magic of a serialized paths index.
const MAGIC: &[u8; 4] = b"FQPS";

/// The serialization version this build writes and reads.
const VERSION: u8 = 5;

/// The fingerprint stamped into a SEGMENT file.
///
/// A segment survives refreshes untouched, so it cannot carry the dataset's
/// generation fingerprint - that changes under it. What binds segments to a
/// dataset state is the manifest, which does carry the fingerprint and names
/// exactly which segment ids compose the index.
const SEGMENT_STAMP: u64 = 0;

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
    /// Per-direction ownership bitmaps in SUFFIX-ARRAY order, present only on
    /// stitch segments (a refresh's addition or retraction). A sweep counts a
    /// window only at a position its direction owns; `None` owns everything -
    /// a base segment counts every window its text holds. Ownership is what
    /// makes overlapping segment texts sound: the same stream position may
    /// appear in several segments' texts, but exactly one segment's ownership
    /// (net of retractions) counts each window form.
    own_fwd: Option<Vec<u64>>,
    own_bwd: Option<Vec<u64>>,
    /// The window-length bound the segment's ownership was constructed for.
    /// A stitch segment's text carries only `max_depth` context around what it
    /// owns, so serving a deeper window would read into a neighboring region;
    /// callers must not sweep it deeper. 0 on base segments (any depth).
    max_depth: u32,
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
        let own = self.own_fwd.as_ref().map_or(0, |bits| bits.len() * 8)
            + self.own_bwd.as_ref().map_or(0, |bits| bits.len() * 8);
        n * (usize::from(self.text.width()) + 2 * (4 + 4 + 4 + 8)) + self.buckets.len() * 4 + own
    }

    /// The sweep arrays of a direction: (sa, lcp, avail, anchor ts), all in
    /// suffix-array order.
    fn arrays(&self, dir: Direction) -> (&[i32], &[i32], &[u32], &[i64]) {
        match dir {
            Direction::Forward => (&self.sa_fwd, &self.lcp_fwd, &self.avail_fwd, &self.ts_fwd),
            Direction::Backward => (&self.sa_bwd, &self.lcp_bwd, &self.avail_bwd, &self.ts_bwd),
        }
    }

    /// Whether the direction owns suffix-array position `at`: bitmap bit, or
    /// everything when the segment carries no bitmap.
    fn owns(&self, dir: Direction, at: usize) -> bool {
        let bits = match dir {
            Direction::Forward => &self.own_fwd,
            Direction::Backward => &self.own_bwd,
        };
        match bits {
            None => true,
            Some(words) => words[at / 64] & (1u64 << (at % 64)) != 0,
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

/// A segment's forward index file (text and forward arrays).
///
/// The name carries the SEGMENT id, not the dataset fingerprint: a segment is
/// immutable and survives a refresh untouched, and it is the manifest that
/// binds a set of segments to one dataset state. Naming segments by
/// fingerprint would mean rewriting every file whenever a generation is
/// appended, which is the cost this whole design exists to avoid.
fn fwd_rel(dataset: &Dataset, segment: u32) -> String {
    format!("{}/paths_f_{segment:08}.psax", dataset.record.location)
}

/// A segment's backward index file (backward arrays only); split from the
/// forward file so the two encode, write, and load on concurrent threads.
fn bwd_rel(dataset: &Dataset, segment: u32) -> String {
    format!("{}/paths_b_{segment:08}.psax", dataset.record.location)
}

/// The manifest's relative path: which segments compose the index, and which
/// actors each one covers.
///
/// It is written LAST, after every segment file, and read FIRST. That order is
/// what makes a partial write safe: without a manifest there is no index, so a
/// crash mid-write leaves a dataset that scans until the next rebuild rather
/// than one whose index silently covers only some of its actors.
fn manifest_rel(dataset: &Dataset) -> String {
    format!(
        "{}/paths_m_{:016x}.psax",
        dataset.record.location,
        fingerprint(dataset)
    )
}

/// One segment of the index: its immutable file id and its sign.
///
/// A `negative` segment is a refresh's RETRACTION: it re-counts, with the
/// PRE-REFRESH availability and timestamps, exactly the window forms that the
/// refresh invalidated in older segments, and the merge subtracts it. The
/// paired addition then counts the current forms. Ownership bitmaps inside
/// the segment files say which positions count; the manifest only carries
/// the sign.
struct SegmentEntry {
    id: u32,
    negative: bool,
}

/// The index's manifest: the segments composing it, and the next free file id.
///
/// The manifest is the only mutable part of the index. A refresh writes a new
/// segment file and a new manifest naming it alongside the existing ones; the
/// existing files are never touched.
struct IndexManifest {
    next_id: u32,
    segments: Vec<SegmentEntry>,
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

/// How one stored actor slice relates to the actor's TRUE stream: how many
/// stream events precede and follow the slice, and which slice positions each
/// direction owns (slice-relative half-open ranges). A whole-stream slice has
/// zero before/after and owns everything.
#[derive(Clone, Copy)]
struct SliceMeta {
    before: u32,
    after: u32,
    own_fwd: stitch::Span,
    own_bwd: stitch::Span,
}

/// One shard's duplicate-collapsed streams: `codes[bounds[i]..bounds[i + 1]]`
/// (and the aligned timestamps) is actor i's sequence.
struct ShardStream {
    codes: Vec<u32>,
    ts: Vec<i64>,
    bounds: Vec<u32>,
    /// The local id of each actor, in the order its stream appears - one
    /// entry per `bounds` interval. A returning actor keeps its local id, so
    /// `(shard, local)` identifies an actor across refreshes.
    actors: Vec<u64>,
    /// Per actor, aligned with `actors`: how the stored slice sits inside the
    /// actor's true stream. `None` when every slice IS the whole stream (a
    /// base segment).
    slices: Option<Vec<SliceMeta>>,
}

/// The actors a set of segment files holds, as `(shard, local)` keys.
///
/// Used on the newest generation's files to learn which actors a refresh
/// touched: the sidecar beside them lists only the actors that are NEW, and a
/// returning actor's stream changing is exactly what makes an old segment's
/// copy of it stale.
pub(crate) fn actors_in_files(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &fq_common::events::EventsConfig,
    files: &[(u32, String)],
) -> Result<std::collections::HashSet<(u32, u64)>, EventError> {
    let mut touched = std::collections::HashSet::new();
    for (shard, rel) in files {
        let seg = crate::store::SegFile::open(io, rel, dataset.kinds.clone(), cfg.cache_bytes)?;
        let directory = seg.directory(io)?;
        let crate::store::CachedItem::Dir(ref dir) = *directory else {
            unreachable!("an actor directory slot holds an actor directory");
        };
        for local in &dir.locals {
            touched.insert((*shard, *local));
        }
    }
    Ok(touched)
}

/// Read every shard's collapsed stream in one sharded dataset pass.
///
/// `keep` restricts the pass to a set of actors, which is what builds a
/// segment over just the actors a refresh touched. Their streams are read in
/// full, across every generation, because a segment answers only for actors
/// whose WHOLE stream it holds.
fn collect_streams(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &fq_common::events::EventsConfig,
    keep: Option<&std::collections::HashSet<(u32, u64)>>,
) -> Result<Vec<ShardStream>, EventError> {
    let mut streams = Vec::new();
    run_sharded(
        io,
        dataset,
        cfg,
        |scan| {
            let shard = scan.shard();
            let mut codes: Vec<u32> = Vec::new();
            let mut ts: Vec<i64> = Vec::new();
            let mut bounds: Vec<u32> = vec![0];
            let (locals, runs) = scan.actor_runs_with_locals();
            let mut actors: Vec<u64> = Vec::with_capacity(locals.len());
            for (local, run) in locals.into_iter().zip(runs) {
                if let Some(wanted) = keep {
                    if !wanted.contains(&(shard, local)) {
                        continue;
                    }
                }
                let start = codes.len();
                scan.for_each_event(&run, |file, row, code| {
                    if codes.len() == start || u64::from(codes[codes.len() - 1]) != code {
                        codes.push(u32::try_from(code).expect("dictionary code fits u32"));
                        ts.push(scan.files[file].ts.i64_at(row)?);
                    }
                    Ok(true)
                })?;
                bounds.push(u32::try_from(codes.len()).expect("shard stream fits u32"));
                actors.push(local);
            }
            Ok(ShardStream {
                codes,
                ts,
                bounds,
                actors,
                slices: None,
            })
        },
        |stream| {
            streams.push(stream);
            Ok(())
        },
    )?;
    Ok(streams)
}

/// The text-order per-position layout of a segment: symbols, anchor
/// timestamps, slice bounds, availability shifts beyond the stored slice, and
/// per-direction ownership flags (all empty when the segment owns everything).
struct TextLayout {
    text: Text,
    ts: Vec<i64>,
    start: Vec<u32>,
    end: Vec<u32>,
    after_of: Vec<u32>,
    before_of: Vec<u32>,
    own_fwd: Vec<u8>,
    own_bwd: Vec<u8>,
}

/// Lay out one actor's slice into the text, pushing its symbols, timestamps,
/// bounds, shifts, and ownership flags, then the trailing separator.
fn layout_actor(layout: &mut TextLayout, stream: &ShardStream, actor: usize, stitched: bool) {
    let lo = stream.bounds[actor] as usize;
    let hi = stream.bounds[actor + 1] as usize;
    let meta = stream.slices.as_ref().map(|slices| slices[actor]);
    let text_lo = u32::try_from(layout.ts.len()).expect("text fits u32");
    let text_hi = text_lo + u32::try_from(hi - lo).expect("stream fits u32");
    for offset in lo..hi {
        let rel = u32::try_from(offset - lo).expect("slice offset fits u32");
        match &mut layout.text {
            Text::Small(text) => {
                text.push(u8::try_from(stream.codes[offset] + 1).expect("code fits u8"));
            }
            Text::Wide(text) => {
                text.push(i32::try_from(stream.codes[offset] + 1).expect("code fits i32"));
            }
        }
        layout.ts.push(stream.ts[offset]);
        layout.start.push(text_lo);
        layout.end.push(text_hi);
        if stitched {
            let (before, after, fwd, bwd) = match meta {
                None => (0, 0, true, true),
                Some(meta) => (
                    meta.before,
                    meta.after,
                    meta.own_fwd.lo <= rel && rel < meta.own_fwd.hi,
                    meta.own_bwd.lo <= rel && rel < meta.own_bwd.hi,
                ),
            };
            layout.before_of.push(before);
            layout.after_of.push(after);
            layout.own_fwd.push(u8::from(fwd));
            layout.own_bwd.push(u8::from(bwd));
        }
    }
    // The separator after the stream: an out-of-alphabet symbol whose
    // start == end == its own position keeps every window off it. Its
    // shifts stay zero and it is never owned, so a slice's true-stream
    // extension can never lift a separator into validity.
    match &mut layout.text {
        Text::Small(text) => text.push(0),
        Text::Wide(text) => text.push(0),
    }
    layout.ts.push(0);
    layout.start.push(text_hi);
    layout.end.push(text_hi);
    if stitched {
        layout.before_of.push(0);
        layout.after_of.push(0);
        layout.own_fwd.push(0);
        layout.own_bwd.push(0);
    }
}

/// Check the stitch construction invariant for every owned position: a window
/// of length up to `min(max_depth, availability)` must lie inside the stored
/// slice, or the sweep would read a neighboring region's symbols as this
/// actor's. A violation is a construction bug, never data-dependent, and it
/// must stop the build rather than ship an index that answers wrongly.
fn check_stitch_bounds(layout: &TextLayout, max_depth: u32) {
    assert!(
        max_depth >= 2,
        "a stitched segment needs a window-length bound of at least 2"
    );
    for pos in 0..layout.ts.len() {
        let here = u32::try_from(pos).expect("position fits u32");
        let in_slice_fwd = layout.end[pos] - here;
        let in_slice_bwd = here - layout.start[pos] + 1;
        if layout.own_fwd[pos] != 0 {
            let avail = in_slice_fwd + layout.after_of[pos];
            assert!(
                max_depth.min(avail) <= in_slice_fwd,
                "stitch construction bug: forward-owned position lacks window text"
            );
        }
        if layout.own_bwd[pos] != 0 {
            let avail = in_slice_bwd + layout.before_of[pos];
            assert!(
                max_depth.min(avail) <= in_slice_bwd,
                "stitch construction bug: backward-owned position lacks window text"
            );
        }
    }
}

/// Scatter a text-order flag into a suffix-array-order bitmap.
fn sa_bitmap(sa: &[i32], flag: impl Fn(usize) -> bool) -> Vec<u64> {
    let mut words = vec![0u64; sa.len().div_ceil(64)];
    for (slot, suffix) in sa.iter().enumerate() {
        let pos = usize::try_from(*suffix).expect("suffix position fits usize");
        if flag(pos) {
            words[slot / 64] |= 1u64 << (slot % 64);
        }
    }
    words
}

/// Assemble a segment from collapsed streams: lay out the separator-joined
/// text with its per-position companions, suffix-sort the text and its
/// reversal concurrently, derive both LCP arrays and the first-symbol
/// buckets, and scatter availability, timestamps, and (for stitch segments)
/// ownership into suffix-array order.
///
/// `max_depth` is the window-length bound a stitch segment was constructed
/// for; a segment whose streams are all whole (no slices) passes 0 and owns
/// everything at any depth. Availability counts TRUE stream events - the
/// slice's span plus the events beyond its edges - so truncation semantics
/// hold even where the stored text is only a slice.
fn assemble(
    streams: &[ShardStream],
    alphabet: u32,
    max_depth: u32,
) -> Result<PathsSa, EventBuildError> {
    let events: usize = streams.iter().map(|stream| stream.codes.len()).sum();
    let actors: usize = streams.iter().map(|stream| stream.bounds.len() - 1).sum();
    let n = events + actors;
    if i32::try_from(n).is_err() {
        return Err(EventBuildError::SuffixTextTooLong { positions: n });
    }
    let small = alphabet <= 254;
    let stitched = streams.iter().any(|stream| stream.slices.is_some());
    let side = if stitched { n } else { 0 };
    let mut layout = TextLayout {
        text: if small {
            Text::Small(Vec::with_capacity(n))
        } else {
            Text::Wide(Vec::with_capacity(n))
        },
        ts: Vec::with_capacity(n),
        start: Vec::with_capacity(n),
        end: Vec::with_capacity(n),
        after_of: Vec::with_capacity(side),
        before_of: Vec::with_capacity(side),
        own_fwd: Vec::with_capacity(side),
        own_bwd: Vec::with_capacity(side),
    };
    for stream in streams {
        for actor in 0..stream.bounds.len() - 1 {
            layout_actor(&mut layout, stream, actor, stitched);
        }
    }
    if stitched {
        check_stitch_bounds(&layout, max_depth);
    }
    let (sa_fwd, lcp_fwd, sa_bwd, lcp_bwd) = sort_both_directions(&layout.text, alphabet)?;
    let buckets = bucket_offsets(&layout.text, alphabet);
    // Scatter availability and anchor timestamps into SUFFIX-ARRAY order, so
    // the sweep never takes a random read: forward availability runs to the
    // actor stream's TRUE end (slice span plus the shift); a backward suffix
    // (of the reversed text) maps onto its origin position, whose
    // availability runs back to the TRUE stream start.
    let last = layout.ts.len().saturating_sub(1);
    let shift_after = |pos: usize| {
        if layout.after_of.is_empty() {
            0
        } else {
            layout.after_of[pos]
        }
    };
    let shift_before = |pos: usize| {
        if layout.before_of.is_empty() {
            0
        } else {
            layout.before_of[pos]
        }
    };
    let (avail_fwd, ts_fwd) = sa_companions(&sa_fwd, |pos| {
        (
            layout.end[pos] - u32::try_from(pos).expect("position fits u32") + shift_after(pos),
            layout.ts[pos],
        )
    });
    let (avail_bwd, ts_bwd) = sa_companions(&sa_bwd, |pos| {
        let origin = last - pos;
        (
            u32::try_from(origin).expect("position fits u32") - layout.start[origin]
                + 1
                + shift_before(origin),
            layout.ts[origin],
        )
    });
    let (own_fwd, own_bwd) = if stitched {
        (
            Some(sa_bitmap(&sa_fwd, |pos| layout.own_fwd[pos] != 0)),
            Some(sa_bitmap(&sa_bwd, |pos| layout.own_bwd[last - pos] != 0)),
        )
    } else {
        (None, None)
    };
    Ok(PathsSa {
        alphabet,
        text: layout.text,
        sa_fwd,
        lcp_fwd,
        avail_fwd,
        ts_fwd,
        sa_bwd,
        lcp_bwd,
        avail_bwd,
        ts_bwd,
        buckets,
        own_fwd,
        own_bwd,
        max_depth: if stitched { max_depth } else { 0 },
    })
}

/// Gather one direction's (availability, anchor ts) companions in suffix-array
/// order, in parallel over output chunks.
fn sa_companions(sa: &[i32], shape: impl Fn(usize) -> (u32, i64) + Sync) -> (Vec<u32>, Vec<i64>) {
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

/// Append one actor's stitch slice to a pair stream: the sliced codes and
/// timestamps, the actor's local id, and the slice's relation to the true
/// stream (ownership converted to slice-relative coordinates).
///
/// `prefix` counts stream events BEFORE the collapsed slice this stitch was
/// computed over (a bounded tail read passes its saturation stand-in; a
/// whole-stream read passes 0). It shifts `before`, and through it the
/// backward availability.
fn push_slice(
    stream: &mut ShardStream,
    local: u64,
    collapsed: &stitch::CollapsedStream,
    slice: stitch::ActorStitch,
    prefix: u32,
) {
    let lo = slice.text.lo as usize;
    let hi = slice.text.hi as usize;
    stream.codes.extend_from_slice(&collapsed.codes[lo..hi]);
    stream.ts.extend_from_slice(&collapsed.ts[lo..hi]);
    stream
        .bounds
        .push(u32::try_from(stream.codes.len()).expect("stream fits u32"));
    stream.actors.push(local);
    let stream_len = u32::try_from(collapsed.codes.len()).expect("stream fits u32");
    let relative = |span: stitch::Span| stitch::Span {
        lo: span.lo - slice.text.lo,
        hi: span.hi - slice.text.lo,
    };
    stream
        .slices
        .as_mut()
        .expect("pair streams carry slices")
        .push(SliceMeta {
            before: slice.text.lo + prefix,
            after: stream_len - slice.text.hi,
            own_fwd: relative(slice.own_fwd),
            own_bwd: relative(slice.own_bwd),
        });
}

/// An empty pair stream, ready for `push_slice`.
fn empty_pair_stream() -> ShardStream {
    ShardStream {
        codes: Vec::new(),
        ts: Vec::new(),
        bounds: vec![0],
        actors: Vec::new(),
        slices: Some(Vec::new()),
    }
}

/// One contiguous stored range of an actor's events:
/// (file index, start row, end row).
type Piece = (usize, u64, u64);

/// One actor's stored event ranges, split into the pieces of the delta files
/// and the pieces of every older generation.
fn split_run(
    scan: &crate::cursor::ShardScan<'_>,
    run: &crate::cursor::ActorRun,
    delta_files: &std::collections::HashSet<String>,
) -> (Vec<Piece>, Vec<Piece>) {
    let pieces: Vec<Piece> = match run {
        crate::cursor::ActorRun::Single { file, start, end } => vec![(*file, *start, *end)],
        crate::cursor::ActorRun::Multi(pieces) => pieces.clone(),
    };
    let mut old = Vec::new();
    let mut delta = Vec::new();
    for piece in pieces {
        if delta_files.contains(scan.files[piece.0].seg.rel()) {
            delta.push(piece);
        } else {
            old.push(piece);
        }
    }
    (old, delta)
}

/// The `(ts, tiebreak)` of one stored row.
fn row_key(
    scan: &crate::cursor::ShardScan<'_>,
    file: usize,
    row: u64,
) -> Result<(i64, i64), EventStoreError> {
    let row = usize::try_from(row).expect("row fits usize");
    Ok((
        scan.files[file].ts.i64_at(row)?,
        scan.files[file].tiebreak.i64_at(row)?,
    ))
}

/// The largest `(ts, tiebreak)` across pieces (each piece is sorted, so its
/// last row is its largest), or None when the pieces hold no event.
fn last_key(
    scan: &crate::cursor::ShardScan<'_>,
    pieces: &[Piece],
) -> Result<Option<(i64, i64)>, EventStoreError> {
    let mut best: Option<(i64, i64)> = None;
    for (file, start, end) in pieces {
        if start < end {
            let key = row_key(scan, *file, end - 1)?;
            best = Some(best.map_or(key, |held| held.max(key)));
        }
    }
    Ok(best)
}

/// The smallest `(ts, tiebreak)` across pieces (first rows).
fn first_key(
    scan: &crate::cursor::ShardScan<'_>,
    pieces: &[Piece],
) -> Result<Option<(i64, i64)>, EventStoreError> {
    let mut best: Option<(i64, i64)> = None;
    for (file, start, end) in pieces {
        if start < end {
            let key = row_key(scan, *file, *start)?;
            best = Some(best.map_or(key, |held| held.min(key)));
        }
    }
    Ok(best)
}

/// Read every event of the given pieces in `(ts, tiebreak)` order.
fn read_events(
    scan: &crate::cursor::ShardScan<'_>,
    pieces: Vec<Piece>,
) -> Result<Vec<(u32, i64)>, EventStoreError> {
    let mut events = Vec::new();
    let run = crate::cursor::ActorRun::Multi(pieces);
    scan.for_each_event(&run, |file, row, code| {
        let when = scan.files[file].ts.i64_at(row)?;
        events.push((u32::try_from(code).expect("dictionary code fits u32"), when));
        Ok(true)
    })?;
    Ok(events)
}

/// Read the TAIL of the merged stream over `pieces`, backward, until it holds
/// `target_runs` collapsed runs or the stream start. Returns the tail in
/// ascending order and whether the stream start was reached.
///
/// The earliest returned run may be cut mid-run, which leaves its kept
/// timestamp later than the run's true first occurrence; callers only place
/// that position in unowned context, where a sweep reads its code and never
/// its timestamp.
fn read_tail(
    scan: &crate::cursor::ShardScan<'_>,
    pieces: &[Piece],
    target_runs: u32,
) -> Result<(Vec<(u32, i64)>, bool), EventStoreError> {
    // Cursor per piece, walking DOWN from the end: (file, next row + 1, start
    // bound, key of row next).
    let mut cursors: Vec<(usize, u64, u64, (i64, i64))> = Vec::new();
    for (file, start, end) in pieces {
        if start < end {
            cursors.push((*file, *end, *start, row_key(scan, *file, end - 1)?));
        }
    }
    let mut descending: Vec<(u32, i64)> = Vec::new();
    let mut runs = 0u32;
    let mut newer_code: Option<u32> = None;
    while !cursors.is_empty() {
        let mut best = 0usize;
        for candidate in 1..cursors.len() {
            if cursors[candidate].3 > cursors[best].3 {
                best = candidate;
            }
        }
        let (file, above, start, key) = cursors[best];
        let row = above - 1;
        let code = u32::try_from(
            scan.files[file]
                .event
                .code_at(usize::try_from(row).expect("row fits usize"))?,
        )
        .expect("dictionary code fits u32");
        if newer_code != Some(code) {
            if runs == target_runs {
                return Ok((reverse_tail(descending), false));
            }
            runs += 1;
            newer_code = Some(code);
        }
        descending.push((code, key.0));
        if row > start {
            cursors[best] = (file, row, start, row_key(scan, file, row - 1)?);
        } else {
            cursors.swap_remove(best);
        }
    }
    Ok((reverse_tail(descending), true))
}

/// Reverse a descending tail into ascending event order.
fn reverse_tail(mut descending: Vec<(u32, i64)>) -> Vec<(u32, i64)> {
    descending.reverse();
    descending
}

/// The stand-in for the unread events before a bounded tail: any value at
/// least `max_depth + 1` makes every backward availability in the slice
/// exceed every legal depth, which is indistinguishable from the true (large)
/// distance for validity and truncation alike - the two comparisons
/// availability feeds are both against depths bounded by `max_depth`.
fn saturated_prefix(max_depth: u32) -> u32 {
    max_depth + 1
}

/// Cut one touched actor's stitches and push them onto the pair streams.
///
/// The common case reads only the delta and a bounded tail of the old stream
/// (the last `2 * max_depth - 1` collapsed runs): a pure append's envelope
/// sits at the boundary, so everything before the tail is common prefix by
/// construction. An actor whose delta interleaves with stored events (a late
/// arrival) falls back to reading its whole stream - the cost is that one
/// actor's history, never the dataset.
fn stitch_actor(
    scan: &crate::cursor::ShardScan<'_>,
    run: &crate::cursor::ActorRun,
    delta_files: &std::collections::HashSet<String>,
    max_depth: u32,
    local: u64,
    addition: &mut ShardStream,
    retraction: &mut ShardStream,
) -> Result<(), EventStoreError> {
    let (old_pieces, delta_pieces) = split_run(scan, run, delta_files);
    let old_last = last_key(scan, &old_pieces)?;
    let delta_first = first_key(scan, &delta_pieces)?;
    let pure_append = match (old_last, delta_first) {
        (Some(last), Some(first)) => last < first,
        _ => true,
    };
    let (old_events, prefix) = if pure_append {
        let (tail, reached_start) = read_tail(scan, &old_pieces, 2 * max_depth - 1)?;
        let prefix = if reached_start {
            0
        } else {
            saturated_prefix(max_depth)
        };
        (tail, prefix)
    } else {
        (read_events(scan, old_pieces.clone())?, 0)
    };
    let merged_events = if pure_append {
        let mut events = old_events.clone();
        events.extend(read_events(scan, delta_pieces)?);
        events
    } else {
        let mut all_pieces = old_pieces;
        all_pieces.extend(delta_pieces);
        read_events(scan, all_pieces)?
    };
    let merged = stitch::collapse(&merged_events);
    let old = stitch::collapse(&old_events);
    let envelope = stitch::envelope(&old, &merged);
    let merged_len = u32::try_from(merged.codes.len()).expect("stream fits u32");
    let old_len = u32::try_from(old.codes.len()).expect("stream fits u32");
    if let Some(slice) = stitch::addition_stitch(merged_len, envelope, max_depth) {
        push_slice(addition, local, &merged, slice, prefix);
    }
    if let Some(slice) = stitch::retraction_stitch(old_len, envelope, max_depth) {
        push_slice(retraction, local, &old, slice, prefix);
    }
    Ok(())
}

/// Read the touched actors' changed regions and cut both halves of a
/// refresh's segment pair. Cost tracks the delta plus a bounded tail per
/// touched actor (`stitch_actor`), never the touched actors' histories.
fn collect_pair_streams(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &fq_common::events::EventsConfig,
    touched: &std::collections::HashSet<(u32, u64)>,
    delta_files: &std::collections::HashSet<String>,
    max_depth: u32,
) -> Result<(Vec<ShardStream>, Vec<ShardStream>), EventError> {
    let mut additions = Vec::new();
    let mut retractions = Vec::new();
    run_sharded(
        io,
        dataset,
        cfg,
        |scan| {
            let shard = scan.shard();
            let mut addition = empty_pair_stream();
            let mut retraction = empty_pair_stream();
            let (locals, runs) = scan.actor_runs_with_locals();
            for (local, run) in locals.into_iter().zip(runs) {
                if !touched.contains(&(shard, local)) {
                    continue;
                }
                stitch_actor(
                    scan,
                    &run,
                    delta_files,
                    max_depth,
                    local,
                    &mut addition,
                    &mut retraction,
                )?;
            }
            Ok((addition, retraction))
        },
        |(addition, retraction)| {
            additions.push(addition);
            retractions.push(retraction);
            Ok(())
        },
    )?;
    Ok((additions, retractions))
}

/// Whether any of a pair's shard streams holds an actor at all.
fn pair_has_streams(streams: &[ShardStream]) -> bool {
    streams.iter().any(|stream| !stream.actors.is_empty())
}

/// The suffix-text positions a pair would occupy: events plus one separator
/// per actor slice, both halves.
fn pair_positions(halves: [&[ShardStream]; 2]) -> usize {
    let mut positions = 0usize;
    for streams in halves {
        for stream in streams {
            positions += stream.codes.len() + stream.actors.len();
        }
    }
    positions
}

/// Extend the index for a refresh with one segment PAIR - an addition owning
/// the changed window forms and a retraction cancelling the stale forms older
/// segments still count - leaving every existing segment file untouched.
///
/// Returns `None` when the caller should rebuild instead: there is no stored
/// manifest to extend (never built, or a version older than this format), or
/// the collected pair's positions reach half the dataset's - the crossover
/// where one sort over the whole dataset is cheaper than one over the pair.
/// Below the crossover the pair's cost is proportional to the delta plus the
/// touched actors' changed envelopes with `max_depth` context - never to
/// their whole histories.
pub(crate) fn append_pair(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &fq_common::events::EventsConfig,
    touched: &std::collections::HashSet<(u32, u64)>,
    delta_files: &[(u32, String)],
    max_depth: u32,
) -> Result<Option<String>, EventError> {
    let Some((manifest_path, manifest)) = read_stored_manifest(io, dataset)? else {
        return Ok(None);
    };
    let started = std::time::Instant::now();
    let mut delta_rels = std::collections::HashSet::new();
    for (_, rel) in delta_files {
        delta_rels.insert(rel.clone());
    }
    let (additions, retractions) =
        collect_pair_streams(io, dataset, cfg, touched, &delta_rels, max_depth)?;
    let collected = started.elapsed().as_secs_f64();
    // The pair costs one suffix sort over its positions; a whole rebuild
    // costs one over the dataset's. A delta touching most actors carries a
    // fixed tail-and-retraction context per actor that can exceed the
    // dataset itself, at which point the rebuild is the CHEAPER way to make
    // the index current - so the choice is by measured positions, not by
    // delta size. Half the dataset's positions is the crossover, counting
    // the pair's own separators against the dataset's events + actors.
    let dataset_positions =
        usize::try_from(dataset.record.measured_events + dataset.record.measured_actors)
            .expect("dataset positions fit usize");
    if pair_positions([&additions, &retractions]) * 2 >= dataset_positions {
        return Ok(None);
    }
    let alphabet = u32::try_from(dataset.event_dict.values.len()).expect("dictionary fits u32");
    let mut segments = manifest.segments;
    let mut next_id = manifest.next_id;
    let mut positions = 0usize;
    let mut written = 0usize;
    for (streams, negative) in [(&additions, false), (&retractions, true)] {
        if !pair_has_streams(streams) {
            continue;
        }
        let index = assemble(streams, alphabet, max_depth)?;
        positions += index.text.len();
        written += write_segment(io, dataset, &index, next_id, SEGMENT_STAMP)?;
        segments.push(SegmentEntry {
            id: next_id,
            negative,
        });
        next_id += 1;
    }
    let next = IndexManifest { next_id, segments };
    let stamp = fingerprint(dataset);
    let next_path = manifest_rel(dataset);
    io.put_verified(&next_path, encode_manifest(&next, stamp))?;
    if manifest_path != next_path {
        io.delete(&manifest_path)?;
    }
    let total = started.elapsed().as_secs_f64();
    Ok(Some(format!(
        "paths index: +pair, {positions} positions, {written} bytes, {total:.3}s \
         (collect {collected:.3}s, stitch {:.3}s)",
        total - collected
    )))
}

pub(crate) fn rebuild(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &fq_common::events::EventsConfig,
) -> Result<String, EventError> {
    for stale in location_files_with_prefix(io, &dataset.record.location, "paths_")? {
        io.delete(&stale)?;
    }
    let started = std::time::Instant::now();
    let streams = collect_streams(io, dataset, cfg, None)?;
    let collected = started.elapsed().as_secs_f64();
    let alphabet = u32::try_from(dataset.event_dict.values.len()).expect("dictionary fits u32");
    let index = assemble(&streams, alphabet, 0)?;
    drop(streams);
    let sorted = started.elapsed().as_secs_f64();
    let positions = index.text.len();
    let stamp = fingerprint(dataset);
    let written = write_segment(io, dataset, &index, 0, SEGMENT_STAMP)?;
    // The manifest goes last: until it exists there is no index, so a crash
    // between the segment files and here leaves a dataset that scans.
    let manifest = IndexManifest {
        next_id: 1,
        segments: vec![SegmentEntry {
            id: 0,
            negative: false,
        }],
    };
    io.put_verified(&manifest_rel(dataset), encode_manifest(&manifest, stamp))?;
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

/// Encode and write one segment's two files, returning the bytes written.
/// The halves encode and write concurrently; each is roughly half the bytes.
fn write_segment(
    io: &StoreIo,
    dataset: &Dataset,
    index: &PathsSa,
    segment: u32,
    stamp: u64,
) -> Result<usize, EventStoreError> {
    std::thread::scope(|scope| {
        let fwd = scope.spawn(|| {
            let bytes = encode_fwd(index, stamp);
            let len = bytes.len();
            io.put_verified(&fwd_rel(dataset, segment), bytes)
                .map(|()| len)
        });
        let bwd = scope.spawn(|| {
            let bytes = encode_bwd(index, stamp);
            let len = bytes.len();
            io.put_verified(&bwd_rel(dataset, segment), bytes)
                .map(|()| len)
        });
        let fwd_len = fwd.join().expect("forward write thread panicked")?;
        let bwd_len = bwd.join().expect("backward write thread panicked")?;
        Ok(fwd_len + bwd_len)
    })
}

/// Serialize the manifest: magic, version, fingerprint, then per segment its
/// id and sign.
fn encode_manifest(manifest: &IndexManifest, fingerprint: u64) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&fingerprint.to_le_bytes());
    out.extend_from_slice(&manifest.next_id.to_le_bytes());
    out.extend_from_slice(&(manifest.segments.len() as u64).to_le_bytes());
    for segment in &manifest.segments {
        out.extend_from_slice(&segment.id.to_le_bytes());
        out.push(u8::from(segment.negative));
    }
    out
}

/// Read a manifest back, raising rather than guessing on anything unexpected:
/// a manifest that cannot be trusted must not produce a partial index.
fn decode_manifest(
    bytes: &[u8],
    rel: &str,
    expected: u64,
) -> Result<IndexManifest, EventStoreError> {
    let corrupt = |detail: String| EventStoreError::Corrupt {
        file: rel.to_string(),
        detail,
    };
    let mut at = 0usize;
    let mut take = |count: usize| -> Result<&[u8], EventStoreError> {
        let end = at + count;
        if end > bytes.len() {
            return Err(EventStoreError::Corrupt {
                file: rel.to_string(),
                detail: "paths manifest ends early".to_string(),
            });
        }
        let slice = &bytes[at..end];
        at = end;
        Ok(slice)
    };
    if take(4)? != MAGIC {
        return Err(corrupt("paths manifest magic".to_string()));
    }
    let version = take(1)?[0];
    if version != VERSION {
        return Err(corrupt(format!("paths manifest version {version}")));
    }
    let stamp = u64::from_le_bytes(take(8)?.try_into().expect("eight bytes"));
    if stamp != expected {
        return Err(corrupt("paths manifest fingerprint".to_string()));
    }
    let next_id = u32::from_le_bytes(take(4)?.try_into().expect("four bytes"));
    let count = u64::from_le_bytes(take(8)?.try_into().expect("eight bytes"));
    let mut segments = Vec::new();
    for _ in 0..count {
        let id = u32::from_le_bytes(take(4)?.try_into().expect("four bytes"));
        let sign = take(1)?[0];
        if sign > 1 {
            return Err(corrupt(format!("paths manifest segment sign {sign}")));
        }
        segments.push(SegmentEntry {
            id,
            negative: sign == 1,
        });
    }
    Ok(IndexManifest { next_id, segments })
}

/// The manifest that is ON DISK, whatever dataset state it was written for,
/// with the path it was read from.
///
/// The query path addresses the manifest by the CURRENT fingerprint, so a
/// stale one is invisible and the dataset scans. The append path cannot: by
/// the time it runs the record has already advanced to the new generation, and
/// the manifest it must extend is the one written for the previous state. More
/// than one manifest means a previous write was interrupted between publishing
/// and cleaning up, and appending to either would be a guess - so it raises.
fn read_stored_manifest(
    io: &StoreIo,
    dataset: &Dataset,
) -> Result<Option<(String, IndexManifest)>, EventError> {
    let mut found = location_files_with_prefix(io, &dataset.record.location, "paths_m_")?;
    if found.is_empty() {
        return Ok(None);
    }
    if found.len() > 1 {
        found.sort();
        return Err(EventStoreError::Corrupt {
            file: found.join(", "),
            detail: "several paths manifests in one dataset location".to_string(),
        }
        .into());
    }
    let path = found.remove(0);
    let bytes = io.get(&path)?;
    if bytes.len() < 13 {
        return Err(EventStoreError::Corrupt {
            file: path,
            detail: "paths manifest ends early".to_string(),
        }
        .into());
    }
    let stamp = u64::from_le_bytes(bytes[5..13].try_into().expect("eight bytes"));
    let manifest = decode_manifest(&bytes, &path, stamp)?;
    Ok(Some((path, manifest)))
}

/// The little-endian byte view of a plain numeric slice.
fn raw_bytes<T>(values: &[T]) -> &[u8] {
    // A plain numeric slice is always viewable as its raw bytes.
    unsafe {
        std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values))
    }
}

/// The shared file header: magic, version, fingerprint, position count,
/// alphabet, symbol width, the stitch window-length bound (0 on a base
/// segment), and whether the file carries an ownership bitmap.
fn encode_header(index: &PathsSa, fingerprint: u64, capacity: usize, has_own: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&fingerprint.to_le_bytes());
    out.extend_from_slice(&(index.text.len() as u64).to_le_bytes());
    out.extend_from_slice(&index.alphabet.to_le_bytes());
    out.push(index.text.width());
    out.extend_from_slice(&index.max_depth.to_le_bytes());
    out.push(u8::from(has_own));
    out
}

/// Encode the forward file: header, text, the forward arrays, then the
/// forward ownership bitmap when the segment is a stitch.
fn encode_fwd(index: &PathsSa, fingerprint: u64) -> Vec<u8> {
    let n = index.text.len();
    let width = usize::from(index.text.width());
    let mut out = encode_header(
        index,
        fingerprint,
        40 + n * (width + 20),
        index.own_fwd.is_some(),
    );
    match &index.text {
        Text::Small(text) => out.extend_from_slice(text),
        Text::Wide(text) => out.extend_from_slice(raw_bytes(text)),
    }
    out.extend_from_slice(raw_bytes(&index.sa_fwd));
    out.extend_from_slice(raw_bytes(&index.lcp_fwd));
    out.extend_from_slice(raw_bytes(&index.avail_fwd));
    out.extend_from_slice(raw_bytes(&index.ts_fwd));
    if let Some(words) = &index.own_fwd {
        out.extend_from_slice(raw_bytes(words));
    }
    out
}

/// Encode the backward file: header, the backward arrays, then the backward
/// ownership bitmap when the segment is a stitch.
fn encode_bwd(index: &PathsSa, fingerprint: u64) -> Vec<u8> {
    let n = index.text.len();
    let mut out = encode_header(index, fingerprint, 40 + n * 20, index.own_bwd.is_some());
    out.extend_from_slice(raw_bytes(&index.sa_bwd));
    out.extend_from_slice(raw_bytes(&index.lcp_bwd));
    out.extend_from_slice(raw_bytes(&index.avail_bwd));
    out.extend_from_slice(raw_bytes(&index.ts_bwd));
    if let Some(words) = &index.own_bwd {
        out.extend_from_slice(raw_bytes(words));
    }
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

/// One parsed file header: the position count, alphabet, symbol width, the
/// stitch window-length bound, and whether an ownership bitmap follows the
/// arrays.
struct FileHeader {
    n: usize,
    alphabet: u32,
    width: u8,
    max_depth: u32,
    has_own: bool,
}

/// Parse and verify one file's header, returning its section cursor and the
/// parsed header.
fn open_sections<'a>(
    bytes: &'a [u8],
    rel: &'a str,
    expected: u64,
) -> Result<(Sections<'a>, FileHeader), EventStoreError> {
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
        return Err(corrupt(format!(
            "paths index version {version}, expected {VERSION}"
        )));
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
    let max_depth = u32::from_le_bytes(sections.take(4)?.try_into().expect("4 bytes"));
    let own_flag = sections.take(1)?[0];
    if own_flag > 1 {
        return Err(corrupt(format!("paths index ownership flag {own_flag}")));
    }
    Ok((
        sections,
        FileHeader {
            n,
            alphabet,
            width,
            max_depth,
            has_own: own_flag == 1,
        },
    ))
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
    let (mut sections, header) = open_sections(fwd_bytes, rel, expected)?;
    let (mut sections_b, header_b) = open_sections(bwd_bytes, rel, expected)?;
    if header.n != header_b.n
        || header.alphabet != header_b.alphabet
        || header.width != header_b.width
        || header.max_depth != header_b.max_depth
        || header.has_own != header_b.has_own
    {
        return Err(corrupt(
            "paths index halves disagree on their header".to_string(),
        ));
    }
    let (n, alphabet, width) = (header.n, header.alphabet, header.width);
    // Every section's bytes are sliced up front, then all nine copy into
    // their typed vectors on concurrent threads - the copies dominate a cold
    // load, and they are independent.
    let text_bytes = sections.take(n * usize::from(width))?;
    let sa_fwd_bytes = sections.take(n * 4)?;
    let lcp_fwd_bytes = sections.take(n * 4)?;
    let avail_fwd_bytes = sections.take(n * 4)?;
    let ts_fwd_bytes = sections.take(n * 8)?;
    let own_fwd_bytes = if header.has_own {
        Some(sections.take(n.div_ceil(64) * 8)?)
    } else {
        None
    };
    let sa_bwd_bytes = sections_b.take(n * 4)?;
    let lcp_bwd_bytes = sections_b.take(n * 4)?;
    let avail_bwd_bytes = sections_b.take(n * 4)?;
    let ts_bwd_bytes = sections_b.take(n * 8)?;
    let own_bwd_bytes = if header.has_own {
        Some(sections_b.take(n.div_ceil(64) * 8)?)
    } else {
        None
    };
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
        own_fwd: own_fwd_bytes.map(copy_typed::<u64>),
        own_bwd: own_bwd_bytes.map(copy_typed::<u64>),
        max_depth: header.max_depth,
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
    load_segments(io, dataset, cfg)?;
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
    if i64::from(spec.depth) > dataset.record.max_paths_depth {
        // Stitch segments only carry ownership and context for windows up to
        // the dataset's bound, so the index cannot answer deeper sweeps; the
        // scan does (run_paths raises first unless the spec forces the scan).
        return Ok(None);
    }
    let Some(held) = load_segments(io, dataset, cfg)? else {
        return Ok(None);
    };
    let mut indexes: Vec<(&PathsSa, bool)> = Vec::with_capacity(held.len());
    for (item, negative) in &held {
        let CachedItem::PathsSa(ref index) = **item else {
            unreachable!("paths index cache slot holds a paths index");
        };
        indexes.push((index, *negative));
    }
    Ok(Some(answer_segments(
        &indexes,
        &dataset.event_dict.values,
        spec,
        anchor,
    )))
}

/// One loaded segment held out of the cache, with its manifest sign.
type HeldSegment = (std::sync::Arc<CachedItem>, bool);

/// Load every segment the manifest names, or `None` when the dataset has no
/// current index at all.
///
/// A manifest naming a segment whose files are missing RAISES rather than
/// answering from the segments that happen to be there: a partial index
/// undercounts, and an undercount looks exactly like a real answer.
fn load_segments(
    io: &StoreIo,
    dataset: &Dataset,
    cfg: &fq_common::events::EventsConfig,
) -> Result<Option<Vec<HeldSegment>>, EventError> {
    let manifest_path = manifest_rel(dataset);
    if !io.exists(&manifest_path)? {
        return Ok(None);
    }
    let expected = fingerprint(dataset);
    let manifest = decode_manifest(&io.get(&manifest_path)?, &manifest_path, expected)?;
    let mut held = Vec::with_capacity(manifest.segments.len());
    for segment in &manifest.segments {
        let rel = fwd_rel(dataset, segment.id);
        let rel_b = bwd_rel(dataset, segment.id);
        let item = cached_paths_sa(io, &rel, cfg.cache_bytes, || {
            // Both halves read concurrently; each is roughly half the bytes.
            let (fwd_bytes, bwd_bytes) = std::thread::scope(|scope| {
                let bwd = scope.spawn(|| io.get(&rel_b));
                (
                    io.get(&rel),
                    bwd.join().expect("backward read thread panicked"),
                )
            });
            Ok(CachedItem::PathsSa(Box::new(decode(
                &fwd_bytes?,
                &bwd_bytes?,
                &rel,
                SEGMENT_STAMP,
            )?)))
        })?;
        held.push((item, segment.negative));
    }
    Ok(Some(held))
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
        // A segmented sweep keeps every distinct window it meets. Pruning to
        // `top` inside one segment would be wrong there: a window outside
        // every segment's best `top` can still lead once the segments are
        // summed, and its count in a segment that dropped it is unrecoverable.
        if self.sweep.top == usize::MAX {
            self.groups.push(group);
            return;
        }
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
    let close = |group: Group,
                 head: &mut Option<Group>,
                 interior: &mut TopSet<'_, '_>,
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
        if !valid || !sweep.index.owns(sweep.dir, at) {
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

/// The array, direction and interval one variant sweeps in one segment, or
/// `None` when the segment cannot hold the anchor at all - an event name it
/// never saw, which is normal for a segment built before that name appeared.
fn sweep_target(
    index: &PathsSa,
    anchor: Option<(u64, bool)>,
) -> Option<(Direction, bool, usize, usize)> {
    match anchor {
        None => Some((
            Direction::Forward,
            false,
            index.buckets[1] as usize,
            index.text.len(),
        )),
        Some((code, forward)) => {
            if code >= u64::from(index.alphabet) {
                return None;
            }
            let symbol = usize::try_from(code + 1).expect("symbol fits usize");
            let dir = if forward {
                Direction::Forward
            } else {
                Direction::Backward
            };
            Some((
                dir,
                true,
                index.buckets[symbol] as usize,
                index.buckets[symbol + 1] as usize,
            ))
        }
    }
}

/// Selection order over merged windows: count descending, then elementwise
/// event-name rank ascending, then the shorter window first. The same order
/// `selection_order` imposes on one segment's groups, expressed over the
/// codes themselves because a merged window belongs to no single segment.
fn merged_order(ranks: &[u32], a: &(Vec<u64>, u64), b: &(Vec<u64>, u64)) -> Ordering {
    b.1.cmp(&a.1).then_with(|| {
        let shared = a.0.len().min(b.0.len());
        for offset in 0..shared {
            let left = ranks[a.0[offset] as usize + 1];
            let right = ranks[b.0[offset] as usize + 1];
            match left.cmp(&right) {
                Ordering::Equal => {}
                unequal => return unequal,
            }
        }
        a.0.len().cmp(&b.0.len())
    })
}

/// Answer one spec from several SIGNED index segments.
///
/// Every window instance of the dataset is owned by exactly one positive
/// segment net of retractions: base segments own everything they hold, a
/// refresh's addition owns the window forms its envelope changed or created,
/// and its retraction re-counts the stale forms older segments still hold so
/// subtracting it cancels them. Counts and in-range totals therefore sum by
/// window CONTENT across segments, signed. A net-negative window or total is
/// arithmetically impossible when the invariant holds, so it stops the query
/// rather than shipping an answer built on a broken index.
pub(crate) fn answer_segments(
    indexes: &[(&PathsSa, bool)],
    names: &[String],
    spec: &PathsSpec,
    anchor: Option<(u64, bool)>,
) -> PathsResult {
    if let [(only, false)] = indexes {
        if only.own_fwd.is_none() {
            return answer(only, names, spec, anchor);
        }
    }
    let (from, to) = range_bounds(spec.range);
    let mut alphabet = 0;
    for (index, _) in indexes {
        alphabet = alphabet.max(index.alphabet);
    }
    let ranks = name_ranks(names, alphabet);
    let mut merged: std::collections::HashMap<Vec<u64>, i64> = std::collections::HashMap::new();
    let mut total = 0i64;
    for (index, negative) in indexes {
        let Some((dir, truncate, lo, hi)) = sweep_target(index, anchor) else {
            continue;
        };
        let sweep = Sweep {
            index,
            dir,
            truncate,
            depth: spec.depth,
            from,
            to,
            ranks: &ranks,
            // Keep every window: see TopSet::offer for why a segment must not
            // prune to `top`.
            top: usize::MAX,
        };
        let (finals, segment_total) = sweep_interval(&sweep, lo, hi);
        let sign: i64 = if *negative { -1 } else { 1 };
        total += sign * i64::try_from(segment_total).expect("window total fits i64");
        for group in finals {
            if group.count == 0 {
                continue;
            }
            let count = sign * i64::try_from(group.count).expect("window count fits i64");
            *merged.entry(index.window_codes(dir, &group)).or_insert(0) += count;
        }
    }
    assert!(
        total >= 0,
        "paths index invariant broken: net in-range window total {total} is negative"
    );
    let mut ordered: Vec<(Vec<u64>, u64)> = Vec::with_capacity(merged.len());
    for (codes, count) in merged {
        assert!(
            count >= 0,
            "paths index invariant broken: window {codes:?} has net count {count}"
        );
        if count > 0 {
            ordered.push((codes, u64::try_from(count).expect("checked non-negative")));
        }
    }
    ordered.sort_unstable_by(|a, b| merged_order(&ranks, a, b));
    ordered.truncate(spec.top as usize);
    let total = u64::try_from(total).expect("checked non-negative");
    let rows = ordered
        .into_iter()
        .enumerate()
        .map(|(at, (codes, count))| PathsRow {
            rank: u32::try_from(at).expect("rank fits u32") + 1,
            steps: codes
                .iter()
                .map(|code| names[usize::try_from(*code).expect("code fits usize")].clone())
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
    let Some((dir, truncate, lo, hi)) = sweep_target(index, anchor) else {
        return PathsResult { rows: Vec::new() };
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
    (range.from.unwrap_or(i64::MIN), range.to.unwrap_or(i64::MAX))
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
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
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
            let mut clock = 1_000 + i64::try_from(rng.next(50)).expect("start offset fits");
            for _ in 0..length {
                let mut code = u32::try_from(rng.next(alphabet)).expect("code fits");
                if codes.last() == Some(&code) {
                    code = (code + 1) % u32::try_from(alphabet).expect("alphabet fits");
                }
                codes.push(code);
                ts.push(clock);
                clock += 1 + i64::try_from(rng.next(40)).expect("step fits");
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
                actors: Vec::new(),
                slices: None,
            })
            .collect();
        for (at, (codes, ts)) in streams.iter().enumerate() {
            let which = at % 3;
            let shard = &mut shards[which];
            shard.codes.extend_from_slice(codes);
            shard.ts.extend_from_slice(ts);
            shard
                .bounds
                .push(u32::try_from(shard.codes.len()).expect("stream fits u32"));
            shard.actors.push(at as u64);
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
        let mut counts: std::collections::BTreeMap<Vec<u32>, u64> =
            std::collections::BTreeMap::new();
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
            force_scan: false,
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
            let built = assemble(&shards, alphabet, 0).expect("assemble");
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

    /// Split the actors round-robin across `segments` indexes, each holding
    /// whole streams - the partition a refresh produces, without the refresh.
    fn segment_indexes(
        streams: &[(Vec<u32>, Vec<i64>)],
        alphabet: u32,
        segments: usize,
    ) -> Vec<PathsSa> {
        let mut groups: Vec<Vec<(Vec<u32>, Vec<i64>)>> = vec![Vec::new(); segments];
        for (at, stream) in streams.iter().enumerate() {
            groups[at % segments].push(stream.clone());
        }
        groups
            .iter()
            .map(|group| assemble(&shard_streams(group), alphabet, 0).expect("assemble"))
            .collect()
    }

    /// Several segments answer exactly as one index does. The interesting
    /// cases are the small `top` values: a window that no single segment
    /// ranks highly can still lead once the segments are summed, so this
    /// fails the moment a segment is allowed to prune to `top` on its own.
    #[test]
    fn segmented_sweeps_match_brute_force() {
        let names: Vec<String> = ["zeta", "alpha", "mid", "beta", "omega"]
            .iter()
            .map(|name| (*name).to_string())
            .collect();
        let alphabet = u32::try_from(names.len()).expect("alphabet fits");
        for seed in [7u64, 99, 1234] {
            let streams = synth_streams(40, u64::from(alphabet), seed);
            let ranges = [
                TimeRange::default(),
                TimeRange {
                    from: Some(1_100),
                    to: Some(1_400),
                },
            ];
            let mut anchors: Vec<Option<(u64, bool)>> = vec![None];
            for code in 0..u64::from(alphabet) {
                anchors.push(Some((code, true)));
                anchors.push(Some((code, false)));
            }
            for segments in [2usize, 3, 5, 17] {
                let indexes = segment_indexes(&streams, alphabet, segments);
                for depth in [2u32, 3, 5, 7, 29] {
                    for range in ranges {
                        for anchor in &anchors {
                            for top in [1u32, 3, 10, 1000] {
                                let spec = spec_of(depth, top, range);
                                let refs: Vec<(&PathsSa, bool)> =
                                    indexes.iter().map(|index| (index, false)).collect();
                                let got = answer_segments(&refs, &names, &spec, *anchor);
                                let want = brute(&streams, &names, &spec, *anchor);
                                let what = format!(
                                    "seed {seed} segments {segments} depth {depth} top {top} \
                                     range {range:?} anchor {anchor:?}"
                                );
                                assert_same(&got, &want, &what);
                            }
                        }
                    }
                }
            }
        }
    }

    /// A segment that never saw the anchor's event contributes nothing rather
    /// than emptying the answer - the case a segment built before an event
    /// name first appeared lands in.
    #[test]
    fn segments_with_a_narrower_alphabet_contribute_nothing() {
        let names: Vec<String> = ["a", "b", "c"].iter().map(|n| (*n).to_string()).collect();
        let streams = synth_streams(12, 3, 5);
        let narrow: Vec<(Vec<u32>, Vec<i64>)> = streams
            .iter()
            .map(|(codes, ts)| {
                let kept: Vec<usize> = (0..codes.len()).filter(|at| codes[*at] < 2).collect();
                let mut out_codes = Vec::new();
                let mut out_ts = Vec::new();
                for at in kept {
                    if out_codes.last() == Some(&codes[at]) {
                        continue;
                    }
                    out_codes.push(codes[at]);
                    out_ts.push(ts[at]);
                }
                (out_codes, out_ts)
            })
            .collect();
        let wide = assemble(&shard_streams(&streams), 3, 0).expect("assemble wide");
        let narrow_index = assemble(&shard_streams(&narrow), 2, 0).expect("assemble narrow");
        let spec = spec_of(2, 10, TimeRange::default());
        let both = answer_segments(
            &[(&wide, false), (&narrow_index, false)],
            &names,
            &spec,
            Some((2, true)),
        );
        let alone = {
            let wide_again = assemble(&shard_streams(&streams), 3, 0).expect("assemble wide");
            answer_segments(&[(&wide_again, false)], &names, &spec, Some((2, true)))
        };
        assert_eq!(
            both.rows.len(),
            alone.rows.len(),
            "narrow segment changed the row count"
        );
        for (left, right) in both.rows.iter().zip(&alone.rows) {
            assert_eq!(left.steps, right.steps);
            assert_eq!(left.occurrences, right.occurrences);
        }
    }

    /// An unknown anchor code (allow_unknown_names) yields an empty result.
    #[test]
    fn unknown_anchor_is_empty() {
        let names = vec!["a".to_string(), "b".to_string()];
        let streams = synth_streams(5, 2, 3);
        let shards = shard_streams(&streams);
        let index = assemble(&shards, 2, 0).expect("assemble");
        let spec = spec_of(3, 10, TimeRange::default());
        let result = answer(&index, &names, &spec, Some((u64::MAX, true)));
        assert!(result.rows.is_empty());
    }

    /// A dataset with no events yields an empty index and empty results.
    #[test]
    fn empty_dataset_is_empty() {
        let index = assemble(&[], 4, 0).expect("assemble empty");
        let names = vec!["a".to_string(); 4];
        let spec = spec_of(2, 10, TimeRange::default());
        let result = answer(&index, &names, &spec, None);
        assert!(result.rows.is_empty());
    }

    /// Merge delta events into a base event list by timestamp, a delta event
    /// landing AFTER base events with the same timestamp (its arrival
    /// tiebreak is later).
    fn merge_events(base: &[(u32, i64)], delta: &[(u32, i64)]) -> Vec<(u32, i64)> {
        let mut merged = Vec::with_capacity(base.len() + delta.len());
        let mut from_delta = delta.iter().peekable();
        for event in base {
            while let Some(pending) = from_delta.peek() {
                if pending.1 < event.1 {
                    merged.push(**pending);
                    from_delta.next();
                } else {
                    break;
                }
            }
            merged.push(*event);
        }
        for pending in from_delta {
            merged.push(*pending);
        }
        merged
    }

    /// Synthesize one refresh round's delta for an actor: appended events,
    /// sometimes a late mid-stream insertion, sometimes a leading duplicate
    /// of the actor's last event (exercising the boundary collapse).
    fn synth_delta(rng: &mut Lcg, base: &[(u32, i64)], alphabet: u64) -> Vec<(u32, i64)> {
        let mut delta: Vec<(u32, i64)> = Vec::new();
        let last_ts = base.last().map_or(1_000, |event| event.1);
        if !base.is_empty() && rng.next(4) == 0 {
            let at = rng.next(base.len() as u64) as usize;
            let code = u32::try_from(rng.next(alphabet)).expect("code fits");
            delta.push((code, base[at].1));
        }
        let mut clock = last_ts;
        let appended = 1 + rng.next(6);
        for step in 0..appended {
            clock += 1 + i64::try_from(rng.next(30)).expect("step fits");
            let code = if step == 0 && rng.next(3) == 0 {
                // Sometimes repeat the last base code, so the append
                // collapses into the existing run.
                base.last().map_or(0, |event| event.0)
            } else {
                u32::try_from(rng.next(alphabet)).expect("code fits")
            };
            delta.push((code, clock));
        }
        delta.sort_by_key(|event| event.1);
        delta
    }

    /// Grow a dataset over several refresh rounds and answer every variant
    /// from base + signed pairs, against brute force over the FINAL streams.
    /// This is the correctness pin of the incremental index: additions own
    /// the changed window forms, retractions cancel the stale ones, and the
    /// signed sum must be EXACTLY the single-index answer at every depth up
    /// to the bound, for every anchor, range, and top.
    #[test]
    fn incremental_pairs_match_brute_force() {
        let names: Vec<String> = ["zeta", "alpha", "mid", "beta", "omega"]
            .iter()
            .map(|name| (*name).to_string())
            .collect();
        let alphabet = u32::try_from(names.len()).expect("alphabet fits");
        for (seed, max_depth) in [(7u64, 4u32), (99, 8), (1234, 8)] {
            let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9));
            let base = synth_streams(30, u64::from(alphabet), seed);
            let mut current: Vec<Vec<(u32, i64)>> = base
                .iter()
                .map(|(codes, ts)| codes.iter().copied().zip(ts.iter().copied()).collect())
                .collect();
            let mut indexes: Vec<(PathsSa, bool)> = vec![(
                assemble(&shard_streams(&base), alphabet, 0).expect("base"),
                false,
            )];
            for round in 0..2 {
                let mut addition = empty_pair_stream();
                let mut retraction = empty_pair_stream();
                // A couple of brand-new actors join every round.
                for _ in 0..2 {
                    current.push(Vec::new());
                }
                for (actor, events) in current.iter_mut().enumerate() {
                    let touched = rng.next(3) != 0;
                    if !touched {
                        continue;
                    }
                    let delta = synth_delta(&mut rng, events, u64::from(alphabet));
                    let merged_events = merge_events(events, &delta);
                    let old = stitch::collapse(events);
                    let merged = stitch::collapse(&merged_events);
                    let envelope = stitch::envelope(&old, &merged);
                    let merged_len = u32::try_from(merged.codes.len()).expect("fits");
                    let old_len = u32::try_from(old.codes.len()).expect("fits");
                    let local = actor as u64;
                    if let Some(slice) = stitch::addition_stitch(merged_len, envelope, max_depth) {
                        push_slice(&mut addition, local, &merged, slice, 0);
                    }
                    if let Some(slice) = stitch::retraction_stitch(old_len, envelope, max_depth) {
                        push_slice(&mut retraction, local, &old, slice, 0);
                    }
                    *events = merged_events;
                }
                if !addition.actors.is_empty() {
                    indexes.push((
                        assemble(&[addition], alphabet, max_depth).expect("addition"),
                        false,
                    ));
                }
                if !retraction.actors.is_empty() {
                    indexes.push((
                        assemble(&[retraction], alphabet, max_depth).expect("retraction"),
                        true,
                    ));
                }
                let finals: Vec<(Vec<u32>, Vec<i64>)> = current
                    .iter()
                    .map(|events| {
                        let collapsed = stitch::collapse(events);
                        (collapsed.codes, collapsed.ts)
                    })
                    .collect();
                let refs: Vec<(&PathsSa, bool)> = indexes
                    .iter()
                    .map(|(index, negative)| (index, *negative))
                    .collect();
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
                for depth in 2..=max_depth {
                    for range in ranges {
                        for anchor in &anchors {
                            for top in [1u32, 3, 10, 1000] {
                                let spec = spec_of(depth, top, range);
                                let got = answer_segments(&refs, &names, &spec, *anchor);
                                let want = brute(&finals, &names, &spec, *anchor);
                                let what = format!(
                                    "seed {seed} max_depth {max_depth} round {round} \
                                     depth {depth} top {top} range {range:?} anchor {anchor:?}"
                                );
                                assert_same(&got, &want, &what);
                            }
                        }
                    }
                }
            }
        }
    }
}
