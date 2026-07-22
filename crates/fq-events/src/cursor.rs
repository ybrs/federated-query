//! Per-shard scan machinery: lazily decoded column accessors over a shard's
//! generation files, actor-run enumeration from the actor directories, and
//! the per-actor generation merge.
//!
//! A shard with one generation presents each actor as one contiguous slice
//! cursor (the common, fastest path). A shard with several generations
//! presents each actor as the k-way merge of its per-generation runs by
//! `(ts, tiebreak)` - the tiebreak column is decoded ONLY on that path; the
//! single-run path never reads it because the materialized order already
//! encodes it.

use std::cell::OnceCell;
use std::sync::Arc;

use crate::error::EventStoreError;
use crate::format::{ActorDirectory, DecodedBlock, PropBlock, PropValues, BLOCK_ROWS};
use crate::store::{CachedItem, SegFile, StoreIo};

/// Core column indices of every segment file.
pub const COL_TS: u32 = 0;
pub const COL_TIEBREAK: u32 = 1;
pub const COL_EVENT: u32 = 2;
/// Property columns start here.
pub const COL_PROPS: u32 = 3;

/// A lazily block-decoded column accessor over one file. Blocks decode on
/// first touch (through the process cache), so a scan that never needs a
/// block - a rare-event funnel whose code never appears in it - never pays
/// its decode.
pub struct ColumnAccessor<'a> {
    file: SegFile,
    io: &'a StoreIo,
    column: u32,
    blocks: Vec<OnceCell<Arc<CachedItem>>>,
}

impl<'a> ColumnAccessor<'a> {
    /// An accessor over `column` of `file`.
    fn new(file: &SegFile, io: &'a StoreIo, column: u32) -> Self {
        let mut blocks = Vec::with_capacity(file.blocks());
        blocks.resize_with(file.blocks(), OnceCell::new);
        Self {
            file: file.clone(),
            io,
            column,
            blocks,
        }
    }

    /// The decoded block holding `row`.
    fn block(&self, row: usize) -> Result<&Arc<CachedItem>, EventStoreError> {
        let index = row / BLOCK_ROWS;
        if let Some(item) = self.blocks[index].get() {
            return Ok(item);
        }
        let loaded = self.file.block(self.io, self.column, index as u32)?;
        Ok(self.blocks[index].get_or_init(|| loaded))
    }

    /// The i64 value at `row` (ts / tiebreak columns).
    pub fn i64_at(&self, row: usize) -> Result<i64, EventStoreError> {
        let item = self.block(row)?;
        let CachedItem::Block(DecodedBlock::I64(values)) = &**item else {
            unreachable!("i64 accessor over a non-i64 column");
        };
        Ok(values[row % BLOCK_ROWS])
    }

    /// The event code at `row`.
    pub fn code_at(&self, row: usize) -> Result<u64, EventStoreError> {
        let item = self.block(row)?;
        let CachedItem::Block(DecodedBlock::Codes(codes)) = &**item else {
            unreachable!("code accessor over a non-code column");
        };
        Ok(codes.get(row % BLOCK_ROWS))
    }

    /// The property block holding `row`, with its in-block offset.
    pub fn prop_at(&self, row: usize) -> Result<(&PropBlock, usize), EventStoreError> {
        let item = self.block(row)?;
        let CachedItem::Block(DecodedBlock::Prop(prop)) = &**item else {
            unreachable!("prop accessor over a non-prop column");
        };
        Ok((prop, row % BLOCK_ROWS))
    }
}

/// A property value borrowed out of a decoded block.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PropRef<'a> {
    Null,
    Code(u64),
    Str(&'a str),
    I64(i64),
    F64(f64),
    Bool(bool),
}

/// Read a property value from a decoded property block.
pub fn prop_value(block: &PropBlock, offset: usize) -> PropRef<'_> {
    if let Some(validity) = &block.validity {
        if validity[offset] == 0 {
            return PropRef::Null;
        }
    }
    match &block.values {
        PropValues::Codes(codes) => PropRef::Code(codes.get(offset)),
        PropValues::I64(values) => PropRef::I64(values[offset]),
        PropValues::F64(values) => PropRef::F64(values[offset]),
        PropValues::Bool(values) => PropRef::Bool(values[offset] != 0),
        PropValues::Str { offsets, bytes } => {
            let start = offsets[offset] as usize;
            let end = offsets[offset + 1] as usize;
            PropRef::Str(std::str::from_utf8(&bytes[start..end]).expect("stored strings are UTF-8"))
        }
    }
}

/// One generation file of a shard, opened for scanning.
pub struct FileScan<'a> {
    pub seg: SegFile,
    dir: Arc<CachedItem>,
    pub ts: ColumnAccessor<'a>,
    pub tiebreak: ColumnAccessor<'a>,
    pub event: ColumnAccessor<'a>,
    props: Vec<OnceCell<ColumnAccessor<'a>>>,
    io: &'a StoreIo,
}

impl<'a> FileScan<'a> {
    /// Open one generation file for scanning.
    fn open(io: &'a StoreIo, seg: SegFile) -> Result<Self, EventStoreError> {
        let dir = seg.directory(io)?;
        let ts = ColumnAccessor::new(&seg, io, COL_TS);
        let tiebreak = ColumnAccessor::new(&seg, io, COL_TIEBREAK);
        let event = ColumnAccessor::new(&seg, io, COL_EVENT);
        let prop_count = seg.kinds.len() - COL_PROPS as usize;
        let mut props = Vec::with_capacity(prop_count);
        props.resize_with(prop_count, OnceCell::new);
        Ok(Self {
            seg,
            dir,
            ts,
            tiebreak,
            event,
            props,
            io,
        })
    }

    /// The actor directory of this file.
    pub fn directory(&self) -> &ActorDirectory {
        let CachedItem::Dir(dir) = &*self.dir else {
            unreachable!("directory cache slot holds a directory");
        };
        dir
    }

    /// The lazily built accessor of property `prop`.
    pub fn prop(&self, prop: usize) -> &ColumnAccessor<'a> {
        self.props[prop]
            .get_or_init(|| ColumnAccessor::new(&self.seg, self.io, COL_PROPS + prop as u32))
    }
}

/// One shard's whole scan state: its generation files, oldest first.
pub struct ShardScan<'a> {
    pub files: Vec<FileScan<'a>>,
}

/// One actor's event runs across a shard's generations.
pub enum ActorRun {
    /// The actor's events live in one file: a contiguous slice.
    Single { file: usize, start: u64, end: u64 },
    /// The actor's events span several files; per file `(file, start, end)`.
    Multi(Vec<(usize, u64, u64)>),
}

impl<'a> ShardScan<'a> {
    /// Open a shard's generation files for scanning.
    pub fn open(io: &'a StoreIo, files: Vec<SegFile>) -> Result<Self, EventStoreError> {
        let mut scans = Vec::with_capacity(files.len());
        for seg in files {
            scans.push(FileScan::open(io, seg)?);
        }
        Ok(Self { files: scans })
    }

    /// Every actor's runs, in ascending local-id order, merged across the
    /// shard's generation files.
    pub fn actor_runs(&self) -> Vec<ActorRun> {
        if self.files.len() == 1 {
            let dir = self.files[0].directory();
            let mut runs = Vec::with_capacity(dir.locals.len());
            for index in 0..dir.locals.len() {
                let (start, end) = dir.range(index);
                runs.push(ActorRun::Single {
                    file: 0,
                    start,
                    end,
                });
            }
            return runs;
        }
        self.merged_actor_runs()
    }

    /// The multi-generation merge of the actor directories.
    fn merged_actor_runs(&self) -> Vec<ActorRun> {
        let dirs: Vec<&ActorDirectory> = self.files.iter().map(FileScan::directory).collect();
        let mut positions = vec![0usize; dirs.len()];
        let mut runs = Vec::new();
        loop {
            let mut next_local: Option<u64> = None;
            for (dir, position) in dirs.iter().zip(&positions) {
                if let Some(local) = dir.locals.get(*position) {
                    next_local = Some(next_local.map_or(*local, |v: u64| v.min(*local)));
                }
            }
            let Some(local) = next_local else {
                return runs;
            };
            let mut pieces: Vec<(usize, u64, u64)> = Vec::new();
            for (file, dir) in dirs.iter().enumerate() {
                let position = &mut positions[file];
                if dir.locals.get(*position) == Some(&local) {
                    let (start, end) = dir.range(*position);
                    pieces.push((file, start, end));
                    *position += 1;
                }
            }
            if pieces.len() == 1 {
                let (file, start, end) = pieces[0];
                runs.push(ActorRun::Single { file, start, end });
            } else {
                runs.push(ActorRun::Multi(pieces));
            }
        }
    }

    /// Visit an actor's events in `(ts, tiebreak)` order as
    /// `(file, row, code)`; `visit` returning false stops early.
    pub fn for_each_event(
        &self,
        run: &ActorRun,
        mut visit: impl FnMut(usize, usize, u64) -> Result<bool, EventStoreError>,
    ) -> Result<(), EventStoreError> {
        match run {
            ActorRun::Single { file, start, end } => {
                let scan = &self.files[*file];
                for row in *start..*end {
                    let row = usize::try_from(row).expect("row fits usize");
                    let code = scan.event.code_at(row)?;
                    if !visit(*file, row, code)? {
                        return Ok(());
                    }
                }
                Ok(())
            }
            ActorRun::Multi(pieces) => self.for_each_merged(pieces, &mut visit),
        }
    }

    /// The k-way `(ts, tiebreak)` merge over an actor's per-file runs.
    fn for_each_merged(
        &self,
        pieces: &[(usize, u64, u64)],
        visit: &mut impl FnMut(usize, usize, u64) -> Result<bool, EventStoreError>,
    ) -> Result<(), EventStoreError> {
        // Cursor per piece: (file, row, end, ts, tiebreak).
        let mut cursors: Vec<(usize, u64, u64, i64, i64)> = Vec::with_capacity(pieces.len());
        for (file, start, end) in pieces {
            if start < end {
                let scan = &self.files[*file];
                let row = usize::try_from(*start).expect("row fits usize");
                let ts = scan.ts.i64_at(row)?;
                let tb = scan.tiebreak.i64_at(row)?;
                cursors.push((*file, *start, *end, ts, tb));
            }
        }
        while !cursors.is_empty() {
            let mut best = 0usize;
            for candidate in 1..cursors.len() {
                let (_, _, _, ts, tb) = cursors[candidate];
                let (_, _, _, best_ts, best_tb) = cursors[best];
                if (ts, tb) < (best_ts, best_tb) {
                    best = candidate;
                }
            }
            let (file, row_index, end, _, _) = cursors[best];
            let scan = &self.files[file];
            let row = usize::try_from(row_index).expect("row fits usize");
            let code = scan.event.code_at(row)?;
            if !visit(file, row, code)? {
                return Ok(());
            }
            let next = row_index + 1;
            if next < end {
                let next_row = usize::try_from(next).expect("row fits usize");
                cursors[best] = (
                    file,
                    next,
                    end,
                    scan.ts.i64_at(next_row)?,
                    scan.tiebreak.i64_at(next_row)?,
                );
            } else {
                cursors.swap_remove(best);
            }
        }
        Ok(())
    }
}
