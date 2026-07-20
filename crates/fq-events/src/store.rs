//! Store IO and the decoded-block cache.
//!
//! All store file bytes move through the `object_store` abstraction (local
//! filesystem backend today; a remote backend is a swap, not a format
//! change). Reads are range reads driven synchronously over a small private
//! multi-thread runtime, so shard workers read concurrently.
//!
//! The process-wide cache holds verified column regions, decoded blocks,
//! actor directories, pre-aggregates, and sidecars keyed by immutable file
//! path, bounded by `events.cache_bytes` with least-recently-used eviction.
//! Files are immutable once published, so eviction is the only invalidation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::error::EventStoreError;
use crate::format::{
    block_count, block_rows, decode_actor_directory, decode_column_region, decode_footer_entries,
    decode_footer_tail, decode_preagg, decode_seg_header, decode_stats, fnv1a64, open_region,
    ActorDirectory, ColumnKind, ColumnRegion, ColumnStats, DecodedBlock, Preagg, RegionEntry,
    SegHeader, REGION_ACTOR_DIR, REGION_BLOCK_STATS, REGION_COLUMN, REGION_PREAGG, SEG_HEADER_LEN,
};

/// The synchronous facade over the event store's object store backend.
pub struct StoreIo {
    root: std::path::PathBuf,
    store: Arc<dyn ObjectStore>,
    runtime: tokio::runtime::Runtime,
}

impl StoreIo {
    /// Open the store rooted at `root`, creating the directory if absent.
    pub fn open(root: &std::path::Path) -> Result<Self, EventStoreError> {
        std::fs::create_dir_all(root).map_err(|error| EventStoreError::Io {
            file: root.display().to_string(),
            detail: format!("cannot create event store root: {error}"),
        })?;
        let store =
            LocalFileSystem::new_with_prefix(root).map_err(|error| EventStoreError::Io {
                file: root.display().to_string(),
                detail: error.to_string(),
            })?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("event store tokio runtime");
        Ok(Self {
            root: root.to_path_buf(),
            store: Arc::new(store),
            runtime,
        })
    }

    /// The store root directory.
    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    /// The IO error for `file`.
    fn io_error(file: &str, error: &object_store::Error) -> EventStoreError {
        EventStoreError::Io {
            file: file.to_string(),
            detail: error.to_string(),
        }
    }

    /// Write a complete file (atomic within the backend: temp name + rename).
    pub fn put(&self, rel: &str, bytes: Vec<u8>) -> Result<(), EventStoreError> {
        let path = StorePath::from(rel);
        self.runtime
            .block_on(self.store.put(&path, PutPayload::from(bytes)))
            .map_err(|error| Self::io_error(rel, &error))?;
        Ok(())
    }

    /// Read a complete file.
    pub fn get(&self, rel: &str) -> Result<Vec<u8>, EventStoreError> {
        let path = StorePath::from(rel);
        let result = self
            .runtime
            .block_on(async {
                let response = self.store.get(&path).await?;
                response.bytes().await
            })
            .map_err(|error| Self::io_error(rel, &error))?;
        Ok(result.to_vec())
    }

    /// Read an exact byte range of a file.
    pub fn get_range(&self, rel: &str, start: u64, end: u64) -> Result<Vec<u8>, EventStoreError> {
        let path = StorePath::from(rel);
        let result = self
            .runtime
            .block_on(self.store.get_range(&path, start..end))
            .map_err(|error| Self::io_error(rel, &error))?;
        Ok(result.to_vec())
    }

    /// Delete one file; a missing file is reported, never ignored silently.
    pub fn delete(&self, rel: &str) -> Result<(), EventStoreError> {
        let path = StorePath::from(rel);
        self.runtime
            .block_on(self.store.delete(&path))
            .map_err(|error| Self::io_error(rel, &error))?;
        Ok(())
    }

    /// Every file currently under `prefix`, as store-relative paths.
    pub fn list(&self, prefix: &str) -> Result<Vec<String>, EventStoreError> {
        use futures::TryStreamExt;
        let path = StorePath::from(prefix);
        let entries: Vec<object_store::ObjectMeta> = self
            .runtime
            .block_on(async { self.store.list(Some(&path)).try_collect::<Vec<_>>().await })
            .map_err(|error| Self::io_error(prefix, &error))?;
        Ok(entries
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect())
    }

    /// The size of one file in bytes.
    pub fn size(&self, rel: &str) -> Result<u64, EventStoreError> {
        let path = StorePath::from(rel);
        let meta = self
            .runtime
            .block_on(self.store.head(&path))
            .map_err(|error| Self::io_error(rel, &error))?;
        Ok(meta.size)
    }
}

/// The parsed, verified metadata of one `.seg` file: header plus the region
/// directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegMeta {
    pub header: SegHeader,
    pub regions: Vec<RegionEntry>,
}

impl SegMeta {
    /// The directory entry of `kind` (+ column for column regions).
    fn region(&self, kind: u8, column: u32, file: &str) -> Result<RegionEntry, EventStoreError> {
        self.regions
            .iter()
            .find(|entry| entry.kind == kind && entry.column == column)
            .copied()
            .ok_or_else(|| EventStoreError::Corrupt {
                file: file.to_string(),
                detail: format!("footer has no region of kind {kind} column {column}"),
            })
    }
}

/// One cached item.
pub enum CachedItem {
    Meta(SegMeta),
    Region(ColumnRegion),
    Block(DecodedBlock),
    Dir(ActorDirectory),
    Stats(Vec<ColumnStats>),
    Preagg(Preagg),
    Sidecar(crate::format::ActorSidecar),
}

impl CachedItem {
    /// Approximate heap bytes, for the cache budget.
    fn heap_bytes(&self) -> usize {
        match self {
            CachedItem::Meta(meta) => meta.regions.len() * 32 + 64,
            CachedItem::Region(region) => region.heap_bytes(),
            CachedItem::Block(block) => block.heap_bytes(),
            CachedItem::Dir(dir) => dir.heap_bytes(),
            CachedItem::Stats(stats) => stats.len() * 64,
            CachedItem::Preagg(preagg) => preagg.heap_bytes(),
            CachedItem::Sidecar(sidecar) => sidecar.heap_bytes(),
        }
    }
}

/// A cache key: the immutable ABSOLUTE file path (two stores may hold equal
/// relative paths) plus the item coordinates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    file: Arc<str>,
    kind: u8,
    column: u32,
    block: u32,
}

impl CacheKey {
    /// The key of one cached item of `file`.
    fn new(file: &Arc<str>, kind: u8, column: u32, block: u32) -> Self {
        Self {
            file: Arc::clone(file),
            kind,
            column,
            block,
        }
    }
}

/// One cache entry.
struct CacheEntry {
    item: Arc<CachedItem>,
    bytes: usize,
    last_used: u64,
}

/// The process-wide decoded-block LRU cache.
pub struct BlockCache {
    inner: Mutex<HashMap<CacheKey, CacheEntry>>,
    bytes: AtomicU64,
    clock: AtomicU64,
}

impl BlockCache {
    /// A fresh, empty cache.
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            bytes: AtomicU64::new(0),
            clock: AtomicU64::new(0),
        }
    }

    /// The one process-wide cache.
    pub fn global() -> &'static BlockCache {
        static CACHE: OnceLock<BlockCache> = OnceLock::new();
        CACHE.get_or_init(BlockCache::new)
    }

    /// Fetch the item under `key`, loading it with `load` on a miss and
    /// evicting least-recently-used entries past `budget` bytes.
    fn get_or_load(
        &self,
        key: CacheKey,
        budget: u64,
        load: impl FnOnce() -> Result<CachedItem, EventStoreError>,
    ) -> Result<Arc<CachedItem>, EventStoreError> {
        let tick = self.clock.fetch_add(1, Ordering::Relaxed);
        {
            let mut map = self.inner.lock().expect("event cache lock poisoned");
            if let Some(entry) = map.get_mut(&key) {
                entry.last_used = tick;
                return Ok(Arc::clone(&entry.item));
            }
        }
        // The load runs outside the lock so concurrent shard workers decode in
        // parallel; a racing duplicate load stores one winner.
        let item = Arc::new(load()?);
        let bytes = item.heap_bytes();
        let mut map = self.inner.lock().expect("event cache lock poisoned");
        if let Some(entry) = map.get_mut(&key) {
            entry.last_used = tick;
            return Ok(Arc::clone(&entry.item));
        }
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        map.insert(
            key,
            CacheEntry {
                item: Arc::clone(&item),
                bytes,
                last_used: tick,
            },
        );
        self.evict_past(&mut map, budget);
        Ok(item)
    }

    /// Evict least-recently-used entries until the budget holds.
    fn evict_past(&self, map: &mut HashMap<CacheKey, CacheEntry>, budget: u64) {
        while self.bytes.load(Ordering::Relaxed) > budget && map.len() > 1 {
            let oldest = map
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            let Some(key) = oldest else {
                return;
            };
            if let Some(entry) = map.remove(&key) {
                self.bytes.fetch_sub(entry.bytes as u64, Ordering::Relaxed);
            }
        }
    }
}

/// One open shard-generation segment file: verified metadata plus cached
/// region/block accessors. Clone is Arc-cheap (the handle carries no decoded
/// data itself).
#[derive(Clone)]
pub struct SegFile {
    rel: Arc<str>,
    /// The absolute path identity cache entries key on.
    cache_name: Arc<str>,
    pub meta: Arc<SegMeta>,
    pub kinds: Arc<Vec<ColumnKind>>,
    cache_budget: u64,
}

impl SegFile {
    /// Open `rel` (verifying magic, version, and footer shape). `kinds` is the
    /// registry-derived column kind list; a mismatch with the stored column
    /// count raises.
    pub fn open(
        io: &StoreIo,
        rel: &str,
        kinds: Arc<Vec<ColumnKind>>,
        cache_budget: u64,
    ) -> Result<Self, EventStoreError> {
        let rel: Arc<str> = Arc::from(rel);
        let cache_name: Arc<str> =
            Arc::from(io.root().join(rel.as_ref()).to_string_lossy().as_ref());
        let key = CacheKey::new(&cache_name, 0xf0, u32::MAX, 0);
        let meta = BlockCache::global().get_or_load(key, cache_budget, || {
            let size = io.size(&rel)?;
            if size < (SEG_HEADER_LEN + 12) as u64 {
                return Err(EventStoreError::Corrupt {
                    file: rel.to_string(),
                    detail: "file shorter than header + trailer".to_string(),
                });
            }
            let tail = io.get_range(&rel, size - 12, size)?;
            let footer_len = u64::from(decode_footer_tail(&tail, &rel)?);
            if footer_len + 12 > size {
                return Err(EventStoreError::Corrupt {
                    file: rel.to_string(),
                    detail: "footer length overruns the file".to_string(),
                });
            }
            let entries_bytes = io.get_range(&rel, size - 12 - footer_len, size - 12)?;
            let regions = decode_footer_entries(&entries_bytes, &rel)?;
            let header_bytes = io.get_range(&rel, 0, SEG_HEADER_LEN as u64)?;
            let header = decode_seg_header(&header_bytes, &rel)?;
            Ok(CachedItem::Meta(SegMeta { header, regions }))
        })?;
        let CachedItem::Meta(ref parsed) = *meta else {
            unreachable!("meta cache slot holds meta");
        };
        if parsed.header.column_count as usize != kinds.len() {
            return Err(EventStoreError::Corrupt {
                file: rel.to_string(),
                detail: format!(
                    "file has {} columns but the registry declares {}",
                    parsed.header.column_count,
                    kinds.len()
                ),
            });
        }
        let meta = Arc::new(parsed.clone());
        Ok(Self {
            rel,
            cache_name,
            meta,
            kinds,
            cache_budget,
        })
    }

    /// The store-relative path.
    pub fn rel(&self) -> &str {
        &self.rel
    }

    /// Events in this file.
    pub fn event_count(&self) -> u64 {
        self.meta.header.event_count
    }

    /// Blocks per column in this file.
    pub fn blocks(&self) -> usize {
        block_count(self.meta.header.event_count)
    }

    /// Read + verify one region's raw bytes.
    fn region_bytes(
        &self,
        io: &StoreIo,
        kind: u8,
        column: u32,
    ) -> Result<Vec<u8>, EventStoreError> {
        let entry = self.meta.region(kind, column, &self.rel)?;
        let bytes = io.get_range(&self.rel, entry.offset, entry.offset + entry.length)?;
        if fnv1a64(&bytes) != entry.checksum {
            return Err(EventStoreError::Corrupt {
                file: self.rel.to_string(),
                detail: format!("region kind {kind} column {column} checksum mismatch"),
            });
        }
        Ok(bytes)
    }

    /// The verified column region of `column` (cached).
    pub fn column_region(
        &self,
        io: &StoreIo,
        column: u32,
    ) -> Result<Arc<CachedItem>, EventStoreError> {
        let key = CacheKey::new(&self.cache_name, 0xf1, column, 0);
        BlockCache::global().get_or_load(key, self.cache_budget, || {
            let bytes = self.region_bytes(io, REGION_COLUMN, column)?;
            Ok(CachedItem::Region(decode_column_region(&bytes, &self.rel)?))
        })
    }

    /// The decoded block `block` of `column` (cached).
    pub fn block(
        &self,
        io: &StoreIo,
        column: u32,
        block: u32,
    ) -> Result<Arc<CachedItem>, EventStoreError> {
        let key = CacheKey::new(&self.cache_name, 0xf2, column, block);
        BlockCache::global().get_or_load(key, self.cache_budget, || {
            let region = self.column_region(io, column)?;
            let CachedItem::Region(ref region) = *region else {
                unreachable!("region cache slot holds a region");
            };
            let rows = block_rows(self.meta.header.event_count, block as usize);
            let decoded =
                region.decode(self.kinds[column as usize], block as usize, rows, &self.rel)?;
            Ok(CachedItem::Block(decoded))
        })
    }

    /// The actor directory (cached).
    pub fn directory(&self, io: &StoreIo) -> Result<Arc<CachedItem>, EventStoreError> {
        let key = CacheKey::new(&self.cache_name, 0xf3, u32::MAX, 0);
        let actor_count = usize::try_from(self.meta.header.actor_count).map_err(|_| {
            EventStoreError::Corrupt {
                file: self.rel.to_string(),
                detail: "actor count is not addressable".to_string(),
            }
        })?;
        BlockCache::global().get_or_load(key, self.cache_budget, || {
            let bytes = self.region_bytes(io, REGION_ACTOR_DIR, u32::MAX)?;
            let payload = open_region(&bytes, &self.rel)?;
            Ok(CachedItem::Dir(decode_actor_directory(
                &payload,
                actor_count,
                &self.rel,
            )?))
        })
    }

    /// The per-column block statistics (cached).
    pub fn stats(&self, io: &StoreIo) -> Result<Arc<CachedItem>, EventStoreError> {
        let key = CacheKey::new(&self.cache_name, 0xf4, u32::MAX, 0);
        let columns = self.kinds.len();
        let blocks = self.blocks();
        BlockCache::global().get_or_load(key, self.cache_budget, || {
            let bytes = self.region_bytes(io, REGION_BLOCK_STATS, u32::MAX)?;
            let payload = open_region(&bytes, &self.rel)?;
            Ok(CachedItem::Stats(decode_stats(
                &payload, columns, blocks, &self.rel,
            )?))
        })
    }

    /// The event-by-day pre-aggregate (cached).
    pub fn preagg(&self, io: &StoreIo) -> Result<Arc<CachedItem>, EventStoreError> {
        let key = CacheKey::new(&self.cache_name, 0xf5, u32::MAX, 0);
        BlockCache::global().get_or_load(key, self.cache_budget, || {
            let bytes = self.region_bytes(io, REGION_PREAGG, u32::MAX)?;
            let payload = open_region(&bytes, &self.rel)?;
            Ok(CachedItem::Preagg(decode_preagg(&payload, &self.rel)?))
        })
    }
}

/// Read and verify one actor sidecar (cached; sidecars are read whole).
pub fn read_sidecar(
    io: &StoreIo,
    rel: &str,
    cache_budget: u64,
) -> Result<Arc<CachedItem>, EventStoreError> {
    let rel_arc: Arc<str> = Arc::from(rel);
    let cache_name: Arc<str> = Arc::from(io.root().join(rel).to_string_lossy().as_ref());
    let key = CacheKey::new(&cache_name, 0xf6, u32::MAX, 0);
    BlockCache::global().get_or_load(key, cache_budget, || {
        let bytes = io.get(&rel_arc)?;
        Ok(CachedItem::Sidecar(crate::format::decode_act(
            &bytes, &rel_arc,
        )?))
    })
}
