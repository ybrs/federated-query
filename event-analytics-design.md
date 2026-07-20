# Event Analytics - Technical Design

Materialized event-analytics store and the four analyses (funnels, retention,
segmentation, paths) for fedq, per `event-analytics-prd.md`. This document is
the complete implementation spec: storage format, identity and dictionaries,
indexes, build/refresh pipelines, per-analysis algorithms and semantics, the
SQL surface, the correctness harness, the performance model, the crate plan,
and the phased build with gates. Every design decision is closed here; the
implementing engineer should not need to make any further design choices.

Status: DESIGN (task 129, step 2 of PRD -> design -> implementation).

Contents:

1. Design foundations - what was measured before deciding
2. Storage format
3. Identity, dictionaries, time, tiebreak
4. Indexes
5. Ingestion / build pipeline
6. Refresh
7. Query semantics and algorithms
8. SQL / statement surface and engine integration
9. Correctness harness
10. Performance model and targets
11. Crate / module plan and phases
12. Risks and rejected alternatives

---

## 1. Design foundations - what was measured before deciding

All decisions below that needed a number were probed on this machine (12
cores, 125 GB RAM, NVMe) against the reference dataset
`benchmarks/events/data/events_medium_parquet/events.parquet` before being
made. These are DESIGN PROBES, not engine performance claims; final engine
numbers come from the events benchmark runner (section 9), per the
perf-numbers rule.

Reference dataset shape (measured):

- 100,000,000 rows, 814 row groups, snappy, 6 columns:
  `entity_id BIGINT`, `ts TIMESTAMP` (microsecond unit), `event_name VARCHAR`,
  `seq BIGINT`, `device VARCHAR`, `country VARCHAR`. No nulls.
- 500,000 actors, ids dense 1..500000 (the GENERAL design must not rely on
  this; see section 3). Events per actor: min 61, median 142, p90 317,
  p99 1000, max 141,456 (one whale actor).
- 20 event names, skewed: `page_view` 22.76M down to `cancel_account` 1.60M.
- `seq` is a global 0..99,999,999 arrival-order counter - the declared
  tiebreak. 708,318 `(entity_id, ts)` groups carry more than one event, so
  the tiebreak is exercised even at second-granularity timestamps.
- `ts` spans 2025-01-01 to 2025-01-31; every value is a whole second in THIS
  dataset (sub-second correctness is proven by crafted inputs, section 9.4).
- The file is in arrival order: every row group spans nearly the full ts range
  and the full actor range (813 of 813 row-group boundaries overlap in ts).

Micro-probes (standalone `rustc -O` binary, single thread unless noted):

- 64-bit hash finalizer (splitmix-style mix): 84 ms per 100M ids
  (0.84 ns/id). Hashing actor ids is free relative to IO.
- `sort_unstable` of 12.5M `(u32, i64, u32)` tuples: 0.68 s. Sorting is not
  the build bottleneck once partitioned (64 such sorts across 12 cores is
  well under 1 s of wall time for 100M events).
- Funnel DP over an in-memory CSR of 100M events (500K actors x 200 events,
  `u8` code + `i64` ts): 142 ms single-threaded. This single number shapes
  the whole index design: a layout-friendly full scan already beats the
  PRD's 1-2 s target by ~10x on one core, so no inverted index is required
  for the latency target (section 4).
- Pre-filtering the same stream to 3 step codes then running the DP: 677 ms
  filter + 42 ms DP - the filter pass costs more than it saves at query time
  unless the filtered set is persisted, which is what block skip stats give
  us for cheap (section 4.2).
- Varint delta-encoding 100M sorted-per-actor microsecond timestamps:
  0.72 s, 4.99 bytes/event before general compression.
- Bitset distinct-actors over 100M events with dense ids: 9 ms.

DuckDB probes (v1.5.3, 12 threads):

- Full scan + aggregate of the 100M parquet: 2.07 s - the floor for any
  approach that re-reads the origin per question, and the decode cost a
  build pass pays once.
- Full 100M external sort by `(entity_id, ts, seq)` + zstd parquet write
  under a 6 GB memory limit: 17.31 s = 5.8M events/s, output 944 MB
  (9.4 bytes/event). This is the build-pipeline proxy: a bounded-memory
  partition+sort+encode of 100M events costs tens of seconds, not minutes.
- Sorted zstd per-column sizes (10M slice): `entity_id` 0.19, `ts` 5.05,
  `event_name` 0.57, `seq` 3.50, `device` 0.19, `country` 0.35 bytes/event.
  Timestamps dominate; the CSR layout deletes the per-event actor column.
- Oracle costs over the raw parquet at 100M (what analysts pay WITHOUT the
  materialized store; first four recorded by the prior baseline run in
  `benchmarks/events/data/references_medium.duckdb` table `baselines`,
  last measured this session):
  - `funnel_common` 38.80 s, `funnel_selective` 1.76 s
  - `segment_events` 0.34 s, `segment_entities` 2.00 s
  - `paths_all` 20.06 s, `paths_anchored` 21.78 s
  - weekly retention grid (birth = `signup`, return = any): 6.75 s

Conclusion drawn from the probes, driving everything below: an
actor-major, time-sorted, dictionary-coded columnar layout makes every one
of the four analyses a single streaming pass at ~1 GB/s-per-core effective
scan rate; the design work is therefore (a) building that layout at
5M+ events/s under bounded memory and (b) pinning semantics exactly so the
fast answer is the oracle answer.

---

## 2. Storage format

### 2.1 What is reused from fq-accel and what is new

fq-accel's `ChunkStore` is Arrow-IPC-relational at every layer: its write API
accepts only `RecordBatch` slices, `view::columns_from_arrow` rejects
non-relational Arrow types (Binary/List/Struct raise `InvalidSchema`), the
read path registers chunks as DataFusion `ArrowFormat` listing tables, and
`next_generation` requires the `chunk-<gen>-<n>.arrow` name grammar. A
custom index format cannot pass through it without gutting those contracts,
so the event store does NOT go through `ChunkStore`.

What IS reused, deliberately and exactly:

- The `object_store` crate (already in the workspace, 0.13.2) as the IO
  boundary, `LocalFileSystem` backend in phase 1, so the S3 flip in PRD
  phase 3 is a backend swap, not a format change.
- The registry discipline: tables in the SAME `<config-stem>.stats.sqlite`
  (rusqlite, WAL, busy retry), a JSON file list in the registry as the ONLY
  source of truth for which files compose a dataset (never a directory
  listing), single-transaction publish, tombstoned drop with a sweep on
  open, write-new-generation-then-swap-then-unlink ordering (POSIX keeps
  in-flight readers safe).
- The location convention: `location_for(name)` = sanitized name +
  16-hex FNV-1a hash of the exact name, same algorithm as
  `fq-accel/src/view.rs`.
- The path convention: the store root is `<config-stem>.events/` next to the
  config file, sibling to `<config-stem>.mv/`. A programmatic config with no
  `source_path` has no event store and event DDL raises, mirroring
  `materialized::open_accelerator`.

### 2.2 On-disk layout

```
<config-stem>.events/
  <dataset_location>/                    one directory per event dataset
    shard-0000/
      gen-0.seg                          shard segment file (sec 2.4)
      gen-0.act                          actor sidecar (sec 2.5)
      gen-1.seg                          appended by REFRESH (sec 6)
      gen-1.act
    shard-0001/
      ...
    dict/
      __event__.gen-0.dict               event-name dictionary (sec 3.3)
      device.gen-0.dict                  one per dictionary-coded property
      country.gen-0.dict
      device.gen-1.dict                  dictionary appends per refresh
```

- Shard count `S` is a power of two fixed at CREATE (section 5.1); shard
  directories are zero-padded decimal.
- A GENERATION `g` is one build or refresh pass. `gen-<g>.seg` holds every
  event ingested in generation `g` whose actor hashes to that shard, sorted;
  a shard untouched by a refresh gets no file for that generation. Files are
  immutable once published.
- The registry (section 2.6) records the exact file list; unknown files in
  the directory are ignored by readers and removed by the tombstone sweep.

### 2.3 Logical model inside a shard

Within a shard, actors are identified by a dense LOCAL id `0..n_shard`
assigned at build (section 3.2). A shard-generation file stores, for the
subset of local actors that have events in that generation:

- an ACTOR DIRECTORY: the sorted local ids present, each with its event
  count - a CSR index over the event columns;
- EVENT COLUMNS, each event's fields laid out column-contiguously, with all
  events sorted by `(local_id, ts, tiebreak)`;
- per-file INDEX SECTIONS (block stats, pre-aggregates - section 4).

Because events are sorted actor-major then time, an actor's events are one
contiguous run in every column, and within the run they are in exact
`(ts, tiebreak)` order. Every analysis in section 7 is a forward scan of
these runs; the materialized ORDER is the primary index.

### 2.4 Segment file format (`gen-<g>.seg`) - byte level

All integers little-endian. `varint` = LEB128 (7 bits per byte, high bit =
continuation). `zigzag(v)` = `(v << 1) ^ (v >> 63)` for signed deltas.
Checksums are FNV-1a 64 (hand-rolled, ~10 lines, same primitive fq-accel
already uses for location hashing - no new dependency).

```
[ header ]
  magic            8 bytes   "FQEVSEG1"
  format_version   u32       1
  shard            u32
  generation       u64
  event_count      u64
  actor_count      u64       actors present in THIS file
  column_count     u32       ts, tiebreak, event, then properties in
                             declared order
[ column region ] x column_count       (sec 2.4.1)
[ actor directory region ]             (sec 2.4.2)
[ block stats region ]                 (sec 4.2)
[ preagg region ]                      (sec 4.3)
[ footer ]
  region directory: column_count + 3 entries of
      { region_kind u8, column_index u32 (or u32::MAX),
        offset u64, length u64, checksum u64 (FNV-1a of the
        compressed region bytes) }
  footer_length    u32
  magic            8 bytes   "FQEVEND1"
```

A reader opens the file, reads the trailing 12 bytes, reads the footer,
verifies every checksum of the regions it touches, and reads only the
regions the query needs. A checksum mismatch or an unknown
`format_version` raises `EventStoreError::Corrupt` - never a partial or
silent read.

#### 2.4.1 Column regions

Every column is stored as a sequence of BLOCKS of `BLOCK_ROWS = 65536`
events (the last block short). Each block is independently encoded then
compressed, so decode is parallel and cache granularity is
(file, column, block). A column region is:

```
block_count u32
block_index: block_count x { compressed_offset u64, compressed_len u32,
                             uncompressed_len u32 }
block bytes, concatenated
```

Per-column-kind block encodings, then `zstd` level 1 over the encoded block
(zstd 0.13.3 is already in the lockfile; level 1 chosen because the probe
showed encode throughput matters more than the last 10% of ratio, and the
dominant `ts` column is already delta-compacted before zstd):

- `ts` (i64 microseconds): first value raw i64, then
  `zigzag varint` delta versus the previous event IN THE BLOCK. Deltas are
  usually small positive (time-sorted within an actor) and occasionally
  negative (actor boundary crossings inside a block), which zigzag handles;
  probe basis: 4.99 B/event before zstd, `ts` was 5.05 B/event under
  plain zstd in the parquet proxy.
- `tiebreak` (i64): same encoding as `ts` (arrival counters correlate with
  time; proxy 3.50 B/event). Read only by build/merge/compaction and by the
  oracle-export path - analyses never need it at query time because the
  materialized order already encodes it (section 3.4).
- dictionary-coded columns (`event`, string properties): per block,
  `width u8` in {1,2,4,8} = the minimal byte width holding the block's
  maximum code, then a fixed-width array of codes. Codes are u64 logically -
  the width adapts to the DATA, never caps it (section 3.3).
- `i64` / `f64` numeric properties: raw fixed-width array.
- `bool` properties: bitmap, 1 bit per event.
- raw-string properties (high-cardinality fallback, section 3.3): per block,
  `varint` lengths then the concatenated UTF-8 heap; zstd level 3 (strings
  compress well and this path is already the slow path).
- nullable columns carry a validity bitmap before the values
  (`has_nulls u8` flag first; bitmap omitted when 0). Null dictionary codes
  are NOT allocated - null is validity, never a sentinel code.

#### 2.4.2 Actor directory region

```
actor_count entries, delta-coded and varint-packed:
  local_id_delta   varint    versus previous entry (entries sorted)
  event_count      varint
first_ts: actor_count x zigzag varint delta   first event ts per actor
                                              in THIS file
```

Prefix-summing `event_count` yields each actor's `[start, end)` event range
in every column of this file (CSR offsets). `first_ts` powers birth lookups
without touching event columns (section 7.2).

### 2.5 Actor sidecar (`gen-<g>.act`)

Identity data for actors FIRST SEEN in this shard-generation
(section 3.2):

```
[ header ]  magic "FQEVACT1", format_version u32, shard u32, generation u64,
            new_actor_count u64
[ hash index ]  new_actor_count x { hash64 u64, local_id u64 },
                sorted by (hash64, key bytes)
[ key heap ]    new_actor_count x { tag u8, len varint, bytes },
                in local_id order, so entry i belongs to
                local_id = first_local_id + i
[ footer ]      first_local_id u64, region directory + checksums as in .seg
```

The hash index maps an incoming actor key to its local id at refresh time
and at query time for actor-scoped debugging; the key heap is the reverse
map (local id -> raw key) used for collision verification and result
export. Both regions are zstd-1 compressed.

### 2.6 Registry tables

Added to the existing `<config-stem>.stats.sqlite` (same connection
discipline as `ViewCatalog`: WAL, `busy_timeout` 5 s, short single
transactions, additive migrations):

```sql
CREATE TABLE IF NOT EXISTS event_datasets (
    name             TEXT PRIMARY KEY,
    location         TEXT NOT NULL,
    source_kind      TEXT NOT NULL,   -- 'table' | 'select'
    source_ref       TEXT NOT NULL,   -- 'ds.schema.table' or the SELECT sql
    actor_column     TEXT NOT NULL,
    time_column      TEXT NOT NULL,
    time_unit        TEXT NOT NULL,   -- 'us' | 'ms' (declared by the source)
    event_column     TEXT NOT NULL,
    tiebreak_column  TEXT,            -- NULL = synthetic ingest ordinal
    property_schema  TEXT NOT NULL,   -- JSON, sec 2.7
    shards           INTEGER NOT NULL,
    file_list        TEXT NOT NULL,   -- JSON, source of truth, sec 2.6.1
    dict_state       TEXT NOT NULL,   -- JSON {column: code_count}, sec 3.3
    refresh_key      TEXT,            -- monotonic column for delta append
    watermark        TEXT,            -- JSON watermark value, sec 6.1
    source_token     TEXT,            -- per-base-table version token
    measured_events  INTEGER NOT NULL,
    measured_actors  INTEGER NOT NULL,
    byte_size        INTEGER NOT NULL,
    build_millis     INTEGER NOT NULL,
    created_at       TEXT NOT NULL,
    refreshed_at     TEXT,
    deleted_at       TEXT             -- tombstone for crash-safe drop
);
```

#### 2.6.1 `file_list` JSON shape

```
{ "generations": [ { "generation": 0,
                     "shards": [ { "shard": 0, "seg": "shard-0000/gen-0.seg",
                                   "act": "shard-0000/gen-0.act",
                                   "events": 1562500, "new_actors": 7812,
                                   "bytes": 14680064 }, ... ],
                     "dicts": [ { "column": "__event__",
                                  "file": "dict/__event__.gen-0.dict",
                                  "codes_after": 20 }, ... ] }, ... ] }
```

Readers resolve every file through this list. `publish` = write all files +
fsync, then one UPDATE of `file_list`/`dict_state`/`watermark`/counters in a
single transaction, then unlink superseded files (compaction/rebuild only).

### 2.7 Property schema JSON

Declared at CREATE, immutable for the dataset's life (a source schema drift
at refresh raises `EventSchemaError`, mirroring `require_same_shape`):

```
[ { "name": "device", "data_type": "Varchar", "encoding": "dict",
    "nullable": true },
  { "name": "amount", "data_type": "Double", "encoding": "f64",
    "nullable": true },
  ... ]
```

`data_type` uses `fq_common::DataType` names. `encoding` is one of
`dict | raw_string | i64 | f64 | bool` and is chosen by the builder
(section 3.3); `dict -> raw_string` promotion during build is recorded here
at publish.

### 2.8 Size accounting (100M reference, from the probe proxy)

Per event: `ts` ~5.0 B + `tiebreak` ~3.5 B + `event` ~0.6 B + `device`
~0.2 B + `country` ~0.35 B, no per-event actor column (CSR), + directory/
stats/preagg/sidecar overhead ~0.2 B. Predicted total ~9.8 B/event,
~0.98 GB at 100M, ~9.8 GB at 1B with the same shape. The measured DuckDB
proxy (which also carries a 0.19 B/event entity column) landed at 944 MB;
the gate in section 11 uses <= 1.2 GB at 100M as the acceptance bound.

---

## 3. Identity, dictionaries, time, tiebreak

### 3.1 Actor identity - any type, via hashing

The actor column may be any of: `BigInt`/`Integer` (dense OR sparse),
`Varchar`/`Text` (including UUID strings), or binary UUID. Identity is
defined over CANONICAL KEY BYTES:

```
key_bytes = tag u8 || payload
  tag 0x01 = signed 64-bit integer, payload = i64 LE (Integer widened)
  tag 0x02 = utf8 string, payload = the bytes
  tag 0x03 = binary (16-byte UUID or arbitrary), payload = the bytes
```

Integer `42` and string `"42"` are therefore DIFFERENT actors; the tag is
part of identity. A null actor id raises `EventBuildError::NullActor` with
the source row's position - an event without a subject is a contract
violation, never dropped.

The routing hash is `hash64(key_bytes) = mix64(fnv1a64(key_bytes))` where
`mix64` is the splitmix64 finalizer (measured 0.84 ns/id; both primitives
are ~5 lines, no new dependency; FNV-1a alone has weak avalanche on short
keys, the finalizer fixes shard skew). `shard = hash64 & (S - 1)`.

Correctness NEVER depends on hash uniqueness: two distinct keys with equal
`hash64` in the same shard are separate actors, disambiguated by comparing
raw key bytes (section 5.3 step 3); the sidecar hash index is sorted by
`(hash64, key_bytes)` so lookups compare keys on hash equality. There is no
`id - min` fast path anywhere; the dense-id benchmark shape takes the exact
same code path as UUIDs.

### 3.2 Dense internal ids

Internal id = `(shard u32, local u64)`. Local ids are assigned per shard,
starting at 0 in generation 0, continuing from `max_local + 1` in later
generations (new actors only; returning actors keep their id via the
sidecar hash index). Assignment order within a build is
`(hash64, key_bytes)` sorted - deterministic, so two builds of the same
input produce byte-identical stores.

Local ids are dense per shard, which is what makes every per-actor
auxiliary structure an ARRAY (first-ts, offsets, per-actor analysis state)
rather than a hash map. Nothing global is ever indexed by actor: analyses
run per shard and merge scalar/grouped partials, so actor-count scaling
never concentrates in one structure. Local ids are u64 in the model and in
every on-disk varint; in-memory per-shard vectors index by `usize`
(a shard with more than `usize::MAX` actors cannot be represented and
raises `EventBuildError::ShardOverflow` - loud, and unreachable before
exabyte scale).

### 3.3 Dictionaries - unbounded cardinality

One append-only dictionary per dictionary-coded column: `__event__` (the
event-name column, reserved name) and each string property. A dictionary
maps `utf8 value -> code u64` in first-seen order.

- Codes are u64 logically. NOTHING in the format or the code fixes a width:
  on disk, blocks byte-pack to the block's maximum code (2.4.1); in memory,
  decoded code vectors are a width-adaptive enum
  `CodeVec { U8(Vec<u8>), U16(Vec<u16>), U32(Vec<u32>), U64(Vec<u64>) }`
  chosen from the dictionary's current code count - an exhaustive match,
  no `_` arm, per the walker rule. A 5,000-event-name product costs 2-byte
  codes; 20 names cost 1 byte; there is no "32 events" anywhere.
- Persistence: `dict/<column>.gen-<g>.dict` holds the values APPENDED in
  generation `g`, in code order:
  `magic "FQEVDCT1" | version u32 | column name (varint len + utf8) |
   first_code u64 | count u64 | offsets u32[count+1] | utf8 heap |
   footer checksum`. Loading a dictionary = concatenating its generations
  in order; `dict_state` in the registry pins the expected total count and
  a count mismatch raises `EventStoreError::Corrupt`.
- Build-time memory: each dictionary's hash map (value -> code) is
  memory-accounted. When a single property's map exceeds
  `events.dict_max_bytes` (default 256 MB, section 8.6), the builder
  PROMOTES that property to `raw_string` encoding: the property's values
  are stored as zstd string blocks (2.4.1), its partial dictionary is
  discarded, already-spilled rows for that property carry their raw value
  from the start (the spill format always carries raw strings for dict
  properties precisely so promotion needs no re-read - section 5.2), and
  the promotion is recorded in `property_schema` and surfaced by
  `SHOW EVENT DATASETS`. Group-bys and filters on a `raw_string` property
  work identically, comparing/grouping strings instead of codes - slower,
  never wrong, never capped. The `__event__` column may NOT be promoted:
  the analyses' inner loops are defined over event codes, and a product
  with more distinct event NAMES than 256 MB of dictionary (~5-10M names)
  is outside the PRD's "hundreds to thousands"; the builder raises
  `EventBuildError::EventNameCardinality` with the measured count so the
  failure is loud and named.
- Query-time lookups: a literal event name or property value in a statement
  is resolved against the dictionary. An event name or breakdown/filter
  PROPERTY that does not exist in the dataset's schema raises
  `EventBindingError` (invalid query - must raise). A property VALUE or
  event name absent from the dictionary is data, not schema - but a typo'd
  event name silently matching nothing is the classic silent-wrong-funnel,
  so: an unknown EVENT NAME raises `EventUnknownName` listing the three
  nearest names by edit distance; setting
  `events.allow_unknown_names = true` (session-mutable, default false)
  turns it into an empty match for pre-registered pipelines that query
  names before first ingest. Unknown property VALUES compile to
  constant-false predicates (matching nothing is the correct answer for
  `country = 'XX'`).

### 3.4 Time and tiebreak

- Timestamps are stored as `i64` microseconds. A microsecond source column
  is taken as-is; a millisecond source is multiplied by 1000 (exact); a
  nanosecond source raises `EventTypeError::NanosecondTime` in phase 1
  (the PRD scopes ms/us; truncating nanos silently would violate full
  precision). Timestamps are naive-UTC, matching the engine's `Timestamp`
  type; all bucketing is UTC (section 7 pins this per analysis).
- The EVENT ORDER for every analysis is the total order
  `(ts, tiebreak)` ascending within an actor. The tiebreak is:
  - the declared `TIEBREAK <column>` (must be an integer column; any other
    type raises `EventTypeError::TiebreakType`), or
  - when absent, the SYNTHETIC ingest ordinal: the row's 0-based position
    in the deterministic source read order of its build/refresh pass.
    Synthetic tiebreaks are deterministic within one build (parquet reads
    are ordered by file/row-group/row) but NOT guaranteed stable across
    rebuilds from a live SQL source; `SHOW EVENT DATASETS` marks the
    tiebreak `synthetic` so this is visible. The oracle harness uses the
    same ordinal (section 9.2).
- The physical sort key of the store is `(local_id, ts, tiebreak)`. Because
  the order is materialized, analyses never read the tiebreak column; it
  exists on disk so that refresh merges, compaction, rebuilds, and the
  oracle export can reproduce the exact order.
- A duplicate `(actor, ts, tiebreak)` triple makes ordering between the two
  events undefined, which would poison funnel/path determinism. The build
  detects duplicates during the per-shard sort (adjacent equal keys) and
  raises `EventBuildError::AmbiguousOrder{actor_key, ts}` telling the user
  to declare a discriminating tiebreak. Synthetic ordinals are unique by
  construction, so datasets without a declared tiebreak never hit this.

---

## 4. Indexes

Every index below is per shard-generation file, built during the encode pass
(section 5.3) from data already in hand - no separate indexing pass - and is
immutable with its file, so refresh correctness is by construction: a new
generation carries its own complete index sections, and compaction rebuilds
them from the merged data it is writing anyway.

### 4.1 The primary index: materialized `(actor, ts, tiebreak)` order + CSR

The actor directory (2.4.2) IS the main index: actor -> contiguous event
range, events pre-sorted. What it accelerates: all four analyses become
single forward passes with O(1) per-event state transitions; per-actor
dedup (retention, uniques) becomes "compare against the previous value"
because each actor's events are contiguous and each (actor, bucket) group
is contiguous too. Size cost: ~3 bytes/actor (delta-varint id + count) +
~2-4 bytes/actor `first_ts` - ~3 MB total at 500K actors, negligible.

### 4.2 Block skip stats

Per column region, per 65,536-event block (2.4.1 `block stats region`):

```
ts column:      min_ts i64, max_ts i64
code columns:   distinct_codes: if <= 16 distinct in the block, the sorted
                code list (count u8 + varint codes); else marker 0xFF plus
                min_code/max_code varints
numeric cols:   min/max (i64 or f64 bits), null_count varint
```

What each accelerates:

- `distinct_codes` on `__event__`: a funnel/retention/paths/segmentation
  query whose step or filter codes do not intersect a block's code set
  skips decompressing that block's OTHER columns entirely (the code column
  itself decodes cheaply, ~0.6 B/event). For rare-event funnels
  (`funnel_selective` class) most blocks are skipped for `ts` decode, the
  expensive column. Common events appear in every block - the skip buys
  nothing there, and does not need to (the 142 ms full-scan probe covers
  that case).
- `min_ts/max_ts`: honest but WEAK here - because blocks are actor-major,
  a block spans nearly the dataset's full time range whenever its actors'
  histories do. It is recorded because it is 16 bytes/block and prunes
  hard-time-filtered queries on datasets whose actors have short lifespans
  (common in real products), but the design does NOT rely on it: time
  filters are applied per event during the scan (section 7.5), and
  segmentation's time-bucket speed comes from 4.3, not from ts pruning.
- property zone maps: prune blocks for numeric property predicates.

Size: ~20-40 bytes per block per column; at 100M events / 65,536 = 1,526
blocks x ~6 columns, under 0.5 MB per dataset. Free.

### 4.3 Per-file event-by-day pre-aggregate

Per shard-generation file (`preagg region`):

```
entry_count u32, then sorted entries
  { event_code varint (delta vs previous entry),
    day u32 varint (delta; days since 1970-01-01 UTC of ts),
    event_count varint }
plus one per-code total: { event_code varint delta, total_count varint }
```

What it accelerates: SEGMENT `count` measures at day/week/month grain
become a merge of sorted entry lists - no event column is touched.
Probe basis: `segment_events` costs DuckDB 342 ms at 100M; the pre-agg
answers it from a few hundred KB in single-digit milliseconds. Week/month
buckets sum day entries (UTC calendar, section 7.3). Hour-grain queries
and any query with a property filter/breakdown fall through to the scan
path - recording hour-grain entries for unbounded event cardinality times
multi-year ranges is the kind of speculative blowup this design refuses
(size = distinct (code, day) pairs actually present, bounded by data, but
hours would 24x it for one sub-case the scan already serves interactively).

Sums are correct across generations and shards by construction (counts are
additive; disjoint actor sets are irrelevant for event counts). UNIQUE
actor measures are deliberately NOT pre-aggregated: distinct counts do not
merge across generations of the same shard, and maintaining exact
mergeable uniqueness structures (per-day actor sets) costs actor-set
storage per bucket. Instead uniques are computed by the streaming-dedup
scan (section 7.3) whose probe basis is 9 ms/100M for the counting
primitive - measured cheap enough that pre-aggregating it buys nothing at
the 100M scale and only ~1 extra second cold at 1B.

### 4.4 Per-actor first-ts

`first_ts` in the actor directory (2.4.2), min-merged across generations at
query time. Accelerates retention's `BIRTH ANY EVENT` (cohort assignment
without touching event columns) and funnel/paths "actor entered range"
short-circuits. 2-4 bytes/actor.

### 4.5 What is deliberately NOT built (and why)

- No per-event-name inverted index / roaring bitmaps: the probe showed the
  full CSR scan at 142 ms single-thread per 100M events; postings would
  save at most that scan on rare-event queries which block skip stats
  (4.2) already serve, would add a per-name posting-merge code path to
  every analysis, and `roaring` is not in the lockfile. Revisit at 1B+ IF
  the events-bench gate shows rare-event funnels missing the latency
  target cold (section 10 shows they should not).
- No global actor hash table: identity stays per shard (3.2).
- No sessionization index: sessions are not in the PRD's four analyses;
  `MAXGAP` on paths (7.4) covers the inactivity-break need at scan time.

---

## 5. Ingestion / build pipeline

### 5.1 Shape and knobs

Two phases: B1 PARTITION (stream source -> dictionary-encode -> hash ->
per-shard spill runs) and B2 FINALIZE (per shard: sort -> assign ids ->
encode -> write), then one registry publish. Nothing ever requires the
dataset, a full column, or a full dictionary-encoded copy resident in RAM;
peak memory is governed by `events.build_memory_bytes` (default 6 GiB) and
is independent of total row count.

- Shard count: `S = clamp(next_power_of_two(est_events * est_row_bytes /
  events.target_shard_bytes), 64, 65536)` with `target_shard_bytes` =
  256 MiB of RAW columnar data and `est_events` from connector metadata
  (parquet: exact footer row count; a SELECT source uses the optimizer's
  estimate and S only affects granularity, not correctness). 100M reference
  -> S = 64 (~1.56M events/shard); 1B -> S = 256; 10B -> S = 2048. S is
  recorded in the registry and never changes for the dataset's life
  (hash routing must stay stable for appends); a dataset that outgrows its
  S by >64x is rebuilt (REBUILD re-derives S).
- Threads: `events.build_threads` (default 0 = all cores). B1 uses the
  source stream's natural parallelism + 1 spill writer per thread-local
  buffer set; B2 runs `min(threads, floor(events.build_memory_bytes /
  peak_shard_bytes))` shard workers concurrently, where `peak_shard_bytes`
  is measured from the largest spill run set (known exactly when B2
  starts).

### 5.2 Phase B1 - partition

Source read: phase 1 ingests from the parquet connector via a STREAMING
pull. `Runtime::execute_source_query` materializes all batches, which
violates bounded memory at 1B rows, so the runtime gains
`execute_source_stream(sql) -> SendableRecordBatchStream` (a thin variant
of the existing path: fq-exec already produces streams internally and
`collect_stream` is the final step; the new API stops before collecting).
The event build consumes batches as they arrive; SELECT-defined datasets
whose plans force materialization (cross-source joins) are accepted with
the memory cost attributed to the QUERY pipeline's existing spill pool,
not the event builder.

Per record batch (validated against the declared schema on the FIRST batch:
missing/mistyped actor/time/event/tiebreak/property columns raise
`EventSchemaError` before any row is processed):

1. Extract and canonicalize the actor key (3.1), compute `hash64` and
   `shard`. Null actor raises. Convert `ts` per `time_unit`; null ts raises
   `EventBuildError::NullTime`.
2. Dictionary-encode `__event__` and every `dict` property against the
   global build dictionaries (hash maps, memory-accounted; promotion rule
   in 3.3). A null event name raises `EventBuildError::NullEventName`.
3. Append the row to the owning shard's write buffer. Spill row format is
   columnar per buffer flush: `hash64 u64`, key bytes (varint len + tag +
   payload), `ts i64`, `tiebreak i64` (declared column or the running
   ingest ordinal), `event code varint`, then properties - dict codes as
   varints AND the raw string retained for dict properties (so promotion
   never re-reads; the raw copy costs spill bytes, not RAM, and spill is
   deleted at publish), numerics fixed-width, nulls as validity bytes.
4. When a shard buffer reaches `buffer_bytes = clamp(
   events.build_memory_bytes / (4 * S), 256 KiB, 4 MiB)`, compress it as
   one lz4 frame (`lz4_flex` 0.13.1, in the lockfile; lz4 over zstd here
   because spill is written once and read once, favor speed) and append to
   `build/spill-<shard>.tmp` under the dataset directory. Buffers are
   thread-local per B1 worker to avoid contention; frames carry
   `worker_id` and a frame ordinal so B2 can reconstruct the global ingest
   ordinal order deterministically for synthetic tiebreaks (ordinals are
   assigned at extraction time in step 1, carried in the frame header as
   the starting ordinal - single-writer per source stream partition keeps
   them dense and deterministic).

B1 memory: dictionaries (bounded by `dict_max_bytes` each, promotion
beyond) + `S x buffer_bytes` x workers' thread-local sets (with the clamp:
64 shards x 4 MiB x 12 = ~3 GiB worst case at default budget; the divisor
4 in `buffer_bytes` keeps the sum under budget/1.5 with headroom measured
into the gate) + in-flight batches. Spill disk: raw-ish columnar + raw
string copies, measured target <= 2.5x the compressed store size
(~2.5 GB at 100M with UUID-free keys; UUID keys add ~1.6 GB - disk, not
RAM).

### 5.3 Phase B2 - per-shard finalize (parallel across shards)

Per shard worker:

1. Stream-read the shard's lz4 frames into columnar vectors (peak =
   `peak_shard_bytes`, ~256 MiB raw at target sizing).
2. Distinct actors: collect `(hash64, key_bytes)` pairs, sort, dedup by
   full key bytes. Equal-hash different-key pairs stay distinct (collision
   correctness). Look up survivors against prior generations' sidecar hash
   indexes (initial build: none); unseen actors get new local ids in
   `(hash64, key_bytes)` order continuing from the shard's `max_local + 1`.
3. Build the per-row local-id column via a temporary
   `hash64 -> candidates` map, comparing key bytes on hash hits.
4. Sort event rows by `(local_id, ts, tiebreak)`: sort a permutation index
   (`Vec<u32>` when rows < 4B else `Vec<u64>` - width enum, no cap), then
   gather each column once. Adjacent-equal sort keys raise
   `AmbiguousOrder` (3.4).
5. Detect whale overflow: if a shard's spill exceeds
   `2 x peak budget` (extreme actor skew - hashing bounds this to
   single-actor whales), fall back to EXTERNAL merge for that shard: sort
   each spill sub-run independently (each fits), then k-way merge-stream
   into the encoder. Same output bytes, bounded memory, slower - and
   logged.
6. Encode and write `gen-<g>.seg`: stream blocks per column (2.4.1),
   accumulate block stats (4.2), pre-agg entries (4.3), the actor
   directory + first_ts (2.4.2); write regions, footer, fsync. Write
   `gen-<g>.act` for new actors. Report `(events, new_actors, bytes,
   per-column bytes)` to the coordinator.
7. Delete the shard's spill file.

Publish: after ALL shards succeed - dictionary generation files, then the
single registry transaction (2.6.1). Any worker error aborts the whole
build, deletes `build/` and any written generation files, and re-raises;
a torn build is never visible because the registry was never updated.

### 5.4 Throughput and memory targets

Measured basis: the DuckDB proxy ran the same logical pipeline (scan 100M
parquet, external sort by the same key under a 6 GB cap, zstd-encode,
write ~1 GB) in 17.31 s = 5.8M events/s on this machine; the component
probes (decode 2.07 s, sorts ~0.7 s aggregate, varint encode 0.72 s,
hash 0.08 s) sum well under that, IO and coordination being the rest.

Targets (gate values, section 11): initial build of the 100M reference in
<= 20 s (>= 5M events/s) with peak RSS <= 6 GiB; stretch 12 s. 1B events:
same RSS ceiling by construction (S grows to 256; per-shard peak
unchanged), extrapolated 170-200 s, store ~9.8 GB, spill ~25 GB transient.
The builder self-reports events/s, peak RSS (from `/proc/self/status`
VmHWM), spill bytes, and per-phase wall time in its status output - the
PRD requires the multiple to be stated, not guessed.

---

## 6. Refresh

Freshness is explicit and user-controlled, exactly like materialized views:
nothing on the query path ever contacts the source.

### 6.1 Incremental append - `REFRESH EVENT DATASET`

Admissible when the dataset was created `FROM <table>` with
`WITH (refresh_key = '<column>')` - a column the user asserts is monotone
over ARRIVAL (an ingest sequence or arrival timestamp; the EVENT ts itself
is admissible only when the source appends in ts order). Without a
`refresh_key`, REFRESH raises `EventRefreshError::NoRefreshKey` naming
REBUILD as the path (a SELECT-defined dataset in phase 1 likewise).

Flow:

1. Read the source token (same connector API the MV path uses via
   `delta::read_tokens`); if unchanged since `refreshed_at`, no-op,
   update `refreshed_at`, report `0 events`.
2. Pull `WHERE <refresh_key> > <watermark literal>` (watermark rendering
   reuses the `fq_accel::watermark::Watermark` value grammar - Int, Text,
   DateDays, TimestampMicros).
3. Run B1+B2 over the delta only, as generation `g+1`: every shard with
   delta events gets `gen-<g+1>.seg/.act`; dictionaries append new codes;
   returning actors resolve to their existing local ids through the
   sidecar hash indexes (5.3 step 2); synthetic tiebreak ordinals continue
   from `measured_events` so they remain unique dataset-wide.
4. Publish: new `file_list` generation entry + `dict_state` + new
   watermark + `refreshed_at`, one transaction.

Late events (older `ts` than already-stored events) are handled CORRECTLY,
not just tolerated: order is imposed per actor at query time by the
generation merge (6.2), so an appended event interleaves into its right
`(ts, tiebreak)` position regardless of which generation holds it. What
append cannot fix is rows the `refresh_key` predicate skips - that is the
user's monotonicity contract, stated in the docs and in the error text.

Index correctness on refresh: block stats and pre-aggs ship inside each new
file (4.x); the day pre-agg needs no rewrite because counts are additive
across generations; `first_ts` is min-merged at query time; retention/
uniques dedup is generation-merge-aware by construction (7.x operate on
the merged per-actor stream).

### 6.2 Query-time generation merge and compaction

A shard with generations `g0..gk` presents each actor as the k-way merge of
its (at most k+1) sorted runs by `(ts, tiebreak)` - a binary heap over run
cursors, O(log k) per event with k typically 1-8. The analyses in
section 7 consume an `ActorEvents` iterator that hides this merge; a
single-generation shard uses a direct slice cursor (the common, fastest
path).

Compaction: when a shard's generation count exceeds
`events.max_generations` (default 8), the NEXT refresh folds that shard:
streams the merge of all its runs through the encoder into one
`gen-<g+1>.seg` (+ merged `.act`), publishes, unlinks the old files.
Compaction is per shard and incremental - a refresh compacts at most
`events.compact_shards_per_refresh` (default: all eligible; the knob
exists to bound refresh latency on huge datasets). Merged output is
byte-deterministic (same total order, same block boundaries).

### 6.3 Full rebuild - `REBUILD EVENT DATASET`

Re-runs the whole build from the source into a FRESH generation-0 file set
(written beside the live one under `rebuild-<utc-nanos>/` inside the
dataset directory), then swaps the registry `file_list` in one transaction
and unlinks the old tree. Re-derives S; resets dictionaries and local ids
(a rebuild is a new dense-id world - nothing external ever holds internal
ids, so this is invisible). REBUILD is also the schema-change path: same
statement after editing the CREATE via DROP+CREATE.

### 6.4 Crash safety

Identical discipline to fq-accel: files first, registry transaction second,
unlinks last; a crash before publish leaves orphan files that the
open-time sweep removes (any file under the dataset directory not in
`file_list` and older than the newest registry write); a crash after
publish but before unlink leaves superseded files the sweep removes the
same way. DROP tombstones (`deleted_at`), deletes files, then purges the
row; `open` finishes interrupted drops.

---

## 7. Query semantics and algorithms

Common machinery for all four analyses:

- Execution: one worker per shard (bounded by cores), each streaming its
  shard's actors through the generation-merged `ActorEvents` cursor;
  partials merge at the coordinator (sums, count maps, duration arrays).
  Distinct-actor counts sum across shards because shards partition actors.
- `FROM <ts> TO <ts>` is the half-open interval `[from, to)` applied to
  event `ts`. Absent bounds default to the dataset's min/max ts.
- `WHERE <predicate>` (grammar in 8.3) filters the EVENT STREAM before the
  analysis sees it: a non-matching event is invisible to steps, returns,
  buckets, and paths alike. Predicates compile to closures over dict codes
  / numeric ranges once per query (value -> code lookups per 3.3); null
  property values fail every comparison except `IS NULL`.
- `BREAKDOWN BY <property>` must name a declared property (else
  `EventBindingError`); its per-analysis value-attribution rule is pinned
  in each subsection. Breakdown values are reported as the property's
  string (dictionary-decoded), with SQL NULL for null property values.
- All bucketing is UTC: `day` = calendar date; `week` = ISO-8601 week
  starting Monday; `month` = calendar month; `hour` = clock hour. Bucket
  labels in results are the bucket's start instant as `Timestamp(us)`.

### 7.1 Funnels - `FUNNEL`

Declarative definition (STRICT_ORDER mode, the phase 1 mode): given steps
`s1..sk` (event names, k in 2..=32 - a parse-time sanity bound on the
STATEMENT, not the data; more steps raises `EventBindingError` with the
bound named), window `W`, and the filtered stream, an actor REACHES depth
`j` iff there exist events `e1 < e2 < ... < ej` (order = `(ts, tiebreak)`,
strictly increasing) with `name(ei) = si` and
`ts(ej) - ts(e1) <= W` (inclusive). Intervening non-step events are
allowed; a later `s1` event can always anchor a fresh attempt (this is the
re-entry rule - it falls OUT of the exists-form rather than being bolted
on). Each actor is counted once per step at its maximum depth.

Algorithm (per actor, single forward pass - the probed DP,
ClickHouse-windowFunnel-style): maintain `anchor[j]` = the LATEST `s1`
timestamp among partial matches of length `j` (latest anchor dominates:
it leaves the largest remaining window; exchange argument in the probe
notes). Per event with code `c`, scanning steps from deepest to shallowest
to avoid double-advancing on one event:

```
for j in (current_max_depth+1 .. 2).rev():
    if c == step[j] and anchor[j-1] set and ts - anchor[j-1] <= W:
        anchor[j] = anchor[j-1]; depth = max(depth, j)
if c == step[1]:
    anchor[1] = ts; depth = max(depth, 1)
```

O(k) per event, no per-actor allocation. Repeated step names
(`A -> B -> A`) work because each slot j matches independently.

Time-to-convert: reported for the WITNESS sequence, pinned as: the
EARLIEST `s1` event from which greedy forward matching (each step matched
by the earliest qualifying event after the previous match, within `W` of
the anchor) reaches the actor's maximum depth; greedy-from-anchor is
optimal for a fixed anchor, so the witness exists whenever the depth does.
Computed in a SECOND pass over only the actors that reached depth >= 2
(state: replay the DP recording, per depth, the earliest anchor achieving
it, then one greedy chase from that anchor). Per-step durations
`ts(ej) - ts(e(j-1))` in microseconds are collected per shard into f64
arrays; exact median/p90 by `select_nth_unstable` after merging shard
partials. If the merged array for a step would exceed 1 GiB (>= ~134M
converting actors), partials spill to lz4 temp runs and the quantile is
selected externally (two-pass bucket narrowing) - exact at any scale.

Breakdown attribution: the property value of the WITNESS ANCHOR event
(the `s1` event defined above). Actors are counted under exactly one
breakdown value. Rationale: anchor attribution is deterministic, matches
"funnel per property value of entry", and is expressible in the oracle.

Result schema (pinned; `breakdown` column present only when the clause is):

```sql
-- FUNNEL result: one row per (breakdown,) step, ordered by
-- (breakdown, step_index)
breakdown                       VARCHAR NULL   -- only with BREAKDOWN BY
step_index                      INTEGER        -- 1-based
step_name                       VARCHAR
entered                         BIGINT         -- actors with depth >= step_index
conversion_from_previous        DOUBLE         -- entered / previous.entered; 1.0 at step 1
conversion_from_start           DOUBLE         -- entered / step1.entered
median_seconds_from_previous    DOUBLE NULL    -- NULL at step 1
p90_seconds_from_previous       DOUBLE NULL    -- NULL at step 1
```

`ANY_ORDER` mode: recognized by the grammar, raises
`EventUnsupported::AnyOrderFunnel` in phase 1 (PRD: flagged, not built).

Complexity: O(events_scanned x k) + O(converters) for the witness pass.
Columns read: `__event__` codes + `ts` (+ filter/breakdown property).
Expected latency: section 10.

### 7.2 Retention / cohorts - `RETENTION`

Definitions (pinned):

- BIRTH: per actor, the first event (in `(ts, tiebreak)` order, within the
  filtered stream and `FROM/TO` range) matching the birth spec -
  `BIRTH ANY EVENT` (first-ever event) or `BIRTH <event>` (first
  occurrence of that name). Actors with no birth event are not in any
  cohort.
- COHORT: actors whose birth falls in the same calendar `PERIOD`
  (`DAY | WEEK | MONTH`, UTC per the common rules). `cohort_start` labels
  it.
- RETURN at offset `n >= 1`: the actor has an event matching the return
  spec (`RETURN ANY EVENT` or `RETURN <event>`) whose ts falls in calendar
  period `cohort_period + n`. Offset 0 rows are reported too (retained =
  actors with a matching return event in the birth period AFTER the birth
  event, both orders compared by `(ts, tiebreak)`).
- BOUNDED mode: retained(n) = actors returning in EXACTLY period n.
  UNBOUNDED mode: retained(n) = actors returning in period n OR LATER
  (rolling retention). Default BOUNDED.
- Grid rows are produced for every cohort in range and offsets
  `0..=PERIODS` (default: through the dataset's max ts); empty cohorts
  produce `cohort_size = 0` rows only for periods that contain at least
  one birth-eligible calendar period inside `FROM/TO` (i.e. calendar
  periods with zero births still appear, with zero counts - the oracle
  pins this).

Algorithm (per actor, ONE forward pass - the sorted layout makes dedup
free): scan events; before birth is found, test the birth predicate
(`first_ts` from 4.4 short-circuits `BIRTH ANY EVENT` with no event-column
read when no filter/range is set); once born, compute each subsequent
matching return event's period offset; offsets arrive NON-DECREASING
(events are time-sorted), so per-actor dedup is "if offset >
last_counted_offset, count it". BOUNDED accumulates into a
`(cohort, offset) -> count` map directly; UNBOUNDED tracks the actor's max
offset and the coordinator suffix-sums the per-cohort max-offset histogram
(`retained_unbounded(n) = sum over m >= n of actors_with_max_offset(m)`),
with offset-0 handling per the definition above. Cohort sizes accumulate
on birth. Breakdown attribution: the property value of the BIRTH event.

Result schema (pinned; ordered by (breakdown,) cohort_start,
period_offset):

```sql
breakdown        VARCHAR NULL      -- only with BREAKDOWN BY
cohort_start     TIMESTAMP         -- period start, UTC
cohort_size      BIGINT
period_offset    INTEGER           -- 0-based
retained         BIGINT
retention_rate   DOUBLE            -- retained / cohort_size; NULL when size 0
```

`AT <n>` returns only the `period_offset = n` rows (the PRD's single
N-period number, still per cohort).

Complexity: O(events_scanned), columns `__event__` + `ts` (+ breakdown
property at birth). The map size is cohorts x offsets - calendar-bounded,
tiny.

### 7.3 Segmentation - `SEGMENT`

Measures (pinned): `COUNT` (matching events per bucket), `UNIQUES`
(distinct actors with a matching event per bucket), `COUNT_PER_UNIQUE`
(their ratio as DOUBLE, NULL when uniques 0). Optional `EVENT <name>`
restricts to one event name (absent = all events); `WHERE` and `FROM/TO`
per the common rules; `BUCKET HOUR | DAY | WEEK | MONTH`.

Fast path: `COUNT` at DAY/WEEK/MONTH grain with no `WHERE` and no
`BREAKDOWN` merges the per-file day pre-aggs (4.3) - no event data
touched; `EVENT <name>` filters pre-agg entries by code. Expected
single-digit ms (section 10).

Scan path (everything else): per shard, stream events; for each matching
event compute `(bucket, [breakdown code])`; accumulate into a hash map
`(bucket, breakdown) -> {count u64, uniques u64, last_actor u64}` where
`last_actor` implements streaming dedup: actors are contiguous, so an
actor contributes to a group's `uniques` only when `last_actor != actor`.
This replaces bitsets entirely (probe basis: the counting primitive at
9 ms/100M; the map stays small because its size is
buckets x breakdown-values actually present - data-bounded). Coordinator
sums count/uniques across shards (disjoint actors). If a pathological
breakdown (millions of values x thousands of buckets) pushes a shard map
past `events.query_memory_bytes` share, the map spills to sorted lz4 runs
merged at the coordinator - exact, bounded, logged.

Breakdown attribution: the property value ON EACH EVENT (an actor may
appear under several values; `uniques` dedups per (bucket, value) group -
pinned, matches the oracle GROUP BY).

Result schema (pinned; ordered by (breakdown,) bucket):

```sql
bucket      TIMESTAMP        -- bucket start, UTC
breakdown   VARCHAR NULL     -- only with BREAKDOWN BY
value       BIGINT           -- COUNT / UNIQUES
-- COUNT_PER_UNIQUE instead pins: value DOUBLE
```

### 7.4 Flows / paths - `PATHS`

Definitions (pinned):

- The actor's PATH STREAM is the filtered event stream with CONSECUTIVE
  DUPLICATE names collapsed (`A A B A -> A B A`; collapse happens after
  filtering, so filtered-out events do not separate duplicates).
- `MAXGAP <interval>` (optional): a gap `> interval` between consecutive
  path-stream events breaks the stream into fragments; paths never cross a
  break. Default: no breaking.
- FORWARD flow `STARTING AT <event>`: every occurrence of the anchor in
  the path stream emits the sequence of it plus the next `DEPTH - 1`
  path-stream events (truncated at fragment end; sequences shorter than 2
  are dropped). Overlapping anchors each emit (pinned - matches the
  recorded `paths_anchored` oracle).
- BACKWARD flow `ENDING AT <event>`: mirror - the `DEPTH - 1` events
  before each anchor, emitted in forward order ending at the anchor.
- UNANCHORED (neither clause): every position in the path stream emits its
  forward window of length `DEPTH` (shorter tail windows are dropped;
  matches the recorded `paths_all` oracle).
- `DEPTH <n>` in 2..=10 (statement bound, raises past it - path
  explosion is exponential in depth and 10 is beyond any Sankey use);
  default 5. `TOP <n>` default 20, max 1000.

Algorithm: per shard, per actor, materialize the collapsed code stream
into a reused scratch buffer (avg 200 codes; a whale actor streams
through a chunked buffer with `DEPTH - 1` overlap so windows never miss a
boundary - bounded memory even for a 100M-event actor); enumerate windows
per the variant; count into a hash map keyed by the packed code sequence
(`SmallVec`-style inline `[u64; 10]` key, hashed as bytes... the key is
the exact code sequence, no lossy hashing - equal counts require equal
sequences). Shard maps merge at the coordinator; `TOP n` selected by
`select_nth_unstable` on `(count desc, path string asc)` - the tie order
is pinned so results are deterministic. Spill rule as in 7.3 for
pathological cardinality (distinct depth-5 paths are data-bounded;
20-name datasets max out at 3.2M).

Result schema (pinned; ordered by rank):

```sql
rank         INTEGER      -- 1-based
path         VARCHAR      -- names joined with ' -> '
occurrences  BIGINT       -- windows counted (not distinct actors; pinned)
share        DOUBLE       -- occurrences / total windows counted
```

### 7.5 Column IO per analysis (what a query actually reads)

| analysis      | columns decoded                              | skip stats used            |
|---------------|----------------------------------------------|----------------------------|
| funnel        | `__event__`, `ts` (+ filter/breakdown prop)  | event codes, ts, zone maps |
| retention     | `__event__`, `ts` (+ props), first_ts        | event codes, ts, zone maps |
| segment fast  | pre-agg region only                          | -                          |
| segment scan  | `__event__`, `ts` (+ props)                  | event codes, ts, zone maps |
| paths         | `__event__`, `ts` (+ filter prop)            | event codes, ts, zone maps |

Decoded blocks are cached in an in-process LRU keyed
`(dataset, file, column, block)` with budget `events.cache_bytes`
(default 8 GiB): warm queries skip file IO AND decompression; the 100M
reference's funnel working set (`ts` + codes decoded, ~1 GB) fits, a 1B
dataset's does not and the LRU makes that graceful, never wrong. The cache
is invalidated by registry `file_list` identity (files are immutable, so
eviction is the only invalidation).

---

## 8. SQL / statement surface and engine integration

The event family is a lexical statement family, classified by
`fq-parse::classify_statement` BEFORE the query pipeline, exactly like the
materialized-view and datasource families. Event statements never enter
parse -> bind -> optimize; they carry their own small grammar with typed,
loud errors. All three front doors get the feature for free because they
only ever call `Runtime::execute`, which returns Arrow for every statement
form.

### 8.1 Statement grammar

Grammar over the existing `fq-parse` `Tokenizer` (quote-aware; keywords
case-insensitive; identifiers lowercased unless quoted; `expect_end`
rejects trailing tokens). `<interval>` = `<n> MINUTE|HOUR|DAY|WEEK`
(singular/plural); `<ts>` = a quoted `'YYYY-MM-DD[ HH:MM:SS[.ffffff]]'`
literal, UTC.

```
CREATE EVENT DATASET <name>
    FROM <datasource>.<schema>.<table> | AS ( <select sql> )
    ACTOR <column>
    TIME <column>
    EVENT <column>
    [ TIEBREAK <column> ]
    [ PROPERTIES ( <column> [, <column>]* ) ]
    [ WITH ( <key> = '<value>' [, ...] ) ]

REFRESH EVENT DATASET <name>
REBUILD EVENT DATASET <name>
DROP EVENT DATASET <name>
SHOW EVENT DATASETS

FUNNEL ( <event> [, <event>]+ ) ON <dataset>
    WINDOW <interval>
    [ MODE STRICT_ORDER | ANY_ORDER ]
    [ FROM <ts> ] [ TO <ts> ]
    [ WHERE <predicate> ]
    [ BREAKDOWN BY <property> ]

RETENTION ON <dataset>
    BIRTH ANY EVENT | BIRTH <event>
    RETURN ANY EVENT | RETURN <event>
    PERIOD DAY | WEEK | MONTH
    [ MODE BOUNDED | UNBOUNDED ]
    [ PERIODS <n> ] [ AT <n> ]
    [ FROM <ts> ] [ TO <ts> ]
    [ WHERE <predicate> ]
    [ BREAKDOWN BY <property> ]

SEGMENT COUNT | UNIQUES | COUNT_PER_UNIQUE ON <dataset>
    [ EVENT <event> ]
    BUCKET HOUR | DAY | WEEK | MONTH
    [ FROM <ts> ] [ TO <ts> ]
    [ WHERE <predicate> ]
    [ BREAKDOWN BY <property> ]

PATHS ON <dataset>
    [ STARTING AT <event> | ENDING AT <event> ]
    [ DEPTH <n> ] [ TOP <n> ]
    [ MAXGAP <interval> ]
    [ FROM <ts> ] [ TO <ts> ]
    [ WHERE <predicate> ]
```

- `<event>` is a quoted string literal (event names are data, not
  identifiers): `FUNNEL ('signup', 'activate')`.
- `<property>` and the column names in CREATE are identifiers.
- `PROPERTIES` defaults to every source column not named as
  actor/time/event/tiebreak. An empty property list is written
  `PROPERTIES ()`.
- `WITH` keys (allowlist; unknown key raises `ParseError::Unsupported`
  with the known-key list): `shards` (power of two, overrides 5.1),
  `refresh_key` (column name, enables 6.1), `time_unit` (`us` default,
  `ms`).
- Clause order is fixed as written (single-pass parser, better errors);
  a missing required clause raises `ParseError::Parse` naming it.

### 8.2 The predicate grammar (`WHERE`)

A deliberately closed expression language over PROPERTIES only (events and
time have their own clauses; actor-level predicates are phase 2):

```
predicate := or_expr
or_expr   := and_expr ( OR and_expr )*
and_expr  := unary ( AND unary )*
unary     := NOT unary | '(' predicate ')' | comparison
comparison:= property ( '=' | '<>' | '!=' | '<' | '<=' | '>' | '>=' ) literal
           | property IN '(' literal ( ',' literal )* ')'
           | property IS [NOT] NULL
literal   := 'string' | number | TRUE | FALSE
```

Parsed by a ~150-line recursive-descent over the family tokenizer (NOT
polyglot: the language is closed, the errors must name properties, and the
statement never reaches the SQL pipeline). Compiled against
`property_schema`: unknown property, or a literal type that does not match
the property's `DataType` (string vs numeric vs bool), raises
`EventBindingError` before any data is touched. Dictionary properties
compare via code lookup (3.3); `raw_string` properties compare strings;
numerics compare natively.

### 8.3 Classification and value types

`fq-parse/src/statement.rs`: extend `Statement<'a>` with

```
CreateEventDataset(EventDatasetDef)
RefreshEventDataset { name: String }
RebuildEventDataset { name: String }
DropEventDataset { name: String }
ShowEventDatasets
EventFunnel(FunnelSpec)
EventRetention(RetentionSpec)
EventSegment(SegmentSpec)
EventPaths(PathsSpec)
```

Dispatch keys: first word `CREATE`/`REFRESH`/`REBUILD`/`DROP`/`SHOW` with
`second_word_is("EVENT")` (REBUILD is a new first-word key used only by
this family), and first words `FUNNEL` / `RETENTION` / `SEGMENT` / `PATHS`.
These four are NOT reserved by the query pipeline today (polyglot would
reject them as SQL anyway), so classification is unambiguous; a document
note in `statement.rs` states the reservation.

The value types (`EventDatasetDef`, `FunnelSpec`, `RetentionSpec`,
`SegmentSpec`, `PathsSpec`, plus `EventPredicate`, `TimeRange`,
`BucketGrain`, `RetentionMode`, `FunnelMode`, `PathAnchor`) live in a new
`fq-common/src/events.rs`, re-exported from `fq_common` - this is the
"event-statement value types" module the architecture doc reserves. They
are plain structs/enums with public fields (serde derives for EXPLAIN
document output later), e.g.:

```
pub struct FunnelSpec {
    pub dataset: String,
    pub steps: Vec<String>,          // 2..=32, enforced at parse
    pub window_micros: i64,
    pub mode: FunnelMode,            // StrictOrder | AnyOrder
    pub range: TimeRange,            // from/to Option<i64> micros
    pub filter: Option<EventPredicate>,
    pub breakdown: Option<String>,
}
```

### 8.4 Runtime dispatch

`fq-runtime/src/lib.rs::execute` gains one match arm per new variant (the
match has no `_` arm, so the compiler enforces coverage), each delegating
to handlers in a new `fq-runtime/src/events.rs`:

- DDL handlers (`create_event_dataset`, `refresh_event_dataset`,
  `rebuild_event_dataset`, `drop_event_dataset`) open the event store
  (mirroring `open_accelerator`; raises without a config path), drive
  `fq-events` build/refresh (sourcing batches via
  `execute_source_stream`), and return `status_result("CREATE EVENT
  DATASET")` etc. - the one-row `status` convention every DDL uses.
- `show_event_datasets` returns the pinned relation:

```sql
name              VARCHAR      -- dataset name
events            BIGINT
actors            BIGINT
bytes             BIGINT
shards            INTEGER
generations       INTEGER      -- max generation count across shards
tiebreak          VARCHAR      -- column name or 'synthetic'
promoted_props    VARCHAR      -- comma list of raw_string promotions, '' if none
watermark         VARCHAR NULL
created           VARCHAR
refreshed         VARCHAR NULL
```

- Analysis handlers (`run_funnel`, `run_retention`, `run_segment`,
  `run_paths`) call `fq_events::exec::*` and convert the typed result
  into the pinned Arrow schema (7.x). `Runtime::describe` gains arms
  returning each form's fixed `(name, DataType)` list (the
  `views_describe_columns` pattern) so PG-wire Describe works without
  executing; the funnel/retention/segment schemas vary only by the
  statement's own clauses (breakdown present or not, measure type), which
  `describe` sees.
- ACL: event DDL registers in `acl::superuser_action` with the same
  gating class as materialized-view DDL; analysis statements are reads.
  Per-dataset grants are phase 3 with the rest of ACL-for-extensions
  (called out in section 12).

Errors: new `RuntimeError::Events(#[from] fq_events::EventError)`;
`EventError` is the fq-events umbrella (`thiserror`) with the variants
named throughout this doc (`EventSchemaError`, `EventBuildError`,
`EventStoreError`, `EventBindingError`, `EventUnknownName`,
`EventRefreshError`, `EventTypeError`, `EventUnsupported`). Every variant
carries the specifics its message needs; nothing is stringly caught and
rewrapped.

### 8.5 Example statements (meaning + result columns)

```sql
-- 1. Materialize the reference dataset (parquet connector 'ev').
CREATE EVENT DATASET web FROM ev.main.events
    ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq
    PROPERTIES (device, country)
    WITH (refresh_key = 'seq');
-- -> status: 'CREATE EVENT DATASET'

-- 2. Weekly signup -> checkout -> purchase funnel, 7-day window, Jan.
FUNNEL ('signup', 'begin_checkout', 'purchase') ON web
    WINDOW 7 DAY FROM '2025-01-01' TO '2025-02-01';
-- -> step_index, step_name, entered, conversion_from_previous,
--    conversion_from_start, median_seconds_from_previous,
--    p90_seconds_from_previous  (3 rows)

-- 3. Same funnel split by device, mobile users only.
FUNNEL ('signup', 'begin_checkout', 'purchase') ON web
    WINDOW 7 DAY WHERE device IN ('ios', 'android')
    BREAKDOWN BY device;
-- -> breakdown + the funnel columns (3 rows per device value)

-- 4. Weekly cohorts by first signup, bounded weekly retention grid.
RETENTION ON web BIRTH 'signup' RETURN ANY EVENT PERIOD WEEK PERIODS 8;
-- -> cohort_start, cohort_size, period_offset, retained, retention_rate

-- 5. Rolling month-3 retention, one number per monthly cohort.
RETENTION ON web BIRTH ANY EVENT RETURN 'purchase'
    PERIOD MONTH MODE UNBOUNDED AT 3;
-- -> cohort_start, cohort_size, period_offset(=3), retained, retention_rate

-- 6. Daily add_to_cart uniques in Germany, by device.
SEGMENT UNIQUES ON web EVENT 'add_to_cart'
    BUCKET DAY WHERE country = 'DE' BREAKDOWN BY device;
-- -> bucket, breakdown, value

-- 7. Daily event totals (pre-agg fast path).
SEGMENT COUNT ON web BUCKET DAY;
-- -> bucket, value

-- 8. Top 20 5-step flows out of search.
PATHS ON web STARTING AT 'search' DEPTH 5 TOP 20;
-- -> rank, path, occurrences, share

-- 9. What led to cancellation (backward flow, 30-minute session gap).
PATHS ON web ENDING AT 'cancel_account' DEPTH 4 MAXGAP 30 MINUTE;
-- -> rank, path, occurrences, share

-- 10. Keep it fresh after new parquet lands.
REFRESH EVENT DATASET web;
-- -> status: 'REFRESH EVENT DATASET: 1200000 events appended'
```

### 8.6 Settings

New `EventsConfig` struct in `fq-common` config (section `events:`,
`deny_unknown_fields`), each field registered as a `SettingDef` (the
completeness test enforces registration):

| setting                      | type  | default | mutability      |
|------------------------------|-------|---------|-----------------|
| `events.build_memory_bytes`  | u64   | 6 GiB   | SessionMutable  |
| `events.build_threads`       | usize | 0 (all) | SessionMutable  |
| `events.target_shard_bytes`  | u64   | 256 MiB | Static          |
| `events.dict_max_bytes`      | u64   | 256 MiB | SessionMutable  |
| `events.cache_bytes`         | u64   | 8 GiB   | SessionMutable  |
| `events.query_memory_bytes`  | u64   | 4 GiB   | SessionMutable  |
| `events.max_generations`     | u64   | 8       | SessionMutable  |
| `events.allow_unknown_names` | bool  | false   | SessionMutable  |

---

## 9. Correctness harness

### 9.1 Structure - one runner

`benchmarks/events/run_events.py` is the suite's SINGLE runner (per the
one-runner rule), with staged modes mirroring the tpcds harness:

- `--mode gen-crafted` - writes the crafted generality datasets (9.4) as
  parquet under `benchmarks/events/data/crafted/` (deterministic seeds).
- `--mode save-refs` - computes every battery entry's ORACLE result and
  oracle wall time via DuckDB SQL over the raw parquet, storing result
  tables + timings in `references_<size>.duckdb` (results are a function
  of the data; computed once, read forever).
- `--mode engine` - drives the fedq engine through `fedq-py` (CREATE
  EVENT DATASET once, then the battery statements), recording results,
  cold/warm timings, and build metrics.
- `--mode compare` - cell-exact comparison of engine vs reference tables
  (order-insensitive on the pinned result ordering keys, float rates
  compared exactly - both sides compute the same integer-derived
  divisions; time-to-convert medians compared exactly as both sides
  select the same element per the pinned witness definition). Any
  mismatch prints the query, both tables, and fails the run.
- The runner prints, before running, how many engine executions the
  invocation performs.

Rust-side unit/property tests live in `fq-events` (format round-trips,
dictionary growth, DP-vs-brute-force on random small inputs with seeds,
spill-forced builds); the SQL battery is the cross-implementation truth.

### 9.2 The oracle SQL (definitive, per analysis)

The oracle operates on a relation `ev(actor, ts, tb, name, props...)`
where `tb` is the declared tiebreak or, for synthetic-tiebreak datasets,
`row_number() over () - 1` on the identically-ordered source read - the
same ordinal the builder assigns. Oracles below are written for the
reference dataset's names; the runner templates them per battery entry.
All were syntax- and cost-validated this session (section 1).

Funnel (k = 3 shown; the runner generates the chase chain for each k -
the exists-form of 7.1 via per-anchor greedy chase, LATERAL):

```sql
WITH ev AS (SELECT entity_id AS actor, ts, seq AS tb, event_name AS name
            FROM read_parquet('events.parquet')
            /* runner adds: WHERE <filter>, ts range */),
anchors AS (SELECT actor, ts t1, tb b1 FROM ev WHERE name = 'signup'),
chase2 AS (
  SELECT a.actor, a.t1, a.b1, l.t2, l.b2 FROM anchors a
  LEFT JOIN LATERAL (
    SELECT e.ts t2, e.tb b2 FROM ev e
    WHERE e.actor = a.actor AND e.name = 'begin_checkout'
      AND (e.ts, e.tb) > (a.t1, a.b1)
      AND e.ts - a.t1 <= INTERVAL 7 DAY
    ORDER BY e.ts, e.tb LIMIT 1) l ON TRUE),
chase3 AS (
  SELECT c.*, l.t3 FROM chase2 c
  LEFT JOIN LATERAL (
    SELECT e.ts t3 FROM ev e
    WHERE e.actor = c.actor AND e.name = 'purchase'
      AND (e.ts, e.tb) > (c.t2, c.b2)
      AND e.ts - c.t1 <= INTERVAL 7 DAY
    ORDER BY e.ts, e.tb LIMIT 1) l ON TRUE),
per_actor AS (
  SELECT actor, max(CASE WHEN t3 IS NOT NULL THEN 3
                         WHEN t2 IS NOT NULL THEN 2 ELSE 1 END) AS depth
  FROM chase3 GROUP BY actor)
SELECT d AS step_index, count(*) AS entered
FROM per_actor, generate_series(1, 3) t(d)
WHERE depth >= d GROUP BY d ORDER BY d;
```

The greedy chase from EVERY anchor implements "max over anchors of the
greedy chain", which equals the exists-form depth (greedy-from-anchor
optimality, 7.1). The time-to-convert oracle extends `per_actor` with
`min(t1) FILTER (WHERE depth = max_depth)` to pick the witness anchor and
re-joins its chase row; medians via `quantile_cont(..., 0.5)` on the same
witness durations. The breakdown oracle groups `per_actor` by the witness
anchor's property value.

Retention (weekly grid, bounded, birth = named event, return = any):

```sql
WITH ev AS (SELECT entity_id AS actor, ts, seq AS tb, event_name AS name
            FROM read_parquet('events.parquet')),
births AS (
  SELECT actor, min_by(ts, (ts, tb)) AS birth_ts,
         min_by(tb, (ts, tb)) AS birth_tb,
         date_trunc('week', min_by(ts, (ts, tb))) AS cohort
  FROM ev WHERE name = 'signup' GROUP BY actor),
rets AS (
  SELECT b.actor, b.cohort,
         CAST((epoch(date_trunc('week', e.ts)) - epoch(b.cohort))
              // (7 * 86400) AS INT) AS off
  FROM births b JOIN ev e USING (actor)
  WHERE (e.ts, e.tb) > (b.birth_ts, b.birth_tb)
  GROUP BY 1, 2, 3),
grid AS (SELECT cohort, off, count(*) AS retained FROM rets GROUP BY 1, 2),
sizes AS (SELECT cohort, count(*) AS cohort_size FROM births GROUP BY 1)
SELECT s.cohort AS cohort_start, s.cohort_size, g.off AS period_offset,
       coalesce(g.retained, 0) AS retained,
       coalesce(g.retained, 0) * 1.0 / s.cohort_size AS retention_rate
FROM sizes s LEFT JOIN grid g USING (cohort)
ORDER BY 1, 3;
```

(UNBOUNDED variant: `max(off)` per actor, then
`count(*) FILTER (WHERE max_off >= k)` per cohort per k. The runner also
materializes zero-birth calendar periods per 7.2.)

Segmentation (daily uniques with breakdown):

```sql
SELECT date_trunc('day', ts) AS bucket, device AS breakdown,
       count(DISTINCT entity_id) AS value
FROM read_parquet('events.parquet')
WHERE event_name = 'add_to_cart' AND country = 'DE'
GROUP BY 1, 2 ORDER BY 2, 1;
```

Paths (forward flow, depth 5, anchored; collapse then windows):

```sql
WITH ev AS (SELECT entity_id AS actor, ts, seq AS tb, event_name AS name
            FROM read_parquet('events.parquet')),
ordered AS (
  SELECT actor, ts, tb, name, lag(name) OVER w AS prev_name
  FROM ev WINDOW w AS (PARTITION BY actor ORDER BY ts, tb)),
collapsed AS (
  SELECT actor, ts, tb, name FROM ordered
  WHERE prev_name IS NULL OR prev_name <> name),
strung AS (
  SELECT actor, name,
         name || ' -> ' || lead(name, 1) OVER w || ' -> '
              || lead(name, 2) OVER w || ' -> '
              || lead(name, 3) OVER w || ' -> ' || lead(name, 4) OVER w
         AS path
  FROM collapsed WINDOW w AS (PARTITION BY actor ORDER BY ts, tb))
SELECT path, count(*) AS occurrences
FROM strung WHERE name = 'search' AND path IS NOT NULL
GROUP BY path ORDER BY occurrences DESC, path LIMIT 20;
```

(`MAXGAP` oracle adds a gap-flag + running session id before collapsing;
backward flow strings `lag` instead of `lead`. `share` divides by
`sum(occurrences) over ()` of the pre-limit relation.)

### 9.3 The battery

~30 entries per dataset, each a (statement, oracle) pair; on the reference
dataset it includes at minimum:

- funnels: common-events 3-step (the 38.8 s oracle), selective 3-step,
  2-step, 5-step, window boundary at exactly `W` (an actor pair
  constructed at distance exactly `W` and `W + 1 us` - inclusive edge),
  re-entry-dependent case (later anchor succeeds where the first fails),
  repeated step name, filter, breakdown, filter + breakdown.
- retention: weekly bounded grid, monthly unbounded, `AT k`, named-birth
  vs any-birth, return-before-birth-in-period-0 case, empty cohort
  periods, single-event actors (cohort of size N, all offsets 0),
  breakdown.
- segmentation: all four grains, count/uniques/count_per_unique, event
  filter, property filter, breakdown, pre-agg-path vs scan-path answer
  equality (same query with and without a `WHERE TRUE`-class filter is
  NOT used - instead the runner compares the fast-path statement against
  the oracle directly, which already proves the pre-agg).
- paths: unanchored, starting-at, ending-at, depths 2/5, MAXGAP,
  collapse-with-filter interaction (filtered event between duplicates).

### 9.4 Generality validation (what the benchmark cannot prove)

The reference dataset is dense-int/20-name/no-null/second-precision. Each
generality MECHANISM gets a crafted dataset (in `--mode gen-crafted`,
~1-5M rows each, oracle-checked with the full battery where applicable):

- `crafted_uuid`: actor ids as UUID strings + a hash-adversarial pair of
  distinct keys placed in the same shard (the harness computes two keys
  with colliding `hash64` by brute force at generation time - feasible
  offline; seeds fixed) - proves hashing + collision disambiguation.
- `crafted_sparse`: int64 actor ids drawn uniformly from the full 2^63
  range - proves no dense-range assumption survives.
- `crafted_wide`: 5,000 distinct event names, 100K distinct values in one
  property (2-byte then wider codes), plus one property whose forced
  `dict_max_bytes = 1 MB` (via the setting) promotes it to `raw_string`
  mid-build - proves unbounded-cardinality paths including promotion.
- `crafted_subsecond`: microsecond timestamps with same-instant clusters,
  including funnel steps separated only by the tiebreak, plus an
  input-row-order SHUFFLE variant that must produce byte-identical
  analysis results (declared tiebreak) - proves precision + determinism.
- `crafted_nulls`: null properties everywhere legal, plus null
  actor/time/name rows that must RAISE (negative tests asserting the
  typed error).
- `crafted_spill`: built twice, once with `events.build_memory_bytes` set
  to the floor (forcing maximal spill + external shard merge) and once
  with defaults - both stores must produce identical battery results, and
  the store files must be byte-identical (deterministic build).
- `crafted_refresh`: split into three arrival slices; build + 2 REFRESHes
  vs one REBUILD of the union must give identical battery results
  (generation merge correctness), including a late-timestamp event in
  slice 3 and a same-`(ts)` tiebreak interleave across generations.

Adversarial/property-based: a seeded generator produces random tiny event
sets (<= 200 events, <= 5 actors, <= 4 names, adversarial ts ties) and
compares the Rust DP/scan implementations against brute-force enumeration
of the declarative definitions (7.x) - 10K cases in CI via `cargo test`,
no SQL involved, catching semantics drift at the unit level.

---

## 10. Performance model and targets

Machine baseline: the 12-core reference box. "Cold" = fresh process, page
cache dropped for the store's files (the runner uses a scratch copy read
with O_DIRECT-equivalent measurement; where not feasible it reports
"cool" = fresh process, OS cache warm, and labels it). "Warm" = second
run in a live runtime (decoded-block cache hit). Every number the feature
reports publicly comes from `run_events.py --mode engine`; the figures
below are the model and the gates.

Build (section 5.4): target >= 5M events/s, 100M in <= 20 s at
<= 6 GiB RSS (measured proxy: 17.31 s / 5.8M events/s under the same
budget doing strictly more encode work). 1B extrapolation: ~170-200 s,
same RSS (bounded by construction), ~9.8 GB store, ~25 GB transient spill.

Store size: <= 1.2 GB at 100M (model 0.98 GB, proxy 944 MB), ~10x that at
1B.

Query model: `cost = read(compressed bytes of needed columns) +
decode(zstd, ~1 GB/s/core aggregate across 12 cores far exceeds NVMe) +
scan(measured DP/dedup rates) + merge(negligible)`. Needed bytes at 100M:
`ts` ~500 MB, `__event__` ~60 MB, one dict property ~20-35 MB.

| analysis (100M, typical)      | cold target | warm target | oracle (DuckDB) |
|-------------------------------|-------------|-------------|-----------------|
| funnel 3-step, common events  | <= 1.5 s    | <= 0.3 s    | 38.80 s         |
| funnel 3-step, selective      | <= 1.0 s    | <= 0.2 s    | 1.76 s          |
| retention weekly grid         | <= 1.5 s    | <= 0.4 s    | 6.75 s          |
| segment COUNT daily (pre-agg) | <= 0.1 s    | <= 0.01 s   | 0.34 s          |
| segment UNIQUES daily         | <= 1.5 s    | <= 0.4 s    | 2.00 s          |
| paths depth-5 anchored        | <= 2.0 s    | <= 1.0 s    | 21.78 s         |

Basis per row: funnel warm = 142 ms single-thread scan probe,
parallelized across shards with merge overhead; funnel cold adds ~560 MB
read + decode; retention/uniques ride the same scan plus the 9 ms/100M
dedup-counting primitive; paths pay hash-map traffic (the most
allocation-heavy analysis, hence the 1 s warm target rather than 0.3);
pre-agg segmentation reads a few hundred KB. At 1B, scan-path numbers
scale ~10x on the cold column (IO-bound) and ~10x on warm scan time
(~1.4 s funnel warm) - still interactive, and the cache holds the hot
columns of ~1B events in the default 8 GiB budget only partially, which
the LRU degrades gracefully.

Where each index buys its speed: the materialized order turns every
analysis into the measured streaming scans (vs. DuckDB's repeated
hash-partition + window sorts - the 38.8 s vs 0.3 s gap IS the layout);
block code-sets rescue rare-event queries from decoding `ts` everywhere;
the day pre-agg turns totals segmentation into metadata; `first_ts`
removes the event scan from any-birth cohort assignment; dictionaries
turn every string comparison in the hot loops into integer compares.

---

## 11. Crate / module plan and phases

### 11.1 Crates

New crate `crates/fq-events` (engine logic; no pyo3, no server code):

```
fq-events
  deps: fq-common, arrow (58, array/schema only), object_store (0.13),
        rusqlite (0.32 bundled), serde/serde_json, thiserror (2),
        zstd (0.13), lz4_flex (0.13), tokio (rt, for object_store IO,
        current-thread like fq-accel)
  NO new external dependencies: parallelism is std::thread::scope with an
  AtomicUsize work index over the shard list (coarse tasks; rayon/crossbeam
  are not in the lockfile and are not needed), hashing/checksums are the
  ~15-line fnv1a64 + splitmix64 primitives.
```

Modules (each with the crate-doc responsibility statement per house
style):

- `model` - dataset descriptor, property schema, the width-adaptive
  `CodeVec`, canonical actor-key encoding.
- `dict` - build-time dictionary maps with memory accounting + promotion;
  generation-file read/write.
- `format` - the `.seg`/`.act`/`.dict` byte formats: block encoders/
  decoders per column kind, region framing, checksums, footers. Every
  encoding is an exhaustive enum match.
- `store` - object_store IO, layout paths, generation publish/sweep/
  unlink discipline, the decoded-block LRU cache.
- `registry` - the `event_datasets` table: open/register/publish/
  tombstone/purge (rusqlite, ViewCatalog discipline).
- `build` - B1 partition (spill writer, ordinals), B2 finalize (dedup,
  id assignment, sort, whale fallback, encode), the coordinator and
  memory governor.
- `refresh` - watermark delta, generation append, compaction, rebuild.
- `cursor` - `ActorEvents`: per-shard actor iteration with the
  generation heap-merge and the single-run fast path.
- `exec` - the four analysis executors + predicate compilation +
  partial-merge + exact-quantile selection (with spill).
- `error` - `EventError` and its component enums.

Changes to existing crates (all additive):

- `fq-common`: `events.rs` value types; `EventsConfig` in config.
- `fq-parse`: `Statement` variants + the family grammar + predicate
  parser.
- `fq-runtime`: `events.rs` handlers, `execute`/`describe` arms,
  `execute_source_stream`, `SettingDef` registrations, `RuntimeError`
  variant.
- `benchmarks/events/run_events.py`: the suite runner (new).

### 11.2 Phases and gates

Phase E1 - format, build, funnel, segmentation (parquet source):

- Scope: `model/dict/format/store/registry/build` complete for initial
  build (single generation); `cursor` single-run path; funnel +
  segmentation executors; CREATE/DROP/SHOW + FUNNEL/SEGMENT statements
  end to end through the CLI front door; runner modes gen-crafted/
  save-refs/engine/compare for the built parts.
- Tests: format round-trip + fuzzed-corruption raises; DP property tests
  vs brute force; battery subset vs oracle on `events_small` +
  `crafted_subsecond`/`crafted_sparse`.
- Gate: battery subset EXACT on small + crafted; 100M build <= 20 s /
  <= 6 GiB RSS (runner-reported); funnel warm <= 1 s on 100M.

Phase E2 - retention, paths, full surface, generality:

- Scope: retention + paths executors; RETENTION/PATHS/REBUILD statements;
  predicate grammar + breakdowns everywhere; unknown-name guard;
  dictionary promotion; whale/spill fallbacks; describe() arms; all three
  front doors.
- Tests: full battery on small + medium; ALL crafted datasets including
  `crafted_uuid` collision pair, `crafted_wide` promotion,
  `crafted_spill` byte-determinism, `crafted_nulls` negative tests;
  10K-case property fuzz in CI.
- Gate: full battery EXACT at 100M (`--mode compare` green); latency
  table of section 10 met warm AND cold; front-door smoke (python,
  PG-wire Describe+Execute, CLI) green.

Phase E3 - refresh and hardening (PRD phase 2 head):

- Scope: REFRESH watermark append, generation merge cursor, compaction,
  `crafted_refresh` battery, `SHOW` freshness fields; perf tail from E2
  gate measurements.
- Tests: refresh-vs-rebuild equivalence battery; compaction determinism;
  crash-point tests (kill between publish steps, reopen, sweep).
- Gate: refresh equivalence exact; append of 1% delta to 100M <= 3 s;
  no E2 gate regression.

Explicitly deferred beyond E3 (PRD phases 2-3): ANY_ORDER funnels
(grammar reserved, raises), actor-level property predicates, hour-grain
pre-aggregation, S3 backend (format is object_store-clean by
construction), per-dataset grants, distribution.

---

## 12. Risks and rejected alternatives

### 12.1 Hard problems and how this design meets them

- Billion-row bounded-memory build: met structurally (per-shard peak is
  set by `target_shard_bytes`, S scales with data, dictionaries are
  budgeted with a defined overflow behavior, sorts are per shard, whale
  shards degrade to external merge). Residual risk: spill IO volume
  (~2.5x store size) makes build disk-bandwidth-bound on slow disks;
  the builder reports spill bytes and per-phase time so this is visible,
  and `target_shard_bytes` can be raised on big-RAM boxes to cut spill
  passes. Not a correctness risk.
- Property-breakdown cardinality: group maps are data-bounded and spill
  to sorted runs past the query budget; the storage side never caps
  (width-adaptive codes, promotion to raw strings). Residual risk: a
  breakdown by a ~million-value property is legal and returns a
  million-series result - slow but exact and bounded; the doc says so and
  the spill path is tested.
- Path enumeration cost: exponential in principle, bounded here by
  DEPTH <= 10, TOP selection after exact counting, and spill-merge for
  map overflow. Residual risk: a 5,000-name dataset at depth 10 could
  spill heavily; the statement bound plus the spill path make it slow,
  never wrong or OOM.
- Same-instant correctness: solved by the total order contract
  (3.4) + the AmbiguousOrder guard + shuffle-invariance tests. Residual
  risk: synthetic tiebreaks on non-deterministic SQL sources are stable
  only per build; surfaced in SHOW and docs rather than hidden.
- Exactness of the fast paths: every fast path (pre-agg, first_ts
  short-circuit, block skips) is an algebraic rewrite of the scan, and
  each is battery-compared against the oracle directly; the block-skip
  code path is additionally exercised by `crafted_wide` (skips actually
  fire there).

### 12.2 Rejected alternatives (with reasons)

- Store as materialized views in fq-accel's ChunkStore: rejected -
  ChunkStore is relational-Arrow-only end to end (write API, schema
  guard, DataFusion read path, name grammar), and a CSR + dictionary +
  index format cannot round-trip it; wrapping indexes as Binary columns
  fails its own schema guard. Reusing its DISCIPLINE without its bytes
  keeps both stores honest.
- Time-major layout (sort by ts, like the origin): rejected - it makes
  segmentation pruning trivial but destroys per-actor sequence locality,
  which is the entire funnel/retention/paths win (DuckDB's 38.8 s funnel
  IS the time-major cost). Segmentation is instead served by pre-aggs.
- Per-event-name roaring-bitmap inverted indexes: rejected for phase 1 -
  measured full-scan speed (142 ms/100M/core) already beats the latency
  target 10x, block code-sets capture the rare-event win at ~0 cost, and
  `roaring` would be a new dependency maintained for no measured benefit.
  Re-evaluate with runner data at 1B+ if cold rare-event funnels miss.
- DataFusion-based analysis execution (express analyses as plans over the
  store): rejected - window-heavy per-actor logic is exactly what generic
  engines do slowly (the oracle numbers), and the custom executors are
  ~200 lines each over one cursor abstraction.
- Global dense actor ids (single dictionary): rejected - a global
  hash->id map is the one structure that grows with total actors and
  must be resident at build; per-shard identity bounds it to 1/S with
  zero correctness cost (distincts sum across disjoint shards).
- `id - min` dense mapping for integer actors: rejected outright -
  PRD non-negotiable; also collapses under one outlier id.
- Arrow-IPC or Parquet as the segment format: rejected - Arrow-IPC lacks
  the delta/varint encodings that halve the dominant ts column and lacks
  block-level skip metadata; Parquet has the encodings but hides block
  layout behind a generic reader, buys nothing for the CSR/actor
  directory/pre-agg sections which would live in sidecars anyway, and
  couples query latency to a general-purpose decode path. The custom
  format is ~600 lines of exhaustively-matched encode/decode with
  checksums, and the probe numbers justify owning it.
- Sessionized paths (gap-based sessions as first-class): rejected for
  phase 1 - the PRD's four analyses do not include sessions; `MAXGAP`
  covers the practical need at query time without a second materialized
  notion of order.

### 12.3 Open risks, stated plainly

- The cold-latency targets at 1B assume ~2 GB/s sequential NVMe read;
  slower storage moves cold funnel to ~5-8 s at 1B (warm is unaffected).
  If that matters for a deployment, the phase-3 S3/backend work should
  add column-region prefetch hints; nothing in the format blocks it.
- `execute_source_stream` is a new runtime seam; cross-source SELECT
  sources still materialize upstream of it (their memory belongs to the
  query pipeline's spill pool, not the builder), so a billion-row
  CROSS-SOURCE defining SELECT is not bounded by this design - parquet/
  single-source pulls are. Stated in the CREATE docs; the builder cannot
  fix the query engine's materialization boundary from below.
- ISO-week/UTC bucketing is pinned and oracle-matched, but analysts may
  expect local-time weeks; a timezone clause is deliberately absent in
  phase 1 (correct and explicit beats configurable and untested) and is
  the first candidate for the phase-2 surface.
