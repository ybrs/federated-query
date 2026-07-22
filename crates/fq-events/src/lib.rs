//! fq-events: the materialized event-analytics store and its four analysis
//! executors (funnels, retention, segmentation, paths).
//!
//! The store holds each dataset as sharded, actor-major, time-sorted,
//! dictionary-coded columnar segment files with per-file indexes (actor
//! directory, block skip stats, event-by-day pre-aggregates), built by a
//! bounded-memory two-phase spill pipeline and registered in the engine's
//! stats SQLite. Analyses are streaming per-shard scans over that layout;
//! they never contact the origin source. fq-runtime owns the statement
//! surface and the source pulls; this crate owns everything below.

pub mod build;
mod build_external;
pub mod cursor;
pub mod dict;
pub mod error;
pub mod exec;
pub mod format;
pub mod model;
mod paths_sa;
mod sais;
pub mod registry;
pub mod store;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use fq_common::events::{EventDatasetDef, EventsConfig, TimeUnit};

use crate::build::{run_build, BatchSource, BuildParams, BuildSpec, DictSeeds};
use crate::error::{EventError, EventStoreError};
use crate::format::{ActorSidecar, ColumnKind};
use crate::model::{DatasetRecord, FileList, GenerationEntry, PropertyEncoding, EVENT_DICT};
use crate::registry::{location_for, Registry};
use crate::store::{read_sidecar, CachedItem, StoreIo};

pub use crate::build::peak_rss_bytes;
pub use crate::error::{
    EventBindingError, EventBuildError, EventRefreshError, EventSchemaError, EventTypeError,
    EventUnknownName, EventUnsupported,
};
pub use crate::exec::{
    run_funnel, run_paths, run_retention, run_segment, FunnelResult, FunnelRow, PathsResult,
    PathsRow, RetentionResult, RetentionRow, SegmentResult, SegmentRow,
};

/// The event store of one engine configuration: the registry rows in the
/// config's stats SQLite plus the segment files under the sibling
/// `<config-stem>.events/` directory.
pub struct EventStore {
    io: StoreIo,
    registry: Registry,
    // Loaded datasets by name, revalidated against the live registry row on
    // every fetch (file list + pinned dictionary counts), so a statement
    // never re-reads the dictionary files of an unchanged dataset and a
    // peer's refresh is picked up on the next fetch.
    datasets: Mutex<HashMap<String, Arc<Dataset>>>,
}

/// What a build reports (measured, never estimated).
#[derive(Debug, Clone)]
pub struct BuildReport {
    pub events: u64,
    pub actors: u64,
    pub byte_size: u64,
    pub shards: u32,
    pub build_millis: u64,
    pub partition_millis: u64,
    pub finalize_millis: u64,
    pub spill_bytes: u64,
    /// This process's peak RSS after the build (`VmHWM`).
    pub peak_rss_bytes: Option<u64>,
    /// Properties promoted to raw-string encoding during the build.
    pub promoted: Vec<String>,
    /// The paths pre-aggregate summary (depths, window count, build seconds -
    /// or why it was skipped), for the DDL status line.
    pub paths_index: String,
}

/// Everything the runtime resolves about a CREATE source before the pull.
pub struct SourceInfo {
    /// `table` or `select`.
    pub kind: String,
    /// `datasource.schema.table` or the defining SELECT text.
    pub reference: String,
    /// The source's estimated row count (parquet footer counts are exact),
    /// for shard-count derivation; None derives the minimum shard count.
    pub estimated_rows: Option<u64>,
    /// The initial watermark JSON (max refresh-key over the pulled rows).
    pub watermark: Option<String>,
    /// The source's version token at pull time.
    pub source_token: Option<String>,
}

/// A loaded dataset: the registry record plus its dictionaries and the
/// per-shard file lists, ready for analyses.
pub struct Dataset {
    pub record: DatasetRecord,
    pub kinds: Arc<Vec<ColumnKind>>,
    pub event_dict: DictLookup,
    /// Per property (positionally): the dictionary of a dict-encoded
    /// property, None otherwise.
    pub prop_dicts: Vec<Option<DictLookup>>,
    /// Per shard: the shard's segment files, oldest generation first.
    shard_segs: Vec<(u32, Vec<String>)>,
}

/// One loaded dictionary: values in code order plus the reverse map.
pub struct DictLookup {
    pub values: Vec<String>,
    pub map: HashMap<String, u64>,
}

impl DictLookup {
    /// A lookup over values in code order.
    fn new(values: Vec<String>) -> Self {
        let mut map = HashMap::with_capacity(values.len());
        for (code, value) in values.iter().enumerate() {
            map.insert(value.clone(), code as u64);
        }
        Self { values, map }
    }
}

impl Dataset {
    /// The per-shard segment file lists.
    pub fn shard_files(&self) -> &[(u32, Vec<String>)] {
        &self.shard_segs
    }

    /// The event name of `code`.
    pub fn event_name(&self, code: u64) -> &str {
        &self.event_dict.values[usize::try_from(code).expect("code fits usize")]
    }

    /// The dictionary value of property `prop` at `code`.
    pub fn dict_value(&self, prop: usize, code: u64) -> &str {
        let dict = self.prop_dicts[prop]
            .as_ref()
            .expect("dict values come from dict properties");
        &dict.values[usize::try_from(code).expect("code fits usize")]
    }
}

impl EventStore {
    /// Open the store: the registry inside `stats_sqlite`, files under
    /// `events_root`. Finishes any interrupted DROP (tombstoned rows whose
    /// files still exist).
    pub fn open(
        stats_sqlite: &std::path::Path,
        events_root: &std::path::Path,
    ) -> Result<Self, EventError> {
        let io = StoreIo::open(events_root)?;
        let registry = Registry::open(stats_sqlite)?;
        let this = Self {
            io,
            registry,
            datasets: Mutex::new(HashMap::new()),
        };
        for (name, location) in this.registry.tombstoned()? {
            this.delete_location(&location)?;
            this.registry.purge(&name)?;
        }
        Ok(this)
    }

    /// The store's IO facade (analysis entry points read through it).
    pub fn io(&self) -> &StoreIo {
        &self.io
    }

    /// Delete every file under a dataset location.
    fn delete_location(&self, location: &str) -> Result<(), EventStoreError> {
        for rel in self.io.list(location)? {
            self.io.delete(&rel)?;
        }
        Ok(())
    }

    /// Every live dataset record, name-ordered.
    pub fn list(&self) -> Result<Vec<DatasetRecord>, EventError> {
        Ok(self.registry.list()?)
    }

    /// Whether a live dataset named `name` exists.
    pub fn exists(&self, name: &str) -> Result<bool, EventError> {
        Ok(self.registry.get_optional(name)?.is_some())
    }

    /// The build spec of a dataset definition.
    fn build_spec(def: &EventDatasetDef, declared: Option<Vec<model::PropertyDef>>) -> BuildSpec {
        BuildSpec {
            actor_column: def.actor_column.clone(),
            time_column: def.time_column.clone(),
            event_column: def.event_column.clone(),
            tiebreak_column: def.tiebreak_column.clone(),
            properties: def.properties.clone(),
            time_unit: def.time_unit,
            declared,
            dataset: def.name.clone(),
        }
    }

    /// The shard count of a new dataset: the declared override, else the
    /// clamp of estimated raw bytes over the target shard size to a power of
    /// two in 64..=65536.
    fn derive_shards(
        def: &EventDatasetDef,
        estimated_rows: Option<u64>,
        cfg: &EventsConfig,
    ) -> u32 {
        if let Some(shards) = def.shards {
            return shards;
        }
        // Estimated raw columnar bytes per event: ts + tiebreak + actor hash
        // routing state + event code + a small per-property share.
        let est_row_bytes = 40u64;
        let est_bytes = estimated_rows.unwrap_or(0).saturating_mul(est_row_bytes);
        let target = (est_bytes / cfg.target_shard_bytes.max(1)).max(1);
        u32::try_from(target.next_power_of_two().clamp(64, 65_536)).expect("clamped to u32 range")
    }

    /// `CREATE EVENT DATASET`: run the initial build (generation 0), write
    /// every file, then publish the registry row in one transaction. Any
    /// failure removes the files it wrote; a torn build is never visible.
    pub fn create_dataset(
        &self,
        def: &EventDatasetDef,
        info: &SourceInfo,
        source: &mut dyn BatchSource,
        cfg: &EventsConfig,
    ) -> Result<BuildReport, EventError> {
        if self.exists(&def.name)? {
            return Err(EventStoreError::NameTaken {
                name: def.name.clone(),
            }
            .into());
        }
        let location = location_for(&def.name);
        let shards = Self::derive_shards(def, info.estimated_rows, cfg);
        let params = BuildParams {
            shards,
            generation: 0,
            memory_bytes: cfg.build_memory_bytes,
            threads: cfg.build_threads,
            dict_max_bytes: cfg.dict_max_bytes,
            ordinal_start: 0,
        };
        let spec = Self::build_spec(def, None);
        let started = std::time::Instant::now();
        let outcome = match run_build(
            &self.io,
            &location,
            &spec,
            &params,
            DictSeeds::default(),
            &[],
            source,
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                // Remove any generation files the failed build wrote.
                self.delete_location(&location).ok();
                return Err(error);
            }
        };
        let record = DatasetRecord {
            name: def.name.clone(),
            location: location.clone(),
            source_kind: info.kind.clone(),
            source_ref: info.reference.clone(),
            actor_column: def.actor_column.clone(),
            time_column: def.time_column.clone(),
            time_unit: match def.time_unit {
                TimeUnit::Micros => "us".to_string(),
                TimeUnit::Millis => "ms".to_string(),
            },
            event_column: def.event_column.clone(),
            tiebreak_column: def.tiebreak_column.clone(),
            properties: outcome.properties.clone(),
            shards,
            file_list: FileList {
                generations: vec![GenerationEntry {
                    generation: 0,
                    shards: outcome.shard_entries.clone(),
                    dicts: outcome.dict_entries.clone(),
                }],
            },
            dict_state: outcome.dict_state.clone(),
            refresh_key: def.refresh_key.clone(),
            // The source tracker saw every pulled row; its maximum is the
            // initial watermark a later delta refresh filters past.
            watermark: source.watermark(),
            source_token: info.source_token.clone(),
            measured_events: i64::try_from(outcome.events).expect("event count fits i64"),
            measured_actors: i64::try_from(outcome.new_actors).expect("actor count fits i64"),
            byte_size: i64::try_from(outcome.byte_size).expect("byte size fits i64"),
            build_millis: i64::try_from(outcome.partition_millis + outcome.finalize_millis)
                .expect("millis fit i64"),
            min_ts: outcome.min_ts,
            max_ts: outcome.max_ts,
            created_at: chrono::Utc::now().to_rfc3339(),
            refreshed_at: None,
        };
        if let Err(error) = self.registry.insert(&record) {
            self.delete_location(&location).ok();
            return Err(error.into());
        }
        // The paths pre-aggregate is part of the create: a failure rolls the
        // whole dataset back (tombstone + delete + purge, the DROP sequence),
        // so a created dataset always carries a current index.
        let paths_index = match self.build_paths_index(&def.name, cfg) {
            Ok(summary) => summary,
            Err(error) => {
                self.registry.tombstone(&def.name).ok();
                self.delete_location(&location).ok();
                self.registry.purge(&def.name).ok();
                return Err(error);
            }
        };
        Ok(Self::report(&outcome, shards, started, paths_index))
    }

    /// Load every dataset's paths index into the warm block cache. Runs on a
    /// background thread right after the store opens; an error surfaces again
    /// on the first real statement, which raises it loudly.
    pub fn prewarm_paths(&self, cfg: &EventsConfig) -> Result<(), EventError> {
        for record in self.list()? {
            let dataset = self.dataset(&record.name)?;
            paths_sa::prewarm(&self.io, &dataset, cfg)?;
        }
        Ok(())
    }

    /// Rebuild the dataset's paths pre-aggregate over its CURRENT generation
    /// set, returning the status summary.
    fn build_paths_index(&self, name: &str, cfg: &EventsConfig) -> Result<String, EventError> {
        let dataset = self.dataset(name)?;
        paths_sa::rebuild(&self.io, &dataset, cfg)
    }

    /// The build report of an outcome.
    fn report(
        outcome: &build::BuildOutcome,
        shards: u32,
        started: std::time::Instant,
        paths_index: String,
    ) -> BuildReport {
        let promoted = outcome
            .properties
            .iter()
            .filter(|def| def.encoding == PropertyEncoding::RawString)
            .map(|def| def.name.clone())
            .collect();
        BuildReport {
            events: outcome.events,
            actors: outcome.new_actors,
            byte_size: outcome.byte_size,
            shards,
            build_millis: started.elapsed().as_millis() as u64,
            partition_millis: outcome.partition_millis,
            finalize_millis: outcome.finalize_millis,
            spill_bytes: outcome.spill_bytes,
            peak_rss_bytes: peak_rss_bytes(),
            promoted,
            paths_index,
        }
    }

    /// Load a dataset for analysis: the record, its dictionaries (verified
    /// against the pinned code counts), and the per-shard file lists. Served
    /// from the in-process cache when the registry row still names the same
    /// files and dictionary counts; anything else reloads.
    pub fn dataset(&self, name: &str) -> Result<Arc<Dataset>, EventError> {
        let record = self.registry.get(name)?;
        {
            let cache = self.datasets.lock().expect("dataset cache lock poisoned");
            if let Some(cached) = cache.get(name) {
                if cached.record.file_list == record.file_list
                    && cached.record.dict_state == record.dict_state
                {
                    return Ok(Arc::clone(cached));
                }
            }
        }
        let loaded = Arc::new(self.load_dataset(record)?);
        self.datasets
            .lock()
            .expect("dataset cache lock poisoned")
            .insert(name.to_string(), Arc::clone(&loaded));
        Ok(loaded)
    }

    /// Assemble a dataset from its registry row (dictionaries and file lists).
    fn load_dataset(&self, record: DatasetRecord) -> Result<Dataset, EventError> {
        let mut kinds = vec![ColumnKind::Time, ColumnKind::Tiebreak, ColumnKind::Event];
        for def in &record.properties {
            kinds.push(ColumnKind::Prop(def.encoding));
        }
        let dict_values = self.load_dictionaries(&record)?;
        let event_dict = DictLookup::new(dict_values.get(EVENT_DICT).cloned().unwrap_or_default());
        let mut prop_dicts = Vec::with_capacity(record.properties.len());
        for def in &record.properties {
            if def.encoding == PropertyEncoding::Dict {
                prop_dicts.push(Some(DictLookup::new(
                    dict_values.get(&def.name).cloned().unwrap_or_default(),
                )));
            } else {
                prop_dicts.push(None);
            }
        }
        let mut per_shard: HashMap<u32, Vec<String>> = HashMap::new();
        for generation in &record.file_list.generations {
            for entry in &generation.shards {
                per_shard
                    .entry(entry.shard)
                    .or_default()
                    .push(entry.seg.clone());
            }
        }
        let mut shard_segs: Vec<(u32, Vec<String>)> = per_shard.into_iter().collect();
        shard_segs.sort_by_key(|(shard, _)| *shard);
        Ok(Dataset {
            record,
            kinds: Arc::new(kinds),
            event_dict,
            prop_dicts,
            shard_segs,
        })
    }

    /// Load every dictionary column of a dataset by concatenating its
    /// generation files in order; a count mismatch against the pinned
    /// `dict_state` is store corruption and raises.
    fn load_dictionaries(
        &self,
        record: &DatasetRecord,
    ) -> Result<HashMap<String, Vec<String>>, EventError> {
        let mut loaded: HashMap<String, Vec<String>> = HashMap::new();
        for generation in &record.file_list.generations {
            for entry in &generation.dicts {
                let bytes = self.io.get(&entry.file)?;
                let (column, first_code, values) = format::decode_dict(&bytes, &entry.file)?;
                if column != entry.column {
                    return Err(EventStoreError::Corrupt {
                        file: entry.file.clone(),
                        detail: format!(
                            "dictionary file is for column '{column}', registry says '{}'",
                            entry.column
                        ),
                    }
                    .into());
                }
                let slot = loaded.entry(column).or_default();
                if slot.len() as u64 != first_code {
                    return Err(EventStoreError::Corrupt {
                        file: entry.file.clone(),
                        detail: format!(
                            "dictionary generations are not contiguous: have {} codes, file \
                             starts at {first_code}",
                            slot.len()
                        ),
                    }
                    .into());
                }
                slot.extend(values);
            }
        }
        for (column, pinned) in &record.dict_state {
            let have = loaded.get(column).map_or(0, Vec::len) as u64;
            if have != *pinned {
                return Err(EventStoreError::Corrupt {
                    file: format!("{}/dict/{column}", record.location),
                    detail: format!(
                        "dictionary '{column}' loads {have} codes but the registry pins {pinned}"
                    ),
                }
                .into());
            }
        }
        Ok(loaded)
    }

    /// The prior sidecars of every shard (for a refresh's id continuation),
    /// indexed by shard.
    fn load_sidecars(
        &self,
        record: &DatasetRecord,
    ) -> Result<Vec<Vec<Arc<ActorSidecar>>>, EventError> {
        let mut per_shard: Vec<Vec<Arc<ActorSidecar>>> =
            (0..record.shards).map(|_| Vec::new()).collect();
        for generation in &record.file_list.generations {
            for entry in &generation.shards {
                let item = read_sidecar(&self.io, &entry.act, u64::MAX)?;
                let CachedItem::Sidecar(ref sidecar) = *item else {
                    unreachable!("sidecar cache slot holds a sidecar");
                };
                per_shard[entry.shard as usize].push(Arc::new(sidecar.clone()));
            }
        }
        Ok(per_shard)
    }

    /// `REFRESH EVENT DATASET` (the delta append): run the build over the
    /// delta rows only as the next generation, then publish. Returns the
    /// appended event count.
    pub fn append_generation_with(
        &self,
        name: &str,
        source: &mut dyn BatchSource,
        source_token: Option<String>,
        cfg: &EventsConfig,
    ) -> Result<u64, EventError> {
        let mut record = self.registry.get(name)?;
        let dataset = self.dataset(name)?;
        let generation = record
            .file_list
            .generations
            .last()
            .map_or(0, |entry| entry.generation + 1);
        let params = BuildParams {
            shards: record.shards,
            generation,
            memory_bytes: cfg.build_memory_bytes,
            threads: cfg.build_threads,
            dict_max_bytes: cfg.dict_max_bytes,
            ordinal_start: u64::try_from(record.measured_events).expect("count is non-negative"),
        };
        let def = self.def_of(&record);
        let spec = Self::build_spec(&def, Some(record.properties.clone()));
        let mut seeds = DictSeeds {
            event: dataset.event_dict.values.clone(),
            props: HashMap::new(),
        };
        for (def, dict) in record.properties.iter().zip(&dataset.prop_dicts) {
            if let Some(dict) = dict {
                seeds.props.insert(def.name.clone(), dict.values.clone());
            }
        }
        let sidecars = self.load_sidecars(&record)?;
        let outcome = run_build(
            &self.io,
            &record.location,
            &spec,
            &params,
            seeds,
            &sidecars,
            source,
        )?;
        if outcome.properties != record.properties && !outcome.shard_entries.is_empty() {
            // A promotion during a refresh would leave earlier generations
            // dictionary-coded under a raw-string schema; the store refuses
            // the mixed state.
            for entry in &outcome.shard_entries {
                self.io.delete(&entry.seg).ok();
                self.io.delete(&entry.act).ok();
            }
            return Err(EventRefreshError::Watermark {
                name: name.to_string(),
                detail: "a property's dictionary outgrew its budget during the refresh; \
                         REBUILD EVENT DATASET to re-encode it as raw strings"
                    .to_string(),
            }
            .into());
        }
        record.file_list.generations.push(GenerationEntry {
            generation,
            shards: outcome.shard_entries.clone(),
            dicts: outcome.dict_entries.clone(),
        });
        record.dict_state = outcome.dict_state.clone();
        // The source tracker saw every pulled row, so its watermark is the
        // exact high-water mark of this append (seeded with the stored one,
        // an empty delta keeps it).
        record.watermark = source.watermark();
        record.source_token = source_token;
        record.measured_events += i64::try_from(outcome.events).expect("event count fits i64");
        record.measured_actors += i64::try_from(outcome.new_actors).expect("actor count fits i64");
        record.byte_size += i64::try_from(outcome.byte_size).expect("byte size fits i64");
        record.min_ts = build::merge_min(record.min_ts, outcome.min_ts);
        record.max_ts = build::merge_max(record.max_ts, outcome.max_ts);
        record.refreshed_at = Some(chrono::Utc::now().to_rfc3339());
        self.registry.publish_update(&record)?;
        // The appended generation invalidated the paths pre-aggregate (its
        // fingerprint no longer matches); rebuild it over the merged stream.
        // On a failure the append itself stays published and PATHS scans
        // (correct, slower); REBUILD EVENT DATASET restores the index.
        self.build_paths_index(name, cfg)?;
        Ok(outcome.events)
    }

    /// The dataset definition a stored record round-trips to (for re-running
    /// builds).
    pub fn def_of(&self, record: &DatasetRecord) -> EventDatasetDef {
        EventDatasetDef {
            name: record.name.clone(),
            source: fq_common::events::EventSource::Select {
                sql: record.source_ref.clone(),
            },
            actor_column: record.actor_column.clone(),
            time_column: record.time_column.clone(),
            event_column: record.event_column.clone(),
            tiebreak_column: record.tiebreak_column.clone(),
            properties: Some(
                record
                    .properties
                    .iter()
                    .map(|def| def.name.clone())
                    .collect(),
            ),
            shards: Some(record.shards),
            refresh_key: record.refresh_key.clone(),
            time_unit: if record.time_unit == "ms" {
                TimeUnit::Millis
            } else {
                TimeUnit::Micros
            },
        }
    }

    /// `REBUILD EVENT DATASET`: re-run the whole build from the source into a
    /// fresh generation-0 tree beside the live one, swap the registry state,
    /// then unlink the old tree. Local ids and dictionaries reset (nothing
    /// external holds internal ids).
    pub fn rebuild_dataset_with(
        &self,
        name: &str,
        source: &mut dyn BatchSource,
        source_token: Option<String>,
        cfg: &EventsConfig,
    ) -> Result<BuildReport, EventError> {
        let mut record = self.registry.get(name)?;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after 1970")
            .as_nanos();
        let rebuild_location = format!("{}/rebuild-{nanos}", record.location);
        let def = self.def_of(&record);
        let shards = Self::derive_shards(&def, u64::try_from(record.measured_events).ok(), cfg);
        let params = BuildParams {
            shards,
            generation: 0,
            memory_bytes: cfg.build_memory_bytes,
            threads: cfg.build_threads,
            dict_max_bytes: cfg.dict_max_bytes,
            ordinal_start: 0,
        };
        // A rebuild is a new dense-id world: no declared schema is pinned (a
        // promotion may resolve differently), no seeds, no priors.
        let spec = Self::build_spec(&def, None);
        let started = std::time::Instant::now();
        let outcome = match run_build(
            &self.io,
            &rebuild_location,
            &spec,
            &params,
            DictSeeds::default(),
            &[],
            source,
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.delete_location(&rebuild_location).ok();
                return Err(error);
            }
        };
        let old_files = Self::all_files(&record);
        record.properties.clone_from(&outcome.properties);
        record.shards = shards;
        record.file_list = FileList {
            generations: vec![GenerationEntry {
                generation: 0,
                shards: outcome.shard_entries.clone(),
                dicts: outcome.dict_entries.clone(),
            }],
        };
        record.dict_state = outcome.dict_state.clone();
        record.watermark = source.watermark();
        record.source_token = source_token;
        record.measured_events = i64::try_from(outcome.events).expect("event count fits i64");
        record.measured_actors = i64::try_from(outcome.new_actors).expect("actor count fits i64");
        record.byte_size = i64::try_from(outcome.byte_size).expect("byte size fits i64");
        record.build_millis =
            i64::try_from(outcome.partition_millis + outcome.finalize_millis).expect("millis fit");
        record.min_ts = outcome.min_ts;
        record.max_ts = outcome.max_ts;
        record.refreshed_at = Some(chrono::Utc::now().to_rfc3339());
        self.registry.publish_update(&record)?;
        for rel in old_files {
            self.io.delete(&rel)?;
        }
        let paths_index = self.build_paths_index(name, cfg)?;
        Ok(Self::report(&outcome, shards, started, paths_index))
    }

    /// Every file the record currently owns.
    fn all_files(record: &DatasetRecord) -> Vec<String> {
        let mut files = Vec::new();
        for generation in &record.file_list.generations {
            for entry in &generation.shards {
                files.push(entry.seg.clone());
                files.push(entry.act.clone());
            }
            for entry in &generation.dicts {
                files.push(entry.file.clone());
            }
        }
        files
    }

    /// `DROP EVENT DATASET`: tombstone, delete files, purge - crash-safe in
    /// that order (`open` finishes an interrupted drop).
    pub fn drop_dataset(&self, name: &str) -> Result<(), EventError> {
        let record = self.registry.get(name)?;
        self.registry.tombstone(name)?;
        self.delete_location(&record.location)?;
        self.registry.purge(name)?;
        self.datasets
            .lock()
            .expect("dataset cache lock poisoned")
            .remove(name);
        Ok(())
    }
}
