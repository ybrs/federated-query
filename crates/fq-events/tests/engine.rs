//! End-to-end store tests: build datasets from in-memory Arrow batches, run
//! the four analyses, and compare against independent brute-force
//! implementations of the declarative definitions, over seeded random inputs
//! with adversarial timestamp ties. Also proves the generality
//! non-negotiables: sparse and string actor ids, unbounded event-name
//! cardinality, forced-spill (external merge) build determinism, and loud
//! corruption failures.

// Generated test values are small by construction (bounded counts, bounded
// ordinals), so widening casts cannot wrap; the seeded battery reads better
// as one function than split across helpers.
#![allow(clippy::cast_possible_wrap, clippy::too_many_lines)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, TimeUnit as ArrowTimeUnit};

use fq_common::events::{
    BucketGrain, EventDatasetDef, EventMatch, EventSource, EventsConfig, FunnelMode, FunnelSpec,
    PathAnchor, PathsSpec, PeriodGrain, RetentionMode, RetentionSpec, SegmentMeasure, SegmentSpec,
    TimeRange, TimeUnit, MICROS_PER_DAY,
};
use fq_events::build::BatchSource;
use fq_events::error::EventError;
use fq_events::{EventStore, SourceInfo};

/// One generated event row.
#[derive(Debug, Clone)]
struct Row {
    actor: ActorId,
    ts: i64,
    tiebreak: i64,
    name: String,
    device: Option<String>,
}

/// A generated actor id: the two key families under test.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ActorId {
    Int(i64),
    Str(String),
}

/// A deterministic splitmix64 generator for the property tests.
struct Rng(u64);

impl Rng {
    /// The next raw 64-bit value.
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A value in `0..bound`.
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// A batch source over pre-built batches.
struct VecSource {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl BatchSource for VecSource {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }
}

/// Build the Arrow batch of a row set (string or int actors).
fn batch_of(rows: &[Row]) -> RecordBatch {
    let string_actors = rows.iter().any(|row| matches!(row.actor, ActorId::Str(_)));
    let actor_field = if string_actors {
        Field::new("actor", ArrowType::Utf8, false)
    } else {
        Field::new("actor", ArrowType::Int64, false)
    };
    let schema = Arc::new(Schema::new(vec![
        actor_field,
        Field::new(
            "ts",
            ArrowType::Timestamp(ArrowTimeUnit::Microsecond, None),
            false,
        ),
        Field::new("name", ArrowType::Utf8, false),
        Field::new("tb", ArrowType::Int64, false),
        Field::new("device", ArrowType::Utf8, true),
    ]));
    let actor: ArrayRef = if string_actors {
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            values.push(match &row.actor {
                ActorId::Str(text) => text.clone(),
                ActorId::Int(value) => value.to_string(),
            });
        }
        Arc::new(StringArray::from(values))
    } else {
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            match row.actor {
                ActorId::Int(value) => values.push(value),
                ActorId::Str(_) => unreachable!("mixed actor kinds in one dataset"),
            }
        }
        Arc::new(Int64Array::from(values))
    };
    let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
        rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
    ));
    let name: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|row| row.name.clone()).collect::<Vec<_>>(),
    ));
    let tb: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|row| row.tiebreak).collect::<Vec<_>>(),
    ));
    let device: ArrayRef = Arc::new(StringArray::from(
        rows.iter()
            .map(|row| row.device.clone())
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, vec![actor, ts, name, tb, device]).expect("batch builds")
}

/// A store in a fresh temp directory.
fn open_store(tag: &str) -> (EventStore, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fqev-test-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let store = EventStore::open(&dir.join("stats.sqlite"), &dir.join("events")).unwrap();
    (store, dir)
}

/// The dataset definition every test uses.
fn dataset_def(name: &str) -> EventDatasetDef {
    EventDatasetDef {
        name: name.to_string(),
        source: EventSource::Select {
            sql: "test".to_string(),
        },
        actor_column: "actor".to_string(),
        time_column: "ts".to_string(),
        event_column: "name".to_string(),
        tiebreak_column: Some("tb".to_string()),
        properties: Some(vec!["device".to_string()]),
        shards: Some(4),
        refresh_key: None,
        time_unit: TimeUnit::Micros,
    }
}

/// A small-config EventsConfig (tiny shards, modest budgets). The block cache
/// is effectively OFF (1 KiB budget, immediate eviction) so every read in
/// this suite exercises the disk path - the corruption tests depend on reads
/// actually touching the (possibly corrupted) bytes, and the index builds
/// would otherwise leave the columns cache-warm.
fn test_config() -> EventsConfig {
    EventsConfig {
        build_threads: 2,
        cache_bytes: 1024,
        ..EventsConfig::default()
    }
}

/// Build a dataset from rows and return it loaded.
fn build_dataset(store: &EventStore, name: &str, rows: &[Row]) -> std::sync::Arc<fq_events::Dataset> {
    let def = dataset_def(name);
    let info = SourceInfo {
        kind: "select".to_string(),
        reference: "test".to_string(),
        estimated_rows: Some(rows.len() as u64),
        watermark: None,
        source_token: None,
    };
    let mut source = VecSource {
        batches: vec![batch_of(rows)].into_iter(),
    };
    store
        .create_dataset(&def, &info, &mut source, &test_config())
        .unwrap();
    store.dataset(name).unwrap()
}

/// The stream order of a row set: `(actor, ts, tiebreak)`.
fn ordered(rows: &[Row]) -> BTreeMap<ActorId, Vec<Row>> {
    let mut per_actor: BTreeMap<ActorId, Vec<Row>> = BTreeMap::new();
    for row in rows {
        per_actor
            .entry(row.actor.clone())
            .or_default()
            .push(row.clone());
    }
    for events in per_actor.values_mut() {
        events.sort_by_key(|row| (row.ts, row.tiebreak));
    }
    per_actor
}

// ---------------------------------------------------------------------------
// Brute-force references (independent of the engine's algorithms)
// ---------------------------------------------------------------------------

/// Whether a chain of the remaining steps exists starting after `position`
/// within `deadline` (exhaustive search over every candidate, not greedy).
fn chain_exists(events: &[Row], steps: &[String], position: usize, deadline: i64) -> bool {
    if steps.is_empty() {
        return true;
    }
    for (index, row) in events.iter().enumerate().skip(position) {
        if row.name == steps[0]
            && row.ts <= deadline
            && chain_exists(events, &steps[1..], index + 1, deadline)
        {
            return true;
        }
    }
    false
}

/// The brute-force exists-form funnel depth of one actor.
fn funnel_depth_bf(events: &[Row], steps: &[String], window: i64) -> usize {
    let mut best = 0usize;
    for depth in (1..=steps.len()).rev() {
        for (index, row) in events.iter().enumerate() {
            if row.name == steps[0]
                && chain_exists(events, &steps[1..depth], index + 1, row.ts + window)
            {
                best = depth;
                break;
            }
        }
        if best == depth {
            break;
        }
    }
    best
}

/// The brute-force entered counts of a funnel over all actors.
fn funnel_bf(rows: &[Row], steps: &[String], window: i64) -> Vec<u64> {
    let mut entered = vec![0u64; steps.len()];
    for events in ordered(rows).values() {
        let depth = funnel_depth_bf(events, steps, window);
        for slot in entered.iter_mut().take(depth) {
            *slot += 1;
        }
    }
    entered
}

/// The day-grain period index of a timestamp.
fn day_index(ts: i64) -> i64 {
    ts.div_euclid(MICROS_PER_DAY)
}

/// The brute-force bounded retention cells: (cohort day, offset) -> retained,
/// plus cohort sizes, for BIRTH named / RETURN any at DAY grain.
fn retention_bf(rows: &[Row], birth: &str) -> (BTreeMap<i64, u64>, BTreeMap<(i64, i64), u64>) {
    let mut sizes: BTreeMap<i64, u64> = BTreeMap::new();
    let mut cells: BTreeMap<(i64, i64), u64> = BTreeMap::new();
    for events in ordered(rows).values() {
        let mut birth_at: Option<(usize, i64)> = None;
        for (index, row) in events.iter().enumerate() {
            if row.name == birth {
                birth_at = Some((index, day_index(row.ts)));
                break;
            }
        }
        let Some((birth_index, cohort)) = birth_at else {
            continue;
        };
        *sizes.entry(cohort).or_insert(0) += 1;
        let mut offsets: BTreeSet<i64> = BTreeSet::new();
        for row in &events[birth_index + 1..] {
            offsets.insert(day_index(row.ts) - cohort);
        }
        for offset in offsets {
            *cells.entry((cohort, offset)).or_insert(0) += 1;
        }
    }
    (sizes, cells)
}

/// The brute-force day-grain segmentation (count, uniques) per bucket.
fn segment_bf(rows: &[Row], event: Option<&str>) -> BTreeMap<i64, (u64, u64)> {
    let mut counts: BTreeMap<i64, u64> = BTreeMap::new();
    let mut actors: BTreeMap<i64, BTreeSet<ActorId>> = BTreeMap::new();
    for row in rows {
        if let Some(wanted) = event {
            if row.name != wanted {
                continue;
            }
        }
        let bucket = day_index(row.ts) * MICROS_PER_DAY;
        *counts.entry(bucket).or_insert(0) += 1;
        actors.entry(bucket).or_default().insert(row.actor.clone());
    }
    let mut merged = BTreeMap::new();
    for (bucket, count) in counts {
        merged.insert(bucket, (count, actors[&bucket].len() as u64));
    }
    merged
}

/// The brute-force unanchored path windows of `depth` (collapse duplicates,
/// full windows only).
fn paths_bf(rows: &[Row], depth: usize) -> BTreeMap<Vec<String>, u64> {
    let mut counts: BTreeMap<Vec<String>, u64> = BTreeMap::new();
    for events in ordered(rows).values() {
        let mut collapsed: Vec<String> = Vec::new();
        for row in events {
            if collapsed.last() != Some(&row.name) {
                collapsed.push(row.name.clone());
            }
        }
        for start in 0..collapsed.len() {
            if start + depth <= collapsed.len() {
                let window = collapsed[start..start + depth].to_vec();
                *counts.entry(window).or_insert(0) += 1;
            }
        }
    }
    counts
}

// ---------------------------------------------------------------------------
// The seeded property battery
// ---------------------------------------------------------------------------

/// Generate one random tiny event set with adversarial timestamp ties.
fn random_rows(rng: &mut Rng, string_actors: bool) -> Vec<Row> {
    let actor_count = 1 + rng.below(5);
    let event_count = 1 + rng.below(200);
    let names = ["alpha", "beta", "gamma", "delta"];
    let devices = [Some("x"), Some("y"), None];
    let mut rows = Vec::with_capacity(event_count as usize);
    for ordinal in 0..event_count {
        let actor_index = rng.below(actor_count) as i64;
        let actor = if string_actors {
            ActorId::Str(format!("user-{actor_index}"))
        } else {
            // Sparse ids: spread actors across the full i64 range.
            ActorId::Int(
                actor_index
                    .wrapping_mul(0x0123_4567_89ab_cdef)
                    .rotate_left(17),
            )
        };
        // Coarse timestamps force heavy (actor, ts) collisions so the
        // tiebreak genuinely orders events; some land on day boundaries.
        let ts = (rng.below(4) as i64) * MICROS_PER_DAY + (rng.below(6) as i64) * 250_000;
        rows.push(Row {
            actor,
            ts,
            tiebreak: ordinal as i64,
            name: names[rng.below(4) as usize].to_string(),
            device: devices[rng.below(3) as usize].map(str::to_string),
        });
    }
    rows
}

#[test]
fn random_inputs_match_brute_force_across_all_analyses() {
    let (store, dir) = open_store("prop");
    let mut rng = Rng(0x5eed_2026_0721);
    let cases = 60;
    for case in 0..cases {
        let string_actors = case % 2 == 1;
        let rows = random_rows(&mut rng, string_actors);
        let name = format!("case{case}");
        let dataset = build_dataset(&store, &name, &rows);
        // A random tiny set may lack one of the four names entirely; the
        // battery wants the empty-match semantics, not the typo guard.
        let mut cfg = test_config();
        cfg.allow_unknown_names = true;

        // Funnel: entered counts vs the exhaustive exists-form.
        let steps = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
        let spec = FunnelSpec {
            dataset: name.clone(),
            steps: steps.clone(),
            window_micros: MICROS_PER_DAY,
            mode: FunnelMode::StrictOrder,
            range: TimeRange::default(),
            filter: None,
            breakdown: None,
        };
        let result = fq_events::run_funnel(store.io(), &dataset, &spec, &cfg).unwrap();
        let expected = funnel_bf(&rows, &steps, MICROS_PER_DAY);
        let mut engine_entered = Vec::new();
        for row in &result.rows {
            engine_entered.push(row.entered);
        }
        assert_eq!(engine_entered, expected, "funnel case {case}");

        // Retention: bounded day grid vs brute force.
        let spec = RetentionSpec {
            dataset: name.clone(),
            birth: EventMatch::Named("alpha".to_string()),
            return_event: EventMatch::Any,
            period: PeriodGrain::Day,
            mode: RetentionMode::Bounded,
            periods: None,
            at: None,
            range: TimeRange::default(),
            filter: None,
            breakdown: None,
        };
        let result = fq_events::run_retention(store.io(), &dataset, &spec, &cfg).unwrap();
        let (sizes, cells) = retention_bf(&rows, "alpha");
        for row in &result.rows {
            let cohort = row.cohort_start / MICROS_PER_DAY;
            assert_eq!(
                row.cohort_size,
                sizes.get(&cohort).copied().unwrap_or(0),
                "retention size case {case} cohort {cohort}"
            );
            assert_eq!(
                row.retained,
                cells
                    .get(&(cohort, i64::from(row.period_offset)))
                    .copied()
                    .unwrap_or(0),
                "retention cell case {case} cohort {cohort} offset {}",
                row.period_offset
            );
        }

        // Segmentation: day counts + uniques vs brute force.
        let spec = SegmentSpec {
            dataset: name.clone(),
            measure: SegmentMeasure::Uniques,
            event: Some("beta".to_string()),
            bucket: BucketGrain::Day,
            range: TimeRange::default(),
            filter: None,
            breakdown: None,
        };
        let result = fq_events::run_segment(store.io(), &dataset, &spec, &cfg).unwrap();
        let expected = segment_bf(&rows, Some("beta"));
        assert_eq!(result.rows.len(), expected.len(), "segment case {case}");
        for row in &result.rows {
            let (count, uniques) = expected[&row.bucket];
            assert_eq!(row.count, count, "segment count case {case}");
            assert_eq!(row.uniques, uniques, "segment uniques case {case}");
        }

        // The pre-agg fast path answers the COUNT shape identically.
        let spec = SegmentSpec {
            dataset: name.clone(),
            measure: SegmentMeasure::Count,
            event: None,
            bucket: BucketGrain::Day,
            range: TimeRange::default(),
            filter: None,
            breakdown: None,
        };
        let result = fq_events::run_segment(store.io(), &dataset, &spec, &cfg).unwrap();
        let expected = segment_bf(&rows, None);
        for row in &result.rows {
            assert_eq!(row.count, expected[&row.bucket].0, "preagg case {case}");
        }

        // Paths: unanchored depth-3 window counts vs brute force.
        let spec = PathsSpec {
            dataset: name.clone(),
            anchor: PathAnchor::Unanchored,
            depth: 3,
            top: 1000,
            maxgap_micros: None,
            range: TimeRange::default(),
            filter: None,
        };
        let result = fq_events::run_paths(store.io(), &dataset, &spec, &cfg).unwrap();
        let expected = paths_bf(&rows, 3);
        let mut engine_counts: BTreeMap<String, u64> = BTreeMap::new();
        for row in &result.rows {
            engine_counts.insert(row.steps.join(" -> "), row.occurrences);
        }
        let mut expected_counts: BTreeMap<String, u64> = BTreeMap::new();
        for (window, count) in expected {
            expected_counts.insert(window.join(" -> "), count);
        }
        assert_eq!(engine_counts, expected_counts, "paths case {case}");

        store.drop_dataset(&name).unwrap();
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn sparse_int_and_string_ids_share_the_same_path_and_stay_distinct() {
    let (store, dir) = open_store("ids");
    // Sparse int64 ids across the full range, including extremes.
    let mut rows = Vec::new();
    let ids = [i64::MIN + 5, -1, 0, 1, i64::MAX - 3, 7_777_777_777_777];
    for (index, id) in ids.iter().enumerate() {
        rows.push(Row {
            actor: ActorId::Int(*id),
            ts: 1_000_000 * index as i64,
            tiebreak: index as i64,
            name: "alpha".to_string(),
            device: None,
        });
        rows.push(Row {
            actor: ActorId::Int(*id),
            ts: 1_000_000 * index as i64 + 500_000,
            tiebreak: 100 + index as i64,
            name: "beta".to_string(),
            device: None,
        });
    }
    let dataset = build_dataset(&store, "sparse", &rows);
    assert_eq!(dataset.record.measured_actors, ids.len() as i64);
    let spec = FunnelSpec {
        dataset: "sparse".to_string(),
        steps: vec!["alpha".to_string(), "beta".to_string()],
        window_micros: MICROS_PER_DAY,
        mode: FunnelMode::StrictOrder,
        range: TimeRange::default(),
        filter: None,
        breakdown: None,
    };
    let result = fq_events::run_funnel(store.io(), &dataset, &spec, &test_config()).unwrap();
    assert_eq!(result.rows[0].entered, ids.len() as u64);
    assert_eq!(result.rows[1].entered, ids.len() as u64);

    // String ids: integer 42 and string "42" are DIFFERENT actors, and UUID
    // strings work unchanged.
    let mut rows = Vec::new();
    let keys = ["42", "550e8400-e29b-41d4-a716-446655440000", "user-a"];
    for (index, key) in keys.iter().enumerate() {
        rows.push(Row {
            actor: ActorId::Str((*key).to_string()),
            ts: 1_000_000 * index as i64,
            tiebreak: index as i64,
            name: "alpha".to_string(),
            device: None,
        });
    }
    let dataset = build_dataset(&store, "uuid", &rows);
    assert_eq!(dataset.record.measured_actors, keys.len() as i64);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn high_event_name_cardinality_is_unbounded() {
    let (store, dir) = open_store("wide");
    // 5000 distinct event names: codes cross the one-byte width; nothing caps
    // the dictionary.
    let mut rows = Vec::new();
    for index in 0..5000i64 {
        rows.push(Row {
            actor: ActorId::Int(index % 7),
            ts: index * 1_000,
            tiebreak: index,
            name: format!("event_kind_{index}"),
            device: None,
        });
    }
    let dataset = build_dataset(&store, "wide", &rows);
    assert_eq!(dataset.event_dict.values.len(), 5000);
    // A funnel over names with codes far above 255 answers correctly.
    let spec = FunnelSpec {
        dataset: "wide".to_string(),
        steps: vec!["event_kind_4993".to_string(), "event_kind_4998".to_string()],
        window_micros: MICROS_PER_DAY,
        mode: FunnelMode::StrictOrder,
        range: TimeRange::default(),
        filter: None,
        breakdown: None,
    };
    let result = fq_events::run_funnel(store.io(), &dataset, &spec, &test_config()).unwrap();
    // Actor 4993 % 7 == 2 fired event_kind_4993 then actor 4998 % 7 == 0
    // fired event_kind_4998: same actor only if 4993 % 7 == 4998 % 7 (false),
    // so step two converts nobody, step one exactly one actor.
    assert_eq!(result.rows[0].entered, 1);
    assert_eq!(result.rows[1].entered, 0);
    // The whole 5000-name dictionary round-trips through the store.
    let reloaded = store.dataset("wide").unwrap();
    assert_eq!(reloaded.event_name(4999), "event_kind_4999");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn forced_spill_build_is_byte_identical_to_the_in_memory_build() {
    let (store, dir) = open_store("spill");
    let mut rng = Rng(77);
    let mut rows = Vec::new();
    for ordinal in 0..20_000 {
        rows.push(Row {
            actor: ActorId::Int(rng.below(50) as i64),
            ts: (rng.below(1000) as i64) * 100_000,
            tiebreak: ordinal,
            name: format!("name_{}", rng.below(6)),
            device: Some(format!("device_{}", rng.below(3))),
        });
    }
    // Default build (in-memory finalize path).
    build_dataset(&store, "plain", &rows);
    // Forced-spill build: a floor memory budget forces the external
    // sort-merge fallback for every shard.
    let def = EventDatasetDef {
        name: "forced".to_string(),
        ..dataset_def("forced")
    };
    let info = SourceInfo {
        kind: "select".to_string(),
        reference: "test".to_string(),
        estimated_rows: Some(rows.len() as u64),
        watermark: None,
        source_token: None,
    };
    let mut config = test_config();
    config.build_memory_bytes = 1;
    let mut source = VecSource {
        batches: vec![batch_of(&rows)].into_iter(),
    };
    store
        .create_dataset(&def, &info, &mut source, &config)
        .unwrap();
    // Both stores hold byte-identical segment files per shard (deterministic
    // build: same total order, same block boundaries).
    let plain = store.dataset("plain").unwrap();
    let forced = store.dataset("forced").unwrap();
    assert_eq!(plain.shard_files().len(), forced.shard_files().len());
    let mut compared = 0;
    for ((_, plain_files), (_, forced_files)) in
        plain.shard_files().iter().zip(forced.shard_files())
    {
        for (plain_rel, forced_rel) in plain_files.iter().zip(forced_files) {
            let plain_bytes = std::fs::read(dir.join("events").join(plain_rel)).unwrap();
            let forced_bytes = std::fs::read(dir.join("events").join(forced_rel)).unwrap();
            assert_eq!(plain_bytes, forced_bytes, "shard files differ");
            compared += 1;
        }
    }
    assert!(compared > 0, "no shard files compared");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn null_actor_and_ambiguous_order_raise() {
    let (store, dir) = open_store("nulls");
    // A NULL actor id raises with the row position.
    let schema = Arc::new(Schema::new(vec![
        Field::new("actor", ArrowType::Int64, true),
        Field::new(
            "ts",
            ArrowType::Timestamp(ArrowTimeUnit::Microsecond, None),
            false,
        ),
        Field::new("name", ArrowType::Utf8, false),
        Field::new("tb", ArrowType::Int64, false),
        Field::new("device", ArrowType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(TimestampMicrosecondArray::from(vec![0i64, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![0i64, 1])),
            Arc::new(StringArray::from(vec![None::<String>, None])),
        ],
    )
    .unwrap();
    let mut source = VecSource {
        batches: vec![batch].into_iter(),
    };
    let info = SourceInfo {
        kind: "select".to_string(),
        reference: "test".to_string(),
        estimated_rows: None,
        watermark: None,
        source_token: None,
    };
    let error = store
        .create_dataset(&dataset_def("nulls"), &info, &mut source, &test_config())
        .unwrap_err();
    assert!(
        matches!(
            error,
            EventError::Build(fq_events::EventBuildError::NullActor { row: 1 })
        ),
        "got {error}"
    );

    // Two events with the same (actor, ts, tiebreak) raise AmbiguousOrder.
    let rows = vec![
        Row {
            actor: ActorId::Int(9),
            ts: 5,
            tiebreak: 7,
            name: "a".to_string(),
            device: None,
        },
        Row {
            actor: ActorId::Int(9),
            ts: 5,
            tiebreak: 7,
            name: "b".to_string(),
            device: None,
        },
    ];
    let mut source = VecSource {
        batches: vec![batch_of(&rows)].into_iter(),
    };
    let error = store
        .create_dataset(
            &dataset_def("dupes"),
            &info_clone(),
            &mut source,
            &test_config(),
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            EventError::Build(fq_events::EventBuildError::AmbiguousOrder { .. })
        ),
        "got {error}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// A fresh SourceInfo (tests reuse the same shape).
fn info_clone() -> SourceInfo {
    SourceInfo {
        kind: "select".to_string(),
        reference: "test".to_string(),
        estimated_rows: None,
        watermark: None,
        source_token: None,
    }
}

#[test]
fn corrupted_segment_bytes_raise_loudly() {
    let (store, dir) = open_store("corrupt");
    let mut rows = Vec::new();
    for ordinal in 0..500 {
        rows.push(Row {
            actor: ActorId::Int(ordinal % 5),
            ts: ordinal * 1_000,
            tiebreak: ordinal,
            name: "alpha".to_string(),
            device: None,
        });
    }
    let dataset = build_dataset(&store, "corrupt", &rows);
    // Flip one byte inside the ts column region (it starts right after the
    // 44-byte header, and a segment scan always reads it).
    let (_, files) = &dataset.shard_files()[0];
    let path = dir.join("events").join(&files[0]);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[50] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
    let spec = SegmentSpec {
        dataset: "corrupt".to_string(),
        measure: SegmentMeasure::Uniques,
        event: None,
        bucket: BucketGrain::Day,
        range: TimeRange::default(),
        filter: None,
        breakdown: None,
    };
    let error = fq_events::run_segment(store.io(), &dataset, &spec, &test_config()).unwrap_err();
    assert!(
        matches!(
            error,
            EventError::Store(fq_events::error::EventStoreError::Corrupt { .. })
        ),
        "got {error}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn unknown_event_name_raises_with_suggestions() {
    let (store, dir) = open_store("names");
    let rows = vec![Row {
        actor: ActorId::Int(1),
        ts: 0,
        tiebreak: 0,
        name: "signup".to_string(),
        device: None,
    }];
    let dataset = build_dataset(&store, "names", &rows);
    let spec = FunnelSpec {
        dataset: "names".to_string(),
        steps: vec!["signup".to_string(), "sign_up".to_string()],
        window_micros: MICROS_PER_DAY,
        mode: FunnelMode::StrictOrder,
        range: TimeRange::default(),
        filter: None,
        breakdown: None,
    };
    let error = fq_events::run_funnel(store.io(), &dataset, &spec, &test_config()).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("sign_up"), "got {message}");
    assert!(message.contains("'signup'"), "got {message}");
    // The escape hatch turns the unknown name into an empty match.
    let mut config = test_config();
    config.allow_unknown_names = true;
    let result = fq_events::run_funnel(store.io(), &dataset, &spec, &config).unwrap();
    assert_eq!(result.rows[0].entered, 1);
    assert_eq!(result.rows[1].entered, 0);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn refresh_appends_a_generation_and_merges_by_time() {
    let (store, dir) = open_store("refresh");
    // Generation 0: two actors with early events.
    let first = vec![
        Row {
            actor: ActorId::Int(1),
            ts: 10_000_000,
            tiebreak: 0,
            name: "alpha".to_string(),
            device: None,
        },
        Row {
            actor: ActorId::Int(2),
            ts: 20_000_000,
            tiebreak: 1,
            name: "alpha".to_string(),
            device: None,
        },
    ];
    build_dataset(&store, "refresh", &first);
    // Generation 1 appends a LATE event for actor 1 (older ts than actor 2's
    // stored event) plus a new actor: the merge must interleave correctly.
    let second = vec![
        Row {
            actor: ActorId::Int(1),
            ts: 15_000_000,
            tiebreak: 2,
            name: "beta".to_string(),
            device: None,
        },
        Row {
            actor: ActorId::Int(3),
            ts: 30_000_000,
            tiebreak: 3,
            name: "alpha".to_string(),
            device: None,
        },
    ];
    let mut source = VecSource {
        batches: vec![batch_of(&second)].into_iter(),
    };
    let appended = store
        .append_generation_with("refresh", &mut source, None, &test_config())
        .unwrap();
    assert_eq!(appended, 2);
    let dataset = store.dataset("refresh").unwrap();
    assert_eq!(dataset.record.measured_events, 4);
    assert_eq!(dataset.record.measured_actors, 3);
    // The funnel sees actor 1's alpha -> beta across generations in time
    // order.
    let spec = FunnelSpec {
        dataset: "refresh".to_string(),
        steps: vec!["alpha".to_string(), "beta".to_string()],
        window_micros: MICROS_PER_DAY,
        mode: FunnelMode::StrictOrder,
        range: TimeRange::default(),
        filter: None,
        breakdown: None,
    };
    let result = fq_events::run_funnel(store.io(), &dataset, &spec, &test_config()).unwrap();
    assert_eq!(result.rows[0].entered, 3);
    assert_eq!(result.rows[1].entered, 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn paths_index_matches_the_scan_for_every_anchor_and_depth() {
    // Streams with short actors (truncated anchored windows), duplicates
    // (collapse), and shared prefixes, so every table family of the index is
    // exercised. MAXGAP i64::MAX forces the scan path while never splitting a
    // fragment, so scan and index answer the same question.
    let mut rng = Rng(0xfeed_beef);
    let names = ["a", "b", "c", "d"];
    let mut rows = Vec::new();
    for actor in 0..24_i64 {
        let length = 1 + rng.below(7) as i64;
        for position in 0..length {
            rows.push(Row {
                actor: ActorId::Int(actor),
                ts: position * 1_000_000,
                tiebreak: position,
                name: names[rng.below(names.len() as u64) as usize].to_string(),
                device: None,
            });
        }
    }
    let (store, dir) = open_store("pidx");
    let dataset = build_dataset(&store, "pidx", &rows);
    let cfg = test_config();
    let anchors = [
        PathAnchor::Unanchored,
        PathAnchor::StartingAt("a".to_string()),
        PathAnchor::EndingAt("c".to_string()),
    ];
    for anchor in &anchors {
        for depth in 2..=7 {
            let spec = |maxgap| PathsSpec {
                dataset: "pidx".to_string(),
                anchor: anchor.clone(),
                depth,
                top: 1000,
                maxgap_micros: maxgap,
                range: TimeRange::default(),
                filter: None,
            };
            let indexed = fq_events::run_paths(store.io(), &dataset, &spec(None), &cfg).unwrap();
            let scanned =
                fq_events::run_paths(store.io(), &dataset, &spec(Some(i64::MAX)), &cfg).unwrap();
            assert_eq!(indexed, scanned, "anchor {anchor:?} depth {depth}");
        }
    }

    // With the index file gone (a dataset from before the index existed, or a
    // crash between publish and index write), the same spec scans and agrees.
    for rel in store.io().list(&dataset.record.location).unwrap() {
        if rel.contains("/paths_") {
            store.io().delete(&rel).unwrap();
        }
    }
    let spec = PathsSpec {
        dataset: "pidx".to_string(),
        anchor: PathAnchor::StartingAt("a".to_string()),
        depth: 4,
        top: 1000,
        maxgap_micros: None,
        range: TimeRange::default(),
        filter: None,
    };
    let fallback = fq_events::run_paths(store.io(), &dataset, &spec, &cfg).unwrap();
    let mut gap_spec = spec;
    gap_spec.maxgap_micros = Some(i64::MAX);
    let scanned = fq_events::run_paths(store.io(), &dataset, &gap_spec, &cfg).unwrap();
    assert_eq!(fallback, scanned);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn ranged_paths_serve_anchor_semantics_from_ts_lists_and_match_the_scan() {
    // Timestamps are spread so ranges cut through streams mid-way. MAXGAP
    // i64::MAX forces the scan while never splitting a fragment, so index and
    // scan answer the same anchor-semantics question.
    let mut rng = Rng(0xabad_cafe);
    let names = ["a", "b", "c", "d"];
    let mut rows = Vec::new();
    for actor in 0..24_i64 {
        let length = 1 + rng.below(7) as i64;
        for position in 0..length {
            rows.push(Row {
                actor: ActorId::Int(actor),
                ts: actor * 10_000_000 + position * 1_000_000,
                tiebreak: position,
                name: names[rng.below(names.len() as u64) as usize].to_string(),
                device: None,
            });
        }
    }
    let (store, dir) = open_store("pidxts");
    let dataset = build_dataset(&store, "pidxts", &rows);
    let cfg = test_config();
    let anchors = [
        PathAnchor::Unanchored,
        PathAnchor::StartingAt("a".to_string()),
        PathAnchor::EndingAt("c".to_string()),
    ];
    let ranges = [
        (Some(40_000_000_i64), Some(160_000_000_i64)),
        (Some(3_000_000), None),
        (None, Some(120_500_000)),
    ];
    for anchor in &anchors {
        for depth in 2..=5 {
            for (from, to) in &ranges {
                let spec = |maxgap| PathsSpec {
                    dataset: "pidxts".to_string(),
                    anchor: anchor.clone(),
                    depth,
                    top: 1000,
                    maxgap_micros: maxgap,
                    range: TimeRange {
                        from: *from,
                        to: *to,
                    },
                    filter: None,
                };
                let indexed =
                    fq_events::run_paths(store.io(), &dataset, &spec(None), &cfg).unwrap();
                let scanned =
                    fq_events::run_paths(store.io(), &dataset, &spec(Some(i64::MAX)), &cfg)
                        .unwrap();
                assert_eq!(indexed, scanned, "anchor {anchor:?} depth {depth} {from:?}..{to:?}");
            }
        }
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_ranged_path_follows_its_anchor_past_the_range_end() {
    // Anchor semantics: the window whose FIRST event is in [from, to) is
    // counted whole, even though its later events fall past `to`.
    let rows = vec![
        Row {
            actor: ActorId::Int(1),
            ts: 1_000_000,
            tiebreak: 0,
            name: "a".to_string(),
            device: None,
        },
        Row {
            actor: ActorId::Int(1),
            ts: 2_000_000,
            tiebreak: 1,
            name: "b".to_string(),
            device: None,
        },
        Row {
            actor: ActorId::Int(1),
            ts: 3_000_000,
            tiebreak: 2,
            name: "c".to_string(),
            device: None,
        },
    ];
    let (store, dir) = open_store("anchorsem");
    let dataset = build_dataset(&store, "anchorsem", &rows);
    let cfg = test_config();
    let spec = PathsSpec {
        dataset: "anchorsem".to_string(),
        anchor: PathAnchor::Unanchored,
        depth: 3,
        top: 10,
        maxgap_micros: None,
        range: TimeRange {
            from: Some(1_000_000),
            to: Some(1_500_000),
        },
        filter: None,
    };
    let result = fq_events::run_paths(store.io(), &dataset, &spec, &cfg).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].steps, vec!["a", "b", "c"]);
    assert_eq!(result.rows[0].occurrences, 1);

    // The same window is NOT counted when the range excludes its anchor.
    let mut excluded = spec;
    excluded.range = TimeRange {
        from: Some(1_500_000),
        to: Some(2_500_000),
    };
    let result = fq_events::run_paths(store.io(), &dataset, &excluded, &cfg).unwrap();
    assert!(result.rows.is_empty(), "anchor outside the range matches nothing");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn indexable_paths_answer_from_the_index_without_touching_segments() {
    // The PROOF that indexable queries are served from the index: delete
    // every segment and actor file after the build, keeping only the index
    // files. An index-served query still answers; a scan-shaped query (MAXGAP
    // forces it) now fails because the data it needs is gone.
    let mut rng = Rng(0x1dea_11fe);
    let names = ["a", "b", "c", "d"];
    let mut rows = Vec::new();
    for actor in 0..24_i64 {
        let length = 2 + rng.below(6) as i64;
        for position in 0..length {
            rows.push(Row {
                actor: ActorId::Int(actor),
                ts: actor * 10_000_000 + position * 1_000_000,
                tiebreak: position,
                name: names[rng.below(names.len() as u64) as usize].to_string(),
                device: None,
            });
        }
    }
    let (store, dir) = open_store("pidxproof");
    let dataset = build_dataset(&store, "pidxproof", &rows);
    let cfg = test_config();
    let spec = |maxgap, range| PathsSpec {
        dataset: "pidxproof".to_string(),
        anchor: PathAnchor::Unanchored,
        depth: 3,
        top: 10,
        maxgap_micros: maxgap,
        range,
        filter: None,
    };
    let expected = fq_events::run_paths(store.io(), &dataset, &spec(None, TimeRange::default()), &cfg)
        .expect("indexed paths");
    assert!(!expected.rows.is_empty());

    for rel in store.io().list(&dataset.record.location).unwrap() {
        let name = rel.rsplit('/').next().unwrap();
        if !name.starts_with("paths_") {
            store.io().delete(&rel).unwrap();
        }
    }
    let from_index =
        fq_events::run_paths(store.io(), &dataset, &spec(None, TimeRange::default()), &cfg)
            .expect("index answers without segments");
    assert_eq!(from_index, expected);
    let ranged = fq_events::run_paths(
        store.io(),
        &dataset,
        &spec(
            None,
            TimeRange {
                from: Some(0),
                to: Some(i64::MAX),
            },
        ),
        &cfg,
    )
    .expect("ts lists answer without segments");
    assert_eq!(ranged, expected);
    assert!(
        fq_events::run_paths(store.io(), &dataset, &spec(Some(i64::MAX), TimeRange::default()), &cfg)
            .is_err(),
        "a scan-shaped query must fail once the segments are gone"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_property_promoted_mid_build_reads_every_row_back_exactly() {
    // Two batches under a tiny dictionary budget: batch one's devices fit the
    // dict (their rows spill CODES ONLY), batch two's flood of distinct
    // values promotes the property mid-build (its rows spill raw strings).
    // The finalize must materialize batch one's strings from the dictionary
    // snapshot, so a breakdown sees every value of both batches exactly.
    let mut first = Vec::new();
    for ordinal in 0..200_i64 {
        first.push(Row {
            actor: ActorId::Int(ordinal % 10),
            ts: ordinal * 1_000,
            tiebreak: ordinal,
            name: "click".to_string(),
            device: Some(if ordinal % 2 == 0 { "alpha" } else { "beta" }.to_string()),
        });
    }
    let mut second = Vec::new();
    for ordinal in 200..500_i64 {
        second.push(Row {
            actor: ActorId::Int(ordinal % 10),
            ts: ordinal * 1_000,
            tiebreak: ordinal,
            name: "click".to_string(),
            device: Some(format!("device-{ordinal}-{}", "x".repeat(64))),
        });
    }
    let (store, dir) = open_store("promomix");
    let def = dataset_def("promomix");
    let info = SourceInfo {
        kind: "select".to_string(),
        reference: "test".to_string(),
        estimated_rows: Some(500),
        watermark: None,
        source_token: None,
    };
    let mut source = VecSource {
        batches: vec![batch_of(&first), batch_of(&second)].into_iter(),
    };
    let cfg = EventsConfig {
        dict_max_bytes: 2048,
        ..test_config()
    };
    let report = store.create_dataset(&def, &info, &mut source, &cfg).unwrap();
    assert!(
        report.promoted.contains(&"device".to_string()),
        "the value flood must promote the property: {:?}",
        report.promoted
    );
    let dataset = store.dataset("promomix").unwrap();
    let spec = SegmentSpec {
        dataset: "promomix".to_string(),
        measure: SegmentMeasure::Count,
        event: None,
        bucket: BucketGrain::Day,
        range: TimeRange::default(),
        filter: None,
        breakdown: Some("device".to_string()),
    };
    let result = fq_events::run_segment(store.io(), &dataset, &spec, &cfg).unwrap();
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for row in &result.rows {
        let device = row.breakdown.clone().expect("breakdown value");
        *counts.entry(device).or_insert(0) += row.count;
    }
    // Batch one: 100 alpha + 100 beta rows resolved from the dictionary
    // snapshot; batch two: 300 distinct raw-string devices, one row each.
    assert_eq!(counts.get("alpha"), Some(&100));
    assert_eq!(counts.get("beta"), Some(&100));
    assert_eq!(counts.len(), 302);
    std::fs::remove_dir_all(&dir).ok();
}
