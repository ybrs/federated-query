//! Event-dataset DDL end to end over a single-file Parquet source with a
//! UTC-timestamp refresh key: CREATE builds and stores the watermark, a file
//! replacement with later rows makes REFRESH pull exactly the delta (the
//! watermark literal must compare correctly inside the source's own SQL
//! dialect), and a refresh key whose type cannot carry a watermark fails at
//! CREATE before any data is pulled.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_runtime::Runtime;
use parquet::arrow::ArrowWriter;
use serde_yaml::Value;

/// A fresh temp sandbox: holds the parquet file and the config path the event
/// store hangs off.
fn sandbox(tag: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fq_evds_{tag}_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create sandbox dir");
    dir
}

/// The event fixture schema: a UTC microsecond timestamp, an event name, and
/// an actor id (mirrors a real event feed's shape).
fn event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "created_at",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("event", ArrowDataType::Utf8, false),
        Field::new("user_id", ArrowDataType::Utf8, false),
    ]))
}

/// Write `micros`/`events`/`users` rows as one parquet file at `path`.
fn write_events(path: &Path, micros: Vec<i64>, events: Vec<&str>, users: Vec<&str>) {
    let created: ArrayRef = Arc::new(TimestampMicrosecondArray::from(micros).with_timezone("UTC"));
    let event: ArrayRef = Arc::new(StringArray::from(events));
    let user: ArrayRef = Arc::new(StringArray::from(users));
    let batch =
        RecordBatch::try_new(event_schema(), vec![created, event, user]).expect("event batch");
    let file = File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

/// A config with one single-file parquet datasource `name` and a source_path
/// inside the sandbox (which gives the runtime an event store).
fn event_config(dir: &Path, name: &str, file: &Path) -> Config {
    let mut params = BTreeMap::new();
    params.insert(
        "file".to_string(),
        Value::String(file.to_string_lossy().to_string()),
    );
    let mut datasources = BTreeMap::new();
    datasources.insert(
        name.to_string(),
        DataSourceConfig {
            name: name.to_string(),
            ty: "parquet".to_string(),
            config: params,
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    Config {
        datasources,
        optimizer: OptimizerConfig::default(),
        executor: ExecutorConfig::default(),
        cost: CostConfig::default(),
        server: ServerConfig::default(),
        accelerator: fq_common::AcceleratorConfig::default(),
        catalog: fq_common::CatalogConfig::default(),
        events: fq_common::EventsConfig::default(),
        diagnostics: fq_common::DiagnosticsConfig::default(),
        source_path: Some(dir.join("config.yaml").to_string_lossy().to_string()),
    }
}

/// The single status cell of a DDL result.
fn status_text(batches: &[RecordBatch]) -> String {
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("status is Utf8");
    column.value(0).to_string()
}

#[test]
fn timestamp_refresh_key_builds_and_refreshes_past_the_watermark() {
    let dir = sandbox("tskey");
    let file = dir.join("clicks.parquet");
    write_events(
        &file,
        vec![1_000_000, 2_000_000, 3_000_000],
        vec!["open", "click", "close"],
        vec!["u1", "u2", "u1"],
    );
    let runtime = Runtime::from_config(&event_config(&dir, "evta", &file)).expect("from_config");

    let (_, batches) = runtime
        .execute(
            "CREATE EVENT DATASET tsds FROM evta.main.evta \
             ACTOR user_id TIME created_at EVENT event \
             WITH (refresh_key = 'created_at')",
        )
        .expect("create event dataset");
    assert!(
        status_text(&batches).contains("3 events"),
        "unexpected status: {}",
        status_text(&batches)
    );

    // Replace the file with the same rows plus one PAST the watermark; the
    // delta filter must pull exactly that one (the rendered timestamp literal
    // compares inside the source dialect).
    write_events(
        &file,
        vec![1_000_000, 2_000_000, 3_000_000, 4_000_000],
        vec!["open", "click", "close", "purchase"],
        vec!["u1", "u2", "u1", "u2"],
    );
    let (_, batches) = runtime
        .execute("REFRESH EVENT DATASET tsds")
        .expect("refresh event dataset");
    assert!(
        status_text(&batches).contains("1 events appended"),
        "unexpected status: {}",
        status_text(&batches)
    );

    // The dataset's distinct event names, sorted, straight from its dictionary.
    let (schema, batches) = runtime
        .execute("SHOW EVENTS FROM tsds")
        .expect("show events");
    assert_eq!(schema.field(0).name(), "event");
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("event names are Utf8");
    let listed: Vec<&str> = (0..names.len()).map(|row| names.value(row)).collect();
    assert_eq!(listed, vec!["click", "close", "open", "purchase"]);

    // An unknown dataset raises, never an empty result.
    assert!(runtime.execute("SHOW EVENTS FROM nope").is_err());

    // PATHS returns one row per step: u2's click->purchase and u1's
    // open->close, each a depth-2 window occurring once, ranked by
    // (occurrences, steps) so click->purchase is path 1.
    let (schema, batches) = runtime
        .execute("PATHS ON tsds DEPTH 2 TOP 5")
        .expect("paths");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["path_id", "seq", "event", "occurrences", "share"]
    );
    let events = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("event names are Utf8");
    let steps: Vec<&str> = (0..events.len()).map(|row| events.value(row)).collect();
    assert_eq!(steps, vec!["click", "purchase", "open", "close"]);
}

#[test]
fn a_refresh_key_that_cannot_carry_a_watermark_raises_at_create() {
    let dir = sandbox("badkey");
    let file = dir.join("scores.parquet");
    // A Float64 refresh key: the type can never carry an exact watermark.
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", ArrowDataType::Utf8, false),
        Field::new("event", ArrowDataType::Utf8, false),
        Field::new("user_id", ArrowDataType::Utf8, false),
        Field::new("score", ArrowDataType::Float64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["2026-01-01 00:00:00"])),
        Arc::new(StringArray::from(vec!["open"])),
        Arc::new(StringArray::from(vec!["u1"])),
        Arc::new(arrow::array::Float64Array::from(vec![1.5])),
    ];
    let batch = RecordBatch::try_new(schema, columns).expect("batch");
    let out = File::create(&file).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(out, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    let runtime = Runtime::from_config(&event_config(&dir, "evtb", &file)).expect("from_config");
    let error = runtime
        .execute(
            "CREATE EVENT DATASET badds FROM evtb.main.evtb \
             ACTOR user_id TIME ts EVENT event \
             WITH (refresh_key = 'score')",
        )
        .unwrap_err();
    assert!(
        format!("{error}").contains("cannot carry an exact watermark"),
        "unexpected error: {error}"
    );
}

#[test]
fn paths_table_function_runs_and_aggregates() {
    let dir = sandbox("pathstvf");
    let file = dir.join("clicks.parquet");
    write_events(
        &file,
        vec![1_000_000, 2_000_000, 3_000_000, 4_000_000],
        vec!["open", "click", "close", "purchase"],
        vec!["u1", "u2", "u1", "u2"],
    );
    let runtime = Runtime::from_config(&event_config(&dir, "evtvf", &file)).expect("from_config");
    runtime
        .execute(
            "CREATE EVENT DATASET web FROM evtvf.main.evtvf \
             ACTOR user_id TIME created_at EVENT event \
             WITH (refresh_key = 'created_at')",
        )
        .expect("create event dataset");

    // The table function surfaces the SAME rows as `PATHS ON web DEPTH 2 TOP 5`,
    // now as a relation SQL can read: u2's click->purchase (path 1) and u1's
    // open->close (path 2).
    let (schema, batches) = runtime
        .execute(
            "SELECT path_id, seq, event, occurrences, share \
             FROM paths('web', depth => 2, top => 5) ORDER BY path_id, seq",
        )
        .expect("paths table function");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["path_id", "seq", "event", "occurrences", "share"]
    );
    let events = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("event names are Utf8");
    let steps: Vec<&str> = (0..events.len()).map(|row| events.value(row)).collect();
    assert_eq!(steps, vec!["click", "purchase", "open", "close"]);

    // Outer SQL aggregation over the analysis output: two paths of two step rows
    // each, so four rows total. This is the whole point - SQL on top of paths().
    let (_, batches) = runtime
        .execute("SELECT count(*) AS n FROM paths('web', depth => 2, top => 5)")
        .expect("count over paths");
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64");
    assert_eq!(n.value(0), 4);

    // GROUP BY over the surfaced relation: one group per path.
    let (_, batches) = runtime
        .execute(
            "SELECT path_id, sum(occurrences) AS occ \
             FROM paths('web', depth => 2, top => 5) GROUP BY path_id",
        )
        .expect("group by over paths");
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
}

#[test]
fn funnel_retention_segment_table_functions_run() {
    let dir = sandbox("analyticstvf");
    let file = dir.join("ev.parquet");
    let day = 86_400_000_000_i64;
    let hour = 3_600_000_000_i64;
    write_events(
        &file,
        vec![day, day + hour, day, 2 * day, 4 * day],
        vec!["view", "buy", "view", "view", "buy"],
        vec!["u1", "u1", "u2", "u3", "u3"],
    );
    let runtime = Runtime::from_config(&event_config(&dir, "evx", &file)).expect("from_config");
    runtime
        .execute(
            "CREATE EVENT DATASET web FROM evx.main.evx \
             ACTOR user_id TIME created_at EVENT event \
             WITH (refresh_key = 'created_at')",
        )
        .expect("create event dataset");

    // funnel(): two ordered steps, view -> buy within the window.
    let (schema, batches) = runtime
        .execute(
            "SELECT step_name, entered FROM funnel('web', 'view', 'buy', window => '30 days') \
             ORDER BY step_index",
        )
        .expect("funnel table function");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["step_name", "entered"]);
    let steps = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("step names are Utf8");
    let step_names: Vec<&str> = (0..steps.len()).map(|row| steps.value(row)).collect();
    assert_eq!(step_names, vec!["view", "buy"]);

    // Outer aggregation over the funnel output: 3 entered view + 2 reached buy.
    let (_, batches) = runtime
        .execute(
            "SELECT sum(entered) AS total FROM funnel('web', 'view', 'buy', window => '30 days')",
        )
        .expect("sum over funnel");
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum is Int64");
    assert_eq!(total.value(0), 5);

    // segment(): count events per day bucket -> three distinct days.
    let (_, batches) = runtime
        .execute("SELECT count(*) AS n FROM segment('web', measure => 'count', bucket => 'day')")
        .expect("segment table function");
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64");
    assert_eq!(n.value(0), 3);

    // retention(): runs end to end and returns at least one cohort row.
    let (_, batches) = runtime
        .execute(
            "SELECT count(*) AS n FROM retention('web', birth => 'view', return => 'buy', \
             period => 'day', periods => 7)",
        )
        .expect("retention table function");
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64");
    assert!(n.value(0) >= 1, "retention returned no rows");
}

#[test]
fn table_function_filter_narrows_the_analysis() {
    let dir = sandbox("tvffilter");
    let file = dir.join("ev.parquet");
    // A device property to filter on: three ios events, one android.
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "created_at",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("event", ArrowDataType::Utf8, false),
        Field::new("user_id", ArrowDataType::Utf8, false),
        Field::new("device", ArrowDataType::Utf8, false),
    ]));
    let day = 86_400_000_000_i64;
    let created: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![day, day, day, 2 * day]).with_timezone("UTC"),
    );
    let event: ArrayRef = Arc::new(StringArray::from(vec!["view", "view", "view", "view"]));
    let user: ArrayRef = Arc::new(StringArray::from(vec!["u1", "u2", "u3", "u4"]));
    let device: ArrayRef = Arc::new(StringArray::from(vec!["ios", "android", "ios", "ios"]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![created, event, user, device]).expect("batch");
    let out = File::create(&file).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(out, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    let runtime = Runtime::from_config(&event_config(&dir, "evf", &file)).expect("from_config");
    runtime
        .execute(
            "CREATE EVENT DATASET web FROM evf.main.evf \
             ACTOR user_id TIME created_at EVENT event PROPERTIES (device) \
             WITH (refresh_key = 'created_at')",
        )
        .expect("create event dataset");

    // Unfiltered: all four view events counted across the day buckets.
    let (_, batches) = runtime
        .execute(
            "SELECT sum(value) AS total FROM segment('web', measure => 'count', bucket => 'day')",
        )
        .expect("segment unfiltered");
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum is Int64");
    assert_eq!(total.value(0), 4);

    // filter => device='ios' narrows to the three ios events.
    let (_, batches) = runtime
        .execute(
            "SELECT sum(value) AS total FROM segment('web', measure => 'count', bucket => 'day', \
             filter => 'device = ''ios''')",
        )
        .expect("segment filtered");
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum is Int64");
    assert_eq!(total.value(0), 3);
}

/// Count the paths index files of a dataset, by kind.
fn index_files(dir: &Path, prefix: &str) -> usize {
    let mut found = 0;
    for entry in walk(dir) {
        if entry
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(prefix))
        {
            found += 1;
        }
    }
    found
}

/// Every file under a directory tree.
fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(walk(&path));
        } else {
            found.push(path);
        }
    }
    found
}

/// A refresh whose delta touches only actors the index has never seen ADDS a
/// segment instead of rebuilding: the existing segment file is left alone, and
/// PATHS answers across both.
///
/// This is the whole point of a segmented index, so the test asserts the
/// mechanism (a second segment file appeared, the first one's bytes did not
/// change) and not just that the answer is right - a full rebuild would give
/// the same answer while doing all the work.
#[test]
fn a_refresh_over_new_actors_appends_a_segment_instead_of_rebuilding() {
    let dir = sandbox("segappend");
    let file = dir.join("clicks.parquet");
    write_events(
        &file,
        vec![1_000_000, 2_000_000],
        vec!["open", "close"],
        vec!["u1", "u1"],
    );
    let runtime = Runtime::from_config(&event_config(&dir, "evta", &file)).expect("from_config");
    runtime
        .execute(
            "CREATE EVENT DATASET segds FROM evta.main.evta \
             ACTOR user_id TIME created_at EVENT event \
             WITH (refresh_key = 'created_at')",
        )
        .expect("create event dataset");

    assert_eq!(index_files(&dir, "paths_f_"), 1, "one segment after create");
    assert_eq!(index_files(&dir, "paths_m_"), 1, "exactly one manifest");
    let first_segment = walk(&dir)
        .into_iter()
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("paths_f_"))
        })
        .expect("a segment file");
    let before = std::fs::read(&first_segment).expect("read segment");

    // The delta belongs entirely to u2, an actor no segment covers.
    write_events(
        &file,
        vec![1_000_000, 2_000_000, 3_000_000, 4_000_000],
        vec!["open", "close", "signup", "purchase"],
        vec!["u1", "u1", "u2", "u2"],
    );
    let (_, batches) = runtime
        .execute("REFRESH EVENT DATASET segds")
        .expect("refresh event dataset");
    assert!(
        status_text(&batches).contains("2 events appended"),
        "unexpected status: {}",
        status_text(&batches)
    );

    assert_eq!(
        index_files(&dir, "paths_f_"),
        2,
        "the refresh should have ADDED a segment, not rebuilt one"
    );
    assert_eq!(
        index_files(&dir, "paths_m_"),
        1,
        "still exactly one manifest"
    );
    let after = std::fs::read(&first_segment).expect("read segment again");
    assert_eq!(before, after, "the existing segment was rewritten");

    // Both actors' windows come back, from two different segments.
    let (_, batches) = runtime
        .execute("PATHS ON segds DEPTH 2 TOP 5")
        .expect("paths");
    let events = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("event names are Utf8");
    let steps: Vec<&str> = (0..events.len()).map(|row| events.value(row)).collect();
    assert_eq!(steps, vec!["open", "close", "signup", "purchase"]);
}

/// A refresh whose delta touches an actor an existing segment already covers
/// appends a segment PAIR - an addition owning the window forms the delta
/// changed and a retraction cancelling the stale forms the base segment still
/// counts - and never rewrites the base segment. The answer counts every
/// window exactly once across the signed segments.
#[test]
fn a_refresh_over_a_returning_actor_appends_a_pair() {
    let dir = sandbox("segrebuild");
    let file = dir.join("clicks.parquet");
    write_events(
        &file,
        vec![1_000_000, 2_000_000],
        vec!["open", "close"],
        vec!["u1", "u1"],
    );
    let runtime = Runtime::from_config(&event_config(&dir, "evta", &file)).expect("from_config");
    runtime
        .execute(
            "CREATE EVENT DATASET retds FROM evta.main.evta \
             ACTOR user_id TIME created_at EVENT event \
             WITH (refresh_key = 'created_at')",
        )
        .expect("create event dataset");
    assert_eq!(index_files(&dir, "paths_f_"), 1);
    let base_segment = walk(&dir)
        .into_iter()
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("paths_f_"))
        })
        .expect("a segment file");
    let before = std::fs::read(&base_segment).expect("read segment");

    // u1 comes back: an actor the base segment already covers.
    write_events(
        &file,
        vec![1_000_000, 2_000_000, 3_000_000],
        vec!["open", "close", "purchase"],
        vec!["u1", "u1", "u1"],
    );
    runtime
        .execute("REFRESH EVENT DATASET retds")
        .expect("refresh event dataset");

    assert_eq!(
        index_files(&dir, "paths_f_"),
        3,
        "a returning actor appends an addition and a retraction"
    );
    assert_eq!(index_files(&dir, "paths_m_"), 1, "exactly one manifest");
    let after = std::fs::read(&base_segment).expect("read segment again");
    assert_eq!(before, after, "the base segment was rewritten");

    // u1's stream is one whole thing, counted once: three events, so the two
    // depth-2 windows open->close and close->purchase, each occurring once.
    let (_, batches) = runtime
        .execute("PATHS ON retds DEPTH 2 TOP 5")
        .expect("paths");
    let events = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("event names are Utf8");
    let steps: Vec<&str> = (0..events.len()).map(|row| events.value(row)).collect();
    assert_eq!(steps, vec!["close", "purchase", "open", "close"]);
    let counts = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("occurrences are i64");
    for row in 0..counts.len() {
        assert_eq!(counts.value(row), 1, "a window must not be double counted");
    }
}

/// The window-length contract: DEPTH above the dataset's max_paths_depth
/// raises a typed error naming the bound and the escape hatch, USING SCAN
/// serves the same statement exactly from the stream scan, and SHOW EVENT
/// DATASETS reports the bound the CREATE declared.
#[test]
fn paths_depth_above_the_bound_raises_and_using_scan_serves_it() {
    let dir = sandbox("depthbound");
    let file = dir.join("clicks.parquet");
    write_events(
        &file,
        vec![1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000],
        vec!["a", "b", "c", "d", "e"],
        vec!["u1", "u1", "u1", "u1", "u1"],
    );
    let runtime = Runtime::from_config(&event_config(&dir, "evta", &file)).expect("from_config");
    runtime
        .execute(
            "CREATE EVENT DATASET boundds FROM evta.main.evta \
             ACTOR user_id TIME created_at EVENT event \
             WITH (refresh_key = 'created_at', max_paths_depth = 4)",
        )
        .expect("create event dataset");

    let error = runtime
        .execute("PATHS ON boundds DEPTH 5 TOP 5")
        .expect_err("depth 5 must exceed the bound of 4");
    let message = error.to_string();
    assert!(
        message.contains("max_paths_depth 4") && message.contains("USING SCAN"),
        "error must name the bound and the escape hatch: {message}"
    );

    // The same statement with USING SCAN answers exactly: one depth-5 window.
    let (_, batches) = runtime
        .execute("PATHS ON boundds DEPTH 5 TOP 5 USING SCAN")
        .expect("paths using scan");
    let events = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("event names are Utf8");
    let steps: Vec<&str> = (0..events.len()).map(|row| events.value(row)).collect();
    assert_eq!(steps, vec!["a", "b", "c", "d", "e"]);

    // Depth AT the bound serves from the index, no opt-in needed.
    let (_, batches) = runtime
        .execute("PATHS ON boundds DEPTH 4 TOP 5")
        .expect("paths at the bound");
    assert!(batches[0].num_rows() > 0, "in-bound depth answers");

    // SHOW reports the declared bound.
    let (schema, batches) = runtime
        .execute("SHOW EVENT DATASETS")
        .expect("show event datasets");
    let column = schema
        .fields()
        .iter()
        .position(|field| field.name() == "max_paths_depth")
        .expect("SHOW carries max_paths_depth");
    let bounds = batches[0]
        .column(column)
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .expect("max_paths_depth is Int32");
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("names are Utf8");
    let row = (0..names.len())
        .find(|row| names.value(*row) == "boundds")
        .expect("boundds listed");
    assert_eq!(bounds.value(row), 4);
}
