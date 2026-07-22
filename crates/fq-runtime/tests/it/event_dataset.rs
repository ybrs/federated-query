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

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
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
    let created: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(micros).with_timezone("UTC"),
    );
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
    assert_eq!(names, vec!["path_id", "seq", "event", "occurrences", "share"]);
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
