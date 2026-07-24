//! Non-materialized views end to end: CREATE / SHOW / DROP, expansion in a
//! query, persistence across a runtime restart, the loud rejections, and the
//! motivating case - a view over a CROSS-DATASOURCE join that an event dataset
//! is then built on.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_runtime::Runtime;
use parquet::arrow::ArrowWriter;
use serde_yaml::Value;

/// A fresh temp sandbox for one test's parquet files and engine state.
fn sandbox(tag: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fq_views_{tag}_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create sandbox dir");
    dir
}

/// Write the event fixture: a UTC timestamp, an event name, and an actor id.
fn write_events(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "created_at",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("event", ArrowDataType::Utf8, false),
        Field::new("user_id", ArrowDataType::Utf8, false),
    ]));
    let day = 86_400_000_000_i64;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampMicrosecondArray::from(vec![day, day + 3_600_000_000, 2 * day])
                .with_timezone("UTC"),
        ),
        Arc::new(StringArray::from(vec!["view", "buy", "view"])),
        Arc::new(StringArray::from(vec!["u1", "u1", "u2"])),
    ];
    write_parquet(path, schema, columns);
}

/// Write the user-dimension fixture, in a SEPARATE datasource.
fn write_users(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", ArrowDataType::Utf8, false),
        Field::new("country", ArrowDataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["u1", "u2"])),
        Arc::new(StringArray::from(vec!["US", "DE"])),
    ];
    write_parquet(path, schema, columns);
}

/// Write one batch as a single-file parquet.
fn write_parquet(path: &Path, schema: Arc<Schema>, columns: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(schema, columns).expect("fixture batch");
    let file = File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

/// A config over the given single-file parquet datasources, with a source path
/// so the runtime has a view/event store.
fn config_for(dir: &Path, sources: &[(&str, &Path)]) -> Config {
    let mut datasources = BTreeMap::new();
    for (name, file) in sources {
        let mut params = BTreeMap::new();
        params.insert(
            "file".to_string(),
            Value::String(file.to_string_lossy().to_string()),
        );
        datasources.insert(
            (*name).to_string(),
            DataSourceConfig {
                name: (*name).to_string(),
                ty: "parquet".to_string(),
                config: params,
                capabilities: Vec::new(),
                change_keys: BTreeMap::new(),
            },
        );
    }
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

/// The single Int64 cell of a one-row result.
fn one_count(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64")
        .value(0)
}

#[test]
fn view_is_created_queried_listed_and_dropped() {
    let dir = sandbox("basic");
    let events = dir.join("ev.parquet");
    write_events(&events);
    let config = config_for(&dir, &[("eva", &events)]);
    let runtime = Runtime::from_config(&config).expect("from_config");

    runtime
        .execute("CREATE VIEW buys AS SELECT user_id, created_at FROM eva.main.eva WHERE event = 'buy'")
        .expect("create view");

    // The view reads current data through its definition.
    let (_, batches) = runtime
        .execute("SELECT count(*) AS n FROM buys")
        .expect("select from view");
    assert_eq!(one_count(&batches), 1);

    // Aliased, and joined back to a real table.
    let (_, batches) = runtime
        .execute("SELECT count(*) AS n FROM buys b JOIN eva.main.eva e ON b.user_id = e.user_id")
        .expect("join view to table");
    assert_eq!(one_count(&batches), 2);

    // SHOW VIEWS lists it with its definition.
    let (schema, batches) = runtime.execute("SHOW VIEWS").expect("show views");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["name", "columns", "definition", "created_at"]);
    let listed = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("names are Utf8");
    assert_eq!(listed.value(0), "buys");

    runtime.execute("DROP VIEW buys").expect("drop view");
    // Gone: the name no longer resolves.
    assert!(runtime.execute("SELECT count(*) FROM buys").is_err());
    // Dropping again raises rather than silently succeeding.
    assert!(runtime.execute("DROP VIEW buys").is_err());
}

#[test]
fn view_survives_a_runtime_restart() {
    let dir = sandbox("persist");
    let events = dir.join("ev.parquet");
    write_events(&events);
    let config = config_for(&dir, &[("eva", &events)]);
    {
        let runtime = Runtime::from_config(&config).expect("from_config");
        runtime
            .execute("CREATE VIEW allev AS SELECT user_id FROM eva.main.eva")
            .expect("create view");
    }
    // A NEW runtime over the same config still resolves the view.
    let reopened = Runtime::from_config(&config).expect("reopen");
    let (_, batches) = reopened
        .execute("SELECT count(*) AS n FROM allev")
        .expect("select from persisted view");
    assert_eq!(one_count(&batches), 3);
}

#[test]
fn view_column_rename_and_or_replace() {
    let dir = sandbox("rename");
    let events = dir.join("ev.parquet");
    write_events(&events);
    let config = config_for(&dir, &[("eva", &events)]);
    let runtime = Runtime::from_config(&config).expect("from_config");

    runtime
        .execute("CREATE VIEW ren (actor, kind) AS SELECT user_id, event FROM eva.main.eva")
        .expect("create view with renames");
    let (_, batches) = runtime
        .execute("SELECT count(*) AS n FROM ren WHERE kind = 'view'")
        .expect("query renamed columns");
    assert_eq!(one_count(&batches), 2);

    // A plain CREATE over an existing name raises; OR REPLACE swaps it.
    assert!(runtime
        .execute("CREATE VIEW ren AS SELECT user_id FROM eva.main.eva")
        .is_err());
    runtime
        .execute("CREATE OR REPLACE VIEW ren AS SELECT user_id FROM eva.main.eva WHERE event = 'buy'")
        .expect("or replace");
    let (_, batches) = runtime
        .execute("SELECT count(*) AS n FROM ren")
        .expect("query replaced view");
    assert_eq!(one_count(&batches), 1);
}

#[test]
fn invalid_view_definitions_raise_at_create() {
    let dir = sandbox("invalid");
    let events = dir.join("ev.parquet");
    write_events(&events);
    let config = config_for(&dir, &[("eva", &events)]);
    let runtime = Runtime::from_config(&config).expect("from_config");

    // Unknown table: raises at CREATE, not at first use.
    assert!(runtime
        .execute("CREATE VIEW bad AS SELECT x FROM nope.main.nope")
        .is_err());
    // Unknown column: same.
    assert!(runtime
        .execute("CREATE VIEW bad AS SELECT not_a_column FROM eva.main.eva")
        .is_err());
    // A rename whose arity does not match the body.
    assert!(runtime
        .execute("CREATE VIEW bad (only_one) AS SELECT user_id, event FROM eva.main.eva")
        .is_err());
    // A view defined in terms of itself is a cycle, caught at CREATE.
    assert!(runtime
        .execute("CREATE VIEW selfref AS SELECT user_id FROM selfref")
        .is_err());
    // None of the failures registered anything.
    let (_, batches) = runtime.execute("SHOW VIEWS").expect("show views");
    assert_eq!(batches[0].num_rows(), 0);
}

#[test]
fn event_dataset_over_a_view_of_a_cross_source_join() {
    let dir = sandbox("crossjoin");
    let events = dir.join("ev.parquet");
    let users = dir.join("us.parquet");
    write_events(&events);
    write_users(&users);
    let config = config_for(&dir, &[("eva", &events), ("usb", &users)]);
    let runtime = Runtime::from_config(&config).expect("from_config");

    // One view names the federated join across TWO datasources.
    runtime
        .execute(
            "CREATE VIEW enriched AS SELECT e.created_at AS ts, e.event AS ev, \
             e.user_id AS uid, u.country AS country \
             FROM eva.main.eva e JOIN usb.main.usb u ON e.user_id = u.user_id",
        )
        .expect("create cross-source view");

    // The event dataset materializes from that view.
    runtime
        .execute(
            "CREATE EVENT DATASET web AS ( SELECT ts, ev, uid, country FROM enriched ) \
             ACTOR uid TIME ts EVENT ev PROPERTIES (country)",
        )
        .expect("event dataset over the view");

    // Analytics over it, including a property that came from the JOINED source.
    let (_, batches) = runtime
        .execute("SELECT sum(value) AS n FROM segment('web', measure => 'count', bucket => 'day')")
        .expect("segment over view-sourced dataset");
    assert_eq!(one_count(&batches), 3);

    let (_, batches) = runtime
        .execute(
            "SELECT sum(value) AS n FROM segment('web', measure => 'count', bucket => 'day', \
             filter => 'country = ''US''')",
        )
        .expect("filtered segment");
    assert_eq!(one_count(&batches), 2);
}
