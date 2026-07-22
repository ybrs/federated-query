//! Event-analytics statement execution: the EVENT DATASET DDL family drives
//! the fq-events store (sourcing batches through the streaming connector
//! pull), and the four analysis statements run fq-events executors and map
//! their pinned result relations onto Arrow. Analyses never touch the origin
//! source; only CREATE / REFRESH / REBUILD pull from it.

use std::sync::{Arc, Mutex};

use arrow::array::{
    Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit as ArrowTimeUnit,
};

use fq_accel::{scan_watermark, Watermark, WatermarkScan};
use fq_common::events::{
    EventDatasetDef, EventSource, FunnelSpec, PathsSpec, RetentionSpec, SegmentMeasure, SegmentSpec,
};
use fq_common::DataType;
use fq_events::build::BatchSource;
use fq_events::error::{EventError, EventRefreshError};
use fq_events::model::{DatasetRecord, PropertyEncoding};
use fq_events::{EventStore, SourceInfo};
use fq_exec::connectors;

use crate::error::RuntimeError;
use crate::materialized::status_result;
use crate::Runtime;

/// The lazily opened event store shared by a runtime's statements.
pub(crate) type EventStoreSlot = Mutex<Option<Arc<EventStore>>>;

/// The event-store root directory that belongs to a config file:
/// `<config-stem>.events/` next to it, sibling to the stats SQLite.
pub(crate) fn events_root_path(source_path: &str) -> std::path::PathBuf {
    let path = std::path::Path::new(source_path);
    let stem = path
        .file_stem()
        .map_or_else(|| "fedq".to_string(), |s| s.to_string_lossy().to_string());
    path.parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(stem + ".events")
}

impl Runtime {
    /// The event store, opened on first use. A programmatic config with no
    /// file path has no event store and every event statement raises.
    fn event_store(&self) -> Result<Arc<EventStore>, RuntimeError> {
        let mut slot = self.events.lock().expect("event store lock poisoned");
        if let Some(store) = slot.as_ref() {
            return Ok(Arc::clone(store));
        }
        let config = self.config_snapshot();
        let Some(source_path) = config.source_path.as_deref() else {
            return Err(RuntimeError::EventStatement(
                "this runtime's config has no file path, so it has no event store; load \
                 the config from a file to use event datasets"
                    .to_string(),
            ));
        };
        let stats_path = crate::stats_sqlite_path(&config)
            .expect("a config with a source path has a stats path");
        let store = Arc::new(EventStore::open(
            &stats_path,
            &events_root_path(source_path),
        )?);
        *slot = Some(Arc::clone(&store));
        Ok(store)
    }

    /// `CREATE EVENT DATASET`: resolve and validate the source, stream its
    /// rows into the initial build, and publish the dataset.
    pub(crate) fn create_event_dataset(
        &self,
        def: &EventDatasetDef,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        let catalog = self.catalog_snapshot();
        if catalog.resolve_table(None, None, &def.name).is_some() {
            return Err(RuntimeError::EventStatement(format!(
                "relation '{}' already exists",
                def.name
            )));
        }
        let config = self.config_snapshot();
        let report = match &def.source {
            EventSource::Table {
                datasource,
                schema,
                table,
            } => {
                let pull = Self::table_pull(&catalog, def, datasource, schema, table)?;
                let info = SourceInfo {
                    kind: "table".to_string(),
                    reference: format!("{datasource}.{schema}.{table}"),
                    estimated_rows: pull.estimated_rows,
                    watermark: None,
                    source_token: pull.source_token.clone(),
                };
                let _scope = connectors::SessionScope::enter(self.session);
                let stream = connectors::fetch_stream(datasource, &pull.sql)?;
                let mut source = StreamSource::new(stream, pull.watermark_column.clone());
                store.create_dataset(def, &info, &mut source, &config.events)?
            }
            EventSource::Select { sql } => {
                if def.refresh_key.is_some() {
                    return Err(RuntimeError::EventStatement(
                        "a SELECT-defined event dataset cannot declare a refresh_key; \
                         its delta cannot be pulled - use REBUILD to bring it forward"
                            .to_string(),
                    ));
                }
                let (_, batches) = self.execute_source_query(sql)?;
                let info = SourceInfo {
                    kind: "select".to_string(),
                    reference: sql.clone(),
                    estimated_rows: None,
                    watermark: None,
                    source_token: None,
                };
                let mut source = ReplaySource {
                    batches: batches.into_iter(),
                };
                store.create_dataset(def, &info, &mut source, &config.events)?
            }
        };
        status_result(&format!(
            "CREATE EVENT DATASET: {} events, {} actors, {} shards, {} bytes, \
             build {:.3}s (partition {:.3}s, finalize {:.3}s), spill {} bytes, \
             peak rss {} bytes{}",
            report.events,
            report.actors,
            report.shards,
            report.byte_size,
            report.build_millis as f64 / 1000.0,
            report.partition_millis as f64 / 1000.0,
            report.finalize_millis as f64 / 1000.0,
            report.spill_bytes,
            report.peak_rss_bytes.unwrap_or(0),
            if report.promoted.is_empty() {
                format!(", {}", report.paths_index)
            } else {
                format!(
                    ", promoted to raw strings: {}, {}",
                    report.promoted.join(", "),
                    report.paths_index
                )
            }
        ))
    }

    /// Resolve one `FROM <ds>.<schema>.<table>` source: validate every
    /// declared column against the catalog, expand the default property set,
    /// and render the explicit pull SQL.
    fn table_pull(
        catalog: &fq_catalog::Catalog,
        def: &EventDatasetDef,
        datasource: &str,
        schema: &str,
        table: &str,
    ) -> Result<TablePull, RuntimeError> {
        let resolved = catalog
            .resolve_table(Some(datasource), Some(schema), table)
            .ok_or_else(|| {
                RuntimeError::EventStatement(format!(
                    "source table '{datasource}.{schema}.{table}' does not exist"
                ))
            })?;
        let all_columns: Vec<String> = resolved
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        let exists = |name: &str| {
            all_columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(name))
        };
        let mut roles = vec![
            def.actor_column.clone(),
            def.time_column.clone(),
            def.event_column.clone(),
        ];
        if let Some(tiebreak) = &def.tiebreak_column {
            roles.push(tiebreak.clone());
        }
        for role in &roles {
            if !exists(role) {
                return Err(RuntimeError::EventStatement(format!(
                    "column '{role}' does not exist in '{datasource}.{schema}.{table}'"
                )));
            }
        }
        let properties: Vec<String> = match &def.properties {
            Some(names) => {
                for name in names {
                    if !exists(name) {
                        return Err(RuntimeError::EventStatement(format!(
                            "PROPERTIES column '{name}' does not exist in \
                             '{datasource}.{schema}.{table}'"
                        )));
                    }
                }
                names.clone()
            }
            None => all_columns
                .iter()
                .filter(|column| !roles.iter().any(|role| role.eq_ignore_ascii_case(column)))
                .cloned()
                .collect(),
        };
        let mut ingest = roles;
        ingest.extend(properties);
        let watermark_column = match &def.refresh_key {
            None => None,
            Some(key) => {
                let position = ingest.iter().position(|c| c.eq_ignore_ascii_case(key));
                let Some(position) = position else {
                    return Err(RuntimeError::EventStatement(format!(
                        "refresh_key '{key}' must be one of the ingested columns \
                         (actor/time/event/tiebreak/properties) so its watermark can be \
                         tracked from the pulled rows"
                    )));
                };
                let column = resolved
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(key))
                    .ok_or_else(|| {
                        RuntimeError::EventStatement(format!(
                            "refresh_key '{key}' does not exist in \
                             '{datasource}.{schema}.{table}'"
                        ))
                    })?;
                require_refresh_key_type(key, column.data_type)?;
                Some((position, key.clone()))
            }
        };
        let column_list: Vec<String> = ingest
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect();
        let sql = format!(
            "SELECT {} FROM \"{schema}\".\"{table}\"",
            column_list.join(", ")
        );
        let estimated_rows = catalog
            .get_datasource(datasource)
            .and_then(|source| source.get_table_metadata(schema, table).ok())
            .and_then(|metadata| metadata.row_count)
            .and_then(|count| u64::try_from(count).ok());
        let source_token = catalog
            .get_datasource(datasource)
            .and_then(|source| source.source_token(schema, table).ok())
            .flatten();
        Ok(TablePull {
            sql,
            estimated_rows,
            source_token,
            watermark_column,
        })
    }

    /// `REFRESH EVENT DATASET`: pull the delta past the stored watermark and
    /// append it as a new generation. No refresh key (or a SELECT source)
    /// raises naming REBUILD as the path.
    pub(crate) fn refresh_event_dataset(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        let record = store.dataset(name)?.record.clone();
        if record.source_kind == "select" {
            return Err(EventError::from(EventRefreshError::SelectSource {
                name: name.to_string(),
            })
            .into());
        }
        let Some(refresh_key) = record.refresh_key.clone() else {
            return Err(EventError::from(EventRefreshError::NoRefreshKey {
                name: name.to_string(),
            })
            .into());
        };
        let (datasource, schema, table) = split_source_ref(&record)?;
        let catalog = self.catalog_snapshot();
        let token = catalog
            .get_datasource(&datasource)
            .and_then(|source| source.source_token(&schema, &table).ok())
            .flatten();
        if token.is_some() && token == record.source_token {
            return status_result(
                "REFRESH EVENT DATASET (no-op: source token unchanged, 0 events appended)",
            );
        }
        let ingest = ingest_columns(&record);
        let position = ingest
            .iter()
            .position(|c| c.eq_ignore_ascii_case(&refresh_key))
            .ok_or_else(|| {
                RuntimeError::EventStatement(format!(
                    "refresh_key '{refresh_key}' is no longer among the dataset's ingested \
                     columns"
                ))
            })?;
        let column_list: Vec<String> = ingest.iter().map(|c| format!("\"{c}\"")).collect();
        let base_sql = format!(
            "SELECT {} FROM \"{schema}\".\"{table}\"",
            column_list.join(", ")
        );
        let sql = match &record.watermark {
            Some(stored) => {
                let literal = watermark_literal(stored, name)?;
                format!("{base_sql} WHERE \"{refresh_key}\" > {literal}")
            }
            None => base_sql,
        };
        let config = self.config_snapshot();
        let _scope = connectors::SessionScope::enter(self.session);
        let stream = connectors::fetch_stream(&datasource, &sql)?;
        let mut source = StreamSource::new(stream, Some((position, refresh_key)));
        source.seed_watermark(record.watermark.clone(), name)?;
        let appended = store.append_generation_with(name, &mut source, token, &config.events)?;
        status_result(&format!(
            "REFRESH EVENT DATASET: {appended} events appended"
        ))
    }

    /// `REBUILD EVENT DATASET`: re-pull the whole source into a fresh tree
    /// and swap it in.
    pub(crate) fn rebuild_event_dataset(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        let record = store.dataset(name)?.record.clone();
        let config = self.config_snapshot();
        let report = if record.source_kind == "select" {
            let (_, batches) = self.execute_source_query(&record.source_ref)?;
            let mut source = ReplaySource {
                batches: batches.into_iter(),
            };
            store.rebuild_dataset_with(name, &mut source, None, &config.events)?
        } else {
            let (datasource, schema, table) = split_source_ref(&record)?;
            let catalog = self.catalog_snapshot();
            let token = catalog
                .get_datasource(&datasource)
                .and_then(|source| source.source_token(&schema, &table).ok())
                .flatten();
            let ingest = ingest_columns(&record);
            let column_list: Vec<String> = ingest.iter().map(|c| format!("\"{c}\"")).collect();
            let sql = format!(
                "SELECT {} FROM \"{schema}\".\"{table}\"",
                column_list.join(", ")
            );
            let watermark_column = record.refresh_key.clone().and_then(|key| {
                let position = ingest.iter().position(|c| c.eq_ignore_ascii_case(&key));
                position.zip(Some(key))
            });
            let _scope = connectors::SessionScope::enter(self.session);
            let stream = connectors::fetch_stream(&datasource, &sql)?;
            let mut source = StreamSource::new(stream, watermark_column);
            store.rebuild_dataset_with(name, &mut source, token, &config.events)?
        };
        status_result(&format!(
            "REBUILD EVENT DATASET: {} events, {} actors, {} shards, build {:.3}s, {}",
            report.events,
            report.actors,
            report.shards,
            report.build_millis as f64 / 1000.0,
            report.paths_index
        ))
    }

    /// `DROP EVENT DATASET`.
    pub(crate) fn drop_event_dataset(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        store.drop_dataset(name)?;
        status_result("DROP EVENT DATASET")
    }

    /// `SHOW EVENT DATASETS`: the pinned listing relation.
    pub(crate) fn show_event_datasets(
        &self,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        let records = store.list()?;
        let schema = show_event_datasets_schema();
        let mut names = Vec::new();
        let mut events = Vec::new();
        let mut actors = Vec::new();
        let mut bytes = Vec::new();
        let mut shards = Vec::new();
        let mut generations = Vec::new();
        let mut tiebreaks = Vec::new();
        let mut promoted = Vec::new();
        let mut watermarks: Vec<Option<String>> = Vec::new();
        let mut created = Vec::new();
        let mut refreshed: Vec<Option<String>> = Vec::new();
        for record in &records {
            names.push(record.name.clone());
            events.push(record.measured_events);
            actors.push(record.measured_actors);
            bytes.push(record.byte_size);
            shards.push(i32::try_from(record.shards).expect("shard count fits i32"));
            generations.push(
                i32::try_from(record.file_list.generations.len()).expect("generations fit i32"),
            );
            tiebreaks.push(
                record
                    .tiebreak_column
                    .clone()
                    .unwrap_or_else(|| "synthetic".to_string()),
            );
            promoted.push(
                record
                    .properties
                    .iter()
                    .filter(|def| def.encoding == PropertyEncoding::RawString)
                    .map(|def| def.name.clone())
                    .collect::<Vec<_>>()
                    .join(","),
            );
            watermarks.push(record.watermark.clone());
            created.push(record.created_at.clone());
            refreshed.push(record.refreshed_at.clone());
        }
        let columns: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(events)),
            Arc::new(Int64Array::from(actors)),
            Arc::new(Int64Array::from(bytes)),
            Arc::new(Int32Array::from(shards)),
            Arc::new(Int32Array::from(generations)),
            Arc::new(StringArray::from(tiebreaks)),
            Arc::new(StringArray::from(promoted)),
            Arc::new(StringArray::from(watermarks)),
            Arc::new(StringArray::from(created)),
            Arc::new(StringArray::from(refreshed)),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
        Ok((schema, vec![batch]))
    }

    /// `SHOW EVENTS FROM <dataset>`: one row per distinct event name, sorted,
    /// read from the dataset's event dictionary - no shard data is touched.
    pub(crate) fn show_events(
        &self,
        dataset: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        let loaded = store.dataset(dataset)?;
        let mut names = loaded.event_dict.values.clone();
        names.sort();
        crate::introspect::text_result(&["event"], vec![names])
    }

    /// `FUNNEL (...) ON <dataset>`.
    pub(crate) fn run_event_funnel(
        &self,
        spec: &FunnelSpec,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        let dataset = store.dataset(&spec.dataset)?;
        let config = self.config_snapshot();
        let result = fq_events::run_funnel(store.io(), &dataset, spec, &config.events)?;
        funnel_to_arrow(&result)
    }

    /// `RETENTION ON <dataset>`.
    pub(crate) fn run_event_retention(
        &self,
        spec: &RetentionSpec,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        let dataset = store.dataset(&spec.dataset)?;
        let config = self.config_snapshot();
        let result = fq_events::run_retention(store.io(), &dataset, spec, &config.events)?;
        retention_to_arrow(&result)
    }

    /// `SEGMENT ... ON <dataset>`.
    pub(crate) fn run_event_segment(
        &self,
        spec: &SegmentSpec,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        let dataset = store.dataset(&spec.dataset)?;
        let config = self.config_snapshot();
        let result = fq_events::run_segment(store.io(), &dataset, spec, &config.events)?;
        segment_to_arrow(&result)
    }

    /// `PATHS ON <dataset>`.
    pub(crate) fn run_event_paths(
        &self,
        spec: &PathsSpec,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let store = self.event_store()?;
        let dataset = store.dataset(&spec.dataset)?;
        let config = self.config_snapshot();
        let result = fq_events::run_paths(store.io(), &dataset, spec, &config.events)?;
        paths_to_arrow(&result)
    }
}

/// One resolved table pull.
struct TablePull {
    sql: String,
    estimated_rows: Option<u64>,
    source_token: Option<String>,
    /// The refresh-key column's position in the pulled schema plus its name.
    watermark_column: Option<(usize, String)>,
}

/// Split a stored `datasource.schema.table` source reference.
fn split_source_ref(record: &DatasetRecord) -> Result<(String, String, String), RuntimeError> {
    let parts: Vec<&str> = record.source_ref.split('.').collect();
    if parts.len() != 3 {
        return Err(RuntimeError::EventStatement(format!(
            "dataset '{}' has a malformed source reference '{}'",
            record.name, record.source_ref
        )));
    }
    Ok((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}

/// The ingest column list of a stored dataset, in build order.
fn ingest_columns(record: &DatasetRecord) -> Vec<String> {
    let mut columns = vec![
        record.actor_column.clone(),
        record.time_column.clone(),
        record.event_column.clone(),
    ];
    if let Some(tiebreak) = &record.tiebreak_column {
        columns.push(tiebreak.clone());
    }
    for def in &record.properties {
        columns.push(def.name.clone());
    }
    columns
}

/// The engine types a refresh_key watermark can carry exactly. Anything else
/// (floats, decimals, booleans, intervals) raises at CREATE, before any data
/// is pulled: the declared key could never drive a delta refresh.
fn require_refresh_key_type(key: &str, data_type: DataType) -> Result<(), RuntimeError> {
    match data_type {
        DataType::Integer
        | DataType::BigInt
        | DataType::Date
        | DataType::Timestamp
        | DataType::Text
        | DataType::Varchar => Ok(()),
        DataType::Float
        | DataType::Double
        | DataType::Decimal
        | DataType::Boolean
        | DataType::Interval
        | DataType::Null => Err(RuntimeError::EventStatement(format!(
            "refresh_key '{key}' has type {}, which cannot carry an exact watermark; \
             use an integer, text, date, or timestamp column",
            data_type.value()
        ))),
    }
}

/// Render a stored watermark JSON as a SQL literal for the delta predicate.
fn watermark_literal(stored: &str, dataset: &str) -> Result<String, RuntimeError> {
    let value: Watermark = serde_json::from_str(stored).map_err(|error| {
        RuntimeError::EventStatement(format!(
            "dataset '{dataset}' has an unreadable stored watermark: {error}"
        ))
    })?;
    Ok(value.sql_literal())
}

/// A batch source over the streaming connector pull that tracks the maximum
/// refresh-key value it saw (the next watermark), via the shared exact
/// watermark scanner (fq-accel) the materialized-view delta path uses.
struct StreamSource {
    stream: connectors::FetchStream,
    watermark_column: Option<(usize, String)>,
    watermark: Option<Watermark>,
}

impl StreamSource {
    /// Wrap one stream.
    fn new(stream: connectors::FetchStream, watermark_column: Option<(usize, String)>) -> Self {
        Self {
            stream,
            watermark_column,
            watermark: None,
        }
    }

    /// Seed the tracker with the stored watermark so an empty delta keeps it.
    fn seed_watermark(
        &mut self,
        stored: Option<String>,
        dataset: &str,
    ) -> Result<(), RuntimeError> {
        if let Some(stored) = stored {
            self.watermark = Some(serde_json::from_str(&stored).map_err(|error| {
                RuntimeError::EventStatement(format!(
                    "dataset '{dataset}' has an unreadable stored watermark: {error}"
                ))
            })?);
        }
        Ok(())
    }

    /// Fold one batch's refresh-key column into the tracked maximum. A column
    /// the scanner cannot carry exactly (unsupported flavor, NULLs) is an
    /// ERROR here, not a fallback: the user DECLARED the refresh key, so a key
    /// that cannot drive a delta must fail the build loudly.
    fn track(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let Some((position, _)) = &self.watermark_column else {
            return Ok(());
        };
        let scanned = scan_watermark(std::slice::from_ref(batch), *position)
            .map_err(|error| error.to_string())?;
        let value = match scanned {
            WatermarkScan::Unsupported(reason) => return Err(reason),
            WatermarkScan::Value(None) => return Ok(()),
            WatermarkScan::Value(Some(value)) => value,
        };
        self.watermark = Some(match self.watermark.take() {
            None => value,
            Some(current) => current.max_with(value).map_err(|error| error.to_string())?,
        });
        Ok(())
    }
}

impl BatchSource for StreamSource {
    /// Pull the next batch, folding the watermark tracker.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        let batch = self
            .stream
            .next_batch()
            .map_err(|error| error.to_string())?;
        if let Some(batch) = &batch {
            self.track(batch)?;
        }
        Ok(batch)
    }

    /// The tracked watermark after the stream is exhausted.
    fn watermark(&self) -> Option<String> {
        self.watermark
            .as_ref()
            .map(|value| serde_json::to_string(value).expect("watermark serializes"))
    }
}

/// A batch source replaying an already materialized result (SELECT-defined
/// datasets; their memory belongs to the query pipeline).
struct ReplaySource {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl BatchSource for ReplaySource {
    /// The next stored batch.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    /// A replayed result carries no refresh watermark.
    fn watermark(&self) -> Option<String> {
        None
    }
}

// ---------------------------------------------------------------------------
// Result relation mapping
// ---------------------------------------------------------------------------

/// The microsecond-timestamp Arrow type of bucket/cohort labels.
fn micros_type() -> ArrowDataType {
    ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None)
}

/// The `SHOW EVENT DATASETS` Arrow schema.
fn show_event_datasets_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("events", ArrowDataType::Int64, false),
        Field::new("actors", ArrowDataType::Int64, false),
        Field::new("bytes", ArrowDataType::Int64, false),
        Field::new("shards", ArrowDataType::Int32, false),
        Field::new("generations", ArrowDataType::Int32, false),
        Field::new("tiebreak", ArrowDataType::Utf8, false),
        Field::new("promoted_props", ArrowDataType::Utf8, false),
        Field::new("watermark", ArrowDataType::Utf8, true),
        Field::new("created", ArrowDataType::Utf8, false),
        Field::new("refreshed", ArrowDataType::Utf8, true),
    ]))
}

/// The `SHOW EVENT DATASETS` describe columns.
pub(crate) fn show_event_datasets_columns() -> Vec<(String, DataType)> {
    vec![
        ("name".to_owned(), DataType::Text),
        ("events".to_owned(), DataType::BigInt),
        ("actors".to_owned(), DataType::BigInt),
        ("bytes".to_owned(), DataType::BigInt),
        ("shards".to_owned(), DataType::Integer),
        ("generations".to_owned(), DataType::Integer),
        ("tiebreak".to_owned(), DataType::Text),
        ("promoted_props".to_owned(), DataType::Text),
        ("watermark".to_owned(), DataType::Text),
        ("created".to_owned(), DataType::Text),
        ("refreshed".to_owned(), DataType::Text),
    ]
}

/// Map a funnel result onto Arrow (breakdown column only when the clause is).
fn funnel_to_arrow(
    result: &fq_events::FunnelResult,
) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let mut fields = Vec::new();
    if result.has_breakdown {
        fields.push(Field::new("breakdown", ArrowDataType::Utf8, true));
    }
    fields.extend([
        Field::new("step_index", ArrowDataType::Int32, false),
        Field::new("step_name", ArrowDataType::Utf8, false),
        Field::new("entered", ArrowDataType::Int64, false),
        Field::new("conversion_from_previous", ArrowDataType::Float64, false),
        Field::new("conversion_from_start", ArrowDataType::Float64, false),
        Field::new("median_seconds_from_previous", ArrowDataType::Float64, true),
        Field::new("p90_seconds_from_previous", ArrowDataType::Float64, true),
    ]);
    let schema = Arc::new(Schema::new(fields));
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
    if result.has_breakdown {
        let breakdowns: Vec<Option<String>> = result
            .rows
            .iter()
            .map(|row| row.breakdown.clone())
            .collect();
        columns.push(Arc::new(StringArray::from(breakdowns)));
    }
    columns.push(Arc::new(Int32Array::from(
        result
            .rows
            .iter()
            .map(|row| i32::try_from(row.step_index).expect("step fits i32"))
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(StringArray::from(
        result
            .rows
            .iter()
            .map(|row| row.step_name.clone())
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Int64Array::from(
        result
            .rows
            .iter()
            .map(|row| i64::try_from(row.entered).expect("count fits i64"))
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Float64Array::from(
        result
            .rows
            .iter()
            .map(|row| row.conversion_from_previous)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Float64Array::from(
        result
            .rows
            .iter()
            .map(|row| row.conversion_from_start)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Float64Array::from(
        result
            .rows
            .iter()
            .map(|row| row.median_seconds_from_previous)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Float64Array::from(
        result
            .rows
            .iter()
            .map(|row| row.p90_seconds_from_previous)
            .collect::<Vec<_>>(),
    )));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
    Ok((schema, vec![batch]))
}

/// The funnel describe columns for a spec.
pub(crate) fn funnel_describe_columns(spec: &FunnelSpec) -> Vec<(String, DataType)> {
    let mut columns = Vec::new();
    if spec.breakdown.is_some() {
        columns.push(("breakdown".to_owned(), DataType::Varchar));
    }
    columns.extend([
        ("step_index".to_owned(), DataType::Integer),
        ("step_name".to_owned(), DataType::Varchar),
        ("entered".to_owned(), DataType::BigInt),
        ("conversion_from_previous".to_owned(), DataType::Double),
        ("conversion_from_start".to_owned(), DataType::Double),
        ("median_seconds_from_previous".to_owned(), DataType::Double),
        ("p90_seconds_from_previous".to_owned(), DataType::Double),
    ]);
    columns
}

/// Map a retention result onto Arrow.
fn retention_to_arrow(
    result: &fq_events::RetentionResult,
) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let mut fields = Vec::new();
    if result.has_breakdown {
        fields.push(Field::new("breakdown", ArrowDataType::Utf8, true));
    }
    fields.extend([
        Field::new("cohort_start", micros_type(), false),
        Field::new("cohort_size", ArrowDataType::Int64, false),
        Field::new("period_offset", ArrowDataType::Int32, false),
        Field::new("retained", ArrowDataType::Int64, false),
        Field::new("retention_rate", ArrowDataType::Float64, true),
    ]);
    let schema = Arc::new(Schema::new(fields));
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
    if result.has_breakdown {
        let breakdowns: Vec<Option<String>> = result
            .rows
            .iter()
            .map(|row| row.breakdown.clone())
            .collect();
        columns.push(Arc::new(StringArray::from(breakdowns)));
    }
    columns.push(Arc::new(TimestampMicrosecondArray::from(
        result
            .rows
            .iter()
            .map(|row| row.cohort_start)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Int64Array::from(
        result
            .rows
            .iter()
            .map(|row| i64::try_from(row.cohort_size).expect("size fits i64"))
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Int32Array::from(
        result
            .rows
            .iter()
            .map(|row| i32::try_from(row.period_offset).expect("offset fits i32"))
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Int64Array::from(
        result
            .rows
            .iter()
            .map(|row| i64::try_from(row.retained).expect("count fits i64"))
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Float64Array::from(
        result
            .rows
            .iter()
            .map(|row| row.retention_rate)
            .collect::<Vec<_>>(),
    )));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
    Ok((schema, vec![batch]))
}

/// The retention describe columns for a spec.
pub(crate) fn retention_describe_columns(spec: &RetentionSpec) -> Vec<(String, DataType)> {
    let mut columns = Vec::new();
    if spec.breakdown.is_some() {
        columns.push(("breakdown".to_owned(), DataType::Varchar));
    }
    columns.extend([
        ("cohort_start".to_owned(), DataType::Timestamp),
        ("cohort_size".to_owned(), DataType::BigInt),
        ("period_offset".to_owned(), DataType::Integer),
        ("retained".to_owned(), DataType::BigInt),
        ("retention_rate".to_owned(), DataType::Double),
    ]);
    columns
}

/// Map a segmentation result onto Arrow: `value` is BIGINT for COUNT /
/// UNIQUES and DOUBLE for COUNT_PER_UNIQUE (NULL when uniques is zero).
fn segment_to_arrow(
    result: &fq_events::SegmentResult,
) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let value_type = match result.measure {
        SegmentMeasure::Count | SegmentMeasure::Uniques => ArrowDataType::Int64,
        SegmentMeasure::CountPerUnique => ArrowDataType::Float64,
    };
    let mut fields = vec![Field::new("bucket", micros_type(), false)];
    if result.has_breakdown {
        fields.push(Field::new("breakdown", ArrowDataType::Utf8, true));
    }
    fields.push(Field::new(
        "value",
        value_type,
        result.measure == SegmentMeasure::CountPerUnique,
    ));
    let schema = Arc::new(Schema::new(fields));
    let mut columns: Vec<Arc<dyn arrow::array::Array>> =
        vec![Arc::new(TimestampMicrosecondArray::from(
            result.rows.iter().map(|row| row.bucket).collect::<Vec<_>>(),
        ))];
    if result.has_breakdown {
        let breakdowns: Vec<Option<String>> = result
            .rows
            .iter()
            .map(|row| row.breakdown.clone())
            .collect();
        columns.push(Arc::new(StringArray::from(breakdowns)));
    }
    match result.measure {
        SegmentMeasure::Count => columns.push(Arc::new(Int64Array::from(
            result
                .rows
                .iter()
                .map(|row| i64::try_from(row.count).expect("count fits i64"))
                .collect::<Vec<_>>(),
        ))),
        SegmentMeasure::Uniques => columns.push(Arc::new(Int64Array::from(
            result
                .rows
                .iter()
                .map(|row| i64::try_from(row.uniques).expect("count fits i64"))
                .collect::<Vec<_>>(),
        ))),
        SegmentMeasure::CountPerUnique => columns.push(Arc::new(Float64Array::from(
            result
                .rows
                .iter()
                .map(|row| {
                    if row.uniques == 0 {
                        None
                    } else {
                        Some(row.count as f64 / row.uniques as f64)
                    }
                })
                .collect::<Vec<_>>(),
        ))),
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
    Ok((schema, vec![batch]))
}

/// The segmentation describe columns for a spec.
pub(crate) fn segment_describe_columns(spec: &SegmentSpec) -> Vec<(String, DataType)> {
    let mut columns = vec![("bucket".to_owned(), DataType::Timestamp)];
    if spec.breakdown.is_some() {
        columns.push(("breakdown".to_owned(), DataType::Varchar));
    }
    columns.push((
        "value".to_owned(),
        match spec.measure {
            SegmentMeasure::Count | SegmentMeasure::Uniques => DataType::BigInt,
            SegmentMeasure::CountPerUnique => DataType::Double,
        },
    ));
    columns
}

/// Map a paths result onto Arrow: one row PER STEP of each ranked path, so the
/// result is directly processable (grouping, joining, pivoting) instead of a
/// display-only joined string. `path_id` is the path's rank; `occurrences` and
/// `share` are the WHOLE path's values, repeated on each of its step rows.
fn paths_to_arrow(
    result: &fq_events::PathsResult,
) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("path_id", ArrowDataType::Int32, false),
        Field::new("seq", ArrowDataType::Int32, false),
        Field::new("event", ArrowDataType::Utf8, false),
        Field::new("occurrences", ArrowDataType::Int64, false),
        Field::new("share", ArrowDataType::Float64, false),
    ]));
    let mut path_ids = Vec::new();
    let mut seqs = Vec::new();
    let mut events = Vec::new();
    let mut occurrences = Vec::new();
    let mut shares = Vec::new();
    for row in &result.rows {
        for (index, step) in row.steps.iter().enumerate() {
            path_ids.push(i32::try_from(row.rank).expect("rank fits i32"));
            seqs.push(i32::try_from(index + 1).expect("step index fits i32"));
            events.push(step.clone());
            occurrences.push(i64::try_from(row.occurrences).expect("count fits i64"));
            shares.push(row.share);
        }
    }
    let columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Int32Array::from(path_ids)),
        Arc::new(Int32Array::from(seqs)),
        Arc::new(StringArray::from(events)),
        Arc::new(Int64Array::from(occurrences)),
        Arc::new(Float64Array::from(shares)),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
    Ok((schema, vec![batch]))
}

/// The paths describe columns.
pub(crate) fn paths_describe_columns() -> Vec<(String, DataType)> {
    vec![
        ("path_id".to_owned(), DataType::Integer),
        ("seq".to_owned(), DataType::Integer),
        ("event".to_owned(), DataType::Varchar),
        ("occurrences".to_owned(), DataType::BigInt),
        ("share".to_owned(), DataType::Double),
    ]
}
