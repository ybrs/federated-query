//! Event-view DDL and analysis-statement execution. An event view is a
//! materialized view (fq-accel owns chunks and lifecycle) plus a role row in
//! the event registry; every intermediate state is a valid plain materialized
//! view, so the two-registry lifecycle needs no cross-table transaction:
//!
//! - CREATE runs the defining SELECT through the normal pipeline, has
//!   fq-events validate the roles and sort the rows (the materialization
//!   contract), persists via the materialized-view store, and registers the
//!   roles LAST.
//! - REFRESH re-runs the stored SELECT, re-applies the whole contract, and
//!   swaps chunks through the store's publication path.
//! - DROP removes the role row FIRST, then the materialized view.
//! - FUNNEL / SEGMENT / PATHS read the view's chunks and run the fq-events
//!   kernels; they see exactly the last pull, like any read of the view.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fq_accel::{Accelerator, ChunkStore, MaterializedView};
use fq_common::events::{EventRoleColumns, FunnelSpec, PathsSpec, SegmentSpec};
use fq_common::DataType;
use fq_events::{
    build_event_view, build_sidecars, read_chunks, read_chunks_columns, run_funnel,
    run_funnel_pruned, run_funnel_row_indexed, run_paths, run_paths_pruned, run_segment,
    EntityBitmaps, EventStream, RowIndex, SegmentAggregate,
};
use fq_events::{EventError, EventViewRegistry, SidecarMeta};

use crate::delta;
use crate::error::RuntimeError;
use crate::materialized::status_result;
use crate::Runtime;

/// Open the event-view role registry that belongs to `config` - the same
/// stats SQLite the materialized-view registry lives in - or None for a
/// config with no file path (event DDL against such a runtime raises).
pub fn open_event_registry(
    config: &fq_common::Config,
) -> Result<Option<EventViewRegistry>, RuntimeError> {
    let Some(path) = crate::stats_sqlite_path(config) else {
        return Ok(None);
    };
    Ok(Some(EventViewRegistry::open(&path.to_string_lossy())?))
}

/// The engine types of the FUNNEL result relation, for `describe`; the
/// executed batches carry the same columns (`fq_events::funnel_schema`).
pub fn funnel_describe_columns() -> Vec<(String, DataType)> {
    vec![
        ("step".to_owned(), DataType::BigInt),
        ("event".to_owned(), DataType::Text),
        ("entities".to_owned(), DataType::BigInt),
        ("conversion_from_start".to_owned(), DataType::Double),
        ("conversion_from_previous".to_owned(), DataType::Double),
    ]
}

/// The engine types of the SEGMENT result relation, for `describe`; the
/// executed batches carry the same columns (`fq_events::segment_schema`).
pub fn segment_describe_columns() -> Vec<(String, DataType)> {
    vec![
        ("bucket".to_owned(), DataType::Timestamp),
        ("value".to_owned(), DataType::BigInt),
    ]
}

/// The engine types of the PATHS result relation, for `describe`; the
/// executed batches carry the same columns (`fq_events::paths_schema`).
pub fn paths_describe_columns() -> Vec<(String, DataType)> {
    vec![
        ("path".to_owned(), DataType::Text),
        ("entities".to_owned(), DataType::BigInt),
        ("depth".to_owned(), DataType::BigInt),
    ]
}

impl Runtime {
    /// `CREATE EVENT VIEW name ENTITY .. TIMESTAMP .. EVENT .. AS select`:
    /// execute the SELECT, enforce the role and sort contract, persist the
    /// sorted rows as a materialized view, then register the roles. A name
    /// that already resolves anywhere raises instead of shadowing.
    pub(crate) fn create_event_view(
        &self,
        name: &str,
        roles: &EventRoleColumns,
        select_sql: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        let registry = self.event_registry()?;
        let catalog = self.catalog_snapshot();
        if catalog.resolve_table(None, None, name).is_some() {
            return Err(RuntimeError::EventView(format!(
                "relation '{name}' already exists"
            )));
        }
        // Capture the base tables' version tokens BEFORE the pull (so they can
        // only understate freshness), like a plain materialized view. An event
        // view stores NO change-key state: the (entity, timestamp) sort spans
        // ALL rows, so a delta append would break the global order the kernels
        // require - every refresh re-pulls whole and re-sorts.
        let plan = delta::classify(&catalog, &self.config_snapshot(), select_sql)?;
        let tokens = delta::read_tokens(&catalog, &plan.base_tables)?;
        let (schema, batches) = self.execute_source_query(select_sql)?;
        let sorted = build_event_view(name, &schema, &batches, roles)?;
        accel.create_view(name, select_sql, &schema, &sorted, tokens, None)?;
        if let Err(error) = registry.register(name, roles) {
            // Never leave the half-created pair behind on a registration
            // race: this call owns the view it just created, so it removes it.
            accel.drop_view(name)?;
            return Err(error.into());
        }
        persist_sidecars(accel, registry, name, roles, &schema, &sorted)?;
        self.install_views()?;
        status_result("CREATE EVENT VIEW")
    }

    /// `REFRESH EVENT VIEW name`: no-op when every base table's version token
    /// still matches the last pull; otherwise re-execute the stored SELECT and
    /// re-apply the whole contract (validate + sort) before the store's atomic
    /// chunk swap. The re-executed SELECT must still produce the stored schema.
    ///
    /// An event view always re-pulls WHOLE (never a delta append): the
    /// (entity, timestamp) sort orders ALL rows, so appending a delta would
    /// break the global order the kernels require. The change-key state stays
    /// None; only the source tokens are recorded (to drive the no-op skip).
    pub(crate) fn refresh_event_view(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        let roles = self.event_roles(name)?;
        let view = accel.view(name)?;
        let catalog = self.catalog_snapshot();
        let plan = delta::classify(&catalog, &self.config_snapshot(), &view.definition_sql)?;
        let tokens = delta::read_tokens(&catalog, &plan.base_tables)?;
        if delta::tokens_allow_skip(&view.source_tokens, &tokens, &plan.base_tables) {
            return status_result("REFRESH EVENT VIEW (no-op: source tokens unchanged)");
        }
        let (schema, batches) = self.execute_source_query(&view.definition_sql)?;
        let sorted = build_event_view(name, &schema, &batches, &roles)?;
        accel.refresh_view(name, &schema, &sorted, &tokens, None)?;
        persist_sidecars(
            accel,
            self.event_registry()?,
            name,
            &roles,
            &schema,
            &sorted,
        )?;
        self.install_views()?;
        status_result("REFRESH EVENT VIEW")
    }

    /// `DROP EVENT VIEW name`: remove the role row (the name stops being an
    /// event view), then the materialized view under it. A plain materialized
    /// view raises here (its form is DROP MATERIALIZED VIEW).
    pub(crate) fn drop_event_view(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        self.event_registry()?.remove(name)?;
        accel.drop_view(name)?;
        self.install_views()?;
        status_result("DROP EVENT VIEW")
    }

    /// `FUNNEL OVER view ...`: run the funnel over the view's chunks. When the
    /// per-event ROW index loads for the current generation it drives the funnel
    /// (materializing only the step-event rows, or falling back to the full scan
    /// when they are too dense); otherwise the entity bitmaps prune by candidate
    /// entity, and with neither the plain full scan runs.
    ///
    /// The funnel consults only the entity, timestamp, and event columns (its
    /// matching is strict-increase, so tie order - the only thing a tiebreak
    /// decides - never changes a count), so the chunks are read projected to
    /// those three columns, skipping the decode of the property and tiebreak
    /// columns that at 100M rows dominate the read.
    pub(crate) fn run_funnel_statement(
        &self,
        spec: &FunnelSpec,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let (schema, batches, roles) = self.read_funnel_columns(&spec.view)?;
        if let Some(index) = self.load_row_index(&spec.view)? {
            return Ok(run_funnel_row_indexed(
                &spec.view, &schema, &batches, &roles, spec, &index,
            )?);
        }
        let stream = EventStream::open(&spec.view, &schema, &batches, &roles)?;
        match self.load_bitmaps(&spec.view)? {
            Some(bitmaps) => Ok(run_funnel_pruned(&stream, spec, &bitmaps)?),
            None => Ok(run_funnel(&stream, spec)?),
        }
    }

    /// An event view's chunks projected to the funnel's three consulted columns
    /// (entity, timestamp, event), plus a tiebreak-free role set naming them.
    /// Projection keeps every row, so the row index's ordinals still address the
    /// same rows; dropping the tiebreak is sound because the funnel is
    /// tie-independent (equal timestamps never advance a strict-increase match).
    fn read_funnel_columns(
        &self,
        view_name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, EventRoleColumns), RuntimeError> {
        let accel = self.accelerator()?;
        let roles = self.event_roles(view_name)?;
        let view = accel.view(view_name)?;
        let (schema, batches) = read_chunks_columns(
            &accel.chunk_paths(&view),
            &[&roles.entity, &roles.timestamp, &roles.event],
        )?;
        let funnel_roles = EventRoleColumns {
            entity: roles.entity,
            timestamp: roles.timestamp,
            event: roles.event,
            tiebreak: None,
        };
        Ok((schema, batches, funnel_roles))
    }

    /// `SEGMENT OVER view ...`: answer from the time-bucketed pre-aggregate when
    /// it loads and covers the bucket (DAY / WEEK / MONTH), avoiding the scan
    /// entirely; otherwise (BY HOUR, or no usable sidecar) run the kernel.
    pub(crate) fn run_segment_statement(
        &self,
        spec: &SegmentSpec,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        if let Some(aggregate) = self.load_segment(&spec.view)? {
            if let Some(result) = aggregate.serve(spec)? {
                return Ok(result);
            }
        }
        let stream = self.open_event_stream(&spec.view)?;
        Ok(run_segment(&stream, spec)?)
    }

    /// `PATHS OVER view ...`: run the paths kernel over the view's chunks, using
    /// the anchor event's bitmap to scan only its entities when the statement is
    /// anchored and the bitmaps load, else the plain full scan.
    pub(crate) fn run_paths_statement(
        &self,
        spec: &PathsSpec,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let stream = self.open_event_stream(&spec.view)?;
        match self.load_bitmaps(&spec.view)? {
            Some(bitmaps) => Ok(run_paths_pruned(&stream, spec, &bitmaps)?),
            None => Ok(run_paths(&stream, spec)?),
        }
    }

    /// The bitmap sidecar for the view's current generation, or None (a missing,
    /// stale-generation, or unreadable file falls back to the plain scan - the
    /// derived structure is never a source of truth).
    fn load_bitmaps(&self, name: &str) -> Result<Option<EntityBitmaps>, RuntimeError> {
        let Some((dir, generation)) = self.sidecar_context(name)? else {
            return Ok(None);
        };
        Ok(EntityBitmaps::load(&path_string(&dir, &bitmaps_name(generation))).ok())
    }

    /// The segment pre-aggregate sidecar for the view's current generation, or
    /// None (same fallback rule as `load_bitmaps`).
    fn load_segment(&self, name: &str) -> Result<Option<SegmentAggregate>, RuntimeError> {
        let Some((dir, generation)) = self.sidecar_context(name)? else {
            return Ok(None);
        };
        Ok(SegmentAggregate::load(&path_string(&dir, &segagg_name(generation))).ok())
    }

    /// The row-index sidecar for the view's current generation, or None (same
    /// fallback rule as `load_bitmaps`: a missing, stale-generation, or
    /// unreadable file falls back to the plain scan).
    fn load_row_index(&self, name: &str) -> Result<Option<RowIndex>, RuntimeError> {
        let Some((dir, generation)) = self.sidecar_context(name)? else {
            return Ok(None);
        };
        Ok(RowIndex::load(&path_string(&dir, &rowindex_name(generation))).ok())
    }

    /// The chunk directory and generation to read sidecars from, but ONLY when
    /// the registry records sidecars for exactly the view's current chunk
    /// generation. A mismatch (a build that predates the structures, or an
    /// interrupted refresh) returns None, so the kernel scans the chunks.
    fn sidecar_context(
        &self,
        name: &str,
    ) -> Result<Option<(std::path::PathBuf, u64)>, RuntimeError> {
        let accel = self.accelerator()?;
        let view = accel.view(name)?;
        let generation = current_generation(&view)?;
        let Some(meta) = self.event_registry()?.sidecar_meta(name)? else {
            return Ok(None);
        };
        if meta.generation != i64::try_from(generation).unwrap_or(i64::MIN) {
            return Ok(None);
        }
        Ok(Some((accel.store_root().join(&view.location), generation)))
    }

    /// Whether `name` is a registered event view (used by the materialized-
    /// view DDL to refuse operating on one: a plain-MV refresh would skip the
    /// sort contract). False when this runtime has no store at all.
    pub(crate) fn is_event_view(&self, name: &str) -> Result<bool, RuntimeError> {
        let Some(registry) = self.events.as_ref() else {
            return Ok(false);
        };
        Ok(registry.get(name)?.is_some())
    }

    /// The contract-verifying stream over an event view's stored chunks.
    fn open_event_stream(&self, view_name: &str) -> Result<EventStream, RuntimeError> {
        let (schema, batches, roles) = self.read_event_view(view_name)?;
        Ok(EventStream::open(view_name, &schema, &batches, &roles)?)
    }

    /// An event view's raw stored chunks (schema + batches) and its roles,
    /// WITHOUT opening a stream. The funnel row-index path takes the raw batches
    /// so it can gather only the step-event rows before normalizing them,
    /// instead of normalizing the whole stream up front.
    fn read_event_view(
        &self,
        view_name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>, EventRoleColumns), RuntimeError> {
        let accel = self.accelerator()?;
        let roles = self.event_roles(view_name)?;
        let view = accel.view(view_name)?;
        let (schema, batches) = read_chunks(&accel.chunk_paths(&view))?;
        Ok((schema, batches, roles))
    }

    /// The roles of a registered event view, or a loud `UnknownEventView`
    /// (accurate even when a plain materialized view holds the name: that
    /// relation is not an event view).
    fn event_roles(&self, name: &str) -> Result<EventRoleColumns, RuntimeError> {
        let roles = self.event_registry()?.get(name)?;
        Ok(roles.ok_or_else(|| EventError::UnknownEventView(name.to_string()))?)
    }

    /// The role registry, or a loud error for a config with no file path.
    fn event_registry(&self) -> Result<&EventViewRegistry, RuntimeError> {
        self.events.as_ref().ok_or_else(|| {
            RuntimeError::EventView(
                "this runtime's config has no file path, so it has no event-view \
                 store; load the config from a file to use event views"
                    .to_string(),
            )
        })
    }
}

/// Build both derived sidecars over the just-published sorted rows and write
/// them beside the chunks under the current generation, then record their
/// generation and sizes in the registry. Old-generation sidecar files are
/// unlinked. The registry record is written LAST, so a crash mid-write leaves
/// the generation unrecorded and the kernels read the plain chunks.
fn persist_sidecars(
    accel: &Accelerator,
    registry: &EventViewRegistry,
    name: &str,
    roles: &EventRoleColumns,
    schema: &SchemaRef,
    sorted: &[RecordBatch],
) -> Result<(), RuntimeError> {
    let view = accel.view(name)?;
    let generation = current_generation(&view)?;
    let directory = accel.store_root().join(&view.location);
    let build = build_sidecars(name, schema, sorted, roles)?;
    let bitmaps_bytes = write_sidecar(
        &directory,
        &bitmaps_name(generation),
        &build.bitmaps.to_bytes()?,
    )?;
    let rowindex_bytes = write_sidecar(
        &directory,
        &rowindex_name(generation),
        &build.row_index.to_bytes()?,
    )?;
    let segagg_bytes = write_sidecar(
        &directory,
        &segagg_name(generation),
        &build.segment.to_bytes()?,
    )?;
    remove_stale_sidecars(&directory, generation)?;
    registry.record_sidecars(
        name,
        &SidecarMeta {
            generation: i64::try_from(generation).expect("generation fits i64"),
            bitmaps_bytes,
            rowindex_bytes,
            segagg_bytes,
        },
    )?;
    Ok(())
}

/// The chunk generation a view currently serves. Every chunk of a whole-rewrite
/// event view shares one generation, so the first chunk names it.
fn current_generation(view: &MaterializedView) -> Result<u64, RuntimeError> {
    let first = view.chunk_list.first().ok_or_else(|| {
        RuntimeError::EventView(format!("event view '{}' has no chunks", view.name))
    })?;
    ChunkStore::next_generation(std::slice::from_ref(first))
        .map(|next| next - 1)
        .map_err(RuntimeError::from)
}

/// The bitmap sidecar file name for a generation.
fn bitmaps_name(generation: u64) -> String {
    format!("bitmaps-{generation}.fqb")
}

/// The row-index sidecar file name for a generation.
fn rowindex_name(generation: u64) -> String {
    format!("rowindex-{generation}.fqr")
}

/// The segment pre-aggregate sidecar file name for a generation.
fn segagg_name(generation: u64) -> String {
    format!("segagg-{generation}.arrow")
}

/// The absolute path string of a file in the sidecar directory.
fn path_string(directory: &std::path::Path, file: &str) -> String {
    directory.join(file).to_string_lossy().to_string()
}

/// Write a sidecar file atomically (a temp file renamed into place, so a
/// concurrent reader never sees a torn file) and return its byte size.
fn write_sidecar(
    directory: &std::path::Path,
    file: &str,
    bytes: &[u8],
) -> Result<i64, RuntimeError> {
    let final_path = directory.join(file);
    let temp_path = directory.join(format!("{file}.tmp-{}", std::process::id()));
    std::fs::write(&temp_path, bytes).map_err(EventError::from)?;
    std::fs::rename(&temp_path, &final_path).map_err(EventError::from)?;
    Ok(i64::try_from(bytes.len()).expect("sidecar size fits i64"))
}

/// Unlink sidecar files of a superseded generation left in the directory (the
/// current generation's files are kept). A file already gone is fine.
fn remove_stale_sidecars(directory: &std::path::Path, keep: u64) -> Result<(), RuntimeError> {
    let keep_files = [bitmaps_name(keep), rowindex_name(keep), segagg_name(keep)];
    let entries = std::fs::read_dir(directory).map_err(EventError::from)?;
    for entry in entries {
        let entry = entry.map_err(EventError::from)?;
        let name = entry.file_name().to_string_lossy().to_string();
        if is_sidecar_file(&name) && !keep_files.contains(&name) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(())
}

/// Whether a directory entry is a sidecar file (and not a chunk or a temp file).
fn is_sidecar_file(name: &str) -> bool {
    let extension = std::path::Path::new(name)
        .extension()
        .and_then(|value| value.to_str());
    (name.starts_with("bitmaps-") && extension == Some("fqb"))
        || (name.starts_with("rowindex-") && extension == Some("fqr"))
        || (name.starts_with("segagg-") && extension == Some("arrow"))
}
