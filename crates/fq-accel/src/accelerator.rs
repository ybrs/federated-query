//! The materialized-view lifecycle over the chunk store and the registry.
//!
//! The ordering invariants that keep chunks and catalog rows consistent:
//!
//! - CREATE writes chunks FIRST, then registers the row; a failed registration
//!   (a concurrent duplicate) removes its own chunks. A crash between the two
//!   leaves an unreferenced directory, which the registration path of a later
//!   create of the same name overwrites generation-0 files of.
//! - Every REFRESH variant (whole, delta append, merge) writes its new
//!   generation's files first, then swaps the catalog row - chunk list, source
//!   tokens, change-key state - in ONE transaction, then unlinks whatever that
//!   swap superseded. A reader that loaded the old row keeps its already-open
//!   files (POSIX).
//! - DROP tombstones the row first (the view stops resolving), then unlinks
//!   the files, then purges the row. `sweep_tombstones` (run on open) finishes
//!   any drop a crash interrupted.

use std::collections::BTreeMap;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::catalog::ViewCatalog;
use crate::error::AccelError;
use crate::merge::{plan_merge, ChunkPlan, StoredChunk};
use crate::store::ChunkStore;
use crate::view::{columns_from_arrow, location_for, ChangeKeyState, MaterializedView};
use crate::watermark::{scan_watermark, WatermarkScan};

/// One config's materialized-view store: registry + chunks + the datasource
/// name it is exposed under.
pub struct Accelerator {
    catalog: ViewCatalog,
    store: ChunkStore,
    datasource_name: String,
}

impl Accelerator {
    /// Open the store that belongs to the config at `config_path`: the
    /// registry inside `<stem>.stats.sqlite` and chunks under `<stem>.mv/`,
    /// both next to the config. Sweeps any drop a crash interrupted.
    pub fn open(config_path: &std::path::Path) -> Result<Self, AccelError> {
        Self::open_with_chunk_bytes(config_path, None)
    }

    /// Open with an explicit chunk-size bound (tests lower it to build
    /// multi-chunk views over small data); None keeps the store default.
    pub fn open_with_chunk_bytes(
        config_path: &std::path::Path,
        chunk_bytes: Option<usize>,
    ) -> Result<Self, AccelError> {
        let stem = config_path
            .file_stem()
            .map_or_else(|| "fedq".to_string(), |s| s.to_string_lossy().to_string());
        let parent = config_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let catalog_path = parent.join(format!("{stem}.stats.sqlite"));
        let store_root = parent.join(format!("{stem}.mv"));
        let catalog = ViewCatalog::open(&catalog_path.to_string_lossy())?;
        let store = match chunk_bytes {
            Some(bytes) => ChunkStore::open_with_chunk_bytes(&store_root, bytes)?,
            None => ChunkStore::open(&store_root)?,
        };
        // The datasource name embeds the store root: unique per config (many
        // runtimes in one process share one process-wide connector registry)
        // and impossible to collide with a user-configured datasource name.
        let datasource_name = format!("accel:{}", store_root.display());
        let accelerator = Self {
            catalog,
            store,
            datasource_name,
        };
        accelerator.sweep_tombstones()?;
        Ok(accelerator)
    }

    /// The datasource name this store's views are exposed under.
    pub fn datasource_name(&self) -> &str {
        &self.datasource_name
    }

    /// The chunk-store root directory.
    pub fn store_root(&self) -> &std::path::Path {
        self.store.root()
    }

    /// Every live view.
    pub fn views(&self) -> Result<Vec<MaterializedView>, AccelError> {
        self.catalog.list_live()
    }

    /// Record one automatic substitution that read `name` in place of a
    /// recompute: advance its `use_count` and add `saved` (the cost model's
    /// estimated saving) to `cost_saved`. Called AFTER a substituted query
    /// executes, off its critical path. A dropped view is a no-op.
    pub fn record_substitution(&self, name: &str, saved: f64) -> Result<(), AccelError> {
        self.catalog.record_substitution(name, saved)
    }

    /// The live view named `name`, or a loud `UnknownView`.
    pub fn view(&self, name: &str) -> Result<MaterializedView, AccelError> {
        self.catalog
            .get(name)?
            .ok_or_else(|| AccelError::UnknownView(name.to_string()))
    }

    /// The absolute chunk-file paths of a view, for the execution plane.
    pub fn chunk_paths(&self, view: &MaterializedView) -> Vec<String> {
        let mut paths = Vec::with_capacity(view.chunk_list.len());
        for chunk in &view.chunk_list {
            paths.push(
                self.store
                    .chunk_path(&view.location, chunk)
                    .to_string_lossy()
                    .to_string(),
            );
        }
        paths
    }

    /// Materialize a new view from an executed result: persist the rows as
    /// generation-0 chunks, then register the catalog row carrying the source
    /// tokens (captured by the caller BEFORE the pull) and, for a view under a
    /// monotonic change key, the initial watermark state. A duplicate name
    /// raises and removes the chunks this call wrote.
    pub fn create_view(
        &self,
        name: &str,
        definition_sql: &str,
        schema: &SchemaRef,
        batches: &[RecordBatch],
        source_tokens: BTreeMap<String, String>,
        change_key: Option<ChangeKeyState>,
    ) -> Result<MaterializedView, AccelError> {
        let columns = columns_from_arrow(schema)?;
        let location = location_for(name);
        let (chunk_list, byte_size) = self.store.write_chunks(&location, 0, schema, batches)?;
        let view = MaterializedView {
            name: name.to_string(),
            definition_sql: definition_sql.to_string(),
            location: location.clone(),
            chunk_list: chunk_list.clone(),
            columns,
            measured_rows: count_rows(batches),
            byte_size,
            // The registry stamps the real timestamps; these placeholders are
            // never written.
            created_at: String::new(),
            refreshed_at: None,
            source_tokens,
            change_key,
            // A new view has served no substitutions yet; the registry stamps
            // and advances these, never this placeholder.
            use_count: 0,
            cost_saved: 0.0,
        };
        if let Err(error) = self.catalog.register(&view) {
            self.store.delete_chunks(&location, &chunk_list)?;
            return Err(error);
        }
        self.view(name)
    }

    /// Replace a view's contents with a freshly executed result (the whole
    /// re-pull): new-generation chunks first, one catalog swap (chunks, rows,
    /// tokens, change-key state together), then unlink the superseded chunks.
    /// The re-executed SELECT must still produce the stored column names and
    /// types; a drifted source schema raises and the view keeps serving its
    /// previous contents.
    pub fn refresh_view(
        &self,
        name: &str,
        schema: &SchemaRef,
        batches: &[RecordBatch],
        source_tokens: &BTreeMap<String, String>,
        change_key: Option<&ChangeKeyState>,
    ) -> Result<MaterializedView, AccelError> {
        let current = self.view(name)?;
        let columns = columns_from_arrow(schema)?;
        require_same_shape(&current, &columns)?;
        let generation = ChunkStore::next_generation(&current.chunk_list)?;
        let (chunk_list, byte_size) =
            self.store
                .write_chunks(&current.location, generation, schema, batches)?;
        self.catalog.publish_refresh(
            name,
            &chunk_list,
            count_rows(batches),
            byte_size,
            source_tokens,
            change_key,
        )?;
        self.store
            .delete_chunks(&current.location, &current.chunk_list)?;
        self.view(name)
    }

    /// Append a delta pull (rows past the watermark) as new-generation chunks.
    /// Existing chunk files are NOT touched: the catalog swap extends the list
    /// and updates the watermark state and tokens in one transaction. An empty
    /// delta writes no files and still publishes the tokens/state (the pull
    /// happened; recording it lets the next refresh skip on equal tokens).
    pub fn refresh_view_append(
        &self,
        name: &str,
        schema: &SchemaRef,
        delta_batches: &[RecordBatch],
        source_tokens: &BTreeMap<String, String>,
        change_key: &ChangeKeyState,
    ) -> Result<MaterializedView, AccelError> {
        let current = self.view(name)?;
        let columns = columns_from_arrow(schema)?;
        require_same_shape(&current, &columns)?;
        let generation = ChunkStore::next_generation(&current.chunk_list)?;
        let written = self.store.write_chunks_from(
            &current.location,
            generation,
            0,
            schema,
            delta_batches,
        )?;
        let mut chunk_list = current.chunk_list.clone();
        let mut new_bytes: i64 = 0;
        for (name, bytes) in written {
            chunk_list.push(name);
            new_bytes += bytes;
        }
        self.catalog.publish_refresh(
            name,
            &chunk_list,
            current.measured_rows + count_rows(delta_batches),
            current.byte_size + new_bytes,
            source_tokens,
            Some(change_key),
        )?;
        self.view(name)
    }

    /// Merge a fresh WHOLE pull into the stored chunks by primary key:
    /// unchanged chunks survive byte-for-byte under their existing names,
    /// chunks holding a changed or deleted key are rewritten at the new
    /// generation, and new keys append after them. One catalog swap publishes
    /// the merged list with the tokens; only then are superseded files
    /// unlinked. The caller has already verified the fresh arrow schema
    /// matches the stored chunks; a mismatch inside raises.
    pub fn refresh_view_merge(
        &self,
        name: &str,
        schema: &SchemaRef,
        fresh_batches: &[RecordBatch],
        key_column: &str,
        source_tokens: &BTreeMap<String, String>,
    ) -> Result<MergeOutcome, AccelError> {
        let current = self.view(name)?;
        let columns = columns_from_arrow(schema)?;
        require_same_shape(&current, &columns)?;
        let key_index = key_column_index(schema, key_column)?;
        let stored = self.read_stored_chunks(&current, schema)?;
        let plan = plan_merge(schema, &stored, fresh_batches, key_index)?;
        let generation = ChunkStore::next_generation(&current.chunk_list)?;
        let published = self.publish_merge(&current, schema, plan, generation, source_tokens)?;
        Ok(published)
    }

    /// Every stored chunk of `view`, verified to carry exactly the arrow
    /// schema of the fresh pull - the merge compares row encodings, which is
    /// only meaningful over one layout.
    fn read_stored_chunks(
        &self,
        view: &MaterializedView,
        schema: &SchemaRef,
    ) -> Result<Vec<StoredChunk>, AccelError> {
        let mut stored = Vec::with_capacity(view.chunk_list.len());
        for chunk in &view.chunk_list {
            let (chunk_schema, batches, byte_size) =
                self.store.read_chunk(&view.location, chunk)?;
            if chunk_schema.fields() != schema.fields() {
                return Err(AccelError::InvalidSchema(format!(
                    "stored chunk '{chunk}' of '{}' has a different arrow layout \
                     than the fresh pull; the caller must re-pull whole",
                    view.name
                )));
            }
            stored.push(StoredChunk {
                name: chunk.clone(),
                batches,
                byte_size,
            });
        }
        Ok(stored)
    }

    /// Write a merge plan's rewrites and inserts at `generation`, swap the
    /// catalog row, then unlink the superseded files.
    fn publish_merge(
        &self,
        current: &MaterializedView,
        schema: &SchemaRef,
        plan: crate::merge::MergePlan,
        generation: u64,
        source_tokens: &BTreeMap<String, String>,
    ) -> Result<MergeOutcome, AccelError> {
        let mut chunk_list = Vec::new();
        let mut superseded = Vec::new();
        let mut byte_size: i64 = 0;
        let mut rewritten = 0usize;
        let mut kept = 0usize;
        let mut next_index = 0usize;
        for chunk_plan in plan.chunk_plans {
            match chunk_plan {
                ChunkPlan::Keep {
                    name,
                    byte_size: chunk_bytes,
                } => {
                    chunk_list.push(name);
                    byte_size += chunk_bytes;
                    kept += 1;
                }
                ChunkPlan::Rewrite {
                    superseded: old,
                    rows,
                } => {
                    superseded.push(old);
                    rewritten += 1;
                    // A chunk every row left is dropped, not rewritten empty.
                    if rows.num_rows() > 0 {
                        let (name, bytes) = self.store.write_chunk(
                            &current.location,
                            generation,
                            next_index,
                            schema,
                            &[&rows],
                        )?;
                        next_index += 1;
                        chunk_list.push(name);
                        byte_size += bytes;
                    }
                }
            }
        }
        let appended =
            self.write_inserts(current, schema, plan.inserts, generation, &mut next_index)?;
        for (name, bytes) in &appended {
            chunk_list.push(name.clone());
            byte_size += bytes;
        }
        if chunk_list.is_empty() {
            // The merged view is empty; keep the at-least-one-chunk invariant
            // (the chunk carries the schema for the execution plane).
            let (name, bytes) =
                self.store
                    .write_chunk(&current.location, generation, next_index, schema, &[])?;
            chunk_list.push(name);
            byte_size += bytes;
        }
        self.catalog.publish_refresh(
            &current.name,
            &chunk_list,
            plan.total_rows,
            byte_size,
            source_tokens,
            None,
        )?;
        self.store.delete_chunks(&current.location, &superseded)?;
        Ok(MergeOutcome {
            view: self.view(&current.name)?,
            chunks_kept: kept,
            chunks_rewritten: rewritten,
            chunks_appended: appended.len(),
        })
    }

    /// Write the merge plan's insert batch (if any) as size-rotated chunks
    /// continuing at `next_index`; returns the (name, bytes) pairs.
    fn write_inserts(
        &self,
        current: &MaterializedView,
        schema: &SchemaRef,
        inserts: Option<RecordBatch>,
        generation: u64,
        next_index: &mut usize,
    ) -> Result<Vec<(String, i64)>, AccelError> {
        let Some(batch) = inserts else {
            return Ok(Vec::new());
        };
        let written = self.store.write_chunks_from(
            &current.location,
            generation,
            *next_index,
            schema,
            std::slice::from_ref(&batch),
        )?;
        *next_index += written.len();
        Ok(written)
    }

    /// The arrow schema of the view's stored chunks (the first chunk's header;
    /// every chunk shares it by construction). Used by the refresh path to
    /// verify a delta/merge result still matches the stored layout before
    /// touching any file.
    pub fn stored_arrow_schema(&self, view: &MaterializedView) -> Result<SchemaRef, AccelError> {
        let first = view.chunk_list.first().ok_or_else(|| {
            AccelError::InvalidSchema(format!("view '{}' has no chunks", view.name))
        })?;
        let (schema, _, _) = self.store.read_chunk(&view.location, first)?;
        Ok(schema)
    }

    /// The append watermark derived from the view's STORED chunks: the max of
    /// output column `column` over every stored row. Used when the registry
    /// carries no usable watermark state (a change key declared after the
    /// view was created); reads local chunk files only, never a source.
    pub fn chunk_watermark(
        &self,
        view: &MaterializedView,
        column: &str,
    ) -> Result<WatermarkScan, AccelError> {
        let mut max: Option<crate::watermark::Watermark> = None;
        for chunk in &view.chunk_list {
            let (schema, batches, _) = self.store.read_chunk(&view.location, chunk)?;
            let index = key_column_index(&schema, column)?;
            match scan_watermark(&batches, index)? {
                WatermarkScan::Unsupported(reason) => {
                    return Ok(WatermarkScan::Unsupported(reason))
                }
                WatermarkScan::Value(None) => {}
                WatermarkScan::Value(Some(value)) => {
                    max = Some(match max {
                        None => value,
                        Some(current) => current.max_with(value)?,
                    });
                }
            }
        }
        Ok(WatermarkScan::Value(max))
    }

    /// Drop a view: tombstone (it stops resolving), unlink its files, purge
    /// the row. An unknown name raises.
    pub fn drop_view(&self, name: &str) -> Result<(), AccelError> {
        let view = self.catalog.tombstone(name)?;
        self.store.delete_chunks(&view.location, &view.chunk_list)?;
        self.store.delete_location(&view.location)?;
        self.catalog.purge(name)
    }

    /// Finish every drop a crash interrupted: a tombstoned row's files are
    /// removed and the row purged.
    fn sweep_tombstones(&self) -> Result<(), AccelError> {
        for view in self.catalog.list_tombstoned()? {
            self.store.delete_chunks(&view.location, &view.chunk_list)?;
            self.store.delete_location(&view.location)?;
            self.catalog.purge(&view.name)?;
        }
        Ok(())
    }
}

/// What a primary-key merge did to the store, for the refresh status row.
#[derive(Debug)]
pub struct MergeOutcome {
    pub view: MaterializedView,
    /// Chunks that survived byte-for-byte.
    pub chunks_kept: usize,
    /// Chunks holding a changed or deleted key (rewritten, or dropped when
    /// every row left).
    pub chunks_rewritten: usize,
    /// New chunks appended for keys no stored chunk held.
    pub chunks_appended: usize,
}

/// The index of `column` in an arrow schema, matched case-insensitively (the
/// binder's resolution rule). The refresh path resolved the column against the
/// view before calling, so a miss here is a drifted store and raises.
fn key_column_index(schema: &SchemaRef, column: &str) -> Result<usize, AccelError> {
    for (index, field) in schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(column) {
            return Ok(index);
        }
    }
    Err(AccelError::InvalidSchema(format!(
        "column '{column}' is not in the view's stored schema"
    )))
}

/// Total row count across the result batches.
fn count_rows(batches: &[RecordBatch]) -> i64 {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    i64::try_from(rows).expect("row count fits i64")
}

/// Raise unless the refreshed result has exactly the stored column names and
/// types, in order. A view's schema is fixed at creation; a source change that
/// alters it needs an explicit DROP + CREATE, not a silent mutation under
/// every reader of the view.
fn require_same_shape(
    current: &MaterializedView,
    refreshed: &[crate::view::ViewColumn],
) -> Result<(), AccelError> {
    let same = current.columns.len() == refreshed.len()
        && current
            .columns
            .iter()
            .zip(refreshed)
            .all(|(a, b)| a.name == b.name && a.data_type == b.data_type);
    if same {
        return Ok(());
    }
    Err(AccelError::InvalidSchema(format!(
        "refresh of '{}' produced a different schema than the view was created \
         with ({} vs {}); DROP and re-CREATE the view to change its shape",
        current.name,
        shape_text(refreshed),
        shape_text(&current.columns),
    )))
}

/// A compact `name TYPE, ...` rendering of a column list for error text.
fn shape_text(columns: &[crate::view::ViewColumn]) -> String {
    let mut parts = Vec::with_capacity(columns.len());
    for column in columns {
        parts.push(format!("{} {}", column.name, column.data_type.value()));
    }
    parts.join(", ")
}
