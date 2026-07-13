//! The materialized-view lifecycle over the chunk store and the registry.
//!
//! The ordering invariants that keep chunks and catalog rows consistent:
//!
//! - CREATE writes chunks FIRST, then registers the row; a failed registration
//!   (a concurrent duplicate) removes its own chunks. A crash between the two
//!   leaves an unreferenced directory, which the registration path of a later
//!   create of the same name overwrites generation-0 files of.
//! - REFRESH writes the new generation's files first, then swaps the catalog
//!   chunk list in one transaction, then unlinks the superseded files. A
//!   reader that loaded the old row keeps its already-open files (POSIX).
//! - DROP tombstones the row first (the view stops resolving), then unlinks
//!   the files, then purges the row. `sweep_tombstones` (run on open) finishes
//!   any drop a crash interrupted.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::catalog::ViewCatalog;
use crate::error::AccelError;
use crate::store::ChunkStore;
use crate::view::{columns_from_arrow, location_for, MaterializedView};

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
        let stem = config_path
            .file_stem()
            .map_or_else(|| "fedq".to_string(), |s| s.to_string_lossy().to_string());
        let parent = config_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let catalog_path = parent.join(format!("{stem}.stats.sqlite"));
        let store_root = parent.join(format!("{stem}.mv"));
        let catalog = ViewCatalog::open(&catalog_path.to_string_lossy())?;
        let store = ChunkStore::open(&store_root)?;
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
    /// generation-0 chunks, then register the catalog row. A duplicate name
    /// raises and removes the chunks this call wrote.
    pub fn create_view(
        &self,
        name: &str,
        definition_sql: &str,
        schema: &SchemaRef,
        batches: &[RecordBatch],
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
        };
        if let Err(error) = self.catalog.register(&view) {
            self.store.delete_chunks(&location, &chunk_list)?;
            return Err(error);
        }
        self.view(name)
    }

    /// Replace a view's contents with a freshly executed result (the whole
    /// re-pull): new-generation chunks first, one catalog swap, then unlink
    /// the superseded chunks. The re-executed SELECT must still produce the
    /// stored column names and types; a drifted source schema raises and the
    /// view keeps serving its previous contents.
    pub fn refresh_view(
        &self,
        name: &str,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<MaterializedView, AccelError> {
        let current = self.view(name)?;
        let columns = columns_from_arrow(schema)?;
        require_same_shape(&current, &columns)?;
        let generation = ChunkStore::next_generation(&current.chunk_list)?;
        let (chunk_list, byte_size) =
            self.store
                .write_chunks(&current.location, generation, schema, batches)?;
        self.catalog
            .publish_refresh(name, &chunk_list, count_rows(batches), byte_size)?;
        self.store
            .delete_chunks(&current.location, &current.chunk_list)?;
        self.view(name)
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
