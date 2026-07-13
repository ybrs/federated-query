//! Materialized-view DDL execution: CREATE runs the defining SELECT through
//! the normal pipeline and persists the result via fq-accel; REFRESH re-runs
//! the stored SELECT and atomically replaces the chunks; DROP removes the
//! registry row and the chunks. Each DDL then re-registers the store's tables
//! with the exec data plane and swaps a fresh catalog snapshot in, so the very
//! next statement binds against the post-DDL state.

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};

use fq_accel::{
    views_schema, Accelerator, MaterializedView, MaterializedViewSource, VIEW_SCHEMA_NAME,
};
use fq_catalog::Catalog;
use fq_exec::connectors;
use fq_optimize::StatisticsCollector;

use crate::error::RuntimeError;
use crate::Runtime;

/// Open the materialized-view store that belongs to `config`, or None for a
/// programmatically built config with no file path (there is nowhere to keep a
/// store; DDL against such a runtime raises).
pub fn open_accelerator(config: &fq_common::Config) -> Result<Option<Accelerator>, RuntimeError> {
    let Some(source_path) = config.source_path.as_deref() else {
        return Ok(None);
    };
    Ok(Some(Accelerator::open(std::path::Path::new(source_path))?))
}

/// Register the store's current views in a catalog (as the datasource the
/// binder resolves) and in the exec data plane (the chunk tables DataFusion
/// reads). Runs at construction and after every DDL.
pub fn register_store(catalog: &mut Catalog, accel: &Accelerator) -> Result<(), RuntimeError> {
    let views = accel.views()?;
    register_data_plane(accel, &views)?;
    catalog.register_datasource(Arc::new(MaterializedViewSource::new(
        accel.datasource_name(),
        views,
    )));
    Ok(())
}

/// Point the exec data plane at the store's current chunk tables.
fn register_data_plane(
    accel: &Accelerator,
    views: &[MaterializedView],
) -> Result<(), RuntimeError> {
    let mut tables = Vec::with_capacity(views.len());
    for view in views {
        tables.push(connectors::MaterializedTable {
            table: view.name.clone(),
            chunks: accel.chunk_paths(view),
        });
    }
    connectors::register_materialized(
        accel.datasource_name().to_string(),
        accel.store_root().to_string_lossy().to_string(),
        tables,
    )?;
    Ok(())
}

impl Runtime {
    /// `CREATE MATERIALIZED VIEW name AS select`: plan and execute the SELECT,
    /// persist its rows as the view's chunks, register it, and expose it as a
    /// relation. A name that already resolves ANYWHERE (a source table or an
    /// existing view - the binder resolves bare names across every schema)
    /// raises instead of shadowing.
    pub(crate) fn create_materialized_view(
        &self,
        name: &str,
        select_sql: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        if self
            .catalog_snapshot()
            .resolve_table(None, None, name)
            .is_some()
        {
            return Err(RuntimeError::MaterializedView(format!(
                "relation '{name}' already exists"
            )));
        }
        let (schema, batches) = self.execute_query(select_sql)?;
        accel.create_view(name, select_sql, &schema, &batches)?;
        self.install_views()?;
        status_result("CREATE MATERIALIZED VIEW")
    }

    /// `REFRESH MATERIALIZED VIEW name`: re-execute the stored SELECT (a whole
    /// re-pull; delta refresh does not exist and its options raise at parse)
    /// and atomically replace the view's chunks.
    pub(crate) fn refresh_materialized_view(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        let view = accel.view(name)?;
        let (schema, batches) = self.execute_query(&view.definition_sql)?;
        accel.refresh_view(name, &schema, &batches)?;
        self.install_views()?;
        status_result("REFRESH MATERIALIZED VIEW")
    }

    /// `DROP MATERIALIZED VIEW name`: remove the registry row and the chunks;
    /// the next statement no longer resolves the name.
    pub(crate) fn drop_materialized_view(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        accel.drop_view(name)?;
        self.install_views()?;
        status_result("DROP MATERIALIZED VIEW")
    }

    /// The store, or a loud error for a config with no file path.
    fn accelerator(&self) -> Result<&Accelerator, RuntimeError> {
        self.accelerator.as_ref().ok_or_else(|| {
            RuntimeError::MaterializedView(
                "this runtime's config has no file path, so it has no materialized-view \
                 store; load the config from a file to use materialized views"
                    .to_string(),
            )
        })
    }

    /// Publish the store's post-DDL state to this runtime: re-register the
    /// exec-plane chunk tables, then swap in a catalog clone carrying the new
    /// view set, and rebuild the statistics collector over it (same learned
    /// catalog; only its in-memory caches reset - DDL is rare).
    fn install_views(&self) -> Result<(), RuntimeError> {
        let accel = self.accelerator()?;
        let views = accel.views()?;
        register_data_plane(accel, &views)?;
        let mut next = (*self.catalog_snapshot()).clone();
        next.insert_schema(
            accel.datasource_name(),
            VIEW_SCHEMA_NAME,
            views_schema(accel.datasource_name(), &views),
        );
        next.register_datasource(Arc::new(MaterializedViewSource::new(
            accel.datasource_name(),
            views,
        )));
        let next = Arc::new(next);
        *self.catalog.write().expect("catalog lock poisoned") = Arc::clone(&next);
        *self.stats.write().expect("stats lock poisoned") =
            Arc::new(StatisticsCollector::new(next, self.learned.clone(), None));
        Ok(())
    }
}

/// The one-row, one-column `status` table a DDL returns (the pg command-tag
/// convention rendered as a result set).
fn status_result(tag: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        ArrowDataType::Utf8,
        false,
    )]));
    let column = Arc::new(StringArray::from(vec![tag.to_string()]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column])
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
    Ok((schema, vec![batch]))
}
