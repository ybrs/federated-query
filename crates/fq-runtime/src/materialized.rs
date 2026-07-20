//! Materialized-view DDL execution: CREATE runs the defining SELECT through
//! the normal pipeline and persists the result via fq-accel; REFRESH brings a
//! view forward (skipping, delta-appending, merging, or re-pulling whole - see
//! `refresh_materialized_view`); DROP removes the registry row and the chunks.
//! Each DDL then re-registers the store's tables with the exec data plane and
//! swaps a fresh catalog snapshot in, so the very next statement binds against
//! the post-DDL state.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};

use fq_accel::{
    scan_watermark, views_schema, Accelerator, ChangeKeyState, MaterializedView,
    MaterializedViewSource, WatermarkScan, VIEW_SCHEMA_NAME,
};
use fq_catalog::Catalog;
use fq_common::DataType;
use fq_exec::connectors;
use fq_optimize::StatisticsCollector;

use crate::delta::{self, RefreshDecision, RefreshPlan};
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
pub fn register_store(
    session: connectors::SessionId,
    catalog: &mut Catalog,
    accel: &Accelerator,
) -> Result<(), RuntimeError> {
    let views = accel.views()?;
    register_data_plane(session, accel, &views)?;
    catalog.register_datasource(Arc::new(MaterializedViewSource::new(
        accel.datasource_name(),
        views,
    )));
    Ok(())
}

/// Point the exec data plane at the store's current chunk tables for `session`.
fn register_data_plane(
    session: connectors::SessionId,
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
        session,
        accel.datasource_name().to_string(),
        accel.store_root().to_string_lossy().to_string(),
        tables,
    )?;
    Ok(())
}

impl Runtime {
    /// `CREATE MATERIALIZED VIEW name AS select`: plan and execute the SELECT,
    /// persist its rows as the view's chunks, register it, and expose it as a
    /// relation. Base-table version tokens are captured BEFORE the pull (so
    /// they can only understate freshness), and a view admitted for delta
    /// append stores its initial watermark. A name that already resolves
    /// ANYWHERE (a source table or an existing view - the binder resolves bare
    /// names across every schema) raises instead of shadowing.
    pub(crate) fn create_materialized_view(
        &self,
        name: &str,
        select_sql: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        let catalog = self.catalog_snapshot();
        if catalog.resolve_table(None, None, name).is_some() {
            return Err(RuntimeError::MaterializedView(format!(
                "relation '{name}' already exists"
            )));
        }
        let plan = delta::classify(&catalog, &self.config_snapshot(), select_sql)?;
        let tokens = delta::read_tokens(&catalog, &plan.base_tables)?;
        let (schema, batches) = self.execute_source_query(select_sql)?;
        let state = initial_change_key_state(&plan, &schema, &batches);
        accel.create_view(name, select_sql, &schema, &batches, tokens, state)?;
        self.install_views()?;
        status_result("CREATE MATERIALIZED VIEW")
    }

    /// `REFRESH MATERIALIZED VIEW name`: bring the view forward and report HOW
    /// in the status row. In order:
    ///
    /// 1. NO-OP when every base table's current version token equals the one
    ///    stored at the last pull - nothing is pulled and nothing is written.
    /// 2. DELTA APPEND when the view's single base table declares a monotonic
    ///    change key and the shape admits it (`crate::delta`): pull rows past
    ///    the watermark, append them as new chunks.
    /// 3. MERGE when the base table declares a primary key: re-pull whole,
    ///    rewrite only the chunks whose rows changed.
    /// 4. WHOLE re-pull otherwise; the status row names the reason.
    pub(crate) fn refresh_materialized_view(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        let view = accel.view(name)?;
        let catalog = self.catalog_snapshot();
        let plan = delta::classify(&catalog, &self.config_snapshot(), &view.definition_sql)?;
        let tokens = delta::read_tokens(&catalog, &plan.base_tables)?;
        if delta::tokens_allow_skip(&view.source_tokens, &tokens, &plan.base_tables) {
            // Nothing is written: the stored row (chunks, tokens, watermark)
            // is already the current state.
            return status_result("REFRESH MATERIALIZED VIEW (no-op: source tokens unchanged)");
        }
        let status = match &plan.decision {
            RefreshDecision::Whole { reason } => {
                self.refresh_whole(accel, &view, &tokens, None, reason)?
            }
            RefreshDecision::Append { output_column } => {
                self.refresh_append(accel, &view, &tokens, output_column)?
            }
            RefreshDecision::Merge { key_column } => {
                self.refresh_merge(accel, &view, &tokens, key_column)?
            }
        };
        self.install_views()?;
        status_result(&status)
    }

    /// Whole re-pull: re-execute the stored SELECT and swap every chunk. When
    /// the view is append-admissible (`append_column`) the fresh watermark is
    /// stored alongside, so the NEXT refresh can pull a delta.
    fn refresh_whole(
        &self,
        accel: &Accelerator,
        view: &MaterializedView,
        tokens: &BTreeMap<String, String>,
        append_column: Option<&str>,
        reason: &str,
    ) -> Result<String, RuntimeError> {
        let (schema, batches) = self.execute_source_query(&view.definition_sql)?;
        let state = match append_column {
            Some(column) => watermark_state(&schema, &batches, column),
            None => None,
        };
        accel.refresh_view(&view.name, &schema, &batches, tokens, state.as_ref())?;
        Ok(format!(
            "REFRESH MATERIALIZED VIEW (whole re-pull: {reason})"
        ))
    }

    /// Delta append: pull rows past the stored high-water mark and append them
    /// as new chunks, never touching existing files. Falls back to a whole
    /// re-pull (stating why) when no usable watermark can be resolved or the
    /// engine's result layout no longer matches the stored chunks.
    fn refresh_append(
        &self,
        accel: &Accelerator,
        view: &MaterializedView,
        tokens: &BTreeMap<String, String>,
        output_column: &str,
    ) -> Result<String, RuntimeError> {
        let watermark = match resolve_watermark(accel, view, output_column)? {
            Ok(watermark) => watermark,
            Err(reason) => {
                return self.refresh_whole(accel, view, tokens, Some(output_column), &reason)
            }
        };
        let sql = delta::delta_sql(
            &view.definition_sql,
            &view.columns,
            output_column,
            watermark.as_ref(),
        );
        let (schema, batches) = self.execute_source_query(&sql)?;
        let stored_schema = accel.stored_arrow_schema(view)?;
        if stored_schema.fields() != schema.fields() {
            return self.refresh_whole(
                accel,
                view,
                tokens,
                Some(output_column),
                "the engine's result layout no longer matches the stored chunks",
            );
        }
        let delta_rows = count_rows(&batches);
        let advanced = match advance_watermark(watermark, &schema, &batches, output_column)? {
            Ok(advanced) => advanced,
            Err(reason) => {
                return self.refresh_whole(accel, view, tokens, Some(output_column), &reason)
            }
        };
        let state = ChangeKeyState {
            column: output_column.to_string(),
            watermark: advanced,
        };
        accel.refresh_view_append(&view.name, &schema, &batches, tokens, &state)?;
        Ok(format!(
            "REFRESH MATERIALIZED VIEW (delta append: {delta_rows} rows)"
        ))
    }

    /// Primary-key merge: re-execute the stored SELECT whole and merge it by
    /// key, rewriting only affected chunks. Falls back to a whole swap when
    /// the engine's result layout no longer matches the stored chunks (a row
    /// diff is only meaningful over one layout).
    fn refresh_merge(
        &self,
        accel: &Accelerator,
        view: &MaterializedView,
        tokens: &BTreeMap<String, String>,
        key_column: &str,
    ) -> Result<String, RuntimeError> {
        let (schema, batches) = self.execute_source_query(&view.definition_sql)?;
        let stored_schema = accel.stored_arrow_schema(view)?;
        if stored_schema.fields() != schema.fields() {
            accel.refresh_view(&view.name, &schema, &batches, tokens, None)?;
            return Ok(
                "REFRESH MATERIALIZED VIEW (whole re-pull: the engine's result layout \
                 no longer matches the stored chunks)"
                    .to_string(),
            );
        }
        let outcome =
            accel.refresh_view_merge(&view.name, &schema, &batches, key_column, tokens)?;
        Ok(format!(
            "REFRESH MATERIALIZED VIEW (merge: {} chunks rewritten, {} kept, {} appended)",
            outcome.chunks_rewritten, outcome.chunks_kept, outcome.chunks_appended
        ))
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
    pub(crate) fn accelerator(&self) -> Result<&Accelerator, RuntimeError> {
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
    pub(crate) fn install_views(&self) -> Result<(), RuntimeError> {
        let accel = self.accelerator()?;
        let views = accel.views()?;
        register_data_plane(self.session, accel, &views)?;
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

    /// `SHOW MATERIALIZED VIEWS`: one row per live view - its name, stored row
    /// count and byte size, creation and last-refresh timestamps, and the
    /// substitution benefit counters (`use_count`, `cost_saved`). Read FRESH
    /// from the registry each time, so the counters reflect every substitution
    /// recorded since (including by other runtimes on the same store), not a
    /// planning snapshot.
    pub(crate) fn show_materialized_views(
        &self,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        let views = accel.views()?;
        let schema = views_result_schema();
        let batch = views_result_batch(&schema, &views)?;
        Ok((schema, vec![batch]))
    }
}

/// The (column name, engine type) pairs `SHOW MATERIALIZED VIEWS` reports for
/// `describe`, matching `views_result_schema` positionally.
pub(crate) fn views_describe_columns() -> Vec<(String, DataType)> {
    vec![
        ("name".to_owned(), DataType::Text),
        ("rows".to_owned(), DataType::BigInt),
        ("bytes".to_owned(), DataType::BigInt),
        ("created".to_owned(), DataType::Text),
        ("refreshed".to_owned(), DataType::Text),
        ("use_count".to_owned(), DataType::BigInt),
        ("cost_saved".to_owned(), DataType::Double),
    ]
}

/// The Arrow result schema of `SHOW MATERIALIZED VIEWS`. `refreshed` is nullable
/// (a view never refreshed since creation has no timestamp).
fn views_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("rows", ArrowDataType::Int64, false),
        Field::new("bytes", ArrowDataType::Int64, false),
        Field::new("created", ArrowDataType::Utf8, false),
        Field::new("refreshed", ArrowDataType::Utf8, true),
        Field::new("use_count", ArrowDataType::Int64, false),
        Field::new("cost_saved", ArrowDataType::Float64, false),
    ]))
}

/// Assemble the seven `SHOW MATERIALIZED VIEWS` columns over the live views.
fn views_result_batch(
    schema: &SchemaRef,
    views: &[MaterializedView],
) -> Result<RecordBatch, RuntimeError> {
    let mut names = Vec::with_capacity(views.len());
    let mut rows = Vec::with_capacity(views.len());
    let mut bytes = Vec::with_capacity(views.len());
    let mut created = Vec::with_capacity(views.len());
    let mut refreshed = Vec::with_capacity(views.len());
    let mut use_counts = Vec::with_capacity(views.len());
    let mut cost_saved = Vec::with_capacity(views.len());
    for view in views {
        names.push(view.name.clone());
        rows.push(view.measured_rows);
        bytes.push(view.byte_size);
        created.push(view.created_at.clone());
        refreshed.push(view.refreshed_at.clone());
        use_counts.push(view.use_count);
        cost_saved.push(view.cost_saved);
    }
    let columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(StringArray::from(names)),
        Arc::new(Int64Array::from(rows)),
        Arc::new(Int64Array::from(bytes)),
        Arc::new(StringArray::from(created)),
        Arc::new(StringArray::from(refreshed)),
        Arc::new(Int64Array::from(use_counts)),
        Arc::new(Float64Array::from(cost_saved)),
    ];
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))
}

/// The watermark a delta pull filters against: the stored state when it
/// tracks the same column, else the max over the STORED chunks (a change key
/// declared after the view was created). `Err(reason)` means the column
/// cannot carry one and the caller re-pulls whole.
fn resolve_watermark(
    accel: &Accelerator,
    view: &MaterializedView,
    output_column: &str,
) -> Result<Result<Option<fq_accel::Watermark>, String>, RuntimeError> {
    if let Some(state) = &view.change_key {
        if state.column == output_column {
            return Ok(Ok(state.watermark.clone()));
        }
    }
    match accel.chunk_watermark(view, output_column)? {
        WatermarkScan::Value(watermark) => Ok(Ok(watermark)),
        WatermarkScan::Unsupported(reason) => Ok(Err(reason)),
    }
}

/// The change-key state a CREATE stores: present only for an append-admitted
/// view whose created rows can carry a watermark. An unsupported column (the
/// scan names the reason) stores no state - the first REFRESH then re-derives
/// or re-pulls whole, stating why.
fn initial_change_key_state(
    plan: &RefreshPlan,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Option<ChangeKeyState> {
    let RefreshDecision::Append { output_column } = &plan.decision else {
        return None;
    };
    watermark_state(schema, batches, output_column)
}

/// The watermark state over freshly pulled batches, or None when the column
/// cannot carry one (the refresh path then re-pulls whole and says so).
fn watermark_state(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    output_column: &str,
) -> Option<ChangeKeyState> {
    let index = column_index(schema, output_column)?;
    match scan_watermark(batches, index) {
        Ok(WatermarkScan::Value(watermark)) => Some(ChangeKeyState {
            column: output_column.to_string(),
            watermark,
        }),
        Ok(WatermarkScan::Unsupported(_)) | Err(_) => None,
    }
}

/// Advance the pull watermark past the delta rows: max(used, delta max). An
/// empty delta keeps the used watermark. `Err(reason)` means the delta rows
/// cannot carry one (NULL arrived, or a type the watermark cannot hold) - the
/// caller re-pulls whole rather than record a watermark that would skip rows.
fn advance_watermark(
    used: Option<fq_accel::Watermark>,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    output_column: &str,
) -> Result<Result<Option<fq_accel::Watermark>, String>, RuntimeError> {
    let Some(index) = column_index(schema, output_column) else {
        return Err(RuntimeError::MaterializedView(format!(
            "delta result lost the change-key column '{output_column}'"
        )));
    };
    match scan_watermark(batches, index)? {
        WatermarkScan::Unsupported(reason) => Ok(Err(reason)),
        WatermarkScan::Value(None) => Ok(Ok(used)),
        WatermarkScan::Value(Some(delta_max)) => match used {
            None => Ok(Ok(Some(delta_max))),
            Some(used) => Ok(Ok(Some(used.max_with(delta_max)?))),
        },
    }
}

/// The index of `column` in an arrow schema, matched case-insensitively.
fn column_index(schema: &SchemaRef, column: &str) -> Option<usize> {
    for (index, field) in schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(column) {
            return Some(index);
        }
    }
    None
}

/// Total row count across result batches.
fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// The one-row, one-column `status` table a DDL returns (the pg command-tag
/// convention rendered as a result set).
pub(crate) fn status_result(tag: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
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
