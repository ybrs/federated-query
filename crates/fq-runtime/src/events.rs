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
//! - FUNNEL / SEGMENT read the view's chunks and run the fq-events kernels;
//!   they see exactly the last pull, like any read of the view.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fq_common::events::{EventRoleColumns, FunnelSpec, SegmentSpec};
use fq_common::DataType;
use fq_events::{build_event_view, read_chunks, run_funnel, run_segment, EventStream};
use fq_events::{EventError, EventViewRegistry};

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
        let (schema, batches) = self.execute_query(select_sql)?;
        let sorted = build_event_view(name, &schema, &batches, roles)?;
        accel.create_view(name, select_sql, &schema, &sorted, tokens, None)?;
        if let Err(error) = registry.register(name, roles) {
            // Never leave the half-created pair behind on a registration
            // race: this call owns the view it just created, so it removes it.
            accel.drop_view(name)?;
            return Err(error.into());
        }
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
        let (schema, batches) = self.execute_query(&view.definition_sql)?;
        let sorted = build_event_view(name, &schema, &batches, &roles)?;
        accel.refresh_view(name, &schema, &sorted, &tokens, None)?;
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

    /// `FUNNEL OVER view ...`: run the funnel kernel over the view's chunks.
    pub(crate) fn run_funnel_statement(
        &self,
        spec: &FunnelSpec,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let stream = self.open_event_stream(&spec.view)?;
        Ok(run_funnel(&stream, spec)?)
    }

    /// `SEGMENT OVER view ...`: run the segmentation kernel over the view's
    /// chunks.
    pub(crate) fn run_segment_statement(
        &self,
        spec: &SegmentSpec,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let stream = self.open_event_stream(&spec.view)?;
        Ok(run_segment(&stream, spec)?)
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
        let accel = self.accelerator()?;
        let roles = self.event_roles(view_name)?;
        let view = accel.view(view_name)?;
        let (schema, batches) = read_chunks(&accel.chunk_paths(&view))?;
        Ok(EventStream::open(view_name, &schema, &batches, &roles)?)
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
