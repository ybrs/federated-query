//! The metadata registry. Ports `catalog/catalog.py`.
//!
//! PORT NOTE - `load_metadata` and `_require_renderable` are DEFERRED to the
//! fq-connectors milestone. They call the full `DataSource` introspection surface
//! (`list_schemas`/`list_tables`/`get_table_metadata`/`map_native_type`) and
//! `plan/arrow_types::is_renderable`, none of which exist yet. No test in
//! test_catalog.py exercises `load_metadata`, so nothing is lost here; when
//! fq-connectors lands, the full `DataSource` trait and this orchestration land
//! with it. Until then the registry (register/get) and the metadata tree are the
//! testable surface.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::schema::{Schema, Table};

/// The datasource interface the catalog depends on.
///
/// Minimal for now (only `name`, which `register_datasource` keys by). The full
/// introspection/statistics/execution surface is added when fq-connectors is
/// built and `Catalog::load_metadata` is ported; the concrete connectors live in
/// fq-connectors and implement this trait (dependency inversion - the abstraction
/// lives with its consumer, so fq-catalog stays a leaf).
pub trait DataSource: Send + Sync {
    /// The registered name of this data source.
    fn name(&self) -> &str;
}

/// Central catalog managing metadata from all data sources.
#[derive(Default)]
pub struct Catalog {
    datasources: BTreeMap<String, Arc<dyn DataSource>>,
    // Keyed by (datasource, schema_name), matching the Python tuple key.
    schemas: BTreeMap<(String, String), Schema>,
    metadata_loaded: bool,
}

impl Catalog {
    /// Construct an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a data source, keyed by its name.
    pub fn register_datasource(&mut self, datasource: Arc<dyn DataSource>) {
        self.datasources
            .insert(datasource.name().to_string(), datasource);
    }

    /// Get a data source by name.
    pub fn get_datasource(&self, name: &str) -> Option<Arc<dyn DataSource>> {
        self.datasources.get(name).cloned()
    }

    /// Insert a schema under (datasource, schema_name). This is the direct
    /// registry write the Python tests do via `catalog.schemas[(...)] = schema`.
    pub fn insert_schema(
        &mut self,
        datasource: impl Into<String>,
        schema_name: impl Into<String>,
        schema: Schema,
    ) {
        self.schemas
            .insert((datasource.into(), schema_name.into()), schema);
    }

    /// Get a schema by data source and name.
    pub fn get_schema(&self, datasource: &str, schema_name: &str) -> Option<&Schema> {
        // BTreeMap keys are owned tuples; look up by borrowing both halves.
        self.schemas
            .get(&(datasource.to_string(), schema_name.to_string()))
    }

    /// Get a table by fully qualified (datasource, schema, table) name.
    pub fn get_table(
        &self,
        datasource: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<&Table> {
        self.get_schema(datasource, schema_name)?
            .get_table(table_name)
    }

    /// Number of registered data sources.
    pub fn datasource_count(&self) -> usize {
        self.datasources.len()
    }

    /// Number of registered schemas.
    pub fn schema_count(&self) -> usize {
        self.schemas.len()
    }

    /// Whether `load_metadata` has run (false until the deferred orchestration
    /// lands with fq-connectors).
    pub fn metadata_loaded(&self) -> bool {
        self.metadata_loaded
    }
}

impl std::fmt::Display for Catalog {
    // Matches Python `Catalog.__repr__`: "Catalog(datasources=<n>, schemas=<n>)".
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Catalog(datasources={}, schemas={})",
            self.datasources.len(),
            self.schemas.len()
        )
    }
}
