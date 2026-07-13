//! The metadata registry. Ports `catalog/catalog.py`.
//!
//! The `DataSource` trait (the catalog/statistics tier a connector exposes) lives
//! in `datasource.rs`; the concrete connectors implement it in fq-connectors
//! (dependency inversion, so fq-catalog needs no driver deps). `load_metadata`
//! introspects every registered source into the schema tree.

use std::collections::BTreeMap;
use std::sync::Arc;

use fq_common::DataType;

use crate::datasource::{DataSource, RenderDialect};
use crate::error::CatalogError;
use crate::schema::{Column, Schema, Table};

/// Central catalog managing metadata from all data sources.
///
/// Clone is cheap-ish (datasource handles are shared `Arc`s; the schema tree
/// is owned metadata): the runtime clones the catalog to apply a materialized-
/// view DDL as a copy-and-swap, so in-flight plans keep a consistent snapshot.
#[derive(Default, Clone)]
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

    /// Resolve a (partial) table reference to its `Table`. `datasource` and
    /// `schema` narrow the search when given (case-insensitive); a bare table name
    /// searches every registered schema. Returns None when no schema holds a
    /// matching table. The binder uses this so a bare (unqualified) table
    /// reference still resolves.
    pub fn resolve_table(
        &self,
        datasource: Option<&str>,
        schema: Option<&str>,
        table: &str,
    ) -> Option<&Table> {
        for ((ds_name, schema_name), schema_obj) in &self.schemas {
            let ds_ok = datasource.is_none_or(|wanted| wanted.eq_ignore_ascii_case(ds_name));
            let schema_ok = schema.is_none_or(|wanted| wanted.eq_ignore_ascii_case(schema_name));
            if ds_ok && schema_ok {
                if let Some(found) = schema_obj.get_table(table) {
                    return Some(found);
                }
            }
        }
        None
    }

    /// Resolve a (partial) table reference to its column names (star expansion).
    pub fn resolve_table_columns(
        &self,
        datasource: Option<&str>,
        schema: Option<&str>,
        table: &str,
    ) -> Option<Vec<String>> {
        self.resolve_table(datasource, schema, table)
            .map(|found| found.columns.iter().map(|col| col.name.clone()).collect())
    }

    /// Number of registered data sources.
    pub fn datasource_count(&self) -> usize {
        self.datasources.len()
    }

    /// The names of every registered data source, sorted for determinism.
    pub fn datasource_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.datasources.keys().cloned().collect();
        names.sort();
        names
    }

    /// The names of the registered REMOTE data sources, sorted - every source
    /// except the internal materialized-view store. Single-source pushdown
    /// resolves a pure-computation (constant/recursive) CTE body to the sole
    /// source when exactly one exists; that target must be a real remote source
    /// the engine can run the body against. The read-only materialized store is
    /// never such a target, so it is excluded from the count (its presence must
    /// not turn a genuine one-source query into a local fallback).
    pub fn remote_datasource_names(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for (name, source) in &self.datasources {
            if source.render_dialect() != RenderDialect::Materialized {
                names.push(name.clone());
            }
        }
        names.sort();
        names
    }

    /// Number of registered schemas.
    pub fn schema_count(&self) -> usize {
        self.schemas.len()
    }

    /// Whether `load_metadata` has run.
    pub fn metadata_loaded(&self) -> bool {
        self.metadata_loaded
    }

    /// Introspect every registered source into the schema tree: discover schemas,
    /// tables, and columns, mapping each column's native type through the source
    /// (so the catalog and execution path agree on what a column is) and rejecting
    /// a non-renderable type loudly. Ports `catalog.py::load_metadata`.
    pub fn load_metadata(&mut self) -> Result<(), CatalogError> {
        // Collect first (immutable borrow of the sources), then insert, so we do
        // not borrow self mutably and immutably at once.
        let mut loaded: Vec<(String, String, Schema)> = Vec::new();
        for (ds_name, source) in &self.datasources {
            for schema_name in source.list_schemas()? {
                let schema = load_one_schema(source.as_ref(), ds_name, &schema_name)?;
                loaded.push((ds_name.clone(), schema_name, schema));
            }
        }
        for (ds_name, schema_name, schema) in loaded {
            self.schemas.insert((ds_name, schema_name), schema);
        }
        self.metadata_loaded = true;
        Ok(())
    }
}

/// Build one schema's `Schema` from a source: every base table, every column
/// mapped to a renderable engine type.
fn load_one_schema(
    source: &dyn DataSource,
    ds_name: &str,
    schema_name: &str,
) -> Result<Schema, CatalogError> {
    let mut schema = Schema::new(schema_name, ds_name);
    for table_name in source.list_tables(schema_name)? {
        let metadata = source.get_table_metadata(schema_name, &table_name)?;
        let mut columns = Vec::with_capacity(metadata.columns.len());
        for column_meta in &metadata.columns {
            let data_type = source.map_native_type(&column_meta.data_type)?;
            require_renderable(&column_meta.name, data_type)?;
            columns.push(Column::new(
                column_meta.name.clone(),
                data_type,
                column_meta.nullable,
            ));
        }
        schema.add_table(Table::new(table_name, columns));
    }
    Ok(schema)
}

/// Raise if a column's mapped `DataType` has no Arrow rendering (a connector
/// bug), with the offending column, rather than crashing mid-query.
fn require_renderable(column_name: &str, data_type: DataType) -> Result<(), CatalogError> {
    if data_type.is_renderable() {
        return Ok(());
    }
    Err(CatalogError::NonRenderableColumn {
        column: column_name.to_string(),
        data_type: data_type.value().to_string(),
    })
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
