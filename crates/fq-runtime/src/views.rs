//! Non-materialized view DDL: CREATE / DROP / SHOW VIEW.
//!
//! A view stores only its defining SELECT. Nothing is precomputed, so a view is
//! always current - the parser expands a reference into the stored query at plan
//! time (see `fq_parse::convert`), and every later pass sees a derived table.
//! That is the whole difference from a materialized view, which serves a stored
//! result and needs refreshing.
//!
//! The definition is parsed AND bound at CREATE, so a broken view raises then
//! rather than at some later reference.

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use fq_accel::StoredView;
use fq_catalog::ViewDef;
use fq_common::DataType;

use crate::error::RuntimeError;
use crate::materialized::status_result;
use crate::Runtime;

impl Runtime {
    /// `CREATE [OR REPLACE] VIEW <name> [(cols)] AS <select>`: validate the
    /// definition, persist it, and install it in the live catalog.
    pub(crate) fn create_view(
        &self,
        name: &str,
        column_aliases: Option<Vec<String>>,
        select_sql: &str,
        or_replace: bool,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        let catalog = self.catalog_snapshot();
        let exists = catalog.get_view(name).is_some();
        if exists && !or_replace {
            return Err(RuntimeError::EventStatement(format!(
                "view '{name}' already exists; use CREATE OR REPLACE VIEW to redefine it"
            )));
        }
        if !exists {
            if let Some(holder) = self.relation_name_holder(name)? {
                return Err(RuntimeError::EventStatement(format!(
                    "relation '{name}' already exists ({holder}); a view cannot shadow it"
                )));
            }
        }
        self.validate_view_definition(name, column_aliases.as_deref(), select_sql)?;
        // `created_at` is stamped by the catalog's clock on write; the value
        // passed here is not read.
        accel.upsert_view(&StoredView {
            name: name.to_string(),
            definition_sql: select_sql.to_string(),
            column_aliases: column_aliases.clone(),
            created_at: String::new(),
        })?;
        self.install_view(
            name,
            ViewDef {
                sql: select_sql.to_string(),
                column_aliases,
            },
        );
        status_result(if or_replace {
            "CREATE OR REPLACE VIEW"
        } else {
            "CREATE VIEW"
        })
    }

    /// Prove the definition parses and binds against the CURRENT catalog, with
    /// the view itself registered so a self-reference is caught here as a cycle
    /// rather than at first use. A failure raises and nothing is persisted.
    fn validate_view_definition(
        &self,
        name: &str,
        column_aliases: Option<&[String]>,
        select_sql: &str,
    ) -> Result<(), RuntimeError> {
        let mut probe = (*self.catalog_snapshot()).clone();
        probe.register_view(
            name,
            ViewDef {
                sql: select_sql.to_string(),
                column_aliases: column_aliases.map(<[String]>::to_vec),
            },
        );
        let plan = fq_parse::parse_with_catalog(select_sql, &probe)?;
        let bound = fq_bind::bind(&probe, plan)?;
        if let Some(renames) = column_aliases {
            let produced = bound.schema().len();
            if renames.len() != produced {
                return Err(RuntimeError::EventStatement(format!(
                    "view '{name}' declares {} column names but its query produces {produced}",
                    renames.len()
                )));
            }
        }
        Ok(())
    }

    /// `DROP VIEW <name>`: a missing name raises rather than silently
    /// succeeding.
    pub(crate) fn drop_view(&self, name: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        if !accel.delete_view(name)? {
            return Err(RuntimeError::EventStatement(format!(
                "view '{name}' does not exist"
            )));
        }
        let mut next = (*self.catalog_snapshot()).clone();
        next.remove_view(name);
        self.swap_catalog(next);
        status_result("DROP VIEW")
    }

    /// `SHOW VIEWS`: every registered view with its definition.
    pub(crate) fn show_views(&self) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let views = self.accelerator()?.list_views()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("columns", ArrowDataType::Utf8, true),
            Field::new("definition", ArrowDataType::Utf8, false),
            Field::new("created_at", ArrowDataType::Utf8, false),
        ]));
        let mut names = Vec::new();
        let mut columns = Vec::new();
        let mut definitions = Vec::new();
        let mut created = Vec::new();
        for view in &views {
            names.push(view.name.clone());
            columns.push(view.column_aliases.as_ref().map(|list| list.join(", ")));
            definitions.push(view.definition_sql.clone());
            created.push(view.created_at.clone());
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(columns)),
                Arc::new(StringArray::from(definitions)),
                Arc::new(StringArray::from(created)),
            ],
        )
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
        Ok((schema, vec![batch]))
    }

    /// Register one view in a fresh catalog snapshot and swap it in.
    fn install_view(&self, name: &str, view: ViewDef) {
        let mut next = (*self.catalog_snapshot()).clone();
        next.register_view(name, view);
        self.swap_catalog(next);
    }
}

/// The `SHOW VIEWS` result columns, for describe.
pub(crate) fn views_describe_columns() -> Vec<(String, DataType)> {
    vec![
        ("name".to_owned(), DataType::Varchar),
        ("columns".to_owned(), DataType::Varchar),
        ("definition".to_owned(), DataType::Varchar),
        ("created_at".to_owned(), DataType::Varchar),
    ]
}

/// Register every persisted view into `catalog` at startup, so a restart over
/// the same config still resolves them.
pub(crate) fn install_persisted_views(
    catalog: &mut fq_catalog::Catalog,
    accel: &fq_accel::Accelerator,
) -> Result<(), RuntimeError> {
    for view in accel.list_views()? {
        catalog.register_view(
            view.name,
            ViewDef {
                sql: view.definition_sql,
                column_aliases: view.column_aliases,
            },
        );
    }
    Ok(())
}
