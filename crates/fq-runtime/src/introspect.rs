//! Catalog introspection statements: `SHOW TABLES` and `DESCRIBE <table>`.
//!
//! Both read the session's catalog snapshot only - the metadata a source loaded
//! at registration - so they never touch a source at execution time.

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};

use fq_catalog::Catalog;
use fq_common::DataType;

use crate::error::RuntimeError;
use crate::Runtime;

/// The ordered result columns of `SHOW TABLES`.
const TABLES_COLUMNS: [&str; 3] = ["datasource", "schema", "table"];

/// The ordered result columns of `DESCRIBE <table>`.
const DESCRIBE_COLUMNS: [&str; 3] = ["column", "type", "nullable"];

impl Runtime {
    /// `SHOW TABLES [FROM <datasource>]`: one (datasource, schema, table) row
    /// per catalog table, narrowed to one datasource when named. An unknown
    /// datasource raises; an unavailable one raises its stored connector error.
    pub(crate) fn show_tables(
        &self,
        datasource: Option<&str>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let catalog = self.catalog_snapshot();
        if let Some(name) = datasource {
            require_known_datasource(&catalog, "SHOW TABLES", name)?;
        }
        let mut names = Vec::new();
        let mut schemas = Vec::new();
        let mut tables = Vec::new();
        for (ds_name, schema_name, table_name) in catalog.list_tables() {
            let wanted = datasource.is_none_or(|want| want.eq_ignore_ascii_case(&ds_name));
            if wanted {
                names.push(ds_name);
                schemas.push(schema_name);
                tables.push(table_name);
            }
        }
        text_result(&TABLES_COLUMNS, vec![names, schemas, tables])
    }

    /// `DESCRIBE <table>`: one (column, type, nullable) row per column of the
    /// resolved table. Missing qualifiers search every registered schema (the
    /// FROM-reference semantics); an unknown reference raises.
    pub(crate) fn describe_table(
        &self,
        datasource: Option<&str>,
        schema: Option<&str>,
        table: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let catalog = self.catalog_snapshot();
        if let Some(name) = datasource {
            require_known_datasource(&catalog, "DESCRIBE", name)?;
        }
        let Some(found) = catalog.resolve_table(datasource, schema, table) else {
            return Err(RuntimeError::Config(format!(
                "DESCRIBE: table '{}' not found in the catalog; SHOW TABLES lists \
                 every table",
                dotted(datasource, schema, table)
            )));
        };
        let mut columns = Vec::new();
        let mut types = Vec::new();
        let mut nullables = Vec::new();
        for column in &found.columns {
            columns.push(column.name.clone());
            types.push(column.data_type.value().to_string());
            nullables.push(nullable_word(column.nullable).to_string());
        }
        text_result(&DESCRIBE_COLUMNS, vec![columns, types, nullables])
    }
}

/// Raise unless `name` is a registered datasource (case-insensitive): an
/// unavailable one raises its stored connector error, any other unknown name
/// raises naming the statement.
fn require_known_datasource(
    catalog: &Catalog,
    statement: &str,
    name: &str,
) -> Result<(), RuntimeError> {
    let known = catalog
        .datasource_names()
        .iter()
        .any(|registered| registered.eq_ignore_ascii_case(name));
    if known {
        return Ok(());
    }
    if let Some(error) = catalog.unavailable_error(name) {
        return Err(RuntimeError::Config(error.to_string()));
    }
    Err(RuntimeError::Config(format!(
        "{statement}: datasource '{name}' does not exist"
    )))
}

/// The dotted reference as the user gave it, for the not-found error.
fn dotted(datasource: Option<&str>, schema: Option<&str>, table: &str) -> String {
    let mut parts = Vec::new();
    if let Some(name) = datasource {
        parts.push(name);
    }
    if let Some(name) = schema {
        parts.push(name);
    }
    parts.push(table);
    parts.join(".")
}

/// The `nullable` cell, in the information_schema convention (`YES` / `NO`).
fn nullable_word(nullable: bool) -> &'static str {
    if nullable {
        "YES"
    } else {
        "NO"
    }
}

/// The (column name, engine type) pairs `describe` reports for SHOW TABLES.
pub(crate) fn tables_describe_columns() -> Vec<(String, DataType)> {
    text_columns(&TABLES_COLUMNS)
}

/// The (column name, engine type) pairs `describe` reports for DESCRIBE.
pub(crate) fn describe_table_columns() -> Vec<(String, DataType)> {
    text_columns(&DESCRIBE_COLUMNS)
}

/// Every named column typed Text (all introspection results are text).
fn text_columns(names: &[&str]) -> Vec<(String, DataType)> {
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        columns.push(((*name).to_string(), DataType::Text));
    }
    columns
}

/// A one-batch, all-text result: fields named `names`, one `StringArray` per
/// entry of `columns`. Shared by every catalog/event introspection statement.
pub(crate) fn text_result(
    names: &[&str],
    columns: Vec<Vec<String>>,
) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let mut fields = Vec::with_capacity(names.len());
    for name in names {
        fields.push(Field::new(*name, ArrowDataType::Utf8, false));
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(columns.len());
    for column in columns {
        arrays.push(Arc::new(StringArray::from(column)));
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
    Ok((schema, vec![batch]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The not-found error names the reference exactly as it was given.
    #[test]
    fn dotted_joins_only_the_given_parts() {
        assert_eq!(dotted(None, None, "t"), "t");
        assert_eq!(dotted(None, Some("s"), "t"), "s.t");
        assert_eq!(dotted(Some("d"), Some("s"), "t"), "d.s.t");
    }

    /// An unknown datasource raises naming the statement; an unavailable one
    /// raises its stored connector error instead.
    #[test]
    fn unknown_and_unavailable_datasources_raise() {
        let mut catalog = Catalog::new();
        catalog.mark_unavailable("dead", "connect refused");
        let unknown = require_known_datasource(&catalog, "SHOW TABLES", "nope").unwrap_err();
        assert!(format!("{unknown}").contains("'nope' does not exist"));
        let dead = require_known_datasource(&catalog, "SHOW TABLES", "dead").unwrap_err();
        assert!(format!("{dead}").contains("connect refused"));
    }
}
