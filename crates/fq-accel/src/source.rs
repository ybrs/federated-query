//! The planner-facing view of the store: a `fq_catalog::DataSource` over the
//! registered views, so binding, star expansion, and statistics treat a
//! materialized view like any other relation with zero special cases.

use fq_catalog::datasource::{
    build_table_statistics, ColumnMetadata, DataSource, DataSourceCapability, RenderDialect,
    TableMetadata, TableStatistics,
};
use fq_catalog::{CatalogError, Column, Schema, Table};
use fq_common::DataType;

use crate::view::MaterializedView;

/// The single schema every materialized view lives under. It is `public`
/// because the parser resolves an UNQUALIFIED table reference with a default
/// schema of `public` - which is exactly how users address a view (`SELECT ...
/// FROM my_view`). Pushed SQL reads `public.<view>` and the execution plane
/// registers the chunk tables under the same schema.
pub const VIEW_SCHEMA_NAME: &str = "public";

/// A catalog datasource over a SNAPSHOT of the registered views. The runtime
/// rebuilds and re-registers it on every CREATE / REFRESH / DROP, so the
/// snapshot is always the post-DDL state.
pub struct MaterializedViewSource {
    name: String,
    views: Vec<MaterializedView>,
}

impl MaterializedViewSource {
    /// A source named `name` over the given view snapshot.
    pub fn new(name: impl Into<String>, views: Vec<MaterializedView>) -> Self {
        Self {
            name: name.into(),
            views,
        }
    }

    /// The view named `table`, or a loud `Source` error: the catalog only asks
    /// for tables this source itself listed, so a miss is a stale snapshot bug.
    fn view(&self, table: &str) -> Result<&MaterializedView, CatalogError> {
        self.views
            .iter()
            .find(|view| view.name.eq_ignore_ascii_case(table))
            .ok_or_else(|| {
                CatalogError::Source(format!(
                    "materialized view '{table}' is not in this source's snapshot"
                ))
            })
    }
}

impl DataSource for MaterializedViewSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn render_dialect(&self) -> RenderDialect {
        RenderDialect::Materialized
    }

    // The store executes pushed SQL through DataFusion over local chunks, so
    // relational pushdown is supported; it is read-only (never a ship target)
    // and the coordinator keeps the constructs the skeleton does not push
    // (CTEs, window functions, subqueries).
    fn capabilities(&self) -> Vec<DataSourceCapability> {
        vec![
            DataSourceCapability::Aggregations,
            DataSourceCapability::Joins,
            DataSourceCapability::Distinct,
            DataSourceCapability::Limit,
            DataSourceCapability::OrderBy,
        ]
    }

    fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
        Ok(vec![VIEW_SCHEMA_NAME.to_string()])
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<String>, CatalogError> {
        if schema != VIEW_SCHEMA_NAME {
            return Err(CatalogError::Source(format!(
                "materialized-view store has only the '{VIEW_SCHEMA_NAME}' schema, \
                 not '{schema}'"
            )));
        }
        let mut names = Vec::with_capacity(self.views.len());
        for view in &self.views {
            names.push(view.name.clone());
        }
        Ok(names)
    }

    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata, CatalogError> {
        if schema != VIEW_SCHEMA_NAME {
            return Err(CatalogError::Source(format!(
                "materialized-view store has only the '{VIEW_SCHEMA_NAME}' schema, \
                 not '{schema}'"
            )));
        }
        let view = self.view(table)?;
        let mut columns = Vec::with_capacity(view.columns.len());
        for column in &view.columns {
            columns.push(ColumnMetadata::new(
                column.name.clone(),
                column.data_type.value(),
                column.nullable,
            ));
        }
        Ok(TableMetadata {
            schema_name: schema.to_string(),
            table_name: view.name.clone(),
            columns,
            row_count: Some(view.measured_rows),
            size_bytes: Some(view.byte_size),
        })
    }

    // The registry measured the exact row count when the view was pulled; no
    // per-column statistics are kept (an honest None, never fabricated).
    fn get_table_statistics(
        &self,
        _schema: &str,
        table: &str,
        _columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        let view = self.view(table)?;
        Ok(Some(build_table_statistics(
            Some(view.measured_rows),
            std::collections::BTreeMap::new(),
        )))
    }

    // Metadata round-trip: the store writes `DataType::value()` names, so the
    // mapping back is the exact inverse - never the fuzzy default mapping,
    // which would fold DECIMAL into DOUBLE.
    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        engine_type_from_value(type_str)
    }
}

/// Parse a `DataType::value()` name back to the enum; an unknown name raises
/// (only names this crate itself wrote are valid).
fn engine_type_from_value(value: &str) -> Result<DataType, CatalogError> {
    match value {
        "INTEGER" => Ok(DataType::Integer),
        "BIGINT" => Ok(DataType::BigInt),
        "FLOAT" => Ok(DataType::Float),
        "DOUBLE" => Ok(DataType::Double),
        "DECIMAL" => Ok(DataType::Decimal),
        "VARCHAR" => Ok(DataType::Varchar),
        "TEXT" => Ok(DataType::Text),
        "BOOLEAN" => Ok(DataType::Boolean),
        "DATE" => Ok(DataType::Date),
        "TIMESTAMP" => Ok(DataType::Timestamp),
        other => Err(CatalogError::UnsupportedColumnType(other.to_string())),
    }
}

/// The catalog `Schema` exposing every view as a table under the view schema,
/// for the
/// runtime's in-place catalog swap after a DDL (the full `load_metadata` path
/// would re-probe every OTHER datasource for nothing).
pub fn views_schema(datasource: &str, views: &[MaterializedView]) -> Schema {
    let mut tables = Vec::with_capacity(views.len());
    for view in views {
        let mut columns = Vec::with_capacity(view.columns.len());
        for column in &view.columns {
            columns.push(Column::new(
                column.name.clone(),
                column.data_type,
                column.nullable,
            ));
        }
        tables.push(Table::new(view.name.clone(), columns));
    }
    Schema::with_tables(VIEW_SCHEMA_NAME, datasource, tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::view::ViewColumn;

    /// A one-column view record for source tests.
    fn view(name: &str) -> MaterializedView {
        MaterializedView {
            name: name.to_string(),
            definition_sql: "SELECT 1 AS a".to_string(),
            location: "loc".to_string(),
            chunk_list: vec!["chunk-0-0.arrow".to_string()],
            columns: vec![ViewColumn {
                name: "a".to_string(),
                data_type: DataType::Decimal,
                nullable: true,
            }],
            measured_rows: 42,
            byte_size: 100,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            refreshed_at: None,
            source_tokens: std::collections::BTreeMap::new(),
            change_key: None,
        }
    }

    #[test]
    fn metadata_roundtrips_every_engine_type() {
        let source = MaterializedViewSource::new("accel", vec![view("mv")]);
        for data_type in [
            DataType::Integer,
            DataType::BigInt,
            DataType::Float,
            DataType::Double,
            DataType::Decimal,
            DataType::Varchar,
            DataType::Text,
            DataType::Boolean,
            DataType::Date,
            DataType::Timestamp,
        ] {
            assert_eq!(
                source.map_native_type(data_type.value()).expect("maps"),
                data_type
            );
        }
        assert!(source.map_native_type("MYSTERY").is_err());
    }

    #[test]
    fn statistics_carry_the_measured_row_count() {
        let source = MaterializedViewSource::new("accel", vec![view("mv")]);
        let stats = source
            .get_table_statistics(VIEW_SCHEMA_NAME, "mv", &[])
            .expect("stats")
            .expect("present");
        assert_eq!(stats.row_count, Some(42));
    }

    #[test]
    fn unknown_schema_or_view_raises() {
        let source = MaterializedViewSource::new("accel", vec![view("mv")]);
        assert!(source.list_tables("main").is_err());
        assert!(source
            .get_table_metadata(VIEW_SCHEMA_NAME, "ghost")
            .is_err());
    }

    #[test]
    fn views_schema_exposes_typed_tables() {
        let schema = views_schema("accel", &[view("mv")]);
        let table = schema.get_table("mv").expect("table");
        assert_eq!(table.columns.len(), 1);
        assert_eq!(table.columns[0].data_type, DataType::Decimal);
    }
}
