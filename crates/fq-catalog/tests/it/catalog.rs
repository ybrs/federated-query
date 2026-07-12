//! Translation of `tests/test_catalog.py` (metadata-tree + registry half).
//!
//! The three `map_native_type` tests in that file exercise the CONNECTOR, not the
//! catalog, and translate into the fq-connectors corpus instead (see lib.rs port
//! note). `test_register_datasource` used a real DuckDBDataSource; here it uses a
//! stub `DataSource` because connectors are a later crate. The object-identity
//! back-ref assertion (`table.schema == schema`) is replaced by asserting the
//! stamped qualifier, which is that back-ref's load-bearing content.

use std::sync::Arc;

use fq_catalog::{
    Catalog, CatalogError, Column, ColumnMetadata, DataSource, DataSourceCapability, Schema, Table,
    TableMetadata, TableStatistics,
};
use fq_common::DataType;

/// One column of the stub's fixed metadata: (name, native type name, nullable).
type StubColumn = (String, String, bool);
/// One table of the stub's fixed metadata: (schema, table, columns).
type StubTable = (String, String, Vec<StubColumn>);

/// A minimal in-crate `DataSource` for catalog tests (real connectors are a later
/// crate). Serves a fixed metadata tree from memory - enough to drive
/// `load_metadata` and the registry without any driver.
struct StubSource {
    name: String,
    tables: Vec<StubTable>,
}

impl StubSource {
    fn empty(name: &str) -> Self {
        Self {
            name: name.to_string(),
            tables: Vec::new(),
        }
    }
}

impl DataSource for StubSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> Vec<DataSourceCapability> {
        vec![DataSourceCapability::Joins]
    }

    fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
        let mut schemas: Vec<String> = self.tables.iter().map(|(s, _, _)| s.clone()).collect();
        schemas.sort();
        schemas.dedup();
        Ok(schemas)
    }

    fn list_tables(&self, schema: &str) -> Result<Vec<String>, CatalogError> {
        let mut names = Vec::new();
        for (s, table, _) in &self.tables {
            if s == schema {
                names.push(table.clone());
            }
        }
        Ok(names)
    }

    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata, CatalogError> {
        for (s, t, cols) in &self.tables {
            if s == schema && t == table {
                let columns = cols
                    .iter()
                    .map(|(name, ty, nullable)| ColumnMetadata::new(name, ty, *nullable))
                    .collect();
                return Ok(TableMetadata {
                    schema_name: schema.to_string(),
                    table_name: table.to_string(),
                    columns,
                    row_count: None,
                    size_bytes: None,
                });
            }
        }
        Err(CatalogError::Source(format!("no table {schema}.{table}")))
    }

    fn get_table_statistics(
        &self,
        _schema: &str,
        _table: &str,
        _columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        Ok(None)
    }

    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        // Model a source interval column as the non-renderable INTERVAL type, so
        // load_metadata's renderability guard is exercised; everything else uses
        // the default mapping.
        if type_str.eq_ignore_ascii_case("INTERVAL") {
            return Ok(DataType::Interval);
        }
        fq_catalog::map_native_type_default(type_str)
    }
}

/// The `sample_schema` fixture: schema "public" on "test_db" with customers(4)
/// and orders(4).
fn sample_schema() -> Schema {
    let customers = Table::new(
        "customers",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar, false),
            Column::new("email", DataType::Varchar, false),
            Column::new("region", DataType::Varchar, true),
        ],
    );
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("customer_id", DataType::Integer, false),
            Column::new("amount", DataType::Double, false),
            Column::new("status", DataType::Varchar, false),
        ],
    );
    Schema::with_tables("public", "test_db", vec![customers, orders])
}

#[test]
fn test_catalog_initialization() {
    let catalog = Catalog::new();
    assert_eq!(catalog.datasource_count(), 0);
    assert_eq!(catalog.schema_count(), 0);
    assert!(!catalog.metadata_loaded());
}

#[test]
fn test_register_datasource() {
    let mut catalog = Catalog::new();
    let ds: Arc<dyn DataSource> = Arc::new(StubSource::empty("test_duck"));
    catalog.register_datasource(ds.clone());

    let got = catalog.get_datasource("test_duck").expect("registered");
    assert!(Arc::ptr_eq(&got, &ds));
}

#[test]
fn test_load_metadata_populates_tree_and_maps_types() {
    let source = StubSource {
        name: "duck".to_string(),
        tables: vec![(
            "main".to_string(),
            "users".to_string(),
            vec![
                ("id".to_string(), "INTEGER".to_string(), false),
                ("name".to_string(), "VARCHAR".to_string(), true),
            ],
        )],
    };
    let mut catalog = Catalog::new();
    catalog.register_datasource(Arc::new(source));
    assert!(!catalog.metadata_loaded());

    catalog.load_metadata().expect("load");

    assert!(catalog.metadata_loaded());
    let table = catalog.get_table("duck", "main", "users").expect("table");
    assert_eq!(table.columns.len(), 2);
    // The native type names were mapped through the source to engine DataTypes.
    assert_eq!(table.get_column("id").unwrap().data_type, DataType::Integer);
    assert_eq!(
        table.get_column("name").unwrap().data_type,
        DataType::Varchar
    );
}

#[test]
fn test_load_metadata_rejects_nonrenderable_type() {
    // A native type that maps to INTERVAL (non-renderable) must raise loudly at
    // load, naming the column - not crash mid-query.
    let source = StubSource {
        name: "duck".to_string(),
        tables: vec![(
            "main".to_string(),
            "t".to_string(),
            vec![("span".to_string(), "INTERVAL".to_string(), true)],
        )],
    };
    let mut catalog = Catalog::new();
    catalog.register_datasource(Arc::new(source));
    let error = catalog.load_metadata().expect_err("must reject");
    match error {
        CatalogError::NonRenderableColumn { column, .. } => assert_eq!(column, "span"),
        other => panic!("expected NonRenderableColumn, got {other:?}"),
    }
}

#[test]
fn test_get_nonexistent_datasource() {
    let catalog = Catalog::new();
    assert!(catalog.get_datasource("nonexistent").is_none());
}

#[test]
fn test_get_schema() {
    let mut catalog = Catalog::new();
    catalog.insert_schema("test_db", "public", sample_schema());

    let schema = catalog.get_schema("test_db", "public").expect("schema");
    assert_eq!(schema.name, "public");
    assert_eq!(schema.datasource, "test_db");
}

#[test]
fn test_get_nonexistent_schema() {
    let catalog = Catalog::new();
    assert!(catalog.get_schema("nonexistent", "public").is_none());
}

#[test]
fn test_get_table() {
    let mut catalog = Catalog::new();
    catalog.insert_schema("test_db", "public", sample_schema());

    let table = catalog
        .get_table("test_db", "public", "customers")
        .expect("table");
    assert_eq!(table.name, "customers");
    assert_eq!(table.columns.len(), 4);
}

#[test]
fn test_get_nonexistent_table() {
    let mut catalog = Catalog::new();
    catalog.insert_schema("test_db", "public", sample_schema());

    assert!(catalog
        .get_table("test_db", "public", "nonexistent")
        .is_none());
    assert!(catalog
        .get_table("nonexistent", "public", "customers")
        .is_none());
}

#[test]
fn test_catalog_repr() {
    let repr = Catalog::new().to_string();
    assert!(repr.contains("Catalog"));
    assert!(repr.contains("datasources=0"));
    assert!(repr.contains("schemas=0"));
}

#[test]
fn test_schema_repr() {
    let repr = sample_schema().to_string();
    assert!(repr.contains("Schema"));
    assert!(repr.contains("test_db.public"));
    assert!(repr.contains("tables=2"));
}

#[test]
fn test_table_repr() {
    let schema = sample_schema();
    let table = schema.get_table("customers").expect("table");
    let repr = table.to_string();
    assert!(repr.contains("Table"));
    assert!(repr.contains("customers"));
    assert!(repr.contains("cols=4"));
}

#[test]
fn test_column_repr() {
    let column = Column::new("id", DataType::Integer, false);
    let repr = column.to_string();
    assert!(repr.contains("Column"));
    assert!(repr.contains("id"));
    assert!(repr.contains("INTEGER"));
}

#[test]
fn test_fully_qualified_names() {
    let mut catalog = Catalog::new();
    catalog.insert_schema("test_db", "public", sample_schema());

    let table = catalog
        .get_table("test_db", "public", "customers")
        .expect("table");
    assert_eq!(table.fully_qualified_name(), "test_db.public.customers");

    let column = table.get_column("id").expect("column");
    assert_eq!(column.fully_qualified_name(), "test_db.public.customers.id");
}

#[test]
fn test_schema_table_column() {
    let table = Table::new(
        "users",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar, true),
        ],
    );

    assert_eq!(table.columns.len(), 2);
    assert_eq!(
        table.get_column("id"),
        Some(&Column::new("id", DataType::Integer, false))
    );
    assert_eq!(
        table.get_column("name"),
        Some(&Column::new("name", DataType::Varchar, true))
    );

    let mut schema = Schema::new("public", "postgres");
    schema.add_table(table);

    let stored = schema.get_table("users").expect("table");
    // The owning schema is denormalized onto the table as a stamped qualifier;
    // assert that qualifier.
    let qualifier = stored.qualifier().expect("stamped");
    assert_eq!(qualifier.datasource, "postgres");
    assert_eq!(qualifier.schema_name, "public");
}

#[test]
fn test_column_case_insensitive_lookup() {
    let schema = sample_schema();
    let table = schema.get_table("customers").expect("table");

    assert!(table.get_column("id").is_some());
    assert!(table.get_column("ID").is_some());
    assert!(table.get_column("Id").is_some());
    assert!(table.get_column("iD").is_some());
}

#[test]
fn test_table_case_insensitive_lookup() {
    let mut schema = Schema::new("public", "test_db");
    schema.add_table(Table::new(
        "MyTable",
        vec![Column::new("id", DataType::Integer, false)],
    ));

    assert!(schema.get_table("mytable").is_some());
    assert!(schema.get_table("MYTABLE").is_some());
    assert!(schema.get_table("MyTable").is_some());
}
