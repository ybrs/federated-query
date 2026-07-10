//! Metadata tree: `Column`, `Table`, `Schema`. Ports `catalog/schema.py`.
//!
//! PORT NOTE - back-references. The Python types carried private parent
//! back-references (`Column._table`, `Table._schema`) used only for
//! `fully_qualified_name` and the `.table` / `.schema` navigational properties.
//! A grep of the engine shows NEITHER the properties NOR `fully_qualified_name`
//! are read anywhere outside the catalog module and its tests - they are not
//! load-bearing. So the Rust tree is a clean OWNED model (Schema owns Tables own
//! Columns) with no `Arc`/`Weak` parent cycles. The load-bearing content of the
//! back-ref (the datasource/schema/table qualifier, needed for FQN) is
//! denormalized onto each child when its table is added to a schema. The
//! object-identity assertions (`table.schema == schema`) retire; the tests
//! assert the qualifier instead.

use fq_common::DataType;

/// The (datasource, schema) a table belongs to, stamped when it is added to a
/// schema. `None` for a free-standing table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableQualifier {
    pub datasource: String,
    pub schema_name: String,
}

/// The (datasource, schema, table) a column belongs to, stamped when its table
/// is added to a schema. `None` for a free-standing column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnQualifier {
    pub datasource: String,
    pub schema_name: String,
    pub table_name: String,
}

/// Column metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    qualifier: Option<ColumnQualifier>,
}

impl Column {
    /// Construct a free-standing column (no owning table yet).
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            qualifier: None,
        }
    }

    /// The owning (datasource, schema, table), once this column's table has been
    /// added to a schema.
    pub fn qualifier(&self) -> Option<&ColumnQualifier> {
        self.qualifier.as_ref()
    }

    /// Fully qualified `datasource.schema.table.column`, or just the column name
    /// while the column has no owning schema yet.
    pub fn fully_qualified_name(&self) -> String {
        match &self.qualifier {
            Some(q) => format!(
                "{}.{}.{}.{}",
                q.datasource, q.schema_name, q.table_name, self.name
            ),
            None => self.name.clone(),
        }
    }
}

impl std::fmt::Display for Column {
    // Matches Python `Column.__repr__`: "Column(<name>, <TYPE>)".
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Column({}, {})", self.name, self.data_type.value())
    }
}

/// Table metadata: a named list of columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    qualifier: Option<TableQualifier>,
}

impl Table {
    /// Construct a free-standing table (not yet in a schema).
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            columns,
            qualifier: None,
        }
    }

    /// Get a column by name, case-insensitively.
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
    }

    /// The owning (datasource, schema), once this table has been added to a schema.
    pub fn qualifier(&self) -> Option<&TableQualifier> {
        self.qualifier.as_ref()
    }

    /// Fully qualified `datasource.schema.table`, or just the table name while it
    /// has no owning schema yet.
    pub fn fully_qualified_name(&self) -> String {
        match &self.qualifier {
            Some(q) => format!("{}.{}.{}", q.datasource, q.schema_name, self.name),
            None => self.name.clone(),
        }
    }

    /// Stamp this table (and its columns) with the owning (datasource, schema).
    /// Called by `Schema::add_table`; carries the load-bearing content of the
    /// retired Python back-references.
    fn stamp(&mut self, datasource: &str, schema_name: &str) {
        self.qualifier = Some(TableQualifier {
            datasource: datasource.to_string(),
            schema_name: schema_name.to_string(),
        });
        for column in &mut self.columns {
            column.qualifier = Some(ColumnQualifier {
                datasource: datasource.to_string(),
                schema_name: schema_name.to_string(),
                table_name: self.name.clone(),
            });
        }
    }
}

impl std::fmt::Display for Table {
    // Matches Python `Table.__repr__`: "Table(<name>, cols=<n>)".
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table({}, cols={})", self.name, self.columns.len())
    }
}

/// Schema metadata: a namespace of tables keyed case-insensitively.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub name: String,
    pub datasource: String,
    // Keyed by lowercase table name so get_table is case-insensitive, matching
    // the Python `_link_tables` normalization.
    tables: std::collections::BTreeMap<String, Table>,
}

impl Schema {
    /// Construct an empty schema.
    pub fn new(name: impl Into<String>, datasource: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            datasource: datasource.into(),
            tables: std::collections::BTreeMap::new(),
        }
    }

    /// Construct a schema from a set of tables, normalizing keys and stamping each
    /// table's qualifier (the `Schema.create` + `_link_tables` path).
    pub fn with_tables(
        name: impl Into<String>,
        datasource: impl Into<String>,
        tables: Vec<Table>,
    ) -> Self {
        let mut schema = Self::new(name, datasource);
        for table in tables {
            schema.add_table(table);
        }
        schema
    }

    /// Add a table, stamping its qualifier and keying it by lowercase name.
    pub fn add_table(&mut self, mut table: Table) {
        table.stamp(&self.datasource, &self.name);
        self.tables.insert(table.name.to_lowercase(), table);
    }

    /// Get a table by name, case-insensitively.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(&name.to_lowercase())
    }

    /// Number of tables in the schema.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

impl std::fmt::Display for Schema {
    // Matches Python `Schema.__repr__`: "Schema(<ds>.<name>, tables=<n>)".
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Schema({}.{}, tables={})",
            self.datasource,
            self.name,
            self.tables.len()
        )
    }
}
