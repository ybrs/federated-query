//! The materialized-view record and its schema mapping.

use std::collections::BTreeMap;

use arrow::datatypes::{DataType as ArrowType, SchemaRef};
use fq_common::DataType;
use serde::{Deserialize, Serialize};

use crate::error::AccelError;
use crate::watermark::Watermark;

/// One output column of a materialized view: the engine type the binder and
/// planner see. The chunks hold the exact executed Arrow data; this is the
/// engine's static declaration of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// One registered materialized view: the catalog row, deserialized.
#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedView {
    pub name: String,
    /// The defining SELECT, stored verbatim; REFRESH re-executes it.
    pub definition_sql: String,
    /// The view's chunk directory name under the store root.
    pub location: String,
    /// The ordered chunk file names inside `location`. THE source of truth for
    /// which files compose the view; readers never trust a directory listing.
    pub chunk_list: Vec<String>,
    pub columns: Vec<ViewColumn>,
    pub measured_rows: i64,
    pub byte_size: i64,
    pub created_at: String,
    /// The last successful REFRESH, or None if never refreshed since creation.
    pub refreshed_at: Option<String>,
    /// Per-base-table version tokens captured just BEFORE the last pull, keyed
    /// `datasource.schema.table`. REFRESH compares them against fresh tokens
    /// and skips the pull when every one matches. Capturing before the pull
    /// means a token can only UNDERSTATE freshness (an extra re-pull), never
    /// overstate it (a wrong skip). Empty when no base table offered a token.
    pub source_tokens: BTreeMap<String, String>,
    /// The delta-append state of the last pull, present only when the view was
    /// last pulled under a monotonic change-key declaration. Merge and whole
    /// re-pulls carry no pull-to-pull state and store None.
    pub change_key: Option<ChangeKeyState>,
    /// How many times automatic substitution has read this view in place of
    /// recomputing a matching query subtree. Advanced by `record_substitution`
    /// after a substituted query executes; persisted, so it accrues across
    /// runtimes over one store.
    pub use_count: i64,
    /// The accumulated cost-model saving of those substitutions, in the cost
    /// model's estimated units (recompute estimate minus cached-read estimate,
    /// summed per reuse). An ESTIMATE, not a measured wall time.
    pub cost_saved: f64,
}

/// The stored delta-append state: which view OUTPUT column carries the
/// monotonic change key, and the high-water mark of the rows pulled so far.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeKeyState {
    /// The view's output column (post-alias) the watermark tracks.
    pub column: String,
    /// The largest value pulled; None while the view holds no rows (the next
    /// delta then pulls everything, which onto an empty view is exact).
    pub watermark: Option<Watermark>,
}

/// Map an executed Arrow result schema onto view columns, validating that the
/// relation is well-formed: no empty and no duplicate column names (a relation
/// with two `a` columns cannot be referenced), and every Arrow type must map to
/// an engine type - an unmapped type raises rather than storing a column the
/// binder would mistype.
pub fn columns_from_arrow(schema: &SchemaRef) -> Result<Vec<ViewColumn>, AccelError> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if field.name().is_empty() {
            return Err(AccelError::InvalidSchema(
                "output column has an empty name; alias every output column".to_string(),
            ));
        }
        if columns.iter().any(|c: &ViewColumn| c.name == *field.name()) {
            return Err(AccelError::InvalidSchema(format!(
                "output column '{}' appears more than once; alias each output \
                 column uniquely",
                field.name()
            )));
        }
        columns.push(ViewColumn {
            name: field.name().clone(),
            data_type: engine_type(field.data_type())?,
            nullable: field.is_nullable(),
        });
    }
    if columns.is_empty() {
        return Err(AccelError::InvalidSchema(
            "the defining SELECT produces no columns".to_string(),
        ));
    }
    Ok(columns)
}

/// The engine type declared for one executed Arrow column type. Exhaustive
/// over what the engine's execution plane produces; any other Arrow type
/// raises, because a column the binder cannot type honestly must not be stored.
fn engine_type(arrow: &ArrowType) -> Result<DataType, AccelError> {
    match arrow {
        ArrowType::Int8
        | ArrowType::Int16
        | ArrowType::Int32
        | ArrowType::UInt8
        | ArrowType::UInt16 => Ok(DataType::Integer),
        ArrowType::Int64 | ArrowType::UInt32 => Ok(DataType::BigInt),
        ArrowType::Float16 | ArrowType::Float32 => Ok(DataType::Float),
        ArrowType::Float64 => Ok(DataType::Double),
        ArrowType::Decimal128(_, _) | ArrowType::Decimal256(_, _) => Ok(DataType::Decimal),
        ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View => Ok(DataType::Text),
        ArrowType::Boolean => Ok(DataType::Boolean),
        ArrowType::Date32 | ArrowType::Date64 => Ok(DataType::Date),
        ArrowType::Timestamp(_, _) => Ok(DataType::Timestamp),
        other => Err(AccelError::InvalidSchema(format!(
            "output column type {other} has no engine type mapping"
        ))),
    }
}

/// The chunk-directory name for a view: the name sanitized to a filesystem-safe
/// alphabet plus a hash of the exact name, so distinct names (which may differ
/// only in characters the sanitizer folds) never share a directory.
pub fn location_for(name: &str) -> String {
    let mut safe = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            safe.push(ch.to_ascii_lowercase());
        } else {
            safe.push('_');
        }
    }
    format!("{safe}-{:016x}", fnv1a(name.as_bytes()))
}

/// 64-bit FNV-1a over the view name; a stable, dependency-free disambiguator
/// for the directory name (uniqueness of the VIEW is enforced by the catalog's
/// primary key, not by this hash).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// An Arrow schema over the given (name, type) pairs, all nullable.
    fn schema(fields: Vec<(&str, ArrowType)>) -> SchemaRef {
        let mut out = Vec::new();
        for (name, data_type) in fields {
            out.push(Field::new(name, data_type, true));
        }
        Arc::new(Schema::new(out))
    }

    #[test]
    fn maps_the_executed_arrow_types() {
        let columns = columns_from_arrow(&schema(vec![
            ("i", ArrowType::Int32),
            ("b", ArrowType::Int64),
            ("d", ArrowType::Float64),
            ("n", ArrowType::Decimal128(18, 2)),
            ("s", ArrowType::Utf8),
            ("dt", ArrowType::Date32),
        ]))
        .expect("all types map");
        let types: Vec<DataType> = columns.iter().map(|c| c.data_type).collect();
        assert_eq!(
            types,
            vec![
                DataType::Integer,
                DataType::BigInt,
                DataType::Double,
                DataType::Decimal,
                DataType::Text,
                DataType::Date,
            ]
        );
    }

    #[test]
    fn duplicate_column_names_raise() {
        let error = columns_from_arrow(&schema(vec![
            ("a", ArrowType::Int32),
            ("a", ArrowType::Int32),
        ]))
        .unwrap_err();
        assert!(matches!(error, AccelError::InvalidSchema(ref m) if m.contains("'a'")));
    }

    #[test]
    fn unmapped_arrow_type_raises() {
        let error = columns_from_arrow(&schema(vec![("x", ArrowType::Binary)])).unwrap_err();
        assert!(matches!(error, AccelError::InvalidSchema(_)));
    }

    #[test]
    fn locations_of_distinct_names_differ_even_when_sanitized_alike() {
        // "a b" and "a_b" both sanitize to "a_b"; the name hash keeps their
        // chunk directories distinct.
        assert_ne!(location_for("a b"), location_for("a_b"));
        assert!(location_for("Sales!").starts_with("sales_-"));
    }
}
