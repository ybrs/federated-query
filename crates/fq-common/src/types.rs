//! Shared primitive types.
//!
//! `DataType` is the engine's SQL type enum. It is used by the catalog,
//! connectors, plan, and cost model alike, so it lives in the leaf crate to keep
//! fq-catalog from depending on fq-plan. Ports the `DataType` enum from
//! `plan/expressions.py`.
//!
//! PORT NOTE: only the enum is shared here. The `DataType` -> Arrow rendering
//! (`arrow_type_for` / `is_renderable` from `plan/arrow_types.py`) stays in
//! fq-plan per the plan's crate assignment.

use serde::{Deserialize, Serialize};

/// SQL data types the engine models. The serialized form and `value()` are the
/// uppercase name (e.g. `BigInt` -> "BIGINT"), matching Python's `DataType.value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataType {
    Integer,
    BigInt,
    Float,
    Double,
    Decimal,
    Varchar,
    Text,
    Boolean,
    Date,
    Timestamp,
    Interval,
    Null,
}

impl DataType {
    /// The canonical uppercase type name (the Python `DataType.value`), used in
    /// EXPLAIN text, node repr, and anywhere the type name is rendered.
    pub fn value(self) -> &'static str {
        match self {
            DataType::Integer => "INTEGER",
            DataType::BigInt => "BIGINT",
            DataType::Float => "FLOAT",
            DataType::Double => "DOUBLE",
            DataType::Decimal => "DECIMAL",
            DataType::Varchar => "VARCHAR",
            DataType::Text => "TEXT",
            DataType::Boolean => "BOOLEAN",
            DataType::Date => "DATE",
            DataType::Timestamp => "TIMESTAMP",
            DataType::Interval => "INTERVAL",
            DataType::Null => "NULL",
        }
    }

    /// Whether this type has an Arrow rendering - the connector type contract
    /// (`arrow_types.py::is_renderable`). Exactly the types a connector's
    /// `map_native_type` may produce; the ones that never reach a column
    /// (INTERVAL, NULL) are not renderable. The catalog rejects a column whose
    /// mapped type is not renderable, so a mistyped column fails loudly at load
    /// rather than crashing mid-query.
    ///
    /// The DataType -> concrete Arrow type object (`arrow_type_for`) lives in
    /// fq-exec, where the `arrow` crate is a dependency; only `is_renderable`
    /// (a pure predicate) is needed this early, so only it lives here.
    pub fn is_renderable(self) -> bool {
        match self {
            DataType::Integer
            | DataType::BigInt
            | DataType::Float
            | DataType::Double
            | DataType::Decimal
            | DataType::Varchar
            | DataType::Text
            | DataType::Boolean
            | DataType::Date
            | DataType::Timestamp => true,
            DataType::Interval | DataType::Null => false,
        }
    }
}

impl std::fmt::Display for DataType {
    // Render as the canonical uppercase name.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.value())
    }
}

#[cfg(test)]
mod tests {
    use super::DataType;

    #[test]
    fn value_matches_python_enum_values() {
        assert_eq!(DataType::Integer.value(), "INTEGER");
        assert_eq!(DataType::BigInt.value(), "BIGINT");
        assert_eq!(DataType::Varchar.value(), "VARCHAR");
        assert_eq!(DataType::Timestamp.value(), "TIMESTAMP");
        assert_eq!(DataType::Null.value(), "NULL");
    }

    #[test]
    fn renderable_types_match_the_contract() {
        // The ten types a connector may produce render; the two that never reach
        // a column do not.
        for renderable in [
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
            assert!(renderable.is_renderable(), "{renderable} should render");
        }
        assert!(!DataType::Interval.is_renderable());
        assert!(!DataType::Null.is_renderable());
    }

    #[test]
    fn serde_roundtrips_through_uppercase_name() {
        // The serialized token equals value(); the stats catalog and IR store
        // type names, so this must stay stable.
        let text = serde_yaml::to_string(&DataType::BigInt).unwrap();
        assert_eq!(text.trim(), "BIGINT");
        let back: DataType = serde_yaml::from_str("BIGINT").unwrap();
        assert_eq!(back, DataType::BigInt);
    }
}
