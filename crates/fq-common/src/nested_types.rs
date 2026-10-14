//! Recursive Arrow metadata shared by catalog binding and execution.

use arrow_schema::DataType as ArrowType;

use crate::DataType;

impl DataType {
    /// Render scalar engine types and native recursive metadata as Arrow types.
    pub fn to_arrow(&self) -> ArrowType {
        match self {
            Self::Integer => ArrowType::Int32,
            Self::BigInt => ArrowType::Int64,
            Self::Float => ArrowType::Float32,
            Self::Double | Self::Decimal => ArrowType::Float64,
            Self::Varchar | Self::Text => ArrowType::Utf8,
            Self::Boolean => ArrowType::Boolean,
            Self::Date => ArrowType::Date32,
            Self::Timestamp => ArrowType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            Self::Interval => ArrowType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            Self::Null => ArrowType::Null,
            Self::Arrow(data_type) => data_type.clone(),
        }
    }
    /// Map scalar types and retain complete metadata for native Arrow values.
    pub fn from_arrow(data_type: &ArrowType) -> Self {
        match data_type {
            ArrowType::Int8
            | ArrowType::Int16
            | ArrowType::Int32
            | ArrowType::UInt8
            | ArrowType::UInt16
            | ArrowType::UInt32 => Self::Integer,
            ArrowType::Int64 | ArrowType::UInt64 => Self::BigInt,
            ArrowType::Float16 | ArrowType::Float32 => Self::Float,
            ArrowType::Float64 | ArrowType::Decimal128(_, _) | ArrowType::Decimal256(_, _) => {
                Self::Double
            }
            ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View => Self::Varchar,
            ArrowType::Boolean => Self::Boolean,
            ArrowType::Date32 | ArrowType::Date64 => Self::Date,
            ArrowType::Timestamp(_, _) => Self::Timestamp,
            ArrowType::Null => Self::Null,
            other => Self::Arrow(other.clone()),
        }
    }

    /// Resolve the element type of a list without inspecting any row values.
    pub fn list_element(&self) -> Option<Self> {
        match self {
            Self::Arrow(
                ArrowType::List(field)
                | ArrowType::LargeList(field)
                | ArrowType::FixedSizeList(field, _),
            ) => Some(Self::from_arrow(field.data_type())),
            _ => None,
        }
    }

    /// Resolve a struct field by its SQL name, preserving its stored spelling.
    pub fn struct_field(&self, name: &str) -> Option<(String, Self)> {
        let Self::Arrow(ArrowType::Struct(fields)) = self else {
            return None;
        };
        let field = fields
            .iter()
            .find(|field| field.name().eq_ignore_ascii_case(name))?;
        Some((field.name().clone(), Self::from_arrow(field.data_type())))
    }

    /// Validate native Arrow metadata recursively before admitting it to a plan.
    pub fn validate_arrow(data_type: &ArrowType) -> Result<(), String> {
        match data_type {
            ArrowType::List(field)
            | ArrowType::LargeList(field)
            | ArrowType::FixedSizeList(field, _)
            | ArrowType::Map(field, _) => Self::validate_arrow(field.data_type()),
            ArrowType::Struct(fields) => validate_fields(fields),
            ArrowType::Union(_, _)
            | ArrowType::RunEndEncoded(_, _)
            | ArrowType::Dictionary(_, _)
            | ArrowType::ListView(_)
            | ArrowType::LargeListView(_) => Err(format!("unsupported Arrow type {data_type}")),
            _ => Ok(()),
        }
    }
}

/// Reject ambiguous field names and recursively validate every struct member.
fn validate_fields(fields: &arrow_schema::Fields) -> Result<(), String> {
    let mut names = std::collections::HashSet::new();
    for field in fields {
        if !names.insert(field.name().to_lowercase()) {
            return Err(format!("duplicate struct field {}", field.name()));
        }
        DataType::validate_arrow(field.data_type())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use std::sync::Arc;

    /// Exercise precision, field spelling, and nullability across persistent metadata.
    #[test]
    fn recursive_metadata_roundtrip_preserves_fields() {
        let amount = Field::new("Amount", ArrowType::Decimal128(18, 4), false);
        let country = Field::new("Country", ArrowType::Utf8, true);
        let fields = ArrowType::Struct(vec![amount, country].into());
        let native = ArrowType::LargeList(Arc::new(Field::new("element", fields, false)));
        let engine = DataType::from_arrow(&native);
        let encoded = engine.value();
        let restored: DataType = serde_json::from_str(&encoded).expect("recursive metadata");
        assert_eq!(restored.to_arrow(), native);
        assert_eq!(
            restored
                .list_element()
                .unwrap()
                .struct_field("COUNTRY")
                .unwrap()
                .0,
            "Country"
        );
    }

    /// Ambiguous nested field names must fail before a query can resolve either spelling.
    #[test]
    fn duplicate_nested_names_raise_case_insensitively() {
        let fields = ArrowType::Struct(
            vec![
                Field::new("Key", ArrowType::Utf8, true),
                Field::new("key", ArrowType::Int64, false),
            ]
            .into(),
        );
        let nested = ArrowType::List(Arc::new(Field::new("item", fields, true)));
        assert!(DataType::validate_arrow(&nested).is_err());
    }
}
