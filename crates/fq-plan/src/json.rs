//! Derive JSON extraction result shapes from bound argument types and literal paths.

use crate::{Expr, LiteralValue};
use arrow_schema::{DataType as ArrowType, Field};
use fq_common::{json_path::JsonPath, DataType};
use std::sync::Arc;

/// Lists of paths and constant wildcard paths return nullable VARCHAR lists.
pub(crate) fn result_type(arguments: &[Expr]) -> DataType {
    let Some(path) = arguments.get(1) else {
        return DataType::Varchar;
    };
    let multiple = path.get_type().list_element().is_some() || literal_wildcard(path);
    if multiple {
        return DataType::Arrow(ArrowType::List(Arc::new(Field::new(
            "item",
            ArrowType::Utf8,
            true,
        ))));
    }
    DataType::Varchar
}

/// Detect wildcard shape without interpreting a dynamic path or reading any rows.
fn literal_wildcard(expression: &Expr) -> bool {
    let Expr::Literal {
        value: LiteralValue::String(path),
        ..
    } = expression
    else {
        return false;
    };
    JsonPath::parse(path).is_ok_and(|path| path.multiple())
}
