//! Validate JSON extraction signatures and constant paths before execution.

use crate::error::BindError;
use arrow_schema::DataType as ArrowType;
use fq_common::json_path::JsonPath;
use fq_plan::{Expr, LiteralValue};

/// Bind only the documented VARCHAR, BIGINT-index and VARCHAR-list signatures.
pub(crate) fn validate(name: &str, arguments: &[Expr]) -> Result<(), BindError> {
    if !matches!(
        name.to_uppercase().as_str(),
        "JSON_EXTRACT_STRING" | "JSON_EXTRACT_PATH_TEXT"
    ) {
        return Ok(());
    }
    if arguments.len() != 2 {
        return Err(BindError::Unsupported(
            "json_extract_string requires two arguments".into(),
        ));
    }
    require_document(&arguments[0])?;
    validate_path(&arguments[1])
}

/// Reject non-string documents rather than relying on source-dependent implicit casts.
fn require_document(argument: &Expr) -> Result<(), BindError> {
    match argument.get_type().to_arrow() {
        ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View | ArrowType::Null => Ok(()),
        other => Err(BindError::Unsupported(format!(
            "JSON document must be VARCHAR, got {other}"
        ))),
    }
}

/// Validate scalar paths eagerly and require explicit path lists to be constants.
fn validate_path(path: &Expr) -> Result<(), BindError> {
    match path {
        Expr::Literal {
            value: LiteralValue::String(value),
            ..
        } => validate_literal(value, false),
        Expr::FunctionCall {
            function_name,
            args,
            ..
        } if function_name.eq_ignore_ascii_case("MAKE_ARRAY") => validate_list(args),
        _ => require_scalar_path(path),
    }
}

/// Preserve integer indexing and dynamic text paths, rejecting every other scalar type.
fn require_scalar_path(path: &Expr) -> Result<(), BindError> {
    match path.get_type().to_arrow() {
        ArrowType::Utf8
        | ArrowType::LargeUtf8
        | ArrowType::Utf8View
        | ArrowType::Null
        | ArrowType::Int32
        | ArrowType::Int64 => Ok(()),
        other => Err(BindError::Unsupported(format!(
            "JSON path must be VARCHAR, BIGINT, or a constant VARCHAR list, got {other}"
        ))),
    }
}

/// Validate every path so invalid queries raise even when the input relation is empty.
fn validate_list(elements: &[Expr]) -> Result<(), BindError> {
    for element in elements {
        match element {
            Expr::Literal {
                value: LiteralValue::String(path),
                ..
            } => validate_literal(path, true)?,
            _ => {
                return Err(BindError::Unsupported(
                    "JSON path lists must contain non-NULL constant VARCHAR paths".into(),
                ))
            }
        }
    }
    Ok(())
}

/// Keep wildcard paths out of explicit path lists whose elements must be scalar strings.
fn validate_literal(path: &str, list: bool) -> Result<(), BindError> {
    let parsed = JsonPath::parse(path).map_err(BindError::Unsupported)?;
    if list && parsed.multiple() {
        return Err(BindError::Unsupported(
            "cannot have wildcards when supplying multiple JSON paths".into(),
        ));
    }
    Ok(())
}
