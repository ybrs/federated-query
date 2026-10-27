//! Native JSON string extraction shared by Parquet SQL and coordinator expressions.

mod document;
mod selection;

use arrow::array::{Array, ArrayRef, ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use document::JsonDocument;
use fq_common::json_path::JsonPath;
use selection::{PathArgument, Selection};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonExtractString {
    signature: Signature,
}

/// Construct the same immutable scalar function for every native execution context.
pub(crate) fn udf() -> ScalarUDF {
    let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let signatures = [DataType::Utf8, DataType::Int64, list]
        .into_iter()
        .map(|path| TypeSignature::Exact(vec![DataType::Utf8, path]))
        .collect();
    ScalarUDF::from(JsonExtractString {
        signature: Signature::one_of(signatures, Volatility::Immutable),
    })
    .with_aliases(["json_extract_path_text"])
}

impl ScalarUDFImpl for JsonExtractString {
    /// Use DuckDB's function name in both emitted SQL and coordinator expressions.
    fn name(&self) -> &'static str {
        "json_extract_string"
    }

    /// Advertise the text, integer-index and path-list signatures.
    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// A scalar path yields text unless a known wildcard changes the result to a list.
    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        match arguments.get(1) {
            Some(DataType::List(_)) => Ok(list_type()),
            Some(_) => Ok(DataType::Utf8),
            None => Err(DataFusionError::Plan(
                "json_extract_string requires two arguments".into(),
            )),
        }
    }

    /// Resolve wildcard output shapes from constants without reading any source data.
    fn return_field_from_args(&self, arguments: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let types: Vec<_> = arguments
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let mut output = self.return_type(&types)?;
        if let Some(Some(value)) = arguments.scalar_arguments.get(1) {
            if selection::scalar(value)?.is_some_and(|selection| selection.multiple()) {
                output = list_type();
            }
        }
        Ok(Arc::new(Field::new(self.name(), output, true)))
    }

    /// Evaluate one batch while preserving scalar broadcasting and native error propagation.
    fn invoke_with_args(&self, arguments: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = arguments
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)));
        let paths = PathArgument::new(&arguments.args[1])?;
        let arrays = ColumnarValue::values_to_arrays(&arguments.args)?;
        let output = evaluate(&arrays[0], &paths, arguments.return_type())?;
        if scalar {
            return ScalarValue::try_from_array(&output, 0).map(ColumnarValue::Scalar);
        }
        Ok(ColumnarValue::Array(output))
    }
}

/// The path-list and wildcard result uses one nullable UTF-8 list element type.
fn list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

/// Select scalar or list output according to the already-bound expression type.
fn evaluate(documents: &ArrayRef, paths: &PathArgument, output: &DataType) -> Result<ArrayRef> {
    if matches!(output, DataType::List(_)) {
        return evaluate_lists(documents, paths);
    }
    let mut values = StringBuilder::new();
    for row in 0..documents.len() {
        let selected = extract_row(documents, paths, row)?;
        values.append_option(scalar_match(selected)?);
    }
    Ok(Arc::new(values.finish()))
}

/// Reject a runtime wildcard whose dynamic path cannot have a fixed scalar result type.
fn scalar_match(selected: Option<(Selection, Vec<Option<String>>)>) -> Result<Option<String>> {
    let Some((selection, values)) = selected else {
        return Ok(None);
    };
    if selection.multiple() {
        return Err(DataFusionError::Execution(
            "wildcard JSON paths must be constant".into(),
        ));
    }
    Ok(values.into_iter().next().flatten())
}

/// Retain NULL lists, empty wildcard matches and NULL elements as distinct results.
fn evaluate_lists(documents: &ArrayRef, paths: &PathArgument) -> Result<ArrayRef> {
    let mut output = ListBuilder::new(StringBuilder::new());
    for row in 0..documents.len() {
        let Some((_, values)) = extract_row(documents, paths, row)? else {
            output.append(false);
            continue;
        };
        for value in values {
            output.values().append_option(value);
        }
        output.append(true);
    }
    Ok(Arc::new(output.finish()))
}

/// Parse only non-NULL documents and propagate malformed JSON rather than hiding it.
fn extract_row(
    documents: &ArrayRef,
    paths: &PathArgument,
    row: usize,
) -> Result<Option<(Selection, Vec<Option<String>>)>> {
    if documents.is_null(row) {
        return Ok(None);
    }
    let Some(selection) = paths.at(row)? else {
        return Ok(None);
    };
    let value = ScalarValue::try_from_array(documents, row)?;
    let ScalarValue::Utf8(Some(text)) = value else {
        return Err(DataFusionError::Execution(
            "JSON document must be VARCHAR".into(),
        ));
    };
    let document = JsonDocument::parse(&text)?;
    let values = selection.extract(&document)?;
    Ok(Some((selection, values)))
}

/// Parse a text path while preserving its exact native syntax error.
fn parse_path(path: &str) -> Result<JsonPath> {
    JsonPath::parse(path).map_err(DataFusionError::Plan)
}

#[cfg(test)]
mod tests;
