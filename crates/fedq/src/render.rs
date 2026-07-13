//! Rendering an engine result (an Arrow schema plus batches) to terminal text in
//! one of three formats.
//!
//! Every format walks the same batches; the difference is the per-cell
//! representation of a value and of a null. `table` draws an aligned ASCII grid
//! and shows a null as the literal `NULL`. `csv` writes RFC-4180 rows and shows a
//! null as an empty field. `json` emits an array of one object per row, with
//! integers/floats/booleans as JSON scalars and a null as JSON `null`; every
//! other type (strings, dates, timestamps, decimals) renders through the shared
//! Arrow text path, matching how the wire server prints those types.

use std::fmt::Write as _;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    RecordBatch, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use clap::ValueEnum;
use serde_json::{Map, Number, Value};

/// The three terminal output formats a run can request.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// An aligned ASCII grid with a bordered header; the default, for humans.
    Table,
    /// RFC-4180 comma-separated values with a header row.
    Csv,
    /// A JSON array of one object per row, values typed where the Arrow type has
    /// a JSON scalar.
    Json,
}

/// Every way rendering a result can fail. Both variants come from a downstream
/// library refusing to represent a cell; neither is swallowed, so a value is
/// never dropped or mislabeled to keep the output flowing.
#[derive(Debug)]
pub enum RenderError {
    /// Arrow's formatter cannot render a column's values as text.
    Format(String),
    /// The CSV or JSON serializer rejected the assembled rows.
    Serialize(String),
}

impl std::fmt::Display for RenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RenderError::Format(detail) => write!(f, "failed to render a result cell: {detail}"),
            RenderError::Serialize(detail) => write!(f, "failed to serialize the result: {detail}"),
        }
    }
}

impl std::error::Error for RenderError {}

/// Render the whole result in the requested format, returning the text to print.
pub fn render(
    format: OutputFormat,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<String, RenderError> {
    match format {
        OutputFormat::Table => render_table(schema, batches),
        OutputFormat::Csv => render_csv(schema, batches),
        OutputFormat::Json => render_json(schema, batches),
    }
}

/// The total number of result rows across all batches, for a caller that prints
/// a row-count summary.
pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// The text formatting for temporal and decimal cells: a timestamp prints with a
/// space between its date and time (Arrow defaults to a `T`), matching the wire
/// server's rendering so the two front ends agree.
fn format_options() -> FormatOptions<'static> {
    FormatOptions::default()
        .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"))
        .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.f%:z"))
}

/// One text formatter per column, borrowing the batch and the shared options for
/// the batch's lifetime. A formatter renders every non-null cell of its column.
fn column_formatters<'a>(
    batch: &'a RecordBatch,
    options: &'a FormatOptions,
) -> Result<Vec<ArrayFormatter<'a>>, RenderError> {
    let mut formatters = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        let formatter = ArrayFormatter::try_new(column.as_ref(), options)
            .map_err(|error| RenderError::Format(error.to_string()))?;
        formatters.push(formatter);
    }
    Ok(formatters)
}

/// The header names of the result, in column order.
fn headers(schema: &SchemaRef) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// Render one cell as display text, or `None` when the slot is null. The caller
/// decides how a null prints (the literal `NULL`, an empty CSV field, ...).
fn cell_text(array: &dyn Array, formatter: &ArrayFormatter, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    Some(formatter.value(row).to_string())
}

/// Render the result as an aligned ASCII grid: a bordered header row followed by
/// one bordered row per record, every column padded to its widest cell. A null
/// cell prints as the literal `NULL`.
fn render_table(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<String, RenderError> {
    let headers = headers(schema);
    let options = format_options();
    let mut rows: Vec<Vec<String>> = Vec::new();
    for batch in batches {
        let formatters = column_formatters(batch, &options)?;
        for row in 0..batch.num_rows() {
            let mut record = Vec::with_capacity(batch.num_columns());
            for (column, formatter) in batch.columns().iter().zip(&formatters) {
                let text =
                    cell_text(column.as_ref(), formatter, row).unwrap_or_else(|| "NULL".to_owned());
                record.push(text);
            }
            rows.push(record);
        }
    }
    Ok(grid(&headers, &rows))
}

/// Assemble the bordered grid string from the header names and the row texts.
fn grid(headers: &[String], rows: &[Vec<String>]) -> String {
    let widths = column_widths(headers, rows);
    let border = border(&widths);
    let mut out = String::new();
    out.push_str(&border);
    out.push('\n');
    out.push_str(&formatted_row(headers, &widths));
    out.push('\n');
    out.push_str(&border);
    out.push('\n');
    for row in rows {
        out.push_str(&formatted_row(row, &widths));
        out.push('\n');
    }
    out.push_str(&border);
    out
}

/// Each column's display width: the longest of its header and every cell in it.
fn column_widths(headers: &[String], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths: Vec<usize> = headers.iter().map(String::len).collect();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if cell.len() > widths[index] {
                widths[index] = cell.len();
            }
        }
    }
    widths
}

/// The horizontal border line (`+----+----+`) for the given column widths.
fn border(widths: &[usize]) -> String {
    let mut out = String::from("+");
    for &width in widths {
        for _ in 0..width + 2 {
            out.push('-');
        }
        out.push('+');
    }
    out
}

/// One grid row (`| a | b |`), each value left-aligned and padded to its column.
fn formatted_row(values: &[String], widths: &[usize]) -> String {
    let mut out = String::from("|");
    for (value, &width) in values.iter().zip(widths) {
        // Left-align by padding the value to the column width; a value can never
        // exceed the width because the width was computed from it.
        let _ = write!(out, " {value:<width$} |");
    }
    out
}

/// Render the result as RFC-4180 CSV: a header row, then one row per record. The
/// csv writer quotes and escapes any field with a comma, quote, or newline; a
/// null slot writes an empty field.
fn render_csv(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<String, RenderError> {
    let options = format_options();
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer
        .write_record(headers(schema))
        .map_err(|error| RenderError::Serialize(error.to_string()))?;
    for batch in batches {
        let formatters = column_formatters(batch, &options)?;
        for row in 0..batch.num_rows() {
            let mut record = Vec::with_capacity(batch.num_columns());
            for (column, formatter) in batch.columns().iter().zip(&formatters) {
                record.push(cell_text(column.as_ref(), formatter, row).unwrap_or_default());
            }
            writer
                .write_record(&record)
                .map_err(|error| RenderError::Serialize(error.to_string()))?;
        }
    }
    let bytes = writer
        .into_inner()
        .map_err(|error| RenderError::Serialize(error.to_string()))?;
    // The csv writer only ever wrote UTF-8 text (headers and rendered cells).
    String::from_utf8(bytes).map_err(|error| RenderError::Serialize(error.to_string()))
}

/// Render the result as a pretty-printed JSON array of one object per row, keyed
/// by column name. Integers, floats, and booleans become JSON scalars; a null
/// slot becomes JSON `null`; every other type renders through the shared text
/// path as a JSON string.
fn render_json(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<String, RenderError> {
    let names = headers(schema);
    let options = format_options();
    let mut records: Vec<Value> = Vec::new();
    for batch in batches {
        let formatters = column_formatters(batch, &options)?;
        for row in 0..batch.num_rows() {
            let mut object = Map::new();
            for (index, name) in names.iter().enumerate() {
                let column = batch.column(index);
                let value = cell_json(column.as_ref(), &formatters[index], row);
                object.insert(name.clone(), value);
            }
            records.push(Value::Object(object));
        }
    }
    serde_json::to_string_pretty(&Value::Array(records))
        .map_err(|error| RenderError::Serialize(error.to_string()))
}

/// Render one cell as a JSON value. A null slot is JSON `null`. An integer,
/// float, or boolean maps to its JSON scalar (the reason `json` exists over
/// `csv`); every other Arrow type falls back to the shared text rendering as a
/// JSON string, so no value is dropped or mislabeled.
fn cell_json(array: &dyn Array, formatter: &ArrayFormatter, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => Value::Bool(downcast::<BooleanArray>(array).value(row)),
        DataType::Int8 => Value::from(downcast::<Int8Array>(array).value(row)),
        DataType::Int16 => Value::from(downcast::<Int16Array>(array).value(row)),
        DataType::Int32 => Value::from(downcast::<Int32Array>(array).value(row)),
        DataType::Int64 => Value::from(downcast::<Int64Array>(array).value(row)),
        DataType::UInt8 => Value::from(downcast::<UInt8Array>(array).value(row)),
        DataType::UInt16 => Value::from(downcast::<UInt16Array>(array).value(row)),
        DataType::UInt32 => Value::from(downcast::<UInt32Array>(array).value(row)),
        DataType::UInt64 => Value::from(downcast::<UInt64Array>(array).value(row)),
        DataType::Float32 => float_value(f64::from(downcast::<Float32Array>(array).value(row))),
        DataType::Float64 => float_value(downcast::<Float64Array>(array).value(row)),
        _ => Value::String(formatter.value(row).to_string()),
    }
}

/// A finite float becomes a JSON number; a non-finite float (NaN, +/-inf) has no
/// JSON number and renders as its text form rather than being silently dropped.
fn float_value(value: f64) -> Value {
    match Number::from_f64(value) {
        Some(number) => Value::Number(number),
        None => Value::String(value.to_string()),
    }
}

/// Downcast an Arrow column to its concrete array type. The caller matched on
/// `array.data_type()` immediately above, so a failed downcast is an invariant
/// violation and panics rather than shipping a wrong value.
fn downcast<T: Array + 'static>(array: &dyn Array) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .expect("arrow column downcast matches its data type")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use arrow_array::{Float64Array, Int32Array, StringArray};

    use super::*;

    /// A two-column batch (an integer id and a string name) with a null in each
    /// column, plus a float column carrying a null, for the render assertions.
    fn sample() -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let ids = Int32Array::from(vec![Some(1), None, Some(3)]);
        let names = StringArray::from(vec![Some("alice"), Some("bob"), None]);
        let scores = Float64Array::from(vec![Some(9.5), None, Some(2.0)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
        )
        .expect("build sample batch");
        (schema, vec![batch])
    }

    #[test]
    fn table_aligns_columns_and_shows_null_literal() {
        let (schema, batches) = sample();
        let out = render(OutputFormat::Table, &schema, &batches).expect("render table");
        let lines: Vec<&str> = out.lines().collect();
        // Header, then two border lines around it, then three data rows, then a
        // closing border: 3 + 3 + 1 = 7 lines.
        assert_eq!(lines.len(), 7, "grid line count:\n{out}");
        assert!(lines[1].contains("id") && lines[1].contains("name") && lines[1].contains("score"));
        // The null id and null name both print as the literal NULL.
        assert!(lines[4].contains("NULL"), "null id row: {}", lines[4]);
        assert!(lines[5].contains("NULL"), "null name row: {}", lines[5]);
        // Every grid line is the same width (columns are aligned).
        let width = lines[0].len();
        for line in &lines {
            assert_eq!(line.len(), width, "ragged line: {line}");
        }
    }

    #[test]
    fn csv_has_header_empty_null_and_quotes_as_needed() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids = Int32Array::from(vec![Some(1), None]);
        // The second name embeds a comma, which must be quoted, not split.
        let names = StringArray::from(vec![Some("alice"), Some("b,ob")]);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(names)])
            .expect("batch");
        let out = render(OutputFormat::Csv, &schema, &[batch]).expect("render csv");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,name");
        assert_eq!(lines[1], "1,alice");
        // A null integer is an empty leading field; the comma value is quoted.
        assert_eq!(lines[2], ",\"b,ob\"");
    }

    #[test]
    fn json_types_scalars_and_nulls() {
        let (schema, batches) = sample();
        let out = render(OutputFormat::Json, &schema, &batches).expect("render json");
        let parsed: Value = serde_json::from_str(&out).expect("valid json");
        let rows = parsed.as_array().expect("top-level array");
        assert_eq!(rows.len(), 3);
        // Row 0: typed integer and float, string name.
        assert_eq!(rows[0]["id"], Value::from(1));
        assert_eq!(rows[0]["name"], Value::from("alice"));
        assert_eq!(rows[0]["score"], Value::from(9.5));
        // Row 1: the null id and null score are JSON null, not the string "NULL".
        assert_eq!(rows[1]["id"], Value::Null);
        assert_eq!(rows[1]["score"], Value::Null);
        // Row 2: the null name is JSON null.
        assert_eq!(rows[2]["name"], Value::Null);
    }

    #[test]
    fn row_count_sums_batches() {
        let (_schema, batches) = sample();
        assert_eq!(row_count(&batches), 3);
    }
}
