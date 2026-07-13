//! Arrow-to-Postgres result conversion.
//!
//! `to_response` turns an engine result (a schema plus Arrow batches) into a
//! pgwire `QueryResponse`: the schema becomes the row description (column name +
//! Postgres type OID), and every cell is encoded in the format the client asked
//! for. `describe_fields` builds the same row description from the engine's
//! static column types (used by the extended protocol's Describe, which resolves
//! the schema without executing). Types the server cannot map raise a proper
//! Postgres error rather than shipping a mislabeled or truncated value.
//!
//! Two encoding paths cover the result types. Booleans, integers, floats, and
//! UTF-8 strings have a native Rust value whose binary and text encodings pgwire
//! produces directly, so they honor either result format a client requests.
//! Dates, timestamps, and decimals are rendered to their canonical Postgres text
//! via Arrow's formatter; a client that asks for these columns in BINARY format
//! is refused loudly, because a text rendering shipped under a binary format code
//! would be silently misread.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use fq_common::DataType as EngineType;
use futures::stream;
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// Build a `QueryResponse` from an engine result, encoding each column in the
/// format `result_format` requests (all-text for the simple query protocol, the
/// client's per-column choice for the extended protocol). The rows are
/// materialized eagerly so that a mid-result encoding failure surfaces as one
/// clean `ErrorResponse` here, before any DataRow is streamed, instead of a
/// truncated result the client would misread.
pub fn to_response(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    result_format: &Format,
) -> PgWireResult<Response> {
    let fields = Arc::new(field_infos(schema, result_format)?);
    let options = text_options();
    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
    for batch in batches {
        let formatters = batch_text_formatters(batch, &options)?;
        for row in 0..batch.num_rows() {
            for (array, formatter) in batch.columns().iter().zip(&formatters) {
                encode_cell(&mut encoder, array.as_ref(), formatter.as_ref(), row)?;
            }
            rows.push(encoder.take_row());
        }
    }
    let stream = stream::iter(rows.into_iter().map(Ok::<_, PgWireError>));
    Ok(Response::Query(QueryResponse::new(fields, stream)))
}

/// Build the row description for a result whose columns are known only by the
/// engine's static types (the extended protocol's Describe path, which plans but
/// does not execute). The format is text: a Describe response's format codes are
/// advisory, and the actual result rows carry the format the client requested at
/// Bind. An engine type the server cannot map fails loudly.
pub fn describe_fields(columns: &[(String, EngineType)]) -> PgWireResult<Vec<FieldInfo>> {
    let mut fields = Vec::with_capacity(columns.len());
    for (name, engine_type) in columns {
        let pg_type = engine_to_pg_type(*engine_type)
            .ok_or_else(|| unsupported_engine_type(name, *engine_type))?;
        fields.push(FieldInfo::new(
            name.clone(),
            None,
            None,
            pg_type,
            FieldFormat::Text,
        ));
    }
    Ok(fields)
}

/// The text formatting used for date/timestamp/decimal cells: Postgres renders a
/// timestamp with a space between date and time (Arrow defaults to a `T`), so the
/// timestamp formats are set explicitly; dates and decimals match Arrow's default.
fn text_options() -> FormatOptions<'static> {
    FormatOptions::default()
        .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"))
        .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.f%:z"))
}

/// Describe each output column from the Arrow schema: its name, its mapped
/// Postgres type, and the format the client requested for it. A column type the
/// server cannot map fails loudly; so does a request to encode a text-rendered
/// type (date/timestamp/decimal) in binary, which the server does not produce.
fn field_infos(schema: &SchemaRef, result_format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for (index, field) in schema.fields().iter().enumerate() {
        let pg_type = arrow_to_pg_type(field.data_type())
            .ok_or_else(|| unsupported_type(field.name(), field.data_type()))?;
        let format = result_format.format_for(index);
        if renders_as_text(field.data_type()) && format == FieldFormat::Binary {
            return Err(unsupported_binary(field.name(), field.data_type()));
        }
        fields.push(FieldInfo::new(
            field.name().clone(),
            None,
            None,
            pg_type,
            format,
        ));
    }
    Ok(fields)
}

/// Build one text formatter per column that renders as text (date, timestamp,
/// decimal), and `None` for the columns encoded natively. A formatter borrows its
/// column and the shared options for the batch's lifetime.
fn batch_text_formatters<'a>(
    batch: &'a RecordBatch,
    options: &'a FormatOptions,
) -> PgWireResult<Vec<Option<ArrayFormatter<'a>>>> {
    let mut formatters = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        if renders_as_text(column.data_type()) {
            let formatter = ArrayFormatter::try_new(column.as_ref(), options)
                .map_err(|error| encode_failure(&error.to_string()))?;
            formatters.push(Some(formatter));
        } else {
            formatters.push(None);
        }
    }
    Ok(formatters)
}

/// Whether the server renders this Arrow type to Postgres text rather than
/// encoding a native value. These are the types with no directly-reusable Rust
/// wire value: dates, timestamps, and decimals.
fn renders_as_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    )
}

/// Map an Arrow data type to the Postgres type used in the row description.
/// Integers widen to the smallest signed Postgres integer that holds every value
/// of the Arrow type, matching how `encode_cell` widens the values. Returns
/// `None` for a type the server does not encode (including a timestamp in a
/// non-UTC zone, which the text renderer cannot honestly place).
fn arrow_to_pg_type(data_type: &DataType) -> Option<Type> {
    Some(match data_type {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::UInt8 | DataType::Int16 => Type::INT2,
        DataType::UInt16 | DataType::Int32 => Type::INT4,
        DataType::UInt32 | DataType::Int64 | DataType::UInt64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => Type::TEXT,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        DataType::Timestamp(_, Some(zone)) if is_utc(zone) => Type::TIMESTAMPTZ,
        DataType::Decimal128(_, _) => Type::NUMERIC,
        _ => return None,
    })
}

/// Map an engine static type to the Postgres type used in a Describe row
/// description. It agrees with `arrow_to_pg_type` at the OID level for every type
/// the executor can produce, so a described column and its executed rows carry
/// the same Postgres type. Returns `None` for a type that never reaches a result
/// column (INTERVAL, NULL).
fn engine_to_pg_type(engine_type: EngineType) -> Option<Type> {
    Some(match engine_type {
        EngineType::Integer => Type::INT4,
        EngineType::BigInt => Type::INT8,
        EngineType::Float => Type::FLOAT4,
        EngineType::Double => Type::FLOAT8,
        EngineType::Decimal => Type::NUMERIC,
        EngineType::Varchar | EngineType::Text => Type::TEXT,
        EngineType::Boolean => Type::BOOL,
        EngineType::Date => Type::DATE,
        EngineType::Timestamp => Type::TIMESTAMP,
        EngineType::Interval | EngineType::Null => return None,
    })
}

/// Whether a timestamp zone names UTC. Only UTC-stored timestamps render to a
/// correct fixed-offset text; any other named zone is refused as unmappable
/// rather than placed at the wrong instant.
fn is_utc(zone: &str) -> bool {
    zone.eq_ignore_ascii_case("UTC") || zone == "+00:00" || zone == "+00" || zone == "Z"
}

/// Encode one cell into the current row. A null slot writes a Postgres NULL
/// regardless of the column type. A column with a text formatter renders through
/// it (date/timestamp/decimal); any other column encodes its widened native
/// value, which pgwire emits in the column's requested text or binary format.
fn encode_cell(
    encoder: &mut DataRowEncoder,
    array: &dyn Array,
    formatter: Option<&ArrayFormatter>,
    row: usize,
) -> PgWireResult<()> {
    if array.is_null(row) {
        return encoder.encode_field(&Option::<i32>::None);
    }
    if let Some(formatter) = formatter {
        // field_infos refused a binary request for a text-rendered column, so
        // the field format here is text and the rendered string is correct.
        let text = formatter.value(row).to_string();
        return encoder.encode_field(&text);
    }
    encode_native(encoder, array, row)
}

/// Encode a non-null cell of a natively-encoded column (boolean, integer, float,
/// or string) as its widened Rust value. Reaching the fallthrough means the
/// schema and the data disagree - `field_infos` accepted a type `encode_cell`
/// then found no encoder for - so it fails loudly rather than shipping a wrong
/// value.
fn encode_native(encoder: &mut DataRowEncoder, array: &dyn Array, row: usize) -> PgWireResult<()> {
    match array.data_type() {
        DataType::Boolean => encoder.encode_field(&column::<BooleanArray>(array).value(row)),
        DataType::Int8 => encoder.encode_field(&i16::from(column::<Int8Array>(array).value(row))),
        DataType::Int16 => encoder.encode_field(&column::<Int16Array>(array).value(row)),
        DataType::Int32 => encoder.encode_field(&column::<Int32Array>(array).value(row)),
        DataType::Int64 => encoder.encode_field(&column::<Int64Array>(array).value(row)),
        DataType::UInt8 => encoder.encode_field(&i16::from(column::<UInt8Array>(array).value(row))),
        DataType::UInt16 => {
            encoder.encode_field(&i32::from(column::<UInt16Array>(array).value(row)))
        }
        DataType::UInt32 => {
            encoder.encode_field(&i64::from(column::<UInt32Array>(array).value(row)))
        }
        DataType::UInt64 => {
            let value = column::<UInt64Array>(array).value(row);
            let signed = i64::try_from(value).map_err(|_| out_of_range(value))?;
            encoder.encode_field(&signed)
        }
        DataType::Float32 => encoder.encode_field(&column::<Float32Array>(array).value(row)),
        DataType::Float64 => encoder.encode_field(&column::<Float64Array>(array).value(row)),
        DataType::Utf8 => encoder.encode_field(&column::<StringArray>(array).value(row)),
        DataType::LargeUtf8 => encoder.encode_field(&column::<LargeStringArray>(array).value(row)),
        other => Err(unsupported_type("<row cell>", other)),
    }
}

/// Downcast an Arrow column to its concrete array type. The caller has already
/// matched on `array.data_type()`, so a failed downcast is an invariant
/// violation and panics rather than shipping a wrong value.
fn column<T: Array + 'static>(array: &dyn Array) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .expect("arrow column downcast matches its data type")
}

/// The error returned when a result column carries an Arrow type the server
/// cannot map to a Postgres type.
fn unsupported_type(name: &str, data_type: &DataType) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        format!(
            "result column \"{name}\" has type {data_type:?}, which fedq-server does not map to a Postgres type"
        ),
    )))
}

/// The error returned when a result column carries an engine type the server
/// cannot map to a Postgres type (INTERVAL, NULL).
fn unsupported_engine_type(name: &str, engine_type: EngineType) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        format!(
            "result column \"{name}\" has type {engine_type}, which fedq-server does not map to a Postgres type"
        ),
    )))
}

/// The error returned when a client requests a date/timestamp/decimal column in
/// binary format, which the server renders only as text.
fn unsupported_binary(name: &str, data_type: &DataType) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        format!(
            "result column \"{name}\" of type {data_type:?} is only encoded in text format; request it in text format"
        ),
    )))
}

/// The error returned when Arrow's formatter cannot render a cell.
fn encode_failure(detail: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        format!("failed to render a result cell as text: {detail}"),
    )))
}

/// The error returned when an unsigned 8-byte value does not fit Postgres INT8.
fn out_of_range(value: u64) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "22003".to_owned(),
        format!("unsigned integer {value} does not fit in a signed 8-byte Postgres integer"),
    )))
}
