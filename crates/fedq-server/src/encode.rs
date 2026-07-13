//! Arrow-to-Postgres result conversion.
//!
//! `to_response` turns an engine result (a schema plus Arrow batches) into a
//! pgwire `QueryResponse`: the schema becomes the row description (column name +
//! Postgres type OID), and every cell is encoded in the text format. Types the
//! server cannot map raise a proper Postgres error rather than shipping a
//! mislabeled or truncated value.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, SchemaRef};
use futures::stream;
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// Build a `QueryResponse` from an engine result. The rows are materialized
/// eagerly so that a mid-result encoding failure surfaces as one clean
/// `ErrorResponse` here, before any DataRow is streamed, instead of a truncated
/// result the client would misread.
pub fn to_response(schema: &SchemaRef, batches: &[RecordBatch]) -> PgWireResult<Response> {
    let fields = Arc::new(field_infos(schema)?);
    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
    for batch in batches {
        for row in 0..batch.num_rows() {
            for column in 0..batch.num_columns() {
                encode_cell(&mut encoder, batch.column(column).as_ref(), row)?;
            }
            rows.push(encoder.take_row());
        }
    }
    let stream = stream::iter(rows.into_iter().map(Ok::<_, PgWireError>));
    Ok(Response::Query(QueryResponse::new(fields, stream)))
}

/// Describe each output column: its user-visible name and its mapped Postgres
/// type, in text format. An unmappable column type fails loudly.
fn field_infos(schema: &SchemaRef) -> PgWireResult<Vec<FieldInfo>> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for (index, field) in schema.fields().iter().enumerate() {
        let pg_type = arrow_to_pg_type(field.data_type())
            .ok_or_else(|| unsupported_type(field.name(), field.data_type()))?;
        fields.push(FieldInfo::new(
            field.name().clone(),
            None,
            None,
            pg_type,
            Format::UnifiedText.format_for(index),
        ));
    }
    Ok(fields)
}

/// Map an Arrow data type to the Postgres type used in the row description.
/// Integers widen to the smallest signed Postgres integer that holds every value
/// of the Arrow type, matching how `encode_cell` widens the values. Returns
/// `None` for a type the server does not encode.
fn arrow_to_pg_type(data_type: &DataType) -> Option<Type> {
    Some(match data_type {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::UInt8 | DataType::Int16 => Type::INT2,
        DataType::UInt16 | DataType::Int32 => Type::INT4,
        DataType::UInt32 | DataType::Int64 | DataType::UInt64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => Type::TEXT,
        _ => return None,
    })
}

/// Encode one cell into the current row. A null slot writes a Postgres NULL
/// regardless of the column type. Non-null values are widened to match the type
/// chosen in `arrow_to_pg_type`.
fn encode_cell(encoder: &mut DataRowEncoder, array: &dyn Array, row: usize) -> PgWireResult<()> {
    if array.is_null(row) {
        return encoder.encode_field(&Option::<i32>::None);
    }
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
        // field_infos already rejected any other type before encoding began, so
        // reaching here means the schema and the data disagree: fail loudly.
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

/// The error returned when a result column carries a type the server cannot map.
fn unsupported_type(name: &str, data_type: &DataType) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        format!(
            "result column \"{name}\" has type {data_type:?}, which fedq-server does not map to a Postgres type"
        ),
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
