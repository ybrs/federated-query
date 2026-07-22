//! The Parquet connector (catalog/statistics tier). A directory of
//! `<table>.parquet` files is one federated source; every file is a table under
//! the schema `main` (the schema the fq-exec DataFusion reader registers the
//! files under, so a pushed `SELECT ... FROM main.<table>` resolves there). A
//! single-file source (`file = ...`) is one table named after the datasource.
//!
//! Everything here reads FOOTERS ONLY - the Parquet metadata block at the tail of
//! each file - so it is O(metadata), never a data scan: the schema, the exact row
//! count, and the row-group min/max/null-count statistics all live in the footer.
//! The row DATA is read by the exec data plane (fq-exec's DataFusion parquet
//! scan), which applies projection and filter (row-group pruning) natively.
//!
//! A Parquet directory is a FILE source: it is not a JOIN target (a cross-table
//! join has no server to run on and executes in the coordinator) and not a SHIP
//! target (the files are read-only), so those two capabilities are absent. Every
//! single-relation shape the engine pushes (filters, projections, aggregates,
//! windows, DISTINCT, ORDER BY, LIMIT) is executed by DataFusion, so those
//! capabilities are present and honest.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};
use fq_catalog::{
    build_column_statistics, build_table_statistics, map_native_type_default, CatalogError,
    ColumnMetadata, ColumnStatistics, DataSource, DataSourceCapability, RenderDialect, StatValue,
    TableMetadata, TableStatistics,
};
use fq_common::DataType;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics as PqStatistics;

/// The one schema every Parquet file is exposed under. It matches the schema the
/// fq-exec reader registers the files in, so a rendered `FROM main.<table>`
/// resolves against DataFusion at fetch time.
const PARQUET_SCHEMA: &str = "main";

/// A directory of `<table>.parquet` files, federated as one read-only source.
#[derive(Debug)]
pub struct ParquetSource {
    name: String,
    /// table name -> its `<dir>/<table>.parquet` path, discovered once at
    /// construction so a metadata/stats read opens the file directly without
    /// re-scanning the directory.
    tables: BTreeMap<String, PathBuf>,
}

impl ParquetSource {
    /// Open a directory of Parquet files as a named source. Every `<name>.parquet`
    /// in the directory becomes a table `<name>`. A single-file source is one
    /// table NAMED AFTER THE DATASOURCE, so `SELECT * FROM <datasource>` resolves
    /// it as a bare table reference. Raises if the directory does not exist,
    /// cannot be read, or holds no Parquet file (a Parquet source pointed at a
    /// directory with no tables is a misconfiguration, not an empty success).
    pub fn open(name: impl Into<String>, path: &str) -> Result<Self, CatalogError> {
        let name = name.into();
        let root = Path::new(path);
        let mut tables = BTreeMap::new();
        if root.is_file() {
            // A single `.parquet` file is one table named after the datasource
            // (the exec plane registers it in DataFusion under the same name).
            if parquet_table_name(root).is_none() {
                return Err(CatalogError::Source(format!(
                    "parquet file '{path}' is not a .parquet file"
                )));
            }
            tables.insert(name.clone(), root.to_path_buf());
        } else {
            let entries = std::fs::read_dir(root).map_err(|error| {
                CatalogError::Source(format!("parquet path '{path}' cannot be read: {error}"))
            })?;
            for entry in entries {
                let child = entry
                    .map_err(|error| {
                        CatalogError::Source(format!("parquet dir '{path}': {error}"))
                    })?
                    .path();
                if let Some(table) = parquet_table_name(&child) {
                    tables.insert(table, child);
                }
            }
            if tables.is_empty() {
                return Err(CatalogError::Source(format!(
                    "parquet dir '{path}' holds no <table>.parquet files"
                )));
            }
        }
        Ok(Self { name, tables })
    }

    /// The path of one table's Parquet file, raising if this source has no such
    /// table (a scan over an unknown table must fail loud, not read nothing).
    fn table_path(&self, table: &str) -> Result<&PathBuf, CatalogError> {
        self.tables.get(table).ok_or_else(|| {
            CatalogError::Source(format!(
                "parquet source '{}' has no table '{table}'",
                self.name
            ))
        })
    }

    /// Open one table's footer reader.
    fn reader(&self, table: &str) -> Result<SerializedFileReader<File>, CatalogError> {
        let path = self.table_path(table)?;
        let file = File::open(path)
            .map_err(|error| CatalogError::Source(format!("open parquet '{table}': {error}")))?;
        SerializedFileReader::new(file).map_err(|error| {
            CatalogError::Source(format!("read parquet footer '{table}': {error}"))
        })
    }
}

/// The table name a `<stem>.parquet` path exposes, or `None` for any other file
/// (a `.crc`, a subdirectory) so unrelated directory entries are skipped.
fn parquet_table_name(path: &Path) -> Option<String> {
    if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
        return None;
    }
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::to_string)
}

impl DataSource for ParquetSource {
    fn name(&self) -> &str {
        &self.name
    }

    /// DataFusion dialect: the pushed scan SQL runs through the exec plane's
    /// DataFusion over the local files, so it must render in DataFusion's own
    /// dialect. Rendering as another engine's dialect drops clauses that engine
    /// treats as its default but DataFusion does not (a DESC ORDER BY without an
    /// explicit NULLS placement sorts nulls first in DataFusion, last in DuckDB).
    fn render_dialect(&self) -> RenderDialect {
        RenderDialect::Parquet
    }

    /// Everything DataFusion executes over a single pushed relation, and nothing
    /// else: no JOIN (a file source has no server; cross-table joins run in the
    /// coordinator) and no SHIP_TARGET (the files are read-only).
    fn capabilities(&self) -> Vec<DataSourceCapability> {
        vec![
            DataSourceCapability::Aggregations,
            DataSourceCapability::WindowFunctions,
            DataSourceCapability::Subqueries,
            DataSourceCapability::Cte,
            DataSourceCapability::Distinct,
            DataSourceCapability::Limit,
            DataSourceCapability::OrderBy,
        ]
    }

    fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
        Ok(vec![PARQUET_SCHEMA.to_string()])
    }

    /// The Parquet files under this source. A schema other than `main` holds none
    /// (this source exposes exactly one schema), which the catalog never asks for.
    fn list_tables(&self, schema: &str) -> Result<Vec<String>, CatalogError> {
        if schema != PARQUET_SCHEMA {
            return Ok(Vec::new());
        }
        Ok(self.tables.keys().cloned().collect())
    }

    /// Column names + engine types + the exact row count, all from the footer. An
    /// Arrow type the engine does not model raises (never coerced to a string).
    fn get_table_metadata(
        &self,
        _schema: &str,
        table: &str,
    ) -> Result<TableMetadata, CatalogError> {
        let reader = self.reader(table)?;
        let file_meta = reader.metadata().file_metadata();
        let arrow_schema =
            parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
                .map_err(|error| {
                    CatalogError::Source(format!("parquet arrow schema '{table}': {error}"))
                })?;
        let mut columns = Vec::with_capacity(arrow_schema.fields().len());
        for field in arrow_schema.fields() {
            columns.push(column_metadata(field)?);
        }
        Ok(TableMetadata {
            schema_name: PARQUET_SCHEMA.to_string(),
            table_name: table.to_string(),
            columns,
            row_count: Some(file_meta.num_rows()),
            size_bytes: None,
        })
    }

    /// The exact row count (footer) plus, for each requested column that carries
    /// row-group statistics, its aggregated min/max and null fraction. Unknown
    /// values stay `None` - never fabricated.
    fn get_table_statistics(
        &self,
        _schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        let reader = self.reader(table)?;
        let row_count = reader.metadata().file_metadata().num_rows();
        let column_stats = column_statistics(&reader, columns, row_count);
        Ok(Some(build_table_statistics(Some(row_count), column_stats)))
    }

    /// Map an Arrow-derived native type name to an engine type through the default
    /// mapping. The names `get_table_metadata` produces are the canonical SQL type
    /// names the default handles.
    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        map_native_type_default(type_str)
    }

    /// The version token is the table file's `(size, mtime_ns)` stamp. Parquet
    /// files are immutable - a change replaces the file - so any change moves
    /// the stamp; the stamp is exact for this one-file-per-table layout. The
    /// file vanishing raises: the directory was scanned at open, so a missing
    /// file is a source that changed shape under the engine.
    fn source_token(&self, _schema: &str, table: &str) -> Result<Option<String>, CatalogError> {
        let path = self.table_path(table)?;
        let metadata = std::fs::metadata(path)
            .map_err(|error| CatalogError::Source(format!("stat parquet '{table}': {error}")))?;
        let mtime_ns = metadata
            .modified()
            .map_err(|error| CatalogError::Source(format!("mtime of parquet '{table}': {error}")))?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| CatalogError::Source(format!("mtime of parquet '{table}': {error}")))?
            .as_nanos();
        Ok(Some(format!(
            "parquet-file:size={},mtime_ns={mtime_ns}",
            metadata.len()
        )))
    }
}

/// Build one column's metadata from its Arrow field: the canonical native type
/// name (raising on an unmodeled Arrow type) and its nullability.
fn column_metadata(field: &ArrowField) -> Result<ColumnMetadata, CatalogError> {
    let native = arrow_type_native_name(field.data_type())?;
    Ok(ColumnMetadata::new(
        field.name().clone(),
        native,
        field.is_nullable(),
    ))
}

/// The canonical SQL type name for an Arrow type, in the vocabulary
/// `map_native_type_default` understands. An Arrow type with no engine modeling
/// (nested lists/structs/maps, raw binary) raises rather than being coerced.
fn arrow_type_native_name(data_type: &ArrowDataType) -> Result<String, CatalogError> {
    if let Some(name) = numeric_native_name(data_type) {
        return Ok(name.to_string());
    }
    if let Some(name) = other_native_name(data_type) {
        return Ok(name.to_string());
    }
    Err(CatalogError::UnsupportedColumnType(format!(
        "{data_type:?}"
    )))
}

/// The native name for a numeric Arrow type (integers widen to INTEGER/BIGINT,
/// floats to REAL/DOUBLE, decimals to DECIMAL), or `None` for a non-numeric type.
fn numeric_native_name(data_type: &ArrowDataType) -> Option<&'static str> {
    match data_type {
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32 => Some("INTEGER"),
        ArrowDataType::Int64 | ArrowDataType::UInt64 => Some("BIGINT"),
        ArrowDataType::Float16 | ArrowDataType::Float32 => Some("REAL"),
        ArrowDataType::Float64 => Some("DOUBLE"),
        ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _) => Some("DECIMAL"),
        _ => None,
    }
}

/// The native name for a boolean, string, or temporal Arrow type, or `None` for a
/// type this helper does not cover (its caller then raises).
fn other_native_name(data_type: &ArrowDataType) -> Option<&'static str> {
    match data_type {
        ArrowDataType::Boolean => Some("BOOLEAN"),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => Some("VARCHAR"),
        ArrowDataType::Date32 | ArrowDataType::Date64 => Some("DATE"),
        ArrowDataType::Timestamp(_, _) => Some("TIMESTAMP"),
        _ => None,
    }
}

/// Aggregate the row-group statistics of each requested column into
/// `ColumnStatistics`. A column with no footer statistics contributes nothing (it
/// stays an honest unknown); an empty `columns` list requests the row count only.
fn column_statistics(
    reader: &SerializedFileReader<File>,
    columns: &[String],
    row_count: i64,
) -> BTreeMap<String, ColumnStatistics> {
    let mut out = BTreeMap::new();
    for column in columns {
        if let Some(stats) = aggregate_column(reader, column, row_count) {
            out.insert(column.clone(), stats);
        }
    }
    out
}

/// Fold one column's per-row-group footer statistics into a single
/// `ColumnStatistics`: min = min of the group mins, max = max of the group maxes,
/// null count = sum of the group null counts. `None` when no row group carries
/// statistics for the column (the column is then an honest unknown).
fn aggregate_column(
    reader: &SerializedFileReader<File>,
    column: &str,
    row_count: i64,
) -> Option<ColumnStatistics> {
    let metadata = reader.metadata();
    let mut null_count: i64 = 0;
    let mut min: Option<StatValue> = None;
    let mut max: Option<StatValue> = None;
    let mut seen = false;
    for group in 0..metadata.num_row_groups() {
        let Some(stats) = column_chunk_stats(metadata.row_group(group), column) else {
            continue;
        };
        seen = true;
        // A null count that overflows i64 is not physically representable; saturate
        // rather than wrap so the derived null fraction can never go negative.
        let group_nulls = i64::try_from(stats.null_count_opt().unwrap_or(0)).unwrap_or(i64::MAX);
        null_count = null_count.saturating_add(group_nulls);
        fold_min(&mut min, stat_min(stats));
        fold_max(&mut max, stat_max(stats));
    }
    if !seen {
        return None;
    }
    Some(build_column_statistics(
        None, null_count, row_count, min, max,
    ))
}

/// The statistics of the named column in one row group, or `None` when the row
/// group has no column of that name or no statistics for it.
fn column_chunk_stats<'a>(
    row_group: &'a parquet::file::metadata::RowGroupMetaData,
    column: &str,
) -> Option<&'a PqStatistics> {
    for index in 0..row_group.num_columns() {
        let chunk = row_group.column(index);
        if chunk.column_path().string() == column {
            return chunk.statistics();
        }
    }
    None
}

/// Keep the smaller of the running min and a new candidate (either may be absent).
fn fold_min(current: &mut Option<StatValue>, candidate: Option<StatValue>) {
    if let Some(value) = candidate {
        let keep = match current.take() {
            Some(existing) if stat_le(&existing, &value) => existing,
            _ => value,
        };
        *current = Some(keep);
    }
}

/// Keep the larger of the running max and a new candidate (either may be absent).
fn fold_max(current: &mut Option<StatValue>, candidate: Option<StatValue>) {
    if let Some(value) = candidate {
        let keep = match current.take() {
            Some(existing) if stat_le(&value, &existing) => existing,
            _ => value,
        };
        *current = Some(keep);
    }
}

/// `a <= b` for two same-variant stat values, on the cost model's ordinal scale.
/// A cross-variant comparison never happens (one column has one type), so it
/// conservatively reports `true` (leaving the running bound unchanged).
fn stat_le(a: &StatValue, b: &StatValue) -> bool {
    match (a, b) {
        (StatValue::Integer(x), StatValue::Integer(y)) => x <= y,
        (StatValue::Float(x), StatValue::Float(y)) => x <= y,
        (StatValue::Text(x), StatValue::Text(y)) => x <= y,
        (StatValue::Boolean(x), StatValue::Boolean(y)) => x <= y,
        _ => true,
    }
}

/// The minimum stat value of a row group's column as a cost-model `StatValue`, or
/// `None` for a type whose ordinal min the cost model does not consume (temporal,
/// decimal - their footer bytes need scale/epoch decoding to be an ordinal).
fn stat_min(stats: &PqStatistics) -> Option<StatValue> {
    match stats {
        PqStatistics::Boolean(v) => v.min_opt().map(|value| StatValue::Boolean(*value)),
        PqStatistics::Int32(v) => v
            .min_opt()
            .map(|value| StatValue::Integer(i64::from(*value))),
        PqStatistics::Int64(v) => v.min_opt().map(|value| StatValue::Integer(*value)),
        PqStatistics::Float(v) => v.min_opt().map(|value| StatValue::Float(f64::from(*value))),
        PqStatistics::Double(v) => v.min_opt().map(|value| StatValue::Float(*value)),
        PqStatistics::ByteArray(v) => v
            .min_opt()
            .and_then(|value| value.as_utf8().ok())
            .map(|text| StatValue::Text(text.to_string())),
        PqStatistics::Int96(_) | PqStatistics::FixedLenByteArray(_) => None,
    }
}

/// The maximum stat value of a row group's column, mirroring `stat_min`.
fn stat_max(stats: &PqStatistics) -> Option<StatValue> {
    match stats {
        PqStatistics::Boolean(v) => v.max_opt().map(|value| StatValue::Boolean(*value)),
        PqStatistics::Int32(v) => v
            .max_opt()
            .map(|value| StatValue::Integer(i64::from(*value))),
        PqStatistics::Int64(v) => v.max_opt().map(|value| StatValue::Integer(*value)),
        PqStatistics::Float(v) => v.max_opt().map(|value| StatValue::Float(f64::from(*value))),
        PqStatistics::Double(v) => v.max_opt().map(|value| StatValue::Float(*value)),
        PqStatistics::ByteArray(v) => v
            .max_opt()
            .and_then(|value| value.as_utf8().ok())
            .map(|text| StatValue::Text(text.to_string())),
        PqStatistics::Int96(_) | PqStatistics::FixedLenByteArray(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_and_float_arrow_types_map_to_widened_names() {
        // Every integer width maps to INTEGER except 64-bit, which maps to BIGINT;
        // the default type mapping then turns those names into engine types.
        assert_eq!(
            arrow_type_native_name(&ArrowDataType::Int16).unwrap(),
            "INTEGER"
        );
        assert_eq!(
            arrow_type_native_name(&ArrowDataType::Int64).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            arrow_type_native_name(&ArrowDataType::UInt64).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            arrow_type_native_name(&ArrowDataType::Float32).unwrap(),
            "REAL"
        );
        assert_eq!(
            arrow_type_native_name(&ArrowDataType::Float64).unwrap(),
            "DOUBLE"
        );
        assert_eq!(
            arrow_type_native_name(&ArrowDataType::Utf8).unwrap(),
            "VARCHAR"
        );
    }

    #[test]
    fn unmodeled_arrow_type_raises_with_its_name() {
        // A raw binary column has no engine modeling; it must raise (never be
        // coerced to a string), carrying the offending Arrow type.
        let error = arrow_type_native_name(&ArrowDataType::Binary).unwrap_err();
        let message = format!("{error}");
        assert!(
            message.contains("Binary"),
            "message names the type: {message}"
        );
    }

    #[test]
    fn fold_min_and_max_keep_the_extremes() {
        // Folding row-group bounds keeps the smallest min and the largest max.
        let mut min = None;
        let mut max = None;
        for value in [5_i64, 2, 9] {
            fold_min(&mut min, Some(StatValue::Integer(value)));
            fold_max(&mut max, Some(StatValue::Integer(value)));
        }
        assert_eq!(min, Some(StatValue::Integer(2)));
        assert_eq!(max, Some(StatValue::Integer(9)));
    }
}
