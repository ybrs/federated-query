//! The Parquet connector (catalog/statistics tier). A directory of
//! `<table>.parquet` files is one federated source; every file is a table under
//! the schema `main` (the schema the fq-exec DataFusion reader registers the
//! files under, so a pushed `SELECT ... FROM main.<table>` resolves there). A
//! single-file source (`file = ...`) is one table named after the datasource.
//!
//! A source can also expose ONE logical table over MANY files, either by glob
//! (`glob = '.../*.parquet'`, recursive `**` supported) or by an explicit list
//! (`files = [...]`) or a manifest text file (`manifest = ...`, one path per
//! line). Those three shapes resolve at registration into an explicit, ordered
//! file manifest that is used identically here and by the fq-exec scan, so
//! metadata and execution never disagree. A multi-file table merges its files'
//! schemas by column name (a column absent from a file reads as NULL there) and
//! folds their footer row counts and min/max/null statistics once.
//!
//! Everything here reads FOOTERS ONLY - the Parquet metadata block at the tail of
//! each file - so it is O(metadata), never a data scan: the schema, the exact row
//! count, and the row-group min/max/null-count statistics all live in the footer.
//! The row DATA is read by the exec data plane (fq-exec's DataFusion parquet
//! scan), which applies projection and filter (row-group pruning) natively.
//!
//! A Parquet source is a FILE source: it is not a JOIN target (a cross-table
//! join has no server to run on and executes in the coordinator) and not a SHIP
//! target (the files are read-only), so those two capabilities are absent. Every
//! single-relation shape the engine pushes (filters, projections, aggregates,
//! windows, DISTINCT, ORDER BY, LIMIT) is executed by DataFusion, so those
//! capabilities are present and honest.

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use fq_catalog::{
    build_column_statistics, map_native_type_default, CatalogError, ColumnMetadata,
    ColumnStatistics, DataSource, DataSourceCapability, RenderDialect, StatValue, TableMetadata,
    TableStatistics,
};
use fq_common::DataType;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics as PqStatistics;

/// The one schema every Parquet file is exposed under. It matches the schema the
/// fq-exec reader registers the files in, so a rendered `FROM main.<table>`
/// resolves against DataFusion at fetch time.
const PARQUET_SCHEMA: &str = "main";

/// One logical table: its resolved, ordered Parquet files. A single-file table
/// holds one path; a multi-file table (`glob`/`files`/`manifest`) holds the
/// resolved manifest in declaration order.
#[derive(Debug)]
struct ParquetTable {
    files: Vec<PathBuf>,
}

/// The metadata of one table, folded once from its files' footers and reused by
/// every later metadata/statistics read so planning stays O(columns), never
/// O(files).
#[derive(Debug)]
struct TableAggregate {
    /// The files' schemas merged by column name; the exec plane is handed this
    /// exact schema, so catalog and execution cannot disagree.
    arrow_schema: SchemaRef,
    /// Each merged column's engine-facing metadata.
    columns: Vec<ColumnMetadata>,
    /// The exact sum of the files' footer row counts.
    row_count: i64,
    /// The exact sum of the files' byte lengths (not a row-count estimate).
    size_bytes: i64,
    /// Per merged column, the folded min/max/null statistics. A column absent
    /// here is an honest unknown.
    stats: BTreeMap<String, ColumnStatistics>,
}

/// The running fold of one column across the files processed so far.
#[derive(Debug)]
struct RunningColumn {
    /// Total nulls so far, counting every row of a file that lacks the column
    /// (union-by-name makes those NULL).
    null_count: i64,
    min: Option<StatValue>,
    max: Option<StatValue>,
    /// False once a file that HAS the column carries no footer statistics for
    /// it: the null fraction would then be understated, so the aggregate is
    /// omitted as an honest unknown.
    complete: bool,
    /// How many files carry the column. Fewer than the table's file count means
    /// some file lacks it, so the merged field must be nullable (union by name
    /// makes those rows NULL).
    present_in: usize,
}

/// One file's folded footer statistics for one column.
#[derive(Debug)]
struct FileColumnStats {
    null_count: i64,
    min: Option<StatValue>,
    max: Option<StatValue>,
}

/// A directory of `<table>.parquet` files, or one logical table over many files,
/// federated as one read-only source.
#[derive(Debug)]
pub struct ParquetSource {
    name: String,
    /// table name -> its resolved file manifest, discovered once at construction
    /// so a metadata/statistics read opens the files directly.
    tables: BTreeMap<String, ParquetTable>,
    /// table name -> its footer aggregate, computed once and memoized.
    aggregates: Mutex<BTreeMap<String, Arc<TableAggregate>>>,
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
        if root.is_file() {
            if parquet_table_name(root).is_none() {
                return Err(CatalogError::Source(format!(
                    "parquet file '{path}' is not a .parquet file"
                )));
            }
            let table = ParquetTable {
                files: vec![root.to_path_buf()],
            };
            return Ok(Self::from_table(name, table));
        }
        let mut tables = BTreeMap::new();
        let entries = std::fs::read_dir(root).map_err(|error| {
            CatalogError::Source(format!("parquet path '{path}' cannot be read: {error}"))
        })?;
        for entry in entries {
            let child = entry
                .map_err(|error| CatalogError::Source(format!("parquet dir '{path}': {error}")))?
                .path();
            if let Some(table) = parquet_table_name(&child) {
                tables.insert(table, ParquetTable { files: vec![child] });
            }
        }
        if tables.is_empty() {
            return Err(CatalogError::Source(format!(
                "parquet dir '{path}' holds no <table>.parquet files"
            )));
        }
        Ok(Self::from_tables(name, tables))
    }

    /// Open ONE table over an explicit list of Parquet files, named after the
    /// source. Every path must be an existing `.parquet` regular file; a path
    /// that repeats raises rather than double-counting its rows.
    pub fn open_files(name: impl Into<String>, files: &[String]) -> Result<Self, CatalogError> {
        let name = name.into();
        let table = resolve_files(&name, files, None)?;
        Ok(Self::from_table(name, table))
    }

    /// Open ONE table over every file matched by `pattern`, named after the
    /// source. Recursive `**` patterns are supported. Matches are sorted
    /// lexicographically so the merged field order and the manifest are
    /// deterministic across filesystems.
    pub fn open_glob(name: impl Into<String>, pattern: &str) -> Result<Self, CatalogError> {
        let name = name.into();
        let mut matches = Vec::new();
        let entries = glob::glob(pattern)
            .map_err(|error| CatalogError::Source(format!("parquet glob '{pattern}': {error}")))?;
        for entry in entries {
            let path = entry.map_err(|error| {
                CatalogError::Source(format!("parquet glob '{pattern}': {error}"))
            })?;
            matches.push(path.to_string_lossy().to_string());
        }
        if matches.is_empty() {
            return Err(CatalogError::Source(format!(
                "parquet glob '{pattern}' matched no files"
            )));
        }
        matches.sort();
        let table = resolve_files(&name, &matches, None)?;
        Ok(Self::from_table(name, table))
    }

    /// Open ONE table over the paths listed in a manifest text file, named after
    /// the source. Blank lines and lines whose first non-space character is `#`
    /// are ignored; a relative path resolves against the manifest's directory.
    pub fn open_manifest(name: impl Into<String>, manifest: &str) -> Result<Self, CatalogError> {
        let name = name.into();
        let path = Path::new(manifest);
        let text = std::fs::read_to_string(path).map_err(|error| {
            CatalogError::Source(format!("parquet manifest '{manifest}': {error}"))
        })?;
        let raw = manifest_entries(&text);
        let table = resolve_files(&name, &raw, path.parent())?;
        Ok(Self::from_table(name, table))
    }

    /// Build a source around one resolved table.
    fn from_table(name: String, table: ParquetTable) -> Self {
        let mut tables = BTreeMap::new();
        tables.insert(name.clone(), table);
        Self::from_tables(name, tables)
    }

    /// Build a source around a resolved table set.
    fn from_tables(name: String, tables: BTreeMap<String, ParquetTable>) -> Self {
        Self {
            name,
            tables,
            aggregates: Mutex::new(BTreeMap::new()),
        }
    }

    /// The table entry, raising if this source has no such table (a scan over an
    /// unknown table must fail loud, not read nothing).
    fn table(&self, table: &str) -> Result<&ParquetTable, CatalogError> {
        self.tables.get(table).ok_or_else(|| {
            CatalogError::Source(format!(
                "parquet source '{}' has no table '{table}'",
                self.name
            ))
        })
    }

    /// The table's memoized footer aggregate, computing it once on first use.
    fn aggregate(&self, table: &str) -> Result<Arc<TableAggregate>, CatalogError> {
        if let Some(found) = self.aggregates.lock().unwrap().get(table) {
            return Ok(Arc::clone(found));
        }
        let computed = Arc::new(self.compute_aggregate(table)?);
        self.aggregates
            .lock()
            .unwrap()
            .insert(table.to_string(), Arc::clone(&computed));
        Ok(computed)
    }

    /// Fold every file's footer into one aggregate: the schema merged by name,
    /// the summed row count and byte size, and the per-column min/max/null
    /// statistics. Reads metadata only, never row data.
    fn compute_aggregate(&self, table: &str) -> Result<TableAggregate, CatalogError> {
        let entry = self.table(table)?;
        let mut merged = ArrowSchema::empty();
        let mut running: BTreeMap<String, RunningColumn> = BTreeMap::new();
        let mut row_count = 0i64;
        let mut size_bytes = 0i64;
        for path in &entry.files {
            let reader = open_reader(path, table)?;
            let file_meta = reader.metadata().file_metadata();
            let schema =
                parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
                    .map_err(|error| {
                    CatalogError::Source(format!(
                        "parquet arrow schema '{table}' ({}): {error}",
                        path.display()
                    ))
                })?;
            let file_rows = file_meta.num_rows();
            fold_file(&mut running, &schema, &reader, row_count, file_rows);
            merged = merge_schema(merged, schema, table)?;
            row_count = row_count.saturating_add(file_rows);
            size_bytes = size_bytes.saturating_add(file_size(path, table)?);
        }
        let merged = finalize_schema(&merged, &running, entry.files.len());
        let columns = merged_columns(&merged)?;
        let stats = finished_stats(&running, row_count);
        Ok(TableAggregate {
            arrow_schema: Arc::new(merged),
            columns,
            row_count,
            size_bytes,
            stats,
        })
    }

    /// Compute and memoize every table's aggregate. The runtime calls this at
    /// registration so the O(files) footer fold happens before the first query,
    /// never inside a plan's budget.
    pub fn ensure_aggregates(&self) -> Result<(), CatalogError> {
        for name in self.tables.keys() {
            self.aggregate(name)?;
        }
        Ok(())
    }

    /// Every table name this source exposes.
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// One table's resolved file paths, in manifest order, for exec registration.
    pub fn table_files(&self, table: &str) -> Result<Vec<String>, CatalogError> {
        let mut files = Vec::with_capacity(self.table(table)?.files.len());
        for path in &self.table(table)?.files {
            files.push(path.to_string_lossy().to_string());
        }
        Ok(files)
    }

    /// One table's merged Arrow schema, for exec registration. Identical to the
    /// schema this tier reports as metadata, so the two tiers cannot disagree.
    pub fn table_arrow_schema(&self, table: &str) -> Result<SchemaRef, CatalogError> {
        Ok(Arc::clone(&self.aggregate(table)?.arrow_schema))
    }
}

/// Resolve and validate a declared list of Parquet paths into a table. A
/// relative path resolves against `base` (the manifest's directory); a path that
/// is empty, does not exist, is not a regular file, is not `.parquet`, or
/// repeats raises.
fn resolve_files(
    table: &str,
    raw: &[String],
    base: Option<&Path>,
) -> Result<ParquetTable, CatalogError> {
    if raw.is_empty() {
        return Err(CatalogError::Source(format!(
            "parquet table '{table}' names no files"
        )));
    }
    let mut files = Vec::with_capacity(raw.len());
    let mut seen = HashSet::new();
    for entry in raw {
        let path = resolve_one(table, entry, base)?;
        if !seen.insert(path.clone()) {
            return Err(CatalogError::Source(format!(
                "parquet table '{table}' names '{}' twice",
                path.display()
            )));
        }
        files.push(path);
    }
    Ok(ParquetTable { files })
}

/// Resolve one declared path: trim, resolve a relative path against `base`, and
/// require an existing `.parquet` regular file.
fn resolve_one(table: &str, entry: &str, base: Option<&Path>) -> Result<PathBuf, CatalogError> {
    let trimmed = entry.trim();
    if trimmed.is_empty() {
        return Err(CatalogError::Source(format!(
            "parquet table '{table}' has an empty file path"
        )));
    }
    let declared = PathBuf::from(trimmed);
    let path = match base {
        Some(base) if declared.is_relative() => base.join(declared),
        _ => declared,
    };
    if parquet_table_name(&path).is_none() {
        return Err(CatalogError::Source(format!(
            "parquet table '{table}' path '{}' is not a .parquet file",
            path.display()
        )));
    }
    if !path.is_file() {
        return Err(CatalogError::Source(format!(
            "parquet table '{table}' path '{}' is missing or not a regular file",
            path.display()
        )));
    }
    Ok(path)
}

/// The non-blank, non-comment lines of a manifest, in order.
fn manifest_entries(text: &str) -> Vec<String> {
    let mut entries = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        entries.push(trimmed.to_string());
    }
    entries
}

/// Merge a file's schema into the accumulated one by column name. A type
/// conflict for one name raises naming the table.
fn merge_schema(
    existing: ArrowSchema,
    incoming: ArrowSchema,
    table: &str,
) -> Result<ArrowSchema, CatalogError> {
    ArrowSchema::try_merge([existing, incoming]).map_err(|error| {
        CatalogError::Source(format!(
            "parquet table '{table}' files have schemas that cannot be merged by name: {error}"
        ))
    })
}

/// Fold one file's columns into the running per-column aggregate. A known column
/// absent from this file gets this file's rows as NULLs (union by name); a column
/// first seen here starts with every earlier file's rows as NULLs.
fn fold_file(
    running: &mut BTreeMap<String, RunningColumn>,
    schema: &ArrowSchema,
    reader: &SerializedFileReader<File>,
    rows_before: i64,
    file_rows: i64,
) {
    for (name, column) in running.iter_mut() {
        if schema.field_with_name(name).is_err() {
            column.null_count = column.null_count.saturating_add(file_rows);
        }
    }
    for field in schema.fields() {
        let stats = file_column_stats(reader, field.name());
        if let Some(column) = running.get_mut(field.name()) {
            column.present_in += 1;
            fold_file_stats(column, stats);
            continue;
        }
        let mut column = RunningColumn {
            null_count: rows_before,
            min: None,
            max: None,
            complete: true,
            present_in: 1,
        };
        fold_file_stats(&mut column, stats);
        running.insert(field.name().clone(), column);
    }
}

/// Fold one file's stats into a running column: add its null count and widen the
/// min/max. No stats for the column marks the aggregate incomplete (honest
/// unknown), since its nulls would then be understated.
fn fold_file_stats(column: &mut RunningColumn, stats: Option<FileColumnStats>) {
    let Some(stats) = stats else {
        column.complete = false;
        return;
    };
    column.null_count = column.null_count.saturating_add(stats.null_count);
    fold_min(&mut column.min, stats.min);
    fold_max(&mut column.max, stats.max);
}

/// Force a merged field to be nullable when any file lacks it. Union by name
/// makes those rows NULL, so a non-nullable marker would be a lie and would make
/// DataFusion reject the NULL fill. Field order, types, and metadata are kept.
fn finalize_schema(
    merged: &ArrowSchema,
    running: &BTreeMap<String, RunningColumn>,
    file_count: usize,
) -> ArrowSchema {
    let mut fields: Vec<Arc<ArrowField>> = Vec::with_capacity(merged.fields().len());
    for field in merged.fields() {
        let absent = running
            .get(field.name())
            .is_none_or(|column| column.present_in < file_count);
        if absent && !field.is_nullable() {
            fields.push(Arc::new(field.as_ref().clone().with_nullable(true)));
        } else {
            fields.push(Arc::clone(field));
        }
    }
    ArrowSchema::new(fields).with_metadata(merged.metadata().clone())
}

/// Build the final per-column statistics, dropping any column whose fold was
/// incomplete. `num_distinct` stays unknown; a null fraction that is known is
/// exact over the summed row count.
fn finished_stats(
    running: &BTreeMap<String, RunningColumn>,
    row_count: i64,
) -> BTreeMap<String, ColumnStatistics> {
    let mut stats = BTreeMap::new();
    for (name, column) in running {
        if !column.complete {
            continue;
        }
        stats.insert(
            name.clone(),
            build_column_statistics(
                None,
                column.null_count,
                row_count,
                column.min.clone(),
                column.max.clone(),
            ),
        );
    }
    stats
}

/// The engine-facing metadata of every merged field, in merged order. An Arrow
/// type the engine does not model raises (never coerced to a string).
fn merged_columns(schema: &ArrowSchema) -> Result<Vec<ColumnMetadata>, CatalogError> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.push(column_metadata(field)?);
    }
    Ok(columns)
}

/// One table file's byte length.
fn file_size(path: &Path, table: &str) -> Result<i64, CatalogError> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        CatalogError::Source(format!(
            "stat parquet '{table}' ({}): {error}",
            path.display()
        ))
    })?;
    i64::try_from(metadata.len()).map_err(|_| {
        CatalogError::Source(format!(
            "parquet file '{}' is too large to size",
            path.display()
        ))
    })
}

/// Open one file's footer reader.
fn open_reader(path: &Path, table: &str) -> Result<SerializedFileReader<File>, CatalogError> {
    let file = File::open(path).map_err(|error| {
        CatalogError::Source(format!(
            "open parquet '{table}' ({}): {error}",
            path.display()
        ))
    })?;
    SerializedFileReader::new(file).map_err(|error| {
        CatalogError::Source(format!(
            "read parquet footer '{table}' ({}): {error}",
            path.display()
        ))
    })
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
    /// coordinator), no SHIP_TARGET (the files are read-only), and no
    /// INTERSECT_EXCEPT_ALL - DataFusion lowers a bare INTERSECT ALL / EXCEPT ALL
    /// to a semi / anti join, which keeps every matching / non-matching left row
    /// instead of the per-row minimum / difference the multiset semantics
    /// require, so those two operations stay in the coordinator.
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

    /// Column names + engine types + the exact row count, all from the footers.
    /// An Arrow type the engine does not model raises (never coerced to a string).
    fn get_table_metadata(
        &self,
        _schema: &str,
        table: &str,
    ) -> Result<TableMetadata, CatalogError> {
        let aggregate = self.aggregate(table)?;
        Ok(TableMetadata {
            schema_name: PARQUET_SCHEMA.to_string(),
            table_name: table.to_string(),
            columns: aggregate.columns.clone(),
            row_count: Some(aggregate.row_count),
            size_bytes: Some(aggregate.size_bytes),
        })
    }

    /// The exact row count (summed footers) plus, for each requested column that
    /// carries row-group statistics in every file that has it, its aggregated
    /// min/max and null fraction. Unknown values stay absent - never fabricated.
    fn get_table_statistics(
        &self,
        _schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        let aggregate = self.aggregate(table)?;
        let mut column_stats = BTreeMap::new();
        for column in columns {
            if let Some(stats) = aggregate.stats.get(column) {
                column_stats.insert(column.clone(), stats.clone());
            }
        }
        Ok(Some(TableStatistics {
            row_count: Some(aggregate.row_count),
            total_size_bytes: aggregate.size_bytes,
            column_stats,
        }))
    }

    /// Map an Arrow-derived native type name to an engine type through the default
    /// mapping. The names `get_table_metadata` produces are the canonical SQL type
    /// names the default handles.
    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        map_native_type_default(type_str)
    }

    /// The version token of a SINGLE-file table: its `(size, mtime_ns)` stamp.
    /// Parquet files are immutable - a change replaces the file - so any change
    /// moves the stamp. A MULTI-file table (`glob`/`files`/`manifest`) returns
    /// `None`: an exact per-file list would be too long to store and compare for
    /// hundreds or thousands of files, and `None` is the honest "cannot answer"
    /// that makes an explicit REFRESH always pull. The file vanishing raises for
    /// the single-file case: the manifest was resolved at open, so a missing file
    /// is a source that changed shape under the engine.
    fn source_token(&self, _schema: &str, table: &str) -> Result<Option<String>, CatalogError> {
        let files = &self.table(table)?.files;
        if files.len() != 1 {
            return Ok(None);
        }
        file_stamp(&files[0], table).map(Some)
    }
}

/// The `(size, mtime_ns)` stamp of one Parquet file, the single-file freshness
/// token. Raises if the file no longer exists.
fn file_stamp(path: &Path, table: &str) -> Result<String, CatalogError> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        CatalogError::Source(format!(
            "stat parquet '{table}' ({}): {error}",
            path.display()
        ))
    })?;
    let mtime_ns = metadata
        .modified()
        .map_err(|error| {
            CatalogError::Source(format!(
                "mtime of parquet '{table}' ({}): {error}",
                path.display()
            ))
        })?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            CatalogError::Source(format!(
                "mtime of parquet '{table}' ({}): {error}",
                path.display()
            ))
        })?
        .as_nanos();
    Ok(format!(
        "parquet-file:size={},mtime_ns={mtime_ns}",
        metadata.len()
    ))
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
/// `map_native_type_default` understands. Recursive fields retain Arrow metadata;
/// a type with no engine modeling raises rather than being coerced.
fn arrow_type_native_name(data_type: &ArrowDataType) -> Result<String, CatalogError> {
    if matches!(
        data_type,
        ArrowDataType::List(_)
            | ArrowDataType::LargeList(_)
            | ArrowDataType::FixedSizeList(_, _)
            | ArrowDataType::Struct(_)
            | ArrowDataType::Map(_, _)
    ) {
        DataType::validate_arrow(data_type).map_err(CatalogError::UnsupportedColumnType)?;
        return Ok(DataType::Arrow(data_type.clone()).value());
    }
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

/// Fold every row group of one file's column into `FileColumnStats`, or `None`
/// when no row group carries statistics for the column. A null count that
/// overflows i64 saturates so the derived null fraction can never go negative.
fn file_column_stats(reader: &SerializedFileReader<File>, column: &str) -> Option<FileColumnStats> {
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
        let group_nulls = i64::try_from(stats.null_count_opt().unwrap_or(0)).unwrap_or(i64::MAX);
        null_count = null_count.saturating_add(group_nulls);
        fold_min(&mut min, stat_min(stats));
        fold_max(&mut max, stat_max(stats));
    }
    if !seen {
        return None;
    }
    Some(FileColumnStats {
        null_count,
        min,
        max,
    })
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
        // Every integer width maps to INTEGER except 64-bit, which maps to
        // BIGINT; the default type mapping then turns those names into engine
        // types.
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

    #[test]
    fn manifest_entries_skip_blanks_and_comments() {
        // Blank lines and `#` comments are dropped; surrounding whitespace is
        // trimmed; order is preserved.
        let entries = manifest_entries("# header\n\na.parquet\n  b.parquet  \n\n# tail\n");
        assert_eq!(
            entries,
            vec!["a.parquet".to_string(), "b.parquet".to_string()]
        );
    }

    #[test]
    fn a_non_parquet_path_in_a_list_raises() {
        // A `.csv` in a parquet list is a misconfiguration, not a skipped file.
        let error = resolve_files("t", &["/x/a.csv".to_string()], None).unwrap_err();
        assert!(format!("{error}").contains("not a .parquet"), "{error}");
    }
}
