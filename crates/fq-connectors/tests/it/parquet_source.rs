//! The Parquet connector against small real Parquet files WRITTEN by the test
//! (via the parquet arrow writer): footer-only metadata, exact row counts, and
//! row-group min/max/null statistics, plus the loud-failure paths.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use fq_catalog::{Catalog, DataSource, DataSourceCapability, RenderDialect, StatValue};
use fq_common::DataType;
use fq_connectors::ParquetSource;
use parquet::arrow::ArrowWriter;

/// A unique empty temp directory for one test (no cross-test file contention).
fn temp_dir(tag: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fq_parquet_{tag}_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Write `batch` as `<dir>/<table>.parquet` (default writer properties, so
/// row-group statistics are recorded in the footer).
fn write_parquet(dir: &Path, table: &str, batch: &RecordBatch) {
    let path = dir.join(format!("{table}.parquet"));
    let file = File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

/// The region fixture: five rows, an INTEGER key, a VARCHAR name, a DOUBLE
/// measure. The key runs 0..=4, the measure 10.0..=50.0 - the assertions read
/// those exact bounds back out of the footer statistics.
fn region_batch() -> RecordBatch {
    let key: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4]));
    let name: ArrayRef = Arc::new(StringArray::from(vec![
        "AFRICA",
        "AMERICA",
        "ASIA",
        "EUROPE",
        "MIDDLE EAST",
    ]));
    let measure: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]));
    let schema = Schema::new(vec![
        Field::new("r_regionkey", ArrowDataType::Int32, false),
        Field::new("r_name", ArrowDataType::Utf8, false),
        Field::new("r_measure", ArrowDataType::Float64, false),
    ]);
    RecordBatch::try_new(Arc::new(schema), vec![key, name, measure]).expect("region batch")
}

#[test]
fn render_dialect_is_parquet_so_datafusion_semantics_hold() {
    // Parquet SQL runs on the exec plane's DataFusion, so it renders the Parquet
    // dialect (DataFusion semantics), not another engine whose defaults DataFusion
    // does not share (a DESC ORDER BY's null placement diverges from DuckDB).
    let dir = temp_dir("dialect");
    write_parquet(&dir, "region", &region_batch());
    let source = ParquetSource::open("pq", dir.to_str().unwrap()).expect("open");
    assert_eq!(source.render_dialect(), RenderDialect::Parquet);
}

#[test]
fn capabilities_exclude_joins_and_ship_target() {
    let dir = temp_dir("caps");
    write_parquet(&dir, "region", &region_batch());
    let source = ParquetSource::open("pq", dir.to_str().unwrap()).expect("open");
    let capabilities = source.capabilities();
    // A file source runs single-relation shapes through DataFusion.
    assert!(source.supports_capability(DataSourceCapability::Aggregations));
    assert!(source.supports_capability(DataSourceCapability::OrderBy));
    // It is neither a join target (cross-table joins run in the coordinator) nor a
    // ship target (read-only files).
    assert!(!capabilities.contains(&DataSourceCapability::Joins));
    assert!(!capabilities.contains(&DataSourceCapability::ShipTarget));
}

#[test]
fn lists_the_single_main_schema_and_its_tables() {
    let dir = temp_dir("list");
    write_parquet(&dir, "region", &region_batch());
    write_parquet(&dir, "nation", &region_batch());
    let source = ParquetSource::open("pq", dir.to_str().unwrap()).expect("open");
    assert_eq!(source.list_schemas().unwrap(), vec!["main".to_string()]);
    // Sorted, from the BTreeMap.
    assert_eq!(
        source.list_tables("main").unwrap(),
        vec!["nation".to_string(), "region".to_string()]
    );
    // A schema this source does not expose holds no tables.
    assert!(source.list_tables("public").unwrap().is_empty());
}

#[test]
fn metadata_maps_footer_schema_to_engine_types_with_exact_row_count() {
    let dir = temp_dir("meta");
    write_parquet(&dir, "region", &region_batch());
    let source = ParquetSource::open("pq", dir.to_str().unwrap()).expect("open");
    let metadata = source
        .get_table_metadata("main", "region")
        .expect("metadata");
    assert_eq!(metadata.row_count, Some(5));
    let names: Vec<&str> = metadata.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["r_regionkey", "r_name", "r_measure"]);
    // The native names map through the source to engine types.
    assert_eq!(
        source
            .map_native_type(&metadata.columns[0].data_type)
            .unwrap(),
        DataType::Integer
    );
    assert_eq!(
        source
            .map_native_type(&metadata.columns[1].data_type)
            .unwrap(),
        DataType::Varchar
    );
    assert_eq!(
        source
            .map_native_type(&metadata.columns[2].data_type)
            .unwrap(),
        DataType::Double
    );
}

#[test]
fn statistics_carry_exact_rows_and_row_group_bounds() {
    let dir = temp_dir("stats");
    write_parquet(&dir, "region", &region_batch());
    let source = ParquetSource::open("pq", dir.to_str().unwrap()).expect("open");
    let stats = source
        .get_table_statistics(
            "main",
            "region",
            &["r_regionkey".to_string(), "r_name".to_string()],
        )
        .expect("stats")
        .expect("some stats");
    assert_eq!(stats.row_count, Some(5));
    let key = &stats.column_stats["r_regionkey"];
    assert_eq!(key.min_value, Some(StatValue::Integer(0)));
    assert_eq!(key.max_value, Some(StatValue::Integer(4)));
    // No nulls in the fixture: the fraction is exactly zero.
    assert!(key.null_fraction.abs() < f64::EPSILON);
    let name = &stats.column_stats["r_name"];
    assert_eq!(name.min_value, Some(StatValue::Text("AFRICA".to_string())));
    assert_eq!(
        name.max_value,
        Some(StatValue::Text("MIDDLE EAST".to_string()))
    );
}

#[test]
fn statistics_with_no_columns_return_the_row_count_only() {
    let dir = temp_dir("rowsonly");
    write_parquet(&dir, "region", &region_batch());
    let source = ParquetSource::open("pq", dir.to_str().unwrap()).expect("open");
    let stats = source
        .get_table_statistics("main", "region", &[])
        .expect("stats")
        .expect("some stats");
    assert_eq!(stats.row_count, Some(5));
    assert!(stats.column_stats.is_empty());
}

#[test]
fn a_parquet_source_loads_into_the_catalog_schema_tree() {
    let dir = temp_dir("catalog");
    write_parquet(&dir, "region", &region_batch());
    let mut catalog = Catalog::new();
    catalog.register_datasource(Arc::new(
        ParquetSource::open("pq", dir.to_str().unwrap()).expect("open"),
    ));
    catalog.load_metadata().expect("load metadata");
    let table = catalog
        .get_table("pq", "main", "region")
        .expect("table in catalog");
    assert_eq!(table.columns.len(), 3);
    assert_eq!(table.columns[0].data_type, DataType::Integer);
}

#[test]
fn an_unmodeled_arrow_column_type_raises_at_metadata() {
    // A binary column has no engine modeling; get_table_metadata must raise rather
    // than coerce it, so a mistyped column fails loud at catalog load.
    let dir = temp_dir("binary");
    let data: ArrayRef = Arc::new(arrow::array::BinaryArray::from(vec![
        b"a".as_ref(),
        b"bb".as_ref(),
    ]));
    let schema = Schema::new(vec![Field::new("blob", ArrowDataType::Binary, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![data]).expect("binary batch");
    write_parquet(&dir, "blobs", &batch);
    let source = ParquetSource::open("pq", dir.to_str().unwrap()).expect("open");
    let error = source
        .get_table_metadata("main", "blobs")
        .expect_err("binary must raise");
    assert!(
        format!("{error}").contains("Unsupported column type"),
        "unexpected error: {error}"
    );
}

#[test]
fn an_unknown_table_raises() {
    let dir = temp_dir("unknown");
    write_parquet(&dir, "region", &region_batch());
    let source = ParquetSource::open("pq", dir.to_str().unwrap()).expect("open");
    assert!(source.get_table_metadata("main", "nope").is_err());
}

#[test]
fn a_directory_with_no_parquet_files_raises() {
    let dir = temp_dir("empty");
    let error = ParquetSource::open("pq", dir.to_str().unwrap())
        .expect_err("an empty parquet dir is a misconfiguration");
    assert!(
        format!("{error}").contains("no <table>.parquet"),
        "unexpected error: {error}"
    );
}

#[test]
fn a_missing_directory_raises() {
    assert!(ParquetSource::open("pq", "/nonexistent/parquet/dir/xyz").is_err());
}

#[test]
fn source_token_stamps_the_table_file_and_moves_when_it_is_replaced() {
    let dir = temp_dir("token");
    write_parquet(&dir, "region", &region_batch());
    let source = ParquetSource::open("pq", dir.to_str().expect("utf-8")).expect("open");

    let first = source
        .source_token("main", "region")
        .expect("token")
        .expect("a parquet table has a token");
    assert_eq!(
        first,
        source
            .source_token("main", "region")
            .expect("token")
            .expect("token"),
        "no change, same token"
    );

    // Replacing the file (the only way parquet data changes) moves the stamp.
    // The replacement carries an extra row group so its SIZE differs; the test
    // does not lean on filesystem mtime granularity.
    let path = dir.join("region.parquet");
    let file = File::create(&path).expect("recreate parquet file");
    let batch = region_batch();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.write(&batch).expect("write batch again");
    writer.close().expect("close writer");
    let replaced = source
        .source_token("main", "region")
        .expect("token")
        .expect("token");
    assert_ne!(first, replaced);

    // A table this source never listed raises loudly.
    assert!(source.source_token("main", "ghost").is_err());
}

#[test]
fn a_single_file_source_names_its_table_after_the_datasource() {
    let dir = temp_dir("singlefile");
    write_parquet(&dir, "region", &region_batch());
    let file = dir.join("region.parquet");
    let source = ParquetSource::open("ev", file.to_str().unwrap()).expect("open");
    assert_eq!(source.list_tables("main").unwrap(), vec!["ev".to_string()]);
    let metadata = source.get_table_metadata("main", "ev").expect("metadata");
    assert_eq!(metadata.row_count, Some(5));

    // A single-file source pointed at a non-parquet file raises.
    let bogus = dir.join("notes.txt");
    std::fs::write(&bogus, "x").expect("write bogus file");
    assert!(ParquetSource::open("ev", bogus.to_str().unwrap()).is_err());
}

/// A batch with an int key and an optional name column, for schema-evolution
/// tests where one file lacks a column.
fn key_batch(keys: Vec<i32>, names: Option<Vec<String>>) -> RecordBatch {
    let key: ArrayRef = Arc::new(Int32Array::from(keys));
    let mut fields = vec![Field::new("k", ArrowDataType::Int32, false)];
    let mut arrays: Vec<ArrayRef> = vec![key];
    if let Some(values) = names {
        fields.push(Field::new("name", ArrowDataType::Utf8, true));
        arrays.push(Arc::new(StringArray::from(values)));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("key batch")
}

#[test]
fn a_glob_exposes_one_table_over_matching_files() {
    let dir = temp_dir("glob");
    write_parquet(&dir, "part-0", &region_batch());
    write_parquet(&dir, "part-1", &region_batch());
    let pattern = dir.join("part-*.parquet");
    let source = ParquetSource::open_glob("events", pattern.to_str().unwrap()).expect("open");
    // ONE table named after the datasource, not one table per file.
    assert_eq!(
        source.list_tables("main").unwrap(),
        vec!["events".to_string()]
    );
    let metadata = source
        .get_table_metadata("main", "events")
        .expect("metadata");
    // Ten rows across two five-row files, exact from the footers.
    assert_eq!(metadata.row_count, Some(10));
    assert_eq!(metadata.columns.len(), 3);
}

#[test]
fn a_files_list_exposes_one_table_over_the_named_files() {
    let dir = temp_dir("files");
    write_parquet(&dir, "left", &region_batch());
    write_parquet(&dir, "right", &region_batch());
    let files = vec![
        dir.join("left.parquet").to_string_lossy().to_string(),
        dir.join("right.parquet").to_string_lossy().to_string(),
    ];
    let source = ParquetSource::open_files("both", &files).expect("open");
    assert_eq!(
        source.list_tables("main").unwrap(),
        vec!["both".to_string()]
    );
    assert_eq!(
        source
            .get_table_metadata("main", "both")
            .expect("metadata")
            .row_count,
        Some(10)
    );
}

#[test]
fn a_manifest_reads_relative_paths_and_ignores_comments() {
    let dir = temp_dir("manifest");
    write_parquet(&dir, "a", &region_batch());
    write_parquet(&dir, "b", &region_batch());
    let manifest = dir.join("list.txt");
    std::fs::write(&manifest, "# comment\n\na.parquet\nb.parquet\n").expect("write manifest");
    let source = ParquetSource::open_manifest("m", manifest.to_str().unwrap()).expect("open");
    assert_eq!(
        source
            .get_table_metadata("main", "m")
            .expect("metadata")
            .row_count,
        Some(10)
    );
}

#[test]
fn union_by_name_fills_a_missing_column_with_nulls() {
    let dir = temp_dir("union");
    write_parquet(
        &dir,
        "with_name",
        &key_batch(vec![1, 2], Some(vec!["a".to_string(), "b".to_string()])),
    );
    write_parquet(&dir, "no_name", &key_batch(vec![3, 4, 5], None));
    let files = vec![
        dir.join("with_name.parquet").to_string_lossy().to_string(),
        dir.join("no_name.parquet").to_string_lossy().to_string(),
    ];
    let source = ParquetSource::open_files("u", &files).expect("open");
    let metadata = source.get_table_metadata("main", "u").expect("metadata");
    let names: Vec<&str> = metadata.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["k", "name"]);
    assert_eq!(metadata.row_count, Some(5));
    // The 3 rows of the file without `name` are NULL there: 3 of 5 rows.
    let stats = source
        .get_table_statistics("main", "u", &["name".to_string()])
        .expect("stats")
        .expect("some stats");
    let name = &stats.column_stats["name"];
    assert!(
        (name.null_fraction - 0.6).abs() < 1e-9,
        "null fraction was {}",
        name.null_fraction
    );
}

#[test]
fn incompatible_types_across_files_raise() {
    let dir = temp_dir("badtype");
    write_parquet(&dir, "int_key", &key_batch(vec![1], None));
    // A Utf8 column named `k` cannot merge with the Int32 `k`.
    let utf8: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
    let schema = Schema::new(vec![Field::new("k", ArrowDataType::Utf8, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![utf8]).expect("utf8 batch");
    write_parquet(&dir, "str_key", &batch);
    let files = vec![
        dir.join("int_key.parquet").to_string_lossy().to_string(),
        dir.join("str_key.parquet").to_string_lossy().to_string(),
    ];
    let source = ParquetSource::open_files("bad", &files).expect("open");
    let error = source
        .get_table_metadata("main", "bad")
        .expect_err("type conflict must raise");
    assert!(format!("{error}").contains("merge"), "{error}");
}

#[test]
fn a_missing_listed_file_raises_at_open() {
    let dir = temp_dir("missing_listed");
    let files = vec![dir.join("ghost.parquet").to_string_lossy().to_string()];
    assert!(ParquetSource::open_files("m", &files).is_err());
}

#[test]
fn a_duplicate_listed_file_raises() {
    let dir = temp_dir("dup");
    write_parquet(&dir, "a", &region_batch());
    let path = dir.join("a.parquet").to_string_lossy().to_string();
    let files = vec![path.clone(), path];
    let error = ParquetSource::open_files("d", &files).expect_err("duplicate must raise");
    assert!(format!("{error}").contains("twice"), "{error}");
}

#[test]
fn a_glob_matching_nothing_raises() {
    let dir = temp_dir("nomatch");
    let pattern = dir.join("nothing-*.parquet");
    assert!(ParquetSource::open_glob("g", pattern.to_str().unwrap()).is_err());
}

#[test]
fn multi_file_statistics_fold_min_max_across_files() {
    let dir = temp_dir("fold");
    write_parquet(&dir, "lo", &region_batch());
    // A second file with keys 10..14, so the fold's max is 14 from it.
    let key: ArrayRef = Arc::new(Int32Array::from(vec![10, 11, 12, 13, 14]));
    let name: ArrayRef = Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E"]));
    let measure: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    let schema = Schema::new(vec![
        Field::new("r_regionkey", ArrowDataType::Int32, false),
        Field::new("r_name", ArrowDataType::Utf8, false),
        Field::new("r_measure", ArrowDataType::Float64, false),
    ]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![key, name, measure]).expect("hi batch");
    write_parquet(&dir, "hi", &batch);
    let files = vec![
        dir.join("lo.parquet").to_string_lossy().to_string(),
        dir.join("hi.parquet").to_string_lossy().to_string(),
    ];
    let source = ParquetSource::open_files("fold", &files).expect("open");
    let stats = source
        .get_table_statistics("main", "fold", &["r_regionkey".to_string()])
        .expect("stats")
        .expect("some stats");
    assert_eq!(stats.row_count, Some(10));
    let key = &stats.column_stats["r_regionkey"];
    assert_eq!(key.min_value, Some(StatValue::Integer(0)));
    assert_eq!(key.max_value, Some(StatValue::Integer(14)));
}

#[test]
fn a_multi_file_source_token_abstains() {
    let dir = temp_dir("token_multi");
    write_parquet(&dir, "a", &region_batch());
    write_parquet(&dir, "b", &region_batch());
    let files = vec![
        dir.join("a.parquet").to_string_lossy().to_string(),
        dir.join("b.parquet").to_string_lossy().to_string(),
    ];
    let source = ParquetSource::open_files("m", &files).expect("open");
    // A multi-file table cannot name one stamp, so it abstains and REFRESH pulls.
    assert!(source.source_token("main", "m").unwrap().is_none());
}

#[test]
fn a_multi_file_table_loads_into_the_catalog() {
    let dir = temp_dir("catalog_multi");
    write_parquet(&dir, "a", &region_batch());
    write_parquet(&dir, "b", &region_batch());
    let pattern = dir.join("*.parquet");
    let source = ParquetSource::open_glob("ev", pattern.to_str().unwrap()).expect("open");
    let mut catalog = Catalog::new();
    catalog.register_datasource(Arc::new(source));
    catalog.load_metadata().expect("load metadata");
    let table = catalog.get_table("ev", "main", "ev").expect("table");
    assert_eq!(table.columns.len(), 3);
}

#[test]
fn statistics_are_cached_and_do_not_reread_footers() {
    let dir = temp_dir("cached");
    write_parquet(&dir, "a", &region_batch());
    write_parquet(&dir, "b", &region_batch());
    let files = vec![
        dir.join("a.parquet").to_string_lossy().to_string(),
        dir.join("b.parquet").to_string_lossy().to_string(),
    ];
    let source = ParquetSource::open_files("c", &files).expect("open");
    // Fold every footer once, as the runtime does at registration.
    source.ensure_aggregates().expect("aggregate");

    // Remove the files: a metadata/statistics read that re-opened a footer would
    // now fail. Cached answers still come back, proving planning is O(cached
    // metadata), not O(files) footer reads.
    std::fs::remove_file(dir.join("a.parquet")).expect("remove a");
    std::fs::remove_file(dir.join("b.parquet")).expect("remove b");

    let metadata = source.get_table_metadata("main", "c").expect("metadata");
    assert_eq!(metadata.row_count, Some(10));
    let stats = source
        .get_table_statistics("main", "c", &["r_regionkey".to_string()])
        .expect("stats")
        .expect("some stats");
    assert_eq!(stats.row_count, Some(10));
    assert_eq!(
        stats.column_stats["r_regionkey"].min_value,
        Some(StatValue::Integer(0))
    );
}

#[test]
fn a_hundred_and_twenty_eight_files_merge_into_one_table() {
    // A long list must not expand into a plan: the manifest resolves to one
    // table with one summed row count, not 128 tables or a UNION.
    let dir = temp_dir("many");
    for index in 0..128 {
        write_parquet(&dir, &format!("part-{index:03}"), &region_batch());
    }
    let pattern = dir.join("part-*.parquet");
    let source = ParquetSource::open_glob("many", pattern.to_str().unwrap()).expect("open");
    let metadata = source.get_table_metadata("main", "many").expect("metadata");
    assert_eq!(metadata.row_count, Some(128 * 5));
    assert_eq!(
        source.list_tables("main").unwrap(),
        vec!["many".to_string()]
    );
}
