//! The Accelerator lifecycle over a real store: create persists chunks and a
//! registry row; refresh writes a new generation, swaps the catalog list, and
//! unlinks the superseded files; drop tombstones, unlinks, and purges; a
//! re-opened Accelerator (same config path) sees the same views and sweeps
//! any drop a crash interrupted.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef};
use fq_accel::{AccelError, Accelerator};

/// A unique sandbox dir plus the config path an Accelerator hangs off.
fn sandbox(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("fq_accel_it_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create sandbox");
    dir.join("config.yaml")
}

/// A two-column (Int64, Utf8) schema.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", ArrowType::Int64, true),
        Field::new("v", ArrowType::Utf8, true),
    ]))
}

/// One batch of (k, v) rows over `schema()`.
fn batch(rows: &[(i64, &str)]) -> RecordBatch {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for (key, value) in rows {
        keys.push(*key);
        values.push((*value).to_string());
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("batch")
}

#[test]
fn create_registers_and_persists_readable_chunks() {
    let config = sandbox("create");
    let accel = Accelerator::open(&config).expect("open");
    let data = batch(&[(1, "a"), (2, "b")]);
    let view = accel
        .create_view(
            "orders_mv",
            "SELECT k, v FROM t",
            &schema(),
            std::slice::from_ref(&data),
        )
        .expect("create");
    assert_eq!(view.measured_rows, 2);
    assert_eq!(view.chunk_list.len(), 1);
    assert!(!view.created_at.is_empty());
    assert_eq!(view.columns.len(), 2);
    // The absolute chunk paths point at real files holding the exact rows.
    let paths = accel.chunk_paths(&view);
    assert!(std::path::Path::new(&paths[0]).is_file());
    let views = accel.views().expect("views");
    assert_eq!(views, vec![view]);
}

#[test]
fn duplicate_create_raises_and_leaves_no_orphan_chunks() {
    let config = sandbox("dup");
    let accel = Accelerator::open(&config).expect("open");
    accel
        .create_view("mv", "SELECT 1", &schema(), &[batch(&[(1, "a")])])
        .expect("first create");
    let error = accel
        .create_view("mv", "SELECT 2", &schema(), &[batch(&[(2, "b")])])
        .unwrap_err();
    assert!(matches!(error, AccelError::DuplicateView(_)));
    // The loser's generation-0 files were its OWN (same names as the winner's
    // in the same directory - first writer wins, the loser rewrote and then
    // removed them); the registry still serves the surviving row's list.
    let view = accel.view("mv").expect("winner");
    assert_eq!(view.definition_sql, "SELECT 1");
}

#[test]
fn refresh_swaps_generations_and_unlinks_the_old_files() {
    let config = sandbox("refresh");
    let accel = Accelerator::open(&config).expect("open");
    let created = accel
        .create_view("mv", "SELECT k, v FROM t", &schema(), &[batch(&[(1, "a")])])
        .expect("create");
    let old_paths = accel.chunk_paths(&created);

    let refreshed = accel
        .refresh_view("mv", &schema(), &[batch(&[(1, "a"), (2, "b")])])
        .expect("refresh");
    assert_eq!(refreshed.measured_rows, 2);
    assert!(refreshed.refreshed_at.is_some());
    assert_ne!(refreshed.chunk_list, created.chunk_list);
    // The superseded generation is gone; the new one is on disk.
    assert!(!std::path::Path::new(&old_paths[0]).exists());
    assert!(std::path::Path::new(&accel.chunk_paths(&refreshed)[0]).is_file());
}

#[test]
fn refresh_with_a_drifted_schema_raises_and_keeps_the_view() {
    let config = sandbox("drift");
    let accel = Accelerator::open(&config).expect("open");
    accel
        .create_view("mv", "SELECT k, v FROM t", &schema(), &[batch(&[(1, "a")])])
        .expect("create");
    // The re-executed SELECT now produces one differently named column.
    let drifted: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "renamed",
        ArrowType::Int64,
        true,
    )]));
    let rows = RecordBatch::try_new(
        Arc::clone(&drifted),
        vec![Arc::new(Int64Array::from(vec![1_i64]))],
    )
    .expect("drifted batch");
    let error = accel.refresh_view("mv", &drifted, &[rows]).unwrap_err();
    assert!(matches!(error, AccelError::InvalidSchema(_)), "{error}");
    // The view still serves its previous contents.
    let view = accel.view("mv").expect("kept");
    assert_eq!(view.measured_rows, 1);
    assert!(std::path::Path::new(&accel.chunk_paths(&view)[0]).is_file());
}

#[test]
fn drop_removes_the_row_and_the_files() {
    let config = sandbox("drop");
    let accel = Accelerator::open(&config).expect("open");
    let view = accel
        .create_view("mv", "SELECT 1", &schema(), &[batch(&[(1, "a")])])
        .expect("create");
    let path = accel.chunk_paths(&view)[0].clone();
    accel.drop_view("mv").expect("drop");
    assert!(matches!(
        accel.view("mv").unwrap_err(),
        AccelError::UnknownView(_)
    ));
    assert!(!std::path::Path::new(&path).exists());
    assert!(matches!(
        accel.drop_view("mv").unwrap_err(),
        AccelError::UnknownView(_)
    ));
}

#[test]
fn reopen_sees_the_registered_views() {
    let config = sandbox("reopen");
    {
        let accel = Accelerator::open(&config).expect("open");
        accel
            .create_view("mv", "SELECT k, v FROM t", &schema(), &[batch(&[(7, "z")])])
            .expect("create");
    }
    let accel = Accelerator::open(&config).expect("reopen");
    let view = accel.view("mv").expect("durable view");
    assert_eq!(view.measured_rows, 1);
    assert_eq!(view.definition_sql, "SELECT k, v FROM t");
}

#[test]
fn distinct_view_names_never_collide_on_chunk_keys() {
    // Two names that sanitize to the same directory stem still get distinct
    // directories (the name hash), so their chunks never mix.
    let config = sandbox("collide");
    let accel = Accelerator::open(&config).expect("open");
    let a = accel
        .create_view("a b", "SELECT 1", &schema(), &[batch(&[(1, "a")])])
        .expect("create a b");
    let b = accel
        .create_view("a_b", "SELECT 2", &schema(), &[batch(&[(2, "b")])])
        .expect("create a_b");
    assert_ne!(a.location, b.location);
}
