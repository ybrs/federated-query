//! Delta-refresh mechanics over a real store: an append leaves every existing
//! chunk file untouched and extends the list; a primary-key merge rewrites
//! ONLY the chunks holding a changed or deleted key; both publish source
//! tokens and (for append) the watermark state in the same swap; the
//! chunk-derived watermark reads back what the chunks hold.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef};
use fq_accel::{AccelError, Accelerator, ChangeKeyState, Watermark, WatermarkScan};

/// A unique sandbox config path; the Accelerator hangs its store off it. The
/// 1-byte chunk bound makes every batch its own chunk, so small fixtures
/// exercise multi-chunk views.
fn small_chunk_accel(tag: &str) -> Accelerator {
    let dir = std::env::temp_dir().join(format!("fq_accel_delta_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create sandbox");
    let config: PathBuf = dir.join("config.yaml");
    Accelerator::open_with_chunk_bytes(&config, Some(1)).expect("open")
}

/// The (k BIGINT, v TEXT) schema.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", ArrowType::Int64, true),
        Field::new("v", ArrowType::Utf8, true),
    ]))
}

/// One (k, v) batch.
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

/// One token map entry.
fn tokens(value: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert("duck.main.t".to_string(), value.to_string());
    map
}

/// Read every row of a view back through the store, in chunk order.
fn view_rows(accel: &Accelerator, name: &str) -> Vec<(i64, String)> {
    let view = accel.view(name).expect("view");
    let mut rows = Vec::new();
    for path in accel.chunk_paths(&view) {
        let bytes = std::fs::read(&path).expect("read chunk");
        let reader = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)
            .expect("ipc reader");
        for read in reader {
            let read = read.expect("batch");
            let keys = read
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("k");
            let values = read
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("v");
            for row in 0..read.num_rows() {
                rows.push((keys.value(row), values.value(row).to_string()));
            }
        }
    }
    rows
}

/// The mtime of one chunk file (to prove an append never rewrote it).
fn chunk_mtime(accel: &Accelerator, name: &str, chunk_index: usize) -> std::time::SystemTime {
    let view = accel.view(name).expect("view");
    let path = &accel.chunk_paths(&view)[chunk_index];
    std::fs::metadata(path)
        .expect("stat chunk")
        .modified()
        .expect("mtime")
}

#[test]
fn append_extends_the_list_and_never_touches_existing_files() {
    let accel = small_chunk_accel("append");
    let created = accel
        .create_view(
            "mv",
            "SELECT k, v FROM t",
            &schema(),
            &[batch(&[(1, "a")]), batch(&[(2, "b")])],
            tokens("t0"),
            Some(ChangeKeyState {
                column: "k".to_string(),
                watermark: Some(Watermark::Int(2)),
            }),
        )
        .expect("create");
    assert_eq!(
        created.chunk_list,
        vec!["chunk-0-0.arrow".to_string(), "chunk-0-1.arrow".to_string()]
    );
    assert_eq!(created.source_tokens, tokens("t0"));
    let old_mtime = chunk_mtime(&accel, "mv", 0);

    let refreshed = accel
        .refresh_view_append(
            "mv",
            &schema(),
            &[batch(&[(3, "c")])],
            &tokens("t1"),
            &ChangeKeyState {
                column: "k".to_string(),
                watermark: Some(Watermark::Int(3)),
            },
        )
        .expect("append");
    // The old generation-0 files are still listed FIRST and untouched; the
    // delta landed as a generation-1 chunk after them.
    assert_eq!(
        refreshed.chunk_list,
        vec![
            "chunk-0-0.arrow".to_string(),
            "chunk-0-1.arrow".to_string(),
            "chunk-1-0.arrow".to_string(),
        ]
    );
    assert_eq!(chunk_mtime(&accel, "mv", 0), old_mtime);
    assert_eq!(refreshed.measured_rows, 3);
    assert_eq!(refreshed.source_tokens, tokens("t1"));
    assert_eq!(
        refreshed.change_key,
        Some(ChangeKeyState {
            column: "k".to_string(),
            watermark: Some(Watermark::Int(3)),
        })
    );
    assert_eq!(
        view_rows(&accel, "mv"),
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string())
        ]
    );
}

#[test]
fn empty_append_writes_no_files_and_still_publishes_tokens() {
    let accel = small_chunk_accel("append_empty");
    accel
        .create_view(
            "mv",
            "SELECT k, v FROM t",
            &schema(),
            &[batch(&[(1, "a")])],
            tokens("t0"),
            None,
        )
        .expect("create");
    let refreshed = accel
        .refresh_view_append(
            "mv",
            &schema(),
            &[],
            &tokens("t1"),
            &ChangeKeyState {
                column: "k".to_string(),
                watermark: Some(Watermark::Int(1)),
            },
        )
        .expect("empty append");
    assert_eq!(refreshed.chunk_list, vec!["chunk-0-0.arrow".to_string()]);
    assert_eq!(refreshed.measured_rows, 1);
    assert_eq!(refreshed.source_tokens, tokens("t1"));
    assert!(refreshed.refreshed_at.is_some());
}

#[test]
fn merge_rewrites_only_the_affected_chunks() {
    let accel = small_chunk_accel("merge");
    // Three single-row chunks: keys 1, 2, 3.
    accel
        .create_view(
            "mv",
            "SELECT k, v FROM t",
            &schema(),
            &[batch(&[(1, "a")]), batch(&[(2, "b")]), batch(&[(3, "c")])],
            tokens("t0"),
            None,
        )
        .expect("create");

    // Fresh pull: key 2 updated, key 3 deleted, key 4 inserted; key 1 intact.
    let fresh = vec![batch(&[(1, "a"), (2, "B"), (4, "d")])];
    let outcome = accel
        .refresh_view_merge("mv", &schema(), &fresh, "k", &tokens("t1"))
        .expect("merge");
    assert_eq!(outcome.chunks_kept, 1);
    assert_eq!(outcome.chunks_rewritten, 2);
    assert_eq!(outcome.chunks_appended, 1);
    // Chunk 0 (key 1) survives under its generation-0 name; the rewrite of
    // key 2 and the appended key 4 are generation 1; key 3's chunk is gone.
    assert_eq!(
        outcome.view.chunk_list,
        vec![
            "chunk-0-0.arrow".to_string(),
            "chunk-1-0.arrow".to_string(),
            "chunk-1-1.arrow".to_string(),
        ]
    );
    assert_eq!(outcome.view.measured_rows, 3);
    assert_eq!(outcome.view.source_tokens, tokens("t1"));
    assert_eq!(outcome.view.change_key, None);
    assert_eq!(
        view_rows(&accel, "mv"),
        vec![
            (1, "a".to_string()),
            (2, "B".to_string()),
            (4, "d".to_string())
        ]
    );
    // The superseded generation-0 files are unlinked.
    let view = accel.view("mv").expect("view");
    let stray = accel
        .store_root()
        .join(&view.location)
        .join("chunk-0-1.arrow");
    assert!(!stray.exists());
}

#[test]
fn merge_with_an_identical_pull_keeps_every_file() {
    let accel = small_chunk_accel("merge_noop");
    accel
        .create_view(
            "mv",
            "SELECT k, v FROM t",
            &schema(),
            &[batch(&[(1, "a")]), batch(&[(2, "b")])],
            tokens("t0"),
            None,
        )
        .expect("create");
    let fresh = vec![batch(&[(1, "a"), (2, "b")])];
    let outcome = accel
        .refresh_view_merge("mv", &schema(), &fresh, "k", &tokens("t1"))
        .expect("merge");
    assert_eq!(outcome.chunks_kept, 2);
    assert_eq!(outcome.chunks_rewritten, 0);
    assert_eq!(outcome.chunks_appended, 0);
    assert_eq!(
        outcome.view.chunk_list,
        vec!["chunk-0-0.arrow".to_string(), "chunk-0-1.arrow".to_string()]
    );
}

#[test]
fn merge_that_empties_the_view_leaves_one_schema_bearing_chunk() {
    let accel = small_chunk_accel("merge_empty");
    accel
        .create_view(
            "mv",
            "SELECT k, v FROM t",
            &schema(),
            &[batch(&[(1, "a")])],
            tokens("t0"),
            None,
        )
        .expect("create");
    let outcome = accel
        .refresh_view_merge("mv", &schema(), &[], "k", &tokens("t1"))
        .expect("merge to empty");
    assert_eq!(outcome.view.measured_rows, 0);
    assert_eq!(outcome.view.chunk_list.len(), 1);
    assert!(view_rows(&accel, "mv").is_empty());
}

#[test]
fn merge_duplicate_fresh_key_raises_and_keeps_the_view() {
    let accel = small_chunk_accel("merge_dup");
    accel
        .create_view(
            "mv",
            "SELECT k, v FROM t",
            &schema(),
            &[batch(&[(1, "a")])],
            tokens("t0"),
            None,
        )
        .expect("create");
    let dup = vec![batch(&[(1, "a"), (1, "b")])];
    let error = accel
        .refresh_view_merge("mv", &schema(), &dup, "k", &tokens("t1"))
        .unwrap_err();
    assert!(matches!(error, AccelError::MergeKey(_)), "{error}");
    // Nothing was published: the view still serves its old row and tokens.
    let view = accel.view("mv").expect("view");
    assert_eq!(view.measured_rows, 1);
    assert_eq!(view.source_tokens, tokens("t0"));
}

#[test]
fn chunk_watermark_reads_the_stored_max_and_stored_schema_roundtrips() {
    let accel = small_chunk_accel("wm");
    let view = accel
        .create_view(
            "mv",
            "SELECT k, v FROM t",
            &schema(),
            &[batch(&[(5, "a")]), batch(&[(9, "b"), (2, "c")])],
            tokens("t0"),
            None,
        )
        .expect("create");
    assert_eq!(
        accel.chunk_watermark(&view, "k").expect("watermark"),
        WatermarkScan::Value(Some(Watermark::Int(9)))
    );
    // The text column watermarks too; an unknown column raises.
    assert_eq!(
        accel.chunk_watermark(&view, "v").expect("watermark"),
        WatermarkScan::Value(Some(Watermark::Text("c".to_string())))
    );
    assert!(accel.chunk_watermark(&view, "ghost").is_err());
    let stored = accel.stored_arrow_schema(&view).expect("schema");
    assert_eq!(stored.fields(), schema().fields());
}

#[test]
fn tokens_and_state_survive_a_reopen() {
    let dir = std::env::temp_dir().join(format!("fq_accel_delta_reopen_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create sandbox");
    let config = dir.join("config.yaml");
    {
        let accel = Accelerator::open_with_chunk_bytes(&config, Some(1)).expect("open");
        accel
            .create_view(
                "mv",
                "SELECT k, v FROM t",
                &schema(),
                &[batch(&[(1, "a")])],
                tokens("t0"),
                Some(ChangeKeyState {
                    column: "k".to_string(),
                    watermark: Some(Watermark::Int(1)),
                }),
            )
            .expect("create");
    }
    let accel = Accelerator::open(&config).expect("reopen");
    let view = accel.view("mv").expect("view");
    assert_eq!(view.source_tokens, tokens("t0"));
    assert_eq!(
        view.change_key,
        Some(ChangeKeyState {
            column: "k".to_string(),
            watermark: Some(Watermark::Int(1)),
        })
    );
}
