//! Primary-key merge planning: diff a view's stored chunks against a fresh
//! pull BY KEY and decide, per chunk, whether it survives byte-for-byte or is
//! rewritten. Only chunks holding a changed or deleted key are rewritten;
//! fresh keys absent from every chunk append at the end. The planner is pure
//! (batches in, a plan out); the Accelerator does the file IO around it.
//!
//! Row identity and row equality both go through ONE `arrow::row` converter
//! per role (key, full row), so comparisons are exact byte comparisons of the
//! normalized row encoding - no per-type equality code, no coercion.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::interleave_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, SortField};

use crate::error::AccelError;

/// One stored chunk's contents, read by the caller (which also knows its
/// on-disk size for the byte accounting of kept chunks).
pub struct StoredChunk {
    pub name: String,
    pub batches: Vec<RecordBatch>,
    pub byte_size: i64,
}

/// What happens to one stored chunk, in `chunk_list` order.
#[derive(Debug)]
pub enum ChunkPlan {
    /// No row in this chunk changed or was deleted: the file survives
    /// byte-for-byte under its existing name.
    Keep { name: String, byte_size: i64 },
    /// At least one row changed or was deleted: the surviving rows (updated
    /// rows replaced in place, deleted rows dropped, original order kept) form
    /// a replacement chunk. An empty batch means every row left the chunk and
    /// the chunk is dropped from the list.
    Rewrite {
        superseded: String,
        rows: RecordBatch,
    },
}

/// The full merge decision over a view.
#[derive(Debug)]
pub struct MergePlan {
    /// Per stored chunk, in order.
    pub chunk_plans: Vec<ChunkPlan>,
    /// Fresh rows whose key no stored chunk holds, in fresh-pull order; they
    /// append as new chunks after the rewrites.
    pub inserts: Option<RecordBatch>,
    /// Total rows the merged view holds.
    pub total_rows: i64,
}

/// A fresh row, addressed into the fresh batches, with its full-row encoding
/// and a taken flag (a second stored row matching the same key is a duplicate
/// stored key and raises).
struct FreshRow {
    batch: usize,
    row: usize,
    full: Vec<u8>,
    taken: bool,
}

/// Plan the merge of `fresh` (the re-executed defining SELECT, whole) into the
/// stored chunks, keyed by column `key_index`. Requires the fresh batches and
/// every stored chunk to share one arrow schema (the caller verifies before
/// pulling chunks in); a duplicate key on either side raises `MergeKey`.
pub fn plan_merge(
    schema: &SchemaRef,
    stored: &[StoredChunk],
    fresh: &[RecordBatch],
    key_index: usize,
) -> Result<MergePlan, AccelError> {
    let converters = Converters::new(schema, key_index)?;
    let mut fresh_rows = index_fresh_rows(&converters, fresh)?;
    let mut chunk_plans = Vec::with_capacity(stored.len());
    let mut total_rows: i64 = 0;
    for chunk in stored {
        let plan = plan_one_chunk(&converters, schema, chunk, fresh, &mut fresh_rows)?;
        total_rows += match &plan {
            ChunkPlan::Keep { .. } => chunk_row_count(chunk),
            ChunkPlan::Rewrite { rows, .. } => i64::try_from(rows.num_rows()).expect("fits i64"),
        };
        chunk_plans.push(plan);
    }
    let inserts = collect_inserts(fresh, &fresh_rows)?;
    if let Some(batch) = &inserts {
        total_rows += i64::try_from(batch.num_rows()).expect("fits i64");
    }
    Ok(MergePlan {
        chunk_plans,
        inserts,
        total_rows,
    })
}

/// The two row converters a merge uses. Both sides of every comparison go
/// through the same converter instance, which is what makes the byte encodings
/// comparable.
struct Converters {
    key: RowConverter,
    full: RowConverter,
    key_index: usize,
}

impl Converters {
    /// Build converters for `schema` with the key at `key_index`.
    fn new(schema: &SchemaRef, key_index: usize) -> Result<Self, AccelError> {
        let key_field = schema.fields().get(key_index).ok_or_else(|| {
            AccelError::MergeKey(format!(
                "key column index {key_index} is outside the {}-column schema",
                schema.fields().len()
            ))
        })?;
        let mut full_fields = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            full_fields.push(SortField::new(field.data_type().clone()));
        }
        Ok(Self {
            key: RowConverter::new(vec![SortField::new(key_field.data_type().clone())])?,
            full: RowConverter::new(full_fields)?,
            key_index,
        })
    }

    /// The (key encoding, full-row encoding) pairs of one batch.
    fn encode(
        &self,
        batch: &RecordBatch,
    ) -> Result<(arrow::row::Rows, arrow::row::Rows), AccelError> {
        let key_rows = self
            .key
            .convert_columns(std::slice::from_ref(batch.column(self.key_index)))?;
        let full_rows = self.full.convert_columns(batch.columns())?;
        Ok((key_rows, full_rows))
    }
}

/// Index every fresh row by its key encoding; a key seen twice violates the
/// primary-key declaration and raises.
fn index_fresh_rows(
    converters: &Converters,
    fresh: &[RecordBatch],
) -> Result<BTreeMap<Vec<u8>, FreshRow>, AccelError> {
    let mut rows = BTreeMap::new();
    for (batch_index, batch) in fresh.iter().enumerate() {
        let (key_rows, full_rows) = converters.encode(batch)?;
        for row in 0..batch.num_rows() {
            let entry = FreshRow {
                batch: batch_index,
                row,
                full: full_rows.row(row).as_ref().to_vec(),
                taken: false,
            };
            if rows
                .insert(key_rows.row(row).as_ref().to_vec(), entry)
                .is_some()
            {
                return Err(AccelError::MergeKey(
                    "the refreshed pull holds the same primary-key value twice; \
                     the declared key is not unique"
                        .to_string(),
                ));
            }
        }
    }
    Ok(rows)
}

/// Where one surviving row comes from while rebuilding a dirty chunk.
enum RowSource {
    /// Row `row` of chunk batch `batch` (unchanged).
    Stored { batch: usize, row: usize },
    /// Row `row` of fresh batch `batch` (the key's updated version).
    Fresh { batch: usize, row: usize },
}

/// Diff one stored chunk against the fresh key index, deciding keep/rewrite
/// and claiming the fresh rows it consumes.
fn plan_one_chunk(
    converters: &Converters,
    schema: &SchemaRef,
    chunk: &StoredChunk,
    fresh: &[RecordBatch],
    fresh_rows: &mut BTreeMap<Vec<u8>, FreshRow>,
) -> Result<ChunkPlan, AccelError> {
    let mut survivors = Vec::new();
    let mut dirty = false;
    for (batch_index, batch) in chunk.batches.iter().enumerate() {
        let (key_rows, full_rows) = converters.encode(batch)?;
        for row in 0..batch.num_rows() {
            match claim_fresh(fresh_rows, key_rows.row(row).as_ref(), &chunk.name)? {
                // Deleted at source: the row leaves the chunk.
                None => dirty = true,
                Some(fresh_row) if fresh_row.full == full_rows.row(row).as_ref() => {
                    survivors.push(RowSource::Stored {
                        batch: batch_index,
                        row,
                    });
                }
                // Updated at source: the fresh version replaces it in place.
                Some(fresh_row) => {
                    dirty = true;
                    survivors.push(RowSource::Fresh {
                        batch: fresh_row.batch,
                        row: fresh_row.row,
                    });
                }
            }
        }
    }
    if !dirty {
        return Ok(ChunkPlan::Keep {
            name: chunk.name.clone(),
            byte_size: chunk.byte_size,
        });
    }
    Ok(ChunkPlan::Rewrite {
        superseded: chunk.name.clone(),
        rows: rebuild_rows(schema, &chunk.batches, fresh, &survivors)?,
    })
}

/// Claim the fresh row matching `key`, or None when the key was deleted at
/// source. A key two stored rows claim is a duplicate STORED key and raises.
fn claim_fresh<'a>(
    fresh_rows: &'a mut BTreeMap<Vec<u8>, FreshRow>,
    key: &[u8],
    chunk_name: &str,
) -> Result<Option<&'a FreshRow>, AccelError> {
    let Some(entry) = fresh_rows.get_mut(key) else {
        return Ok(None);
    };
    if entry.taken {
        return Err(AccelError::MergeKey(format!(
            "stored chunk '{chunk_name}' holds a primary-key value another \
             stored row already claimed; the stored chunks violate the key"
        )));
    }
    entry.taken = true;
    Ok(Some(entry))
}

/// Materialize the surviving rows of a dirty chunk (stored + fresh mix, in
/// stored order) as one batch via arrow's interleave.
fn rebuild_rows(
    schema: &SchemaRef,
    chunk_batches: &[RecordBatch],
    fresh: &[RecordBatch],
    survivors: &[RowSource],
) -> Result<RecordBatch, AccelError> {
    let mut all_batches: Vec<&RecordBatch> = Vec::with_capacity(chunk_batches.len() + fresh.len());
    all_batches.extend(chunk_batches.iter());
    all_batches.extend(fresh.iter());
    if all_batches.is_empty() {
        // A batch-less chunk against an empty fresh pull: nothing survives.
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    let mut indices = Vec::with_capacity(survivors.len());
    for source in survivors {
        match source {
            RowSource::Stored { batch, row } => indices.push((*batch, *row)),
            RowSource::Fresh { batch, row } => indices.push((chunk_batches.len() + batch, *row)),
        }
    }
    Ok(interleave_record_batch(&all_batches, &indices)?)
}

/// The unclaimed fresh rows, in fresh-pull order, as one batch; None when
/// every fresh key already lived in a chunk.
fn collect_inserts(
    fresh: &[RecordBatch],
    fresh_rows: &BTreeMap<Vec<u8>, FreshRow>,
) -> Result<Option<RecordBatch>, AccelError> {
    let mut positions = Vec::new();
    for entry in fresh_rows.values() {
        if !entry.taken {
            positions.push((entry.batch, entry.row));
        }
    }
    if positions.is_empty() {
        return Ok(None);
    }
    // Fresh-pull order, not key order: appended rows keep the order the
    // defining SELECT produced them in.
    positions.sort_unstable();
    let refs: Vec<&RecordBatch> = fresh.iter().collect();
    Ok(Some(interleave_record_batch(&refs, &positions)?))
}

/// Total rows across a chunk's batches.
fn chunk_row_count(chunk: &StoredChunk) -> i64 {
    let rows: usize = chunk.batches.iter().map(RecordBatch::num_rows).sum();
    i64::try_from(rows).expect("row count fits i64")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowType, Field, Schema};
    use std::sync::Arc;

    /// The (k BIGINT, v TEXT) test schema.
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

    /// A named stored chunk over one batch.
    fn chunk(name: &str, rows: &[(i64, &str)]) -> StoredChunk {
        StoredChunk {
            name: name.to_string(),
            batches: vec![batch(rows)],
            byte_size: 100,
        }
    }

    /// Flatten a batch back to (k, v) rows.
    fn rows_of(batch: &RecordBatch) -> Vec<(i64, String)> {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("k");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("v");
        let mut out = Vec::new();
        for row in 0..batch.num_rows() {
            out.push((keys.value(row), values.value(row).to_string()));
        }
        out
    }

    #[test]
    fn untouched_chunks_survive_and_only_affected_ones_rewrite() {
        let stored = vec![
            chunk("chunk-0-0.arrow", &[(1, "a"), (2, "b")]),
            chunk("chunk-0-1.arrow", &[(3, "c"), (4, "d")]),
        ];
        // Key 3 updated, key 5 inserted; chunk 0 is untouched.
        let fresh = vec![batch(&[(1, "a"), (2, "b"), (3, "C"), (4, "d"), (5, "e")])];
        let plan = plan_merge(&schema(), &stored, &fresh, 0).expect("plan");
        assert_eq!(plan.total_rows, 5);
        assert!(
            matches!(&plan.chunk_plans[0], ChunkPlan::Keep { name, byte_size: 100 } if name == "chunk-0-0.arrow")
        );
        match &plan.chunk_plans[1] {
            ChunkPlan::Rewrite { superseded, rows } => {
                assert_eq!(superseded, "chunk-0-1.arrow");
                assert_eq!(
                    rows_of(rows),
                    vec![(3, "C".to_string()), (4, "d".to_string())]
                );
            }
            ChunkPlan::Keep { .. } => panic!("chunk 1 must rewrite"),
        }
        let inserts = plan.inserts.expect("insert batch");
        assert_eq!(rows_of(&inserts), vec![(5, "e".to_string())]);
    }

    #[test]
    fn deletes_drop_rows_and_an_emptied_chunk_rewrites_to_zero_rows() {
        let stored = vec![
            chunk("chunk-0-0.arrow", &[(1, "a")]),
            chunk("chunk-0-1.arrow", &[(2, "b"), (3, "c")]),
        ];
        // Key 1 deleted entirely; key 3 deleted from chunk 1.
        let fresh = vec![batch(&[(2, "b")])];
        let plan = plan_merge(&schema(), &stored, &fresh, 0).expect("plan");
        assert_eq!(plan.total_rows, 1);
        match &plan.chunk_plans[0] {
            ChunkPlan::Rewrite { rows, .. } => assert_eq!(rows.num_rows(), 0),
            ChunkPlan::Keep { .. } => panic!("emptied chunk must rewrite"),
        }
        match &plan.chunk_plans[1] {
            ChunkPlan::Rewrite { rows, .. } => {
                assert_eq!(rows_of(rows), vec![(2, "b".to_string())]);
            }
            ChunkPlan::Keep { .. } => panic!("chunk 1 lost a row"),
        }
        assert!(plan.inserts.is_none());
    }

    #[test]
    fn an_identical_fresh_pull_keeps_every_chunk() {
        let stored = vec![chunk("chunk-0-0.arrow", &[(1, "a"), (2, "b")])];
        let fresh = vec![batch(&[(1, "a"), (2, "b")])];
        let plan = plan_merge(&schema(), &stored, &fresh, 0).expect("plan");
        assert!(matches!(&plan.chunk_plans[0], ChunkPlan::Keep { .. }));
        assert!(plan.inserts.is_none());
        assert_eq!(plan.total_rows, 2);
    }

    #[test]
    fn duplicate_keys_raise_on_either_side() {
        let stored = vec![chunk("chunk-0-0.arrow", &[(1, "a")])];
        let dup_fresh = vec![batch(&[(1, "a"), (1, "b")])];
        let error = plan_merge(&schema(), &stored, &dup_fresh, 0).unwrap_err();
        assert!(matches!(error, AccelError::MergeKey(_)), "{error}");

        let dup_stored = vec![chunk("chunk-0-0.arrow", &[(1, "a"), (1, "b")])];
        let fresh = vec![batch(&[(1, "a")])];
        let error = plan_merge(&schema(), &dup_stored, &fresh, 0).unwrap_err();
        assert!(matches!(error, AccelError::MergeKey(_)), "{error}");
    }

    #[test]
    fn inserts_keep_fresh_pull_order() {
        let stored = vec![chunk("chunk-0-0.arrow", &[(5, "e")])];
        // New keys arrive out of key order; the appended batch preserves the
        // SELECT's order, not the key order.
        let fresh = vec![batch(&[(9, "i"), (5, "e"), (2, "b")])];
        let plan = plan_merge(&schema(), &stored, &fresh, 0).expect("plan");
        let inserts = plan.inserts.expect("inserts");
        assert_eq!(
            rows_of(&inserts),
            vec![(9, "i".to_string()), (2, "b".to_string())]
        );
    }
}
