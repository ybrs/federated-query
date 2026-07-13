//! The chunk store: one materialized view = one directory of framed Arrow IPC
//! chunk files under the store root. All file IO goes through `object_store`;
//! the backend is the local filesystem today, and an S3 bucket is a
//! configuration change with no change to the chunk model.
//!
//! Chunk files are immutable once written: a refresh writes a NEW generation
//! of files (`chunk-<generation>-<n>.arrow`) and the catalog's chunk-list swap
//! is what publishes them; superseded files are unlinked after the swap. A
//! reader that already opened a file keeps reading it after the unlink (POSIX
//! keeps the inode alive), so an in-flight scan is never corrupted.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::error::AccelError;

/// The soft chunk-size bound: a chunk closes once its accumulated Arrow buffer
/// bytes pass this, so one view is a bounded set of moderately sized files
/// (mmap-friendly reads, and a later delta refresh rewrites file-sized units).
const CHUNK_BYTES: usize = 64 * 1024 * 1024;

/// A materialized-view chunk store rooted at one directory.
pub struct ChunkStore {
    root: std::path::PathBuf,
    store: Arc<dyn ObjectStore>,
    /// A private current-thread runtime driving the async `object_store` API;
    /// store operations are synchronous to their callers.
    runtime: tokio::runtime::Runtime,
}

impl ChunkStore {
    /// Open the store rooted at `root`, creating the directory if absent.
    pub fn open(root: &std::path::Path) -> Result<Self, AccelError> {
        std::fs::create_dir_all(root).map_err(|error| {
            AccelError::InvalidSchema(format!(
                "cannot create materialized-view store root '{}': {error}",
                root.display()
            ))
        })?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(root)?);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("chunk-store tokio runtime");
        Ok(Self {
            root: root.to_path_buf(),
            store,
            runtime,
        })
    }

    /// The store root directory.
    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    /// The absolute filesystem path of one chunk (handed to the execution
    /// plane, which reads the files through DataFusion's own object store).
    pub fn chunk_path(&self, location: &str, chunk: &str) -> std::path::PathBuf {
        self.root.join(location).join(chunk)
    }

    /// Write `batches` as generation `generation` chunk files under
    /// `location`, rotating to a new chunk at the size bound. Always writes at
    /// least one chunk (an empty result still persists its schema). Returns
    /// the ordered chunk file names and the total bytes written. Each file put
    /// is atomic (temp name + rename inside the backend), and nothing is
    /// published until the caller swaps the catalog chunk list.
    pub fn write_chunks(
        &self,
        location: &str,
        generation: u64,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<(Vec<String>, i64), AccelError> {
        let mut names = Vec::new();
        let mut total_bytes: i64 = 0;
        let mut pending: Vec<&RecordBatch> = Vec::new();
        let mut pending_bytes = 0usize;
        for batch in batches {
            pending.push(batch);
            pending_bytes += batch.get_array_memory_size();
            if pending_bytes >= CHUNK_BYTES {
                total_bytes +=
                    self.put_chunk(location, generation, &mut names, schema, &pending)?;
                pending.clear();
                pending_bytes = 0;
            }
        }
        if !pending.is_empty() || names.is_empty() {
            total_bytes += self.put_chunk(location, generation, &mut names, schema, &pending)?;
        }
        Ok((names, total_bytes))
    }

    /// Frame one chunk file from `batches` and put it as the next chunk of
    /// this generation; returns its byte size.
    fn put_chunk(
        &self,
        location: &str,
        generation: u64,
        names: &mut Vec<String>,
        schema: &SchemaRef,
        batches: &[&RecordBatch],
    ) -> Result<i64, AccelError> {
        let name = format!("chunk-{generation}-{}.arrow", names.len());
        let mut writer = FileWriter::try_new(Vec::new(), schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        let buffer = writer.into_inner()?;
        let bytes = i64::try_from(buffer.len()).expect("chunk size fits i64");
        let path = StorePath::parse(format!("{location}/{name}"))?;
        self.runtime
            .block_on(self.store.as_ref().put(&path, PutPayload::from(buffer)))?;
        names.push(name);
        Ok(bytes)
    }

    /// Read one chunk back as Arrow batches (the store's own verification and
    /// test surface; query reads go through the execution plane).
    pub fn read_chunk(
        &self,
        location: &str,
        chunk: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), AccelError> {
        let path = StorePath::parse(format!("{location}/{chunk}"))?;
        let bytes = self
            .runtime
            .block_on(async { self.store.as_ref().get(&path).await?.bytes().await })?;
        let reader = FileReader::try_new(std::io::Cursor::new(bytes), None)?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }
        Ok((schema, batches))
    }

    /// Delete the named chunk files under `location`. A file already gone is
    /// fine (a crashed earlier unlink); any other failure raises.
    pub fn delete_chunks(&self, location: &str, chunks: &[String]) -> Result<(), AccelError> {
        for chunk in chunks {
            let path = StorePath::parse(format!("{location}/{chunk}"))?;
            match self.runtime.block_on(self.store.as_ref().delete(&path)) {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// Delete EVERYTHING under `location` (the final sweep of a dropped view:
    /// the listed chunks are already gone, this clears any stragglers a crash
    /// left behind).
    pub fn delete_location(&self, location: &str) -> Result<(), AccelError> {
        let prefix = StorePath::parse(location)?;
        let store = Arc::clone(&self.store);
        let objects: Vec<StorePath> = self.runtime.block_on(async {
            store
                .list(Some(&prefix))
                .map_ok(|meta| meta.location)
                .try_collect()
                .await
        })?;
        for path in objects {
            match self.runtime.block_on(self.store.as_ref().delete(&path)) {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        // The (now empty) directory itself is filesystem residue outside the
        // object model; removing it is tidiness, and a failure means a file
        // still exists, which the next sweep handles.
        let _ = std::fs::remove_dir(self.root.join(location));
        Ok(())
    }

    /// The highest generation number among `chunks` plus one: the generation a
    /// refresh writes. Chunk names are `chunk-<generation>-<n>.arrow` by
    /// construction; a name this store did not write raises.
    pub fn next_generation(chunks: &[String]) -> Result<u64, AccelError> {
        let mut highest = 0u64;
        for chunk in chunks {
            let generation = chunk
                .strip_prefix("chunk-")
                .and_then(|rest| rest.split('-').next())
                .and_then(|generation| generation.parse::<u64>().ok())
                .ok_or_else(|| {
                    AccelError::InvalidSchema(format!(
                        "chunk name '{chunk}' is not of the form chunk-<generation>-<n>.arrow"
                    ))
                })?;
            highest = highest.max(generation);
        }
        Ok(highest + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType as ArrowType, Field, Schema};

    /// A one-column Int64 batch holding `values`.
    fn batch(values: &[i64]) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", ArrowType::Int64, true)]));
        let array = Arc::new(Int64Array::from(values.to_vec()));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).expect("batch");
        (schema, batch)
    }

    /// A fresh store under a unique temp dir.
    fn temp_store() -> ChunkStore {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("fq_accel_store_{}_{id}", std::process::id()));
        ChunkStore::open(&root).expect("open store")
    }

    #[test]
    fn write_then_read_roundtrips_rows() {
        let store = temp_store();
        let (schema, data) = batch(&[1, 2, 3]);
        let (names, bytes) = store
            .write_chunks("loc", 0, &schema, std::slice::from_ref(&data))
            .expect("write");
        assert_eq!(names, vec!["chunk-0-0.arrow".to_string()]);
        assert!(bytes > 0);
        let (read_schema, read) = store.read_chunk("loc", &names[0]).expect("read");
        assert_eq!(read_schema.as_ref(), schema.as_ref());
        assert_eq!(read, vec![data]);
    }

    #[test]
    fn empty_result_still_writes_a_schema_bearing_chunk() {
        let store = temp_store();
        let (schema, _) = batch(&[]);
        let (names, _) = store.write_chunks("loc", 0, &schema, &[]).expect("write");
        assert_eq!(names.len(), 1);
        let (read_schema, read) = store.read_chunk("loc", &names[0]).expect("read");
        assert_eq!(read_schema.as_ref(), schema.as_ref());
        assert!(read.is_empty());
    }

    #[test]
    fn delete_chunks_tolerates_already_gone_files() {
        let store = temp_store();
        let (schema, data) = batch(&[1]);
        let (names, _) = store
            .write_chunks("loc", 0, &schema, &[data])
            .expect("write");
        store.delete_chunks("loc", &names).expect("delete once");
        store.delete_chunks("loc", &names).expect("delete twice");
    }

    #[test]
    fn delete_location_removes_stragglers() {
        let store = temp_store();
        let (schema, data) = batch(&[1]);
        store
            .write_chunks("loc", 0, &schema, &[data])
            .expect("write");
        store.delete_location("loc").expect("sweep");
        assert!(!store.root().join("loc").exists());
    }

    #[test]
    fn next_generation_steps_past_the_highest() {
        let chunks = vec!["chunk-0-0.arrow".to_string(), "chunk-2-1.arrow".to_string()];
        assert_eq!(ChunkStore::next_generation(&chunks).expect("parse"), 3);
        assert!(ChunkStore::next_generation(&["stray.arrow".to_string()]).is_err());
    }
}
