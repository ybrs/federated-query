//! fq-accel: the materialized-view store.
//!
//! A materialized view is a named, user-managed cached result: `CREATE
//! MATERIALIZED VIEW <name> AS <select>` runs the SELECT once and persists the
//! rows; the view then serves as a queryable relation until an explicit
//! `REFRESH MATERIALIZED VIEW` re-pulls it whole. Freshness is entirely
//! user-controlled: nothing on the query path checks the sources.
//!
//! The crate owns two pieces and the coordinator over them:
//!
//! - [`ChunkStore`]: one view = one directory of framed Arrow IPC chunk files
//!   under the store root (`<config-stem>.mv/` next to the config). Every
//!   read, write, and delete goes through the `object_store` abstraction, so
//!   the local filesystem backend can become an S3 bucket by configuration
//!   with no change to the chunk model.
//! - [`ViewCatalog`]: the `materialized_views` table in the SAME SQLite file
//!   as the learned-stats catalog (`<config-stem>.stats.sqlite`). A row
//!   carries the view name, its defining SQL, the ordered chunk list (the
//!   source of truth for which files compose the view - never a directory
//!   listing), the output schema, sizes, and timestamps. The
//!   `source_tokens` / `change_key` columns exist in the schema but are not
//!   read or written: refresh is a whole re-pull, and a change-keyed delta
//!   refresh raises as unsupported at the statement layer.
//! - [`Accelerator`]: the create/refresh/drop lifecycle, keeping chunks and
//!   catalog rows consistent (write-then-register on create, publish-then-
//!   unlink on refresh, tombstone-then-unlink-then-purge on drop, with a
//!   tombstone sweep on open for crash recovery).
//!
//! This is its own crate (not part of fq-catalog or fq-exec) because the store
//! needs Arrow + object_store + SQLite together: fq-catalog must stay
//! driver- and Arrow-free for the binder, and fq-exec is the imported
//! execution engine. fq-runtime composes both: it executes the defining
//! SELECT, hands the Arrow result here, and registers the store's tables with
//! the fq-exec data plane. The planner sees a view through
//! [`MaterializedViewSource`], a `fq_catalog::DataSource` over the registry,
//! so binding, star expansion, and statistics need no special cases.

pub mod accelerator;
pub mod catalog;
pub mod error;
pub mod source;
pub mod store;
pub mod view;

pub use accelerator::Accelerator;
pub use catalog::ViewCatalog;
pub use error::AccelError;
pub use source::{views_schema, MaterializedViewSource, VIEW_SCHEMA_NAME};
pub use store::ChunkStore;
pub use view::{columns_from_arrow, MaterializedView, ViewColumn};
