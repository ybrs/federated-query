//! fq-accel: the materialized-view store.
//!
//! A materialized view is a named, user-managed cached result: `CREATE
//! MATERIALIZED VIEW <name> AS <select>` runs the SELECT once and persists the
//! rows; the view then serves as a queryable relation until an explicit
//! `REFRESH MATERIALIZED VIEW` brings it forward. Freshness is entirely
//! user-controlled: nothing on the query path checks the sources.
//!
//! The crate owns the storage half of REFRESH; the runtime owns the policy
//! (which base tables changed, whether the view's shape admits a delta). The
//! three refresh mechanics here are:
//!
//! - WHOLE re-pull ([`Accelerator::refresh_view`]): a full new generation of
//!   chunks replaces the old one.
//! - DELTA APPEND ([`Accelerator::refresh_view_append`]): rows past the
//!   monotonic change key's watermark land as NEW chunks; existing files are
//!   never touched. The watermark ([`Watermark`], stored as
//!   [`ChangeKeyState`]) advances in the same catalog transaction.
//! - PRIMARY-KEY MERGE ([`Accelerator::refresh_view_merge`]): a fresh whole
//!   pull is diffed by key against the stored chunks; only chunks holding a
//!   changed or deleted key are rewritten, new keys append.
//!
//! The pieces:
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
//!   listing), the output schema, sizes, timestamps, the per-base-table
//!   source tokens captured before the last pull (REFRESH skips the pull when
//!   they all still match), and the delta-append watermark state. Every
//!   refresh publication swaps all of that in one transaction.
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
pub mod merge;
pub mod source;
pub mod store;
pub mod view;
pub mod watermark;

pub use accelerator::{Accelerator, MergeOutcome};
pub use catalog::{DynamicDatasource, Grant, User, ViewCatalog};
pub use error::AccelError;
pub use source::{views_schema, MaterializedViewSource, VIEW_SCHEMA_NAME};
pub use store::ChunkStore;
pub use view::{columns_from_arrow, ChangeKeyState, MaterializedView, ViewColumn};
pub use watermark::{scan_watermark, Watermark, WatermarkScan};
