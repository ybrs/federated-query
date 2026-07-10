//! fq-catalog: the metadata model and datasource registry (and, later, the
//! learned-stats store). Ports `catalog/catalog.py`, `catalog/schema.py`, and
//! `catalog/stats_catalog.py`.
//!
//! PORT NOTES (never silent):
//! - Parent back-references (`Column._table`, `Table._schema`) retire; they are
//!   not read anywhere in the engine. FQN content is denormalized onto children.
//!   See `schema.rs`.
//! - `Catalog::load_metadata` and `_require_renderable` are deferred to the
//!   fq-connectors milestone (they need the full `DataSource` trait +
//!   `is_renderable`). See `catalog.rs`.
//! - The connector `map_native_type` tests that live in test_catalog.py
//!   (`test_type_mapping`, `test_unknown_type_raises`,
//!   `test_postgres_maps_uuid_consistently_with_execution`) test the CONNECTOR,
//!   not the catalog; they translate into the fq-connectors test corpus
//!   (test_datasources), not here.
//! - `stats_catalog.rs` (rusqlite, byte-compatible SQLite schema) ports the
//!   direct StatsCatalog surface; its cross-crate consumer tests
//!   (`_persist_observations`, CostModel/StatisticsCollector overlay) translate
//!   with fq-physical/fq-runtime and fq-optimize. See that module's port note.

pub mod catalog;
pub mod schema;
pub mod stats_catalog;

pub use catalog::{Catalog, DataSource};
pub use schema::{Column, ColumnQualifier, Schema, Table, TableQualifier};
pub use stats_catalog::{group_key, Clock, StatsCatalog, StatsError};
