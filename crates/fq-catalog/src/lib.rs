//! fq-catalog: the metadata model and datasource registry (and, later, the
//! learned-stats store). Ports `catalog/catalog.py`, `catalog/schema.py`, and
//! `catalog/stats_catalog.py`.
//!
//! PORT NOTES (never silent):
//! - Parent back-references (`Column._table`, `Table._schema`) retire; they are
//!   not read anywhere in the engine. FQN content is denormalized onto children.
//!   See `schema.rs`.
//! - The `DataSource` trait (the catalog/statistics tier of a connector) + its
//!   value types + the default `map_native_type` live in `datasource.rs`; the
//!   concrete Postgres/DuckDB connectors implement it in fq-connectors. The
//!   default-mapping tests live with the logic (here); the pg-uuid override test
//!   lives in fq-connectors with the override.
//! - `stats_catalog.rs` ports the direct StatsCatalog surface; its cross-crate
//!   consumer tests (`_persist_observations`, CostModel/StatisticsCollector
//!   overlay) translate with fq-physical/fq-runtime and fq-optimize.

pub mod catalog;
pub mod datasource;
pub mod error;
pub mod schema;
pub mod stats_catalog;

pub use catalog::Catalog;
pub use datasource::{
    build_column_statistics, build_table_statistics, map_native_type_default,
    metadata_from_information_schema, ColumnMetadata, ColumnStatistics, DataSource,
    DataSourceCapability, RenderDialect, StatValue, TableMetadata, TableStatistics,
};
pub use error::CatalogError;
pub use schema::{Column, ColumnQualifier, Schema, Table, TableQualifier};
pub use stats_catalog::{group_key, Clock, StatsCatalog, StatsError};
