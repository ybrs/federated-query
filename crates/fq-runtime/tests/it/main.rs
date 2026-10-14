//! One integration-test binary for the crate: each module is a test suite.

mod acl;
mod concurrency;
mod cross_source;
mod cross_source_clickhouse;
mod ctes;
mod delta_refresh;
mod dense_joins;
mod duckdb_runtime;
mod dynamic_catalog;
mod event_dataset;
mod materialized_views;
mod parquet_runtime;
mod settings;
mod state_store;
mod substitution;
mod unnest;
mod views;
