//! One integration-test binary for the crate: each module is a test suite.

mod cross_source;
mod cross_source_clickhouse;
mod ctes;
mod delta_refresh;
mod duckdb_runtime;
mod materialized_views;
mod parquet_runtime;
