//! fq-connectors: concrete data-source connectors implementing the
//! `fq_catalog::DataSource` catalog/statistics tier against real drivers.
//!
//! Ports `datasources/duckdb.py` and `datasources/postgresql.py`. The DATA-PLANE
//! fetch tier (Arrow streaming, ship_table, ctid-parallel) is NOT here - it is a
//! separate concern that moves in with fq-exec (today's fedqrs connectors.rs,
//! de-pyo3-ified).
//!
//! Only DuckDB and Postgres have connectors; there is no `parquet`/`clickhouse`
//! metadata surface, and registering such a source is rejected loudly.

pub mod duckdb_source;
pub mod postgres_source;

pub use duckdb_source::DuckDbSource;
pub use postgres_source::PostgresSource;
