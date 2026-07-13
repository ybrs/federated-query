//! fq-connectors: concrete data-source connectors implementing the
//! `fq_catalog::DataSource` catalog/statistics tier against real drivers.
//!
//! Ports `datasources/duckdb.py`, `datasources/postgresql.py`, and
//! `datasources/parquet.py`. The DATA-PLANE fetch tier (Arrow streaming,
//! ship_table, ctid-parallel, the DataFusion parquet scan) is NOT here - it is a
//! separate concern that lives with fq-exec (today's fedqrs connectors.rs,
//! de-pyo3-ified).
//!
//! DuckDB, Postgres, ClickHouse, MySQL, and Parquet have connectors; the Parquet
//! one is footer-only (schema, exact row counts, row-group statistics), a
//! read-only file source. Any other source kind has no metadata surface and is
//! rejected loudly at registration.

pub mod clickhouse_source;
pub mod duckdb_source;
pub mod mysql_source;
pub mod parquet_source;
pub mod postgres_source;

pub use clickhouse_source::ClickHouseSource;
pub use duckdb_source::DuckDbSource;
pub use mysql_source::MySqlSource;
pub use parquet_source::ParquetSource;
pub use postgres_source::PostgresSource;
