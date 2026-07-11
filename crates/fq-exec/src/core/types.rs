// Imported fedqrs engine core - already validated by the TPC tallies; pedantic-
// cleaned incrementally, not on import.
#![allow(clippy::all, clippy::pedantic)]
//! Small shared value types.

/// The kind of a datasource, used to pick the SQL dialect and read path.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DsKind {
    Postgres,
    DuckDb,
    Parquet,
}
