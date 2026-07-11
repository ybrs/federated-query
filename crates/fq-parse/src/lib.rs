//! fq-parse: SQL text -> `fq_plan::LogicalPlan`.
//!
//! polyglot-sql is the parser; the `Converter` (see `convert.rs`) walks its AST
//! into the logical model and rejects unsupported SQL loudly. Ports
//! `parser/parser.py`, `parser/dialect.py`, `processor/query_preprocessor.py`.
//!
//! Coverage: base tables + JOINs + derived tables in FROM, catalog-driven star
//! expansion, WHERE / GROUP BY (five aggregates) / HAVING / ORDER BY /
//! LIMIT-OFFSET / DISTINCT, VALUES, binary set operations, and the expression
//! nodes (columns, literals, binary/unary ops, IS NULL, Cast, Case, In, Between,
//! and the subquery expressions). Each unsupported construct raises
//! `ParseError::Unsupported`, never silently dropped. Still raising: CTEs (WITH),
//! comma joins, typed scalar functions, windows.

pub mod convert;
pub mod error;
pub mod expr;

pub use error::ParseError;

use fq_catalog::Catalog;
use polyglot_sql::DialectType;

/// Parse a single SQL statement into a logical plan, expanding `SELECT *` against
/// `catalog`. The canonical internal dialect is Postgres (the engine renders
/// Postgres-form SQL, transpiling per source at execute time).
pub fn parse_with_catalog(
    sql: &str,
    catalog: &Catalog,
) -> Result<fq_plan::LogicalPlan, ParseError> {
    let statements = polyglot_sql::parse(sql, DialectType::PostgreSQL)
        .map_err(|error| ParseError::Parse(format!("{error:?}")))?;
    let [statement] = statements.as_slice() else {
        return Err(ParseError::MultiStatement);
    };
    convert::Converter::new(catalog).query(statement)
}

/// Parse without a catalog. A `SELECT *` cannot expand and raises; structural
/// queries with explicit columns parse fully.
pub fn parse(sql: &str) -> Result<fq_plan::LogicalPlan, ParseError> {
    parse_with_catalog(sql, &Catalog::new())
}
