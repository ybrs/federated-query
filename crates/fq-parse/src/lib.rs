//! fq-parse: SQL text -> `fq_plan::LogicalPlan`.
//!
//! polyglot-sql is the parser; this crate walks its AST into the logical model
//! and rejects unsupported SQL loudly. Ports `parser/parser.py`,
//! `parser/dialect.py`, `processor/query_preprocessor.py`.
//!
//! Build stage: the structural core - single-table SELECT with the full clause
//! pipeline (WHERE / GROUP BY / HAVING / ORDER BY / LIMIT), the five aggregates,
//! and the common scalar expression nodes. NOT yet handled (each raises
//! `ParseError::Unsupported`, never silently dropped): joins, star expansion
//! (catalog-driven), DISTINCT, CTEs, set operations, derived tables, subquery
//! expressions, typed scalar functions, Cast/Case/In/Between. These are the next
//! increments.

pub mod error;
pub mod expr;
pub mod select;

pub use error::ParseError;

use polyglot_sql::expressions::Expression;
use polyglot_sql::DialectType;

/// Parse a single SQL statement into a logical plan. The canonical internal
/// dialect is Postgres (the engine renders Postgres-form SQL, transpiling per
/// source at execute time).
pub fn parse(sql: &str) -> Result<fq_plan::LogicalPlan, ParseError> {
    let statements = polyglot_sql::parse(sql, DialectType::PostgreSQL)
        .map_err(|error| ParseError::Parse(format!("{error:?}")))?;
    let [statement] = statements.as_slice() else {
        return Err(ParseError::MultiStatement);
    };
    dispatch(statement)
}

/// Dispatch a top-level statement to its converter.
fn dispatch(statement: &Expression) -> Result<fq_plan::LogicalPlan, ParseError> {
    match statement {
        Expression::Select(select) => select::convert_select(select),
        other => Err(ParseError::Unsupported(format!(
            "statement `{}`",
            other.variant_name()
        ))),
    }
}
