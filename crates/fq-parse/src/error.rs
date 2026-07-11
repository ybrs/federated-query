//! Parse errors.

use thiserror::Error;

/// A failure turning SQL text into a logical plan.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    /// polyglot-sql could not parse the SQL.
    #[error("parse error: {0}")]
    Parse(String),

    /// More than one statement was given where exactly one is required.
    #[error("expected a single SQL statement")]
    MultiStatement,

    /// A construct that parses but the engine does not (yet) plan. Ports the
    /// Python `UnsupportedSQLError`: the engine fails fast rather than silently
    /// dropping a clause and returning a wrong answer.
    #[error("unsupported SQL: {0}")]
    Unsupported(String),
}
