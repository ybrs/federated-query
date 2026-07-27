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
//! the subquery expressions, and window functions). Named WINDOW clauses are
//! inlined into their `OVER w` references before conversion. Each unsupported
//! construct raises `ParseError::Unsupported`, never silently dropped. Still
//! raising: WITH RECURSIVE.

pub mod convert;
pub mod error;
mod events;
pub mod expr;
pub mod functions;
pub mod statement;
mod window;

pub use error::ParseError;
pub use statement::{classify_statement, GrantObject, Statement};

use fq_catalog::Catalog;
use polyglot_sql::DialectType;

/// Parse a single SQL statement into a logical plan, expanding `SELECT *` against
/// `catalog`. The canonical internal dialect is Postgres (the engine renders
/// Postgres-form SQL, transpiling per source at execute time).
pub fn parse_with_catalog(
    sql: &str,
    catalog: &Catalog,
) -> Result<fq_plan::LogicalPlan, ParseError> {
    if let Some((format, inner)) = explain::split(sql)? {
        // Fresh Explain wrapping the statement it describes - no base to copy
        // from. Field list (input/format) is the complete Explain struct.
        return Ok(fq_plan::LogicalPlan::Explain(fq_plan::logical::Explain {
            input: Box::new(parse_with_catalog(inner, catalog)?),
            format,
        }));
    }
    let mut statements = polyglot_sql::parse(sql, DialectType::PostgreSQL)
        .map_err(|error| ParseError::Parse(format!("{error:?}")))?;
    if statements.len() != 1 {
        return Err(ParseError::MultiStatement);
    }
    let statement = window::inline_named_windows(statements.remove(0))?;
    convert::Converter::new(catalog).query(&statement)
}

/// EXPLAIN belongs to this crate, not to a caller scanning the SQL text: it is
/// SQL syntax, its option list needs parsing, and the statement it describes is
/// parsed by the same code that parses any statement.
///
/// polyglot maps EXPLAIN onto DESCRIBE rather than a plan-describing node, so
/// the prefix is split off here before the remainder is handed to it.
pub mod explain {
    use super::ParseError;
    use fq_plan::logical::ExplainFormat;

    /// Split a leading `EXPLAIN [(FORMAT {TEXT|JSON})]` off `sql`, returning the
    /// requested format and the statement it describes. `Ok(None)` when `sql` is
    /// not an EXPLAIN.
    ///
    /// An option list that is present but not understood RAISES. `EXPLAIN
    /// (ANALYZE)` asks to execute and measure; returning an unmeasured plan
    /// would answer a different question than the one asked.
    pub fn split(sql: &str) -> Result<Option<(ExplainFormat, &str)>, ParseError> {
        let trimmed = sql.trim_start();
        let mut chars = trimmed.char_indices();
        for expected in "explain".chars() {
            match chars.next() {
                Some((_, actual)) if actual.eq_ignore_ascii_case(&expected) => {}
                _ => return Ok(None),
            }
        }
        // The keyword must end here, not be the prefix of an identifier.
        let rest = match chars.next() {
            Some((index, next)) if next.is_whitespace() => trimmed[index..].trim_start(),
            Some((index, '(')) => trimmed[index..].trim_start(),
            _ => return Ok(None),
        };
        let Some(body) = rest.strip_prefix('(') else {
            return Ok(Some((ExplainFormat::Text, rest)));
        };
        let Some(close) = body.find(')') else {
            return Err(ParseError::Unsupported(
                "EXPLAIN option list is missing its closing parenthesis".to_string(),
            ));
        };
        Ok(Some((
            format_of(&body[..close])?,
            body[close + 1..].trim_start(),
        )))
    }

    /// The render format an option list names. FORMAT is the only option; any
    /// other raises rather than being dropped.
    fn format_of(options: &str) -> Result<ExplainFormat, ParseError> {
        let trimmed = options.trim();
        let unsupported = |what: &str| {
            ParseError::Unsupported(format!(
                "EXPLAIN option `{what}`; supported: FORMAT TEXT, FORMAT JSON"
            ))
        };
        let mut words = trimmed.split_whitespace();
        let Some(keyword) = words.next() else {
            return Err(unsupported(trimmed));
        };
        if !keyword.eq_ignore_ascii_case("format") {
            return Err(unsupported(trimmed));
        }
        match words.next().map(str::to_ascii_uppercase).as_deref() {
            Some("TEXT") => Ok(ExplainFormat::Text),
            Some("JSON") => Ok(ExplainFormat::Json),
            other => Err(unsupported(other.unwrap_or(""))),
        }
    }
}

/// Parse without a catalog. A `SELECT *` cannot expand and raises; structural
/// queries with explicit columns parse fully.
pub fn parse(sql: &str) -> Result<fq_plan::LogicalPlan, ParseError> {
    parse_with_catalog(sql, &Catalog::new())
}
