//! The Bind-time parameter substitution seam.
//!
//! The engine's `Runtime::execute` takes a SQL string and has no parameter
//! placeholders of its own. The extended query protocol, however, sends a
//! statement with `$1`, `$2`, ... placeholders at Parse and their values at Bind.
//! This module bridges the two: it renders each bound value to a SQL literal and
//! splices it in for its placeholder, producing an ordinary SQL string to hand to
//! the engine. The substituted SQL is what the engine plans and runs; this
//! module is the single seam between wire parameters and engine SQL.
//!
//! A value arrives in the client's chosen format (text or binary); pgwire's
//! `Portal::parameter` decodes either into a typed Rust value, so both formats are
//! accepted for the types the server maps (small/regular/big integers, 4/8-byte
//! floats, booleans, and text). A parameter of any other type is refused loudly,
//! naming the type - the engine would otherwise receive a value it cannot place.
//! A NULL value renders as the SQL keyword `NULL`.
//!
//! Placeholders are spliced only where they are real: text inside string literals,
//! quoted identifiers, line and block comments, and dollar-quoted strings is
//! copied through untouched, so a literal `$1` inside a string is never rewritten.

use pgwire::api::portal::Portal;
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// Render every bound parameter to a SQL literal and splice each in for its
/// `$n` placeholder in the portal's statement, returning ready-to-run SQL.
pub fn substitute(portal: &Portal<String>) -> PgWireResult<String> {
    let literals = render_params(portal)?;
    splice_with(&portal.statement.statement, |number| {
        literals
            .get(number - 1)
            .cloned()
            .ok_or_else(|| missing_param(number, literals.len()))
    })
}

/// Splice a typed `NULL` in for every placeholder in `sql`, producing
/// placeholder-free SQL whose result schema equals the parameterized
/// statement's (a parameter's value never changes the output column types). Used
/// by the extended protocol's statement Describe, which resolves the schema
/// before any value is bound. A placeholder past `param_types` (a client that
/// declared no types) becomes an untyped `NULL`.
pub fn substitute_nulls(sql: &str, param_types: &[Option<Type>]) -> PgWireResult<String> {
    splice_with(sql, |number| {
        let declared = param_types.get(number - 1).cloned().flatten();
        Ok(null_literal(declared))
    })
}

/// A `NULL` cast to the placeholder's declared SQL type when that type is known,
/// so a bare projected parameter still resolves a concrete result type; an
/// unknown or unmapped type yields an untyped `NULL`.
fn null_literal(declared: Option<Type>) -> String {
    match declared.and_then(|pg_type| sql_type_name(&pg_type)) {
        Some(name) => format!("CAST(NULL AS {name})"),
        None => "NULL".to_owned(),
    }
}

/// The SQL type name a declared parameter type casts to, or `None` for a type the
/// server does not map.
fn sql_type_name(pg_type: &Type) -> Option<&'static str> {
    match param_kind(pg_type)? {
        ParamKind::Int2 => Some("SMALLINT"),
        ParamKind::Int4 => Some("INTEGER"),
        ParamKind::Int8 => Some("BIGINT"),
        ParamKind::Float4 => Some("REAL"),
        ParamKind::Float8 => Some("DOUBLE"),
        ParamKind::Bool => Some("BOOLEAN"),
        ParamKind::Text => Some("VARCHAR"),
    }
}

/// Render each of the portal's bound parameters, in order, to its SQL literal.
fn render_params(portal: &Portal<String>) -> PgWireResult<Vec<String>> {
    let mut literals = Vec::with_capacity(portal.parameters.len());
    for index in 0..portal.parameters.len() {
        literals.push(render_param(portal, index)?);
    }
    Ok(literals)
}

/// Render one bound parameter to a SQL literal, decoding it as the Rust type its
/// declared Postgres type maps to. A parameter with no declared type is treated
/// as text (an undecorated literal); a type the server does not map is refused.
fn render_param(portal: &Portal<String>, index: usize) -> PgWireResult<String> {
    let declared = portal
        .statement
        .parameter_types
        .get(index)
        .cloned()
        .flatten();
    let pg_type = declared.unwrap_or(Type::UNKNOWN);
    let kind = param_kind(&pg_type).ok_or_else(|| unsupported_param(index, &pg_type))?;
    match kind {
        ParamKind::Int2 => Ok(display_literal(
            portal.parameter::<i16>(index, &Type::INT2)?,
        )),
        ParamKind::Int4 => Ok(display_literal(
            portal.parameter::<i32>(index, &Type::INT4)?,
        )),
        ParamKind::Int8 => Ok(display_literal(
            portal.parameter::<i64>(index, &Type::INT8)?,
        )),
        ParamKind::Float4 => Ok(display_literal(
            portal.parameter::<f32>(index, &Type::FLOAT4)?,
        )),
        ParamKind::Float8 => Ok(display_literal(
            portal.parameter::<f64>(index, &Type::FLOAT8)?,
        )),
        ParamKind::Bool => Ok(bool_literal(portal.parameter::<bool>(index, &Type::BOOL)?)),
        ParamKind::Text => Ok(text_literal(
            portal.parameter::<String>(index, &Type::TEXT)?,
        )),
    }
}

/// The parameter value shapes the server renders. Every other Postgres type is
/// unmapped and refused.
enum ParamKind {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Bool,
    Text,
}

/// Classify a declared parameter type into the value shape used to decode and
/// render it, or `None` for a type the server does not map. An unknown-typed
/// parameter is decoded as text, matching Postgres's own treatment of an
/// undecorated string literal.
fn param_kind(pg_type: &Type) -> Option<ParamKind> {
    if pg_type == &Type::INT2 {
        Some(ParamKind::Int2)
    } else if pg_type == &Type::INT4 {
        Some(ParamKind::Int4)
    } else if pg_type == &Type::INT8 {
        Some(ParamKind::Int8)
    } else if pg_type == &Type::FLOAT4 {
        Some(ParamKind::Float4)
    } else if pg_type == &Type::FLOAT8 {
        Some(ParamKind::Float8)
    } else if pg_type == &Type::BOOL {
        Some(ParamKind::Bool)
    } else if is_text_type(pg_type) {
        Some(ParamKind::Text)
    } else {
        None
    }
}

/// Whether a type is one the server renders as a quoted text literal: the string
/// types plus the unknown type Postgres assigns to an undecorated literal.
fn is_text_type(pg_type: &Type) -> bool {
    pg_type == &Type::TEXT
        || pg_type == &Type::VARCHAR
        || pg_type == &Type::BPCHAR
        || pg_type == &Type::NAME
        || pg_type == &Type::UNKNOWN
}

/// Render a value whose text form is a valid unquoted SQL literal (integers and
/// floats). A NULL value renders as the SQL keyword.
fn display_literal<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
}

/// Render a boolean as a SQL boolean literal, or NULL.
fn bool_literal(value: Option<bool>) -> String {
    match value {
        Some(true) => "TRUE".to_owned(),
        Some(false) => "FALSE".to_owned(),
        None => "NULL".to_owned(),
    }
}

/// Render text as a single-quoted SQL string literal with embedded quotes
/// doubled (standard-conforming string escaping), or NULL.
fn text_literal(value: Option<String>) -> String {
    match value {
        Some(text) => format!("'{}'", text.replace('\'', "''")),
        None => "NULL".to_owned(),
    }
}

/// Copy `sql`, replacing each `$n` placeholder with `resolve(n)` (1-based), while
/// copying string literals, quoted identifiers, comments, and dollar-quoted
/// strings through unchanged so a `$n` inside them is never rewritten. `resolve`
/// supplies the literal for a placeholder and fails loudly for one it cannot.
fn splice_with<F>(sql: &str, resolve: F) -> PgWireResult<String>
where
    F: Fn(usize) -> PgWireResult<String>,
{
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut index = 0;
    while index < chars.len() {
        index = match chars[index] {
            '\'' | '"' => copy_quoted(&chars, index, &mut out),
            '-' if peek(&chars, index + 1) == Some('-') => {
                copy_line_comment(&chars, index, &mut out)
            }
            '/' if peek(&chars, index + 1) == Some('*') => {
                copy_block_comment(&chars, index, &mut out)
            }
            '$' => step_dollar(&chars, index, &resolve, &mut out)?,
            other => {
                out.push(other);
                index + 1
            }
        };
    }
    Ok(out)
}

/// Advance over a `$` at `start`: substitute a `$n` placeholder via `resolve`,
/// copy a dollar-quoted string through unchanged, or copy a lone `$`. Returns the
/// index just past what it consumed.
fn step_dollar<F>(
    chars: &[char],
    start: usize,
    resolve: &F,
    out: &mut String,
) -> PgWireResult<usize>
where
    F: Fn(usize) -> PgWireResult<String>,
{
    if peek(chars, start + 1).is_some_and(|c| c.is_ascii_digit()) {
        let (number, end) = read_placeholder(chars, start)?;
        out.push_str(&resolve(number)?);
        return Ok(end);
    }
    if let Some(end) = copy_dollar_quote(chars, start, out) {
        return Ok(end);
    }
    out.push('$');
    Ok(start + 1)
}

/// Read a `$n` placeholder beginning at `start` (a `$` before a digit) and return
/// its 1-based number and the index just past the digits.
fn read_placeholder(chars: &[char], start: usize) -> PgWireResult<(usize, usize)> {
    let mut index = start + 1;
    while index < chars.len() && chars[index].is_ascii_digit() {
        index += 1;
    }
    let digits: String = chars[start + 1..index].iter().collect();
    let number: usize = digits.parse().map_err(|_| {
        protocol_violation(format!("parameter placeholder ${digits} is out of range"))
    })?;
    if number == 0 {
        return Err(protocol_violation(
            "parameter placeholder $0 is invalid; placeholders start at $1".to_owned(),
        ));
    }
    Ok((number, index))
}

/// The character at `index`, or `None` past the end.
fn peek(chars: &[char], index: usize) -> Option<char> {
    chars.get(index).copied()
}

/// Copy a quoted region (a `'...'` string or a `"..."` identifier) starting at
/// its opening quote, honoring the doubled-quote escape, and return the index
/// just past the closing quote. An unterminated region is copied to the end.
fn copy_quoted(chars: &[char], start: usize, out: &mut String) -> usize {
    let quote = chars[start];
    out.push(quote);
    let mut index = start + 1;
    while index < chars.len() {
        let current = chars[index];
        out.push(current);
        if current == quote {
            if peek(chars, index + 1) == Some(quote) {
                out.push(quote);
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }
    index
}

/// Copy a `--` line comment through the end of its line, returning the index of
/// the newline (or the end of input).
fn copy_line_comment(chars: &[char], start: usize, out: &mut String) -> usize {
    let mut index = start;
    while index < chars.len() && chars[index] != '\n' {
        out.push(chars[index]);
        index += 1;
    }
    index
}

/// Copy a `/* ... */` block comment, returning the index just past the closing
/// `*/`. An unterminated comment is copied to the end.
fn copy_block_comment(chars: &[char], start: usize, out: &mut String) -> usize {
    out.push('/');
    out.push('*');
    let mut index = start + 2;
    while index < chars.len() {
        if chars[index] == '*' && peek(chars, index + 1) == Some('/') {
            out.push('*');
            out.push('/');
            return index + 2;
        }
        out.push(chars[index]);
        index += 1;
    }
    index
}

/// If `start` opens a dollar-quoted string (`$tag$...$tag$` with an empty or
/// identifier tag), copy the whole string through `out` and return the index just
/// past its closing tag; otherwise return `None`. An unterminated string is
/// copied to the end.
fn copy_dollar_quote(chars: &[char], start: usize, out: &mut String) -> Option<usize> {
    let mut cursor = start + 1;
    while cursor < chars.len() && (chars[cursor].is_alphanumeric() || chars[cursor] == '_') {
        cursor += 1;
    }
    if peek(chars, cursor) != Some('$') {
        return None;
    }
    let tag: Vec<char> = chars[start..=cursor].to_vec();
    for ch in &tag {
        out.push(*ch);
    }
    let mut index = cursor + 1;
    while index < chars.len() {
        if chars[index..].starts_with(tag.as_slice()) {
            for ch in &tag {
                out.push(*ch);
            }
            return Some(index + tag.len());
        }
        out.push(chars[index]);
        index += 1;
    }
    Some(index)
}

/// The error returned for a parameter whose declared type the server does not map
/// to a SQL literal.
fn unsupported_param(index: usize, pg_type: &Type) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        format!(
            "bind parameter ${} has type {}, which fedq-server does not accept as a parameter",
            index + 1,
            pg_type.name()
        ),
    )))
}

/// The error returned when a `$n` placeholder has no corresponding bound value.
fn missing_param(number: usize, supplied: usize) -> PgWireError {
    protocol_violation(format!(
        "SQL references parameter ${number} but the bind message supplied {supplied} parameter(s)"
    ))
}

/// A protocol-violation error carrying `message`.
fn protocol_violation(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "08P01".to_owned(),
        message,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Splice with each `$n` resolved to `<n>`, so the placeholder positions the
    /// scanner rewrote are visible in the output.
    fn mark_placeholders(sql: &str) -> String {
        splice_with(sql, |number| Ok(format!("<{number}>"))).expect("splice")
    }

    #[test]
    fn splices_placeholders_outside_opaque_regions() {
        assert_eq!(
            mark_placeholders("SELECT $1, $2 FROM t WHERE a = $10"),
            "SELECT <1>, <2> FROM t WHERE a = <10>"
        );
    }

    #[test]
    fn leaves_placeholders_inside_strings_and_comments_untouched() {
        assert_eq!(mark_placeholders("SELECT '$1' AS c"), "SELECT '$1' AS c");
        assert_eq!(
            mark_placeholders("SELECT \"$1\" FROM t"),
            "SELECT \"$1\" FROM t"
        );
        assert_eq!(
            mark_placeholders("SELECT $1 -- $2\nFROM t"),
            "SELECT <1> -- $2\nFROM t"
        );
        assert_eq!(
            mark_placeholders("SELECT $1 /* $2 */ FROM t"),
            "SELECT <1> /* $2 */ FROM t"
        );
        assert_eq!(
            mark_placeholders("SELECT $tag$ $1 $tag$, $1"),
            "SELECT $tag$ $1 $tag$, <1>"
        );
    }

    #[test]
    fn doubled_quote_inside_a_string_does_not_end_it() {
        assert_eq!(
            mark_placeholders("SELECT 'it''s $1' , $1"),
            "SELECT 'it''s $1' , <1>"
        );
    }

    #[test]
    fn a_placeholder_without_a_value_is_refused() {
        let result = splice_with("SELECT $2", |_| Err(missing_param(2, 1)));
        assert!(result.is_err());
    }

    #[test]
    fn null_substitution_casts_to_the_declared_type() {
        let types = vec![Some(Type::INT4), None];
        assert_eq!(
            substitute_nulls("SELECT $1, $2 FROM t", &types).expect("nulls"),
            "SELECT CAST(NULL AS INTEGER), NULL FROM t"
        );
    }

    #[test]
    fn text_literal_doubles_embedded_quotes() {
        assert_eq!(text_literal(Some("O'Brien".to_owned())), "'O''Brien'");
        assert_eq!(text_literal(None), "NULL");
    }

    #[test]
    fn numeric_and_bool_literals_render_bare() {
        assert_eq!(display_literal(Some(42i32)), "42");
        assert_eq!(display_literal::<i32>(None), "NULL");
        assert_eq!(bool_literal(Some(true)), "TRUE");
        assert_eq!(bool_literal(Some(false)), "FALSE");
    }
}
