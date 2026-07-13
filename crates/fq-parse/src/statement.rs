//! Statement classification: materialized-view DDL vs a plain query.
//!
//! The engine's statement surface is queries plus exactly three DDL forms:
//!
//! - `CREATE MATERIALIZED VIEW <name> AS <select>`
//! - `REFRESH MATERIALIZED VIEW <name>`
//! - `DROP MATERIALIZED VIEW <name>`
//!
//! `classify_statement` recognizes the DDL forms LEXICALLY (a quote-aware
//! tokenizer, no full SQL parse) and returns everything else untouched as
//! `Statement::Query`, which the caller feeds through the normal
//! parse -> bind -> plan pipeline. Every Postgres DDL option outside the three
//! plain forms raises `ParseError::Unsupported` naming the option - never a
//! silent partial acceptance:
//!
//! - `IF NOT EXISTS` / `IF EXISTS`: a create that silently does nothing hides a
//!   name collision, and a drop that silently does nothing hides a typo'd name.
//! - `WITH DATA` / `WITH NO DATA`: creation always populates; there is no
//!   deferred-population form.
//! - `REFRESH ... CONCURRENTLY` and any refresh data option: how a refresh
//!   pulls (delta vs whole) is decided by the engine from the datasource
//!   change-key config, not by statement options; publication is atomic and
//!   never blocks readers, so CONCURRENTLY has no meaning here.
//! - Storage options (`WITH (...)`, `USING`, `TABLESPACE`), `CASCADE`/`RESTRICT`,
//!   and a schema-qualified view name: a materialized view lives in the engine's
//!   own store, so none of these have a meaning here.

use crate::error::ParseError;

/// One SQL statement, classified: a materialized-view DDL form, or a plain
/// query passed through as text for the normal parse pipeline.
#[derive(Debug, PartialEq, Eq)]
pub enum Statement<'a> {
    /// Anything that is not materialized-view DDL; the full original text.
    Query(&'a str),
    /// `CREATE MATERIALIZED VIEW <name> AS <select>`: the view name and the
    /// defining SELECT text (stored verbatim as the view's definition).
    CreateMaterializedView { name: String, select_sql: &'a str },
    /// `REFRESH MATERIALIZED VIEW <name>`.
    RefreshMaterializedView { name: String },
    /// `DROP MATERIALIZED VIEW <name>`.
    DropMaterializedView { name: String },
}

/// Classify one SQL statement. Materialized-view DDL is recognized by its
/// leading keywords; any other text (SELECT, VALUES, other DDL the engine does
/// not speak) is returned as `Statement::Query` for the normal parser, which
/// raises its own loud error on SQL it cannot plan.
pub fn classify_statement(sql: &str) -> Result<Statement<'_>, ParseError> {
    let mut tokens = Tokenizer::new(sql);
    let Some(first) = tokens.peek_word() else {
        return Ok(Statement::Query(sql));
    };
    match first.to_ascii_uppercase().as_str() {
        // REFRESH exists only for materialized views, so any REFRESH is ours.
        "REFRESH" => classify_refresh(&mut tokens),
        "CREATE" if second_word_is(sql, "MATERIALIZED") => classify_create(&mut tokens),
        "DROP" if second_word_is(sql, "MATERIALIZED") => classify_drop(&mut tokens),
        _ => Ok(Statement::Query(sql)),
    }
}

/// Whether the statement's second word token is `keyword` (case-insensitive).
/// Distinguishes `CREATE MATERIALIZED VIEW` / `DROP MATERIALIZED VIEW` from
/// every other CREATE/DROP, which stay `Statement::Query`.
fn second_word_is(sql: &str, keyword: &str) -> bool {
    let mut tokens = Tokenizer::new(sql);
    tokens.next_token();
    match tokens.peek_word() {
        Some(word) => word.eq_ignore_ascii_case(keyword),
        None => false,
    }
}

/// Parse `CREATE MATERIALIZED VIEW <name> AS <select>`, raising on every
/// unsupported creation option.
fn classify_create<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("CREATE")?;
    tokens.expect_keyword("MATERIALIZED")?;
    tokens.expect_keyword("VIEW")?;
    if tokens.peek_word_is("IF") {
        return Err(ParseError::Unsupported(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS: a create that silently does \
             nothing hides a name collision; create without IF NOT EXISTS"
                .to_string(),
        ));
    }
    let name = view_name(tokens)?;
    reject_pre_as_options(tokens)?;
    tokens.expect_keyword("AS")?;
    let select_sql = tokens.rest().trim();
    if select_sql.is_empty() {
        return Err(ParseError::Parse(
            "CREATE MATERIALIZED VIEW has no defining SELECT after AS".to_string(),
        ));
    }
    reject_trailing_data_option(select_sql)?;
    Ok(Statement::CreateMaterializedView { name, select_sql })
}

/// Raise on the creation options that may sit between the view name and AS
/// (`USING <method>`, `WITH (<storage params>)`, `TABLESPACE <name>`); a
/// materialized view lives in the engine's own chunk store, so none apply.
fn reject_pre_as_options(tokens: &mut Tokenizer<'_>) -> Result<(), ParseError> {
    for option in ["USING", "WITH", "TABLESPACE"] {
        if tokens.peek_word_is(option) {
            return Err(ParseError::Unsupported(format!(
                "CREATE MATERIALIZED VIEW ... {option}: storage options do not \
                 apply to the engine's materialized-view store"
            )));
        }
    }
    Ok(())
}

/// Raise when the defining SELECT ends with `WITH DATA` / `WITH NO DATA`.
/// Creation always populates the view, so the qualifier has no meaning here;
/// accepting-and-ignoring it would misstate what WITH NO DATA asks for.
fn reject_trailing_data_option(select_sql: &str) -> Result<(), ParseError> {
    let mut words = Vec::new();
    let mut tokens = Tokenizer::new(select_sql);
    while let Some(token) = tokens.next_token() {
        if let Token::Word(word) = token {
            words.push(word.to_ascii_uppercase());
        } else {
            words.push(String::new());
        }
    }
    let tail_two = words.len() >= 2 && words[words.len() - 2..] == ["WITH", "DATA"];
    let tail_three = words.len() >= 3 && words[words.len() - 3..] == ["WITH", "NO", "DATA"];
    if tail_two || tail_three {
        return Err(ParseError::Unsupported(
            "CREATE MATERIALIZED VIEW ... WITH [NO] DATA: creation always \
             populates the view; drop the WITH [NO] DATA qualifier"
                .to_string(),
        ));
    }
    Ok(())
}

/// Parse `REFRESH MATERIALIZED VIEW <name>`, raising on CONCURRENTLY and on
/// any trailing tokens (WITH [NO] DATA, a second name). The statement takes no
/// options: whether the refresh pulls a delta or the whole view is decided by
/// the engine from the datasource change-key config, and chunk publication is
/// already atomic (readers are never blocked), so CONCURRENTLY has no meaning.
fn classify_refresh<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("REFRESH")?;
    tokens.expect_keyword("MATERIALIZED")?;
    tokens.expect_keyword("VIEW")?;
    if tokens.peek_word_is("CONCURRENTLY") {
        return Err(ParseError::Unsupported(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY: publication is atomic and \
             readers are never blocked, so CONCURRENTLY has no meaning here"
                .to_string(),
        ));
    }
    let name = view_name(tokens)?;
    expect_end(tokens, "REFRESH MATERIALIZED VIEW")?;
    Ok(Statement::RefreshMaterializedView { name })
}

/// Parse `DROP MATERIALIZED VIEW <name>`, raising on IF EXISTS (a drop that
/// silently does nothing hides a typo'd name) and on any trailing tokens
/// (CASCADE/RESTRICT, a second name).
fn classify_drop<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("DROP")?;
    tokens.expect_keyword("MATERIALIZED")?;
    tokens.expect_keyword("VIEW")?;
    if tokens.peek_word_is("IF") {
        return Err(ParseError::Unsupported(
            "DROP MATERIALIZED VIEW IF EXISTS: a drop that silently does nothing \
             hides a typo'd view name; drop without IF EXISTS"
                .to_string(),
        ));
    }
    let name = view_name(tokens)?;
    expect_end(tokens, "DROP MATERIALIZED VIEW")?;
    Ok(Statement::DropMaterializedView { name })
}

/// Read the view name: a single unqualified identifier. An unquoted name
/// lowercases (the Postgres identifier rule); a double-quoted name keeps its
/// exact spelling. A qualified name (`schema.view`) raises: the view store is
/// a single flat namespace.
fn view_name(tokens: &mut Tokenizer<'_>) -> Result<String, ParseError> {
    let name = match tokens.next_token() {
        Some(Token::Word(word)) => word.to_lowercase(),
        Some(Token::QuotedIdent(name)) => name,
        Some(Token::Other(ch)) => {
            return Err(ParseError::Parse(format!(
                "expected a materialized view name, found '{ch}'"
            )))
        }
        Some(Token::StringLiteral) | None => {
            return Err(ParseError::Parse(
                "expected a materialized view name".to_string(),
            ))
        }
    };
    if name.is_empty() {
        return Err(ParseError::Parse(
            "materialized view name is empty".to_string(),
        ));
    }
    if matches!(tokens.peek(), Some(Token::Other('.'))) {
        return Err(ParseError::Unsupported(format!(
            "materialized view name '{name}...' is schema-qualified; the view \
             store is a single flat namespace, use an unqualified name"
        )));
    }
    Ok(name)
}

/// Raise if any token follows where the statement must end; a trailing token is
/// an option this DDL form does not take.
fn expect_end(tokens: &mut Tokenizer<'_>, form: &str) -> Result<(), ParseError> {
    match tokens.next_token() {
        None => Ok(()),
        Some(token) => Err(ParseError::Unsupported(format!(
            "{form} takes a bare view name; unexpected trailing '{}'",
            token.describe()
        ))),
    }
}

/// One lexical token of the DDL surface.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token<'a> {
    /// A bare word (identifier or keyword), as written.
    Word(&'a str),
    /// A `"double quoted"` identifier with `""` escapes resolved.
    QuotedIdent(String),
    /// A `'single quoted'` string literal (content irrelevant to the DDL shape).
    StringLiteral,
    /// Any other single character (punctuation, operator).
    Other(char),
}

impl Token<'_> {
    /// A short rendering for error messages.
    fn describe(&self) -> String {
        match self {
            Token::Word(word) => (*word).to_string(),
            Token::QuotedIdent(name) => format!("\"{name}\""),
            Token::StringLiteral => "<string literal>".to_string(),
            Token::Other(ch) => ch.to_string(),
        }
    }
}

/// A minimal quote-aware SQL tokenizer: words, double-quoted identifiers,
/// single-quoted string literals, and single punctuation characters. Enough to
/// recognize the DDL keyword shapes without misreading quoted text; a statement
/// ending inside an unterminated quote raises.
struct Tokenizer<'a> {
    input: &'a str,
    /// Byte offset of the next unread character.
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    /// Start tokenizing at the beginning of `input`.
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    /// The unread remainder of the input (used for the `AS <select>` tail).
    fn rest(&self) -> &'a str {
        &self.input[self.pos..]
    }

    /// Advance past whitespace.
    fn skip_whitespace(&mut self) {
        let remainder = &self.input[self.pos..];
        let trimmed = remainder.trim_start();
        self.pos += remainder.len() - trimmed.len();
    }

    /// The next token, or None at end of input.
    fn next_token(&mut self) -> Option<Token<'a>> {
        self.skip_whitespace();
        let mut chars = self.input[self.pos..].chars();
        let first = chars.next()?;
        match first {
            '"' => Some(self.read_quoted('"')),
            '\'' => Some(self.read_quoted('\'')),
            ch if is_word_char(ch) => Some(self.read_word()),
            ch => {
                self.pos += ch.len_utf8();
                Some(Token::Other(ch))
            }
        }
    }

    /// Peek the next token without consuming it.
    fn peek(&mut self) -> Option<Token<'a>> {
        let saved = self.pos;
        let token = self.next_token();
        self.pos = saved;
        token
    }

    /// Peek the next token if it is a bare word.
    fn peek_word(&mut self) -> Option<&'a str> {
        match self.peek() {
            Some(Token::Word(word)) => Some(word),
            _ => None,
        }
    }

    /// Whether the next token is the bare word `keyword` (case-insensitive).
    fn peek_word_is(&mut self, keyword: &str) -> bool {
        match self.peek_word() {
            Some(word) => word.eq_ignore_ascii_case(keyword),
            None => false,
        }
    }

    /// Consume the next token, requiring it to be `keyword` (case-insensitive).
    fn expect_keyword(&mut self, keyword: &str) -> Result<(), ParseError> {
        match self.next_token() {
            Some(Token::Word(word)) if word.eq_ignore_ascii_case(keyword) => Ok(()),
            Some(token) => Err(ParseError::Parse(format!(
                "expected '{keyword}', found '{}'",
                token.describe()
            ))),
            None => Err(ParseError::Parse(format!(
                "expected '{keyword}', found end of statement"
            ))),
        }
    }

    /// Read a bare word starting at the current position.
    fn read_word(&mut self) -> Token<'a> {
        let start = self.pos;
        for ch in self.input[self.pos..].chars() {
            if !is_word_char(ch) {
                break;
            }
            self.pos += ch.len_utf8();
        }
        Token::Word(&self.input[start..self.pos])
    }

    /// Read a quoted token (`"` identifier or `'` string) with doubled-quote
    /// escapes. An unterminated quote consumes to end of input; the DDL layer
    /// then fails on the malformed shape rather than misreading past the quote.
    fn read_quoted(&mut self, quote: char) -> Token<'a> {
        self.pos += quote.len_utf8();
        let mut content = String::new();
        let mut chars = self.input[self.pos..].chars().peekable();
        while let Some(ch) = chars.next() {
            self.pos += ch.len_utf8();
            if ch == quote {
                // A doubled quote is an escaped quote character; anything else
                // closes the token.
                if chars.peek() == Some(&quote) {
                    chars.next();
                    self.pos += quote.len_utf8();
                    content.push(quote);
                    continue;
                }
                break;
            }
            content.push(ch);
        }
        if quote == '"' {
            Token::QuotedIdent(content)
        } else {
            Token::StringLiteral
        }
    }
}

/// Whether a character continues a bare word token.
fn is_word_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '$'
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Classify and unwrap, panicking with the error for a readable failure.
    fn classify(sql: &str) -> Statement<'_> {
        classify_statement(sql).expect("statement classifies")
    }

    #[test]
    fn plain_select_is_a_query() {
        assert_eq!(
            classify("SELECT 1 FROM t"),
            Statement::Query("SELECT 1 FROM t")
        );
    }

    #[test]
    fn other_create_forms_stay_queries() {
        // CREATE TABLE is not materialized-view DDL; it passes through so the
        // normal parser raises its own loud unsupported error.
        assert_eq!(
            classify("CREATE TABLE t (a INT)"),
            Statement::Query("CREATE TABLE t (a INT)")
        );
        assert_eq!(classify("DROP TABLE t"), Statement::Query("DROP TABLE t"));
    }

    #[test]
    fn create_extracts_name_and_select() {
        let statement = classify("CREATE MATERIALIZED VIEW mv1 AS SELECT a FROM duck.main.t");
        assert_eq!(
            statement,
            Statement::CreateMaterializedView {
                name: "mv1".to_string(),
                select_sql: "SELECT a FROM duck.main.t",
            }
        );
    }

    #[test]
    fn create_lowercases_an_unquoted_name() {
        let statement = classify("CREATE MATERIALIZED VIEW MyView AS SELECT 1");
        assert!(matches!(
            statement,
            Statement::CreateMaterializedView { ref name, .. } if name == "myview"
        ));
    }

    #[test]
    fn create_keeps_a_quoted_name_exact() {
        let statement = classify("CREATE MATERIALIZED VIEW \"MyView\" AS SELECT 1");
        assert!(matches!(
            statement,
            Statement::CreateMaterializedView { ref name, .. } if name == "MyView"
        ));
    }

    #[test]
    fn create_if_not_exists_raises() {
        let error = classify_statement("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1")
            .unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("IF NOT EXISTS")));
    }

    #[test]
    fn create_with_data_option_raises() {
        for tail in ["WITH DATA", "WITH NO DATA", "with no data"] {
            let sql = format!("CREATE MATERIALIZED VIEW mv AS SELECT a FROM t {tail}");
            let error = classify_statement(&sql).unwrap_err();
            assert!(
                matches!(error, ParseError::Unsupported(ref m) if m.contains("WITH [NO] DATA")),
                "{tail}"
            );
        }
    }

    #[test]
    fn select_ending_in_a_string_literal_is_not_a_data_option() {
        // The trailing-token check is quote-aware: a literal 'WITH DATA' is one
        // string token, not the WITH DATA keywords.
        let statement = classify("CREATE MATERIALIZED VIEW mv AS SELECT 'WITH DATA'");
        assert!(matches!(
            statement,
            Statement::CreateMaterializedView { .. }
        ));
    }

    #[test]
    fn create_storage_options_raise() {
        for sql in [
            "CREATE MATERIALIZED VIEW mv WITH (fillfactor=70) AS SELECT 1",
            "CREATE MATERIALIZED VIEW mv USING heap AS SELECT 1",
            "CREATE MATERIALIZED VIEW mv TABLESPACE ts AS SELECT 1",
        ] {
            let error = classify_statement(sql).unwrap_err();
            assert!(matches!(error, ParseError::Unsupported(_)), "{sql}");
        }
    }

    #[test]
    fn create_qualified_name_raises() {
        let error = classify_statement("CREATE MATERIALIZED VIEW main.mv AS SELECT 1").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("qualified")));
    }

    #[test]
    fn create_without_as_raises() {
        let error = classify_statement("CREATE MATERIALIZED VIEW mv SELECT 1").unwrap_err();
        assert!(matches!(error, ParseError::Parse(_)));
    }

    #[test]
    fn create_with_empty_select_raises() {
        let error = classify_statement("CREATE MATERIALIZED VIEW mv AS").unwrap_err();
        assert!(matches!(error, ParseError::Parse(_)));
    }

    #[test]
    fn refresh_extracts_the_name() {
        assert_eq!(
            classify("REFRESH MATERIALIZED VIEW mv1"),
            Statement::RefreshMaterializedView {
                name: "mv1".to_string()
            }
        );
    }

    #[test]
    fn refresh_concurrently_raises() {
        let error = classify_statement("REFRESH MATERIALIZED VIEW CONCURRENTLY mv").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("CONCURRENTLY")));
    }

    #[test]
    fn refresh_with_data_option_raises() {
        let error = classify_statement("REFRESH MATERIALIZED VIEW mv WITH NO DATA").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(_)));
    }

    #[test]
    fn drop_extracts_the_name() {
        assert_eq!(
            classify("DROP MATERIALIZED VIEW mv1"),
            Statement::DropMaterializedView {
                name: "mv1".to_string()
            }
        );
    }

    #[test]
    fn drop_if_exists_raises() {
        let error = classify_statement("DROP MATERIALIZED VIEW IF EXISTS mv").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("IF EXISTS")));
    }

    #[test]
    fn drop_cascade_raises() {
        for sql in [
            "DROP MATERIALIZED VIEW mv CASCADE",
            "DROP MATERIALIZED VIEW mv RESTRICT",
            "DROP MATERIALIZED VIEW mv, mv2",
        ] {
            let error = classify_statement(sql).unwrap_err();
            assert!(matches!(error, ParseError::Unsupported(_)), "{sql}");
        }
    }

    #[test]
    fn empty_input_is_a_query() {
        // Blank text is not DDL; the normal parser raises its own error on it.
        assert_eq!(classify("   "), Statement::Query("   "));
    }
}
