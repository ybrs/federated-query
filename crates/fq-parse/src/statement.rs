//! Statement classification: the engine's non-SELECT statement surface vs a
//! plain query.
//!
//! The engine's statement surface is queries plus three materialized-view DDL
//! forms, four settings statements, and the event-analytics forms:
//!
//! - `CREATE MATERIALIZED VIEW <name> AS <select>`
//! - `REFRESH MATERIALIZED VIEW <name>`
//! - `DROP MATERIALIZED VIEW <name>`
//! - `SHOW SETTINGS` / `SHOW SETTING <name>`
//! - `SET <name> = <value>` (also `SET <name> TO <value>`)
//! - `RESET <name>` / `RESET ALL`
//! - `CREATE / REFRESH / DROP EVENT VIEW`, `FUNNEL`, `SEGMENT`, `PATHS` (the
//!   event-analytics grammars live in `event_statement`)
//!
//! `classify_statement` recognizes these forms LEXICALLY (a quote-aware
//! tokenizer, no full SQL parse) and returns everything else untouched as
//! `Statement::Query`, which the caller feeds through the normal
//! parse -> bind -> plan pipeline. A bare `SHOW <x>` that is not `SHOW SETTING(S)`
//! stays a `Statement::Query` (the normal parser raises its own loud error on
//! it); every `SET`/`RESET` is a settings statement, since the engine has no
//! other SET meaning. Every Postgres DDL option outside the three plain
//! materialized-view forms raises `ParseError::Unsupported` naming the option -
//! never a silent partial acceptance:
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
use fq_common::events::{EventRoleColumns, FunnelSpec, PathsSpec, SegmentSpec};

/// One SQL statement, classified: a materialized-view DDL form, a settings
/// statement, an event-analytics form, or a plain query passed through as text
/// for the normal parse pipeline.
#[derive(Debug, PartialEq, Eq)]
pub enum Statement<'a> {
    /// Anything that is not a recognized statement form; the full original text.
    Query(&'a str),
    /// `CREATE MATERIALIZED VIEW <name> AS <select>`: the view name and the
    /// defining SELECT text (stored verbatim as the view's definition).
    CreateMaterializedView { name: String, select_sql: &'a str },
    /// `REFRESH MATERIALIZED VIEW <name>`.
    RefreshMaterializedView { name: String },
    /// `DROP MATERIALIZED VIEW <name>`.
    DropMaterializedView { name: String },
    /// `SHOW SETTINGS`: list every engine setting.
    ShowSettings,
    /// `SHOW MATERIALIZED VIEWS`: list every registered materialized view with
    /// its size, timestamps, and substitution benefit counters.
    ShowMaterializedViews,
    /// `SHOW SETTING <name>`: show the one setting named (dotted, e.g.
    /// `optimizer.planning_budget_ms`).
    ShowSetting { name: String },
    /// `SET <name> = <value>` (or `SET <name> TO <value>`): change one
    /// session-mutable setting on the live runtime. `value` is the raw text after
    /// the assignment (surrounding quotes stripped); the runtime type-checks it.
    SetSetting { name: String, value: String },
    /// `RESET <name>` restores one setting to its default; `RESET ALL`
    /// (`name` is `None`) restores every session override.
    ResetSetting { name: Option<String> },
    /// `CREATE EVENT VIEW <name> ENTITY <col> TIMESTAMP <col> EVENT <col>
    /// [TIEBREAK <col>] AS <select>`: the view name, its role columns, and
    /// the defining SELECT.
    CreateEventView {
        name: String,
        roles: EventRoleColumns,
        select_sql: &'a str,
    },
    /// `REFRESH EVENT VIEW <name>`.
    RefreshEventView { name: String },
    /// `DROP EVENT VIEW <name>`.
    DropEventView { name: String },
    /// `FUNNEL OVER <view> STEPS (...) WITHIN <n> <unit>`.
    Funnel(FunnelSpec),
    /// `SEGMENT OVER <view> MEASURE <m> [EVENT '<name>'] BY <bucket>`.
    Segment(SegmentSpec),
    /// `PATHS OVER <view> [STARTING AT '<name>'] MAX DEPTH <n> TOP <k>`.
    Paths(PathsSpec),
    /// `CREATE DATASOURCE <name> TYPE <kind> WITH ( key = 'value', ... )`: the
    /// datasource name, its connector kind, and the ordered connection params.
    /// Values are strings (a bare integer is accepted for numeric params); the
    /// runtime validates them against the per-kind param allowlist.
    CreateDatasource {
        name: String,
        kind: String,
        params: Vec<(String, String)>,
    },
    /// `DROP DATASOURCE <name>`.
    DropDatasource { name: String },
    /// `SHOW DATASOURCES`: list every bootstrap and dynamic datasource.
    ShowDatasources,
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
        "REFRESH" if second_word_is(sql, "EVENT") => {
            crate::event_statement::classify_refresh_event(&mut tokens)
        }
        // Any other REFRESH is the materialized-view form (REFRESH exists for
        // nothing else), so its errors name what that form expects.
        "REFRESH" => classify_refresh(&mut tokens),
        "CREATE" if second_word_is(sql, "MATERIALIZED") => classify_create(&mut tokens),
        "CREATE" if second_word_is(sql, "EVENT") => {
            crate::event_statement::classify_create_event(&mut tokens)
        }
        "CREATE" if second_word_is(sql, "DATASOURCE") => classify_create_datasource(&mut tokens),
        // `CREATE OR REPLACE DATASOURCE` has `OR` as its second word, so it does
        // not match the arm above; route it here to raise (OR REPLACE would
        // silently repoint a live name).
        "CREATE" if create_or_replace_datasource(sql) => Err(ParseError::Unsupported(
            "CREATE OR REPLACE DATASOURCE: a replace silently repoints a live name \
             while other sessions may hold the old source; use DROP DATASOURCE then \
             CREATE DATASOURCE"
                .to_string(),
        )),
        "DROP" if second_word_is(sql, "MATERIALIZED") => classify_drop(&mut tokens),
        "DROP" if second_word_is(sql, "EVENT") => {
            crate::event_statement::classify_drop_event(&mut tokens)
        }
        "DROP" if second_word_is(sql, "DATASOURCE") => classify_drop_datasource(&mut tokens),
        // ALTER exists in the engine only as a rejected datasource form: an
        // in-place repoint with partial-failure semantics has no atomic
        // validate-then-persist, so a change is DROP + CREATE.
        "ALTER" if second_word_is(sql, "DATASOURCE") => Err(ParseError::Unsupported(
            "ALTER DATASOURCE: an in-place repoint has no atomic validate-then-persist \
             step; change a datasource with DROP DATASOURCE then CREATE DATASOURCE"
                .to_string(),
        )),
        "FUNNEL" => crate::event_statement::classify_funnel(&mut tokens),
        "SEGMENT" => crate::event_statement::classify_segment(&mut tokens),
        "PATHS" => crate::event_statement::classify_paths(&mut tokens),
        // SHOW is ours only for SETTING(S) and MATERIALIZED VIEWS; any other
        // SHOW passes through to the normal parser.
        "SHOW" if second_word_is(sql, "SETTINGS") || second_word_is(sql, "SETTING") => {
            classify_show(&mut tokens)
        }
        "SHOW" if second_word_is(sql, "MATERIALIZED") => classify_show_materialized(&mut tokens),
        "SHOW" if second_word_is(sql, "DATASOURCES") => classify_show_datasources(&mut tokens),
        // The engine has no SET/RESET beyond the settings surface, so every one
        // is ours (an unknown name then raises loudly at the runtime).
        "SET" => classify_set(sql, &mut tokens),
        "RESET" => classify_reset(&mut tokens),
        _ => Ok(Statement::Query(sql)),
    }
}

/// Parse `SHOW SETTINGS` (all settings) or `SHOW SETTING <name>` (one setting).
/// A trailing token after `SETTINGS`, or a missing name after `SETTING`, raises.
fn classify_show<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("SHOW")?;
    if tokens.peek_word_is("SETTINGS") {
        tokens.expect_keyword("SETTINGS")?;
        expect_end(tokens, "SHOW SETTINGS")?;
        return Ok(Statement::ShowSettings);
    }
    tokens.expect_keyword("SETTING")?;
    let name = dotted_name(tokens)?;
    expect_end(tokens, "SHOW SETTING <name>")?;
    Ok(Statement::ShowSetting { name })
}

/// Parse `SHOW MATERIALIZED VIEWS`. A trailing token, or `VIEW` (singular, no
/// per-view SHOW form exists), raises rather than being silently accepted.
fn classify_show_materialized<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("SHOW")?;
    tokens.expect_keyword("MATERIALIZED")?;
    tokens.expect_keyword("VIEWS")?;
    expect_end(tokens, "SHOW MATERIALIZED VIEWS")?;
    Ok(Statement::ShowMaterializedViews)
}

/// The connector kinds a `CREATE DATASOURCE` may name, the same set the runtime
/// dispatches on. An unknown kind raises at classify.
const DATASOURCE_KINDS: [&str; 6] = [
    "duckdb",
    "postgres",
    "postgresql",
    "clickhouse",
    "mysql",
    "parquet",
];

/// Whether the statement is `CREATE OR REPLACE DATASOURCE ...` (words two, three,
/// four are `OR REPLACE DATASOURCE`), so it can be rejected rather than falling
/// through to the normal parser as an unrecognized query.
fn create_or_replace_datasource(sql: &str) -> bool {
    let mut tokens = Tokenizer::new(sql);
    tokens.next_token();
    for expected in ["OR", "REPLACE", "DATASOURCE"] {
        match tokens.peek_word() {
            Some(word) if word.eq_ignore_ascii_case(expected) => {
                tokens.next_token();
            }
            _ => return false,
        }
    }
    true
}

/// Parse `CREATE DATASOURCE <name> TYPE <kind> WITH ( key = 'value', ... )`,
/// raising on IF NOT EXISTS and on an unknown connector kind.
fn classify_create_datasource<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("CREATE")?;
    tokens.expect_keyword("DATASOURCE")?;
    if tokens.peek_word_is("IF") {
        return Err(ParseError::Unsupported(
            "CREATE DATASOURCE IF NOT EXISTS: a create that silently does nothing \
             hides a name collision; create without IF NOT EXISTS"
                .to_string(),
        ));
    }
    let name = view_name(tokens, "datasource")?;
    tokens.expect_keyword("TYPE")?;
    let kind = datasource_kind(tokens)?;
    tokens.expect_keyword("WITH")?;
    let params = parse_with_params(tokens)?;
    expect_end(tokens, "CREATE DATASOURCE")?;
    Ok(Statement::CreateDatasource { name, kind, params })
}

/// Read the connector kind after `TYPE`: one bare word, lowercased, checked
/// against the supported set. An unknown kind raises naming it.
fn datasource_kind(tokens: &mut Tokenizer<'_>) -> Result<String, ParseError> {
    let kind = match tokens.next_token() {
        Some(Token::Word(word)) => word.to_lowercase(),
        Some(token) => {
            return Err(ParseError::Parse(format!(
                "expected a datasource kind after TYPE, found '{}'",
                token.describe()
            )))
        }
        None => {
            return Err(ParseError::Parse(
                "expected a datasource kind after TYPE".to_string(),
            ))
        }
    };
    if DATASOURCE_KINDS.contains(&kind.as_str()) {
        return Ok(kind);
    }
    Err(ParseError::Unsupported(format!(
        "datasource kind '{kind}' is not supported; use one of duckdb, postgres, \
         clickhouse, mysql, parquet"
    )))
}

/// Parse the `( key = 'value' [, key = 'value'] )` connection-param list. Keys
/// are bare words, lowercased; a duplicate key raises. Values are string
/// literals or a bare word (a bare integer for a numeric param). An empty list
/// `( )` is accepted; each kind's required params are checked at validation.
fn parse_with_params(tokens: &mut Tokenizer<'_>) -> Result<Vec<(String, String)>, ParseError> {
    expect_char(tokens, '(', "WITH (")?;
    let mut params: Vec<(String, String)> = Vec::new();
    if consume_char(tokens, ')') {
        return Ok(params);
    }
    loop {
        let (key, value) = parse_one_param(tokens)?;
        if params.iter().any(|(existing, _)| existing == &key) {
            return Err(ParseError::Parse(format!(
                "WITH parameter '{key}' is set more than once"
            )));
        }
        params.push((key, value));
        if consume_char(tokens, ')') {
            return Ok(params);
        }
        expect_char(tokens, ',', "WITH parameter list")?;
    }
}

/// Parse one `key = value` connection-param entry.
fn parse_one_param(tokens: &mut Tokenizer<'_>) -> Result<(String, String), ParseError> {
    let key = match tokens.next_token() {
        Some(Token::Word(word)) => word.to_lowercase(),
        Some(token) => {
            return Err(ParseError::Parse(format!(
                "expected a WITH parameter name, found '{}'",
                token.describe()
            )))
        }
        None => {
            return Err(ParseError::Parse(
                "expected a WITH parameter name".to_string(),
            ))
        }
    };
    expect_char(tokens, '=', "WITH parameter")?;
    let value = match tokens.next_token() {
        Some(Token::StringLiteral(text)) => text,
        Some(Token::Word(word)) => word.to_string(),
        Some(token) => {
            return Err(ParseError::Parse(format!(
                "expected a value for WITH parameter '{key}', found '{}'",
                token.describe()
            )))
        }
        None => {
            return Err(ParseError::Parse(format!(
                "expected a value for WITH parameter '{key}'"
            )))
        }
    };
    Ok((key, value))
}

/// Consume the next token, requiring it to be the single character `ch`.
fn expect_char(tokens: &mut Tokenizer<'_>, ch: char, form: &str) -> Result<(), ParseError> {
    match tokens.next_token() {
        Some(Token::Other(found)) if found == ch => Ok(()),
        Some(token) => Err(ParseError::Parse(format!(
            "expected '{ch}' in {form}, found '{}'",
            token.describe()
        ))),
        None => Err(ParseError::Parse(format!(
            "expected '{ch}' in {form}, found end of statement"
        ))),
    }
}

/// Consume the next token if it is the single character `ch`, returning whether
/// it was consumed.
fn consume_char(tokens: &mut Tokenizer<'_>, ch: char) -> bool {
    if matches!(tokens.peek(), Some(Token::Other(found)) if found == ch) {
        tokens.next_token();
        return true;
    }
    false
}

/// Parse `DROP DATASOURCE <name>`, raising on IF EXISTS (a drop that silently
/// does nothing hides a typo'd name) and on any trailing token (CASCADE /
/// RESTRICT, a second name).
fn classify_drop_datasource<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("DROP")?;
    tokens.expect_keyword("DATASOURCE")?;
    if tokens.peek_word_is("IF") {
        return Err(ParseError::Unsupported(
            "DROP DATASOURCE IF EXISTS: a drop that silently does nothing hides a \
             typo'd datasource name; drop without IF EXISTS"
                .to_string(),
        ));
    }
    let name = view_name(tokens, "datasource")?;
    expect_end(tokens, "DROP DATASOURCE")?;
    Ok(Statement::DropDatasource { name })
}

/// Parse `SHOW DATASOURCES`. A trailing token raises rather than being silently
/// accepted.
fn classify_show_datasources<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("SHOW")?;
    tokens.expect_keyword("DATASOURCES")?;
    expect_end(tokens, "SHOW DATASOURCES")?;
    Ok(Statement::ShowDatasources)
}

/// Parse `SET <name> = <value>` / `SET <name> TO <value>`. The name is a dotted
/// identifier; the value is the remaining text, trimmed, with one layer of
/// surrounding quotes removed (the runtime type-checks it against the setting).
fn classify_set<'a>(sql: &'a str, tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("SET")?;
    let name = dotted_name(tokens)?;
    expect_assignment(tokens)?;
    let value = unquote(tokens.rest().trim());
    if value.is_empty() {
        return Err(ParseError::Parse(format!(
            "SET {name} has no value after the '='"
        )));
    }
    // sql is borrowed only to tie the returned Statement's lifetime to the input;
    // SET carries owned strings, so nothing of sql is retained.
    let _ = sql;
    Ok(Statement::SetSetting { name, value })
}

/// Consume the `=` or `TO` that separates a SET name from its value.
fn expect_assignment(tokens: &mut Tokenizer<'_>) -> Result<(), ParseError> {
    if tokens.peek_word_is("TO") {
        tokens.expect_keyword("TO")?;
        return Ok(());
    }
    match tokens.next_token() {
        Some(Token::Other('=')) => Ok(()),
        Some(token) => Err(ParseError::Parse(format!(
            "expected '=' or 'TO' in SET, found '{}'",
            token.describe()
        ))),
        None => Err(ParseError::Parse(
            "expected '=' or 'TO' in SET, found end of statement".to_string(),
        )),
    }
}

/// Parse `RESET <name>` or `RESET ALL`. `ALL` yields `None` (reset every
/// override); a name yields `Some(name)`.
fn classify_reset<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("RESET")?;
    if tokens.peek_word_is("ALL") {
        tokens.expect_keyword("ALL")?;
        expect_end(tokens, "RESET ALL")?;
        return Ok(Statement::ResetSetting { name: None });
    }
    let name = dotted_name(tokens)?;
    expect_end(tokens, "RESET <name>")?;
    Ok(Statement::ResetSetting { name: Some(name) })
}

/// Read a dotted setting name (`word` or `word.word.word`), lowercasing each
/// bare word (setting names are lowercase). A missing first word raises.
fn dotted_name(tokens: &mut Tokenizer<'_>) -> Result<String, ParseError> {
    let mut parts = vec![name_word(tokens)?];
    while matches!(tokens.peek(), Some(Token::Other('.'))) {
        tokens.next_token();
        parts.push(name_word(tokens)?);
    }
    Ok(parts.join("."))
}

/// Read one identifier word of a dotted name, lowercased.
fn name_word(tokens: &mut Tokenizer<'_>) -> Result<String, ParseError> {
    match tokens.next_token() {
        Some(Token::Word(word)) => Ok(word.to_lowercase()),
        Some(token) => Err(ParseError::Parse(format!(
            "expected a setting name, found '{}'",
            token.describe()
        ))),
        None => Err(ParseError::Parse(
            "expected a setting name, found end of statement".to_string(),
        )),
    }
}

/// Strip one matching layer of single or double quotes from a SET value, so
/// `'text'` and `"text"` yield `text` while a bare `100000` is untouched.
fn unquote(value: &str) -> String {
    let bytes = value.as_bytes();
    let quoted = value.len() >= 2
        && (bytes[0] == b'\'' || bytes[0] == b'"')
        && bytes[bytes.len() - 1] == bytes[0];
    if quoted {
        return value[1..value.len() - 1].to_string();
    }
    value.to_string()
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
    let name = view_name(tokens, "materialized view")?;
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
    let name = view_name(tokens, "materialized view")?;
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
    let name = view_name(tokens, "materialized view")?;
    expect_end(tokens, "DROP MATERIALIZED VIEW")?;
    Ok(Statement::DropMaterializedView { name })
}

/// Read a view name: a single unqualified identifier. An unquoted name
/// lowercases (the Postgres identifier rule); a double-quoted name keeps its
/// exact spelling. A qualified name (`schema.view`) raises: the view store is
/// a single flat namespace. `what` names the object kind in error text.
pub(crate) fn view_name(tokens: &mut Tokenizer<'_>, what: &str) -> Result<String, ParseError> {
    let name = match tokens.next_token() {
        Some(Token::Word(word)) => word.to_lowercase(),
        Some(Token::QuotedIdent(name)) => name,
        Some(Token::Other(ch)) => {
            return Err(ParseError::Parse(format!(
                "expected a {what} name, found '{ch}'"
            )))
        }
        Some(Token::StringLiteral(_)) | None => {
            return Err(ParseError::Parse(format!("expected a {what} name")))
        }
    };
    if name.is_empty() {
        return Err(ParseError::Parse(format!("{what} name is empty")));
    }
    if matches!(tokens.peek(), Some(Token::Other('.'))) {
        return Err(ParseError::Unsupported(format!(
            "{what} name '{name}...' is schema-qualified; the view \
             store is a single flat namespace, use an unqualified name"
        )));
    }
    Ok(name)
}

/// Raise if any token follows where the statement must end; a trailing token is
/// an option this statement form does not take.
pub(crate) fn expect_end(tokens: &mut Tokenizer<'_>, form: &str) -> Result<(), ParseError> {
    match tokens.next_token() {
        None => Ok(()),
        Some(token) => Err(ParseError::Unsupported(format!(
            "{form} takes no further tokens; unexpected trailing '{}'",
            token.describe()
        ))),
    }
}

/// One lexical token of the statement surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Token<'a> {
    /// A bare word (identifier or keyword), as written.
    Word(&'a str),
    /// A `"double quoted"` identifier with `""` escapes resolved.
    QuotedIdent(String),
    /// A `'single quoted'` string literal with `''` escapes resolved (funnel
    /// steps and segment event filters read the content).
    StringLiteral(String),
    /// Any other single character (punctuation, operator).
    Other(char),
}

impl Token<'_> {
    /// A short rendering for error messages.
    pub(crate) fn describe(&self) -> String {
        match self {
            Token::Word(word) => (*word).to_string(),
            Token::QuotedIdent(name) => format!("\"{name}\""),
            Token::StringLiteral(content) => format!("'{content}'"),
            Token::Other(ch) => ch.to_string(),
        }
    }
}

/// A minimal quote-aware SQL tokenizer: words, double-quoted identifiers,
/// single-quoted string literals, and single punctuation characters. Enough to
/// recognize the DDL keyword shapes without misreading quoted text; a statement
/// ending inside an unterminated quote raises.
pub(crate) struct Tokenizer<'a> {
    input: &'a str,
    /// Byte offset of the next unread character.
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    /// Start tokenizing at the beginning of `input`.
    pub(crate) fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    /// The unread remainder of the input (used for the `AS <select>` tail).
    pub(crate) fn rest(&self) -> &'a str {
        &self.input[self.pos..]
    }

    /// Advance past whitespace.
    fn skip_whitespace(&mut self) {
        let remainder = &self.input[self.pos..];
        let trimmed = remainder.trim_start();
        self.pos += remainder.len() - trimmed.len();
    }

    /// The next token, or None at end of input.
    pub(crate) fn next_token(&mut self) -> Option<Token<'a>> {
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
    pub(crate) fn peek(&mut self) -> Option<Token<'a>> {
        let saved = self.pos;
        let token = self.next_token();
        self.pos = saved;
        token
    }

    /// Peek the next token if it is a bare word.
    pub(crate) fn peek_word(&mut self) -> Option<&'a str> {
        match self.peek() {
            Some(Token::Word(word)) => Some(word),
            _ => None,
        }
    }

    /// Whether the next token is the bare word `keyword` (case-insensitive).
    pub(crate) fn peek_word_is(&mut self, keyword: &str) -> bool {
        match self.peek_word() {
            Some(word) => word.eq_ignore_ascii_case(keyword),
            None => false,
        }
    }

    /// Consume the next token, requiring it to be `keyword` (case-insensitive).
    pub(crate) fn expect_keyword(&mut self, keyword: &str) -> Result<(), ParseError> {
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
            Token::StringLiteral(content)
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

    #[test]
    fn show_settings_lists_all() {
        assert_eq!(classify("SHOW SETTINGS"), Statement::ShowSettings);
        assert_eq!(classify("show settings"), Statement::ShowSettings);
    }

    #[test]
    fn show_materialized_views_classifies() {
        assert_eq!(
            classify("SHOW MATERIALIZED VIEWS"),
            Statement::ShowMaterializedViews
        );
        assert_eq!(
            classify("show materialized views"),
            Statement::ShowMaterializedViews
        );
    }

    #[test]
    fn show_materialized_view_singular_raises() {
        // No per-view SHOW form: the singular keyword is not silently accepted.
        let error = classify_statement("SHOW MATERIALIZED VIEW").unwrap_err();
        assert!(matches!(error, ParseError::Parse(_)));
    }

    #[test]
    fn show_materialized_views_with_trailing_token_raises() {
        let error = classify_statement("SHOW MATERIALIZED VIEWS extra").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(_)));
    }

    #[test]
    fn show_setting_extracts_a_dotted_name() {
        assert_eq!(
            classify("SHOW SETTING optimizer.planning_budget_ms"),
            Statement::ShowSetting {
                name: "optimizer.planning_budget_ms".to_string()
            }
        );
    }

    #[test]
    fn show_setting_without_a_name_raises() {
        let error = classify_statement("SHOW SETTING").unwrap_err();
        assert!(matches!(error, ParseError::Parse(_)));
    }

    #[test]
    fn show_settings_with_trailing_token_raises() {
        let error = classify_statement("SHOW SETTINGS extra").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(_)));
    }

    #[test]
    fn other_show_stays_a_query() {
        // SHOW that is not SHOW SETTING(S) passes through to the normal parser.
        assert_eq!(
            classify("SHOW search_path"),
            Statement::Query("SHOW search_path")
        );
    }

    #[test]
    fn set_extracts_name_and_value() {
        assert_eq!(
            classify("SET optimizer.ship_local_floor = 5000"),
            Statement::SetSetting {
                name: "optimizer.ship_local_floor".to_string(),
                value: "5000".to_string()
            }
        );
    }

    #[test]
    fn set_accepts_the_to_keyword() {
        assert_eq!(
            classify("SET optimizer.enable_join_reordering TO false"),
            Statement::SetSetting {
                name: "optimizer.enable_join_reordering".to_string(),
                value: "false".to_string()
            }
        );
    }

    #[test]
    fn set_strips_surrounding_quotes() {
        assert_eq!(
            classify("SET cost.network_rtt_ms = '12.5'"),
            Statement::SetSetting {
                name: "cost.network_rtt_ms".to_string(),
                value: "12.5".to_string()
            }
        );
    }

    #[test]
    fn set_without_a_value_raises() {
        let error = classify_statement("SET optimizer.ship_local_floor =").unwrap_err();
        assert!(matches!(error, ParseError::Parse(_)));
    }

    #[test]
    fn set_without_assignment_raises() {
        let error = classify_statement("SET optimizer.ship_local_floor 5000").unwrap_err();
        assert!(matches!(error, ParseError::Parse(_)));
    }

    #[test]
    fn reset_extracts_a_name() {
        assert_eq!(
            classify("RESET optimizer.ship_local_floor"),
            Statement::ResetSetting {
                name: Some("optimizer.ship_local_floor".to_string())
            }
        );
    }

    #[test]
    fn reset_all_has_no_name() {
        assert_eq!(
            classify("RESET ALL"),
            Statement::ResetSetting { name: None }
        );
    }

    #[test]
    fn reset_with_trailing_token_raises() {
        let error = classify_statement("RESET optimizer.ship_local_floor now").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(_)));
    }

    #[test]
    fn create_datasource_extracts_name_kind_and_params() {
        let statement = classify(
            "CREATE DATASOURCE sales TYPE postgres WITH (host = 'pg1', port = 5432, \
             schemas = 'public,inventory')",
        );
        assert_eq!(
            statement,
            Statement::CreateDatasource {
                name: "sales".to_string(),
                kind: "postgres".to_string(),
                params: vec![
                    ("host".to_string(), "pg1".to_string()),
                    ("port".to_string(), "5432".to_string()),
                    ("schemas".to_string(), "public,inventory".to_string()),
                ],
            }
        );
    }

    #[test]
    fn create_datasource_lowercases_name_and_keys() {
        let statement = classify("CREATE DATASOURCE Sales TYPE DuckDB WITH (PATH = '/tmp/a.db')");
        assert_eq!(
            statement,
            Statement::CreateDatasource {
                name: "sales".to_string(),
                kind: "duckdb".to_string(),
                params: vec![("path".to_string(), "/tmp/a.db".to_string())],
            }
        );
    }

    #[test]
    fn create_datasource_keeps_a_quoted_name_exact() {
        let statement = classify("CREATE DATASOURCE \"Sales\" TYPE duckdb WITH (path = '/a')");
        assert!(matches!(
            statement,
            Statement::CreateDatasource { ref name, .. } if name == "Sales"
        ));
    }

    #[test]
    fn create_datasource_if_not_exists_raises() {
        let error =
            classify_statement("CREATE DATASOURCE IF NOT EXISTS d TYPE duckdb WITH (path='/a')")
                .unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("IF NOT EXISTS")));
    }

    #[test]
    fn create_datasource_or_replace_raises() {
        let error =
            classify_statement("CREATE OR REPLACE DATASOURCE d TYPE duckdb WITH (path='/a')")
                .unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("OR REPLACE")));
    }

    #[test]
    fn create_datasource_unknown_kind_raises() {
        let error =
            classify_statement("CREATE DATASOURCE d TYPE oracle WITH (path='/a')").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("oracle")));
    }

    #[test]
    fn create_datasource_qualified_name_raises() {
        let error =
            classify_statement("CREATE DATASOURCE a.b TYPE duckdb WITH (path='/a')").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("qualified")));
    }

    #[test]
    fn create_datasource_duplicate_param_raises() {
        let error =
            classify_statement("CREATE DATASOURCE d TYPE duckdb WITH (path='/a', path='/b')")
                .unwrap_err();
        assert!(matches!(error, ParseError::Parse(ref m) if m.contains("more than once")));
    }

    #[test]
    fn create_datasource_empty_param_list_classifies() {
        let statement = classify("CREATE DATASOURCE d TYPE duckdb WITH ()");
        assert!(matches!(
            statement,
            Statement::CreateDatasource { ref params, .. } if params.is_empty()
        ));
    }

    #[test]
    fn alter_datasource_raises() {
        let error = classify_statement("ALTER DATASOURCE d TYPE duckdb").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("ALTER DATASOURCE")));
    }

    #[test]
    fn drop_datasource_extracts_the_name() {
        assert_eq!(
            classify("DROP DATASOURCE sales"),
            Statement::DropDatasource {
                name: "sales".to_string()
            }
        );
    }

    #[test]
    fn drop_datasource_if_exists_raises() {
        let error = classify_statement("DROP DATASOURCE IF EXISTS d").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("IF EXISTS")));
    }

    #[test]
    fn drop_datasource_cascade_raises() {
        for sql in ["DROP DATASOURCE d CASCADE", "DROP DATASOURCE d RESTRICT"] {
            let error = classify_statement(sql).unwrap_err();
            assert!(matches!(error, ParseError::Unsupported(_)), "{sql}");
        }
    }

    #[test]
    fn show_datasources_classifies() {
        assert_eq!(classify("SHOW DATASOURCES"), Statement::ShowDatasources);
        assert_eq!(classify("show datasources"), Statement::ShowDatasources);
    }

    #[test]
    fn show_datasources_with_trailing_token_raises() {
        let error = classify_statement("SHOW DATASOURCES extra").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(_)));
    }
}
