//! The event-analytics statement grammars: EVENT VIEW DDL and the FUNNEL /
//! SEGMENT / PATHS analysis statements, parsed with the same quote-aware
//! tokenizer as the materialized-view DDL.
//!
//! Clauses appear in a FIXED order (the grammars below); a missing, misplaced,
//! or unknown clause raises naming what was expected. Surfaces that are
//! designed but not implemented raise `ParseError::Unsupported` naming
//! themselves: property WHERE filters and property GROUP BY, and FROM/TO time
//! ranges on the analyses (the interim expression of a filter is the event
//! view's defining SELECT).

use fq_common::events::{
    EventRoleColumns, EventWindow, FunnelSpec, PathsSpec, SegmentMeasure, SegmentSpec, TimeBucket,
    WindowUnit,
};

use crate::error::ParseError;
use crate::statement::{expect_end, view_name, Statement, Token, Tokenizer};

/// Parse `CREATE EVENT VIEW <name> ENTITY <col> TIMESTAMP <col> EVENT <col>
/// [TIEBREAK <col>] AS <select>`. The three leading role clauses are
/// required, in that order, each naming one output column of the defining
/// SELECT; TIEBREAK optionally names a fourth column that orders events
/// sharing an (entity, timestamp) pair. Every other output column is a
/// property.
pub(crate) fn classify_create_event<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("CREATE")?;
    tokens.expect_keyword("EVENT")?;
    tokens.expect_keyword("VIEW")?;
    if tokens.peek_word_is("IF") {
        return Err(ParseError::Unsupported(
            "CREATE EVENT VIEW IF NOT EXISTS: a create that silently does nothing \
             hides a name collision; create without IF NOT EXISTS"
                .to_string(),
        ));
    }
    let name = view_name(tokens, "event view")?;
    let roles = role_columns(tokens)?;
    tokens.expect_keyword("AS")?;
    let select_sql = tokens.rest().trim();
    if select_sql.is_empty() {
        return Err(ParseError::Parse(
            "CREATE EVENT VIEW has no defining SELECT after AS".to_string(),
        ));
    }
    Ok(Statement::CreateEventView {
        name,
        roles,
        select_sql,
    })
}

/// Parse the fixed-order role clauses: `ENTITY <col> TIMESTAMP <col> EVENT
/// <col> [TIEBREAK <col>]`.
fn role_columns(tokens: &mut Tokenizer<'_>) -> Result<EventRoleColumns, ParseError> {
    tokens.expect_keyword("ENTITY")?;
    let entity = column_name(tokens, "ENTITY")?;
    tokens.expect_keyword("TIMESTAMP")?;
    let timestamp = column_name(tokens, "TIMESTAMP")?;
    tokens.expect_keyword("EVENT")?;
    let event = column_name(tokens, "EVENT")?;
    let tiebreak = if tokens.peek_word_is("TIEBREAK") {
        tokens.expect_keyword("TIEBREAK")?;
        Some(column_name(tokens, "TIEBREAK")?)
    } else {
        None
    };
    Ok(EventRoleColumns {
        entity,
        timestamp,
        event,
        tiebreak,
    })
}

/// Read one role's column name: an unquoted identifier lowercases, a
/// double-quoted one keeps its exact spelling (the same rule as view names).
fn column_name(tokens: &mut Tokenizer<'_>, role: &str) -> Result<String, ParseError> {
    match tokens.next_token() {
        Some(Token::Word(word)) => Ok(word.to_lowercase()),
        Some(Token::QuotedIdent(name)) => Ok(name),
        Some(token) => Err(ParseError::Parse(format!(
            "{role} takes a column name, found '{}'",
            token.describe()
        ))),
        None => Err(ParseError::Parse(format!(
            "{role} takes a column name, found end of statement"
        ))),
    }
}

/// Parse `REFRESH EVENT VIEW <name>`: re-executes the stored SELECT and
/// re-applies the whole sort contract; there are no refresh options.
pub(crate) fn classify_refresh_event<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("REFRESH")?;
    tokens.expect_keyword("EVENT")?;
    tokens.expect_keyword("VIEW")?;
    let name = view_name(tokens, "event view")?;
    expect_end(tokens, "REFRESH EVENT VIEW")?;
    Ok(Statement::RefreshEventView { name })
}

/// Parse `DROP EVENT VIEW <name>`, raising on IF EXISTS (a drop that silently
/// does nothing hides a typo'd name) and on any trailing tokens.
pub(crate) fn classify_drop_event<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("DROP")?;
    tokens.expect_keyword("EVENT")?;
    tokens.expect_keyword("VIEW")?;
    if tokens.peek_word_is("IF") {
        return Err(ParseError::Unsupported(
            "DROP EVENT VIEW IF EXISTS: a drop that silently does nothing hides a \
             typo'd view name; drop without IF EXISTS"
                .to_string(),
        ));
    }
    let name = view_name(tokens, "event view")?;
    expect_end(tokens, "DROP EVENT VIEW")?;
    Ok(Statement::DropEventView { name })
}

/// Parse `FUNNEL OVER <view> STEPS ('a', 'b', ...) WITHIN <n> <unit>`.
pub(crate) fn classify_funnel<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("FUNNEL")?;
    tokens.expect_keyword("OVER")?;
    let view = view_name(tokens, "event view")?;
    tokens.expect_keyword("STEPS")?;
    let steps = step_list(tokens)?;
    tokens.expect_keyword("WITHIN")?;
    let within = window_duration(tokens)?;
    reject_designed_clauses(tokens, "FUNNEL")?;
    expect_end(tokens, "FUNNEL")?;
    Ok(Statement::Funnel(FunnelSpec {
        view,
        steps,
        within,
    }))
}

/// Parse the parenthesized step list: 2 to 16 single-quoted event names,
/// comma-separated.
fn step_list(tokens: &mut Tokenizer<'_>) -> Result<Vec<String>, ParseError> {
    expect_char(
        tokens,
        '(',
        "STEPS takes a parenthesized list of event names",
    )?;
    let mut steps = vec![step_name(tokens)?];
    loop {
        match tokens.next_token() {
            Some(Token::Other(',')) => steps.push(step_name(tokens)?),
            Some(Token::Other(')')) => break,
            Some(token) => {
                return Err(ParseError::Parse(format!(
                    "STEPS list expects ',' or ')', found '{}'",
                    token.describe()
                )))
            }
            None => {
                return Err(ParseError::Parse(
                    "STEPS list is not closed with ')'".to_string(),
                ))
            }
        }
    }
    if !(2..=16).contains(&steps.len()) {
        return Err(ParseError::Parse(format!(
            "a funnel takes 2 to 16 steps, got {}",
            steps.len()
        )));
    }
    Ok(steps)
}

/// Read one step's event name: a single-quoted string literal.
fn step_name(tokens: &mut Tokenizer<'_>) -> Result<String, ParseError> {
    match tokens.next_token() {
        Some(Token::StringLiteral(name)) => Ok(name),
        Some(token) => Err(ParseError::Parse(format!(
            "a funnel step is a quoted event name like 'signup', found '{}'",
            token.describe()
        ))),
        None => Err(ParseError::Parse(
            "a funnel step is a quoted event name like 'signup', found end of \
             statement"
                .to_string(),
        )),
    }
}

/// Parse `WITHIN`'s duration: a positive integer count and a unit word.
fn window_duration(tokens: &mut Tokenizer<'_>) -> Result<EventWindow, ParseError> {
    Ok(EventWindow {
        count: positive_count(tokens, "WITHIN")?,
        unit: window_unit(tokens)?,
    })
}

/// Read the window unit word (singular or plural accepted).
fn window_unit(tokens: &mut Tokenizer<'_>) -> Result<WindowUnit, ParseError> {
    let Some(Token::Word(word)) = tokens.next_token() else {
        return Err(ParseError::Parse(
            "WITHIN takes a unit: SECONDS, MINUTES, HOURS, or DAYS".to_string(),
        ));
    };
    match word.to_ascii_uppercase().as_str() {
        "SECOND" | "SECONDS" => Ok(WindowUnit::Seconds),
        "MINUTE" | "MINUTES" => Ok(WindowUnit::Minutes),
        "HOUR" | "HOURS" => Ok(WindowUnit::Hours),
        "DAY" | "DAYS" => Ok(WindowUnit::Days),
        other => Err(ParseError::Parse(format!(
            "WITHIN unit must be SECONDS, MINUTES, HOURS, or DAYS, found '{other}'"
        ))),
    }
}

/// Parse `SEGMENT OVER <view> MEASURE EVENTS|ENTITIES [EVENT '<name>']
/// BY HOUR|DAY|WEEK|MONTH`.
pub(crate) fn classify_segment<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("SEGMENT")?;
    tokens.expect_keyword("OVER")?;
    let view = view_name(tokens, "event view")?;
    tokens.expect_keyword("MEASURE")?;
    let measure = segment_measure(tokens)?;
    let event = optional_event_filter(tokens)?;
    tokens.expect_keyword("BY")?;
    let bucket = time_bucket(tokens)?;
    reject_designed_clauses(tokens, "SEGMENT")?;
    expect_end(tokens, "SEGMENT")?;
    Ok(Statement::Segment(SegmentSpec {
        view,
        measure,
        event,
        bucket,
    }))
}

/// Read the measure word: EVENTS (event count) or ENTITIES (distinct
/// entities).
fn segment_measure(tokens: &mut Tokenizer<'_>) -> Result<SegmentMeasure, ParseError> {
    let Some(Token::Word(word)) = tokens.next_token() else {
        return Err(ParseError::Parse(
            "MEASURE takes EVENTS or ENTITIES".to_string(),
        ));
    };
    match word.to_ascii_uppercase().as_str() {
        "EVENTS" => Ok(SegmentMeasure::Events),
        "ENTITIES" => Ok(SegmentMeasure::Entities),
        other => Err(ParseError::Parse(format!(
            "MEASURE must be EVENTS or ENTITIES, found '{other}'"
        ))),
    }
}

/// Read the optional `EVENT '<name>'` filter clause.
fn optional_event_filter(tokens: &mut Tokenizer<'_>) -> Result<Option<String>, ParseError> {
    if !tokens.peek_word_is("EVENT") {
        return Ok(None);
    }
    tokens.expect_keyword("EVENT")?;
    match tokens.next_token() {
        Some(Token::StringLiteral(name)) => Ok(Some(name)),
        Some(token) => Err(ParseError::Parse(format!(
            "EVENT takes a quoted event name like 'purchase', found '{}'",
            token.describe()
        ))),
        None => Err(ParseError::Parse(
            "EVENT takes a quoted event name like 'purchase', found end of statement".to_string(),
        )),
    }
}

/// Read the bucket word: HOUR, DAY, WEEK, or MONTH.
fn time_bucket(tokens: &mut Tokenizer<'_>) -> Result<TimeBucket, ParseError> {
    let Some(Token::Word(word)) = tokens.next_token() else {
        return Err(ParseError::Parse(
            "BY takes a time bucket: HOUR, DAY, WEEK, or MONTH".to_string(),
        ));
    };
    match word.to_ascii_uppercase().as_str() {
        "HOUR" => Ok(TimeBucket::Hour),
        "DAY" => Ok(TimeBucket::Day),
        "WEEK" => Ok(TimeBucket::Week),
        "MONTH" => Ok(TimeBucket::Month),
        other => Err(ParseError::Parse(format!(
            "BY bucket must be HOUR, DAY, WEEK, or MONTH, found '{other}'"
        ))),
    }
}

/// Parse `PATHS OVER <view> [STARTING AT '<name>'] MAX DEPTH <n> TOP <k>`.
pub(crate) fn classify_paths<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("PATHS")?;
    tokens.expect_keyword("OVER")?;
    let view = view_name(tokens, "event view")?;
    let starting_at = optional_starting_at(tokens)?;
    tokens.expect_keyword("MAX")?;
    tokens.expect_keyword("DEPTH")?;
    let max_depth = positive_count(tokens, "MAX DEPTH")?;
    tokens.expect_keyword("TOP")?;
    let top = positive_count(tokens, "TOP")?;
    reject_designed_clauses(tokens, "PATHS")?;
    expect_end(tokens, "PATHS")?;
    Ok(Statement::Paths(PathsSpec {
        view,
        starting_at,
        max_depth,
        top,
    }))
}

/// Read the optional `STARTING AT '<name>'` anchor clause.
fn optional_starting_at(tokens: &mut Tokenizer<'_>) -> Result<Option<String>, ParseError> {
    if !tokens.peek_word_is("STARTING") {
        return Ok(None);
    }
    tokens.expect_keyword("STARTING")?;
    tokens.expect_keyword("AT")?;
    match tokens.next_token() {
        Some(Token::StringLiteral(name)) => Ok(Some(name)),
        Some(token) => Err(ParseError::Parse(format!(
            "STARTING AT takes a quoted event name like 'signup', found '{}'",
            token.describe()
        ))),
        None => Err(ParseError::Parse(
            "STARTING AT takes a quoted event name like 'signup', found end of \
             statement"
                .to_string(),
        )),
    }
}

/// Read one clause's positive integer count, naming the clause on failure.
fn positive_count(tokens: &mut Tokenizer<'_>, clause: &str) -> Result<i64, ParseError> {
    let count = match tokens.next_token() {
        Some(Token::Word(word)) => word.parse::<i64>().map_err(|_| {
            ParseError::Parse(format!("{clause} takes an integer count, found '{word}'"))
        })?,
        Some(token) => {
            return Err(ParseError::Parse(format!(
                "{clause} takes an integer count, found '{}'",
                token.describe()
            )))
        }
        None => {
            return Err(ParseError::Parse(format!(
                "{clause} takes an integer count, found end of statement"
            )))
        }
    };
    if count <= 0 {
        return Err(ParseError::Parse(format!(
            "{clause} needs a positive count, got {count}"
        )));
    }
    Ok(count)
}

/// Raise on the clauses that are designed but not implemented, naming each:
/// property filters (WHERE), property group-by (GROUP BY), and FROM/TO time
/// ranges. An event view whose defining SELECT carries the filter is the
/// implemented expression of the same intent.
fn reject_designed_clauses(tokens: &mut Tokenizer<'_>, form: &str) -> Result<(), ParseError> {
    let designed = [
        (
            "WHERE",
            "property filters are not implemented; put the filter in the event \
             view's defining SELECT",
        ),
        ("GROUP", "property group-by is not implemented"),
        ("FROM", "FROM/TO time ranges are not implemented"),
    ];
    for (keyword, message) in designed {
        if tokens.peek_word_is(keyword) {
            return Err(ParseError::Unsupported(format!(
                "{form} ... {keyword}: {message}"
            )));
        }
    }
    Ok(())
}

/// Consume one punctuation character or raise `context`.
fn expect_char(
    tokens: &mut Tokenizer<'_>,
    expected: char,
    context: &str,
) -> Result<(), ParseError> {
    match tokens.next_token() {
        Some(Token::Other(ch)) if ch == expected => Ok(()),
        Some(token) => Err(ParseError::Parse(format!(
            "{context}, found '{}'",
            token.describe()
        ))),
        None => Err(ParseError::Parse(format!(
            "{context}, found end of statement"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statement::classify_statement;

    /// Classify and unwrap, panicking with the error for a readable failure.
    fn classify(sql: &str) -> Statement<'_> {
        classify_statement(sql).expect("statement classifies")
    }

    #[test]
    fn create_event_view_extracts_name_roles_and_select() {
        let statement = classify(
            "CREATE EVENT VIEW ev ENTITY user_id TIMESTAMP ts EVENT name \
             AS SELECT user_id, ts, name, device FROM duck.main.events",
        );
        assert_eq!(
            statement,
            Statement::CreateEventView {
                name: "ev".to_string(),
                roles: EventRoleColumns {
                    entity: "user_id".to_string(),
                    timestamp: "ts".to_string(),
                    event: "name".to_string(),
                    tiebreak: None,
                },
                select_sql: "SELECT user_id, ts, name, device FROM duck.main.events",
            }
        );
    }

    #[test]
    fn create_event_view_takes_an_optional_tiebreak_role() {
        let statement = classify(
            "CREATE EVENT VIEW ev ENTITY user_id TIMESTAMP ts EVENT name TIEBREAK seq \
             AS SELECT user_id, ts, name, seq FROM duck.main.events",
        );
        assert_eq!(
            statement,
            Statement::CreateEventView {
                name: "ev".to_string(),
                roles: EventRoleColumns {
                    entity: "user_id".to_string(),
                    timestamp: "ts".to_string(),
                    event: "name".to_string(),
                    tiebreak: Some("seq".to_string()),
                },
                select_sql: "SELECT user_id, ts, name, seq FROM duck.main.events",
            }
        );
    }

    #[test]
    fn tiebreak_follows_event_and_needs_a_column_name() {
        // TIEBREAK before the EVENT clause is a misplaced clause.
        let misplaced = classify_statement(
            "CREATE EVENT VIEW ev ENTITY u TIMESTAMP t TIEBREAK s EVENT e AS SELECT 1",
        )
        .unwrap_err();
        assert!(
            matches!(misplaced, ParseError::Parse(ref m) if m.contains("EVENT")),
            "{misplaced}"
        );
        // TIEBREAK with no column name raises naming the clause.
        let missing = classify_statement(
            "CREATE EVENT VIEW ev ENTITY u TIMESTAMP t EVENT e TIEBREAK 'seq' AS SELECT 1",
        )
        .unwrap_err();
        assert!(
            matches!(missing, ParseError::Parse(ref m) if m.contains("TIEBREAK")),
            "{missing}"
        );
    }

    #[test]
    fn create_event_view_role_clauses_are_fixed_order() {
        let error = classify_statement(
            "CREATE EVENT VIEW ev TIMESTAMP ts ENTITY user_id EVENT name AS SELECT 1",
        )
        .unwrap_err();
        assert!(
            matches!(error, ParseError::Parse(ref m) if m.contains("ENTITY")),
            "{error}"
        );
    }

    #[test]
    fn create_event_view_if_not_exists_raises() {
        let error = classify_statement(
            "CREATE EVENT VIEW IF NOT EXISTS ev ENTITY u TIMESTAMP t EVENT e AS SELECT 1",
        )
        .unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("IF NOT EXISTS")));
    }

    #[test]
    fn create_event_view_without_select_raises() {
        let error =
            classify_statement("CREATE EVENT VIEW ev ENTITY u TIMESTAMP t EVENT e AS").unwrap_err();
        assert!(matches!(error, ParseError::Parse(_)));
    }

    #[test]
    fn refresh_and_drop_event_view_extract_the_name() {
        assert_eq!(
            classify("REFRESH EVENT VIEW ev"),
            Statement::RefreshEventView {
                name: "ev".to_string()
            }
        );
        assert_eq!(
            classify("DROP EVENT VIEW ev"),
            Statement::DropEventView {
                name: "ev".to_string()
            }
        );
    }

    #[test]
    fn drop_event_view_if_exists_raises() {
        let error = classify_statement("DROP EVENT VIEW IF EXISTS ev").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("IF EXISTS")));
    }

    #[test]
    fn funnel_extracts_view_steps_and_window() {
        let statement =
            classify("FUNNEL OVER ev STEPS ('signup', 'activate', 'purchase') WITHIN 7 DAYS");
        assert_eq!(
            statement,
            Statement::Funnel(FunnelSpec {
                view: "ev".to_string(),
                steps: vec![
                    "signup".to_string(),
                    "activate".to_string(),
                    "purchase".to_string(),
                ],
                within: EventWindow {
                    count: 7,
                    unit: WindowUnit::Days,
                },
            })
        );
    }

    #[test]
    fn funnel_step_count_bounds_raise_at_parse() {
        let error = classify_statement("FUNNEL OVER ev STEPS ('a') WITHIN 1 DAY").unwrap_err();
        assert!(
            matches!(error, ParseError::Parse(ref m) if m.contains("2 to 16")),
            "{error}"
        );
    }

    #[test]
    fn funnel_rejects_bad_windows() {
        for sql in [
            "FUNNEL OVER ev STEPS ('a', 'b') WITHIN 0 DAYS",
            "FUNNEL OVER ev STEPS ('a', 'b') WITHIN -3 DAYS",
            "FUNNEL OVER ev STEPS ('a', 'b') WITHIN 3 FORTNIGHTS",
            "FUNNEL OVER ev STEPS ('a', 'b') WITHIN soon DAYS",
        ] {
            assert!(classify_statement(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn funnel_step_names_keep_quoted_content_exactly() {
        let statement = classify("FUNNEL OVER ev STEPS ('Sign Up', 'it''s') WITHIN 1 HOUR");
        let Statement::Funnel(spec) = statement else {
            panic!("expected a funnel");
        };
        assert_eq!(spec.steps, vec!["Sign Up".to_string(), "it's".to_string()]);
    }

    #[test]
    fn segment_extracts_measure_filter_and_bucket() {
        assert_eq!(
            classify("SEGMENT OVER ev MEASURE ENTITIES EVENT 'purchase' BY DAY"),
            Statement::Segment(SegmentSpec {
                view: "ev".to_string(),
                measure: SegmentMeasure::Entities,
                event: Some("purchase".to_string()),
                bucket: TimeBucket::Day,
            })
        );
        assert_eq!(
            classify("SEGMENT OVER ev MEASURE EVENTS BY MONTH"),
            Statement::Segment(SegmentSpec {
                view: "ev".to_string(),
                measure: SegmentMeasure::Events,
                event: None,
                bucket: TimeBucket::Month,
            })
        );
    }

    #[test]
    fn designed_but_unimplemented_clauses_raise_naming_themselves() {
        for (sql, named) in [
            (
                "SEGMENT OVER ev MEASURE EVENTS BY DAY WHERE device = 'ios'",
                "property filters",
            ),
            (
                "SEGMENT OVER ev MEASURE EVENTS BY DAY GROUP BY country",
                "property group-by",
            ),
            (
                "FUNNEL OVER ev STEPS ('a', 'b') WITHIN 1 DAY FROM '2026-01-01'",
                "time ranges",
            ),
        ] {
            let error = classify_statement(sql).unwrap_err();
            assert!(
                matches!(error, ParseError::Unsupported(ref m) if m.contains(named)),
                "{sql}: {error}"
            );
        }
    }

    #[test]
    fn paths_extracts_view_depth_and_top() {
        assert_eq!(
            classify("PATHS OVER ev MAX DEPTH 5 TOP 10"),
            Statement::Paths(PathsSpec {
                view: "ev".to_string(),
                starting_at: None,
                max_depth: 5,
                top: 10,
            })
        );
        assert_eq!(
            classify("PATHS OVER ev STARTING AT 'signup' MAX DEPTH 3 TOP 7"),
            Statement::Paths(PathsSpec {
                view: "ev".to_string(),
                starting_at: Some("signup".to_string()),
                max_depth: 3,
                top: 7,
            })
        );
    }

    #[test]
    fn paths_rejects_bad_counts_and_missing_clauses() {
        for sql in [
            "PATHS OVER ev MAX DEPTH 0 TOP 10",
            "PATHS OVER ev MAX DEPTH -2 TOP 10",
            "PATHS OVER ev MAX DEPTH 5 TOP 0",
            "PATHS OVER ev MAX DEPTH soon TOP 10",
            "PATHS OVER ev TOP 10",
            "PATHS OVER ev MAX DEPTH 5",
            "PATHS OVER ev STARTING AT signup MAX DEPTH 5 TOP 10",
        ] {
            assert!(classify_statement(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn paths_designed_clauses_raise_naming_themselves() {
        let error = classify_statement("PATHS OVER ev MAX DEPTH 5 TOP 10 WHERE device = 'ios'")
            .unwrap_err();
        assert!(
            matches!(error, ParseError::Unsupported(ref m) if m.contains("property filters")),
            "{error}"
        );
        let trailing = classify_statement("PATHS OVER ev MAX DEPTH 5 TOP 10 EXTRA").unwrap_err();
        assert!(matches!(trailing, ParseError::Unsupported(_)), "{trailing}");
    }

    #[test]
    fn create_event_without_view_raises() {
        // Every `CREATE EVENT ...` is claimed by this grammar, so a form the
        // engine does not speak (Postgres CREATE EVENT TRIGGER) raises loudly
        // at the third word instead of passing through half-recognized.
        let error = classify_statement("CREATE EVENT TRIGGER t ON ddl_command_start").unwrap_err();
        assert!(matches!(error, ParseError::Parse(_)), "{error}");
    }
}
