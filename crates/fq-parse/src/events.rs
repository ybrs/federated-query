//! The event-analytics statement grammar: CREATE / REFRESH / REBUILD / DROP /
//! SHOW EVENT DATASET plus the four analysis statements FUNNEL, RETENTION,
//! SEGMENT, and PATHS.
//!
//! The family is lexical, like the materialized-view DDL: it parses over the
//! quote-aware `Tokenizer` and never enters the SQL pipeline. Clause order is
//! fixed as written in each form (single-pass parsing gives precise errors);
//! a missing required clause raises naming it, and a trailing token raises.
//! Event names are quoted string literals (they are data, not identifiers);
//! property and column names are identifiers. The `WHERE` clause is the closed
//! predicate language over declared properties; it is type-checked against the
//! dataset's property schema by the event engine, not here.

use fq_common::events::{
    days_from_civil, BucketGrain, CompareOp, EventDatasetDef, EventMatch, EventPredicate,
    EventSource, FunnelMode, FunnelSpec, PathAnchor, PathsSpec, PeriodGrain, PredicateLiteral,
    RetentionMode, RetentionSpec, SegmentMeasure, SegmentSpec, TimeRange, TimeUnit,
};

use crate::error::ParseError;
use crate::statement::{
    consume_char, consume_keyword, describe_opt, expect_char, expect_end, parse_with_params,
    view_name, Statement, Token, Tokenizer,
};

/// Parse `CREATE EVENT DATASET <name> FROM <ds>.<schema>.<table> | AS (<sql>)
/// ACTOR <c> TIME <c> EVENT <c> [TIEBREAK <c>] [PROPERTIES (...)] [WITH (...)]`.
pub(crate) fn classify_create_event<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("CREATE")?;
    tokens.expect_keyword("EVENT")?;
    tokens.expect_keyword("DATASET")?;
    if tokens.peek_word_is("IF") {
        return Err(ParseError::Unsupported(
            "CREATE EVENT DATASET IF NOT EXISTS: a create that silently does nothing \
             hides a name collision; create without IF NOT EXISTS"
                .to_string(),
        ));
    }
    let name = view_name(tokens, "event dataset")?;
    let source = parse_event_source(tokens)?;
    tokens.expect_keyword("ACTOR")?;
    let actor_column = identifier(tokens, "ACTOR column")?;
    tokens.expect_keyword("TIME")?;
    let time_column = identifier(tokens, "TIME column")?;
    tokens.expect_keyword("EVENT")?;
    let event_column = identifier(tokens, "EVENT column")?;
    let tiebreak_column = if consume_keyword(tokens, "TIEBREAK") {
        Some(identifier(tokens, "TIEBREAK column")?)
    } else {
        None
    };
    let properties = parse_properties(tokens)?;
    let (shards, refresh_key, time_unit) = parse_create_with(tokens)?;
    expect_end(tokens, "CREATE EVENT DATASET")?;
    Ok(Statement::CreateEventDataset(EventDatasetDef {
        name,
        source,
        actor_column,
        time_column,
        event_column,
        tiebreak_column,
        properties,
        shards,
        refresh_key,
        time_unit,
    }))
}

/// Parse the `FROM <ds>.<schema>.<table>` or `AS ( <select sql> )` source of a
/// CREATE EVENT DATASET.
fn parse_event_source(tokens: &mut Tokenizer<'_>) -> Result<EventSource, ParseError> {
    if consume_keyword(tokens, "AS") {
        let sql = tokens.take_parenthesized("CREATE EVENT DATASET AS ( <select> )")?;
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(ParseError::Parse(
                "CREATE EVENT DATASET AS ( ) has an empty defining SELECT".to_string(),
            ));
        }
        return Ok(EventSource::Select {
            sql: sql.to_string(),
        });
    }
    tokens.expect_keyword("FROM")?;
    let datasource = identifier(tokens, "source datasource")?;
    expect_char(tokens, '.', "FROM <datasource>.<schema>.<table>")?;
    let schema = identifier(tokens, "source schema")?;
    expect_char(tokens, '.', "FROM <datasource>.<schema>.<table>")?;
    let table = identifier(tokens, "source table")?;
    Ok(EventSource::Table {
        datasource,
        schema,
        table,
    })
}

/// Parse the optional `PROPERTIES ( <column> [, <column>]* )` clause. The
/// clause absent means "every source column not otherwise named"; an explicit
/// empty list `PROPERTIES ()` means no properties.
fn parse_properties(tokens: &mut Tokenizer<'_>) -> Result<Option<Vec<String>>, ParseError> {
    if !consume_keyword(tokens, "PROPERTIES") {
        return Ok(None);
    }
    expect_char(tokens, '(', "PROPERTIES (")?;
    let mut columns = Vec::new();
    if consume_char(tokens, ')') {
        return Ok(Some(columns));
    }
    loop {
        let column = identifier(tokens, "PROPERTIES column")?;
        if columns.contains(&column) {
            return Err(ParseError::Parse(format!(
                "PROPERTIES lists column '{column}' more than once"
            )));
        }
        columns.push(column);
        if consume_char(tokens, ')') {
            return Ok(Some(columns));
        }
        expect_char(tokens, ',', "PROPERTIES list")?;
    }
}

/// The `WITH` keys a CREATE EVENT DATASET accepts.
const CREATE_WITH_KEYS: [&str; 3] = ["shards", "refresh_key", "time_unit"];

/// Parse the optional `WITH ( key = 'value', ... )` clause of a CREATE EVENT
/// DATASET against the allowlist; an unknown key raises listing the known
/// keys.
#[allow(clippy::type_complexity)]
fn parse_create_with(
    tokens: &mut Tokenizer<'_>,
) -> Result<(Option<u32>, Option<String>, TimeUnit), ParseError> {
    if !consume_keyword(tokens, "WITH") {
        return Ok((None, None, TimeUnit::Micros));
    }
    let params = parse_with_params(tokens)?;
    let mut shards = None;
    let mut refresh_key = None;
    let mut time_unit = TimeUnit::Micros;
    for (key, value) in params {
        match key.as_str() {
            "shards" => shards = Some(parse_shards(&value)?),
            "refresh_key" => refresh_key = Some(value),
            "time_unit" => time_unit = parse_time_unit(&value)?,
            other => {
                return Err(ParseError::Unsupported(format!(
                    "CREATE EVENT DATASET WITH key '{other}' is not supported; known keys \
                     are {}",
                    CREATE_WITH_KEYS.join(", ")
                )))
            }
        }
    }
    Ok((shards, refresh_key, time_unit))
}

/// Parse the `shards` WITH value: a power of two in 1..=65536.
fn parse_shards(value: &str) -> Result<u32, ParseError> {
    let shards: u32 = value
        .parse()
        .map_err(|_| ParseError::Parse(format!("WITH shards value '{value}' is not an integer")))?;
    if shards == 0 || !shards.is_power_of_two() || shards > 65_536 {
        return Err(ParseError::Parse(format!(
            "WITH shards value {shards} must be a power of two in 1..=65536"
        )));
    }
    Ok(shards)
}

/// Parse the `time_unit` WITH value: `us` or `ms`.
fn parse_time_unit(value: &str) -> Result<TimeUnit, ParseError> {
    match value {
        "us" => Ok(TimeUnit::Micros),
        "ms" => Ok(TimeUnit::Millis),
        other => Err(ParseError::Parse(format!(
            "WITH time_unit value '{other}' is not supported; use 'us' or 'ms'"
        ))),
    }
}

/// Parse `REFRESH EVENT DATASET <name>`.
pub(crate) fn classify_refresh_event<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("REFRESH")?;
    tokens.expect_keyword("EVENT")?;
    tokens.expect_keyword("DATASET")?;
    let name = view_name(tokens, "event dataset")?;
    expect_end(tokens, "REFRESH EVENT DATASET")?;
    Ok(Statement::RefreshEventDataset { name })
}

/// Parse `REBUILD EVENT DATASET <name>`. REBUILD exists for nothing but this
/// family, so any other second word raises naming the form.
pub(crate) fn classify_rebuild_event<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("REBUILD")?;
    tokens.expect_keyword("EVENT")?;
    tokens.expect_keyword("DATASET")?;
    let name = view_name(tokens, "event dataset")?;
    expect_end(tokens, "REBUILD EVENT DATASET")?;
    Ok(Statement::RebuildEventDataset { name })
}

/// Parse `DROP EVENT DATASET <name>`.
pub(crate) fn classify_drop_event<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("DROP")?;
    tokens.expect_keyword("EVENT")?;
    tokens.expect_keyword("DATASET")?;
    if tokens.peek_word_is("IF") {
        return Err(ParseError::Unsupported(
            "DROP EVENT DATASET IF EXISTS: a drop that silently does nothing hides a \
             typo'd dataset name; drop without IF EXISTS"
                .to_string(),
        ));
    }
    let name = view_name(tokens, "event dataset")?;
    expect_end(tokens, "DROP EVENT DATASET")?;
    Ok(Statement::DropEventDataset { name })
}

/// Parse `SHOW EVENT DATASETS`.
pub(crate) fn classify_show_event<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("SHOW")?;
    tokens.expect_keyword("EVENT")?;
    tokens.expect_keyword("DATASETS")?;
    expect_end(tokens, "SHOW EVENT DATASETS")?;
    Ok(Statement::ShowEventDatasets)
}

/// Parse `FUNNEL ( <event> [, <event>]+ ) ON <dataset> WINDOW <interval>
/// [MODE ...] [FROM ...] [TO ...] [WHERE ...] [BREAKDOWN BY <property>]`.
pub(crate) fn classify_funnel<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("FUNNEL")?;
    let steps = parse_step_list(tokens)?;
    tokens.expect_keyword("ON")?;
    let dataset = view_name(tokens, "event dataset")?;
    tokens.expect_keyword("WINDOW")?;
    let window_micros = parse_interval(tokens, "WINDOW")?;
    let mode = parse_funnel_mode(tokens)?;
    let range = parse_time_range(tokens)?;
    let filter = parse_where(tokens)?;
    let breakdown = parse_breakdown(tokens)?;
    expect_end(tokens, "FUNNEL")?;
    Ok(Statement::EventFunnel(FunnelSpec {
        dataset,
        steps,
        window_micros,
        mode,
        range,
        filter,
        breakdown,
    }))
}

/// Parse the parenthesized funnel step list: at least two quoted event names.
fn parse_step_list(tokens: &mut Tokenizer<'_>) -> Result<Vec<String>, ParseError> {
    expect_char(tokens, '(', "FUNNEL ( <event>, ... )")?;
    let mut steps = vec![event_literal(tokens, "funnel step")?];
    while consume_char(tokens, ',') {
        steps.push(event_literal(tokens, "funnel step")?);
    }
    expect_char(tokens, ')', "FUNNEL ( <event>, ... )")?;
    if steps.len() < 2 {
        return Err(ParseError::Parse(
            "FUNNEL needs at least two steps".to_string(),
        ));
    }
    Ok(steps)
}

/// Parse the optional `MODE STRICT_ORDER | ANY_ORDER` clause of a funnel.
fn parse_funnel_mode(tokens: &mut Tokenizer<'_>) -> Result<FunnelMode, ParseError> {
    if !consume_keyword(tokens, "MODE") {
        return Ok(FunnelMode::StrictOrder);
    }
    if consume_keyword(tokens, "STRICT_ORDER") {
        return Ok(FunnelMode::StrictOrder);
    }
    if consume_keyword(tokens, "ANY_ORDER") {
        return Ok(FunnelMode::AnyOrder);
    }
    Err(ParseError::Parse(format!(
        "expected STRICT_ORDER or ANY_ORDER after MODE, found '{}'",
        describe_opt(tokens.peek().as_ref())
    )))
}

/// Parse `RETENTION ON <dataset> BIRTH ... RETURN ... PERIOD ... [MODE ...]
/// [PERIODS <n>] [AT <n>] [FROM ...] [TO ...] [WHERE ...] [BREAKDOWN BY ...]`.
pub(crate) fn classify_retention<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("RETENTION")?;
    tokens.expect_keyword("ON")?;
    let dataset = view_name(tokens, "event dataset")?;
    tokens.expect_keyword("BIRTH")?;
    let birth = parse_event_match(tokens, "BIRTH")?;
    tokens.expect_keyword("RETURN")?;
    let return_event = parse_event_match(tokens, "RETURN")?;
    tokens.expect_keyword("PERIOD")?;
    let period = parse_period_grain(tokens)?;
    let mode = parse_retention_mode(tokens)?;
    let periods = parse_optional_count(tokens, "PERIODS")?;
    let at = parse_optional_count(tokens, "AT")?;
    let range = parse_time_range(tokens)?;
    let filter = parse_where(tokens)?;
    let breakdown = parse_breakdown(tokens)?;
    expect_end(tokens, "RETENTION")?;
    Ok(Statement::EventRetention(RetentionSpec {
        dataset,
        birth,
        return_event,
        period,
        mode,
        periods,
        at,
        range,
        filter,
        breakdown,
    }))
}

/// Parse a birth/return spec: `ANY EVENT` or a quoted event name.
fn parse_event_match(tokens: &mut Tokenizer<'_>, clause: &str) -> Result<EventMatch, ParseError> {
    if consume_keyword(tokens, "ANY") {
        tokens.expect_keyword("EVENT")?;
        return Ok(EventMatch::Any);
    }
    Ok(EventMatch::Named(event_literal(
        tokens,
        &format!("{clause} event"),
    )?))
}

/// Parse the `PERIOD DAY | WEEK | MONTH` grain.
fn parse_period_grain(tokens: &mut Tokenizer<'_>) -> Result<PeriodGrain, ParseError> {
    if consume_keyword(tokens, "DAY") {
        return Ok(PeriodGrain::Day);
    }
    if consume_keyword(tokens, "WEEK") {
        return Ok(PeriodGrain::Week);
    }
    if consume_keyword(tokens, "MONTH") {
        return Ok(PeriodGrain::Month);
    }
    Err(ParseError::Parse(format!(
        "expected DAY, WEEK, or MONTH after PERIOD, found '{}'",
        describe_opt(tokens.peek().as_ref())
    )))
}

/// Parse the optional `MODE BOUNDED | UNBOUNDED` clause of a retention.
fn parse_retention_mode(tokens: &mut Tokenizer<'_>) -> Result<RetentionMode, ParseError> {
    if !consume_keyword(tokens, "MODE") {
        return Ok(RetentionMode::Bounded);
    }
    if consume_keyword(tokens, "BOUNDED") {
        return Ok(RetentionMode::Bounded);
    }
    if consume_keyword(tokens, "UNBOUNDED") {
        return Ok(RetentionMode::Unbounded);
    }
    Err(ParseError::Parse(format!(
        "expected BOUNDED or UNBOUNDED after MODE, found '{}'",
        describe_opt(tokens.peek().as_ref())
    )))
}

/// Parse an optional `<keyword> <n>` clause into a non-negative count.
fn parse_optional_count(
    tokens: &mut Tokenizer<'_>,
    keyword: &str,
) -> Result<Option<u32>, ParseError> {
    if !consume_keyword(tokens, keyword) {
        return Ok(None);
    }
    Ok(Some(unsigned_number(tokens, keyword)?))
}

/// Parse `SEGMENT COUNT | UNIQUES | COUNT_PER_UNIQUE ON <dataset>
/// [EVENT <event>] BUCKET <grain> [FROM ...] [TO ...] [WHERE ...]
/// [BREAKDOWN BY ...]`.
pub(crate) fn classify_segment<'a>(
    tokens: &mut Tokenizer<'a>,
) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("SEGMENT")?;
    let measure = parse_segment_measure(tokens)?;
    tokens.expect_keyword("ON")?;
    let dataset = view_name(tokens, "event dataset")?;
    let event = if consume_keyword(tokens, "EVENT") {
        Some(event_literal(tokens, "SEGMENT event")?)
    } else {
        None
    };
    tokens.expect_keyword("BUCKET")?;
    let bucket = parse_bucket_grain(tokens)?;
    let range = parse_time_range(tokens)?;
    let filter = parse_where(tokens)?;
    let breakdown = parse_breakdown(tokens)?;
    expect_end(tokens, "SEGMENT")?;
    Ok(Statement::EventSegment(SegmentSpec {
        dataset,
        measure,
        event,
        bucket,
        range,
        filter,
        breakdown,
    }))
}

/// Parse the segmentation measure keyword.
fn parse_segment_measure(tokens: &mut Tokenizer<'_>) -> Result<SegmentMeasure, ParseError> {
    if consume_keyword(tokens, "COUNT") {
        return Ok(SegmentMeasure::Count);
    }
    if consume_keyword(tokens, "UNIQUES") {
        return Ok(SegmentMeasure::Uniques);
    }
    if consume_keyword(tokens, "COUNT_PER_UNIQUE") {
        return Ok(SegmentMeasure::CountPerUnique);
    }
    Err(ParseError::Parse(format!(
        "expected COUNT, UNIQUES, or COUNT_PER_UNIQUE after SEGMENT, found '{}'",
        describe_opt(tokens.peek().as_ref())
    )))
}

/// Parse the `BUCKET HOUR | DAY | WEEK | MONTH` grain.
fn parse_bucket_grain(tokens: &mut Tokenizer<'_>) -> Result<BucketGrain, ParseError> {
    if consume_keyword(tokens, "HOUR") {
        return Ok(BucketGrain::Hour);
    }
    if consume_keyword(tokens, "DAY") {
        return Ok(BucketGrain::Day);
    }
    if consume_keyword(tokens, "WEEK") {
        return Ok(BucketGrain::Week);
    }
    if consume_keyword(tokens, "MONTH") {
        return Ok(BucketGrain::Month);
    }
    Err(ParseError::Parse(format!(
        "expected HOUR, DAY, WEEK, or MONTH after BUCKET, found '{}'",
        describe_opt(tokens.peek().as_ref())
    )))
}

/// Parse `PATHS ON <dataset> [STARTING AT <event> | ENDING AT <event>]
/// [DEPTH <n>] [TOP <n>] [MAXGAP <interval>] [FROM ...] [TO ...] [WHERE ...]`.
pub(crate) fn classify_paths<'a>(tokens: &mut Tokenizer<'a>) -> Result<Statement<'a>, ParseError> {
    tokens.expect_keyword("PATHS")?;
    tokens.expect_keyword("ON")?;
    let dataset = view_name(tokens, "event dataset")?;
    let anchor = parse_path_anchor(tokens)?;
    let depth = parse_optional_count(tokens, "DEPTH")?.unwrap_or(5);
    let top = parse_optional_count(tokens, "TOP")?.unwrap_or(20);
    let maxgap_micros = if consume_keyword(tokens, "MAXGAP") {
        Some(parse_interval(tokens, "MAXGAP")?)
    } else {
        None
    };
    let range = parse_time_range(tokens)?;
    let filter = parse_where(tokens)?;
    expect_end(tokens, "PATHS")?;
    Ok(Statement::EventPaths(PathsSpec {
        dataset,
        anchor,
        depth,
        top,
        maxgap_micros,
        range,
        filter,
    }))
}

/// Parse the optional `STARTING AT <event>` / `ENDING AT <event>` anchor.
fn parse_path_anchor(tokens: &mut Tokenizer<'_>) -> Result<PathAnchor, ParseError> {
    if consume_keyword(tokens, "STARTING") {
        tokens.expect_keyword("AT")?;
        return Ok(PathAnchor::StartingAt(event_literal(
            tokens,
            "STARTING AT event",
        )?));
    }
    if consume_keyword(tokens, "ENDING") {
        tokens.expect_keyword("AT")?;
        return Ok(PathAnchor::EndingAt(event_literal(
            tokens,
            "ENDING AT event",
        )?));
    }
    Ok(PathAnchor::Unanchored)
}

/// Parse the optional `FROM '<ts>'` / `TO '<ts>'` bounds into a half-open
/// microsecond range.
fn parse_time_range(tokens: &mut Tokenizer<'_>) -> Result<TimeRange, ParseError> {
    let from = if consume_keyword(tokens, "FROM") {
        Some(timestamp_literal(tokens, "FROM")?)
    } else {
        None
    };
    let to = if consume_keyword(tokens, "TO") {
        Some(timestamp_literal(tokens, "TO")?)
    } else {
        None
    };
    Ok(TimeRange { from, to })
}

/// Parse the optional `BREAKDOWN BY <property>` clause.
fn parse_breakdown(tokens: &mut Tokenizer<'_>) -> Result<Option<String>, ParseError> {
    if !consume_keyword(tokens, "BREAKDOWN") {
        return Ok(None);
    }
    tokens.expect_keyword("BY")?;
    Ok(Some(identifier(tokens, "BREAKDOWN BY property")?))
}

/// Parse a `<n> MINUTE|HOUR|DAY|WEEK` interval (singular or plural unit) into
/// microseconds.
fn parse_interval(tokens: &mut Tokenizer<'_>, clause: &str) -> Result<i64, ParseError> {
    let count = i64::from(unsigned_number(tokens, clause)?);
    let unit = match tokens.next_token() {
        Some(Token::Word(word)) => word.to_ascii_uppercase(),
        other => {
            return Err(ParseError::Parse(format!(
                "expected an interval unit after {clause} <n>, found '{}'",
                describe_opt(other.as_ref())
            )))
        }
    };
    let unit_micros = match unit.as_str() {
        "MINUTE" | "MINUTES" => 60_000_000_i64,
        "HOUR" | "HOURS" => 3_600_000_000_i64,
        "DAY" | "DAYS" => 86_400_000_000_i64,
        "WEEK" | "WEEKS" => 604_800_000_000_i64,
        other => {
            return Err(ParseError::Parse(format!(
                "interval unit '{other}' is not supported; use MINUTE, HOUR, DAY, or WEEK"
            )))
        }
    };
    count
        .checked_mul(unit_micros)
        .ok_or_else(|| ParseError::Parse(format!("{clause} interval overflows microseconds")))
}

/// Parse a quoted `'YYYY-MM-DD[ HH:MM:SS[.ffffff]]'` literal into UTC
/// microseconds since the epoch.
fn timestamp_literal(tokens: &mut Tokenizer<'_>, clause: &str) -> Result<i64, ParseError> {
    let text = match tokens.next_token() {
        Some(Token::StringLiteral(text)) => text,
        other => {
            return Err(ParseError::Parse(format!(
                "expected a quoted timestamp after {clause}, found '{}'",
                describe_opt(other.as_ref())
            )))
        }
    };
    micros_of_timestamp_text(&text).ok_or_else(|| {
        ParseError::Parse(format!(
            "{clause} timestamp '{text}' is not of the form \
             'YYYY-MM-DD[ HH:MM:SS[.ffffff]]'"
        ))
    })
}

/// The UTC microseconds of a `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` text, or None
/// when the text does not match the form (including out-of-range fields).
fn micros_of_timestamp_text(text: &str) -> Option<i64> {
    let (date_part, time_part) = match text.split_once(' ') {
        Some((date, time)) => (date, Some(time)),
        None => (text, None),
    };
    let mut date_fields = date_part.split('-');
    let year: i64 = date_fields.next()?.parse().ok()?;
    let month: u32 = date_fields.next()?.parse().ok()?;
    let day: u32 = date_fields.next()?.parse().ok()?;
    if date_fields.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let day_micros = days_from_civil(year, month, day).checked_mul(86_400_000_000)?;
    let Some(time_text) = time_part else {
        return Some(day_micros);
    };
    let (clock, fraction) = match time_text.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (time_text, None),
    };
    let mut clock_fields = clock.split(':');
    let hour: i64 = clock_fields.next()?.parse().ok()?;
    let minute: i64 = clock_fields.next()?.parse().ok()?;
    let second: i64 = clock_fields.next()?.parse().ok()?;
    if clock_fields.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let micros = match fraction {
        None => 0,
        Some(digits) => {
            if digits.is_empty() || digits.len() > 6 || !digits.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            let value: i64 = digits.parse().ok()?;
            value * 10_i64.pow(6 - u32::try_from(digits.len()).ok()?)
        }
    };
    day_micros.checked_add(((hour * 60 + minute) * 60 + second) * 1_000_000 + micros)
}

/// Parse the optional `WHERE <predicate>` clause of an analysis statement.
fn parse_where(tokens: &mut Tokenizer<'_>) -> Result<Option<EventPredicate>, ParseError> {
    if !consume_keyword(tokens, "WHERE") {
        return Ok(None);
    }
    Ok(Some(parse_or(tokens)?))
}

/// Parse an OR chain of the predicate grammar.
fn parse_or(tokens: &mut Tokenizer<'_>) -> Result<EventPredicate, ParseError> {
    let mut terms = vec![parse_and(tokens)?];
    while consume_keyword(tokens, "OR") {
        terms.push(parse_and(tokens)?);
    }
    if terms.len() == 1 {
        return Ok(terms.pop().expect("one term"));
    }
    Ok(EventPredicate::Or(terms))
}

/// Parse an AND chain of the predicate grammar.
fn parse_and(tokens: &mut Tokenizer<'_>) -> Result<EventPredicate, ParseError> {
    let mut terms = vec![parse_unary(tokens)?];
    while consume_keyword(tokens, "AND") {
        terms.push(parse_unary(tokens)?);
    }
    if terms.len() == 1 {
        return Ok(terms.pop().expect("one term"));
    }
    Ok(EventPredicate::And(terms))
}

/// Parse a NOT, a parenthesized predicate, or a comparison.
fn parse_unary(tokens: &mut Tokenizer<'_>) -> Result<EventPredicate, ParseError> {
    if consume_keyword(tokens, "NOT") {
        return Ok(EventPredicate::Not(Box::new(parse_unary(tokens)?)));
    }
    if consume_char(tokens, '(') {
        let inner = parse_or(tokens)?;
        expect_char(tokens, ')', "WHERE ( <predicate> )")?;
        return Ok(inner);
    }
    parse_comparison(tokens)
}

/// Parse `property <op> literal`, `property IN (...)`, or
/// `property IS [NOT] NULL`.
fn parse_comparison(tokens: &mut Tokenizer<'_>) -> Result<EventPredicate, ParseError> {
    let property = identifier(tokens, "WHERE property")?;
    if consume_keyword(tokens, "IS") {
        let negated = consume_keyword(tokens, "NOT");
        tokens.expect_keyword("NULL")?;
        return Ok(EventPredicate::IsNull { property, negated });
    }
    if consume_keyword(tokens, "IN") {
        expect_char(tokens, '(', "WHERE <property> IN (")?;
        let mut literals = vec![predicate_literal(tokens)?];
        while consume_char(tokens, ',') {
            literals.push(predicate_literal(tokens)?);
        }
        expect_char(tokens, ')', "WHERE <property> IN ( ... )")?;
        return Ok(EventPredicate::InList { property, literals });
    }
    let op = comparison_op(tokens)?;
    let literal = predicate_literal(tokens)?;
    Ok(EventPredicate::Compare {
        property,
        op,
        literal,
    })
}

/// Parse one comparison operator, combining two-character forms.
fn comparison_op(tokens: &mut Tokenizer<'_>) -> Result<CompareOp, ParseError> {
    match tokens.next_token() {
        Some(Token::Other('=')) => Ok(CompareOp::Eq),
        Some(Token::Other('<')) => {
            if consume_char(tokens, '>') {
                return Ok(CompareOp::Neq);
            }
            if consume_char(tokens, '=') {
                return Ok(CompareOp::Lte);
            }
            Ok(CompareOp::Lt)
        }
        Some(Token::Other('>')) => {
            if consume_char(tokens, '=') {
                return Ok(CompareOp::Gte);
            }
            Ok(CompareOp::Gt)
        }
        Some(Token::Other('!')) => {
            expect_char(tokens, '=', "WHERE comparison '!='")?;
            Ok(CompareOp::Neq)
        }
        other => Err(ParseError::Parse(format!(
            "expected a comparison operator, found '{}'",
            describe_opt(other.as_ref())
        ))),
    }
}

/// Parse one predicate literal: a quoted string, a (possibly negative,
/// possibly fractional) number, or TRUE/FALSE.
fn predicate_literal(tokens: &mut Tokenizer<'_>) -> Result<PredicateLiteral, ParseError> {
    if consume_keyword(tokens, "TRUE") {
        return Ok(PredicateLiteral::Bool(true));
    }
    if consume_keyword(tokens, "FALSE") {
        return Ok(PredicateLiteral::Bool(false));
    }
    match tokens.next_token() {
        Some(Token::StringLiteral(text)) => Ok(PredicateLiteral::Str(text)),
        Some(Token::Other('-')) => Ok(negate(&number_literal(tokens)?)),
        Some(Token::Word(digits)) => number_from_word(tokens, digits),
        other => Err(ParseError::Parse(format!(
            "expected a literal (string, number, TRUE, or FALSE), found '{}'",
            describe_opt(other.as_ref())
        ))),
    }
}

/// Negate a parsed numeric literal.
fn negate(literal: &PredicateLiteral) -> PredicateLiteral {
    match literal {
        PredicateLiteral::Int(value) => PredicateLiteral::Int(-value),
        PredicateLiteral::Float(value) => PredicateLiteral::Float(-value),
        PredicateLiteral::Str(_) | PredicateLiteral::Bool(_) => {
            unreachable!("number_literal only returns numeric literals")
        }
    }
}

/// Parse the numeric token(s) after a unary minus.
fn number_literal(tokens: &mut Tokenizer<'_>) -> Result<PredicateLiteral, ParseError> {
    match tokens.next_token() {
        Some(Token::Word(digits)) => number_from_word(tokens, digits),
        other => Err(ParseError::Parse(format!(
            "expected digits after '-', found '{}'",
            describe_opt(other.as_ref())
        ))),
    }
}

/// Build a numeric literal from a leading digit word, consuming a following
/// `.digits` fraction when present.
fn number_from_word(
    tokens: &mut Tokenizer<'_>,
    digits: &str,
) -> Result<PredicateLiteral, ParseError> {
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ParseError::Parse(format!(
            "expected a literal, found '{digits}'"
        )));
    }
    if matches!(tokens.peek(), Some(Token::Other('.'))) {
        tokens.next_token();
        let fraction = match tokens.next_token() {
            Some(Token::Word(word)) if word.bytes().all(|b| b.is_ascii_digit()) => word,
            other => {
                return Err(ParseError::Parse(format!(
                    "expected fraction digits after '{digits}.', found '{}'",
                    describe_opt(other.as_ref())
                )))
            }
        };
        let value: f64 = format!("{digits}.{fraction}").parse().map_err(|_| {
            ParseError::Parse(format!("number '{digits}.{fraction}' does not parse"))
        })?;
        return Ok(PredicateLiteral::Float(value));
    }
    let value: i64 = digits
        .parse()
        .map_err(|_| ParseError::Parse(format!("integer '{digits}' does not parse")))?;
    Ok(PredicateLiteral::Int(value))
}

/// Parse a quoted event-name literal (event names are data, not identifiers).
fn event_literal(tokens: &mut Tokenizer<'_>, what: &str) -> Result<String, ParseError> {
    match tokens.next_token() {
        Some(Token::StringLiteral(text)) => Ok(text),
        other => Err(ParseError::Parse(format!(
            "expected a quoted {what} name, found '{}'",
            describe_opt(other.as_ref())
        ))),
    }
}

/// Parse one identifier: a bare word (lowercased, the Postgres rule) or a
/// quoted identifier (exact spelling).
fn identifier(tokens: &mut Tokenizer<'_>, what: &str) -> Result<String, ParseError> {
    match tokens.next_token() {
        Some(Token::Word(word)) => Ok(word.to_lowercase()),
        Some(Token::QuotedIdent(name)) if !name.is_empty() => Ok(name),
        other => Err(ParseError::Parse(format!(
            "expected a {what} identifier, found '{}'",
            describe_opt(other.as_ref())
        ))),
    }
}

/// Parse one unsigned integer token for a `<keyword> <n>` clause.
fn unsigned_number(tokens: &mut Tokenizer<'_>, clause: &str) -> Result<u32, ParseError> {
    match tokens.next_token() {
        Some(Token::Word(digits)) if digits.bytes().all(|b| b.is_ascii_digit()) => digits
            .parse()
            .map_err(|_| ParseError::Parse(format!("{clause} value '{digits}' does not parse"))),
        other => Err(ParseError::Parse(format!(
            "expected an unsigned integer after {clause}, found '{}'",
            describe_opt(other.as_ref())
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify_statement;

    /// Classify and unwrap, panicking with the error for a readable failure.
    fn classify(sql: &str) -> Statement<'_> {
        classify_statement(sql).expect("statement classifies")
    }

    #[test]
    fn create_event_dataset_parses_the_full_form() {
        let statement = classify(
            "CREATE EVENT DATASET web FROM ev.main.events \
             ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq \
             PROPERTIES (device, country) WITH (refresh_key = 'seq')",
        );
        let Statement::CreateEventDataset(def) = statement else {
            panic!("expected CreateEventDataset");
        };
        assert_eq!(def.name, "web");
        assert_eq!(
            def.source,
            EventSource::Table {
                datasource: "ev".to_string(),
                schema: "main".to_string(),
                table: "events".to_string(),
            }
        );
        assert_eq!(def.actor_column, "entity_id");
        assert_eq!(def.time_column, "ts");
        assert_eq!(def.event_column, "event_name");
        assert_eq!(def.tiebreak_column.as_deref(), Some("seq"));
        assert_eq!(
            def.properties,
            Some(vec!["device".to_string(), "country".to_string()])
        );
        assert_eq!(def.refresh_key.as_deref(), Some("seq"));
        assert_eq!(def.time_unit, TimeUnit::Micros);
        assert_eq!(def.shards, None);
    }

    #[test]
    fn create_event_dataset_select_source_keeps_the_sql() {
        let statement = classify(
            "CREATE EVENT DATASET w AS (SELECT a, b FROM t WHERE (x) > 1) \
             ACTOR a TIME b EVENT c PROPERTIES ()",
        );
        let Statement::CreateEventDataset(def) = statement else {
            panic!("expected CreateEventDataset");
        };
        assert_eq!(
            def.source,
            EventSource::Select {
                sql: "SELECT a, b FROM t WHERE (x) > 1".to_string()
            }
        );
        assert_eq!(def.properties, Some(Vec::new()));
    }

    #[test]
    fn create_event_dataset_unknown_with_key_raises() {
        let error = classify_statement(
            "CREATE EVENT DATASET w FROM d.s.t ACTOR a TIME b EVENT c WITH (bogus = '1')",
        )
        .unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(ref m) if m.contains("bogus")));
    }

    #[test]
    fn event_ddl_statement_forms_classify() {
        assert_eq!(
            classify("REFRESH EVENT DATASET web"),
            Statement::RefreshEventDataset {
                name: "web".to_string()
            }
        );
        assert_eq!(
            classify("REBUILD EVENT DATASET web"),
            Statement::RebuildEventDataset {
                name: "web".to_string()
            }
        );
        assert_eq!(
            classify("DROP EVENT DATASET web"),
            Statement::DropEventDataset {
                name: "web".to_string()
            }
        );
        assert_eq!(
            classify("SHOW EVENT DATASETS"),
            Statement::ShowEventDatasets
        );
    }

    #[test]
    fn funnel_parses_steps_window_and_clauses() {
        let statement = classify(
            "FUNNEL ('signup', 'begin_checkout', 'purchase') ON web WINDOW 7 DAY \
             FROM '2025-01-01' TO '2025-02-01' WHERE device IN ('ios', 'android') \
             BREAKDOWN BY device",
        );
        let Statement::EventFunnel(spec) = statement else {
            panic!("expected EventFunnel");
        };
        assert_eq!(spec.steps, ["signup", "begin_checkout", "purchase"]);
        assert_eq!(spec.window_micros, 7 * 86_400_000_000);
        assert_eq!(spec.mode, FunnelMode::StrictOrder);
        assert_eq!(spec.range.from, Some(1_735_689_600_000_000));
        assert_eq!(spec.range.to, Some(1_738_368_000_000_000));
        assert_eq!(spec.breakdown.as_deref(), Some("device"));
        assert!(matches!(
            spec.filter,
            Some(EventPredicate::InList { ref property, ref literals })
                if property == "device" && literals.len() == 2
        ));
    }

    #[test]
    fn funnel_with_one_step_raises() {
        let error = classify_statement("FUNNEL ('a') ON web WINDOW 1 DAY").unwrap_err();
        assert!(matches!(error, ParseError::Parse(ref m) if m.contains("two steps")));
    }

    #[test]
    fn timestamp_literals_carry_full_microsecond_precision() {
        let statement = classify(
            "SEGMENT COUNT ON web BUCKET DAY FROM '2025-01-02 03:04:05.000007' \
             TO '2025-01-02 03:04:06'",
        );
        let Statement::EventSegment(spec) = statement else {
            panic!("expected EventSegment");
        };
        let base = 1_735_787_045_000_000;
        assert_eq!(spec.range.from, Some(base + 7));
        assert_eq!(spec.range.to, Some(base + 1_000_000));
    }

    #[test]
    fn malformed_timestamp_raises() {
        let error =
            classify_statement("SEGMENT COUNT ON web BUCKET DAY FROM '2025-13-01'").unwrap_err();
        assert!(matches!(error, ParseError::Parse(_)));
    }

    #[test]
    fn retention_parses_modes_and_counts() {
        let statement = classify(
            "RETENTION ON web BIRTH 'signup' RETURN ANY EVENT PERIOD WEEK \
             MODE UNBOUNDED PERIODS 8 AT 3",
        );
        let Statement::EventRetention(spec) = statement else {
            panic!("expected EventRetention");
        };
        assert_eq!(spec.birth, EventMatch::Named("signup".to_string()));
        assert_eq!(spec.return_event, EventMatch::Any);
        assert_eq!(spec.period, PeriodGrain::Week);
        assert_eq!(spec.mode, RetentionMode::Unbounded);
        assert_eq!(spec.periods, Some(8));
        assert_eq!(spec.at, Some(3));
    }

    #[test]
    fn paths_parses_anchor_depth_top_and_maxgap() {
        let statement =
            classify("PATHS ON web ENDING AT 'cancel_account' DEPTH 4 TOP 50 MAXGAP 30 MINUTE");
        let Statement::EventPaths(spec) = statement else {
            panic!("expected EventPaths");
        };
        assert_eq!(
            spec.anchor,
            PathAnchor::EndingAt("cancel_account".to_string())
        );
        assert_eq!(spec.depth, 4);
        assert_eq!(spec.top, 50);
        assert_eq!(spec.maxgap_micros, Some(30 * 60_000_000));
    }

    #[test]
    fn paths_defaults_depth_and_top() {
        let Statement::EventPaths(spec) = classify("PATHS ON web") else {
            panic!("expected EventPaths");
        };
        assert_eq!(spec.anchor, PathAnchor::Unanchored);
        assert_eq!(spec.depth, 5);
        assert_eq!(spec.top, 20);
        assert_eq!(spec.maxgap_micros, None);
    }

    #[test]
    fn predicate_grammar_covers_all_forms() {
        let statement = classify(
            "SEGMENT UNIQUES ON web BUCKET DAY WHERE \
             NOT (country = 'DE' OR amount >= 10.5) AND flag = TRUE \
             AND device IS NOT NULL AND score <> -3",
        );
        let Statement::EventSegment(spec) = statement else {
            panic!("expected EventSegment");
        };
        let Some(EventPredicate::And(terms)) = spec.filter else {
            panic!("expected a top-level AND");
        };
        assert_eq!(terms.len(), 4);
        assert!(matches!(terms[0], EventPredicate::Not(_)));
        assert!(matches!(
            terms[2],
            EventPredicate::IsNull { negated: true, .. }
        ));
        assert!(matches!(
            terms[3],
            EventPredicate::Compare {
                op: CompareOp::Neq,
                literal: PredicateLiteral::Int(-3),
                ..
            }
        ));
    }

    #[test]
    fn trailing_tokens_raise() {
        let error = classify_statement("SHOW EVENT DATASETS extra").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(_)));
        let error = classify_statement("PATHS ON web DEPTH 3 nonsense").unwrap_err();
        assert!(matches!(error, ParseError::Unsupported(_)));
    }

    #[test]
    fn any_order_mode_is_recognized_by_the_grammar() {
        let Statement::EventFunnel(spec) =
            classify("FUNNEL ('a', 'b') ON web WINDOW 1 HOUR MODE ANY_ORDER")
        else {
            panic!("expected EventFunnel");
        };
        assert_eq!(spec.mode, FunnelMode::AnyOrder);
    }
}
