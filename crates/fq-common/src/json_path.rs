//! Parse JSON Pointer and DuckDB lookup paths without reading source rows.

/// One lookup applied to a JSON value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonStep {
    Field(String),
    Index(i64),
    ArrayIndex(u64),
    Descendants { include_self: bool },
    FromEnd(u64),
    Missing,
    Pointer(String),
    ObjectWildcard,
    ArrayWildcard,
    Recursive(Box<JsonStep>),
}

/// A validated lookup, including whether it produces a list of matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonPath {
    pub steps: Vec<JsonStep>,
}

impl JsonPath {
    /// Parse root, plain keys, JSON Pointer, or a dollar-prefixed lookup path.
    pub fn parse(path: &str) -> Result<Self, String> {
        if let Some(pointer) = path.strip_prefix('/') {
            return Ok(pointer_path(pointer));
        }
        if let Some(lookup) = path.strip_prefix('$') {
            return lookup_path(lookup);
        }
        let steps = if path.is_empty() {
            Vec::new()
        } else {
            vec![field_kind(path.into())]
        };
        Ok(Self { steps })
    }

    /// Wildcard and recursive lookups produce lists even when they find one value.
    pub fn multiple(&self) -> bool {
        self.steps.iter().any(JsonStep::multiple)
    }
}

impl JsonStep {
    /// Identify the lookup operations that can return more than one value.
    fn multiple(&self) -> bool {
        matches!(
            self,
            Self::ObjectWildcard
                | Self::ArrayWildcard
                | Self::Recursive(_)
                | Self::Descendants { .. }
        )
    }
}

/// Decode each JSON Pointer token without treating its contents as path syntax.
fn pointer_path(pointer: &str) -> JsonPath {
    let mut steps = Vec::new();
    for token in pointer.split('/') {
        let Some(token) = pointer_token(token) else {
            return JsonPath {
                steps: vec![JsonStep::Missing],
            };
        };
        steps.push(JsonStep::Pointer(token));
    }
    JsonPath { steps }
}

/// Preserve slash and tilde escapes, rejecting malformed escape sequences.
fn pointer_token(token: &str) -> Option<String> {
    for escaped in token.split('~').skip(1) {
        if !escaped.starts_with(['0', '1']) {
            return None;
        }
    }
    Some(token.replace("~1", "/").replace("~0", "~"))
}

/// Consume a lookup path as field, array, and recursive steps.
fn lookup_path(mut remaining: &str) -> Result<JsonPath, String> {
    let mut steps = Vec::new();
    while !remaining.is_empty() {
        let (step, rest) = next_step(remaining)?;
        steps.push(step);
        remaining = rest;
    }
    Ok(JsonPath { steps })
}

/// Parse one delimited lookup step and retain the unconsumed suffix.
fn next_step(input: &str) -> Result<(JsonStep, &str), String> {
    if let Some(rest) = input.strip_prefix(".**") {
        return Ok((
            JsonStep::Descendants {
                include_self: !rest.is_empty(),
            },
            rest,
        ));
    }
    if let Some(rest) = input.strip_prefix("..") {
        return recursive_lookup(rest);
    }
    regular_step(input)
}

/// Parse a nonrecursive field or array lookup.
fn regular_step(input: &str) -> Result<(JsonStep, &str), String> {
    if let Some(rest) = input.strip_prefix('.') {
        return field_step(rest);
    }
    if let Some(rest) = input.strip_prefix('[') {
        return array_step(rest);
    }
    Err(format!("JSON path error near {input:?}"))
}

/// Recursive wildcards enumerate descendants; keyed recursion evaluates every container.
fn recursive_lookup(input: &str) -> Result<(JsonStep, &str), String> {
    let (step, suffix) = recursive_step(input)?;
    let step = match step {
        JsonStep::ObjectWildcard => JsonStep::Descendants {
            include_self: !suffix.is_empty(),
        },
        other => JsonStep::Recursive(Box::new(other)),
    };
    Ok((step, suffix))
}

/// Recursive array indexing and recursive object fields use the same lookup rules.
fn recursive_step(input: &str) -> Result<(JsonStep, &str), String> {
    match input.strip_prefix('[') {
        Some(rest) => array_step(rest),
        None => field_step(input),
    }
}

/// Decode a quoted key or consume one literal field name.
fn field_step(input: &str) -> Result<(JsonStep, &str), String> {
    if input.starts_with('"') {
        let (key, rest) = quoted_key(input)?;
        return Ok((field_kind(key), rest));
    }
    unquoted_field(input)
}

/// Consume an unquoted field while keeping path separators outside the key.
fn unquoted_field(input: &str) -> Result<(JsonStep, &str), String> {
    if let Some(rest) = input.strip_prefix('*') {
        return Ok((JsonStep::ObjectWildcard, rest));
    }
    let end = input.find(['.', '[']).unwrap_or(input.len());
    let key = &input[..end];
    if key.is_empty() {
        return Err("JSON path contains an empty field".into());
    }
    let step = if key == "*" {
        JsonStep::ObjectWildcard
    } else {
        JsonStep::Field(key.into())
    };
    Ok((step, &input[end..]))
}

/// Classify a decoded key while preserving wildcard result typing.
fn field_kind(key: String) -> JsonStep {
    if key == "*" {
        JsonStep::ObjectWildcard
    } else {
        JsonStep::Field(key)
    }
}

/// Quoted keys escape quotes and backslashes while retaining other backslash pairs.
fn quoted_key(input: &str) -> Result<(String, &str), String> {
    let mut escaped = false;
    let mut key = String::new();
    for (position, character) in input.char_indices().skip(1) {
        match (character, escaped) {
            ('"', false) => return finish_quoted(key, &input[position + 1..]),
            ('\\', false) => escaped = true,
            (character, true) => {
                append_escaped(&mut key, character);
                escaped = false;
            }
            (character, false) => key.push(character),
        }
    }
    Err("JSON path has an unterminated quoted key".into())
}

/// Empty quoted fields are invalid lookup paths.
fn finish_quoted(key: String, rest: &str) -> Result<(String, &str), String> {
    if key.is_empty() {
        return Err("JSON path contains an empty quoted field".into());
    }
    Ok((key, rest))
}

/// Only quote and backslash escape sequences remove their leading backslash.
fn append_escaped(key: &mut String, character: char) {
    if !matches!(character, '"' | '\\') {
        key.push('\\');
    }
    key.push(character);
}

/// Parse zero-based, negative, and hash-prefixed negative array indexes.
fn array_step(input: &str) -> Result<(JsonStep, &str), String> {
    let end = input
        .find(']')
        .ok_or("JSON path has an unterminated array index")?;
    let token = &input[..end];
    if token == "*" {
        return Ok((JsonStep::ArrayWildcard, &input[end + 1..]));
    }
    Ok((array_index_step(token)?, &input[end + 1..]))
}

/// Accept decimal indexes and SQLite's missing-element hash index.
fn array_index_step(token: &str) -> Result<JsonStep, String> {
    if token == "#" {
        return Ok(JsonStep::Missing);
    }
    let index = token.strip_prefix('#').unwrap_or(token);
    if let Some(magnitude) = index.strip_prefix('-') {
        return decimal_index(magnitude).map(JsonStep::FromEnd);
    }
    if token.starts_with('#') {
        return Err(format!("invalid JSON array index {token:?}"));
    }
    decimal_index(index).map(JsonStep::ArrayIndex)
}

/// Indexes contain one to nineteen decimal digits, without a sign or whitespace.
fn decimal_index(token: &str) -> Result<u64, String> {
    if token.is_empty() || token.len() > 19 || !token.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("invalid JSON array index {token:?}"));
    }
    token
        .parse()
        .map_err(|error| format!("invalid JSON array index {token:?}: {error}"))
}
