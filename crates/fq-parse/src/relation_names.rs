//! Database-qualified relation names for stored-view grants.

use crate::statement::{consume_char, expect_end, read_ident, Tokenizer};
use crate::ParseError;
use fq_common::database::RelationName;

/// Parse a bare or database-qualified relation, preserving quoted components.
pub fn parse_relation_name(text: &str) -> Result<RelationName, ParseError> {
    let mut tokens = Tokenizer::new(text);
    let name = read_relation_name(&mut tokens)?;
    expect_end(&mut tokens, "relation name")?;
    Ok(name)
}

/// Consume at most two components; the caller rejects trailing tokens in its grammar.
pub(crate) fn read_relation_name(tokens: &mut Tokenizer<'_>) -> Result<RelationName, ParseError> {
    let first = read_ident(tokens, "relation")?;
    if consume_char(tokens, '.') {
        let name = read_ident(tokens, "relation")?;
        return Ok(RelationName {
            database: Some(first),
            name,
        });
    }
    Ok(RelationName {
        database: None,
        name: first,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Quoted dots and doubled quotes belong to a component rather than a separator.
    #[test]
    fn quoted_components_preserve_identity() {
        let parsed = parse_relation_name("\"Project.1\".\"events\"\"live\"").expect("qualified");
        assert_eq!(parsed.database.as_deref(), Some("Project.1"));
        assert_eq!(parsed.name, "events\"live");
        assert_eq!(
            parse_relation_name("\"db.events\"")
                .expect("local")
                .database,
            None
        );
        assert_eq!(
            parse_relation_name("PROJECT.Events")
                .expect("normalized")
                .name,
            "events"
        );
    }

    /// Malformed names never fall back to a local string lookup.
    #[test]
    fn invalid_name_arity_and_tokens_raise() {
        for invalid in [
            "",
            "db.",
            "a.b.c",
            ".a",
            "a trailing",
            "\"unterminated",
            "\"\"",
        ] {
            assert!(parse_relation_name(invalid).is_err(), "{invalid}");
        }
    }
}
