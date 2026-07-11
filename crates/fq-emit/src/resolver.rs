//! Column-reference resolution strategies for SQL emission.
//!
//! The one expression emitter serves two paths that differ only in how a
//! `ColumnRef` becomes SQL text: the remote path emits quoted, table-qualified
//! source columns; the merge path resolves to the physical Arrow column name a
//! node exposes. A resolver encapsulates exactly that difference.

use std::collections::HashMap;

use crate::error::EmitError;
use crate::ident::quote_ident;

/// Turns an engine `(table, column)` reference into canonical Postgres SQL text.
pub trait ColumnResolver {
    /// Render one (table, column) reference to canonical Postgres SQL text.
    fn resolve(&self, table: Option<&str>, column: &str) -> Result<String, EmitError>;
}

/// Remote path: emit a quoted, table-qualified column for the source DB.
/// Identifiers are always quoted so the source sees exactly the intended name;
/// `*` (bare or `alias.*`) maps to a star. Never raises.
#[derive(Debug, Clone, Copy, Default)]
pub struct SourceResolver;

impl ColumnResolver for SourceResolver {
    /// Quote the identifier(s); route `*` to a (possibly qualified) star.
    fn resolve(&self, table: Option<&str>, column: &str) -> Result<String, EmitError> {
        if column == "*" {
            return Ok(match table {
                Some(name) => format!("{}.*", quote_ident(name)),
                None => "*".to_string(),
            });
        }
        Ok(match table {
            Some(name) => format!("{}.{}", quote_ident(name), quote_ident(column)),
            None => quote_ident(column),
        })
    }
}

/// The canonical resolver for the engine's internal Postgres-form SQL, shared by
/// every diagnostic/string caller (the Python `CANONICAL_SOURCE_RESOLVER`).
pub const CANONICAL_SOURCE_RESOLVER: SourceResolver = SourceResolver;

/// The {(table, column) -> physical_name} alias map a merge relation carries.
pub type AliasMap = HashMap<(Option<String>, String), String>;

/// Merge path: resolve a column to its physical name in the merge relation.
/// The DuckDB merge engine operates on fetched Arrow relations whose columns are
/// the nodes' physical output names; a `(table, column)` reference maps through
/// the node's alias map to that physical name. `*` stays a star.
#[derive(Debug, Clone, Default)]
pub struct MergeResolver {
    aliases: AliasMap,
}

impl MergeResolver {
    /// Build a merge resolver over the given alias map (may be empty).
    pub fn new(aliases: AliasMap) -> Self {
        Self { aliases }
    }

    /// The alias-map keys rendered as sorted "table.column"/"column" strings, for
    /// the deterministic `exposes` list of a resolution error.
    fn exposed(&self) -> Vec<String> {
        let mut exposes = Vec::with_capacity(self.aliases.len());
        for (table, column) in self.aliases.keys() {
            match table {
                Some(name) => exposes.push(format!("{name}.{column}")),
                None => exposes.push(column.clone()),
            }
        }
        exposes.sort();
        exposes
    }
}

impl ColumnResolver for MergeResolver {
    /// Map the reference to its physical column name via the alias map.
    ///
    /// An unqualified reference (no table) is emitted as the bare (quoted) name -
    /// that is the physical name an aggregate output / exposed column carries. A
    /// QUALIFIED reference must be present in the alias map; if it is not, the
    /// plan produced a reference the relation does not expose, so it RAISES rather
    /// than dropping the qualifier and guessing by bare name.
    fn resolve(&self, table: Option<&str>, column: &str) -> Result<String, EmitError> {
        if column == "*" {
            return Ok("*".to_string());
        }
        let key = (table.map(str::to_string), column.to_string());
        if let Some(qualifier) = table {
            if !self.aliases.contains_key(&key) {
                return Err(EmitError::ColumnResolution {
                    table: qualifier.to_string(),
                    column: column.to_string(),
                    exposes: self.exposed(),
                });
            }
        }
        let name = self.aliases.get(&key).map_or(column, String::as_str);
        Ok(quote_ident(name))
    }
}

#[cfg(test)]
mod tests {
    use super::{AliasMap, ColumnResolver, MergeResolver, SourceResolver};
    use crate::error::EmitError;

    #[test]
    fn source_resolver_qualifies_and_quotes() {
        let resolver = SourceResolver;
        assert_eq!(resolver.resolve(Some("a"), "c").unwrap(), "\"a\".\"c\"");
        assert_eq!(resolver.resolve(None, "c").unwrap(), "\"c\"");
        assert_eq!(resolver.resolve(Some("a"), "*").unwrap(), "\"a\".*");
        assert_eq!(resolver.resolve(None, "*").unwrap(), "*");
    }

    fn populated_map() -> AliasMap {
        let mut map = AliasMap::new();
        map.insert(
            (Some("t".to_string()), "c".to_string()),
            "phys_c".to_string(),
        );
        map
    }

    #[test]
    fn merge_resolver_maps_qualified_to_physical_name() {
        let resolver = MergeResolver::new(populated_map());
        assert_eq!(resolver.resolve(Some("t"), "c").unwrap(), "\"phys_c\"");
    }

    #[test]
    fn merge_resolver_emits_unqualified_bare_name() {
        // An aggregate output carries its physical name unqualified; never raises.
        let resolver = MergeResolver::new(AliasMap::new());
        assert_eq!(resolver.resolve(None, "agg1").unwrap(), "\"agg1\"");
    }

    #[test]
    fn merge_resolver_star_is_bare() {
        let resolver = MergeResolver::new(populated_map());
        assert_eq!(resolver.resolve(Some("t"), "*").unwrap(), "*");
    }

    #[test]
    fn merge_resolver_raises_on_unexposed_reference() {
        // An invalid-query / unexposed-ref guard: qualified ref absent from the map.
        let resolver = MergeResolver::new(populated_map());
        let err = resolver.resolve(Some("t"), "missing").unwrap_err();
        assert_eq!(
            err,
            EmitError::ColumnResolution {
                table: "t".to_string(),
                column: "missing".to_string(),
                exposes: vec!["t.c".to_string()],
            }
        );
    }
}
