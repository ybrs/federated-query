//! FROM / JOIN / derived-table / CTE-reference TEXT builders for single-source
//! pushdown. fq-emit deliberately leaves FROM/JOIN rendering to the node crate
//! ("a FROM/JOIN renderer likewise belongs to that node, not here"), so these
//! canonical-Postgres relation-text helpers live here and compose
//! `fq_emit::quote_ident`. Shared later with the remote-scan node.

use fq_emit::quote_ident;
use fq_plan::logical::CteRef;

/// Build a FROM/JOIN table reference: `"schema"."name" AS "alias" <sample>`. Port
/// of `build_table_ref`. `schema` None emits a bare (registered-relation) name;
/// `sample` is verbatim Postgres-form TABLESAMPLE text appended as given (the
/// per-dialect transpile happens later in fq-exec).
pub(crate) fn table_ref(
    name: &str,
    schema: Option<&str>,
    alias: Option<&str>,
    sample: Option<&str>,
) -> String {
    let mut text = match schema {
        Some(schema_name) => format!("{}.{}", quote_ident(schema_name), quote_ident(name)),
        None => quote_ident(name),
    };
    if let Some(alias_name) = alias {
        text.push_str(" AS ");
        text.push_str(&quote_ident(alias_name));
    }
    if let Some(sample_text) = sample {
        text.push(' ');
        text.push_str(sample_text);
    }
    text
}

/// Wrap a rendered sub-SELECT as an aliased derived table `(<body>) AS "alias"`.
pub(crate) fn derived_table(body_sql: &str, alias: &str) -> String {
    format!("({body_sql}) AS {}", quote_ident(alias))
}

/// Build a CTE reference for a FROM/JOIN position: the bare (unquoted) CTE name,
/// keeping a distinct alias (`name AS "alias"`) only when the alias differs. Port
/// of `_cte_ref_sql` (the CTE name is an engine-generated safe identifier, so it
/// is emitted unquoted, matching `exp.to_identifier` without `quoted=True`).
pub(crate) fn cte_ref_sql(node: &CteRef) -> String {
    let mut text = node.name.clone();
    if let Some(alias) = &node.alias {
        if alias != &node.name {
            text.push_str(" AS ");
            text.push_str(&quote_ident(alias));
        }
    }
    text
}
