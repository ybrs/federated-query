//! Shared SQL-clause builders over the canonical Postgres text emitter.
//!
//! One implementation each of the SELECT list, ORDER BY, GROUP BY / GROUPING
//! SETS, the set-op keyword, and the SELECT skeleton. Every node that emits SQL
//! (remote scans, single-source pushdown, the local DuckDB merge operators)
//! composes these, so clause rendering exists in exactly one place. These return
//! keyword-less fragments (except `assemble_select`, which builds a full SELECT).
//!
//! Ports `plan/emit/clauses.py`. The per-physical-node query assembly
//! (`PhysicalRemoteQuery._build_query`, single-source pushdown, injected/lateral
//! island SQL) is DEFERRED to fq-physical, which owns the subtree and composes
//! this toolkit; a FROM/JOIN renderer likewise belongs to that node, not here.

use fq_plan::{Expr, NullsOrder, SetOpKind};

use crate::error::EmitError;
use crate::expr::{render_expr, render_ordered_keys};
use crate::ident::quote_ident;
use crate::resolver::ColumnResolver;

/// Render the SELECT list to a comma-separated fragment. A parallel output name
/// (present and non-empty) aliases its expression as `<sql> AS "name"`; a falsy
/// name emits the expression bare. The alias is quoted via `quote_ident`.
pub fn select_list<N: AsRef<str>>(
    exprs: &[Expr],
    names: &[N],
    resolver: &dyn ColumnResolver,
) -> Result<String, EmitError> {
    let mut parts = Vec::with_capacity(exprs.len());
    for (index, expr) in exprs.iter().enumerate() {
        let mut fragment = render_expr(expr, resolver)?;
        if let Some(name) = names.get(index).map(AsRef::as_ref) {
            if !name.is_empty() {
                fragment = format!("{fragment} AS {}", quote_ident(name));
            }
        }
        parts.push(fragment);
    }
    Ok(parts.join(", "))
}

/// Build the ORDER BY fragment (no leading `ORDER BY`), or None when there are no
/// keys. Uses the shared ordered-key rule, so NULLS placement never drifts from
/// the window emitter's.
pub fn order_by(
    keys: &[Expr],
    ascending: &[bool],
    nulls: &[Option<NullsOrder>],
    resolver: &dyn ColumnResolver,
) -> Result<Option<String>, EmitError> {
    if keys.is_empty() {
        return Ok(None);
    }
    Ok(Some(render_ordered_keys(keys, ascending, nulls, resolver)?))
}

/// Build the GROUP BY / GROUPING SETS fragment (no leading `GROUP BY`), or None
/// when there is nothing to group by. `grouping_sets` Some takes precedence over
/// plain keys; each inner set renders as `(members)`, an empty set as `()`.
pub fn group_by(
    group_keys: &[Expr],
    grouping_sets: Option<&[Vec<Expr>]>,
    resolver: &dyn ColumnResolver,
) -> Result<Option<String>, EmitError> {
    if let Some(sets) = grouping_sets {
        return Ok(Some(render_grouping_sets(sets, resolver)?));
    }
    if group_keys.is_empty() {
        return Ok(None);
    }
    Ok(Some(render_key_list(group_keys, resolver)?))
}

/// Render `GROUPING SETS ((a, b), (a), ())` from explicit grouping sets.
fn render_grouping_sets(
    sets: &[Vec<Expr>],
    resolver: &dyn ColumnResolver,
) -> Result<String, EmitError> {
    let mut rendered = Vec::with_capacity(sets.len());
    for grouping_set in sets {
        rendered.push(format!("({})", render_key_list(grouping_set, resolver)?));
    }
    Ok(format!("GROUPING SETS ({})", rendered.join(", ")))
}

/// Render a comma-joined list of key expressions.
fn render_key_list(keys: &[Expr], resolver: &dyn ColumnResolver) -> Result<String, EmitError> {
    let mut rendered = Vec::with_capacity(keys.len());
    for key in keys {
        rendered.push(render_expr(key, resolver)?);
    }
    Ok(rendered.join(", "))
}

/// The keyword for a set operation; a non-distinct op appends ` ALL`. The one
/// mapping shared by the remote set-op node and single-source pushdown. Returned
/// as a static per (kind, distinct) pair so no allocation is needed.
pub fn set_op_keyword(kind: SetOpKind, distinct: bool) -> &'static str {
    match (kind, distinct) {
        (SetOpKind::Union, true) => "UNION",
        (SetOpKind::Union, false) => "UNION ALL",
        (SetOpKind::Intersect, true) => "INTERSECT",
        (SetOpKind::Intersect, false) => "INTERSECT ALL",
        (SetOpKind::Except, true) => "EXCEPT",
        (SetOpKind::Except, false) => "EXCEPT ALL",
    }
}

/// The already-rendered text pieces of one SELECT (Options for absent clauses).
/// Grouping the fragments in a struct keeps `assemble_select` from carrying a
/// dozen positional parameters.
#[derive(Debug, Default, Clone, Copy)]
pub struct SelectPieces<'a> {
    /// The base relation text (e.g. `"schema"."t" AS "a"` or a derived table).
    pub from_clause: &'a str,
    /// The `select_list` output.
    pub select_items: &'a str,
    /// A pre-rendered join chain (None or "" when there are none).
    pub joins: Option<&'a str>,
    /// The predicate SQL (no `WHERE` keyword).
    pub where_: Option<&'a str>,
    /// The `group_by` fragment (no `GROUP BY` keyword).
    pub group: Option<&'a str>,
    /// The predicate SQL (no `HAVING` keyword).
    pub having: Option<&'a str>,
    /// A plain `DISTINCT` (ignored when `distinct_on` is set).
    pub distinct: bool,
    /// Comma-joined `DISTINCT ON` key SQL (no `DISTINCT ON` keyword); wins over
    /// `distinct`.
    pub distinct_on: Option<&'a str>,
    /// The `order_by` fragment (no `ORDER BY` keyword).
    pub order: Option<&'a str>,
    /// A row limit, when present.
    pub limit: Option<i64>,
    /// A row offset; kept even without a LIMIT (0 means absent).
    pub offset: i64,
}

/// Compose a full SELECT statement from already-rendered pieces - the one
/// skeleton builder every remote/merge query uses. Output order mirrors the
/// Python assemble_select + apply_distinct + apply_limit_offset; DISTINCT ON wins
/// over DISTINCT, and OFFSET is kept even without a LIMIT.
pub fn assemble_select(pieces: &SelectPieces) -> String {
    let mut sql = String::from("SELECT");
    sql.push_str(&distinct_prefix(pieces.distinct, pieces.distinct_on));
    sql.push(' ');
    sql.push_str(pieces.select_items);
    sql.push_str(" FROM ");
    sql.push_str(pieces.from_clause);
    push_part(&mut sql, " ", pieces.joins);
    push_part(&mut sql, " WHERE ", pieces.where_);
    push_part(&mut sql, " GROUP BY ", pieces.group);
    push_part(&mut sql, " HAVING ", pieces.having);
    push_part(&mut sql, " ORDER BY ", pieces.order);
    push_limit_offset(&mut sql, pieces.limit, pieces.offset);
    sql
}

/// The DISTINCT prefix that follows `SELECT`: `DISTINCT ON (keys)` wins over a
/// plain `DISTINCT`; neither renders nothing.
fn distinct_prefix(distinct: bool, distinct_on: Option<&str>) -> String {
    if let Some(keys) = distinct_on {
        return format!(" DISTINCT ON ({keys})");
    }
    if distinct {
        return " DISTINCT".to_string();
    }
    String::new()
}

/// Append `prefix + value` when `value` is present and non-empty.
fn push_part(sql: &mut String, prefix: &str, value: Option<&str>) {
    if let Some(text) = value {
        if !text.is_empty() {
            sql.push_str(prefix);
            sql.push_str(text);
        }
    }
}

/// Append the LIMIT/OFFSET tail; OFFSET is valid and kept without a LIMIT.
fn push_limit_offset(sql: &mut String, limit: Option<i64>, offset: i64) {
    if let Some(limit) = limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&limit.to_string());
    }
    if offset != 0 {
        sql.push_str(" OFFSET ");
        sql.push_str(&offset.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::{assemble_select, group_by, order_by, select_list, set_op_keyword, SelectPieces};
    use crate::dialect::{to_source_sql, Dialect};
    use crate::resolver::{ColumnResolver, SourceResolver};
    use fq_common::DataType;
    use fq_plan::{ColumnRef, Expr, LiteralValue, NullsOrder, SetOpKind};

    fn sink() -> impl ColumnResolver {
        SourceResolver
    }

    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    fn int(value: i64) -> Expr {
        Expr::Literal {
            value: LiteralValue::Integer(value),
            data_type: DataType::Integer,
        }
    }

    #[test]
    fn select_list_with_and_without_aliases() {
        let resolver = sink();
        let exprs = vec![col("t", "a"), col("t", "b")];
        let names = vec!["alias_a".to_string(), String::new()];
        assert_eq!(
            select_list(&exprs, &names, &resolver).unwrap(),
            "\"t\".\"a\" AS \"alias_a\", \"t\".\"b\""
        );
        // No names at all -> every item bare.
        let none: [&str; 0] = [];
        assert_eq!(
            select_list(&exprs, &none, &resolver).unwrap(),
            "\"t\".\"a\", \"t\".\"b\""
        );
    }

    #[test]
    fn order_by_nulls_defaults_and_override() {
        let resolver = sink();
        let keys = vec![col("t", "a")];
        assert_eq!(
            order_by(&keys, &[true], &[None], &resolver).unwrap(),
            Some("\"t\".\"a\" NULLS LAST".to_string())
        );
        assert_eq!(
            order_by(&keys, &[false], &[None], &resolver).unwrap(),
            Some("\"t\".\"a\" DESC NULLS FIRST".to_string())
        );
        assert_eq!(
            order_by(&keys, &[true], &[Some(NullsOrder::First)], &resolver).unwrap(),
            Some("\"t\".\"a\" NULLS FIRST".to_string())
        );
        assert_eq!(order_by(&[], &[], &[], &resolver).unwrap(), None);
    }

    #[test]
    fn group_by_plain_sets_and_empty() {
        let resolver = sink();
        let keys = vec![col("t", "a"), col("t", "b")];
        assert_eq!(
            group_by(&keys, None, &resolver).unwrap(),
            Some("\"t\".\"a\", \"t\".\"b\"".to_string())
        );
        let sets = vec![
            vec![col("t", "a"), col("t", "b")],
            vec![col("t", "a")],
            vec![],
        ];
        assert_eq!(
            group_by(&[], Some(&sets), &resolver).unwrap(),
            Some("GROUPING SETS ((\"t\".\"a\", \"t\".\"b\"), (\"t\".\"a\"), ())".to_string())
        );
        assert_eq!(group_by(&[], None, &resolver).unwrap(), None);
    }

    #[test]
    fn set_op_keywords() {
        assert_eq!(set_op_keyword(SetOpKind::Union, true), "UNION");
        assert_eq!(set_op_keyword(SetOpKind::Union, false), "UNION ALL");
        assert_eq!(set_op_keyword(SetOpKind::Except, false), "EXCEPT ALL");
        assert_eq!(set_op_keyword(SetOpKind::Intersect, true), "INTERSECT");
    }

    #[test]
    fn assemble_single_table_scan() {
        let resolver = sink();
        let items = select_list(&[col("t", "a")], &["a".to_string()], &resolver).unwrap();
        let pieces = SelectPieces {
            from_clause: "\"schema\".\"t\" AS \"t\"",
            select_items: &items,
            ..SelectPieces::default()
        };
        let sql = assemble_select(&pieces);
        assert_eq!(
            sql,
            "SELECT \"t\".\"a\" AS \"a\" FROM \"schema\".\"t\" AS \"t\""
        );
        to_source_sql(&sql, Dialect::Postgres).unwrap();
    }

    #[test]
    fn assemble_full_query_with_all_clauses() {
        let resolver = sink();
        let items = select_list(&[col("t", "a")], &["a".to_string()], &resolver).unwrap();
        let where_pred = crate::render_expr(
            &Expr::BinaryOp {
                op: fq_plan::BinaryOpType::Gt,
                left: Box::new(col("t", "a")),
                right: Box::new(int(1)),
            },
            &resolver,
        )
        .unwrap();
        let group = group_by(&[col("t", "a")], None, &resolver)
            .unwrap()
            .unwrap();
        let having = crate::render_expr(
            &Expr::BinaryOp {
                op: fq_plan::BinaryOpType::Gt,
                left: Box::new(col("t", "a")),
                right: Box::new(int(0)),
            },
            &resolver,
        )
        .unwrap();
        let order = order_by(&[col("t", "a")], &[true], &[None], &resolver)
            .unwrap()
            .unwrap();
        let pieces = SelectPieces {
            from_clause: "\"schema\".\"t\" AS \"t\"",
            select_items: &items,
            where_: Some(&where_pred),
            group: Some(&group),
            having: Some(&having),
            distinct: true,
            order: Some(&order),
            limit: Some(10),
            offset: 5,
            ..SelectPieces::default()
        };
        let sql = assemble_select(&pieces);
        assert_eq!(
            sql,
            "SELECT DISTINCT \"t\".\"a\" AS \"a\" FROM \"schema\".\"t\" AS \"t\" \
WHERE (\"t\".\"a\" > 1) GROUP BY \"t\".\"a\" HAVING (\"t\".\"a\" > 0) \
ORDER BY \"t\".\"a\" NULLS LAST LIMIT 10 OFFSET 5"
        );
        to_source_sql(&sql, Dialect::Postgres).unwrap();
    }

    #[test]
    fn distinct_on_wins_over_distinct() {
        let resolver = sink();
        let items = select_list(&[col("t", "a")], &["a".to_string()], &resolver).unwrap();
        let pieces = SelectPieces {
            from_clause: "\"schema\".\"t\" AS \"t\"",
            select_items: &items,
            distinct: true,
            distinct_on: Some("\"t\".\"a\""),
            ..SelectPieces::default()
        };
        let sql = assemble_select(&pieces);
        assert_eq!(
            sql,
            "SELECT DISTINCT ON (\"t\".\"a\") \"t\".\"a\" AS \"a\" FROM \"schema\".\"t\" AS \"t\""
        );
        to_source_sql(&sql, Dialect::Postgres).unwrap();
    }
}
