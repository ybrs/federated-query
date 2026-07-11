//! Column-reference RETAG transforms - the whole replacement for the retired
//! `_serialize_*` expression layer. Each takes `&Expr` and returns an owned `Expr`
//! whose `ColumnRef`s are retagged to the merge-relation the fragment reads;
//! everything else is a structural clone. fq-exec later reads `col.table` as the
//! relation tag and `col.column` as the physical column name.

use std::collections::HashSet;

use fq_plan::physical::{physical_column_name, ColumnAliasMap, PhysicalNestedLoopJoin};
use fq_plan::{ColumnRef, Expr};

/// Retag every column of `expr` to `(input_name, physical_name)` resolved through
/// `aliases`. Replaces `_expr_over` + `_serialize_expr` + every `_serialize_*`:
/// each `Expr::Column(c)` becomes a column tagged with the single input relation
/// (`in_0`, `in_left`, ...) and named by its physical output column.
pub fn over_input(expr: &Expr, input_name: &str, aliases: &ColumnAliasMap) -> Expr {
    retag(expr.clone(), &mut |col| ColumnRef {
        table: Some(input_name.to_string()),
        column: physical_column_name(&col, aliases),
        data_type: col.data_type,
    })
}

/// Retag every column of a nested-loop-join condition to `in_left` / `in_right` by
/// the side that owns it. Replaces `_two_sided_column_fn`: a column whose qualifier
/// is a left relation reads `in_left.<physical>`, otherwise `in_right.<physical>`
/// (the default-to-right rule Python uses).
pub fn two_sided(expr: &Expr, join: &PhysicalNestedLoopJoin) -> Expr {
    let left_aliases = join.left.column_aliases();
    let right_aliases = join.right.column_aliases();
    let left_tables = relation_names(&left_aliases);
    retag(expr.clone(), &mut |col| {
        let key = (col.table.clone(), col.column.clone());
        if col.table.is_some() && left_tables.contains(&col.table) {
            let physical = left_aliases.get(&key).cloned().unwrap_or(col.column);
            return ColumnRef {
                table: Some("in_left".to_string()),
                column: physical,
                data_type: col.data_type,
            };
        }
        let physical = right_aliases.get(&key).cloned().unwrap_or(col.column);
        ColumnRef {
            table: Some("in_right".to_string()),
            column: physical,
            data_type: col.data_type,
        }
    })
}

/// A source-side expression whose columns keep their OWN qualifier: an identity
/// clone. Replaces `expr_to_ir` / `_plain_column` (used for a scan filter Expr,
/// rendered against the source, not a merge relation).
pub fn plain(expr: &Expr) -> Expr {
    expr.clone()
}

/// The set of relation qualifiers a column-alias map exposes. Ports
/// `_relation_names`.
pub(crate) fn relation_names(aliases: &ColumnAliasMap) -> HashSet<Option<String>> {
    let mut names = HashSet::new();
    for (table, _column) in aliases.keys() {
        names.insert(table.clone());
    }
    names
}

/// Rebuild `expr`, applying `f` to every `ColumnRef` at any depth. Recurses via the
/// exhaustive `Expr::map_children`, so a new expression variant cannot silently
/// skip retagging.
fn retag(expr: Expr, f: &mut impl FnMut(ColumnRef) -> ColumnRef) -> Expr {
    if let Expr::Column(col) = expr {
        return Expr::Column(f(col));
    }
    expr.map_children(&mut |child| retag(child, f))
}
