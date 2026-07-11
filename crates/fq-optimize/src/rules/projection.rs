//! `ProjectionPushdown`: prune every scan to the columns the query actually
//! references, collected globally. The ONE rule that can raise: its pruner
//! reaching an unmodeled plan node. Ports `ProjectionPushdownRule`.

use std::collections::HashSet;

use fq_plan::expr::Expr;
use fq_plan::logical::{Cte, LogicalPlan, Scan, SubqueryScan};

use crate::error::OptimizeError;
use crate::pushdown::qualified_or_bare_names;

use super::{
    plan_node_name, set_operation_branches, with_set_operation_branches, OptimizationRule,
};

/// Push projections to eliminate unused columns early.
pub struct ProjectionPushdown;

impl OptimizationRule for ProjectionPushdown {
    fn name(&self) -> &'static str {
        "ProjectionPushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        // No explicit projection anywhere means SELECT * - never prune.
        if !has_explicit_projection(&plan) {
            return Ok(plan);
        }
        let mut required = HashSet::new();
        collect_required_columns(&plan, &mut required)?;
        prune_columns(plan, &required)
    }
}

/// Whether the plan pins an explicit output schema at its root chain (pruning is
/// only safe below an explicit Projection/Aggregate). Every pass-through node
/// must be seen through here. Ports `_has_explicit_projection`.
fn has_explicit_projection(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Filter(node) => has_explicit_projection(&node.input),
        LogicalPlan::Limit(node) => has_explicit_projection(&node.input),
        LogicalPlan::Sort(node) => has_explicit_projection(&node.input),
        LogicalPlan::GroupedLimit(node) => has_explicit_projection(&node.input),
        LogicalPlan::SingleRowGuard(node) => has_explicit_projection(&node.input),
        LogicalPlan::Cte(node) => has_explicit_projection(&node.child),
        LogicalPlan::SetOperation(_) | LogicalPlan::Union(_) => plan
            .children()
            .iter()
            .all(|child| has_explicit_projection(child)),
        _ => false,
    }
}

/// Every column the plan references anywhere, as QUALIFIED `table.col` names
/// (bare only for unqualified refs). Annotation-driven via `direct_expressions`
/// and total; deliberately crosses naming-scope boundaries (global
/// over-collection can only over-KEEP a column, never lose one). Ports
/// `_collect_required_columns`.
fn collect_required_columns(
    plan: &LogicalPlan,
    columns: &mut HashSet<String>,
) -> Result<(), OptimizeError> {
    for expression in plan.direct_expressions() {
        collect_expression(expression, columns)?;
    }
    for child in plan.children() {
        collect_required_columns(child, columns)?;
    }
    Ok(())
}

/// Names from one expression tree, EXCLUDING count-star calls (COUNT(*) needs no
/// columns; its `*` argument must not poison the required set). Ports
/// `_collect_expression`.
///
/// A subquery-bearing Expr (`Subquery`/`Exists`/`InSubquery`/`QuantifiedComparison`)
/// cannot appear here: decorrelation removes every Expr-level subquery before the
/// optimizer runs. If one survives, its columns are invisible to `Expr::children`
/// (which stops at subquery boundaries), so silently collecting nothing would let
/// a NEEDED column be pruned - a wrong-rows failure. Raise loudly instead: this is
/// a decorrelation-invariant violation, never a valid plan to prune.
fn collect_expression(
    expression: &Expr,
    columns: &mut HashSet<String>,
) -> Result<(), OptimizeError> {
    if is_subquery_node(expression) {
        return Err(OptimizeError::SubquerySurvived(expression.variant_label()));
    }
    if is_count_star(expression) {
        return Ok(());
    }
    if let Expr::Column(_) = expression {
        columns.extend(qualified_or_bare_names(expression));
        return Ok(());
    }
    for child in expression.children() {
        collect_expression(child, columns)?;
    }
    Ok(())
}

/// Whether an expression IS one of the subquery-bearing nodes decorrelation must
/// have removed before the optimizer runs.
fn is_subquery_node(expression: &Expr) -> bool {
    matches!(
        expression,
        Expr::Subquery { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
    )
}

/// A COUNT over a single star argument - the row-count aggregate. Ports
/// `_is_count_star`.
fn is_count_star(expression: &Expr) -> bool {
    let Expr::FunctionCall {
        function_name,
        args,
        ..
    } = expression
    else {
        return false;
    };
    if !function_name.eq_ignore_ascii_case("COUNT") || args.len() != 1 {
        return false;
    }
    matches!(&args[0], Expr::Column(column) if column.column == "*")
}

/// Prune every scan in the plan to the required qualified names. Pass-through
/// nodes recurse with the same global set; scope roots prune inside only when
/// their root pins an explicit output schema. An unknown node type RAISES (the
/// one raise site). Ports `_prune_columns`.
fn prune_columns(
    plan: LogicalPlan,
    required: &HashSet<String>,
) -> Result<LogicalPlan, OptimizeError> {
    match plan {
        LogicalPlan::Scan(scan) => Ok(prune_scan_columns(*scan, required)),
        LogicalPlan::SubqueryScan(node) => prune_subquery_scan(node, required),
        LogicalPlan::Cte(node) => prune_cte(node, required),
        set_op @ (LogicalPlan::SetOperation(_) | LogicalPlan::Union(_)) => {
            prune_branches(set_op, required)
        }
        leaf @ (LogicalPlan::Values(_) | LogicalPlan::CteRef(_)) => Ok(leaf),
        other => prune_through(other, required),
    }
}

/// Pass-through nodes: prune each child with the same global set. Any node
/// outside the modeled set RAISES `PruneNoRule`. Ports `_prune_through`.
fn prune_through(
    plan: LogicalPlan,
    required: &HashSet<String>,
) -> Result<LogicalPlan, OptimizeError> {
    let modeled = matches!(
        plan,
        LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::GroupedLimit(_)
            | LogicalPlan::SingleRowGuard(_)
            | LogicalPlan::LateralJoin(_)
            | LogicalPlan::Explain(_)
    );
    if !modeled {
        return Err(OptimizeError::PruneNoRule(plan_node_name(&plan)));
    }
    plan.try_map_children(|child| prune_columns(child, required))
}

/// A derived table prunes inside only when its body pins an explicit output
/// schema (a bare-plan body's scan columns ARE the derived table's schema). Ports
/// `_prune_subquery_scan`.
fn prune_subquery_scan(
    mut node: SubqueryScan,
    required: &HashSet<String>,
) -> Result<LogicalPlan, OptimizeError> {
    if !has_explicit_projection(&node.input) {
        return Ok(LogicalPlan::SubqueryScan(node));
    }
    let pruned = prune_columns(*node.input, required)?;
    node.input = Box::new(pruned);
    Ok(LogicalPlan::SubqueryScan(node))
}

/// Prune a WITH query's main child always; the named subplan only when its own
/// root pins an explicit output schema. Ports `_prune_cte`.
fn prune_cte(mut cte: Cte, required: &HashSet<String>) -> Result<LogicalPlan, OptimizeError> {
    let new_child = prune_columns(*cte.child, required)?;
    let cte_plan = *cte.cte_plan;
    let new_cte_plan = if has_explicit_projection(&cte_plan) {
        prune_columns(cte_plan, required)?
    } else {
        cte_plan
    };
    cte.child = Box::new(new_child);
    cte.cte_plan = Box::new(new_cte_plan);
    Ok(LogicalPlan::Cte(cte))
}

/// Prune inside each explicitly-projected branch of a set operation (its root
/// Projection pins the branch output list, so pruning below cannot disturb
/// cross-branch positional alignment). Ports `_prune_branches`.
fn prune_branches(
    plan: LogicalPlan,
    required: &HashSet<String>,
) -> Result<LogicalPlan, OptimizeError> {
    let branches = set_operation_branches(&plan);
    let mut new_branches = Vec::with_capacity(branches.len());
    for branch in branches {
        if has_explicit_projection(&branch) {
            new_branches.push(prune_columns(branch, required)?);
        } else {
            new_branches.push(branch);
        }
    }
    Ok(with_set_operation_branches(plan, new_branches))
}

/// Prune a scan to its referenced columns, under the safety guards. Ports
/// `_prune_scan_columns`.
fn prune_scan_columns(scan: Scan, required: &HashSet<String>) -> LogicalPlan {
    if scan_prune_blocked(&scan, required) {
        return LogicalPlan::Scan(Box::new(scan));
    }
    let mut needed = Vec::new();
    for column in &scan.columns {
        if scan_keeps_column(&scan, column, required) {
            needed.push(column.clone());
        }
    }
    // Empty means no reference names this relation (an EXISTS-only or hand-built
    // shape): keep everything - a zero-column scan is not representable as SQL.
    if needed.is_empty() || needed.len() == scan.columns.len() {
        return LogicalPlan::Scan(Box::new(scan));
    }
    let mut scan = scan;
    scan.columns = needed;
    LogicalPlan::Scan(Box::new(scan))
}

/// Guards blocking a scan from pruning: a pushed-aggregate scan's columns are its
/// OUTPUT shape and a DISTINCT scan's columns are its dedup set (both semantic);
/// a star reference means keep-all. Ports `_scan_prune_blocked`.
fn scan_prune_blocked(scan: &Scan, required: &HashSet<String>) -> bool {
    if scan.aggregates.is_some() || scan.group_by.is_some() || scan.output_names.is_some() {
        return true;
    }
    if scan.distinct {
        return true;
    }
    star_required(scan, required)
}

/// Whether a bare `*` or this relation's `t.*` is required. Ports `_star_required`.
fn star_required(scan: &Scan, required: &HashSet<String>) -> bool {
    if required.contains("*") {
        return true;
    }
    scan_qualifiers(scan)
        .iter()
        .any(|name| required.contains(&format!("{name}.*")))
}

/// A column stays when referenced bare or through any of the scan's relation
/// names (alias and physical table name both tolerated). Ports
/// `_scan_keeps_column`.
fn scan_keeps_column(scan: &Scan, column: &str, required: &HashSet<String>) -> bool {
    if required.contains(column) {
        return true;
    }
    scan_qualifiers(scan)
        .iter()
        .any(|name| required.contains(&format!("{name}.{column}")))
}

/// The relation names a reference may use for this scan. Ports `_scan_qualifiers`.
fn scan_qualifiers(scan: &Scan) -> Vec<String> {
    let mut names = vec![scan.table_name.clone()];
    if let Some(alias) = &scan.alias {
        if alias != &scan.table_name {
            names.push(alias.clone());
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{BinaryOpType, ColumnRef, LiteralValue};
    use fq_plan::logical::{Filter, Join, JoinType, Projection};

    /// A scan reading `columns` from `table`, aliased to `table`.
    fn scan(table: &str, columns: &[&str]) -> LogicalPlan {
        let mut node = Scan::new(
            "ds",
            "public",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        );
        node.alias = Some(table.to_string());
        LogicalPlan::Scan(Box::new(node))
    }

    /// A qualified column reference.
    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    #[test]
    fn no_projection_anywhere_leaves_plan_untouched() {
        let plan = LogicalPlan::Filter(Filter {
            input: Box::new(scan("t", &["a", "b", "c"])),
            predicate: col("t", "a"),
        });
        assert_eq!(ProjectionPushdown.apply(plan.clone()).unwrap(), plan);
    }

    #[test]
    fn surviving_subquery_expression_raises_rather_than_prunes() {
        // A subquery Expr must never reach the optimizer (decorrelation removes
        // it). If one does, its columns are invisible to the pruner, so pruning
        // would silently drop a needed column - raise loudly instead of doing so.
        let predicate = Expr::InSubquery {
            value: Box::new(col("t", "a")),
            subquery: Box::new(scan("other", &["a"])),
            negated: false,
        };
        let plan = LogicalPlan::Projection(Projection {
            input: Box::new(LogicalPlan::Filter(Filter {
                input: Box::new(scan("t", &["a", "b"])),
                predicate,
            })),
            expressions: vec![col("t", "a")],
            aliases: vec!["a".to_string()],
            distinct: false,
            distinct_on: None,
        });
        let err = ProjectionPushdown
            .apply(plan)
            .expect_err("subquery survived");
        assert!(matches!(err, OptimizeError::SubquerySurvived(_)));
    }

    #[test]
    fn join_query_prunes_each_scan_to_referenced_columns() {
        // SELECT o.id FROM orders o JOIN customer c ON o.cust_id = c.id
        //   WHERE c.region = 'EU'
        let condition = Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(col("orders", "cust_id")),
            right: Box::new(col("customer", "id")),
        };
        let join = LogicalPlan::Join(Join {
            left: Box::new(scan("orders", &["id", "cust_id", "extra"])),
            right: Box::new(scan("customer", &["id", "region", "unused"])),
            join_type: JoinType::Inner,
            condition: Some(condition),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        let filter = LogicalPlan::Filter(Filter {
            input: Box::new(join),
            predicate: Expr::BinaryOp {
                op: BinaryOpType::Eq,
                left: Box::new(col("customer", "region")),
                right: Box::new(Expr::Literal {
                    value: LiteralValue::String("EU".to_string()),
                    data_type: DataType::Varchar,
                }),
            },
        });
        let projection = LogicalPlan::Projection(Projection {
            input: Box::new(filter),
            expressions: vec![col("orders", "id")],
            aliases: vec!["id".to_string()],
            distinct: false,
            distinct_on: None,
        });
        let result = ProjectionPushdown.apply(projection).unwrap();
        // Dig out the two scans and assert their pruned column lists.
        let mut scans = Vec::new();
        collect_scans(&result, &mut scans);
        let orders = scans.iter().find(|s| s.table_name == "orders").unwrap();
        let customer = scans.iter().find(|s| s.table_name == "customer").unwrap();
        assert_eq!(
            orders.columns,
            vec!["id".to_string(), "cust_id".to_string()]
        );
        assert_eq!(
            customer.columns,
            vec!["id".to_string(), "region".to_string()]
        );
    }

    #[test]
    fn count_star_does_not_block_pruning() {
        // SELECT COUNT(*) FROM orders o -- the star arg must not keep every column
        let count_star = Expr::FunctionCall {
            function_name: "COUNT".to_string(),
            args: vec![Expr::Column(ColumnRef::new(None, "*", None))],
            is_aggregate: true,
            distinct: false,
            within_group_key: None,
            within_group_desc: false,
        };
        let mut columns = HashSet::new();
        collect_expression(&count_star, &mut columns).unwrap();
        assert!(columns.is_empty(), "COUNT(*) collects no columns");
    }

    #[test]
    fn prune_no_rule_raises_on_unmodeled_node() {
        // A Projection whose input is a Values node - Values is a leaf, so it is
        // reached only via prune_through if we hand it directly. Build a plan that
        // routes an unmodeled node into prune_through by wrapping a Values under a
        // modeled Projection but forcing a prune of a Values-through path.
        // Directly exercise prune_through with a genuinely unmodeled node.
        let values = LogicalPlan::Values(fq_plan::logical::Values {
            rows: vec![],
            output_names: vec![],
        });
        let required = HashSet::new();
        // Values is handled by prune_columns (returned as-is), so it does NOT
        // raise; instead prove prune_through raises on something unmodeled by
        // handing it a CteRef (a leaf prune_columns returns, but prune_through
        // would reject). We test prune_through's guard directly.
        let cte_ref = LogicalPlan::CteRef(fq_plan::logical::CteRef {
            name: "w".to_string(),
            alias: None,
            columns: None,
            output_names: None,
        });
        assert!(matches!(
            prune_through(cte_ref, &required),
            Err(OptimizeError::PruneNoRule("CteRef"))
        ));
        let _ = values;
    }

    #[test]
    fn distinct_scan_is_not_pruned() {
        let mut node = Scan::new("ds", "public", "t", vec!["a".to_string(), "b".to_string()]);
        node.alias = Some("t".to_string());
        node.distinct = true;
        let scan = LogicalPlan::Scan(Box::new(node));
        // Only t.a is required, but DISTINCT columns are the dedup set: keep both.
        let mut required = HashSet::new();
        required.insert("t.a".to_string());
        let LogicalPlan::Scan(pruned) = prune_columns(scan, &required).unwrap() else {
            panic!("still a scan");
        };
        assert_eq!(pruned.columns, vec!["a".to_string(), "b".to_string()]);
    }

    /// Collect every Scan node in a plan (test helper).
    fn collect_scans<'a>(plan: &'a LogicalPlan, out: &mut Vec<&'a Scan>) {
        if let LogicalPlan::Scan(scan) = plan {
            out.push(scan);
        }
        for child in plan.children() {
            collect_scans(child, out);
        }
    }
}
