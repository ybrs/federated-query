//! `ProjectionPushdown`: prune every scan to the columns the query actually
//! references, collected globally. The ONE rule that can raise: its pruner
//! reaching an unmodeled plan node. Ports `ProjectionPushdownRule`.

use std::collections::HashSet;

use fq_plan::expr::Expr;
use fq_plan::logical::{Cte, LogicalPlan, Projection, Scan, SubqueryScan, Union};

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
        collect_required_columns(&plan, &mut required, true)?;
        prune_columns(plan, &required, true)
    }
}

/// Whether a union's branch projections may be NARROWED: the union must be
/// non-distinct (dedup runs over the full output row), every branch root must be
/// a plain projection (no DISTINCT / DISTINCT ON of its own), and all branches
/// must agree on arity. Narrowing drops the same positions from every branch, so
/// only shapes where a dropped position is invisible to the consumer qualify;
/// positional consumers are excluded by the caller's `narrow` flag.
fn narrowable_union(union: &Union) -> bool {
    if union.distinct {
        return false;
    }
    let mut arity = None;
    for branch in &union.inputs {
        let Some(projection) = branch_projection(branch) else {
            return false;
        };
        if projection.distinct || projection.distinct_on.is_some() {
            return false;
        }
        if *arity.get_or_insert(projection.expressions.len()) != projection.expressions.len() {
            return false;
        }
    }
    arity.is_some()
}

/// The schema-pinning projection of a union branch, seen through the
/// row-schema-preserving single-input wrappers a branch may carry above it
/// (predicate pushdown distributes filters into branches before this rule
/// runs). None when the branch bottoms out in anything else.
fn branch_projection(branch: &LogicalPlan) -> Option<&Projection> {
    match branch {
        LogicalPlan::Projection(projection) => Some(projection),
        LogicalPlan::Filter(node) => branch_projection(&node.input),
        LogicalPlan::Sort(node) => branch_projection(&node.input),
        LogicalPlan::Limit(node) => branch_projection(&node.input),
        LogicalPlan::GroupedLimit(node) => branch_projection(&node.input),
        LogicalPlan::SingleRowGuard(node) => branch_projection(&node.input),
        _ => None,
    }
}

/// Per-child "a union child may be narrowed here" flags, aligned with
/// `children()` order. False under POSITIONAL consumers: a set operation
/// compares full rows, an outer union aligns branches by position, and a
/// column_names rename maps positions to new names. Everything else resolves
/// the union output by name, so dropping an unreferenced position is invisible.
fn child_narrow_flags(plan: &LogicalPlan) -> Vec<bool> {
    match plan {
        LogicalPlan::SubqueryScan(node) => vec![node.column_names.is_none()],
        LogicalPlan::Cte(node) => vec![node.column_names.is_none(), true],
        LogicalPlan::Union(_) | LogicalPlan::SetOperation(_) => {
            plan.children().iter().map(|_| false).collect()
        }
        _ => plan.children().iter().map(|_| true).collect(),
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
    narrow: bool,
) -> Result<(), OptimizeError> {
    // A narrowable union's plain passthrough entries are NOT requirements: which
    // of them survive is decided at the union during pruning (by what the plan
    // ABOVE references), and the kept ones re-enter the required set there. Only
    // computed entries always contribute. This is what lets a decorrelation flag
    // union's every-column passthrough stop pinning every scan column globally.
    if let LogicalPlan::Union(union) = plan {
        if narrow && narrowable_union(union) {
            for branch in &union.inputs {
                collect_narrowable_branch(branch, columns)?;
            }
            return Ok(());
        }
    }
    for expression in plan.direct_expressions() {
        collect_expression(expression, columns)?;
    }
    for (child, child_narrow) in plan.children().into_iter().zip(child_narrow_flags(plan)) {
        collect_required_columns(child, columns, child_narrow)?;
    }
    Ok(())
}

/// Requirements of one narrowable-union branch: the wrappers above the pinning
/// projection contribute their expressions normally (a branch filter's
/// references must keep their positions), the projection contributes only its
/// COMPUTED entries (plain passthrough entries are decided at the union during
/// pruning), and the projection's input subtree collects normally.
fn collect_narrowable_branch(
    branch: &LogicalPlan,
    columns: &mut HashSet<String>,
) -> Result<(), OptimizeError> {
    if let LogicalPlan::Projection(projection) = branch {
        for expression in &projection.expressions {
            if !matches!(expression, Expr::Column(_)) {
                collect_expression(expression, columns)?;
            }
        }
        return collect_required_columns(&projection.input, columns, true);
    }
    for expression in branch.direct_expressions() {
        collect_expression(expression, columns)?;
    }
    match branch {
        LogicalPlan::Filter(node) => collect_narrowable_branch(&node.input, columns),
        LogicalPlan::Sort(node) => collect_narrowable_branch(&node.input, columns),
        LogicalPlan::Limit(node) => collect_narrowable_branch(&node.input, columns),
        LogicalPlan::GroupedLimit(node) => collect_narrowable_branch(&node.input, columns),
        LogicalPlan::SingleRowGuard(node) => collect_narrowable_branch(&node.input, columns),
        _ => unreachable!("narrowable_union admits only wrapper chains over a projection"),
    }
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
    narrow: bool,
) -> Result<LogicalPlan, OptimizeError> {
    match plan {
        LogicalPlan::Scan(scan) => Ok(prune_scan_columns(*scan, required)),
        LogicalPlan::SubqueryScan(node) => prune_subquery_scan(node, required),
        LogicalPlan::Cte(node) => prune_cte(node, required),
        LogicalPlan::Union(union) if narrow && narrowable_union(&union) => {
            narrow_union(union, required)
        }
        set_op @ (LogicalPlan::SetOperation(_) | LogicalPlan::Union(_)) => {
            prune_branches(set_op, required)
        }
        leaf @ (LogicalPlan::Values(_) | LogicalPlan::CteRef(_)) => Ok(leaf),
        other => prune_through(other, required),
    }
}

/// Narrow a flag-style union: keep a position when any branch computes it (a
/// non-column expression) or its output name (branch 0's alias, the union's
/// schema) is column-referenced anywhere in the required set; drop the rest from
/// EVERY branch. The kept entries' columns re-enter the required set for the
/// branch subtrees - the collect phase deliberately skipped them, so this
/// augmentation is what the scans below actually see. When nothing is droppable
/// (or nothing is kept) the union is rebuilt whole with the same augmentation.
///
/// The keep test matches by column NAME across all qualifications (over-keeping
/// on a name collision is safe; dropping a referenced position is not), and a
/// required `*` keeps everything.
fn narrow_union(union: Union, required: &HashSet<String>) -> Result<LogicalPlan, OptimizeError> {
    let referenced_names: HashSet<&str> = required
        .iter()
        .map(|name| match name.rsplit_once('.') {
            Some((_, column)) => column,
            None => name.as_str(),
        })
        .collect();
    let star_required = referenced_names.contains("*");
    let arity = union
        .inputs
        .first()
        .and_then(|branch| branch_projection(branch))
        .map_or(0, |projection| projection.expressions.len());
    let mut keep = vec![star_required; arity];
    for branch in &union.inputs {
        let projection =
            branch_projection(branch).expect("narrowable_union checked the branch shape");
        for (index, expression) in projection.expressions.iter().enumerate() {
            if !matches!(expression, Expr::Column(_)) {
                keep[index] = true;
            }
        }
    }
    if let Some(first) = union.inputs.first().and_then(|b| branch_projection(b)) {
        for (index, alias) in first.aliases.iter().enumerate() {
            if referenced_names.contains(alias.as_str()) {
                keep[index] = true;
            }
        }
    }
    // An entirely unreferenced union output keeps every position: dropping all
    // of them would leave an empty projection, which is not representable.
    if !keep.contains(&true) {
        keep = vec![true; arity];
    }
    let mut augmented = required.clone();
    for branch in &union.inputs {
        let projection =
            branch_projection(branch).expect("narrowable_union checked the branch shape");
        for (index, expression) in projection.expressions.iter().enumerate() {
            if keep[index] {
                if let Expr::Column(_) = expression {
                    augmented.extend(qualified_or_bare_names(expression));
                }
            }
        }
    }
    let mut union = union;
    let mut new_branches = Vec::with_capacity(union.inputs.len());
    for branch in union.inputs {
        new_branches.push(narrow_branch(branch, &keep, &augmented)?);
    }
    union.inputs = new_branches;
    Ok(LogicalPlan::Union(union))
}

/// Rebuild one union branch with its pinning projection narrowed to the kept
/// positions: wrappers above the projection are preserved, the projection keeps
/// only `keep`-marked entries, and its input subtree prunes against the
/// augmented required set.
fn narrow_branch(
    branch: LogicalPlan,
    keep: &[bool],
    augmented: &HashSet<String>,
) -> Result<LogicalPlan, OptimizeError> {
    match branch {
        LogicalPlan::Projection(mut projection) => {
            let mut expressions = Vec::new();
            let mut aliases = Vec::new();
            for (index, (expression, alias)) in projection
                .expressions
                .into_iter()
                .zip(projection.aliases)
                .enumerate()
            {
                if keep[index] {
                    expressions.push(expression);
                    aliases.push(alias);
                }
            }
            projection.expressions = expressions;
            projection.aliases = aliases;
            projection.input = Box::new(prune_columns(*projection.input, augmented, true)?);
            Ok(LogicalPlan::Projection(projection))
        }
        LogicalPlan::Filter(mut node) => {
            node.input = Box::new(narrow_branch(*node.input, keep, augmented)?);
            Ok(LogicalPlan::Filter(node))
        }
        LogicalPlan::Sort(mut node) => {
            node.input = Box::new(narrow_branch(*node.input, keep, augmented)?);
            Ok(LogicalPlan::Sort(node))
        }
        LogicalPlan::Limit(mut node) => {
            node.input = Box::new(narrow_branch(*node.input, keep, augmented)?);
            Ok(LogicalPlan::Limit(node))
        }
        LogicalPlan::GroupedLimit(mut node) => {
            node.input = Box::new(narrow_branch(*node.input, keep, augmented)?);
            Ok(LogicalPlan::GroupedLimit(node))
        }
        LogicalPlan::SingleRowGuard(mut node) => {
            node.input = Box::new(narrow_branch(*node.input, keep, augmented)?);
            Ok(LogicalPlan::SingleRowGuard(node))
        }
        _ => unreachable!("narrowable_union admits only wrapper chains over a projection"),
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
    plan.try_map_children(|child| prune_columns(child, required, true))
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
    // A column_names rename maps the input's outputs POSITIONALLY, so a union
    // below it must not be narrowed; a plain alias resolves by name and may be.
    let narrow = node.column_names.is_none();
    let pruned = prune_columns(*node.input, required, narrow)?;
    node.input = Box::new(pruned);
    Ok(LogicalPlan::SubqueryScan(node))
}

/// Prune a WITH query's main child always; the named subplan only when its own
/// root pins an explicit output schema. Ports `_prune_cte`.
fn prune_cte(mut cte: Cte, required: &HashSet<String>) -> Result<LogicalPlan, OptimizeError> {
    let new_child = prune_columns(*cte.child, required, true)?;
    let cte_plan = *cte.cte_plan;
    // WITH x(a, b) renames the body's outputs POSITIONALLY, so a union body must
    // not be narrowed under declared column names.
    let narrow_body = cte.column_names.is_none();
    let new_cte_plan = if has_explicit_projection(&cte_plan) {
        prune_columns(cte_plan, required, narrow_body)?
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
            // A branch aligns with its siblings by position: a union branch that
            // is itself a union must not be narrowed.
            new_branches.push(prune_columns(branch, required, false)?);
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
        let LogicalPlan::Scan(pruned) = prune_columns(scan, &required, true).unwrap() else {
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

    /// A boolean-flag union branch: passthrough columns of `t` plus a literal
    /// flag, the shape decorrelation's `join_flag` builds (test helper).
    fn flag_branch_projection(flag_value: bool) -> LogicalPlan {
        LogicalPlan::Projection(Projection {
            input: Box::new(scan("t", &["x", "y", "z"])),
            expressions: vec![
                col("t", "x"),
                col("t", "y"),
                col("t", "z"),
                Expr::Literal {
                    value: LiteralValue::Boolean(flag_value),
                    data_type: DataType::Boolean,
                },
            ],
            aliases: vec![
                "x".to_string(),
                "y".to_string(),
                "z".to_string(),
                "f".to_string(),
            ],
            distinct: false,
            distinct_on: None,
        })
    }

    /// Projection(t.x) over Filter(f OR t.y = 1) over `inner`: the consumer shape
    /// above a flag union - references x, y and the flag, never z (test helper).
    fn flag_union_consumer(inner: LogicalPlan) -> LogicalPlan {
        let flag_or_y = Expr::BinaryOp {
            op: BinaryOpType::Or,
            left: Box::new(Expr::Column(ColumnRef::new(
                None,
                "f",
                Some(DataType::Boolean),
            ))),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOpType::Eq,
                left: Box::new(col("t", "y")),
                right: Box::new(Expr::Literal {
                    value: LiteralValue::Integer(1),
                    data_type: DataType::Integer,
                }),
            }),
        };
        LogicalPlan::Projection(Projection {
            input: Box::new(LogicalPlan::Filter(Filter {
                input: Box::new(inner),
                predicate: flag_or_y,
            })),
            expressions: vec![col("t", "x")],
            aliases: vec!["x".to_string()],
            distinct: false,
            distinct_on: None,
        })
    }

    #[test]
    fn flag_union_passthrough_narrows_to_referenced_outputs() {
        // The flag union's branch projections enumerate every input column; only
        // x, y and the flag are referenced above, so z is dropped from both
        // branches and the scans prune to [x, y].
        let union = LogicalPlan::Union(fq_plan::logical::Union {
            inputs: vec![flag_branch_projection(true), flag_branch_projection(false)],
            distinct: false,
        });
        let result = ProjectionPushdown
            .apply(flag_union_consumer(union))
            .unwrap();
        let mut scans = Vec::new();
        collect_scans(&result, &mut scans);
        assert_eq!(scans.len(), 2);
        for scan in &scans {
            assert_eq!(scan.columns, vec!["x".to_string(), "y".to_string()]);
        }
        let mut branch_arities = Vec::new();
        collect_union_branch_arities(&result, &mut branch_arities);
        assert_eq!(branch_arities, vec![3, 3], "x, y and the flag survive");
    }

    #[test]
    fn flag_union_with_pushed_branch_filters_still_narrows() {
        // Predicate pushdown distributes the flag filter INTO the branches before
        // this rule runs, so each branch is Filter over Projection. The filter
        // references y and the flag; the consumer references x; z is dropped and
        // the scans prune to [x, y].
        let filtered_branch = |flag_value: bool| {
            let flag_or_y = Expr::BinaryOp {
                op: BinaryOpType::Or,
                left: Box::new(Expr::Column(ColumnRef::new(
                    None,
                    "f",
                    Some(DataType::Boolean),
                ))),
                right: Box::new(Expr::BinaryOp {
                    op: BinaryOpType::Eq,
                    left: Box::new(col("t", "y")),
                    right: Box::new(Expr::Literal {
                        value: LiteralValue::Integer(1),
                        data_type: DataType::Integer,
                    }),
                }),
            };
            LogicalPlan::Filter(Filter {
                input: Box::new(flag_branch_projection(flag_value)),
                predicate: flag_or_y,
            })
        };
        let union = LogicalPlan::Union(fq_plan::logical::Union {
            inputs: vec![filtered_branch(true), filtered_branch(false)],
            distinct: false,
        });
        let root = LogicalPlan::Projection(Projection {
            input: Box::new(union),
            expressions: vec![col("t", "x")],
            aliases: vec!["x".to_string()],
            distinct: false,
            distinct_on: None,
        });
        let result = ProjectionPushdown.apply(root).unwrap();
        let mut scans = Vec::new();
        collect_scans(&result, &mut scans);
        assert_eq!(scans.len(), 2);
        for scan in &scans {
            assert_eq!(scan.columns, vec!["x".to_string(), "y".to_string()]);
        }
        let mut branch_arities = Vec::new();
        collect_union_branch_arities(&result, &mut branch_arities);
        assert_eq!(branch_arities, vec![3, 3], "x, y and the flag survive");
    }

    #[test]
    fn distinct_union_is_not_narrowed() {
        // A DISTINCT union dedups over its full output row: dropping a column
        // changes the row set, so the branches keep every entry and the scans
        // keep every enumerated column.
        let union = LogicalPlan::Union(fq_plan::logical::Union {
            inputs: vec![flag_branch_projection(true), flag_branch_projection(false)],
            distinct: true,
        });
        let result = ProjectionPushdown
            .apply(flag_union_consumer(union))
            .unwrap();
        let mut scans = Vec::new();
        collect_scans(&result, &mut scans);
        for scan in &scans {
            assert_eq!(
                scan.columns,
                vec!["x".to_string(), "y".to_string(), "z".to_string()]
            );
        }
        let mut branch_arities = Vec::new();
        collect_union_branch_arities(&result, &mut branch_arities);
        assert_eq!(branch_arities, vec![4, 4]);
    }

    #[test]
    fn union_under_renaming_subquery_scan_is_not_narrowed() {
        // A SubqueryScan with column_names renames the union output POSITIONALLY:
        // narrowing would shift the positions under the rename, so the union
        // keeps every entry.
        let union = LogicalPlan::Union(fq_plan::logical::Union {
            inputs: vec![flag_branch_projection(true), flag_branch_projection(false)],
            distinct: false,
        });
        let renamed = LogicalPlan::SubqueryScan(SubqueryScan {
            input: Box::new(union),
            alias: "sq".to_string(),
            column_names: Some(vec![
                "x2".to_string(),
                "y2".to_string(),
                "z2".to_string(),
                "f2".to_string(),
            ]),
        });
        let root = LogicalPlan::Projection(Projection {
            input: Box::new(LogicalPlan::Filter(Filter {
                input: Box::new(renamed),
                predicate: Expr::Column(ColumnRef::new(
                    Some("sq".to_string()),
                    "f2",
                    Some(DataType::Boolean),
                )),
            })),
            expressions: vec![Expr::Column(ColumnRef::new(
                Some("sq".to_string()),
                "x2",
                Some(DataType::Integer),
            ))],
            aliases: vec!["x2".to_string()],
            distinct: false,
            distinct_on: None,
        });
        let result = ProjectionPushdown.apply(root).unwrap();
        let mut branch_arities = Vec::new();
        collect_union_branch_arities(&result, &mut branch_arities);
        assert_eq!(branch_arities, vec![4, 4]);
    }

    /// The expression arity of every union branch's pinning projection, seen
    /// through wrapper nodes (test helper).
    fn collect_union_branch_arities(plan: &LogicalPlan, out: &mut Vec<usize>) {
        if let LogicalPlan::Union(union) = plan {
            for branch in &union.inputs {
                if let Some(projection) = branch_projection(branch) {
                    out.push(projection.expressions.len());
                }
            }
        }
        for child in plan.children() {
            collect_union_branch_arities(child, out);
        }
    }
}
