//! The value-position path: thread joins under a running plan so a subquery in a
//! SELECT list, an ORDER BY key, or a compound predicate becomes a column read.
//! A scalar subquery becomes a LEFT join providing its value; a boolean subquery
//! becomes a SEMI/ANTI branch pair unioned with a flag column. Ports
//! `_rewrite_value_expr`, `_join_scalar`, `_join_flag`, the projection/sort
//! rewrites, and `_tighten_scalar_equality`.

use std::collections::HashSet;

use fq_plan::expr::{column_refs, BinaryOpType, Expr, LiteralValue};
use fq_plan::logical::{Join, JoinType, LogicalPlan, Projection, Scan, Sort, Union};

use crate::boolean::{any_has_subquery, constant_exists_value};
use crate::error::DecorrelationError;
use crate::helpers::{bool_literal, qualified_col, unqualified_col};
use crate::{Decorrelator, Result};

impl Decorrelator {
    /// Rewrite projection expressions, threading each subquery value in as a join
    /// underneath. A scalar becomes a LEFT join; a boolean becomes the flag union.
    pub(crate) fn rewrite_projection(&mut self, node: Projection) -> Result<LogicalPlan> {
        let mut plan = self.rewrite_plan(*node.input)?;
        let mut expressions = Vec::with_capacity(node.expressions.len());
        for expr in node.expressions {
            let (rewritten, next) = self.rewrite_value_expr(expr, plan)?;
            plan = next;
            expressions.push(rewritten);
        }
        Ok(LogicalPlan::Projection(Projection {
            input: Box::new(plan),
            expressions,
            ..node
        }))
    }

    /// Rewrite sort keys; prune the helper columns added for subquery sort keys.
    pub(crate) fn rewrite_sort(&mut self, node: Sort) -> Result<LogicalPlan> {
        let Sort {
            input,
            sort_keys,
            ascending,
            nulls_order,
        } = node;
        let input_plan = self.rewrite_plan(*input)?;
        let rebuilt = Sort {
            input: Box::new(input_plan),
            sort_keys,
            ascending,
            nulls_order,
        };
        if !any_has_subquery(&rebuilt.sort_keys) {
            return Ok(LogicalPlan::Sort(rebuilt));
        }
        self.rewrite_sort_with_subqueries(rebuilt)
    }

    /// Join subquery sort-key values below the sort, then re-project.
    ///
    /// A correlated sort-key subquery references columns of the relation below the
    /// SELECT projection; those columns may be projected away, so the join is
    /// planted beneath the projection, the projection widened to carry the helper
    /// columns, and the result pruned back to the original output. `node.input` is
    /// already the decorrelated input.
    fn rewrite_sort_with_subqueries(&mut self, node: Sort) -> Result<LogicalPlan> {
        let original_names = node.input.schema();
        if matches!(node.input.as_ref(), LogicalPlan::Projection(_)) {
            self.sort_subqueries_below_projection(node, &original_names)
        } else {
            self.sort_subqueries_inline(node, &original_names)
        }
    }

    /// Plant sort-key subquery joins directly above the input, then prune.
    fn sort_subqueries_inline(
        &mut self,
        node: Sort,
        original_names: &[String],
    ) -> Result<LogicalPlan> {
        let Sort {
            input,
            sort_keys,
            ascending,
            nulls_order,
        } = node;
        let (keys, plan) = self.rewrite_sort_keys(sort_keys, *input)?;
        let sorted = LogicalPlan::Sort(Sort {
            input: Box::new(plan),
            sort_keys: keys,
            ascending,
            nulls_order,
        });
        Ok(prune_to_names(sorted, original_names))
    }

    /// Plant sort-key joins beneath the projection where the columns exist.
    fn sort_subqueries_below_projection(
        &mut self,
        node: Sort,
        original_names: &[String],
    ) -> Result<LogicalPlan> {
        let Sort {
            input,
            sort_keys,
            ascending,
            nulls_order,
        } = node;
        let LogicalPlan::Projection(projection) = *input else {
            return Err(DecorrelationError::Invariant(
                "sort-below-projection expected a projection input".to_string(),
            ));
        };
        let Projection {
            input: proj_input,
            expressions,
            aliases,
            distinct,
            distinct_on,
        } = projection;
        let rewritten = self.rewrite_plan(*proj_input)?;
        let (keys, plan) = self.rewrite_sort_keys(sort_keys, rewritten)?;
        let widened =
            widen_projection_for_keys(expressions, aliases, distinct, distinct_on, plan, &keys);
        let sorted = LogicalPlan::Sort(Sort {
            input: Box::new(widened),
            sort_keys: keys,
            ascending,
            nulls_order,
        });
        Ok(prune_to_names(sorted, original_names))
    }

    /// Thread every sort key through the value rewrite, accumulating the plan.
    fn rewrite_sort_keys(
        &mut self,
        sort_keys: Vec<Expr>,
        input_plan: LogicalPlan,
    ) -> Result<(Vec<Expr>, LogicalPlan)> {
        let mut plan = input_plan;
        let mut keys = Vec::with_capacity(sort_keys.len());
        for key in sort_keys {
            let (rewritten, next) = self.rewrite_value_expr(key, plan)?;
            plan = next;
            keys.push(rewritten);
        }
        Ok((keys, plan))
    }

    /// Rewrite one expression, threading joins through the plan. Scalar subqueries
    /// become LEFT joins providing a value column; boolean subqueries become
    /// SEMI/ANTI branch pairs providing a flag column.
    pub(crate) fn rewrite_value_expr(
        &mut self,
        expr: Expr,
        plan: LogicalPlan,
    ) -> Result<(Expr, LogicalPlan)> {
        if let Some(constant) = constant_exists_value(&expr) {
            return Ok((bool_literal(constant), plan));
        }
        match expr {
            Expr::Subquery { subquery } => self.join_scalar(*subquery, plan),
            Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::QuantifiedComparison { .. } => {
                self.join_flag(expr, plan)
            }
            other => self.rewrite_value_children(other, plan),
        }
    }

    /// Recurse value rewriting through an expression's children, threading the
    /// plan left-to-right. A child rewrite that errors latches and propagates.
    fn rewrite_value_children(
        &mut self,
        expr: Expr,
        plan: LogicalPlan,
    ) -> Result<(Expr, LogicalPlan)> {
        let mut plan_slot: Option<LogicalPlan> = Some(plan);
        let mut error: Option<DecorrelationError> = None;
        let rebuilt = expr.map_children(&mut |child| {
            if error.is_some() {
                return child;
            }
            let plan = plan_slot
                .take()
                .expect("threaded plan is present between children");
            match self.rewrite_value_expr(child, plan) {
                Ok((rewritten, next)) => {
                    plan_slot = Some(next);
                    rewritten
                }
                Err(err) => {
                    // Latch the error and return a discarded placeholder; the whole
                    // rebuilt tree is thrown away below when the error propagates.
                    error = Some(err);
                    bool_literal(false)
                }
            }
        });
        if let Some(err) = error {
            return Err(err);
        }
        Ok((
            rebuilt,
            plan_slot.expect("threaded plan is present after the last child"),
        ))
    }

    /// LEFT-join a scalar subquery's value onto the plan. A correlation that
    /// cannot flatten to a set-based join (non-equality crossing an aggregate or
    /// LIMIT) is the Neumann-Kemper dependent-join fallback. `NonFlattenable` is
    /// caught ONLY here (the "catch once" rule); every other error propagates.
    fn join_scalar(
        &mut self,
        subquery: LogicalPlan,
        plan: LogicalPlan,
    ) -> Result<(Expr, LogicalPlan)> {
        // The dependent-join fallback re-rewrites the original subquery, so keep a
        // copy before `prepare_scalar` consumes it (only used on the fallback arm).
        let prepared = match self.preparer().prepare_scalar(subquery.clone()) {
            Ok(prepared) => prepared,
            Err(DecorrelationError::NonFlattenable(_)) => {
                return self.unnest_dependent_scalar(subquery, plan);
            }
            Err(other) => return Err(other),
        };
        // Uncorrelated -> ON TRUE; correlated -> ON the pulled condition.
        let condition = prepared.condition.unwrap_or_else(|| bool_literal(true));
        let joined = LogicalPlan::Join(Join {
            left: Box::new(plan),
            right: Box::new(prepared.plan),
            join_type: JoinType::Left,
            condition: Some(condition),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        Ok((prepared.replacement, joined))
    }

    /// Produce a boolean flag column for a subquery predicate in a value position.
    /// The plan splits into a SEMI branch (matching rows, tagged TRUE) and an ANTI
    /// branch (non-matching rows, tagged FALSE); their non-distinct union restores
    /// the full input row set exactly once, each row carrying its flag.
    pub(crate) fn join_flag(
        &mut self,
        expr: Expr,
        plan: LogicalPlan,
    ) -> Result<(Expr, LogicalPlan)> {
        let (right, condition, positive) = self.semi_anti_parts(expr)?;
        let flag_name = format!("{}_flag", self.next_prefix());
        let match_branch = flag_branch(
            plan.clone(),
            right.clone(),
            condition.clone(),
            JoinType::Semi,
            positive,
            &flag_name,
        )?;
        let miss_branch = flag_branch(
            plan,
            right,
            condition,
            JoinType::Anti,
            !positive,
            &flag_name,
        )?;
        let union = LogicalPlan::Union(Union {
            inputs: vec![match_branch, miss_branch],
            distinct: false,
        });
        Ok((unqualified_col(&flag_name), union))
    }
}

/// One half of a flag pair: join, then append the literal flag over a passthrough
/// of every left-input column.
///
/// PORT NOTE (the `*` red flag): Python emits a transient `ColumnRef("*")` /
/// alias `"*"` passthrough, forbidden here. Instead the branch's left-input
/// columns are expanded to one QUALIFIED `ColumnRef` per column (their owning
/// relation's qualifier read from the plan); a shape whose columns cannot be
/// qualified raises loudly rather than emitting an unqualified or starred column.
fn flag_branch(
    plan: LogicalPlan,
    right: LogicalPlan,
    condition: Option<Expr>,
    join_type: JoinType,
    flag_value: bool,
    flag_name: &str,
) -> Result<LogicalPlan> {
    let names = plan.schema();
    let mut expressions = passthrough_columns(&plan)?;
    if expressions.len() != names.len() {
        return Err(DecorrelationError::Invariant(format!(
            "flag-branch passthrough produced {} columns for {} outputs",
            expressions.len(),
            names.len()
        )));
    }
    let mut aliases = names;
    expressions.push(bool_literal(flag_value));
    aliases.push(flag_name.to_string());
    let joined = LogicalPlan::Join(Join {
        left: Box::new(plan),
        right: Box::new(right),
        join_type,
        condition,
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    });
    Ok(LogicalPlan::Projection(Projection {
        input: Box::new(joined),
        expressions,
        aliases,
        distinct: false,
        distinct_on: None,
    }))
}

/// One QUALIFIED column reference per output column of `plan`, in schema order:
/// the passthrough a flag branch carries through alongside its flag. Raises when a
/// column's owning relation is unavailable (an aggregate/values boundary), so no
/// unqualified or starred passthrough column is ever produced.
fn passthrough_columns(plan: &LogicalPlan) -> Result<Vec<Expr>> {
    match plan {
        LogicalPlan::Scan(scan) => Ok(relation_columns(&scan_qualifier(scan), &plan.schema())),
        LogicalPlan::SubqueryScan(node) => Ok(relation_columns(&node.alias, &plan.schema())),
        LogicalPlan::CteRef(node) => {
            let qualifier = node.alias.clone().unwrap_or_else(|| node.name.clone());
            Ok(relation_columns(&qualifier, &plan.schema()))
        }
        LogicalPlan::Filter(node) => passthrough_columns(&node.input),
        LogicalPlan::Sort(node) => passthrough_columns(&node.input),
        LogicalPlan::Limit(node) => passthrough_columns(&node.input),
        LogicalPlan::GroupedLimit(node) => passthrough_columns(&node.input),
        LogicalPlan::SingleRowGuard(node) => passthrough_columns(&node.input),
        LogicalPlan::Explain(node) => passthrough_columns(&node.input),
        LogicalPlan::Cte(node) => passthrough_columns(&node.child),
        LogicalPlan::Join(node) => join_passthrough(node),
        LogicalPlan::LateralJoin(node) => {
            let mut columns = passthrough_columns(&node.left)?;
            columns.extend(passthrough_columns(&node.right)?);
            Ok(columns)
        }
        LogicalPlan::SetOperation(node) => passthrough_columns(&node.left),
        LogicalPlan::Union(node) => match node.inputs.first() {
            Some(first) => passthrough_columns(first),
            None => Err(DecorrelationError::Invariant(
                "an empty union has no passthrough columns".to_string(),
            )),
        },
        LogicalPlan::Projection(node) => projection_passthrough(node),
        LogicalPlan::Aggregate(_) | LogicalPlan::Values(_) => Err(DecorrelationError::Unsupported(
            "cannot qualify a boolean-subquery flag passthrough over an aggregate or values \
                 input"
                .to_string(),
        )),
    }
}

/// The passthrough columns of a join: left columns, plus right columns unless the
/// join is existential (SEMI/ANTI expose only the left side).
fn join_passthrough(node: &Join) -> Result<Vec<Expr>> {
    let mut columns = passthrough_columns(&node.left)?;
    if !matches!(node.join_type, JoinType::Semi | JoinType::Anti) {
        columns.extend(passthrough_columns(&node.right)?);
    }
    Ok(columns)
}

/// The passthrough columns of a projection: a plain column output is referenced
/// by its (qualified) source ref; a computed output has no owning relation, so it
/// is referenced unqualified by its alias (e.g. a nested flag column).
fn projection_passthrough(node: &Projection) -> Result<Vec<Expr>> {
    if node.aliases.iter().any(|alias| alias == "*") {
        return Err(DecorrelationError::Invariant(
            "a star projection cannot appear after binding".to_string(),
        ));
    }
    let mut columns = Vec::with_capacity(node.expressions.len());
    for (expr, alias) in node.expressions.iter().zip(node.aliases.iter()) {
        match expr {
            Expr::Column(col) => columns.push(Expr::Column(col.clone())),
            _ => columns.push(unqualified_col(alias)),
        }
    }
    Ok(columns)
}

/// The relation qualifier a scan's output columns carry: its explicit alias when
/// it differs from the base table name, else the table name.
fn scan_qualifier(scan: &Scan) -> String {
    match &scan.alias {
        Some(alias) if *alias != scan.table_name => alias.clone(),
        _ => scan.table_name.clone(),
    }
}

/// One qualified column reference per name, all qualified to `qualifier`.
fn relation_columns(qualifier: &str, names: &[String]) -> Vec<Expr> {
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        columns.push(qualified_col(qualifier, name));
    }
    columns
}

/// Re-project over `plan`, passing through the sort-key helper columns the
/// projection did not already output so the ORDER BY above can still see them.
fn widen_projection_for_keys(
    mut expressions: Vec<Expr>,
    mut aliases: Vec<String>,
    distinct: bool,
    distinct_on: Option<Vec<Expr>>,
    plan: LogicalPlan,
    keys: &[Expr],
) -> LogicalPlan {
    let existing: HashSet<String> = aliases.iter().cloned().collect();
    for name in sort_helper_columns(keys, &existing) {
        expressions.push(unqualified_col(&name));
        aliases.push(name);
    }
    LogicalPlan::Projection(Projection {
        input: Box::new(plan),
        expressions,
        aliases,
        distinct,
        distinct_on,
    })
}

/// Names referenced by the sort keys that the projection does not already output.
fn sort_helper_columns(keys: &[Expr], existing: &HashSet<String>) -> Vec<String> {
    let mut helpers: Vec<String> = Vec::new();
    for key in keys {
        for col in column_refs(key) {
            if !existing.contains(&col.column) && !helpers.contains(&col.column) {
                helpers.push(col.column.clone());
            }
        }
    }
    helpers
}

/// Project a plan back down to the given output columns, dropping the helper
/// columns added for subquery sort keys.
fn prune_to_names(plan: LogicalPlan, names: &[String]) -> LogicalPlan {
    let mut expressions = Vec::with_capacity(names.len());
    for name in names {
        expressions.push(unqualified_col(name));
    }
    LogicalPlan::Projection(Projection {
        input: Box::new(plan),
        expressions,
        aliases: names.to_vec(),
        distinct: false,
        distinct_on: None,
    })
}

/// An equality residual against the just-attached scalar's value column turns its
/// LEFT-ON-TRUE join into an INNER equi join. The filter would drop NULL
/// extensions and non-matching rows anyway (`col = NULL` is UNKNOWN), so the
/// multiset is identical - and an equi join is visible to semi-join reduction
/// where a cross join plus filter is not. Ports `_tighten_scalar_equality`.
pub(crate) fn tighten_scalar_equality(rewritten: &Expr, plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Join(join) = plan else {
        return None;
    };
    if join.join_type != JoinType::Left {
        return None;
    }
    if !is_true_literal(join.condition.as_ref()) {
        return None;
    }
    if !equality_splits_join_sides(rewritten, join) {
        return None;
    }
    Some(LogicalPlan::Join(Join {
        join_type: JoinType::Inner,
        condition: Some(rewritten.clone()),
        ..join.clone()
    }))
}

/// Whether a join condition is the unconditional TRUE literal.
fn is_true_literal(expr: Option<&Expr>) -> bool {
    matches!(
        expr,
        Some(Expr::Literal {
            value: LiteralValue::Boolean(true),
            ..
        })
    )
}

/// Whether the residual is a plain equality with one plain column per join side
/// (the shape an equi hash join consumes exactly). Decorrelation's `__subq` names
/// are globally unique, so right-side name membership is unambiguous.
fn equality_splits_join_sides(rewritten: &Expr, join: &Join) -> bool {
    let Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left,
        right,
    } = rewritten
    else {
        return false;
    };
    let (Expr::Column(first), Expr::Column(second)) = (left.as_ref(), right.as_ref()) else {
        return false;
    };
    let right_names: HashSet<String> = join.right.schema().into_iter().collect();
    right_names.contains(&first.column) != right_names.contains(&second.column)
}
