//! The subquery preparer: turn one subquery plan into a join-ready right side.
//!
//! Correlated predicates are pulled out of the subquery's filter spine (and
//! legally through aggregates and limits), the inner columns they reference are
//! exposed under deterministic `<prefix>_*` names, and the caller receives the
//! renamed plan plus the join condition. Ports `_SubqueryPreparer` and
//! `_PreparedSubquery` from `decorrelation.py`.

use std::collections::{HashMap, HashSet};

use fq_common::DataType;
use fq_plan::expr::{column_refs, BinaryOpType, ColumnRef, Expr, LiteralValue, NullsOrder};
use fq_plan::logical::{
    Aggregate, Filter, Limit, LogicalPlan, Projection, SingleRowGuard, Sort, SubqueryScan,
};

use crate::error::DecorrelationError;
use crate::helpers::{
    and_join, binary, collect_inner_aliases, is_correlated, is_outer_ref, qualified_col,
    references_inner, references_outer, relation_output_types, replace_column_refs,
    typed_output_refs, unqualified_col,
};
use crate::{Decorrelator, Result};

/// A pending ORDER BY captured for a per-key LIMIT: (keys, ascending, nulls).
type OrderSpec = (Vec<Expr>, Vec<bool>, Option<Vec<Option<NullsOrder>>>);

/// The exposed correlation-key projection entries: (inner expr to project, alias).
type Exposures = Vec<(Expr, String)>;

/// A subquery transformed into a join-ready right side. `plan` exposes
/// deterministically renamed output columns; `condition` holds the rewritten
/// correlation conjuncts (None when uncorrelated); `values` are the renamed
/// output column names usable in comparisons; `alias` is the boundary prefix.
pub(crate) struct PreparedSubquery {
    pub plan: LogicalPlan,
    pub condition: Option<Expr>,
    pub values: Vec<String>,
    /// The data type of each value column in `values`, positionally aligned. A
    /// comparison against a value column reads this type onto its qualified ref.
    pub value_types: Vec<DataType>,
    pub alias: String,
}

/// A scalar subquery's join-ready right side plus the expression that stands in
/// for the scalar in the outer row. `condition` is the pulled correlation
/// predicate (None when uncorrelated); `replacement` is the `<prefix>`-qualified
/// value ref (or `COALESCE(ref, 0)` for a COUNT). Ports `_PreparedScalar`.
pub(crate) struct PreparedScalar {
    pub plan: LogicalPlan,
    pub condition: Option<Expr>,
    pub replacement: Expr,
}

/// Transforms one subquery plan into a join-ready right side.
pub(crate) struct SubqueryPreparer<'a> {
    decorrelator: &'a mut Decorrelator,
    prefix: String,
    inner_aliases: HashSet<String>,
    /// Correlated conjuncts pulled from the spine; MUTATED IN PLACE by index in
    /// `widen_aggregate` (a widened key rewrites its own entry), so it stays a Vec.
    pulled: Vec<Expr>,
    pending_limit: Option<u64>,
    pending_order: Option<Sort>,
}

impl<'a> SubqueryPreparer<'a> {
    /// Capture the owning decorrelator and this subquery's name prefix.
    pub(crate) fn new(decorrelator: &'a mut Decorrelator, prefix: String) -> Self {
        Self {
            decorrelator,
            prefix,
            inner_aliases: HashSet::new(),
            pulled: Vec::new(),
            pending_limit: None,
            pending_order: None,
        }
    }

    /// Prepare an EXISTS subquery: only correlation columns matter.
    pub(crate) fn prepare_exists(&mut self, subquery: LogicalPlan) -> Result<PreparedSubquery> {
        let plan = self.decorrelator.rewrite_plan(subquery)?;
        self.inner_aliases = collect_inner_aliases(&plan);
        if !is_correlated(&plan, &self.inner_aliases) {
            // Uncorrelated EXISTS only asks whether any row exists, so the right
            // side is the subplan capped at one row with no correlation condition.
            let capped = PreparedSubquery {
                // Fresh LIMIT 1 capping the uncorrelated existence subplan - no base
                // to copy from. Field list (input/limit/offset) is the complete Limit.
                plan: LogicalPlan::Limit(Limit {
                    input: Box::new(plan),
                    limit: Some(1),
                    offset: 0,
                }),
                condition: None,
                values: Vec::new(),
                value_types: Vec::new(),
                alias: self.prefix.clone(),
            };
            return Ok(self.finalize(capped));
        }
        let core = peel_exists_top(plan);
        let stripped = self.strip(core)?;
        let assembled = self.assemble(stripped, Vec::new(), Vec::new())?;
        Ok(self.finalize(assembled))
    }

    /// Prepare an IN/ANY/ALL subquery: expose its value columns.
    pub(crate) fn prepare_values(&mut self, subquery: LogicalPlan) -> Result<PreparedSubquery> {
        let plan = self.decorrelator.rewrite_plan(subquery)?;
        self.inner_aliases = collect_inner_aliases(&plan);
        let allow_wrappers = !is_correlated(&plan, &self.inner_aliases);
        let (core, value_exprs) = self.peel_values_top(plan, allow_wrappers)?;
        for value_expr in &value_exprs {
            if references_outer(value_expr, &self.inner_aliases) {
                return Err(DecorrelationError::Unsupported(
                    "Outer reference in a subquery's output column is not supported".to_string(),
                ));
            }
        }
        let stripped = self.strip(core)?;
        let mut value_names = Vec::with_capacity(value_exprs.len());
        for index in 0..value_exprs.len() {
            value_names.push(format!("{}_v{index}", self.prefix));
        }
        let assembled = self.assemble(stripped, value_exprs, value_names)?;
        Ok(self.finalize(assembled))
    }

    /// Wrap a prepared subquery under its prefix alias as a derived relation.
    fn finalize(&self, mut prepared: PreparedSubquery) -> PreparedSubquery {
        // Fresh boundary alias wrapping the prepared plan - no base to copy from.
        // Field list (input/alias/column_names) is the complete SubqueryScan struct.
        prepared.plan = LogicalPlan::SubqueryScan(SubqueryScan {
            input: Box::new(prepared.plan),
            alias: self.prefix.clone(),
            column_names: None,
        });
        prepared
    }

    // --- value-column extraction -------------------------------------------

    /// Split a subquery into its core and its output value expressions.
    ///
    /// `allow_wrappers` keeps a Sort or HAVING filter (or a set operation) as part
    /// of the value relation; enabled only for uncorrelated value subqueries.
    fn peel_values_top(
        &mut self,
        plan: LogicalPlan,
        allow_wrappers: bool,
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        match plan {
            LogicalPlan::Limit(node) => {
                self.record_limit(node.offset, node.limit)?;
                self.peel_values_top(*node.input, allow_wrappers)
            }
            LogicalPlan::Projection(node) => self.peel_values_projection(node),
            LogicalPlan::Values(node) => peel_values_node(node),
            LogicalPlan::Aggregate(node) => {
                let types: Vec<DataType> = node.aggregates.iter().map(Expr::get_type).collect();
                let refs = typed_output_refs(&node.output_names, &types)?;
                Ok((LogicalPlan::Aggregate(node), refs))
            }
            LogicalPlan::Sort(mut node) if allow_wrappers => {
                let (core, values) = self.peel_values_top(*node.input, true)?;
                node.input = Box::new(core);
                Ok((LogicalPlan::Sort(node), values))
            }
            LogicalPlan::Filter(mut node) if allow_wrappers => {
                let (core, values) = self.peel_values_top(*node.input, true)?;
                node.input = Box::new(core);
                Ok((LogicalPlan::Filter(node), values))
            }
            LogicalPlan::SetOperation(node) if allow_wrappers => {
                let plan = LogicalPlan::SetOperation(node);
                let refs = relation_value_refs(&plan)?;
                Ok((plan, refs))
            }
            other => Err(DecorrelationError::Unsupported(format!(
                "Unsupported subquery output shape: {}",
                plan_label(&other)
            ))),
        }
    }

    /// A projection's value expressions, rejecting star and keeping DISTINCT whole.
    fn peel_values_projection(&self, node: Projection) -> Result<(LogicalPlan, Vec<Expr>)> {
        reject_star_values(&node.expressions)?;
        if node.distinct || node.distinct_on.is_some() {
            return self.peel_distinct_projection(node);
        }
        let values = node.expressions.clone();
        Ok((*node.input, values))
    }

    /// A DISTINCT projection stays whole (peeling it would drop the dedup); a
    /// correlated one is rejected because its keys are not folded into DISTINCT.
    fn peel_distinct_projection(&self, node: Projection) -> Result<(LogicalPlan, Vec<Expr>)> {
        let plan = LogicalPlan::Projection(node);
        if is_correlated(&plan, &self.inner_aliases) {
            return Err(DecorrelationError::Unsupported(
                "DISTINCT in a correlated value subquery is not supported".to_string(),
            ));
        }
        let refs = relation_value_refs(&plan)?;
        Ok((plan, refs))
    }

    // --- correlation stripping ---------------------------------------------

    /// Remove correlated predicates from the subquery's filter spine.
    fn strip(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(node) => self.strip_filter(node),
            LogicalPlan::Aggregate(node) => self.strip_aggregate(node),
            LogicalPlan::Limit(node) => self.strip_limit(node),
            other => self.strip_other(other),
        }
    }

    /// Pass through a Sort onto its stripped input; stop at every other node.
    fn strip_other(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Sort(mut node) => {
                node.input = Box::new(self.strip(*node.input)?);
                Ok(LogicalPlan::Sort(node))
            }
            other => Ok(other),
        }
    }

    /// Pull correlated conjuncts out of a filter into `self.pulled`.
    fn strip_filter(&mut self, node: Filter) -> Result<LogicalPlan> {
        let stripped_input = self.strip(*node.input)?;
        let mut kept = Vec::new();
        let mut pulled_here = Vec::new();
        for conjunct in fq_plan::expr::split_conjuncts(&node.predicate) {
            if references_outer(conjunct, &self.inner_aliases) {
                pulled_here.push(conjunct.clone());
            } else {
                kept.push(conjunct.clone());
            }
        }
        let stripped_input = if let LogicalPlan::Aggregate(aggregate) = stripped_input {
            let (widened, new_kept, new_pulled) = self.hoist_having(aggregate, kept, pulled_here);
            kept = new_kept;
            pulled_here = new_pulled;
            LogicalPlan::Aggregate(widened)
        } else {
            stripped_input
        };
        self.pulled.extend(pulled_here);
        if kept.is_empty() {
            return Ok(stripped_input);
        }
        // Fresh filter: the predicate is rebuilt from only the non-correlated kept
        // conjuncts (no field survives a base). Field list is the complete Filter.
        Ok(LogicalPlan::Filter(Filter {
            input: Box::new(stripped_input),
            predicate: and_join(kept)?,
        }))
    }

    /// Materialize HAVING aggregate calls as new aggregate outputs so a kept or
    /// pulled predicate reads them by name (a post-aggregate filter cannot recompute).
    fn hoist_having(
        &self,
        aggregate: Aggregate,
        kept: Vec<Expr>,
        pulled: Vec<Expr>,
    ) -> (Aggregate, Vec<Expr>, Vec<Expr>) {
        let mut aggregates = aggregate.aggregates.clone();
        let mut names = aggregate.output_names.clone();
        let rewritten_kept = self.hoist_each(kept, &mut aggregates, &mut names);
        let rewritten_pulled = self.hoist_each(pulled, &mut aggregates, &mut names);
        let widened = Aggregate {
            aggregates,
            output_names: names,
            ..aggregate
        };
        (widened, rewritten_kept, rewritten_pulled)
    }

    /// Hoist aggregate calls out of each conjunct into the aggregate output list.
    fn hoist_each(
        &self,
        conjuncts: Vec<Expr>,
        aggregates: &mut Vec<Expr>,
        names: &mut Vec<String>,
    ) -> Vec<Expr> {
        let mut rewritten = Vec::with_capacity(conjuncts.len());
        for conjunct in conjuncts {
            rewritten.push(self.hoist_having_expr(conjunct, aggregates, names));
        }
        rewritten
    }

    /// Replace one aggregate call with a reference to a newly materialized output.
    fn hoist_having_expr(
        &self,
        expr: Expr,
        aggregates: &mut Vec<Expr>,
        names: &mut Vec<String>,
    ) -> Expr {
        if expr.is_aggregate_call() {
            let name = format!("{}_h{}", self.prefix, names.len());
            let data_type = expr.get_type();
            names.push(name.clone());
            aggregates.push(expr);
            return unqualified_col(&name, data_type);
        }
        expr.map_children(&mut |child| self.hoist_having_expr(child, aggregates, names))
    }

    /// Pull correlated key equalities through an aggregate, widening its GROUP BY.
    fn strip_aggregate(&mut self, node: Aggregate) -> Result<LogicalPlan> {
        let before = self.pulled.len();
        let stripped_input = self.strip(*node.input)?;
        let node = Aggregate {
            input: Box::new(stripped_input),
            ..node
        };
        if self.pulled.len() == before {
            return Ok(LogicalPlan::Aggregate(node));
        }
        let widened = self.widen_aggregate(node, before)?;
        Ok(LogicalPlan::Aggregate(widened))
    }

    /// Add correlation keys to an aggregate (already carrying its stripped input)
    /// and rewrite the pulled predicates to join on the exposed keys.
    fn widen_aggregate(&mut self, node: Aggregate, start: usize) -> Result<Aggregate> {
        let crossing: Vec<Expr> = self.pulled[start..].to_vec();
        let mut group_by = node.group_by.clone();
        let had_group_by = !node.group_by.is_empty();
        let mut aggregates = node.aggregates.clone();
        let mut names = node.output_names.clone();
        for (offset, predicate) in crossing.into_iter().enumerate() {
            let (inner_ref, outer_side) = self.key_equality(&predicate)?;
            add_group_key(&mut group_by, &inner_ref, had_group_by)?;
            let key_name = format!("{}_g{}", self.prefix, start + offset);
            let key_type = inner_ref
                .data_type
                .expect("widened correlation key column carries its bound type");
            aggregates.push(Expr::Column(inner_ref));
            names.push(key_name.clone());
            let renamed = qualified_col(&self.prefix, &key_name, key_type);
            self.pulled[start + offset] = binary(BinaryOpType::Eq, outer_side, renamed);
        }
        Ok(Aggregate {
            group_by,
            aggregates,
            output_names: names,
            ..node
        })
    }

    /// Decompose an equality correlation predicate into (inner key column, outer side).
    fn key_equality(&self, predicate: &Expr) -> Result<(ColumnRef, Expr)> {
        if !matches!(
            predicate,
            Expr::BinaryOp {
                op: BinaryOpType::Eq,
                ..
            }
        ) {
            return Err(DecorrelationError::NonFlattenable(
                "Only equality correlation predicates can cross an aggregate or limit".to_string(),
            ));
        }
        self.classify_equality_sides(predicate).ok_or_else(|| {
            DecorrelationError::Unsupported(
                "Correlation predicate must compare an inner column with an outer expression"
                    .to_string(),
            )
        })
    }

    /// Find which equality side is the pure inner key column.
    fn classify_equality_sides(&self, predicate: &Expr) -> Option<(ColumnRef, Expr)> {
        let Expr::BinaryOp { left, right, .. } = predicate else {
            return None;
        };
        if self.is_inner_column(left) && !references_inner(right, &self.inner_aliases) {
            if let Expr::Column(col) = left.as_ref() {
                return Some((col.clone(), right.as_ref().clone()));
            }
        }
        if self.is_inner_column(right) && !references_inner(left, &self.inner_aliases) {
            if let Expr::Column(col) = right.as_ref() {
                return Some((col.clone(), left.as_ref().clone()));
            }
        }
        None
    }

    /// Whether an expression is a single column reference into the subquery.
    fn is_inner_column(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Column(col) => col
                .table
                .as_ref()
                .is_some_and(|table| self.inner_aliases.contains(table)),
            _ => false,
        }
    }

    /// Convert a correlated inner LIMIT into a pending per-key limit.
    fn strip_limit(&mut self, mut node: Limit) -> Result<LogicalPlan> {
        let before = self.pulled.len();
        // In-place: set the stripped input on the owned node; limit/offset preserved.
        node.input = Box::new(self.strip(*node.input)?);
        if self.pulled.len() == before {
            return Ok(LogicalPlan::Limit(node));
        }
        // A correlated LIMIT is pulled up as a per-key limit; the Limit node is
        // dropped and its stripped input becomes the result.
        self.record_limit(node.offset, node.limit)?;
        Ok(*node.input)
    }

    /// Record a subquery-level LIMIT for later per-key placement.
    fn record_limit(&mut self, offset: u64, limit: Option<u64>) -> Result<()> {
        if offset != 0 {
            return Err(DecorrelationError::Unsupported(
                "OFFSET in a correlated subquery is not supported".to_string(),
            ));
        }
        if self.pending_limit.is_some() {
            return Err(DecorrelationError::Unsupported(
                "Nested LIMITs in a subquery are not supported".to_string(),
            ));
        }
        self.pending_limit = limit;
        Ok(())
    }

    // --- assembly ----------------------------------------------------------

    /// Build the renamed right side and the join condition.
    fn assemble(
        &mut self,
        core: LogicalPlan,
        value_exprs: Vec<Expr>,
        value_names: Vec<String>,
    ) -> Result<PreparedSubquery> {
        let (exposures, condition) = self.expose_correlation_columns()?;
        self.validate_no_outer_left(&core)?;
        let value_exprs = self.partition_window_values(value_exprs, &exposures)?;
        let value_types: Vec<DataType> = value_exprs.iter().map(Expr::get_type).collect();
        let mut expressions = Vec::new();
        let mut aliases = Vec::new();
        for (expr, alias) in exposures {
            expressions.push(expr);
            aliases.push(alias);
        }
        expressions.extend(value_exprs);
        aliases.extend(value_names.iter().cloned());
        if expressions.is_empty() {
            return Err(DecorrelationError::Unsupported(
                "Subquery rewrite produced no output columns".to_string(),
            ));
        }
        let mut non_keys = value_names.clone();
        let order = self.build_order(&mut expressions, &mut aliases, &mut non_keys);
        // The projected columns' types, positionally aligned with `aliases`, read off
        // each defining expression; a per-key LIMIT reads its key columns' types here.
        let alias_types: Vec<DataType> = expressions.iter().map(Expr::get_type).collect();
        // Fresh projection exposing the renamed correlation keys and value columns -
        // no base to copy from. Field list is the complete Projection struct.
        let plan = LogicalPlan::Projection(Projection {
            input: Box::new(core),
            expressions,
            aliases: aliases.clone(),
            distinct: false,
            distinct_on: None,
        });
        let plan = self.apply_pending_limit(plan, &aliases, &alias_types, &non_keys, order)?;
        Ok(PreparedSubquery {
            plan,
            condition,
            values: value_names,
            value_types,
            alias: self.prefix.clone(),
        })
    }

    /// Rename the pulled predicates' inner columns and build the condition.
    fn expose_correlation_columns(&self) -> Result<(Exposures, Option<Expr>)> {
        let mut exposures: Exposures = Vec::new();
        let mut exposure_names: HashMap<(Option<String>, String), Expr> = HashMap::new();
        let mut rewritten = Vec::new();
        for predicate in &self.pulled {
            self.expose_predicate_refs(predicate, &mut exposures, &mut exposure_names)?;
            let mapped = replace_column_refs(predicate.clone(), &|col: &ColumnRef| {
                exposure_names
                    .get(&(col.table.clone(), col.column.clone()))
                    .cloned()
            });
            rewritten.push(mapped);
        }
        if rewritten.is_empty() {
            return Ok((exposures, None));
        }
        Ok((exposures, Some(and_join(rewritten)?)))
    }

    /// Allocate renamed outputs for a predicate's inner columns.
    ///
    /// A pulled predicate becomes the join condition ABOVE the SubqueryScan
    /// boundary, so every column in it must be either the outer side (qualified to
    /// an outer relation, left untouched) or an inner side exposed and re-qualified
    /// to the boundary alias. An UNqualified ref reaching here is a manufactured
    /// name that would escape into the condition unresolved: the correlated
    /// HAVING-aggregate hoist is the only source (a `<prefix>_h*` output the
    /// assemble projection does not expose). Rather than ship a condition
    /// referencing a column the boundary never exposes, raise loudly.
    fn expose_predicate_refs(
        &self,
        predicate: &Expr,
        exposures: &mut Exposures,
        exposure_names: &mut HashMap<(Option<String>, String), Expr>,
    ) -> Result<()> {
        for col in column_refs(predicate) {
            let key = (col.table.clone(), col.column.clone());
            if exposure_names.contains_key(&key) {
                continue;
            }
            if self.is_renamed_ref(col) {
                // A key already widened into the aggregate below: expose it by its
                // inner physical name; the condition keeps the qualified ref.
                let data_type = col
                    .data_type
                    .expect("widened correlation key carries its bound type");
                exposures.push((unqualified_col(&col.column, data_type), col.column.clone()));
                exposure_names.insert(key, Expr::Column(col.clone()));
                continue;
            }
            if col.table.is_none() {
                return Err(DecorrelationError::Unsupported(format!(
                    "correlated HAVING over aggregate {} cannot be flattened: the \
                     hoisted output does not cross the subquery boundary",
                    col.column
                )));
            }
            if !self.is_inner_column(&Expr::Column(col.clone())) {
                continue;
            }
            let name = format!("{}_k{}", self.prefix, exposures.len());
            let data_type = col
                .data_type
                .expect("exposed correlation key carries its bound type");
            exposures.push((Expr::Column(col.clone()), name.clone()));
            exposure_names.insert(key, qualified_col(&self.prefix, &name, data_type));
        }
        Ok(())
    }

    /// Whether a ref is one this subquery already exposed (matched by qualifier).
    fn is_renamed_ref(&self, col: &ColumnRef) -> bool {
        col.table.as_deref() == Some(self.prefix.as_str())
    }

    /// Fail if correlated references remain anywhere in the stripped subplan.
    fn validate_no_outer_left(&self, plan: &LogicalPlan) -> Result<()> {
        for expr in plan.direct_expressions() {
            for col in column_refs(expr) {
                if is_outer_ref(col, &self.inner_aliases) {
                    return Err(DecorrelationError::Unsupported(format!(
                        "Correlated reference {} is in an unsupported position",
                        ref_label(col)
                    )));
                }
            }
        }
        for child in plan.children() {
            self.validate_no_outer_left(child)?;
        }
        Ok(())
    }

    /// Partition any window value by the correlation-key columns.
    fn partition_window_values(
        &self,
        value_exprs: Vec<Expr>,
        exposures: &Exposures,
    ) -> Result<Vec<Expr>> {
        let keys: Vec<Expr> = exposures.iter().map(|(expr, _)| expr.clone()).collect();
        if keys.is_empty() {
            return Ok(value_exprs);
        }
        let mut rewritten = Vec::with_capacity(value_exprs.len());
        for expr in value_exprs {
            if contains_window(&expr) {
                self.reject_non_equi_window()?;
                rewritten.push(prepend_window_partitions(expr, &keys));
            } else {
                rewritten.push(expr);
            }
        }
        Ok(rewritten)
    }

    /// A correlated window only lifts to PARTITION BY under equi-correlation.
    fn reject_non_equi_window(&self) -> Result<()> {
        for predicate in &self.pulled {
            if !is_equality(predicate) {
                return Err(DecorrelationError::Unsupported(
                    "Only equality correlation can partition a window".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Map a pending ORDER BY onto projected aliases, projecting any missing key.
    fn build_order(
        &self,
        expressions: &mut Vec<Expr>,
        aliases: &mut Vec<String>,
        non_keys: &mut Vec<String>,
    ) -> Option<OrderSpec> {
        let order = self.pending_order.as_ref()?;
        let mut order_keys = Vec::new();
        for sort_key in &order.sort_keys {
            // The projected order column carries the sort key's type: the alias reads
            // that same expression, projected as an extra when it was not already out.
            let data_type = sort_key.get_type();
            let alias = self.order_alias(sort_key, expressions, aliases, non_keys);
            order_keys.push(unqualified_col(&alias, data_type));
        }
        Some((
            order_keys,
            order.ascending.clone(),
            order.nulls_order.clone(),
        ))
    }

    /// Alias of a projected ORDER BY column, projecting it as an extra if absent.
    fn order_alias(
        &self,
        sort_key: &Expr,
        expressions: &mut Vec<Expr>,
        aliases: &mut Vec<String>,
        non_keys: &mut Vec<String>,
    ) -> String {
        for (index, expr) in expressions.iter().enumerate() {
            if expr == sort_key {
                return aliases[index].clone();
            }
        }
        let alias = format!("{}_o{}", self.prefix, aliases.len());
        expressions.push(sort_key.clone());
        aliases.push(alias.clone());
        non_keys.push(alias.clone());
        alias
    }

    /// Re-attach a recorded subquery LIMIT, per-key when correlated.
    fn apply_pending_limit(
        &self,
        plan: LogicalPlan,
        aliases: &[String],
        alias_types: &[DataType],
        non_keys: &[String],
        order: Option<OrderSpec>,
    ) -> Result<LogicalPlan> {
        let Some(limit) = self.pending_limit else {
            return Ok(plan);
        };
        let mut keys = Vec::new();
        for (alias, data_type) in aliases.iter().zip(alias_types.iter()) {
            if !non_keys.contains(alias) {
                keys.push(unqualified_col(alias, *data_type));
            }
        }
        if keys.is_empty() {
            return Ok(unkeyed_limit(plan, order, limit));
        }
        self.require_equi_correlation_for_limit()?;
        Ok(keyed_limit(plan, keys, order, limit))
    }

    /// A per-key LIMIT is only correct under equi-correlation: each outer row must
    /// map to exactly one key group. A non-equality correlation matches many
    /// groups, so a per-key limit would not be a per-outer-row limit. Unlike the
    /// aggregate path, a LIMIT never routes through `key_equality`, so this guard
    /// is the ONLY thing that catches a non-equi correlated LIMIT.
    fn require_equi_correlation_for_limit(&self) -> Result<()> {
        for predicate in &self.pulled {
            if !is_equality(predicate) {
                return Err(DecorrelationError::NonFlattenable(
                    "Only equality correlation predicates can cross an aggregate or limit"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// The scalar-subquery preparation path: a single value column, cardinality
/// rules, and the LEFT-join right side. Ports `prepare_scalar` and its helpers.
impl SubqueryPreparer<'_> {
    /// Prepare a scalar subquery: single value plus cardinality rules.
    pub(crate) fn prepare_scalar(&mut self, subquery: LogicalPlan) -> Result<PreparedScalar> {
        let plan = self.decorrelator.rewrite_plan(subquery)?;
        self.inner_aliases = collect_inner_aliases(&plan);
        let plan = self.peel_scalar_limit_order(plan)?;
        match plan {
            LogicalPlan::Aggregate(node) => self.prepare_scalar_aggregate(node),
            other => self.prepare_scalar_row(other),
        }
    }

    /// Peel a top LIMIT and the ORDER BY it caps into pending state, so they are
    /// reattached as an order-aware per-key limit once the join side is assembled.
    fn peel_scalar_limit_order(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let plan = match plan {
            LogicalPlan::Limit(node) => {
                self.record_limit(node.offset, node.limit)?;
                *node.input
            }
            other => other,
        };
        if self.pending_limit.is_some() {
            if let LogicalPlan::Sort(sort) = plan {
                // Keep the whole Sort as pending order; only its keys/direction are
                // read later, so cloning the (now-detached) input keeps it a Sort.
                let input = sort.input.as_ref().clone();
                self.pending_order = Some(sort);
                return Ok(input);
            }
            return Ok(plan);
        }
        Ok(plan)
    }

    /// Scalar over an aggregate: hoist aggregate calls, join on keys.
    fn prepare_scalar_aggregate(&mut self, plan: Aggregate) -> Result<PreparedScalar> {
        if plan.aggregates.len() != 1 {
            return Err(DecorrelationError::Unsupported(
                "Scalar subquery must return exactly one column".to_string(),
            ));
        }
        let mut hoisted: Vec<(Expr, String)> = Vec::new();
        let replacement = self.hoist_aggregate_calls(plan.aggregates[0].clone(), &mut hoisted);
        if hoisted.is_empty() {
            return Err(DecorrelationError::Unsupported(
                "Scalar aggregate subquery has no aggregate function".to_string(),
            ));
        }
        let original_grouped = !plan.group_by.is_empty();
        let core = rebuild_hoisted_aggregate(plan, &hoisted);
        let stripped = self.strip(core)?;
        let (value_exprs, value_names) = hoisted_value_refs(&hoisted);
        let prepared = self.assemble(stripped, value_exprs, value_names)?;
        let condition = prepared.condition.clone();
        let guarded = guard_scalar(prepared, original_grouped)?;
        Ok(self.finalize_scalar(guarded, condition, replacement))
    }

    /// Replace aggregate calls with renamed refs, collecting them. COUNT refs are
    /// wrapped in COALESCE(.., 0): after the LEFT join an absent group must read
    /// as zero, matching SQL COUNT semantics.
    fn hoist_aggregate_calls(&self, expr: Expr, hoisted: &mut Vec<(Expr, String)>) -> Expr {
        if expr.is_aggregate_call() {
            let name = format!("{}_v{}", self.prefix, hoisted.len());
            let value_ref = qualified_col(&self.prefix, &name, expr.get_type());
            let is_count = matches!(
                &expr,
                Expr::FunctionCall { function_name, .. }
                    if function_name.eq_ignore_ascii_case("COUNT")
            );
            hoisted.push((expr, name));
            if is_count {
                return coalesce_zero(value_ref);
            }
            return value_ref;
        }
        expr.map_children(&mut |child| self.hoist_aggregate_calls(child, hoisted))
    }

    /// Scalar over plain rows: project the value, guard cardinality.
    fn prepare_scalar_row(&mut self, plan: LogicalPlan) -> Result<PreparedScalar> {
        let (core, value_exprs) = self.peel_values_top(plan, false)?;
        if value_exprs.len() != 1 {
            return Err(DecorrelationError::Unsupported(
                "Scalar subquery must return exactly one column".to_string(),
            ));
        }
        self.reject_outer_refs_in_value(&value_exprs[0])?;
        let value_type = value_exprs[0].get_type();
        let stripped = self.strip(core)?;
        let value_name = format!("{}_v0", self.prefix);
        let prepared = self.assemble(stripped, value_exprs, vec![value_name.clone()])?;
        let condition = prepared.condition.clone();
        // LIMIT 1 makes the body provably single-row, so no guard; anything else
        // needs the runtime cardinality guard.
        let needs_guard = self.pending_limit != Some(1);
        let guarded = guard_scalar(prepared, needs_guard)?;
        let replacement = qualified_col(&self.prefix, &value_name, value_type);
        Ok(self.finalize_scalar(guarded, condition, replacement))
    }

    /// Outer references in a non-aggregated scalar value are unsupported: an outer
    /// ref is legal only inside the correlation predicate, never the projected value.
    fn reject_outer_refs_in_value(&self, expr: &Expr) -> Result<()> {
        for col in column_refs(expr) {
            if is_outer_ref(col, &self.inner_aliases) {
                return Err(DecorrelationError::Unsupported(
                    "Outer reference in a non-aggregated scalar subquery value is not supported"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Wrap a scalar subquery at its alias boundary as a derived relation. The
    /// correlation condition and replacement already carry the prefix qualifier on
    /// the subquery's own output columns, so nothing unqualified escapes.
    fn finalize_scalar(
        &self,
        guarded: LogicalPlan,
        condition: Option<Expr>,
        replacement: Expr,
    ) -> PreparedScalar {
        PreparedScalar {
            // Fresh boundary alias wrapping the guarded scalar side - no base to copy
            // from. Field list (input/alias/column_names) is the complete SubqueryScan.
            plan: LogicalPlan::SubqueryScan(SubqueryScan {
                input: Box::new(guarded),
                alias: self.prefix.clone(),
                column_names: None,
            }),
            condition,
            replacement,
        }
    }
}

/// Wrap the scalar side in a runtime cardinality guard when it could not be
/// proven single-row. With no keys the guard allows at most one row total; with
/// keys, at most one row per correlation-key tuple (the key columns are the schema
/// names that are not value columns).
fn guard_scalar(prepared: PreparedSubquery, needs_guard: bool) -> Result<LogicalPlan> {
    if !needs_guard {
        return Ok(prepared.plan);
    }
    let names = prepared.plan.schema();
    let types = relation_output_types(&prepared.plan)?;
    if names.len() != types.len() {
        return Err(DecorrelationError::Invariant(format!(
            "scalar side exposes {} columns but {} derived types",
            names.len(),
            types.len()
        )));
    }
    let mut keys = Vec::new();
    for (name, data_type) in names.iter().zip(types.iter()) {
        if !prepared.values.contains(name) {
            keys.push(unqualified_col(name, *data_type));
        }
    }
    // Fresh cardinality guard wrapping the scalar side - no base to copy from. Field
    // list (input/keys) is the complete SingleRowGuard struct.
    Ok(LogicalPlan::SingleRowGuard(SingleRowGuard {
        input: Box::new(prepared.plan),
        keys,
    }))
}

/// Rebuild the aggregate so it outputs only the hoisted pure aggregate calls; the
/// non-aggregate parts of the select expression were captured into the replacement.
fn rebuild_hoisted_aggregate(plan: Aggregate, hoisted: &[(Expr, String)]) -> LogicalPlan {
    let mut aggregates = Vec::with_capacity(hoisted.len());
    let mut names = Vec::with_capacity(hoisted.len());
    for (call, name) in hoisted {
        aggregates.push(call.clone());
        names.push(name.clone());
    }
    LogicalPlan::Aggregate(Aggregate {
        aggregates,
        output_names: names,
        ..plan
    })
}

/// Projection expressions/names for the hoisted aggregate outputs. These are
/// unqualified refs read from the aggregate output BELOW the alias boundary; the
/// wrapping SubqueryScan qualifies them.
fn hoisted_value_refs(hoisted: &[(Expr, String)]) -> (Vec<Expr>, Vec<String>) {
    let mut exprs = Vec::with_capacity(hoisted.len());
    let mut names = Vec::with_capacity(hoisted.len());
    for (call, name) in hoisted {
        exprs.push(unqualified_col(name, call.get_type()));
        names.push(name.clone());
    }
    (exprs, names)
}

/// COALESCE(value_ref, 0): after a LEFT join an absent group reads NULL, and SQL
/// COUNT must read as zero. Shared with the dependent-join (N-K) COUNT fix.
pub(crate) fn coalesce_zero(value_ref: Expr) -> Expr {
    // Fresh COALESCE(value, 0) call built from scratch - no base to copy from. Both
    // the FunctionCall and its inner integer Literal are complete variant field lists.
    Expr::FunctionCall {
        function_name: "COALESCE".to_string(),
        args: vec![
            value_ref,
            Expr::Literal {
                value: LiteralValue::Integer(0),
                data_type: DataType::Integer,
            },
        ],
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
        filter: None,
    }
}

/// Strip layers that do not affect existence (SELECT list, ORDER, LIMIT).
fn peel_exists_top(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Projection(node) => peel_exists_top(*node.input),
        LogicalPlan::Sort(node) => peel_exists_top(*node.input),
        LogicalPlan::Limit(node) => peel_exists_top(*node.input),
        other => other,
    }
}

/// A constant single-row Values subquery is its own core; expose each output name.
fn peel_values_node(node: fq_plan::logical::Values) -> Result<(LogicalPlan, Vec<Expr>)> {
    if node.rows.len() != 1 {
        return Err(DecorrelationError::Unsupported(
            "Multi-row VALUES subqueries not supported".to_string(),
        ));
    }
    let types: Vec<DataType> = node.rows[0].iter().map(Expr::get_type).collect();
    let refs = typed_output_refs(&node.output_names, &types)?;
    Ok((LogicalPlan::Values(node), refs))
}

/// Typed references to a relation's output columns, deriving each column's type
/// from the expression that defines it.
fn relation_value_refs(plan: &LogicalPlan) -> Result<Vec<Expr>> {
    let names = plan.schema();
    let types = relation_output_types(plan)?;
    typed_output_refs(&names, &types)
}

/// A star projection has no determinate value columns.
fn reject_star_values(expressions: &[Expr]) -> Result<()> {
    for expr in expressions {
        if let Expr::Column(col) = expr {
            if col.column == "*" {
                return Err(DecorrelationError::Unsupported(
                    "SELECT * subqueries cannot be used as value subqueries".to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// Add a correlation key to the grouping, validating legality. A subquery with an
/// original GROUP BY cannot gain a key outside it (changes granularity); a global
/// aggregate may. `had_group_by` is the ORIGINAL state.
fn add_group_key(group_by: &mut Vec<Expr>, key: &ColumnRef, had_group_by: bool) -> Result<()> {
    for existing in group_by.iter() {
        if let Expr::Column(col) = existing {
            if col.table == key.table && col.column == key.column {
                return Ok(());
            }
        }
    }
    if had_group_by {
        return Err(DecorrelationError::Unsupported(format!(
            "Correlation key {} is not part of the subquery's GROUP BY",
            ref_label(key)
        )));
    }
    group_by.push(Expr::Column(key.clone()));
    Ok(())
}

/// A plain LIMIT, ordered when the subquery had ORDER BY ... LIMIT.
fn unkeyed_limit(plan: LogicalPlan, order: Option<OrderSpec>, limit: u64) -> LogicalPlan {
    let plan = match order {
        // Fresh sort from the captured ORDER BY parts - no base to copy from. Field
        // list is the complete Sort struct.
        Some((keys, ascending, nulls)) => LogicalPlan::Sort(Sort {
            input: Box::new(plan),
            sort_keys: keys,
            ascending,
            nulls_order: nulls,
        }),
        None => plan,
    };
    // Fresh LIMIT over the (optionally sorted) plan - no base to copy from. Field
    // list (input/limit/offset) is the complete Limit struct.
    LogicalPlan::Limit(Limit {
        input: Box::new(plan),
        limit: Some(limit),
        offset: 0,
    })
}

/// A per-key (grouped) LIMIT, ordered within each key when requested.
fn keyed_limit(
    plan: LogicalPlan,
    keys: Vec<Expr>,
    order: Option<OrderSpec>,
    limit: u64,
) -> LogicalPlan {
    let (order_by_keys, order_by_ascending, order_by_nulls) = match order {
        Some((okeys, ascending, nulls)) => (Some(okeys), Some(ascending), nulls),
        None => (None, None, None),
    };
    // Fresh per-key limit from the keys and captured ORDER BY - no base to copy
    // from. Field list is the complete GroupedLimit struct.
    LogicalPlan::GroupedLimit(fq_plan::logical::GroupedLimit {
        input: Box::new(plan),
        keys,
        limit,
        order_by_keys,
        order_by_ascending,
        order_by_nulls,
    })
}

/// Whether an expression is a plain equality binary op.
fn is_equality(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOpType::Eq,
            ..
        }
    )
}

/// Whether an expression tree contains a window function anywhere.
fn contains_window(expr: &Expr) -> bool {
    if matches!(expr, Expr::Window { .. }) {
        return true;
    }
    expr.children().iter().any(|child| contains_window(child))
}

/// Prepend the correlation keys to every window PARTITION BY in the tree.
fn prepend_window_partitions(expr: Expr, keys: &[Expr]) -> Expr {
    let mut rebuilt = expr.map_children(&mut |child| prepend_window_partitions(child, keys));
    // In-place: prepend the correlation keys to only the partition_by field, leaving
    // the window function, order keys, and frame untouched (update!/..base cannot
    // target an enum variant, so &mut on the field is the idiom).
    if let Expr::Window { partition_by, .. } = &mut rebuilt {
        let mut new_partition = keys.to_vec();
        new_partition.extend(std::mem::take(partition_by));
        *partition_by = new_partition;
    }
    rebuilt
}

/// A `table.column` label for an error message (there is no to_sql in fq-plan).
fn ref_label(col: &ColumnRef) -> String {
    match &col.table {
        Some(table) => format!("{table}.{}", col.column),
        None => col.column.clone(),
    }
}

/// A node-kind label for the "unsupported output shape" error message.
fn plan_label(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan(_) => "Scan",
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::SetOperation(_) => "SetOperation",
        LogicalPlan::Explain(_) => "Explain",
        LogicalPlan::Cte(_) => "Cte",
        LogicalPlan::CteRef(_) => "CteRef",
        LogicalPlan::Values(_) => "Values",
        LogicalPlan::SubqueryScan(_) => "SubqueryScan",
        LogicalPlan::SingleRowGuard(_) => "SingleRowGuard",
        LogicalPlan::GroupedLimit(_) => "GroupedLimit",
        LogicalPlan::LateralJoin(_) => "LateralJoin",
    }
}
