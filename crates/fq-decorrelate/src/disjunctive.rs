//! The disjunctive path: an OR of subqueries. The common case - an OR of positive
//! same-key existentials - collapses to ONE SEMI join over the UNION of their key
//! domains; anything else takes the boolean-flag path, one flag column per
//! disjunct OR'd in a single filter. Ports `_expand_or`, the domain-union family,
//! and `_disjunct_term`.

use fq_plan::expr::{split_conjuncts, split_disjuncts, BinaryOpType, ColumnRef, Expr};
use fq_plan::logical::{Join, JoinType, LogicalPlan, Projection, SubqueryScan, Union};

use crate::boolean::constant_exists_value;
use crate::helpers::{bool_literal, expression_has_subquery, is_subquery_node, or_join};
use crate::{Decorrelator, Result};

/// One disjunct's contribution to a domain-union SEMI: the outer key expression,
/// the disjunct's prepared relation, and that relation's correlation key column.
type DomainPart = (Expr, LogicalPlan, ColumnRef);

impl Decorrelator {
    /// Rewrite OR-of-subqueries: the domain-union SEMI collapse if every disjunct
    /// is a positive same-key existential, else a filter over per-disjunct flags.
    pub(crate) fn expand_or(
        &mut self,
        input_plan: LogicalPlan,
        disjuncts: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        if let Some((outer_expr, parts)) = self.domain_union_parts(&disjuncts)? {
            return Ok(self.build_domain_semi(input_plan, outer_expr, parts));
        }
        let mut plan = input_plan;
        let mut terms = Vec::with_capacity(disjuncts.len());
        for disjunct in disjuncts {
            let (term, next) = self.disjunct_term(disjunct, plan)?;
            plan = next;
            terms.push(term);
        }
        // Fresh filter: the predicate is the OR of the per-disjunct flag terms over
        // the join-threaded plan - no base to copy from. Field list is complete.
        Ok(LogicalPlan::Filter(fq_plan::logical::Filter {
            input: Box::new(plan),
            predicate: or_join(terms)?,
        }))
    }

    /// A conjunct that is an OR of >= 2 positive same-key existentials yields the
    /// domain-union parts directly (q10/q35's `... AND (EXISTS(ws) OR EXISTS(cs))`);
    /// any other shape returns None and the caller takes the flag path.
    pub(crate) fn or_conjunct_domain_parts(
        &mut self,
        conjunct: &Expr,
    ) -> Result<Option<(Expr, Vec<DomainPart>)>> {
        let disjuncts = split_disjuncts(conjunct);
        if disjuncts.len() < 2 {
            return Ok(None);
        }
        let owned: Vec<Expr> = disjuncts.into_iter().cloned().collect();
        self.domain_union_parts(&owned)
    }

    /// The per-disjunct domain parts when every disjunct is a positive existential
    /// whose correlation is a single equality on the SAME outer expression; None
    /// otherwise (the flag path). Computing the parts advances the name counter
    /// exactly as the flag path would re-advance it, matching Python.
    fn domain_union_parts(
        &mut self,
        disjuncts: &[Expr],
    ) -> Result<Option<(Expr, Vec<DomainPart>)>> {
        let mut parts = Vec::with_capacity(disjuncts.len());
        for disjunct in disjuncts {
            match self.domain_union_part(disjunct)? {
                Some(part) => parts.push(part),
                None => return Ok(None),
            }
        }
        let Some((first_outer, _, _)) = parts.first() else {
            return Ok(None);
        };
        let outer_expr = first_outer.clone();
        for (other_outer, _, _) in &parts[1..] {
            if *other_outer != outer_expr {
                return Ok(None);
            }
        }
        Ok(Some((outer_expr, parts)))
    }

    /// (outer key expr, subquery relation, its key column) for one disjunct, or
    /// None when it is not a positive EXISTS/IN with a single plain-equality
    /// correlation (NOT EXISTS/NOT IN carry null-aware conditions a domain union
    /// cannot express; multi-key correlation is out of scope here).
    fn domain_union_part(&mut self, disjunct: &Expr) -> Result<Option<DomainPart>> {
        if !matches!(disjunct, Expr::Exists { .. } | Expr::InSubquery { .. }) {
            return Ok(None);
        }
        if constant_exists_value(disjunct).is_some() {
            return Ok(None);
        }
        let (right, condition, positive) = self.semi_anti_parts(disjunct.clone())?;
        if !positive {
            return Ok(None);
        }
        let Some(condition) = condition else {
            return Ok(None);
        };
        let LogicalPlan::SubqueryScan(scan) = &right else {
            return Ok(None);
        };
        let alias = scan.alias.clone();
        match single_equality_orient(&condition, &alias) {
            Some((outer, key)) => Ok(Some((outer, right, key))),
            None => Ok(None),
        }
    }

    /// Assemble the SEMI join over the unioned per-disjunct key domains: one SEMI
    /// join, no input replication. Each branch projects its relation's key column
    /// to a single canonical name so the branches union positionally.
    pub(crate) fn build_domain_semi(
        &mut self,
        input_plan: LogicalPlan,
        outer_expr: Expr,
        parts: Vec<DomainPart>,
    ) -> LogicalPlan {
        let prefix = self.next_prefix();
        let key_name = format!("{prefix}_k0");
        let mut branches = Vec::with_capacity(parts.len());
        for (_, right, key) in parts {
            // Fresh projection of each disjunct's relation down to its single key
            // column under a canonical name - no base to copy from. Field list is
            // the complete Projection struct.
            branches.push(LogicalPlan::Projection(Projection {
                input: Box::new(right),
                expressions: vec![Expr::Column(key)],
                aliases: vec![key_name.clone()],
                distinct: false,
                distinct_on: None,
            }));
        }
        // UNION ALL: the SEMI join tests membership, so duplicate keys across (or
        // within) branches change nothing and the dedup is skipped.
        let union = LogicalPlan::Union(Union {
            inputs: branches,
            distinct: false,
        });
        // Fresh boundary alias wrapping the unioned key domains - no base to copy
        // from. Field list is the complete SubqueryScan struct.
        let domain = LogicalPlan::SubqueryScan(SubqueryScan {
            input: Box::new(union),
            alias: prefix.clone(),
            column_names: None,
        });
        // The domain key inherits the outer key's type: the equality's two sides
        // share it, and the outer side is bound.
        let key_type = outer_expr.get_type();
        // Fresh equality built from the outer key and the domain key column - no
        // base to copy from. Field list is the complete BinaryOp variant.
        let condition = Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(outer_expr),
            right: Box::new(Expr::Column(ColumnRef::new(
                Some(prefix),
                key_name,
                Some(key_type),
            ))),
        };
        // Fresh SEMI join over the outer plan and the key domain - no base to copy
        // from. Field list is the complete Join struct (no stamped estimates yet).
        LogicalPlan::Join(Join {
            left: Box::new(input_plan),
            right: Box::new(domain),
            join_type: JoinType::Semi,
            condition: Some(condition),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        })
    }

    /// Turn one OR disjunct into a boolean term over the grown plan.
    fn disjunct_term(&mut self, disjunct: Expr, plan: LogicalPlan) -> Result<(Expr, LogicalPlan)> {
        if let Some(constant) = constant_exists_value(&disjunct) {
            return Ok((bool_literal(constant), plan));
        }
        if is_subquery_node(&disjunct) {
            return self.join_flag(disjunct, plan);
        }
        if !expression_has_subquery(&disjunct) {
            return Ok((disjunct, plan));
        }
        self.rewrite_value_expr(disjunct, plan)
    }
}

/// Split a one-conjunct plain equality into its (outer side, subquery key column)
/// when exactly one side is a plain column of the subquery relation `alias`, and
/// the outer side does not itself reference that relation.
fn single_equality_orient(condition: &Expr, alias: &str) -> Option<(Expr, ColumnRef)> {
    let conjuncts = split_conjuncts(condition);
    if conjuncts.len() != 1 {
        return None;
    }
    let Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left,
        right,
    } = conjuncts[0]
    else {
        return None;
    };
    orient_domain_equality(left, right, alias)
}

/// Orient an equality to (outer side, subquery key column), rejecting the shape
/// unless exactly one side is a plain column of the subquery relation and the
/// outer side does not reference that relation.
fn orient_domain_equality(left: &Expr, right: &Expr, alias: &str) -> Option<(Expr, ColumnRef)> {
    let left_is_key = is_relation_column(left, alias);
    let right_is_key = is_relation_column(right, alias);
    if left_is_key == right_is_key {
        return None;
    }
    let (outer, key_expr) = if left_is_key {
        (right, left)
    } else {
        (left, right)
    };
    for col in fq_plan::expr::column_refs(outer) {
        if col.table.as_deref() == Some(alias) {
            return None;
        }
    }
    match key_expr {
        Expr::Column(col) => Some((outer.clone(), col.clone())),
        _ => None,
    }
}

/// Whether an expression is a plain column of the given relation.
fn is_relation_column(expr: &Expr, alias: &str) -> bool {
    matches!(expr, Expr::Column(col) if col.table.as_deref() == Some(alias))
}
