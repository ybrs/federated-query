//! The Neumann-Kemper dependent-join path: unnest a correlated scalar subquery
//! that could NOT flatten to a set join (a non-equi correlation crossing an
//! aggregate or a per-outer-row LIMIT) into ordinary relational algebra.
//!
//! The shape is always: build the DISTINCT domain of the outer correlation
//! values, INNER-join it to the subquery's inner relation on the (rewritten)
//! correlation, reduce once per domain value (Case A: aggregate; Case B: top-k
//! via ROW_NUMBER), then LEFT-join the reduced relation back onto the outer plan
//! on the free variables. No correlation survives. Ports `_unnest_dependent_scalar`
//! and its builders from `decorrelation.py`.
//!
//! The LATERAL fallback (`lateral_scalar`) is the last resort for a subquery
//! shape neither recognizer matches: it emits a `LateralJoin` node (which the
//! engine cannot execute) rather than a broken plan, and raises if even the
//! lateral body is unshaped. A user-written LATERAL is not reachable through the
//! current parser/binder (joins bind to `Join`, never `LateralJoin`), so
//! `rewrite_lateral_join` recurses in place - the documented safe default.

use std::collections::HashSet;

use fq_common::DataType;
use fq_plan::expr::{
    column_refs, split_conjuncts, BinaryOpType, ColumnRef, Expr, LiteralValue, NullsOrder,
};
use fq_plan::logical::{
    Aggregate, Filter, Join, JoinType, LateralJoin, LogicalPlan, Projection, SubqueryScan,
};

use crate::error::DecorrelationError;
use crate::helpers::{
    and_join, binary, collect_inner_aliases, is_outer_ref, qualified_col, references_outer,
    relation_output_types, replace_column_refs,
};
use crate::prepare::coalesce_zero;
use crate::{Decorrelator, Result};

/// The synthetic aggregate/value output column of a dependent relation.
const UNNEST_VALUE: &str = "nk_value";
/// The synthetic ROW_NUMBER rank column of a top-k dependent relation.
const UNNEST_RANK: &str = "nk_rank";

/// An INSERTION-ORDERED map from a free var's `(table, column)` to its domain
/// column reference (`d{i}`). It MUST stay insertion-ordered: the `d0/d1/...`
/// columns are aligned across four iteration sites (domain projection, aggregate
/// group keys and passthrough outputs, join-back equalities, and the value ref),
/// so a `HashMap`/`BTreeMap` (which reorders) would silently misalign them.
type DomMap = Vec<((Option<String>, String), ColumnRef)>;

/// A pending ORDER BY captured for a top-k subquery: (keys, ascending, nulls).
type OrderSpec = (Vec<Expr>, Vec<bool>, Option<Vec<Option<NullsOrder>>>);

/// The domain-join inputs shared by the Case A and Case B builders: the domain
/// relation, the subquery's inner relation, and the correlation split into the
/// outer-referencing conjuncts and the inner-only conjuncts.
struct DomainJoin {
    domain: LogicalPlan,
    inner: LogicalPlan,
    correlated: Vec<Expr>,
    local: Vec<Expr>,
}

/// A recognized Case A body: `Aggregate([one agg], no GROUP BY, Filter(...))`.
struct DependentAggregate {
    correlated: Vec<Expr>,
    local: Vec<Expr>,
    inner: LogicalPlan,
    free_vars: Vec<ColumnRef>,
    aggregates: Vec<Expr>,
}

/// A recognized Case B body: `Limit[Sort?[Projection(one value)[Filter(...)]]]`.
struct DependentLimit {
    value: Expr,
    order: Option<OrderSpec>,
    limit: Option<u64>,
    correlated: Vec<Expr>,
    local: Vec<Expr>,
    inner: LogicalPlan,
    free_vars: Vec<ColumnRef>,
}

impl Decorrelator {
    /// Neumann-Kemper unnesting of a correlated scalar subquery: decorrelate the
    /// subquery's own inner subqueries, then dispatch to the aggregate (Case A) or
    /// top-k (Case B) rewrite, falling back to a LATERAL for any other shape.
    pub(crate) fn unnest_dependent_scalar(
        &mut self,
        subquery: LogicalPlan,
        plan: LogicalPlan,
    ) -> Result<(Expr, LogicalPlan)> {
        let body = self.rewrite_plan(subquery)?;
        if let Some(shape) = dependent_shape(&body) {
            return self.assemble_unnested(plan, shape);
        }
        if let Some(shape) = dependent_limit_shape(&body) {
            return self.assemble_unnested_limit(plan, shape);
        }
        self.lateral_scalar(body, plan)
    }

    /// Build the domain -> dependent aggregate -> LEFT join-back rewrite (Case A).
    fn assemble_unnested(
        &mut self,
        plan: LogicalPlan,
        shape: DependentAggregate,
    ) -> Result<(Expr, LogicalPlan)> {
        let DependentAggregate {
            correlated,
            local,
            inner,
            free_vars,
            aggregates,
        } = shape;
        let dom_prefix = self.next_prefix();
        let dep_prefix = self.next_prefix();
        // The outer plan is both the domain source and the join-back left side, so
        // the domain gets its own clone.
        let (domain, dom_map) = build_domain(plan.clone(), &free_vars, &dom_prefix);
        let dependent = build_dependent_relation(
            &aggregates,
            &[UNNEST_VALUE.to_string()],
            DomainJoin {
                domain,
                inner,
                correlated,
                local,
            },
            &dom_map,
            &dep_prefix,
        )?;
        let joined = join_back(plan, dependent, &free_vars, &dom_map, &dep_prefix)?;
        Ok((scalar_value_ref(&aggregates[0], &dep_prefix), joined))
    }

    /// Build the domain -> top-k-per-domain -> LEFT join-back rewrite (Case B).
    fn assemble_unnested_limit(
        &mut self,
        plan: LogicalPlan,
        shape: DependentLimit,
    ) -> Result<(Expr, LogicalPlan)> {
        let DependentLimit {
            value,
            order,
            limit,
            correlated,
            local,
            inner,
            free_vars,
        } = shape;
        let dom_prefix = self.next_prefix();
        let dep_prefix = self.next_prefix();
        // The re-exposed top-k value carries the subquery value expression's type.
        let value_type = value.get_type();
        // The outer plan is both the domain source and the join-back left side, so
        // the domain gets its own clone (`join_back` below consumes the original).
        let (domain, dom_map) = build_domain(plan.clone(), &free_vars, &dom_prefix);
        let top_k = self.build_top_k_relation(
            &[value],
            &[UNNEST_VALUE.to_string()],
            order.as_ref(),
            limit,
            DomainJoin {
                domain,
                inner,
                correlated,
                local,
            },
            &dom_map,
            &dep_prefix,
        )?;
        let joined = join_back(plan, top_k, &free_vars, &dom_map, &dep_prefix)?;
        // A row subquery yields NULL (not 0) when no row matched, so no COALESCE.
        Ok((qualified_col(&dep_prefix, UNNEST_VALUE, value_type), joined))
    }

    /// `domain JOIN inner`, ranked by `ROW_NUMBER()` per domain value, capped to
    /// the top-k rows, projected to (domain columns, output values) with the rank
    /// dropped. With no LIMIT (a set body) every matching row is kept unranked.
    fn build_top_k_relation(
        &mut self,
        value_exprs: &[Expr],
        value_names: &[String],
        order: Option<&OrderSpec>,
        limit: Option<u64>,
        join: DomainJoin,
        dom_map: &DomMap,
        prefix: &str,
    ) -> Result<LogicalPlan> {
        let condition = dependent_join_condition(&join.correlated, &join.local, dom_map)?;
        let joined = inner_join(join.domain, join.inner, condition);
        let Some(limit) = limit else {
            let projected = project_join_values(joined, value_exprs, value_names, dom_map);
            return Ok(aliased(projected, prefix));
        };
        let win_prefix = self.next_prefix();
        let ranked = rank_by_row_number(
            joined,
            value_exprs,
            value_names,
            order,
            dom_map,
            &win_prefix,
        );
        // Fresh filter capping the ranked rows to the top-k per domain value - no
        // base to copy from. Field list (input/predicate) is the complete Filter.
        let capped = LogicalPlan::Filter(Filter {
            input: Box::new(ranked),
            predicate: row_number_cap(&win_prefix, limit)?,
        });
        let value_types: Vec<DataType> = value_exprs.iter().map(Expr::get_type).collect();
        let projected =
            project_domain_values(capped, value_names, &value_types, dom_map, &win_prefix);
        Ok(aliased(projected, prefix))
    }

    /// Last-resort LATERAL join for a correlated scalar neither recognizer matched:
    /// the still-correlated body is exposed under a fresh alias with its single
    /// value renamed, and a `LateralJoin` evaluates it per outer row.
    fn lateral_scalar(
        &mut self,
        body: LogicalPlan,
        plan: LogicalPlan,
    ) -> Result<(Expr, LogicalPlan)> {
        let prefix = self.next_prefix();
        let value_name = format!("{prefix}_v0");
        let right = aliased(project_lateral_value(body, &value_name)?, &prefix);
        // The lateral body exposes exactly its single renamed value; its type is that
        // value column's, read off the body's derived output schema.
        let value_type = *relation_output_types(&right)?.first().ok_or_else(|| {
            DecorrelationError::Invariant(
                "a lateral scalar body exposes no value column".to_string(),
            )
        })?;
        // Fresh lateral join wrapping the outer plan and the per-outer-row body - no
        // base to copy from. Field list (left/right/join_type) is complete.
        let joined = LogicalPlan::LateralJoin(LateralJoin {
            left: Box::new(plan),
            right: Box::new(right),
            join_type: JoinType::Left,
        });
        Ok((qualified_col(&prefix, &value_name, value_type), joined))
    }

    /// Rewrite a `LateralJoin` node in place: recurse into both sides and keep the
    /// node. The current parser/binder never produces a `LateralJoin` (a
    /// user-written LATERAL binds as an ordinary `Join`), so there is no reachable
    /// body to unnest here; a `LateralJoin` the scalar fallback itself produced is
    /// already fully built and is never re-entered through this path.
    pub(crate) fn rewrite_lateral_join(&mut self, mut node: LateralJoin) -> Result<LogicalPlan> {
        // In-place: recurse into both sides and keep the node; join_type preserved.
        node.left = Box::new(self.rewrite_plan(*node.left)?);
        node.right = Box::new(self.rewrite_plan(*node.right)?);
        Ok(LogicalPlan::LateralJoin(node))
    }
}

/// Recognize `Aggregate([one agg], no GROUP BY, Filter(correlated, inner))`, the
/// Case A shape. Returns None (declining to the next fallback) when the body is a
/// different shape or carries no correlation / no free variables.
fn dependent_shape(body: &LogicalPlan) -> Option<DependentAggregate> {
    let LogicalPlan::Aggregate(aggregate) = body else {
        return None;
    };
    if !aggregate.group_by.is_empty() || aggregate.aggregates.len() != 1 {
        return None;
    }
    let LogicalPlan::Filter(filter) = aggregate.input.as_ref() else {
        return None;
    };
    let inner_aliases = collect_inner_aliases(body);
    let (correlated, local) = split_by_outer(&filter.predicate, &inner_aliases);
    // Free vars come from the correlation predicate AND the aggregate value itself
    // (e.g. MAX(x + o.f)); both are rewritten to the domain column.
    let mut sources = correlated.clone();
    sources.extend(aggregate.aggregates.iter().cloned());
    let free_vars = distinct_outer_refs(&sources, &inner_aliases);
    if correlated.is_empty() || free_vars.is_empty() {
        return None;
    }
    Some(DependentAggregate {
        correlated,
        local,
        inner: filter.input.as_ref().clone(),
        free_vars,
        aggregates: aggregate.aggregates.clone(),
    })
}

/// Recognize `Limit[Sort?[Projection(one value)[Filter(correlated, inner)]]]`, the
/// Case B top-k-per-outer-row shape. Returns None when the body is a different
/// shape or carries no correlation / no free variables.
fn dependent_limit_shape(body: &LogicalPlan) -> Option<DependentLimit> {
    let LogicalPlan::Limit(limit_node) = body else {
        return None;
    };
    let (order, below) = peel_sort(&limit_node.input);
    let LogicalPlan::Projection(projection) = below else {
        return None;
    };
    if projection.expressions.len() != 1 {
        return None;
    }
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        return None;
    };
    let inner_aliases = collect_inner_aliases(body);
    let (correlated, local) = split_by_outer(&filter.predicate, &inner_aliases);
    let free_vars = distinct_outer_refs(&correlated, &inner_aliases);
    if correlated.is_empty() || free_vars.is_empty() {
        return None;
    }
    Some(DependentLimit {
        value: projection.expressions[0].clone(),
        order,
        limit: limit_node.limit,
        correlated,
        local,
        inner: filter.input.as_ref().clone(),
        free_vars,
    })
}

/// Split a leading `Sort` into its (keys, ascending, nulls) order and the node
/// below it; an absent Sort yields `(None, node)`.
fn peel_sort(node: &LogicalPlan) -> (Option<OrderSpec>, &LogicalPlan) {
    match node {
        LogicalPlan::Sort(sort) => (
            Some((
                sort.sort_keys.clone(),
                sort.ascending.clone(),
                sort.nulls_order.clone(),
            )),
            sort.input.as_ref(),
        ),
        other => (None, other),
    }
}

/// Split a conjunctive predicate into (references-outer, inner-only) conjuncts.
fn split_by_outer(predicate: &Expr, inner_aliases: &HashSet<String>) -> (Vec<Expr>, Vec<Expr>) {
    let mut correlated = Vec::new();
    let mut local = Vec::new();
    for conjunct in split_conjuncts(predicate) {
        if references_outer(conjunct, inner_aliases) {
            correlated.push(conjunct.clone());
        } else {
            local.push(conjunct.clone());
        }
    }
    (correlated, local)
}

/// The distinct outer column references (free vars) across the predicates, in
/// first-seen order (the order the domain columns `d0, d1, ...` follow).
fn distinct_outer_refs(predicates: &[Expr], inner_aliases: &HashSet<String>) -> Vec<ColumnRef> {
    let mut seen: HashSet<(Option<String>, String)> = HashSet::new();
    let mut free_vars = Vec::new();
    for predicate in predicates {
        for col in column_refs(predicate) {
            if is_outer_ref(col, inner_aliases)
                && seen.insert((col.table.clone(), col.column.clone()))
            {
                free_vars.push(col.clone());
            }
        }
    }
    free_vars
}

/// The DISTINCT outer correlation values, aliased so their `d{i}` columns are
/// qualified. Returns the aliased domain relation and the insertion-ordered map
/// from each free var to its domain column reference.
fn build_domain(plan: LogicalPlan, free_vars: &[ColumnRef], prefix: &str) -> (LogicalPlan, DomMap) {
    let mut names = Vec::with_capacity(free_vars.len());
    let mut dom_map: DomMap = Vec::with_capacity(free_vars.len());
    for (index, free_var) in free_vars.iter().enumerate() {
        let name = format!("d{index}");
        names.push(name.clone());
        // The domain column stands for its free variable, so it carries the bound
        // outer column's type.
        let data_type = free_var
            .data_type
            .expect("dependent-join free variable carries its bound type");
        dom_map.push((
            (free_var.table.clone(), free_var.column.clone()),
            ColumnRef::new(Some(prefix.to_string()), name, Some(data_type)),
        ));
    }
    // Fresh DISTINCT projection of the outer free vars into the domain relation - no
    // base to copy from. Field list is the complete Projection struct.
    let distinct = LogicalPlan::Projection(Projection {
        input: Box::new(plan),
        expressions: free_vars.iter().map(|c| Expr::Column(c.clone())).collect(),
        aliases: names,
        distinct: true,
        distinct_on: None,
    });
    (aliased(distinct, prefix), dom_map)
}

/// Aggregate once per domain value over `domain JOIN inner ON correlation`. The
/// domain columns go into BOTH `group_by` AND the head of the aggregate outputs
/// (as passthroughs) so the wrapping alias re-exposes `d{i}` for the join-back.
/// `outputs`/`names` are lists so this serves a scalar (one value) and a LATERAL.
fn build_dependent_relation(
    outputs: &[Expr],
    names: &[String],
    join: DomainJoin,
    dom_map: &DomMap,
    prefix: &str,
) -> Result<LogicalPlan> {
    let condition = dependent_join_condition(&join.correlated, &join.local, dom_map)?;
    let dependent_input = inner_join(join.domain, join.inner, condition);
    let mut group_by = Vec::with_capacity(dom_map.len());
    let mut select_list = Vec::with_capacity(dom_map.len() + outputs.len());
    let mut output_names = Vec::with_capacity(dom_map.len() + names.len());
    for (_, domain_ref) in dom_map {
        group_by.push(Expr::Column(domain_ref.clone()));
        // The group key is repeated as a passthrough output, not just a grouping
        // key, so the alias above re-exposes d{i} for the join-back.
        select_list.push(Expr::Column(domain_ref.clone()));
        output_names.push(domain_ref.column.clone());
    }
    for output in outputs {
        select_list.push(replace_outer_with_domain(output.clone(), dom_map));
    }
    output_names.extend(names.iter().cloned());
    // Fresh aggregate reducing the domain join once per domain value - no base to
    // copy from. Field list is the complete Aggregate struct.
    let aggregate = LogicalPlan::Aggregate(Aggregate {
        input: Box::new(dependent_input),
        group_by,
        aggregates: select_list,
        output_names,
        grouping_sets: None,
    });
    Ok(aliased(aggregate, prefix))
}

/// The dependent join condition: the correlation with outer refs rewritten to
/// their domain columns, AND any inner-only conjuncts.
fn dependent_join_condition(correlated: &[Expr], local: &[Expr], dom_map: &DomMap) -> Result<Expr> {
    let mut rewritten = Vec::with_capacity(correlated.len() + local.len());
    for conjunct in correlated {
        rewritten.push(replace_outer_with_domain(conjunct.clone(), dom_map));
    }
    rewritten.extend(local.iter().cloned());
    and_join(rewritten)
}

/// LEFT-join a reduced dependent relation back onto the outer plan, equating each
/// outer free var to the dependent relation's re-exposed domain column. LEFT so an
/// outer row with no matching group survives with a NULL value.
fn join_back(
    plan: LogicalPlan,
    dependent: LogicalPlan,
    free_vars: &[ColumnRef],
    dom_map: &DomMap,
    dep_prefix: &str,
) -> Result<LogicalPlan> {
    let condition = join_back_condition(free_vars, dom_map, dep_prefix)?;
    // Fresh LEFT join-back of the reduced dependent relation onto the outer plan - no
    // base to copy from. Field list is the complete Join struct (no estimates yet).
    Ok(LogicalPlan::Join(Join {
        left: Box::new(plan),
        right: Box::new(dependent),
        join_type: JoinType::Left,
        condition: Some(condition),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    }))
}

/// Equate each outer free var to the dependent relation's domain column `d{i}`,
/// ANDed together - the join-back condition.
fn join_back_condition(
    free_vars: &[ColumnRef],
    dom_map: &DomMap,
    dep_prefix: &str,
) -> Result<Expr> {
    let mut equalities = Vec::with_capacity(free_vars.len());
    for free_var in free_vars {
        let domain_ref = dom_lookup(dom_map, free_var.table.as_ref(), &free_var.column)
            .ok_or_else(|| {
                DecorrelationError::Invariant(
                    "a free variable has no domain column in the dependent-join map".to_string(),
                )
            })?;
        let domain_type = domain_ref
            .data_type
            .expect("domain column carries its bound type");
        let dependent_ref = qualified_col(dep_prefix, &domain_ref.column, domain_type);
        equalities.push(binary(
            BinaryOpType::Eq,
            Expr::Column(free_var.clone()),
            dependent_ref,
        ));
    }
    and_join(equalities)
}

/// The dependent aggregate's value, qualified to the dependent alias. A COUNT of
/// an empty group must read 0, not the LEFT join's NULL (the Kim 1982 count bug),
/// so a COUNT value is wrapped in `COALESCE(value, 0)`.
fn scalar_value_ref(aggregate: &Expr, dep_prefix: &str) -> Expr {
    let value_ref = qualified_col(dep_prefix, UNNEST_VALUE, aggregate.get_type());
    if is_count_aggregate(aggregate) {
        return coalesce_zero(value_ref);
    }
    value_ref
}

/// Whether an aggregate is a COUNT (0 over an empty group, not NULL).
fn is_count_aggregate(aggregate: &Expr) -> bool {
    matches!(
        aggregate,
        Expr::FunctionCall { function_name, .. } if function_name.eq_ignore_ascii_case("COUNT")
    )
}

/// Project (domain columns, output values) directly over the domain join for a
/// no-LIMIT (set) body, rewriting any outer reference in a value to its domain
/// column. Every matching row is kept - there is no ranking.
fn project_join_values(
    joined: LogicalPlan,
    value_exprs: &[Expr],
    value_names: &[String],
    dom_map: &DomMap,
) -> LogicalPlan {
    let mut expressions: Vec<Expr> = domain_columns(dom_map);
    let mut names = domain_names(dom_map);
    for value in value_exprs {
        expressions.push(replace_outer_with_domain(value.clone(), dom_map));
    }
    names.extend(value_names.iter().cloned());
    // Fresh projection of (domain columns, output values) over the domain join - no
    // base to copy from. Field list is the complete Projection struct.
    LogicalPlan::Projection(Projection {
        input: Box::new(joined),
        expressions,
        aliases: names,
        distinct: false,
        distinct_on: None,
    })
}

/// Project (domain columns, output values, `ROW_NUMBER()` per domain) over the
/// domain join, aliased. Any outer reference in a value is rewritten to its domain
/// column; the rank's alias is what the `rn <= limit` cap filters on.
fn rank_by_row_number(
    joined: LogicalPlan,
    value_exprs: &[Expr],
    value_names: &[String],
    order: Option<&OrderSpec>,
    dom_map: &DomMap,
    win_prefix: &str,
) -> LogicalPlan {
    let window = row_number_window(domain_columns(dom_map), order);
    let mut expressions: Vec<Expr> = domain_columns(dom_map);
    let mut names = domain_names(dom_map);
    for value in value_exprs {
        expressions.push(replace_outer_with_domain(value.clone(), dom_map));
    }
    names.extend(value_names.iter().cloned());
    expressions.push(window);
    names.push(UNNEST_RANK.to_string());
    // Fresh projection of (domain columns, values, ROW_NUMBER rank) - no base to
    // copy from. Field list is the complete Projection struct.
    let projection = LogicalPlan::Projection(Projection {
        input: Box::new(joined),
        expressions,
        aliases: names,
        distinct: false,
        distinct_on: None,
    });
    aliased(projection, win_prefix)
}

/// A `ROW_NUMBER()` window partitioned by the domain columns, ordered as the
/// subquery asked (empty order when it had none).
fn row_number_window(partition_by: Vec<Expr>, order: Option<&OrderSpec>) -> Expr {
    // Fresh ROW_NUMBER() call built from scratch - no base to copy from. Field list
    // is the complete FunctionCall variant.
    let row_number = Expr::FunctionCall {
        function_name: "ROW_NUMBER".to_string(),
        args: Vec::new(),
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    };
    let (order_keys, order_ascending, order_nulls) = match order {
        Some((keys, ascending, nulls)) => {
            let nulls = nulls.clone().unwrap_or_else(|| vec![None; keys.len()]);
            (keys.clone(), ascending.clone(), nulls)
        }
        None => (Vec::new(), Vec::new(), Vec::new()),
    };
    // Fresh window wrapping ROW_NUMBER() with the domain partition/order - no base to
    // copy from. Field list is the complete Window variant.
    Expr::Window {
        function: Box::new(row_number),
        partition_by,
        order_keys,
        order_ascending,
        order_nulls,
        frame: None,
    }
}

/// The `nk_rank <= limit` predicate keeping the top-k rows per domain value.
fn row_number_cap(win_prefix: &str, limit: u64) -> Result<Expr> {
    let bound = i64::try_from(limit).map_err(|_| {
        DecorrelationError::Unsupported("LIMIT exceeds the supported integer range".to_string())
    })?;
    // Fresh integer literal for the row-number bound - no base to copy from. Field
    // list (value/data_type) is the complete Literal variant.
    let bound_literal = Expr::Literal {
        value: LiteralValue::Integer(bound),
        data_type: DataType::Integer,
    };
    Ok(binary(
        BinaryOpType::Lte,
        // ROW_NUMBER() ranks as a BigInt, so the rank column is BigInt-typed.
        qualified_col(win_prefix, UNNEST_RANK, DataType::BigInt),
        bound_literal,
    ))
}

/// Project the ranked, capped relation down to (domain columns, values), dropping
/// the rank column. Each output is re-exposed from the ranked relation's alias.
fn project_domain_values(
    capped: LogicalPlan,
    value_names: &[String],
    value_types: &[DataType],
    dom_map: &DomMap,
    win_prefix: &str,
) -> LogicalPlan {
    let mut expressions = Vec::with_capacity(dom_map.len() + value_names.len());
    let mut names = Vec::with_capacity(dom_map.len() + value_names.len());
    for (_, domain_ref) in dom_map {
        let domain_type = domain_ref
            .data_type
            .expect("domain column carries its bound type");
        expressions.push(qualified_col(win_prefix, &domain_ref.column, domain_type));
        names.push(domain_ref.column.clone());
    }
    for (value_name, value_type) in value_names.iter().zip(value_types.iter()) {
        expressions.push(qualified_col(win_prefix, value_name, *value_type));
        names.push(value_name.clone());
    }
    // Fresh projection of (domain columns, values) dropping the rank - no base to
    // copy from. Field list is the complete Projection struct.
    LogicalPlan::Projection(Projection {
        input: Box::new(capped),
        expressions,
        aliases: names,
        distinct: false,
        distinct_on: None,
    })
}

/// Rename a single-column subquery body's output to `value_name` for a LATERAL.
/// An unshaped body raises rather than emitting a broken lateral relation.
fn project_lateral_value(node: LogicalPlan, value_name: &str) -> Result<LogicalPlan> {
    match node {
        LogicalPlan::Projection(mut projection) => {
            projection.aliases = vec![value_name.to_string()];
            Ok(LogicalPlan::Projection(projection))
        }
        LogicalPlan::Aggregate(mut aggregate) => {
            aggregate.output_names = vec![value_name.to_string()];
            Ok(LogicalPlan::Aggregate(aggregate))
        }
        LogicalPlan::Limit(mut node) => {
            node.input = Box::new(project_lateral_value(*node.input, value_name)?);
            Ok(LogicalPlan::Limit(node))
        }
        LogicalPlan::Sort(mut node) => {
            node.input = Box::new(project_lateral_value(*node.input, value_name)?);
            Ok(LogicalPlan::Sort(node))
        }
        LogicalPlan::Filter(mut node) => {
            node.input = Box::new(project_lateral_value(*node.input, value_name)?);
            Ok(LogicalPlan::Filter(node))
        }
        _ => Err(DecorrelationError::Unsupported(
            "Unsupported scalar subquery body for LATERAL".to_string(),
        )),
    }
}

/// The domain column references (`d{i}`, qualified to the domain alias), in order.
fn domain_columns(dom_map: &DomMap) -> Vec<Expr> {
    dom_map
        .iter()
        .map(|(_, domain_ref)| Expr::Column(domain_ref.clone()))
        .collect()
}

/// The domain column names (`d0, d1, ...`), in order.
fn domain_names(dom_map: &DomMap) -> Vec<String> {
    dom_map
        .iter()
        .map(|(_, domain_ref)| domain_ref.column.clone())
        .collect()
}

/// Look up a free var's domain column reference by its `(table, column)` key.
fn dom_lookup<'a>(
    dom_map: &'a DomMap,
    table: Option<&String>,
    column: &str,
) -> Option<&'a ColumnRef> {
    dom_map
        .iter()
        .find(|((t, c), _)| t.as_ref() == table && c == column)
        .map(|(_, domain_ref)| domain_ref)
}

/// Rewrite every outer reference in an expression to its domain column, leaving
/// inner and constant references untouched.
fn replace_outer_with_domain(expr: Expr, dom_map: &DomMap) -> Expr {
    replace_column_refs(expr, &|col: &ColumnRef| {
        dom_lookup(dom_map, col.table.as_ref(), &col.column)
            .map(|domain_ref| Expr::Column(domain_ref.clone()))
    })
}

/// A plain INNER join over the two relations on `condition`.
fn inner_join(left: LogicalPlan, right: LogicalPlan, condition: Expr) -> LogicalPlan {
    // Fresh INNER join over two relations - no base to copy from. Field list is the
    // complete Join struct (no stamped estimates yet).
    LogicalPlan::Join(Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        condition: Some(condition),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    })
}

/// Wrap a relation under a boundary alias so its columns are addressable and
/// qualified from the relation above.
fn aliased(input: LogicalPlan, alias: &str) -> LogicalPlan {
    // Fresh boundary alias wrapping a relation - no base to copy from. Field list
    // (input/alias/column_names) is the complete SubqueryScan struct.
    LogicalPlan::SubqueryScan(SubqueryScan {
        input: Box::new(input),
        alias: alias.to_string(),
        column_names: None,
    })
}
