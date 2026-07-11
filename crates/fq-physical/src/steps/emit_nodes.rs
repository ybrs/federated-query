//! One emitter per `PhysicalPlan` variant (the `_NODE_EMITTERS` table). The walk is
//! bottom-up: each node emits its steps and returns the binding holding its output,
//! which a parent resolves its expressions against. Ports the `_emit_*` functions of
//! `executor/rust_ir.py`.
//!
//! JOIN REDUCTION IS DEFERRED: `emit_join` emits both sides in full then builds the
//! hash-join fragment (correct; the semi-join `InjectedScan`/`CollectDistinct` path
//! is a later stage). Every emitter is otherwise a faithful port.

use std::collections::BTreeMap;

use fq_emit::set_op_keyword;
use fq_plan::expr::{contains_grouping, contains_window};
use fq_plan::physical::{
    physical_column_name, ColumnAliasMap, PhysicalCte, PhysicalCteMergeQuery, PhysicalCteScan,
    PhysicalFilter, PhysicalGroupedLimit, PhysicalHashAggregate, PhysicalHashJoin, PhysicalLimit,
    PhysicalNestedLoopJoin, PhysicalPlan, PhysicalProjection, PhysicalSetOperation,
    PhysicalShipment, PhysicalSingleRowGuard, PhysicalSort, PhysicalUnion, PhysicalValues,
    PhysicalWindow,
};
use fq_plan::{output_column_names, ColumnRef, Expr, JoinType, NullsOrder};

use super::cse::{emit_step_once, scan_share_key};
use super::error::StepError;
use super::expr_retag::{over_input, relation_names, two_sided};
use super::render_sql::{
    render_aggregate_sql, render_grouped_limit_sql, render_values_sql, render_window_sql,
    GROUPED_LIMIT_INDEX_COL, MERGE_GROUPED_LIMIT_RELATION, MERGE_WINDOW_RELATION,
};
use super::scan_spec::{source_scan_spec, variant_name};
use super::types::{
    AggCall, AggSelectItem, Fragment, JoinKind, Projection, SortKey, Step, WithinGroup,
};
use super::{merge_step, node_id, raw_sql_step, Ctx};

/// Emit steps for `node`, returning the binding that holds its output. First checks
/// the injected-base cache (a base already reduced returns that binding), then
/// dispatches on the variant. Ports `_emit` + `_NODE_EMITTERS`. An unmodeled node is
/// a loud typed error, never a silent default.
pub fn emit(node: &PhysicalPlan, ctx: &mut Ctx) -> Result<String, StepError> {
    if let Some(cached) = ctx.injected.get(&node_id(node)) {
        return Ok(cached.clone());
    }
    match node {
        PhysicalPlan::Scan(_) | PhysicalPlan::RemoteQuery(_) | PhysicalPlan::RemoteSetOp(_) => {
            emit_source(node, ctx)
        }
        PhysicalPlan::HashJoin(join) => emit_join(node, join, ctx),
        PhysicalPlan::NestedLoopJoin(join) => emit_nested_loop_join(node, join, ctx),
        PhysicalPlan::Projection(projection) => emit_projection(projection, ctx),
        PhysicalPlan::HashAggregate(aggregate) => emit_aggregate(aggregate, ctx),
        PhysicalPlan::Sort(sort) => emit_sort(sort, ctx),
        PhysicalPlan::Filter(filter) => emit_filter(filter, ctx),
        PhysicalPlan::Limit(limit) => emit_limit(limit, ctx),
        PhysicalPlan::Values(values) => emit_values(values, ctx),
        PhysicalPlan::CteMergeQuery(cte_merge) => emit_cte_merge(cte_merge, ctx),
        PhysicalPlan::CteScan(cte_scan) => emit_cte_scan(cte_scan, ctx),
        PhysicalPlan::Shipment(shipment) => emit_shipment(shipment, ctx),
        PhysicalPlan::AliasedRelation(aliased) => emit(&aliased.input, ctx),
        PhysicalPlan::Window(window) => emit_window(window, ctx),
        PhysicalPlan::GroupedLimit(grouped) => emit_grouped_limit(grouped, ctx),
        PhysicalPlan::SingleRowGuard(guard) => emit_single_row_guard(guard, ctx),
        PhysicalPlan::Union(union) => emit_union(union, ctx),
        PhysicalPlan::SetOperation(set_op) => emit_set_operation(set_op, ctx),
        PhysicalPlan::Cte(_) => Err(StepError::UnsupportedNode("Cte")),
        PhysicalPlan::RemoteJoin(_) => Err(StepError::UnsupportedNode("RemoteJoin")),
        PhysicalPlan::LateralJoin(_) => Err(StepError::UnsupportedNode("LateralJoin")),
        PhysicalPlan::Explain(_) => Err(StepError::UnsupportedNode("Explain")),
        PhysicalPlan::Gather(_) => Err(StepError::UnsupportedNode("Gather")),
    }
}

/// A source scan run natively (structured+parallel when it qualifies). Ports
/// `_emit_source` (the observation recording is deferred).
fn emit_source(node: &PhysicalPlan, ctx: &mut Ctx) -> Result<String, StepError> {
    let binding = ctx.names.binding();
    let step = Step::SourceScan {
        datasource: source_datasource(node)?,
        scan: source_scan_spec(node)?,
        binding,
        materialize: false,
    };
    let key = scan_share_key(node, &step);
    Ok(emit_step_once(ctx, step, Some(key)))
}

/// The datasource name of a source node. Ports the `node.datasource` reads.
fn source_datasource(node: &PhysicalPlan) -> Result<String, StepError> {
    match node {
        PhysicalPlan::Scan(scan) => Ok(scan.datasource.clone()),
        PhysicalPlan::RemoteQuery(remote) => Ok(remote.datasource.clone()),
        PhysicalPlan::RemoteSetOp(set_op) => Ok(set_op.datasource.clone()),
        other => Err(StepError::NoSourceSql(variant_name(other))),
    }
}

/// A hash join over its two sides, then a `hash_join` fragment. Ports `_emit_join`
/// WITHOUT the semi-join reduction (both sides emit in full).
fn emit_join(
    node: &PhysicalPlan,
    join: &PhysicalHashJoin,
    ctx: &mut Ctx,
) -> Result<String, StepError> {
    let kind = join_kind(join.join_type)?;
    let left_binding = emit(&join.left, ctx)?;
    let right_binding = emit(&join.right, ctx)?;
    let fragment = Fragment::HashJoin {
        join_type: kind,
        left_keys: key_names(&join.left_keys, &join.left),
        right_keys: key_names(&join.right_keys, &join.right),
        project: join_output_projection(node, &join.left, &join.right),
    };
    let name = ctx.names.fragment();
    ctx.fragments.insert(name.clone(), fragment);
    Ok(merge_step(
        ctx,
        &name,
        join_inputs(left_binding, right_binding),
    ))
}

/// A non-equi (nested-loop) join. Ports `_emit_nested_loop_join`.
fn emit_nested_loop_join(
    node: &PhysicalPlan,
    join: &PhysicalNestedLoopJoin,
    ctx: &mut Ctx,
) -> Result<String, StepError> {
    let kind = nested_loop_kind(join)?;
    let left_binding = emit(&join.left, ctx)?;
    let right_binding = emit(&join.right, ctx)?;
    let condition = join
        .condition
        .as_ref()
        .map(|condition| two_sided(condition, join));
    let fragment = Fragment::NestedLoopJoin {
        join_type: kind,
        condition,
        project: join_output_projection(node, &join.left, &join.right),
    };
    let name = ctx.names.fragment();
    ctx.fragments.insert(name.clone(), fragment);
    Ok(merge_step(
        ctx,
        &name,
        join_inputs(left_binding, right_binding),
    ))
}

/// The `{in_left, in_right}` input map of a join merge step.
fn join_inputs(left: String, right: String) -> BTreeMap<String, String> {
    let mut inputs = BTreeMap::new();
    inputs.insert("in_left".to_string(), left);
    inputs.insert("in_right".to_string(), right);
    inputs
}

/// The IR join kind for a physical join type. Ports `_JOIN_KINDS`; an unmapped type
/// (e.g. CROSS, which only a nested-loop join carries) RAISES.
fn join_kind(join_type: JoinType) -> Result<JoinKind, StepError> {
    match join_type {
        JoinType::Inner => Ok(JoinKind::Inner),
        JoinType::Left => Ok(JoinKind::Left),
        JoinType::Right => Ok(JoinKind::Right),
        JoinType::Full => Ok(JoinKind::Full),
        JoinType::Semi => Ok(JoinKind::Semi),
        JoinType::Anti => Ok(JoinKind::Anti),
        JoinType::Cross => Err(StepError::UnsupportedJoinType("CROSS".to_string())),
    }
}

/// The IR join kind for a nested-loop join: CROSS is a conditionless INNER join (the
/// engine reads an absent condition as the cross product). Ports `_nested_loop_kind`.
fn nested_loop_kind(join: &PhysicalNestedLoopJoin) -> Result<JoinKind, StepError> {
    if join.join_type == JoinType::Cross {
        return Ok(JoinKind::Inner);
    }
    join_kind(join.join_type)
}

/// Physical column names of a join's keys against a child's aliases. Ports
/// `_key_names`.
fn key_names(keys: &[Expr], child: &PhysicalPlan) -> Vec<String> {
    let aliases = child.column_aliases();
    let mut names = Vec::with_capacity(keys.len());
    for key in keys {
        names.push(key_name(key, &aliases));
    }
    names
}

/// The physical name of one join key (a column reference after binding).
fn key_name(key: &Expr, aliases: &ColumnAliasMap) -> String {
    match key {
        Expr::Column(col) => physical_column_name(col, aliases),
        _ => panic!("a join key must be a column reference after binding"),
    }
}

/// Every join output column, from whichever side owns it, canonically named and
/// alias-uniquified. Ports `_join_output_projection`.
fn join_output_projection(
    node: &PhysicalPlan,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
) -> Vec<Projection> {
    let left_aliases = left.column_aliases();
    let right_aliases = right.column_aliases();
    let left_tables = relation_names(&left_aliases);
    let mut project = Vec::new();
    for ((table, column), out_name) in node.column_aliases() {
        let key = (table.clone(), column);
        let (relation, physical) = if left_tables.contains(&table) {
            ("in_left", resolve_side(&left_aliases, &key))
        } else {
            ("in_right", resolve_side(&right_aliases, &key))
        };
        project.push(Projection {
            expr: side_column(relation, &physical),
            alias: out_name,
        });
    }
    uniquify_aliases(&mut project);
    project
}

/// The physical name a side exposes for a join output key.
fn resolve_side(aliases: &ColumnAliasMap, key: &(Option<String>, String)) -> String {
    aliases.get(key).cloned().unwrap_or_else(|| key.1.clone())
}

/// A projection item selecting `relation`.`name`, aliased to `name`.
fn side_column(relation: &str, name: &str) -> Expr {
    Expr::Column(ColumnRef {
        table: Some(relation.to_string()),
        column: name.to_string(),
        data_type: None,
    })
}

/// Suffix any repeated output alias (`name`, `name_1`, `name_2`, ...) so a self-join
/// result has unique column names. Ports `_uniquify_aliases`.
fn uniquify_aliases(project: &mut [Projection]) {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for item in project.iter_mut() {
        let name = item.alias.clone();
        match seen.get_mut(&name) {
            None => {
                seen.insert(name, 0);
            }
            Some(count) => {
                *count += 1;
                item.alias = format!("{name}_{count}");
            }
        }
    }
}

/// A projection, as a `project` fragment. A cross-source DISTINCT ON RAISES (no
/// source ordering to pick a survivor). Ports `_emit_projection`.
fn emit_projection(node: &PhysicalProjection, ctx: &mut Ctx) -> Result<String, StepError> {
    if node.distinct_on.is_some() {
        return Err(StepError::DistinctOnCrossSource);
    }
    let child = emit(&node.input, ctx)?;
    let aliases = node.input.column_aliases();
    let project = projection_items(&node.expressions, &node.output_names, &aliases, &node.input);
    let fragment = Fragment::Project {
        project,
        distinct: node.distinct,
    };
    let name = ctx.names.fragment();
    ctx.fragments.insert(name.clone(), fragment);
    Ok(merge_step(ctx, &name, single_input(child)))
}

/// Serialize each output expression aliased to its output name; a `*` column
/// expands to one item per input column. Ports `_projection_items` +
/// `_append_star_items`.
fn projection_items(
    expressions: &[Expr],
    names: &[String],
    aliases: &ColumnAliasMap,
    input: &PhysicalPlan,
) -> Vec<Projection> {
    let mut items = Vec::new();
    for (expr, name) in expressions.iter().zip(names) {
        if is_star_column(expr) {
            for input_name in output_column_names(input) {
                items.push(Projection {
                    expr: side_column("in_0", &input_name),
                    alias: input_name,
                });
            }
        } else {
            items.push(Projection {
                expr: over_input(expr, "in_0", aliases),
                alias: name.clone(),
            });
        }
    }
    items
}

/// Whether an expression is the `*` wildcard column reference.
fn is_star_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(col) if col.column == "*")
}

/// A GROUP BY / GROUPING SETS aggregate, as an `aggregate` fragment - or a rendered
/// raw `SELECT ... GROUP BY` when an output carries a window. Ports `_emit_aggregate`.
fn emit_aggregate(node: &PhysicalHashAggregate, ctx: &mut Ctx) -> Result<String, StepError> {
    let child = emit(&node.input, ctx)?;
    let aliases = node.input.column_aliases();
    if node.aggregates.iter().any(contains_window) {
        if node.aggregates.iter().any(contains_grouping) {
            // GROUPING() inside a window needs the two-stage split (DataFusion gap).
            return Err(StepError::WindowSplitUnsupported);
        }
        let sql = render_aggregate_sql(node, &aliases)?;
        return Ok(raw_sql_step(ctx, sql, single_input(child)));
    }
    let fragment = aggregate_fragment(node, &aliases);
    let name = ctx.names.fragment();
    ctx.fragments.insert(name.clone(), fragment);
    Ok(merge_step(ctx, &name, single_input(child)))
}

/// Build the aggregate fragment: select list, group-by keys, optional grouping sets,
/// all retagged over `in_0`. Ports `_aggregate_fragment`.
fn aggregate_fragment(node: &PhysicalHashAggregate, aliases: &ColumnAliasMap) -> Fragment {
    let mut select = Vec::with_capacity(node.aggregates.len());
    for (expr, name) in node.aggregates.iter().zip(&node.output_names) {
        select.push(aggregate_item(expr, name, aliases));
    }
    let mut group_by = Vec::with_capacity(node.group_by.len());
    for key in &node.group_by {
        group_by.push(over_input(key, "in_0", aliases));
    }
    let grouping_sets = node
        .grouping_sets
        .as_ref()
        .map(|sets| retag_sets(sets, aliases))
        .unwrap_or_default();
    Fragment::Aggregate {
        select,
        group_by,
        grouping_sets,
    }
}

/// Retag each grouping set's expressions over `in_0`.
fn retag_sets(sets: &[Vec<Expr>], aliases: &ColumnAliasMap) -> Vec<Vec<Expr>> {
    let mut result = Vec::with_capacity(sets.len());
    for group_set in sets {
        let mut exprs = Vec::with_capacity(group_set.len());
        for expr in group_set {
            exprs.push(over_input(expr, "in_0", aliases));
        }
        result.push(exprs);
    }
    result
}

/// One aggregate output: an aggregate call or a plain grouping expression. Ports
/// `_aggregate_item`.
fn aggregate_item(expr: &Expr, name: &str, aliases: &ColumnAliasMap) -> AggSelectItem {
    if let Expr::FunctionCall {
        is_aggregate: true, ..
    } = expr
    {
        return AggSelectItem {
            expr: None,
            agg: Some(agg_call(expr, aliases)),
            alias: name.to_string(),
        };
    }
    AggSelectItem {
        expr: Some(over_input(expr, "in_0", aliases)),
        agg: None,
        alias: name.to_string(),
    }
}

/// Serialize an aggregate FunctionCall: `count(*)` is the star form, an ordered-set
/// aggregate carries its WITHIN GROUP. `func` is kept as authored (NOT lowercased).
/// Ports `_agg_call`.
fn agg_call(expr: &Expr, aliases: &ColumnAliasMap) -> AggCall {
    let Expr::FunctionCall {
        function_name,
        args,
        distinct,
        within_group_key,
        within_group_desc,
        ..
    } = expr
    else {
        panic!("agg_call expects an aggregate FunctionCall");
    };
    let star = is_star_arg(args);
    let call_args = if star {
        Vec::new()
    } else {
        let mut serialized = Vec::with_capacity(args.len());
        for arg in args {
            serialized.push(over_input(arg, "in_0", aliases));
        }
        serialized
    };
    let within_group = within_group_key.as_ref().map(|key| WithinGroup {
        key: over_input(key, "in_0", aliases),
        desc: *within_group_desc,
    });
    AggCall {
        func: function_name.clone(),
        distinct: *distinct,
        star,
        args: call_args,
        within_group,
    }
}

/// True when the single argument is the `*` wildcard column. Ports `_is_star_arg`.
fn is_star_arg(args: &[Expr]) -> bool {
    args.len() == 1 && is_star_column(&args[0])
}

/// A boolean filter, as a `filter` fragment. Ports `_emit_filter`.
fn emit_filter(node: &PhysicalFilter, ctx: &mut Ctx) -> Result<String, StepError> {
    let child = emit(&node.input, ctx)?;
    let predicate = over_input(&node.predicate, "in_0", &node.input.column_aliases());
    let fragment = Fragment::Filter { predicate };
    let name = ctx.names.fragment();
    ctx.fragments.insert(name.clone(), fragment);
    Ok(merge_step(ctx, &name, single_input(child)))
}

/// The scalar-subquery cardinality guard, as a `single_row_guard` fragment. Ports
/// `_emit_single_row_guard`.
fn emit_single_row_guard(
    node: &PhysicalSingleRowGuard,
    ctx: &mut Ctx,
) -> Result<String, StepError> {
    let child = emit(&node.input, ctx)?;
    let aliases = node.input.column_aliases();
    let mut keys = Vec::with_capacity(node.keys.len());
    for key in &node.keys {
        keys.push(over_input(key, "in_0", &aliases));
    }
    let fragment = Fragment::SingleRowGuard { keys };
    let name = ctx.names.fragment();
    ctx.fragments.insert(name.clone(), fragment);
    Ok(merge_step(ctx, &name, single_input(child)))
}

/// An ORDER BY, as a `sort` fragment. Ports `_emit_sort` + `_sort_keys`.
fn emit_sort(node: &PhysicalSort, ctx: &mut Ctx) -> Result<String, StepError> {
    let child = emit(&node.input, ctx)?;
    let aliases = node.input.column_aliases();
    let mut keys = Vec::with_capacity(node.sort_keys.len());
    for (index, expr) in node.sort_keys.iter().enumerate() {
        let ascending = node.ascending[index];
        keys.push(SortKey {
            expr: over_input(expr, "in_0", &aliases),
            ascending,
            nulls_first: nulls_first(node.nulls_order.as_deref(), index, ascending),
        });
    }
    let fragment = Fragment::Sort { keys };
    let name = ctx.names.fragment();
    ctx.fragments.insert(name.clone(), fragment);
    Ok(merge_step(ctx, &name, single_input(child)))
}

/// NULL placement: explicit if given, else the SQL default (DESC first). Ports
/// `_nulls_first`.
fn nulls_first(nulls_order: Option<&[Option<NullsOrder>]>, index: usize, ascending: bool) -> bool {
    let Some(order) = nulls_order else {
        return !ascending;
    };
    if order.is_empty() {
        return !ascending;
    }
    match order[index] {
        None => !ascending,
        Some(NullsOrder::First) => true,
        Some(NullsOrder::Last) => false,
    }
}

/// A LIMIT/OFFSET, as a `limit` fragment. Ports `_emit_limit`.
fn emit_limit(node: &PhysicalLimit, ctx: &mut Ctx) -> Result<String, StepError> {
    let child = emit(&node.input, ctx)?;
    let fragment = Fragment::Limit {
        limit: node.limit,
        offset: node.offset,
    };
    let name = ctx.names.fragment();
    ctx.fragments.insert(name.clone(), fragment);
    Ok(merge_step(ctx, &name, single_input(child)))
}

/// A VALUES relation, rendered to raw SQL over no inputs. Ports `_emit_values`.
fn emit_values(node: &PhysicalValues, ctx: &mut Ctx) -> Result<String, StepError> {
    let sql = render_values_sql(&node.rows, &node.output_names)?;
    Ok(raw_sql_step(ctx, sql, BTreeMap::new()))
}

/// A whole WITH rendered as SQL over named inputs. Ports `_emit_cte_merge`.
fn emit_cte_merge(node: &PhysicalCteMergeQuery, ctx: &mut Ctx) -> Result<String, StepError> {
    let mut inputs = BTreeMap::new();
    for (name, subtree) in &node.inputs {
        inputs.insert(name.clone(), emit(subtree, ctx)?);
    }
    Ok(raw_sql_step(ctx, node.sql.clone(), inputs))
}

/// A CTE reference: the producer's body is emitted (and executed) ONCE; later
/// references reuse the binding. Ports `_emit_cte_scan`.
fn emit_cte_scan(node: &PhysicalCteScan, ctx: &mut Ctx) -> Result<String, StepError> {
    let producer = node.producer.as_ref();
    if let Some(cached) = ctx.cte_bindings.get(&node_id(producer)) {
        return Ok(cached.clone());
    }
    let PhysicalPlan::Cte(cte) = producer else {
        return Err(StepError::UnsupportedNode("CteScan.producer must be a Cte"));
    };
    let mut binding = emit(&cte.body, ctx)?;
    if let Some(names) = &cte.column_names {
        binding = relabel_columns(binding, cte, names, ctx);
    }
    ctx.cte_bindings.insert(node_id(producer), binding.clone());
    Ok(binding)
}

/// Project a binding's columns to a CTE's declared names, positionally. Ports
/// `_relabel_columns`.
fn relabel_columns(binding: String, cte: &PhysicalCte, names: &[String], ctx: &mut Ctx) -> String {
    let mut project = Vec::new();
    for (source, target) in output_column_names(&cte.body).iter().zip(names) {
        project.push(Projection {
            expr: side_column("in_0", source),
            alias: target.clone(),
        });
    }
    let fragment = Fragment::Project {
        project,
        distinct: false,
    };
    let name = ctx.names.fragment();
    ctx.fragments.insert(name.clone(), fragment);
    merge_step(ctx, &name, single_input(binding))
}

/// Ship a foreign relation into a target source, then run the island. The ORDER is
/// load-bearing (ship BEFORE the island scan). Ports `_emit_shipment`.
fn emit_shipment(node: &PhysicalShipment, ctx: &mut Ctx) -> Result<String, StepError> {
    let body_binding = emit(&node.body, ctx)?;
    ctx.steps.push(Step::Ship {
        datasource: node.datasource.clone(),
        input: body_binding,
        table: node.table.clone(),
    });
    emit(&node.child, ctx)
}

/// A window-bearing projection, rendered to raw SQL over `in_window`. Ports
/// `_emit_window`.
fn emit_window(node: &PhysicalWindow, ctx: &mut Ctx) -> Result<String, StepError> {
    let child = emit(&node.input, ctx)?;
    let sql = render_window_sql(node, &node.input.column_aliases())?;
    let mut inputs = BTreeMap::new();
    inputs.insert(MERGE_WINDOW_RELATION.to_string(), child);
    Ok(raw_sql_step(ctx, sql, inputs))
}

/// A per-key LIMIT, wrapped in the row-index CTE the merge engine adds. Ports
/// `_emit_grouped_limit`.
fn emit_grouped_limit(node: &PhysicalGroupedLimit, ctx: &mut Ctx) -> Result<String, StepError> {
    let child = emit(&node.input, ctx)?;
    let names = output_column_names(&node.input);
    let node_sql = render_grouped_limit_sql(node, &names)?;
    let sql = format!(
        "WITH {MERGE_GROUPED_LIMIT_RELATION} AS \
         (SELECT *, ROW_NUMBER() OVER () AS \"{GROUPED_LIMIT_INDEX_COL}\" \
         FROM __gl_input) {node_sql}"
    );
    let mut inputs = BTreeMap::new();
    inputs.insert("__gl_input".to_string(), child);
    Ok(raw_sql_step(ctx, sql, inputs))
}

/// A cross-source UNION [ALL], as a raw_sql fragment over its branches. Ports
/// `_emit_union`.
fn emit_union(node: &PhysicalUnion, ctx: &mut Ctx) -> Result<String, StepError> {
    let (inputs, selects) = branch_inputs(ctx, node.inputs.iter().collect::<Vec<_>>())?;
    let keyword = if node.distinct { "UNION" } else { "UNION ALL" };
    let sql = selects.join(&format!(" {keyword} "));
    Ok(raw_sql_step(ctx, sql, inputs))
}

/// A cross-source INTERSECT / EXCEPT [ALL], as a raw_sql fragment. Ports
/// `_emit_set_operation`.
fn emit_set_operation(node: &PhysicalSetOperation, ctx: &mut Ctx) -> Result<String, StepError> {
    let (inputs, selects) = branch_inputs(ctx, vec![&node.left, &node.right])?;
    let keyword = set_op_keyword(node.kind, node.distinct);
    let sql = selects.join(&format!(" {keyword} "));
    Ok(raw_sql_step(ctx, sql, inputs))
}

/// Emit each set-operation branch to an `in_<i>` binding; return the inputs map and
/// the per-branch `SELECT * FROM in_<i>` statements. Ports `_branch_inputs`.
fn branch_inputs(
    ctx: &mut Ctx,
    branches: Vec<&PhysicalPlan>,
) -> Result<(BTreeMap<String, String>, Vec<String>), StepError> {
    let mut inputs = BTreeMap::new();
    let mut selects = Vec::with_capacity(branches.len());
    for (index, child) in branches.into_iter().enumerate() {
        let name = format!("in_{index}");
        inputs.insert(name.clone(), emit(child, ctx)?);
        selects.push(format!("SELECT * FROM {name}"));
    }
    Ok((inputs, selects))
}

/// The `{in_0: binding}` single-input map of a merge step.
fn single_input(binding: String) -> BTreeMap<String, String> {
    let mut inputs = BTreeMap::new();
    inputs.insert("in_0".to_string(), binding);
    inputs
}
