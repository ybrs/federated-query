//! Per-node SQL renderers, composing fq-emit's clause builders. Ports the render
//! methods that in Python lived on the physical nodes: a source scan's SELECT
//! (`PhysicalScan._build_ast` / `_render_source_sql`),
//! and the merge-engine raw-SQL renderers (`_aggregate_sql`, `_window_sql`,
//! `_grouped_limit_sql`, `_render_values_sql`).
//!
//! DIALECT: a scan renders to its SOURCE dialect (from the scan's
//! `datasource_kind`); the merge raw-SQL strings render to `DuckDb` (the merge
//! engine's dialect, matching the Python `dialect="duckdb"`).

use fq_common::DataType;
use fq_emit::clauses::{
    assemble_select, group_by, order_by, select_list, set_op_keyword, SelectPieces,
};
use fq_emit::resolver::{AliasMap, MergeResolver};
use fq_emit::{quote_ident, render_expr, to_source_sql, Dialect, SourceResolver};
use fq_plan::expr::{aggregate_output_map, contains_window, split_where_having};
use fq_plan::physical::{
    ColumnAliasMap, DatasourceKind, PhysicalGroupedLimit, PhysicalHashAggregate, PhysicalPlan,
    PhysicalRemoteSetOp, PhysicalScan, PhysicalWindow,
};
use fq_plan::{ColumnRef, Expr};

use super::error::StepError;

/// The merge relation a window operator's input registers under (`in_window`),
/// matching the fedqrs engine. Ports `_MERGE_WINDOW_RELATION`.
pub const MERGE_WINDOW_RELATION: &str = "in_window";
/// The merge relation a window-bearing aggregate registers under (`in_0`).
/// Ports `_MERGE_AGGREGATE_RELATION`.
pub const MERGE_AGGREGATE_RELATION: &str = "in_0";
/// The merge relation a grouped-limit registers its input under. Ports
/// `_MERGE_GROUPED_LIMIT_RELATION` (must match the fedqrs engine).
pub const MERGE_GROUPED_LIMIT_RELATION: &str = "in_grouped_limit";
/// The synthetic row-order index a grouped-limit orders by. Ports
/// `_GROUPED_LIMIT_INDEX_COL`.
pub const GROUPED_LIMIT_INDEX_COL: &str = "__gl_idx";
/// The synthetic per-key row-number a grouped-limit filters on. Ports
/// `_GROUPED_LIMIT_RN_COL`.
pub const GROUPED_LIMIT_RN_COL: &str = "__gl_rn";

/// The fq-emit render dialect for a scan's source. A scan's raw SQL runs on its
/// source, so it transpiles to that source's dialect.
pub fn scan_dialect(kind: DatasourceKind) -> Dialect {
    match kind {
        DatasourceKind::Postgres => Dialect::Postgres,
        DatasourceKind::DuckDb => Dialect::DuckDb,
        DatasourceKind::ClickHouse => Dialect::ClickHouse,
        DatasourceKind::MySql => Dialect::MySql,
    }
}

/// A `MergeResolver` over a node's column-alias map (BTreeMap -> the resolver's
/// HashMap). The merge path resolves references to physical merge-relation names.
pub fn merge_resolver(aliases: &ColumnAliasMap) -> MergeResolver {
    let mut map = AliasMap::new();
    for (key, physical) in aliases {
        map.insert(key.clone(), physical.clone());
    }
    MergeResolver::new(map)
}

/// Render a source scan to its source-dialect SQL. Ports `PhysicalScan._build_ast`
/// and `_render_source_sql`: build the canonical Postgres SELECT, then transpile to
/// the scan's source dialect.
pub fn render_scan_sql(scan: &PhysicalScan) -> Result<String, StepError> {
    let canonical = render_scan_canonical(scan)?;
    Ok(to_source_sql(
        &canonical,
        scan_dialect(scan.datasource_kind),
    )?)
}

/// Build a scan's canonical Postgres SELECT (no transpile): split the folded filter
/// into WHERE/HAVING, build the SELECT items (aggregates, or the concrete columns; a
/// `*` RAISES), the FROM via `relation_sql::table_ref`, GROUP BY / ORDER BY /
/// DISTINCT / LIMIT / OFFSET, then assemble. The single-transpile callers (scan
/// source SQL, remote set-op branch) compose this.
pub fn render_scan_canonical(scan: &PhysicalScan) -> Result<String, StepError> {
    let (where_pred, having_pred) = scan_where_having(scan);
    let where_sql = render_opt(where_pred.as_ref())?;
    let having_sql = render_opt(having_pred.as_ref())?;
    let items = scan_select_items(scan)?;
    let from_clause = crate::relation_sql::table_ref(
        &scan.table_name,
        non_empty(&scan.schema_name),
        scan.alias.as_deref(),
        scan.sample.as_deref(),
    );
    let group = group_by(
        scan.group_by.as_deref().unwrap_or(&[]),
        scan.grouping_sets.as_deref(),
        &SourceResolver,
    )?;
    let order = order_by(
        scan.order_by_keys.as_deref().unwrap_or(&[]),
        scan.order_by_ascending.as_deref().unwrap_or(&[]),
        scan.order_by_nulls.as_deref().unwrap_or(&[]),
        &SourceResolver,
    )?;
    let pieces = SelectPieces {
        from_clause: &from_clause,
        select_items: &items,
        joins: None,
        where_: where_sql.as_deref(),
        group: group.as_deref(),
        having: having_sql.as_deref(),
        distinct: scan.distinct,
        distinct_on: None,
        order: order.as_deref(),
        limit: scan
            .limit
            .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
        offset: i64::try_from(scan.offset).unwrap_or(i64::MAX),
    };
    Ok(assemble_select(&pieces))
}

/// Render a same-source set operation (`A UNION ALL B`, nested set ops, ORDER
/// BY/LIMIT/OFFSET) to its source-dialect SQL. Ports `PhysicalRemoteSetOp._build_query`
/// / `build_remote_ast`: combine each branch's canonical Postgres SELECT with the
/// set-op keyword, wrap the outer ORDER BY/LIMIT/OFFSET, then transpile ONCE.
pub fn render_remote_set_op(set_op: &PhysicalRemoteSetOp) -> Result<String, StepError> {
    let canonical = remote_set_op_canonical(set_op)?;
    Ok(to_source_sql(&canonical, set_op_dialect(set_op)?)?)
}

/// A remote set op's canonical Postgres text (branches combined + outer
/// ORDER BY/LIMIT/OFFSET wrapper), before transpile. Ports `build_remote_ast`.
fn remote_set_op_canonical(set_op: &PhysicalRemoteSetOp) -> Result<String, StepError> {
    let left = set_op_branch_canonical(&set_op.left)?;
    let right = set_op_branch_canonical(&set_op.right)?;
    let keyword = set_op_keyword(set_op.kind, set_op.distinct);
    let combined = format!("{left} {keyword} {right}");
    if set_op.order_by_keys.is_none() && set_op.limit.is_none() && set_op.offset == 0 {
        return Ok(combined);
    }
    wrap_set_op_order_limit(set_op, &combined)
}

/// One set-op branch's canonical Postgres text: a scan renders its SELECT; a nested
/// set op renders (parenthesized to preserve associativity) its own combined form.
/// Ports `_branch_ast` (a branch is a Scan or a nested RemoteSetOp).
fn set_op_branch_canonical(branch: &PhysicalPlan) -> Result<String, StepError> {
    match branch {
        PhysicalPlan::Scan(scan) => render_scan_canonical(scan),
        PhysicalPlan::RemoteSetOp(nested) => Ok(format!("({})", remote_set_op_canonical(nested)?)),
        other => Err(StepError::NoSourceSql(super::scan_spec::variant_name(
            other,
        ))),
    }
}

/// Wrap a set operation in an outer `SELECT * FROM (<body>) AS "_setop"` carrying
/// ORDER BY / LIMIT / OFFSET. Ports `_wrap_order_limit`.
fn wrap_set_op_order_limit(set_op: &PhysicalRemoteSetOp, body: &str) -> Result<String, StepError> {
    let from_clause = crate::relation_sql::derived_table(body, "_setop");
    let order = order_by(
        set_op.order_by_keys.as_deref().unwrap_or(&[]),
        set_op.order_by_ascending.as_deref().unwrap_or(&[]),
        set_op.order_by_nulls.as_deref().unwrap_or(&[]),
        &SourceResolver,
    )?;
    let pieces = SelectPieces {
        from_clause: &from_clause,
        select_items: "*",
        joins: None,
        where_: None,
        group: None,
        having: None,
        distinct: false,
        distinct_on: None,
        order: order.as_deref(),
        limit: set_op
            .limit
            .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
        offset: i64::try_from(set_op.offset).unwrap_or(i64::MAX),
    };
    Ok(assemble_select(&pieces))
}

/// The source render dialect of a remote set op, from a leaf Scan branch (every
/// branch is the same source). Ports the `self.datasource_connection` transpile
/// target; a branch that is neither a Scan nor a nested set op RAISES.
fn set_op_dialect(set_op: &PhysicalRemoteSetOp) -> Result<Dialect, StepError> {
    match set_op.left.as_ref() {
        PhysicalPlan::Scan(scan) => Ok(scan_dialect(scan.datasource_kind)),
        PhysicalPlan::RemoteSetOp(nested) => set_op_dialect(nested),
        other => Err(StepError::NoSourceSql(super::scan_spec::variant_name(
            other,
        ))),
    }
}

/// Partition a scan's folded filter into (WHERE, HAVING). Ports
/// `PhysicalScan._split_where_having`: a plain scan keeps everything in WHERE; a
/// grouped/aggregate scan routes aggregate-output conjuncts to HAVING.
fn scan_where_having(scan: &PhysicalScan) -> (Option<Expr>, Option<Expr>) {
    let Some(filters) = &scan.filters else {
        return (None, None);
    };
    let grouped = scan.group_by.as_ref().is_some_and(|keys| !keys.is_empty())
        || scan
            .aggregates
            .as_ref()
            .is_some_and(|aggs| !aggs.is_empty());
    if !grouped {
        return (Some(filters.clone()), None);
    }
    let names = scan.output_names.as_deref().unwrap_or(&[]);
    let aggregates = scan.aggregates.as_deref().unwrap_or(&[]);
    let output_map = aggregate_output_map(names, aggregates);
    split_where_having(filters, &output_map)
}

/// The scan's SELECT-list fragment: the aggregate list (aliased) when aggregates
/// are folded, else the concrete quoted columns (a `*` RAISES - a star must never
/// survive into a bound scan). Ports `PhysicalScan._select_items`.
fn scan_select_items(scan: &PhysicalScan) -> Result<String, StepError> {
    if let Some(aggregates) = &scan.aggregates {
        if !aggregates.is_empty() {
            let names = scan.output_names.as_deref().unwrap_or(&[]);
            return Ok(select_list(aggregates, names, &SourceResolver)?);
        }
    }
    if scan.columns.iter().any(|column| column == "*") {
        return Err(StepError::StarInScanColumns);
    }
    let mut parts = Vec::with_capacity(scan.columns.len());
    for column in &scan.columns {
        parts.push(quote_ident(column));
    }
    Ok(parts.join(", "))
}

/// Render `SELECT <exprs> AS <names> FROM in_window` for the merge engine. Ports
/// `PhysicalWindow._window_sql`; columns resolve to physical merge names, the whole
/// expression (window included) transpiles to DuckDb.
pub fn render_window_sql(
    node: &PhysicalWindow,
    aliases: &ColumnAliasMap,
) -> Result<String, StepError> {
    let resolver = merge_resolver(aliases);
    let items = select_list(&node.expressions, &node.output_names, &resolver)?;
    let sql = format!("SELECT {items} FROM {MERGE_WINDOW_RELATION}");
    Ok(to_source_sql(&sql, Dialect::DuckDb)?)
}

/// Render `SELECT <outputs> FROM in_0 [GROUP BY <keys>]` for the merge engine, used
/// when a grouped output carries a window. Ports `PhysicalHashAggregate._aggregate_sql`
/// / `_render_aggregate_sql`.
pub fn render_aggregate_sql(
    node: &PhysicalHashAggregate,
    aliases: &ColumnAliasMap,
) -> Result<String, StepError> {
    render_grouped_select(
        &node.aggregates,
        &node.output_names,
        &node.group_by,
        node.grouping_sets.as_deref(),
        aliases,
    )
}

/// Render `SELECT <exprs> AS <names> FROM in_0 [GROUP BY <keys>]` for the merge
/// engine over the given outputs and grouping. Both the single-stage aggregate SQL
/// and a two-stage split's stage 1 compose it. Ports `_render_aggregate_sql`.
fn render_grouped_select(
    exprs: &[Expr],
    names: &[String],
    keys: &[Expr],
    grouping_sets: Option<&[Vec<Expr>]>,
    aliases: &ColumnAliasMap,
) -> Result<String, StepError> {
    let resolver = merge_resolver(aliases);
    let items = select_list(exprs, names, &resolver)?;
    let mut sql = format!("SELECT {items} FROM {MERGE_AGGREGATE_RELATION}");
    if let Some(group) = group_by(keys, grouping_sets, &resolver)? {
        sql = format!("{sql} GROUP BY {group}");
    }
    Ok(to_source_sql(&sql, Dialect::DuckDb)?)
}

/// The two-stage SQL for a window over a GROUPING() aggregate (the DataFusion
/// planner cannot run GROUPING() inside a window). Stage 1 is the GROUP BY whose
/// outputs are the non-window outputs plus every window operand (grouping()/
/// aggregate calls and key columns) materialized under a hidden name; stage 2 is
/// the window SELECT over stage 1's result, each operand replaced by its stage-1
/// output column and presenting exactly the declared output names. Ports
/// `PhysicalHashAggregate.split_window_aggregate_sqls`.
///
/// The split rewrites each window operand to a plain stage-1 column - sound only
/// while every window's FUNCTION is itself a window function (rank, lead, ...). A
/// window whose function is an aggregate/column would be swallowed into a bare
/// `col OVER (...)`, so such a shape RAISES rather than mis-render.
pub fn render_split_window_aggregate_sqls(
    node: &PhysicalHashAggregate,
    aliases: &ColumnAliasMap,
) -> Result<(String, String), StepError> {
    if !node.aggregates.iter().all(window_functions_are_sound) {
        return Err(StepError::WindowSplitUnsupported);
    }
    let mut stage1_exprs: Vec<Expr> = Vec::new();
    let mut stage1_names: Vec<String> = Vec::new();
    for (expr, name) in node.aggregates.iter().zip(&node.output_names) {
        if !contains_window(expr) {
            stage1_exprs.push(expr.clone());
            stage1_names.push(name.clone());
        }
    }
    let mut stage2_items: Vec<Expr> = Vec::with_capacity(node.aggregates.len());
    for (expr, name) in node.aggregates.iter().zip(&node.output_names) {
        stage2_items.push(stage2_item(
            expr,
            name,
            &mut stage1_exprs,
            &mut stage1_names,
        ));
    }
    let stage1_sql = render_grouped_select(
        &stage1_exprs,
        &stage1_names,
        &node.group_by,
        node.grouping_sets.as_deref(),
        aliases,
    )?;
    let stage2_sql = render_stage2_window(&stage2_items, &stage1_names, &node.output_names)?;
    Ok((stage1_sql, stage2_sql))
}

/// Whether every window inside `expr` has a FUNCTION the two-stage split can carry:
/// a real window function (not an aggregate/column the operand rewrite would replace
/// with a bare column, yielding invalid `col OVER (...)`).
fn window_functions_are_sound(expr: &Expr) -> bool {
    if let Expr::Window { function, .. } = expr {
        if is_stage1_operand(function) {
            return false;
        }
    }
    expr.children()
        .iter()
        .all(|child| window_functions_are_sound(child))
}

/// One stage-2 SELECT item: a pass-through of an already-aggregated output by name,
/// or the window expression rewritten to read stage-1 output columns. Ports
/// `_stage2_item`.
fn stage2_item(expr: &Expr, name: &str, exprs: &mut Vec<Expr>, names: &mut Vec<String>) -> Expr {
    if !contains_window(expr) {
        // The stage-1 output passes through stage 2 unchanged, referenced by name.
        return merge_output_ref(name.to_string(), None);
    }
    materialize_window_operands(expr.clone(), exprs, names)
}

/// Rewrite a window-bearing expression to read stage-1 output columns: a window is
/// rebuilt so its operands live in stage 1 (see `rebuild_window`); any other node
/// whose leaf is a stage-1 operand becomes a reference to the stage-1 output
/// computing it (appended as a hidden output when none does yet). Ports
/// `_materialize_window_operands`.
fn materialize_window_operands(expr: Expr, exprs: &mut Vec<Expr>, names: &mut Vec<String>) -> Expr {
    if let Expr::Window { .. } = &expr {
        return rebuild_window(expr, exprs, names);
    }
    if is_stage1_operand(&expr) {
        return stage1_output_ref(&expr, exprs, names);
    }
    expr.map_children(&mut |child| materialize_window_operands(child, exprs, names))
}

/// Rebuild a window over stage-1 columns: the function's leaf operands become
/// stage-1 column refs, while each PARTITION BY / ORDER BY expression is
/// materialized WHOLE as one stage-1 column so the window partitions and orders by
/// plain columns. A computed PARTITION BY expression (`grouping(a)+grouping(b)`) is
/// a group-level value a stage-1 column carries exactly, and the merge engine
/// requires a plain ordered column there, rejecting the inline expression.
fn rebuild_window(window: Expr, exprs: &mut Vec<Expr>, names: &mut Vec<String>) -> Expr {
    let Expr::Window {
        function,
        partition_by,
        order_keys,
        order_ascending,
        order_nulls,
        frame,
    } = window
    else {
        return window;
    };
    let mut new_partition = Vec::with_capacity(partition_by.len());
    for item in partition_by {
        new_partition.push(stage1_output_ref(&item, exprs, names));
    }
    let mut new_order = Vec::with_capacity(order_keys.len());
    for key in order_keys {
        new_order.push(stage1_output_ref(&key, exprs, names));
    }
    // Variant rebuild: the pattern moved every field out of `window`, so no base
    // remains for `..`/update!; the compiler forces all fields listed (loud). The
    // function keeps leaf materialization; partition/order become stage-1 columns.
    Expr::Window {
        function: Box::new(materialize_window_operands(*function, exprs, names)),
        partition_by: new_partition,
        order_keys: new_order,
        order_ascending,
        order_nulls,
        frame,
    }
}

/// A leaf stage 1 must compute: a column, an aggregate call, or GROUPING() (an
/// aggregate-scoped function the window stage cannot run). Ports `_is_stage1_operand`.
fn is_stage1_operand(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::FunctionCall {
            function_name,
            is_aggregate,
            ..
        } => *is_aggregate || function_name.eq_ignore_ascii_case("grouping"),
        _ => false,
    }
}

/// The stage-1 output column computing `expr`, appending a hidden output (`__w<n>`)
/// when no existing output matches structurally. Ports `_stage1_output_ref`.
fn stage1_output_ref(expr: &Expr, exprs: &mut Vec<Expr>, names: &mut Vec<String>) -> Expr {
    for (index, existing) in exprs.iter().enumerate() {
        if existing == expr {
            return merge_output_ref(names[index].clone(), Some(expr.get_type()));
        }
    }
    let name = format!("__w{}", names.len());
    exprs.push(expr.clone());
    names.push(name.clone());
    merge_output_ref(name, Some(expr.get_type()))
}

/// Render the stage-2 window SELECT over the stage-1 relation: a resolver mapping
/// each stage-1 output name to itself, the window items aliased to the declared
/// output names. Ports `_stage2_sql`.
fn render_stage2_window(
    items: &[Expr],
    stage1_names: &[String],
    output_names: &[String],
) -> Result<String, StepError> {
    let mut aliases = ColumnAliasMap::new();
    for name in stage1_names {
        aliases.insert((None, name.clone()), name.clone());
    }
    let resolver = merge_resolver(&aliases);
    let select = select_list(items, output_names, &resolver)?;
    Ok(to_source_sql(
        &format!("SELECT {select} FROM {MERGE_AGGREGATE_RELATION}"),
        Dialect::DuckDb,
    )?)
}

/// A bare reference to a merge-relation output column by name (`table = None`) - the
/// form a two-stage window's stage-2 items and its materialized operands use; the
/// stage-2 resolver maps `(None, name)` to the physical column.
fn merge_output_ref(name: String, data_type: Option<DataType>) -> Expr {
    // Fresh ColumnRef for a merge output column: there is no source ColumnRef to
    // copy from, so every field is listed (unqualified name, carried type).
    Expr::Column(ColumnRef {
        table: None,
        column: name,
        data_type,
    })
}

/// Render the grouped-limit node SQL (before the engine's `WITH` wrapper): keep
/// each key group's first `limit` rows by `ROW_NUMBER()`, ordered stably by the
/// synthetic row index. Ports `PhysicalGroupedLimit._grouped_limit_sql`. `names`
/// are the input's output column names (the visible columns, minus the synthetic).
pub fn render_grouped_limit_sql(
    node: &PhysicalGroupedLimit,
    names: &[String],
) -> Result<String, StepError> {
    let resolver = merge_resolver(&ColumnAliasMap::new());
    let partition = render_partition(&node.keys, &resolver)?;
    let order = grouped_limit_order(node, &resolver)?;
    let window = format!("ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order})");
    let inner =
        format!("SELECT *, {window} AS {GROUPED_LIMIT_RN_COL} FROM {MERGE_GROUPED_LIMIT_RELATION}");
    let select_list = quote_names(names);
    let node_sql = format!(
        "SELECT {select_list} FROM ({inner}) AS _t \
         WHERE {GROUPED_LIMIT_RN_COL} <= {} ORDER BY {}",
        node.limit,
        quote_ident(GROUPED_LIMIT_INDEX_COL)
    );
    Ok(to_source_sql(&node_sql, Dialect::DuckDb)?)
}

/// The PARTITION BY list of a grouped-limit window, each key rendered against the
/// merge relation. Ports `_partition_clause`.
fn render_partition(keys: &[Expr], resolver: &MergeResolver) -> Result<String, StepError> {
    let mut parts = Vec::with_capacity(keys.len());
    for key in keys {
        parts.push(render_expr(key, resolver)?);
    }
    Ok(parts.join(", "))
}

/// The ORDER BY of a grouped-limit window: the ordering keys, then the synthetic
/// row index as a stable tiebreak. Ports `_merge_order_clause`.
fn grouped_limit_order(
    node: &PhysicalGroupedLimit,
    resolver: &MergeResolver,
) -> Result<String, StepError> {
    let mut parts = Vec::new();
    if let Some(keys) = &node.order_by_keys {
        if !keys.is_empty() {
            if let Some(fragment) = order_by(
                keys,
                node.order_by_ascending.as_deref().unwrap_or(&[]),
                node.order_by_nulls.as_deref().unwrap_or(&[]),
                resolver,
            )? {
                parts.push(fragment);
            }
        }
    }
    parts.push(format!("{} ASC", quote_ident(GROUPED_LIMIT_INDEX_COL)));
    Ok(parts.join(", "))
}

/// Render a constant VALUES relation to merge-engine SQL:
/// `SELECT * FROM (VALUES (..),(..)) AS v("n0","n1")`. Ports `_render_values_sql`;
/// a non-literal cell RAISES (`_values_cell`).
pub fn render_values_sql(rows: &[Vec<Expr>], names: &[String]) -> Result<String, StepError> {
    let mut tuples = Vec::with_capacity(rows.len());
    for row in rows {
        tuples.push(render_values_row(row)?);
    }
    let columns = quote_names(names);
    let sql = format!(
        "SELECT * FROM (VALUES {}) AS v({})",
        tuples.join(", "),
        columns
    );
    Ok(to_source_sql(&sql, Dialect::DuckDb)?)
}

/// One VALUES row rendered as `(c0, c1, ...)`; every cell must be a literal.
fn render_values_row(row: &[Expr]) -> Result<String, StepError> {
    let mut cells = Vec::with_capacity(row.len());
    for cell in row {
        if !matches!(cell, Expr::Literal { .. }) {
            return Err(StepError::NonLiteralValuesCell);
        }
        cells.push(render_expr(cell, &SourceResolver)?);
    }
    Ok(format!("({})", cells.join(", ")))
}

/// Render an optional predicate to canonical source SQL (columns keep their own
/// qualifier, resolved by the source resolver).
fn render_opt(expr: Option<&Expr>) -> Result<Option<String>, StepError> {
    match expr {
        None => Ok(None),
        Some(expr) => Ok(Some(render_expr(expr, &SourceResolver)?)),
    }
}

/// A schema name mapped to None when empty (a bare, registered relation name).
fn non_empty(schema: &str) -> Option<&str> {
    if schema.is_empty() {
        None
    } else {
        Some(schema)
    }
}

/// Quote each name and join with commas.
fn quote_names(names: &[String]) -> String {
    let mut parts = Vec::with_capacity(names.len());
    for name in names {
        parts.push(quote_ident(name));
    }
    parts.join(", ")
}
