//! Per-node SQL renderers, composing fq-emit's clause builders. Ports the render
//! methods dropped from `fq-plan`'s physical.rs (deferred here per the fq-emit lib
//! note): a source scan's SELECT (`PhysicalScan._build_ast` / `_render_source_sql`),
//! and the merge-engine raw-SQL renderers (`_aggregate_sql`, `_window_sql`,
//! `_grouped_limit_sql`, `_render_values_sql`).
//!
//! DIALECT: a scan renders to its SOURCE dialect (from the scan's
//! `datasource_kind`); the merge raw-SQL strings render to `DuckDb` (the merge
//! engine's dialect, matching the Python `dialect="duckdb"`).

use fq_emit::clauses::{
    assemble_select, group_by, order_by, select_list, set_op_keyword, SelectPieces,
};
use fq_emit::resolver::{AliasMap, MergeResolver};
use fq_emit::{quote_ident, render_expr, to_source_sql, Dialect, SourceResolver};
use fq_plan::expr::{aggregate_output_map, split_where_having};
use fq_plan::physical::{
    ColumnAliasMap, DatasourceKind, PhysicalGroupedLimit, PhysicalHashAggregate, PhysicalPlan,
    PhysicalRemoteSetOp, PhysicalScan, PhysicalWindow,
};
use fq_plan::Expr;

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
    let resolver = merge_resolver(aliases);
    let items = select_list(&node.aggregates, &node.output_names, &resolver)?;
    let mut sql = format!("SELECT {items} FROM {MERGE_AGGREGATE_RELATION}");
    if let Some(group) = group_by(&node.group_by, node.grouping_sets.as_deref(), &resolver)? {
        sql = format!("{sql} GROUP BY {group}");
    }
    Ok(to_source_sql(&sql, Dialect::DuckDb)?)
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
