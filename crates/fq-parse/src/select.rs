//! polyglot-sql `Select` -> `fq_plan::LogicalPlan`.
//!
//! Builds the plan clause by clause in the Python order: FROM -> WHERE -> GROUP
//! BY -> HAVING -> SELECT -> ORDER BY -> LIMIT. Unsupported clauses raise (the
//! defensive allowlist posture).
//!
//! Coverage (this stage): a single base table in FROM (joins are the next
//! increment), WHERE, GROUP BY + the five aggregates, HAVING, ORDER BY, LIMIT /
//! OFFSET. Stars, DISTINCT, CTEs, set operations, derived tables, and joins raise.

use fq_plan::expr::NullsOrder;
use fq_plan::{
    Aggregate, Expr, Filter, Join, JoinType, Limit, LogicalPlan, Projection, Scan, Sort,
};
use polyglot_sql::expressions::{Expression, JoinKind, Ordered, Select, TableRef};

use crate::error::ParseError;
use crate::expr::convert_expr;

/// Convert a SELECT into a logical plan.
pub fn convert_select(select: &Select) -> Result<LogicalPlan, ParseError> {
    reject_unsupported_clauses(select)?;
    let from = build_from(select)?;
    let filtered = apply_where(from, select)?;
    let projected = apply_projection(filtered, select)?;
    let sorted = apply_order_by(projected, select)?;
    apply_limit(sorted, select)
}

/// Raise on any clause this stage does not handle, so nothing is silently
/// dropped. Mirrors the Python `SUPPORTED_SELECT_ARGS` allowlist.
fn reject_unsupported_clauses(select: &Select) -> Result<(), ParseError> {
    let unsupported = [
        (select.distinct, "DISTINCT"),
        (select.distinct_on.is_some(), "DISTINCT ON"),
        (select.with.is_some(), "WITH (CTE)"),
        (select.qualify.is_some(), "QUALIFY"),
        (select.prewhere.is_some(), "PREWHERE"),
        (!select.lateral_views.is_empty(), "LATERAL VIEW"),
        (select.distribute_by.is_some(), "DISTRIBUTE BY"),
        (select.cluster_by.is_some(), "CLUSTER BY"),
        (select.sort_by.is_some(), "SORT BY"),
        (select.windows.is_some(), "WINDOW"),
        (select.sample.is_some(), "TABLESAMPLE"),
        (select.top.is_some(), "TOP"),
        (select.fetch.is_some(), "FETCH"),
        (select.limit_by.is_some(), "LIMIT BY"),
        (select.connect.is_some(), "CONNECT BY"),
        (select.into.is_some(), "SELECT INTO"),
    ];
    for (present, name) in unsupported {
        if present {
            return Err(ParseError::Unsupported(name.to_string()));
        }
    }
    Ok(())
}

/// Build the FROM tree: the base table then each JOIN folded left-deep over it.
/// A single non-table FROM item (derived table, VALUES) and comma joins raise for
/// now.
fn build_from(select: &Select) -> Result<LogicalPlan, ParseError> {
    let from = select
        .from
        .as_ref()
        .ok_or_else(|| ParseError::Unsupported("FROM-less SELECT".to_string()))?;
    let [only] = from.expressions.as_slice() else {
        return Err(ParseError::Unsupported("comma-joined FROM".to_string()));
    };
    let mut plan = scan_of(select, only)?;
    for join in &select.joins {
        plan = apply_join(select, plan, join)?;
    }
    Ok(plan)
}

/// A base-table FROM/JOIN item as a `Scan`. Derived tables and VALUES raise.
fn scan_of(select: &Select, item: &Expression) -> Result<LogicalPlan, ParseError> {
    let Expression::Table(table) = item else {
        return Err(ParseError::Unsupported(format!(
            "FROM/JOIN {}",
            item.variant_name()
        )));
    };
    Ok(LogicalPlan::Scan(Box::new(scan_from_table(select, table))))
}

/// Fold one JOIN onto the left plan.
fn apply_join(
    select: &Select,
    left: LogicalPlan,
    join: &polyglot_sql::expressions::Join,
) -> Result<LogicalPlan, ParseError> {
    let right = scan_of(select, &join.this)?;
    let (join_type, natural) = map_join_kind(join.kind)?;
    let condition = match &join.on {
        Some(on) => Some(convert_expr(on)?),
        None => None,
    };
    let using = if join.using.is_empty() {
        None
    } else {
        Some(join.using.iter().map(|ident| ident.name.clone()).collect())
    };
    Ok(LogicalPlan::Join(Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        condition,
        natural,
        using,
        estimated_rows: None,
        estimate_defaults: None,
    }))
}

/// Map a polyglot join kind to `(JoinType, natural)`; the exotic kinds (APPLY,
/// ASOF, STRAIGHT, LATERAL) raise for now.
fn map_join_kind(kind: JoinKind) -> Result<(JoinType, bool), ParseError> {
    let mapped = match kind {
        JoinKind::Inner => (JoinType::Inner, false),
        JoinKind::Left => (JoinType::Left, false),
        JoinKind::Right => (JoinType::Right, false),
        JoinKind::Full | JoinKind::Outer => (JoinType::Full, false),
        JoinKind::Cross => (JoinType::Cross, false),
        JoinKind::Semi | JoinKind::LeftSemi => (JoinType::Semi, false),
        JoinKind::Anti | JoinKind::LeftAnti => (JoinType::Anti, false),
        JoinKind::Natural => (JoinType::Inner, true),
        JoinKind::NaturalLeft => (JoinType::Left, true),
        JoinKind::NaturalRight => (JoinType::Right, true),
        JoinKind::NaturalFull => (JoinType::Full, true),
        other => return Err(ParseError::Unsupported(format!("{other:?} JOIN"))),
    };
    Ok(mapped)
}

/// Assemble a `Scan` for one table, over-collecting the column names referenced
/// for it (qualified by its alias/name, plus unqualified refs - the binder prunes
/// and expands against the catalog). `catalog`/`schema` default to the engine
/// conventions when the reference omits them.
fn scan_from_table(select: &Select, table: &TableRef) -> Scan {
    let datasource = table
        .catalog
        .as_ref()
        .map_or_else(String::new, |ident| ident.name.clone());
    let schema_name = table
        .schema
        .as_ref()
        .map_or_else(|| "public".to_string(), |ident| ident.name.clone());
    let alias = table.alias.as_ref().map(|ident| ident.name.clone());
    // The name a qualified reference uses for this table: its alias if any.
    let key = alias.clone().unwrap_or_else(|| table.name.name.clone());
    let mut scan = Scan::new(
        datasource,
        schema_name,
        table.name.name.clone(),
        columns_for_table(select, &key),
    );
    scan.alias = alias;
    scan
}

/// Every root expression that may reference columns (SELECT list, WHERE, GROUP
/// BY, HAVING, ORDER BY, and each JOIN condition).
fn column_roots(select: &Select) -> Vec<&Expression> {
    let mut roots: Vec<&Expression> = select.expressions.iter().collect();
    if let Some(where_clause) = &select.where_clause {
        roots.push(&where_clause.this);
    }
    if let Some(group_by) = &select.group_by {
        roots.extend(group_by.expressions.iter());
    }
    if let Some(having) = &select.having {
        roots.push(&having.this);
    }
    if let Some(order_by) = &select.order_by {
        for ordered in &order_by.expressions {
            roots.push(&ordered.this);
        }
    }
    for join in &select.joins {
        if let Some(on) = &join.on {
            roots.push(on);
        }
    }
    roots
}

/// Distinct column names referenced for a table `key`: those qualified by `key`
/// plus every unqualified reference (over-collection; the binder resolves which
/// table actually owns an unqualified column).
fn columns_for_table(select: &Select, key: &str) -> Vec<String> {
    let mut columns: Vec<String> = Vec::new();
    for root in column_roots(select) {
        for found in polyglot_sql::traversal::get_columns(root) {
            if let Expression::Column(column) = found {
                let matches = match &column.table {
                    Some(qualifier) => qualifier.name == key,
                    None => true,
                };
                if matches && !columns.contains(&column.name.name) {
                    columns.push(column.name.name.clone());
                }
            }
        }
    }
    columns
}

/// Wrap the input in a `Filter` for the WHERE clause, if present.
fn apply_where(input: LogicalPlan, select: &Select) -> Result<LogicalPlan, ParseError> {
    let Some(where_clause) = &select.where_clause else {
        return Ok(input);
    };
    Ok(LogicalPlan::Filter(Filter {
        input: Box::new(input),
        predicate: convert_expr(&where_clause.this)?,
    }))
}

/// Realize the SELECT list: an `Aggregate` when GROUP BY is present or any output
/// is an aggregate, else a `Projection`. HAVING wraps the aggregate.
fn apply_projection(input: LogicalPlan, select: &Select) -> Result<LogicalPlan, ParseError> {
    let mut expressions = Vec::with_capacity(select.expressions.len());
    let mut names = Vec::with_capacity(select.expressions.len());
    for (index, item) in select.expressions.iter().enumerate() {
        let (expr, name) = select_item(item, index)?;
        expressions.push(expr);
        names.push(name);
    }
    if select.group_by.is_some() || expressions.iter().any(is_aggregate_tree) {
        return build_aggregate(input, select, expressions, names);
    }
    Ok(LogicalPlan::Projection(Projection {
        input: Box::new(input),
        expressions,
        aliases: names,
        distinct: false,
        distinct_on: None,
    }))
}

/// Build the `Aggregate` node (+ a HAVING `Filter` when present). The aggregate's
/// `aggregates` list is the whole SELECT list (group columns and agg calls),
/// positionally aligned with `output_names`.
fn build_aggregate(
    input: LogicalPlan,
    select: &Select,
    expressions: Vec<Expr>,
    names: Vec<String>,
) -> Result<LogicalPlan, ParseError> {
    let mut group_by = Vec::new();
    if let Some(clause) = &select.group_by {
        for key in &clause.expressions {
            group_by.push(convert_expr(key)?);
        }
    }
    let aggregate = LogicalPlan::Aggregate(Aggregate {
        input: Box::new(input),
        group_by,
        aggregates: expressions,
        output_names: names,
        grouping_sets: None,
    });
    let Some(having) = &select.having else {
        return Ok(aggregate);
    };
    Ok(LogicalPlan::Filter(Filter {
        input: Box::new(aggregate),
        predicate: convert_expr(&having.this)?,
    }))
}

/// One SELECT-list item as `(expression, output_name)`. An alias names the
/// output; a bare column takes its own name; anything else gets a positional name
/// (the binder can refine unnamed outputs).
fn select_item(item: &Expression, index: usize) -> Result<(Expr, String), ParseError> {
    match item {
        Expression::Star(_) => Err(ParseError::Unsupported("SELECT *".to_string())),
        Expression::Alias(alias) => Ok((convert_expr(&alias.this)?, alias.alias.name.clone())),
        Expression::Column(column) => Ok((convert_expr(item)?, column.name.name.clone())),
        other => Ok((convert_expr(other)?, format!("col_{index}"))),
    }
}

/// Whether a converted expression tree contains an aggregate function call.
fn is_aggregate_tree(expr: &Expr) -> bool {
    fq_plan::contains_aggregate(expr)
}

/// Wrap the input in a `Sort` for the ORDER BY clause, if present.
fn apply_order_by(input: LogicalPlan, select: &Select) -> Result<LogicalPlan, ParseError> {
    let Some(order_by) = &select.order_by else {
        return Ok(input);
    };
    let mut sort_keys = Vec::with_capacity(order_by.expressions.len());
    let mut ascending = Vec::with_capacity(order_by.expressions.len());
    let mut nulls_order = Vec::with_capacity(order_by.expressions.len());
    for ordered in &order_by.expressions {
        sort_keys.push(convert_expr(&ordered.this)?);
        ascending.push(!ordered.desc);
        nulls_order.push(nulls_of(ordered));
    }
    Ok(LogicalPlan::Sort(Sort {
        input: Box::new(input),
        sort_keys,
        ascending,
        nulls_order: Some(nulls_order),
    }))
}

/// The NULLS FIRST/LAST of an ORDER BY key, or None when unspecified.
fn nulls_of(ordered: &Ordered) -> Option<NullsOrder> {
    ordered.nulls_first.map(|first| {
        if first {
            NullsOrder::First
        } else {
            NullsOrder::Last
        }
    })
}

/// Wrap the input in a `Limit` for LIMIT and/or OFFSET, if present.
fn apply_limit(input: LogicalPlan, select: &Select) -> Result<LogicalPlan, ParseError> {
    let limit = match &select.limit {
        Some(clause) => Some(limit_value(&clause.this)?),
        None => None,
    };
    let offset = match &select.offset {
        Some(clause) => limit_value(&clause.this)?,
        None => 0,
    };
    if limit.is_none() && offset == 0 {
        return Ok(input);
    }
    Ok(LogicalPlan::Limit(Limit {
        input: Box::new(input),
        limit,
        offset,
    }))
}

/// Read a LIMIT/OFFSET count as a non-negative integer literal.
fn limit_value(expr: &Expression) -> Result<u64, ParseError> {
    if let Expression::Literal(literal) = expr {
        if let polyglot_sql::expressions::Literal::Number(text) = literal.as_ref() {
            if let Ok(value) = text.parse::<u64>() {
                return Ok(value);
            }
        }
    }
    Err(ParseError::Unsupported(
        "non-integer LIMIT/OFFSET".to_string(),
    ))
}
