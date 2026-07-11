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
use fq_plan::{Aggregate, Expr, Filter, Limit, LogicalPlan, Projection, Scan, Sort};
use polyglot_sql::expressions::{Expression, Ordered, Select, TableRef};

use crate::error::ParseError;
use crate::expr::convert_expr;

/// Convert a SELECT into a logical plan.
pub fn convert_select(select: &Select) -> Result<LogicalPlan, ParseError> {
    reject_unsupported_clauses(select)?;
    let scan = build_scan(select)?;
    let filtered = apply_where(scan, select)?;
    let projected = apply_projection(filtered, select)?;
    let sorted = apply_order_by(projected, select)?;
    apply_limit(sorted, select)
}

/// Raise on any clause this stage does not handle, so nothing is silently
/// dropped. Mirrors the Python `SUPPORTED_SELECT_ARGS` allowlist.
fn reject_unsupported_clauses(select: &Select) -> Result<(), ParseError> {
    let unsupported = [
        (!select.joins.is_empty(), "JOIN"),
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

/// Build the base `Scan` from a single-table FROM. Multiple FROM items (comma
/// joins) and non-table FROM items (derived tables, VALUES) raise for now.
fn build_scan(select: &Select) -> Result<LogicalPlan, ParseError> {
    let from = select
        .from
        .as_ref()
        .ok_or_else(|| ParseError::Unsupported("FROM-less SELECT".to_string()))?;
    let [only] = from.expressions.as_slice() else {
        return Err(ParseError::Unsupported("comma-joined FROM".to_string()));
    };
    let Expression::Table(table) = only else {
        return Err(ParseError::Unsupported(format!(
            "FROM {}",
            only.variant_name()
        )));
    };
    Ok(LogicalPlan::Scan(Box::new(scan_from_table(select, table))))
}

/// Assemble a `Scan` for one table, over-collecting the referenced column names
/// (the binder prunes/expands them against the catalog). `catalog`/`schema`
/// default to the engine conventions when the reference omits them.
fn scan_from_table(select: &Select, table: &TableRef) -> Scan {
    let datasource = table
        .catalog
        .as_ref()
        .map_or_else(String::new, |ident| ident.name.clone());
    let schema_name = table
        .schema
        .as_ref()
        .map_or_else(|| "public".to_string(), |ident| ident.name.clone());
    let mut scan = Scan::new(
        datasource,
        schema_name,
        table.name.name.clone(),
        referenced_columns(select),
    );
    scan.alias = table.alias.as_ref().map(|ident| ident.name.clone());
    scan
}

/// Distinct column names referenced anywhere in the SELECT (over-collection; the
/// binder prunes/expands against the catalog).
fn referenced_columns(select: &Select) -> Vec<String> {
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
    let mut columns: Vec<String> = Vec::new();
    for root in roots {
        for found in polyglot_sql::traversal::get_columns(root) {
            if let Expression::Column(column) = found {
                if !columns.contains(&column.name.name) {
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
