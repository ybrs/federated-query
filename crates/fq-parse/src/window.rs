//! Named WINDOW clause inlining: rewrite each `OVER w` / `OVER (w ...)` reference
//! into its full window specification before conversion, so the converter only
//! ever sees inline windows. A named window is scoped to the query block whose
//! WINDOW clause defines it.

use std::collections::HashMap;

use polyglot_sql::expressions::{Expression, Over, Select};
use polyglot_sql::traversal::transform;

use crate::error::ParseError;

/// Inline every named-window reference in `statement`. Each SELECT block resolves
/// its `OVER w` references against its own WINDOW clause; an `OVER w` with no
/// matching definition is left intact and rejected later by the converter.
pub(crate) fn inline_named_windows(statement: Expression) -> Result<Expression, ParseError> {
    transform(statement, &|node| match node {
        Expression::Select(select) => resolve_block(*select).map(Some),
        other => Ok(Some(other)),
    })
    .map_err(|error| ParseError::Parse(format!("{error:?}")))
}

/// Resolve one SELECT block's named-window references. Builds the block's window
/// map from its WINDOW clause, rewrites each reference in the projection and
/// QUALIFY, then drops the (now-consumed) WINDOW clause.
fn resolve_block(mut select: Select) -> polyglot_sql::Result<Expression> {
    let named_windows = window_map(&select);
    if named_windows.is_empty() {
        return Ok(Expression::Select(Box::new(select)));
    }
    let projection = std::mem::take(&mut select.expressions);
    select.expressions = resolve_list(projection, &named_windows)?;
    if let Some(mut qualify) = select.qualify.take() {
        qualify.this = resolve_refs(qualify.this, &named_windows)?;
        select.qualify = Some(qualify);
    }
    select.windows = None;
    Ok(Expression::Select(Box::new(select)))
}

/// The block's `window name -> specification` map, lowercased for lookup.
fn window_map(select: &Select) -> HashMap<String, Over> {
    let mut map = HashMap::new();
    if let Some(windows) = &select.windows {
        for named in windows {
            map.insert(named.name.name.to_lowercase(), named.spec.clone());
        }
    }
    map
}

/// Resolve window references in a list of projection expressions.
fn resolve_list(
    expressions: Vec<Expression>,
    named_windows: &HashMap<String, Over>,
) -> polyglot_sql::Result<Vec<Expression>> {
    let mut resolved = Vec::with_capacity(expressions.len());
    for expression in expressions {
        resolved.push(resolve_refs(expression, named_windows)?);
    }
    Ok(resolved)
}

/// Rewrite every `OVER w` reference in one expression tree into its definition.
fn resolve_refs(
    expression: Expression,
    named_windows: &HashMap<String, Over>,
) -> polyglot_sql::Result<Expression> {
    transform(expression, &|node| {
        if let Expression::WindowFunction(mut window) = node {
            inline_reference(&mut window.over, named_windows);
            return Ok(Some(Expression::WindowFunction(window)));
        }
        Ok(Some(node))
    })
}

/// If `over` names a window defined in `named_windows`, merge the definition into
/// it and clear the name; an unknown name is left for the converter to reject.
fn inline_reference(over: &mut Over, named_windows: &HashMap<String, Over>) {
    let Some(name) = &over.window_name else {
        return;
    };
    let Some(spec) = named_windows.get(&name.name.to_lowercase()) else {
        return;
    };
    *over = merge_over(spec, over);
}

/// Merge a named-window definition `spec` with a `reference` that names it. The
/// definition supplies PARTITION BY; the reference may add ORDER BY and a frame
/// (SQL forbids a reference from repartitioning), so its ORDER BY/frame win when
/// present.
fn merge_over(spec: &Over, reference: &Over) -> Over {
    let partition_by = if spec.partition_by.is_empty() {
        reference.partition_by.clone()
    } else {
        spec.partition_by.clone()
    };
    let order_by = if reference.order_by.is_empty() {
        spec.order_by.clone()
    } else {
        reference.order_by.clone()
    };
    // Fresh merged OVER built from the definition + reference - no `..base`, so a
    // new field on Over surfaces here. Field list (window_name/partition_by/
    // order_by/frame/alias) is the complete Over struct.
    Over {
        window_name: None,
        partition_by,
        order_by,
        frame: reference.frame.clone().or_else(|| spec.frame.clone()),
        alias: reference.alias.clone(),
    }
}
