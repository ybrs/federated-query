//! Compose typed list expansion on DataFusion's existing UNNEST operator.

use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::prelude::{col, lit, when};

use crate::core::expr::{scalar_function, to_df_expr};
use crate::core::ir::Projection;

/// Expand prepared list columns, retaining the input columns and ordinality.
fn plain_expansion(
    frame: DataFrame,
    arguments: &[Projection],
    outer: bool,
    ordinality: Option<&str>,
    retain_input: bool,
) -> Result<DataFrame, DataFusionError> {
    let (arguments, input_ordinal, outputs) = private_arguments(&frame, arguments, ordinality);
    let prepared = prepare_arguments(frame, &arguments)?;
    let length = longest_list(&prepared, &arguments)?;
    let (prepared, inputs) = add_ordinality(
        prepared,
        &arguments,
        length,
        outer,
        input_ordinal.as_deref(),
    )?;
    let prepared = preserve_outer(prepared, &arguments, outer)?;
    let expanded = crate::list_expansion::expand(prepared, &inputs, outputs.clone())?;
    if retain_input {
        return Ok(expanded);
    }
    expanded.select(outputs.iter().map(|name| physical_column(name)))
}

/// Compute each argument once and assign its unique physical output name.
fn prepare_arguments(
    frame: DataFrame,
    arguments: &[Projection],
) -> Result<DataFrame, DataFusionError> {
    let mut expressions = input_columns(&frame);
    for argument in arguments {
        expressions.push(to_df_expr(&argument.expr)?.alias(&argument.alias));
    }
    frame.select(expressions)
}

/// Preserve the exact physical input-column names, including names with dots.
fn input_columns(frame: &DataFrame) -> Vec<Expr> {
    let mut expressions = Vec::new();
    for (qualifier, field) in frame.schema().iter() {
        expressions.push(Expr::Column(datafusion::common::Column::new(
            qualifier.cloned(),
            field.name(),
        )));
    }
    expressions
}

/// Compute the zip expansion length, treating absent lists as zero-length.
fn longest_list(frame: &DataFrame, arguments: &[Projection]) -> Result<Expr, DataFusionError> {
    let mut lengths = vec![lit(0_i64)];
    for argument in arguments {
        let length = scalar_function("array_length", vec![col(&argument.alias)])?;
        let length = length.cast_to(&arrow_schema::DataType::Int64, frame.schema())?;
        lengths.push(scalar_function("coalesce", vec![length, lit(0_i64)])?);
    }
    scalar_function("greatest", lengths)
}

/// Replace absent outer inputs with one typed null while preserving nonempty lists.
fn preserve_outer(
    frame: DataFrame,
    arguments: &[Projection],
    outer: bool,
) -> Result<DataFrame, DataFusionError> {
    if !outer {
        return Ok(frame);
    }
    let mut expressions = input_columns(&frame);
    for argument in arguments {
        let replacement = outer_list(&frame, &argument.alias)?;
        let index = frame
            .schema()
            .index_of_column_by_name(None, &argument.alias)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("missing prepared list {}", argument.alias))
            })?;
        expressions[index] = replacement.alias(&argument.alias);
    }
    frame.select(expressions)
}

/// Construct one null element with the original list's element type.
fn outer_list(frame: &DataFrame, name: &str) -> Result<Expr, DataFusionError> {
    let value = col(name);
    let data_type = value.get_type(frame.schema())?;
    let element = list_element(&data_type)?;
    let null = lit(ScalarValue::try_new_null(element)?);
    let singleton = scalar_function("make_array", vec![null])?;
    let length = scalar_function("array_length", vec![value.clone()])?;
    let empty = scalar_function("coalesce", vec![length, lit(0_i64)])?.eq(lit(0_i64));
    when(empty, singleton).otherwise(value)
}

/// Reject a scalar argument before building an UNNEST plan.
fn list_element(
    data_type: &arrow_schema::DataType,
) -> Result<&arrow_schema::DataType, DataFusionError> {
    match data_type {
        arrow_schema::DataType::List(field)
        | arrow_schema::DataType::LargeList(field)
        | arrow_schema::DataType::FixedSizeList(field, _) => Ok(field.data_type()),
        other => Err(DataFusionError::Plan(format!(
            "UNNEST requires a list, got {other}"
        ))),
    }
}

/// Add positions to the same zip expansion as the list values.
fn add_ordinality(
    frame: DataFrame,
    arguments: &[Projection],
    length: Expr,
    outer: bool,
    ordinality: Option<&str>,
) -> Result<(DataFrame, Vec<String>), DataFusionError> {
    let mut names = Vec::new();
    for argument in arguments {
        names.push(argument.alias.clone());
    }
    let Some(name) = ordinality else {
        return Ok((frame, names));
    };
    let mut expressions = input_columns(&frame);
    expressions.push(ordinal_list(length, outer)?.alias(name));
    names.push(name.to_string());
    Ok((frame.select(expressions)?, names))
}

/// Give preserved empty outer inputs null ordinality, otherwise positions start at one.
fn ordinal_list(length: Expr, outer: bool) -> Result<Expr, DataFusionError> {
    let range = scalar_function(
        "range",
        vec![lit(1_i64), length.clone() + lit(1_i64), lit(1_i64)],
    )?;
    if !outer {
        return Ok(range);
    }
    let singleton = scalar_function("make_array", vec![lit(ScalarValue::Int64(None))])?;
    when(length.eq(lit(0_i64)), singleton).otherwise(range)
}
/// Match expanded rows while retaining parents whose lists have no matching element.
pub(crate) async fn expand_lists(
    frame: DataFrame,
    arguments: &[Projection],
    outer: bool,
    ordinality: Option<&str>,
    retain_input: bool,
    predicate: Option<&crate::core::ir::IrExpr>,
) -> Result<DataFrame, DataFusionError> {
    let Some(predicate) = predicate else {
        return plain_expansion(frame, arguments, outer, ordinality, retain_input);
    };
    if !outer || !retain_input {
        return Err(DataFusionError::Plan(
            "matching expansion requires retained outer input".to_string(),
        ));
    }
    match_outer(frame, arguments, ordinality, predicate).await
}

/// Materialize stable parent identities before expansion can multiply or remove rows.
async fn match_outer(
    frame: DataFrame,
    arguments: &[Projection],
    ordinality: Option<&str>,
    predicate: &crate::core::ir::IrExpr,
) -> Result<DataFrame, DataFusionError> {
    let key = parent_key(&frame, arguments, ordinality);
    let mut outputs = physical_columns(&frame);
    let numbered =
        frame.with_column(&key, datafusion::functions_window::row_number::row_number())?;
    let cached = numbered.cache().await?.alias("in_0")?;
    let right_key = parent_key(&cached, arguments, ordinality);
    let expanded =
        plain_expansion(cached.clone(), arguments, false, ordinality, true)?.alias("in_0")?;
    let matched = expanded.filter(to_df_expr(predicate)?)?;
    let matched =
        expansion_matches(matched, &key, &right_key, arguments, ordinality)?.alias("matches")?;
    outputs.extend(expansion_columns(arguments, ordinality));
    join_outer_matches(cached, matched, &key, &right_key)?.select(outputs)
}

/// Join successful expansion values back to the stable, distinct parent relation.
fn join_outer_matches(
    parents: DataFrame,
    matched: DataFrame,
    key: &str,
    right_key: &str,
) -> Result<DataFrame, DataFusionError> {
    parents.alias("parents")?.join(
        matched,
        datafusion::logical_expr::JoinType::Left,
        &[key],
        &[right_key],
        None,
    )
}

/// Allocate a parent identity outside the input and expansion column namespaces.
fn parent_key(frame: &DataFrame, arguments: &[Projection], ordinality: Option<&str>) -> String {
    let mut used = std::collections::HashSet::new();
    for (_, field) in frame.schema().iter() {
        used.insert(field.name().clone());
    }
    for argument in arguments {
        used.insert(argument.alias.clone());
    }
    used.extend(ordinality.map(str::to_string));
    (0..=used.len())
        .map(|index| format!("__fedq_parent_{index}"))
        .find(|name| !used.contains(name))
        .expect("more candidates than existing columns")
}

/// Read physical names exactly, without interpreting dots as SQL qualification.
fn physical_columns(frame: &DataFrame) -> Vec<Expr> {
    let mut columns = Vec::new();
    for (_, field) in frame.schema().iter() {
        columns.push(physical_column(field.name()));
    }
    columns
}

/// Build a column reference to one exact physical field name.
fn physical_column(name: &str) -> Expr {
    Expr::Column(datafusion::common::Column::new_unqualified(name))
}

/// Project only expansion values and the matching parent key from successful rows.
fn expansion_matches(
    frame: DataFrame,
    key: &str,
    right_key: &str,
    arguments: &[Projection],
    ordinality: Option<&str>,
) -> Result<DataFrame, DataFusionError> {
    let mut expressions = vec![physical_column(key).alias(right_key)];
    expressions.extend(expansion_columns(arguments, ordinality));
    frame.select(expressions)
}

/// Name each element output and its optional original-list position.
fn expansion_columns(arguments: &[Projection], ordinality: Option<&str>) -> Vec<Expr> {
    let mut columns = Vec::new();
    for argument in arguments {
        columns.push(physical_column(&argument.alias));
    }
    if let Some(name) = ordinality {
        columns.push(physical_column(name));
    }
    columns
}

/// Name list inputs separately from element outputs so field rewrites cannot cross expansion.
fn private_arguments(
    frame: &DataFrame,
    arguments: &[Projection],
    ordinality: Option<&str>,
) -> (Vec<Projection>, Option<String>, Vec<String>) {
    let mut used = std::collections::HashSet::new();
    for (_, field) in frame.schema().iter() {
        used.insert(field.name().clone());
    }
    let mut outputs = Vec::new();
    for argument in arguments {
        outputs.push(argument.alias.clone());
        used.insert(argument.alias.clone());
    }
    outputs.extend(ordinality.map(str::to_string));
    used.extend(ordinality.map(str::to_string));
    let mut prepared = arguments.to_vec();
    for argument in &mut prepared {
        argument.alias = private_name(&mut used);
    }
    let ordinal = ordinality.map(|_| private_name(&mut used));
    (prepared, ordinal, outputs)
}

/// Allocate a physical list name outside the parent and element namespaces.
fn private_name(used: &mut std::collections::HashSet<String>) -> String {
    let name = (0..=used.len())
        .map(|index| format!("__fedq_list_{index}"))
        .find(|name| !used.contains(name))
        .expect("more candidates than existing columns");
    used.insert(name.clone());
    name
}
