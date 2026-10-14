//! Bind typed struct access and list expansion against catalog metadata.

use fq_catalog::{Column, Table};
use fq_common::DataType;
use fq_plan::{Expr, LiteralValue, LogicalPlan, Unnest};

use crate::binder::Binder;
use crate::error::BindError;

/// Reject a relation alias that would make parent and element references ambiguous.
pub(crate) fn require_unique_alias(
    alias: &str,
    scope: &[(String, Table)],
) -> Result<(), BindError> {
    if scope
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case(alias))
    {
        return Err(BindError::Unsupported(format!(
            "duplicate relation alias {alias}"
        )));
    }
    Ok(())
}

impl Binder<'_> {
    /// Bind list arguments against the parent scope and validate all output names.
    pub(crate) fn bind_unnest(&mut self, mut node: Unnest) -> Result<LogicalPlan, BindError> {
        node.input = Box::new(self.bind(*node.input)?);
        node.arguments = if node.retain_input {
            self.with_scope(&node.input, |binder| binder.bind_expr_list(&node.arguments))?
        } else {
            self.bind_expr_list(&node.arguments)?
        };
        for argument in &mut node.arguments {
            normalize_null_list(argument);
        }
        validate_unnest(&node)?;
        let predicate = node.predicate.take();
        let mut plan = LogicalPlan::Unnest(node);
        bind_expansion_predicate(self, &mut plan, predicate)?;
        Ok(plan)
    }
}

/// DuckDB expands an untyped NULL as an empty INTEGER list with no output rows.
fn normalize_null_list(argument: &mut Expr) {
    if !matches!(
        argument,
        Expr::Literal {
            value: LiteralValue::Null,
            ..
        }
    ) {
        return;
    }
    // This is a new typed empty-array expression replacing an untyped NULL input.
    // Every FunctionCall field is supplied, with no elements or aggregate modifiers.
    *argument = Expr::FunctionCall {
        function_name: "MAKE_ARRAY".to_string(),
        args: Vec::new(),
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
        filter: None,
    };
}

/// Validate argument arity, element types, and the output relation's unique names.
fn validate_unnest(node: &Unnest) -> Result<(), BindError> {
    if node.arguments.is_empty() || node.arguments.len() != node.output_names.len() {
        return Err(BindError::Unsupported(
            "UNNEST argument/output arity mismatch".to_string(),
        ));
    }
    for argument in &node.arguments {
        require_list(argument)?;
    }
    require_output_names(node.output_names.iter().chain(node.ordinality.iter()))
}

/// Reject duplicate element and ordinal names in one expansion relation.
fn require_output_names<'a>(outputs: impl Iterator<Item = &'a String>) -> Result<(), BindError> {
    let mut names = std::collections::HashSet::new();
    for name in outputs {
        if !names.insert(name.to_lowercase()) {
            return Err(BindError::Unsupported(format!(
                "duplicate UNNEST output {name}"
            )));
        }
    }
    Ok(())
}

/// Reject scalar inputs before execution can manufacture an expansion result.
fn require_list(argument: &Expr) -> Result<(), BindError> {
    let data_type = argument.get_type();
    if data_type.list_element().is_none() {
        return Err(BindError::Unsupported(format!(
            "UNNEST requires a list, got {data_type}"
        )));
    }
    Ok(())
}

/// Build the expansion relation from its validated list element types.
pub(crate) fn unnest_table(node: &Unnest) -> Table {
    let mut columns = Vec::new();
    for (argument, name) in node.arguments.iter().zip(&node.output_names) {
        let data_type = argument
            .get_type()
            .list_element()
            .expect("bound UNNEST list");
        columns.push(Column::new(name.clone(), data_type, true));
    }
    if let Some(name) = &node.ordinality {
        columns.push(Column::new(name.clone(), DataType::BigInt, node.outer));
    }
    Table::new(node.alias.clone(), columns)
}

/// Validate structured access and canonicalize the actual field spelling.
pub(crate) fn validate_nested_call(name: &str, args: &mut [Expr]) -> Result<(), BindError> {
    match name.to_uppercase().as_str() {
        "STRUCT_EXTRACT" => validate_field(args),
        "MAKE_ARRAY" => validate_array(args),
        "__FEDQ_UNNEST" => Err(BindError::Unsupported(
            "UNNEST is only valid in SELECT or FROM".to_string(),
        )),
        _ => Ok(()),
    }
}

/// Require a known struct field and a constant field name.
fn validate_field(args: &mut [Expr]) -> Result<(), BindError> {
    if args.len() != 2 {
        return Err(BindError::Unsupported(
            "STRUCT_EXTRACT requires two arguments".to_string(),
        ));
    }
    let name = field_name(&args[1])?;
    let data_type = args[0].get_type();
    let Some((stored_name, _)) = data_type.struct_field(name) else {
        return Err(BindError::Unsupported(format!(
            "field {name} does not exist in {data_type}"
        )));
    };
    let Expr::Literal { value, .. } = &mut args[1] else {
        unreachable!("field literal");
    };
    *value = LiteralValue::String(stored_name);
    Ok(())
}

/// Structured field names are literal strings, never runtime values.
fn field_name(expression: &Expr) -> Result<&str, BindError> {
    match expression {
        Expr::Literal {
            value: LiteralValue::String(name),
            ..
        } => Ok(name),
        _ => Err(BindError::Unsupported(
            "structured field name must be a string literal".to_string(),
        )),
    }
}

/// Reject incompatible array elements rather than giving a guessed list type.
fn validate_array(args: &[Expr]) -> Result<(), BindError> {
    let mut element = DataType::Null;
    for argument in args {
        element = compatible_element(element, argument.get_type())?;
    }
    Ok(())
}

/// Null elements are neutral; equal types and numeric scalar types can share a list.
fn compatible_element(left: DataType, right: DataType) -> Result<DataType, BindError> {
    if left == DataType::Null {
        return Ok(right);
    }
    if right == DataType::Null {
        return Ok(left);
    }
    compatible_known_elements(left, &right)
}

/// Equal types and scalar numbers share an array; other combinations raise.
fn compatible_known_elements(left: DataType, right: &DataType) -> Result<DataType, BindError> {
    if &left == right {
        return Ok(left);
    }
    if numeric(&left) && numeric(right) {
        return Ok(left);
    }
    Err(BindError::Unsupported(format!(
        "incompatible array elements: {left} and {right}"
    )))
}

/// Identify the scalar numeric types that array construction can widen safely.
fn numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Integer
            | DataType::BigInt
            | DataType::Float
            | DataType::Double
            | DataType::Decimal
    )
}

/// Construct validated field access from a resolved, qualified struct column.
pub(crate) fn field_access(source: Expr, name: &str) -> Result<Expr, BindError> {
    let mut args = vec![source, field_literal(name)];
    validate_field(&mut args)?;
    // This is a new field-access call built from a resolved column and a field name.
    // Every FunctionCall field is supplied; structured access is a plain scalar call.
    Ok(Expr::FunctionCall {
        function_name: "STRUCT_EXTRACT".to_string(),
        args,
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
        filter: None,
    })
}

/// Construct a constant name for a structured field selected during resolution.
fn field_literal(name: &str) -> Expr {
    // This is a new constant derived from the unresolved field's spelling.
    // Both Literal fields are supplied: the name and its scalar string type.
    Expr::Literal {
        value: LiteralValue::String(name.to_string()),
        data_type: DataType::Varchar,
    }
}

/// Bind the matching predicate against both retained parents and expansion outputs.
fn bind_expansion_predicate(
    binder: &mut Binder<'_>,
    plan: &mut LogicalPlan,
    predicate: Option<Expr>,
) -> Result<(), BindError> {
    let Some(predicate) = predicate else {
        return Ok(());
    };
    let bound = binder.with_scope(plan, |binder| binder.bind_expr(&predicate))?;
    let LogicalPlan::Unnest(node) = plan else {
        unreachable!("expansion predicate");
    };
    node.predicate = Some(bound);
    Ok(())
}
