//! Convert list expansion and structured value access into typed plan inputs.

use fq_plan::{ColumnRef, Expr, Filter, JoinType, LogicalPlan, Projection, Unnest, Values};
use polyglot_sql::expressions::{Expression, Identifier, UnnestFunc};

use crate::convert::{ColumnSource, Converter, FromTable};
use crate::error::ParseError;
use crate::expr::scalar_function_call;

/// Identify an UNNEST FROM item together with its explicit output names.
pub(crate) fn unnest_item(item: &Expression) -> Option<(&UnnestFunc, Option<&str>, &[Identifier])> {
    match item {
        Expression::Unnest(function) => Some((
            function,
            function.alias.as_ref().map(|alias| alias.name.as_str()),
            &[],
        )),
        Expression::Alias(alias) => {
            let Expression::Unnest(function) = &alias.this else {
                return None;
            };
            Some((function, Some(&alias.alias.name), &alias.column_aliases))
        }
        _ => None,
    }
}

impl Converter<'_> {
    /// Expand a FROM-less SELECT over one seed row without exposing seed columns.
    pub(crate) fn fromless_unnest(
        &self,
        expressions: Vec<Expr>,
        aliases: Vec<String>,
        select: &polyglot_sql::expressions::Select,
    ) -> Result<LogicalPlan, ParseError> {
        require_constant_expansion(&expressions)?;
        let (mut input, expressions) = self.expand_select_unnests(unnest_seed(), expressions)?;
        let LogicalPlan::Unnest(expansion) = &mut input else {
            unreachable!("SELECT expansion");
        };
        expansion.retain_input = false;
        // This is a new projection realizing the parsed FROM-less SELECT list.
        // All fields are supplied; its only input relation is the list expansion.
        Ok(LogicalPlan::Projection(Projection {
            input: Box::new(input),
            expressions,
            aliases,
            distinct: select.distinct,
            distinct_on: self.distinct_on(select)?,
        }))
    }

    /// Fold a supported UNNEST join onto its parent relation.
    pub(crate) fn apply_unnest_join(
        &self,
        left: LogicalPlan,
        join: &polyglot_sql::expressions::Join,
        tables: &mut Vec<FromTable>,
    ) -> Result<LogicalPlan, ParseError> {
        let (kind, natural) = crate::convert::map_join_kind(join.kind)?;
        require_join_modifiers(join, natural)?;
        if !matches!(kind, JoinType::Inner | JoinType::Cross | JoinType::Left) {
            return Err(ParseError::Unsupported(
                "UNNEST requires an INNER, CROSS, or LEFT join".to_string(),
            ));
        }
        let outer = kind == JoinType::Left;
        let expanded = self.unnest_from(left, &join.this, outer, true, tables)?;
        self.apply_unnest_condition(expanded, join.on.as_ref(), outer)
    }

    /// Keep outer matching inside expansion; inner matching can use a normal filter.
    fn apply_unnest_condition(
        &self,
        mut expanded: LogicalPlan,
        predicate: Option<&Expression>,
        outer: bool,
    ) -> Result<LogicalPlan, ParseError> {
        let Some(predicate) = predicate else {
            return Ok(expanded);
        };
        if matches!(predicate, Expression::Boolean(value) if value.value) {
            return Ok(expanded);
        }
        if outer {
            let LogicalPlan::Unnest(node) = &mut expanded else {
                unreachable!("outer expansion");
            };
            node.predicate = Some(self.expr(predicate)?);
            return Ok(expanded);
        }
        // This is a new filter over the expanded relation from the JOIN predicate.
        // Both fields are supplied so the parsed predicate is never dropped.
        Ok(LogicalPlan::Filter(Filter {
            input: Box::new(expanded),
            predicate: self.expr(predicate)?,
        }))
    }
    /// Allocate a synthetic relation name unique within the converted statement.
    pub(crate) fn unnest_alias(&self, input: &LogicalPlan) -> String {
        let next = self.unnest_counter.get();
        let width = relation_count(input);
        let index = (next..=next + width)
            .find(|index| !has_relation(input, &format!("__fedq_unnest_{index}")))
            .expect("more candidates than relations");
        self.unnest_counter.set(index + 1);
        format!("__fedq_unnest_{index}")
    }

    /// Preserve the parsed list argument; expansion is introduced by the SELECT pipeline.
    pub(crate) fn unnest_expression(&self, function: &UnnestFunc) -> Result<Expr, ParseError> {
        if function.with_ordinality
            || function.offset_alias.is_some()
            || !function.expressions.is_empty()
        {
            return Err(ParseError::Unsupported(
                "SELECT UNNEST takes one list argument".to_string(),
            ));
        }
        Ok(scalar_function_call(
            "__fedq_unnest".to_string(),
            vec![self.expr(&function.this)?],
        ))
    }

    /// Expand SELECT-list calls together, so unequal lists zip with null padding.
    pub(crate) fn expand_select_unnests(
        &self,
        input: LogicalPlan,
        expressions: Vec<Expr>,
    ) -> Result<(LogicalPlan, Vec<Expr>), ParseError> {
        let alias = self.unnest_alias(&input);
        let mut arguments = Vec::new();
        let mut converted = Vec::new();
        for expression in expressions {
            converted.push(extract_unnest(expression, &alias, &mut arguments)?);
        }
        if arguments.is_empty() {
            return Ok((input, converted));
        }
        let names = expansion_names(arguments.len());
        Ok((
            new_unnest(input, arguments, names, alias, false, None, true),
            converted,
        ))
    }

    /// Expand a correlated FROM list, retaining its parent relation when present.
    pub(crate) fn unnest_from(
        &self,
        input: LogicalPlan,
        item: &Expression,
        outer: bool,
        retain_input: bool,
        tables: &mut Vec<FromTable>,
    ) -> Result<LogicalPlan, ParseError> {
        let (function, alias, columns) = unnest_item(item).expect("UNNEST item");
        validate_from_unnest(function)?;
        let alias = alias.unwrap_or("unnest").to_string();
        let (names, ordinality) = from_names(columns, function.with_ordinality)?;
        register_expansion(tables, &alias, &names, ordinality.as_deref());
        let arguments = vec![self.expr(&function.this)?];
        Ok(new_unnest(
            input,
            arguments,
            names,
            alias,
            outer,
            ordinality,
            retain_input,
        ))
    }
}

/// Reject join modifiers that have no meaning in the list expansion operator.
fn require_join_modifiers(
    join: &polyglot_sql::expressions::Join,
    natural: bool,
) -> Result<(), ParseError> {
    let modifiers = [
        natural,
        !join.using.is_empty(),
        !join.pivots.is_empty(),
        join.match_condition.is_some(),
        join.join_hint.is_some(),
    ];
    if modifiers.into_iter().any(std::convert::identity) {
        return Err(ParseError::Unsupported("UNNEST join modifier".to_string()));
    }
    Ok(())
}

/// Reject source column references in a standalone expansion before creating its seed.
fn require_constant_expansion(expressions: &[Expr]) -> Result<(), ParseError> {
    for expression in expressions {
        if !fq_plan::column_refs(expression).is_empty() {
            return Err(ParseError::Unsupported(
                "column reference without FROM".to_string(),
            ));
        }
    }
    Ok(())
}

/// Bound the internal-name search by the number of nodes that can name relations.
fn relation_count(plan: &LogicalPlan) -> usize {
    1 + plan
        .children()
        .iter()
        .map(|child| relation_count(child))
        .sum::<usize>()
}

/// Check all named input relations before allocating an internal expansion alias.
fn has_relation(plan: &LogicalPlan, name: &str) -> bool {
    let local = match plan {
        LogicalPlan::Scan(node) => node.alias.as_deref().unwrap_or(&node.table_name),
        LogicalPlan::SubqueryScan(node) => &node.alias,
        LogicalPlan::Unnest(node) => &node.alias,
        LogicalPlan::CteRef(node) => node.alias.as_deref().unwrap_or(&node.name),
        LogicalPlan::TableFunction(node) => &node.alias,
        _ => "",
    };
    local.eq_ignore_ascii_case(name)
        || plan
            .children()
            .iter()
            .any(|child| has_relation(child, name))
}

/// A constant row supplies cardinality for standalone expansions without reading data.
pub(crate) fn unnest_seed() -> LogicalPlan {
    // This is a new cardinality seed, with one explicitly named constant column.
    // Both Values fields are supplied; the enclosing expansion hides the seed output.
    LogicalPlan::Values(Values {
        rows: vec![vec![seed_literal()]],
        output_names: vec!["__fedq_seed".to_string()],
    })
}

/// Construct the one scalar value needed to produce a standalone expansion row.
fn seed_literal() -> Expr {
    // This is a new constant establishing standalone expansion cardinality.
    // Both Literal fields carry its integer value and resolved scalar type.
    Expr::Literal {
        value: fq_plan::LiteralValue::Integer(1),
        data_type: fq_common::DataType::Integer,
    }
}

/// Reject additional arguments and offset syntax that do not have DuckDB semantics.
fn validate_from_unnest(function: &UnnestFunc) -> Result<(), ParseError> {
    if !function.expressions.is_empty() || function.offset_alias.is_some() {
        return Err(ParseError::Unsupported(
            "FROM UNNEST takes one list argument and optional WITH ORDINALITY".to_string(),
        ));
    }
    Ok(())
}

/// Resolve standard FROM output aliases, including the optional ordinal column.
fn from_names(
    columns: &[Identifier],
    with_ordinality: bool,
) -> Result<(Vec<String>, Option<String>), ParseError> {
    let width = 1 + usize::from(with_ordinality);
    if columns.len() > width {
        return Err(ParseError::Unsupported(
            "too many UNNEST output aliases".to_string(),
        ));
    }
    let name = columns
        .first()
        .map_or("unnest", |column| column.name.as_str());
    let ordinal = with_ordinality.then(|| {
        columns
            .get(1)
            .map_or("ordinality", |column| column.name.as_str())
            .to_string()
    });
    Ok((vec![name.to_string()], ordinal))
}

/// Expose expansion outputs to star expansion without exposing internal seed columns.
fn register_expansion(
    tables: &mut Vec<FromTable>,
    alias: &str,
    names: &[String],
    ordinal: Option<&str>,
) {
    let mut columns = names.to_vec();
    if let Some(name) = ordinal {
        columns.push(name.to_string());
    }
    tables.push(FromTable {
        key: alias.to_string(),
        columns: ColumnSource::Explicit(columns),
    });
}

/// Replace each expansion call with a qualified reference to its manufactured output.
fn extract_unnest(
    expression: Expr,
    alias: &str,
    arguments: &mut Vec<Expr>,
) -> Result<Expr, ParseError> {
    if is_unnest_call(&expression) {
        return extract_call(expression, alias, arguments);
    }
    expression.try_map_children(&mut |child| extract_unnest(child, alias, arguments))
}

/// Identify the internal expansion expression while preserving other function calls.
fn is_unnest_call(expression: &Expr) -> bool {
    matches!(expression, Expr::FunctionCall { function_name, .. } if function_name == "__fedq_unnest")
}

/// Extract one list argument; nested direct calls are rejected rather than misordered.
fn extract_call(
    expression: Expr,
    alias: &str,
    arguments: &mut Vec<Expr>,
) -> Result<Expr, ParseError> {
    let argument = single_unnest_argument(expression)?;
    if contains_unnest(&argument) {
        return Err(ParseError::Unsupported(
            "nested UNNEST calls require a derived relation".to_string(),
        ));
    }
    let name = format!("value_{}", arguments.len());
    arguments.push(argument);
    Ok(Expr::Column(ColumnRef::new(
        Some(alias.to_string()),
        name,
        None,
    )))
}

/// Require exactly one list expression before extracting a SELECT expansion call.
fn single_unnest_argument(expression: Expr) -> Result<Expr, ParseError> {
    let Expr::FunctionCall { mut args, .. } = expression else {
        unreachable!("UNNEST call");
    };
    if args.len() != 1 {
        return Err(ParseError::Unsupported(
            "SELECT UNNEST requires one list argument".to_string(),
        ));
    }
    Ok(args.remove(0))
}

/// Detect row expansion beneath an expression without descending into subqueries.
pub(crate) fn contains_unnest(expression: &Expr) -> bool {
    is_unnest_call(expression)
        || expression
            .children()
            .iter()
            .any(|child| contains_unnest(child))
}

/// Assign stable names to the outputs of one SELECT-list zip expansion.
fn expansion_names(width: usize) -> Vec<String> {
    let mut names = Vec::with_capacity(width);
    for index in 0..width {
        names.push(format!("value_{index}"));
    }
    names
}

/// Construct a new expansion with every field supplied by the parsed SQL shape.
fn new_unnest(
    input: LogicalPlan,
    arguments: Vec<Expr>,
    output_names: Vec<String>,
    alias: String,
    outer: bool,
    ordinality: Option<String>,
    retain_input: bool,
) -> LogicalPlan {
    // This is a new relational operator; the source is a plan plus parsed options.
    // The complete field list retains input, arguments, names, alias, and row semantics.
    LogicalPlan::Unnest(Unnest {
        input: Box::new(input),
        arguments,
        output_names,
        alias,
        outer,
        ordinality,
        retain_input,
        predicate: None,
    })
}
impl Converter<'_> {
    /// Lower grouped list expansion after aggregation, with qualified intermediate outputs.
    pub(crate) fn grouped_unnest(
        &self,
        input: LogicalPlan,
        select: &polyglot_sql::expressions::Select,
        expressions: Vec<Expr>,
        names: Vec<String>,
    ) -> Result<LogicalPlan, ParseError> {
        let alias = self.unnest_alias(&input);
        let (grouped, outputs) = grouped_outputs(expressions, &alias)?;
        let grouped_names = expansion_names(grouped.len());
        let aggregate = self.build_aggregate(input, select, grouped, grouped_names)?;
        // This is a new relation exposing every aggregate output under one qualifier.
        // All fields preserve the aggregate input and its declared column names.
        let input = LogicalPlan::SubqueryScan(fq_plan::SubqueryScan {
            input: Box::new(aggregate),
            alias,
            column_names: None,
        });
        let (input, outputs) = self.expand_select_unnests(input, outputs)?;
        // This is a new projection restoring the parsed SELECT expressions after expansion.
        // Every field preserves output names and the absence of DISTINCT in grouped queries.
        Ok(LogicalPlan::Projection(Projection {
            input: Box::new(input),
            expressions: outputs,
            aliases: names,
            distinct: false,
            distinct_on: None,
        }))
    }
}

/// Separate aggregate outputs from expressions evaluated after list expansion.
fn grouped_outputs(
    expressions: Vec<Expr>,
    alias: &str,
) -> Result<(Vec<Expr>, Vec<Expr>), ParseError> {
    let mut grouped = Vec::new();
    let mut outputs = Vec::new();
    for expression in expressions {
        outputs.push(grouped_expression(expression, alias, &mut grouped)?);
    }
    Ok((grouped, outputs))
}

/// Move expressions without expansion into aggregate outputs, then reference those outputs.
fn grouped_expression(
    expression: Expr,
    alias: &str,
    grouped: &mut Vec<Expr>,
) -> Result<Expr, ParseError> {
    if contains_unnest(&expression) {
        return expression.try_map_children(&mut |child| grouped_expression(child, alias, grouped));
    }
    let name = format!("value_{}", grouped.len());
    grouped.push(expression);
    Ok(Expr::Column(ColumnRef::new(
        Some(alias.to_string()),
        name,
        None,
    )))
}
