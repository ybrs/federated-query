//! The step bridge: `fq_physical::build_steps` output -> the engine's `core::ir`.
//!
//! fq-physical is the planning crate; it produces `BuiltSteps` whose steps and
//! fragments hold `fq_plan::Expr` directly (the expression tree, already retagged
//! to its execution relation). The engine (`engine::execute`) consumes `core::ir`,
//! whose expressions are the flat `IrExpr` sub-IR that lowers to DataFusion.
//!
//! This module owns the one layer that fq-physical intentionally SKIPPED: the
//! `fq_plan::Expr -> IrExpr` lowering that used to live in `executor/rust_ir.py`
//! (`_serialize_*`). The step/fragment/scan shapes were ported FROM `core::ir`, so
//! the structural conversion is a field-by-field copy; the only real work is the
//! expression lowering and the loud refusal of any expression that has no
//! execution form (a surviving subquery/window/interval). A crash never ships a
//! lie: an unmapped node RAISES `ExecError` rather than emit a wrong node.

use std::collections::BTreeMap;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fq_common::DataType;
use fq_physical::steps::{self, build_steps, BuiltSteps};
use fq_plan::physical::PhysicalPlan;
use fq_plan::{output_column_names, BinaryOpType, Expr, LiteralValue, UnaryOpType};

use crate::core::ir;
use crate::engine::execute;
use crate::error::{ExecError, ExecResult};

/// The engine's full result: the Arrow schema and batches, the per-step
/// `(binding, measured-rows)` stream, and the provenance mapping each binding to
/// what its measurement MEANS for the learned-stats catalog (the write path the
/// runtime persists after the result is materialized).
pub struct PlanExecution {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub measurements: Vec<(String, usize)>,
    pub observations: BTreeMap<String, steps::Observation>,
}

/// Plan a physical tree into steps, lower them to `core::ir`, and run the engine
/// for one session. The whole Rust pipeline (parse -> ... -> plan feeds this)
/// terminates here in an Arrow result plus the per-step measured-row observations
/// and their provenance.
///
/// `session` names the runtime whose datasources the reads resolve to. It is
/// installed on this (driving) thread for the whole execution, so every
/// registry/connection lookup keys on it, and every stale session's pooled
/// connections on this thread are reaped first.
pub fn execute_plan(
    session: crate::connectors::SessionId,
    plan: &PhysicalPlan,
) -> ExecResult<PlanExecution> {
    let _scope = crate::connectors::SessionScope::enter(session);
    crate::connectors::reap_dead_sessions();
    let mut built = build_steps(plan).map_err(|error| ExecError::runtime(error.to_string()))?;
    let observations = std::mem::take(&mut built.observations);
    // The result column names come from the plan root (the top projection/scan
    // output). The engine returns the final binding's schema; these names are the
    // user-visible ordering the runtime layer renames onto it.
    let outputs = output_column_names(plan);
    let ir = to_ir(built, outputs)?;
    let (schema, batches, measurements) = execute(&ir)?;
    Ok(PlanExecution {
        schema,
        batches,
        measurements,
        observations,
    })
}

/// Convert a `BuiltSteps` bundle (plus the plan's output column names) into the
/// engine `core::ir::Ir`. Fallible because every expression it carries is lowered
/// through `serialize_expr`, which refuses an unmapped node.
pub fn to_ir(built: BuiltSteps, outputs: Vec<String>) -> ExecResult<ir::Ir> {
    let mut steps = Vec::with_capacity(built.steps.len());
    for step in built.steps {
        steps.push(convert_step(step)?);
    }
    let mut fragments = BTreeMap::new();
    for (name, fragment) in built.fragments {
        fragments.insert(name, convert_fragment(fragment)?);
    }
    Ok(ir::Ir {
        outputs,
        steps,
        fragments,
    })
}

/// Convert one orchestration step, lowering its scan filter where present.
fn convert_step(step: steps::Step) -> ExecResult<ir::Step> {
    match step {
        steps::Step::SourceScan {
            datasource,
            scan,
            binding,
            materialize,
        } => Ok(ir::Step::SourceScan {
            datasource,
            scan: convert_scan_spec(scan)?,
            binding,
            materialize,
        }),
        steps::Step::CollectDistinct {
            input,
            key,
            cap,
            binding,
        } => Ok(ir::Step::CollectDistinct {
            input,
            key,
            cap,
            binding,
        }),
        steps::Step::InjectedScan {
            datasource,
            scan,
            inject_column,
            keys_from,
            binding,
            inject_column_ndv,
            extra_injections,
        } => convert_injected_scan(
            datasource,
            scan,
            inject_column,
            keys_from,
            binding,
            inject_column_ndv,
            extra_injections,
        ),
        steps::Step::Ship {
            datasource,
            input,
            table,
        } => Ok(ir::Step::Ship {
            datasource,
            input,
            table,
        }),
        steps::Step::Merge {
            fragment,
            inputs,
            binding,
        } => Ok(ir::Step::Merge {
            fragment,
            inputs,
            binding,
        }),
        steps::Step::Return { input } => Ok(ir::Step::Return { input }),
    }
}

/// Convert an injected (key-reduced) scan, lowering its scan and each extra
/// IN-list key set.
fn convert_injected_scan(
    datasource: String,
    scan: steps::ScanSpec,
    inject_column: String,
    keys_from: String,
    binding: String,
    inject_column_ndv: Option<i64>,
    extra_injections: Vec<steps::ExtraInjection>,
) -> ExecResult<ir::Step> {
    let mut extras = Vec::with_capacity(extra_injections.len());
    for extra in extra_injections {
        extras.push(ir::ExtraInjection {
            column: extra.column,
            keys_from: extra.keys_from,
        });
    }
    Ok(ir::Step::InjectedScan {
        datasource,
        scan: convert_scan_spec(scan)?,
        inject_column,
        keys_from,
        binding,
        inject_column_ndv: convert_ndv(inject_column_ndv)?,
        extra_injections: extras,
    })
}

/// Convert a source scan, lowering its optional structured filter.
fn convert_scan_spec(scan: steps::ScanSpec) -> ExecResult<ir::ScanSpec> {
    Ok(ir::ScanSpec {
        raw_sql: scan.raw_sql,
        schema: scan.schema,
        table: scan.table,
        alias: scan.alias,
        columns: scan.columns,
        filter: serialize_optional(scan.filter.as_ref())?,
        limit: convert_limit(scan.limit)?,
        distinct: scan.distinct,
        parallel: scan.parallel,
        injected_sql: scan.injected_sql,
    })
}

/// Convert one local relational fragment, lowering every expression it holds.
fn convert_fragment(fragment: steps::Fragment) -> ExecResult<ir::Fragment> {
    match fragment {
        steps::Fragment::HashJoin {
            join_type,
            left_keys,
            right_keys,
            project,
        } => Ok(ir::Fragment::HashJoin {
            join_type: convert_join_kind(join_type),
            left_keys,
            right_keys,
            project: convert_projections(project)?,
        }),
        steps::Fragment::NestedLoopJoin {
            join_type,
            condition,
            project,
        } => Ok(ir::Fragment::NestedLoopJoin {
            join_type: convert_join_kind(join_type),
            condition: serialize_optional(condition.as_ref())?,
            project: convert_projections(project)?,
        }),
        steps::Fragment::Project { project, distinct } => Ok(ir::Fragment::Project {
            project: convert_projections(project)?,
            distinct,
        }),
        steps::Fragment::Aggregate {
            select,
            group_by,
            grouping_sets,
        } => Ok(ir::Fragment::Aggregate {
            select: convert_agg_select(select)?,
            group_by: serialize_exprs(&group_by)?,
            grouping_sets: convert_grouping_sets(grouping_sets)?,
        }),
        steps::Fragment::Sort { keys } => Ok(ir::Fragment::Sort {
            keys: convert_sort_keys(keys)?,
        }),
        steps::Fragment::Filter { predicate } => Ok(ir::Fragment::Filter {
            predicate: serialize_expr(&predicate)?,
        }),
        steps::Fragment::Limit { limit, offset } => Ok(ir::Fragment::Limit {
            limit: convert_limit(limit)?,
            offset: convert_offset(offset)?,
        }),
        steps::Fragment::SingleRowGuard { keys } => Ok(ir::Fragment::SingleRowGuard {
            keys: serialize_exprs(&keys)?,
        }),
        steps::Fragment::RawSql { sql } => Ok(ir::Fragment::RawSql { sql }),
    }
}

/// Convert a projection list (each item's expression is lowered).
fn convert_projections(project: Vec<steps::Projection>) -> ExecResult<Vec<ir::Projection>> {
    let mut out = Vec::with_capacity(project.len());
    for item in project {
        out.push(ir::Projection {
            expr: serialize_expr(&item.expr)?,
            alias: item.alias,
        });
    }
    Ok(out)
}

/// Convert an aggregate select list: each item is exactly one of a grouping
/// expression or an aggregate call, aliased.
fn convert_agg_select(select: Vec<steps::AggSelectItem>) -> ExecResult<Vec<ir::AggSelectItem>> {
    let mut out = Vec::with_capacity(select.len());
    for item in select {
        let agg = match item.agg {
            Some(call) => Some(convert_agg_call(call)?),
            None => None,
        };
        out.push(ir::AggSelectItem {
            expr: serialize_optional(item.expr.as_ref())?,
            agg,
            alias: item.alias,
        });
    }
    Ok(out)
}

/// Convert one aggregate call, lowering its arguments and WITHIN GROUP key.
fn convert_agg_call(call: steps::AggCall) -> ExecResult<ir::AggCall> {
    let within_group = match call.within_group {
        Some(group) => Some(ir::WithinGroup {
            key: serialize_expr(&group.key)?,
            desc: group.desc,
        }),
        None => None,
    };
    Ok(ir::AggCall {
        func: call.func,
        distinct: call.distinct,
        star: call.star,
        args: serialize_exprs(&call.args)?,
        within_group,
    })
}

/// Convert a sort-key list (each key's expression is lowered).
fn convert_sort_keys(keys: Vec<steps::SortKey>) -> ExecResult<Vec<ir::SortKey>> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        out.push(ir::SortKey {
            expr: serialize_expr(&key.expr)?,
            ascending: key.ascending,
            nulls_first: key.nulls_first,
        });
    }
    Ok(out)
}

/// Convert the grouping-sets lists (each set's expressions are lowered).
fn convert_grouping_sets(sets: Vec<Vec<Expr>>) -> ExecResult<Vec<Vec<ir::IrExpr>>> {
    let mut out = Vec::with_capacity(sets.len());
    for set in sets {
        out.push(serialize_exprs(&set)?);
    }
    Ok(out)
}

/// Map a physical join kind onto the engine's join kind (identical variants).
fn convert_join_kind(kind: steps::JoinKind) -> ir::JoinKind {
    match kind {
        steps::JoinKind::Inner => ir::JoinKind::Inner,
        steps::JoinKind::Left => ir::JoinKind::Left,
        steps::JoinKind::Right => ir::JoinKind::Right,
        steps::JoinKind::Full => ir::JoinKind::Full,
        steps::JoinKind::Semi => ir::JoinKind::Semi,
        steps::JoinKind::Anti => ir::JoinKind::Anti,
    }
}

/// Narrow a `u64` row limit to the engine's `usize`, refusing an out-of-range
/// value rather than truncating it.
fn convert_limit(limit: Option<u64>) -> ExecResult<Option<usize>> {
    match limit {
        None => Ok(None),
        Some(value) => usize::try_from(value).map(Some).map_err(|_| {
            ExecError::runtime(format!("scan/limit value {value} is not addressable"))
        }),
    }
}

/// Narrow a `u64` OFFSET to the engine's `usize`, refusing an out-of-range value.
fn convert_offset(offset: u64) -> ExecResult<usize> {
    usize::try_from(offset)
        .map_err(|_| ExecError::runtime(format!("limit offset {offset} is not addressable")))
}

/// Narrow a signed injection NDV to the engine's `u64` count. An NDV is a
/// distinct-value count, so a negative value is a corrupt statistic and RAISES
/// rather than wrapping to a huge count.
fn convert_ndv(ndv: Option<i64>) -> ExecResult<Option<u64>> {
    match ndv {
        None => Ok(None),
        Some(value) => u64::try_from(value).map(Some).map_err(|_| {
            ExecError::runtime(format!("negative injection NDV {value} is not a count"))
        }),
    }
}

/// Lower an optional expression, preserving None.
fn serialize_optional(expr: Option<&Expr>) -> ExecResult<Option<ir::IrExpr>> {
    match expr {
        Some(expr) => Ok(Some(serialize_expr(expr)?)),
        None => Ok(None),
    }
}

/// Lower a slice of expressions in order.
fn serialize_exprs(exprs: &[Expr]) -> ExecResult<Vec<ir::IrExpr>> {
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        out.push(serialize_expr(expr)?);
    }
    Ok(out)
}

/// Lower one `fq_plan::Expr` into the engine's `IrExpr`. Ports the `_serialize_*`
/// family of `executor/rust_ir.py` exactly: columns keep their own qualifier
/// (fq-physical retagged them already), EXTRACT lowers to `date_part`, BETWEEN to
/// an AND of two comparisons, IS [NOT] NULL to `IsNull`, and CAST to an Arrow type
/// name. Any node with no execution form (a subquery/window/interval/tuple that
/// should have been removed before execution) RAISES rather than emit a wrong node.
pub fn serialize_expr(expr: &Expr) -> ExecResult<ir::IrExpr> {
    match expr {
        Expr::Column(column) => Ok(ir::IrExpr::Column {
            relation: column.table.clone(),
            name: column.column.clone(),
        }),
        Expr::Literal { value, .. } => Ok(ir::IrExpr::Literal {
            value: literal_value(value),
        }),
        Expr::BinaryOp { op, left, right } => Ok(ir::IrExpr::Binary {
            op: binary_op_token(*op)?.to_string(),
            left: Box::new(serialize_expr(left)?),
            right: Box::new(serialize_expr(right)?),
        }),
        Expr::UnaryOp { op, operand } => serialize_unary(*op, operand),
        Expr::Cast {
            expr,
            target_type,
            data_type,
        } => Ok(ir::IrExpr::Cast {
            expr: Box::new(serialize_expr(expr)?),
            to: cast_arrow_type(*data_type, target_type)?.to_string(),
        }),
        Expr::Case {
            when_clauses,
            else_result,
        } => serialize_case(when_clauses, else_result.as_deref()),
        Expr::InList { value, options } => Ok(ir::IrExpr::InList {
            expr: Box::new(serialize_expr(value)?),
            list: serialize_exprs(options)?,
            negated: false,
        }),
        Expr::Between {
            value,
            lower,
            upper,
        } => serialize_between(value, lower, upper),
        Expr::Like {
            case_insensitive,
            expr,
            pattern,
            escape,
        } => serialize_like(*case_insensitive, expr, pattern, escape.as_deref()),
        Expr::Extract { field, source } => serialize_extract(field, source),
        Expr::FunctionCall {
            function_name,
            args,
            is_aggregate,
            ..
        } => serialize_call(function_name, args, *is_aggregate),
        Expr::Window { .. }
        | Expr::Interval { .. }
        | Expr::Tuple { .. }
        | Expr::Subquery { .. }
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. } => Err(ExecError::runtime(format!(
            "expression '{}' has no execution IR form; a subquery/window/interval \
             must be removed by an earlier pass before execution",
            expr.variant_label()
        ))),
    }
}

/// Lower a function/aggregate call to its execution IR. A coordinator `avg` over a
/// double-typed argument wraps that argument in a cast to double. The engine models a
/// DECIMAL source column's logical type as double, yet DataFusion reads the arrow
/// Decimal128 the source actually returns and its avg(Decimal(p,s)) yields
/// Decimal(p+4, s+4) - scale 6 for a scale-2 input, which truncates the true average -
/// while every source's avg and the DuckDB oracle return double. Casting the argument
/// first forces double-precision averaging that matches them; over a genuine double it
/// is a value-preserving no-op. Only a single-argument double/decimal avg is rewritten;
/// integer avg (already double in both engines) and other functions serialize unchanged.
/// This IR only ever executes on the DataFusion coordinator, so source-pushdown SQL
/// (rendered separately by fq-emit) is untouched.
fn serialize_call(
    function_name: &str,
    args: &[Expr],
    is_aggregate: bool,
) -> ExecResult<ir::IrExpr> {
    let name = function_name.to_lowercase();
    if is_aggregate
        && name == "avg"
        && args.len() == 1
        && matches!(args[0].get_type(), DataType::Double | DataType::Decimal)
    {
        return Ok(ir::IrExpr::Function {
            name,
            args: vec![ir::IrExpr::Cast {
                expr: Box::new(serialize_expr(&args[0])?),
                to: "float64".to_string(),
            }],
        });
    }
    Ok(ir::IrExpr::Function {
        name,
        args: serialize_exprs(args)?,
    })
}

/// Lower a unary op: NOT / negate become `Unary`, IS [NOT] NULL become `IsNull`.
fn serialize_unary(op: UnaryOpType, operand: &Expr) -> ExecResult<ir::IrExpr> {
    let operand = Box::new(serialize_expr(operand)?);
    match op {
        UnaryOpType::Not => Ok(ir::IrExpr::Unary {
            op: "not".to_string(),
            operand,
        }),
        UnaryOpType::Negate => Ok(ir::IrExpr::Unary {
            op: "neg".to_string(),
            operand,
        }),
        UnaryOpType::IsNull => Ok(ir::IrExpr::IsNull {
            expr: operand,
            negated: false,
        }),
        UnaryOpType::IsNotNull => Ok(ir::IrExpr::IsNull {
            expr: operand,
            negated: true,
        }),
    }
}

/// Lower a searched CASE (fq-plan already lowered the simple form): a list of
/// WHEN/THEN plus an optional ELSE, with no operand.
fn serialize_case(
    when_clauses: &[(Expr, Expr)],
    else_result: Option<&Expr>,
) -> ExecResult<ir::IrExpr> {
    let mut whens = Vec::with_capacity(when_clauses.len());
    for (condition, result) in when_clauses {
        whens.push(ir::WhenThen {
            when: serialize_expr(condition)?,
            then: serialize_expr(result)?,
        });
    }
    Ok(ir::IrExpr::Case {
        operand: None,
        whens,
        else_expr: serialize_optional(else_result)?.map(Box::new),
    })
}

/// Lower BETWEEN as `(value >= lower) AND (value <= upper)`.
fn serialize_between(value: &Expr, lower: &Expr, upper: &Expr) -> ExecResult<ir::IrExpr> {
    let value = serialize_expr(value)?;
    let ge = ir::IrExpr::Binary {
        op: ">=".to_string(),
        left: Box::new(value.clone()),
        right: Box::new(serialize_expr(lower)?),
    };
    let le = ir::IrExpr::Binary {
        op: "<=".to_string(),
        left: Box::new(value),
        right: Box::new(serialize_expr(upper)?),
    };
    Ok(ir::IrExpr::Binary {
        op: "and".to_string(),
        left: Box::new(ge),
        right: Box::new(le),
    })
}

/// Lower `expr [I]LIKE pattern [ESCAPE 'c']` to the engine's `Like` IR. The escape
/// string is a single character (validated at parse); a longer string here is a
/// contract violation and RAISES rather than silently truncate.
fn serialize_like(
    case_insensitive: bool,
    expr: &Expr,
    pattern: &Expr,
    escape: Option<&str>,
) -> ExecResult<ir::IrExpr> {
    let escape_char = match escape {
        Some(text) => Some(single_escape_char(text)?),
        None => None,
    };
    Ok(ir::IrExpr::Like {
        expr: Box::new(serialize_expr(expr)?),
        pattern: Box::new(serialize_expr(pattern)?),
        case_insensitive,
        escape: escape_char,
    })
}

/// The one `char` of a LIKE escape string, or an error if it is not exactly one.
fn single_escape_char(text: &str) -> ExecResult<char> {
    let mut chars = text.chars();
    match (chars.next(), chars.next()) {
        (Some(character), None) => Ok(character),
        _ => Err(ExecError::runtime(format!(
            "LIKE ESCAPE must be a single character, got '{text}'"
        ))),
    }
}

/// Lower `EXTRACT(field FROM source)` to `date_part('field', source)`.
fn serialize_extract(field: &str, source: &Expr) -> ExecResult<ir::IrExpr> {
    let field_literal = ir::IrExpr::Literal {
        value: ir::LiteralValue::Str {
            value: field.to_lowercase(),
        },
    };
    Ok(ir::IrExpr::Function {
        name: "date_part".to_string(),
        args: vec![field_literal, serialize_expr(source)?],
    })
}

/// Map a typed literal onto the engine's typed-literal form.
fn literal_value(value: &LiteralValue) -> ir::LiteralValue {
    match value {
        LiteralValue::Null => ir::LiteralValue::Null,
        LiteralValue::Boolean(value) => ir::LiteralValue::Bool { value: *value },
        LiteralValue::Integer(value) => ir::LiteralValue::Int { value: *value },
        LiteralValue::Float(value) => ir::LiteralValue::Float { value: *value },
        LiteralValue::String(value) => ir::LiteralValue::Str {
            value: value.clone(),
        },
    }
}

/// The engine's binary-operator token for a `BinaryOpType`. String concat lowers
/// to the `||` token; the two regex matches have no execution IR form and RAISE.
fn binary_op_token(op: BinaryOpType) -> ExecResult<&'static str> {
    Ok(match op {
        BinaryOpType::Add => "+",
        BinaryOpType::Subtract => "-",
        BinaryOpType::Multiply => "*",
        BinaryOpType::Divide => "/",
        BinaryOpType::Modulo => "%",
        BinaryOpType::Eq => "=",
        BinaryOpType::Neq => "!=",
        BinaryOpType::Lt => "<",
        BinaryOpType::Lte => "<=",
        BinaryOpType::Gt => ">",
        BinaryOpType::Gte => ">=",
        BinaryOpType::And => "and",
        BinaryOpType::Or => "or",
        BinaryOpType::NullSafeEq => "is_not_distinct_from",
        BinaryOpType::NullSafeNeq => "is_distinct_from",
        BinaryOpType::Concat => "||",
        BinaryOpType::RegexMatch | BinaryOpType::RegexImatch => {
            return Err(ExecError::runtime(format!(
                "binary operator '{}' has no execution IR form",
                op.value()
            )))
        }
    })
}

/// The Arrow type name for a CAST target, from the resolved `DataType`. Decimals
/// map to `float64` (the engine treats them as float, matching the Python path).
/// An unbound cast (no resolved type) or a non-castable type RAISES.
fn cast_arrow_type(data_type: Option<DataType>, target_type: &str) -> ExecResult<&'static str> {
    data_type.and_then(arrow_cast_name).ok_or_else(|| {
        ExecError::runtime(format!("cast to '{target_type}' has no execution IR form"))
    })
}

/// The Arrow type name for a castable engine type, or None for a type with no
/// cast form (TIMESTAMP / INTERVAL / NULL). Ports the Python `_CAST_TYPES` map.
fn arrow_cast_name(data_type: DataType) -> Option<&'static str> {
    match data_type {
        DataType::Integer => Some("int32"),
        DataType::BigInt => Some("int64"),
        DataType::Float => Some("float32"),
        DataType::Double | DataType::Decimal => Some("float64"),
        DataType::Varchar | DataType::Text => Some("utf8"),
        DataType::Boolean => Some("boolean"),
        DataType::Date => Some("date32"),
        DataType::Timestamp | DataType::Interval | DataType::Null => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_plan::ColumnRef;

    /// A qualified, bound column reference for lowering tests.
    fn column(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    #[test]
    fn column_keeps_its_qualifier() {
        let lowered = serialize_expr(&column("t", "a")).unwrap();
        match lowered {
            ir::IrExpr::Column { relation, name } => {
                assert_eq!(relation.as_deref(), Some("t"));
                assert_eq!(name, "a");
            }
            other => panic!("expected a column, got {other:?}"),
        }
    }

    #[test]
    fn between_lowers_to_and_of_two_comparisons() {
        let expr = Expr::Between {
            value: Box::new(column("t", "a")),
            lower: Box::new(Expr::Literal {
                value: LiteralValue::Integer(1),
                data_type: DataType::Integer,
            }),
            upper: Box::new(Expr::Literal {
                value: LiteralValue::Integer(9),
                data_type: DataType::Integer,
            }),
        };
        match serialize_expr(&expr).unwrap() {
            ir::IrExpr::Binary { op, left, right } => {
                assert_eq!(op, "and");
                assert!(matches!(*left, ir::IrExpr::Binary { ref op, .. } if op == ">="));
                assert!(matches!(*right, ir::IrExpr::Binary { ref op, .. } if op == "<="));
            }
            other => panic!("expected an AND, got {other:?}"),
        }
    }

    #[test]
    fn like_lowers_to_like_ir_with_escape() {
        let expr = Expr::Like {
            case_insensitive: true,
            expr: Box::new(column("t", "a")),
            pattern: Box::new(Expr::Literal {
                value: LiteralValue::String("x@%".to_string()),
                data_type: DataType::Varchar,
            }),
            escape: Some("@".to_string()),
        };
        match serialize_expr(&expr).unwrap() {
            ir::IrExpr::Like {
                case_insensitive,
                escape,
                ..
            } => {
                assert!(case_insensitive);
                assert_eq!(escape, Some('@'));
            }
            other => panic!("expected a Like, got {other:?}"),
        }
    }

    #[test]
    fn like_escape_longer_than_one_char_raises() {
        // The parser validates escape length, so a multi-character escape reaching
        // lowering is a contract violation and must raise, not silently truncate.
        let expr = Expr::Like {
            case_insensitive: false,
            expr: Box::new(column("t", "a")),
            pattern: Box::new(Expr::Literal {
                value: LiteralValue::String("x".to_string()),
                data_type: DataType::Varchar,
            }),
            escape: Some("!!".to_string()),
        };
        assert!(serialize_expr(&expr).is_err());
    }

    #[test]
    fn extract_lowers_to_date_part() {
        let expr = Expr::Extract {
            field: "YEAR".to_string(),
            source: Box::new(column("t", "d")),
        };
        match serialize_expr(&expr).unwrap() {
            ir::IrExpr::Function { name, args } => {
                assert_eq!(name, "date_part");
                assert_eq!(args.len(), 2);
                assert!(
                    matches!(&args[0], ir::IrExpr::Literal { value: ir::LiteralValue::Str { value } } if value == "year")
                );
            }
            other => panic!("expected date_part, got {other:?}"),
        }
    }

    #[test]
    fn is_not_null_lowers_to_negated_is_null() {
        let expr = Expr::UnaryOp {
            op: UnaryOpType::IsNotNull,
            operand: Box::new(column("t", "a")),
        };
        assert!(matches!(
            serialize_expr(&expr).unwrap(),
            ir::IrExpr::IsNull { negated: true, .. }
        ));
    }

    #[test]
    fn cast_uses_the_resolved_arrow_type_name() {
        let expr = Expr::Cast {
            expr: Box::new(column("t", "a")),
            target_type: "BIGINT".to_string(),
            data_type: Some(DataType::BigInt),
        };
        assert!(matches!(
            serialize_expr(&expr).unwrap(),
            ir::IrExpr::Cast { to, .. } if to == "int64"
        ));
    }

    fn typed_column(table: &str, name: &str, data_type: DataType) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(data_type),
        ))
    }

    fn avg_call(arg: Expr) -> Expr {
        Expr::FunctionCall {
            function_name: "AVG".to_string(),
            args: vec![arg],
            is_aggregate: true,
            distinct: false,
            within_group_key: None,
            within_group_desc: false,
        }
    }

    #[test]
    fn avg_decimal_argument_wraps_in_a_double_cast() {
        // A DECIMAL source column binds to the logical Double type.
        let expr = avg_call(typed_column("o", "price", DataType::Double));
        match serialize_expr(&expr).unwrap() {
            ir::IrExpr::Function { name, args } => {
                assert_eq!(name, "avg");
                assert_eq!(args.len(), 1);
                assert!(
                    matches!(&args[0], ir::IrExpr::Cast { to, .. } if to == "float64"),
                    "expected a float64 cast, got {:?}",
                    args[0]
                );
            }
            other => panic!("expected avg function, got {other:?}"),
        }
    }

    #[test]
    fn avg_integer_argument_serializes_unchanged() {
        let expr = avg_call(typed_column("o", "qty", DataType::BigInt));
        match serialize_expr(&expr).unwrap() {
            ir::IrExpr::Function { name, args } => {
                assert_eq!(name, "avg");
                assert!(
                    matches!(&args[0], ir::IrExpr::Column { .. }),
                    "integer avg must not be cast, got {:?}",
                    args[0]
                );
            }
            other => panic!("expected avg function, got {other:?}"),
        }
    }

    #[test]
    fn unmapped_expression_raises_rather_than_lie() {
        let interval = Expr::Interval {
            value: "1".to_string(),
            unit: Some("DAY".to_string()),
        };
        assert!(serialize_expr(&interval).is_err());
        let regex = Expr::BinaryOp {
            op: BinaryOpType::RegexMatch,
            left: Box::new(column("t", "a")),
            right: Box::new(column("t", "b")),
        };
        assert!(serialize_expr(&regex).is_err());
    }

    #[test]
    fn concat_lowers_to_the_double_pipe_token() {
        let concat = Expr::BinaryOp {
            op: BinaryOpType::Concat,
            left: Box::new(column("t", "a")),
            right: Box::new(column("t", "b")),
        };
        match serialize_expr(&concat).unwrap() {
            ir::IrExpr::Binary { op, .. } => assert_eq!(op, "||"),
            other => panic!("expected a binary ||, got {other:?}"),
        }
    }
}
