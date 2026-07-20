// Imported fedqrs engine core - already validated by the TPC tallies; pedantic-
// cleaned incrementally, not on import.
#![allow(clippy::all, clippy::pedantic)]
//! Translate the expression sub-IR into DataFusion `Expr`.
//!
//! One translation, two consumers: DataFusion's unparser renders these to
//! dialect-correct source SQL (`sql.rs`), and the DataFrame API builds local
//! operators from them (`engine.rs`). Anything unmapped raises, never silently
//! degrades.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow::array::Array;
use arrow::datatypes::DataType;
use datafusion::common::{Column, DataFusionError, ScalarValue, TableReference};
use datafusion::functions::all_default_functions;
use datafusion::functions_aggregate::all_default_aggregate_functions;
use datafusion::logical_expr::expr::{AggregateFunction, Case, InList, ScalarFunction};
use datafusion::logical_expr::{AggregateUDF, BinaryExpr, Cast, Expr, Operator, ScalarUDF};
use datafusion::prelude::lit;

use crate::core::ir::{IrExpr, LiteralValue};

fn plan_err<T>(msg: impl Into<String>) -> Result<T, DataFusionError> {
    Err(DataFusionError::Plan(msg.into()))
}

/// Build a DataFusion `Expr` from an IR expression.
pub fn to_df_expr(e: &IrExpr) -> Result<Expr, DataFusionError> {
    match e {
        IrExpr::Column { relation, name } => {
            let rel = relation.as_ref().map(|r| TableReference::bare(r.clone()));
            Ok(Expr::Column(Column::new(rel, name.clone())))
        }
        IrExpr::Literal { value } => Ok(lit(scalar(value))),
        IrExpr::Binary { op, left, right } => {
            let l = to_df_expr(left)?;
            let r = to_df_expr(right)?;
            Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(l),
                operator(op)?,
                Box::new(r),
            )))
        }
        IrExpr::Unary { op, operand } => {
            let inner = to_df_expr(operand)?;
            match op.as_str() {
                "not" => Ok(Expr::Not(Box::new(inner))),
                "neg" | "-" => Ok(Expr::Negative(Box::new(inner))),
                other => plan_err(format!("unsupported unary operator '{other}'")),
            }
        }
        IrExpr::Cast { expr, to } => {
            let inner = to_df_expr(expr)?;
            Ok(Expr::Cast(Cast::new(Box::new(inner), data_type(to)?)))
        }
        IrExpr::Case {
            operand,
            whens,
            else_expr,
        } => {
            let base = match operand {
                Some(o) => Some(Box::new(to_df_expr(o)?)),
                None => None,
            };
            let mut pairs = Vec::with_capacity(whens.len());
            for wt in whens {
                pairs.push((
                    Box::new(to_df_expr(&wt.when)?),
                    Box::new(to_df_expr(&wt.then)?),
                ));
            }
            let els = match else_expr {
                Some(x) => Some(Box::new(to_df_expr(x)?)),
                None => None,
            };
            Ok(Expr::Case(Case::new(base, pairs, els)))
        }
        IrExpr::InList {
            expr,
            list,
            negated,
        } => {
            let inner = to_df_expr(expr)?;
            let mut items = Vec::with_capacity(list.len());
            for it in list {
                items.push(to_df_expr(it)?);
            }
            Ok(Expr::InList(InList::new(Box::new(inner), items, *negated)))
        }
        IrExpr::IsNull { expr, negated } => {
            let inner = to_df_expr(expr)?;
            Ok(if *negated {
                inner.is_not_null()
            } else {
                inner.is_null()
            })
        }
        IrExpr::Function { name, args } => {
            let mut df_args = Vec::with_capacity(args.len());
            for arg in args {
                df_args.push(to_df_expr(arg)?);
            }
            build_function(name, df_args)
        }
    }
}

/// Build a scalar-or-aggregate function call by name. Scalars are tried first
/// (`date_part`, `substr`, ...); an aggregate (`sum`, `avg`, ...) is allowed so
/// an aggregate can appear inside a larger expression (e.g. `100 * sum(x) / sum(y)`).
fn build_function(name: &str, args: Vec<Expr>) -> Result<Expr, DataFusionError> {
    if let Some(udf) = scalar_registry().get(name).cloned() {
        return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(udf, args)));
    }
    if let Some(udf) = aggregate_registry().get(name).cloned() {
        let call = AggregateFunction::new_udf(udf, args, false, None, Vec::new(), None);
        return Ok(Expr::AggregateFunction(call));
    }
    Err(DataFusionError::Plan(format!(
        "function '{name}' not supported"
    )))
}

fn scalar_registry() -> &'static HashMap<String, Arc<ScalarUDF>> {
    static REGISTRY: OnceLock<HashMap<String, Arc<ScalarUDF>>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut map = HashMap::new();
        for udf in all_default_functions() {
            map.insert(udf.name().to_string(), udf.clone());
            for alias in udf.aliases() {
                map.insert(alias.to_string(), udf.clone());
            }
        }
        map
    })
}

fn aggregate_registry() -> &'static HashMap<String, Arc<AggregateUDF>> {
    static REGISTRY: OnceLock<HashMap<String, Arc<AggregateUDF>>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut map = HashMap::new();
        for udf in all_default_aggregate_functions() {
            map.insert(udf.name().to_string(), udf.clone());
            for alias in udf.aliases() {
                map.insert(alias.to_string(), udf.clone());
            }
        }
        map
    })
}

fn scalar(v: &LiteralValue) -> ScalarValue {
    match v {
        LiteralValue::Int { value } => ScalarValue::Int64(Some(*value)),
        LiteralValue::Float { value } => ScalarValue::Float64(Some(*value)),
        LiteralValue::Str { value } => ScalarValue::Utf8(Some(value.clone())),
        LiteralValue::Bool { value } => ScalarValue::Boolean(Some(*value)),
        LiteralValue::Null => ScalarValue::Null,
    }
}

fn operator(op: &str) -> Result<Operator, DataFusionError> {
    Ok(match op {
        "+" => Operator::Plus,
        "-" => Operator::Minus,
        "*" => Operator::Multiply,
        "/" => Operator::Divide,
        "%" => Operator::Modulo,
        "=" => Operator::Eq,
        "!=" | "<>" => Operator::NotEq,
        "<" => Operator::Lt,
        "<=" => Operator::LtEq,
        ">" => Operator::Gt,
        ">=" => Operator::GtEq,
        "and" => Operator::And,
        "or" => Operator::Or,
        "like" => Operator::LikeMatch,
        "ilike" => Operator::ILikeMatch,
        "is_distinct_from" => Operator::IsDistinctFrom,
        "is_not_distinct_from" => Operator::IsNotDistinctFrom,
        "||" => Operator::StringConcat,
        other => return plan_err(format!("unsupported binary operator '{other}'")),
    })
}

/// Map an Arrow type name (as Python emits it) to a DataFusion `DataType`.
pub fn data_type(name: &str) -> Result<DataType, DataFusionError> {
    Ok(match name {
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "utf8" | "string" => DataType::Utf8,
        "boolean" | "bool" => DataType::Boolean,
        "date32" => DataType::Date32,
        other => return plan_err(format!("unsupported cast target type '{other}'")),
    })
}

/// Extract a scalar literal from an Arrow array cell, for building a dynamic
/// `IN (...)` list from runtime-computed key values.
pub fn literal_from_array(array: &dyn Array, index: usize) -> Result<Expr, DataFusionError> {
    let value = ScalarValue::try_from_array(array, index)?;
    Ok(lit(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ir::IrExpr;

    fn parse(json: &str) -> IrExpr {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn translates_binary_comparison() {
        let e = parse(
            r#"{"node":"binary","op":"=","left":{"node":"column","relation":"t","name":"a"},
                "right":{"node":"literal","value":{"lit":"int","value":1}}}"#,
        );
        assert!(matches!(to_df_expr(&e).unwrap(), Expr::BinaryExpr(_)));
    }

    #[test]
    fn rejects_unsupported_operator() {
        let e = parse(
            r#"{"node":"binary","op":"~~","left":{"node":"column","name":"a"},
                "right":{"node":"column","name":"b"}}"#,
        );
        assert!(to_df_expr(&e).is_err());
    }

    #[test]
    fn cast_and_is_null() {
        let cast = parse(r#"{"node":"cast","expr":{"node":"column","name":"a"},"to":"int64"}"#);
        assert!(matches!(to_df_expr(&cast).unwrap(), Expr::Cast(_)));
        let isnull =
            parse(r#"{"node":"is_null","expr":{"node":"column","name":"a"},"negated":true}"#);
        assert!(to_df_expr(&isnull).is_ok());
    }

    #[test]
    fn rejects_unknown_cast_type() {
        let e = parse(r#"{"node":"cast","expr":{"node":"column","name":"a"},"to":"jsonb"}"#);
        assert!(to_df_expr(&e).is_err());
    }

    #[test]
    fn double_pipe_maps_to_string_concat() {
        assert_eq!(operator("||").unwrap(), Operator::StringConcat);
    }
}
