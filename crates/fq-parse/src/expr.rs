//! polyglot-sql expression -> `fq_plan::Expr`.
//!
//! polyglot's `Expression` is a ~1000-variant enum (a dedicated variant per known
//! function). Exhaustive handling is impossible, so this is an ALLOWLIST: the
//! constructs the engine supports are converted; every other variant RAISES
//! `ParseError::Unsupported` (the Python `SUPPORTED_*_ARGS` posture - fail fast,
//! never silently drop a construct).
//!
//! Coverage (this stage): columns, literals, comparison / logical / arithmetic /
//! concat binary ops, NOT / negate, IS NULL / IS NOT NULL, parentheses, the five
//! aggregate functions, and anonymous (unknown-name) scalar calls. Cast, Case,
//! In, Between, typed scalar functions, and the subquery expressions raise for
//! now and are the next increment.

use fq_plan::expr::LiteralValue;
use fq_plan::{BinaryOpType, ColumnRef, Expr, UnaryOpType};
use polyglot_sql::expressions::{BinaryOp, Expression, Literal};

use crate::error::ParseError;

/// Convert one polyglot expression into an engine expression.
pub fn convert_expr(expr: &Expression) -> Result<Expr, ParseError> {
    match expr {
        Expression::Column(column) => Ok(Expr::Column(ColumnRef::new(
            column.table.as_ref().map(|ident| ident.name.clone()),
            column.name.name.clone(),
            None,
        ))),
        Expression::Literal(literal) => convert_literal(literal),
        Expression::Boolean(boolean) => Ok(Expr::Literal {
            value: LiteralValue::Boolean(boolean.value),
            data_type: fq_common::DataType::Boolean,
        }),
        Expression::Null(_) => Ok(Expr::Literal {
            value: LiteralValue::Null,
            data_type: fq_common::DataType::Null,
        }),
        Expression::Paren(paren) => convert_expr(&paren.this),
        Expression::Alias(alias) => convert_expr(&alias.this),
        Expression::Not(unary) => convert_unary(UnaryOpType::Not, &unary.this),
        Expression::Neg(unary) => convert_unary(UnaryOpType::Negate, &unary.this),
        Expression::Is(binary) => convert_is(binary),
        _ => convert_binary_or_aggregate(expr),
    }
}

/// Second dispatch tier: the binary operators, the aggregate functions, and the
/// anonymous scalar call, else raise. Split out to keep `convert_expr` flat.
fn convert_binary_or_aggregate(expr: &Expression) -> Result<Expr, ParseError> {
    if let Some(op) = binary_op_type(expr) {
        return convert_binary(op, binary_operands(expr));
    }
    match expr {
        Expression::Count(count) => convert_count(count),
        Expression::Sum(agg) => convert_aggregate("SUM", agg),
        Expression::Avg(agg) => convert_aggregate("AVG", agg),
        Expression::Min(agg) => convert_aggregate("MIN", agg),
        Expression::Max(agg) => convert_aggregate("MAX", agg),
        Expression::Anonymous(anon) => convert_anonymous(anon),
        other => Err(ParseError::Unsupported(format!(
            "expression `{}`",
            other.variant_name()
        ))),
    }
}

/// Map a polyglot binary-operator variant to the engine operator, or None.
fn binary_op_type(expr: &Expression) -> Option<BinaryOpType> {
    let op = match expr {
        Expression::And(_) => BinaryOpType::And,
        Expression::Or(_) => BinaryOpType::Or,
        Expression::Add(_) => BinaryOpType::Add,
        Expression::Sub(_) => BinaryOpType::Subtract,
        Expression::Mul(_) => BinaryOpType::Multiply,
        Expression::Div(_) => BinaryOpType::Divide,
        Expression::Mod(_) => BinaryOpType::Modulo,
        Expression::Eq(_) => BinaryOpType::Eq,
        Expression::Neq(_) => BinaryOpType::Neq,
        Expression::Lt(_) => BinaryOpType::Lt,
        Expression::Lte(_) => BinaryOpType::Lte,
        Expression::Gt(_) => BinaryOpType::Gt,
        Expression::Gte(_) => BinaryOpType::Gte,
        Expression::Concat(_) => BinaryOpType::Concat,
        Expression::NullSafeEq(_) => BinaryOpType::NullSafeEq,
        _ => return None,
    };
    Some(op)
}

/// The `(left, right)` of any of the `Box<BinaryOp>` variants handled above.
fn binary_operands(expr: &Expression) -> &BinaryOp {
    match expr {
        Expression::And(binary)
        | Expression::Or(binary)
        | Expression::Add(binary)
        | Expression::Sub(binary)
        | Expression::Mul(binary)
        | Expression::Div(binary)
        | Expression::Mod(binary)
        | Expression::Eq(binary)
        | Expression::Neq(binary)
        | Expression::Lt(binary)
        | Expression::Lte(binary)
        | Expression::Gt(binary)
        | Expression::Gte(binary)
        | Expression::Concat(binary)
        | Expression::NullSafeEq(binary) => binary,
        _ => unreachable!("binary_operands called on a non-binary expression"),
    }
}

/// Build an engine binary op from operands.
fn convert_binary(op: BinaryOpType, binary: &BinaryOp) -> Result<Expr, ParseError> {
    Ok(Expr::BinaryOp {
        op,
        left: Box::new(convert_expr(&binary.left)?),
        right: Box::new(convert_expr(&binary.right)?),
    })
}

/// Build an engine unary op from an operand.
fn convert_unary(op: UnaryOpType, operand: &Expression) -> Result<Expr, ParseError> {
    Ok(Expr::UnaryOp {
        op,
        operand: Box::new(convert_expr(operand)?),
    })
}

/// Convert `x IS NULL` / `x IS NOT NULL` into the engine unary form. Any other
/// IS shape (IS TRUE, IS DISTINCT FROM, ...) is not yet supported.
fn convert_is(binary: &BinaryOp) -> Result<Expr, ParseError> {
    match &binary.right {
        Expression::Null(_) => convert_unary(UnaryOpType::IsNull, &binary.left),
        Expression::Not(inner) if matches!(inner.this, Expression::Null(_)) => {
            convert_unary(UnaryOpType::IsNotNull, &binary.left)
        }
        other => Err(ParseError::Unsupported(format!(
            "IS <{}>",
            other.variant_name()
        ))),
    }
}

/// Convert a numeric/string/etc. literal.
fn convert_literal(literal: &Literal) -> Result<Expr, ParseError> {
    match literal {
        Literal::Number(text) => Ok(convert_number(text)),
        Literal::String(text) => Ok(Expr::Literal {
            value: LiteralValue::String(text.clone()),
            data_type: fq_common::DataType::Varchar,
        }),
        other => Err(ParseError::Unsupported(format!("literal {other:?}"))),
    }
}

/// A numeric literal: integer when it parses as one, else a double.
fn convert_number(text: &str) -> Expr {
    if let Ok(integer) = text.parse::<i64>() {
        return Expr::Literal {
            value: LiteralValue::Integer(integer),
            data_type: fq_common::DataType::Integer,
        };
    }
    let value = text.parse::<f64>().unwrap_or(f64::NAN);
    Expr::Literal {
        value: LiteralValue::Float(value),
        data_type: fq_common::DataType::Double,
    }
}

/// Convert COUNT(*) / COUNT(expr) / COUNT(DISTINCT expr).
fn convert_count(count: &polyglot_sql::expressions::CountFunc) -> Result<Expr, ParseError> {
    let args = match &count.this {
        Some(inner) => vec![convert_expr(inner)?],
        None => Vec::new(),
    };
    Ok(Expr::FunctionCall {
        function_name: "COUNT".to_string(),
        args,
        is_aggregate: true,
        distinct: count.distinct,
        within_group_key: None,
        within_group_desc: false,
    })
}

/// Convert SUM/AVG/MIN/MAX(expr).
fn convert_aggregate(
    name: &str,
    agg: &polyglot_sql::expressions::AggFunc,
) -> Result<Expr, ParseError> {
    Ok(Expr::FunctionCall {
        function_name: name.to_string(),
        args: vec![convert_expr(&agg.this)?],
        is_aggregate: true,
        distinct: agg.distinct,
        within_group_key: None,
        within_group_desc: false,
    })
}

/// Convert an anonymous (unknown-name) scalar function call. Typed scalar
/// function variants (UPPER, SUBSTRING, ...) are a later increment.
fn convert_anonymous(anon: &polyglot_sql::expressions::Anonymous) -> Result<Expr, ParseError> {
    let name = match anon.this.as_ref() {
        Expression::Column(column) => column.name.name.clone(),
        Expression::Var(var) => var.this.clone(),
        other => {
            return Err(ParseError::Unsupported(format!(
                "function name `{}`",
                other.variant_name()
            )))
        }
    };
    let mut args = Vec::with_capacity(anon.expressions.len());
    for argument in &anon.expressions {
        args.push(convert_expr(argument)?);
    }
    Ok(Expr::FunctionCall {
        function_name: name,
        args,
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    })
}
