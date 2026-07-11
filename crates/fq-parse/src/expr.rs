//! polyglot-sql expression -> `fq_plan::Expr` (methods on `Converter`).
//!
//! polyglot's `Expression` is a ~1000-variant enum (a dedicated variant per known
//! function). Exhaustive handling is impossible, so this is an ALLOWLIST: the
//! constructs the engine supports are converted; every other variant RAISES
//! `ParseError::Unsupported` (the Python `SUPPORTED_*_ARGS` posture - fail fast,
//! never silently drop a construct). Expression conversion is a method on
//! `Converter` because the subquery-bearing nodes recurse back into query
//! conversion (which needs the catalog for star expansion).

use fq_plan::expr::LiteralValue;
use fq_plan::{BinaryOpType, ColumnRef, Expr, Quantifier, UnaryOpType};
use polyglot_sql::expressions::{
    AggFunc, Anonymous, Between, BinaryOp, Case, Cast, CountFunc, DataType, Expression, In,
    Literal, QuantifiedExpr,
};

use crate::convert::Converter;
use crate::error::ParseError;

impl Converter<'_> {
    /// Convert one polyglot expression into an engine expression.
    pub(crate) fn expr(&self, expr: &Expression) -> Result<Expr, ParseError> {
        match expr {
            Expression::Column(column) => Ok(Expr::Column(ColumnRef::new(
                column.table.as_ref().map(|ident| ident.name.clone()),
                column.name.name.clone(),
                None,
            ))),
            Expression::Literal(literal) => convert_literal(literal),
            Expression::Boolean(boolean) => Ok(bool_literal(boolean.value)),
            Expression::Null(_) => Ok(Expr::Literal {
                value: LiteralValue::Null,
                data_type: fq_common::DataType::Null,
            }),
            Expression::Paren(paren) => self.expr(&paren.this),
            Expression::Alias(alias) => self.expr(&alias.this),
            Expression::Not(unary) => self.unary(UnaryOpType::Not, &unary.this),
            Expression::Neg(unary) => self.unary(UnaryOpType::Negate, &unary.this),
            Expression::Is(binary) => self.convert_is(binary),
            Expression::Cast(cast) => self.convert_cast(cast),
            Expression::Case(case) => self.convert_case(case),
            Expression::Between(between) => self.convert_between(between),
            Expression::In(in_expr) => self.convert_in(in_expr),
            Expression::Exists(exists) => self.convert_exists(exists),
            Expression::Subquery(subquery) => self.convert_scalar_subquery(subquery),
            Expression::Any(quantified) => self.convert_quantified(Quantifier::Any, quantified),
            Expression::All(quantified) => self.convert_quantified(Quantifier::All, quantified),
            _ => self.convert_binary_or_aggregate(expr),
        }
    }

    /// Second dispatch tier: the binary operators, the aggregate functions, and
    /// the anonymous scalar call, else raise.
    fn convert_binary_or_aggregate(&self, expr: &Expression) -> Result<Expr, ParseError> {
        if let Some(op) = binary_op_type(expr) {
            return self.binary(op, binary_operands(expr));
        }
        match expr {
            Expression::Count(count) => self.convert_count(count),
            Expression::Sum(agg) => self.convert_aggregate("SUM", agg),
            Expression::Avg(agg) => self.convert_aggregate("AVG", agg),
            Expression::Min(agg) => self.convert_aggregate("MIN", agg),
            Expression::Max(agg) => self.convert_aggregate("MAX", agg),
            Expression::Anonymous(anon) => self.convert_anonymous(anon),
            other => self.scalar_function(other),
        }
    }

    /// Build an engine binary op from operands.
    fn binary(&self, op: BinaryOpType, binary: &BinaryOp) -> Result<Expr, ParseError> {
        Ok(Expr::BinaryOp {
            op,
            left: Box::new(self.expr(&binary.left)?),
            right: Box::new(self.expr(&binary.right)?),
        })
    }

    /// Build an engine unary op from an operand.
    fn unary(&self, op: UnaryOpType, operand: &Expression) -> Result<Expr, ParseError> {
        Ok(Expr::UnaryOp {
            op,
            operand: Box::new(self.expr(operand)?),
        })
    }

    /// Convert `x IS NULL` / `x IS NOT NULL`; other IS shapes are not yet handled.
    fn convert_is(&self, binary: &BinaryOp) -> Result<Expr, ParseError> {
        match &binary.right {
            Expression::Null(_) => self.unary(UnaryOpType::IsNull, &binary.left),
            Expression::Not(inner) if matches!(inner.this, Expression::Null(_)) => {
                self.unary(UnaryOpType::IsNotNull, &binary.left)
            }
            other => Err(ParseError::Unsupported(format!(
                "IS <{}>",
                other.variant_name()
            ))),
        }
    }

    /// Convert `CAST(x AS type)` (and `x::type`). The engine keeps the target type
    /// as SQL text so the cast re-renders verbatim to a source.
    fn convert_cast(&self, cast: &Cast) -> Result<Expr, ParseError> {
        Ok(Expr::Cast {
            expr: Box::new(self.expr(&cast.this)?),
            target_type: data_type_sql(&cast.to)?,
            data_type: None,
        })
    }

    /// Convert CASE. A simple CASE (`CASE x WHEN v THEN ...`) is lowered to the
    /// searched form (`CASE WHEN x = v THEN ...`) so the plan carries only one
    /// shape - a function-bearing operand refuses (it would duplicate side
    /// effects across the expanded equalities).
    fn convert_case(&self, case: &Case) -> Result<Expr, ParseError> {
        let operand = match &case.operand {
            Some(operand) => Some(self.expr(operand)?),
            None => None,
        };
        let mut when_clauses = Vec::with_capacity(case.whens.len());
        for (condition, result) in &case.whens {
            let condition = match &operand {
                Some(operand) => searched_condition(operand.clone(), self.expr(condition)?),
                None => self.expr(condition)?,
            };
            when_clauses.push((condition, self.expr(result)?));
        }
        let else_result = match &case.else_ {
            Some(else_expr) => Some(Box::new(self.expr(else_expr)?)),
            None => None,
        };
        Ok(Expr::Case {
            when_clauses,
            else_result,
        })
    }

    /// Convert `x [NOT] BETWEEN low AND high`. NOT wraps the range test.
    fn convert_between(&self, between: &Between) -> Result<Expr, ParseError> {
        if between.symmetric == Some(true) {
            return Err(ParseError::Unsupported("BETWEEN SYMMETRIC".to_string()));
        }
        let range = Expr::Between {
            value: Box::new(self.expr(&between.this)?),
            lower: Box::new(self.expr(&between.low)?),
            upper: Box::new(self.expr(&between.high)?),
        };
        Ok(negate_if(between.not, range))
    }

    /// Convert `x [NOT] IN (list)` or `x [NOT] IN (subquery)`.
    fn convert_in(&self, in_expr: &In) -> Result<Expr, ParseError> {
        let value = self.expr(&in_expr.this)?;
        if let Some(query) = &in_expr.query {
            return Ok(Expr::InSubquery {
                value: Box::new(value),
                subquery: Box::new(self.query(query)?),
                negated: in_expr.not,
            });
        }
        let mut options = Vec::with_capacity(in_expr.expressions.len());
        for option in &in_expr.expressions {
            options.push(self.expr(option)?);
        }
        let list = Expr::InList {
            value: Box::new(value),
            options,
        };
        Ok(negate_if(in_expr.not, list))
    }

    /// Convert `[NOT] EXISTS (subquery)`.
    fn convert_exists(
        &self,
        exists: &polyglot_sql::expressions::Exists,
    ) -> Result<Expr, ParseError> {
        Ok(Expr::Exists {
            subquery: Box::new(self.query(&exists.this)?),
            negated: exists.not,
        })
    }

    /// Convert a scalar subquery `(SELECT ...)` used as a value.
    fn convert_scalar_subquery(
        &self,
        subquery: &polyglot_sql::expressions::Subquery,
    ) -> Result<Expr, ParseError> {
        Ok(Expr::Subquery {
            subquery: Box::new(self.query(&subquery.this)?),
        })
    }

    /// Convert `left op ANY/ALL (subquery)`.
    fn convert_quantified(
        &self,
        quantifier: Quantifier,
        quantified: &QuantifiedExpr,
    ) -> Result<Expr, ParseError> {
        let operator =
            quantified.op.as_ref().map(quantified_op).ok_or_else(|| {
                ParseError::Unsupported("quantified comparison operator".to_string())
            })?;
        Ok(Expr::QuantifiedComparison {
            operator,
            quantifier,
            left: Box::new(self.expr(&quantified.this)?),
            subquery: Box::new(self.query(&quantified.subquery)?),
        })
    }

    /// Convert COUNT(*) / COUNT(expr) / COUNT(DISTINCT expr).
    fn convert_count(&self, count: &CountFunc) -> Result<Expr, ParseError> {
        let args = match &count.this {
            Some(inner) => vec![self.expr(inner)?],
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
    fn convert_aggregate(&self, name: &str, agg: &AggFunc) -> Result<Expr, ParseError> {
        Ok(Expr::FunctionCall {
            function_name: name.to_string(),
            args: vec![self.expr(&agg.this)?],
            is_aggregate: true,
            distinct: agg.distinct,
            within_group_key: None,
            within_group_desc: false,
        })
    }

    /// Convert an anonymous (unknown-name) scalar function call. Typed scalar
    /// function variants (UPPER, SUBSTRING, ...) are handled in `functions.rs`.
    fn convert_anonymous(&self, anon: &Anonymous) -> Result<Expr, ParseError> {
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
            args.push(self.expr(argument)?);
        }
        Ok(scalar_function_call(name, args))
    }
}

/// Build a scalar function-call expression from a name and converted args.
pub(crate) fn scalar_function_call(name: String, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall {
        function_name: name,
        args,
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    }
}

/// Wrap an expression in NOT when `negate` is set.
fn negate_if(negate: bool, expr: Expr) -> Expr {
    if negate {
        Expr::UnaryOp {
            op: UnaryOpType::Not,
            operand: Box::new(expr),
        }
    } else {
        expr
    }
}

/// A boolean literal expression.
fn bool_literal(value: bool) -> Expr {
    Expr::Literal {
        value: LiteralValue::Boolean(value),
        data_type: fq_common::DataType::Boolean,
    }
}

/// The searched-form condition for one simple-CASE branch: `operand = value`.
fn searched_condition(operand: Expr, value: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left: Box::new(operand),
        right: Box::new(value),
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

/// The `BinaryOp` of any of the `Box<BinaryOp>` variants handled above.
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

/// Map a polyglot quantified-comparison operator to the engine binary operator.
fn quantified_op(op: &polyglot_sql::expressions::QuantifiedOp) -> BinaryOpType {
    use polyglot_sql::expressions::QuantifiedOp;
    match op {
        QuantifiedOp::Eq => BinaryOpType::Eq,
        QuantifiedOp::Neq => BinaryOpType::Neq,
        QuantifiedOp::Lt => BinaryOpType::Lt,
        QuantifiedOp::Lte => BinaryOpType::Lte,
        QuantifiedOp::Gt => BinaryOpType::Gt,
        QuantifiedOp::Gte => BinaryOpType::Gte,
    }
}

/// Convert a numeric/string literal.
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

/// Render a polyglot `DataType` back to SQL type text for a CAST target. Covers
/// the types the engine models; an exotic cast target raises rather than
/// rendering something the source would reject.
fn data_type_sql(data_type: &DataType) -> Result<String, ParseError> {
    use DataType as D;
    let text = match data_type {
        D::Boolean => "BOOLEAN".to_string(),
        D::TinyInt { .. } => "TINYINT".to_string(),
        D::SmallInt { .. } => "SMALLINT".to_string(),
        D::Int { .. } => "INTEGER".to_string(),
        D::BigInt { .. } => "BIGINT".to_string(),
        D::Float { .. } => "FLOAT".to_string(),
        D::Double { .. } => "DOUBLE".to_string(),
        D::Decimal { precision, scale } => decimal_sql(*precision, *scale),
        D::Char { length } => sized("CHAR", *length),
        D::VarChar { length, .. } => sized("VARCHAR", *length),
        D::String { .. } => "VARCHAR".to_string(),
        D::Text | D::TextWithLength { .. } => "TEXT".to_string(),
        D::Date => "DATE".to_string(),
        D::Time { .. } => "TIME".to_string(),
        D::Timestamp { .. } => "TIMESTAMP".to_string(),
        D::Uuid => "UUID".to_string(),
        other => {
            return Err(ParseError::Unsupported(format!(
                "CAST target type {other:?}"
            )))
        }
    };
    Ok(text)
}

/// `DECIMAL`, `DECIMAL(p)`, or `DECIMAL(p, s)` from optional precision/scale.
fn decimal_sql(precision: Option<u32>, scale: Option<u32>) -> String {
    match (precision, scale) {
        (Some(precision), Some(scale)) => format!("DECIMAL({precision}, {scale})"),
        (Some(precision), None) => format!("DECIMAL({precision})"),
        _ => "DECIMAL".to_string(),
    }
}

/// A type name with an optional `(length)` suffix.
fn sized(name: &str, length: Option<u32>) -> String {
    match length {
        Some(length) => format!("{name}({length})"),
        None => name.to_string(),
    }
}
