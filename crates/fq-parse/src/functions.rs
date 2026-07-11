//! Typed scalar functions -> `fq_plan::Expr` (methods on `Converter`).
//!
//! polyglot gives each known function its own AST variant (UPPER -> `Upper`,
//! SUBSTRING -> `Substring`, ...), so there is no generic name+args accessor.
//! This module maps the functions the engine and the benchmark corpus use into a
//! generic `FunctionCall` (or `Expr::Extract`), preserving the SQL name so the
//! call re-renders to the source. An unhandled function raises
//! `ParseError::Unsupported` with its variant name, never silently dropped.

use fq_plan::Expr;
use polyglot_sql::expressions::Expression;

use crate::convert::Converter;
use crate::error::ParseError;
use crate::expr::scalar_function_call;

impl Converter<'_> {
    /// Convert a typed scalar-function expression. Handles the corpus set
    /// (COALESCE, SUBSTRING, ROUND, NULLIF, REPLACE, EXTRACT, UPPER/LOWER, ABS,
    /// LENGTH, TRIM, CEIL/FLOOR, LEFT/RIGHT); anything else raises.
    pub(crate) fn scalar_function(&self, expr: &Expression) -> Result<Expr, ParseError> {
        match expr {
            // Single-argument functions (UnaryFunc { this }).
            Expression::Upper(func) => self.named_call("UPPER", &[&func.this]),
            Expression::Lower(func) => self.named_call("LOWER", &[&func.this]),
            Expression::Length(func) => self.named_call("LENGTH", &[&func.this]),
            Expression::Abs(func) => self.named_call("ABS", &[&func.this]),
            Expression::Ceil(func) => self.named_call("CEIL", &[&func.this]),
            Expression::Floor(func) => self.named_call("FLOOR", &[&func.this]),
            // Variable / multi-argument functions.
            Expression::Coalesce(func) => {
                let args: Vec<&Expression> = func.expressions.iter().collect();
                self.named_call("COALESCE", &args)
            }
            Expression::NullIf(func) => self.named_call("NULLIF", &[&func.this, &func.expression]),
            Expression::Replace(func) => {
                self.named_call("REPLACE", &[&func.this, &func.old, &func.new])
            }
            Expression::Substring(func) => self.convert_substring(func),
            Expression::Round(func) => self.convert_round(func),
            Expression::Trim(func) => self.convert_trim(func),
            Expression::Left(func) => self.named_call("LEFT", &[&func.this, &func.length]),
            Expression::Right(func) => self.named_call("RIGHT", &[&func.this, &func.length]),
            Expression::Extract(func) => self.convert_extract(func),
            // The generic function node (name + args), for functions with no
            // dedicated variant.
            Expression::Function(func) => self.convert_generic_function(func),
            other => Err(ParseError::Unsupported(format!(
                "function `{}`",
                other.variant_name()
            ))),
        }
    }

    /// Convert the generic `Function { name, args, distinct }` node into a
    /// `FunctionCall`, preserving the name (uppercased) so it re-renders.
    fn convert_generic_function(
        &self,
        func: &polyglot_sql::expressions::Function,
    ) -> Result<Expr, ParseError> {
        let mut args = Vec::with_capacity(func.args.len());
        for arg in &func.args {
            args.push(self.expr(arg)?);
        }
        Ok(Expr::FunctionCall {
            function_name: func.name.to_uppercase(),
            args,
            is_aggregate: false,
            distinct: func.distinct,
            within_group_key: None,
            within_group_desc: false,
        })
    }

    /// Build a `FunctionCall` from a SQL name and a list of argument expressions.
    fn named_call(&self, name: &str, args: &[&Expression]) -> Result<Expr, ParseError> {
        let mut converted = Vec::with_capacity(args.len());
        for arg in args {
            converted.push(self.expr(arg)?);
        }
        Ok(scalar_function_call(name.to_string(), converted))
    }

    /// SUBSTRING(this FROM start [FOR length]).
    fn convert_substring(
        &self,
        func: &polyglot_sql::expressions::SubstringFunc,
    ) -> Result<Expr, ParseError> {
        let mut args = vec![self.expr(&func.this)?, self.expr(&func.start)?];
        if let Some(length) = &func.length {
            args.push(self.expr(length)?);
        }
        Ok(scalar_function_call("SUBSTRING".to_string(), args))
    }

    /// ROUND(this [, decimals]).
    fn convert_round(
        &self,
        func: &polyglot_sql::expressions::RoundFunc,
    ) -> Result<Expr, ParseError> {
        let mut args = vec![self.expr(&func.this)?];
        if let Some(decimals) = &func.decimals {
            args.push(self.expr(decimals)?);
        }
        Ok(scalar_function_call("ROUND".to_string(), args))
    }

    /// TRIM(this [, characters]). The default both-sides trim is supported; an
    /// explicit LEADING/TRAILING trim raises (the position is not yet modeled)
    /// rather than silently dropping it.
    fn convert_trim(&self, func: &polyglot_sql::expressions::TrimFunc) -> Result<Expr, ParseError> {
        if func.position != polyglot_sql::expressions::TrimPosition::Both {
            return Err(ParseError::Unsupported("LEADING/TRAILING TRIM".to_string()));
        }
        let mut args = vec![self.expr(&func.this)?];
        if let Some(characters) = &func.characters {
            args.push(self.expr(characters)?);
        }
        Ok(scalar_function_call("TRIM".to_string(), args))
    }

    /// EXTRACT(field FROM source) -> the engine's `Extract` node.
    fn convert_extract(
        &self,
        func: &polyglot_sql::expressions::ExtractFunc,
    ) -> Result<Expr, ParseError> {
        Ok(Expr::Extract {
            field: format!("{:?}", func.field).to_uppercase(),
            source: Box::new(self.expr(&func.this)?),
        })
    }
}
