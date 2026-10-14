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

/// Scalar functions carried through by name: polyglot's variant name paired with
/// the SQL name the engine renders. Their operands are read generically, so an
/// entry is one line rather than an arm.
///
/// Only plain scalar functions whose operands are positional belong here. Kept
/// out deliberately: aggregates (they need `is_aggregate`, which drives the
/// grouping gates), functions with non-positional syntax such as `POSITION(x IN
/// y)`, and non-deterministic ones such as `RANDOM()` whose result changes per
/// evaluation, so pushing or re-evaluating a call changes the answer.
const PASSTHROUGH_FUNCTIONS: &[(&str, &str)] = &[
    ("sqrt", "SQRT"),
    ("ln", "LN"),
    ("exp", "EXP"),
    ("initcap", "INITCAP"),
    ("sin", "SIN"),
    ("cos", "COS"),
    ("tan", "TAN"),
    ("degrees", "DEGREES"),
    ("radians", "RADIANS"),
    ("sign", "SIGN"),
    ("cbrt", "CBRT"),
    ("year", "YEAR"),
    ("month", "MONTH"),
    ("day", "DAY"),
    ("hour", "HOUR"),
    ("minute", "MINUTE"),
    ("second", "SECOND"),
    ("nvl", "NVL"),
    ("starts_with", "STARTS_WITH"),
    ("ends_with", "ENDS_WITH"),
    ("contains", "CONTAINS"),
    ("levenshtein", "LEVENSHTEIN"),
    ("mod_func", "MOD"),
];

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
            other => self.passthrough_function(other),
        }
    }

    /// Convert a function polyglot gives a dedicated variant but this module has
    /// no bespoke arm for, using the AST's generic operand accessors. The name is
    /// looked up in `PASSTHROUGH_FUNCTIONS`; a variant absent from that table
    /// raises as before.
    ///
    /// Membership is a table rather than a blanket fallback because this arm sees
    /// EVERY expression variant the converter does not handle, not only functions.
    /// `a IS TRUE` and `a & b` arrive here too, and rewriting either as a
    /// `FUNCTION(a, b)` call would change what the query means without an error.
    fn passthrough_function(&self, expr: &Expression) -> Result<Expr, ParseError> {
        let variant = expr.variant_name();
        let Some((_, sql_name)) = PASSTHROUGH_FUNCTIONS
            .iter()
            .find(|(known, _)| *known == variant)
        else {
            return Err(ParseError::Unsupported(format!("function `{variant}`")));
        };
        // Operands in call order: the primary operand, then a binary function's
        // second, then any variadic tail. A variant carries only the ones its
        // shape defines, so the concatenation is the argument list.
        let mut args = Vec::new();
        if let Some(this) = expr.get_this() {
            args.push(self.expr(this)?);
        }
        if let Some(second) = expr.get_expression() {
            args.push(self.expr(second)?);
        }
        for extra in expr.get_expressions() {
            args.push(self.expr(extra)?);
        }
        // Every function in the table takes at least one operand, so an empty
        // list means this variant keeps its operands in fields the generic
        // accessors do not reach. Emitting the call anyway would render
        // `NAME()` and silently drop what the user wrote.
        if args.is_empty() {
            return Err(ParseError::Unsupported(format!(
                "function `{variant}`: operands are not reachable generically"
            )));
        }
        Ok(scalar_function_call((*sql_name).to_string(), args))
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
        if func.name.eq_ignore_ascii_case("unnest") {
            if func.distinct {
                return Err(ParseError::Unsupported("UNNEST DISTINCT".to_string()));
            }
            return Ok(scalar_function_call(func.name.clone(), args));
        }
        // Fresh function call built from the parsed generic Function node - no base to
        // copy from. Field list (function_name/args/is_aggregate/distinct/
        // within_group_key/within_group_desc/filter) is the complete FunctionCall
        // variant. The generic Function node carries no FILTER (that clause only
        // attaches to typed aggregate nodes), so filter is None.
        Ok(Expr::FunctionCall {
            function_name: func.name.to_uppercase(),
            args,
            is_aggregate: false,
            distinct: func.distinct,
            within_group_key: None,
            within_group_desc: false,
            filter: None,
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
    /// explicit LEADING/TRAILING trim raises (the position is not modeled)
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
        // Fresh EXTRACT node built from the parsed field + source - no base to copy
        // from. Field list (field/source) is the complete Extract variant.
        Ok(Expr::Extract {
            field: format!("{:?}", func.field).to_uppercase(),
            source: Box::new(self.expr(&func.this)?),
        })
    }
}
