//! Expression binding: resolve every `ColumnRef` leaf and re-type a Cast, leaving
//! the tree shape otherwise unchanged.
//!
//! A fallible recursive rebuild (not `map_children`, which is infallible) so a
//! resolution error propagates. Ports the `_bind_expr_dispatch` /
//! `_bind_compound_expr` path. A subquery-bearing expression binds through
//! `bind_subquery_expr`, which rebinds its subplan with the enclosing scopes
//! visible.

use fq_catalog::map_native_type_default;
use fq_common::DataType;
use fq_plan::Expr;

use crate::binder::Binder;
use crate::error::BindError;

/// Resolve a CAST's SQL target-type text to an engine type; an unmodeled target
/// raises (matching binder.py) rather than leaving an untyped Cast.
pub(crate) fn cast_target_type(target_type: &str) -> Result<DataType, BindError> {
    map_native_type_default(target_type)
        .map_err(|_| BindError::Unsupported(format!("CAST target type '{target_type}'")))
}

/// The engine's division semantics are DuckDB's: `/` over numeric operands is
/// DOUBLE division (1 / 2 = 0.5, and a DECIMAL dividend divides at full double
/// precision), on every source and on the coordinator. A verbatim division is
/// evaluated by each source's own rules instead - most dialects truncate
/// int / int, and DataFusion divides DECIMAL operands at a fixed result scale
/// that truncates the true quotient - so a bound numeric division is rewritten
/// here, the one place every plan passes through, into
/// `CAST(left AS DOUBLE) / right`, a form with the SAME meaning in every
/// dialect the engine renders to. A DOUBLE-typed operand is not exempt: a
/// DECIMAL source column is modeled as DOUBLE, so only the explicit cast
/// guarantees the source divides the decimal it actually stores as a double.
/// Any other operator, a division already carrying the cast, and any division
/// whose operand types this cannot see through, are returned unchanged.
fn normalize_division(op: fq_plan::BinaryOpType, left: Expr, right: Expr) -> Expr {
    let rewrite = op == fq_plan::BinaryOpType::Divide
        && certainly_numeric(&left)
        && certainly_numeric(&right);
    let left = if rewrite { cast_to_double(left) } else { left };
    // Fresh bound copy assembled from the already-bound operands the caller
    // handed in (enum variants take no `..base`). Field list (op/left/right)
    // is complete.
    Expr::BinaryOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Whether a BOUND expression is integer-typed beyond doubt. Conservative by
/// construction: only shapes whose type is structurally certain count, so a
/// shape this cannot see through (a subquery, a window, a function outside
/// the known-integer set) keeps its written division instead of gaining a
/// cast on a guess.
fn certainly_integer(expr: &Expr) -> bool {
    match expr {
        Expr::Column(column) => integer_type(column.data_type.as_ref()),
        Expr::Literal { data_type, .. } => integer_type(Some(data_type)),
        Expr::Cast { data_type, .. } => integer_type(data_type.as_ref()),
        Expr::UnaryOp {
            op: fq_plan::UnaryOpType::Negate,
            operand,
        } => certainly_integer(operand),
        Expr::BinaryOp { op, left, right } => {
            integer_arithmetic(*op) && certainly_integer(left) && certainly_integer(right)
        }
        Expr::FunctionCall {
            function_name,
            args,
            ..
        } => integer_function(function_name, args),
        Expr::Case {
            when_clauses,
            else_result,
        } => integer_case(when_clauses, else_result.as_deref()),
        _ => false,
    }
}

/// Whether a function call is integer-typed beyond doubt: `count` always is,
/// the string-measuring functions return integers in every dialect the engine
/// renders to, and the aggregates/functions that carry their argument's type
/// are integer when every argument certainly is.
fn integer_function(function_name: &str, args: &[Expr]) -> bool {
    let name = function_name.to_ascii_lowercase();
    if matches!(
        name.as_str(),
        "count" | "length" | "char_length" | "character_length" | "strpos" | "position"
    ) {
        return true;
    }
    if matches!(name.as_str(), "sum" | "min" | "max" | "abs") {
        return !args.is_empty() && args.iter().all(certainly_integer);
    }
    false
}

/// Whether every branch a CASE can produce is certainly integer. A missing
/// ELSE produces NULL, which the division rewrite's cast carries unchanged, so
/// it does not disqualify the CASE.
fn integer_case(when_clauses: &[(Expr, Expr)], else_result: Option<&Expr>) -> bool {
    if when_clauses.is_empty() {
        return false;
    }
    let branches_integer = when_clauses
        .iter()
        .all(|(_when, then)| certainly_integer(then));
    branches_integer && else_result.is_none_or(certainly_integer)
}

/// Whether this operator keeps integer operands integer.
fn integer_arithmetic(op: fq_plan::BinaryOpType) -> bool {
    matches!(
        op,
        fq_plan::BinaryOpType::Add
            | fq_plan::BinaryOpType::Subtract
            | fq_plan::BinaryOpType::Multiply
            | fq_plan::BinaryOpType::Modulo
    )
}

/// Whether the resolved type is one of the integer types.
fn integer_type(data_type: Option<&DataType>) -> bool {
    matches!(data_type, Some(DataType::Integer | DataType::BigInt))
}

/// Whether a BOUND expression is numeric beyond doubt: certainly integer or
/// certainly double. The gate the division rewrite requires of both operands.
fn certainly_numeric(expr: &Expr) -> bool {
    certainly_integer(expr) || certainly_double(expr)
}

/// Whether a BOUND expression is DOUBLE-typed beyond doubt. A DECIMAL source
/// column is modeled as DOUBLE, so a certainly-double shape may be backed by a
/// decimal at the source - which is exactly why the normalizations that call
/// this must force double evaluation explicitly. Conservative by construction,
/// like `certainly_integer`: a shape this cannot see through (a subquery, a
/// window, a function outside the known set) is not certain.
fn certainly_double(expr: &Expr) -> bool {
    match expr {
        Expr::Column(column) => double_type(column.data_type.as_ref()),
        Expr::Literal { data_type, .. } => double_type(Some(data_type)),
        Expr::Cast { data_type, .. } => double_type(data_type.as_ref()),
        Expr::UnaryOp {
            op: fq_plan::UnaryOpType::Negate,
            operand,
        } => certainly_double(operand),
        Expr::BinaryOp { op, left, right } => {
            double_arithmetic(*op)
                && certainly_numeric(left)
                && certainly_numeric(right)
                && (certainly_double(left) || certainly_double(right))
        }
        Expr::FunctionCall {
            function_name,
            args,
            ..
        } => double_function(function_name, args),
        Expr::Case {
            when_clauses,
            else_result,
        } => double_case(when_clauses, else_result.as_deref()),
        _ => false,
    }
}

/// Whether a function call is DOUBLE-typed beyond doubt: `avg` always is (the
/// engine derives every average as DOUBLE), and the aggregates/functions that
/// carry their argument's type are double when every argument certainly is.
fn double_function(function_name: &str, args: &[Expr]) -> bool {
    let name = function_name.to_ascii_lowercase();
    if name == "avg" {
        return args.len() == 1;
    }
    if matches!(name.as_str(), "sum" | "min" | "max" | "abs") {
        return !args.is_empty() && args.iter().all(certainly_double);
    }
    false
}

/// Whether every branch a CASE can produce is certainly double. A missing ELSE
/// produces NULL, which double arithmetic carries unchanged, so it does not
/// disqualify the CASE.
fn double_case(when_clauses: &[(Expr, Expr)], else_result: Option<&Expr>) -> bool {
    if when_clauses.is_empty() {
        return false;
    }
    let branches_double = when_clauses
        .iter()
        .all(|(_when, then)| certainly_double(then));
    branches_double && else_result.is_none_or(certainly_double)
}

/// Whether this operator's result is double when one numeric operand is.
fn double_arithmetic(op: fq_plan::BinaryOpType) -> bool {
    matches!(
        op,
        fq_plan::BinaryOpType::Add
            | fq_plan::BinaryOpType::Subtract
            | fq_plan::BinaryOpType::Multiply
            | fq_plan::BinaryOpType::Divide
    )
}

/// Whether the resolved type is DOUBLE.
fn double_type(data_type: Option<&DataType>) -> bool {
    matches!(data_type, Some(DataType::Double))
}

/// Whether an expression is already an explicit cast to a double-producing SQL
/// type, i.e. already the normalized form the division rewrite would create.
fn is_double_cast(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Cast { target_type, .. }
            if target_type.to_ascii_uppercase().contains("DOUBLE")
    )
}

/// Wrap a bound expression in an explicit DOUBLE cast, the one form every
/// dialect and the coordinator evaluate as IEEE double. Returns the expression
/// unchanged when it already is that cast.
fn cast_to_double(expr: Expr) -> Expr {
    if is_double_cast(&expr) {
        return expr;
    }
    // A genuinely fresh Cast synthesized around the bound expression - there
    // is no base node to copy from. Field list (expr/target_type/data_type)
    // is complete.
    Expr::Cast {
        expr: Box::new(expr),
        target_type: "DOUBLE".to_string(),
        data_type: Some(DataType::Double),
    }
}

/// Apply the engine's expression semantics - DuckDB's - to a bound function
/// call. AVG over a certainly-double argument gains an explicit DOUBLE cast so
/// every source averages at double precision (a DECIMAL column is modeled as
/// DOUBLE, and DataFusion's avg over the decimal a source actually stores
/// yields a fixed-scale decimal that truncates the true average). round() over
/// a certainly-double argument is rewritten into half-away-from-zero
/// arithmetic, because PostgreSQL rounds a double half to even. Every other
/// call is returned unchanged.
fn normalize_function_call(call: Expr) -> Expr {
    if let Some(rewritten) = normalize_avg(&call) {
        return rewritten;
    }
    if let Some(rewritten) = normalize_round(&call) {
        return rewritten;
    }
    call
}

/// Rewrite `avg(x)` over a certainly-double `x` into `avg(CAST(x AS DOUBLE))`.
/// The cast is a value-preserving no-op over a genuine double, and over a
/// decimal-backed column it forces the double-precision average the engine
/// defines. An avg already carrying the cast, or over any other shape, is left
/// for the fragment oracle to compare.
fn normalize_avg(call: &Expr) -> Option<Expr> {
    let Expr::FunctionCall {
        function_name,
        args,
        is_aggregate,
        distinct,
        within_group_key,
        within_group_desc,
        filter,
    } = call
    else {
        return None;
    };
    let applies = function_name.eq_ignore_ascii_case("avg")
        && args.len() == 1
        && certainly_double(&args[0])
        && !is_double_cast(&args[0]);
    if !applies {
        return None;
    }
    // Fresh copy of the bound call with its single argument wrapped in the
    // DOUBLE cast; every other field is cloned from the source call. Field
    // list (function_name/args/is_aggregate/distinct/within_group_key/
    // within_group_desc/filter) is complete.
    Some(Expr::FunctionCall {
        function_name: function_name.clone(),
        args: vec![cast_to_double(args[0].clone())],
        is_aggregate: *is_aggregate,
        distinct: *distinct,
        within_group_key: within_group_key.clone(),
        within_group_desc: *within_group_desc,
        filter: filter.clone(),
    })
}

/// Rewrite `round(x)` / `round(x, n)` over a certainly-double `x` into
/// half-away-from-zero arithmetic (see `half_away_from_zero`). Only the shapes
/// whose meaning is structurally certain rewrite: a plain scalar call whose
/// scale, when present, is an integer literal in 0..=15 (the range where
/// 10^n is an exact double). Everything else keeps its written form.
fn normalize_round(call: &Expr) -> Option<Expr> {
    let Expr::FunctionCall {
        function_name,
        args,
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        filter: None,
        ..
    } = call
    else {
        return None;
    };
    let applies = function_name.eq_ignore_ascii_case("round")
        && !args.is_empty()
        && certainly_double(&args[0]);
    if !applies {
        return None;
    }
    match args.len() {
        1 => Some(half_away_from_zero(cast_to_double(args[0].clone()))),
        2 => normalize_scaled_round(&args[0], &args[1]),
        _ => None,
    }
}

/// Rewrite a two-argument round when its scale is an integer literal: scale 0
/// is plain rounding, and a scale in 1..=15 rounds `x * 10^n` and divides the
/// result back down, matching DuckDB's own scaled rounding of a double.
fn normalize_scaled_round(value: &Expr, scale: &Expr) -> Option<Expr> {
    let Expr::Literal {
        value: fq_plan::LiteralValue::Integer(digits),
        ..
    } = scale
    else {
        return None;
    };
    let base = cast_to_double(value.clone());
    if *digits == 0 {
        return Some(half_away_from_zero(base));
    }
    if !(1..=15).contains(digits) {
        return None;
    }
    let power = double_literal(10f64.powi(*digits as i32));
    let scaled = binary(fq_plan::BinaryOpType::Multiply, base, power.clone());
    let rounded = half_away_from_zero(scaled);
    Some(binary(fq_plan::BinaryOpType::Divide, rounded, power))
}

/// The half-away-from-zero rounding of a double, as arithmetic every dialect
/// and the coordinator evaluate bit-identically:
///
///   CASE WHEN TRUNC(x) = x THEN x
///        ELSE TRUNC(x) + TRUNC((x - TRUNC(x)) * 2) END
///
/// The fractional remainder `x - TRUNC(x)` is exact in IEEE arithmetic, so
/// doubling it crosses +-1 exactly when the fraction reaches one half; the
/// guard branch returns an integral, infinite, or NaN input unchanged, exactly
/// as DuckDB's native round does.
fn half_away_from_zero(value: Expr) -> Expr {
    let away = binary(
        fq_plan::BinaryOpType::Add,
        trunc(value.clone()),
        trunc(binary(
            fq_plan::BinaryOpType::Multiply,
            binary(
                fq_plan::BinaryOpType::Subtract,
                value.clone(),
                trunc(value.clone()),
            ),
            integer_literal(2),
        )),
    );
    let already_integral = binary(
        fq_plan::BinaryOpType::Eq,
        trunc(value.clone()),
        value.clone(),
    );
    // A genuinely fresh CASE assembled from the parts built above - there is
    // no base node to copy from. Field list (when_clauses/else_result) is
    // complete.
    Expr::Case {
        when_clauses: vec![(already_integral, value)],
        else_result: Some(Box::new(away)),
    }
}

/// A fresh scalar TRUNC call over one bound argument.
fn trunc(argument: Expr) -> Expr {
    // A genuinely fresh scalar call synthesized by the round rewrite - there
    // is no base node to copy from. Field list (function_name/args/
    // is_aggregate/distinct/within_group_key/within_group_desc/filter) is
    // complete.
    Expr::FunctionCall {
        function_name: "trunc".to_string(),
        args: vec![argument],
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
        filter: None,
    }
}

/// A fresh binary operation over two bound operands.
fn binary(op: fq_plan::BinaryOpType, left: Expr, right: Expr) -> Expr {
    // A genuinely fresh operation synthesized by the round rewrite - there is
    // no base node to copy from. Field list (op/left/right) is complete.
    Expr::BinaryOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// A fresh integer literal.
fn integer_literal(value: i64) -> Expr {
    // A genuinely fresh literal synthesized by the round rewrite - there is no
    // base node to copy from. Field list (value/data_type) is complete.
    Expr::Literal {
        value: fq_plan::LiteralValue::Integer(value),
        data_type: DataType::Integer,
    }
}

/// A fresh double literal.
fn double_literal(value: f64) -> Expr {
    // A genuinely fresh literal synthesized by the round rewrite - there is no
    // base node to copy from. Field list (value/data_type) is complete.
    Expr::Literal {
        value: fq_plan::LiteralValue::Float(value),
        data_type: DataType::Double,
    }
}

impl Binder<'_> {
    /// Bind one expression against the current scope chain.
    pub(crate) fn bind_expr(&mut self, expr: &Expr) -> Result<Expr, BindError> {
        match expr {
            Expr::Column(column) => {
                // A bare reference to an output alias (HAVING/ORDER BY over a
                // Projection/Aggregate) resolves to that output, not a base column.
                if column.table.is_none() {
                    if let Some(data_type) = self.output_alias_type(&column.column) {
                        return Ok(Expr::Column(fq_plan::ColumnRef::new(
                            None,
                            column.column.clone(),
                            Some(data_type),
                        )));
                    }
                }
                self.resolve_in_scopes(column)
            }
            Expr::Literal { .. } | Expr::Interval { .. } => Ok(expr.clone()),
            // Fresh bound copy: the source is a `&Expr` borrow and an Expr enum variant
            // takes no `..base`/`update!`, so it is rebuilt from its rebound operands
            // with `op` copied from the source. Field list (op/left/right) is complete.
            // Division is then normalized to the engine's semantics (see
            // `normalize_division`).
            Expr::BinaryOp { op, left, right } => Ok(normalize_division(
                *op,
                self.bind_expr(left)?,
                self.bind_expr(right)?,
            )),
            // Fresh bound copy from the borrowed source (no `..base` on an enum variant):
            // rebound operand, `op` copied. Field list (op/operand) is complete.
            Expr::UnaryOp { op, operand } => Ok(Expr::UnaryOp {
                op: *op,
                operand: Box::new(self.bind_expr(operand)?),
            }),
            Expr::Cast {
                expr: inner,
                target_type,
                ..
            } => {
                let bound_inner = Box::new(self.bind_expr(inner)?);
                // Resolve the engine type from the SQL target-type text. An
                // unmodeled cast target RAISES (matching binder.py) rather than
                // leaving an untyped Cast, which would later panic in get_type.
                let data_type = Some(crate::expr::cast_target_type(target_type)?);
                // Fresh bound copy from the borrowed source (no `..base` on an enum
                // variant): expr rebound, target_type copied, data_type resolved above.
                // Field list (expr/target_type/data_type) is complete.
                Ok(Expr::Cast {
                    expr: bound_inner,
                    target_type: target_type.clone(),
                    data_type,
                })
            }
            Expr::Between {
                value,
                lower,
                upper,
            } => {
                // Fresh bound copy from the borrowed source (no `..base` on an enum
                // variant): all three operands rebound. Field list (value/lower/upper)
                // is complete.
                Ok(Expr::Between {
                    value: Box::new(self.bind_expr(value)?),
                    lower: Box::new(self.bind_expr(lower)?),
                    upper: Box::new(self.bind_expr(upper)?),
                })
            }
            // Fresh bound copy from the borrowed source (no `..base` on an enum variant):
            // value and options rebound. Field list (value/options) is complete.
            Expr::InList { value, options } => Ok(Expr::InList {
                value: Box::new(self.bind_expr(value)?),
                options: self.bind_expr_list(options)?,
            }),
            Expr::Like {
                case_insensitive,
                expr: inner,
                pattern,
                escape,
            } => {
                // Fresh bound copy from the borrowed source (no `..base` on an enum
                // variant): both operands rebound, case_insensitive and escape copied.
                // Field list (case_insensitive/expr/pattern/escape) is complete.
                Ok(Expr::Like {
                    case_insensitive: *case_insensitive,
                    expr: Box::new(self.bind_expr(inner)?),
                    pattern: Box::new(self.bind_expr(pattern)?),
                    escape: escape.clone(),
                })
            }
            Expr::Case {
                when_clauses,
                else_result,
            } => self.bind_case(when_clauses, else_result.as_deref()),
            Expr::FunctionCall { .. } => self.bind_function_call(expr),
            // Fresh bound copy from the borrowed source (no `..base` on an enum variant):
            // source rebound, `field` copied. Field list (field/source) is complete.
            Expr::Extract { field, source } => Ok(Expr::Extract {
                field: field.clone(),
                source: Box::new(self.bind_expr(source)?),
            }),
            Expr::Window { .. } => self.bind_window(expr),
            // Fresh bound copy from the borrowed source (no `..base` on an enum variant):
            // items rebound. Field list (items) is complete.
            Expr::Tuple { items } => Ok(Expr::Tuple {
                items: self.bind_expr_list(items)?,
            }),
            Expr::Subquery { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. } => self.bind_subquery_expr(expr),
        }
    }

    /// Bind a subquery-bearing expression: its subplan binds with the enclosing
    /// scopes visible (correlated refs resolve), and the outer operand (an IN
    /// value, a quantified left side) binds normally.
    fn bind_subquery_expr(&mut self, expr: &Expr) -> Result<Expr, BindError> {
        match expr {
            // Fresh bound copy from the borrowed source (no `..base` on an enum variant):
            // subplan rebound. Field list (subquery) is complete.
            Expr::Subquery { subquery } => Ok(Expr::Subquery {
                subquery: Box::new(self.bind_subplan(subquery)?),
            }),
            // Fresh bound copy from the borrowed source (no `..base` on an enum variant):
            // subplan rebound, `negated` copied. Field list (subquery/negated) is complete.
            Expr::Exists { subquery, negated } => Ok(Expr::Exists {
                subquery: Box::new(self.bind_subplan(subquery)?),
                negated: *negated,
            }),
            Expr::InSubquery {
                value,
                subquery,
                negated,
            } => {
                let bound_value = Box::new(self.bind_expr(value)?);
                let bound_subquery = Box::new(self.bind_subplan(subquery)?);
                // Fresh bound copy from the borrowed source (no `..base` on an enum
                // variant): value and subplan rebound, `negated` copied. Field list
                // (value/subquery/negated) is complete.
                Ok(Expr::InSubquery {
                    value: bound_value,
                    subquery: bound_subquery,
                    negated: *negated,
                })
            }
            Expr::QuantifiedComparison {
                operator,
                quantifier,
                left,
                subquery,
            } => {
                let bound_left = Box::new(self.bind_expr(left)?);
                let bound_subquery = Box::new(self.bind_subplan(subquery)?);
                // Fresh bound copy from the borrowed source (no `..base` on an enum
                // variant): left and subplan rebound; operator/quantifier copied. Field
                // list (operator/quantifier/left/subquery) is complete.
                Ok(Expr::QuantifiedComparison {
                    operator: *operator,
                    quantifier: *quantifier,
                    left: bound_left,
                    subquery: bound_subquery,
                })
            }
            _ => unreachable!("bind_subquery_expr called on a non-subquery expression"),
        }
    }

    /// Bind a scalar/aggregate function call: rebind its args and WITHIN GROUP key.
    fn bind_function_call(&mut self, expr: &Expr) -> Result<Expr, BindError> {
        let Expr::FunctionCall {
            function_name,
            args,
            is_aggregate,
            distinct,
            within_group_key,
            within_group_desc,
            filter,
        } = expr
        else {
            unreachable!("bind_function_call called on a non-FunctionCall expression");
        };
        let mut bound_args = self.bind_expr_list(args)?;
        crate::nested::validate_nested_call(function_name, &mut bound_args)?;
        let bound_within_group = self.bind_opt(within_group_key.as_deref())?;
        let bound_filter = self.bind_opt(filter.as_deref())?;
        // Fresh bound copy from the borrowed source (no `..base` on an enum variant):
        // args, within_group_key, and the FILTER predicate rebound; function_name and
        // the is_aggregate/distinct/within_group_desc flags copied. Field list
        // (function_name/args/is_aggregate/distinct/within_group_key/
        // within_group_desc/filter) is complete. The bound call is then
        // normalized to the engine's expression semantics (see
        // `normalize_function_call`).
        Ok(normalize_function_call(Expr::FunctionCall {
            function_name: function_name.clone(),
            args: bound_args,
            is_aggregate: *is_aggregate,
            distinct: *distinct,
            within_group_key: bound_within_group,
            within_group_desc: *within_group_desc,
            filter: bound_filter,
        }))
    }

    /// Bind a window expression: rebind its function, PARTITION BY, and ORDER BY.
    fn bind_window(&mut self, expr: &Expr) -> Result<Expr, BindError> {
        let Expr::Window {
            function,
            partition_by,
            order_keys,
            order_ascending,
            order_nulls,
            frame,
        } = expr
        else {
            unreachable!("bind_window called on a non-Window expression");
        };
        let bound_function = Box::new(self.bind_expr(function)?);
        let bound_partition = self.bind_expr_list(partition_by)?;
        let bound_order = self.bind_expr_list(order_keys)?;
        // Fresh bound copy from the borrowed source (no `..base` on an enum variant):
        // function/partition_by/order_keys rebound; the order_ascending, order_nulls,
        // and frame descriptors copied. Field list (function/partition_by/order_keys/
        // order_ascending/order_nulls/frame) is complete.
        Ok(Expr::Window {
            function: bound_function,
            partition_by: bound_partition,
            order_keys: bound_order,
            order_ascending: order_ascending.clone(),
            order_nulls: order_nulls.clone(),
            frame: frame.clone(),
        })
    }

    /// Bind each expression in a list.
    pub(crate) fn bind_expr_list(&mut self, exprs: &[Expr]) -> Result<Vec<Expr>, BindError> {
        let mut bound = Vec::with_capacity(exprs.len());
        for expr in exprs {
            bound.push(self.bind_expr(expr)?);
        }
        Ok(bound)
    }

    /// Bind an optional (boxed) expression.
    fn bind_opt(&mut self, expr: Option<&Expr>) -> Result<Option<Box<Expr>>, BindError> {
        match expr {
            Some(expr) => Ok(Some(Box::new(self.bind_expr(expr)?))),
            None => Ok(None),
        }
    }

    /// Bind a CASE's branch conditions/results and its ELSE.
    fn bind_case(
        &mut self,
        when_clauses: &[(Expr, Expr)],
        else_result: Option<&Expr>,
    ) -> Result<Expr, BindError> {
        let mut bound = Vec::with_capacity(when_clauses.len());
        for (condition, result) in when_clauses {
            bound.push((self.bind_expr(condition)?, self.bind_expr(result)?));
        }
        // Fresh bound copy from the borrowed CASE (no `..base` on an enum variant): the
        // branch clauses and ELSE are rebound. Field list (when_clauses/else_result) is
        // complete.
        Ok(Expr::Case {
            when_clauses: bound,
            else_result: self.bind_opt(else_result)?,
        })
    }
}
