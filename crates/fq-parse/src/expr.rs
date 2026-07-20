//! polyglot-sql expression -> `fq_plan::Expr` (methods on `Converter`).
//!
//! polyglot's `Expression` is a ~1000-variant enum (a dedicated variant per known
//! function). Exhaustive handling is impossible, so this is an ALLOWLIST: the
//! constructs the engine supports are converted; every other variant RAISES
//! `ParseError::Unsupported` (the Python `SUPPORTED_*_ARGS` posture - fail fast,
//! never silently drop a construct). Expression conversion is a method on
//! `Converter` because the subquery-bearing nodes recurse back into query
//! conversion (which needs the catalog for star expansion).

use fq_plan::expr::{LiteralValue, NullsOrder};
use fq_plan::{BinaryOpType, ColumnRef, Expr, Quantifier, UnaryOpType};
use polyglot_sql::expressions::{
    AggFunc, Anonymous, Between, BinaryOp, Case, Cast, CountFunc, DataType, Expression, In,
    IntervalUnit, IntervalUnitSpec, LeadLagFunc, LikeOp, Literal, NTileFunc, Ordered,
    QuantifiedExpr, ValueFunc, WindowFrame, WindowFrameBound, WindowFrameExclude, WindowFrameKind,
    WindowFunction,
};

use crate::convert::{nulls_of, Converter};
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
            // Fresh NULL literal built from the parsed AST - no base to copy from.
            // Field list (value/data_type) is the complete Literal variant.
            Expression::Null(_) => Ok(Expr::Literal {
                value: LiteralValue::Null,
                data_type: fq_common::DataType::Null,
            }),
            Expression::Paren(paren) => self.expr(&paren.this),
            Expression::Alias(alias) => self.expr(&alias.this),
            Expression::Interval(interval) => convert_interval(interval),
            Expression::Not(unary) => self.unary(UnaryOpType::Not, &unary.this),
            Expression::Neg(unary) => self.unary(UnaryOpType::Negate, &unary.this),
            // `x IS NULL` / `x IS NOT NULL` - polyglot's dedicated variant.
            Expression::IsNull(is_null) => {
                let op = if is_null.not {
                    UnaryOpType::IsNotNull
                } else {
                    UnaryOpType::IsNull
                };
                self.unary(op, &is_null.this)
            }
            // The generic `a IS <primary>` form (IS TRUE, IS NULL via the older
            // shape) - only the NULL cases are supported; the rest raise.
            Expression::Is(binary) => self.convert_is(binary),
            Expression::Cast(cast) => self.convert_cast(cast),
            Expression::Case(case) => self.convert_case(case),
            Expression::Between(between) => self.convert_between(between),
            Expression::Like(like) => self.convert_like(like, BinaryOpType::Like),
            Expression::ILike(like) => self.convert_like(like, BinaryOpType::Ilike),
            Expression::RegexpLike(regexp) => self.convert_regexp(regexp),
            Expression::In(in_expr) => self.convert_in(in_expr),
            Expression::Exists(exists) => self.convert_exists(exists),
            Expression::Subquery(subquery) => self.convert_scalar_subquery(subquery),
            Expression::Any(quantified) => self.convert_quantified(Quantifier::Any, quantified),
            Expression::All(quantified) => self.convert_quantified(Quantifier::All, quantified),
            Expression::WindowFunction(window) => self.convert_window(window),
            Expression::WithinGroup(within) => self.convert_within_group(within),
            // MEDIAN has no canonical Postgres form; it lowers to an ordered-set
            // PERCENTILE_CONT(0.5) over the argument column.
            Expression::Median(agg) => self.convert_median(agg),
            // A set operation is a query, not a scalar value. It reaches the
            // expression path only when the parser attaches a trailing
            // UNION/INTERSECT/EXCEPT to a scalar subquery operand; the WHERE and
            // HAVING shapes are repaired before conversion (`detach_from_select`
            // in convert.rs), so anything arriving here sits in a clause the
            // repair does not cover and raises naming the actual construct.
            Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_) => Err(
                ParseError::Unsupported("set operation used as a scalar expression".to_string()),
            ),
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
            Expression::StddevSamp(agg) => self.convert_aggregate("STDDEV_SAMP", agg),
            Expression::StddevPop(agg) => self.convert_aggregate("STDDEV_POP", agg),
            Expression::Stddev(agg) => self.convert_aggregate("STDDEV", agg),
            Expression::Variance(agg) => self.convert_aggregate("VARIANCE", agg),
            Expression::VarPop(agg) => self.convert_aggregate("VAR_POP", agg),
            Expression::VarSamp(agg) => self.convert_aggregate("VAR_SAMP", agg),
            Expression::ArrayAgg(agg) => self.convert_aggregate("ARRAY_AGG", agg),
            // LOGICAL_AND/LOGICAL_OR are polyglot's canonical names; BOOL_AND/BOOL_OR
            // are the Postgres form the canonical emitter renders.
            Expression::LogicalAnd(agg) => self.convert_aggregate("BOOL_AND", agg),
            Expression::LogicalOr(agg) => self.convert_aggregate("BOOL_OR", agg),
            Expression::StringAgg(agg) => self.convert_string_agg(agg),
            Expression::Anonymous(anon) => self.convert_anonymous(anon),
            other => self.scalar_function(other),
        }
    }

    /// Build an engine binary op from operands.
    fn binary(&self, op: BinaryOpType, binary: &BinaryOp) -> Result<Expr, ParseError> {
        // Fresh binary op built from the parsed operands - no base to copy from.
        // Field list (op/left/right) is the complete BinaryOp variant.
        Ok(Expr::BinaryOp {
            op,
            left: Box::new(self.expr(&binary.left)?),
            right: Box::new(self.expr(&binary.right)?),
        })
    }

    /// Build an engine unary op from an operand.
    fn unary(&self, op: UnaryOpType, operand: &Expression) -> Result<Expr, ParseError> {
        // Fresh unary op built from the parsed operand - no base to copy from.
        // Field list (op/operand) is the complete UnaryOp variant.
        Ok(Expr::UnaryOp {
            op,
            operand: Box::new(self.expr(operand)?),
        })
    }

    /// Convert a regexp match (`col ~ pattern`, polyglot's REGEXP_LIKE) to the
    /// `RegexMatch` binary op. Case-insensitive/other flags are not modeled and
    /// raise rather than being silently dropped.
    fn convert_regexp(
        &self,
        regexp: &polyglot_sql::expressions::RegexpFunc,
    ) -> Result<Expr, ParseError> {
        if regexp.flags.is_some() {
            return Err(ParseError::Unsupported("REGEXP with flags".to_string()));
        }
        // Fresh regexp-match binary op built from the parsed subject + pattern - no
        // base to copy from. Field list (op/left/right) is the complete BinaryOp
        // variant.
        Ok(Expr::BinaryOp {
            op: BinaryOpType::RegexMatch,
            left: Box::new(self.expr(&regexp.this)?),
            right: Box::new(self.expr(&regexp.pattern)?),
        })
    }

    /// Convert `x IS NULL` / `x IS NOT NULL`; any other IS shape raises.
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
        // Fresh cast built from the parsed CAST - no base to copy from. Field list
        // (expr/target_type/data_type) is the complete Cast variant.
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
            Some(operand) => {
                let converted = self.expr(operand)?;
                // A function-bearing operand is duplicated into every branch by
                // the searched-form lowering; a volatile function (random(), ...)
                // would then be re-evaluated per branch and could match the wrong
                // arm. The engine carries no volatility metadata, so refuse.
                if expr_has_function_call(&converted) {
                    return Err(ParseError::Unsupported(
                        "simple CASE with a function-call operand".to_string(),
                    ));
                }
                Some(converted)
            }
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
        // Fresh CASE built from the parsed WHEN/ELSE branches - no base to copy from.
        // Field list (when_clauses/else_result) is the complete Case variant.
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
        // Fresh range test built from the parsed BETWEEN bounds - no base to copy
        // from. Field list (value/lower/upper) is the complete Between variant.
        let range = Expr::Between {
            value: Box::new(self.expr(&between.this)?),
            lower: Box::new(self.expr(&between.low)?),
            upper: Box::new(self.expr(&between.high)?),
        };
        Ok(negate_if(between.not, range))
    }

    /// Convert `x LIKE y` / `x ILIKE y` to a binary op. An ESCAPE clause or a
    /// quantifier (LIKE ANY/ALL) is not modeled, so it raises rather than drop it.
    fn convert_like(&self, like: &LikeOp, op: BinaryOpType) -> Result<Expr, ParseError> {
        if like.escape.is_some() {
            return Err(ParseError::Unsupported("LIKE ... ESCAPE".to_string()));
        }
        if like.quantifier.is_some() {
            return Err(ParseError::Unsupported("quantified LIKE".to_string()));
        }
        // Fresh LIKE/ILIKE binary op built from the parsed operands - no base to copy
        // from. Field list (op/left/right) is the complete BinaryOp variant.
        Ok(Expr::BinaryOp {
            op,
            left: Box::new(self.expr(&like.left)?),
            right: Box::new(self.expr(&like.right)?),
        })
    }

    /// Convert `x [NOT] IN (list)` or `x [NOT] IN (subquery)`.
    fn convert_in(&self, in_expr: &In) -> Result<Expr, ParseError> {
        let value = self.expr(&in_expr.this)?;
        if let Some(query) = &in_expr.query {
            // Fresh IN-subquery predicate built from the parsed value + subquery - no
            // base to copy from. Field list (value/subquery/negated) is complete.
            return Ok(Expr::InSubquery {
                value: Box::new(value),
                subquery: Box::new(self.query(query)?),
                negated: in_expr.not,
            });
        }
        if in_expr.unnest.is_some() {
            // `x IN UNNEST(array)` - a distinct shape; do not treat it as an
            // empty IN-list (which would be an always-false predicate).
            return Err(ParseError::Unsupported("IN UNNEST".to_string()));
        }
        let mut options = Vec::with_capacity(in_expr.expressions.len());
        for option in &in_expr.expressions {
            options.push(self.expr(option)?);
        }
        // Fresh IN-list predicate built from the parsed value + option list - no base
        // to copy from. Field list (value/options) is the complete InList variant.
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
        // Fresh EXISTS predicate built from the parsed subquery - no base to copy
        // from. Field list (subquery/negated) is the complete Exists variant.
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
        // Fresh scalar-subquery expression built from the parsed subquery - no base
        // to copy from. Field list (subquery) is the complete Subquery variant.
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
        // Fresh quantified comparison built from the parsed ANY/ALL - no base to copy
        // from. Field list (operator/quantifier/left/subquery) is complete.
        Ok(Expr::QuantifiedComparison {
            operator,
            quantifier,
            left: Box::new(self.expr(&quantified.this)?),
            subquery: Box::new(self.query(&quantified.subquery)?),
        })
    }

    /// Convert `func OVER (PARTITION BY ... ORDER BY ... frame)` into
    /// `Expr::Window`. A named-window reference (`OVER w`) and Oracle's KEEP
    /// clause are not modeled and raise rather than dropping window semantics.
    fn convert_window(&self, window: &WindowFunction) -> Result<Expr, ParseError> {
        if window.keep.is_some() {
            return Err(ParseError::Unsupported("window KEEP clause".to_string()));
        }
        let over = &window.over;
        if over.window_name.is_some() {
            return Err(ParseError::Unsupported(
                "named window reference".to_string(),
            ));
        }
        let function = Box::new(self.convert_window_function(&window.this)?);
        let mut partition_by = Vec::with_capacity(over.partition_by.len());
        for part in &over.partition_by {
            partition_by.push(self.expr(part)?);
        }
        let (order_keys, order_ascending, order_nulls) =
            self.convert_window_order(&over.order_by)?;
        let frame = match &over.frame {
            Some(frame) => Some(render_window_frame(frame)?),
            None => None,
        };
        // Fresh window expression built from the parsed OVER clause - no base to
        // copy from. Field list (function/partition_by/order_keys/order_ascending/
        // order_nulls/frame) is the complete Window variant.
        Ok(Expr::Window {
            function,
            partition_by,
            order_keys,
            order_ascending,
            order_nulls,
            frame,
        })
    }

    /// Convert the function under an OVER. The pure window functions (ROW_NUMBER,
    /// RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTILE) are meaningful
    /// only as a windowed call, so they are recognized here, not in the general
    /// expression path where a bare `rank()` still raises. Every other window
    /// function (SUM/AVG/... over a window) converts through the normal path.
    fn convert_window_function(&self, this: &Expression) -> Result<Expr, ParseError> {
        match this {
            Expression::RowNumber(_) => Ok(ranking_call("ROW_NUMBER")),
            Expression::Rank(rank) => plain_ranking("RANK", &rank.args, rank.order_by.as_deref()),
            Expression::DenseRank(dense) => plain_ranking("DENSE_RANK", &dense.args, None),
            Expression::Lag(func) => self.convert_lead_lag("LAG", func),
            Expression::Lead(func) => self.convert_lead_lag("LEAD", func),
            Expression::FirstValue(func) => self.convert_value_fn("FIRST_VALUE", func),
            Expression::LastValue(func) => self.convert_value_fn("LAST_VALUE", func),
            Expression::NTile(func) => self.convert_ntile(func),
            other => self.expr(other),
        }
    }

    /// Convert LAG/LEAD into `NAME(expr [, offset [, default]])`. The engine's
    /// window plan carries no null-treatment modifier, so IGNORE NULLS raises
    /// rather than dropping it. A default without an offset is not expressible
    /// as positional args and raises.
    fn convert_lead_lag(&self, name: &str, func: &LeadLagFunc) -> Result<Expr, ParseError> {
        if func.ignore_nulls == Some(true) {
            return Err(ParseError::Unsupported(format!("IGNORE NULLS in {name}")));
        }
        if func.default.is_some() && func.offset.is_none() {
            return Err(ParseError::Unsupported(format!(
                "{name} default value without an offset"
            )));
        }
        let mut args = vec![self.expr(&func.this)?];
        if let Some(offset) = &func.offset {
            args.push(self.expr(offset)?);
        }
        if let Some(default) = &func.default {
            args.push(self.expr(default)?);
        }
        Ok(scalar_function_call(name.to_string(), args))
    }

    /// Convert FIRST_VALUE/LAST_VALUE into `NAME(expr)`. IGNORE NULLS and a
    /// function-internal ORDER BY have no engine window-plan form and raise.
    fn convert_value_fn(&self, name: &str, func: &ValueFunc) -> Result<Expr, ParseError> {
        if func.ignore_nulls == Some(true) {
            return Err(ParseError::Unsupported(format!("IGNORE NULLS in {name}")));
        }
        if !func.order_by.is_empty() {
            return Err(ParseError::Unsupported(format!("ORDER BY inside {name}")));
        }
        Ok(scalar_function_call(
            name.to_string(),
            vec![self.expr(&func.this)?],
        ))
    }

    /// Convert NTILE into `NTILE(bucket_count)`. NTILE requires a bucket count;
    /// its absence raises rather than emitting an invalid `NTILE()`. A
    /// function-internal ORDER BY has no engine window-plan form and raises.
    fn convert_ntile(&self, func: &NTileFunc) -> Result<Expr, ParseError> {
        if func.order_by.is_some() {
            return Err(ParseError::Unsupported("ORDER BY inside NTILE".to_string()));
        }
        let bucket = func
            .num_buckets
            .as_ref()
            .ok_or_else(|| ParseError::Unsupported("NTILE without a bucket count".to_string()))?;
        Ok(scalar_function_call(
            "NTILE".to_string(),
            vec![self.expr(bucket)?],
        ))
    }

    /// Convert a window's ORDER BY into aligned (keys, ascending, nulls) lists.
    fn convert_window_order(&self, order_by: &[Ordered]) -> Result<WindowOrder, ParseError> {
        let mut keys = Vec::with_capacity(order_by.len());
        let mut ascending = Vec::with_capacity(order_by.len());
        let mut nulls = Vec::with_capacity(order_by.len());
        for ordered in order_by {
            keys.push(self.expr(&ordered.this)?);
            ascending.push(!ordered.desc);
            nulls.push(nulls_of(ordered));
        }
        Ok((keys, ascending, nulls))
    }

    /// Convert COUNT(*) / COUNT(expr) / COUNT(DISTINCT expr).
    fn convert_count(&self, count: &CountFunc) -> Result<Expr, ParseError> {
        let args = match &count.this {
            Some(inner) => vec![self.expr(inner)?],
            None => Vec::new(),
        };
        // Fresh COUNT call built from the parsed COUNT(*)/COUNT(expr) - no base to
        // copy from. Field list (function_name/args/is_aggregate/distinct/
        // within_group_key/within_group_desc) is the complete FunctionCall variant.
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
        // Fresh SUM/AVG/MIN/MAX call built from the parsed aggregate - no base to copy
        // from. Field list (function_name/args/is_aggregate/distinct/within_group_key/
        // within_group_desc) is the complete FunctionCall variant.
        Ok(Expr::FunctionCall {
            function_name: name.to_string(),
            args: vec![self.expr(&agg.this)?],
            is_aggregate: true,
            distinct: agg.distinct,
            within_group_key: None,
            within_group_desc: false,
        })
    }

    /// Convert STRING_AGG(value, separator). The separator is a plain argument in
    /// the canonical form. An in-aggregate ORDER BY, FILTER, or LIMIT changes the
    /// concatenation and is not modeled, so it raises rather than being dropped.
    fn convert_string_agg(
        &self,
        agg: &polyglot_sql::expressions::StringAggFunc,
    ) -> Result<Expr, ParseError> {
        if agg.order_by.as_ref().is_some_and(|order| !order.is_empty()) {
            return Err(ParseError::Unsupported(
                "ORDER BY inside STRING_AGG".to_string(),
            ));
        }
        if agg.filter.is_some() || agg.limit.is_some() {
            return Err(ParseError::Unsupported(
                "FILTER/LIMIT inside STRING_AGG".to_string(),
            ));
        }
        let mut args = vec![self.expr(&agg.this)?];
        if let Some(separator) = &agg.separator {
            args.push(self.expr(separator)?);
        }
        // Fresh STRING_AGG call built from value + separator - no base to copy from.
        // Field list (function_name/args/is_aggregate/distinct/within_group_key/
        // within_group_desc) is the complete FunctionCall variant.
        Ok(Expr::FunctionCall {
            function_name: "STRING_AGG".to_string(),
            args,
            is_aggregate: true,
            distinct: agg.distinct,
            within_group_key: None,
            within_group_desc: false,
        })
    }

    /// Convert an ordered-set aggregate `<agg> WITHIN GROUP (ORDER BY key)`
    /// (PERCENTILE_CONT/PERCENTILE_DISC/MODE). Exactly one ordering key is
    /// modeled; a multi-key WITHIN GROUP raises rather than dropping keys.
    fn convert_within_group(
        &self,
        within: &polyglot_sql::expressions::WithinGroup,
    ) -> Result<Expr, ParseError> {
        let [ordered] = within.order_by.as_slice() else {
            return Err(ParseError::Unsupported(
                "WITHIN GROUP with other than one ordering key".to_string(),
            ));
        };
        let (name, args, distinct) = self.within_group_inner(&within.this)?;
        let key = Box::new(self.expr(&ordered.this)?);
        // Fresh ordered-set aggregate call - no base to copy from. Field list
        // (function_name/args/is_aggregate/distinct/within_group_key/
        // within_group_desc) is the complete FunctionCall variant.
        Ok(Expr::FunctionCall {
            function_name: name,
            args,
            is_aggregate: true,
            distinct,
            within_group_key: Some(key),
            within_group_desc: ordered.desc,
        })
    }

    /// The (name, converted args, distinct) of the aggregate inside a WITHIN
    /// GROUP. PERCENTILE_CONT/DISC arrive as a generic AggregateFunction; MODE()
    /// arrives as the dedicated Mode node with no arguments. Anything else raises.
    fn within_group_inner(
        &self,
        inner: &Expression,
    ) -> Result<(String, Vec<Expr>, bool), ParseError> {
        match inner {
            Expression::AggregateFunction(func) => {
                let mut args = Vec::with_capacity(func.args.len());
                for arg in &func.args {
                    args.push(self.expr(arg)?);
                }
                Ok((func.name.to_uppercase(), args, func.distinct))
            }
            Expression::Mode(agg) => Ok(("MODE".to_string(), Vec::new(), agg.distinct)),
            other => Err(ParseError::Unsupported(format!(
                "ordered-set aggregate `{}`",
                other.variant_name()
            ))),
        }
    }

    /// Lower MEDIAN(x) to PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x): MEDIAN
    /// has no canonical Postgres form, PERCENTILE_CONT does.
    fn convert_median(&self, agg: &AggFunc) -> Result<Expr, ParseError> {
        let key = Box::new(self.expr(&agg.this)?);
        // Fresh PERCENTILE_CONT call standing in for MEDIAN - no base to copy from.
        // Field list (function_name/args/is_aggregate/distinct/within_group_key/
        // within_group_desc) is the complete FunctionCall variant.
        Ok(Expr::FunctionCall {
            function_name: "PERCENTILE_CONT".to_string(),
            args: vec![Expr::Literal {
                value: LiteralValue::Float(0.5),
                data_type: fq_common::DataType::Double,
            }],
            is_aggregate: true,
            distinct: agg.distinct,
            within_group_key: Some(key),
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
    // Fresh scalar function call built from a name + converted args - no base to copy
    // from. Field list (function_name/args/is_aggregate/distinct/within_group_key/
    // within_group_desc) is the complete FunctionCall variant.
    Expr::FunctionCall {
        function_name: name,
        args,
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    }
}

/// A window ORDER BY as aligned (keys, ascending, nulls) lists.
type WindowOrder = (Vec<Expr>, Vec<bool>, Vec<Option<NullsOrder>>);

/// A ranking window function with no in-function ORDER BY or hypothetical-set
/// arguments (the only form the engine models). The DuckDB `RANK(ORDER BY x)` and
/// Oracle `RANK(v) WITHIN GROUP (...)` forms carry ranking semantics the bare
/// call cannot, so they raise rather than being silently dropped.
fn plain_ranking(
    name: &str,
    args: &[Expression],
    order_by: Option<&[Ordered]>,
) -> Result<Expr, ParseError> {
    if !args.is_empty() {
        return Err(ParseError::Unsupported(
            "hypothetical-set ranking arguments".to_string(),
        ));
    }
    if order_by.is_some() {
        return Err(ParseError::Unsupported(
            "ORDER BY inside a ranking function".to_string(),
        ));
    }
    Ok(ranking_call(name))
}

/// A ranking window function (`RANK()` etc.) as a NON-aggregate, zero-argument
/// call. It must not be aggregate: the emitter renders a zero-argument aggregate
/// as the star form (`COUNT()` -> `COUNT(*)`), which would make `RANK()` render
/// as the invalid `RANK(*)`.
fn ranking_call(name: &str) -> Expr {
    scalar_function_call(name.to_string(), Vec::new())
}

/// Render a window frame (`ROWS/RANGE/GROUPS ...`) back to SQL text, carried
/// verbatim on the Window node so it re-renders to a source. Covers the standard
/// bounds and the EXCLUDE clause.
fn render_window_frame(frame: &WindowFrame) -> Result<String, ParseError> {
    let kind = match frame.kind {
        WindowFrameKind::Rows => "ROWS",
        WindowFrameKind::Range => "RANGE",
        WindowFrameKind::Groups => "GROUPS",
    };
    let body = match &frame.end {
        Some(end) => format!(
            "BETWEEN {} AND {}",
            frame_bound(&frame.start)?,
            frame_bound(end)?
        ),
        None => frame_bound(&frame.start)?,
    };
    let mut text = format!("{kind} {body}");
    if let Some(exclude) = &frame.exclude {
        text.push_str(" EXCLUDE ");
        text.push_str(frame_exclude(*exclude));
    }
    Ok(text)
}

/// One window-frame bound as SQL text.
fn frame_bound(bound: &WindowFrameBound) -> Result<String, ParseError> {
    let text = match bound {
        WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
        WindowFrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
        WindowFrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
        WindowFrameBound::Preceding(value) => format!("{} PRECEDING", frame_value(value)?),
        WindowFrameBound::Following(value) => format!("{} FOLLOWING", frame_value(value)?),
        WindowFrameBound::BarePreceding => "PRECEDING".to_string(),
        WindowFrameBound::BareFollowing => "FOLLOWING".to_string(),
        WindowFrameBound::Value(value) => frame_value(value)?,
    };
    Ok(text)
}

/// Render a frame-bound offset expression (the `1` in `1 PRECEDING`) to SQL text
/// via polyglot's generator, so the frame re-renders to a source.
fn frame_value(value: &Expression) -> Result<String, ParseError> {
    polyglot_sql::generate(value, polyglot_sql::DialectType::PostgreSQL)
        .map_err(|error| ParseError::Unsupported(format!("window frame bound: {error:?}")))
}

/// The keyword text for a frame EXCLUDE clause.
fn frame_exclude(exclude: WindowFrameExclude) -> &'static str {
    match exclude {
        WindowFrameExclude::CurrentRow => "CURRENT ROW",
        WindowFrameExclude::Group => "GROUP",
        WindowFrameExclude::Ties => "TIES",
        WindowFrameExclude::NoOthers => "NO OTHERS",
    }
}

/// Whether a converted expression tree contains any function call (all function
/// forms normalize to `Expr::FunctionCall`).
fn expr_has_function_call(expr: &Expr) -> bool {
    matches!(expr, Expr::FunctionCall { .. })
        || expr.children().iter().any(|c| expr_has_function_call(c))
}

/// Wrap an expression in NOT when `negate` is set.
fn negate_if(negate: bool, expr: Expr) -> Expr {
    if negate {
        // Fresh NOT wrapper around the built expression - no base to copy from.
        // Field list (op/operand) is the complete UnaryOp variant.
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
    // Fresh boolean literal built from a bool constant - no base to copy from.
    // Field list (value/data_type) is the complete Literal variant.
    Expr::Literal {
        value: LiteralValue::Boolean(value),
        data_type: fq_common::DataType::Boolean,
    }
}

/// The searched-form condition for one simple-CASE branch: `operand = value`.
fn searched_condition(operand: Expr, value: Expr) -> Expr {
    // Fresh equality lowering one simple-CASE branch to searched form - no base to
    // copy from. Field list (op/left/right) is the complete BinaryOp variant.
    Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left: Box::new(operand),
        right: Box::new(value),
    }
}

/// Map a polyglot binary-operator variant to the engine operator, or None.
pub(crate) fn binary_op_type(expr: &Expression) -> Option<BinaryOpType> {
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
        Expression::NullSafeNeq(_) => BinaryOpType::NullSafeNeq,
        _ => return None,
    };
    Some(op)
}

/// The `BinaryOp` of any of the `Box<BinaryOp>` variants handled above.
pub(crate) fn binary_operands(expr: &Expression) -> &BinaryOp {
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
        | Expression::NullSafeEq(binary)
        | Expression::NullSafeNeq(binary) => binary,
        _ => unreachable!("binary_operands called on a non-binary expression"),
    }
}

/// The `BinaryOp` of any of the `Box<BinaryOp>` variants, mutably. Covers the
/// same variant list as `binary_operands`.
pub(crate) fn binary_operands_mut(expr: &mut Expression) -> &mut BinaryOp {
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
        | Expression::NullSafeEq(binary)
        | Expression::NullSafeNeq(binary) => binary,
        _ => unreachable!("binary_operands_mut called on a non-binary expression"),
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

/// Convert an INTERVAL literal into `Expr::Interval`. The value must be a
/// string or numeric literal; a simple unit keyword (`INTERVAL '1' YEAR`)
/// carries through as its uppercase name (pluralized when written plural).
/// Column-valued intervals and span units (`HOUR TO SECOND`) are not modeled
/// and raise rather than being dropped.
fn convert_interval(interval: &polyglot_sql::expressions::Interval) -> Result<Expr, ParseError> {
    let Some(this) = &interval.this else {
        return Err(ParseError::Unsupported(
            "INTERVAL without a value".to_string(),
        ));
    };
    let value = match this {
        Expression::Literal(literal) => match literal.as_ref() {
            Literal::String(text) | Literal::Number(text) => text.clone(),
            other => {
                return Err(ParseError::Unsupported(format!(
                    "INTERVAL value literal `{other:?}`"
                )));
            }
        },
        other => {
            return Err(ParseError::Unsupported(format!(
                "INTERVAL value `{}`",
                other.variant_name()
            )));
        }
    };
    let unit = match &interval.unit {
        None => None,
        Some(IntervalUnitSpec::Simple { unit, use_plural }) => {
            Some(interval_unit_name(*unit, *use_plural))
        }
        Some(other) => {
            return Err(ParseError::Unsupported(format!(
                "INTERVAL unit `{other:?}`"
            )));
        }
    };
    // Fresh interval literal built from the parsed token - no base to copy from.
    // Field list (value/unit) is the complete Interval variant.
    Ok(Expr::Interval { value, unit })
}

/// The SQL keyword for a simple interval unit, pluralized when written plural.
fn interval_unit_name(unit: IntervalUnit, use_plural: bool) -> String {
    let base = match unit {
        IntervalUnit::Year => "YEAR",
        IntervalUnit::Quarter => "QUARTER",
        IntervalUnit::Month => "MONTH",
        IntervalUnit::Week => "WEEK",
        IntervalUnit::Day => "DAY",
        IntervalUnit::Hour => "HOUR",
        IntervalUnit::Minute => "MINUTE",
        IntervalUnit::Second => "SECOND",
        IntervalUnit::Millisecond => "MILLISECOND",
        IntervalUnit::Microsecond => "MICROSECOND",
        IntervalUnit::Nanosecond => "NANOSECOND",
    };
    if use_plural {
        format!("{base}S")
    } else {
        base.to_string()
    }
}

/// Convert a numeric/string literal.
fn convert_literal(literal: &Literal) -> Result<Expr, ParseError> {
    match literal {
        Literal::Number(text) => convert_number(text),
        // Fresh string literal built from the parsed token - no base to copy from.
        // Field list (value/data_type) is the complete Literal variant.
        Literal::String(text) => Ok(Expr::Literal {
            value: LiteralValue::String(text.clone()),
            data_type: fq_common::DataType::Varchar,
        }),
        // Fresh DATE literal carrying its type so the emitter re-renders the
        // `DATE '...'` form rather than a bare string - no base to copy from. Field
        // list (value/data_type) is the complete Literal variant.
        Literal::Date(text) => Ok(Expr::Literal {
            value: LiteralValue::String(text.clone()),
            data_type: fq_common::DataType::Date,
        }),
        // Fresh TIMESTAMP literal (DATETIME is the same value shape) carrying its
        // type so the emitter re-renders `TIMESTAMP '...'` - no base to copy from.
        // Field list (value/data_type) is the complete Literal variant.
        Literal::Timestamp(text) | Literal::Datetime(text) => Ok(Expr::Literal {
            value: LiteralValue::String(text.clone()),
            data_type: fq_common::DataType::Timestamp,
        }),
        other => Err(ParseError::Unsupported(format!("literal {other:?}"))),
    }
}

/// A numeric literal: integer when it parses as one, else a double; a numeric
/// token that parses as neither raises rather than becoming a silent NaN.
fn convert_number(text: &str) -> Result<Expr, ParseError> {
    if let Ok(integer) = text.parse::<i64>() {
        // Fresh integer literal built from the parsed numeric token - no base to copy
        // from. Field list (value/data_type) is the complete Literal variant.
        return Ok(Expr::Literal {
            value: LiteralValue::Integer(integer),
            data_type: fq_common::DataType::Integer,
        });
    }
    let value = text
        .parse::<f64>()
        .map_err(|_| ParseError::Unsupported(format!("numeric literal '{text}'")))?;
    // Fresh float literal built from the parsed numeric token - no base to copy from.
    // Field list (value/data_type) is the complete Literal variant.
    Ok(Expr::Literal {
        value: LiteralValue::Float(value),
        data_type: fq_common::DataType::Double,
    })
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
