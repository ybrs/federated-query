//! Expression binding: resolve every `ColumnRef` leaf and re-type a Cast, leaving
//! the tree shape otherwise unchanged.
//!
//! A fallible recursive rebuild (not `map_children`, which is infallible) so a
//! resolution error propagates. Ports the `_bind_expr_dispatch` /
//! `_bind_compound_expr` path. The subquery-bearing expressions are the next
//! increment (they need the enclosing scopes threaded into their subplan).

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
            Expr::BinaryOp { op, left, right } => Ok(Expr::BinaryOp {
                op: *op,
                left: Box::new(self.bind_expr(left)?),
                right: Box::new(self.bind_expr(right)?),
            }),
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
        let bound_args = self.bind_expr_list(args)?;
        let bound_within_group = self.bind_opt(within_group_key.as_deref())?;
        let bound_filter = self.bind_opt(filter.as_deref())?;
        // Fresh bound copy from the borrowed source (no `..base` on an enum variant):
        // args, within_group_key, and the FILTER predicate rebound; function_name and
        // the is_aggregate/distinct/within_group_desc flags copied. Field list
        // (function_name/args/is_aggregate/distinct/within_group_key/
        // within_group_desc/filter) is complete.
        Ok(Expr::FunctionCall {
            function_name: function_name.clone(),
            args: bound_args,
            is_aggregate: *is_aggregate,
            distinct: *distinct,
            within_group_key: bound_within_group,
            within_group_desc: *within_group_desc,
            filter: bound_filter,
        })
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
