//! Expression binding: resolve every `ColumnRef` leaf and re-type a Cast, leaving
//! the tree shape otherwise unchanged.
//!
//! A fallible recursive rebuild (not `map_children`, which is infallible) so a
//! resolution error propagates. Ports the `_bind_expr_dispatch` /
//! `_bind_compound_expr` path. The subquery-bearing expressions are the next
//! increment (they need the enclosing scopes threaded into their subplan).

use fq_catalog::map_native_type_default;
use fq_plan::Expr;

use crate::binder::Binder;
use crate::error::BindError;

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
            Expr::BinaryOp { op, left, right } => Ok(Expr::BinaryOp {
                op: *op,
                left: Box::new(self.bind_expr(left)?),
                right: Box::new(self.bind_expr(right)?),
            }),
            Expr::UnaryOp { op, operand } => Ok(Expr::UnaryOp {
                op: *op,
                operand: Box::new(self.bind_expr(operand)?),
            }),
            Expr::Cast {
                expr: inner,
                target_type,
                ..
            } => Ok(Expr::Cast {
                expr: Box::new(self.bind_expr(inner)?),
                target_type: target_type.clone(),
                // Resolve the engine type from the SQL target-type text (used by
                // local evaluation); an unmodeled type leaves it unresolved.
                data_type: map_native_type_default(target_type).ok(),
            }),
            Expr::Between {
                value,
                lower,
                upper,
            } => Ok(Expr::Between {
                value: Box::new(self.bind_expr(value)?),
                lower: Box::new(self.bind_expr(lower)?),
                upper: Box::new(self.bind_expr(upper)?),
            }),
            Expr::InList { value, options } => Ok(Expr::InList {
                value: Box::new(self.bind_expr(value)?),
                options: self.bind_expr_list(options)?,
            }),
            Expr::Case {
                when_clauses,
                else_result,
            } => self.bind_case(when_clauses, else_result.as_deref()),
            Expr::FunctionCall {
                function_name,
                args,
                is_aggregate,
                distinct,
                within_group_key,
                within_group_desc,
            } => Ok(Expr::FunctionCall {
                function_name: function_name.clone(),
                args: self.bind_expr_list(args)?,
                is_aggregate: *is_aggregate,
                distinct: *distinct,
                within_group_key: self.bind_opt(within_group_key.as_deref())?,
                within_group_desc: *within_group_desc,
            }),
            Expr::Extract { field, source } => Ok(Expr::Extract {
                field: field.clone(),
                source: Box::new(self.bind_expr(source)?),
            }),
            Expr::Window {
                function,
                partition_by,
                order_keys,
                order_ascending,
                order_nulls,
                frame,
            } => Ok(Expr::Window {
                function: Box::new(self.bind_expr(function)?),
                partition_by: self.bind_expr_list(partition_by)?,
                order_keys: self.bind_expr_list(order_keys)?,
                order_ascending: order_ascending.clone(),
                order_nulls: order_nulls.clone(),
                frame: frame.clone(),
            }),
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
            Expr::Subquery { subquery } => Ok(Expr::Subquery {
                subquery: Box::new(self.bind_subplan(subquery)?),
            }),
            Expr::Exists { subquery, negated } => Ok(Expr::Exists {
                subquery: Box::new(self.bind_subplan(subquery)?),
                negated: *negated,
            }),
            Expr::InSubquery {
                value,
                subquery,
                negated,
            } => Ok(Expr::InSubquery {
                value: Box::new(self.bind_expr(value)?),
                subquery: Box::new(self.bind_subplan(subquery)?),
                negated: *negated,
            }),
            Expr::QuantifiedComparison {
                operator,
                quantifier,
                left,
                subquery,
            } => Ok(Expr::QuantifiedComparison {
                operator: *operator,
                quantifier: *quantifier,
                left: Box::new(self.bind_expr(left)?),
                subquery: Box::new(self.bind_subplan(subquery)?),
            }),
            _ => unreachable!("bind_subquery_expr called on a non-subquery expression"),
        }
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
        Ok(Expr::Case {
            when_clauses: bound,
            else_result: self.bind_opt(else_result)?,
        })
    }
}
