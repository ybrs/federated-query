//! The canonical Postgres emitter: `Expr` -> canonical Postgres SQL text.
//!
//! An exhaustive match over `Expr` with no `_` arm, so a new variant breaks this
//! at compile time (ports the Python `SqlglotEmitter` visitor). Parenthesization
//! is LOAD-BEARING and kept exactly as the Python emitter: predicate terms are
//! AND-joined as strings by the clause builders, so an un-parenthesized OR/AND
//! term would silently re-associate.
//!
//! RETIRED-BY-TYPE-SYSTEM: the Python allowlist-lookup raises for an unmapped
//! binary/unary operator and the `_literal_to_ast` raise for an unmapped data
//! type are gone - the enums are closed and matched exhaustively, so the compiler
//! is the guard (see error.rs).

use fq_common::DataType;
use fq_plan::{Expr, LiteralValue, NullsOrder, UnaryOpType};

use crate::error::EmitError;
use crate::resolver::ColumnResolver;

/// Render an expression tree to canonical Postgres SQL text using `resolver` for
/// every column reference.
pub fn render_expr(expr: &Expr, resolver: &dyn ColumnResolver) -> Result<String, EmitError> {
    match expr {
        Expr::Column(col) => resolver.resolve(col.table.as_deref(), &col.column),
        Expr::Literal { value, data_type } => render_literal(value, *data_type),
        Expr::BinaryOp { op, left, right } => Ok(format!(
            "({} {} {})",
            render_expr(left, resolver)?,
            op.value(),
            render_expr(right, resolver)?
        )),
        Expr::UnaryOp { op, operand } => render_unary(*op, operand, resolver),
        Expr::FunctionCall {
            function_name,
            args,
            distinct,
            within_group_key,
            within_group_desc,
            ..
        } => render_function(
            function_name,
            args,
            *distinct,
            within_group_key.as_deref(),
            *within_group_desc,
            resolver,
        ),
        Expr::Case {
            when_clauses,
            else_result,
        } => render_case(when_clauses, else_result.as_deref(), resolver),
        Expr::InList { value, options } => Ok(format!(
            "({} IN ({}))",
            render_expr(value, resolver)?,
            render_all(options, resolver)?.join(", ")
        )),
        Expr::Between {
            value,
            lower,
            upper,
        } => Ok(format!(
            "({} BETWEEN {} AND {})",
            render_expr(value, resolver)?,
            render_expr(lower, resolver)?,
            render_expr(upper, resolver)?
        )),
        Expr::Cast {
            expr, target_type, ..
        } => Ok(format!(
            "CAST({} AS {})",
            render_expr(expr, resolver)?,
            target_type
        )),
        Expr::Extract { field, source } => Ok(format!(
            "EXTRACT({} FROM {})",
            field,
            render_expr(source, resolver)?
        )),
        Expr::Interval { value, unit } => Ok(render_interval(value, unit.as_deref())),
        Expr::Tuple { items } => Ok(format!("({})", render_all(items, resolver)?.join(", "))),
        Expr::Window { .. } => render_window(expr, resolver),
        // Subquery-bearing nodes must be decorrelated before emission; each RAISES.
        Expr::Subquery { .. } => Err(EmitError::SubqueryReachedEmit("SubqueryExpression")),
        Expr::Exists { .. } => Err(EmitError::SubqueryReachedEmit("ExistsExpression")),
        Expr::InSubquery { .. } => Err(EmitError::SubqueryReachedEmit("InSubquery")),
        Expr::QuantifiedComparison { .. } => {
            Err(EmitError::SubqueryReachedEmit("QuantifiedComparison"))
        }
    }
}

/// Render a typed literal. A NULL value OR a `DataType::Null` forces `NULL` (the
/// Python `value is None OR data_type == NULL`); otherwise the LiteralValue shape
/// decides the token. The enum is closed, so there is no unmapped-type raise; the
/// one runtime raise is a non-finite float, which has no canonical SQL token.
fn render_literal(value: &LiteralValue, data_type: DataType) -> Result<String, EmitError> {
    if data_type == DataType::Null {
        return Ok("NULL".to_string());
    }
    Ok(match value {
        LiteralValue::Null => "NULL".to_string(),
        LiteralValue::Boolean(true) => "TRUE".to_string(),
        LiteralValue::Boolean(false) => "FALSE".to_string(),
        LiteralValue::Integer(i) => i.to_string(),
        LiteralValue::Float(f) if !f.is_finite() => {
            return Err(EmitError::UnrepresentableLiteral(f.to_string()));
        }
        LiteralValue::Float(f) => f.to_string(),
        LiteralValue::String(s) => format!("'{}'", s.replace('\'', "''")),
    })
}

/// Render a unary operator, whole thing parenthesized. The prefix/postfix
/// placement differs per op, so an explicit match is clearer than op.value().
fn render_unary(
    op: UnaryOpType,
    operand: &Expr,
    resolver: &dyn ColumnResolver,
) -> Result<String, EmitError> {
    let inner = render_expr(operand, resolver)?;
    Ok(match op {
        UnaryOpType::Not => format!("(NOT {inner})"),
        // A sign-leading operand (a negative numeric literal, or a nested negate)
        // would fuse the prefix `-` with the operand's `-` into `--`, which starts
        // a SQL line comment and swallows the rest of the statement. Wrap such an
        // operand in parens so the tokens never touch.
        UnaryOpType::Negate if inner.starts_with('-') => format!("(-({inner}))"),
        UnaryOpType::Negate => format!("(-{inner})"),
        UnaryOpType::IsNull => format!("({inner} IS NULL)"),
        UnaryOpType::IsNotNull => format!("({inner} IS NOT NULL)"),
    })
}

/// Render a function/aggregate call, with DISTINCT and an ordered-set WITHIN
/// GROUP. The name is kept verbatim (a generic call); dialect-specific function
/// translation happens at the transpile boundary.
fn render_function(
    name: &str,
    args: &[Expr],
    distinct: bool,
    within_group_key: Option<&Expr>,
    within_group_desc: bool,
    resolver: &dyn ColumnResolver,
) -> Result<String, EmitError> {
    let rendered = render_all(args, resolver)?.join(", ");
    let mut call = if distinct {
        format!("{name}(DISTINCT {rendered})")
    } else {
        format!("{name}({rendered})")
    };
    if let Some(key) = within_group_key {
        // Direction is emitted only for DESC; an explicit ASC inside WITHIN GROUP
        // defeats polyglot's ordered-set aggregate transpilation. No NULLS here.
        let direction = if within_group_desc { " DESC" } else { "" };
        call = format!(
            "{call} WITHIN GROUP (ORDER BY {}{direction})",
            render_expr(key, resolver)?
        );
    }
    Ok(call)
}

/// Render a CASE as WHEN/THEN branches plus an optional ELSE.
fn render_case(
    when_clauses: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    resolver: &dyn ColumnResolver,
) -> Result<String, EmitError> {
    let mut sql = String::from("CASE");
    for (condition, result) in when_clauses {
        sql.push_str(" WHEN ");
        sql.push_str(&render_expr(condition, resolver)?);
        sql.push_str(" THEN ");
        sql.push_str(&render_expr(result, resolver)?);
    }
    if let Some(default) = else_result {
        sql.push_str(" ELSE ");
        sql.push_str(&render_expr(default, resolver)?);
    }
    sql.push_str(" END");
    Ok(sql)
}

/// Render an INTERVAL literal, with an optional verbatim unit (e.g. `DAY`).
fn render_interval(value: &str, unit: Option<&str>) -> String {
    match unit {
        Some(unit) => format!("INTERVAL '{value}' {unit}"),
        None => format!("INTERVAL '{value}'"),
    }
}

/// Render `function OVER (PARTITION BY ... ORDER BY ... frame)`; `OVER ()` when
/// all three parts are absent.
fn render_window(expr: &Expr, resolver: &dyn ColumnResolver) -> Result<String, EmitError> {
    let Expr::Window {
        function,
        partition_by,
        order_keys,
        order_ascending,
        order_nulls,
        frame,
    } = expr
    else {
        unreachable!("render_window is only called for Expr::Window");
    };
    let mut parts: Vec<String> = Vec::new();
    if !partition_by.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            render_all(partition_by, resolver)?.join(", ")
        ));
    }
    if !order_keys.is_empty() {
        parts.push(format!(
            "ORDER BY {}",
            render_ordered_keys(order_keys, order_ascending, order_nulls, resolver)?
        ));
    }
    if let Some(frame_text) = frame {
        parts.push(frame_text.clone());
    }
    Ok(format!(
        "{} OVER ({})",
        render_expr(function, resolver)?,
        parts.join(" ")
    ))
}

/// Render sort keys to a comma-joined ORDER BY fragment via the shared rule. The
/// single place ORDER BY keys are lowered, used by both the window emitter and
/// the ORDER BY clause builder.
pub(crate) fn render_ordered_keys(
    keys: &[Expr],
    ascending: &[bool],
    nulls: &[Option<NullsOrder>],
    resolver: &dyn ColumnResolver,
) -> Result<String, EmitError> {
    let mut ordered = Vec::with_capacity(keys.len());
    for (index, key) in keys.iter().enumerate() {
        let key_sql = render_expr(key, resolver)?;
        ordered.push(ordered_key(&key_sql, index, ascending, nulls));
    }
    Ok(ordered.join(", "))
}

/// Render one ORDER BY key with explicit direction + NULLS placement. The single
/// place NULLS is decided: use the plan's value, else the canonical Postgres
/// default (FIRST for DESC, LAST for ASC). NULLS is always emitted, since leaving
/// it implicit lets a dialect flip NULL ordering. ASC is never emitted (Python
/// omits it); only DESC is explicit.
pub(crate) fn ordered_key(
    key_sql: &str,
    index: usize,
    ascending: &[bool],
    nulls: &[Option<NullsOrder>],
) -> String {
    let desc = ascending.get(index) == Some(&false);
    let placement = nulls.get(index).copied().flatten().unwrap_or(if desc {
        NullsOrder::First
    } else {
        NullsOrder::Last
    });
    let nulls_token = match placement {
        NullsOrder::First => "NULLS FIRST",
        NullsOrder::Last => "NULLS LAST",
    };
    let direction = if desc { " DESC" } else { "" };
    format!("{key_sql}{direction} {nulls_token}")
}

/// Render each expression in a slice to canonical text, preserving order.
fn render_all(exprs: &[Expr], resolver: &dyn ColumnResolver) -> Result<Vec<String>, EmitError> {
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        out.push(render_expr(expr, resolver)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::render_expr;
    use crate::dialect::{to_source_sql, Dialect};
    use crate::error::EmitError;
    use crate::resolver::{ColumnResolver, SourceResolver};
    use fq_common::DataType;
    use fq_plan::logical::Values;
    use fq_plan::{
        BinaryOpType, ColumnRef, Expr, LiteralValue, LogicalPlan, NullsOrder, UnaryOpType,
    };

    fn render(expr: &Expr) -> String {
        let resolver = SourceResolver;
        let sink: &dyn ColumnResolver = &resolver;
        render_expr(expr, sink).unwrap()
    }

    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    fn int(value: i64) -> Expr {
        Expr::Literal {
            value: LiteralValue::Integer(value),
            data_type: DataType::Integer,
        }
    }

    fn string(value: &str) -> Expr {
        Expr::Literal {
            value: LiteralValue::String(value.to_string()),
            data_type: DataType::Varchar,
        }
    }

    #[test]
    fn column_uses_the_resolver() {
        assert_eq!(render(&col("t", "a")), "\"t\".\"a\"");
    }

    #[test]
    fn literals_of_each_shape() {
        assert_eq!(
            render(&Expr::Literal {
                value: LiteralValue::Null,
                data_type: DataType::Integer
            }),
            "NULL"
        );
        assert_eq!(
            render(&Expr::Literal {
                value: LiteralValue::Boolean(true),
                data_type: DataType::Boolean
            }),
            "TRUE"
        );
        assert_eq!(
            render(&Expr::Literal {
                value: LiteralValue::Boolean(false),
                data_type: DataType::Boolean
            }),
            "FALSE"
        );
        assert_eq!(render(&int(42)), "42");
        assert_eq!(
            render(&Expr::Literal {
                value: LiteralValue::Float(2.5),
                data_type: DataType::Double
            }),
            "2.5"
        );
        assert_eq!(render(&string("O'Brien")), "'O''Brien'");
    }

    #[test]
    fn data_type_null_forces_null_even_with_a_value() {
        assert_eq!(
            render(&Expr::Literal {
                value: LiteralValue::Integer(7),
                data_type: DataType::Null
            }),
            "NULL"
        );
    }

    #[test]
    fn binary_ops_are_fully_parenthesized_at_each_level() {
        let plus = Expr::BinaryOp {
            op: BinaryOpType::Add,
            left: Box::new(col("t", "a")),
            right: Box::new(int(1)),
        };
        assert_eq!(render(&plus), "(\"t\".\"a\" + 1)");

        let eq = Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(col("t", "a")),
            right: Box::new(string("x")),
        };
        assert_eq!(render(&eq), "(\"t\".\"a\" = 'x')");

        let and = Expr::BinaryOp {
            op: BinaryOpType::And,
            left: Box::new(plus),
            right: Box::new(eq),
        };
        assert_eq!(render(&and), "((\"t\".\"a\" + 1) AND (\"t\".\"a\" = 'x'))");
    }

    #[test]
    fn unary_ops_in_the_right_shape() {
        let is_null = Expr::UnaryOp {
            op: UnaryOpType::IsNull,
            operand: Box::new(col("t", "a")),
        };
        assert_eq!(render(&is_null), "(\"t\".\"a\" IS NULL)");
        let is_not_null = Expr::UnaryOp {
            op: UnaryOpType::IsNotNull,
            operand: Box::new(col("t", "a")),
        };
        assert_eq!(render(&is_not_null), "(\"t\".\"a\" IS NOT NULL)");
        let not = Expr::UnaryOp {
            op: UnaryOpType::Not,
            operand: Box::new(is_null.clone()),
        };
        assert_eq!(render(&not), "(NOT (\"t\".\"a\" IS NULL))");
        let neg = Expr::UnaryOp {
            op: UnaryOpType::Negate,
            operand: Box::new(col("t", "a")),
        };
        assert_eq!(render(&neg), "(-\"t\".\"a\")");
    }

    fn func(name: &str, args: Vec<Expr>, distinct: bool) -> Expr {
        Expr::FunctionCall {
            function_name: name.to_string(),
            args,
            is_aggregate: true,
            distinct,
            within_group_key: None,
            within_group_desc: false,
        }
    }

    #[test]
    fn function_calls_with_distinct() {
        assert_eq!(
            render(&func("SUM", vec![col("t", "x")], false)),
            "SUM(\"t\".\"x\")"
        );
        assert_eq!(
            render(&func("COUNT", vec![col("t", "x")], true)),
            "COUNT(DISTINCT \"t\".\"x\")"
        );
    }

    #[test]
    fn ordered_set_within_group_asc_and_desc() {
        let asc = Expr::FunctionCall {
            function_name: "PERCENTILE_CONT".to_string(),
            args: vec![Expr::Literal {
                value: LiteralValue::Float(0.5),
                data_type: DataType::Double,
            }],
            is_aggregate: true,
            distinct: false,
            within_group_key: Some(Box::new(col("t", "x"))),
            within_group_desc: false,
        };
        assert_eq!(
            render(&asc),
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY \"t\".\"x\")"
        );
        let desc = Expr::FunctionCall {
            function_name: "PERCENTILE_CONT".to_string(),
            args: vec![Expr::Literal {
                value: LiteralValue::Float(0.5),
                data_type: DataType::Double,
            }],
            is_aggregate: true,
            distinct: false,
            within_group_key: Some(Box::new(col("t", "x"))),
            within_group_desc: true,
        };
        assert_eq!(
            render(&desc),
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY \"t\".\"x\" DESC)"
        );
    }

    #[test]
    fn case_with_and_without_else() {
        let with_else = Expr::Case {
            when_clauses: vec![(
                Expr::BinaryOp {
                    op: BinaryOpType::Gt,
                    left: Box::new(col("t", "a")),
                    right: Box::new(int(0)),
                },
                string("pos"),
            )],
            else_result: Some(Box::new(string("neg"))),
        };
        assert_eq!(
            render(&with_else),
            "CASE WHEN (\"t\".\"a\" > 0) THEN 'pos' ELSE 'neg' END"
        );
        let without_else = Expr::Case {
            when_clauses: vec![(
                Expr::BinaryOp {
                    op: BinaryOpType::Gt,
                    left: Box::new(col("t", "a")),
                    right: Box::new(int(0)),
                },
                string("pos"),
            )],
            else_result: None,
        };
        assert_eq!(
            render(&without_else),
            "CASE WHEN (\"t\".\"a\" > 0) THEN 'pos' END"
        );
    }

    #[test]
    fn in_list_between_cast_extract_interval_tuple() {
        let in_list = Expr::InList {
            value: Box::new(col("t", "a")),
            options: vec![int(1), int(2)],
        };
        assert_eq!(render(&in_list), "(\"t\".\"a\" IN (1, 2))");

        let between = Expr::Between {
            value: Box::new(col("t", "a")),
            lower: Box::new(int(1)),
            upper: Box::new(int(9)),
        };
        assert_eq!(render(&between), "(\"t\".\"a\" BETWEEN 1 AND 9)");

        let cast = Expr::Cast {
            expr: Box::new(col("t", "a")),
            target_type: "INTEGER".to_string(),
            data_type: Some(DataType::Integer),
        };
        assert_eq!(render(&cast), "CAST(\"t\".\"a\" AS INTEGER)");

        let cast_param = Expr::Cast {
            expr: Box::new(col("t", "a")),
            target_type: "DECIMAL(10,2)".to_string(),
            data_type: Some(DataType::Decimal),
        };
        assert_eq!(render(&cast_param), "CAST(\"t\".\"a\" AS DECIMAL(10,2))");

        let extract = Expr::Extract {
            field: "YEAR".to_string(),
            source: Box::new(col("t", "d")),
        };
        assert_eq!(render(&extract), "EXTRACT(YEAR FROM \"t\".\"d\")");

        let interval_unit = Expr::Interval {
            value: "2".to_string(),
            unit: Some("DAY".to_string()),
        };
        assert_eq!(render(&interval_unit), "INTERVAL '2' DAY");
        let interval_bare = Expr::Interval {
            value: "3".to_string(),
            unit: None,
        };
        assert_eq!(render(&interval_bare), "INTERVAL '3'");

        let tuple = Expr::Tuple {
            items: vec![int(1), string("x")],
        };
        assert_eq!(render(&tuple), "(1, 'x')");
    }

    fn window(
        partition_by: Vec<Expr>,
        order_keys: Vec<Expr>,
        order_ascending: Vec<bool>,
        order_nulls: Vec<Option<NullsOrder>>,
        frame: Option<String>,
    ) -> Expr {
        Expr::Window {
            function: Box::new(func("ROW_NUMBER", vec![], false)),
            partition_by,
            order_keys,
            order_ascending,
            order_nulls,
            frame,
        }
    }

    #[test]
    fn window_partition_order_defaults_and_overrides() {
        let asc = window(
            vec![col("t", "p")],
            vec![col("t", "k")],
            vec![true],
            vec![None],
            None,
        );
        assert_eq!(
            render(&asc),
            "ROW_NUMBER() OVER (PARTITION BY \"t\".\"p\" ORDER BY \"t\".\"k\" NULLS LAST)"
        );
        let desc = window(vec![], vec![col("t", "k")], vec![false], vec![None], None);
        assert_eq!(
            render(&desc),
            "ROW_NUMBER() OVER (ORDER BY \"t\".\"k\" DESC NULLS FIRST)"
        );
        let override_nulls = window(
            vec![],
            vec![col("t", "k")],
            vec![true],
            vec![Some(NullsOrder::First)],
            None,
        );
        assert_eq!(
            render(&override_nulls),
            "ROW_NUMBER() OVER (ORDER BY \"t\".\"k\" NULLS FIRST)"
        );
    }

    #[test]
    fn window_with_frame_and_bare_over() {
        let framed = window(
            vec![],
            vec![col("t", "k")],
            vec![true],
            vec![None],
            Some("ROWS BETWEEN 1 PRECEDING AND CURRENT ROW".to_string()),
        );
        assert_eq!(
            render(&framed),
            "ROW_NUMBER() OVER (ORDER BY \"t\".\"k\" NULLS LAST ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
        );
        let bare = window(vec![], vec![], vec![], vec![], None);
        assert_eq!(render(&bare), "ROW_NUMBER() OVER ()");
    }

    fn empty_subplan() -> Box<LogicalPlan> {
        Box::new(LogicalPlan::Values(Values {
            rows: Vec::new(),
            output_names: Vec::new(),
        }))
    }

    #[test]
    fn subquery_variants_each_raise_with_their_label() {
        let resolver = SourceResolver;
        let sink: &dyn ColumnResolver = &resolver;
        let cases = [
            (
                Expr::Subquery {
                    subquery: empty_subplan(),
                },
                "SubqueryExpression",
            ),
            (
                Expr::Exists {
                    subquery: empty_subplan(),
                    negated: false,
                },
                "ExistsExpression",
            ),
            (
                Expr::InSubquery {
                    value: Box::new(col("t", "a")),
                    subquery: empty_subplan(),
                    negated: false,
                },
                "InSubquery",
            ),
            (
                Expr::QuantifiedComparison {
                    operator: BinaryOpType::Eq,
                    quantifier: fq_plan::Quantifier::Any,
                    left: Box::new(col("t", "a")),
                    subquery: empty_subplan(),
                },
                "QuantifiedComparison",
            ),
        ];
        for (expr, label) in cases {
            assert_eq!(
                render_expr(&expr, sink).unwrap_err(),
                EmitError::SubqueryReachedEmit(label)
            );
        }
    }

    #[test]
    fn canonical_shapes_round_trip_through_polyglot() {
        // The load-bearing invariant: our canonical text parses as Postgres and
        // transpiles to DuckDb (polyglot re-parses it at the transpile boundary).
        let predicate = Expr::BinaryOp {
            op: BinaryOpType::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOpType::Gt,
                left: Box::new(col("t", "a")),
                right: Box::new(int(1)),
            }),
            right: Box::new(Expr::UnaryOp {
                op: UnaryOpType::IsNotNull,
                operand: Box::new(col("t", "b")),
            }),
        };
        let case = Expr::Case {
            when_clauses: vec![(
                Expr::BinaryOp {
                    op: BinaryOpType::Gt,
                    left: Box::new(col("t", "a")),
                    right: Box::new(int(0)),
                },
                string("pos"),
            )],
            else_result: Some(Box::new(string("neg"))),
        };
        let cast = Expr::Cast {
            expr: Box::new(col("t", "a")),
            target_type: "INTEGER".to_string(),
            data_type: Some(DataType::Integer),
        };
        let interval = Expr::Interval {
            value: "2".to_string(),
            unit: Some("DAY".to_string()),
        };
        let win = window(
            vec![col("t", "p")],
            vec![col("t", "k")],
            vec![false],
            vec![Some(NullsOrder::Last)],
            Some("ROWS BETWEEN 1 PRECEDING AND CURRENT ROW".to_string()),
        );
        for expr in [predicate, case, cast, interval, win] {
            let rendered = format!("SELECT {} FROM \"main\".\"t\"", render(&expr));
            to_source_sql(&rendered, Dialect::Postgres)
                .unwrap_or_else(|e| panic!("postgres round-trip failed for {rendered}: {e}"));
            to_source_sql(&rendered, Dialect::DuckDb)
                .unwrap_or_else(|e| panic!("duckdb round-trip failed for {rendered}: {e}"));
        }
    }

    #[test]
    fn divide_round_trips() {
        let div = Expr::BinaryOp {
            op: BinaryOpType::Divide,
            left: Box::new(col("t", "a")),
            right: Box::new(int(2)),
        };
        assert_eq!(render(&div), "(\"t\".\"a\" / 2)");
        let rendered = format!("SELECT {} FROM \"main\".\"t\"", render(&div));
        to_source_sql(&rendered, Dialect::Postgres).unwrap();
        to_source_sql(&rendered, Dialect::DuckDb).unwrap();
    }

    #[test]
    fn negate_of_sign_leading_operand_stays_parseable() {
        // A negative numeric literal under NEGATE must not fuse into `--` (a SQL
        // line comment). The int and float cases both parenthesize the operand.
        for value in [
            Expr::Literal {
                value: LiteralValue::Integer(-5),
                data_type: DataType::Integer,
            },
            Expr::Literal {
                value: LiteralValue::Float(-2.5),
                data_type: DataType::Double,
            },
        ] {
            let neg = Expr::UnaryOp {
                op: UnaryOpType::Negate,
                operand: Box::new(value),
            };
            let text = render(&neg);
            assert!(text.starts_with("(-("), "expected wrapped operand: {text}");
            let rendered = format!("SELECT {text} FROM \"main\".\"t\"");
            to_source_sql(&rendered, Dialect::Postgres).unwrap();
            to_source_sql(&rendered, Dialect::DuckDb).unwrap();
        }
        // A plain negate of a non-sign operand keeps the compact form.
        let neg_col = Expr::UnaryOp {
            op: UnaryOpType::Negate,
            operand: Box::new(col("t", "a")),
        };
        assert_eq!(render(&neg_col), "(-\"t\".\"a\")");
    }

    #[test]
    fn non_finite_float_literal_raises() {
        // NaN / Infinity have no canonical SQL token; the emitter RAISES rather
        // than shipping an unparseable `NaN`/`inf`.
        for f in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let lit = Expr::Literal {
                value: LiteralValue::Float(f),
                data_type: DataType::Double,
            };
            let resolver = SourceResolver;
            let sink: &dyn ColumnResolver = &resolver;
            assert!(matches!(
                render_expr(&lit, sink),
                Err(EmitError::UnrepresentableLiteral(_))
            ));
        }
    }
}
