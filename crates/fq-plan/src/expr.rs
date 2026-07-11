//! Expression nodes and the shared expression-tree walkers. Ports
//! `plan/expressions.py`.
//!
//! `Expr` is ONE enum with a variant per expression node. Every traversal is an
//! exhaustive `match` with no `_` arm, so adding a variant breaks every walker at
//! compile time - this is what replaces `test_expression_walker_exhaustiveness.py`
//! (the compiler now owns that guarantee).
//!
//! PORT NOTES (never silent):
//! - The `ExpressionVisitor` ABC and per-node `accept`/`visit_*` machinery retire.
//!   With an enum there is no double-dispatch to keep in sync; a `match` is the
//!   dispatch. `test_visitor_interface_covers_every_expression_type` is therefore
//!   compiler-enforced; the dispatch test becomes `variant_label` (below) and the
//!   nested-collection test becomes a `column_refs` ordering test.
//! - `Literal.value: Any` becomes the typed `LiteralValue` enum. `NULLS
//!   FIRST/LAST` string fields become the `NullsOrder` enum.
//! - `to_sql` (emitter delegation) is DEFERRED to fq-emit; nothing in fq-plan
//!   renders SQL.
//! - `split_where_having` / `aggregate_output_map` (shared utilities that live in
//!   expressions.py) are DEFERRED until their consumers - the binder and the
//!   pushdown rules - land, so they are ported with tests then, not dead now.

use fq_common::DataType;

use crate::logical::LogicalPlan;

/// A literal value. Replaces Python's untyped `Literal.value: Any` with the SQL
/// literal shapes the parser actually produces.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// NULLS ordering for a sort/window ORDER BY key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

/// Binary operator types. The string form (`value()`) is the SQL token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOpType {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    // Comparison
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    // Null-safe comparison (NULL is a comparable value)
    NullSafeEq,
    NullSafeNeq,
    // Logical
    And,
    Or,
    // String
    Concat,
    Like,
    Ilike,
    RegexMatch,
    RegexImatch,
}

impl BinaryOpType {
    /// The SQL token for this operator (Python's `BinaryOpType.value`).
    pub fn value(self) -> &'static str {
        match self {
            BinaryOpType::Add => "+",
            BinaryOpType::Subtract => "-",
            BinaryOpType::Multiply => "*",
            BinaryOpType::Divide => "/",
            BinaryOpType::Modulo => "%",
            BinaryOpType::Eq => "=",
            BinaryOpType::Neq => "!=",
            BinaryOpType::Lt => "<",
            BinaryOpType::Lte => "<=",
            BinaryOpType::Gt => ">",
            BinaryOpType::Gte => ">=",
            BinaryOpType::NullSafeEq => "IS NOT DISTINCT FROM",
            BinaryOpType::NullSafeNeq => "IS DISTINCT FROM",
            BinaryOpType::And => "AND",
            BinaryOpType::Or => "OR",
            BinaryOpType::Concat => "||",
            BinaryOpType::Like => "LIKE",
            BinaryOpType::Ilike => "ILIKE",
            BinaryOpType::RegexMatch => "~",
            BinaryOpType::RegexImatch => "~*",
        }
    }

    /// Whether this operator yields a boolean (comparison / logical / matching).
    fn is_boolean_result(self) -> bool {
        matches!(
            self,
            BinaryOpType::And
                | BinaryOpType::Or
                | BinaryOpType::Eq
                | BinaryOpType::Neq
                | BinaryOpType::Lt
                | BinaryOpType::Lte
                | BinaryOpType::Gt
                | BinaryOpType::Gte
                | BinaryOpType::Like
                | BinaryOpType::Ilike
                | BinaryOpType::NullSafeEq
                | BinaryOpType::NullSafeNeq
                | BinaryOpType::RegexMatch
                | BinaryOpType::RegexImatch
        )
    }
}

/// Unary operator types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOpType {
    Not,
    Negate,
    IsNull,
    IsNotNull,
}

impl UnaryOpType {
    /// The SQL token for this operator (Python's `UnaryOpType.value`).
    pub fn value(self) -> &'static str {
        match self {
            UnaryOpType::Not => "NOT",
            UnaryOpType::Negate => "-",
            UnaryOpType::IsNull => "IS NULL",
            UnaryOpType::IsNotNull => "IS NOT NULL",
        }
    }
}

/// Quantifier for a quantified comparison (`op ANY/SOME/ALL (subquery)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantifier {
    Any,
    Some,
    All,
}

impl Quantifier {
    /// The SQL keyword for this quantifier.
    pub fn value(self) -> &'static str {
        match self {
            Quantifier::Any => "ANY",
            Quantifier::Some => "SOME",
            Quantifier::All => "ALL",
        }
    }
}

/// A column reference. A standalone struct (not inline variant fields) so the
/// walkers can collect `&ColumnRef` directly. `table` is None for an unqualified
/// reference (which must not survive binding); `data_type` is set during binding.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
    pub data_type: Option<DataType>,
}

impl ColumnRef {
    /// Build a (possibly qualified) column reference.
    pub fn new(
        table: Option<String>,
        column: impl Into<String>,
        data_type: Option<DataType>,
    ) -> Self {
        Self {
            table,
            column: column.into(),
            data_type,
        }
    }
}

/// A WHEN branch of a CASE expression: (condition, result).
pub type WhenClause = (Expr, Expr);

/// An expression node. One enum, exhaustively matched by every walker.
///
/// The subquery-bearing variants (`Subquery`, `Exists`, `InSubquery`,
/// `QuantifiedComparison`) hold a `LogicalPlan` and exist ONLY before
/// decorrelation removes them. Generic walks do not descend into them (a
/// subquery's references belong to its own scope) - see `children`.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(ColumnRef),
    Literal {
        value: LiteralValue,
        data_type: DataType,
    },
    BinaryOp {
        op: BinaryOpType,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOpType,
        operand: Box<Expr>,
    },
    FunctionCall {
        function_name: String,
        args: Vec<Expr>,
        is_aggregate: bool,
        distinct: bool,
        /// Ordered-set aggregates carry their WITHIN GROUP sort key here.
        within_group_key: Option<Box<Expr>>,
        within_group_desc: bool,
    },
    Case {
        when_clauses: Vec<WhenClause>,
        else_result: Option<Box<Expr>>,
    },
    InList {
        value: Box<Expr>,
        options: Vec<Expr>,
    },
    Between {
        value: Box<Expr>,
        lower: Box<Expr>,
        upper: Box<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        /// Original SQL type text, so the cast re-renders verbatim to a source.
        target_type: String,
        data_type: Option<DataType>,
    },
    Window {
        function: Box<Expr>,
        partition_by: Vec<Expr>,
        order_keys: Vec<Expr>,
        order_ascending: Vec<bool>,
        order_nulls: Vec<Option<NullsOrder>>,
        /// Raw frame clause text (`ROWS BETWEEN ...`), verbatim; None when absent.
        frame: Option<String>,
    },
    Extract {
        field: String,
        source: Box<Expr>,
    },
    Interval {
        value: String,
        unit: Option<String>,
    },
    Tuple {
        items: Vec<Expr>,
    },
    // --- subquery-bearing (pre-decorrelation only) ---
    Subquery {
        subquery: Box<LogicalPlan>,
    },
    Exists {
        subquery: Box<LogicalPlan>,
        negated: bool,
    },
    InSubquery {
        value: Box<Expr>,
        subquery: Box<LogicalPlan>,
        negated: bool,
    },
    QuantifiedComparison {
        operator: BinaryOpType,
        quantifier: Quantifier,
        left: Box<Expr>,
        subquery: Box<LogicalPlan>,
    },
}

impl Expr {
    /// The hook label this node dispatched to under the retired visitor (e.g.
    /// `column_ref`, `binary_op`). Kept as the compile-checked stand-in for the
    /// Python visitor-dispatch test; each variant maps to a distinct label.
    pub fn variant_label(&self) -> &'static str {
        match self {
            Expr::Column(_) => "column_ref",
            Expr::Literal { .. } => "literal",
            Expr::BinaryOp { .. } => "binary_op",
            Expr::UnaryOp { .. } => "unary_op",
            Expr::FunctionCall { .. } => "function_call",
            Expr::Case { .. } => "case_expr",
            Expr::InList { .. } => "in_list",
            Expr::Between { .. } => "between",
            Expr::Cast { .. } => "cast",
            Expr::Window { .. } => "window_expr",
            Expr::Extract { .. } => "extract",
            Expr::Interval { .. } => "interval",
            Expr::Tuple { .. } => "tuple",
            Expr::Subquery { .. } => "subquery",
            Expr::Exists { .. } => "exists",
            Expr::InSubquery { .. } => "in_subquery",
            Expr::QuantifiedComparison { .. } => "quantified_comparison",
        }
    }

    /// The data type of this expression. Mirrors `Expression.get_type`.
    ///
    /// # Panics
    /// Panics on an unbound `Column`/`Cast` (no `data_type` set) - the Python
    /// `NotImplementedError("Type must be set during binding")` contract: a
    /// programming error (get_type before binding), surfaced loudly rather than
    /// defaulted to a wrong type.
    pub fn get_type(&self) -> DataType {
        match self {
            Expr::Column(col) => col
                .data_type
                .expect("ColumnRef type must be set during binding"),
            Expr::Cast { data_type, .. } => {
                data_type.expect("Cast type must be set during binding")
            }
            Expr::Literal { data_type, .. } => *data_type,
            Expr::BinaryOp { op, left, right } => binary_op_type(*op, left, right),
            Expr::UnaryOp { op, operand } => match op {
                UnaryOpType::Not | UnaryOpType::IsNull | UnaryOpType::IsNotNull => {
                    DataType::Boolean
                }
                UnaryOpType::Negate => operand.get_type(),
            },
            Expr::FunctionCall { function_name, .. } => function_call_type(function_name),
            Expr::Case {
                when_clauses,
                else_result,
            } => case_type(when_clauses, else_result.as_deref()),
            Expr::InList { .. } | Expr::Between { .. } => DataType::Boolean,
            Expr::Window { function, .. } => function.get_type(),
            Expr::Extract { .. } => DataType::BigInt,
            Expr::Interval { .. } => DataType::Interval,
            Expr::Tuple { .. } | Expr::Subquery { .. } => DataType::Null,
            Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::QuantifiedComparison { .. } => {
                DataType::Boolean
            }
        }
    }

    /// Direct child expressions of this node. Exhaustive: a new variant forces a
    /// new arm at compile time, so a tree walk can never silently drop children.
    ///
    /// Leaves (`Column`/`Literal`/`Interval`) have none, and the subquery-bearing
    /// nodes deliberately expose none so a generic walk never descends into a
    /// subquery's inner scope (matches Python `_NO_CHILD_EXPRESSIONS`).
    pub fn children(&self) -> Vec<&Expr> {
        match self {
            Expr::Column(_)
            | Expr::Literal { .. }
            | Expr::Interval { .. }
            | Expr::Subquery { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. } => Vec::new(),
            Expr::BinaryOp { left, right, .. } => vec![left, right],
            Expr::UnaryOp { operand, .. } => vec![operand],
            Expr::Cast { expr, .. } => vec![expr],
            Expr::Extract { source, .. } => vec![source],
            Expr::FunctionCall {
                args,
                within_group_key,
                ..
            } => {
                let mut children: Vec<&Expr> = args.iter().collect();
                if let Some(key) = within_group_key {
                    children.push(key);
                }
                children
            }
            Expr::Case {
                when_clauses,
                else_result,
            } => {
                let mut children = Vec::new();
                for (condition, result) in when_clauses {
                    children.push(condition);
                    children.push(result);
                }
                if let Some(else_expr) = else_result {
                    children.push(else_expr);
                }
                children
            }
            Expr::InList { value, options } => {
                let mut children = vec![value.as_ref()];
                children.extend(options.iter());
                children
            }
            Expr::Between {
                value,
                lower,
                upper,
            } => vec![value, lower, upper],
            Expr::Tuple { items } => items.iter().collect(),
            Expr::Window {
                function,
                partition_by,
                order_keys,
                ..
            } => {
                let mut children = vec![function.as_ref()];
                children.extend(partition_by.iter());
                children.extend(order_keys.iter());
                children
            }
        }
    }

    /// Rebuild this node with `f` applied to each direct child expression. Leaves
    /// and subquery-boundary nodes pass through unchanged. Mirrors Python
    /// `map_children`; exhaustive by construction.
    #[must_use]
    pub fn map_children(self, f: &mut impl FnMut(Expr) -> Expr) -> Expr {
        match self {
            Expr::Column(_)
            | Expr::Literal { .. }
            | Expr::Interval { .. }
            | Expr::Subquery { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. } => self,
            Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
                op,
                left: Box::new(f(*left)),
                right: Box::new(f(*right)),
            },
            Expr::UnaryOp { op, operand } => Expr::UnaryOp {
                op,
                operand: Box::new(f(*operand)),
            },
            Expr::Cast {
                expr,
                target_type,
                data_type,
            } => Expr::Cast {
                expr: Box::new(f(*expr)),
                target_type,
                data_type,
            },
            Expr::Extract { field, source } => Expr::Extract {
                field,
                source: Box::new(f(*source)),
            },
            Expr::FunctionCall {
                function_name,
                args,
                is_aggregate,
                distinct,
                within_group_key,
                within_group_desc,
            } => Expr::FunctionCall {
                function_name,
                args: map_vec(args, f),
                is_aggregate,
                distinct,
                within_group_key: within_group_key.map(|key| Box::new(f(*key))),
                within_group_desc,
            },
            Expr::Case {
                when_clauses,
                else_result,
            } => Expr::Case {
                when_clauses: when_clauses
                    .into_iter()
                    .map(|(condition, result)| (f(condition), f(result)))
                    .collect(),
                else_result: else_result.map(|else_expr| Box::new(f(*else_expr))),
            },
            Expr::InList { value, options } => Expr::InList {
                value: Box::new(f(*value)),
                options: map_vec(options, f),
            },
            Expr::Between {
                value,
                lower,
                upper,
            } => Expr::Between {
                value: Box::new(f(*value)),
                lower: Box::new(f(*lower)),
                upper: Box::new(f(*upper)),
            },
            Expr::Tuple { items } => Expr::Tuple {
                items: map_vec(items, f),
            },
            Expr::Window {
                function,
                partition_by,
                order_keys,
                order_ascending,
                order_nulls,
                frame,
            } => Expr::Window {
                function: Box::new(f(*function)),
                partition_by: map_vec(partition_by, f),
                order_keys: map_vec(order_keys, f),
                order_ascending,
                order_nulls,
                frame,
            },
        }
    }

    /// Whether this expression is an aggregate function call.
    pub fn is_aggregate_call(&self) -> bool {
        matches!(
            self,
            Expr::FunctionCall {
                is_aggregate: true,
                ..
            }
        )
    }
}

/// Apply `f` to each expression in a vector.
fn map_vec(items: Vec<Expr>, f: &mut impl FnMut(Expr) -> Expr) -> Vec<Expr> {
    items.into_iter().map(f).collect()
}

/// Result type of a binary op: boolean for comparison/logical/matching, VARCHAR
/// for concat, else the wider of the two numeric operand types.
fn binary_op_type(op: BinaryOpType, left: &Expr, right: &Expr) -> DataType {
    if op.is_boolean_result() {
        return DataType::Boolean;
    }
    if op == BinaryOpType::Concat {
        return DataType::Varchar;
    }
    wider_numeric(left.get_type(), right.get_type())
}

/// Numeric widening rank for arithmetic result inference; None for non-numeric.
fn numeric_rank(data_type: DataType) -> Option<u8> {
    match data_type {
        DataType::Boolean => Some(0),
        DataType::Integer => Some(1),
        DataType::BigInt => Some(2),
        DataType::Decimal => Some(3),
        DataType::Float => Some(4),
        DataType::Double => Some(5),
        _ => None,
    }
}

/// The wider of two numeric operand types; falls back to `left` when either is
/// non-numeric (date/interval arithmetic handled elsewhere).
fn wider_numeric(left: DataType, right: DataType) -> DataType {
    match (numeric_rank(left), numeric_rank(right)) {
        (Some(l), Some(r)) if r > l => right,
        _ => left,
    }
}

/// Result type of a function call by family: COUNT/SUM and the integer ranking
/// window functions yield BIGINT, AVG yields DOUBLE, anything else VARCHAR.
fn function_call_type(function_name: &str) -> DataType {
    match function_name.to_uppercase().as_str() {
        "COUNT" | "SUM" | "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => DataType::BigInt,
        "AVG" => DataType::Double,
        _ => DataType::Varchar,
    }
}

/// Result type of a CASE: the type of the first result, else the ELSE, else NULL.
fn case_type(when_clauses: &[WhenClause], else_result: Option<&Expr>) -> DataType {
    if let Some((_, result)) = when_clauses.first() {
        return result.get_type();
    }
    if let Some(else_expr) = else_result {
        return else_expr.get_type();
    }
    DataType::Null
}

// ---------------------------------------------------------------------------
// Shared expression-tree utilities - the single home for these operations.
// ---------------------------------------------------------------------------

/// Every `ColumnRef` in an expression tree, in left-to-right order, not
/// descending into subqueries.
pub fn column_refs(expr: &Expr) -> Vec<&ColumnRef> {
    let mut refs = Vec::new();
    collect_column_refs(expr, &mut refs);
    refs
}

/// Recursive helper for `column_refs`.
fn collect_column_refs<'a>(expr: &'a Expr, refs: &mut Vec<&'a ColumnRef>) {
    if let Expr::Column(col) = expr {
        refs.push(col);
    }
    for child in expr.children() {
        collect_column_refs(child, refs);
    }
}

/// Whether an expression tree contains an aggregate function call.
pub fn contains_aggregate(expr: &Expr) -> bool {
    if expr.is_aggregate_call() {
        return true;
    }
    expr.children()
        .iter()
        .any(|child| contains_aggregate(child))
}

/// Whether an expression tree contains a window function. Ports the Python
/// `_expression_contains_window`; used to route a windowed aggregate output
/// through the raw SQL emitter (a window over grouped aggregates cannot be
/// expressed by the structured aggregate fragment).
pub fn contains_window(expr: &Expr) -> bool {
    if matches!(expr, Expr::Window { .. }) {
        return true;
    }
    expr.children().iter().any(|child| contains_window(child))
}

/// Flatten the top-level AND chain of a predicate into its conjuncts.
pub fn split_conjuncts(expr: &Expr) -> Vec<&Expr> {
    let mut out = Vec::new();
    split_on(expr, BinaryOpType::And, &mut out);
    out
}

/// Flatten the top-level OR chain of a predicate into its disjuncts.
pub fn split_disjuncts(expr: &Expr) -> Vec<&Expr> {
    let mut out = Vec::new();
    split_on(expr, BinaryOpType::Or, &mut out);
    out
}

/// Recursive helper: collect the operands of a top-level chain of `chain_op`.
fn split_on<'a>(expr: &'a Expr, chain_op: BinaryOpType, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryOp { op, left, right } = expr {
        if *op == chain_op {
            split_on(left, chain_op, out);
            split_on(right, chain_op, out);
            return;
        }
    }
    out.push(expr);
}

/// AND a list of predicates into one expression, or None when empty.
pub fn combine_and(terms: Vec<Expr>) -> Option<Expr> {
    combine(terms, BinaryOpType::And)
}

/// OR a list of predicates into one expression, or None when empty.
pub fn combine_or(terms: Vec<Expr>) -> Option<Expr> {
    combine(terms, BinaryOpType::Or)
}

/// Left-fold a list of predicates with a binary operator.
fn combine(terms: Vec<Expr>, op: BinaryOpType) -> Option<Expr> {
    let mut iter = terms.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, term| Expr::BinaryOp {
        op,
        left: Box::new(acc),
        right: Box::new(term),
    }))
}

/// None-safe AND of two predicate expressions.
pub fn and_expressions(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (None, right) => right,
        (left, None) => left,
        (Some(left), Some(right)) => Some(Expr::BinaryOp {
            op: BinaryOpType::And,
            left: Box::new(left),
            right: Box::new(right),
        }),
    }
}

/// Map from an aggregate output name to its aggregate expression.
///
/// The `output_map` for `split_where_having`: an aggregate-bearing scan/node
/// carries positionally aligned `output_names` and `aggregates`; each output that
/// is an aggregate call maps to its expression (e.g. `total -> SUM(x)`). Ports
/// `aggregate_output_map`.
pub fn aggregate_output_map(
    output_names: &[String],
    aggregates: &[Expr],
) -> std::collections::HashMap<String, Expr> {
    let mut mapping = std::collections::HashMap::new();
    for (name, expression) in output_names.iter().zip(aggregates.iter()) {
        if expression.is_aggregate_call() {
            mapping.insert(name.clone(), expression.clone());
        }
    }
    mapping
}

/// Split a merged scan predicate into `(where_predicate, having_predicate)`.
///
/// The single source of truth for WHERE-vs-HAVING placement (ports
/// `split_where_having`). When a GROUP BY query folds into one scan, its WHERE
/// and HAVING conjuncts live together in the scan filter. A conjunct is HAVING if
/// it references an aggregate-output column (a key of `output_map`, e.g. the
/// alias `total` the binder rewrote `SUM(x)` to) OR directly contains an
/// aggregate; its aggregate-output refs are substituted back to the aggregate
/// expression so the source sees `HAVING SUM(x) > 10` rather than an alias. Every
/// other conjunct is WHERE. Either side may be None.
// The output map is always built by `aggregate_output_map` (default hasher), so
// generalizing over the hasher would only add noise.
#[allow(clippy::implicit_hasher)]
pub fn split_where_having(
    predicate: &Expr,
    output_map: &std::collections::HashMap<String, Expr>,
) -> (Option<Expr>, Option<Expr>) {
    let mut where_terms = Vec::new();
    let mut having_terms = Vec::new();
    for conjunct in split_conjuncts(predicate) {
        if is_having_conjunct(conjunct, output_map) {
            having_terms.push(substitute_aggregate_refs(conjunct.clone(), output_map));
        } else {
            where_terms.push(conjunct.clone());
        }
    }
    (combine_and(where_terms), combine_and(having_terms))
}

/// Whether a conjunct belongs in HAVING (references an aggregate).
fn is_having_conjunct(expr: &Expr, output_map: &std::collections::HashMap<String, Expr>) -> bool {
    if contains_aggregate(expr) {
        return true;
    }
    column_refs(expr)
        .iter()
        .any(|col| output_map.contains_key(&col.column))
}

/// Replace aggregate-output column refs with their aggregate expressions.
fn substitute_aggregate_refs(
    expr: Expr,
    output_map: &std::collections::HashMap<String, Expr>,
) -> Expr {
    if let Expr::Column(col) = &expr {
        if let Some(aggregate) = output_map.get(&col.column) {
            return aggregate.clone();
        }
        return expr;
    }
    expr.map_children(&mut |child| substitute_aggregate_refs(child, output_map))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> Expr {
        Expr::Column(ColumnRef::new(None, name, Some(DataType::Integer)))
    }

    fn int(value: i64) -> Expr {
        Expr::Literal {
            value: LiteralValue::Integer(value),
            data_type: DataType::Integer,
        }
    }

    fn window(inner: Expr) -> Expr {
        Expr::Window {
            function: Box::new(inner),
            partition_by: Vec::new(),
            order_keys: Vec::new(),
            order_ascending: Vec::new(),
            order_nulls: Vec::new(),
            frame: None,
        }
    }

    #[test]
    fn contains_window_detects_direct_and_nested() {
        // Direct window.
        assert!(contains_window(&window(col("a"))));
        // Window nested inside an arithmetic expression is found via children.
        let nested = Expr::BinaryOp {
            op: BinaryOpType::Add,
            left: Box::new(window(col("a"))),
            right: Box::new(int(1)),
        };
        assert!(contains_window(&nested));
        // No window present.
        assert!(!contains_window(&Expr::BinaryOp {
            op: BinaryOpType::Add,
            left: Box::new(col("a")),
            right: Box::new(int(1)),
        }));
    }

    #[test]
    fn get_type_comparison_is_boolean() {
        let expr = Expr::BinaryOp {
            op: BinaryOpType::Gt,
            left: Box::new(col("a")),
            right: Box::new(int(3)),
        };
        assert_eq!(expr.get_type(), DataType::Boolean);
    }

    #[test]
    fn get_type_arithmetic_widens() {
        let expr = Expr::BinaryOp {
            op: BinaryOpType::Multiply,
            left: Box::new(Expr::Column(ColumnRef::new(
                None,
                "a",
                Some(DataType::Integer),
            ))),
            right: Box::new(Expr::Column(ColumnRef::new(
                None,
                "b",
                Some(DataType::Double),
            ))),
        };
        assert_eq!(expr.get_type(), DataType::Double);
    }

    #[test]
    fn split_conjuncts_flattens_and_chain() {
        // (a > 1) AND (b > 2) AND (c > 3)
        let a = Expr::BinaryOp {
            op: BinaryOpType::Gt,
            left: Box::new(col("a")),
            right: Box::new(int(1)),
        };
        let b = Expr::BinaryOp {
            op: BinaryOpType::Gt,
            left: Box::new(col("b")),
            right: Box::new(int(2)),
        };
        let c = Expr::BinaryOp {
            op: BinaryOpType::Gt,
            left: Box::new(col("c")),
            right: Box::new(int(3)),
        };
        let combined = combine_and(vec![a.clone(), b.clone(), c.clone()]).unwrap();
        let parts = split_conjuncts(&combined);
        assert_eq!(parts, vec![&a, &b, &c]);
    }

    #[test]
    fn combine_and_empty_is_none() {
        assert_eq!(combine_and(vec![]), None);
    }

    fn sum_of(name: &str) -> Expr {
        Expr::FunctionCall {
            function_name: "SUM".to_string(),
            args: vec![col(name)],
            is_aggregate: true,
            distinct: false,
            within_group_key: None,
            within_group_desc: false,
        }
    }

    fn gt(left: Expr, value: i64) -> Expr {
        Expr::BinaryOp {
            op: BinaryOpType::Gt,
            left: Box::new(left),
            right: Box::new(int(value)),
        }
    }

    #[test]
    fn split_where_having_routes_and_substitutes() {
        // Folded predicate: (a > 1) AND (total > 10), where `total` is SUM(x).
        let output_map =
            aggregate_output_map(&["total".to_string()], std::slice::from_ref(&sum_of("x")));
        let predicate = combine_and(vec![gt(col("a"), 1), gt(col("total"), 10)]).unwrap();

        let (where_pred, having_pred) = split_where_having(&predicate, &output_map);

        // WHERE keeps the plain conjunct.
        assert_eq!(where_pred, Some(gt(col("a"), 1)));
        // HAVING carries the aggregate conjunct with `total` substituted to SUM(x).
        assert_eq!(having_pred, Some(gt(sum_of("x"), 10)));
    }

    #[test]
    fn split_where_having_all_where_when_no_aggregate() {
        let output_map = aggregate_output_map(&[], &[]);
        let predicate = gt(col("a"), 1);
        let (where_pred, having_pred) = split_where_having(&predicate, &output_map);
        assert_eq!(where_pred, Some(gt(col("a"), 1)));
        assert_eq!(having_pred, None);
    }

    #[test]
    fn contains_aggregate_finds_nested_sum() {
        // UPPER(SUM(a))
        let sum = Expr::FunctionCall {
            function_name: "SUM".to_string(),
            args: vec![col("a")],
            is_aggregate: true,
            distinct: false,
            within_group_key: None,
            within_group_desc: false,
        };
        let upper = Expr::FunctionCall {
            function_name: "UPPER".to_string(),
            args: vec![sum],
            is_aggregate: false,
            distinct: false,
            within_group_key: None,
            within_group_desc: false,
        };
        assert!(contains_aggregate(&upper));
        assert!(!contains_aggregate(&col("a")));
    }
}
