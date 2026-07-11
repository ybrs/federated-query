//! Logical plan nodes. Ports `plan/logical.py`.
//!
//! `LogicalPlan` is ONE enum wrapping a struct per node type. `children` and
//! `schema` are exhaustive matches, so a new node forces a new arm at compile
//! time.
//!
//! PORT NOTES (never silent):
//! - `try_map_children` is the fallible recurse-and-rebuild the decorrelator and
//!   the pushdown rules share (ports `transform_children`/`with_children`). It
//!   landed with its first consumer, fq-decorrelate.
//! - The `LogicalPlanVisitor` ABC retires (a `match` is the dispatch).
//! - `NodeId` (Python `id()`-identity for binder caches / injection dedup / CTE
//!   body sharing) is DEFERRED: the nodes are pure structural values now, and the
//!   identity mechanism lands with its first consumer (the binder), chosen clean
//!   rather than stamped speculatively.
//! - `direct_expressions` (Python's field-introspection over pydantic
//!   annotations) is a typed exhaustive accessor here; it landed with its first
//!   consumer, the decorrelation post-pass invariant (no subquery expression
//!   survives).

use std::collections::BTreeMap;

use crate::expr::{Expr, NullsOrder};

/// Join types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    /// For EXISTS.
    Semi,
    /// For NOT EXISTS.
    Anti,
}

impl JoinType {
    /// The SQL keyword for this join type (Python's `JoinType.value`).
    pub fn value(self) -> &'static str {
        match self {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            JoinType::Right => "RIGHT",
            JoinType::Full => "FULL",
            JoinType::Cross => "CROSS",
            JoinType::Semi => "SEMI",
            JoinType::Anti => "ANTI",
        }
    }
}

/// Aggregate function kinds (the named set the engine models directly).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

/// SQL set-operation kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

impl SetOpKind {
    /// The SQL keyword for this set-operation kind.
    pub fn value(self) -> &'static str {
        match self {
            SetOpKind::Union => "UNION",
            SetOpKind::Intersect => "INTERSECT",
            SetOpKind::Except => "EXCEPT",
        }
    }
}

/// Supported EXPLAIN output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    Text,
    Json,
}

impl ExplainFormat {
    /// The keyword for this format.
    pub fn value(self) -> &'static str {
        match self {
            ExplainFormat::Text => "TEXT",
            ExplainFormat::Json => "JSON",
        }
    }
}

/// Scan a base table from a data source, with any clauses pushdown folded in.
#[derive(Debug, Clone, PartialEq)]
pub struct Scan {
    pub datasource: String,
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub filters: Option<Expr>,
    pub alias: Option<String>,
    /// TABLESAMPLE clause as Postgres-form SQL, transpiled when rendered.
    pub sample: Option<String>,
    pub group_by: Option<Vec<Expr>>,
    pub grouping_sets: Option<Vec<Vec<Expr>>>,
    pub aggregates: Option<Vec<Expr>>,
    pub output_names: Option<Vec<String>>,
    pub limit: Option<u64>,
    pub offset: u64,
    pub order_by_keys: Option<Vec<Expr>>,
    pub order_by_ascending: Option<Vec<bool>>,
    pub order_by_nulls: Option<Vec<Option<NullsOrder>>>,
    pub distinct: bool,
    /// Cost-model row estimate under local filters, stamped by join ordering.
    pub estimated_rows: Option<u64>,
    /// Source-catalog NDV per join-key column (base, unfiltered).
    pub column_ndv: Option<BTreeMap<String, i64>>,
}

impl Scan {
    /// Build a minimal scan reading `columns` from a source table; all optional
    /// clauses default to absent. Derive richer scans with struct update syntax.
    pub fn new(
        datasource: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            datasource: datasource.into(),
            schema_name: schema_name.into(),
            table_name: table_name.into(),
            columns,
            filters: None,
            alias: None,
            sample: None,
            group_by: None,
            grouping_sets: None,
            aggregates: None,
            output_names: None,
            limit: None,
            offset: 0,
            order_by_keys: None,
            order_by_ascending: None,
            order_by_nulls: None,
            distinct: false,
            estimated_rows: None,
            column_ndv: None,
        }
    }

    /// Output column names: the aggregate output names when present, else the
    /// read column list.
    fn schema(&self) -> Vec<String> {
        match &self.output_names {
            Some(names) => names.clone(),
            None => self.columns.clone(),
        }
    }
}

/// Projection (SELECT list) over an input.
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    pub input: Box<LogicalPlan>,
    pub expressions: Vec<Expr>,
    pub aliases: Vec<String>,
    pub distinct: bool,
    /// DISTINCT ON keys; None for a plain projection.
    pub distinct_on: Option<Vec<Expr>>,
}

impl Projection {
    /// Output names: the alias list, expanding any `*` from the input schema so
    /// the projection reports concrete names (joins above need them).
    fn schema(&self) -> Vec<String> {
        if !self.aliases.iter().any(|alias| alias == "*") {
            return self.aliases.clone();
        }
        let mut names = Vec::new();
        for alias in &self.aliases {
            if alias == "*" {
                names.extend(self.input.schema());
            } else {
                names.push(alias.clone());
            }
        }
        names
    }
}

/// Filter rows by a predicate. A Filter over an Aggregate is a HAVING clause.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub input: Box<LogicalPlan>,
    pub predicate: Expr,
}

/// Join two inputs.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
    /// None for cross/NATURAL/USING joins.
    pub condition: Option<Expr>,
    pub natural: bool,
    pub using: Option<Vec<String>>,
    /// Estimated output rows, stamped by cost-based join ordering.
    pub estimated_rows: Option<u64>,
    /// Provenance of every defaulted statistic behind the estimate.
    pub estimate_defaults: Option<Vec<String>>,
}

impl Join {
    /// Output names: left columns only for SEMI/ANTI (existential filters), else
    /// left followed by right.
    fn schema(&self) -> Vec<String> {
        let mut names = self.left.schema();
        if matches!(self.join_type, JoinType::Semi | JoinType::Anti) {
            return names;
        }
        names.extend(self.right.schema());
        names
    }
}

/// Aggregate with grouping.
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<Expr>,
    pub aggregates: Vec<Expr>,
    pub output_names: Vec<String>,
    /// Expanded ROLLUP/CUBE/GROUPING SETS; None for a single-level GROUP BY.
    pub grouping_sets: Option<Vec<Vec<Expr>>>,
}

/// Sort rows by keys.
#[derive(Debug, Clone, PartialEq)]
pub struct Sort {
    pub input: Box<LogicalPlan>,
    pub sort_keys: Vec<Expr>,
    pub ascending: Vec<bool>,
    pub nulls_order: Option<Vec<Option<NullsOrder>>>,
}

/// Limit and/or offset. `limit` is None for an OFFSET without a row cap.
#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
    pub input: Box<LogicalPlan>,
    pub limit: Option<u64>,
    pub offset: u64,
}

/// N-ary union of inputs sharing a schema. `distinct` is UNION vs UNION ALL.
#[derive(Debug, Clone, PartialEq)]
pub struct Union {
    pub inputs: Vec<LogicalPlan>,
    pub distinct: bool,
}

impl Union {
    /// Output names: the first input's schema (all inputs share it), or empty.
    fn schema(&self) -> Vec<String> {
        self.inputs
            .first()
            .map(LogicalPlan::schema)
            .unwrap_or_default()
    }
}

/// Binary SQL set operation (UNION / INTERSECT / EXCEPT). `distinct` is the bare
/// form vs the ALL form.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub kind: SetOpKind,
    pub distinct: bool,
}

/// EXPLAIN wrapper around a plan.
#[derive(Debug, Clone, PartialEq)]
pub struct Explain {
    pub input: Box<LogicalPlan>,
    pub format: ExplainFormat,
}

/// Common table expression: a named subplan plus a child that can reference it.
#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: String,
    pub cte_plan: Box<LogicalPlan>,
    pub child: Box<LogicalPlan>,
    pub recursive: bool,
    pub column_names: Option<Vec<String>>,
}

/// A reference to a CTE by name in a FROM/JOIN position.
#[derive(Debug, Clone, PartialEq)]
pub struct CteRef {
    pub name: String,
    pub alias: Option<String>,
    pub columns: Option<Vec<String>>,
    pub output_names: Option<Vec<String>>,
}

impl CteRef {
    /// Output names: the filled output schema, else the referenced columns, else
    /// empty.
    fn schema(&self) -> Vec<String> {
        self.output_names
            .clone()
            .or_else(|| self.columns.clone())
            .unwrap_or_default()
    }
}

/// In-memory constant rows (a FROM-less SELECT).
#[derive(Debug, Clone, PartialEq)]
pub struct Values {
    pub rows: Vec<Vec<Expr>>,
    pub output_names: Vec<String>,
}

/// A derived table: a subplan exposed under an alias.
#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryScan {
    pub input: Box<LogicalPlan>,
    pub alias: String,
    /// `AS alias(col, ...)` positional rename list; None keeps the subplan names.
    pub column_names: Option<Vec<String>>,
}

/// Runtime cardinality guard for decorrelated scalar subqueries: with no keys,
/// at most one row total; with keys, at most one row per key tuple.
#[derive(Debug, Clone, PartialEq)]
pub struct SingleRowGuard {
    pub input: Box<LogicalPlan>,
    pub keys: Vec<Expr>,
}

/// Per-key LIMIT produced by decorrelating a correlated LIMIT.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupedLimit {
    pub input: Box<LogicalPlan>,
    pub keys: Vec<Expr>,
    pub limit: u64,
    pub order_by_keys: Option<Vec<Expr>>,
    pub order_by_ascending: Option<Vec<bool>>,
    pub order_by_nulls: Option<Vec<Option<NullsOrder>>>,
}

/// A dependent (lateral) join: the right side may reference left columns. The
/// decorrelation fallback for a correlation that cannot be flattened.
#[derive(Debug, Clone, PartialEq)]
pub struct LateralJoin {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
}

/// A logical plan node. One enum over the node structs above.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    // Scan is boxed: it carries ~20 fields (all the folded clauses), so an inline
    // variant would pad every other variant to its size.
    Scan(Box<Scan>),
    Projection(Projection),
    Filter(Filter),
    Join(Join),
    Aggregate(Aggregate),
    Sort(Sort),
    Limit(Limit),
    Union(Union),
    SetOperation(SetOperation),
    Explain(Explain),
    Cte(Cte),
    CteRef(CteRef),
    Values(Values),
    SubqueryScan(SubqueryScan),
    SingleRowGuard(SingleRowGuard),
    GroupedLimit(GroupedLimit),
    LateralJoin(LateralJoin),
}

impl LogicalPlan {
    /// Direct child plan nodes. Exhaustive: a new node forces a new arm.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan(_) | LogicalPlan::CteRef(_) | LogicalPlan::Values(_) => Vec::new(),
            LogicalPlan::Projection(node) => vec![&node.input],
            LogicalPlan::Filter(node) => vec![&node.input],
            LogicalPlan::Aggregate(node) => vec![&node.input],
            LogicalPlan::Sort(node) => vec![&node.input],
            LogicalPlan::Limit(node) => vec![&node.input],
            LogicalPlan::Explain(node) => vec![&node.input],
            LogicalPlan::SubqueryScan(node) => vec![&node.input],
            LogicalPlan::SingleRowGuard(node) => vec![&node.input],
            LogicalPlan::GroupedLimit(node) => vec![&node.input],
            LogicalPlan::Join(node) => vec![&node.left, &node.right],
            LogicalPlan::SetOperation(node) => vec![&node.left, &node.right],
            LogicalPlan::LateralJoin(node) => vec![&node.left, &node.right],
            LogicalPlan::Cte(node) => vec![&node.cte_plan, &node.child],
            LogicalPlan::Union(node) => node.inputs.iter().collect(),
        }
    }

    /// Output column names of this node. Exhaustive: a new node forces a new arm.
    pub fn schema(&self) -> Vec<String> {
        match self {
            LogicalPlan::Scan(node) => node.schema(),
            LogicalPlan::Projection(node) => node.schema(),
            LogicalPlan::Filter(node) => node.input.schema(),
            LogicalPlan::Join(node) => node.schema(),
            LogicalPlan::Aggregate(node) => node.output_names.clone(),
            LogicalPlan::Sort(node) => node.input.schema(),
            LogicalPlan::Limit(node) => node.input.schema(),
            LogicalPlan::Union(node) => node.schema(),
            LogicalPlan::SetOperation(node) => node.left.schema(),
            LogicalPlan::Explain(_) => vec!["plan".to_string()],
            LogicalPlan::Cte(node) => node.child.schema(),
            LogicalPlan::CteRef(node) => node.schema(),
            LogicalPlan::Values(node) => node.output_names.clone(),
            LogicalPlan::SubqueryScan(node) => node.input.schema(),
            LogicalPlan::SingleRowGuard(node) => node.input.schema(),
            LogicalPlan::GroupedLimit(node) => node.input.schema(),
            LogicalPlan::LateralJoin(node) => {
                let mut names = node.left.schema();
                names.extend(node.right.schema());
                names
            }
        }
    }

    /// Rebuild this node with `f` applied to each direct child plan, propagating
    /// any error `f` returns. Leaves (`Scan`/`CteRef`/`Values`) pass through
    /// unchanged. Exhaustive: a new node forces a new arm. Ports Python
    /// `transform_children`/`with_children` (the recurse-and-rebuild the
    /// decorrelator and pushdown rules share), made fallible so a rewrite can
    /// fail loudly mid-tree rather than swallow the error.
    pub fn try_map_children<E>(
        self,
        mut f: impl FnMut(LogicalPlan) -> Result<LogicalPlan, E>,
    ) -> Result<LogicalPlan, E> {
        Ok(match self {
            LogicalPlan::Scan(_) | LogicalPlan::CteRef(_) | LogicalPlan::Values(_) => self,
            LogicalPlan::Projection(mut node) => {
                node.input = Box::new(f(*node.input)?);
                LogicalPlan::Projection(node)
            }
            LogicalPlan::Filter(mut node) => {
                node.input = Box::new(f(*node.input)?);
                LogicalPlan::Filter(node)
            }
            LogicalPlan::Aggregate(mut node) => {
                node.input = Box::new(f(*node.input)?);
                LogicalPlan::Aggregate(node)
            }
            LogicalPlan::Sort(mut node) => {
                node.input = Box::new(f(*node.input)?);
                LogicalPlan::Sort(node)
            }
            LogicalPlan::Limit(mut node) => {
                node.input = Box::new(f(*node.input)?);
                LogicalPlan::Limit(node)
            }
            LogicalPlan::Explain(mut node) => {
                node.input = Box::new(f(*node.input)?);
                LogicalPlan::Explain(node)
            }
            LogicalPlan::SubqueryScan(mut node) => {
                node.input = Box::new(f(*node.input)?);
                LogicalPlan::SubqueryScan(node)
            }
            LogicalPlan::SingleRowGuard(mut node) => {
                node.input = Box::new(f(*node.input)?);
                LogicalPlan::SingleRowGuard(node)
            }
            LogicalPlan::GroupedLimit(mut node) => {
                node.input = Box::new(f(*node.input)?);
                LogicalPlan::GroupedLimit(node)
            }
            LogicalPlan::Join(mut node) => {
                node.left = Box::new(f(*node.left)?);
                node.right = Box::new(f(*node.right)?);
                LogicalPlan::Join(node)
            }
            LogicalPlan::SetOperation(mut node) => {
                node.left = Box::new(f(*node.left)?);
                node.right = Box::new(f(*node.right)?);
                LogicalPlan::SetOperation(node)
            }
            LogicalPlan::LateralJoin(mut node) => {
                node.left = Box::new(f(*node.left)?);
                node.right = Box::new(f(*node.right)?);
                LogicalPlan::LateralJoin(node)
            }
            LogicalPlan::Cte(mut node) => {
                node.cte_plan = Box::new(f(*node.cte_plan)?);
                node.child = Box::new(f(*node.child)?);
                LogicalPlan::Cte(node)
            }
            LogicalPlan::Union(mut node) => {
                let mut inputs = Vec::with_capacity(node.inputs.len());
                for input in node.inputs {
                    inputs.push(f(input)?);
                }
                node.inputs = inputs;
                LogicalPlan::Union(node)
            }
        })
    }

    /// Every expression attached DIRECTLY to this node, in a stable order.
    /// Exhaustive: a new node forces a new arm, so a plan walk can never silently
    /// miss a node's expressions. Does NOT descend into child plans - it reports
    /// only this node's own expressions (predicates, projections, group/agg/sort
    /// keys, ...). The decorrelation post-pass invariant walks this to prove no
    /// subquery expression survived. Ports Python `direct_expressions`.
    pub fn direct_expressions(&self) -> Vec<&Expr> {
        match self {
            LogicalPlan::Scan(node) => {
                let mut exprs = Vec::new();
                if let Some(filters) = &node.filters {
                    exprs.push(filters);
                }
                extend_opt_vec(&mut exprs, node.group_by.as_deref());
                extend_opt_grouping_sets(&mut exprs, node.grouping_sets.as_deref());
                extend_opt_vec(&mut exprs, node.aggregates.as_deref());
                extend_opt_vec(&mut exprs, node.order_by_keys.as_deref());
                exprs
            }
            LogicalPlan::Projection(node) => {
                let mut exprs: Vec<&Expr> = node.expressions.iter().collect();
                extend_opt_vec(&mut exprs, node.distinct_on.as_deref());
                exprs
            }
            LogicalPlan::Filter(node) => vec![&node.predicate],
            LogicalPlan::Join(node) => node.condition.iter().collect(),
            LogicalPlan::Aggregate(node) => {
                let mut exprs: Vec<&Expr> = node.group_by.iter().collect();
                exprs.extend(node.aggregates.iter());
                extend_opt_grouping_sets(&mut exprs, node.grouping_sets.as_deref());
                exprs
            }
            LogicalPlan::Sort(node) => node.sort_keys.iter().collect(),
            LogicalPlan::Values(node) => node.rows.iter().flatten().collect(),
            LogicalPlan::SingleRowGuard(node) => node.keys.iter().collect(),
            LogicalPlan::GroupedLimit(node) => {
                let mut exprs: Vec<&Expr> = node.keys.iter().collect();
                extend_opt_vec(&mut exprs, node.order_by_keys.as_deref());
                exprs
            }
            LogicalPlan::Limit(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::SetOperation(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Cte(_)
            | LogicalPlan::CteRef(_)
            | LogicalPlan::SubqueryScan(_)
            | LogicalPlan::LateralJoin(_) => Vec::new(),
        }
    }
}

/// Append every expression of an optional expression vector to `exprs`.
fn extend_opt_vec<'a>(exprs: &mut Vec<&'a Expr>, source: Option<&'a [Expr]>) {
    if let Some(items) = source {
        exprs.extend(items.iter());
    }
}

/// Append every expression of optional grouping sets (a vec of vecs) to `exprs`.
fn extend_opt_grouping_sets<'a>(exprs: &mut Vec<&'a Expr>, source: Option<&'a [Vec<Expr>]>) {
    if let Some(sets) = source {
        for set in sets {
            exprs.extend(set.iter());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{ColumnRef, Expr};
    use fq_common::DataType;

    fn scan(table: &str, columns: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(Box::new(Scan::new(
            "ds",
            "public",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        )))
    }

    #[test]
    fn scan_schema_uses_columns_then_output_names() {
        let LogicalPlan::Scan(mut node) = scan("t", &["a", "b"]) else {
            unreachable!()
        };
        assert_eq!(node.schema(), vec!["a", "b"]);
        node.output_names = Some(vec!["x".to_string()]);
        assert_eq!(node.schema(), vec!["x"]);
    }

    #[test]
    fn projection_star_expands_from_input() {
        let projection = LogicalPlan::Projection(Projection {
            input: Box::new(scan("t", &["a", "b"])),
            expressions: vec![],
            aliases: vec!["*".to_string()],
            distinct: false,
            distinct_on: None,
        });
        assert_eq!(projection.schema(), vec!["a", "b"]);
    }

    #[test]
    fn semi_join_schema_is_left_only() {
        let key = Expr::Column(ColumnRef::new(
            Some("l".to_string()),
            "id",
            Some(DataType::Integer),
        ));
        let join = LogicalPlan::Join(Join {
            left: Box::new(scan("l", &["id", "name"])),
            right: Box::new(scan("r", &["id", "total"])),
            join_type: JoinType::Semi,
            condition: Some(key),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        assert_eq!(join.schema(), vec!["id", "name"]);
    }

    #[test]
    fn inner_join_schema_is_left_then_right() {
        let join = LogicalPlan::Join(Join {
            left: Box::new(scan("l", &["id"])),
            right: Box::new(scan("r", &["total"])),
            join_type: JoinType::Inner,
            condition: None,
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        assert_eq!(join.schema(), vec!["id", "total"]);
    }

    #[test]
    fn children_arity_per_node() {
        assert_eq!(scan("t", &["a"]).children().len(), 0);
        let filter = LogicalPlan::Filter(Filter {
            input: Box::new(scan("t", &["a"])),
            predicate: Expr::Literal {
                value: crate::expr::LiteralValue::Boolean(true),
                data_type: DataType::Boolean,
            },
        });
        assert_eq!(filter.children().len(), 1);
    }

    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    #[test]
    fn try_map_children_rebuilds_both_join_sides() {
        // A Join over two scans: swap every child scan for a fresh single-column
        // scan; both sides must be rebuilt (proves left AND right are visited).
        let join = LogicalPlan::Join(Join {
            left: Box::new(scan("l", &["id", "name"])),
            right: Box::new(scan("r", &["id", "total"])),
            join_type: JoinType::Inner,
            condition: None,
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        let rebuilt = join
            .try_map_children(|_| -> Result<LogicalPlan, ()> { Ok(scan("x", &["marker"])) })
            .unwrap();
        assert_eq!(rebuilt.schema(), vec!["marker", "marker"]);
    }

    #[test]
    fn try_map_children_leaf_passes_through_untouched() {
        let leaf = scan("t", &["a"]);
        let mapped = leaf
            .clone()
            .try_map_children(|_| -> Result<LogicalPlan, ()> { panic!("leaf has no children") })
            .unwrap();
        assert_eq!(mapped, leaf);
    }

    #[test]
    fn try_map_children_propagates_error() {
        let filter = LogicalPlan::Filter(Filter {
            input: Box::new(scan("t", &["a"])),
            predicate: col("t", "a"),
        });
        let result: Result<LogicalPlan, &str> =
            filter.try_map_children(|_| Err("child rewrite failed"));
        assert_eq!(result, Err("child rewrite failed"));
    }

    #[test]
    fn direct_expressions_covers_filter_and_aggregate() {
        let predicate = col("t", "a");
        let filter = LogicalPlan::Filter(Filter {
            input: Box::new(scan("t", &["a"])),
            predicate: predicate.clone(),
        });
        assert_eq!(filter.direct_expressions(), vec![&predicate]);

        let group = col("t", "region");
        let aggregate = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(scan("t", &["region", "amount"])),
            group_by: vec![group.clone()],
            aggregates: vec![col("t", "amount")],
            output_names: vec!["region".to_string(), "amount".to_string()],
            grouping_sets: None,
        });
        // group_by then aggregates, no descent into the input scan.
        assert_eq!(aggregate.direct_expressions().len(), 2);
        assert_eq!(aggregate.direct_expressions()[0], &group);
    }

    #[test]
    fn direct_expressions_empty_for_bare_nodes() {
        // A Limit carries no expressions of its own.
        let limit = LogicalPlan::Limit(Limit {
            input: Box::new(scan("t", &["a"])),
            limit: Some(10),
            offset: 0,
        });
        assert!(limit.direct_expressions().is_empty());
    }
}
