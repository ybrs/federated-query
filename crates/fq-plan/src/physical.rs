//! Physical plan nodes. Ports the NODE STRUCTS of `plan/physical.py`.
//!
//! `PhysicalPlan` is ONE enum wrapping a struct per node type; `children` is an
//! exhaustive match. These are a pure plan representation - never executed here;
//! the Rust engine (fq-exec) is the one execution path.
//!
//! SCOPE: the structural node data + `children`, plus the typed `schema()`
//! (name+DataType pairs, no Arrow) added with its first consumer, dim shipping
//! (fq-physical). The Arrow-typed schema and the `column_aliases()` resolution map
//! were NOT needed: a bound `ColumnRef` carries its `DataType`, so `Expr::get_type`
//! types expressions without an input schema or kernels. Nodes whose output types
//! only a live source/engine can supply (non-seeded Scan/RemoteQuery, RemoteJoin,
//! RemoteSetOp, CteMergeQuery, LateralJoin, Gather) panic loudly in `schema()`
//! rather than guess - dim shipping's fallback root never reaches them.
//!
//! PORT NOTES (never silent - clean Rust over Python parity, per project
//! guidance):
//! - `datasource_connection: Any` on scan/remote/gather nodes is DROPPED: a live
//!   connection is runtime state, not plan data. Each node names its source by
//!   string (`datasource`); fq-exec resolves the connection by name.
//! - The private derivation caches (`_schema`, `_column_alias_map`,
//!   `_derived_cache` + the `__init_subclass__` wrapping) are DROPPED;
//!   memoization (e.g. `OnceCell`) is added by the consumer if profiling wants it.
//! - Pre-rendered source SQL (`query_ast`, `lateral_sql`, the CTE-merge `sql`,
//!   `Gather.query`) is held as a `String`: the node carries the SQL to run on
//!   the source. fq-emit produces it; fq-emit may later reshape this to render
//!   on demand from the subtree instead.
//! - `seeded_schema: pa.Schema` becomes `Option<SeededSchema>` (name+DataType
//!   pairs) - no Arrow dependency in fq-plan yet; fq-exec converts to Arrow.
//! - `group_observation` (learned-catalog provenance stamp) and a scan's
//!   `dynamic_filter_values` (EXPLAIN-sampled key values) are DEFERRED to their
//!   producers in fq-physical / fq-exec / the EXPLAIN path.

use std::collections::BTreeMap;

use fq_common::DataType;

use crate::expr::{contains_window, BinaryOpType, Expr, NullsOrder};
use crate::logical::{ExplainFormat, JoinType, SetOpKind};

/// A `(table, column) -> physical output column name` map. `table` is None for
/// an unqualified reference. The resolution key the IR serializer uses.
pub type ColumnAliasMap = BTreeMap<(Option<String>, String), String>;

/// A seeded output schema: ordered (column name, engine type) pairs. Replaces the
/// Arrow `pa.Schema` seeded onto shipped islands; fq-exec converts to Arrow.
pub type SeededSchema = Vec<(String, DataType)>;

/// A learned group-count provenance stamp. Ports the Python `group_observation`
/// dict carried on aggregate producers: `subject` is the learned-stats subject
/// key (a table name or a subplan signature) and `columns` the grouping columns,
/// so a later run can read back the measured group count for this exact grouping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupObservation {
    pub subject: String,
    pub columns: Vec<String>,
}

/// Which side of a hash join is the build side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildSide {
    Left,
    Right,
}

/// Materializes a cross-source CTE body once, shared by every reference.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalCte {
    pub name: String,
    pub body: Box<PhysicalPlan>,
    pub column_names: Option<Vec<String>>,
}

/// Reads a materialized CTE's rows; one per reference, shares the producer.
/// `producer` is a `PhysicalPlan::Cte` (uniform traversal over the enum).
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalCteScan {
    pub producer: Box<PhysicalPlan>,
    pub alias: Option<String>,
}

/// Ships a foreign relation INTO a target source as a temp table, then runs the
/// child island there (dim shipping). Only the island output crosses the boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalShipment {
    pub table: String,
    pub datasource: String,
    pub body: Box<PhysicalPlan>,
    pub child: Box<PhysicalPlan>,
}

/// Re-exposes a derived relation's columns under its subquery alias.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalAliasedRelation {
    pub input: Box<PhysicalPlan>,
    pub alias: String,
}

/// Runs a whole (possibly recursive) WITH inside the merge engine. `inputs` are
/// the base relations materialized and registered under generated names that the
/// rendered `sql` references.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalCteMergeQuery {
    pub sql: String,
    pub inputs: BTreeMap<String, PhysicalPlan>,
    pub output_names: Vec<String>,
}

/// Reads one table from one source, with any pushed clauses folded in. When it is
/// the probe of a semi-join reduction, `dynamic_filter_keys` carries the injected
/// join keys.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalScan {
    pub datasource: String,
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub filters: Option<Expr>,
    pub sample: Option<String>,
    pub group_by: Option<Vec<Expr>>,
    pub grouping_sets: Option<Vec<Vec<Expr>>>,
    pub aggregates: Option<Vec<Expr>>,
    pub output_names: Option<Vec<String>>,
    pub alias: Option<String>,
    pub limit: Option<u64>,
    pub offset: u64,
    pub order_by_keys: Option<Vec<Expr>>,
    pub order_by_ascending: Option<Vec<bool>>,
    pub order_by_nulls: Option<Vec<Option<NullsOrder>>>,
    pub distinct: bool,
    /// Semi-join reduction: keys ANDed into the scan as an IN-list at execution.
    pub dynamic_filter_keys: Option<Vec<Expr>>,
    pub estimated_rows: Option<u64>,
    pub column_ndv: Option<BTreeMap<String, i64>>,
    pub seeded_schema: Option<SeededSchema>,
}

/// Projection (SELECT list) over an input.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalProjection {
    pub input: Box<PhysicalPlan>,
    pub expressions: Vec<Expr>,
    pub output_names: Vec<String>,
    pub distinct: bool,
    pub distinct_on: Option<Vec<Expr>>,
}

/// A projection carrying window functions, emitted as a SQL fragment for the
/// merge engine.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalWindow {
    pub input: Box<PhysicalPlan>,
    pub expressions: Vec<Expr>,
    pub output_names: Vec<String>,
}

/// Filter rows by a predicate.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalFilter {
    pub input: Box<PhysicalPlan>,
    pub predicate: Expr,
}

/// A cross-source hash join with explicit build side and equi-key sides.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalHashJoin {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub join_type: JoinType,
    pub left_keys: Vec<Expr>,
    pub right_keys: Vec<Expr>,
    pub build_side: BuildSide,
    pub estimated_rows: Option<u64>,
    pub estimate_defaults: Option<Vec<String>>,
}

/// A join kept entirely on one source. `left`/`right` are `PhysicalPlan::Scan`s.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalRemoteJoin {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub join_type: JoinType,
    pub condition: Expr,
    pub group_by: Option<Vec<Expr>>,
    pub grouping_sets: Option<Vec<Vec<Expr>>>,
    pub aggregates: Option<Vec<Expr>>,
    pub output_names: Option<Vec<String>>,
    pub distinct: bool,
    pub order_by_keys: Option<Vec<Expr>>,
    pub order_by_ascending: Option<Vec<bool>>,
    pub order_by_nulls: Option<Vec<Option<NullsOrder>>>,
}

/// A cross-source nested-loop join (the fallback when no equi-key exists).
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalNestedLoopJoin {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub join_type: JoinType,
    pub condition: Option<Expr>,
    pub estimated_rows: Option<u64>,
    pub estimate_defaults: Option<Vec<String>>,
}

/// A cross-source hash aggregate.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalHashAggregate {
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<Expr>,
    pub aggregates: Vec<Expr>,
    pub output_names: Vec<String>,
    pub grouping_sets: Option<Vec<Vec<Expr>>>,
    /// Learned group-count provenance for the coordinator aggregate, stamped by
    /// the physical planner; None until then.
    pub group_observation: Option<GroupObservation>,
}

impl PhysicalHashAggregate {
    /// Whether an output aggregate expression carries a window function. Such an
    /// aggregate (e.g. `sum(sum(x)) OVER (...) ... GROUP BY ...`) is valid SQL but
    /// the structured aggregate fragment cannot express it, so it is emitted
    /// through the raw aggregate-SQL path instead. Ports `has_window_output`.
    pub fn has_window_output(&self) -> bool {
        self.aggregates.iter().any(contains_window)
    }
}

/// A cross-source sort.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalSort {
    pub input: Box<PhysicalPlan>,
    pub sort_keys: Vec<Expr>,
    pub ascending: Vec<bool>,
    pub nulls_order: Option<Vec<Option<NullsOrder>>>,
}

/// A cross-source limit/offset.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalLimit {
    pub input: Box<PhysicalPlan>,
    pub limit: Option<u64>,
    pub offset: u64,
}

/// In-memory constant rows.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalValues {
    pub rows: Vec<Vec<Expr>>,
    pub output_names: Vec<String>,
}

/// An entire same-source subtree rendered as one query, executed in one round
/// trip. `sql` is the rendered source SQL; `column_alias_map` resolves references
/// to physical output names.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalRemoteQuery {
    pub datasource: String,
    pub sql: String,
    pub output_names: Vec<String>,
    pub column_alias_map: ColumnAliasMap,
    pub estimated_rows: Option<u64>,
    pub output_estimated_rows: Option<u64>,
    pub column_ndv: Option<BTreeMap<String, i64>>,
    pub seeded_schema: Option<SeededSchema>,
    /// Learned group-count provenance stamped when this remote query is a shipped
    /// dimension island whose aggregate collapses on the fact source; None
    /// otherwise.
    pub group_observation: Option<GroupObservation>,
}

/// An n-ary union of inputs sharing a schema.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalUnion {
    pub inputs: Vec<PhysicalPlan>,
    pub distinct: bool,
}

/// A set operation kept entirely on one source.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalRemoteSetOp {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub kind: SetOpKind,
    pub distinct: bool,
    pub datasource: String,
    pub order_by_keys: Option<Vec<Expr>>,
    pub order_by_ascending: Option<Vec<bool>>,
    pub order_by_nulls: Option<Vec<Option<NullsOrder>>>,
    pub limit: Option<u64>,
    pub offset: u64,
}

/// A cross-source binary set operation.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalSetOperation {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub kind: SetOpKind,
    pub distinct: bool,
}

/// Runtime at-most-one-row(-per-key) guard for decorrelated scalar subqueries.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalSingleRowGuard {
    pub input: Box<PhysicalPlan>,
    pub keys: Vec<Expr>,
}

/// Per-key LIMIT (decorrelated correlated LIMIT).
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalGroupedLimit {
    pub input: Box<PhysicalPlan>,
    pub keys: Vec<Expr>,
    pub limit: u64,
    pub order_by_keys: Option<Vec<Expr>>,
    pub order_by_ascending: Option<Vec<bool>>,
    pub order_by_nulls: Option<Vec<Option<NullsOrder>>>,
}

/// A dependent (lateral) join whose right side (`lateral_sql`) is evaluated per
/// left row. `correlations` pairs each inner column with its comparison operator
/// and the outer column it correlates to, so the base can be reduced to the
/// left's correlation domain (a dynamic filter) before transfer.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalLateralJoin {
    pub left: Box<PhysicalPlan>,
    pub left_name: String,
    pub left_alias: String,
    pub base_scan: Box<PhysicalPlan>,
    pub base_name: String,
    pub lateral_sql: String,
    pub output_names: Vec<String>,
    pub join_type: JoinType,
    pub correlations: Vec<(String, BinaryOpType, String)>,
}

/// Builds the EXPLAIN document without executing.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalExplain {
    pub child: Box<PhysicalPlan>,
    pub format: ExplainFormat,
}

/// A raw SQL read from one source (an unelaborated remote read).
#[derive(Debug, Clone, PartialEq)]
pub struct Gather {
    pub datasource: String,
    pub query: String,
}

/// A physical plan node. One enum over the node structs above.
///
/// The large-payload nodes (`Scan`, `RemoteQuery`, `RemoteJoin`, `RemoteSetOp`,
/// `LateralJoin`) are boxed so an inline variant does not pad every other variant
/// to its size.
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    Cte(PhysicalCte),
    CteScan(PhysicalCteScan),
    Shipment(PhysicalShipment),
    AliasedRelation(PhysicalAliasedRelation),
    CteMergeQuery(PhysicalCteMergeQuery),
    Scan(Box<PhysicalScan>),
    Projection(PhysicalProjection),
    Window(PhysicalWindow),
    Filter(PhysicalFilter),
    HashJoin(PhysicalHashJoin),
    RemoteJoin(Box<PhysicalRemoteJoin>),
    NestedLoopJoin(PhysicalNestedLoopJoin),
    HashAggregate(PhysicalHashAggregate),
    Sort(PhysicalSort),
    Limit(PhysicalLimit),
    Values(PhysicalValues),
    RemoteQuery(Box<PhysicalRemoteQuery>),
    Union(PhysicalUnion),
    RemoteSetOp(Box<PhysicalRemoteSetOp>),
    SetOperation(PhysicalSetOperation),
    SingleRowGuard(PhysicalSingleRowGuard),
    GroupedLimit(PhysicalGroupedLimit),
    LateralJoin(Box<PhysicalLateralJoin>),
    Explain(PhysicalExplain),
    Gather(Gather),
}

impl PhysicalPlan {
    /// Direct child plan nodes. Exhaustive: a new node forces a new arm.
    pub fn children(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::Scan(_)
            | PhysicalPlan::Values(_)
            | PhysicalPlan::RemoteQuery(_)
            | PhysicalPlan::Gather(_) => Vec::new(),
            PhysicalPlan::Cte(node) => vec![&node.body],
            PhysicalPlan::CteScan(node) => vec![&node.producer],
            PhysicalPlan::Shipment(node) => vec![&node.body, &node.child],
            PhysicalPlan::AliasedRelation(node) => vec![&node.input],
            PhysicalPlan::CteMergeQuery(node) => node.inputs.values().collect(),
            PhysicalPlan::Projection(node) => vec![&node.input],
            PhysicalPlan::Window(node) => vec![&node.input],
            PhysicalPlan::Filter(node) => vec![&node.input],
            PhysicalPlan::HashJoin(node) => vec![&node.left, &node.right],
            PhysicalPlan::RemoteJoin(node) => vec![&node.left, &node.right],
            PhysicalPlan::NestedLoopJoin(node) => vec![&node.left, &node.right],
            PhysicalPlan::HashAggregate(node) => vec![&node.input],
            PhysicalPlan::Sort(node) => vec![&node.input],
            PhysicalPlan::Limit(node) => vec![&node.input],
            PhysicalPlan::Union(node) => node.inputs.iter().collect(),
            PhysicalPlan::RemoteSetOp(node) => vec![&node.left, &node.right],
            PhysicalPlan::SetOperation(node) => vec![&node.left, &node.right],
            PhysicalPlan::SingleRowGuard(node) => vec![&node.input],
            PhysicalPlan::GroupedLimit(node) => vec![&node.input],
            PhysicalPlan::LateralJoin(node) => vec![&node.left, &node.base_scan],
            PhysicalPlan::Explain(node) => vec![&node.child],
        }
    }
}

impl PhysicalPlan {
    /// The output schema as ordered `(column name, DataType)` pairs. Ports the
    /// per-node `schema()` of `plan/physical.py`, but types every expression
    /// through `Expr::get_type` (a bound column already carries its type) rather
    /// than evaluating Arrow kernels on an empty batch - so fq-plan needs no Arrow
    /// crate and no live source connection. fq-exec converts these pairs to Arrow.
    ///
    /// # Panics
    /// A node whose output types cannot be derived without executing or probing a
    /// source (a non-seeded remote read/scan, a merge-engine `WITH`, a cross-source
    /// LATERAL) panics loudly rather than inventing a schema - a crash never ships
    /// a lie. Every node dim shipping's fallback reaches (an aggregate/projection
    /// under row-preserving wrappers, a join, a seeded read, values) IS derivable.
    /// Panics too on an unbound column/cast (`get_type`'s binding contract).
    pub fn schema(&self) -> Vec<(String, DataType)> {
        match self {
            PhysicalPlan::Cte(node) => cte_schema(node),
            PhysicalPlan::CteScan(node) => node.producer.schema(),
            PhysicalPlan::Shipment(node) => node.child.schema(),
            PhysicalPlan::AliasedRelation(node) => node.input.schema(),
            PhysicalPlan::Scan(node) => seeded_schema(node.seeded_schema.as_ref(), "PhysicalScan"),
            PhysicalPlan::Projection(node) => projection_schema(node),
            PhysicalPlan::Window(node) => typed_outputs(&node.expressions, &node.output_names),
            PhysicalPlan::Filter(node) => node.input.schema(),
            PhysicalPlan::HashJoin(node) => join_schema(node.join_type, &node.left, &node.right),
            PhysicalPlan::NestedLoopJoin(node) => {
                join_schema(node.join_type, &node.left, &node.right)
            }
            PhysicalPlan::HashAggregate(node) => {
                typed_outputs(&node.aggregates, &node.output_names)
            }
            PhysicalPlan::Sort(node) => node.input.schema(),
            PhysicalPlan::Limit(node) => node.input.schema(),
            PhysicalPlan::Values(node) => values_schema(node),
            PhysicalPlan::RemoteQuery(node) => {
                seeded_schema(node.seeded_schema.as_ref(), "PhysicalRemoteQuery")
            }
            PhysicalPlan::Union(node) => first_input_schema(&node.inputs),
            PhysicalPlan::SetOperation(node) => node.left.schema(),
            PhysicalPlan::SingleRowGuard(node) => node.input.schema(),
            PhysicalPlan::GroupedLimit(node) => node.input.schema(),
            PhysicalPlan::Explain(_) => vec![("plan".to_string(), DataType::Varchar)],
            // Underivable without executing/probing the source: a remote join/set-op
            // whose column types only the source knows, the merge-engine WITH the
            // engine types natively, a cross-source LATERAL with no native connector,
            // and a bare Gather. Python probes; fq-plan cannot, so it panics loudly.
            PhysicalPlan::RemoteJoin(_) => source_typed("PhysicalRemoteJoin"),
            PhysicalPlan::RemoteSetOp(_) => source_typed("PhysicalRemoteSetOp"),
            PhysicalPlan::CteMergeQuery(_) => source_typed("PhysicalCteMergeQuery"),
            PhysicalPlan::LateralJoin(_) => source_typed("PhysicalLateralJoin"),
            PhysicalPlan::Gather(_) => source_typed("Gather"),
        }
    }
}

/// Pair each output expression with its name, typing it through `Expr::get_type`.
/// The two lists are parallel (one name per output expression); a length mismatch
/// is a planner bug, surfaced loudly rather than silently truncated by a zip.
fn typed_outputs(exprs: &[Expr], names: &[String]) -> Vec<(String, DataType)> {
    assert_eq!(
        exprs.len(),
        names.len(),
        "physical schema: output expression/name arity mismatch"
    );
    let mut fields = Vec::with_capacity(names.len());
    for (name, expr) in names.iter().zip(exprs) {
        fields.push((name.clone(), expr.get_type()));
    }
    fields
}

/// A projection's output schema: each expression typed by `get_type`, except a
/// `*` column reference, which splices in every input column (name + type) in
/// order - the one place the input schema is consulted.
fn projection_schema(node: &PhysicalProjection) -> Vec<(String, DataType)> {
    let mut fields = Vec::new();
    for (index, expr) in node.expressions.iter().enumerate() {
        if let Expr::Column(column) = expr {
            if column.column == "*" {
                fields.extend(node.input.schema());
                continue;
            }
        }
        fields.push((node.output_names[index].clone(), expr.get_type()));
    }
    fields
}

/// A Values node's schema: type each output column from the first row's constant.
fn values_schema(node: &PhysicalValues) -> Vec<(String, DataType)> {
    let first_row = node
        .rows
        .first()
        .expect("PhysicalValues carries at least one row to type its columns");
    let mut fields = Vec::with_capacity(node.output_names.len());
    for (index, name) in node.output_names.iter().enumerate() {
        fields.push((name.clone(), first_row[index].get_type()));
    }
    fields
}

/// A CTE's schema: the body's, relabeled by `column_names` when a rename list is
/// set and differs from the body's own names (Python `PhysicalCTE.schema`).
fn cte_schema(node: &PhysicalCte) -> Vec<(String, DataType)> {
    let base = node.body.schema();
    let Some(names) = &node.column_names else {
        return base;
    };
    if base.iter().map(|(name, _)| name).eq(names.iter()) {
        return base;
    }
    let mut fields = Vec::with_capacity(names.len());
    for (index, name) in names.iter().enumerate() {
        fields.push((name.clone(), base[index].1));
    }
    fields
}

/// A binary join's output schema: left columns keep their names; a colliding right
/// column is renamed under the left-wins rule (`right_output_name`). A SEMI/ANTI
/// join outputs left columns only. Ports `_join_output_schema`.
fn join_schema(
    join_type: JoinType,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
) -> Vec<(String, DataType)> {
    let mut fields = left.schema();
    if matches!(join_type, JoinType::Semi | JoinType::Anti) {
        return fields;
    }
    let left_names: Vec<String> = fields.iter().map(|(name, _)| name.clone()).collect();
    for (name, data_type) in right.schema() {
        fields.push((right_output_name(&name, &left_names), data_type));
    }
    fields
}

/// The output name of a right-side column under the left-wins collision rule: a
/// name colliding with a left column is prefixed `right_`, suffixed `_1`, `_2` ...
/// until unique among the left names. Ports `_right_output_name`.
fn right_output_name(name: &str, left_names: &[String]) -> String {
    if !left_names.iter().any(|existing| existing == name) {
        return name.to_string();
    }
    let candidate = format!("right_{name}");
    let mut unique = candidate.clone();
    let mut suffix = 1;
    while left_names.iter().any(|existing| existing == &unique) {
        unique = format!("{candidate}_{suffix}");
        suffix += 1;
    }
    unique
}

/// The first input's schema of an n-ary union. A union always has at least one
/// branch; an empty one is a planner bug surfaced loudly.
fn first_input_schema(inputs: &[PhysicalPlan]) -> Vec<(String, DataType)> {
    inputs
        .first()
        .expect("PhysicalUnion has at least one input branch")
        .schema()
}

/// A seeded read's schema, or a loud panic when no schema is seeded: fq-plan has
/// no source connection to probe, so a real (non-seeded) remote read/scan cannot
/// be typed here - only a shipped-dimension read (seeded at construction) can.
fn seeded_schema(seeded: Option<&SeededSchema>, node: &str) -> Vec<(String, DataType)> {
    match seeded {
        Some(schema) => schema.clone(),
        None => panic!(
            "{node}::schema needs a live source connection or a seeded schema; \
             fq-plan cannot derive it (seed it via dim shipping, or type it in fq-exec)"
        ),
    }
}

/// Loud panic for a node whose output types only the execution engine or source
/// can supply. Ports the `raise NotImplementedError` sites of `plan/physical.py`.
fn source_typed(node: &str) -> ! {
    panic!("{node}::schema is computed by the execution engine, not derivable in fq-plan")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan() -> PhysicalPlan {
        PhysicalPlan::Scan(Box::new(PhysicalScan {
            datasource: "pg".to_string(),
            schema_name: "public".to_string(),
            table_name: "t".to_string(),
            columns: vec!["a".to_string()],
            filters: None,
            sample: None,
            group_by: None,
            grouping_sets: None,
            aggregates: None,
            output_names: None,
            alias: None,
            limit: None,
            offset: 0,
            order_by_keys: None,
            order_by_ascending: None,
            order_by_nulls: None,
            distinct: false,
            dynamic_filter_keys: None,
            estimated_rows: None,
            column_ndv: None,
            seeded_schema: None,
        }))
    }

    #[test]
    fn leaf_nodes_have_no_children() {
        assert_eq!(scan().children().len(), 0);
        assert_eq!(
            PhysicalPlan::Values(PhysicalValues {
                rows: vec![],
                output_names: vec!["x".to_string()],
            })
            .children()
            .len(),
            0
        );
    }

    #[test]
    fn unary_and_binary_children_counts() {
        let filter = PhysicalPlan::Filter(PhysicalFilter {
            input: Box::new(scan()),
            predicate: Expr::Literal {
                value: crate::expr::LiteralValue::Boolean(true),
                data_type: DataType::Boolean,
            },
        });
        assert_eq!(filter.children().len(), 1);

        let join = PhysicalPlan::HashJoin(PhysicalHashJoin {
            left: Box::new(scan()),
            right: Box::new(scan()),
            join_type: JoinType::Inner,
            left_keys: vec![],
            right_keys: vec![],
            build_side: BuildSide::Right,
            estimated_rows: None,
            estimate_defaults: None,
        });
        assert_eq!(join.children().len(), 2);
    }

    #[test]
    fn shipment_traverses_body_and_child() {
        let shipment = PhysicalPlan::Shipment(PhysicalShipment {
            table: "dim".to_string(),
            datasource: "pg".to_string(),
            body: Box::new(scan()),
            child: Box::new(scan()),
        });
        assert_eq!(shipment.children().len(), 2);
    }

    #[test]
    fn cte_merge_query_children_are_its_inputs() {
        let mut inputs = BTreeMap::new();
        inputs.insert("r0".to_string(), scan());
        inputs.insert("r1".to_string(), scan());
        let merge = PhysicalPlan::CteMergeQuery(PhysicalCteMergeQuery {
            sql: "WITH ...".to_string(),
            inputs,
            output_names: vec!["a".to_string()],
        });
        assert_eq!(merge.children().len(), 2);
    }

    // ---- schema() ----------------------------------------------------------

    use crate::expr::{ColumnRef, LiteralValue};

    /// A qualified column expression of the given type.
    fn column(table: &str, name: &str, data_type: DataType) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(data_type),
        ))
    }

    /// An integer literal expression.
    fn int_literal(value: i64) -> Expr {
        Expr::Literal {
            value: LiteralValue::Integer(value),
            data_type: DataType::Integer,
        }
    }

    /// A `count(*)` aggregate call (types as BIGINT via `get_type`).
    fn count_star() -> Expr {
        Expr::FunctionCall {
            function_name: "count".to_string(),
            args: vec![],
            is_aggregate: true,
            distinct: false,
            within_group_key: None,
            within_group_desc: false,
        }
    }

    /// A single-column Values relation exposing `name: Integer`.
    fn values(name: &str) -> PhysicalPlan {
        PhysicalPlan::Values(PhysicalValues {
            rows: vec![vec![int_literal(1)]],
            output_names: vec![name.to_string()],
        })
    }

    #[test]
    fn seeded_scan_returns_its_seeded_schema() {
        let seeded: SeededSchema = vec![("d_date".to_string(), DataType::Varchar)];
        let PhysicalPlan::Scan(mut base) = scan() else {
            unreachable!()
        };
        base.seeded_schema = Some(seeded.clone());
        assert_eq!(PhysicalPlan::Scan(base).schema(), seeded);
    }

    #[test]
    fn projection_types_each_output_via_get_type() {
        let projection = PhysicalPlan::Projection(PhysicalProjection {
            input: Box::new(scan()),
            expressions: vec![column("t", "region", DataType::Varchar), int_literal(1)],
            output_names: vec!["region".to_string(), "one".to_string()],
            distinct: false,
            distinct_on: None,
        });
        assert_eq!(
            projection.schema(),
            vec![
                ("region".to_string(), DataType::Varchar),
                ("one".to_string(), DataType::Integer),
            ]
        );
    }

    #[test]
    fn projection_star_column_splices_the_input_schema() {
        let star = PhysicalPlan::Projection(PhysicalProjection {
            input: Box::new(values("only")),
            expressions: vec![Expr::Column(ColumnRef::new(None, "*", None))],
            output_names: vec!["ignored".to_string()],
            distinct: false,
            distinct_on: None,
        });
        assert_eq!(star.schema(), vec![("only".to_string(), DataType::Integer)]);
    }

    #[test]
    fn aggregate_schema_is_its_output_list_typed() {
        let aggregate = PhysicalPlan::HashAggregate(PhysicalHashAggregate {
            input: Box::new(scan()),
            group_by: vec![column("t", "region", DataType::Varchar)],
            aggregates: vec![column("t", "region", DataType::Varchar), count_star()],
            output_names: vec!["region".to_string(), "cnt".to_string()],
            grouping_sets: None,
            group_observation: None,
        });
        assert_eq!(
            aggregate.schema(),
            vec![
                ("region".to_string(), DataType::Varchar),
                ("cnt".to_string(), DataType::BigInt),
            ]
        );
    }

    #[test]
    fn join_schema_renames_colliding_right_columns() {
        let join = PhysicalPlan::HashJoin(PhysicalHashJoin {
            left: Box::new(values("a")),
            right: Box::new(values("a")),
            join_type: JoinType::Inner,
            left_keys: vec![],
            right_keys: vec![],
            build_side: BuildSide::Right,
            estimated_rows: None,
            estimate_defaults: None,
        });
        assert_eq!(
            join.schema(),
            vec![
                ("a".to_string(), DataType::Integer),
                ("right_a".to_string(), DataType::Integer),
            ]
        );
    }

    #[test]
    fn semi_join_schema_keeps_left_columns_only() {
        let join = PhysicalPlan::HashJoin(PhysicalHashJoin {
            left: Box::new(values("a")),
            right: Box::new(values("a")),
            join_type: JoinType::Semi,
            left_keys: vec![],
            right_keys: vec![],
            build_side: BuildSide::Right,
            estimated_rows: None,
            estimate_defaults: None,
        });
        assert_eq!(join.schema(), vec![("a".to_string(), DataType::Integer)]);
    }

    #[test]
    fn seeded_remote_query_returns_its_seeded_schema() {
        let seeded: SeededSchema = vec![
            ("category".to_string(), DataType::Varchar),
            ("cnt".to_string(), DataType::BigInt),
        ];
        let remote = PhysicalPlan::RemoteQuery(Box::new(PhysicalRemoteQuery {
            datasource: "duck".to_string(),
            sql: "SELECT ...".to_string(),
            output_names: vec!["category".to_string(), "cnt".to_string()],
            column_alias_map: ColumnAliasMap::new(),
            estimated_rows: None,
            output_estimated_rows: None,
            column_ndv: None,
            seeded_schema: Some(seeded.clone()),
            group_observation: None,
        }));
        assert_eq!(remote.schema(), seeded);
    }

    #[test]
    fn values_schema_types_the_first_row() {
        assert_eq!(
            values("x").schema(),
            vec![("x".to_string(), DataType::Integer)]
        );
    }

    #[test]
    fn union_schema_is_its_first_branch() {
        let union = PhysicalPlan::Union(PhysicalUnion {
            inputs: vec![values("first"), values("second")],
            distinct: false,
        });
        assert_eq!(
            union.schema(),
            vec![("first".to_string(), DataType::Integer)]
        );
    }

    #[test]
    #[should_panic(expected = "needs a live source connection")]
    fn non_seeded_remote_query_schema_panics_loudly() {
        let remote = PhysicalPlan::RemoteQuery(Box::new(PhysicalRemoteQuery {
            datasource: "duck".to_string(),
            sql: "SELECT ...".to_string(),
            output_names: vec!["a".to_string()],
            column_alias_map: ColumnAliasMap::new(),
            estimated_rows: None,
            output_estimated_rows: None,
            column_ndv: None,
            seeded_schema: None,
            group_observation: None,
        }));
        let _ = remote.schema();
    }
}
