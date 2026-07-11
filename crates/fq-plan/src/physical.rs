//! Physical plan nodes. Ports the NODE STRUCTS of `plan/physical.py`.
//!
//! `PhysicalPlan` is ONE enum wrapping a struct per node type; `children` is an
//! exhaustive match. These are a pure plan representation - never executed here;
//! the Rust engine (fq-exec) is the one execution path.
//!
//! SCOPE (Stage B): the structural node data + `children`. The Arrow-typed
//! `schema()` and the `column_aliases()` resolution map are DEFERRED to Stage C /
//! fq-emit (they need the arrow crate and the alias-map helpers, and their
//! consumers - the IR serializer and the emitter - do not exist yet).
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
}
