//! Scan-spec builders + parallel-scan eligibility. Ports `raw_scan_spec`,
//! `structured_scan_spec`, `_reject_nonplain_scan`, `_source_scan_spec`,
//! `_injected_probe_spec`, and the parallel-scan gates of `executor/rust_ir.py`.

use fq_plan::physical::{DatasourceKind, PhysicalScan};
use fq_plan::PhysicalPlan;

use super::error::StepError;
use super::expr_retag::plain;
use super::render_sql::{render_remote_set_op, render_scan_sql};
use super::types::ScanSpec;

/// A plain PostgreSQL scan at least this many estimated rows reads through the
/// engine's ctid-partitioned parallel path. Below this size the per-partition round
/// trips cost more than they save. Ports `PARALLEL_SCAN_MIN_ROWS`.
pub const PARALLEL_SCAN_MIN_ROWS: u64 = 50_000;

/// The scan spec for a plain source read: a structured spec marked `parallel` for a
/// big plain PostgreSQL scan (the engine reads it ctid-partitioned), pre-rendered
/// SQL otherwise. Ports `_source_scan_spec`. Since the filter is now a carriable
/// `Expr` (not a serialized sub-IR), an eligible scan's structured spec can no
/// longer fail on the filter, so the Python try/except-UnsupportedIR fallback
/// collapses to the eligibility check alone.
pub fn source_scan_spec(node: &PhysicalPlan) -> Result<ScanSpec, StepError> {
    if !parallel_scan_eligible(node) {
        return raw_scan_spec(node);
    }
    let PhysicalPlan::Scan(scan) = node else {
        return raw_scan_spec(node);
    };
    let mut spec = structured_scan_spec(scan)?;
    spec.parallel = true;
    Ok(spec)
}

/// A scan spec carrying pre-rendered source SQL: the scan's rendered SELECT, or a
/// remote query's already-rendered SQL. Ports `raw_scan_spec`. A node that renders
/// no source SQL (a bare set-op that reached here) RAISES.
pub fn raw_scan_spec(node: &PhysicalPlan) -> Result<ScanSpec, StepError> {
    match node {
        PhysicalPlan::Scan(scan) => Ok(ScanSpec::raw(render_scan_sql(scan)?)),
        PhysicalPlan::RemoteQuery(remote) => Ok(ScanSpec::raw(remote.sql.clone())),
        PhysicalPlan::RemoteSetOp(set_op) => Ok(ScanSpec::raw(render_remote_set_op(set_op)?)),
        other => Err(StepError::NoSourceSql(variant_name(other))),
    }
}

/// A structured scan spec for a plain single-table scan: the table, its concrete
/// columns, schema/alias qualifiers, filter (own-qualifier `Expr`), and DISTINCT.
/// Required when the engine must inject a dynamic `col IN (...)` filter. Ports
/// `structured_scan_spec` + `_add_scan_qualifiers` + `_add_scan_predicates`. GUARD:
/// a `*` in the columns RAISES (a star must never survive into a bound scan).
pub fn structured_scan_spec(scan: &PhysicalScan) -> Result<ScanSpec, StepError> {
    reject_nonplain_scan(scan)?;
    if scan.columns.iter().any(|column| column == "*") {
        return Err(StepError::StarInScanColumns);
    }
    let mut spec = ScanSpec::empty();
    spec.table = Some(scan.table_name.clone());
    spec.columns.clone_from(&scan.columns);
    if !scan.schema_name.is_empty() {
        spec.schema = Some(scan.schema_name.clone());
    }
    if let Some(alias) = &scan.alias {
        spec.alias = Some(alias.clone());
    }
    if let Some(filter) = &scan.filters {
        spec.filter = Some(plain(filter));
    }
    spec.distinct = scan.distinct;
    Ok(spec)
}

/// A structured spec for a plain probe base; pre-rendered SQL for a pushed remote
/// query OR an aggregate scan (its output columns take the injected filter through
/// the engine's output wrapper). Ports `_injected_probe_spec`.
///
/// Not implemented (correct, slower): the `injected_sql` optimization (placing the key
/// filter INSIDE the island/aggregate base relation rather than wrapping its output)
/// is not built here (the RemoteQuery holds only rendered `sql`, not the AST the
/// Python re-parses). Without it the engine wraps the base output, which is exactly
/// as correct; only a source-side pushdown speedup is forfeited.
pub fn injected_probe_spec(base: &PhysicalPlan) -> Result<ScanSpec, StepError> {
    match base {
        PhysicalPlan::RemoteQuery(_) => raw_scan_spec(base),
        PhysicalPlan::Scan(scan) if scan_is_aggregate(scan) => raw_scan_spec(base),
        PhysicalPlan::Scan(scan) => structured_scan_spec(scan),
        other => raw_scan_spec(other),
    }
}

/// Refuse a scan that already folds aggregation/ordering/limits: it cannot take a
/// composed `col IN (...)` filter in its own WHERE. Ports `_reject_nonplain_scan`.
fn reject_nonplain_scan(scan: &PhysicalScan) -> Result<(), StepError> {
    if scan_is_aggregate(scan) || scan_is_ordered_or_limited(scan) {
        return Err(StepError::NonPlainScan);
    }
    Ok(())
}

/// Whether a scan is a big, plain, partition-safe PostgreSQL table read. Ports
/// `_parallel_scan_eligible`.
pub fn parallel_scan_eligible(node: &PhysicalPlan) -> bool {
    is_big_postgres_scan(node) && partition_safe_scan(node)
}

/// A PhysicalScan on a PostgreSQL source whose row estimate clears the parallel
/// threshold. An unestimated (or non-Postgres) scan stays single-stream. Ports
/// `_is_big_postgres_scan` (the `isinstance(PostgreSQLDataSource)` check reads the
/// stamped `datasource_kind`).
fn is_big_postgres_scan(node: &PhysicalPlan) -> bool {
    let PhysicalPlan::Scan(scan) = node else {
        return false;
    };
    if scan.datasource_kind != DatasourceKind::Postgres {
        return false;
    }
    scan.estimated_rows
        .is_some_and(|rows| rows >= PARALLEL_SCAN_MIN_ROWS)
}

/// Whether ctid-partitioned reads return exactly this scan's rows: DISTINCT and
/// TABLESAMPLE are not partition-safe. Ports `_partition_safe_scan`.
fn partition_safe_scan(node: &PhysicalPlan) -> bool {
    let PhysicalPlan::Scan(scan) = node else {
        return false;
    };
    if scan.distinct || scan.sample.is_some() {
        return false;
    }
    injectable_scan(node)
}

/// A plain single-table scan the engine can inject `col IN (...)` into (mirrors
/// `reject_nonplain_scan`, so an eligible scan's structured spec never raises).
/// Ports `_injectable_scan`; false for anything that is not a scan.
pub fn injectable_scan(node: &PhysicalPlan) -> bool {
    match node {
        PhysicalPlan::Scan(scan) => !scan_is_aggregate(scan) && !scan_is_ordered_or_limited(scan),
        _ => false,
    }
}

/// Whether a scan folds an aggregate/grouping shape.
fn scan_is_aggregate(scan: &PhysicalScan) -> bool {
    scan.aggregates
        .as_ref()
        .is_some_and(|aggs| !aggs.is_empty())
        || scan.group_by.as_ref().is_some_and(|keys| !keys.is_empty())
        || scan
            .grouping_sets
            .as_ref()
            .is_some_and(|sets| !sets.is_empty())
}

/// Whether a scan folds ordering/limit/offset.
fn scan_is_ordered_or_limited(scan: &PhysicalScan) -> bool {
    scan.order_by_keys
        .as_ref()
        .is_some_and(|keys| !keys.is_empty())
        || scan.limit.is_some()
        || scan.offset != 0
}

/// The variant name of a physical node, for error text.
pub fn variant_name(node: &PhysicalPlan) -> &'static str {
    match node {
        PhysicalPlan::Cte(_) => "Cte",
        PhysicalPlan::CteScan(_) => "CteScan",
        PhysicalPlan::Shipment(_) => "Shipment",
        PhysicalPlan::AliasedRelation(_) => "AliasedRelation",
        PhysicalPlan::CteMergeQuery(_) => "CteMergeQuery",
        PhysicalPlan::Scan(_) => "Scan",
        PhysicalPlan::Projection(_) => "Projection",
        PhysicalPlan::Window(_) => "Window",
        PhysicalPlan::Filter(_) => "Filter",
        PhysicalPlan::HashJoin(_) => "HashJoin",
        PhysicalPlan::RemoteJoin(_) => "RemoteJoin",
        PhysicalPlan::NestedLoopJoin(_) => "NestedLoopJoin",
        PhysicalPlan::HashAggregate(_) => "HashAggregate",
        PhysicalPlan::Sort(_) => "Sort",
        PhysicalPlan::Limit(_) => "Limit",
        PhysicalPlan::Values(_) => "Values",
        PhysicalPlan::RemoteQuery(_) => "RemoteQuery",
        PhysicalPlan::Union(_) => "Union",
        PhysicalPlan::RemoteSetOp(_) => "RemoteSetOp",
        PhysicalPlan::SetOperation(_) => "SetOperation",
        PhysicalPlan::SingleRowGuard(_) => "SingleRowGuard",
        PhysicalPlan::GroupedLimit(_) => "GroupedLimit",
        PhysicalPlan::LateralJoin(_) => "LateralJoin",
        PhysicalPlan::Explain(_) => "Explain",
        PhysicalPlan::Gather(_) => "Gather",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{BinaryOpType, ColumnRef, Expr, LiteralValue};
    use fq_plan::physical::PhysicalScan;

    /// A base scan builder with all knobs defaulted; the test overrides fields.
    fn base_scan() -> PhysicalScan {
        PhysicalScan {
            datasource: "pg".to_string(),
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec!["id".to_string(), "amount".to_string()],
            filters: None,
            sample: None,
            group_by: None,
            grouping_sets: None,
            aggregates: None,
            output_names: None,
            alias: Some("orders".to_string()),
            limit: None,
            offset: 0,
            order_by_keys: None,
            order_by_ascending: None,
            order_by_nulls: None,
            distinct: false,
            dynamic_filter_keys: None,
            estimated_rows: Some(PARALLEL_SCAN_MIN_ROWS),
            column_ndv: None,
            seeded_schema: None,
            datasource_kind: DatasourceKind::Postgres,
        }
    }

    /// `orders.amount > 100`, a carriable scan filter.
    fn amount_filter() -> Expr {
        Expr::BinaryOp {
            op: BinaryOpType::Gt,
            left: Box::new(Expr::Column(ColumnRef::new(
                Some("orders".to_string()),
                "amount",
                Some(DataType::Double),
            ))),
            right: Box::new(Expr::Literal {
                value: LiteralValue::Integer(100),
                data_type: DataType::Integer,
            }),
        }
    }

    fn node(scan: PhysicalScan) -> PhysicalPlan {
        PhysicalPlan::Scan(Box::new(scan))
    }

    #[test]
    fn big_plain_pg_scan_is_structured_and_parallel() {
        let mut scan = base_scan();
        scan.filters = Some(amount_filter());
        let spec = source_scan_spec(&node(scan)).expect("spec");
        assert!(spec.parallel, "big plain PG scan must parallelize");
        assert_eq!(spec.table, Some("orders".to_string()));
        assert!(spec.raw_sql.is_none(), "structured spec, no raw SQL");
        assert!(spec.filter.is_some(), "the scan filter is carried");
        assert_eq!(spec.columns, vec!["id".to_string(), "amount".to_string()]);
    }

    #[test]
    fn small_pg_scan_falls_back_to_raw() {
        let mut scan = base_scan();
        scan.estimated_rows = Some(PARALLEL_SCAN_MIN_ROWS - 1);
        let spec = source_scan_spec(&node(scan)).expect("spec");
        assert!(!spec.parallel);
        assert!(
            spec.raw_sql.is_some(),
            "small scan reads single-stream raw SQL"
        );
    }

    #[test]
    fn distinct_scan_is_not_partition_safe() {
        let mut scan = base_scan();
        scan.distinct = true;
        let spec = source_scan_spec(&node(scan)).expect("spec");
        assert!(!spec.parallel);
        assert!(spec.raw_sql.is_some());
    }

    #[test]
    fn sampled_scan_is_not_partition_safe() {
        let mut scan = base_scan();
        scan.sample = Some("TABLESAMPLE BERNOULLI (10)".to_string());
        let spec = source_scan_spec(&node(scan)).expect("spec");
        assert!(!spec.parallel);
    }

    #[test]
    fn ordered_scan_is_not_injectable() {
        let mut scan = base_scan();
        scan.order_by_keys = Some(vec![Expr::Column(ColumnRef::new(
            Some("orders".to_string()),
            "id",
            Some(DataType::Integer),
        ))]);
        scan.order_by_ascending = Some(vec![true]);
        let spec = source_scan_spec(&node(scan)).expect("spec");
        assert!(!spec.parallel);
    }

    #[test]
    fn non_postgres_scan_never_parallelizes() {
        let mut scan = base_scan();
        scan.datasource_kind = DatasourceKind::DuckDb;
        let spec = source_scan_spec(&node(scan)).expect("spec");
        assert!(
            !spec.parallel,
            "only Postgres scans use the ctid-parallel path"
        );
    }

    #[test]
    fn star_in_scan_columns_raises() {
        let mut scan = base_scan();
        scan.columns = vec!["*".to_string()];
        let err = structured_scan_spec(&scan).unwrap_err();
        assert!(matches!(err, StepError::StarInScanColumns));
    }
}
