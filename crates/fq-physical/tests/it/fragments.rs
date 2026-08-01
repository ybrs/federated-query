//! Pushdown-fragment collection tests: which nodes of a physical plan are a
//! round trip to a source, what text each one carries, and where collection
//! stops descending.

use std::collections::BTreeMap;

use fq_common::DataType;
use fq_physical::collect_pushdown_fragments;
use fq_physical::steps::{render_scan_canonical, render_scan_sql};
use fq_plan::expr::{ColumnRef, Expr};
use fq_plan::logical::{JoinType, SetOpKind};
use fq_plan::physical::{
    BuildSide, DatasourceKind, PhysicalHashJoin, PhysicalPlan, PhysicalRemoteQuery,
    PhysicalRemoteSetOp, PhysicalScan,
};

/// A plain scan of `main.<table>` on `<datasource>`, aliased to the table name.
fn scan_node(
    datasource: &str,
    table: &str,
    columns: &[&str],
    kind: DatasourceKind,
) -> PhysicalScan {
    let mut column_names = Vec::new();
    for column in columns {
        column_names.push((*column).to_string());
    }
    PhysicalScan {
        datasource: datasource.to_string(),
        schema_name: "main".to_string(),
        table_name: table.to_string(),
        columns: column_names,
        filters: None,
        sample: None,
        group_by: None,
        grouping_sets: None,
        aggregates: None,
        output_names: None,
        alias: Some(table.to_string()),
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
        datasource_kind: kind,
    }
}

/// A DuckDB scan as a plan node.
fn duck_scan(table: &str, columns: &[&str]) -> PhysicalPlan {
    PhysicalPlan::Scan(Box::new(scan_node(
        "duck",
        table,
        columns,
        DatasourceKind::DuckDb,
    )))
}

/// A qualified integer column reference.
fn col(table: &str, name: &str) -> Expr {
    Expr::Column(ColumnRef::new(
        Some(table.to_string()),
        name,
        Some(DataType::Integer),
    ))
}

/// A remote island whose sent text and canonical text are distinguishable.
fn remote_query_node() -> PhysicalRemoteQuery {
    PhysicalRemoteQuery {
        datasource: "duck".to_string(),
        sql: "SELECT source_marker FROM t".to_string(),
        canonical_sql: "SELECT canonical_marker FROM t".to_string(),
        output_names: vec!["k".to_string()],
        column_alias_map: BTreeMap::new(),
        estimated_rows: None,
        output_estimated_rows: None,
        column_ndv: None,
        seeded_schema: None,
        group_observation: None,
    }
}

/// A UNION ALL of two DuckDB scans, kept whole on the source.
fn remote_set_op_node() -> PhysicalPlan {
    PhysicalPlan::RemoteSetOp(Box::new(PhysicalRemoteSetOp {
        left: Box::new(duck_scan("left_branch", &["k"])),
        right: Box::new(duck_scan("right_branch", &["k"])),
        kind: SetOpKind::Union,
        distinct: false,
        datasource: "duck".to_string(),
        order_by_keys: None,
        order_by_ascending: None,
        order_by_nulls: None,
        limit: None,
        offset: 0,
    }))
}

/// An inner hash join of two plan nodes on `k`.
fn join_of(left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
    PhysicalPlan::HashJoin(PhysicalHashJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        left_keys: vec![col("orders", "k")],
        right_keys: vec![col("lineitem", "k")],
        build_side: BuildSide::Right,
        estimated_rows: None,
        estimate_defaults: None,
    })
}

#[test]
fn plain_scan_is_one_fragment_carrying_its_source_and_canonical_text() {
    // A MySQL-seated scan: its sent text is backtick-quoted MySQL while the
    // canonical form stays double-quoted Postgres, so the two fields are
    // distinguishable and a swap cannot pass.
    let scan = scan_node("shop", "orders", &["id", "total"], DatasourceKind::MySql);
    let fragments =
        collect_pushdown_fragments(&PhysicalPlan::Scan(Box::new(scan.clone()))).expect("collect");

    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].datasource, "shop");
    assert_eq!(
        fragments[0].source_sql,
        render_scan_sql(&scan).expect("sql")
    );
    assert_eq!(
        fragments[0].canonical_sql,
        render_scan_canonical(&scan).expect("canonical")
    );
    assert_ne!(fragments[0].source_sql, fragments[0].canonical_sql);
    assert!(fragments[0].source_sql.contains('`'), "{:?}", fragments[0]);
    assert!(
        fragments[0].canonical_sql.contains('"'),
        "{:?}",
        fragments[0]
    );
    assert_eq!(fragments[0].output_names, vec!["id", "total"]);
    assert!(!fragments[0].has_dynamic_filter);
}

#[test]
fn remote_query_keeps_canonical_and_sent_text_in_their_own_fields() {
    // `canonical_sql` is the node's canonical text and `source_sql` is the text
    // the node sends; the two markers fail the test if they are swapped.
    let remote = remote_query_node();
    let fragments =
        collect_pushdown_fragments(&PhysicalPlan::RemoteQuery(Box::new(remote))).expect("collect");

    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].datasource, "duck");
    assert_eq!(fragments[0].source_sql, "SELECT source_marker FROM t");
    assert_eq!(fragments[0].canonical_sql, "SELECT canonical_marker FROM t");
    assert_eq!(fragments[0].output_names, vec!["k"]);
}

#[test]
fn remote_set_op_is_one_fragment_not_one_per_branch() {
    // The branches are part of the text the source receives, so collection must
    // not descend into them: two scans under one pushed set operation are ONE
    // round trip, not two.
    let fragments = collect_pushdown_fragments(&remote_set_op_node()).expect("collect");

    assert_eq!(fragments.len(), 1, "{fragments:?}");
    assert_eq!(fragments[0].datasource, "duck");
    assert!(
        fragments[0].source_sql.to_uppercase().contains("UNION"),
        "{:?}",
        fragments[0]
    );
    assert!(
        fragments[0].source_sql.contains("left_branch")
            && fragments[0].source_sql.contains("right_branch"),
        "{:?}",
        fragments[0]
    );
    assert_eq!(fragments[0].output_names, vec!["k"]);
    assert!(!fragments[0].has_dynamic_filter);
}

#[test]
fn interior_node_yields_one_fragment_per_side_in_plan_order() {
    // A join reads nothing itself; each of its scans is its own round trip, and
    // the left side is collected before the right.
    let join = join_of(
        duck_scan("orders", &["k"]),
        duck_scan("lineitem", &["k", "qty"]),
    );
    let fragments = collect_pushdown_fragments(&join).expect("collect");

    assert_eq!(fragments.len(), 2, "{fragments:?}");
    assert!(fragments[0].source_sql.contains("orders"), "{fragments:?}");
    assert!(
        fragments[1].source_sql.contains("lineitem"),
        "{fragments:?}"
    );
    assert_eq!(fragments[1].output_names, vec!["k", "qty"]);
}

#[test]
fn dynamic_filter_flag_follows_the_scan_key_list() {
    // A scan with injected join keys sends a base query the source then receives
    // an IN-list on top of; without keys the sent text is the whole query.
    let mut probe = scan_node("duck", "orders", &["k"], DatasourceKind::DuckDb);
    probe.dynamic_filter_keys = Some(vec![col("orders", "k")]);
    let probe_fragments =
        collect_pushdown_fragments(&PhysicalPlan::Scan(Box::new(probe))).expect("collect");
    assert!(probe_fragments[0].has_dynamic_filter);

    let plain = collect_pushdown_fragments(&duck_scan("orders", &["k"])).expect("collect");
    assert!(!plain[0].has_dynamic_filter);
}

#[test]
fn remote_query_never_reports_a_dynamic_filter() {
    // A rendered island carries its whole predicate in its text; nothing is
    // appended to it at execution time.
    let fragments = collect_pushdown_fragments(&join_of(
        PhysicalPlan::RemoteQuery(Box::new(remote_query_node())),
        duck_scan("lineitem", &["k"]),
    ))
    .expect("collect");

    assert_eq!(fragments.len(), 2, "{fragments:?}");
    assert!(!fragments[0].has_dynamic_filter);
}
