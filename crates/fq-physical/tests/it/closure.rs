//! Referential-closure tests: which qualified references a plan may make, and
//! which ones the guard refuses before anything executes.

use std::collections::BTreeMap;

use fq_common::DataType;
use fq_physical::check_referential_closure;
use fq_physical::error::PhysicalError;
use fq_plan::expr::{ColumnRef, Expr};
use fq_plan::logical::{JoinType, SetOpKind};
use fq_plan::physical::{
    BuildSide, DatasourceKind, PhysicalAliasedRelation, PhysicalHashJoin, PhysicalPlan,
    PhysicalProjection, PhysicalRemoteQuery, PhysicalScan, PhysicalSetOperation, PhysicalUnion,
};

/// A DuckDB scan of `main.<table>` aliased to its own name.
fn scan(table: &str, columns: &[&str]) -> PhysicalPlan {
    let mut column_names = Vec::new();
    for column in columns {
        column_names.push((*column).to_string());
    }
    PhysicalPlan::Scan(Box::new(PhysicalScan {
        datasource: "duck".to_string(),
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
        datasource_kind: DatasourceKind::DuckDb,
    }))
}

/// A qualified integer column reference.
fn col(table: &str, name: &str) -> Expr {
    Expr::Column(ColumnRef::new(
        Some(table.to_string()),
        name,
        Some(DataType::Integer),
    ))
}

/// A hash join on one key from each side.
fn join(left: PhysicalPlan, right: PhysicalPlan, left_key: Expr, right_key: Expr) -> PhysicalPlan {
    PhysicalPlan::HashJoin(PhysicalHashJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        left_keys: vec![left_key],
        right_keys: vec![right_key],
        build_side: BuildSide::Right,
        estimated_rows: None,
        estimate_defaults: None,
    })
}

/// A projection of the given expressions under the given output names.
fn project(input: PhysicalPlan, expressions: Vec<Expr>, names: &[&str]) -> PhysicalPlan {
    let mut output_names = Vec::new();
    for name in names {
        output_names.push((*name).to_string());
    }
    PhysicalPlan::Projection(PhysicalProjection {
        input: Box::new(input),
        expressions,
        physical_names: output_names.clone(),
        output_names,
        distinct: false,
        distinct_on: None,
    })
}

/// A pushed island exposing exactly the references in its column-alias map.
fn island(exposed: &[(&str, &str)], output_names: &[&str]) -> PhysicalPlan {
    let mut alias_map = BTreeMap::new();
    for (table, column) in exposed {
        alias_map.insert(
            (Some((*table).to_string()), (*column).to_string()),
            (*column).to_string(),
        );
    }
    let mut names = Vec::new();
    for name in output_names {
        names.push((*name).to_string());
    }
    PhysicalPlan::RemoteQuery(Box::new(PhysicalRemoteQuery {
        datasource: "duck".to_string(),
        sql: "SELECT 1".to_string(),
        canonical_sql: "SELECT 1".to_string(),
        output_names: names,
        column_alias_map: alias_map,
        estimated_rows: None,
        output_estimated_rows: None,
        column_ndv: None,
        seeded_schema: None,
        group_observation: None,
    }))
}

/// The reference named by an `UnproducibleColumn`, or a panic naming what came
/// back instead.
fn rejected_reference(result: Result<(), PhysicalError>) -> String {
    match result {
        Err(PhysicalError::UnproducibleColumn { reference, .. }) => reference,
        Err(other) => panic!("expected UnproducibleColumn, got {other:?}"),
        Ok(()) => panic!("expected the closure check to reject this plan"),
    }
}

/// A join over two scans may key on either side's own columns.
#[test]
fn a_join_may_key_on_columns_its_scans_read() {
    let plan = join(
        scan("orders", &["order_id", "customer_id"]),
        scan("customers", &["customer_id"]),
        col("orders", "customer_id"),
        col("customers", "customer_id"),
    );
    assert!(check_referential_closure(&plan).is_ok());
}

/// A join keying on a column no child reads is refused, naming that reference.
#[test]
fn a_join_keying_on_a_column_no_child_reads_is_refused() {
    let plan = join(
        scan("orders", &["order_id"]),
        scan("customers", &["customer_id"]),
        col("orders", "customer_id"),
        col("customers", "customer_id"),
    );
    assert_eq!(
        rejected_reference(check_referential_closure(&plan)),
        "orders.customer_id"
    );
}

/// A pushed island exposes exactly what its column-alias map answers: a join
/// keying on a relation the island dropped is refused. This is the shape a
/// co-located island takes when its SELECT list omits a derived relation's
/// columns.
#[test]
fn a_join_over_an_island_that_dropped_the_key_is_refused() {
    let plan = join(
        island(&[("dd", "dept"), ("dd", "region")], &["dept", "region"]),
        scan("fact_sales", &["sale_id", "item_key"]),
        col("m", "ik"),
        col("fact_sales", "item_key"),
    );
    assert_eq!(rejected_reference(check_referential_closure(&plan)), "m.ik");
}

/// The same join is accepted once the island's alias map answers the reference.
#[test]
fn the_same_join_is_accepted_when_the_island_exposes_the_key() {
    let plan = join(
        island(
            &[("dd", "dept"), ("dd", "region"), ("m", "ik")],
            &["dept", "region", "ik"],
        ),
        scan("fact_sales", &["sale_id", "item_key"]),
        col("m", "ik"),
        col("fact_sales", "item_key"),
    );
    assert!(check_referential_closure(&plan).is_ok());
}

/// An aliased relation exposes its input's OUTPUT names under the alias, not the
/// source columns those names were computed from.
#[test]
fn an_alias_exposes_the_renamed_output_not_the_source_column() {
    let inner = project(
        scan("customers", &["name"]),
        vec![col("customers", "name")],
        &["cust_name"],
    );
    let aliased = PhysicalPlan::AliasedRelation(PhysicalAliasedRelation {
        input: Box::new(inner),
        alias: "sub".to_string(),
    });
    let renamed = project(aliased, vec![col("sub", "cust_name")], &["cust_name"]);
    assert!(check_referential_closure(&renamed).is_ok());

    let inner_name = project(
        scan("customers", &["name"]),
        vec![col("customers", "name")],
        &["cust_name"],
    );
    let aliased_again = PhysicalPlan::AliasedRelation(PhysicalAliasedRelation {
        input: Box::new(inner_name),
        alias: "sub".to_string(),
    });
    let pre_rename = project(aliased_again, vec![col("sub", "name")], &["name"]);
    assert_eq!(
        rejected_reference(check_referential_closure(&pre_rename)),
        "sub.name"
    );
}

/// An alias over a UNION exposes the first branch's names; a union carries one
/// schema across its branches, so naming it by the alias must resolve.
#[test]
fn an_alias_over_a_union_exposes_the_branch_names() {
    let union = PhysicalPlan::Union(PhysicalUnion {
        inputs: vec![
            project(
                scan("orders", &["order_id"]),
                vec![col("orders", "order_id")],
                &["k"],
            ),
            project(
                scan("customers", &["customer_id"]),
                vec![col("customers", "customer_id")],
                &["k"],
            ),
        ],
        distinct: false,
    });
    let aliased = PhysicalPlan::AliasedRelation(PhysicalAliasedRelation {
        input: Box::new(union),
        alias: "u".to_string(),
    });
    let plan = project(aliased, vec![col("u", "k")], &["k"]);
    assert!(check_referential_closure(&plan).is_ok());
}

/// An alias over a set operation exposes its LEFT branch's names, which is the
/// schema a set operation carries.
#[test]
fn an_alias_over_a_set_operation_exposes_the_left_branch_names() {
    let set_op = PhysicalPlan::SetOperation(PhysicalSetOperation {
        left: Box::new(project(
            scan("orders", &["order_id"]),
            vec![col("orders", "order_id")],
            &["k"],
        )),
        right: Box::new(project(
            scan("customers", &["customer_id"]),
            vec![col("customers", "customer_id")],
            &["k"],
        )),
        kind: SetOpKind::Intersect,
        distinct: true,
    });
    let aliased = PhysicalPlan::AliasedRelation(PhysicalAliasedRelation {
        input: Box::new(set_op),
        alias: "u".to_string(),
    });
    let plan = project(aliased, vec![col("u", "k")], &["k"]);
    assert!(check_referential_closure(&plan).is_ok());
}

/// An unqualified reference names no relation, so the check passes over it: it
/// is a computed output carrying its own physical name.
#[test]
fn an_unqualified_reference_is_not_checked() {
    let plan = project(
        scan("orders", &["order_id"]),
        vec![Expr::Column(ColumnRef::new(None, "anything", None))],
        &["anything"],
    );
    assert!(check_referential_closure(&plan).is_ok());
}
