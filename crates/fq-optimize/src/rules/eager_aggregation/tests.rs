//! Tests for eager aggregation. The rule is driven over a real `CostModel` above
//! an in-crate fake catalog with TWO datasources (a "duck" fact and a "pg" dim),
//! so `single_source` is false and the collapse gate sees real row/NDV statistics.

use super::*;

use std::collections::BTreeMap;
use std::sync::Arc;

use fq_catalog::datasource::{
    map_native_type_default, ColumnStatistics, DataSource, DataSourceCapability, TableMetadata,
    TableStatistics,
};
use fq_catalog::{Catalog, CatalogError};
use fq_common::{CostConfig, DataType};
use fq_plan::expr::{BinaryOpType, LiteralValue};

use crate::statistics::StatisticsCollector;

// ------------------------------- cost harness -------------------------------

/// A configurable in-crate source: a name plus a table -> (row_count, per-column
/// NDV) map. `Send + Sync` as the trait requires.
struct FakeSource {
    source_name: String,
    tables: BTreeMap<String, (Option<i64>, BTreeMap<String, ColumnStatistics>)>,
}

impl DataSource for FakeSource {
    fn name(&self) -> &str {
        &self.source_name
    }
    fn capabilities(&self) -> Vec<DataSourceCapability> {
        vec![]
    }
    fn list_schemas(&self) -> std::result::Result<Vec<String>, CatalogError> {
        Ok(vec!["main".to_string()])
    }
    fn list_tables(&self, _schema: &str) -> std::result::Result<Vec<String>, CatalogError> {
        Ok(self.tables.keys().cloned().collect())
    }
    fn get_table_metadata(
        &self,
        schema: &str,
        table: &str,
    ) -> std::result::Result<TableMetadata, CatalogError> {
        Ok(TableMetadata {
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            columns: vec![],
            row_count: None,
            size_bytes: None,
        })
    }
    fn get_table_statistics(
        &self,
        _schema: &str,
        table: &str,
        columns: &[String],
    ) -> std::result::Result<Option<TableStatistics>, CatalogError> {
        let Some((row_count, all_stats)) = self.tables.get(table) else {
            return Ok(None);
        };
        let mut column_stats = BTreeMap::new();
        for column in columns {
            if let Some(stats) = all_stats.get(column) {
                column_stats.insert(column.clone(), stats.clone());
            }
        }
        Ok(Some(TableStatistics {
            row_count: *row_count,
            total_size_bytes: 0,
            column_stats,
        }))
    }
    fn map_native_type(&self, type_str: &str) -> std::result::Result<DataType, CatalogError> {
        map_native_type_default(type_str)
    }
}

/// A NDV statistics entry.
fn ndv_stats(num_distinct: i64) -> ColumnStatistics {
    ColumnStatistics {
        num_distinct: Some(num_distinct),
        null_fraction: 0.0,
        avg_width: 8,
        min_value: None,
        max_value: None,
    }
}

/// A seeded table: (name, row_count, per-column (name, NDV)).
type TableSeed<'a> = (&'a str, i64, &'a [(&'a str, i64)]);

/// A cost model over the given per-source table seeds.
fn model(sources: &[(&str, &[TableSeed])]) -> CostModel {
    let mut catalog = Catalog::new();
    for (source_name, tables) in sources {
        let mut table_map = BTreeMap::new();
        for &(name, rows, columns) in *tables {
            let mut column_map = BTreeMap::new();
            for &(column, ndv) in columns {
                column_map.insert(column.to_string(), ndv_stats(ndv));
            }
            table_map.insert(name.to_string(), (Some(rows), column_map));
        }
        catalog.register_datasource(Arc::new(FakeSource {
            source_name: (*source_name).to_string(),
            tables: table_map,
        }));
    }
    let stats = StatisticsCollector::new(Arc::new(catalog), None, None);
    CostModel::new(CostConfig::default(), Some(stats))
}

/// The rule over a shared handle to the given cost model.
fn rule(cost: CostModel) -> EagerAggregation {
    EagerAggregation::new(Rc::new(RefCell::new(cost)))
}

// ------------------------------- plan builders ------------------------------

/// A qualified integer column `table.column`.
fn col(table: &str, column: &str) -> Expr {
    Expr::Column(ColumnRef::new(
        Some(table.to_string()),
        column,
        Some(DataType::Integer),
    ))
}

/// `left = right`.
fn eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// `left > right`.
fn gt(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOpType::Gt,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// A plain SUM aggregate call over an integer column.
fn sum(table: &str, column: &str) -> Expr {
    Expr::FunctionCall {
        function_name: "SUM".to_string(),
        args: vec![col(table, column)],
        is_aggregate: true,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    }
}

/// The merge SUM the rewrite builds over a partial's synthetic column: a SUM whose
/// argument is `alias.column` typed BIGINT (SUM's result type, stamped by the
/// merge rebuild from the original call's `get_type`).
fn merge_sum(alias: &str, column: &str) -> Expr {
    Expr::FunctionCall {
        function_name: "SUM".to_string(),
        args: vec![Expr::Column(ColumnRef::new(
            Some(alias.to_string()),
            column,
            Some(DataType::BigInt),
        ))],
        is_aggregate: true,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    }
}

/// A base scan under `source`, aliased.
fn scan(source: &str, table: &str, alias: &str, columns: &[&str]) -> LogicalPlan {
    let mut node = Scan::new(
        source,
        "main",
        table,
        columns.iter().map(|c| (*c).to_string()).collect(),
    );
    node.alias = Some(alias.to_string());
    LogicalPlan::Scan(Box::new(node))
}

/// An inner join with a condition.
fn inner(left: LogicalPlan, right: LogicalPlan, condition: Expr) -> LogicalPlan {
    LogicalPlan::Join(Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        condition: Some(condition),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    })
}

/// An aggregate node.
fn aggregate(
    input: LogicalPlan,
    group_by: Vec<Expr>,
    aggregates: Vec<Expr>,
    output_names: &[&str],
) -> LogicalPlan {
    LogicalPlan::Aggregate(Aggregate {
        input: Box::new(input),
        group_by,
        aggregates,
        output_names: output_names.iter().map(|n| (*n).to_string()).collect(),
        grouping_sets: None,
    })
}

/// The q04-family plan: fact `orders` (duck) joined to dim `customer` (pg),
/// grouped by the dim's segment with SUM over a fact column.
fn q04_plan() -> LogicalPlan {
    let join = inner(
        scan("duck", "orders", "o", &["custkey", "total"]),
        scan("pg", "customer", "c", &["custkey", "mktsegment"]),
        eq(col("o", "custkey"), col("c", "custkey")),
    );
    aggregate(
        join,
        vec![col("c", "mktsegment")],
        vec![col("c", "mktsegment"), sum("o", "total")],
        &["mktsegment", "revenue"],
    )
}

/// The q04 cost model: a 1M-row fact (custkey NDV 200k) and a 500k-row dim (over
/// the ship budget), so the partial collapses 1M -> 200k (ratio 0.2, fires).
fn q04_model() -> CostModel {
    model(&[
        ("duck", &[("orders", 1_000_000, &[("custkey", 200_000)])]),
        (
            "pg",
            &[(
                "customer",
                500_000,
                &[("custkey", 500_000), ("mktsegment", 5)],
            )],
        ),
    ])
}

// ------------------------------- assertions ---------------------------------

/// The eager partial aggregate wrapped in the `__eager_` SubqueryScan, if any.
fn find_eager_partial(plan: &LogicalPlan) -> Option<&Aggregate> {
    if let LogicalPlan::SubqueryScan(subquery) = plan {
        if subquery.alias.starts_with(EAGER_PREFIX) {
            if let LogicalPlan::Aggregate(aggregate) = subquery.input.as_ref() {
                return Some(aggregate);
            }
        }
    }
    plan.children().into_iter().find_map(find_eager_partial)
}

// ------------------------------- shape declines -----------------------------

#[test]
fn grouping_sets_decline() {
    let LogicalPlan::Aggregate(mut agg) = q04_plan() else {
        unreachable!()
    };
    agg.grouping_sets = Some(vec![vec![col("c", "mktsegment")], vec![]]);
    let plan = LogicalPlan::Aggregate(agg);
    assert_eq!(rule(q04_model()).apply(plan.clone()).unwrap(), plan);
}

#[test]
fn non_sum_aggregate_declines() {
    let join = inner(
        scan("duck", "orders", "o", &["custkey", "total"]),
        scan("pg", "customer", "c", &["custkey", "mktsegment"]),
        eq(col("o", "custkey"), col("c", "custkey")),
    );
    let count = Expr::FunctionCall {
        function_name: "COUNT".to_string(),
        args: vec![col("o", "total")],
        is_aggregate: true,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    };
    let plan = aggregate(
        join,
        vec![col("c", "mktsegment")],
        vec![col("c", "mktsegment"), count],
        &["mktsegment", "cnt"],
    );
    assert_eq!(rule(q04_model()).apply(plan.clone()).unwrap(), plan);
}

#[test]
fn distinct_sum_declines() {
    let join = inner(
        scan("duck", "orders", "o", &["custkey", "total"]),
        scan("pg", "customer", "c", &["custkey", "mktsegment"]),
        eq(col("o", "custkey"), col("c", "custkey")),
    );
    let distinct_sum = Expr::FunctionCall {
        function_name: "SUM".to_string(),
        args: vec![col("o", "total")],
        is_aggregate: true,
        distinct: true,
        within_group_key: None,
        within_group_desc: false,
    };
    let plan = aggregate(
        join,
        vec![col("c", "mktsegment")],
        vec![col("c", "mktsegment"), distinct_sum],
        &["mktsegment", "revenue"],
    );
    assert_eq!(rule(q04_model()).apply(plan.clone()).unwrap(), plan);
}

#[test]
fn single_source_tree_declines() {
    // Both scans read "duck": a partial there is pure overhead, so it declines.
    let join = inner(
        scan("duck", "orders", "o", &["custkey", "total"]),
        scan("duck", "customer", "c", &["custkey", "mktsegment"]),
        eq(col("o", "custkey"), col("c", "custkey")),
    );
    let plan = aggregate(
        join,
        vec![col("c", "mktsegment")],
        vec![col("c", "mktsegment"), sum("o", "total")],
        &["mktsegment", "revenue"],
    );
    let cost = model(&[(
        "duck",
        &[
            ("orders", 1_000_000, &[("custkey", 200_000)]),
            (
                "customer",
                500_000,
                &[("custkey", 500_000), ("mktsegment", 5)],
            ),
        ],
    )]);
    assert_eq!(rule(cost).apply(plan.clone()).unwrap(), plan);
}

#[test]
fn non_equi_conjunct_declines() {
    let join = inner(
        scan("duck", "orders", "o", &["custkey", "total"]),
        scan("pg", "customer", "c", &["custkey", "mktsegment"]),
        gt(col("o", "custkey"), col("c", "custkey")),
    );
    let plan = aggregate(
        join,
        vec![col("c", "mktsegment")],
        vec![col("c", "mktsegment"), sum("o", "total")],
        &["mktsegment", "revenue"],
    );
    assert_eq!(rule(q04_model()).apply(plan.clone()).unwrap(), plan);
}

#[test]
fn left_join_tree_declines() {
    let join = LogicalPlan::Join(Join {
        left: Box::new(scan("duck", "orders", "o", &["custkey", "total"])),
        right: Box::new(scan("pg", "customer", "c", &["custkey", "mktsegment"])),
        join_type: JoinType::Left,
        condition: Some(eq(col("o", "custkey"), col("c", "custkey"))),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    });
    let plan = aggregate(
        join,
        vec![col("c", "mktsegment")],
        vec![col("c", "mktsegment"), sum("o", "total")],
        &["mktsegment", "revenue"],
    );
    assert_eq!(rule(q04_model()).apply(plan.clone()).unwrap(), plan);
}

// ------------------------------- idempotence --------------------------------

#[test]
fn already_split_plan_is_not_resplit() {
    let once = rule(q04_model()).apply(q04_plan()).unwrap();
    assert_ne!(once, q04_plan(), "the first pass must fire");
    let twice = rule(q04_model()).apply(once.clone()).unwrap();
    assert_eq!(
        twice, once,
        "a plan holding an eager partial does not re-split"
    );
}

// ------------------------------- ship-budget gate ---------------------------

#[test]
fn peeled_dim_under_ship_budget_declines() {
    // Customer is 100k rows (< 200k budget): full dim shipping dominates, so eager
    // aggregation steps aside and declines.
    let cost = model(&[
        ("duck", &[("orders", 1_000_000, &[("custkey", 200_000)])]),
        (
            "pg",
            &[(
                "customer",
                100_000,
                &[("custkey", 100_000), ("mktsegment", 5)],
            )],
        ),
    ]);
    let plan = q04_plan();
    assert_eq!(rule(cost).apply(plan.clone()).unwrap(), plan);
}

// ------------------------------- collapse gate ------------------------------

#[test]
fn collapse_ratio_above_half_declines() {
    // custkey NDV 800k over 1M fact rows -> ratio 0.8 > 0.5: a non-collapsing
    // partial is pure overhead, so it declines.
    let cost = model(&[
        ("duck", &[("orders", 1_000_000, &[("custkey", 800_000)])]),
        (
            "pg",
            &[(
                "customer",
                500_000,
                &[("custkey", 500_000), ("mktsegment", 5)],
            )],
        ),
    ]);
    let plan = q04_plan();
    assert_eq!(rule(cost).apply(plan.clone()).unwrap(), plan);
}

#[test]
fn gap_fed_collapse_ratio_declines() {
    // No NDV for the partial's group key (custkey): the group estimate rests on a
    // gap, so the gate declines rather than trust a bound.
    let cost = model(&[
        ("duck", &[("orders", 1_000_000, &[])]),
        (
            "pg",
            &[(
                "customer",
                500_000,
                &[("custkey", 500_000), ("mktsegment", 5)],
            )],
        ),
    ]);
    let plan = q04_plan();
    assert_eq!(rule(cost).apply(plan.clone()).unwrap(), plan);
}

// ------------------------------- happy path ---------------------------------

#[test]
fn q04_shape_pushes_a_partial_below_the_dim() {
    let result = rule(q04_model()).apply(q04_plan()).unwrap();
    // The final merge aggregate keeps the original output contract.
    let LogicalPlan::Aggregate(final_agg) = &result else {
        panic!("expected a final merge Aggregate");
    };
    assert_eq!(final_agg.output_names, vec!["mktsegment", "revenue"]);
    assert_eq!(final_agg.group_by, vec![col("c", "mktsegment")]);
    // The merge SUM reads the partial's synthetic column under the alias.
    assert_eq!(
        final_agg.aggregates[1],
        merge_sum("__eager_0", "__eager_sum_0")
    );
    // The join back is INNER on the alias key = the dim key.
    let LogicalPlan::Join(join) = final_agg.input.as_ref() else {
        panic!("expected the peeled dim joined back above the partial");
    };
    assert_eq!(
        join.condition,
        Some(eq(col("__eager_0", "custkey"), col("c", "custkey")))
    );
    assert!(
        matches!(join.right.as_ref(), LogicalPlan::Scan(scan) if scan.table_name == "customer")
    );
    // The partial groups the fact by its join key and sums the fact column.
    let partial = find_eager_partial(&result).expect("an eager partial");
    assert_eq!(partial.group_by, vec![col("o", "custkey")]);
    assert_eq!(partial.output_names, vec!["custkey", "__eager_sum_0"]);
    assert_eq!(partial.aggregates[1], sum("o", "total"));
}

#[test]
fn multi_sum_naming_and_constant_tag_survive() {
    let join = inner(
        scan("duck", "orders", "o", &["custkey", "total", "tax"]),
        scan("pg", "customer", "c", &["custkey", "mktsegment"]),
        eq(col("o", "custkey"), col("c", "custkey")),
    );
    let tag = Expr::Literal {
        value: LiteralValue::String("sale".to_string()),
        data_type: DataType::Varchar,
    };
    let plan = aggregate(
        join,
        vec![col("c", "mktsegment")],
        vec![
            col("c", "mktsegment"),
            tag.clone(),
            sum("o", "total"),
            sum("o", "tax"),
        ],
        &["mktsegment", "type", "rev", "tax"],
    );
    let result = rule(q04_model()).apply(plan).unwrap();
    let LogicalPlan::Aggregate(final_agg) = &result else {
        panic!("expected a final merge Aggregate");
    };
    // Constant tag passes through unchanged; the two SUMs merge over __eager_sum_0/1.
    assert_eq!(final_agg.aggregates[1], tag);
    assert_eq!(
        final_agg.aggregates[2],
        merge_sum("__eager_0", "__eager_sum_0")
    );
    assert_eq!(
        final_agg.aggregates[3],
        merge_sum("__eager_0", "__eager_sum_1")
    );
    let partial = find_eager_partial(&result).expect("an eager partial");
    assert_eq!(
        partial.output_names,
        vec!["custkey", "__eager_sum_0", "__eager_sum_1"]
    );
}

#[test]
fn best_of_selection_peels_the_lowest_ratio_subset() {
    // Two decorating dims; peeling BOTH collapses the fact to a tiny (ck,dk) group
    // count (ratio ~9e-6), beating either single-dim peel, so both are peeled and
    // only the fact survives inside the partial.
    let join = inner(
        inner(
            scan("duck", "orders", "o", &["ck", "dk", "total"]),
            scan("pg", "customer", "c", &["ck", "seg"]),
            eq(col("o", "ck"), col("c", "ck")),
        ),
        scan("pg", "region", "d", &["dk", "reg"]),
        eq(col("o", "dk"), col("d", "dk")),
    );
    let plan = aggregate(
        join,
        vec![col("c", "seg"), col("d", "reg")],
        vec![col("c", "seg"), col("d", "reg"), sum("o", "total")],
        &["seg", "reg", "revenue"],
    );
    let cost = model(&[
        ("duck", &[("orders", 1_000_000, &[("ck", 3), ("dk", 3)])]),
        (
            "pg",
            &[
                ("customer", 300_000, &[("ck", 300_000), ("seg", 1000)]),
                ("region", 300_000, &[("dk", 300_000), ("reg", 1000)]),
            ],
        ),
    ]);
    let result = rule(cost).apply(plan).unwrap();
    let partial = find_eager_partial(&result).expect("an eager partial");
    // Both fact keys anchor the partial (both dims were peeled); the key order
    // follows the pooled-conjunct order (outer join condition o.dk first).
    assert_eq!(partial.group_by, vec![col("o", "dk"), col("o", "ck")]);
}

// ------------------------------- driver scope -------------------------------

#[test]
fn driver_validates_the_eager_scope() {
    use crate::RuleBasedOptimizer;
    // The rule alone through the driver: validate_scope accepts the rewrite (the
    // partial's SubqueryScan alias re-qualifies every synthetic output).
    let optimizer = RuleBasedOptimizer::new(vec![Box::new(EagerAggregation::new(Rc::new(
        RefCell::new(q04_model()),
    )))]);
    let result = optimizer.optimize(q04_plan()).unwrap();
    assert!(find_eager_partial(&result).is_some());
}

// ------------------------------- raise paths --------------------------------

#[test]
fn require_all_placed_raises_on_a_dropped_conjunct() {
    let leftover = eq(col("o", "custkey"), col("c", "custkey"));
    assert!(matches!(
        require_all_placed(std::slice::from_ref(&leftover)),
        Err(OptimizeError::EagerAggregation(_))
    ));
    assert!(require_all_placed(&[]).is_ok());
}

#[test]
fn next_joinable_raises_on_an_unreachable_dim() {
    // A peeled dim "z" that no lifted conjunct links to the scope is a partitioning
    // bug: crash rather than manufacture a cross join.
    let LogicalPlan::Scan(dim) = scan("pg", "zdim", "z", &["k"]) else {
        unreachable!()
    };
    let remaining = vec![dim.as_ref()];
    let unrelated = eq(col("o", "custkey"), col("c", "custkey"));
    let scope: HashSet<String> = std::iter::once("__eager_0".to_string()).collect();
    assert!(matches!(
        next_joinable(&remaining, std::slice::from_ref(&unrelated), &scope),
        Err(OptimizeError::EagerAggregation(_))
    ));
}
