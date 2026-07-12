//! Single-source pushdown behavioral tests at the PhysicalRemoteQuery level.
//!
//! Two drivers are used. The precise-SHAPE cases (join / SEMI-ANTI -> EXISTS /
//! nullable-right ON carry / star expansion / DISTINCT ON / estimates+NDV) call
//! `SingleSourcePushdown::try_build` on hand-built logical plans and assert on the
//! rendered `PhysicalRemoteQuery.sql` (and its estimate/alias/NDV fields). The
//! end-to-end cases (aggregate, CTE, set-op-in-a-subquery) drive real SQL through
//! parse -> bind -> decorrelate -> optimize -> plan and assert the produced remote
//! query. This is the Rust analogue of the Python e2e_pushdown suites, which
//! assert on the query the source receives (pre-transpile canonical Postgres).

use std::collections::BTreeMap;
use std::sync::Arc;

use fq_catalog::datasource::{DataSource, DataSourceCapability, TableMetadata, TableStatistics};
use fq_catalog::{Catalog, CatalogError, Column, Schema, Table};
use fq_common::{CostConfig, DataType, OptimizerConfig};
use fq_optimize::{build_optimizer, CostModel};
use fq_parse::parse_with_catalog;
use fq_physical::single_source::SingleSourcePushdown;
use fq_physical::PhysicalPlanner;
use fq_plan::expr::{BinaryOpType, ColumnRef, Expr, LiteralValue};
use fq_plan::logical::{
    Aggregate, Join, JoinType, LogicalPlan, Projection, Scan, SetOpKind, SetOperation,
};
use fq_plan::physical::{PhysicalPlan, PhysicalRemoteQuery};

// --------------------------------------------------------------------------
// Catalog + mock datasource (mirrors tests/plan.rs)
// --------------------------------------------------------------------------

/// A metadata-only source with configurable capabilities.
struct MockSource {
    name: String,
    capabilities: Vec<DataSourceCapability>,
}

impl DataSource for MockSource {
    fn name(&self) -> &str {
        &self.name
    }
    fn capabilities(&self) -> Vec<DataSourceCapability> {
        self.capabilities.clone()
    }
    fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
        Ok(vec![])
    }
    fn list_tables(&self, _schema: &str) -> Result<Vec<String>, CatalogError> {
        Ok(vec![])
    }
    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata, CatalogError> {
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
        _table: &str,
        _columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        Ok(None)
    }
    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        fq_catalog::datasource::map_native_type_default(type_str)
    }
}

/// A two-source catalog: `pg.public` (orders, customer) and `duck.main`
/// (lineitem). Both support JOINS.
fn catalog() -> Arc<Catalog> {
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("cust_id", DataType::Integer, true),
            Column::new("region", DataType::Varchar, true),
            Column::new("amount", DataType::Double, false),
        ],
    );
    let customer = Table::new(
        "customer",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("nation", DataType::Varchar, true),
        ],
    );
    let lineitem = Table::new(
        "lineitem",
        vec![
            Column::new("l_orderkey", DataType::Integer, false),
            Column::new("l_quantity", DataType::Double, false),
        ],
    );
    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![orders, customer]),
    );
    catalog.insert_schema(
        "duck",
        "main",
        Schema::with_tables("main", "duck", vec![lineitem]),
    );
    catalog.register_datasource(Arc::new(MockSource {
        name: "pg".to_string(),
        capabilities: vec![DataSourceCapability::Joins],
    }));
    catalog.register_datasource(Arc::new(MockSource {
        name: "duck".to_string(),
        capabilities: vec![DataSourceCapability::Joins],
    }));
    Arc::new(catalog)
}

// --------------------------------------------------------------------------
// hand-built builders
// --------------------------------------------------------------------------

/// A qualified column reference of the given type.
fn typed_col(table: &str, name: &str, data_type: DataType) -> Expr {
    Expr::Column(ColumnRef::new(
        Some(table.to_string()),
        name,
        Some(data_type),
    ))
}

/// A qualified integer column reference.
fn col(table: &str, name: &str) -> Expr {
    typed_col(table, name, DataType::Integer)
}

/// A base scan aliased to its table name.
fn scan(datasource: &str, schema: &str, table: &str, columns: &[&str]) -> Scan {
    let mut scan = Scan::new(
        datasource,
        schema,
        table,
        columns.iter().map(|c| (*c).to_string()).collect(),
    );
    scan.alias = Some(table.to_string());
    scan
}

/// `left = right`.
fn eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// A single-column projection selecting `expr AS alias`.
fn project(input: LogicalPlan, expr: Expr, alias: &str) -> LogicalPlan {
    LogicalPlan::Projection(Projection {
        input: Box::new(input),
        expressions: vec![expr],
        aliases: vec![alias.to_string()],
        distinct: false,
        distinct_on: None,
    })
}

/// A join of two nodes with an ON condition.
fn join(
    left: LogicalPlan,
    right: LogicalPlan,
    join_type: JoinType,
    condition: Expr,
) -> LogicalPlan {
    LogicalPlan::Join(Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        condition: Some(condition),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    })
}

/// Build a pushdown pass over the catalog and try to absorb a subtree.
fn try_build(catalog: &Arc<Catalog>, node: &LogicalPlan) -> Option<PhysicalRemoteQuery> {
    let pushdown = SingleSourcePushdown::new(Arc::clone(catalog));
    pushdown
        .try_build(node)
        .expect("try_build never raises here")
}

// --------------------------------------------------------------------------
// end-to-end pipeline driver
// --------------------------------------------------------------------------

fn optimize(catalog: &Catalog, sql: &str) -> LogicalPlan {
    let parsed = parse_with_catalog(sql, catalog).expect("parse");
    let bound = fq_bind::bind(catalog, parsed).expect("bind");
    let decorrelated = fq_decorrelate::decorrelate(bound).expect("decorrelate");
    let cost_model = CostModel::new(CostConfig::default(), None);
    let optimizer = build_optimizer(&OptimizerConfig::default(), cost_model);
    optimizer.optimize(decorrelated).expect("optimize")
}

fn plan_sql(catalog: &Arc<Catalog>, sql: &str) -> PhysicalPlan {
    let logical = optimize(catalog, sql);
    let mut planner = PhysicalPlanner::new(Arc::clone(catalog), None);
    planner.plan(&logical).expect("plan")
}

/// Every remote query in a physical tree.
fn remote_queries(plan: &PhysicalPlan) -> Vec<&PhysicalRemoteQuery> {
    let mut out = Vec::new();
    collect_remotes(plan, &mut out);
    out
}

fn collect_remotes<'a>(plan: &'a PhysicalPlan, out: &mut Vec<&'a PhysicalRemoteQuery>) {
    if let PhysicalPlan::RemoteQuery(remote) = plan {
        out.push(remote);
    }
    for child in plan.children() {
        collect_remotes(child, out);
    }
}

// ==========================================================================
// absorb_join: same-source inner join renders one flat SELECT
// ==========================================================================

#[test]
fn same_source_inner_join_renders_one_select() {
    let catalog = catalog();
    let node = project(
        join(
            LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id", "cust_id"]))),
            LogicalPlan::Scan(Box::new(scan("pg", "public", "customer", &["id"]))),
            JoinType::Inner,
            eq(col("orders", "cust_id"), col("customer", "id")),
        ),
        col("orders", "id"),
        "id",
    );
    let remote = try_build(&catalog, &node).expect("same-source join pushes");
    assert_eq!(
        remote.sql,
        "SELECT \"orders\".\"id\" AS \"id\" FROM \"public\".\"orders\" AS \"orders\" \
         INNER JOIN \"public\".\"customer\" AS \"customer\" \
         ON (\"orders\".\"cust_id\" = \"customer\".\"id\")"
    );
    assert_eq!(remote.datasource, "pg");
    assert_eq!(remote.output_names, vec!["id".to_string()]);
    assert_eq!(
        remote
            .column_alias_map
            .get(&(Some("orders".to_string()), "id".to_string())),
        Some(&"id".to_string())
    );
}

// ==========================================================================
// absorb_filter: a WHERE predicate on a scan is pushed into the remote query
// ==========================================================================

#[test]
fn scan_filter_is_pushed_to_where() {
    let catalog = catalog();
    let mut orders = scan("pg", "public", "orders", &["id"]);
    orders.filters = Some(Expr::BinaryOp {
        op: BinaryOpType::Gt,
        left: Box::new(col("orders", "id")),
        right: Box::new(Expr::Literal {
            value: LiteralValue::Integer(10),
            data_type: DataType::Integer,
        }),
    });
    let node = project(
        join(
            LogicalPlan::Scan(Box::new(orders)),
            LogicalPlan::Scan(Box::new(scan("pg", "public", "customer", &["id"]))),
            JoinType::Inner,
            eq(col("orders", "cust_id"), col("customer", "id")),
        ),
        col("orders", "id"),
        "id",
    );
    let remote = try_build(&catalog, &node).expect("pushes");
    assert!(
        remote.sql.contains("WHERE (\"orders\".\"id\" > 10)"),
        "the scan filter is a WHERE term: {}",
        remote.sql
    );
}

// ==========================================================================
// SEMI / ANTI -> WHERE [NOT] EXISTS (sqlglot did this for free; we build it)
// ==========================================================================

#[test]
fn semi_join_renders_where_exists() {
    let catalog = catalog();
    let node = project(
        join(
            LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id", "cust_id"]))),
            LogicalPlan::Scan(Box::new(scan("pg", "public", "customer", &["id"]))),
            JoinType::Semi,
            eq(col("orders", "cust_id"), col("customer", "id")),
        ),
        col("orders", "id"),
        "id",
    );
    let remote = try_build(&catalog, &node).expect("pushes");
    assert_eq!(
        remote.sql,
        "SELECT \"orders\".\"id\" AS \"id\" FROM \"public\".\"orders\" AS \"orders\" \
         WHERE EXISTS(SELECT 1 FROM \"public\".\"customer\" AS \"customer\" \
         WHERE (\"orders\".\"cust_id\" = \"customer\".\"id\"))"
    );
    assert!(!remote.sql.contains("SEMI"), "no SEMI keyword survives");
}

#[test]
fn anti_join_renders_where_not_exists() {
    let catalog = catalog();
    let node = project(
        join(
            LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id", "cust_id"]))),
            LogicalPlan::Scan(Box::new(scan("pg", "public", "customer", &["id"]))),
            JoinType::Anti,
            eq(col("orders", "cust_id"), col("customer", "id")),
        ),
        col("orders", "id"),
        "id",
    );
    let remote = try_build(&catalog, &node).expect("pushes");
    assert!(
        remote.sql.contains("WHERE NOT EXISTS(SELECT 1 FROM"),
        "ANTI renders NOT EXISTS: {}",
        remote.sql
    );
}

// ==========================================================================
// nullable-right filter rides the JOIN CONDITION (the q13 differential)
// ==========================================================================

#[test]
fn left_join_filtered_right_rides_the_on_condition() {
    let catalog = catalog();
    let mut customer = scan("pg", "public", "customer", &["id", "nation"]);
    customer.filters = Some(Expr::BinaryOp {
        op: BinaryOpType::Neq,
        left: Box::new(typed_col("customer", "nation", DataType::Varchar)),
        right: Box::new(Expr::Literal {
            value: LiteralValue::String("x".to_string()),
            data_type: DataType::Varchar,
        }),
    });
    let node = project(
        join(
            LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id", "cust_id"]))),
            LogicalPlan::Scan(Box::new(customer)),
            JoinType::Left,
            eq(col("orders", "cust_id"), col("customer", "id")),
        ),
        col("orders", "id"),
        "id",
    );
    let remote = try_build(&catalog, &node).expect("pushes");
    assert!(remote.sql.contains("LEFT JOIN"), "{}", remote.sql);
    // The right filter is ANDed onto the ON, NOT hoisted to a global WHERE (which
    // would drop the preserved side's null-extended rows).
    assert!(
        remote
            .sql
            .contains("AND ((\"customer\".\"nation\" != 'x'))"),
        "the filter rides the ON: {}",
        remote.sql
    );
    assert!(
        !remote.sql.contains("WHERE"),
        "no global WHERE: {}",
        remote.sql
    );
}

#[test]
fn full_join_filtered_right_declines() {
    // FULL with a filtered right has no ON that can carry the conjunct without
    // resurrecting filtered-out right-only rows -> decline (fall back to local).
    let catalog = catalog();
    let mut customer = scan("pg", "public", "customer", &["id", "nation"]);
    customer.filters = Some(Expr::BinaryOp {
        op: BinaryOpType::Neq,
        left: Box::new(typed_col("customer", "nation", DataType::Varchar)),
        right: Box::new(Expr::Literal {
            value: LiteralValue::String("x".to_string()),
            data_type: DataType::Varchar,
        }),
    });
    let node = project(
        join(
            LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id", "cust_id"]))),
            LogicalPlan::Scan(Box::new(customer)),
            JoinType::Full,
            eq(col("orders", "cust_id"), col("customer", "id")),
        ),
        col("orders", "id"),
        "id",
    );
    assert!(try_build(&catalog, &node).is_none());
}

// ==========================================================================
// star expansion over a join (bare SELECT * -> explicit unique-aliased columns)
// ==========================================================================

#[test]
fn star_over_a_join_expands_to_unique_aliased_columns() {
    let catalog = catalog();
    // A bare join, no projection: the SELECT list is expanded from the base scans.
    let node = join(
        LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id", "cust_id"]))),
        LogicalPlan::Scan(Box::new(scan(
            "pg",
            "public",
            "customer",
            &["id", "nation"],
        ))),
        JoinType::Inner,
        eq(col("orders", "cust_id"), col("customer", "id")),
    );
    let remote = try_build(&catalog, &node).expect("bare same-source join pushes");
    // Both `id` columns are listed; the second is deduped to `id_1`.
    assert!(
        remote.sql.contains("\"orders\".\"id\" AS \"id\""),
        "{}",
        remote.sql
    );
    assert!(
        remote.sql.contains("\"customer\".\"id\" AS \"id_1\""),
        "collision deduped: {}",
        remote.sql
    );
    assert_eq!(
        remote.output_names,
        vec![
            "id".to_string(),
            "cust_id".to_string(),
            "id_1".to_string(),
            "nation".to_string(),
        ]
    );
}

// ==========================================================================
// DISTINCT ON always pushes (only correct at the source)
// ==========================================================================

#[test]
fn distinct_on_pushes_even_for_a_single_table() {
    let catalog = catalog();
    let node = LogicalPlan::Projection(Projection {
        input: Box::new(LogicalPlan::Scan(Box::new(scan(
            "pg",
            "public",
            "orders",
            &["id", "region"],
        )))),
        expressions: vec![col("orders", "id")],
        aliases: vec!["id".to_string()],
        distinct: false,
        distinct_on: Some(vec![typed_col("orders", "region", DataType::Varchar)]),
    });
    let remote = try_build(&catalog, &node).expect("DISTINCT ON pushes");
    assert_eq!(
        remote.sql,
        "SELECT DISTINCT ON (\"orders\".\"region\") \"orders\".\"id\" AS \"id\" \
         FROM \"public\".\"orders\" AS \"orders\""
    );
}

// ==========================================================================
// estimate + NDV annotations on the finished remote query
// ==========================================================================

#[test]
fn estimates_and_ndv_are_read_from_the_stamped_scans() {
    let catalog = catalog();
    let mut orders = scan("pg", "public", "orders", &["id", "cust_id"]);
    orders.estimated_rows = Some(1000);
    let mut ndv = BTreeMap::new();
    ndv.insert("cust_id".to_string(), 50);
    orders.column_ndv = Some(ndv);
    let mut customer = scan("pg", "public", "customer", &["id"]);
    customer.estimated_rows = Some(200);
    let inner_join = LogicalPlan::Join(Join {
        left: Box::new(LogicalPlan::Scan(Box::new(orders))),
        right: Box::new(LogicalPlan::Scan(Box::new(customer))),
        join_type: JoinType::Inner,
        condition: Some(eq(col("orders", "cust_id"), col("customer", "id"))),
        natural: false,
        using: None,
        estimated_rows: Some(300),
        estimate_defaults: None,
    });
    let node = project(inner_join, col("orders", "cust_id"), "ck");
    let remote = try_build(&catalog, &node).expect("pushes");
    // root_estimate = the LARGEST base scan (the reduction FLOOR), not the join out.
    assert_eq!(remote.estimated_rows, Some(1000));
    // output_estimate descends the projection to the join's own estimate.
    assert_eq!(remote.output_estimated_rows, Some(300));
    // The NDV is keyed by the OUTPUT name carrying cust_id (`ck`).
    let column_ndv = remote.column_ndv.expect("cust_id carries an NDV");
    assert_eq!(column_ndv.get("ck"), Some(&50));
}

// ==========================================================================
// declines: cross-source, plain single-table, top-level set operation
// ==========================================================================

#[test]
fn cross_source_join_declines() {
    let catalog = catalog();
    let node = project(
        join(
            LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id"]))),
            LogicalPlan::Scan(Box::new(scan("duck", "main", "lineitem", &["l_orderkey"]))),
            JoinType::Inner,
            eq(col("orders", "id"), col("lineitem", "l_orderkey")),
        ),
        col("orders", "id"),
        "id",
    );
    assert!(try_build(&catalog, &node).is_none());
}

#[test]
fn plain_single_table_projection_declines() {
    // A bare column projection over one scan is left to the scan-pushdown path.
    let catalog = catalog();
    let node = project(
        LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id"]))),
        col("orders", "id"),
        "id",
    );
    assert!(try_build(&catalog, &node).is_none());
}

#[test]
fn top_level_set_operation_declines() {
    let catalog = catalog();
    let node = LogicalPlan::SetOperation(SetOperation {
        left: Box::new(project(
            LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id"]))),
            col("orders", "id"),
            "id",
        )),
        right: Box::new(project(
            LogicalPlan::Scan(Box::new(scan("pg", "public", "customer", &["id"]))),
            col("customer", "id"),
            "id",
        )),
        kind: SetOpKind::Union,
        distinct: true,
    });
    assert!(try_build(&catalog, &node).is_none());
}

#[test]
fn source_without_join_capability_declines() {
    // pg lacks JOINS: `finish` refuses (the planner keeps it local).
    let orders = Table::new("orders", vec![Column::new("id", DataType::Integer, false)]);
    let customer = Table::new(
        "customer",
        vec![Column::new("id", DataType::Integer, false)],
    );
    let mut cat = Catalog::new();
    cat.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![orders, customer]),
    );
    cat.register_datasource(Arc::new(MockSource {
        name: "pg".to_string(),
        capabilities: vec![],
    }));
    let catalog = Arc::new(cat);
    let node = project(
        join(
            LogicalPlan::Scan(Box::new(scan("pg", "public", "orders", &["id", "cust_id"]))),
            LogicalPlan::Scan(Box::new(scan("pg", "public", "customer", &["id"]))),
            JoinType::Inner,
            eq(col("orders", "cust_id"), col("customer", "id")),
        ),
        col("orders", "id"),
        "id",
    );
    assert!(try_build(&catalog, &node).is_none());
}

// ==========================================================================
// aggregate over a same-source join folds into one grouped remote query
// ==========================================================================

#[test]
fn aggregate_over_same_source_join_pushes_with_group_by() {
    let catalog = catalog();
    // Hand-built so the shape is exact: Aggregate(GROUP BY region, count(*)) over a
    // same-source join.
    let inner_join = join(
        LogicalPlan::Scan(Box::new(scan(
            "pg",
            "public",
            "orders",
            &["id", "cust_id", "region"],
        ))),
        LogicalPlan::Scan(Box::new(scan("pg", "public", "customer", &["id"]))),
        JoinType::Inner,
        eq(col("orders", "cust_id"), col("customer", "id")),
    );
    let count_star = Expr::FunctionCall {
        function_name: "COUNT".to_string(),
        args: vec![Expr::Literal {
            value: LiteralValue::Integer(1),
            data_type: DataType::Integer,
        }],
        is_aggregate: true,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    };
    let node = LogicalPlan::Aggregate(Aggregate {
        input: Box::new(inner_join),
        group_by: vec![typed_col("orders", "region", DataType::Varchar)],
        aggregates: vec![typed_col("orders", "region", DataType::Varchar), count_star],
        output_names: vec!["region".to_string(), "cnt".to_string()],
        grouping_sets: None,
    });
    let remote = try_build(&catalog, &node).expect("aggregate over same-source join pushes");
    assert!(remote.sql.contains("COUNT(1) AS \"cnt\""), "{}", remote.sql);
    assert!(
        remote.sql.contains("GROUP BY \"orders\".\"region\""),
        "{}",
        remote.sql
    );
    assert!(remote.sql.contains("INNER JOIN"), "{}", remote.sql);
}

// ==========================================================================
// end-to-end: a same-source CTE pushes whole as a WITH statement
// ==========================================================================

#[test]
fn same_source_cte_pushes_as_one_with_statement() {
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "WITH recent AS (SELECT orders.id AS id FROM pg.public.orders) \
         SELECT r.id FROM recent AS r JOIN pg.public.customer ON r.id = customer.id",
    );
    let remotes = remote_queries(&physical);
    assert_eq!(remotes.len(), 1, "the whole CTE query is one remote query");
    let sql = &remotes[0].sql;
    assert!(sql.starts_with("WITH recent AS ("), "leading WITH: {sql}");
    assert!(sql.contains("JOIN"), "the child joins the CTE: {sql}");
}

// ==========================================================================
// end-to-end: a CTE whose body is a UNION pushes as one WITH statement, with the
// set-op body rendered UNWRAPPED (render_cte_set_body + render_branch + set_op_sql)
// ==========================================================================

#[test]
fn cte_with_a_union_body_pushes_whole() {
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "WITH u AS ( \
           SELECT orders.id AS id FROM pg.public.orders \
           UNION ALL \
           SELECT customer.id AS id FROM pg.public.customer \
         ) SELECT u.id FROM u",
    );
    let remotes = remote_queries(&physical);
    assert_eq!(
        remotes.len(),
        1,
        "the whole CTE-union query is one remote query"
    );
    let sql = &remotes[0].sql;
    assert!(sql.starts_with("WITH u AS ("), "leading WITH: {sql}");
    assert!(
        sql.contains("UNION ALL"),
        "the CTE body unions the branches: {sql}"
    );
    assert!(
        sql.contains("FROM u"),
        "the child reads the CTE by name: {sql}"
    );
}
