//! The bind-time plan-depth guard: past `fq_common::MAX_PLAN_DEPTH` a query
//! raises here instead of overflowing the stack in a later pass.
//!
//! Every pass after the binder recurses once per plan level. Measured in a
//! debug build on a `QUERY_THREAD_STACK_BYTES` thread, a 402-level join chain
//! plans and a 427-level one dies by SIGSEGV carrying no message at all - so
//! the depth a query reaches is refused where it can still be reported.

use fq_bind::{bind, BindError};
use fq_catalog::{Catalog, Column, Schema, Table};
use fq_common::{DataType, MAX_PLAN_DEPTH};
use fq_parse::parse_with_catalog;
use fq_plan::LogicalPlan;

/// A catalog with one table, pg.public.users(id INT).
fn catalog() -> Catalog {
    let users = Table::new("users", vec![Column::new("id", DataType::Integer, false)]);
    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![users]),
    );
    catalog
}

/// Parse + bind one statement against that catalog.
fn bind_sql(sql: &str) -> Result<LogicalPlan, BindError> {
    let catalog = catalog();
    let plan = parse_with_catalog(sql, &catalog).expect("parse");
    bind(&catalog, plan)
}

/// A chain join over `aliases` copies of users, each joined to the previous.
/// Its plan is one level per join, so its depth tracks `aliases` directly.
fn chain_sql(aliases: usize) -> String {
    let mut parts = vec!["pg.public.users a0".to_string()];
    for index in 1..aliases {
        let previous = index - 1;
        parts.push(format!(
            "JOIN pg.public.users a{index} ON a{previous}.id = a{index}.id"
        ));
    }
    format!("SELECT a0.id FROM {}", parts.join(" "))
}

/// `levels` nested `id IN (SELECT ...)` predicates: depth carried entirely by
/// subquery EXPRESSIONS rather than by child plans.
fn nested_in_sql(levels: usize) -> String {
    let mut sql = "SELECT id FROM pg.public.users".to_string();
    for _ in 0..levels {
        sql = format!("SELECT id FROM pg.public.users WHERE id IN ({sql})");
    }
    sql
}

/// Run `probe` on a thread with the stack every query thread gets. Binding a
/// plan recurses once per level, and cargo's 2MiB test threads overflow inside
/// the binder around 195 levels in a debug build - below the limit this file
/// asserts, and far below what the engine's own threads carry.
fn on_a_query_thread(probe: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(fq_common::QUERY_THREAD_STACK_BYTES)
        .spawn(probe)
        .expect("probe thread")
        .join()
        .expect("probe thread panicked");
}

/// The measured depth of a statement's parsed plan.
fn parsed_depth(sql: &str) -> usize {
    let catalog = catalog();
    parse_with_catalog(sql, &catalog)
        .expect("parse")
        .max_depth()
}

#[test]
fn a_query_just_under_the_limit_binds() {
    on_a_query_thread(|| {
        let sql = chain_sql(MAX_PLAN_DEPTH - 6);
        assert!(parsed_depth(&sql) < MAX_PLAN_DEPTH);
        let bound = bind_sql(&sql).expect("a plan inside the depth limit binds");
        assert!(bound.max_depth() <= MAX_PLAN_DEPTH);
    });
}

#[test]
fn a_query_past_the_limit_raises_naming_its_depth() {
    on_a_query_thread(|| {
        let sql = chain_sql(MAX_PLAN_DEPTH + 10);
        let depth = parsed_depth(&sql);
        assert!(depth > MAX_PLAN_DEPTH);
        let error = bind_sql(&sql).expect_err("a plan past the depth limit must raise");
        assert_eq!(
            error,
            BindError::PlanTooDeep {
                depth,
                limit: MAX_PLAN_DEPTH
            }
        );
        assert_eq!(
            error.to_string(),
            format!("query nests {depth} plan levels; the engine's limit is {MAX_PLAN_DEPTH}")
        );
    });
}

#[test]
fn depth_reached_through_subquery_expressions_is_refused_too() {
    // Each level adds plan nodes only through the IN subquery's expression, so
    // a guard that stopped at the subquery boundary would measure a depth of a
    // handful here and let the query through to the pass that overflows.
    on_a_query_thread(|| {
        let sql = nested_in_sql(MAX_PLAN_DEPTH);
        assert!(parsed_depth(&sql) > MAX_PLAN_DEPTH);
        let error = bind_sql(&sql).expect_err("subquery-carried depth must raise");
        assert!(matches!(error, BindError::PlanTooDeep { .. }), "{error}");
    });
}
