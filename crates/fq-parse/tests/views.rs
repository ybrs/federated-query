//! Non-materialized view expansion: an unqualified FROM reference to a
//! registered view becomes its defining query wrapped as a derived table, so
//! every later pass treats it like an inline subquery. Covers the rename list,
//! aliasing, star expansion, and the two loud rejections (definition cycle,
//! rename arity).

use fq_catalog::{Catalog, Column, Schema, Table, ViewDef};
use fq_common::DataType;
use fq_parse::{parse_with_catalog, ParseError};
use fq_plan::LogicalPlan;

fn catalog_with_view(name: &str, sql: &str, aliases: Option<Vec<String>>) -> Catalog {
    let users = Table::new("users", vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Varchar, true),
    ]);
    let mut c = Catalog::new();
    c.insert_schema("pg", "public", Schema::with_tables("public", "pg", vec![users]));
    c.register_view(name, ViewDef { sql: sql.into(), column_aliases: aliases });
    c
}

#[test]
fn view_expands_in_from() {
    let c = catalog_with_view("uv", "SELECT id, name FROM pg.public.users", None);
    let plan = parse_with_catalog("SELECT id FROM uv", &c).expect("parse");
    let LogicalPlan::Projection(p) = &plan else { panic!("expected Projection") };
    let LogicalPlan::SubqueryScan(sq) = p.input.as_ref() else {
        panic!("expected SubqueryScan, got {:?}", p.input)
    };
    assert_eq!(sq.alias, "uv");
}

#[test]
fn view_star_expands() {
    let c = catalog_with_view("uv", "SELECT id, name FROM pg.public.users", None);
    let plan = parse_with_catalog("SELECT * FROM uv", &c).expect("parse");
    let LogicalPlan::Projection(p) = &plan else { panic!("expected Projection") };
    assert_eq!(p.aliases, vec!["id", "name"]);
}

#[test]
fn view_column_rename_applies() {
    let c = catalog_with_view("uv", "SELECT id, name FROM pg.public.users",
        Some(vec!["a".into(), "b".into()]));
    let plan = parse_with_catalog("SELECT a FROM uv", &c).expect("parse");
    let LogicalPlan::Projection(p) = &plan else { panic!("expected Projection") };
    let LogicalPlan::SubqueryScan(sq) = p.input.as_ref() else { panic!("expected SubqueryScan") };
    assert_eq!(sq.column_names, Some(vec!["a".to_string(), "b".to_string()]));
}

#[test]
fn view_alias_in_from() {
    let c = catalog_with_view("uv", "SELECT id, name FROM pg.public.users", None);
    let plan = parse_with_catalog("SELECT v.id FROM uv v", &c).expect("parse");
    let LogicalPlan::Projection(p) = &plan else { panic!("expected Projection") };
    let LogicalPlan::SubqueryScan(sq) = p.input.as_ref() else { panic!("expected SubqueryScan") };
    assert_eq!(sq.alias, "v");
}

#[test]
fn a_cte_of_the_same_name_shadows_the_view() {
    // SQL scoping: the WITH name is in scope, so `uv` is the CTE here and the
    // view is not expanded. The reference becomes a CteRef, not a SubqueryScan.
    let c = catalog_with_view("uv", "SELECT id, name FROM pg.public.users", None);
    let plan = parse_with_catalog("WITH uv AS (SELECT id FROM pg.public.users) SELECT id FROM uv", &c)
        .expect("parse");
    let LogicalPlan::Cte(cte) = &plan else { panic!("expected Cte, got {plan:?}") };
    let LogicalPlan::Projection(p) = cte.child.as_ref() else { panic!("expected Projection") };
    assert!(
        matches!(p.input.as_ref(), LogicalPlan::CteRef(_)),
        "the CTE shadows the view, got {:?}",
        p.input
    );
}

#[test]
fn a_qualified_name_is_never_a_view() {
    // Views are unqualified names. `pg.public.uv` names a table in a datasource
    // schema, so it stays a Scan of that (nonexistent) table for the binder to
    // reject - it never expands into the view sharing the last component.
    let c = catalog_with_view("uv", "SELECT id, name FROM pg.public.users", None);
    let plan = parse_with_catalog("SELECT id FROM pg.public.uv", &c).expect("parse");
    let LogicalPlan::Projection(p) = &plan else { panic!("expected Projection") };
    let LogicalPlan::Scan(scan) = p.input.as_ref() else {
        panic!("a qualified name must stay a Scan, got {:?}", p.input)
    };
    assert_eq!(scan.table_name, "uv");
}

#[test]
fn self_referencing_view_raises() {
    let c = catalog_with_view("uv", "SELECT id FROM uv", None);
    let err = parse_with_catalog("SELECT id FROM uv", &c).unwrap_err();
    assert!(matches!(err, ParseError::Unsupported(_)));
}

#[test]
fn view_rename_arity_mismatch_raises() {
    let c = catalog_with_view("uv", "SELECT id, name FROM pg.public.users", Some(vec!["only".into()]));
    let err = parse_with_catalog("SELECT only FROM uv", &c).unwrap_err();
    assert!(matches!(err, ParseError::Parse(_)));
}
