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
