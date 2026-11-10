//! Relation namespace ownership and identity-scoped grant validation.

use super::*;
use fq_common::security::{GrantSubject, ObjectKey};

/// Allocate a fresh local catalog without any server identity or result store.
fn catalog() -> DatabaseCatalog {
    let path = std::env::temp_dir().join(format!("fq_relations_{}.sqlite", RelationId::new()));
    DatabaseCatalog::open(&path.to_string_lossy()).expect("local catalog")
}

/// Publish a view identity for namespace and privilege tests.
fn view(catalog: &DatabaseCatalog, name: &str) -> RelationRecord {
    let relation = catalog
        .reserve_relation(name, RelationKind::View, "root")
        .expect("reserve");
    catalog.publish_relation(relation.id).expect("publish");
    catalog.find_relation(name).expect("lookup").expect("view")
}

/// Views and materialized views share one reservation namespace.
#[test]
fn relation_names_cannot_change_kind_through_replace() {
    let catalog = catalog();
    let target = view(&catalog, "build");
    assert!(catalog
        .reserve_relation("build", RelationKind::MaterializedView, "root")
        .is_err());
    let reservation = catalog
        .reserve_relation("pending", RelationKind::View, "root")
        .expect("reserve");
    catalog
        .drop_relation(reservation.id)
        .expect("abandon reservation");
    catalog.drop_relation(target.id).expect("drop");
    let recreated = view(&catalog, "build");
    assert_ne!(target.id, recreated.id);
}

/// Structured grants must reference an existing relation of the same real category.
#[test]
fn grant_kind_cannot_impersonate_another_relation() {
    let catalog = catalog();
    let target = view(&catalog, "build");
    assert!(catalog
        .grant(
            GrantSubject::Public,
            &ObjectKey::MaterializedView {
                relation: target.id
            },
            "root"
        )
        .is_err());
    assert!(catalog
        .grant(
            GrantSubject::Public,
            &ObjectKey::View {
                relation: RelationId::new()
            },
            "root"
        )
        .is_err());
    catalog
        .grant(
            GrantSubject::Public,
            &ObjectKey::View {
                relation: target.id,
            },
            "root",
        )
        .expect("view grant");
}
