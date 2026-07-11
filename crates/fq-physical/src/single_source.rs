//! Same-source pushdown: tries to absorb a whole single-source subtree into ONE
//! remote SQL query. Ported in a later milestone (SPEC-single-source.md); here it
//! is stubbed to decline every subtree, so the planner lowers single-source
//! subtrees to LOCAL nodes.

use std::collections::HashMap;
use std::sync::Arc;

use fq_catalog::Catalog;
use fq_emit::EmitError;
use fq_plan::logical::{LogicalPlan, Scan};
use fq_plan::physical::PhysicalRemoteQuery;

/// Identity map from a base scan (keyed by its pointer, matching Python
/// `id(scan)`) to the merge-engine relation name it renders as. Valid only while
/// the borrowed plan tree the keys point into is alive; the caller builds it from
/// the SAME `&Scan` nodes it hands to `render_correlated_sql`.
pub type ScanNames = HashMap<*const Scan, String>;

/// Same-source pushdown pass. Holds the catalog so it can resolve tables and
/// datasource capabilities when it renders (later milestone).
pub struct SingleSourcePushdown {
    // M2: the render/absorb logic reads the catalog to resolve tables + caps. The
    // stub declines before touching it, so it is held-but-unread until then.
    #[allow(dead_code)]
    catalog: Arc<Catalog>,
}

impl SingleSourcePushdown {
    /// Build a pushdown pass over a shared catalog.
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    /// Try to collapse a subtree living on one source into a single remote query.
    /// `None` declines (the planner falls back to local lowering).
    // M2: implemented in a later milestone (SPEC-single-source.md).
    pub fn try_build(&self, _node: &LogicalPlan) -> Result<Option<PhysicalRemoteQuery>, EmitError> {
        Ok(None)
    }

    /// Render a single-source subtree to canonical Postgres SQL with base scans
    /// named for the merge engine. Used by cross-source LATERAL and recursive CTE.
    /// `None` when the subtree is not renderable.
    // M2: implemented in a later milestone (SPEC-single-source.md).
    pub fn render_correlated_sql(
        &self,
        _node: &LogicalPlan,
        _scan_names: &ScanNames,
    ) -> Result<Option<String>, EmitError> {
        Ok(None)
    }
}

/// Whether two datasource names identify the same source. A None name matches
/// nothing (including None). The single definition both the pushdown builder and
/// the physical planner use.
pub fn same_source(left: Option<&str>, right: Option<&str>) -> bool {
    matches!((left, right), (Some(l), Some(r)) if l == r)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_source_matches_equal_names_only() {
        assert!(same_source(Some("pg"), Some("pg")));
        assert!(!same_source(Some("pg"), Some("duck")));
        assert!(!same_source(Some("pg"), None));
        assert!(!same_source(None, None));
    }
}
