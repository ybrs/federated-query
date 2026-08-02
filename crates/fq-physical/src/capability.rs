//! The capability gate: the ONE place that answers whether a source may be sent
//! a construct.
//!
//! Every pushdown decision in this crate - the single-source island, the remote
//! join, the remote set operation, the clause folds in the planner, the
//! dim-shipping target - routes through `declares`, so the rule has exactly one
//! implementation rather than one per call site. The answer defaults to DENY: a
//! source that does not name a capability does not receive the construct that
//! needs it, and the pass declines structurally so the subtree runs in the
//! coordinator instead.

use fq_catalog::datasource::{DataSource, DataSourceCapability};
use fq_catalog::Catalog;
use fq_plan::physical::PhysicalPlan;

/// Whether a resolved source declares `capability`. An UNRESOLVED source (`None`)
/// declares nothing: it cannot be asked, so it is not trusted with the construct.
pub(crate) fn declares(source: Option<&dyn DataSource>, capability: DataSourceCapability) -> bool {
    match source {
        Some(source) => source.supports_capability(capability),
        None => false,
    }
}

/// Whether the source registered under `datasource` declares `capability`. The
/// catalog-lookup adapter over `declares`; a name no source answers to declares
/// nothing.
pub(crate) fn source_declares(
    catalog: &Catalog,
    datasource: &str,
    capability: DataSourceCapability,
) -> bool {
    declares(catalog.get_datasource(datasource).as_deref(), capability)
}

/// The source a lowered node's SQL runs on, or `None` when the node is a
/// coordinator operator with no single source. A remote join's candidacy placed
/// both of its sides on ONE source, so its left side names that source.
pub(crate) fn pushed_datasource(node: &PhysicalPlan) -> Option<&str> {
    match node {
        PhysicalPlan::Scan(scan) => Some(&scan.datasource),
        PhysicalPlan::RemoteQuery(remote) => Some(&remote.datasource),
        PhysicalPlan::RemoteSetOp(remote) => Some(&remote.datasource),
        PhysicalPlan::RemoteJoin(remote) => pushed_datasource(&remote.left),
        _ => None,
    }
}
