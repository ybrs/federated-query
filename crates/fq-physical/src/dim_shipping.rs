//! Dim shipping: collapses a shippable cross-source subtree into one island by
//! shipping the foreign (dimension) relations INTO the local (fact) source as
//! temp tables. Ported in a later milestone (SPEC-dim-shipping.md); here it is
//! stubbed to decline every subtree.
//!
//! The rule is STATELESS: every collaborator (cost model, catalog, single-source
//! pushdown, ship-name counter, sub-planner) is reached through the
//! `&mut PhysicalPlanner` handed to `try_ship`, so no aliasing back-reference is
//! held (Python's `DimShipping(self)` cycle is inverted into this parameter).

use fq_plan::logical::LogicalPlan;
use fq_plan::physical::PhysicalPlan;

use crate::error::PhysicalError;
use crate::planner::PhysicalPlanner;

/// Dim-shipping rule. A unit value; all state lives on the planner it is handed.
pub struct DimShipping;

impl DimShipping {
    /// Try to collapse `node` into one shipped island, or decline. Err only on a
    /// genuine cost-model failure, never as a decline.
    // M3: implemented in a later milestone (SPEC-dim-shipping.md).
    pub fn try_ship(
        &self,
        _planner: &mut PhysicalPlanner,
        _node: &LogicalPlan,
    ) -> Result<Option<PhysicalPlan>, PhysicalError> {
        Ok(None)
    }
}
