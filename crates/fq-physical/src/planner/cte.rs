//! CTE lowering: the cross-source producer registry (materialize once, share by
//! name) and the recursive cross-source CTE merge query. Ports `_plan_cte` and
//! its helpers. Same-source CTEs were already pushed whole by single-source
//! pushdown before this point.

use std::collections::{BTreeMap, HashMap};

use fq_plan::logical::{Cte, CteRef, LogicalPlan, Scan};
use fq_plan::physical::{PhysicalCte, PhysicalCteMergeQuery, PhysicalCteScan, PhysicalPlan};

use super::PhysicalPlanner;
use crate::error::PhysicalError;

impl PhysicalPlanner {
    /// Plan a CTE that single-source pushdown declined (it spans sources): a
    /// recursive one runs wholly in the merge engine; a non-recursive one
    /// materializes its body once and feeds every reference. Ports `_plan_cte`.
    pub(super) fn plan_cte(&mut self, cte: &Cte) -> Result<PhysicalPlan, PhysicalError> {
        if cte.recursive {
            self.plan_recursive_cte_merge(cte)
        } else {
            self.plan_cross_source_cte(cte)
        }
    }

    /// Materialize the body once; register the producer; plan the child with the
    /// name in scope; restore the registry (on BOTH the Ok and Err paths, so a
    /// stale producer never leaks). Ports `_plan_cross_source_cte`.
    fn plan_cross_source_cte(&mut self, cte: &Cte) -> Result<PhysicalPlan, PhysicalError> {
        let body = self.plan_node(&cte.cte_plan)?;
        let producer = PhysicalCte {
            name: cte.name.clone(),
            body: Box::new(body),
            column_names: cte.column_names.clone(),
        };
        let saved = self.cte_producers.insert(cte.name.clone(), producer);
        let child = self.plan_node(&cte.child);
        match saved {
            Some(previous) => {
                self.cte_producers.insert(cte.name.clone(), previous);
            }
            None => {
                self.cte_producers.remove(&cte.name);
            }
        }
        child
    }

    /// Resolve a CTE reference to a scan over its materialized producer. Ports
    /// `_plan_cte_ref`. RAISE when the producer is not in scope.
    pub(super) fn plan_cte_ref(&mut self, node: &CteRef) -> Result<PhysicalPlan, PhysicalError> {
        let Some(producer) = self.cte_producers.get(&node.name) else {
            return Err(PhysicalError::CteNotInScope {
                name: node.name.clone(),
            });
        };
        Ok(PhysicalPlan::CteScan(PhysicalCteScan {
            producer: Box::new(PhysicalPlan::Cte(producer.clone())),
            alias: node.alias.clone(),
        }))
    }

    /// Run a cross-source recursive CTE wholly in the merge engine: every base
    /// source relation is materialized under a generated name that the rendered
    /// WITH RECURSIVE references. Ports `_plan_recursive_cte_merge`.
    fn plan_recursive_cte_merge(&mut self, cte: &Cte) -> Result<PhysicalPlan, PhysicalError> {
        let cte_as_logical = LogicalPlan::Cte(cte.clone());
        let mut scans: Vec<&Scan> = Vec::new();
        Self::collect_base_scans(&cte_as_logical, &mut scans);
        let mut scan_names: HashMap<*const Scan, String> = HashMap::new();
        let mut inputs: BTreeMap<String, PhysicalPlan> = BTreeMap::new();
        for (index, &scan) in scans.iter().enumerate() {
            let register_name = format!("cte_src_{index}");
            scan_names.insert(std::ptr::from_ref(scan), register_name.clone());
            inputs.insert(register_name, self.plan_scan_node(scan)?);
        }
        let Some(sql) = self
            .single_source()
            .render_correlated_sql(&cte_as_logical, &scan_names)?
        else {
            return Err(PhysicalError::RecursiveCteNotRenderable {
                name: cte.name.clone(),
            });
        };
        Ok(PhysicalPlan::CteMergeQuery(PhysicalCteMergeQuery {
            sql,
            inputs,
            output_names: cte_as_logical.schema(),
        }))
    }
}
