//! Lower list expansion to a coordinator operator with unique physical columns.

use std::collections::HashSet;

use fq_plan::physical::PhysicalUnnest;
use fq_plan::{PhysicalPlan, Unnest};

use super::PhysicalPlanner;
use crate::error::PhysicalError;

impl PhysicalPlanner {
    /// Plan the parent and assign names that cannot collide with its columns.
    pub(crate) fn plan_unnest(&mut self, node: &Unnest) -> Result<PhysicalPlan, PhysicalError> {
        let input = self.plan_node(&node.input)?;
        let width = node.arguments.len() + usize::from(node.ordinality.is_some());
        let physical_names = output_names(&input, width);
        // This is a new physical operator lowered from a logical expansion.
        // All fields preserve arguments, aliases, ordinality, and row-preservation semantics.
        Ok(PhysicalPlan::Unnest(PhysicalUnnest {
            input: Box::new(input),
            arguments: node.arguments.clone(),
            output_names: node.output_names.clone(),
            physical_names,
            alias: node.alias.clone(),
            outer: node.outer,
            ordinality: node.ordinality.clone(),
            retain_input: node.retain_input,
            predicate: node.predicate.clone(),
        }))
    }
}

/// Generate output names outside the parent operator's physical namespace.
fn output_names(input: &PhysicalPlan, width: usize) -> Vec<String> {
    let mut used: HashSet<String> = fq_plan::output_column_names(input).into_iter().collect();
    let mut names = Vec::with_capacity(width);
    for index in 0..width {
        let name = (0..=used.len())
            .map(|suffix| format!("__fedq_unnest_{index}_{suffix}"))
            .find(|name| !used.contains(name))
            .expect("more candidates than existing columns");
        used.insert(name.clone());
        names.push(name);
    }
    names
}
