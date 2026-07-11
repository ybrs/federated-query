//! The fixpoint driver (`RuleBasedOptimizer`) and the factory (`build_optimizer`).
//! Ports `RuleBasedOptimizer.optimize` and `factory.build_optimizer` (the
//! pushdown subset).

use std::cell::RefCell;
use std::rc::Rc;

use fq_common::OptimizerConfig;
use fq_decorrelate::scope::validate_scope;
use fq_plan::logical::{Explain, LogicalPlan};

use crate::cost::CostModel;
use crate::error::OptimizeError;

use super::{
    AggregatePushdown, CTEUnionFilterPushdown, EagerAggregation, JoinOrdering, LimitPushdown,
    OptimizationRule, OrderByPushdown, PredicatePushdown, ProjectionPushdown, SemiJoinPushdown,
};

/// The hard iteration backstop: a full sweep that changes nothing breaks the
/// loop earlier, so this only bounds a pathologically oscillating rule set.
const MAX_ITERATIONS: usize = 10;

/// A rule-based query optimizer: an ordered rule stack applied in a fixpoint
/// loop. Ports `RuleBasedOptimizer`.
pub struct RuleBasedOptimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl RuleBasedOptimizer {
    /// Build an optimizer over an explicit rule stack (registration order is
    /// application order).
    pub fn new(rules: Vec<Box<dyn OptimizationRule>>) -> Self {
        Self { rules }
    }

    /// The registered rule names, in application order (for tests / EXPLAIN).
    pub fn rule_names(&self) -> Vec<&'static str> {
        let mut names = Vec::with_capacity(self.rules.len());
        for rule in &self.rules {
            names.push(rule.name());
        }
        names
    }

    /// Optimize a logical plan by applying every rule iteratively until a full
    /// sweep changes nothing (fixpoint) or the iteration backstop is hit. An
    /// `Explain` is unwrapped, its query optimized, then re-wrapped so EXPLAIN
    /// reports the plan of the optimized query. Ports `optimize`.
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        if let LogicalPlan::Explain(explain) = plan {
            let optimized = self.optimize(*explain.input)?;
            return Ok(LogicalPlan::Explain(Explain {
                input: Box::new(optimized),
                format: explain.format,
            }));
        }

        let mut current = plan;
        let mut iteration = 0;
        while iteration < MAX_ITERATIONS {
            iteration += 1;
            let mut changed = false;
            for rule in &self.rules {
                let before = current.clone();
                let after = rule.apply(current)?;
                if after != before {
                    changed = true;
                    // The loud safety net: a rule that produced a mis-scoped plan
                    // fails HERE, at the rule that built it, not downstream.
                    validate_scope(&after, &format!("after {}", rule.name()))?;
                }
                current = after;
            }
            if !changed {
                break;
            }
        }
        Ok(current)
    }
}

/// The standard pushdown rule stack, honoring the optimizer configuration. Ports
/// `build_optimizer`.
///
/// The two cost-gated rules share ONE estimator through an `Rc<RefCell<CostModel>>`
/// handle: `EagerAggregation` is ALWAYS on (it needs the cost model even when join
/// reordering is off and `JoinOrdering` is not built), and `JoinOrdering` needs the
/// same model, so a single `CostModel` is wrapped once and cloned into each. The
/// optimizer is single-threaded (a fixpoint loop over one plan), so the non-`Send`
/// `Rc`/`RefCell` is fine; the plan owner would switch to `Arc<Mutex<..>>` if a
/// `Send` bound ever appears.
pub fn build_optimizer(config: &OptimizerConfig, cost_model: CostModel) -> RuleBasedOptimizer {
    let shared = Rc::new(RefCell::new(cost_model));
    let mut rules: Vec<Box<dyn OptimizationRule>> = Vec::new();

    if config.enable_predicate_pushdown {
        // Slot 1: CTEUnionFilterPushdown runs BEFORE PredicatePushdown so the
        // ordinary pushdown then sinks the union filter it inserts to the body's
        // scans (which is what lets semi-join reduction inject dimension keys).
        rules.push(Box::new(CTEUnionFilterPushdown));
        rules.push(Box::new(PredicatePushdown));
    }

    // Always on. Before join ordering: pushing a selective SEMI/ANTI down changes
    // the region the reorderer sees (a reduced input, not a top-level filter).
    rules.push(Box::new(SemiJoinPushdown));

    // Slot 4 (always on): EagerAggregation replaces the fact atom with a partial
    // aggregate before join ordering re-costs the region.
    rules.push(Box::new(EagerAggregation::new(shared.clone())));

    if config.enable_join_reordering {
        // JoinOrdering reads folded equi conditions and embedded scan filters, and
        // must run BEFORE projection pushdown prunes columns.
        rules.push(Box::new(JoinOrdering::new(
            shared.clone(),
            config.max_join_reorder_size as usize,
        )));
    }

    if config.enable_projection_pushdown {
        rules.push(Box::new(ProjectionPushdown));
    }

    rules.push(Box::new(AggregatePushdown));
    rules.push(Box::new(OrderByPushdown));
    rules.push(Box::new(LimitPushdown));

    RuleBasedOptimizer::new(rules)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::CostConfig;
    use fq_plan::expr::{Expr, LiteralValue};
    use fq_plan::logical::{Filter, Scan};

    /// A stats-free cost model (the registration-order tests never estimate).
    fn cost_model() -> CostModel {
        CostModel::new(CostConfig::default(), None)
    }

    /// A one-column scan under a data source.
    fn scan(table: &str, columns: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(Box::new(Scan::new(
            "ds",
            "public",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        )))
    }

    /// A boolean literal predicate.
    fn bool_lit(value: bool) -> Expr {
        Expr::Literal {
            value: LiteralValue::Boolean(value),
            data_type: fq_common::DataType::Boolean,
        }
    }

    /// A rule that never changes the plan (proves the driver terminates at once).
    struct Identity;
    impl OptimizationRule for Identity {
        fn name(&self) -> &'static str {
            "Identity"
        }
        fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
            Ok(plan)
        }
    }

    /// A rule that wraps a bare scan in a Filter EXACTLY once (idempotent): fires
    /// on the first pass, then declines because the input is already a Filter.
    struct WrapOnce;
    impl OptimizationRule for WrapOnce {
        fn name(&self) -> &'static str {
            "WrapOnce"
        }
        fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
            if matches!(plan, LogicalPlan::Scan(_)) {
                return Ok(LogicalPlan::Filter(Filter {
                    input: Box::new(plan),
                    predicate: bool_lit(true),
                }));
            }
            Ok(plan)
        }
    }

    /// A rule that ALWAYS reports a change (swaps between two distinct plans) so
    /// the fixpoint never settles - exercises the iteration backstop.
    struct Oscillate;
    impl OptimizationRule for Oscillate {
        fn name(&self) -> &'static str {
            "Oscillate"
        }
        fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
            // Toggle the single scan column so the plan differs every pass.
            if let LogicalPlan::Scan(mut node) = plan {
                node.columns = if node.columns == vec!["a".to_string()] {
                    vec!["b".to_string()]
                } else {
                    vec!["a".to_string()]
                };
                return Ok(LogicalPlan::Scan(node));
            }
            Ok(plan)
        }
    }

    #[test]
    fn identity_rule_reaches_fixpoint_immediately() {
        let optimizer = RuleBasedOptimizer::new(vec![Box::new(Identity)]);
        let plan = scan("t", &["a"]);
        assert_eq!(optimizer.optimize(plan.clone()).unwrap(), plan);
    }

    #[test]
    fn one_shot_rule_fires_once_then_fixpoints() {
        let optimizer = RuleBasedOptimizer::new(vec![Box::new(WrapOnce)]);
        let result = optimizer.optimize(scan("t", &["a"])).unwrap();
        // A single Filter wraps the scan (fired exactly once; no double wrap).
        let LogicalPlan::Filter(filter) = result else {
            panic!("expected a Filter at the root");
        };
        assert!(matches!(*filter.input, LogicalPlan::Scan(_)));
    }

    #[test]
    fn oscillating_rule_stops_at_the_iteration_backstop() {
        let optimizer = RuleBasedOptimizer::new(vec![Box::new(Oscillate)]);
        // Never settles, but the backstop guarantees termination and a result.
        let result = optimizer.optimize(scan("t", &["a"])).unwrap();
        assert!(matches!(result, LogicalPlan::Scan(_)));
    }

    #[test]
    fn explain_is_unwrapped_optimized_and_rewrapped() {
        let optimizer = RuleBasedOptimizer::new(vec![Box::new(WrapOnce)]);
        let plan = LogicalPlan::Explain(Explain {
            input: Box::new(scan("t", &["a"])),
            format: fq_plan::logical::ExplainFormat::Text,
        });
        let result = optimizer.optimize(plan).unwrap();
        let LogicalPlan::Explain(explain) = result else {
            panic!("Explain must be re-wrapped around the optimized query");
        };
        // The wrapped query was optimized (WrapOnce fired under the Explain).
        assert!(matches!(*explain.input, LogicalPlan::Filter(_)));
    }

    #[test]
    fn build_optimizer_registers_rules_in_the_spec_order() {
        let optimizer = build_optimizer(&OptimizerConfig::default(), cost_model());
        // Default config enables predicate pushdown and join reordering:
        // CTEUnionFilterPushdown leads (before PredicatePushdown), EagerAggregation
        // sits always-on after SemiJoinPushdown, JoinOrdering before projection.
        assert_eq!(
            optimizer.rule_names(),
            vec![
                "CTEUnionFilterPushdown",
                "PredicatePushdown",
                "SemiJoinPushdown",
                "EagerAggregation",
                "JoinOrdering",
                "ProjectionPushdown",
                "AggregatePushdown",
                "OrderByPushdown",
                "LimitPushdown",
            ]
        );
    }

    #[test]
    fn build_optimizer_honors_config_gates() {
        let config = OptimizerConfig {
            enable_predicate_pushdown: false,
            enable_projection_pushdown: false,
            enable_join_reordering: false,
            enable_decorrelation: true,
            max_join_reorder_size: 10,
        };
        let optimizer = build_optimizer(&config, cost_model());
        // Predicate/projection pushdown AND join ordering drop out; the always-on
        // rules remain. CTEUnionFilterPushdown drops with predicate pushdown, but
        // EagerAggregation stays - it is always on.
        assert_eq!(
            optimizer.rule_names(),
            vec![
                "SemiJoinPushdown",
                "EagerAggregation",
                "AggregatePushdown",
                "OrderByPushdown",
                "LimitPushdown",
            ]
        );
    }
}
