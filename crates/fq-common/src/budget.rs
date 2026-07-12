//! The planning wall-clock budget.
//!
//! Planning is O(metadata) BY DESIGN: it may read catalogs, cached statistics,
//! and source planner estimates (EXPLAIN), never data. Wall-clock is the
//! enforcement: a plan that runs past its budget is doing something O(data) (or
//! a remote probe is misbehaving), so the pipeline KILLS it and reports where
//! the time went instead of silently scaling plan time with data size. There is
//! deliberately NO off switch - a genuine edge case raises the configured
//! budget explicitly (`optimizer.planning_budget_ms`), it does not disable the
//! guard.

use std::time::{Duration, Instant};

/// A started planning clock with a hard limit. `Copy` so every planning tier
/// (pipeline stages, statistics collection) shares the same start instant.
#[derive(Debug, Clone, Copy)]
pub struct PlanBudget {
    started: Instant,
    limit: Duration,
}

impl PlanBudget {
    /// Start the clock with a limit in milliseconds.
    #[must_use]
    pub fn start(limit_ms: u64) -> Self {
        Self {
            started: Instant::now(),
            limit: Duration::from_millis(limit_ms),
        }
    }

    /// Milliseconds elapsed since the clock started (unrounded).
    #[must_use]
    pub fn elapsed_ms(&self) -> f64 {
        self.started.elapsed().as_secs_f64() * 1000.0
    }

    /// The configured limit in milliseconds.
    #[must_use]
    pub fn limit_ms(&self) -> u64 {
        self.limit.as_millis() as u64
    }

    /// Whether the elapsed wall clock has passed the limit.
    #[must_use]
    pub fn exceeded(&self) -> bool {
        self.started.elapsed() > self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::PlanBudget;

    #[test]
    fn a_zero_budget_is_immediately_exceeded() {
        let budget = PlanBudget::start(0);
        assert!(budget.exceeded());
        assert!(budget.elapsed_ms() >= 0.0);
    }

    #[test]
    fn a_generous_budget_is_not_exceeded() {
        let budget = PlanBudget::start(60_000);
        assert!(!budget.exceeded());
        assert_eq!(budget.limit_ms(), 60_000);
    }
}
