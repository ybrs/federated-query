//! Step building: `PhysicalPlan` -> an ordered `Vec<Step>` + a named `Fragment`
//! map that fq-exec consumes in-process. The crate's FINAL planning output. Ports
//! `executor/rust_ir.py` (`build_ir_with_observations` + the emit walk + CSE).
//!
//! No JSON, no serde: `Step` / `Fragment` / `ScanSpec` hold `fq_plan::Expr`
//! directly (retagged in place, section `expr_retag`), and the enum IS the tag.
//!
//! STAGE STATUS: the straight node walk (`emit_nodes`) and step CSE (`cse`) are
//! implemented. Semi-join REDUCTION (the `InjectedScan`/`CollectDistinct` path) and
//! the OBSERVATION provenance recorders are deferred; a join emits both sides in
//! full (correct, just reads more), and `observations` is empty (no learned-stats
//! provenance, execution unaffected). Both are safe, correct-but-unoptimized
//! deferrals, not silent wrong behavior.

mod cse;
mod emit_nodes;
pub mod error;
mod expr_retag;
mod render_sql;
mod scan_spec;
mod types;

use std::collections::{BTreeMap, HashMap};

use fq_plan::PhysicalPlan;

pub use error::StepError;
pub use scan_spec::PARALLEL_SCAN_MIN_ROWS;
pub use types::{
    AggCall, AggSelectItem, BuiltSteps, ExtraInjection, Fragment, JoinKind, Observation,
    Projection, ScanSpec, SortKey, Step, WithinGroup,
};

/// Raw-pointer identity of a plan node, the sound analogue of Python's `id(node)`
/// while the plan tree is borrowed immutably for the whole walk. Ports section 2c.
type NodeId = *const PhysicalPlan;

/// The stable identity of a plan node (its address). The walk never mutates or
/// clones a sub-plan, so this pointer is valid for `Ctx`'s lifetime.
fn node_id(node: &PhysicalPlan) -> NodeId {
    std::ptr::from_ref(node)
}

/// Hands out unique binding (`b1`, `b2`, ...) and fragment (`f1`, ...) names.
/// Ports `_Names` (pre-increment counters, so the first ids are `b1` / `f1`).
struct Names {
    binding_counter: u32,
    fragment_counter: u32,
}

impl Names {
    /// Start both counters at zero.
    fn new() -> Self {
        Self {
            binding_counter: 0,
            fragment_counter: 0,
        }
    }

    /// The next unique binding name.
    fn binding(&mut self) -> String {
        self.binding_counter += 1;
        format!("b{}", self.binding_counter)
    }

    /// The next unique fragment name.
    fn fragment(&mut self) -> String {
        self.fragment_counter += 1;
        format!("f{}", self.fragment_counter)
    }
}

/// The mutable accumulator threaded through the bottom-up walk. Ports `_Ctx`; maps
/// keyed by `NodeId` where Python keyed on `id(node)`.
struct Ctx {
    steps: Vec<Step>,
    fragments: BTreeMap<String, Fragment>,
    names: Names,
    /// share-key -> index into `steps` (the CSE cache; the step lives in `steps`,
    /// widen/narrow mutate it in place by index).
    step_cache: HashMap<String, usize>,
    /// CTE producer id -> its single emitted binding (emit + execute a CTE once).
    cte_bindings: HashMap<NodeId, String>,
    /// base node id -> binding already emitted with an injected filter (reduction);
    /// read by `emit` to short-circuit an already-reduced base. Unused until the
    /// reduction stage populates it (kept so the walk's cache check is in place).
    injected: HashMap<NodeId, String>,
    observations: BTreeMap<String, Observation>,
}

impl Ctx {
    /// A fresh, empty context.
    fn new() -> Self {
        Self {
            steps: Vec::new(),
            fragments: BTreeMap::new(),
            names: Names::new(),
            step_cache: HashMap::new(),
            cte_bindings: HashMap::new(),
            injected: HashMap::new(),
            observations: BTreeMap::new(),
        }
    }
}

/// Build the ordered step list + fragment map for a physical plan root. fq-physical's
/// final planning output; fq-exec consumes `BuiltSteps`. Ports
/// `build_ir_with_observations`.
///
/// `build_steps` needs NEITHER the catalog NOR the cost model NOR the stats catalog:
/// every statistic it consults is already a field stamped on the plan nodes by the
/// physical planner (including the per-scan `datasource_kind` used for the parallel
/// gate and source dialect).
pub fn build_steps(plan: &PhysicalPlan) -> Result<BuiltSteps, StepError> {
    let mut ctx = Ctx::new();
    let binding = emit_nodes::emit(plan, &mut ctx)?;
    ctx.steps.push(Step::Return { input: binding });
    Ok(BuiltSteps {
        outputs: fq_plan::output_column_names(plan),
        steps: ctx.steps,
        fragments: ctx.fragments,
        observations: ctx.observations,
    })
}

/// Append a merge step running `fragment` over `inputs`; return its binding. Ports
/// `_merge_step`.
fn merge_step(ctx: &mut Ctx, fragment: &str, inputs: BTreeMap<String, String>) -> String {
    let binding = ctx.names.binding();
    ctx.steps.push(Step::Merge {
        fragment: fragment.to_string(),
        inputs,
        binding: binding.clone(),
    });
    binding
}

/// Register a `raw_sql` fragment running `sql` over `inputs`, and a merge step over
/// it; return its binding. Ports `_raw_sql_step`.
fn raw_sql_step(ctx: &mut Ctx, sql: String, inputs: BTreeMap<String, String>) -> String {
    let fragment = ctx.names.fragment();
    ctx.fragments
        .insert(fragment.clone(), Fragment::RawSql { sql });
    merge_step(ctx, &fragment, inputs)
}
