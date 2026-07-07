# Fragment fusion (lazy bindings) - plan

Goal: stop draining every Arrow stream at every step. Fuse merge fragments
into one streaming DataFusion execution per pipeline region, materializing
only at genuine pipeline breakers - so DataFusion's spilling operators (sort,
grouped aggregate, nested-loop join, sort-merge buffered side; all verified
present in the DF54 sources with the default OsTmpDirectory DiskManager)
actually get to run, and the per-step collect/re-register latency disappears.

Targets: the SF10 memory wall (q23/q67/q78 exhaust the 32GB pool in
fedq_collect, q64 hits the 40GB RSS watchdog) and the super-linear family
(q05 24x, q39 19x, q06 20x, q31 20x at SF10). Branch:
`feature/fragment-fusion` in BOTH repos (experimental; the previous branches
are the rollback).

ASCII only. Status: IN PROGRESS.

## Diagnosis recap (verified on q67's IR at SF10)

Islands and reductions work as designed: four source reads, each one pushed
query, the fact pre-reduced by date keys. The wall is the coordinator chain
`join -> join -> join -> ROLLUP aggregate -> window`: every step drains its
DataFusion output stream into an in-memory binding (collect_tracked) and the
next step re-registers it as a MemTable in a fresh SessionContext - roughly
four fact-sized intermediates resident at once. Sources are drained the same
way (`for batch in reader { push }`). The only true stream survivors are the
outer boundaries (source -> engine, engine -> Python).

Genuine pipeline breakers - places that NEED a complete input:
- `collect_distinct` (the keys go INTO the probe's SQL text),
- a multi-consumer binding (CTE materialize-once, build side reused),
- the final `return`.
Everything else drains only because the interpreter materializes per step.
One coupling makes it worse: in join cascades the emitter collects the next
dimension's keys FROM THE PREVIOUS JOIN'S OUTPUT, so each join output is
itself a breaker input.

## Design

### Phase A - lazy bindings in the engine (IR unchanged)

`Binding` grows a `Lazy(LogicalPlan)` variant. A merge step no longer
executes: it builds its fragment's DataFrame in a scratch SessionContext -
materialized inputs registered as MemTable, lazy inputs as a ViewTable
wrapping the input's LogicalPlan (providers travel inside the plan, so a
plan built in one context executes fine from another) - and stores the
resulting LogicalPlan as the binding. Execution happens only when a binding
is FORCED:

- `collect_distinct` input, `injected_scan` keys, and `return` force a
  collect (through collect_tracked - still pool-accounted, and the fused
  plan's operators spill through the shared RuntimeEnv while it runs);
- a binding with use-count > 1 (binding_use_counts already exists) is
  materialized at FIRST consumption - re-executing a lazy plan per consumer
  would be the CTE re-emission disease again;
- single-use lazy bindings compose without ever touching memory.

Effect: a chain of merge fragments with no breaker in between becomes ONE
DataFusion execution that streams end to end. Sources stay drained for now
(they feed injections and multi-consumers anyway; Phase C revisits).

Notes:
- raw_sql fragments work unchanged: `ctx.sql` resolves `in_0` against the
  registered view/table names in the scratch context.
- Per-step profiling moves to the forcing points (a lazy step is ~0ms; the
  breaker pays for the whole region - log it as such).
- The executed-schema reconciliation (nullable widening etc.) applies at
  materialization points exactly as today; INSIDE a fused region DataFusion's
  own type coercion governs, which is what a single-engine plan does anyway.

### Phase B - re-anchor key collection to base bindings (emitter)

In a cascade, collect the next probe's keys from the base binding that
CARRIES the key column (the already-materialized fact scan) instead of the
previous join's output, when the emitter can trace the key column to such a
binding via column_aliases. The traded selectivity (superset keys - e.g.
stores touched by ANY sale rather than in-window sales) costs a slightly
fatter dim read; it buys the join cascade fusing into one streaming region.
When the key column is not traceable to a live base binding, keep today's
behavior (force the join output - correct, just unfused).

### Phase C (evaluate after A+B) - streaming source providers

Wrap the ADBC/DuckDB readers in a streaming TableProvider so single-use
source reads feed the fused plan without the intermediate Vec. Single-use
only; multi-consumer sources stay materialized. Decide from the A+B numbers.

## Verification gates (after each phase)

- Full pytest suite (1278 green today).
- TPC-DS tallies: SF0.1 (99|0|0), SF1 (99|0|0), SF10 (95|0|4 -> expect
  99|0|0 after A+B) - plus peak-RSS observation on q67/q23/q78/q64.
- TPC-H fedpgduck SF1 (22/22 at 1.91x) - the reduction-heavy benchmark most
  sensitive to Phase B's re-anchoring policy.

## Rollback

Everything lands on `feature/fragment-fusion` (both repos). Going back is
`git checkout feature/cost-based-optimizer` (federated-query) +
`git checkout main` (fedqrs) + one maturin rebuild.
