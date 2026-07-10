# Python -> Rust planning rewrite - assessment and plan

Whether and how to move the ~30k-line Python side (parser, binder,
decorrelator, optimizer, physical planner, IR emitter) into the Rust engine.
Assessed 2026-07-10 with measurements and a transpiler spike.

## The measured problem

- Steady-state planning costs 11-26ms per small query (parse 1-8ms, optimize
  2-11ms, physical 2-6ms, bind ~1ms), plus IR JSON serialization. The PLAN
  CACHE already erases this for REPEATED statements; a first-sight query
  still pays it, and the new cold/warm harness split keeps that number
  visible (q03 SF0.1: warm 19ms / cold 110ms - the cold delta is stats
  fetches + probe + planning + connection setup).
- The GIL: planning serializes CONCURRENT queries. The roadmap's multi-query
  phase (notebook fan-out, dashboard tiles) hits this wall structurally -
  the strongest argument for the rewrite, stronger than the per-query floor.
- The accelerator phase adds per-query Python work (cache lookups,
  invalidation, rewrite decisions) on exactly the latency-sensitive path.

## The sqlglot question - resolved by spike (2026-07-10)

polyglot (github.com/tobilg/polyglot, `polyglot-sql` crate, v0.5.15) is a
Rust SQL transpiler covering 32 dialects that passes 10,220+ of SQLGLOT'S
OWN fixture tests at 100 percent. Spiked against THIS engine:

- Parsing: all 121 benchmark queries (TPC-DS 99 + TPC-H 22) parse with ZERO
  failures at 0.36ms average - 10-20x faster than sqlglot.
- Transpile fidelity on OUR EMITTED SQL: every canonical-postgres render the
  engine sent through the to_source_sql boundary across 14 TPC-DS queries
  was re-rendered by polyglot and EXECUTED against the DuckDB file:
  25 of 25 comparable renders returned identical results (the other 13
  referenced per-connection temp state the probe connection lacks - a test
  artifact, not a transpile failure). Zero transpile errors.

Residual risks: 0.5.x maturity and bus factor versus sqlglot's ecosystem.
Two mitigations: (a) the differential harness below catches regressions
per-release; (b) a Rust planner does not strictly NEED a transpiler at all -
we control every emitted SQL shape, and fedqrs_core::sql already renders
scans/temp-joins per DsKind directly; polyglot serves as the INPUT parser
and a migration bridge, with direct per-dialect emission as the end state.

## What the 30k lines are, by porting risk

- plan/expressions model (pydantic): mechanical; Rust enums + exhaustive
  matches are a strictly better fit for the walker-descent lesson (the
  compiler enforces what five perf bugs taught).
- parser (sqlglot AST -> logical) + binder: mechanical-moderate; polyglot
  AST in, same logical model out.
- optimizer rules (pushdowns, join ordering, CTE union filter, eager agg):
  the most CHURNED code - porting freezes iteration speed, so port only
  once the current perf program plateaus.
- decorrelation: the most SUBTLE code (N-K, disjunctive, laterals); stable
  now, port late with its full test corpus.
- physical planner + single-source pushdown + dim shipping + rust_ir:
  rendering becomes direct per-dialect emission; the IR JSON boundary
  DISAPPEARS (the plan is already in-process).
- stats collector / plan cache / runtime glue: small; stats stay
  network-bound regardless of language.

## Migration shape: move the pipeline CUT right-to-left

The pipeline is SQL -> parse -> bind -> decorrelate -> optimize -> physical
-> IR -> engine. Today's Python/Rust cut is at IR. Each phase moves the cut
one stage left, with the stage's OUTPUT as a serialized contract and a
DIFFERENTIAL GATE (run both implementations, compare stage output on all 121
queries x placements; the results-level tallies stay the final arbiter -
they are language-agnostic and already exist).

- R0 (now, free): keep landing product features in Python; the accelerator
  ships first. Freeze the IR schema as a versioned contract.
- R1: PHYSICAL planning + SQL emission + IR construction in Rust; cut =
  optimized logical plan (JSON). Kills the transpile boundary and the IR
  serialization; the spike already de-risked the dialect question.
- R2: OPTIMIZER rules in Rust; cut = decorrelated plan. Port order inside:
  stable rules first (pushdowns), join ordering + eager agg last.
- R3: DECORRELATION; cut = bound plan. Highest subtlety - port with a
  dedicated plan-diff corpus harvested from the full suite.
- R4: PARSER + BINDER (polyglot); cut = SQL text. Python planning retires;
  the GIL leaves the query path entirely.

Effort: R1-R4 is a multi-month single-stream program (est. 35-45k lines of
Rust plus targeted test ports; the 1329-test Python suite remains the
reference oracle until each stage's differential gate holds).

## Recommendation (resequenced 2026-07-10 after review)

YES, strategically - the GIL argument alone decides it once multi-query
lands. SEQUENCING: R1 FIRST, THEN the accelerator, then R2-R4.

The original draft put the accelerator before R1; the review caught why
that is wrong: the accelerator's machinery CONCENTRATES at exactly the
layer R1 moves. Fragments are WRITTEN by the engine (Rust holds every
materialized binding), READ as local scans (the engine already reads
parquet), and USED at plan time by matching subplan signatures and
REWRITING the physical plan to scan the fragment - physical-planning/IR
logic. Built in Python first, that logic is built twice (R1 re-ports it
immediately) and in between the language boundary grows a new bidirectional
protocol (Python decides -> Rust writes -> Python re-evaluates -> Rust
reads) that R1 then has to cut through.

The refinement that keeps this from meaning "everything in Rust first": the
accelerator never touches the parser, the decorrelator, or the logical
optimizer rules (R2-R4) - porting those first buys it nothing. Fragment
substitution sits at the physical/IR level; its plan-quality input
(measured cardinalities per subplan signature) is already in the learned
catalog, which Rust reads directly at that point (rusqlite - the
adaptive-catalog plan's original Phase C note). Python keeps the logical
half, where iteration is cheap and fragments never reach.

Order of work: R1 (physical planner + single-source SQL emission + dim
shipping + IR construction, ~8-9k of the densest lines; dialect risk
retired by the spike; gated by the logical-plan-JSON differential + the
tallies) -> accelerator, Rust-native at that layer -> R2-R4 as the
optimizer/decorrelator stabilize. Re-evaluate polyglot vs direct emission
at R1 entry with its own fidelity gate.
