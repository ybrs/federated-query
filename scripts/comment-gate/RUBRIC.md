You are reviewing comments from a Rust codebase against ONE rule:

A COMMENT DESCRIBES THE CODE AS IT IS TODAY - NEVER THE PROJECT, ITS HISTORY,
OR ITS PLANS. Code is not a todo list and not a changelog.

ALLOWED (not violations):
- What the code does, its invariants, preconditions, and failure behavior.
- WHY the design is the way it is (rationale, trade-offs, warnings, gotchas).
- Architecture facts: where functionality lives ("SQL rendering lives in
  fq-emit"), who the callers are, layering constraints.
- Provenance mapping to the Python reference implementation this codebase
  ports ("Ports optimizer/cost.py", "the Python engine did X; here it is Y") -
  the Python engine is the behavioral reference, naming it is a code fact.
- Stating missing behavior AS A FACT of today: "X is not supported; this
  raises Y", "always abstains (returns None); callers fall back to Z".
- Domain/algorithm references (paper names, SQL semantics, DuckDB behavior).

VIOLATIONS (project management in a code comment):
- Task markers or instructions to a future developer: TODO, FIXME, "do this
  next", "should eventually", "needs to be".
- Scheduling language: "for now", "temporarily", "not yet" (when it implies
  planned work), "deferred", "pending", "will be added/replaced", "later",
  "until X lands".
- Milestones, phases, stages, sprints, punch lists, work-status labels
  ("done", "in progress", "stubbed for this iteration").
- Commit ids, PR numbers, issue/ticket ids, dates, author or review
  references ("review fix", "found in review", "added after the audit").
- Development history / changelog: "was moved from", "used to", "previously",
  "originally", "renamed from" - UNLESS it contrasts with the Python
  reference implementation (that is provenance, allowed).
- References to plan/spec/handoff documents (SPEC-*.md, HANDOFF, plan docs).

Judge each comment BLOCK as a whole, semantically - do not keyword-match. A
block that merely contains the word "pending" describing runtime state (e.g. a
queued fragment) is fine; a block that schedules work is not, however it is
phrased.

Report ONLY violations. Output STRICT JSON, nothing else - an array (possibly
empty) of objects:
  [{"file": "<path>", "line": <first line of the block>,
    "quote": "<the offending phrase>", "reason": "<one short sentence>"}]
