Plan: Close ORDER BY pushdown gaps

Goal: broaden ORDER BY pushdown beyond the current “Sort → Projection/Filter → Scan” shape and ensure alias/star handling is robust.

Current gaps (updated)
- Sort on aggregates: pushdown now annotates scans when sort keys align with group-by columns; still limited to simple group-by scans.
- Sort inside subqueries/CTEs: parser doesn’t build plans for WITH/subselects; no pushdown possible until parser support exists.
- Sort across set operations: UNION/INTERSECT/EXCEPT now annotate children with sort metadata but keep top Sort; further improvements depend on execution guarantees.
- NULLS FIRST/LAST fidelity: metadata captured but not validated per datasource; could diverge from backend behavior.
- Window functions: unsupported in expressions; ORDER BY tied to windows not handled.
- Expression-based ORDER BY: non-ColumnRef keys are intentionally non-pushable; add rewrite support if/when expression evaluation is supported.
Completed
- Alias-safe pushdown: ORDER BY on projection aliases rewrites to source columns and pushes into scans (binder + optimizer).
- Join-side pushdown: ORDER BY columns exclusive to one join input are annotated on that Scan while retaining top-level Sort for correctness.
- Guardrails: only ColumnRef sort keys are pushable; complex expressions stay local.

Plan of attack
1) Alias-safe pushdown (done)
   - Map ORDER BY aliases to source ColumnRefs and push to scans; tests in logical optimizer suite.
2) Join-aware pushdown (done)
   - Detect sort keys that belong to one join side and annotate that Scan; retain top Sort.
3) Aggregate-aware pushdown (todo)
   - When Aggregate is pushed to Scan (group_by present), allow Sort on group_by columns to annotate Scan.
4) Expression guardrails (done)
   - Complex sort keys remain local; revisit when expression pushdown is supported.
5) Parser roadmap (future)
   - Add subquery/CTE handling so ORDER BY inside derived tables/CTEs can be considered for pushdown.

Validation
- Unit tests: alias and join-side pushdowns added; add aggregate group-by push cases when implemented.
- Functional pushdown tests: confirm emitted SQL carries ORDER BY for supported shapes.


For the rest: 

  - Subqueries/CTEs: The parser/binder only handles top-level SELECT/EXPLAIN (no WITH,
    no derived tables). Until we can parse and bind subselects/CTEs into logical plans,
    there’s nowhere to attach ORDER BY metadata. Work required: extend parser/binder to
    build plans for derived tables/CTEs, then thread pushdown rules through those nodes.
  - NULLS FIRST/LAST fidelity: We currently pass nulls_order through but don’t ask the
    datasource whether it supports that exact dialect or override defaults. To make this
    reliable, we’d need per-datasource capability flags and SQL generation that emits
    explicit NULLS modifiers or emulation (e.g., CASE ordering) when unsupported. That’s
    non-trivial without a SQL serializer that’s aware of dialect quirks.
  - Window-function sorts: Window functions aren’t supported in our expression/physical
    layers (no execution or binding for OVER clauses). ORDER BY tied to windows can’t be
    planned/pushed until window support exists end-to-end.
  - Expression-based sort pushdown: We only push ColumnRef sorts. Pushing expressions
    would require: (a) expression normalization/rewrite to datasource SQL, (b) ensuring
    the datasource can evaluate the expression identically (type/NULL semantics), and
    (c) binding those expressions to scan outputs for alias resolution. Without an
    expression SQL generator and pushdown capability checks, it’s safer to keep them
    local.
