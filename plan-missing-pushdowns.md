Plan: Close ORDER BY pushdown gaps

Goal: broaden ORDER BY pushdown beyond the current “Sort → Project/Filter → Scan” shape and ensure alias/star handling is robust.

Current gaps (updated)
- Sort on aggregates: pushdown implemented only when sort keys align with group-by columns; deeper aggregate pushdown to scans still limited to simple group-by.
- Sort inside subqueries/CTEs: parser doesn’t build plans for WITH/subselects; no pushdown possible until parser support exists.
- Sort across set operations: UNION/INTERSECT/EXCEPT return Sort unchanged; evaluate if/when to push.
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
