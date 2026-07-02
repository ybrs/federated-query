Plan: Close ORDER BY pushdown gaps


Current gaps (updated)
- Sort on aggregates: pushdown now annotates scans when sort keys align with group-by columns; still limited to simple group-by scans.
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
    build plans for derived tables/CTEs, then thread pushdown rules through those nodes.
    datasource whether it supports that exact dialect or override defaults. To make this
    planned/pushed until window support exists end-to-end.
  - Expression-based sort pushdown: We only push ColumnRef sorts. Pushing expressions
    would require: (a) expression normalization/rewrite to datasource SQL, (b) ensuring
    the datasource can evaluate the expression identically (type/NULL semantics), and
    (c) binding those expressions to scan outputs for alias resolution. Without an
    local.
