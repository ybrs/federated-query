# Join Pushdown Implementation Plan

## Goals
- Push qualifying joins down into remote data sources so they execute server-side.
- Preserve correctness and readability by keeping each function under 20 lines and cyclomatic complexity ≤4, per repository standards.
- Maintain full compatibility with existing predicate, projection, aggregate, and limit pushdowns.

## Scope
- Same-data-source equi-joins (initial focus on INNER joins; extend later once stable).
- SQL generation for remote joins, including filters, projections, and aggregates already merged into the child scans.
- Planner and optimizer plumbing to create the new physical operator when safe.
- Comprehensive regression tests (unit + e2e) that prove SQL pushdown behavior and result parity with local joins.

## Key Changes
1. **Alias propagation**
   - Parser: populate `Scan.alias` directly from table aliases when building FROM/JOIN trees.
   - Ensure binder/optimizer preserve aliases through rewrites and pushed-down scans.

2. **Remote join planning**
   - Introduce helper(s) in the physical planner to detect when both sides are `Scan` nodes against the same `datasource` and the data source advertises `DataSourceCapability.JOINS`.
   - Constrain first iteration to INNER joins with equi-conditions to keep risk low.

3. **`PhysicalRemoteJoin` operator**
   - Encapsulate SQL assembly for two-table SELECT statements with JOIN clauses.
   - Reuse `Expression.to_sql()` for predicates/conditions, quoting table names and aliases consistently with existing `PhysicalScan`.
   - Delegate schema discovery and execution to the underlying data source.

4. **Safety fallbacks**
   - If any prerequisite fails (capability, condition shape, mixed datasources), planner must fall back to the current `PhysicalHashJoin` / `PhysicalNestedLoopJoin`.

5. **Testing**
   - Unit: parser alias propagation, planner join detection, SQL string builder edge cases.
   - E2E: extend `tests/test_pushdown_real_e2e.py` with INNER/LEFT/RIGHT scenarios and assert captured SQL contains JOIN clauses; verify result correctness (especially NULL handling) against baseline.
   - Regression: ensure non-pushdown paths still work by covering mixed-datasource joins and unsupported join types.

## Execution Steps
1. Update parser + supporting rules/binder to carry aliases through scans.
2. Implement `PhysicalRemoteJoin` (schema caching, SQL builder, execution).
3. Enhance `PhysicalPlanner._plan_join` with guarded pushdown detection.
4. Add targeted unit tests (parser alias, planner decision logic).
5. Add new e2e test cases confirming SQL pushdown + results.
6. Run full pushdown e2e suite and relevant join tests.

## Risks & Mitigations
- **Incorrect SQL quoting / alias clashes:** centralize SQL formatting helpers; add tests with overlapping column names to confirm de-duplication matches existing behavior.
- **Capability mismatches:** check `DataSourceCapability.JOINS` before attempting pushdown and respect future capability flags.
- **Complexity creep:** factor helper methods aggressively to stay within cyclomatic + line limits.

## Testing Checklist
- `pytest tests/test_parser.py::TestParserJoinAliases`
- `pytest tests/test_physical_planner.py::TestJoinPushdownPlanner` (new)
- `pytest tests/test_pushdown_real_e2e.py::TestJoinPushdownReal`
- `pytest tests/test_e2e_joins.py`
- Full `pytest tests/test_pushdown_real_e2e.py` to ensure all pushdowns still pass.
