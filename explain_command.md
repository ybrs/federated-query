## Explain Command Specification

### Goal
Add `EXPLAIN` support to the SQL pipeline so users can run statements such as:
```
EXPLAIN SELECT * FROM orders WHERE order_date >= '2024-01-01';
```
and receive the planned execution steps instead of executing the query.

### High-Level Behavior
1. Parser recognizes `EXPLAIN` and produces a logical plan marked as `explain_only`.
2. Binder, optimizer, and planner operate exactly as they do for regular queries to produce the physical plan.
3. Executor bypasses actual execution when `explain_only` is set and instead formats and returns a textual plan description similar to PostgreSQL (tree layout, estimated cost, and source hints).

### Output Format
* Textual rows, each row describing one node in the physical plan.
* Include node type, estimated cost (if available), row estimate, and relevant attributes (e.g., join predicate, filters, data source).
* Example row text: `PhysicalHashJoin  cost=120.5 rows=500 predicate=(orders.customer_id = customers.id)`
* Result should be consumable as a single-column table so existing fetching APIs still work.

### Error Handling
`EXPLAIN` should surface the same errors a normal query would (parse, bind, optimize) but never touches underlying data sources because no execution occurs.

## Implementation Plan

1. **Parser Layer**
   - Extend `Parser` to detect `EXPLAIN` keyword and flag the wrapped statement.
   - Produce a `LogicalExplain` node (new class) or annotate existing logical root with `explain_only`.

2. **Logical & Physical Plan Additions**
   - Add `LogicalExplain` and `PhysicalExplain` nodes in `federated_query/plan/logical.py` and `federated_query/plan/physical.py`.
   - `PhysicalExplain` holds the child physical plan that would normally run.

3. **Planner Changes**
   - Update the planner to translate `LogicalExplain` into `PhysicalExplain`.
   - Ensure the child logical plan is planned normally so optimizations still apply.

4. **Executor Support**
   - Add execution path for `PhysicalExplain` that formats the child plan into plan rows.
   - Reuse existing plan tree printing utilities if available; otherwise introduce a focused formatter under `federated_query/executor`.

5. **Plan Formatting Utility**
   - Implement helper functions to traverse physical plans and produce row strings with indentation to show tree structure.
   - Include cost and row estimates from `PhysicalPlanNode` metadata (extend nodes if needed).

6. **Tests & Validation**
   - Add unit tests covering parser recognition and executor output for simple plans.
   - Add integration-style test ensuring `Executor.execute` on `EXPLAIN` returns formatted rows without hitting data sources.

7. **Documentation**
   - Update README or relevant docs explaining `EXPLAIN` usage once implementation is complete.
