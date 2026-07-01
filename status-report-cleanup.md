# Cleanup status - what has been done so far (2026-06-27)

Scope so far: started removing duplicated logic in `federated_query/`. Full test
suite green throughout: **962 passed, 3 skipped, 22 xfailed**
(`POSTGRES_DB=duckpoc pytest`). jscpd clone count: 35 (baseline) -> 33 (now).

## Tooling
- Installed `jscpd` (copy-paste detector). Run: `jscpd federated_query --min-lines 5 --min-tokens 40 --format python`.

## Inventory
- Ran jscpd over `federated_query/` (35 textual clones at baseline).
- Ran read-only reviews for semantic duplication (same purpose, different code)
  across: SQL generation, expression/predicate utilities, datasource connectors,
  parser/binder/preprocessor/optimizer.

## Shared expression utilities (done)
Added a single canonical home in `plan/expressions.py`:
`expression_children`, `map_children`, `column_refs`, `split_conjuncts`,
`split_disjuncts`, `combine_and`, `combine_or`, `and_expressions`,
`is_aggregate_call`, `SUBQUERY_NODE_TYPES`, `contains_aggregate`,
`aggregate_output_map`, `split_where_having`.

Migrated these modules onto the canonical helpers and deleted their local copies:
- `optimizer/decorrelation.py` - removed ~13 duplicate defs (its own conjunct
  splitters, 4 rebuild functions, 3 child-walkers, column-ref collector,
  subquery-type tuple; `_and_join`/`_or_join` reduced to raise-on-empty wrappers
  over `combine_and`/`combine_or`).
- `plan/physical.py` - collapsed the 6-function window-resolver family
  (`_resolve_window_operands`/`_resolve_window_containers`/`_resolve_window_case`/
  `_resolve_function_columns`/`_resolve_window_expr`/`_resolve_window_list`) into
  ~4 lines via `map_children`; removed `_split_and`/`_join_and`/`_and_filters`/
  `_and_expr`/`_predicate_uses_aggregates`.
- `optimizer/rules.py` - `_extract_columns`, `_extract_column_refs` rebuilt on
  `column_refs`; removed `_predicate_children`/`_container_predicate_children`/
  `_case_predicate_children`/`_case_columns`/`_window_children`; both expr-level
  `_merge_filters` delegate to `and_expressions`.
- `optimizer/single_source_pushdown.py` - repointed to the canonical helpers.
- `optimizer/physical_planner.py` - removed its `_split_and`; uses canonical
  `expression_children`.
- Removed the now-unused imports left behind by the above.

## WHERE/HAVING splitter consolidated (done)
- Two divergent WHERE/HAVING splitters existed (`PhysicalScan._partition_conjuncts`
  /`_predicate_uses_aggregates` vs `single_source._split_scan_filter`/
  `_route_conjunct`/`_references_aggregate`/`_substitute_aggregate_refs`).
- Replaced both with one shared `split_where_having` in `plan/expressions.py`.
- Deleted `PhysicalScan._partition_conjuncts`, `_predicate_uses_aggregates`, and
  `single_source`'s `_route_conjunct`/`_aggregate_expr_map`/`_references_aggregate`/
  `_substitute_aggregate_refs`.
- Side effect: this fixed a single-source `GROUP BY ... HAVING` crash (HAVING was
  being emitted as a WHERE on the aggregate alias). The tracking xfail flipped to
  a passing test; two EXPLAIN-only tests that asserted the wrong WHERE were
  corrected to assert HAVING.

## NOT done yet
- The two SQL emitters (`single_source_pushdown` and the per-node `_build_query`
  on `PhysicalScan`/`PhysicalRemoteJoin`/`PhysicalRemoteSetOp`) are both still
  live; per-node clause rendering (SELECT/column/table/ORDER BY/GROUP BY/LIMIT)
  is still duplicated (most of the remaining jscpd clones in `physical.py`).
- Datasource connector duplication (config/metadata/type-map), binder
  `Binder` vs `SubqueryPlanBinder` fork, pushdown-rule recursion in `rules.py`,
  and a no-duplication enforcement gate: none started.
