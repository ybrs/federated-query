# DuckDB Reconstruction Contract Coverage

This document records the source-derived reconstruction contracts enforced by
`tests/test_duckdb_reconstruct.py`. The full file currently passes with:

```text
831 passed
```

## SELECT-Reachable Handler Contracts

These names are derived from DuckDB optimized plans dumped from real SQL
contract cases. They are not invented classifications.

| DuckDB logical node | Contract case names | Tests |
| --- | --- | --- |
| `UNNEST` | `unnest`, `unpivot_basic` | `test_each_select_reachable_source_logical_node_has_handler`, `test_observed_select_nodes_have_handlers`, `test_all_contract_cases_reach_required_optimized_nodes`, `test_all_contract_cases_dump_serializable_optimized_json`, `test_reconstruction_contract_cases[unnest-*]`, `test_reconstruction_contract_cases[unpivot_basic-*]`, `test_audit_mandatory_plan_shapes_execute[unnest-*]` |
| `SAMPLE` | `sample`, `sample_reservoir` | `test_each_select_reachable_source_logical_node_has_handler`, `test_observed_select_nodes_have_handlers`, `test_all_contract_cases_reach_required_optimized_nodes`, `test_all_contract_cases_dump_serializable_optimized_json`, `test_reconstruction_contract_cases[sample-*]`, `test_reconstruction_contract_cases[sample_reservoir-*]`, `test_audit_mandatory_plan_shapes_execute[sample-*]` |
| `CTE` | `materialized_cte_scan` | `test_each_select_reachable_source_logical_node_has_handler`, `test_observed_select_nodes_have_handlers`, `test_all_contract_cases_reach_required_optimized_nodes`, `test_all_contract_cases_dump_serializable_optimized_json`, `test_reconstruction_contract_cases[materialized_cte_scan-*]`, `test_audit_mandatory_plan_shapes_execute[materialized_cte_scan-*]` |
| `CTE_SCAN` | `materialized_cte_scan`, `recursive_cte` | `test_each_select_reachable_source_logical_node_has_handler`, `test_observed_select_nodes_have_handlers`, `test_all_contract_cases_reach_required_optimized_nodes`, `test_all_contract_cases_dump_serializable_optimized_json`, `test_reconstruction_contract_cases[materialized_cte_scan-*]`, `test_reconstruction_contract_cases[recursive_cte-*]`, `test_audit_mandatory_plan_shapes_execute[materialized_cte_scan-*]`, `test_audit_mandatory_plan_shapes_execute[recursive_cte-*]` |
| `REC_CTE` | `recursive_cte` | `test_each_select_reachable_source_logical_node_has_handler`, `test_observed_select_nodes_have_handlers`, `test_all_contract_cases_reach_required_optimized_nodes`, `test_all_contract_cases_dump_serializable_optimized_json`, `test_reconstruction_contract_cases[recursive_cte-*]`, `test_audit_mandatory_plan_shapes_execute[recursive_cte-*]` |
| `POSITIONAL_JOIN` | `positional_join` | `test_each_select_reachable_source_logical_node_has_handler`, `test_observed_select_nodes_have_handlers`, `test_all_contract_cases_reach_required_optimized_nodes`, `test_all_contract_cases_dump_serializable_optimized_json`, `test_reconstruction_contract_cases[positional_join-*]`, `test_audit_mandatory_plan_shapes_execute[positional_join-*]` |
| `ASOF_JOIN` | `asof_join` | `test_each_select_reachable_source_logical_node_has_handler`, `test_observed_select_nodes_have_handlers`, `test_all_contract_cases_reach_required_optimized_nodes`, `test_all_contract_cases_dump_serializable_optimized_json`, `test_reconstruction_contract_cases[asof_join-*]`, `test_audit_mandatory_plan_shapes_execute[asof_join-*]` |

## All Observed Handler Nodes

The optimized SELECT contract matrix observes these relation nodes and requires
each one to have a handler and a direct contract case.

`AGGREGATE`, `ANY_JOIN`, `ASOF_JOIN`, `COMPARISON_JOIN`, `CROSS_PRODUCT`,
`CTE`, `CTE_SCAN`, `DELIM_GET`, `DELIM_JOIN`, `DISTINCT`, `DUMMY_SCAN`,
`EMPTY_RESULT`, `EXCEPT`, `FILTER`, `INTERSECT`, `LIMIT`, `ORDER_BY`,
`POSITIONAL_JOIN`, `PROJECTION`, `REC_CTE`, `SAMPLE`, `TOP_N`, `UNION`,
`UNNEST`, `WINDOW`

## All Reconstruction Contract Cases

The full row-equivalence, placeholder, temp-view, alias, and projection
contract matrix contains these cases.

`aggregate_arg_min`, `aggregate_filter_clause`, `aggregate_internal_function`,
`aggregate_multiple_ordered`, `aggregate_ordered_string_agg`,
`aggregate_scalar_no_group`, `any_join_arbitrary_condition`,
`any_quantified_subquery`, `argmax_scalar_limit`, `asof_join`,
`correlated_group_by`, `correlated_having`, `correlated_select`,
`correlated_where`, `cross_product_join`, `derived_cte_alias_lineage`,
`distinct`, `dummy_scan`, `duplicate_temp_view_names_multi_temp`,
`empty_result_projection_schema`, `except`, `exists_semi_rhs_filter`,
`exists_subquery_projection_boolean`, `full_join_variant`,
`generated_alias_collision`, `in_subquery`,
`in_subquery_projection_boolean`, `intersect`, `is_not_distinct_join`,
`join_derived_rhs_lineage`, `left_join_variant`, `limit`,
`limit_offset_window`, `materialized_cte_scan`,
`multiple_ctes_same_base_aliases`, `nested_join_conditions`,
`non_materialized_cte`, `not_exists_anti_join`, `not_in_null_sensitive`,
`not_in_subquery`, `order_by`, `pivot_explicit_in`,
`plain_inner_join_node`, `positional_join`,
`projection_over_aggregate_lineage`, `projection_over_window_lineage`,
`recursive_cte`, `repeated_alias_filter`, `repeated_base_cte_outer_alias`,
`right_join_variant`, `sample`, `sample_reservoir`,
`scalar_subquery_projection_join`, `set_operation_output_name_lineage`,
`slot_mapping_multi_projection`, `top_n`, `union`, `union_all`, `unnest`,
`unpivot_basic`, `window_dense_rank`, `window_first_value`, `window_lag`,
`window_rank`, `window_row_number`, `window_rows_frame`, `window_sum`

## Non-SELECT Source Contracts

Command-plan contracts prove these source-derived nodes with optimized JSON from
real command SQL.

`ALTER`, `COPY_TO_FILE`, `CREATE_INDEX`, `CREATE_MACRO`, `CREATE_SCHEMA`,
`CREATE_SEQUENCE`, `CREATE_TABLE`, `CREATE_TYPE`, `CREATE_VIEW`, `DELETE`,
`DROP`, `EXECUTE`, `EXPRESSION_GET`, `INSERT`, `LOAD`, `MERGE_INTO`,
`PRAGMA`, `PREPARE`, `RESET`, `SET`, `TRANSACTION`, `UPDATE`,
`UPDATE_EXTENSIONS`, `VACUUM`

Source-evidence contracts cover these nodes with exact DuckDB source evidence
because they are statement-binder, internal-column-data, decorrelation-lowered,
or extension-only paths instead of optimized SELECT reconstruction handlers.

`ATTACH`, `CHUNK_GET`, `CONNECT`, `COPY_DATABASE`, `CREATE_SECRET`,
`CREATE_TRIGGER`, `CUSTOM_OP`, `DEPENDENT_JOIN`, `DETACH`, `DISCONNECT`,
`EXPLAIN`, `EXPORT`

Optimized-lowering contracts cover these source names by proving their real SQL
lowers into optimized nodes already covered by the SELECT matrix.

`GET`, `PIVOT`

## Handler Semantic Contracts

These contracts enforce parse, execution, placeholder, alias, projection, and
row-equivalence behavior for the reconstruction paths covered by the full
contract matrix.

| Contract | Contract case names | Tests |
| --- | --- | --- |
| SEMI/ANTI joins expose RHS-only predicates after the join | `exists_semi_rhs_filter`, `not_exists_anti_join` | `test_reconstruction_contract_cases[exists_semi_rhs_filter-*]`, `test_reconstruction_contract_cases[not_exists_anti_join-*]` |
| Correlated `DELIM_GET` predicates are dropped or rewritten incorrectly | `correlated_select`, `correlated_where`, `correlated_having` | `test_reconstruction_contract_cases[correlated_select-*]`, `test_reconstruction_contract_cases[correlated_where-*]`, `test_reconstruction_contract_cases[correlated_having-*]` |
| Scalar `ORDER BY` / `LIMIT` rewrites leak slots or wrong aggregate SQL | `limit_offset_window`, `argmax_scalar_limit` | `test_reconstruction_contract_cases[limit_offset_window-*]`, `test_reconstruction_contract_cases[argmax_scalar_limit-*]` |
| Repeated base aliases lose alias lineage or predicates | `repeated_alias_filter`, `repeated_base_cte_outer_alias`, `multiple_ctes_same_base_aliases` | `test_reconstruction_contract_cases[repeated_alias_filter-*]`, `test_reconstruction_contract_cases[repeated_base_cte_outer_alias-*]`, `test_reconstruction_contract_cases[multiple_ctes_same_base_aliases-*]` |
| Generated aliases collide with user aliases | `generated_alias_collision` | `test_reconstruction_contract_cases[generated_alias_collision-*]` |
| Derived table and CTE aliases lose lineage | `derived_cte_alias_lineage`, `non_materialized_cte`, `join_derived_rhs_lineage` | `test_reconstruction_contract_cases[derived_cte_alias_lineage-*]`, `test_reconstruction_contract_cases[non_materialized_cte-*]`, `test_reconstruction_contract_cases[join_derived_rhs_lineage-*]` |
| Temporary view serialization emits wrong semantics or duplicate names | `duplicate_temp_view_names_multi_temp` | `test_reconstruction_contract_cases[duplicate_temp_view_names_multi_temp-*]`, `test_temp_view_uniqueness_guard_rejects_duplicate_names`, `test_ordered_temp_view_contract_executes_views_before_final_select` |
| Projection lineage is wrong through aggregate, window, join, set, and empty nodes | `projection_over_aggregate_lineage`, `projection_over_window_lineage`, `join_derived_rhs_lineage`, `set_operation_output_name_lineage`, `slot_mapping_multi_projection`, `empty_result_projection_schema` | `test_reconstruction_contract_cases[projection_over_aggregate_lineage-*]`, `test_reconstruction_contract_cases[projection_over_window_lineage-*]`, `test_reconstruction_contract_cases[join_derived_rhs_lineage-*]`, `test_reconstruction_contract_cases[set_operation_output_name_lineage-*]`, `test_reconstruction_contract_cases[slot_mapping_multi_projection-*]`, `test_reconstruction_contract_cases[empty_result_projection_schema-*]`, `test_all_contract_cases_have_expected_output_columns` |
| Empty results do not preserve required output schema | `empty_result_projection_schema` | `test_reconstruction_contract_cases[empty_result_projection_schema-*]`, `test_audit_empty_result_preserves_projection_schema`, `test_empty_rhs_single_join_fails_instead_of_emitting_invalid_placeholder` |
| Internal aggregate functions leak into generated SQL | `aggregate_internal_function` | `test_reconstruction_contract_cases[aggregate_internal_function-*]`, `test_audit_aggregate_internal_functions_are_executable`, `test_internal_placeholder_guard_rejects_forbidden_tokens` |
| Set operations need full row and column contracts | `union`, `union_all`, `intersect`, `except`, `set_operation_output_name_lineage` | `test_reconstruction_contract_cases[union-*]`, `test_reconstruction_contract_cases[union_all-*]`, `test_reconstruction_contract_cases[intersect-*]`, `test_reconstruction_contract_cases[except-*]`, `test_reconstruction_contract_cases[set_operation_output_name_lineage-*]`, `test_union_reconstructs_set_operation` |
| Window functions need full row and column contracts | `window_row_number`, `window_rank`, `window_dense_rank`, `window_sum`, `window_lag`, `window_first_value`, `window_rows_frame`, `projection_over_window_lineage` | `test_reconstruction_contract_cases[window_row_number-*]`, `test_reconstruction_contract_cases[window_rank-*]`, `test_reconstruction_contract_cases[window_dense_rank-*]`, `test_reconstruction_contract_cases[window_sum-*]`, `test_reconstruction_contract_cases[window_lag-*]`, `test_reconstruction_contract_cases[window_first_value-*]`, `test_reconstruction_contract_cases[window_rows_frame-*]`, `test_reconstruction_contract_cases[projection_over_window_lineage-*]` |
| Joins with nested conditions and null-safe equality need row equivalence | `nested_join_conditions`, `is_not_distinct_join`, `any_join_arbitrary_condition`, `plain_inner_join_node`, `cross_product_join` | `test_reconstruction_contract_cases[nested_join_conditions-*]`, `test_reconstruction_contract_cases[is_not_distinct_join-*]`, `test_reconstruction_contract_cases[any_join_arbitrary_condition-*]`, `test_reconstruction_contract_cases[plain_inner_join_node-*]`, `test_reconstruction_contract_cases[cross_product_join-*]` |
| Join type variants need optimized `extra_info` and row-equivalence contracts | `left_join_variant`, `right_join_variant`, `full_join_variant` | `test_reconstruction_contract_cases[left_join_variant-*]`, `test_reconstruction_contract_cases[right_join_variant-*]`, `test_reconstruction_contract_cases[full_join_variant-*]`, `test_contract_cases_reach_required_extra_info_values` |
| Scalar and MARK subquery joins need valid SQL and row equivalence | `scalar_subquery_projection_join`, `in_subquery_projection_boolean`, `exists_subquery_projection_boolean`, `not_in_null_sensitive` | `test_reconstruction_contract_cases[scalar_subquery_projection_join-*]`, `test_reconstruction_contract_cases[in_subquery_projection_boolean-*]`, `test_reconstruction_contract_cases[exists_subquery_projection_boolean-*]`, `test_reconstruction_contract_cases[not_in_null_sensitive-*]`, `test_contract_cases_reach_required_extra_info_values` |
| Subquery forms need full row equivalence | `exists_semi_rhs_filter`, `not_exists_anti_join`, `in_subquery`, `not_in_subquery`, `any_quantified_subquery`, `correlated_select`, `correlated_where`, `correlated_group_by`, `correlated_having` | `test_reconstruction_contract_cases[exists_semi_rhs_filter-*]`, `test_reconstruction_contract_cases[not_exists_anti_join-*]`, `test_reconstruction_contract_cases[in_subquery-*]`, `test_reconstruction_contract_cases[not_in_subquery-*]`, `test_reconstruction_contract_cases[any_quantified_subquery-*]`, `test_reconstruction_contract_cases[correlated_select-*]`, `test_reconstruction_contract_cases[correlated_where-*]`, `test_reconstruction_contract_cases[correlated_group_by-*]`, `test_reconstruction_contract_cases[correlated_having-*]` |

## Explicit Blocker Categories

The test suite maps these requested blocker categories to concrete contract
cases and fails if any category loses its case coverage.

`SEMI/ANTI joins`, `DELIM_GET correlated predicates`,
`ORDER BY LIMIT OFFSET scalar rewrites`, `repeated base table aliases`,
`derived table and CTE alias lineage`, `generated alias collisions`,
`duplicate temp view names`, `projection lineage`,
`aggregate internal functions`, `set operations`, `window functions`,
`UNNEST`, `SAMPLE`, `CTE and CTE_SCAN`, `REC_CTE`, `POSITIONAL_JOIN`,
`ASOF_JOIN`, `nested joins`, `correlated subqueries`, `subquery forms`,
`empty result`

## Coverage Gates

These tests fail if a contract case is missing, not source-derived, not tied to
optimized JSON, or not tied to output-column assertions.

- `test_all_review_contract_case_names_are_present`
- `test_reconstruction_contract_case_names_are_unique`
- `test_required_review_contract_case_names_are_unique`
- `test_mandatory_plan_shape_case_names_are_unique`
- `test_mandatory_plan_shape_cases_are_in_contract_matrix`
- `test_all_contract_cases_reach_required_optimized_nodes`
- `test_all_contract_cases_dump_serializable_optimized_json`
- `test_contract_cases_reach_required_extra_info_values`
- `test_required_extra_info_assertions_have_contract_cases`
- `test_all_contract_cases_have_required_optimized_node_assertions`
- `test_all_required_optimized_node_assertions_have_contract_cases`
- `test_all_contract_cases_have_expected_output_columns`
- `test_all_expected_output_columns_have_contract_cases`
- `test_command_plan_contract_cases_reach_required_nodes`
- `test_command_plan_contract_cases_dump_serializable_json`
- `test_every_source_node_has_select_or_command_contract_coverage`
- `test_all_accounted_source_nodes_exist_in_duckdb_source`
- `test_source_node_accounting_buckets_are_disjoint`
- `test_command_contract_case_names_are_unique`
- `test_command_contract_node_names_are_unique`
- `test_command_contract_nodes_exist_in_duckdb_source`
- `test_source_evidence_node_names_are_unique`
- `test_source_evidence_case_keys_are_unique`
- `test_source_evidence_nodes_exist_in_manifest`
- `test_source_evidence_nodes_exist_in_duckdb_source`
- `test_lowered_contract_node_names_are_unique`
- `test_lowered_contract_nodes_exist_in_manifest`
- `test_pivot_contract_proves_optimized_lowering`
- `test_get_source_node_is_covered_by_dynamic_scan_contract`
- `test_observed_select_nodes_have_handlers`
- `test_observed_select_nodes_have_direct_contract_cases`
- `test_synthetic_only_handler_names_are_unique`
- `test_synthetic_only_handlers_have_source_evidence`
- `test_synthetic_only_handler_source_cases_are_exact`
- `test_explicit_root_preservation_cases_match_contract_matrix`
- `test_explicit_root_preservation_case_names_are_unique`
- `test_only_approved_cases_preserve_original_sql`
- `test_native_reconstruction_does_not_preserve_unapproved_cases`
- `test_native_preserved_cases_return_exact_original_sql`
- `test_native_preserved_case_names_are_real_contract_cases`
- `test_native_preserved_case_names_have_evidence_assertions`
- `test_native_preservation_evidence_points_to_approved_cases`
- `test_native_preserved_cases_have_plan_or_sql_evidence`
- `test_coverage_document_listed_gates_exist_in_test_file`
- `test_coverage_document_gate_names_are_complete`
- `test_coverage_document_lists_every_contract_case`
- `test_coverage_document_lists_every_command_contract_node`
- `test_coverage_document_lists_every_source_evidence_node`
- `test_coverage_document_lists_every_lowered_contract_node`
- `test_coverage_document_lists_every_observed_handler_node`
- `test_coverage_document_lists_every_explicit_user_requirement_group`
- `test_coverage_document_pass_count_is_current`
- `test_explicit_user_requirement_groups_have_contract_cases`
- `test_explicit_user_requirement_cases_have_required_node_assertions`
- `test_explicit_user_requirement_cases_have_output_column_assertions`
- `test_reconstruction_contract_cases_use_full_ladder_helper`
