# Project Review

## Issue 1: Physical operators access Arrow columns by name (Severity: Critical)
Several physical operators call `RecordBatch.column` with a string column name, but PyArrow's `RecordBatch.column` expects an integer index. This raises a `TypeError` during execution, breaking projections, filters, joins, and other downstream operators. Examples include `PhysicalProject._project_batch` (line 146), `PhysicalFilter._evaluate_value` (line 247), and `PhysicalHashJoin._extract_key_values` (line 339) in `federated_query/plan/physical.py`.

## Issue 2: Hash aggregate output order does not match the SELECT list (Severity: High)
`PhysicalHashAggregate._extract_column_values` assumes the first `len(group_by)` output columns correspond to the GROUP BY keys in GROUP BY order, with all aggregates following. When the SELECT list reorders those expressions (e.g., `SELECT COUNT(*), customer_id ... GROUP BY customer_id` or swaps GROUP BY columns), the operator emits mismatched data—grouping keys appear under the wrong aliases and types. The incorrect ordering logic lives in `federated_query/plan/physical.py` lines 741-749.

## Issue 3: Hash aggregate returns no row for empty inputs without GROUP BY (Severity: High)
For aggregates without grouping (e.g., `SELECT COUNT(*) FROM table`) on empty inputs, `_execute_streaming` never materializes the default accumulator because no rows are processed, leaving the hash table empty. `_finalize_aggregates` then returns an empty batch, and `execute` suppresses it because `num_rows` is zero, so the query yields no result instead of a row with zeros/nulls. The relevant logic is in `federated_query/plan/physical.py` lines 595-603, 621-626, and 730-732.
