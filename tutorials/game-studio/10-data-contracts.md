# 10. What the results establish and their data contracts

Across the first five questions, FedQ moves work according to what each
operation needs. Predicates that need only one source's fields can stay with that
source. Aggregations that need only those fields can stay there too. Work
that combines owners remains in the coordinator in these plans. Correlated
existence tests first become joins, so the same placement machinery can
handle them. A datasource can appear in several fragments when the query
needs different grains or filters from it.

Dialect rendering makes those source operations executable in the relevant
upstream. Arrow gives their results a common representation. The coordinator
then performs the remaining joins, calculations, and ordering with one
expression-semantics contract. The client submits the business query once
and receives its named result columns through one connection.

These are observed plans over a fixed synthetic dataset, not latency claims.
A live plan can change with statistics, settings, placement decisions, and
materialized relations. Row/cost annotations are omitted from the structural
trees here; a live EXPLAIN's estimates are not measured execution time or
actual result counts. Correctness must survive a different physical plan.
The read-query verifier compares all seven baseline answers with DuckDB. The
accelerator harness also checks the original and post-insert cached/live states.

The examples also rely on explicit data contracts: shared player identity,
one registration per player, a fixed observation period, and unchanged
subscription plans. A live changing subscription needs effective-dated plan
history to attribute a ticket to the plan at the time it opened. Reads from
separate operational systems should not be described as a globally atomic
snapshot merely because one SQL statement combines them.

---

[Contents](README.md) | [Previous: 09-accelerator-measurements.md](09-accelerator-measurements.md) | [Next: Substitution boundaries](11-when-substitution-does-not-apply.md)
