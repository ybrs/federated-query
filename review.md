# Code Review

## High Severity

### 1. DuckDB metadata queries are vulnerable to SQL injection and break on quoted identifiers
The DuckDB connector builds metadata queries by interpolating the schema, table, and column names directly into SQL strings. Those identifiers ultimately come from user queries that the binder resolves, so this lets a crafted table reference inject arbitrary SQL. Even if you assume trusted names, any schema/table/column that requires quoting (mixed case, spaces, reserved words) will cause these queries to fail, so production deployments cannot safely introspect metadata. These calls need to use parameter binding or explicit identifier quoting before we can trust this connector in production.【F:federated_query/datasources/duckdb.py†L120-L151】

### 2. Aggregate output columns are misaligned when SELECT lists aggregates before group keys
`Parser._extract_aggregates_from_select` forwards the SELECT list in its original order to the physical aggregate plan. The physical operator then assumes the first `len(group_by)` positions correspond to grouping keys and pulls values from `group_key[col_idx]`. If a query orders the SELECT list differently (for example `SELECT SUM(price), category FROM ... GROUP BY category`), the first slot actually belongs to the aggregate expression, so the executor will return the group key value under the aggregate alias and the aggregation result is shifted or crashes. This breaks correctness for perfectly valid SQL and needs to be fixed before production use.【F:federated_query/parser/parser.py†L470-L498】【F:federated_query/plan/physical.py†L734-L749】

### 3. LEFT and FULL OUTER JOINs raise `NotImplementedError`
Both hash and nested-loop join operators call `_create_left_outer_batch/_row` when a build/probe side has no match, but those helpers currently raise `NotImplementedError`. Any LEFT or FULL OUTER JOIN that actually needs to emit unmatched rows will therefore crash at runtime, making those join types unusable. That is a blocker for production adoption and needs resolution.【F:federated_query/plan/physical.py†L309-L388】【F:federated_query/plan/physical.py†L438-L553】

