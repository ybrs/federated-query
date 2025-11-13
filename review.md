# Federated Query Engine Review

## Critical Issues

1. **PyArrow column access uses string keys, causing runtime failures in multiple operators**  
   The execution layer assumes `pyarrow.RecordBatch.column` accepts column names, but the API expects positional indexes. As a result, projections, filters, joins, and aggregations call `batch.column(expr.column)` or `table.column(col_name)` with string arguments, which raises `TypeError: an integer is required` when the query reaches the executor. 【F:federated_query/plan/physical.py†L141-L152】【F:federated_query/plan/physical.py†L222-L248】【F:federated_query/plan/physical.py†L330-L343】【F:federated_query/plan/physical.py†L658-L681】  
   *Example SQL:* `SELECT o.customer_id FROM postgres.public.orders o WHERE o.amount > 0;` fails once the physical `PhysicalProject` or `PhysicalFilter` runs, because both operators attempt to pull Arrow columns by name.  
   *Suggested fix:* look up the numeric index via `batch.schema.get_field_index(name)` (or use `batch.column(i)`/`batch.select`) before dereferencing, and apply the same fix everywhere a column is fetched by name.

2. **Star projections drop subsequent expressions**  
   `PhysicalProject._project_batch` returns the entire input batch as soon as it encounters a `ColumnRef('*')`, which prevents any later expressions from being produced. The executor therefore ignores requested computed columns or re-aliases whenever `*` appears anywhere in the select list. 【F:federated_query/plan/physical.py†L141-L152】  
   *Example SQL:* `SELECT *, o.amount * 2 AS amount_double FROM postgres.public.orders o;` returns only the original table columns—the `amount_double` column is silently discarded because the `*` short-circuits the projection loop.  
   *Suggested fix:* expand `*` into the underlying field list instead of returning early, then continue evaluating the remaining select expressions.

3. **Left/full joins raise `NotImplementedError` in the executor**  
   The hash and nested-loop join operators advertise support for `JoinType.LEFT`/`JoinType.FULL`, but their outer-row builders are explicitly `NotImplemented`. When the optimizer emits a left join, execution fails as soon as an unmatched row appears. 【F:federated_query/plan/physical.py†L306-L388】  
   *Example SQL:* `SELECT c.id, o.id FROM duckdb.main.customers c LEFT JOIN postgres.public.orders o ON c.id = o.customer_id;` throws `NotImplementedError("LEFT OUTER JOIN not yet fully implemented")` once a customer has no orders.  
   *Suggested fix:* implement outer-row materialization (NULL padding on the non-build side) before marking the join as supporting LEFT/FULL semantics, or downgrade those join types to a planning error until the executor can produce the correct output.

## Enhancements & Improvements

1. **Add ORDER BY / OFFSET support to the logical parser**  
   `_convert_select` currently wires together FROM, WHERE, GROUP BY, HAVING, SELECT, and LIMIT, but there is no hook for ORDER BY (or OFFSET). Users cannot issue a basic `ORDER BY` query even though both data sources support it. 【F:federated_query/parser/parser.py†L40-L120】  
   *Example SQL:* `SELECT o.id, o.amount FROM postgres.public.orders o ORDER BY o.amount DESC;` is rejected during parsing.  
   *Improvement:* add an `_build_order_by_clause` step that creates a `Sort` logical node, plumbs it through binding/physical planning, and respects OFFSET support where available.

2. **Preserve real column types when introspecting PostgreSQL schemas**  
   `PostgreSQLDataSource._build_arrow_fields` currently hard-codes every field as `pa.string()`, so consumers lose numeric/temporal typing information even though PostgreSQL exposes it via the cursor description. 【F:federated_query/datasources/postgresql.py†L248-L275】  
   *Example SQL:* `SELECT id, amount, order_date FROM postgres.public.orders;` returns an Arrow schema where every column is `string`, breaking vectorized arithmetic and predicate pushdown.  
   *Improvement:* map psycopg2 type codes (`desc[1]`) to Arrow types (e.g., `pa.int64()`, `pa.float64()`, `pa.timestamp('us')`) so the executor and downstream clients maintain correct typing.

