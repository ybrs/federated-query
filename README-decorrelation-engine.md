# Decorrelation Engine

The decorrelation engine rewrites every subquery in a bound logical plan into a
filters, and projections. After it runs, **no subquery expression node remains**
in the plan: an `EXISTS`/`IN`/`ANY`/`ALL` becomes a join, and a scalar

This matters because a flat join plan is something the rest of the engine
already knows how to optimize, push down to a single source, or split across
sources. A decorrelated `EXISTS` "is just a join", so pushing it to a source is
the same problem as ordinary join pushdown.

> **Design rule:** physical subquery planning is a last resort, ideally never
> reached. Decorrelate fully, then push the resulting join.

  physical plan` (`federated_query/optimizer/decorrelation.py`).
- **Failure mode:** every unsupported shape **fails fast** with a clear
  (see [Unsupported cases](#unsupported-cases--fail-fast)).
- **Tests:** every supported pattern is exercised in `tests/e2e_decorrelation/`
  and `tests/e2e_pushdown/test_subqueries.py`; every unsupported pattern is
  pinned in `tests/e2e_decorrelation/test_error_cases.py`.

## What "no subquery" means: expression vs. derived table

Decorrelation removes subquery **expressions**, not every parenthesized
`SELECT`. The two are different things that share syntax:

  evaluated on its own; logically it runs once per outer row. **These are what
  decorrelation eliminates.**
  self-contained (references nothing outer), e.g.
  relation: computed once, then joined. **These are allowed and expected.**

**Litmus test:** does the parenthesized `SELECT` reference a column of the
*outer* query? If yes, it is a (correlated) subquery expression; if no, it is a
derived table.

join input (its own operator subtree). The output examples below show
reappear only because that subtree is serialized back to SQL to push to a

## How to read the examples

Each example shows the **input SQL** and the **decorrelated query that is pushed
to the source** (captured from `EXPLAIN (FORMAT JSON)`). The pushed SQL is the
clearest view of the rewrite: a subquery in the input becomes a join in the
output. Names like `__subq_0_v0` (value), `__subq_0_k0` (correlation key), and
`__subq_0_g0` (group key) are **engine-generated synthetic columns**; `subq_0`
is a derived-relation alias.

Examples use two single-source (DuckDB) tables:

```sql
orders   (order_id, product_id, customer_id, quantity, price, status, region, created_at)
products (id, category, name, price, base_price, status)
```

---


A correlated or uncorrelated `EXISTS` / `IN` / `= ANY` keeps the outer rows that
match (a **SEMI** join). The negated forms `NOT EXISTS` / `NOT IN` / `> ALL`
keep the rows that do **not** match (an **ANTI** join). The correlation
predicate (`P.id = O.product_id`) becomes the join's `ON` condition.


```sql
-- input
SELECT order_id FROM orders O
WHERE EXISTS (SELECT 1 FROM products P WHERE P.id = O.product_id AND P.price > 100);

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS O
SEMI JOIN (SELECT P.id AS "__subq_0_k0" FROM "main"."products" AS P
           WHERE (P.price > 100)) AS subq_0
  ON (__subq_0_k0 = O.product_id);
```


```sql
-- input
SELECT order_id FROM orders O
WHERE NOT EXISTS (SELECT 1 FROM products P WHERE P.id = O.product_id);

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS O
ANTI JOIN (SELECT P.id AS "__subq_1_k0" FROM "main"."products" AS P) AS subq_0
  ON (__subq_1_k0 = O.product_id);
```


```sql
-- input
SELECT order_id FROM orders
WHERE product_id IN (SELECT id FROM products WHERE price > 100);

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
SEMI JOIN (SELECT products.id AS "__subq_2_v0" FROM "main"."products" AS products
           WHERE (products.price > 100)) AS subq_0
  ON (product_id = __subq_2_v0);
```


`NOT IN` follows SQL three-valued logic: a `NULL` on either side makes the
comparison `UNKNOWN`, which must drop the outer row. The join condition is made
NULL-aware so the ANTI join removes those rows.

```sql
-- input
SELECT order_id FROM orders
WHERE product_id NOT IN (SELECT id FROM products WHERE status = 'discontinued');

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
ANTI JOIN (SELECT products.id AS "__subq_3_v0" FROM "main"."products" AS products
           WHERE (products.status = 'discontinued')) AS subq_0
  ON (((product_id = __subq_3_v0) OR (product_id IS NULL)) OR (__subq_3_v0 IS NULL));
```


```sql
-- input
SELECT order_id FROM orders
WHERE price = ANY (SELECT price FROM orders WHERE region = 'EU');

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
SEMI JOIN (SELECT orders.price AS "__subq_4_v0" FROM "main"."orders" AS orders
           WHERE (orders.region = 'EU')) AS subq_0
  ON (price = __subq_4_v0);
```


engine keeps outer rows with no violating match (ANTI join), counting a `NULL`
comparison as a violation.

```sql
-- input
SELECT order_id FROM orders
WHERE price > ALL (SELECT price FROM orders WHERE region = 'US');

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
ANTI JOIN (SELECT orders.price AS "__subq_5_v0" FROM "main"."orders" AS orders
           WHERE (orders.region = 'US')) AS subq_0
  ON (((NOT (price > __subq_5_v0)) OR (price IS NULL)) OR (__subq_5_v0 IS NULL));
```

---


A scalar subquery returns one row and one column. It becomes a `LEFT JOIN` to an
aggregate so the outer row keeps its value even when the subquery matches nothing
(a `LEFT JOIN` yields `NULL` there). A correlated scalar joins on the
correlation key; an uncorrelated one joins `ON TRUE`.


```sql
-- input
SELECT order_id FROM orders WHERE price = (SELECT MAX(price) FROM orders);

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
LEFT JOIN (SELECT MAX(orders.price) AS "__subq_6_v0" FROM "main"."orders" AS orders) AS subq_0
  ON TRUE
WHERE (price = __subq_6_v0);
```


The correlation (`region = O.region`) becomes a **group key** of the aggregate
(`GROUP BY orders.region`) and the join key (`O.region = __subq_7_g0`), so each
outer row reads the aggregate computed for its own group.

```sql
-- input
SELECT order_id FROM orders O
WHERE price > (SELECT AVG(price) FROM orders WHERE region = O.region);

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS O
LEFT JOIN (SELECT orders.region AS "__subq_7_g0", AVG(orders.price) AS "__subq_7_v0"
           FROM "main"."orders" AS orders GROUP BY orders.region) AS subq_0
  ON (O.region = __subq_7_g0)
WHERE (price > __subq_7_v0);
```


A scalar subquery can appear in the projection. A correlated `COUNT` is wrapped
reads `NULL`, which must become `0` to match SQL `COUNT` semantics.

```sql
-- input
SELECT order_id,
       (SELECT COUNT(*) FROM products P WHERE P.id = O.product_id) AS pc
FROM orders O;

-- decorrelated
SELECT order_id AS "order_id", COALESCE(__subq_8_v0, 0) AS "pc"
FROM "main"."orders" AS O
LEFT JOIN (SELECT P.id AS "__subq_8_g0", COUNT(*) AS "__subq_8_v0"
           FROM "main"."products" AS P GROUP BY P.id) AS subq_0
  ON (O.product_id = __subq_8_g0);
```

### Cardinality guard

A correlated scalar subquery that the engine cannot **prove** returns at most one
row per key is wrapped in a runtime `SingleRowGuard`. If a key actually produces
more than one row, execution raises `CardinalityViolationError: more than one
(A subquery aggregated by its correlation key is provably single-row and is not
guarded.)


the first `n` rows **per correlation key** (an order-aware `GroupedLimit`). The
correlation column becomes the per-key partition; the `ORDER BY` decides which
row survives. `LIMIT 1` makes it provably single-row, so **no cardinality guard
is added**.

```sql
-- input: companies has duplicate ids (dirty data); LIMIT 1 picks one
SELECT o.order_id,
       (SELECT c.name FROM companies c
        WHERE c.id = o.company_id ORDER BY c.name LIMIT 1) AS company_name
FROM orders o;
```

```
-- decorrelated plan (no subquery expression; GroupedLimit runs in the merge engine)
Projection [ o.order_id, __subq_0_v0 AS company_name ]
```

An `ORDER BY` column that is **not** the selected value (e.g.
column the `GroupedLimit` sorts on. Scope: this covers **equi-correlation**
(`c.id = o.company_id`); a non-equi correlation still needs a general dependent
join. See the [cost note](#cost-note-making-the-decorrelated-join-cheap) on
keeping the inner scan from going wide.


When a scalar subquery's correlation is **non-equality** (`<`/`>`/`!=`) and it
aggregates or `LIMIT`s, it cannot flatten to a per-key join (an outer row maps
dependent join evaluated per outer row. No subquery *expression* remains (a
the pattern-based forms can't cover.

```sql
-- input: priciest product still cheaper than each order (non-equi: p.price < o.price)
SELECT o.order_id,
       (SELECT MAX(p.price) FROM products p WHERE p.price < o.price) AS m
FROM orders o;

-- decorrelated (pushed to the source as one query)
SELECT o.order_id AS "order_id", __subq_1_v0 AS "m"
FROM "main"."orders" AS o
LEFT JOIN LATERAL (SELECT MAX(p.price) AS "__subq_1_v0"
                   FROM "main"."products" AS p WHERE (p.price < o.price)) AS subq_0
  ON TRUE;
```

Scope: **same-source** pushes the whole `LATERAL` to the one source. A
**cross-source** `LATERAL` (left on A, subquery on B) runs in the in-memory
DuckDB **merge engine**: the left and the right's base relation are materialized
into Arrow and the merge engine decorrelates the `LATERAL`. The base relation is
*sound superset* (the merge engine re-applies the exact correlation), so it only
shrinks transfer, never the answer. Only a single base relation per cross-source
`LATERAL` is supported today; more than one fails fast.

A **user-written** `LATERAL` is also accepted (parsed to the same `LateralJoin`
node) and bound with the left relation in scope, so its correlation resolves:

```sql
SELECT o.order_id, t.name
FROM orders o
LEFT JOIN LATERAL (SELECT p.name FROM products p
                   WHERE p.id = o.product_id ORDER BY p.name LIMIT 1) t ON true;
-- LEFT JOIN LATERAL keeps non-matching outer rows (value NULL); a comma /
-- CROSS lateral drops them (INNER). Same-source push only, as above.
```

---

## Value subqueries with a shaped body

For an **uncorrelated** `IN` / `ANY` / `ALL`, the subquery body may itself be a
grouped or ordered/limited relation. The body is kept intact as the SEMI/ANTI
join's derived relation; only its output column is renamed to the join value.


The `HAVING` is preserved on the derived relation (it is **not** demoted to a
`WHERE`); aggregate references it carries are rendered as real aggregate calls.

```sql
-- input
SELECT order_id FROM orders
WHERE region IN (SELECT region FROM orders GROUP BY region HAVING COUNT(*) >= 2);

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
SEMI JOIN (SELECT orders.region AS "__subq_9_v0" FROM "main"."orders" AS orders
           GROUP BY orders.region HAVING (COUNT(*) >= 2)) AS subq_0
  ON (region = __subq_9_v0);
```


`ORDER BY` + `LIMIT` shape the result set (a "top-N" set), so both are kept
inside the derived relation.

```sql
-- input
SELECT order_id FROM orders
WHERE product_id IN (SELECT id FROM products ORDER BY price DESC LIMIT 5);

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
SEMI JOIN (SELECT products.id AS "__subq_10_v0" FROM "main"."products" AS products
           ORDER BY products.price DESC NULLS FIRST LIMIT 5) AS subq_0
  ON (product_id = __subq_10_v0);
```


A set-operation subquery body is bound (both branches), kept intact through
decorrelation, and rendered as a derived relation; the engine's
no-subquery-expression invariant still holds (the set operation is a relation).
its source and the SEMI join in the merge engine.

```sql
-- input
SELECT order_id FROM orders
WHERE region IN (SELECT region FROM orders WHERE price > 100
                 UNION
                 SELECT region FROM orders WHERE quantity > 10);

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
SEMI JOIN (SELECT region AS "__subq_0_v0"
           FROM (SELECT orders.region FROM "main"."orders" AS orders WHERE (orders.price > 100)
                 UNION
                 SELECT orders.region FROM "main"."orders" AS orders WHERE (orders.quantity > 10)
                ) AS subq_0) AS subq_0
  ON (region = __subq_0_v0);
```

---

## Composition


Each subquery in the `WHERE` becomes its own join; they chain left-deep.

```sql
-- input
SELECT order_id FROM orders
WHERE product_id IN (SELECT id FROM products WHERE price > 100)
  AND region     IN (SELECT DISTINCT region FROM orders WHERE quantity > 10);

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
SEMI JOIN (SELECT products.id AS "__subq_11_v0" FROM "main"."products" AS products
           WHERE (products.price > 100)) AS subq_0 ON (product_id = __subq_11_v0)
SEMI JOIN (SELECT orders.region AS "__subq_12_v0" FROM "main"."orders" AS orders
           WHERE (orders.quantity > 10)) AS subq_1 ON (region = __subq_12_v0);
```


A subquery inside a subquery is decorrelated innermost-first; the inner join
lives inside the outer join's derived relation.

```sql
-- input
SELECT order_id FROM orders
WHERE product_id IN (
  SELECT id FROM products WHERE price > (SELECT AVG(price) FROM products));

-- decorrelated
SELECT order_id AS "order_id"
FROM "main"."orders" AS orders
SEMI JOIN (SELECT products.id AS "__subq_13_v0" FROM "main"."products" AS products
           LEFT JOIN (SELECT AVG(products.price) AS "__subq_14_v0"
                      FROM "main"."products" AS products) AS subq_0 ON TRUE
           WHERE (products.price > __subq_14_v0)) AS subq_0
  ON (product_id = __subq_13_v0);
```

---

## Supported cases at a glance

| Input pattern | Decorrelated form |
|---|---|
| `EXISTS` (correlated / uncorrelated) | SEMI join |
| `NOT EXISTS` | ANTI join |
| `= ANY` / `op ANY` / `SOME` | SEMI join |
| `op ALL` | ANTI join (NULL-aware) |
| Scalar in SELECT / WHERE / HAVING | LEFT join (+ `COALESCE` for `COUNT`) |
| Non-equi correlated scalar (aggregate / LIMIT), same source | `LATERAL` (dependent) join |
| Correlated subquery (`= outer.col`) | correlation becomes the join key |
| Multiple subqueries | chained joins |
| Nested subqueries | nested joins, innermost-first |

---


These patterns are unnestable in principle (a general dependent join would
handle them) but the current pattern-based engine **rejects them with a clear
error** rather than guessing. Each example below shows the input and the exact
error it raises; each is pinned by a test in
`tests/e2e_decorrelation/test_error_cases.py`.

### Shape / structure limits

unsupported.

```sql
SELECT order_id FROM orders O
WHERE price = (SELECT price FROM orders X WHERE X.region = O.region
               ORDER BY price LIMIT 1 OFFSET 2);
-- DecorrelationError: OFFSET in a correlated subquery is not supported
```


```sql
SELECT order_id FROM orders
WHERE price = (SELECT price, quantity FROM orders LIMIT 1);
-- DecorrelationError: Scalar subquery must return exactly one column
-- Needs: row-value (tuple) comparison so the outer side can match multiple subquery columns.
```


```sql
SELECT order_id FROM orders O
WHERE price > (SELECT * FROM products P WHERE P.id = O.product_id);
-- DecorrelationError: SELECT * subqueries cannot be used as value subqueries
-- Needs: star expansion against catalog metadata (resolve * to explicit columns) before decorrelation.
```

**Subquery in a `GROUP BY` position.**

```sql
SELECT COUNT(*) FROM orders GROUP BY (SELECT MAX(price) FROM products);
-- DecorrelationError: Subqueries in GROUP BY are not supported
-- Needs: general dependent-join handling for subqueries outside WHERE/SELECT (hoist as a join input, group by its output column).
```

### Correlation limits

levels up (the canonical dependent-join case).

```sql
SELECT order_id FROM orders O
WHERE EXISTS (SELECT 1 FROM products P
              WHERE EXISTS (SELECT 1 FROM customers C
                            WHERE C.customer_id = O.customer_id));
-- DecorrelationError: Correlated reference O.customer_id is in an unsupported position
-- Needs: a general dependent join (apply / magic-set decorrelation) that threads correlation through intermediate relations, not just the immediate parent.
```

key actually returns more than one row trips the cardinality guard during
execution.

```sql
SELECT customer_id,
       (SELECT price FROM orders X WHERE X.customer_id = O.customer_id) AS p
FROM orders O;
-- CardinalityViolationError: Scalar subquery returned more than one row for a correlation key (1,)
```

### Operator limits

**Multi-column quantified comparison** (`ANY` / `ALL`).

```sql
SELECT order_id FROM orders WHERE price = ANY (SELECT price, quantity FROM orders);
-- DecorrelationError: Quantified comparison subquery must return one column
-- Needs: row-value (tuple) comparison in the quantified-join condition.
```


```sql
SELECT name FROM products WHERE NOT (name LIKE ALL (SELECT name FROM products));
-- DecorrelationError: Cannot negate quantified operator: LIKE
```

### Syntax not yet parsed/bound

These are rejected before decorrelation, at parse/bind time.

```sql
-- Window function in a subquery
SELECT order_id,
       (SELECT ROW_NUMBER() OVER (ORDER BY price) FROM products
        WHERE id = O.product_id LIMIT 1) AS rnk
FROM orders O;
-- ValueError: Unsupported expression type: <class 'sqlglot.expressions.query.Window'>
-- Needs: window-function binding (OVER) and a physical window operator.

-- CTE (any WITH, including recursive)
WITH RECURSIVE t AS (
  SELECT order_id FROM orders WHERE order_id = 1
  UNION ALL SELECT order_id FROM orders
) SELECT * FROM t;
-- ValueError: WITH clauses (CTEs) are not supported yet
-- Needs: CTE binding as named relations (plus a recursive fixpoint operator for WITH RECURSIVE).
```

> [Value subqueries with a shaped body](#value-subqueries-with-a-shaped-body))
> and the **correlated** scalar pick-one/dedup form (see
> are supported. Only the equi-correlation case is handled; a non-equi
> correlation still needs a general dependent join.

---

## Decorrelation vs. pushdown

Decorrelation **always** runs and always produces a subquery-free plan. Whether
that plan is then sent to a source as a **single** SQL query is a separate
pushdown optimization (`optimizer/single_source_pushdown.py`): it fires when the
whole join subtree targets one data source. When a subquery spans sources, the
scans. The examples above are captured from the single-source path, so they show
the full decorrelated query pushed as one statement.

### Cost note: making the decorrelated join cheap

the physical plan. Decorrelation trades a nested-loop subquery for a join; the
one cost to watch is that collapsing a "first/aggregate row per correlation key"
subquery into a join can reduce the *whole* inner table once (group/partition
over every key), even when the outer side references only a few keys.

The fix keeps the join and feeds it the outer keys, so the inner side only
touches relevant rows:

  `orders.company_id`), so unrelated rows are never grouped/sorted.
  from the outer (build) side and push it as a runtime filter into the inner
  reads only matching rows. This recovers the index-lookup efficiency of a
  per-row lookup *within* the decorrelated join.

it. The lever is **which rows the inner scan reads**, not how the query is
here: it partitions the entire inner table, prunes badly through the join, and
