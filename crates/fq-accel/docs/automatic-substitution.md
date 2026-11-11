[Accelerator documentation](../README.md)

# Automatic substitution

Substitution matches on the OPTIMIZED logical plan. The view's defining SELECT
is run through the same parse -> bind -> decorrelate -> optimize pipeline the
incoming query uses, giving a canonical plan; a subtree of the incoming plan
that is structurally EQUAL to that canonical plan is replaced with a scan of the
view. Largest matching subtree wins.

## The exact definition, re-issued

```sql
EXPLAIN SELECT o_region, sum(o_total) AS revenue FROM wh.main.orders GROUP BY o_region;
```

```text
MaterializedScan [region_rev] :: SELECT "o_region", "revenue" FROM "public"."region_rev"
```

The twelve-row source scan and its aggregate collapse to a read of the
three stored rows.

## The same query, cosmetically different

Matching is on the optimized plan, so differences the pipeline erases before the
plan is formed do not matter, and differences it carries INTO the plan do. Run
against `region_rev`, here is exactly what survives and what does not:

```text
exact                            -> SUBSTITUTES
lowercase keywords + odd whitespace/newlines  -> SUBSTITUTES
UPPERCASE keywords               -> SUBSTITUTES
explicit table alias = table name (FROM orders AS orders)  -> SUBSTITUTES
different table alias (FROM orders t, t.o_region ...)      -> cold
GROUP BY ordinal (GROUP BY 1 instead of GROUP BY o_region) -> cold
different OUTPUT alias (sum(o_total) AS total_rev)         -> cold
```

Be precise about the boundary. The optimize stage normalizes lexical noise -
whitespace, newlines, keyword case, and quoting all disappear before the plan
exists, so those variants match. It does NOT canonicalize:

- TABLE ALIASES. A user alias becomes the qualifier stamped on every column
  reference in the plan. `FROM orders t` carries `t.o_region`; the view's
  canonical carries `orders.o_region` (the default alias is the table name).
  Different qualifier, different plan, no match. An explicit alias that happens
  to equal the default (`FROM orders AS orders`) matches because it produces the
  identical qualifier.
- ORDINAL GROUP BY. `GROUP BY 1` stays an ordinal in the plan; `GROUP BY
  o_region` is a column. Different nodes, no match.
- OUTPUT COLUMN NAMES. The stored view reports its own column names, and the
  planner also enforces a name guard: the matched subtree's output names must
  equal the view's stored names in order. Rename the output (`AS total_rev`) and
  the guard declines.

This is a conservative design: a difference the optimizer does not fold declines
rather than risking a wrong match. It never returns a wrong answer; at worst it
misses a substitution you might have expected.

## A view matching a subtree of a bigger query

The view does not have to be the whole query. Here the `region_rev` aggregate
appears as a derived table feeding a join to `region_dim`, and only that subtree
is substituted:

```sql
EXPLAIN SELECT g.o_region, g.revenue, d.rd_manager
        FROM (SELECT o_region, sum(o_total) AS revenue
              FROM wh.main.orders GROUP BY o_region) g
        JOIN wh.main.region_dim d ON g.o_region = d.rd_name;
```

```text
Projection
  HashJoin [INNER]
    AliasedRelation
      MaterializedScan [region_rev] :: SELECT "o_region", "revenue" FROM "public"."region_rev"
    Scan [wh] +inject(rd_name) :: SELECT "rd_name", "rd_manager" FROM "main"."region_dim" AS "d"
```

The aggregate side reads the view; the dimension side still scans the source.
The rest of the plan (the join, the projection) is built as usual over the
reduced tree.

[Accelerator documentation](../README.md)
