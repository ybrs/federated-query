[Accelerator documentation](../README.md)

# Freshness and refresh

Serving trusts the last pull. If the source changes and you do not REFRESH, the
substituted read keeps returning the rows the view was built from. This is the
freshness contract, demonstrated end to end (the source is mutated between two
runtime processes so the change is genuinely committed to the file):

```text
CREATE MATERIALIZED VIEW us_total AS
  SELECT sum(o_total) AS s FROM wh.main.orders WHERE o_region = 'US'

-- CREATE populated the view; the original SELECT now reuses it:
SELECT sum(o_total) AS s FROM wh.main.orders WHERE o_region = 'US'   -> s = 570

-- the source is then mutated: INSERT ... ('US', 1000.0)

-- substitution ON: still the last pull
SELECT ... WHERE o_region = 'US'                                     -> s = 570
  MaterializedScan [us_total] :: SELECT "s" FROM "public"."us_total"

-- kill switch OFF: recompute sees the live source
SET accelerator.enable_substitution = false
SELECT ... WHERE o_region = 'US'                                     -> s = 1570

-- REFRESH moves the view forward, then substitution serves the new rows
REFRESH MATERIALIZED VIEW us_total
  -> "REFRESH MATERIALIZED VIEW (whole re-pull: table 'wh.main.orders' has no declared change key)"
RESET accelerator.enable_substitution
SELECT ... WHERE o_region = 'US'                                     -> s = 1570
```

Substitution did not return a wrong answer - it returned exactly the rows the
view holds. Bringing those rows up to date is YOUR REFRESH, never a query-path
check. Declared `change_keys` enable delta append or primary-key merge only for
eligible single-scan, rowwise definitions. Joins and aggregates use a full
re-pull even when their base tables declare change keys.

[Accelerator documentation](../README.md)
