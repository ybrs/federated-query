-- q05 reordered (cost-aware): q05's join graph is CYCLIC - customer and
-- supplier are linked both directly (c_nationkey = s_nationkey) and through
-- orders/lineitem. A naive "small dims first" order joins customer to supplier
-- on nationkey early and builds a ~60M-row same-nation customer x supplier
-- intermediate (10x SLOWER than the original). The right order drives from the
-- ASIA suppliers into lineitem/orders and joins customer LAST, carrying both
-- c_custkey = o_custkey and c_nationkey = s_nationkey so it stays tiny.
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM region
JOIN nation ON n_regionkey = r_regionkey
JOIN supplier ON s_nationkey = n_nationkey
JOIN lineitem ON l_suppkey = s_suppkey
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON c_custkey = o_custkey AND c_nationkey = s_nationkey
WHERE
    r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC;
