-- q09 reordered: the original FROM order (part, supplier, ...) starts with
-- part x supplier, which share NO predicate = a 200k x 10k cartesian product
-- (the query that OOM-killed at SF1). Reorder so part (filtered by p_name)
-- drives into lineitem, then supplier, partsupp, orders, nation along edges.
SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM part
    JOIN lineitem ON p_partkey = l_partkey
    JOIN supplier ON s_suppkey = l_suppkey
    JOIN partsupp ON ps_suppkey = l_suppkey AND ps_partkey = l_partkey
    JOIN orders ON o_orderkey = l_orderkey
    JOIN nation ON s_nationkey = n_nationkey
    WHERE
        p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;
