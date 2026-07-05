-- q07 reordered: join nation n1 -> supplier -> lineitem -> orders -> customer
-- -> nation n2 along predicate edges. Every join has a condition (the original
-- FROM order supplier, lineitem, ... is already connected here, so this mostly
-- tests whether ordering alone helps when there is no cross product to remove).
SELECT
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        extract(year FROM l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM nation n1
    JOIN supplier ON s_nationkey = n1.n_nationkey
    JOIN lineitem ON s_suppkey = l_suppkey
    JOIN orders ON o_orderkey = l_orderkey
    JOIN customer ON c_custkey = o_custkey
    JOIN nation n2 ON c_nationkey = n2.n_nationkey
    WHERE
        ((n1.n_name = 'FRANCE'
                AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY'
                AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;
