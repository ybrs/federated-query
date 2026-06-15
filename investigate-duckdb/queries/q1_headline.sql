.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
WITH daily AS (
  SELECT a.day, c.name AS category, count(*) AS accesses
  FROM access_logs a
  JOIN pg.files f      ON f.id = a.file_id
  JOIN pg.categories c ON c.id = f.category_id
  WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07'
  GROUP BY a.day, c.name
)
SELECT day, category, accesses,
       rank() OVER (PARTITION BY day ORDER BY accesses DESC) AS rnk
FROM daily
QUALIFY rnk <= 3
ORDER BY day, rnk;
