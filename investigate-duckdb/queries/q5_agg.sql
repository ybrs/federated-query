.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
SELECT category_id, count(*) n, sum(size_bytes) total
FROM pg.files GROUP BY category_id ORDER BY n DESC LIMIT 5;
