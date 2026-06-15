.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
SELECT f.category_id, count(*) AS accesses
FROM access_logs a
JOIN pg.files f ON f.id = a.file_id
WHERE a.day = DATE '2026-02-01' AND a.user_id = 7
GROUP BY f.category_id ORDER BY accesses DESC LIMIT 5;
