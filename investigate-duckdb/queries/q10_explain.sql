.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
EXPLAIN
SELECT c.name, count(*) FROM access_logs a
JOIN pg.files f ON f.id=a.file_id
JOIN pg.categories c ON c.id=f.category_id
WHERE f.size_bytes > 9000000
GROUP BY c.name;
