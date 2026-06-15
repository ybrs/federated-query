.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
SELECT c.name, count(*) AS accesses
FROM access_logs a
JOIN pg.files f      ON f.id = a.file_id
JOIN pg.categories c ON c.id = f.category_id
WHERE a.action = 'download'
GROUP BY c.name ORDER BY accesses DESC LIMIT 5;
