.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
SELECT c.name, count(*) AS n
FROM pg.files f JOIN pg.categories c ON c.id = f.category_id
WHERE c.is_active
GROUP BY c.name
ORDER BY n DESC LIMIT 5;
