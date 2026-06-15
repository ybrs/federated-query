.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
SELECT id, category_id, size_bytes
FROM pg.files
WHERE category_id = 42 AND size_bytes > 5000000;
