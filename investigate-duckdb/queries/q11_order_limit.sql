.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
SELECT id, size_bytes FROM pg.files ORDER BY size_bytes DESC LIMIT 10;
