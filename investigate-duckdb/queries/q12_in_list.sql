.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
SELECT id, category_id FROM pg.files WHERE id IN (10, 250, 999, 40000, 123456);
