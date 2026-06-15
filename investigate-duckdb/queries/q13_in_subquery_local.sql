.mode duckbox
LOAD postgres;
ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);
CREATE TEMP TABLE hot_files AS SELECT * FROM (VALUES (10),(250),(999),(40000),(123456)) t(fid);
SELECT id, category_id FROM pg.files WHERE id IN (SELECT fid FROM hot_files);
