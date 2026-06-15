-- PostgreSQL side of the DuckDB-pushdown POC.
-- Load into the duckpoc database:  ./pg.sh -d duckpoc -f schema.sql
-- (create the db first: ./pg.sh -d postgres -c "CREATE DATABASE duckpoc")

DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS categories;

CREATE TABLE categories (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    parent_id   integer,
    is_active   boolean NOT NULL DEFAULT true,
    created_at  timestamp NOT NULL DEFAULT now()
);

INSERT INTO categories (id, name, parent_id, is_active, created_at)
SELECT g,
       'category_' || g,
       CASE WHEN g > 100 THEN (g % 100) + 1 ELSE NULL END,
       (g % 7 <> 0),
       now() - (g % 365) * interval '1 day'
FROM generate_series(1, 50000) g;

CREATE TABLE files (
    id           integer PRIMARY KEY,
    category_id  integer NOT NULL REFERENCES categories(id),
    filename     text NOT NULL,
    size_bytes   bigint NOT NULL,
    owner_id     integer NOT NULL,
    created_at   timestamp NOT NULL DEFAULT now()
);

INSERT INTO files (id, category_id, filename, size_bytes, owner_id, created_at)
SELECT g,
       (g % 50000) + 1,
       'file_' || g || '.dat',
       (g::bigint * 7919) % 10000000,   -- bigint cast: g*7919 overflows int at g>271k
       (g % 1000) + 1,
       now() - (g % 730) * interval '1 day'
FROM generate_series(1, 400000) g;

CREATE INDEX idx_files_category ON files(category_id);
ANALYZE categories;
ANALYZE files;
