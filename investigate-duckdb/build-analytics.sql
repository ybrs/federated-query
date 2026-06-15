-- DuckDB side of the POC: 10M-row access_logs fact table.
-- Build:  ./bin/duckdb-stable analytics-poc.duckdb < build-analytics.sql
CREATE TABLE access_logs AS
SELECT
    i AS id,
    -- skew: square of a uniform pick keeps low ids hotter -> popular files
    (1 + (CAST(pow(random(), 2) * 399999 AS BIGINT))) AS file_id,
    TIMESTAMP '2026-01-01 00:00:00'
        + (CAST(random() * 90 AS INTEGER)) * INTERVAL '1 day'
        + (CAST(random() * 86400 AS INTEGER)) * INTERVAL '1 second' AS accessed_at,
    (['view','download','preview','share'])[1 + CAST(random()*4 AS INTEGER)] AS action,
    CAST(random() * 5000000 AS BIGINT) AS bytes_sent,
    (1 + CAST(random() * 9999 AS INTEGER)) AS user_id
FROM range(1, 10000001) t(i);

ALTER TABLE access_logs ADD COLUMN day DATE;
UPDATE access_logs SET day = CAST(accessed_at AS DATE);
