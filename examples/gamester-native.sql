CREATE EVENT DATASET gamester_events AS (
    SELECT
        COALESCE(r.user_id, r.device_id) AS actor_id,
        r.event_time AS ts,
        json_extract_string(r.event_type, '$') AS event_name,
        CAST(ROW_NUMBER() OVER (
            PARTITION BY COALESCE(r.user_id, r.device_id), r.event_time
            ORDER BY r.event_id, r.uuid
        ) AS BIGINT) AS seq,
        r.platform,
        r.country,
        r.os_name,
        r.version_name AS app_version
    FROM gamester_raw.main.gamester_raw AS r
)
ACTOR actor_id TIME ts EVENT event_name TIEBREAK seq
PROPERTIES (platform, country, os_name, app_version)
WITH (time_unit = 'us');
