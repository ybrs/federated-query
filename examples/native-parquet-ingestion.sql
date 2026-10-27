CREATE EVENT DATASET product_events AS (
    SELECT
        COALESCE(r.user_id, r.device_id) AS actor_id,
        r.event_time AS ts,
        json_extract_string(r.event_type, '$') AS event_name,
        r.event_id AS event_id,
        r.uuid AS uuid,
        r.platform,
        r.country,
        r.os_name,
        r.version_name AS app_version
    FROM events_raw.main.events_raw AS r
)
ACTOR actor_id TIME ts EVENT event_name TIEBREAK (event_id, uuid)
PROPERTIES (platform, country, os_name, app_version)
WITH (time_unit = 'us');
