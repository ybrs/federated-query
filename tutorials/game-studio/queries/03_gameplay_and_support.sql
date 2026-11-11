WITH attempts AS (
    SELECT e.game_version, e.level_number, COUNT(*) AS attempts,
           SUM(CASE WHEN e.event_name = 'level_failed' THEN 1 ELSE 0 END) AS failures
    FROM gameplay.gameplay.events AS e
    WHERE e.event_name IN ('level_failed', 'level_up')
    GROUP BY e.game_version, e.level_number
), contacts AS (
    SELECT e.game_version, e.level_number,
           COUNT(DISTINCT t.ticket_id) AS tickets_from_affected_players
    FROM gameplay.gameplay.events AS e
    JOIN crm.support.tickets AS t ON e.player_id = t.player_id
    WHERE e.event_name = 'level_failed' AND t.category = 'gameplay'
    GROUP BY e.game_version, e.level_number
)
SELECT a.game_version, a.level_number, a.attempts, a.failures,
       a.failures * 100.0 / a.attempts AS failure_percent,
       COALESCE(c.tickets_from_affected_players, 0) AS tickets_from_affected_players
FROM attempts AS a
LEFT JOIN contacts AS c
  ON a.game_version = c.game_version AND a.level_number = c.level_number
ORDER BY a.game_version, a.level_number
