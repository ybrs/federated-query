WITH player_revenue AS (
    SELECT t.player_id, SUM(t.amount_cents) AS revenue_cents
    FROM billing.public.transactions AS t
    WHERE t.status = 'settled'
    GROUP BY t.player_id
), player_support AS (
    SELECT t.player_id, SUM(t.handling_minutes) AS support_minutes
    FROM crm.support.tickets AS t
    GROUP BY t.player_id
), player_activity AS (
    SELECT e.player_id, COUNT(*) AS sessions
    FROM gameplay.gameplay.events AS e
    WHERE e.event_name = 'session_start'
    GROUP BY e.player_id
)
SELECT w.channel, COUNT(*) AS registered_players,
       SUM(COALESCE(r.revenue_cents, 0)) AS revenue_cents,
       SUM(COALESCE(s.support_minutes, 0)) AS support_minutes,
       SUM(COALESCE(a.sessions, 0)) AS sessions
FROM acquisition.main.web_events AS w
LEFT JOIN player_revenue AS r ON w.player_id = r.player_id
LEFT JOIN player_support AS s ON w.player_id = s.player_id
LEFT JOIN player_activity AS a ON w.player_id = a.player_id
WHERE w.event_name = 'registration'
GROUP BY w.channel
ORDER BY w.channel
