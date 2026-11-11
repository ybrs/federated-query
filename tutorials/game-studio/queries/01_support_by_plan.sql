WITH support_per_player AS (
    SELECT t.player_id, COUNT(*) AS tickets,
           SUM(t.handling_minutes) AS handling_minutes,
           SUM(CASE WHEN t.closed_at IS NULL THEN 1 ELSE 0 END) AS open_tickets
    FROM crm.support.tickets AS t
    GROUP BY t.player_id
)
SELECT p.subscription_plan, COUNT(*) AS players,
       SUM(COALESCE(s.tickets, 0)) AS tickets,
       SUM(COALESCE(s.open_tickets, 0)) AS open_tickets,
       SUM(COALESCE(s.handling_minutes, 0)) AS handling_minutes,
       SUM(COALESCE(s.tickets, 0)) * 100.0 / COUNT(*) AS tickets_per_100_players
FROM billing.public.players AS p
LEFT JOIN support_per_player AS s ON p.player_id = s.player_id
GROUP BY p.subscription_plan
ORDER BY p.subscription_plan
