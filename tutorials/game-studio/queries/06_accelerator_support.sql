SELECT p.subscription_plan,
       COUNT(DISTINCT p.player_id) AS supported_players,
       COUNT(*) AS gameplay_tickets,
       SUM(t.handling_minutes) AS handling_minutes
FROM billing.public.players AS p
JOIN crm.support.tickets AS t ON p.player_id = t.player_id
JOIN (
    SELECT e.player_id
    FROM gameplay.gameplay.events AS e
    WHERE e.event_name = 'session_start'
    GROUP BY e.player_id
) AS active ON p.player_id = active.player_id
WHERE t.category = 'gameplay'
GROUP BY p.subscription_plan
