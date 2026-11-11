SELECT p.player_id, p.subscription_plan
FROM billing.public.players AS p
WHERE EXISTS (
    SELECT 1
    FROM crm.support.tickets AS t
    WHERE t.player_id = p.player_id AND t.category = 'gameplay'
)
AND EXISTS (
    SELECT 1
    FROM gameplay.gameplay.events AS e
    WHERE e.player_id = p.player_id
      AND e.game_version = '1.1'
      AND e.level_number = 7
      AND e.event_name = 'level_failed'
)
ORDER BY p.player_id
