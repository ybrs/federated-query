DELETE FROM tickets
WHERE player_id = 1 AND category = 'gameplay' AND closed_at IS NULL
  AND (
      (ticket_id = 900000001 AND opened_at = '2026-01-30 12:00:00'
       AND handling_minutes = 17)
      OR
      (ticket_id = 900000002 AND opened_at = '2026-01-30 13:00:00'
       AND handling_minutes = 23)
  );
