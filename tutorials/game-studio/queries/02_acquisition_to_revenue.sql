WITH funnel AS (
    SELECT w.channel,
           COUNT(DISTINCT w.visitor_id) AS visitors,
           COUNT(DISTINCT w.player_id) AS registrations
    FROM acquisition.main.web_events AS w
    GROUP BY w.channel
), revenue_per_player AS (
    SELECT t.player_id, SUM(t.amount_cents) AS revenue_cents
    FROM billing.public.transactions AS t
    WHERE t.status = 'settled'
    GROUP BY t.player_id
), channel_revenue AS (
    SELECT w.channel, COUNT(r.player_id) AS paying_players,
           SUM(COALESCE(r.revenue_cents, 0)) AS revenue_cents
    FROM acquisition.main.web_events AS w
    LEFT JOIN revenue_per_player AS r ON w.player_id = r.player_id
    WHERE w.event_name = 'registration'
    GROUP BY w.channel
)
SELECT f.channel, f.visitors, f.registrations,
       COALESCE(c.paying_players, 0) AS paying_players,
       COALESCE(c.revenue_cents, 0) AS revenue_cents,
       f.registrations * 100.0 / f.visitors AS registration_percent,
       COALESCE(c.paying_players, 0) * 100.0 / NULLIF(f.registrations, 0) AS payer_percent
FROM funnel AS f
LEFT JOIN channel_revenue AS c ON f.channel = c.channel
ORDER BY f.channel
