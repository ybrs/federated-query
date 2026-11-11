SELECT review.subscription_plan,
       review.supported_players,
       review.gameplay_tickets,
       review.handling_minutes,
       revenue.revenue_cents
FROM (
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
) AS review
JOIN (
    SELECT customer.subscription_plan,
           SUM(payment.amount_cents) AS revenue_cents
    FROM billing.public.players AS customer
    JOIN billing.public.transactions AS payment
        ON customer.player_id = payment.player_id
    WHERE payment.status = 'settled'
    GROUP BY customer.subscription_plan
) AS revenue ON review.subscription_plan = revenue.subscription_plan
ORDER BY review.handling_minutes DESC
LIMIT 2;
