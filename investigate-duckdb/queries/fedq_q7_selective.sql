SELECT f.category_id, count(*) AS accesses
FROM analytics.main.access_logs a
JOIN pg.public.files f ON f.id = a.file_id
WHERE a.day = DATE '2026-02-01' AND a.user_id = 7
GROUP BY f.category_id ORDER BY accesses DESC LIMIT 5;
