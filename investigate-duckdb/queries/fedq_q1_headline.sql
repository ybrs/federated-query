SELECT a.day, c.name AS category, count(*) AS accesses
FROM analytics.main.access_logs a
JOIN pg.public.files f ON f.id = a.file_id
JOIN pg.public.categories c ON c.id = f.category_id
WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07'
GROUP BY a.day, c.name
ORDER BY a.day, accesses DESC;
