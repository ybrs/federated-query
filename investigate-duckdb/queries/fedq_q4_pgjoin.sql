SELECT c.name, count(*) AS n
FROM pg.public.files f JOIN pg.public.categories c ON c.id = f.category_id
WHERE c.is_active = true
GROUP BY c.name ORDER BY n DESC LIMIT 5;
