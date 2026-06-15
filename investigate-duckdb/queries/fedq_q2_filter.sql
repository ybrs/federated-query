SELECT id, category_id, size_bytes FROM pg.public.files
WHERE category_id = 42 AND size_bytes > 5000000;
