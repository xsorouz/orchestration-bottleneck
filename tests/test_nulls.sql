-- Vérifie l'absence de valeurs nulles
SELECT 'erp_clean' AS table_name, COUNT(*) AS nb_nulls FROM erp_clean
WHERE product_id IS NULL OR onsale_web IS NULL OR price IS NULL OR stock_quantity IS NULL OR stock_status IS NULL
UNION ALL
SELECT 'web_clean', COUNT(*) FROM web_clean
WHERE sku IS NULL OR post_title IS NULL OR post_status IS NULL
UNION ALL
SELECT 'liaison_clean', COUNT(*) FROM liaison_clean
WHERE product_id IS NULL OR id_web IS NULL;
