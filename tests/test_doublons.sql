-- Vérifie qu'il n'y a pas de doublons
SELECT 'erp_clean' AS table_name, COUNT(*) - COUNT(DISTINCT product_id) AS nb_doublons FROM erp_clean
UNION ALL
SELECT 'web_clean', COUNT(*) - COUNT(DISTINCT sku) FROM web_clean
UNION ALL
SELECT 'liaison_clean', COUNT(*) - COUNT(DISTINCT product_id) FROM liaison_clean;
