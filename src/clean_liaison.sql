CREATE OR REPLACE TABLE liaison_clean AS
SELECT DISTINCT
    product_id
  , id_web
FROM liaison_raw
WHERE product_id IS NOT NULL
  AND id_web IS NOT NULL;
