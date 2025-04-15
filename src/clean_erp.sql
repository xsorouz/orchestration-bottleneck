CREATE OR REPLACE TABLE erp_clean AS
SELECT DISTINCT
    product_id
  , onsale_web
  , price
  , stock_quantity
  , stock_status
FROM erp_raw
WHERE product_id IS NOT NULL
  AND onsale_web IS NOT NULL
  AND price IS NOT NULL
  AND stock_quantity IS NOT NULL
  AND stock_status IS NOT NULL;
