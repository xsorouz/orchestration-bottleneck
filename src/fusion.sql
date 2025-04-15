CREATE OR REPLACE TABLE fusion AS
SELECT
    e.product_id
  , e.onsale_web
  , e.price
  , e.stock_quantity
  , e.stock_status
  , w.post_title
  , w.post_excerpt
  , w.post_status
  , w.post_type
  , w.average_rating
  , w.total_sales
FROM erp_clean e
JOIN liaison_clean l ON e.product_id = l.product_id
JOIN web_clean w ON l.id_web = w.sku;
