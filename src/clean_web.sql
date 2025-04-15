CREATE OR REPLACE TABLE web_clean AS
SELECT DISTINCT
    sku
  , virtual
  , downloadable
  , rating_count
  , average_rating
  , total_sales
  , tax_status
  , tax_class
  , post_author
  , post_date
  , post_date_gmt
  , post_content
  , post_title
  , post_excerpt
  , post_status
  , comment_status
  , ping_status
  , post_password
  , post_name
  , post_modified
  , post_modified_gmt
  , post_content_filtered
  , post_parent
  , guid
  , menu_order
  , post_type
  , post_mime_type
  , comment_count
FROM web_raw
WHERE sku IS NOT NULL;
