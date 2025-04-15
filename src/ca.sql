CREATE OR REPLACE TABLE ca_par_produit AS
SELECT
    product_id
  , post_title
  , price
  , stock_quantity
  , ROUND(price * stock_quantity, 2) AS chiffre_affaires
FROM fusion
WHERE price IS NOT NULL AND stock_quantity IS NOT NULL;

-- Table pour le CA global
CREATE OR REPLACE TABLE ca_total AS
SELECT
    ROUND(SUM(chiffre_affaires), 2) AS ca_total
FROM ca_par_produit;
