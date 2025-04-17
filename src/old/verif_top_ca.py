import duckdb
import pandas as pd

con = duckdb.connect("data.duckdb")

# Top 10 produits par chiffre d'affaires
df_top = con.execute("""
    SELECT
        product_id,
        post_title,
        price,
        stock_quantity,
        ROUND(price * stock_quantity, 2) AS chiffre_affaires
    FROM fusion
    WHERE stock_quantity > 0
      AND stock_status = 'instock'
    ORDER BY chiffre_affaires DESC
    LIMIT 10
""").fetchdf()

print(df_top.to_string(index=False))
