import duckdb
import pandas as pd
from pathlib import Path

# Connexion à DuckDB
con = duckdb.connect("data.duckdb")

# Application des filtres stricts
df_debug = con.execute("""
    SELECT
        product_id,
        post_title,
        price,
        stock_quantity,
        ROUND(price * stock_quantity, 2) AS chiffre_affaires
    FROM fusion
    WHERE stock_quantity > 0
      AND stock_status = 'instock'
      AND price <= 50
      AND stock_quantity <= 200
""").fetchdf()

# Calcul du CA total
ca_total = df_debug["chiffre_affaires"].sum()
print(f"💰 CA total filtré (<= 50 €, stock <= 200) : {ca_total:,.2f} €")
print(f"📦 Produits comptabilisés : {len(df_debug)}")

# Export du résultat
OUTPUT_PATH = Path("data/outputs")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
df_debug.to_csv(OUTPUT_PATH / "ca_debug_filtered.csv", index=False)
