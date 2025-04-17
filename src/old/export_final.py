
import duckdb
import pandas as pd
from pathlib import Path

OUTPUT_PATH = Path("data/outputs")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Export du CA par produit
df_ca = duckdb.sql("SELECT * FROM ca_par_produit").to_df()
df_ca.to_excel(OUTPUT_PATH / "rapport_ca_par_produit.xlsx", index=False)

# Export du CA total
df_total = duckdb.sql("SELECT * FROM ca_total").to_df()
df_total.to_excel(OUTPUT_PATH / "rapport_ca_total.xlsx", index=False)

print("✅ Export CA réalisé : fichiers Excel générés.")
