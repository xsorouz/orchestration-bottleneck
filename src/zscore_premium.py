
import duckdb
import pandas as pd
from pathlib import Path

# Chemin du dossier de sortie
OUTPUT_PATH = Path("data/outputs")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Charger les données depuis la table fusion
df = duckdb.sql("SELECT * FROM fusion").to_df()

# Calcul du z-score
mean_price = df["price"].mean()
std_price = df["price"].std()

df["z_score"] = (df["price"] - mean_price) / std_price
df["type"] = df["z_score"].apply(lambda z: "millésimé" if z > 2 else "ordinaire")

# Export des vins millésimés
df[df["type"] == "millésimé"].to_csv(OUTPUT_PATH / "vins_millesimes.csv", index=False)
df[df["type"] == "ordinaire"].to_csv(OUTPUT_PATH / "vins_ordinaires.csv", index=False)

# Statistiques affichées dans les logs
print(f"Nombre de vins millésimés : {df[df['type'] == 'millésimé'].shape[0]}")
