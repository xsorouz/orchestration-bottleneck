
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path

# Définir les chemins
RAW_PATH = Path("data/raw")
PROCESSED_PATH = Path("data/processed")
OUTPUT_PATH = Path("data/outputs")

# Créer les dossiers si besoin
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Étape 1 - Nettoyage des fichiers (suppression des nulls et doublons)
def clean_file(filename: str, table_name: str):
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT DISTINCT * FROM read_excel('{RAW_PATH / filename}')
        WHERE * IS NOT NULL
    """)

clean_file("Fichier_erp.xlsx", "erp_clean")
clean_file("Fichier_web.xlsx", "web_clean")
clean_file("fichier_liaison.xlsx", "liaison_clean")

# Étape 2 - Jointure des sources via fichier de liaison
duckdb.sql("""
    CREATE OR REPLACE TABLE fusion AS
    SELECT
        e.id_produit,
        e.nom AS nom_erp,
        w.nom AS nom_web,
        e.quantite,
        w.prix
    FROM erp_clean e
    JOIN liaison_clean l ON e.id_produit = l.id_produit_erp
    JOIN web_clean w ON l.id_produit_web = w.id_produit
""")

# Étape 3 - Calcul du chiffre d'affaires
duckdb.sql("""
    CREATE OR REPLACE TABLE ca_par_produit AS
    SELECT
        id_produit,
        nom_erp,
        SUM(quantite * prix) AS chiffre_affaires
    FROM fusion
    GROUP BY id_produit, nom_erp
""")

# Export du rapport CA
df_ca = duckdb.sql("SELECT * FROM ca_par_produit").to_df()
df_ca.to_excel(OUTPUT_PATH / "rapport_chiffre_affaires.xlsx", index=False)

# Calcul CA total
ca_total = df_ca["chiffre_affaires"].sum()
print(f"💰 Chiffre d'affaires total : {ca_total:.2f} €")

# Étape 4 - Identification des vins premium

df = duckdb.sql("SELECT * FROM fusion").to_df()

# Z-score
mean_price = df["prix"].mean()
std_price = df["prix"].std()
df["z_score"] = (df["prix"] - mean_price) / std_price
df["is_premium_z"] = df["z_score"] > 2

# IQR
q1 = df["prix"].quantile(0.25)
q3 = df["prix"].quantile(0.75)
iqr = q3 - q1
threshold_iqr = q3 + 1.5 * iqr
df["is_premium_iqr"] = df["prix"] > threshold_iqr

# Statut final (si l'une des méthodes identifie le vin comme premium)
df["type"] = df.apply(
    lambda row: "premium" if row["is_premium_z"] or row["is_premium_iqr"] else "ordinaire",
    axis=1
)

# Export des résultats
df[df["type"] == "premium"].to_csv(OUTPUT_PATH / "vins_premium.csv", index=False)
df[df["type"] == "ordinaire"].to_csv(OUTPUT_PATH / "vins_ordinaires.csv", index=False)

print(f"🍷 Nombre de vins premium détectés : {df[df['type'] == 'premium'].shape[0]}")
print("✅ Pipeline exécuté avec succès.")
