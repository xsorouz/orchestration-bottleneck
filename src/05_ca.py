# === Script 05 - Calcul du chiffre d'affaires (version finale validée) ===

# ==============================================================================
# 🔧 IMPORTS DES LIBRAIRIES
# ==============================================================================

# duckdb : moteur de base de données relationnelle rapide et léger, utilisé ici en local.
import duckdb

# pandas : pour manipuler les résultats SQL sous forme de DataFrames et faciliter les exports.
import pandas as pd

# pathlib : permet de gérer les chemins de fichiers/dossiers de manière propre et multiplateforme.
from pathlib import Path

# loguru : bibliothèque de logging moderne et élégante, utilisée pour journaliser chaque étape.
from loguru import logger

logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")  # warnings, errors et criticals

# ==============================================================================
# 📂 CONFIGURATION DES CHEMINS ET DES LOGS
# ==============================================================================

# Création du dossier "logs" s’il n’existe pas
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)

# Configuration du fichier de log avec rotation automatique à 500 Ko
logger.add(LOGS_PATH / "ca.log", level="INFO", rotation="500 KB")

# Création du dossier "data/outputs" pour enregistrer les fichiers de sortie
OUTPUT_PATH = Path("data/outputs")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


# ==============================================================================
# 🔌 CONNEXION À LA BASE DUCKDB
# ==============================================================================

try:
    # Connexion à la base locale DuckDB (fichier data.duckdb)
    con = duckdb.connect("data.duckdb")
    logger.info("🦆 Connexion à DuckDB établie.")
except Exception as e:
    logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
    exit(1)


# ==============================================================================
# 📈 CALCUL DU CHIFFRE D'AFFAIRES PAR PRODUIT
# ==============================================================================

try:
    # Création d'une table contenant le CA par produit, uniquement pour les produits en stock
    con.execute("""
        CREATE OR REPLACE TABLE ca_par_produit AS
        SELECT
            product_id,
            post_title,
            price,
            stock_quantity,
            ROUND(price * stock_quantity, 2) AS chiffre_affaires
        FROM fusion
        WHERE stock_quantity > 0
          AND stock_status = 'instock'
    """)
    logger.success("✅ Table ca_par_produit créée avec filtrage stock_quantity > 0 et stock_status = 'instock'.")

    # Création d’une table de synthèse avec le CA total
    con.execute("""
        CREATE OR REPLACE TABLE ca_total AS
        SELECT ROUND(SUM(chiffre_affaires), 2) AS ca_total
        FROM ca_par_produit
    """)
    logger.success("✅ Table ca_total créée.")

except Exception as e:
    logger.error(f"❌ Erreur lors du calcul du CA : {e}")
    exit(1)


# ==============================================================================
# ✅ VALIDATION, TESTS DE COHÉRENCE ET EXPORTS
# ==============================================================================

try:
    # Chargement des résultats dans des DataFrames pandas
    df_ca = con.execute("SELECT * FROM ca_par_produit").fetchdf()
    df_total = con.execute("SELECT * FROM ca_total").fetchdf()
    ca_total = df_total['ca_total'][0]

    # --- TEST 1 : Vérification du montant total attendu ---
    assert round(ca_total, 2) == 387837.60, f"CA total incorrect : {ca_total} € (attendu : 387 837.60 €)"
    logger.success(f"💰 CA TOTAL = {ca_total:,.2f} € ✅")
    logger.info(f"📦 Produits comptabilisés : {len(df_ca)}")

    # --- TEST 2 : Tests de cohérence des données ---
    assert len(df_ca) == 573, f"❌ Le nombre de lignes dans ca_par_produit est incorrect : {len(df_ca)} (attendu : 573)"
    assert df_ca[["price", "stock_quantity", "chiffre_affaires"]].isnull().sum().sum() == 0, "❌ Valeurs nulles détectées"
    assert (df_ca["chiffre_affaires"] < 0).sum() == 0, "❌ CA négatif détecté"
    logger.success("🧪 Tests de cohérence des données CA validés ✅")

    # --- STATISTIQUES ET APERÇUS ---
    # Top 5 produits par CA
    top_5 = df_ca.sort_values("chiffre_affaires", ascending=False).head(5)
    logger.debug("🏆 Top 5 produits par chiffre d'affaires :")
    logger.debug(top_5[["product_id", "post_title", "chiffre_affaires"]].to_string(index=False))

    # Produit avec le plus grand stock
    most_stock = df_ca.sort_values("stock_quantity", ascending=False).head(1)
    logger.debug("📦 Produit avec le PLUS de stock :")
    logger.debug(most_stock[["product_id", "post_title", "stock_quantity"]].to_string(index=False))

    # Produit le plus cher
    most_expensive = df_ca.sort_values("price", ascending=False).head(1)
    logger.debug("💸 Produit le PLUS CHER :")
    logger.debug(most_expensive[["product_id", "post_title", "price"]].to_string(index=False))

    # Produit au CA maximum
    top_ca = df_ca.loc[df_ca["chiffre_affaires"].idxmax()]
    logger.debug(f"🔥 Produit au CA max : {top_ca['post_title']} — {top_ca['chiffre_affaires']} €")

    # Moyenne et médiane du CA
    logger.info(f"📊 Moyenne du CA : {df_ca['chiffre_affaires'].mean():,.2f} €")
    logger.info(f"📊 Médiane du CA : {df_ca['chiffre_affaires'].median():,.2f} €")

    # --- EXPORTS DES DONNÉES ---
    df_ca.to_csv(OUTPUT_PATH / "ca_par_produit.csv", index=False)
    df_total.to_csv(OUTPUT_PATH / "ca_total.csv", index=False)
    df_ca.to_excel(OUTPUT_PATH / "ca_par_produit.xlsx", index=False)

    logger.success("📁 Fichiers exportés : ca_par_produit.csv / .xlsx, ca_total.csv")

except Exception as e:
    logger.error(f"❌ Erreur finale du CA : {e}")
    exit(1)
