# === Script 05 - Nettoyage complet et préparation des fichiers pour la suite du pipeline ===
# Ce script analyse les fichiers CSV bruts depuis MinIO,
# applique un nettoyage enrichi (suppression lignes/colonnes vides, exclusions métiers spécifiques),
# génère un résumé statistique, stocke les résultats nettoyés dans 'data/outputs/',
# et crée une base DuckDB pour les traitements suivants.

import duckdb
import pandas as pd
from pathlib import Path
from loguru import logger
import sys
import warnings

warnings.filterwarnings("ignore")

# ----------------------------------------------------------------------
# Configuration des logs
# ----------------------------------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "clean_data.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Chemins
# ----------------------------------------------------------------------
RAW_PATH = Path("data/raw")
OUTPUTS_PATH = Path("data/outputs")

RAW_PATH.mkdir(parents=True, exist_ok=True)
OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------
# Fichiers attendus
# ----------------------------------------------------------------------
erp_csv = RAW_PATH / "erp.csv"
web_csv = RAW_PATH / "web.csv"
liaison_csv = RAW_PATH / "liaison.csv"

# ----------------------------------------------------------------------
# Chargement et analyse initiale
# ----------------------------------------------------------------------
logger.info("📊 Lecture et analyse initiale des fichiers CSV...")

try:
    df_erp = pd.read_csv(erp_csv)
    df_web = pd.read_csv(web_csv)
    df_liaison = pd.read_csv(liaison_csv)

    logger.info(f"ERP     : {len(df_erp)} lignes")
    logger.info(f"WEB     : {len(df_web)} lignes")
    logger.info(f"LIAISON : {len(df_liaison)} lignes")

    logger.info(f"ERP - lignes vides : {df_erp.isnull().all(axis=1).sum()}")
    logger.info(f"WEB - lignes vides : {df_web.isnull().all(axis=1).sum()}")
    logger.info(f"LIAISON - lignes vides : {df_liaison.isnull().all(axis=1).sum()}")

except Exception as e:
    logger.error(f"❌ Erreur lors du chargement initial : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Connexion DuckDB
# ----------------------------------------------------------------------
try:
    Path("data").mkdir(exist_ok=True)
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🦆 Connexion à DuckDB établie.")
except Exception as e:
    logger.error(f"❌ Connexion à DuckDB échouée : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Nettoyage métier
# ----------------------------------------------------------------------
try:
    # Nettoyage ERP
    con.execute("""
        CREATE OR REPLACE TABLE erp_clean AS
        SELECT * FROM read_csv_auto('data/raw/erp.csv')
        WHERE product_id IS NOT NULL
          AND onsale_web IS NOT NULL
          AND price IS NOT NULL AND price > 0
          AND stock_quantity IS NOT NULL
          AND stock_status IS NOT NULL
    """)
    logger.success("✅ Table 'erp_clean' créée.")

    # Nettoyage WEB
    con.execute("""
        CREATE OR REPLACE TABLE web_clean AS
        SELECT * FROM read_csv_auto('data/raw/web.csv')
        WHERE sku IS NOT NULL
    """)
    logger.success("✅ Table 'web_clean' créée.")

    # Nettoyage LIAISON
    con.execute("""
        CREATE OR REPLACE TABLE liaison_clean AS
        SELECT * FROM read_csv_auto('data/raw/liaison.csv')
        WHERE product_id IS NOT NULL
          AND id_web IS NOT NULL
    """)
    logger.success("✅ Table 'liaison_clean' créée.")

except Exception as e:
    logger.error(f"❌ Erreur lors du nettoyage avec DuckDB : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Export des fichiers nettoyés
# ----------------------------------------------------------------------
try:
    con.execute("COPY erp_clean TO 'data/outputs/erp_clean.csv' (HEADER, DELIMITER ',')")
    con.execute("COPY web_clean TO 'data/outputs/web_clean.csv' (HEADER, DELIMITER ',')")
    con.execute("COPY liaison_clean TO 'data/outputs/liaison_clean.csv' (HEADER, DELIMITER ',')")

    logger.success("📁 Fichiers nettoyés exportés dans 'data/outputs/'.")

except Exception as e:
    logger.error(f"❌ Erreur lors de l'export des fichiers nettoyés : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Résumé statistique
# ----------------------------------------------------------------------
try:
    resume_df = pd.DataFrame({
        "source": ["erp", "web", "liaison"],
        "nb_lignes_initiales": [len(df_erp), len(df_web), len(df_liaison)],
        "nb_apres_nettoyage": [
            con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0],
            con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0],
            con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0],
        ]
    })

    resume_df["nb_exclues"] = resume_df["nb_lignes_initiales"] - resume_df["nb_apres_nettoyage"]
    resume_df.to_csv(OUTPUTS_PATH / "resume_stats.csv", index=False)

    logger.success("📈 Résumé statistique exporté : 'resume_stats.csv'.")

except Exception as e:
    logger.error(f"❌ Erreur lors de la génération du résumé : {e}")
    exit(1)

logger.success("🎯 Nettoyage complet terminé avec succès.")
