# === Script 08 - Dédoublonnage des fichiers nettoyés avec DuckDB ===
# Ce script dédoublonne les tables nettoyées en supprimant les doublons
# selon des règles spécifiques, et vérifie que les résultats sont corrects.

import duckdb
from pathlib import Path
from loguru import logger
import sys
import warnings

warnings.filterwarnings("ignore")

# ----------------------------------------------------------------------
# Configuration du logger
# ----------------------------------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "dedoublonnage.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Connexion à DuckDB
# ----------------------------------------------------------------------
try:
    Path("data").mkdir(exist_ok=True)
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.success("✅ Connexion à DuckDB établie dans 'data/bottleneck.duckdb'.")
except Exception as e:
    logger.error(f"❌ Échec de connexion à DuckDB : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Dédoublonnage ERP (agrégation)
# ----------------------------------------------------------------------
try:
    con.execute("""
        CREATE OR REPLACE TABLE erp_dedup AS
        SELECT 
            product_id,
            MAX(onsale_web)     AS onsale_web,
            MAX(price)          AS price,
            MAX(stock_quantity) AS stock_quantity,
            MAX(stock_status)   AS stock_status
        FROM read_csv_auto('data/outputs/erp_clean.csv')
        GROUP BY product_id
    """)
    logger.success("✅ Table erp_dedup créée avec agrégation sur product_id.")
except Exception as e:
    logger.error(f"❌ Erreur lors du dédoublonnage ERP : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Dédoublonnage Liaison (agrégation)
# ----------------------------------------------------------------------
try:
    con.execute("""
        CREATE OR REPLACE TABLE liaison_dedup AS
        SELECT 
            product_id,
            MIN(id_web) AS id_web
        FROM read_csv_auto('data/outputs/liaison_clean.csv')
        GROUP BY product_id
    """)
    logger.success("✅ Table liaison_dedup créée avec agrégation sur product_id.")
except Exception as e:
    logger.error(f"❌ Erreur lors du dédoublonnage Liaison : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Dédoublonnage Web (row_number + filtre produit)
# ----------------------------------------------------------------------
try:
    con.execute("""
        CREATE OR REPLACE TABLE web_dedup AS
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY sku
                ORDER BY post_date DESC
            ) AS rn
            FROM read_csv_auto('data/outputs/web_clean.csv')
            WHERE post_type = 'product'
        )
        WHERE rn = 1
    """)
    logger.success("✅ Table web_dedup créée avec filtrage post_type = 'product' et row_number.")
except Exception as e:
    logger.error(f"❌ Erreur lors du dédoublonnage Web : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Validation du dédoublonnage
# ----------------------------------------------------------------------
try:
    nb_erp = con.execute("SELECT COUNT(*) FROM erp_dedup").fetchone()[0]
    nb_web = con.execute("SELECT COUNT(*) FROM web_dedup").fetchone()[0]
    nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_dedup").fetchone()[0]

    assert nb_erp > 0, "❌ Table erp_dedup vide"
    assert nb_web > 0, "❌ Table web_dedup vide"
    assert nb_liaison > 0, "❌ Table liaison_dedup vide"

    logger.info(f"✔️  Lignes dédoublonnées - ERP: {nb_erp}, Web: {nb_web}, Liaison: {nb_liaison}")
    logger.success("🎯 Dédoublonnage terminé avec succès et validé.")

except Exception as e:
    logger.error(f"❌ Échec dans la validation du dédoublonnage : {e}")
    exit(1)
