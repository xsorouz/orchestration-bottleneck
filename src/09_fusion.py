# === Script 09 - Fusion des tables dédoublonnées en une seule table finale ===
# Ce script réalise la jointure entre les tables erp_dedup, liaison_dedup et web_dedup,
# vérifie que le nombre de lignes correspond à l'attendu (714),
# et exporte le résultat final dans 'data/outputs/fusion.csv'.

import duckdb
import pandas as pd
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
logger.add(LOGS_PATH / "fusion.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Connexion à DuckDB
# ----------------------------------------------------------------------
try:
    Path("data").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.success("✅ Connexion à DuckDB établie dans 'data/bottleneck.duckdb'.")
except Exception as e:
    logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Création de la table fusion
# ----------------------------------------------------------------------
try:
    con.execute("""
        CREATE OR REPLACE TABLE fusion AS
        SELECT
            e.product_id,
            e.onsale_web,
            e.price,
            e.stock_quantity,
            e.stock_status,
            w.post_title,
            w.post_excerpt,
            w.post_status,
            w.post_type,
            w.average_rating,
            w.total_sales
        FROM erp_dedup e
        JOIN liaison_dedup l ON e.product_id = l.product_id
        JOIN web_dedup w ON l.id_web = w.sku
    """)
    logger.success("✅ Table 'fusion' créée par jointure entre ERP, Liaison et Web.")
except Exception as e:
    logger.error(f"❌ Erreur lors de la création de la table fusion : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Validation et export de la fusion
# ----------------------------------------------------------------------
try:
    nb_rows = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]
    assert nb_rows == 714, f"❌ La table fusion contient {nb_rows} lignes (attendu : 714)"
    logger.info(f"✔️  Nombre de lignes fusionnées : {nb_rows} (attendu : 714)")

    # Export au format CSV
    output_path = Path("data/outputs/fusion.csv")
    fusion_df = con.execute("SELECT * FROM fusion").fetchdf()
    fusion_df.to_csv(output_path, index=False)
    logger.success(f"📁 Table fusion exportée sous '{output_path}'.")
except Exception as e:
    logger.error(f"❌ Erreur dans la validation ou l'export de la table fusion : {e}")
    exit(1)
