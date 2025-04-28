# === Script 07 - Test d'absence de valeurs nulles ===
# Ce script vérifie qu'aucune valeur critique n'est NULL dans les tables nettoyées
# après application du nettoyage métier.

import duckdb
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
logger.add(LOGS_PATH / "test_nulls_clean_data.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Connexion à DuckDB
# ----------------------------------------------------------------------
try:
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🧪 Connexion à DuckDB établie.")
except Exception as e:
    logger.error(f"❌ Connexion échouée : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Tests d'absence de valeurs NULL
# ----------------------------------------------------------------------
try:
    erp_nulls = con.execute("""
        SELECT COUNT(*) FROM erp_clean
        WHERE product_id IS NULL OR onsale_web IS NULL
        OR price IS NULL OR stock_quantity IS NULL
        OR stock_status IS NULL
    """).fetchone()[0]

    web_nulls = con.execute("""
        SELECT COUNT(*) FROM web_clean
        WHERE sku IS NULL
    """).fetchone()[0]

    liaison_nulls = con.execute("""
        SELECT COUNT(*) FROM liaison_clean
        WHERE product_id IS NULL OR id_web IS NULL
    """).fetchone()[0]

    assert erp_nulls == 0, f"❌ Valeurs NULL détectées dans erp_clean : {erp_nulls}"
    assert web_nulls == 0, f"❌ Valeurs NULL détectées dans web_clean : {web_nulls}"
    assert liaison_nulls == 0, f"❌ Valeurs NULL détectées dans liaison_clean : {liaison_nulls}"

    logger.success("✅ Aucune valeur NULL détectée dans les tables nettoyées.")
    logger.success("🎯 Validation d'absence de NULLs réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans le test de NULLs : {e}")
    exit(1)
