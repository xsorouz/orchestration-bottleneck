# === Script de test - Vérification des doublons (tables nettoyées) ===

import duckdb
from pathlib import Path
from loguru import logger
import sys

logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "test_doublons.log", level="INFO", rotation="500 KB")

try:
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🧪 Connexion à DuckDB réussie.")
except Exception as e:
    logger.error(f"❌ Connexion échouée : {e}")
    exit(1)

try:
    erp_dup = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM erp_clean
    """).fetchone()[0]
    web_dup = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT sku) FROM web_clean
    """).fetchone()[0]
    liaison_dup = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM liaison_clean
    """).fetchone()[0]

    assert erp_dup == 0, f"❌ Doublons détectés dans erp_clean : {erp_dup}"
    assert web_dup == 0, f"❌ Doublons détectés dans web_clean : {web_dup}"
    assert liaison_dup == 0, f"❌ Doublons détectés dans liaison_clean : {liaison_dup}"

    logger.success("✅ Aucun doublon détecté dans les clés primaires.")
    logger.success("🎉 Validation des doublons réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans les tests de doublons : {e}")
    exit(1)
