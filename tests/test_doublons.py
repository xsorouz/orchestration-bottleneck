# === Script de test - Vérification de l'absence de doublons après dédoublonnage ===

import duckdb
from pathlib import Path
from loguru import logger
import sys

# Logger
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
        SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM erp_dedup
    """).fetchone()[0]
    web_dup = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT sku) FROM web_dedup
    """).fetchone()[0]
    liaison_dup = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM liaison_dedup
    """).fetchone()[0]

    assert erp_dup == 0, f"❌ Doublons détectés dans erp_dedup : {erp_dup}"
    assert web_dup == 0, f"❌ Doublons détectés dans web_dedup : {web_dup}"
    assert liaison_dup == 0, f"❌ Doublons détectés dans liaison_dedup : {liaison_dup}"

    logger.success("✅ Aucun doublon détecté après dédoublonnage.")
    logger.success("🎉 Validation post-dédoublonnage réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans les tests de doublons : {e}")
    exit(1)
