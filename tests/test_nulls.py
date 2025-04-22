# === Script de test - Vérification des valeurs nulles (tables nettoyées) ===

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
logger.add(LOGS_PATH / "test_nulls.log", level="INFO", rotation="500 KB")

try:
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🧪 Connexion à DuckDB réussie.")
except Exception as e:
    logger.error(f"❌ Connexion échouée : {e}")
    exit(1)

try:
    # === Test nulls ERP ===
    erp_nulls = con.execute("""
        SELECT COUNT(*) FROM erp_clean
        WHERE product_id IS NULL OR onsale_web IS NULL
        OR price IS NULL OR stock_quantity IS NULL OR stock_status IS NULL
    """).fetchone()[0]
    assert erp_nulls == 0, f"❌ Valeurs nulles détectées dans erp_clean : {erp_nulls}"

    # === Test nulls WEB ===
    web_nulls = con.execute("""
        SELECT COUNT(*) FROM web_clean
        WHERE sku IS NULL OR post_title IS NULL OR post_status IS NULL
    """).fetchone()[0]
    assert web_nulls == 0, f"❌ Valeurs nulles détectées dans web_clean : {web_nulls}"

    # === Test nulls LIAISON ===
    liaison_nulls = con.execute("""
        SELECT COUNT(*) FROM liaison_clean
        WHERE product_id IS NULL OR id_web IS NULL
    """).fetchone()[0]
    assert liaison_nulls == 0, f"❌ Valeurs nulles détectées dans liaison_clean : {liaison_nulls}"

    logger.success("✅ Aucun champ NULL dans les données nettoyées.")
    logger.success("🎉 Validation des valeurs nulles réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans les tests de nulls : {e}")
    exit(1)
