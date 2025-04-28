# === Script de test 08 - Validation de la création des tables dédoublonnées ===
# Ce script vérifie que les tables erp_dedup, web_dedup, liaison_dedup
# existent dans DuckDB et contiennent des données.

import duckdb
from pathlib import Path
from loguru import logger
import sys
import warnings

warnings.filterwarnings("ignore")

# ----------------------------------------------------------------------
# Logger
# ----------------------------------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "test_08_dedoublonnage.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Connexion à DuckDB
# ----------------------------------------------------------------------
try:
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🧪 Connexion à DuckDB réussie.")
except Exception as e:
    logger.error(f"❌ Connexion échouée : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Validation des tables dédoublonnées
# ----------------------------------------------------------------------
try:
    nb_erp     = con.execute("SELECT COUNT(*) FROM erp_dedup").fetchone()[0]
    nb_web     = con.execute("SELECT COUNT(*) FROM web_dedup").fetchone()[0]
    nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_dedup").fetchone()[0]

    assert nb_erp > 0, "❌ La table erp_dedup est vide"
    assert nb_web > 0, "❌ La table web_dedup est vide"
    assert nb_liaison > 0, "❌ La table liaison_dedup est vide"

    logger.success(f"✅ erp_dedup : {nb_erp} lignes")
    logger.success(f"✅ web_dedup : {nb_web} lignes")
    logger.success(f"✅ liaison_dedup : {nb_liaison} lignes")
    logger.success("🎯 Test de création des tables dédoublonnées réussi.")

except Exception as e:
    logger.error(f"❌ Erreur lors du test de dédoublonnage : {e}")
    exit(1)
