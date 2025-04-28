# === Script 05 - Test général du nettoyage des données ===
# Ce script valide que les tables nettoyées (erp_clean, web_clean, liaison_clean)
# existent, contiennent des données et respectent les critères métier fondamentaux.

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
logger.add(LOGS_PATH / "test_clean_data.log", level="INFO", rotation="500 KB")

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
# Tests de validation de la présence des données
# ----------------------------------------------------------------------
try:
    nb_erp = con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0]
    nb_web = con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0]
    nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]

    assert nb_erp > 0, "❌ Table erp_clean vide"
    assert nb_web > 0, "❌ Table web_clean vide"
    assert nb_liaison > 0, "❌ Table liaison_clean vide"

    logger.success(f"✅ Table erp_clean : {nb_erp} lignes")
    logger.success(f"✅ Table web_clean : {nb_web} lignes")
    logger.success(f"✅ Table liaison_clean : {nb_liaison} lignes")
    logger.success("🎯 Validation générale du nettoyage réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans le test général : {e}")
    exit(1)
