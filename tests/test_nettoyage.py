# === Script de test - Validation du nettoyage des données (étape 02) ===
# Ce script vérifie que les tables nettoyées (erp_clean, web_clean, liaison_clean)
# ont bien été créées, contiennent des données, et respectent les attentes métier.

import duckdb
from pathlib import Path
from loguru import logger
import sys

# === Configuration du logger ===
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "test_nettoyage.log", level="INFO", rotation="500 KB")

# === Connexion à DuckDB ===
try:
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🧪 Connexion à DuckDB réussie.")
except Exception as e:
    logger.error(f"❌ Connexion échouée : {e}")
    exit(1)

# === Tests de validation ===
try:
    nb_erp = con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0]
    nb_web = con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0]
    nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]

    # Vérification : les tables existent et contiennent des données
    assert nb_erp > 0,      "❌ La table erp_clean est vide"
    assert nb_web > 0,      "❌ La table web_clean est vide"
    assert nb_liaison > 0,  "❌ La table liaison_clean est vide"

    logger.success(f"✅ Table erp_clean : {nb_erp} lignes")
    logger.success(f"✅ Table web_clean : {nb_web} lignes")
    logger.success(f"✅ Table liaison_clean : {nb_liaison} lignes")
    logger.success("🎉 Validation du nettoyage réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans les tests de nettoyage : {e}")
    exit(1)
