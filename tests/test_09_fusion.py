# === Script de test 09 - Validation de la fusion des données ===
# Ce script vérifie que la table fusion existe, contient 714 lignes,
# et que les colonnes principales sont présentes.

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
logger.add(LOGS_PATH / "test_09_fusion.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Connexion à DuckDB
# ----------------------------------------------------------------------
try:
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.success("✅ Connexion à DuckDB réussie.")
except Exception as e:
    logger.error(f"❌ Connexion échouée : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Vérifications
# ----------------------------------------------------------------------
try:
    nb_rows = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]
    assert nb_rows == 714, f"❌ La table fusion contient {nb_rows} lignes (attendu : 714)"
    logger.success(f"✅ Table fusion : {nb_rows} lignes (attendu : 714)")

    columns = con.execute("PRAGMA table_info('fusion')").fetchdf()["name"].tolist()
    for col in ["product_id", "price", "stock_status", "post_title"]:
        assert col in columns, f"❌ Colonne manquante : {col}"
    logger.success("✅ Toutes les colonnes clés sont présentes dans fusion.")

    logger.success("🎉 Validation complète de la fusion réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans les tests de validation de la fusion : {e}")
    exit(1)
