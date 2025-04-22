# === Script de test - Validation de la fusion des données (étape 04) ===
# Ce script vérifie que la table `fusion` a été correctement créée,
# contient exactement 714 lignes, et que les colonnes clés sont présentes.

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
logger.add(LOGS_PATH / "test_fusion.log", level="INFO", rotation="500 KB")

# === Connexion à la base DuckDB ===
try:
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🧪 Connexion à DuckDB réussie.")
except Exception as e:
    logger.error(f"❌ Connexion échouée : {e}")
    exit(1)

# === Validation de la table fusion ===
try:
    nb_rows = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]
    assert nb_rows == 714, f"❌ La table fusion ne contient pas 714 lignes (actuel : {nb_rows})"
    logger.success(f"✅ Table fusion : {nb_rows} lignes (attendu : 714)")

    # Vérification que certaines colonnes attendues existent
    columns = con.execute("PRAGMA table_info('fusion')").fetchdf()["name"].tolist()
    for col in ["product_id", "price", "stock_status", "post_title"]:
        assert col in columns, f"❌ Colonne manquante dans fusion : {col}"
    logger.success("✅ Colonnes clés présentes dans la table fusion")

    logger.success("🎉 Validation de la fusion réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans les tests de fusion : {e}")
    exit(1)
