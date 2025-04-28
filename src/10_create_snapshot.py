# === Script 10 - Snapshot de la base DuckDB après fusion ===
# Ce script copie le fichier de base de données après fusion
# pour créer une sauvegarde figée du travail de nettoyage, dédoublonnage et fusion.

import shutil
from pathlib import Path
from loguru import logger
import sys

# Configuration du logger
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "snapshot_duckdb.log", level="INFO", rotation="500 KB")

# Chemins
DATA_PATH = Path("data")
SOURCE_DUCKDB = DATA_PATH / "bottleneck.duckdb"
SNAPSHOT_DIR = DATA_PATH / "snapshots"
SNAPSHOT_FILE = SNAPSHOT_DIR / "bottleneck_fusion_ok.duckdb"

# Création du dossier snapshots/ si besoin
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

try:
    shutil.copy2(SOURCE_DUCKDB, SNAPSHOT_FILE)
    logger.success(f"✅ Snapshot créé : {SNAPSHOT_FILE}")
except Exception as e:
    logger.error(f"❌ Erreur lors de la création du snapshot : {e}")
    exit(1)

logger.success("🎯 Sauvegarde de la base DuckDB après fusion réussie.")
