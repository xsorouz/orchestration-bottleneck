# === Script 01 - Conversion de fichiers Excel en CSV (robuste) ===
# Ce script lit les fichiers Excel extraits, les convertit en CSV dans un dossier
# spécifique, et journalise précisément chaque étape et éventuelle anomalie.

import pandas as pd
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
logger.add(LOGS_PATH / "conversion_excel_csv.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Chemins d'entrée et de sortie
# ----------------------------------------------------------------------
EXTRACTED_PATH = Path("data/raw/bottleneck")  # Dossier contenant les fichiers Excel extraits
CSV_OUTPUT_PATH = Path("data/raw")             # Destination des fichiers CSV

CSV_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------
# Mapping des fichiers à traiter
# ----------------------------------------------------------------------
files_mapping = {
    "Fichier_erp.xlsx": "erp.csv",
    "Fichier_web.xlsx": "web.csv",
    "fichier_liaison.xlsx": "liaison.csv"
}

# ----------------------------------------------------------------------
# Fonction utilitaire pour nettoyer les DataFrames
# ----------------------------------------------------------------------
def clean_dataframe(df):
    # Supprime les colonnes vides éventuelles
    df = df.dropna(how='all', axis=1)
    # Supprime les lignes vides éventuelles
    df = df.dropna(how='all', axis=0)
    return df

# ----------------------------------------------------------------------
# Conversion Excel -> CSV
# ----------------------------------------------------------------------
logger.info("🔄 Début de la conversion des fichiers Excel en CSV...")

for excel_filename, csv_filename in files_mapping.items():
    excel_path = EXTRACTED_PATH / excel_filename
    csv_path = CSV_OUTPUT_PATH / csv_filename

    if not excel_path.exists():
        logger.error(f"❌ Fichier Excel introuvable : {excel_filename}")
        exit(1)

    try:
        df = pd.read_excel(excel_path)
        df = clean_dataframe(df)  # Nettoyage de base avant export

        df.to_csv(csv_path, index=False)

        # Vérifications post-conversion
        if not csv_path.exists():
            raise FileNotFoundError(f"Fichier CSV non généré : {csv_filename}")

        if df.empty:
            raise ValueError(f"DataFrame vide après conversion : {csv_filename}")

        logger.success(f"✅ {excel_filename} ➔ {csv_filename} ({len(df)} lignes)")

    except Exception as e:
        logger.error(f"❌ Erreur sur {excel_filename} : {e}")
        exit(1)

logger.success("🎯 Tous les fichiers Excel ont été convertis et nettoyés avec succès.")
