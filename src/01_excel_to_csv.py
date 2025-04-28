# === Script 01 - Conversion de fichiers Excel en CSV (robuste et sécurisé) ===
# Ce script convertit les fichiers Excel extraits en fichiers CSV,
# en assurant un nettoyage minimal des données et une journalisation claire.

import pandas as pd
from pathlib import Path
from loguru import logger
import sys
import warnings

warnings.filterwarnings("ignore")

# ==============================================================================
# Configuration des logs
# ==============================================================================
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "conversion_excel_csv.log", level="INFO", rotation="500 KB")

# ==============================================================================
# Chemins d'entrée (fichiers Excel) et de sortie (fichiers CSV)
# ==============================================================================
EXTRACTED_PATH = Path("data/raw")   # 📂 Contient les fichiers .xlsx extraits
CSV_OUTPUT_PATH = Path("data/raw")  # 📄 Destination des CSV

CSV_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# Liste des fichiers à traiter
# ==============================================================================
files_mapping = {
    "Fichier_erp.xlsx": "erp.csv",
    "Fichier_web.xlsx": "web.csv",
    "fichier_liaison.xlsx": "liaison.csv"
}

# ==============================================================================
# Fonction utilitaire : nettoyage simple des DataFrames
# ==============================================================================
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all", axis=0)  # Supprime les lignes complètement vides
    df = df.dropna(how="all", axis=1)  # Supprime les colonnes complètement vides
    return df

# ==============================================================================
# Début du traitement
# ==============================================================================
logger.info("🔄 Début de la conversion des fichiers Excel en CSV...")

for excel_file, csv_file in files_mapping.items():
    excel_path = EXTRACTED_PATH / excel_file
    csv_path = CSV_OUTPUT_PATH / csv_file

    if not excel_path.exists():
        logger.error(f"❌ Fichier Excel introuvable : {excel_file}")
        sys.exit(1)

    try:
        # Chargement
        df = pd.read_excel(excel_path)

        # Nettoyage
        df = clean_dataframe(df)

        # Sauvegarde en CSV
        df.to_csv(csv_path, index=False)

        # Vérifications
        if not csv_path.exists():
            raise FileNotFoundError(f"Fichier CSV non généré : {csv_file}")
        if df.empty:
            raise ValueError(f"DataFrame vide après conversion : {csv_file}")

        logger.success(f"✅ {excel_file} ➔ {csv_file} ({len(df)} lignes)")

    except Exception as e:
        logger.error(f"❌ Erreur lors du traitement de {excel_file} : {e}")
        sys.exit(1)

# ==============================================================================
# Fin du script
# ==============================================================================
logger.success("🎯 Tous les fichiers Excel ont été convertis et nettoyés avec succès.")

