# === Script 01 - Conversion Excel -> CSV (avec loguru) ===
# Ce script lit des fichiers Excel depuis un répertoire raw, les convertit en CSV,
# et journalise chaque étape à l'aide de loguru.

import pandas as pd                     # Pandas pour la manipulation des DataFrame et la lecture/écriture de fichiers
from pathlib import Path                # Gestion portable des chemins de fichiers
from loguru import logger               # Logger simple à configurer et riche en fonctionnalités

logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")  # warnings, errors et criticals

# ------------------------------------------------------------------------------
# Configuration des logs
# - Création du dossier de logs s'il n'existe pas
# - Ajout d'un fichier de log (niveau INFO, rotation tous les 500 KB)
# ------------------------------------------------------------------------------
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "conversion.log", level="INFO", rotation="500 KB")

# ------------------------------------------------------------------------------
# Chemins d'entrée et de sortie
# RAW_PATH : dossier contenant les fichiers Excel
# CSV_PATH : dossier de destination pour les CSV générés
# ------------------------------------------------------------------------------
RAW_PATH = Path("data/raw/bottleneck")
CSV_PATH = Path("data/raw")
CSV_PATH.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------------------
# Correspondance entre les fichiers Excel à convertir et leurs noms de sortie CSV
# ------------------------------------------------------------------------------
files = {
    "Fichier_erp.xlsx":      "erp.csv",
    "Fichier_web.xlsx":      "web.csv",
    "fichier_liaison.xlsx":  "liaison.csv"
}

logger.info("🔄 Début de la conversion Excel vers CSV...")

# ------------------------------------------------------------------------------
# Boucle de conversion : pour chaque fichier Excel
# 1. Lecture avec pandas.read_excel
# 2. Écriture en CSV sans index
# 3. Log de succès avec le nombre de lignes
# 4. Vérifications : existence du fichier et non-vacuité
# ------------------------------------------------------------------------------
for excel_file, csv_file in files.items():
    excel_path = RAW_PATH / excel_file   # Chemin complet vers le fichier Excel source
    csv_path = CSV_PATH / csv_file       # Chemin complet vers le fichier CSV cible

    try:
        df = pd.read_excel(excel_path)          # Lecture du fichier Excel
        df.to_csv(csv_path, index=False)        # Écriture en CSV sans la colonne d'index
        logger.success(f"{excel_file} → {csv_file} ({len(df)} lignes)")  # Log de succès

        # Vérifications post-conversion
        assert csv_path.exists(), f"Fichier non généré : {csv_file}"
        assert len(df) > 0,         f"Fichier vide : {csv_file}"
    except Exception as e:
        # En cas d'erreur, log de l'erreur et sortie du script
        logger.error(f"❌ Erreur sur {excel_file} : {e}")
        exit(1)

# ------------------------------------------------------------------------------
# Fin du script : tous les fichiers ont été traités avec succès
# ------------------------------------------------------------------------------
logger.info("🎉 Tous les fichiers Excel ont été convertis en CSV avec succès.")
