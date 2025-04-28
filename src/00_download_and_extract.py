# === Script 00 - Téléchargement et extraction ZIP (robuste et compatible S3) ===
# Ce script télécharge une archive ZIP depuis une URL, l'extrait localement
# dans un dossier 'data/raw/', avec nettoyage des noms de fichiers pour éviter
# tout problème d'upload futur vers MinIO/S3. Tous les événements sont journalisés.

import requests
from zipfile import ZipFile
from io import BytesIO
from pathlib import Path
import unicodedata
import re
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
logger.add(LOGS_PATH / "download_extract.log", level="INFO", rotation="500 KB")

# ==============================================================================
# Paramètres du téléchargement
# ==============================================================================
ZIP_URL = (
    "https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/"
    "922_P10/bottleneck.zip"
)
RAW_PATH = Path("data/raw")    # Répertoire cible des fichiers extraits
RAW_PATH.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# Fonction utilitaire : normalisation sécurisée des noms de fichiers
# ==============================================================================
def normalize_filename(filename: str) -> str:
    nfkd_form = unicodedata.normalize('NFKD', filename)
    only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('ASCII')
    safe = re.sub(r'[^A-Za-z0-9_.-]', '_', only_ascii)
    return safe

# ==============================================================================
# Étape 1 : Téléchargement de l'archive ZIP
# ==============================================================================
logger.info("📦 Début du téléchargement de l'archive ZIP...")

try:
    response = requests.get(ZIP_URL)
    response.raise_for_status()
    logger.success("✅ Archive ZIP téléchargée avec succès.")
except Exception as e:
    logger.error(f"❌ Erreur lors du téléchargement : {e}")
    sys.exit(1)

# ==============================================================================
# Étape 2 : Extraction sécurisée du contenu de l'archive
# ==============================================================================
logger.info("📂 Début de l'extraction de l'archive...")

try:
    with ZipFile(BytesIO(response.content)) as zip_ref:
        for member in zip_ref.infolist():
            original_name = Path(member.filename).name
            if not original_name:
                continue  # Ignore les répertoires vides

            safe_name = normalize_filename(original_name)
            target_path = RAW_PATH / safe_name

            # Écriture sécurisée
            with open(target_path, "wb") as f_out:
                f_out.write(zip_ref.read(member))
            
            logger.info(f"✅ Fichier extrait et normalisé : {safe_name}")

    logger.success(f"✅ Extraction complète vers '{RAW_PATH.resolve()}'")
except Exception as e:
    logger.error(f"❌ Erreur lors de l'extraction : {e}")
    sys.exit(1)

# ==============================================================================
# Étape 3 : Validation finale de la présence des fichiers attendus
# ==============================================================================
expected_files = [
    "Fichier_erp.xlsx",
    "Fichier_web.xlsx",
    "fichier_liaison.xlsx",
]
expected_files_normalized = [normalize_filename(f) for f in expected_files]

missing_files = [f for f in expected_files_normalized if not (RAW_PATH / f).exists()]

if missing_files:
    logger.error(f"❌ Fichiers manquants après extraction : {missing_files}")
    sys.exit(1)

logger.success("🎯 Tous les fichiers Excel attendus sont présents après extraction :")
for f in expected_files_normalized:
    logger.info(f"   - {f}")

logger.success("🎉 Téléchargement et extraction terminés sans erreur.")

