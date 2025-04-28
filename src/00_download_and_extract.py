# === Script 00 - Téléchargement et extraction ZIP (robuste, S3-compliant) ===
# Ce script télécharge une archive ZIP depuis une URL, l'extrait localement
# dans un dossier raw/, tout en nettoyant les noms des fichiers pour assurer
# une compatibilité avec MinIO/S3. Chaque étape est loguée.

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

# ----------------------------------------------------------------------
# Configuration des logs
# ----------------------------------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "download_extract.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Paramètres de l'archive
# ----------------------------------------------------------------------
ZIP_URL = (
    "https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/"
    "922_P10/bottleneck.zip"
)

RAW_PATH = Path("data/raw")
EXTRACTED_PATH = RAW_PATH 

RAW_PATH.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------
# Fonction utilitaire pour nettoyer un nom de fichier
# ----------------------------------------------------------------------
def normalize_filename(filename: str) -> str:
    # Enlève les accents
    nfkd_form = unicodedata.normalize('NFKD', filename)
    only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('ASCII')
    # Remplace les caractères spéciaux par des "_"
    safe = re.sub(r'[^A-Za-z0-9_.-]', '_', only_ascii)
    return safe

# ----------------------------------------------------------------------
# Étape 1 : Téléchargement
# ----------------------------------------------------------------------
logger.info("📦 Téléchargement de l'archive ZIP en cours...")

try:
    response = requests.get(ZIP_URL)
    response.raise_for_status()
    logger.success("✅ Archive ZIP téléchargée avec succès.")
except Exception as e:
    logger.error(f"❌ Erreur lors du téléchargement : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Étape 2 : Extraction de l'archive
# ----------------------------------------------------------------------
try:
    with ZipFile(BytesIO(response.content)) as zip_ref:
        for member in zip_ref.infolist():
            original_name = Path(member.filename).name
            if not original_name:
                continue  # Ignore les dossiers vides

            safe_name = normalize_filename(original_name)
            target_path = EXTRACTED_PATH / safe_name

            # Écriture sécurisée du fichier
            with open(target_path, "wb") as f_out:
                f_out.write(zip_ref.read(member))
            logger.info(f"✅ Fichier extrait et normalisé : {safe_name}")

    logger.success(f"✅ Archive extraite dans {EXTRACTED_PATH.resolve()}")
except Exception as e:
    logger.error(f"❌ Erreur lors de l'extraction : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Étape 3 : Validation de la présence des fichiers attendus
# ----------------------------------------------------------------------
expected_files = [
    "Fichier_erp.xlsx",
    "Fichier_web.xlsx",
    "fichier_liaison.xlsx",
]

# Après normalisation
expected_files = [normalize_filename(f) for f in expected_files]

missing = [f for f in expected_files if not (EXTRACTED_PATH / f).exists()]

if missing:
    logger.error(f"❌ Fichiers manquants après extraction : {missing}")
    exit(1)

logger.success("🎯 Tous les fichiers Excel attendus sont présents :")
for f in expected_files:
    logger.info(f"   - bottleneck/{f}")

logger.info("🎉 Téléchargement et extraction terminés avec succès.")
