# === Script 00 - Téléchargement et extraction ZIP (avec loguru) ===
# Ce script télécharge une archive ZIP depuis une URL, l'extrait localement,
# puis vérifie la présence des fichiers Excel attendus, tout en journalisant
# chaque étape via loguru.

import requests                         # Librairie pour effectuer des requêtes HTTP (telechargement de fichiers)
from zipfile import ZipFile             # Module pour manipuler les archives ZIP
from io import BytesIO                  # Permet de traiter un flux d'octets en mémoire comme un fichier
from pathlib import Path                # Gestion portable des chemins de fichiers
from loguru import logger                # Logger performant pour tracer les événements et erreurs

# ------------------------------------------------------------------------------
# Configuration des logs
# - Crée le dossier de logs
# - Ajoute un fichier de log avec rotation
# ------------------------------------------------------------------------------
LOGS_PATH = Path("data/logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)  # Crée le répertoire et parents sans erreur s'il existe
# Fichier de log : niveau INFO, rotation tous les 500 KB
logger.add(LOGS_PATH / "download_extract.log", level="INFO", rotation="500 KB")

# ------------------------------------------------------------------------------
# Paramètres de configuration
# ------------------------------------------------------------------------------
ZIP_URL = (
    "https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/"
    "922_P10/bottleneck.zip"
)
RAW_PATH = Path("data/raw")              # Répertoire où stocker l'archive et son contenu
EXTRACTED_PATH = RAW_PATH / "bottleneck"  # Sous-dossier créé après extraction

# Crée le dossier `data/raw` si nécessaire
RAW_PATH.mkdir(parents=True, exist_ok=True)

logger.info("📦 Téléchargement de l’archive en cours...")

# ------------------------------------------------------------------------------
# Étape 1 : Téléchargement de l'archive ZIP
# ------------------------------------------------------------------------------
try:
    response = requests.get(ZIP_URL)       # Envoie la requête GET
    response.raise_for_status()            # Vérifie le code HTTP (lève en cas d'erreur)
    logger.success("✅ Archive téléchargée avec succès.")
except Exception as e:
    logger.error(f"❌ Erreur lors du téléchargement : {e}")
    exit(1)                                 # Quitte le script en cas d'erreur

# ------------------------------------------------------------------------------
# Étape 2 : Extraction du contenu de l'archive ZIP
# ------------------------------------------------------------------------------
try:
    # Utilise BytesIO pour lire les octets téléchargés comme un fichier ZIP
    with ZipFile(BytesIO(response.content)) as zip_ref:
        zip_ref.extractall(RAW_PATH)       # Extrait tous les fichiers dans RAW_PATH
    logger.success(f"✅ Archive extraite dans : {EXTRACTED_PATH.resolve()}")
except Exception as e:
    logger.error(f"❌ Erreur lors de l’extraction : {e}")
    exit(1)

# ------------------------------------------------------------------------------
# Étape 3 : Validation de la présence des fichiers Excel attendus
# ------------------------------------------------------------------------------
expected_files = [
    "Fichier_erp.xlsx",
    "Fichier_web.xlsx",
    "fichier_liaison.xlsx"
]

# Vérifie l'existence de chaque fichier dans le dossier extrait
missing = [f for f in expected_files if not (EXTRACTED_PATH / f).exists()]

if missing:
    # Log et sortie en erreur si un ou plusieurs fichiers manquent
    logger.error(f"❌ Fichiers manquants après extraction : {missing}")
    exit(1)

# Log de succès et listing des fichiers trouvés
logger.success("✅ Tous les fichiers Excel attendus sont présents :")
for f in expected_files:
    logger.info(f"   - bottleneck/{f}")
