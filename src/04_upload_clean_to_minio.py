# === Script 04 - Téléchargement des fichiers bruts depuis MinIO vers data/raw/ ===
# Ce script télécharge les fichiers CSV extraits (erp.csv, web.csv, liaison.csv)
# depuis le bucket MinIO 'bottleneck', et les stocke localement dans 'data/raw/'.
# Ces fichiers serviront ensuite pour les étapes de nettoyage.
#
# Toutes les opérations sont loguées avec Loguru.

import boto3
from pathlib import Path
from loguru import logger
import sys
import warnings
from botocore.exceptions import ClientError

warnings.filterwarnings("ignore")
 
# ----------------------------------------------------------------------
# Configuration des logs
# ----------------------------------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "download_from_minio.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Paramètres MinIO
# ----------------------------------------------------------------------
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET_NAME = "bottleneck"
PREFIX = "data/raw/"

# ----------------------------------------------------------------------
# Connexion MinIO
# ----------------------------------------------------------------------
try:
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )
    logger.success("✅ Connexion à MinIO établie avec succès.")
except Exception as e:
    logger.error(f"❌ Échec de connexion à MinIO : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Liste des fichiers à télécharger
# ----------------------------------------------------------------------
files_to_download = ["erp.csv", "web.csv", "liaison.csv"]

LOCAL_PATH = Path("data/raw")
LOCAL_PATH.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------
# Téléchargement de chaque fichier
# ----------------------------------------------------------------------
for filename in files_to_download:
    s3_key = f"{PREFIX}{filename}"
    local_file = LOCAL_PATH / filename

    try:
        s3_client.download_file(BUCKET_NAME, s3_key, str(local_file))
        logger.success(f"✅ Fichier téléchargé : {filename}")
    except ClientError as e:
        logger.error(f"❌ Erreur lors du téléchargement de {filename} : {e}")
        exit(1)

logger.success("🎯 Tous les fichiers CSV ont été téléchargés depuis MinIO avec succès.")
