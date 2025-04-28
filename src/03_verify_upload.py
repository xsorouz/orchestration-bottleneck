# === Script 03 - Vérification des fichiers uploadés sur MinIO (avec loguru) ===
# Ce script liste les fichiers présents dans MinIO, vérifie la présence attendue, et journalise.

import boto3
from pathlib import Path
from loguru import logger
import sys
import warnings

warnings.filterwarnings("ignore")

# -------------------------------------------------------------------
# Configuration des logs
# -------------------------------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "verify_upload.log", level="INFO", rotation="500 KB")

# -------------------------------------------------------------------
# Paramètres de connexion MinIO
# -------------------------------------------------------------------
MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET_NAME = "bottleneck"
PREFIX = "data/raw/"

# -------------------------------------------------------------------
# Connexion MinIO
# -------------------------------------------------------------------
try:
    s3 = boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )
    logger.success("✅ Connexion à MinIO pour la vérification établie.")
except Exception as e:
    logger.error(f"❌ Erreur de connexion MinIO : {e}")
    exit(1)

# -------------------------------------------------------------------
# Vérification des fichiers
# -------------------------------------------------------------------
logger.info(f"🔍 Listing des fichiers sous 's3://{BUCKET_NAME}/{PREFIX}'...")

try:
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    contents = response.get('Contents', [])

    if not contents:
        logger.error("❌ Aucun fichier trouvé dans le dossier data/raw de MinIO.")
        exit(1)

    logger.success("✅ Fichiers trouvés :")
    for obj in contents:
        logger.info(f"   - {obj['Key']} ({obj['Size']} octets)")

    if len(contents) < 3:
        logger.error("❌ Moins de 3 fichiers présents. Vérifiez l'upload.")
        exit(1)

    logger.success("🎯 Tous les fichiers attendus sont présents dans MinIO.")

except Exception as e:
    logger.error(f"❌ Erreur lors de la vérification MinIO : {e}")
    exit(1)
