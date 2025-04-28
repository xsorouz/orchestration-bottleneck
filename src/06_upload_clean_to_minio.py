# === Script 06 - Upload des fichiers nettoyés vers MinIO (outputs) ===
# Ce script charge les fichiers nettoyés présents dans 'data/outputs/'
# et les envoie dans le bucket MinIO sous le chemin 'data/outputs/'.

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
logger.add(LOGS_PATH / "upload_clean_to_minio.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Paramètres MinIO
# ----------------------------------------------------------------------
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET_NAME = "bottleneck"
DESTINATION_PREFIX = "data/outputs/"

# ----------------------------------------------------------------------
# Connexion au client MinIO
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
    logger.error(f"❌ Échec de la connexion à MinIO : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Vérification de l'existence du bucket
# ----------------------------------------------------------------------
try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
    logger.success(f"✅ Bucket '{BUCKET_NAME}' disponible.")
except ClientError as e:
    logger.error(f"❌ Bucket '{BUCKET_NAME}' introuvable ou inaccessible : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Liste des fichiers locaux à uploader
# ----------------------------------------------------------------------
OUTPUTS_PATH = Path("data/outputs")
files_to_upload = ["erp_clean.csv", "web_clean.csv", "liaison_clean.csv"]

# ----------------------------------------------------------------------
# Upload de chaque fichier
# ----------------------------------------------------------------------
logger.info("📤 Début de l'upload des fichiers nettoyés vers MinIO...")

for filename in files_to_upload:
    local_path = OUTPUTS_PATH / filename
    s3_key = f"{DESTINATION_PREFIX}{filename}"

    if not local_path.exists():
        logger.error(f"❌ Fichier local manquant pour upload : {local_path}")
        exit(1)

    try:
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )
        logger.success(f"✅ Upload réussi : {filename} ➔ {s3_key}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'upload de {filename} : {e}")
        exit(1)

logger.success("🎯 Tous les fichiers nettoyés ont été uploadés avec succès dans MinIO sous 'data/outputs/'.")
