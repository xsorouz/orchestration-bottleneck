# === Script 02 - Upload sécurisé des fichiers CSV vers MinIO (avec loguru) ===
# Ce script envoie les CSV générés vers un bucket MinIO, en vérifiant
# la connexion, la présence des fichiers locaux, l’existence du bucket,
# et la réussite de chaque transfert.

import boto3
from pathlib import Path
from loguru import logger
import sys
import warnings
from botocore.exceptions import ClientError

warnings.filterwarnings("ignore")

# ==============================================================================
# Configuration des logs
# ==============================================================================
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "upload_minio.log", level="INFO", rotation="500 KB")

# ==============================================================================
# Paramètres MinIO
# ==============================================================================
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET_NAME = "bottleneck"
DESTINATION_PREFIX = "data/raw/"

# ==============================================================================
# Connexion au client MinIO
# ==============================================================================
try:
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",  # MinIO accepte n'importe quelle région
    )
    logger.success("✅ Connexion à MinIO établie avec succès.")
except Exception as e:
    logger.error(f"❌ Échec de la connexion à MinIO : {e}")
    sys.exit(1)

# ==============================================================================
# Vérification de l'existence du bucket
# ==============================================================================
try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
    logger.success(f"✅ Bucket '{BUCKET_NAME}' accessible.")
except ClientError as e:
    logger.error(f"❌ Bucket '{BUCKET_NAME}' introuvable ou inaccessible : {e}")
    sys.exit(1)

# ==============================================================================
# Liste des fichiers locaux à uploader
# ==============================================================================
CSV_PATH = Path("data/raw")
files_to_upload = ["erp.csv", "web.csv", "liaison.csv"]

# ==============================================================================
# Upload de chaque fichier avec journalisation
# ==============================================================================
logger.info("🚀 Début de l'upload des fichiers CSV vers MinIO...")

for filename in files_to_upload:
    local_path = CSV_PATH / filename
    s3_key = f"{DESTINATION_PREFIX}{filename}"

    if not local_path.exists():
        logger.error(f"❌ Fichier local manquant : {local_path}")
        sys.exit(1)

    try:
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=BUCKET_NAME,
            Key=s3_key
        )
        logger.success(f"✅ Upload réussi : {filename} ➔ {s3_key}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'upload de {filename} : {e}")
        sys.exit(1)

# ==============================================================================
# Fin du script
# ==============================================================================
logger.success("🎉 Tous les fichiers CSV ont été uploadés avec succès vers MinIO.")
