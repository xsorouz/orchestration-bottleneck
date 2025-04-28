# === Script 02 - Upload des fichiers CSV vers MinIO (avec loguru) ===
# Ce script envoie les fichiers CSV générés dans le bucket MinIO, sous un chemin précis.

import boto3                        # Boto3 pour interagir avec MinIO (API compatible S3)
from pathlib import Path             # Gestion portable des chemins de fichiers
from loguru import logger            # Logger performant
import sys

import warnings
warnings.filterwarnings("ignore")

# Configuration des logs
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")

LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "upload_minio.log", level="INFO", rotation="500 KB")

# ------------------------------------------------------------------------
# Paramètres de connexion à MinIO
# ------------------------------------------------------------------------
MINIO_ENDPOINT = "minio:9000"    # ⚡ Adresse MinIO (Docker: c’est le nom du service)
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET_NAME = "bottleneck"
DESTINATION_PREFIX = "data/raw/"  # Chemin dans MinIO (le "répertoire" cible)

# ------------------------------------------------------------------------
# Connexion au client MinIO
# ------------------------------------------------------------------------
try:
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",  # Peu importe pour MinIO
    )
    logger.success("✅ Connexion à MinIO établie.")
except Exception as e:
    logger.error(f"❌ Erreur de connexion à MinIO : {e}")
    exit(1)

# ------------------------------------------------------------------------
# Fichiers à uploader
# ------------------------------------------------------------------------
CSV_PATH = Path("data/raw")
files_to_upload = ["erp.csv", "web.csv", "liaison.csv"]

# ------------------------------------------------------------------------
# Upload de chaque fichier
# ------------------------------------------------------------------------
for filename in files_to_upload:
    local_path = CSV_PATH / filename
    s3_key = f"{DESTINATION_PREFIX}{filename}"  # Exemple : "data/raw/erp.csv"

    if not local_path.exists():
        logger.error(f"❌ Fichier manquant : {local_path}")
        exit(1)

    try:
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=BUCKET_NAME,
            Key=s3_key
        )
        logger.success(f"✅ {filename} uploadé avec succès sur MinIO ({s3_key})")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'upload de {filename} : {e}")
        exit(1)

logger.info("🎉 Tous les fichiers CSV ont été uploadés avec succès sur MinIO.")
