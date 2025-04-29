from pathlib import Path
import boto3
from loguru import logger
import sys

# Constantes
LOGS_PATH = Path("logs")
BUCKET_NAME = "bottleneck"
MINIO_ENDPOINT = "http://host.docker.internal:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

# Client S3
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
)

# Récupérer les logs
logs_files = list(LOGS_PATH.glob("*.log"))
if not logs_files:
    logger.warning("⚠️ Aucun fichier log à uploader.")
    sys.exit(0)

# Upload
for log_file in logs_files:
    s3_key = f"logs/{log_file.name}"
    try:
        s3_client.upload_file(str(log_file), BUCKET_NAME, s3_key)
        logger.success(f"✅ Log uploadée : {log_file.name}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'upload de {log_file.name} : {e}")
        sys.exit(1)

logger.success("🎉 Tous les fichiers logs ont été uploadés avec succès dans MinIO.")
