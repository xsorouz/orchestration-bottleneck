# === Script 03 - Vérification de la présence des fichiers CSV dans MinIO (avec loguru) ===
# Ce script contrôle l'existence et la taille des fichiers uploadés dans le bucket MinIO.

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

LOGS_PATH = Path("./logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "verify_upload.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Paramètres MinIO
# ----------------------------------------------------------------------
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET_NAME = "bottleneck"
PREFIX = "data/raw/"  # Le sous-dossier à vérifier dans MinIO

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
    logger.error(f"❌ Échec de connexion à MinIO : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Vérification de l'existence du bucket
# ----------------------------------------------------------------------
try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
    logger.success(f"✅ Bucket '{BUCKET_NAME}' accessible.")
except ClientError as e:
    logger.error(f"❌ Bucket '{BUCKET_NAME}' inaccessible ou inexistant : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Listing des fichiers dans le bucket
# ----------------------------------------------------------------------
try:
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    contents = response.get('Contents', [])

    if not contents:
        logger.error(f"❌ Aucun fichier trouvé dans {BUCKET_NAME}/{PREFIX}.")
        exit(1)

    logger.success(f"✅ {len(contents)} fichier(s) trouvé(s) dans {BUCKET_NAME}/{PREFIX} :")
    for obj in contents:
        logger.info(f"   - {obj['Key']} ({obj['Size']} octets)")

    # Vérification que tous les fichiers attendus sont présents
    expected_files = {f"{PREFIX}erp.csv", f"{PREFIX}web.csv", f"{PREFIX}liaison.csv"}
    found_files = {obj['Key'] for obj in contents}

    if not expected_files.issubset(found_files):
        logger.error(f"❌ Fichiers manquants. Attendus : {expected_files}. Trouvés : {found_files}.")
        exit(1)

    logger.success("🎯 Tous les fichiers attendus sont présents dans MinIO.")

except Exception as e:
    logger.error(f"❌ Erreur lors de la vérification des fichiers dans MinIO : {e}")
    exit(1)
