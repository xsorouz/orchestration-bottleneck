from pathlib import Path
import boto3
from loguru import logger

LOGS_PATH = Path("logs")
BUCKET_NAME = "bottleneck"
MINIO_ENDPOINT = "http://host.docker.internal:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
)

for log_file in LOGS_PATH.glob("*.log"):
    s3_key = f"logs/{log_file.name}"
    try:
        s3_client.upload_file(str(log_file), BUCKET_NAME, s3_key)
        logger.success(f"✅ Log uploadée : {log_file.name}")
    except Exception as e:
        logger.warning(f"⚠️ Erreur d’upload {log_file.name} : {e}")
