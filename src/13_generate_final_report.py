# === Script 13 - Génération du rapport final et upload dans MinIO ===
# Ce script synthétise tout le pipeline et archive le rapport dans MinIO.

import duckdb
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from loguru import logger
import sys
import warnings

warnings.filterwarnings("ignore")

# ----------------------------------------------------------------------
# Configuration logger
# ----------------------------------------------------------------------
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "rapport_final.log", level="INFO", rotation="500 KB")

# ----------------------------------------------------------------------
# Connexion DuckDB
# ----------------------------------------------------------------------
try:
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.success("✅ Connexion à DuckDB établie.")
except Exception as e:
    logger.error(f"❌ Connexion à DuckDB échouée : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Récupération des métriques pipeline
# ----------------------------------------------------------------------
try:
    logger.info("📋 Récupération des informations du pipeline...")

    metrics = {}

    # Bruts
    metrics["ERP_brut"] = pd.read_csv("data/raw/erp.csv").shape[0]
    metrics["Web_brut"] = pd.read_csv("data/raw/web.csv").shape[0]
    metrics["Liaison_brut"] = pd.read_csv("data/raw/liaison.csv").shape[0]

    # Nettoyés
    metrics["ERP_nettoye"] = con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0]
    metrics["Web_nettoye"] = con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0]
    metrics["Liaison_nettoye"] = con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]

    # Dédoublonnés
    metrics["ERP_dedup"] = con.execute("SELECT COUNT(*) FROM erp_dedup").fetchone()[0]
    metrics["Web_dedup"] = con.execute("SELECT COUNT(*) FROM web_dedup").fetchone()[0]
    metrics["Liaison_dedup"] = con.execute("SELECT COUNT(*) FROM liaison_dedup").fetchone()[0]

    # Fusion
    metrics["Fusion"] = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]

    # CA
    metrics["CA_total"] = con.execute("SELECT ca_total FROM ca_total").fetchone()[0]
    metrics["Produits_CA"] = con.execute("SELECT COUNT(*) FROM ca_par_produit").fetchone()[0]

    # Z-score
    metrics["Vins_millesimes"] = pd.read_csv("data/outputs/vins_millesimes.csv").shape[0]

    logger.success("✅ Collecte des données réussie.")

except Exception as e:
    logger.error(f"❌ Erreur récupération pipeline : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Construction du DataFrame de rapport
# ----------------------------------------------------------------------
try:
    df_report = pd.DataFrame([
        {"Étape": "Brut - ERP",         "Résultat": metrics["ERP_brut"],          "Attendu": ""},
        {"Étape": "Brut - Web",         "Résultat": metrics["Web_brut"],          "Attendu": ""},
        {"Étape": "Brut - Liaison",     "Résultat": metrics["Liaison_brut"],      "Attendu": ""},
        {"Étape": "Nettoyé - ERP",      "Résultat": metrics["ERP_nettoye"],       "Attendu": ""},
        {"Étape": "Nettoyé - Web",      "Résultat": metrics["Web_nettoye"],       "Attendu": ""},
        {"Étape": "Nettoyé - Liaison",  "Résultat": metrics["Liaison_nettoye"],   "Attendu": ""},
        {"Étape": "Dédoublonné - ERP",  "Résultat": metrics["ERP_dedup"],         "Attendu": "825"},
        {"Étape": "Dédoublonné - Web",  "Résultat": metrics["Web_dedup"],         "Attendu": "714"},
        {"Étape": "Dédoublonné - Liaison", "Résultat": metrics["Liaison_dedup"],  "Attendu": "825"},
        {"Étape": "Fusion finale",      "Résultat": metrics["Fusion"],            "Attendu": "714"},
        {"Étape": "Produits CA",        "Résultat": metrics["Produits_CA"],       "Attendu": "573"},
        {"Étape": "CA Total (€)",       "Résultat": round(metrics["CA_total"], 2),"Attendu": "387837.60"},
        {"Étape": "Vins Millésimés",    "Résultat": metrics["Vins_millesimes"],   "Attendu": "30"},
    ])

    OUTPUTS_PATH = Path("data/outputs")
    OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    df_report.to_csv(OUTPUTS_PATH / "rapport_final.csv", index=False)
    df_report.to_excel(OUTPUTS_PATH / "rapport_final.xlsx", index=False)
    logger.success("📄 Rapport final exporté en CSV et XLSX.")

except Exception as e:
    logger.error(f"❌ Erreur export rapport : {e}")
    exit(1)

# ----------------------------------------------------------------------
# Upload du rapport dans MinIO
# ----------------------------------------------------------------------
MINIO_ENDPOINT = "http://host.docker.internal:9000"
ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET_NAME = "bottleneck"
DESTINATION_PREFIX = "data/outputs/"

try:
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )

    for filename in ["rapport_final.csv", "rapport_final.xlsx"]:
        local_file = OUTPUTS_PATH / filename
        s3_key = f"{DESTINATION_PREFIX}{filename}"
        s3_client.upload_file(str(local_file), BUCKET_NAME, s3_key)
        logger.success(f"🚀 Upload réussi : {filename} vers {s3_key}")

except Exception as e:
    logger.error(f"❌ Erreur upload MinIO : {e}")
    exit(1)

logger.success("🎯 Rapport final complet archivé avec succès dans MinIO.")
