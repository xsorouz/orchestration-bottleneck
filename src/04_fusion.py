# === Script 04 - Fusion ERP / Liaison / Web (version chiffres officiels) ===
# Ce script réalise la jointure des tables dédoublonnées (ERP, Liaison, Web)
# dans DuckDB, vérifie que le nombre de lignes corresponde aux chiffres attendus,
# et exporte le résultat pour archivage.

import duckdb                          # DuckDB pour exécuter du SQL localement
from pathlib import Path                # Gestion portable des chemins de fichiers
from loguru import logger               # Logger simple à configurer et riche en fonctionnalités
import pandas as pd                     # Pandas pour exporter les résultats en CSV

import sys

logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")  # warnings, errors et criticals

# ------------------------------------------------------------------------------
# Configuration des logs
# - Crée le répertoire "logs" s'il n'existe pas
# - Ajoute un fichier de log "fusion.log" avec rotation automatique
# ------------------------------------------------------------------------------
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "fusion.log", level="INFO", rotation="500 KB")

# ------------------------------------------------------------------------------
# Préparation du dossier de sortie
# - OUTPUT_PATH : répertoire pour les exports post-fusion
# ------------------------------------------------------------------------------
OUTPUT_PATH = Path("data/outputs")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------------------
# Connexion à DuckDB
# - Ouvre ou crée la base de données "data.duckdb"
# ------------------------------------------------------------------------------
try:
    Path("data").mkdir(exist_ok=True)  # S'assurer que le dossier "data" existe
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🦆 Connexion à DuckDB établie dans le dossier 'data'.")
except Exception as e:
    logger.error(f"❌ Connexion à DuckDB échouée : {e}")
    exit(1)

# ------------------------------------------------------------------------------
# Étape de fusion : création de la table `fusion`
# - Joint les tables erp_dedup, liaison_dedup, et web_dedup
# - Utilise l'association liaison_dedup.id_web = web_dedup.sku
# ------------------------------------------------------------------------------
try:
    con.execute("""
        CREATE OR REPLACE TABLE fusion AS
        SELECT
            e.product_id,
            e.onsale_web,
            e.price,
            e.stock_quantity,
            e.stock_status,
            w.post_title,
            w.post_excerpt,
            w.post_status,
            w.post_type,
            w.average_rating,
            w.total_sales
        FROM erp_dedup e
        JOIN liaison_dedup l ON e.product_id = l.product_id
        JOIN web_dedup w ON l.id_web = w.sku
    """)
    logger.success("✅ Table fusion créée (jointure via liaison_dedup.id_web = web_dedup.sku).")
except Exception as e:
    logger.error(f"❌ Erreur lors de la fusion : {e}")
    exit(1)

# ------------------------------------------------------------------------------
# Validation du nombre de lignes
# - Vérifie que la table `fusion` contient exactement 714 lignes attendues
# ------------------------------------------------------------------------------
try:
    nb_fusion = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]
    assert nb_fusion == 714, f"La fusion ne contient pas 714 lignes (actuel : {nb_fusion})"
    logger.info(f"✔️  Nombre de lignes fusionnées : {nb_fusion} ✅ (attendu : 714)")

    # Export de contrôle : toute la table fusion
    df_fusion = con.execute("SELECT * FROM fusion").fetchdf()
    df_fusion.to_csv(OUTPUT_PATH / "fusion.csv", index=False)
    logger.success("📁 Export fusion.csv créé dans data/outputs.")
except Exception as e:
    logger.error(f"❌ Validation échouée : {e}")
    exit(1)
