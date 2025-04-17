# === Script 03 - Dédoublonnage (avec loguru et validations) ===
# Ce script supprime les doublons des tables nettoyées dans DuckDB, en agrégant
# ou en utilisant ROW_NUMBER selon le cas, et valide les résultats.

import duckdb                        # DuckDB pour exécuter des requêtes SQL locales
from pathlib import Path              # Gestion portable des chemins de fichiers
from loguru import logger             # Logger performant pour tracer les événements et erreurs

import sys

logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")  # warnings, errors et criticals

# ------------------------------------------------------------------------------
# Configuration des logs
# - Crée le dossier "logs" si nécessaire
# - Fichier de log "dedoublonnage.log" (niveau INFO, rotation tous les 500 KB)
# ------------------------------------------------------------------------------
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "dedoublonnage.log", level="INFO", rotation="500 KB")

# ------------------------------------------------------------------------------
# Connexion à DuckDB
# - Ouvre ou crée le fichier de base de données "data.duckdb"
# ------------------------------------------------------------------------------
try:
    Path("data").mkdir(exist_ok=True)  # S'assurer que le dossier "data" existe
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🦆 Connexion à DuckDB établie dans le dossier 'data'.")
except Exception as e:
    logger.error(f"❌ Connexion à DuckDB échouée : {e}")
    exit(1)

# ------------------------------------------------------------------------------
# Étape 1 : Dédoublonnage ERP
# - Agrégation sur product_id pour conserver des valeurs uniques par produit
# - Pour chaque groupe, on prend la valeur max pour chaque colonne pertinente
# ------------------------------------------------------------------------------
try:
    con.execute("""
        CREATE OR REPLACE TABLE erp_dedup AS
        SELECT 
            product_id,
            MAX(onsale_web)     AS onsale_web,
            MAX(price)          AS price,
            MAX(stock_quantity) AS stock_quantity,
            MAX(stock_status)   AS stock_status
        FROM erp_clean
        GROUP BY product_id
    """)
    logger.success("✅ Table erp_dedup créée par agrégation.")
except Exception as e:
    logger.error(f"❌ Erreur pendant le dédoublonnage ERP : {e}")
    exit(1)

# ------------------------------------------------------------------------------
# Étape 2 : Dédoublonnage LIAISON
# - Agrégation sur product_id pour conserver une seule association id_web par produit
# - On prend la valeur MIN(id_web) pour garantir consistance
# ------------------------------------------------------------------------------
try:
    con.execute("""
        CREATE OR REPLACE TABLE liaison_dedup AS
        SELECT 
            product_id,
            MIN(id_web) AS id_web
        FROM liaison_clean
        GROUP BY product_id
    """)
    logger.success("✅ Table liaison_dedup créée.")
except Exception as e:
    logger.error(f"❌ Erreur pendant le dédoublonnage Liaison : {e}")
    exit(1)

# ------------------------------------------------------------------------------
# Étape 3 : Dédoublonnage WEB
# - Filtre sur post_type = 'product' pour ne garder que les produits
# - Utilisation de ROW_NUMBER() pour sélectionner la ligne la plus récente par sku
# ------------------------------------------------------------------------------
try:
    con.execute("""
        CREATE OR REPLACE TABLE web_dedup AS
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY sku
                ORDER BY post_date DESC
            ) AS rn
            FROM web_clean
            WHERE post_type = 'product'
        )
        WHERE rn = 1
    """)
    logger.success("✅ Table web_dedup créée avec filtrage sur post_type = 'product'.")
except Exception as e:
    logger.error(f"❌ Erreur pendant le dédoublonnage Web : {e}")
    exit(1)

# ------------------------------------------------------------------------------
# Validation post-dédoublonnage
# - Compte les lignes dans chaque table dédoublonnée
# - Vérifie que chaque table contient au moins une ligne
# ------------------------------------------------------------------------------
try:
    nb_erp     = con.execute("SELECT COUNT(*) FROM erp_dedup").fetchone()[0]
    nb_web     = con.execute("SELECT COUNT(*) FROM web_dedup").fetchone()[0]
    nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_dedup").fetchone()[0]

    # Assertions pour s'assurer que les tables ne sont pas vides
    assert nb_erp > 0,      "ERP dédoublonné vide"
    assert nb_web > 0,      "Web dédoublonné vide"
    assert nb_liaison > 0,  "Liaison dédoublonné vide"

    # Log des résultats de la validation
    logger.info(
        f"✔️  Lignes dédoublonnées - ERP: {nb_erp}, Web: {nb_web}, Liaison: {nb_liaison}"
    )
    logger.success("🎉 Étape de dédoublonnage terminée avec validations.")
except Exception as e:
    logger.error(f"❌ Échec dans les tests post-dédoublonnage : {e}")
    exit(1)
