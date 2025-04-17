# === Script 02 - Nettoyage enrichi sans filtre stock_status ===
# Ce script réalise une analyse initiale des CSV, applique un nettoyage métier
# avec DuckDB en excluant le filtre sur `stock_status`, journalise chaque étape
# via loguru, et génère un résumé des opérations.

import duckdb                        # DuckDB pour exécution locale de SQL
import pandas as pd                  # Pandas pour l'analyse initiale des CSV
from pathlib import Path             # Gestion portable des chemins
from loguru import logger            # Logger riche en fonctionnalités

import sys

logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")  # warnings, errors et criticals

# ------------------------------------------------------------------------------
# Configuration des logs
# - Crée le dossier "logs" si nécessaire
# - Fichier de log "nettoyage.log" avec rotation tous les 500 KB
# ------------------------------------------------------------------------------
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "nettoyage.log", level="INFO", rotation="500 KB")

# ------------------------------------------------------------------------------
# Définition des chemins d'entrée et de sortie
# - CSV_DIR : dossier source des CSV bruts
# - OUTPUT_DIR : dossier pour les exports après traitement
# ------------------------------------------------------------------------------
CSV_DIR = Path("data/raw")
OUTPUT_DIR = Path("data/outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Fichiers CSV en entrée
erp_csv     = CSV_DIR / "erp.csv"
web_csv     = CSV_DIR / "web.csv"
liaison_csv = CSV_DIR / "liaison.csv"

logger.info("📊 Lecture des fichiers CSV pour analyse initiale...")

# ------------------------------------------------------------------------------
# Analyse initiale avec pandas : comptage des lignes et détections spécifiques
# ------------------------------------------------------------------------------
try:
    # Chargement des DataFrame
    df_erp     = pd.read_csv(erp_csv)
    df_web     = pd.read_csv(web_csv)
    df_liaison = pd.read_csv(liaison_csv)

    # Log des tailles initiales
    logger.info(f"ERP    : {len(df_erp)} lignes | WEB : {len(df_web)} | LIAISON : {len(df_liaison)}")

    # Comptage des lignes totalement vides dans chaque DataFrame
    logger.info(f"ERP    - lignes vides    : {df_erp.isnull().all(axis=1).sum()}")
    logger.info(f"WEB    - lignes vides    : {df_web.isnull().all(axis=1).sum()}")
    logger.info(f"LIAISON- lignes vides    : {df_liaison.isnull().all(axis=1).sum()}")

    # Analyse métier WEB : lignes sans SKU mais autres champs renseignés
    df_web_sku_null = df_web[df_web["sku"].isnull()]
    ignored_cols = ["virtual", "downloadable", "rating_count"]
    cols_to_check = [col for col in df_web.columns if col not in ignored_cols + ["sku"]]
    df_web_partial = df_web_sku_null[df_web_sku_null[cols_to_check].notnull().any(axis=1)]

    logger.warning(f"WEB - {len(df_web_sku_null)} lignes avec sku NULL")
    logger.warning(f"WEB - {len(df_web_partial)} lignes avec sku NULL mais données partiellement présentes")

    # Exemples de lignes partielles (niveau debug)
    if len(df_web_partial) > 0:
        logger.debug("Exemple de ligne partielle :")
        logger.debug(df_web_partial[cols_to_check].head(2).to_string(index=False))

    # Analyse métier ERP : exclusion des lignes où price ≤ 0
    df_erp_exclu = df_erp[df_erp["price"] <= 0]
    exclu_price = len(df_erp_exclu)
    logger.warning(f"ERP - {exclu_price} lignes exclues car price ≤ 0")

    # Analyse métier LIAISON : exclusion des lignes id_web NULL
    df_liaison_exclu = df_liaison[df_liaison["id_web"].isnull()]
    logger.warning(f"LIAISON - {len(df_liaison_exclu)} lignes exclues car id_web NULL")

    # Export des lignes exclues pour audit
    df_erp_exclu.to_csv(OUTPUT_DIR / "erp_exclu.csv", index=False)
    df_liaison_exclu.to_csv(OUTPUT_DIR / "liaison_exclu.csv", index=False)
    logger.info("📁 Lignes exclues exportées dans data/outputs")

except Exception as e:
    # Log de l'erreur et sortie en cas de problème d'analyse
    logger.error(f"❌ Erreur pendant l'analyse initiale : {e}")
    exit(1)

# ------------------------------------------------------------------------------
# Connexion à DuckDB et nettoyage métier (sans filtre `stock_status`)
# ------------------------------------------------------------------------------
try:
    con = duckdb.connect("data.duckdb")
    logger.info("🦆 Connexion à DuckDB établie.")
except Exception as e:
    logger.error(f"❌ Connexion à DuckDB échouée : {e}")
    exit(1)

try:
    # Nettoyage ERP : filtre sur product_id, onsale_web, price > 0, stock_quantity présent
    con.execute("""
        CREATE OR REPLACE TABLE erp_clean AS
        SELECT * FROM read_csv_auto('data/raw/erp.csv')
        WHERE product_id IS NOT NULL
          AND onsale_web IS NOT NULL
          AND price IS NOT NULL AND price > 0
          AND stock_quantity IS NOT NULL
          AND stock_status IS NOT NULL
    """)
    logger.success("✅ Table erp_clean créée sans filtre stock_status.")

    # Nettoyage WEB : conserve les lignes avec SKU non nul
    con.execute("""
        CREATE OR REPLACE TABLE web_clean AS
        SELECT * FROM read_csv_auto('data/raw/web.csv')
        WHERE sku IS NOT NULL
    """)
    logger.success("✅ Table web_clean créée.")

    # Nettoyage LIAISON : conserve les associations product_id/id_web valides
    con.execute("""
        CREATE OR REPLACE TABLE liaison_clean AS
        SELECT * FROM read_csv_auto('data/raw/liaison.csv')
        WHERE product_id IS NOT NULL
          AND id_web IS NOT NULL
    """)
    logger.success("✅ Table liaison_clean créée.")

except Exception as e:
    # Erreur durant le nettoyage SQL
    logger.error(f"❌ Erreur lors du nettoyage dans DuckDB : {e}")
    exit(1)

# ------------------------------------------------------------------------------
# Validation post-nettoyage et génération d'un résumé
# ------------------------------------------------------------------------------
try:
    # Comptage des lignes dans chaque table nettoyée
    nb_erp     = con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0]
    nb_web     = con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0]
    nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]

    # Assertions pour s'assurer que les tables ne sont pas vides
    assert nb_erp     > 0, "ERP nettoyé vide"
    assert nb_web     > 0, "Web nettoyé vide"
    assert nb_liaison > 0, "Liaison nettoyé vide"

    logger.info(f"✔️  Lignes après nettoyage - ERP: {nb_erp}, Web: {nb_web}, Liaison: {nb_liaison}")

    # Construction du DataFrame de résumé
    resume_df = pd.DataFrame({
        "source": ["erp", "web", "liaison"],
        "nb_lignes_initiales": [len(df_erp), len(df_web), len(df_liaison)],
        "nb_apres_nettoyage"  : [nb_erp, nb_web, nb_liaison],
        "nb_exclues"          : [exclu_price, len(df_web[df_web["sku"].isnull()]), len(df_liaison_exclu)]
    })

    # Export du résumé sous forme de CSV
    resume_df.to_csv(OUTPUT_DIR / "resume_stats.csv", index=False)
    logger.success("📈 Fichier de résumé exporté : resume_stats.csv")
    logger.success("🎉 Nettoyage terminé avec succès.")

except Exception as e:
    # Erreur dans la validation ou l'export du résumé
    logger.error(f"❌ Erreur dans les validations post-nettoyage : {e}")
    exit(1)
