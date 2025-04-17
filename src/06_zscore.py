# === Script 06 - Détection des vins millésimés (version attendue : 30 vins) ===

# ==============================================================================
# 🔧 IMPORTS DES LIBRAIRIES
# ==============================================================================

# duckdb : moteur SQL léger pour l’analyse locale des données.
import duckdb

# pandas : pour manipuler les résultats SQL en DataFrame et effectuer des calculs.
import pandas as pd

# pathlib : permet une gestion multiplateforme des chemins de fichiers et dossiers.
from pathlib import Path

# loguru : bibliothèque de logging moderne et facile à utiliser pour tracer les étapes du script.
from loguru import logger

import sys

logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")  # warnings, errors et criticals

# ==============================================================================
# 📂 CONFIGURATION DES DOSSIERS ET DU LOGGING
# ==============================================================================

# Création du dossier des logs s’il n’existe pas
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)

# Ajout d’un fichier de log avec rotation automatique à 500 Ko
logger.add(LOGS_PATH / "zscore.log", level="INFO", rotation="500 KB")

# Création du dossier des fichiers de sortie
OUTPUT_PATH = Path("data/outputs")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


# ==============================================================================
# 🔌 CONNEXION À LA BASE DUCKDB
# ==============================================================================

try:
    con = duckdb.connect("data.duckdb")
    logger.info("🦆 Connexion à DuckDB établie.")
except Exception as e:
    logger.error(f"❌ Connexion échouée : {e}")
    exit(1)


# ==============================================================================
# 📊 CHARGEMENT DES DONNÉES & CALCUL DU Z-SCORE
# ==============================================================================

try:
    # Requête SQL pour récupérer les produits avec un prix renseigné
    df = con.execute("""
        SELECT product_id, post_title, price
        FROM fusion
        WHERE price IS NOT NULL
    """).fetchdf()

    # Calcul du Z-score sur la colonne "price"
    df["z_score"] = (df["price"] - df["price"].mean()) / df["price"].std()

    # Classification : un vin est "millésimé" si son Z-score > 2
    df["type"] = df["z_score"].apply(lambda z: "millésimé" if z > 2 else "ordinaire")

    # Comptage pour le suivi métier
    nb_millesimes = (df["type"] == "millésimé").sum()
    nb_total = len(df)

    logger.info(f"🍷 Vins millésimés détectés (z > 2) : {nb_millesimes} (attendu : 30)")
    logger.info(f"📦 Vins ordinaires : {nb_total - nb_millesimes}")

    # --- APERÇU DES RÉSULTATS ---
    # Top 5 des vins millésimés les plus atypiques
    top5 = df[df["type"] == "millésimé"].sort_values("z_score", ascending=False).head(5)
    logger.debug("🏆 Top 5 vins millésimés :")
    logger.debug(top5[["product_id", "post_title", "price", "z_score"]].to_string(index=False))

    # --- EXPORT DES DONNÉES ---
    df[df["type"] == "millésimé"].to_csv(OUTPUT_PATH / "vins_millesimes.csv", index=False)
    df[df["type"] == "ordinaire"].to_csv(OUTPUT_PATH / "vins_ordinaires.csv", index=False)
    logger.success("📁 Export : vins_millesimes.csv & vins_ordinaires.csv")

except Exception as e:
    logger.error(f"❌ Erreur durant le traitement Z-score : {e}")
    exit(1)


# ==============================================================================
# ✅ TESTS DE VALIDATION DES DONNÉES
# ==============================================================================

try:
    # Vérification du nombre attendu de vins millésimés
    assert nb_millesimes == 30, f"❌ Nombre de vins millésimés incorrect : {nb_millesimes} (attendu : 30)"

    # Vérification de l'absence de valeurs nulles
    assert df[["price", "z_score"]].isnull().sum().sum() == 0, "❌ Valeurs nulles dans price ou z_score"

    # Vérification de l'absence de valeurs infinies (cas de division par 0)
    assert df["z_score"].isin([float('inf'), float('-inf')]).sum() == 0, "❌ Z-scores infinis détectés"

    logger.success("🧪 Tests de cohérence Z-score validés ✅")

except Exception as e:
    logger.error(f"❌ Échec dans les tests de validation Z-score : {e}")
    exit(1)
