# === Script de test - Validation du calcul du chiffre d'affaires (étape 05) ===
# Ce script vérifie la cohérence des tables `ca_par_produit` et `ca_total` :
# montant total, absence de valeurs nulles ou négatives, nombre de lignes.

import duckdb
from pathlib import Path
from loguru import logger
import sys

# === Configuration du logger ===
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "test_ca.log", level="INFO", rotation="500 KB")

# === Connexion à la base DuckDB ===
try:
    con = duckdb.connect("data/bottleneck.duckdb")
    logger.info("🧪 Connexion à DuckDB réussie.")
except Exception as e:
    logger.error(f"❌ Connexion échouée : {e}")
    exit(1)

# === Vérifications de cohérence CA ===
try:
    ca_total = con.execute("SELECT ca_total FROM ca_total").fetchone()[0]
    assert round(ca_total, 2) == 387837.60, f"❌ Montant CA incorrect : {ca_total} € (attendu : 387 837.60 €)"
    logger.success(f"💰 CA total = {ca_total:,.2f} € ✅")

    # Nombre de produits comptabilisés
    nb_produits = con.execute("SELECT COUNT(*) FROM ca_par_produit").fetchone()[0]
    assert nb_produits == 573, f"❌ Nombre de lignes dans ca_par_produit incorrect : {nb_produits} (attendu : 573)"
    logger.success(f"📦 Produits comptabilisés : {nb_produits}")

    # Vérification de l'absence de valeurs nulles ou négatives
    nulls = con.execute("""
        SELECT COUNT(*) FROM ca_par_produit
        WHERE price IS NULL OR stock_quantity IS NULL OR chiffre_affaires IS NULL
    """).fetchone()[0]
    assert nulls == 0, f"❌ Valeurs nulles détectées dans ca_par_produit : {nulls}"

    neg = con.execute("""
        SELECT COUNT(*) FROM ca_par_produit WHERE chiffre_affaires < 0
    """).fetchone()[0]
    assert neg == 0, f"❌ CA négatif détecté sur {neg} ligne(s)"

    logger.success("✅ Tests de cohérence CA validés (pas de nulls ni de CA négatif)")
    logger.success("🎉 Validation du calcul du CA réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans les tests de chiffre d'affaires : {e}")
    exit(1)
