# === Script de test - Validation de la détection des vins millésimés (étape 06) ===
# Ce script vérifie le bon calcul du Z-score, le nombre de vins millésimés détectés,
# l'absence de valeurs nulles et infinies.

import pandas as pd
from pathlib import Path
from loguru import logger
import sys

# === Configuration du logger ===
logger.remove()
logger.add(sys.stdout, level="INFO", filter=lambda record: record["level"].name == "INFO")
logger.add(sys.stderr, level="WARNING")
LOGS_PATH = Path("logs")
LOGS_PATH.mkdir(parents=True, exist_ok=True)
logger.add(LOGS_PATH / "test_zscore.log", level="INFO", rotation="500 KB")

# === Chargement des résultats Z-score ===
try:
    vins_millesimes_path = Path("data/outputs/vins_millesimes.csv")
    assert vins_millesimes_path.exists(), "❌ Le fichier vins_millesimes.csv est introuvable"

    df = pd.read_csv(vins_millesimes_path)

    nb_millesimes = df.shape[0]
    assert nb_millesimes == 30, f"❌ Nombre de vins millésimés incorrect : {nb_millesimes} (attendu : 30)"
    logger.success(f"🍷 Nombre de vins millésimés détectés : {nb_millesimes} ✅")

    # Vérification des colonnes critiques
    for col in ["price", "z_score"]:
        assert df[col].isnull().sum() == 0, f"❌ Valeurs nulles dans la colonne {col}"
        assert df[col].isin([float('inf'), float('-inf')]).sum() == 0, f"❌ Valeurs infinies dans la colonne {col}"

    logger.success("✅ Aucun NaN ni Inf dans les colonnes price et z_score")
    logger.success("🎉 Validation du Z-score réussie.")

except Exception as e:
    logger.error(f"❌ Erreur dans les tests Z-score : {e}")
    exit(1)
