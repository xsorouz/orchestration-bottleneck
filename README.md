# README - Workflow Kestra : Bottleneck Pipeline v2

## 🌟 Contexte

Ce workflow Kestra automatise l'ensemble du pipeline de traitement de données pour le projet **BottleNeck**.
Il prend en charge toutes les étapes : de l'extraction des données brutes depuis MinIO, au nettoyage, dédoublonnage, fusion, calculs analytiques (CA, vins millésimés), jusqu'à la génération d'un rapport final.

---

## ✨ Architecture Globale

- **Stockage** : Données sources et outputs dans MinIO (bucket `bottleneck`)
- **Base de données** : DuckDB locale (`data/bottleneck.duckdb`)
- **Orchestration** : Workflow Kestra avec planification mensuelle (15 à 9h)
- **Scripts Python** : Centralisés dans `src/` et `tests/`

---

## 📚 Déroulement du Workflow

1. **Vérification MinIO** : Contrôle que le bucket existe.
2. **Extraction** : Téléchargement des données brutes `erp.csv`, `web.csv`, `liaison.csv` depuis MinIO.
3. **Validation initiale** : Vérification de la présence des fichiers.
4. **Nettoyage** : Suppression lignes/colonnes vides sur les fichiers CSV.
5. **Tests nettoyage** : Validation du contenu nettoyé.
6. **Upload fichiers nettoyés** : Envoi des CSV propres vers MinIO.
7. **Download fichiers nettoyés** : Récupération pour traitement local.
8. **Dédoublonnage** : Normalisation et suppression de doublons dans DuckDB.
9. **Tests dédoublonnage** : Contrôle de l'unicité des clés primaires.
10. **Snapshot de la base** : Création d'une sauvegarde de `bottleneck.duckdb`.
11. **Calcul CA par produit** : Génération des fichiers `ca_par_produit.csv`, `ca_total.csv`, puis upload.
12. **Tests CA** : Validation du montant du CA total et cohérence des données.
13. **Détection vins millésimés** : Calcul du Z-Score sur les prix + upload.
14. **Tests Z-Score** : Validation du nombre de vins millésimés.
15. **Rapport final** : Récapitulatif loggué de toutes les étapes et KPIs.

---

## 🔄 Déclenchement

- Programmé avec un **cron** : `0 9 15 * *` (tous les 15 du mois à 9h00 heure de Paris).
- ❌ Retry automatique : 2 tentatives supplémentaires en cas d'échec de toute étape (intervalle de 2 minutes).

---

## 📊 Stack technique utilisée

| Outil | Rôle |
|:------|:-----|
| **Kestra** | Orchestration des workflows |
| **DuckDB** | Traitement analytique local |
| **MinIO** | Stockage objets (données brutes et outputs) |
| **Python 3.11+** | Langage de traitement |
| **Loguru** | Gestion avancée des logs |
| **Pandas** | Manipulation de données |
| **Boto3** | Interactions avec MinIO |

---

## ⚠️ Bonnes pratiques adoptées

- Scripts Python unitaires et spécifiques à chaque étape.
- Validation systématique par des scripts de test dédiés.
- Gestion des erreurs et des exceptions à chaque étape.
- Logs exhaustifs à chaque phase du traitement.
- Ré-uploads MinIO immédiat après traitements critiques (nettoyage, CA, vins).

---

## 🔧 Configuration Kestra

Veiller à disposer dans Kestra des plugins suivants :
- `io.kestra.plugin.scripts.python`
- `io.kestra.plugin.core.flow.WorkingDirectory`
- `io.kestra.plugin.core.trigger.Schedule`

---

## ⚡ Améliorations futures possibles

- Mise en parallèle de certaines étapes pour accélérer l'exécution.
- Ajout de contrôles plus fins sur la qualité des données.
- Gestion automatique des archives de snapshots précédents.

---

## 🚀 Déploiement et Execution

1. Importer le workflow YAML dans Kestra.
2. Définir les variables d'environnement si besoin (MinIO credentials).
3. Lancer manuellement la première exécution pour vérification.
4. Activer le déclencheur programmé.

---

**👋 Bravo, votre pipeline de traitement avancé est maintenant totalement industrialisé et automatisé !**

