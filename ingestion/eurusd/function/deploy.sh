#!/usr/bin/env bash
# =====================================================================================
# Déploie (ou redéploie) la Cloud Function 2e génération eurusd-daily.
#
# ATTENTION : ce script MODIFIE le projet GCP marches-data (Cloud Build, Artifact Registry,
# Cloud Run). Ne le lancer qu'avec l'accord explicite du propriétaire du projet, et après
# setup_gcp.sh.
#
# --project=marches-data est passé explicitement : le projet par défaut de gcloud sur ce
# poste est un autre projet.
#
# Idempotent : redéployer avec le même code crée une nouvelle révision du même service,
# sans dupliquer la fonction.
# Usage : bash ingestion/eurusd/function/deploy.sh
# =====================================================================================

set -euo pipefail

PROJECT_ID="marches-data"
REGION="europe-west1"
FUNCTION_NAME="eurusd-daily"
SA_EMAIL="cf-eurusd-ingest@${PROJECT_ID}.iam.gserviceaccount.com"
BUCKET="marches-data-raw-hrestif"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

# 1. Paquet source propre : main.py + requirements.txt à la racine, module ingestion.eurusd.
bash "${SCRIPT_DIR}/build.sh" "${BUILD_DIR}"

# 2. Déploiement.
#    --runtime=python313 : runtime pris en charge par Cloud Run functions (fin de support
#        prévue en 2029), même version que le .venv local (Python 3.13). python314 existe
#        aussi, mais on garde la version testée en local.
#    --trigger-http --no-allow-unauthenticated : appel HTTP avec jeton d'identité
#        obligatoire (Airflow, invoke.sh) ; aucun accès public.
#    --service-account : identité d'exécution, qui n'a le droit d'écrire que sous
#        gs://marches-data-raw-hrestif/raw/eurusd/ (setup_gcp.sh).
#    --timeout=300s : cas normal en quelques secondes. Au pire, fetch_raw peut dépasser
#        300 s (timeout=60 de requests s'applique à la connexion PUIS entre deux paquets,
#        3 tentatives + pauses de 5 s et 10 s). La fonction est alors coupée (504) : rien
#        n'est écrit ou seulement le brut (le JSONL est écrit en dernier), et Airflow
#        relance. Le timeout HTTP côté Airflow devra être supérieur à 300 s.
#    --memory=256Mi : ~4 Ko de données par collecte, largement suffisant.
#    --max-instances=1 : une seule collecte à la fois (pas de concurrence d'écriture sur
#        les mêmes objets) ; un 2e appel simultané sera refusé (429), pas exécuté en double.
#    --set-env-vars=BUCKET=... : bucket cible lu par main.py (pas un secret).
#        Remplace toutes les variables d'environnement existantes de la fonction.
gcloud functions deploy "${FUNCTION_NAME}" \
    --project="${PROJECT_ID}" \
    --gen2 \
    --runtime=python313 \
    --region="${REGION}" \
    --source="${BUILD_DIR}" \
    --entry-point=collect_eurusd_daily \
    --trigger-http \
    --no-allow-unauthenticated \
    --service-account="${SA_EMAIL}" \
    --timeout=300s \
    --memory=256Mi \
    --max-instances=1 \
    --set-env-vars="BUCKET=${BUCKET}"

echo "Déployé. Appel de test : bash ${SCRIPT_DIR}/invoke.sh YYYY-MM-DD"
