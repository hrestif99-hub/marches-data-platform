#!/usr/bin/env bash
# =====================================================================================
# Appelle la Cloud Function déployée eurusd-daily pour une date logique, et affiche la
# réponse.
#
# ATTENTION : l'appel ÉCRIT (ou réécrit) deux objets dans
# gs://marches-data-raw-hrestif/raw/eurusd/ecb/ingestion_date=<date>/ : il modifie donc
# GCP. Ne le lancer qu'avec l'accord du propriétaire du projet.
#
# Authentification : jeton d'identité de l'utilisateur gcloud connecté
# (`gcloud auth print-identity-token`) ; il doit avoir le droit d'invoquer le service
# (roles/run.invoker, inclus dans roles/owner).
#
# Usage : bash ingestion/eurusd/function/invoke.sh 2026-10-27
# Code de sortie : 0 si HTTP 200, 1 sinon (la réponse JSON est affichée dans tous les cas).
# =====================================================================================

set -euo pipefail

PROJECT_ID="marches-data"
REGION="europe-west1"
FUNCTION_NAME="eurusd-daily"

DATE="${1:-}"
if [[ ! "${DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Usage : $0 YYYY-MM-DD" >&2
    exit 2
fi

# URL HTTPS du service (champ serviceConfig.uri d'une fonction 2e génération).
URL="$(gcloud functions describe "${FUNCTION_NAME}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --gen2 \
    --format="value(serviceConfig.uri)")"
if [[ -z "${URL}" ]]; then
    echo "ERREUR : URL introuvable pour ${FUNCTION_NAME} (fonction déployée ?)." >&2
    exit 1
fi

TOKEN="$(gcloud auth print-identity-token)"

echo "POST ${URL}  {\"date\": \"${DATE}\"}"
BODY_FILE="$(mktemp)"
trap 'rm -f "${BODY_FILE}"' EXIT

# --max-time 330 : un peu plus que le timeout de la fonction (300 s).
HTTP_CODE="$(curl -sS -X POST "${URL}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"date\": \"${DATE}\"}" \
    --max-time 330 \
    -o "${BODY_FILE}" \
    -w "%{http_code}")"

echo "HTTP ${HTTP_CODE}"
cat "${BODY_FILE}"
echo

[[ "${HTTP_CODE}" == "200" ]]
