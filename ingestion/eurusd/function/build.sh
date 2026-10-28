#!/usr/bin/env bash
# Assemble le paquet source de la Cloud Function eurusd-daily (aucun appel à GCP).
#
# Une Cloud Function 2e génération attend, à la RACINE du dossier source :
#   main.py           (le point d'entrée collect_eurusd_daily)
#   requirements.txt  (dépendances installées par Cloud Build)
# et main.py importe `ingestion.eurusd.load_history` : on copie donc ce module à côté,
# avec ses __init__.py. Résultat :
#
#   <build>/
#     main.py
#     requirements.txt
#     ingestion/__init__.py
#     ingestion/eurusd/__init__.py
#     ingestion/eurusd/load_history.py
#
# Rien d'autre : pas de .env, pas d'output/, pas de tests, pas de __pycache__.
#
# Usage : bash ingestion/eurusd/function/build.sh [dossier_de_build]
#   Défaut : ingestion/eurusd/function/build/ (ignoré par .gitignore).
#   Le dossier est vidé puis recréé à chaque appel (build propre).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BUILD_DIR="${1:-$SCRIPT_DIR/build}"

# Fichier témoin : prouve qu'un dossier existant a été créé par ce script. Sans lui, on
# refuse de vider un dossier non vide (protection contre un mauvais argument).
MARKER=".eurusd-function-build"

if [[ -d "$BUILD_DIR" ]]; then
    if [[ -n "$(ls -A "$BUILD_DIR")" && ! -f "$BUILD_DIR/$MARKER" ]]; then
        echo "ERREUR : $BUILD_DIR existe, n'est pas vide et n'a pas été créé par build.sh." >&2
        echo "         Choisir un autre dossier ou le vider à la main." >&2
        exit 1
    fi
    rm -rf "$BUILD_DIR"
fi

mkdir -p "$BUILD_DIR/ingestion/eurusd"
touch "$BUILD_DIR/$MARKER"

cp "$SCRIPT_DIR/main.py"                          "$BUILD_DIR/main.py"
cp "$SCRIPT_DIR/requirements.txt"                 "$BUILD_DIR/requirements.txt"
cp "$REPO_ROOT/ingestion/__init__.py"             "$BUILD_DIR/ingestion/__init__.py"
cp "$REPO_ROOT/ingestion/eurusd/__init__.py"      "$BUILD_DIR/ingestion/eurusd/__init__.py"
cp "$REPO_ROOT/ingestion/eurusd/load_history.py"  "$BUILD_DIR/ingestion/eurusd/load_history.py"

echo "Paquet de la fonction assemblé dans : $BUILD_DIR"
(cd "$BUILD_DIR" && find . -type f ! -name "$MARKER" | sort)
