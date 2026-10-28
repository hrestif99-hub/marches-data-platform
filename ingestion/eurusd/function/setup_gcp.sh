#!/usr/bin/env bash
# =====================================================================================
# Préparation GCP de la Cloud Function eurusd-daily. À LANCER UNE SEULE FOIS.
#
# ATTENTION : ce script MODIFIE le projet GCP marches-data (API, compte de service, IAM).
# Ne le lancer qu'avec l'accord explicite du propriétaire du projet.
#
# Chaque commande passe --project=marches-data : le projet par défaut de gcloud sur ce
# poste est un AUTRE projet, on ne s'appuie donc jamais sur `gcloud config`.
#
# Idempotence : chaque étape peut être relancée sans effet de bord (voir les commentaires).
# Usage : bash ingestion/eurusd/function/setup_gcp.sh
# =====================================================================================

set -euo pipefail

PROJECT_ID="marches-data"
BUCKET="marches-data-raw-hrestif"
SA_NAME="cf-eurusd-ingest"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
# Préfixe autorisé : la fonction n'écrit que sous raw/eurusd/ (cf. Collection.gcs_paths).
OBJECT_PREFIX="raw/eurusd/"

echo "== Projet : ${PROJECT_ID} (passé explicitement à chaque commande)"

# -------------------------------------------------------------------------------------
# 1. API nécessaires à une Cloud Function 2e génération.
#    - cloudfunctions : l'API de déploiement (gcloud functions deploy) ;
#    - run            : une fonction 2e gén. EST un service Cloud Run ;
#    - cloudbuild     : construit l'image à partir du code source ;
#    - artifactregistry : stocke l'image construite (dépôt gcf-artifacts) ;
#    - logging        : logs de build et d'exécution.
#    Idempotent : activer une API déjà active ne fait rien.
# -------------------------------------------------------------------------------------
echo "== 1. Activation des API"
gcloud services enable \
    cloudfunctions.googleapis.com \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    logging.googleapis.com \
    iam.googleapis.com \
    --project="${PROJECT_ID}"

# -------------------------------------------------------------------------------------
# 2. Compte de service d'EXÉCUTION de la fonction (identité utilisée pour écrire dans GCS).
#    Un compte dédié plutôt que le compte Compute par défaut : il n'a QUE le droit
#    d'écrire sous raw/eurusd/ (étape 3), rien d'autre.
#    `gcloud iam service-accounts create` N'EST PAS idempotent (erreur si le compte
#    existe déjà) : on teste d'abord son existence avec `describe`.
# -------------------------------------------------------------------------------------
echo "== 2. Compte de service ${SA_EMAIL}"
if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "   Déjà présent, rien à faire."
else
    gcloud iam service-accounts create "${SA_NAME}" \
        --project="${PROJECT_ID}" \
        --display-name="Cloud Function eurusd-daily (ingestion EUR/USD)" \
        --description="Ecrit les fichiers bruts EUR/USD sous gs://${BUCKET}/${OBJECT_PREFIX}"
    # Note : la propagation IAM d'un compte tout juste créé peut prendre quelques
    # secondes ; si l'étape 3 échoue avec « does not exist », relancer le script.
fi

# -------------------------------------------------------------------------------------
# 3. Droit d'écriture LIMITÉ au préfixe raw/eurusd/ du bucket.
#
#    Rôle roles/storage.objectUser : créer, lire, remplacer et supprimer des objets.
#    La fonction a besoin de create ET delete : réécrire un objet existant (relance du
#    même jour, décision « écraser ») exige storage.objects.delete en plus de create.
#    (roles/storage.objectCreator ne suffirait donc pas.)
#
#    Condition IAM (CEL) sur le nom de la ressource. Format documenté pour un objet :
#        projects/_/buckets/<BUCKET>/objects/<NOM_DE_L_OBJET>
#    (le « _ » remplace l'identifiant du projet, c'est voulu). D'où :
#        resource.name.startsWith("projects/_/buckets/marches-data-raw-hrestif/objects/raw/eurusd/")
#
#    Prérequis : l'accès uniforme au niveau du bucket (uniform bucket-level access) doit
#    être activé, sinon GCS refuse les conditions IAM sur le bucket. Vérifié juste avant.
#
#    Limite connue : storage.objects.list est évalué au niveau du BUCKET, pas de l'objet ;
#    la condition sur le préfixe d'objet ne l'accorde donc pas. Sans importance ici : la
#    fonction ne liste ni ne lit rien, elle ne fait que des uploads.
#
#    Syntaxe gcloud : --condition=expression=...,title=...,description=...
#    (paires KEY=VALUE séparées par des virgules : ni l'expression ni la description ne
#    doivent contenir de virgule). Les guillemets doubles de l'expression CEL sont échappés
#    pour bash (\"). Une condition interdit les rôles de base (owner/editor/viewer) : ok.
#
#    Idempotent : ajouter une liaison (rôle + membre + condition) identique à une liaison
#    existante ne crée pas de doublon (gcloud fusionne dans la liaison existante).
# -------------------------------------------------------------------------------------
echo "== 3. Rôle conditionnel sur gs://${BUCKET}/${OBJECT_PREFIX}"

# Garde-fou : les noms de bucket sont globaux, --project ne protège pas d'une faute de
# frappe. On vérifie que le bucket appartient bien au projet marches-data.
PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")"
BUCKET_PROJECT_NUMBER="$(gcloud storage buckets describe "gs://${BUCKET}" \
    --project="${PROJECT_ID}" --format="value(project_number)")"
if [[ -n "${BUCKET_PROJECT_NUMBER}" && "${BUCKET_PROJECT_NUMBER}" != "${PROJECT_NUMBER}" ]]; then
    echo "ERREUR : gs://${BUCKET} appartient au projet ${BUCKET_PROJECT_NUMBER}, pas à ${PROJECT_ID} (${PROJECT_NUMBER})." >&2
    exit 1
fi

UBLA="$(gcloud storage buckets describe "gs://${BUCKET}" \
    --project="${PROJECT_ID}" \
    --format="value(uniform_bucket_level_access)")"
if [[ "${UBLA,,}" != "true" ]]; then
    echo "ERREUR : l'accès uniforme n'est pas activé sur gs://${BUCKET} (valeur lue : '${UBLA}')." >&2
    echo "         Les conditions IAM l'exigent. Arrêt sans rien modifier sur le bucket." >&2
    exit 1
fi

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --project="${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.objectUser" \
    --condition="expression=resource.name.startsWith(\"projects/_/buckets/${BUCKET}/objects/${OBJECT_PREFIX}\"),title=eurusd-raw-prefix-only,description=Ecriture limitee a raw/eurusd/"

# -------------------------------------------------------------------------------------
# 4. Droits de BUILD (Cloud Build) pour le déploiement d'une fonction 2e génération.
#
#    Pourquoi : `gcloud functions deploy` envoie le code source dans un bucket géré
#    (gcf-v2-sources-*), puis Cloud Build construit une image et la pousse dans Artifact
#    Registry. Ce build tourne sous un compte de service de BUILD. Depuis 2024, pour les
#    projets récents, c'est par défaut le compte Compute Engine par défaut
#    (<NUMERO_PROJET>-compute@developer.gserviceaccount.com), et la contrainte
#    d'organisation iam.automaticIamGrantsForDefaultServiceAccounts empêche de lui donner
#    automatiquement le rôle Éditeur. Résultat fréquent : le build échoue avec « missing
#    permission » (lecture du code source, écriture de l'image ou des logs).
#    Correctif documenté par Google : donner roles/cloudbuild.builds.builder à ce compte.
#
#    Ce rôle est au niveau du PROJET (pas d'un bucket) : c'est un droit large. Alternative
#    plus stricte : un compte de build dédié, passé à deploy.sh via --build-service-account.
# -------------------------------------------------------------------------------------
#    DÉCISION (relecture) : étape NON exécutée par défaut. Ce rôle au niveau du projet
#    donnerait au compte Compute par défaut, partagé, des droits larges (dont des
#    permissions storage.objects.* sur tout le projet), ce qui contournerait la
#    restriction raw/eurusd/ de l'étape 3. Dans un projet personnel sans organisation,
#    le compte Compute a en général déjà ce qu'il faut. On déploie donc d'abord SANS ce
#    rôle ; seulement si le build échoue faute de droits, on crée un compte de build
#    dédié (rôles minimaux) passé à deploy.sh avec --build-service-account.
echo "== 4. Droits de build : rien à faire (voir le commentaire ci-dessus)"

# -------------------------------------------------------------------------------------
# Non fait ici (à savoir) :
# - Le compte qui DÉPLOIE (vous) doit pouvoir « agir en tant que » cf-eurusd-ingest
#   (roles/iam.serviceAccountUser) : inclus dans roles/owner.
# - Le compte qui APPELLERA la fonction depuis Airflow aura besoin de roles/run.invoker
#   sur le service eurusd-daily : étape suivante (Airflow), pas encore créée.
# -------------------------------------------------------------------------------------
echo "== Terminé. Vérification possible :"
echo "   gcloud storage buckets get-iam-policy gs://${BUCKET} --project=${PROJECT_ID}"
