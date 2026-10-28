# ingestion/eurusd/function : Cloud Function de collecte quotidienne EUR/USD

Cloud Function HTTP 2e génération `eurusd-daily` (point d'entrée `collect_eurusd_daily`),
appelée par Airflow (cf. « Flux quotidien » du `CLAUDE.md`). Elle :

1. lit la date **logique** dans le corps `POST {"date": "YYYY-MM-DD"}` (aujourd'hui UTC si
   le corps est vide, si `date` est absente ou `null`) ;
2. appelle `collect(mode="daily")` de `ingestion/eurusd/load_history.py` (fenêtre glissante
   de 14 jours, tous les contrôles) : **rien n'est réimplémenté** ;
3. écrit dans GCS, aux chemins de `Collection.gcs_paths`, **le `.json` brut puis le `.jsonl`
   en dernier** : le JSONL est ce que charge le `COPY INTO`, il n'apparaît que si tout le reste
   a réussi ;
4. répond en JSON.

```
gs://marches-data-raw-hrestif/raw/eurusd/ecb/ingestion_date=<D>/
    ecb_EXR_D.USD.EUR.SP00.A_daily_<D>.json   (réponse BCE telle que reçue, application/json)
    eurusd_ecb_daily_<D>.jsonl                (1 ligne par fixing, application/x-ndjson)
```

Relance du même jour : les deux objets, au nom fixe, sont **écrasés** (décision validée).
Le format JSONL est celui de `load_history.to_jsonl_text`, partagé avec `write_jsonl` :
fichier local et objet GCS sont identiques octet pour octet (testé).

## Contrat HTTP

| Code | Cas | `error` |
|---|---|---|
| 200 | succès | |
| 400 | corps non JSON / pas un objet, date invalide ou future | `bad_request`, `invalid_date` |
| 405 | méthode autre que `POST` (en-tête `Allow: POST`) | `method_not_allowed` |
| 422 | format BCE inattendu ou contrôle échoué (fenêtre vide, donnée périmée…) | `invalid_data` |
| 502 | erreur réseau ou HTTP de l'API BCE (après 3 tentatives) | `upstream_error` |
| 500 | `BUCKET` manquante, panne GCS ou erreur imprévue (trace dans les logs seulement) | `config_error`, `internal_error` |

Succès :

```json
{"status": "ok", "ingestion_date": "2026-10-27", "rows": 11,
 "first_quote_date": "2026-10-13", "last_quote_date": "2026-10-27",
 "gcs_jsonl_uri": "gs://…/eurusd_ecb_daily_2026-10-27.jsonl",
 "gcs_raw_uri": "gs://…/ecb_EXR_D.USD.EUR.SP00.A_daily_2026-10-27.json"}
```

Erreur : `{"status": "error", "error": "<code>", "message": "<message clair>"}`.

Si le brut est écrit mais que l'upload du JSONL échoue (500), le brut reste seul : sans
effet sur le `COPY INTO` (son `PATTERN` ne charge que `*.jsonl`), et remplacé à la relance.

## Configuration

Variables d'environnement, aucun secret (l'API BCE n'a pas de clé) :

- `BUCKET` : bucket cible, **obligatoire** en mode GCS (vérifiée avant tout appel réseau) ;
- `LOCAL_OUTPUT_DIR` : si définie, écriture dans ce dossier avec la même arborescence que le
  bucket, **sans GCP** (`BUCKET` n'est alors pas lue). Pour les tests locaux uniquement.

Logs : JSON structuré (`severity`, `message`) sur Cloud Run (variable `K_SERVICE`), texte
lisible en local.

## Tester en local (sans GCP)

```bash
# Dépendances (functions-framework, google-cloud-storage) : dans requirements-dev.txt
.venv/Scripts/python -m pip install -r requirements-dev.txt

# Tests unitaires (sans réseau ni GCP)
.venv/Scripts/python -m pytest ingestion/eurusd/function -q

# Serveur local : PYTHONPATH = racine du repo, pour importer ingestion.eurusd
LOCAL_OUTPUT_DIR=/chemin/vers/un/dossier/temporaire PYTHONPATH="$(pwd -W)" \
  .venv/Scripts/functions-framework --target=collect_eurusd_daily \
  --source=ingestion/eurusd/function/main.py --port=8085

# Dans un autre terminal (appel réel à l'API BCE publique, écriture locale)
curl -X POST http://127.0.0.1:8085/ -H 'Content-Type: application/json' -d '{"date": "2026-10-27"}'
```

Choisir pour `LOCAL_OUTPUT_DIR` un dossier **hors du repo** (aucune donnée brute dans Git).

## Paquet de déploiement : `build.sh`

Une fonction 2e génération veut `main.py` et `requirements.txt` à la racine du dossier
source, et `main.py` importe `ingestion.eurusd.load_history`. `build.sh` assemble un dossier
propre (par défaut `ingestion/eurusd/function/build/`, ignoré par Git) :

```
main.py, requirements.txt, ingestion/__init__.py, ingestion/eurusd/__init__.py,
ingestion/eurusd/load_history.py   (+ un fichier témoin vide .eurusd-function-build)
```

Pas de `.env`, pas d'`output/`, pas de tests. Le témoin permet de vider sans risque un
build précédent ; le script refuse de vider un dossier non vide qu'il n'a pas créé.
`build.sh` ne touche pas à GCP.

## Déploiement sur GCP : ordre des scripts

**Chacun de ces trois scripts modifie GCP : ne les lancer qu'avec l'accord explicite du
propriétaire du projet.** Tous passent `--project=marches-data` (le projet par défaut de
`gcloud` sur ce poste est un autre projet). Région `europe-west1`.

1. `bash ingestion/eurusd/function/setup_gcp.sh` : **une seule fois**. Active les API
   (`cloudfunctions`, `run`, `cloudbuild`, `artifactregistry`, `logging`), crée le compte de
   service `cf-eurusd-ingest`, lui donne `roles/storage.objectUser` sur le bucket avec une
   condition IAM limitée au préfixe `raw/eurusd/`, et donne `roles/cloudbuild.builds.builder`
   au compte Compute par défaut (droits de build). Relançable sans effet de bord.
2. `bash ingestion/eurusd/function/deploy.sh` : `build.sh` puis
   `gcloud functions deploy eurusd-daily --gen2 --runtime=python313 …` (HTTP, accès
   authentifié seulement, timeout 300 s, 256 Mi, 1 instance max, `BUCKET` en variable).
3. `bash ingestion/eurusd/function/invoke.sh 2026-10-27` : appel authentifié (jeton
   d'identité `gcloud`), affiche la réponse ; **écrit** les deux objets du jour dans GCS.

Timeout de 300 s : une collecte normale prend quelques secondes. Au pire, `fetch_raw` peut
dépasser 300 s (le timeout de 60 s de `requests` s'applique à la connexion puis entre deux
paquets, sur 3 tentatives avec pauses) : la fonction est alors coupée (504), rien n'est écrit
ou seulement le brut, le JSONL étant écrit en dernier, et Airflow relance. Le timeout HTTP
côté Airflow devra être supérieur à 300 s.
