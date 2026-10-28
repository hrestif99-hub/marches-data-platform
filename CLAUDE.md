# Marchés Data Platform

Projet de data engineering : collecter, stocker et transformer les cours de l'**or**, du **bitcoin**, du **pétrole** et du **dollar** (EUR/USD).

## Objectif
- Historique de l'or obtenu par **scraping** (chargement initial, une seule fois).
- Données quotidiennes des autres actifs via des **APIs**.
- Entrepôt dans **Snowflake**, transformations avec **dbt**, orchestration avec **Airflow**.

## Architecture
```
Sources (scraping or + APIs)
   → Cloud Functions (GCP) déclenchées par Airflow (appel HTTP authentifié)
   → Cloud Storage (GCS) : fichiers bruts
   → Snowflake (stage externe + COPY INTO)
       RAW      : données brutes, telles que reçues
       STAGING  : nettoyées, typées, dédoublonnées (dbt)
       MARTS    : tables prêtes pour l'analyse (dbt)
   → Airflow orchestre tout : Cloud Function → Snowflake → dbt → tests
     (pas de Cloud Scheduler : un seul orchestrateur)
CI/CD : GitHub Actions
```

## Structure du repo
- `ingestion/` : scripts Python (scraper or, clients API), un sous-dossier par source
- `dbt/` : projet dbt (modèles staging et marts, tests)
- `airflow/dags/` : DAGs Airflow
- `.github/workflows/` : CI/CD
- `.claude/agents/` : sous-agents Claude Code

## Stack
Python 3.13, Snowflake, dbt, Airflow, GCP (GCS, Cloud Functions, IAM), GitHub Actions, Docker.

## Conventions
- Code et commentaires : noms en anglais, explications en français.
- Noms de tables et colonnes en `snake_case`.
- Chaque ligne en RAW porte : `source`, `ingested_at` (UTC).
- Dates en UTC, prix avec leur devise explicite (`price_usd`, `price_eur`).
- Fichiers bruts dans GCS : `gs://<bucket>/raw/<actif>/<source>/ingestion_date=YYYY-MM-DD/`.

## Qualité des données
- **Bitcoin en USDT, pas en USD** : la source (Binance Vision, paire BTCUSDT) cote en USDT.
  Colonnes suffixées `_usdt`, aucune conversion implicite en USD ; ne pas comparer ni
  joindre directement avec des prix `_usd` sans le signaler.
- **Journée Binance clôturée à minuit UTC** : chaque bougie journalière couvre 00:00–23:59:59 UTC,
  alors que les autres sources ont leur propre heure de référence (fixing BCE à Francfort, etc.).
- **Bitcoin cote 7j/7** : il a une ligne par jour calendaire, week-ends et jours fériés compris,
  contrairement à l'or, au pétrole et à l'EUR/USD (jours ouvrés seulement). En tenir compte
  pour les contrôles de trous de dates et pour les jointures entre actifs.

## Flux quotidien (décisions validées)
Modèle défini pour l'EUR/USD, à reprendre pour le pétrole puis le bitcoin (l'or reste en
collecte unique).
- **Orchestration** : Airflow seul (local, Docker) appelle la Cloud Function, lance le
  `COPY INTO` puis `dbt build`. Pas de Cloud Scheduler.
- **Fenêtre glissante** : chaque collecte demande les **14 derniers jours** à la source
  (pas tout l'historique, pas seulement la veille). Elle rattrape week-ends, jours fériés,
  collectes manquées et corrections. Les jours en double entre fichiers sont dédoublonnés
  en STAGING (dernière collecte gardée).
- **Horaire** : EUR/USD collecté vers **17 h 30 heure de Paris**, jours ouvrés (la BCE
  publie vers 16 h).
- **Noms de fichiers distincts** : la collecte quotidienne écrit
  `<actif>_<source>_daily_<D>.jsonl` (ex. `eurusd_ecb_daily_2026-10-28.jsonl`), l'historique
  garde `<actif>_<source>_<D>.jsonl`. Un run quotidien ne réécrit donc jamais un fichier
  d'historique, même à la même `ingestion_date`.
- **Idempotence (relance le même jour)** : le fichier GCS quotidien du jour, au nom fixe, est
  **réécrit** ; côté Snowflake, dans une transaction : `DELETE` des lignes RAW venant de **ce
  fichier précis** (`WHERE file_name = '<chemin du fichier du jour>'`, jamais toute
  l'`ingestion_date`), puis `COPY INTO ... FILES = ('<chemin du fichier du jour>') FORCE = TRUE`.
  `FORCE` n'est permis que limité à ce seul fichier : sur tout le préfixe, il rechargerait
  tous les fichiers et dupliquerait RAW. Toujours une seule copie de chaque fichier en RAW,
  quel que soit le nombre de relances.
- **Fraîcheur, deux contrôles** : `ingested_at` (la collecte a-t-elle tourné ? détecte une
  panne du pipeline) **et** `max(quote_date)` en staging (la source publie-t-elle encore ?
  détecte une donnée périmée, que la fenêtre glissante masquerait sinon).
- **Stage** : la définition de `MARCHES.RAW.GCS_RAW_STAGE`, créé à la main, doit être
  relevée (`DESC STAGE`) et versionnée dans `snowflake/`.

## Décisions en attente
- **Bitcoin : écrire `open_time_utc` dans RAW.** Aujourd'hui le loader lit l'heure
  d'ouverture des bougies Binance (`open_time`), vérifie qu'elle tombe à minuit UTC
  (sinon `RawFormatError`), mais n'écrit que `quote_date`. `stg_bitcoin__binance_vision`
  fait donc confiance au loader et dbt ne peut pas contrôler la date. Décision : le loader
  d'historique **et** la collecte quotidienne écriront `open_time_utc` (ISO UTC) dans RAW,
  pour que dbt dérive et vérifie `quote_date` lui-même. L'historique sera rechargé dans RAW
  à ce moment-là (nouveau fichier GCS + COPY INTO, avec confirmation).
- **Bitcoin : pas de float avant RAW.** Le loader convertit aujourd'hui prix et volumes en
  `float` Python, ce qui arrondit déjà les dernières décimales (notamment `quote_volume_usdt`).
  Décision : à la même occasion, les valeurs resteront en texte (chaîne du CSV Binance) ou en
  `Decimal` jusqu'à RAW, sans passer par un float ; le typage exact se fait en dbt.

## Sécurité
- Aucun secret dans le code ni dans Git : tout passe par `.env` (local) ou Secret Manager / secrets GitHub.
- `.env.example` liste les variables nécessaires, sans valeurs.

## Scraping
- Source retenue pour l'historique de l'or : **Boursorama**, à titre pédagogique.
  Décision consciente du propriétaire du projet : les CGU de Boursorama (§2.1) interdisent
  la récupération automatique et le `robots.txt` bloque `*/cours/historique/*`.
- Garde-fous obligatoires :
  - **Collecte unique** : un seul passage pour l'historique, jamais planifié ni relancé en boucle.
  - **Pauses d'au moins 3 secondes** entre chaque requête, user-agent explicite, nombre de requêtes minimal.
  - **Aucune donnée brute commitée dans Git** : la sortie locale va dans `ingestion/gold/output/` (ignoré par `.gitignore`).
  - Données à usage privé uniquement, pas de rediffusion.
- Pour toute autre source : respecter le `robots.txt` et les CGU du site.

## Règles pour les agents
- Petites modifications, une tâche à la fois.
- Toujours expliquer ce qui a été fait et pourquoi, en quelques lignes.
- Ne jamais lancer de commande qui supprime des données ou modifie Snowflake/GCP sans confirmation.
- Ajouter des tests pour tout nouveau modèle dbt.
