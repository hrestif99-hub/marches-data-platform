# Marchés Data Platform

Projet de data engineering : collecter, stocker et transformer les cours de l'**or**, du **bitcoin**, du **pétrole** et du **dollar** (EUR/USD).

## Objectif
- Historique de l'or obtenu par **scraping** (chargement initial, une seule fois).
- Données quotidiennes des autres actifs via des **APIs**.
- Entrepôt dans **Snowflake**, transformations avec **dbt**, orchestration avec **Airflow**.

## Architecture
```
Sources (scraping or + APIs)
   → Cloud Functions (GCP) déclenchées par Cloud Scheduler
   → Cloud Storage (GCS) : fichiers bruts
   → Snowflake (stage externe + COPY INTO)
       RAW      : données brutes, telles que reçues
       STAGING  : nettoyées, typées, dédoublonnées (dbt)
       MARTS    : tables prêtes pour l'analyse (dbt)
   → Airflow orchestre GCS → Snowflake → dbt → tests
CI/CD : GitHub Actions
```

## Structure du repo
- `ingestion/` : scripts Python (scraper or, clients API), un sous-dossier par source
- `dbt/` : projet dbt (modèles staging et marts, tests)
- `airflow/dags/` : DAGs Airflow
- `.github/workflows/` : CI/CD
- `.claude/agents/` : sous-agents Claude Code

## Stack
Python 3.13, Snowflake, dbt, Airflow, GCP (GCS, Cloud Functions, Cloud Scheduler, IAM), GitHub Actions, Docker.

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
