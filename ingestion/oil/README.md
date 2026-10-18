# ingestion/oil : prix spot Brent et WTI (EIA)

Historique complet des prix spot journaliers du pétrole, en **USD par baril**, via l'API v2
de l'EIA, route `https://api.eia.gov/v2/petroleum/pri/spt/data/` :
- `RBRTE` : Brent (Europe Brent Spot Price FOB) -> `product = "brent"` ;
- `RWTC` : WTI (Cushing, OK WTI Spot Price FOB) -> `product = "wti"`.

## Clé API

Clé gratuite : https://www.eia.gov/opendata/register.php. Elle se met dans `.env` à la
racine du repo (jamais commité ; voir `.env.example`) :

```
EIA_API_KEY=<votre clé>
```

Le script la charge avec `python-dotenv` (une variable d'environnement déjà définie est
prioritaire). La clé n'est jamais loggée : elle passe via `params`, l'URL loggée est
construite sans elle, et elle est masquée (`***REDACTED***`) dans les messages d'erreur.
Défense en profondeur sur les logs : le logger `urllib3` est forcé en WARNING (il logge en
DEBUG la ligne de requête avec `?api_key=...`), et un filtre de masquage est posé sur les
handlers racine, sur `urllib3` et sur `ingestion.oil`. Le masquage porte sur la clé exacte
chargée et envoyée (après suppression des espaces).
Si elle est absente, le script s'arrête **sans aucune requête réseau** (code 4).

## Usage

Depuis la racine du repo :

```bash
.venv/Scripts/python -m pip install -r requirements.txt
.venv/Scripts/python -m ingestion.oil.load_history
.venv/Scripts/python -m pytest ingestion/oil/tests
```

Paramètres envoyés : `frequency=daily`, `data[0]=value`, `facets[series][]=RBRTE`,
`facets[series][]=RWTC`, tri par `period` puis `series` (ascendant, ordre stable pour la
pagination), `offset`, `length=5000`.

## Pagination et sorties

L'API renvoie au plus 5000 lignes par requête : le script avance `offset` jusqu'à
`response.total`. Il s'arrête en erreur (format inattendu, code 3) si :
- `response.total` change entre deux pages (données mises à jour pendant le run : relancer) ;
- le nombre de lignes reçues diffère de `response.total` à la fin ;
- une page est vide avant d'atteindre le total, ou au-delà de 50 pages.

Pages brutes et idempotence :
1. les pages s'écrivent d'abord dans `output/raw_eia_spt_<ingestion_date>.tmp/`, vidé au
   début du run s'il existe (reste d'un run interrompu) ;
2. une fois toutes les pages récupérées et toutes les lignes validées, l'ancien
   `output/raw_eia_spt_<ingestion_date>/` est supprimé et le dossier `.tmp` est renommé à sa place.

Un run interrompu laisse donc intact le jeu de pages précédent (jamais de mélange), et un
run réussi ne laisse aucune page obsolète. Les suppressions sont limitées à ces deux
dossiers (nom vérifié, parent = dossier de sortie).

Sorties dans `ingestion/oil/output/` (ignoré par Git) :
- `raw_eia_spt_<ingestion_date>/page_<NNNN>_offset_<offset>.json` : une page brute par
  requête, validée comme JSON avant écriture. Seule altération : l'EIA renvoie la clé en
  écho dans `request.params.api_key`, elle y est remplacée par `***REDACTED***` ;
- `oil_eia_<ingestion_date>.jsonl` : une ligne par jour **et** par série, avec `source`,
  `series_id`, `product`, `product_name`, `series_description`, `quote_date`, `price_usd`,
  `currency` (`USD`), `unit` (`barrel`), `raw_units` (`$/BBL`), `ingested_at` (UTC).

Les lignes sans valeur sont ignorées (comptées dans les logs) ; une unité autre que `$/BBL`,
une série inattendue ou un doublon (jour, série) arrêtent le script (format inattendu).
Relancer le même jour écrase les mêmes fichiers (idempotent).

## Comportement réseau et codes de sortie

- API publique : re-téléchargement complet à chaque exécution.
- User-agent `marches-data-platform/0.1`, timeout 60 s, 3 tentatives max par page sur
  erreur réseau / 5xx / 429 (pauses 5 s puis 10 s) ; aucune relance sur les autres 4xx.
- Codes de sortie : 0 succès, 1 erreur imprévue, 2 échec HTTP/réseau, 3 format inattendu,
  4 clé absente, 5 clé refusée (HTTP 401/403).

Pas d'upload GCS dans ce script.
