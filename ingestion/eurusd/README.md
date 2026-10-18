# ingestion/eurusd : taux de référence EUR/USD (BCE)

Historique complet (depuis le 1999-01-04) du taux de référence journalier EUR/USD publié
par la BCE, série `EXR.D.USD.EUR.SP00.A`, via l'API publique du Data Portal (sans clé) :
`https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=jsondata`.

La valeur est le nombre d'**USD pour 1 EUR** (fixing de 14h15 CET).

## Usage

Depuis la racine du repo :

```bash
.venv/Scripts/python -m pip install -r requirements.txt
.venv/Scripts/python -m ingestion.eurusd.load_history
.venv/Scripts/python -m pytest ingestion/eurusd/tests
```

Sorties dans `ingestion/eurusd/output/` (ignoré par Git) :
- `ecb_EXR_D.USD.EUR.SP00.A_<ingestion_date>.json` : réponse SDMX-JSON brute, telle que reçue
  (écrite seulement après validation complète, voir plus bas) ;
- `eurusd_ecb_<ingestion_date>.jsonl` : une ligne par jour de fixing, avec `source`,
  `series_key`, `quote_date`, `rate_usd_per_eur`, `base_currency` (`EUR`),
  `quote_currency` (`USD`), `unit` (`USD_per_EUR`), `obs_status`, `ingested_at` (UTC).

Relancer le même jour écrase les mêmes fichiers (idempotent).

## Format SDMX-JSON

`dataSets[0].series["0:0:0:0:0"].observations` est un dictionnaire `{"<i>": [valeur, statut, ...]}`
où `<i>` indexe `structure.dimensions.observation[TIME_PERIOD].values` (la date) et `statut`
indexe les valeurs de l'attribut `OBS_STATUS` (`A` normal, `H` jour férié / week-end).
Les observations à valeur nulle (jours sans fixing, statut `H`) sont ignorées et comptées
dans les logs. Aucun arrondi ni comblement de trous : c'est le rôle de STAGING (dbt).

## Validation (avant toute écriture)

Le brut et le JSONL ne sont écrits **qu'une fois toutes les validations passées**. En cas
d'échec, aucun fichier du jour n'est créé ni remplacé (les fichiers d'un run précédent
restent intacts). Contrôles, chacun en `RawFormatError` (code 3) :
- structure : chaque niveau attendu est un objet / une liste du bon type ;
- index d'observation canoniques (`"0"`, `"1"`… ; refus de `"-1"`, `"01"`, `"+1"`) et compris
  dans `TIME_PERIOD.values` ;
- dates au format strict `YYYY-MM-DD` et valides (`date.fromisoformat`) ; `id` absent refusé ;
- aucune `quote_date` en double ;
- valeur numérique, finie (pas de `NaN` / `Infinity`) et strictement positive ;
- complétude :
  - première date = `EXPECTED_FIRST_DATE` (`1999-01-04`) ;
  - au moins `MIN_ROWS` (7000) lignes ;
  - dernière date vieille d'au plus `MAX_STALENESS_DAYS` (10) jours calendaires par rapport
    à la date d'exécution, pour tolérer les fêtes de fin d'année.

## Comportement réseau

- API publique : re-téléchargement complet à chaque exécution (~1 Mo).
- User-agent `marches-data-platform/0.1`, timeout 60 s, 3 tentatives max sur erreur
  réseau / 5xx / 429 (pauses 5 s puis 10 s) ; aucune relance sur les autres 4xx.
- Codes de sortie : 0 succès, 1 erreur imprévue, 2 échec HTTP/réseau, 3 format inattendu
  (JSON invalide, structure SDMX inattendue).

Pas d'upload GCS dans ce script.
