# ingestion/eurusd : taux de référence EUR/USD (BCE)

Taux de référence journalier EUR/USD publié par la BCE, série `EXR.D.USD.EUR.SP00.A`, via
l'API publique du Data Portal (sans clé) :
`https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=jsondata`.

La valeur est le nombre d'**USD pour 1 EUR** (fixing de 14h15 CET).

## Usage

Depuis la racine du repo :

```bash
.venv/Scripts/python -m pip install -r requirements.txt

# Historique complet (chargement unique) : mode par défaut, comportement inchangé
.venv/Scripts/python -m ingestion.eurusd.load_history
.venv/Scripts/python -m ingestion.eurusd.load_history --mode history

# Collecte quotidienne : fenêtre glissante des 14 derniers jours
.venv/Scripts/python -m ingestion.eurusd.load_history --mode daily
.venv/Scripts/python -m ingestion.eurusd.load_history --mode daily --date 2026-10-17

.venv/Scripts/python -m pytest ingestion/eurusd/tests
```

## Deux modes

| | `history` (défaut) | `daily` |
|---|---|---|
| Requête | tout l'historique, aucun filtre | `startPeriod = date − 14 j`, `endPeriod = date` |
| Volume | ~7100 lignes, ~1 Mo | ~10 lignes, ~4 Ko |
| Usage | chargement initial, **une seule fois** | collecte quotidienne (Airflow, ~17 h 30 Paris) |
| Contrôles propres | première date = `1999-01-04`, au moins `MIN_ROWS` (7000) lignes, dernière date ≤ `MAX_STALENESS_DAYS` (10) jours de l'**heure réelle** | au moins une observation, toutes les dates dans `[date − 14 j, date]`, dernière date ≤ `MAX_STALENESS_DAYS` (10) jours de **`--date`** |
| `collection_mode` / `window_start` / `window_end` | `history` / `null` / `null` | `daily` / `date − 14 j` / `date` |

Les contrôles communs aux deux modes sont décrits plus bas (structure SDMX, dates strictes,
doublons, valeurs finies et > 0, valeurs nulles ignorées et loggées).

### Fenêtre glissante (mode `daily`)

- Durée : constante `WINDOW_DAYS = 14`, bornes **incluses** des deux côtés (vérifié sur l'API).
- Elle rattrape les week-ends, les jours fériés TARGET, les collectes manquées et les
  corrections de la BCE. Les jours présents dans plusieurs fichiers sont dédoublonnés en
  STAGING (`stg_eurusd__ecb` garde la collecte la plus récente, par `ingested_at`).
- Grâce à `endPeriod`, relancer pour une date passée redonne le même contenu : les
  rattrapages sont reproductibles. Exemple : `--date 2026-04-06` (lundi de Pâques, pas de
  fixing) réussit, la fenêtre fournit les fixings jusqu'au 2026-04-02 (Jeudi saint).
- Une fenêtre sans aucune observation donne une erreur (code 3). Attention, l'API renvoie
  alors **HTTP 200 avec un corps vide** (pas un 404) : le script le détecte explicitement.

### `--date` (date logique) et `ingested_at` (heure réelle)

- `--date` est la date **logique** de la collecte, celle qu'Airflow passera. Elle sert
  d'`ingestion_date` dans les noms de fichiers et, en `daily`, borne la fenêtre et sert de
  référence au contrôle de fraîcheur. Défaut : aujourd'hui en UTC. Format strict
  `YYYY-MM-DD` ; une date invalide ou future est rejetée (code 4) sans aucun appel réseau.
- `ingested_at` est l'heure **réelle** (UTC) de l'exécution, écrite dans chaque ligne.
- Exemple : un rattrapage `--date 2026-10-17` lancé le 2026-10-28 écrit
  `eurusd_ecb_daily_2026-10-17.jsonl` avec `ingested_at = "2026-10-28T…Z"`.
- En mode `history`, `--date` ne change que le nom des fichiers : l'historique n'a pas de
  `endPeriod` et sa fraîcheur reste jugée par rapport à l'heure réelle.

## Sorties

Dans `ingestion/eurusd/output/` (ignoré par Git). Les noms **dépendent du mode** :

| Mode | JSONL | Réponse brute |
|---|---|---|
| `history` | `eurusd_ecb_<D>.jsonl` | `ecb_EXR_D.USD.EUR.SP00.A_<D>.json` |
| `daily` | `eurusd_ecb_daily_<D>.jsonl` | `ecb_EXR_D.USD.EUR.SP00.A_daily_<D>.json` |

- La réponse brute est le SDMX-JSON tel que reçu (écrit seulement après validation
  complète, voir plus bas).
- Le JSONL contient une ligne par jour de fixing, avec `source`, `series_key`, `quote_date`,
  `rate_usd_per_eur`, `base_currency` (`EUR`), `quote_currency` (`USD`), `unit`
  (`USD_per_EUR`), `obs_status`, `ingested_at` (UTC), puis les champs de traçabilité
  `collection_mode`, `window_start`, `window_end`. Aucun champ existant n'est renommé (le
  staging dbt les lit). RAW stocke la ligne en `VARIANT`, donc les champs ajoutés ne
  cassent pas le `COPY INTO`.

Chemins GCS correspondants (`Collection.gcs_paths` / `gcs_object_paths(date, mode)`, pour
l'étape Cloud Function) : `raw/eurusd/ecb/ingestion_date=<D>/<nom du JSONL>`, et le `.json`
brut à côté. Exemple en daily :
`raw/eurusd/ecb/ingestion_date=2026-10-17/eurusd_ecb_daily_2026-10-17.jsonl`.
Le `PATTERN` du `COPY INTO` (`.*ingestion_date=\d{4}-\d{2}-\d{2}/.*[.]jsonl`) charge le JSONL
des deux modes et ignore le `.json` brut (vérifié par un test).

Les noms locaux et les chemins GCS sont tous calculés par une seule fonction,
`file_names(mode, ingestion_date)`, et ne peuvent donc pas diverger.

## Idempotence

- Relancer pour la même `--date` et le même mode **réécrit** les deux mêmes fichiers, sans
  en créer de nouveau. Seul `ingested_at` change ; le brut est identique octet pour octet si
  la BCE n'a rien corrigé.
- **Règle : les noms sont distincts selon le mode, et l'historique n'est jamais réécrit par
  un run quotidien.** Un `daily --date 2026-10-18` écrit `eurusd_ecb_daily_2026-10-18.jsonl`
  à côté de l'historique `eurusd_ecb_2026-10-18.jsonl`, sans le toucher. L'historique reste
  un chargement unique.
- Côté Snowflake, le rechargement d'un run (relance du même jour) supprimera les lignes RAW
  **de ce fichier précis** (`file_name`), et non toute l'`ingestion_date`, avant le
  `COPY INTO ... FORCE = TRUE` de ce fichier. Les lignes de l'historique, qui viennent d'un
  autre fichier, ne sont donc jamais supprimées par un rechargement quotidien.

## Organisation du code

- `collect(mode, logical_date, now)` : télécharge et valide, **sans rien écrire** ; renvoie
  un `Collection` (réponse brute en texte, enregistrements, noms de fichiers, fenêtre).
- `write_local(collection, output_dir)` : écriture locale atomique (temporaire puis remplacement).
- `run(...)` = `collect` puis `write_local` ; `main(argv)` = CLI.
- `to_jsonl_text(records)` : seul endroit où est défini le format JSONL, utilisé par
  `write_jsonl` et par la Cloud Function (mêmes octets en local et dans GCS).
- La Cloud Function (`function/`, voir son README) réutilise `collect` puis
  `Collection.gcs_paths` (noms issus de `file_names`).

## Format SDMX-JSON

`dataSets[0].series["0:0:0:0:0"].observations` est un dictionnaire `{"<i>": [valeur, statut, ...]}`
où `<i>` indexe `structure.dimensions.observation[TIME_PERIOD].values` (la date) et `statut`
indexe les valeurs de l'attribut `OBS_STATUS` (`A` normal, `H` jour férié / week-end).
Les observations à valeur nulle (jours sans fixing, statut `H`) sont ignorées et comptées
dans les logs. Aucun arrondi ni comblement de trous : c'est le rôle de STAGING (dbt).

## Validation (avant toute écriture)

Le brut et le JSONL ne sont écrits **qu'une fois toutes les validations passées**. En cas
d'échec, aucun fichier du jour n'est créé ni remplacé (les fichiers d'un run précédent
restent intacts). Contrôles communs, chacun en `RawFormatError` (code 3) :
- structure : chaque niveau attendu est un objet / une liste du bon type ;
- index d'observation canoniques (`"0"`, `"1"`… ; refus de `"-1"`, `"01"`, `"+1"`) et compris
  dans `TIME_PERIOD.values` ;
- dates au format strict `YYYY-MM-DD` et valides (`date.fromisoformat`) ; `id` absent refusé ;
- aucune `quote_date` en double ;
- valeur numérique, finie (pas de `NaN` / `Infinity`) et strictement positive.

Puis les contrôles propres au mode (voir le tableau « Deux modes »).

## Comportement réseau

- API publique : `history` re-télécharge tout (~1 Mo), `daily` ~4 Ko.
- User-agent `marches-data-platform/0.1`, timeout 60 s, 3 tentatives max sur erreur
  réseau / 5xx / 429 (pauses 5 s puis 10 s) ; aucune relance sur les autres 4xx.
- Codes de sortie : 0 succès, 1 erreur imprévue, 2 échec HTTP/réseau, 3 format inattendu
  ou contrôle échoué (JSON invalide, structure SDMX, fenêtre vide, donnée périmée),
  4 argument invalide (mode inconnu, `--date` invalide ou future).

Pas d'upload GCS dans ce script : c'est le rôle de la Cloud Function, `function/`.
