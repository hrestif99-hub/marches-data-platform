# ingestion/gold : historique de l'or (Boursorama)

Chargement **unique** de l'historique quotidien de l'or (symbole `_GC`, USD par once troy)
depuis l'endpoint `GetTicksEOD` de Boursorama (`length=7300`, plafond de l'endpoint).

## Usage

Depuis la racine du repo :

```bash
.venv/Scripts/python -m pip install -r requirements.txt       # exécution
.venv/Scripts/python -m pip install -r requirements-dev.txt   # + outils de test (pytest)
.venv/Scripts/python -m ingestion.gold.load_history
.venv/Scripts/python -m pytest ingestion/gold/tests
```

Sorties dans `ingestion/gold/output/` :
- `eod_7300.json` : réponse brute, jamais modifiée une fois écrite ;
- `gold_boursorama_<ingestion_date>.jsonl` : une ligne par jour, mapping 1:1 des champs
  (`d` -> `quote_date`, `o/h/l/c` -> `open_usd/high_usd/low_usd/price_usd`, `v` -> `volume`)
  plus `source`, `symbol`, `instrument_name`, `currency`, `unit`, `ingested_at` (UTC).
  Relancer le même jour écrase le fichier (idempotent).
- `eod_7300.invalid.<timestamp>.txt` : réponse non-JSON conservée pour analyse (ex. page
  HTML anti-bot) ; jamais relue comme brut valide.

### Relance un autre jour

Relancer un autre jour produit un **nouveau** `gold_boursorama_<date>.jsonl` qui contient
**tout l'historique** (fenêtre glissante de 20 ans de l'endpoint), pas seulement les
nouveaux jours. Il ne faut donc charger en RAW **qu'un seul** de ces fichiers ; en cas de
chargement multiple, le dédoublonnage se fait en STAGING (dbt) sur
`(source, symbol, quote_date)`.

Aucune correction OHLC ni comblement de trous ici : c'est le rôle de STAGING (dbt).
Pas d'upload GCS dans ce script.

## Qualité des données : 589 lignes OHLC incohérentes

Sur les 5110 lignes collectées le 2026-10-18, **589 lignes** ont au moins une incohérence OHLC.
Règle de calcul (définie dans la description du modèle dbt) : une ligne avec un OHLC complet
est incohérente si au moins une de ces conditions est vraie (une même ligne peut en cumuler
plusieurs : les 589 sont des lignes distinctes, d'où 1 + 63 + 529 = 593 ≠ 589) :

| Condition | Lignes |
|---|---|
| `high_usd < low_usd` (la plus grave) | 1 |
| `open_usd` hors de `[low_usd, high_usd]` | 63 |
| `price_usd` (clôture) hors de `[low_usd, high_usd]` | 529 |
| **Au moins une des trois** | **589** |

Les 638 lignes du 2006-10-18 au 2009-04-06 n'ont que la clôture : Boursorama la recopie dans
open/high/low (barre « plate »). Elles ont `has_full_ohlc = false` et ne sont pas comptées
comme incohérentes.

Ces lignes ne sont pas corrigées. Le modèle dbt `stg_gold__boursorama` les signale avec
`is_high_below_low`, `is_open_outside_range`, `is_close_outside_range` et le drapeau global
`is_ohlc_inconsistent`. Deux tests en `warn` les remontent :
`assert_stg_gold__boursorama_high_gte_low` (1 ligne) et
`assert_stg_gold__boursorama_ohlc_consistent` (589 lignes). Mettre ces chiffres à jour
après chaque rechargement de l'historique.

## Garde-fous

- Si `output/eod_7300.json` existe, **aucune requête réseau** n'est faite (visible dans les logs).
- Sinon : une seule requête, user-agent explicite
  `marches-data-platform/0.1 (+https://github.com/hrestif99-hub/marches-data-platform)`,
  timeout 30 s, 3 tentatives max sur erreur réseau/5xx avec pauses >= 5 s.
- En-tête `X-Requested-With: XMLHttpRequest` : identique à l'appel XHR du navigateur
  (cohérence), ce n'est pas un contournement d'anti-bot.
- Réponse non-JSON : extrait loggué (300 caractères max), réponse sauvegardée dans
  `output/eod_7300.invalid.<timestamp>.txt`, sortie en code 3.
- Écritures atomiques (`.tmp` puis remplacement, `.tmp` supprimé en cas d'échec).
- Arrêt immédiat, sans contournement, sur HTTP 403/429 ou captcha.
- Codes de sortie : 0 succès, 1 erreur, 2 accès bloqué, 3 format inattendu.

## CGU et robots.txt

Les CGU de Boursorama (§2.1) interdisent la récupération automatique et le `robots.txt`
bloque `*/cours/historique/*`. Cette collecte est une décision pédagogique consciente du
propriétaire du projet (voir `CLAUDE.md`) : collecte unique, jamais planifiée,
**aucune donnée brute dans Git** (`ingestion/gold/output/` est ignoré), usage privé, pas de rediffusion.
