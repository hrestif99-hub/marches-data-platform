# ingestion/bitcoin : bougies journalières BTCUSDT (Binance Vision)

Historique complet (depuis le 2017-08-17) des bougies 1d de la paire spot **BTCUSDT**,
téléchargé **uniquement** depuis les fichiers publics `https://data.binance.vision/data/spot/` :
- mois complets : `monthly/klines/BTCUSDT/1d/BTCUSDT-1d-YYYY-MM.zip` (+ `.CHECKSUM`),
  de 2017-08 jusqu'au mois précédent ;
- mois en cours : `daily/klines/BTCUSDT/1d/BTCUSDT-1d-YYYY-MM-DD.zip` (+ `.CHECKSUM`), du 1er
  jusqu'à la veille (la bougie du jour n'est pas close) ;
- si le zip mensuel du mois précédent n'est pas encore publié (début de mois), ses jours sont
  pris dans les fichiers quotidiens.

Ni `api.binance.com` (son `robots.txt` contient `Disallow: /`) ni `data-api.binance.vision`.

## Licence et conditions

Les jeux de données Binance Vision sont sous licence **CC BY-NC-SA 4.0**
([TERMS_AND_CONDITIONS](https://github.com/binance/binance-public-data/blob/master/TERMS_AND_CONDITIONS.md),
v1.0) :
- §4.1 : usages non commerciaux autorisés, dont les « non-monetized open educational
  projects » et la « purely personal non-production research ». C'est le cadre de ce
  projet pédagogique personnel : stockage privé, sans usage commercial ;
- §4.3 : pas de revente, de sous-licence ni d'hébergement public des données ;
- §4.5 : en cas de publication d'un travail dérivé (graphique, article, dépôt public),
  **créditer « Binance Vision »** et garder la même licence ;
- §7 : respecter les limites de débit et ne pas contourner les restrictions. Ici, on fait
  une requête à la fois, avec au moins 1 s de pause entre deux requêtes.

## USDT n'est pas USD

Les prix sont en **USDT** (stablecoin Tether), pas en dollars. L'écart est en général
inférieur à 0,1 %, mais il a dépassé plusieurs pour cent lors de décrochages (mai 2022 par
exemple). Les champs sont donc suffixés `_usdt`, et `quote_currency = "USDT"`. Aucune
conversion n'est faite ici : si elle devient nécessaire, elle se fera en aval (dbt).

## Usage

Depuis la racine du repo :

```bash
.venv/Scripts/python -m ingestion.bitcoin.load_history          # réutilise les mois déjà téléchargés
.venv/Scripts/python -m ingestion.bitcoin.load_history --full   # re-télécharge tout
.venv/Scripts/python -m pytest ingestion/bitcoin/tests -p no:cacheprovider
```

## Sorties (`ingestion/bitcoin/output/`, ignoré par Git)

- `raw_binance_vision_<ingestion_date>/` : les zip et `.CHECKSUM` **tels que reçus**, avec
  leurs noms d'origine ;
- `bitcoin_binance_vision_<ingestion_date>.jsonl` : une ligne par jour, avec les champs
  `source` (`binance_vision`), `symbol` (`BTCUSDT`), `quote_date`, `open_usdt`, `high_usdt`,
  `low_usdt`, `close_usdt`, `base_currency` (`BTC`), `quote_currency` (`USDT`), `volume_btc`,
  `quote_volume_usdt`, `number_of_trades`, `raw_file` (chemin du zip relatif à `output/`) et
  `ingested_at` (UTC).

Mécanisme (identique au pétrole) :
1. Tout s'écrit d'abord dans `raw_binance_vision_<date>.tmp/`. Ce dossier est vidé au début
   du run s'il existe.
2. Une fois tous les fichiers vérifiés et toutes les lignes validées, l'ancien dossier du jour
   est supprimé et le dossier `.tmp` est renommé à sa place.
3. Le JSONL est ensuite écrit via un fichier temporaire.

En cas d'échec, aucun fichier publié n'est remplacé, et un run réussi ne laisse aucun
fichier obsolète. `_safe_rmtree` ne supprime que ces dossiers : le nom est vérifié et le
parent doit être le dossier de sortie. Relancer le même jour remplace les mêmes fichiers
(idempotent).

**Réutilisation locale** : un mois complet ne change plus. Les zip mensuels du dernier
dossier brut publié sont donc copiés au lieu d'être re-téléchargés, à condition que leur
sha256 corresponde toujours à leur `.CHECKSUM` local. Sinon, ils sont re-téléchargés. Les
fichiers quotidiens sont toujours re-téléchargés. Avec `--full`, on ignore la copie locale.

**Limite de la réutilisation** : les mois réutilisés ne sont vérifiés que contre leur
`.CHECKSUM` **local**, sans aucune requête vers Binance. Si Binance republiait un mois
corrigé, un run normal ne le verrait pas. Lancer `--full` de temps en temps resynchronise
tout l'historique avec Binance.

**Précision** : le JSONL contient des `float` (double IEEE 754). La valeur décimale exacte
n'y est pas garantie au-delà d'environ 15 chiffres significatifs. Le brut (zip) garde la
précision exacte publiée par Binance.

## Validation (avant toute publication)

Toute violation lève `RawFormatError` (code 3), sauf mention contraire :
- le sha256 de chaque zip doit égaler son `.CHECKSUM` (format `<sha256>  <nom du zip>`) ;
- zip illisible (corrompu, flux deflate invalide) ou CSV non UTF-8 : erreur ;
- chaque zip contient exactement un CSV, avec ou sans ligne d'en-tête (les deux sont gérés).
  Une première ligne n'est prise pour un en-tête que si elle commence par `open_time` ; toute
  autre première ligne non numérique est une erreur. Le CSV doit avoir 12 colonnes : `open_time, open, high, low, close, volume, close_time,
  quote_asset_volume, number_of_trades, taker_buy_base_volume, taker_buy_quote_volume, ignore` ;
- **unité des timestamps** : depuis le 2025-01-01, les fichiers spot sont en microsecondes
  (millisecondes avant). L'unité est détectée par valeur : µs si `>= 1e15`, ms si `>= 1e11`,
  erreur sinon. `open_time` doit tomber à minuit UTC, et la date obtenue doit appartenir au
  mois ou au jour du fichier, ce qui attrape aussi une unité mal détectée ;
- prix finis et `> 0`, volumes finis et `>= 0`, `number_of_trades` entier ;
- **cohérence OHLC : erreur, pas warning** (`low <= min(open, close)` et
  `high >= max(open, close)`). Les bougies sont calculées par Binance à partir des trades,
  donc une incohérence signale un fichier corrompu ou un décalage de colonnes ;
- pas de `quote_date` en double ;
- complétude :
  - la première date doit être `2017-08-17` ;
  - **trous de jours** : ils sont loggés en WARNING, avec la liste des dates. On les tolère
    jusqu'à `MAX_MISSING_DAYS` (7) au total, au cas où une journée d'arrêt de Binance
    n'aurait pas de bougie. Au-delà, c'est une erreur (fichier probablement tronqué).
    Aucun comblement ici ;
  - la dernière date doit dater d'au plus `MAX_STALENESS_DAYS` (3) jours.

## Comportement réseau et codes de sortie

- User-agent `marches-data-platform/0.1`, timeout de 60 s, au moins 1 s de pause entre la
  **fin** d'une requête (réponse ou erreur) et l'envoi de la suivante. 3 tentatives au
  maximum sur erreur réseau, 5xx ou 429 (pauses de 5 s puis 10 s). Aucune relance sur les
  autres 4xx.
- Redirections refusées (`allow_redirects=False`) : un 3xx lève `FetchError` (code 2), et
  toute URL hors de `https://data.binance.vision/data/spot/` est refusée avant envoi. Aucun
  appel ne peut sortir de `data.binance.vision`.
- **404** :
  - en fin de plage, cela veut dire « pas encore publié » : c'est toléré et loggé (dernier
    jour, ou dernier mois avec repli sur les fichiers quotidiens) ;
  - au milieu de la plage (un mois intermédiaire, ou un jour suivi d'un jour publié), c'est
    une erreur `FetchError` (code 2).
- Volume d'un run complet : 2 requêtes (zip et CHECKSUM) par mois depuis 2017-08 et par jour
  du mois en cours, soit environ 250 requêtes et 4 à 5 minutes. Avec la réutilisation locale,
  seuls les fichiers quotidiens, et les mois absents ou modifiés, sont téléchargés.
- Codes de sortie (mêmes conventions que l'EUR/USD) : 0 succès, 1 erreur imprévue,
  2 échec HTTP ou réseau (dont un 404 au milieu de la plage), 3 format inattendu
  (checksum, zip, CSV, valeurs, complétude).

Pas d'upload GCS dans ce script.
