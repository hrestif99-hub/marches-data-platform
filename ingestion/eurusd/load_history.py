"""Taux de référence EUR/USD de la BCE (série EXR.D.USD.EUR.SP00.A) : historique et quotidien.

API publique du Data Portal de la BCE, sans clé. Format SDMX-JSON (`format=jsondata`) :
- la valeur est le nombre d'USD pour 1 EUR (taux de référence de 14h15 CET) ;
- les jours sans fixing (week-ends, jours fériés TARGET) peuvent apparaître avec une
  valeur nulle et le statut `H` : ils sont ignorés (et comptés dans les logs).

Deux modes :
- `history` (défaut) : tout l'historique, contrôles de complétude (premier fixing
  1999-01-04, MIN_ROWS). Chargement unique.
- `daily` : fenêtre glissante [date - WINDOW_DAYS, date] (startPeriod / endPeriod), pour
  la collecte quotidienne orchestrée par Airflow. Grâce à endPeriod, relancer pour une
  date passée redonne le même contenu (rattrapage reproductible).

Deux dates distinctes :
- `ingestion_date` = `--date`, la date LOGIQUE de la collecte (celle qu'Airflow passe) :
  elle nomme les fichiers et, en daily, borne la fenêtre et sert de référence à la
  fraîcheur ; par défaut, aujourd'hui en UTC ;
- `ingested_at` = l'heure RÉELLE (UTC) de l'exécution, écrite dans chaque ligne.
  Un rattrapage du 2026-10-17 lancé le 2026-10-28 a ingestion_date=2026-10-17 et
  ingested_at=2026-10-28T...Z.

La collecte (`collect`, sans écriture) est séparée de l'écriture locale (`write_local`)
pour que la Cloud Function réutilise la même collecte et écrive dans GCS
(`Collection.gcs_paths`). Relancer pour la même date et le même mode réécrit les mêmes
fichiers (idempotent). Les noms dépendent du mode (`file_names`) : un run daily
n'écrit jamais au chemin du fichier d'historique, même à la même ingestion_date.

Usage : python -m ingestion.eurusd.load_history [--mode history|daily] [--date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SOURCE = "ecb"
FLOW = "EXR"
SERIES_KEY = "D.USD.EUR.SP00.A"
URL = f"https://data-api.ecb.europa.eu/service/data/{FLOW}/{SERIES_KEY}"
PARAMS = {"format": "jsondata"}  # mode history : tout l'historique, aucun filtre de période
USER_AGENT = "marches-data-platform/0.1"
TIMEOUT_S = 60
MAX_ATTEMPTS = 3
BACKOFF_S = 5
RETRY_STATUSES = {429, 500, 502, 503, 504}

BASE_CURRENCY = "EUR"
QUOTE_CURRENCY = "USD"
UNIT = "USD_per_EUR"

# Contrôles de complétude (seuils de production : ne pas les baisser pour les tests).
EXPECTED_FIRST_DATE = "1999-01-04"  # premier fixing de l'euro (mode history seulement)
MIN_ROWS = 7000  # ~7100 jours de fixing fin 2026 (mode history seulement)
MAX_STALENESS_DAYS = 10  # jours calendaires : tolère les fêtes de fin d'année
ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

# Mode daily : fenêtre glissante [date - WINDOW_DAYS, date], bornes incluses (décision
# « Flux quotidien » du CLAUDE.md) : rattrape week-ends, fériés, collectes manquées.
WINDOW_DAYS = 14

MODE_HISTORY = "history"
MODE_DAILY = "daily"
MODES = (MODE_HISTORY, MODE_DAILY)

# Codes de sortie. 4 = argument invalide, distinct du 2 (réseau) qu'argparse utiliserait.
EXIT_OK = 0
EXIT_UNEXPECTED = 1
EXIT_FETCH = 2
EXIT_FORMAT = 3
EXIT_USAGE = 4

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

logger = logging.getLogger("ingestion.eurusd")


class FetchError(RuntimeError):
    """Échec HTTP ou réseau (après relances éventuelles)."""


class RawFormatError(ValueError):
    """La réponse brute n'a pas la structure SDMX-JSON attendue, ou un contrôle échoue."""


class UsageError(ValueError):
    """Argument invalide : mode inconnu, date mal formée ou dans le futur."""


def window_bounds(logical_date: date) -> tuple[date, date]:
    """Fenêtre glissante du mode daily, bornes incluses."""
    return logical_date - timedelta(days=WINDOW_DAYS), logical_date


def build_params(mode: str, logical_date: date) -> dict[str, str]:
    """Paramètres de l'API : aucun filtre en history, fenêtre [date - 14 j, date] en daily."""
    if mode == MODE_HISTORY:
        return dict(PARAMS)
    if mode == MODE_DAILY:
        start, end = window_bounds(logical_date)
        # startPeriod et endPeriod sont inclusifs (vérifié sur l'API le 2026-10-28).
        return {**PARAMS, "startPeriod": start.isoformat(), "endPeriod": end.isoformat()}
    raise UsageError(f"Mode inconnu : {mode!r} (attendu : {', '.join(MODES)}).")


def fetch_raw(params: dict[str, str] | None = None) -> str:
    """Télécharge la série (tout l'historique par défaut) ; relances limitées sur réseau / 5xx / 429."""
    import requests

    params = dict(PARAMS) if params is None else params
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    query = "&".join(f"{k}={v}" for k, v in params.items())
    last_error = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        logger.info("Requête HTTP GET %s?%s (tentative %d/%d)", URL, query, attempt, MAX_ATTEMPTS)
        try:
            resp = requests.get(URL, params=params, headers=headers, timeout=TIMEOUT_S)
        except requests.RequestException as exc:
            last_error = f"erreur réseau : {exc}"
            logger.warning("Erreur réseau : %s", exc)
        else:
            if resp.status_code == 200:
                return resp.text
            last_error = f"HTTP {resp.status_code} : {resp.text[:200]!r}"
            if resp.status_code not in RETRY_STATUSES:
                # 4xx (404 = série inconnue / aucune donnée) : relancer ne sert à rien.
                raise FetchError(last_error)
            logger.warning("Réponse HTTP %d, nouvelle tentative possible", resp.status_code)
        if attempt < MAX_ATTEMPTS:
            wait = BACKOFF_S * attempt
            logger.info("Pause de %d s avant nouvelle tentative", wait)
            time.sleep(wait)
    raise FetchError(f"Échec du téléchargement après {MAX_ATTEMPTS} tentatives ({last_error})")


def parse_json(text: str) -> dict[str, Any]:
    """Valide que le texte est un objet JSON (avant toute écriture)."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RawFormatError(f"JSON invalide : {exc}") from exc
    if not isinstance(payload, dict):
        raise RawFormatError("La racine JSON n'est pas un objet.")
    return payload


def write_raw(text: str, path: Path) -> None:
    """Écrit la réponse telle que reçue (atomique : temporaire puis remplacement)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="")
    tmp.replace(path)
    logger.info("Réponse brute écrite telle quelle : %s (%d octets)", path, len(text))


def _require_dict(value: Any, where: str) -> dict[str, Any]:
    """Refuse une structure inattendue (code 3) plutôt qu'un AttributeError (code 1)."""
    if not isinstance(value, dict):
        raise RawFormatError(f"{where} : objet JSON attendu, reçu {type(value).__name__}.")
    return value


def _require_list(value: Any, where: str) -> list[Any]:
    if not isinstance(value, list):
        raise RawFormatError(f"{where} : liste JSON attendue, reçu {type(value).__name__}.")
    return value


def _parse_date(value: Any, where: str) -> str:
    """Date stricte YYYY-MM-DD (date.fromisoformat seul accepterait aussi 20261016)."""
    if not isinstance(value, str) or not ISO_DATE_RE.fullmatch(value):
        raise RawFormatError(f"{where} : date invalide {value!r} (format YYYY-MM-DD attendu).")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise RawFormatError(f"{where} : date invalide {value!r} ({exc}).") from exc
    return value


def _status_codes(structure: dict[str, Any]) -> list[str | None]:
    """Liste des codes OBS_STATUS (l'index 1 des observations y fait référence)."""
    attributes = structure.get("attributes", {})
    attrs = attributes.get("observation", []) if isinstance(attributes, dict) else []
    for attr in attrs if isinstance(attrs, list) else []:
        if isinstance(attr, dict) and attr.get("id") == "OBS_STATUS":
            values = attr.get("values", [])
            if isinstance(values, list):
                return [v.get("id") if isinstance(v, dict) else None for v in values]
    return []


def extract_observations(payload: Any) -> tuple[list[dict[str, Any]], list[str]]:
    """Renvoie ([{quote_date, value, obs_status}], dates ignorées car valeur nulle).

    Structure SDMX-JSON : dataSets[0].series["0:0:0:0:0"].observations = {"<i>": [valeur, ...]}
    où <i> indexe structure.dimensions.observation[TIME_PERIOD].values[i].id (la date).
    """
    payload = _require_dict(payload, "racine")
    datasets = _require_list(payload.get("dataSets"), "dataSets")
    structure = _require_dict(payload.get("structure"), "structure")
    dimensions = _require_dict(structure.get("dimensions"), "structure.dimensions")
    time_dims = _require_list(dimensions.get("observation"), "structure.dimensions.observation")
    if len(datasets) != 1:
        raise RawFormatError("dataSets doit contenir exactement un élément.")
    dataset = _require_dict(datasets[0], "dataSets[0]")
    series = _require_dict(dataset.get("series"), "dataSets[0].series")
    if len(series) != 1:
        raise RawFormatError("dataSets[0].series doit contenir exactement une série.")
    the_series = _require_dict(next(iter(series.values())), "série")
    observations = _require_dict(the_series.get("observations"), "observations")
    if not observations:
        raise RawFormatError("Aucune observation dans la série.")

    time_dim = next(
        (d for d in time_dims if isinstance(d, dict) and d.get("id") == "TIME_PERIOD"), None
    )
    if time_dim is None:
        raise RawFormatError("Dimension TIME_PERIOD absente.")
    time_values = _require_list(time_dim.get("values"), "TIME_PERIOD.values")
    dates = [
        _parse_date(
            _require_dict(v, f"TIME_PERIOD.values[{i}]").get("id"), f"TIME_PERIOD.values[{i}].id"
        )
        for i, v in enumerate(time_values)
    ]
    statuses = _status_codes(structure)

    rows: list[dict[str, Any]] = []
    skipped: list[str] = []
    seen: set[str] = set()
    for key, obs in observations.items():
        # Index canonique uniquement : "-1", "01", " 1", "+1" sont refusés.
        if not key.isascii() or not key.isdigit() or str(int(key)) != key:
            raise RawFormatError(f"Index d'observation non canonique : {key!r}")
        index = int(key)
        if index >= len(dates):
            raise RawFormatError(
                f"Index d'observation {index} hors de TIME_PERIOD.values ({len(dates)} dates)."
            )
        quote_date = dates[index]
        if quote_date in seen:
            raise RawFormatError(f"quote_date en double : {quote_date}")
        seen.add(quote_date)
        if not isinstance(obs, list) or not obs:
            raise RawFormatError(f"Observation {quote_date} : format inattendu {obs!r}")
        value = obs[0]
        status_idx = obs[1] if len(obs) > 1 else None
        status = (
            statuses[status_idx]
            if isinstance(status_idx, int) and 0 <= status_idx < len(statuses)
            else None
        )
        if value is None:
            skipped.append(quote_date)
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RawFormatError(f"Observation {quote_date} : valeur non numérique {value!r}")
        if not math.isfinite(value) or value <= 0:
            raise RawFormatError(f"Observation {quote_date} : taux non fini ou <= 0 ({value!r})")
        rows.append({"quote_date": quote_date, "value": value, "obs_status": status})
    rows.sort(key=lambda r: r["quote_date"])
    return rows, sorted(skipped)


def check_completeness(
    rows: list[dict[str, Any]],
    now: datetime,
    first_date: str = EXPECTED_FIRST_DATE,
    min_rows: int = MIN_ROWS,
    max_staleness_days: int = MAX_STALENESS_DAYS,
) -> None:
    """Mode history : vérifie que l'historique est complet ; seuils par défaut = production."""
    if not rows:
        raise RawFormatError("Aucune observation non nulle.")
    if rows[0]["quote_date"] != first_date:
        raise RawFormatError(
            f"Première date {rows[0]['quote_date']} au lieu de {first_date} : historique tronqué ?"
        )
    if len(rows) < min_rows:
        raise RawFormatError(
            f"{len(rows)} lignes seulement (minimum attendu {min_rows}) : historique incomplet ?"
        )
    last = date.fromisoformat(rows[-1]["quote_date"])
    age = (now.astimezone(timezone.utc).date() - last).days
    if age > max_staleness_days:
        raise RawFormatError(
            f"Dernière date {last} vieille de {age} jours (maximum {max_staleness_days}) : "
            "série non mise à jour ?"
        )


def check_window(
    rows: list[dict[str, Any]],
    skipped: list[str],
    logical_date: date,
    max_staleness_days: int = MAX_STALENESS_DAYS,
) -> None:
    """Mode daily : remplace première date et MIN_ROWS, propres à l'historique.

    - toutes les dates (nulles comprises) dans [date - WINDOW_DAYS, date] ;
    - au moins une observation non nulle dans la fenêtre ;
    - dernière quote_date à au plus max_staleness_days de la date LOGIQUE (pas de l'heure
      réelle) : un rattrapage d'une date passée est jugé par rapport à cette date.
    """
    start, end = window_bounds(logical_date)
    outside = sorted(
        d for d in [r["quote_date"] for r in rows] + skipped
        if not start <= date.fromisoformat(d) <= end
    )
    if outside:
        raise RawFormatError(
            f"{len(outside)} date(s) hors de la fenêtre [{start}, {end}], ex. : "
            f"{', '.join(outside[:5])}"
        )
    if not rows:
        raise RawFormatError(f"Aucune observation non nulle dans la fenêtre [{start}, {end}].")
    last = date.fromisoformat(rows[-1]["quote_date"])
    age = (logical_date - last).days
    if age > max_staleness_days:
        raise RawFormatError(
            f"Dernière date {last} vieille de {age} jours au {logical_date} "
            f"(maximum {max_staleness_days}) : série non mise à jour ?"
        )


def to_record(row: dict[str, Any], ingested_at: str) -> dict[str, Any]:
    """Mapping 1:1 d'une observation, sans arrondi ni comblement de trous."""
    return {
        "source": SOURCE,
        "series_key": f"{FLOW}.{SERIES_KEY}",
        "quote_date": row["quote_date"],
        "rate_usd_per_eur": row["value"],
        "base_currency": BASE_CURRENCY,
        "quote_currency": QUOTE_CURRENCY,
        "unit": UNIT,
        "obs_status": row["obs_status"],
        "ingested_at": ingested_at,
    }


def add_collection_fields(
    record: dict[str, Any], mode: str, window_start: str | None, window_end: str | None
) -> dict[str, Any]:
    """Champs de traçabilité ajoutés après les champs existants (aucun n'est renommé)."""
    return {
        **record,
        "collection_mode": mode,
        "window_start": window_start,
        "window_end": window_end,
    }


def file_names(mode: str, ingestion_date: str) -> tuple[str, str]:
    """SEUL endroit où sont construits les noms de fichiers : (jsonl, réponse brute).

    - history : eurusd_ecb_<D>.jsonl, ecb_EXR_D.USD.EUR.SP00.A_<D>.json (noms inchangés) ;
    - daily   : eurusd_ecb_daily_<D>.jsonl, ecb_EXR_D.USD.EUR.SP00.A_daily_<D>.json.
    Noms distincts : un run daily ne peut pas réécrire le fichier d'historique.
    """
    _parse_date(ingestion_date, "ingestion_date")
    if mode == MODE_HISTORY:
        tag = ""
    elif mode == MODE_DAILY:
        tag = f"{MODE_DAILY}_"
    else:
        raise UsageError(f"Mode inconnu : {mode!r} (attendu : {', '.join(MODES)}).")
    return (
        f"eurusd_{SOURCE}_{tag}{ingestion_date}.jsonl",
        f"ecb_{FLOW}_{SERIES_KEY}_{tag}{ingestion_date}.json",
    )


def gcs_object_paths(ingestion_date: str, mode: str) -> tuple[str, str]:
    """Chemins GCS relatifs au bucket (convention CLAUDE.md) : (jsonl, réponse brute).

    raw/eurusd/ecb/ingestion_date=<D>/<nom de file_names>, le .json brut à côté.
    Le PATTERN du COPY INTO (`.*ingestion_date=\\d{4}-\\d{2}-\\d{2}/.*[.]jsonl`) ne charge que
    le JSONL. `mode` est obligatoire : pas de chemin d'historique par défaut. Étape B.
    """
    jsonl, raw = file_names(mode, ingestion_date)
    prefix = f"raw/eurusd/{SOURCE}/ingestion_date={ingestion_date}"
    return f"{prefix}/{jsonl}", f"{prefix}/{raw}"


def to_jsonl_text(records: list[dict[str, Any]]) -> str:
    """SEUL endroit où est défini le format JSONL : une ligne JSON par enregistrement.

    UTF-8 non échappé (ensure_ascii=False), séparateur "\\n" (y compris après la dernière
    ligne). Partagé par write_jsonl (local) et la Cloud Function (GCS) : mêmes octets.
    """
    return "".join(json.dumps(rec, ensure_ascii=False) + "\n" for rec in records)


def write_jsonl(records: list[dict[str, Any]], path: Path) -> None:
    """Écrit via un fichier temporaire puis remplace : relance = même fichier écrasé."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".jsonl.tmp")
    # newline="\n" : aucune traduction en \r\n sous Windows, octets identiques à GCS.
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        f.write(to_jsonl_text(records))
    tmp.replace(path)


@dataclass(frozen=True)
class Collection:
    """Résultat d'une collecte entièrement validée, prêt à écrire (local ou GCS)."""

    mode: str
    ingestion_date: str  # date logique (--date) : nomme les fichiers
    ingested_at: str  # heure réelle UTC de l'exécution
    window_start: str | None  # None en mode history
    window_end: str | None
    raw_text: str  # réponse telle que reçue
    records: list[dict[str, Any]]

    # Noms locaux et chemins GCS dérivés de (mode, ingestion_date) via file_names :
    # ils ne peuvent pas diverger.
    @property
    def jsonl_filename(self) -> str:
        return file_names(self.mode, self.ingestion_date)[0]

    @property
    def raw_filename(self) -> str:
        return file_names(self.mode, self.ingestion_date)[1]

    @property
    def gcs_paths(self) -> tuple[str, str]:
        """(jsonl, réponse brute), relatifs au bucket."""
        return gcs_object_paths(self.ingestion_date, self.mode)


def parse_logical_date(value: str | None, now: datetime) -> date:
    """--date : YYYY-MM-DD strict, pas dans le futur (UTC) ; aujourd'hui UTC par défaut."""
    today = now.astimezone(timezone.utc).date()
    if value is None:
        return today
    if not isinstance(value, str) or not ISO_DATE_RE.fullmatch(value):
        raise UsageError(f"--date invalide {value!r} : format YYYY-MM-DD attendu.")
    try:
        logical = date.fromisoformat(value)
    except ValueError as exc:
        raise UsageError(f"--date invalide {value!r} ({exc}).") from exc
    if logical > today:
        raise UsageError(f"--date {logical} est dans le futur (aujourd'hui UTC : {today}).")
    return logical


def collect(
    mode: str = MODE_HISTORY,
    logical_date: str | None = None,
    now: datetime | None = None,
) -> Collection:
    """Télécharge et valide entièrement, SANS rien écrire.

    Lève UsageError (argument), FetchError (réseau) ou RawFormatError (contrôle échoué).
    """
    if mode not in MODES:
        raise UsageError(f"Mode inconnu : {mode!r} (attendu : {', '.join(MODES)}).")
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    logical = parse_logical_date(logical_date, now)
    ingestion_date = logical.isoformat()

    if mode == MODE_HISTORY:
        window_start = window_end = None
        text = fetch_raw()  # tout l'historique, comme avant
    else:
        start, end = window_bounds(logical)
        window_start, window_end = start.isoformat(), end.isoformat()
        text = fetch_raw(build_params(mode, logical))
        if not text.strip():
            # Constaté le 2026-10-28 : une fenêtre sans observation renvoie HTTP 200 et un
            # corps vide (pas un 404) ; message explicite plutôt que « JSON invalide ».
            raise RawFormatError(
                f"Réponse vide : aucune observation dans la fenêtre [{window_start}, {window_end}]."
            )

    payload = parse_json(text)
    rows, skipped = extract_observations(payload)
    if skipped:
        logger.info(
            "%d observations nulles ignorées (jours sans fixing), ex. : %s",
            len(skipped), ", ".join(skipped[:5]),
        )
    if mode == MODE_HISTORY:
        # Fraîcheur par rapport à l'heure réelle : l'historique n'a pas de endPeriod.
        check_completeness(rows, now)
    else:
        check_window(rows, skipped, logical)
    records = [
        add_collection_fields(to_record(r, ingested_at), mode, window_start, window_end)
        for r in rows
    ]
    return Collection(
        mode=mode,
        ingestion_date=ingestion_date,
        ingested_at=ingested_at,
        window_start=window_start,
        window_end=window_end,
        raw_text=text,
        records=records,
    )


def write_local(collection: Collection, output_dir: Path | None = None) -> Path:
    """Écrit le brut puis le JSONL (chacun atomique) ; mêmes noms à chaque relance."""
    output_dir = output_dir or OUTPUT_DIR
    write_raw(collection.raw_text, output_dir / collection.raw_filename)
    out = output_dir / collection.jsonl_filename
    write_jsonl(collection.records, out)
    logger.info(
        "Mode %s, ingestion_date=%s : %d lignes écrites dans %s (du %s au %s)",
        collection.mode, collection.ingestion_date, len(collection.records), out,
        collection.records[0]["quote_date"], collection.records[-1]["quote_date"],
    )
    return out


def run(
    now: datetime | None = None,
    output_dir: Path | None = None,
    mode: str = MODE_HISTORY,
    logical_date: str | None = None,
) -> Path:
    """Collecte et valide entièrement, PUIS écrit le brut et le JSONL.

    Si un contrôle échoue, aucun fichier du jour n'est créé ni remplacé.
    """
    return write_local(collect(mode=mode, logical_date=logical_date, now=now), output_dir)


class _ArgumentParser(argparse.ArgumentParser):
    """argparse sort en code 2 sur erreur, déjà pris par l'échec réseau : on lève UsageError."""

    def error(self, message: str) -> None:  # type: ignore[override]
        raise UsageError(message)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = _ArgumentParser(
        prog="python -m ingestion.eurusd.load_history",
        description="Taux de référence EUR/USD de la BCE : historique complet ou fenêtre glissante.",
    )
    parser.add_argument(
        "--mode", choices=MODES, default=MODE_HISTORY,
        help="history (défaut) : tout l'historique ; daily : fenêtre [date - 14 j, date].",
    )
    parser.add_argument(
        "--date", dest="logical_date", default=None, metavar="YYYY-MM-DD",
        help="Date logique de la collecte (ingestion_date des fichiers). Défaut : aujourd'hui UTC.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """argv None = aucun argument (mode history, date du jour) ; sys.argv n'est lu qu'en CLI."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    try:
        args = parse_args([] if argv is None else argv)
        # Seuls les arguments fournis sont transmis : sans argument, run() comme avant.
        kwargs: dict[str, Any] = {}
        if args.mode != MODE_HISTORY:
            kwargs["mode"] = args.mode
        if args.logical_date is not None:
            kwargs["logical_date"] = args.logical_date
        run(**kwargs)
    except UsageError as exc:
        logger.error("Argument invalide : %s", exc)
        return EXIT_USAGE
    except FetchError as exc:
        logger.error("Téléchargement impossible : %s", exc)
        return EXIT_FETCH
    except RawFormatError as exc:
        logger.error("Format inattendu ou contrôle échoué : %s", exc)
        return EXIT_FORMAT
    except Exception:
        logger.exception("Échec du chargement EUR/USD")
        return EXIT_UNEXPECTED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
