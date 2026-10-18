"""Historique du taux de référence EUR/USD de la BCE (série EXR.D.USD.EUR.SP00.A).

API publique du Data Portal de la BCE, sans clé. Format SDMX-JSON (`format=jsondata`) :
- la valeur est le nombre d'USD pour 1 EUR (taux de référence de 14h15 CET) ;
- les jours sans fixing (week-ends, jours fériés TARGET) peuvent apparaître avec une
  valeur nulle et le statut `H` : ils sont ignorés (et comptés dans les logs).

Contrairement à l'or, l'API est publique : on re-télécharge tout l'historique à chaque
exécution. Relancer le même jour écrase les mêmes fichiers (idempotent).

Usage : python -m ingestion.eurusd.load_history
"""

from __future__ import annotations

import json
import logging
import math
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

SOURCE = "ecb"
FLOW = "EXR"
SERIES_KEY = "D.USD.EUR.SP00.A"
URL = f"https://data-api.ecb.europa.eu/service/data/{FLOW}/{SERIES_KEY}"
PARAMS = {"format": "jsondata"}  # tout l'historique : aucun filtre de période
USER_AGENT = "marches-data-platform/0.1"
TIMEOUT_S = 60
MAX_ATTEMPTS = 3
BACKOFF_S = 5
RETRY_STATUSES = {429, 500, 502, 503, 504}

BASE_CURRENCY = "EUR"
QUOTE_CURRENCY = "USD"
UNIT = "USD_per_EUR"

# Contrôles de complétude (seuils de production : ne pas les baisser pour les tests).
EXPECTED_FIRST_DATE = "1999-01-04"  # premier fixing de l'euro
MIN_ROWS = 7000  # ~7100 jours de fixing fin 2026
MAX_STALENESS_DAYS = 10  # jours calendaires : tolère les fêtes de fin d'année
ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

logger = logging.getLogger("ingestion.eurusd")


class FetchError(RuntimeError):
    """Échec HTTP ou réseau (après relances éventuelles)."""


class RawFormatError(ValueError):
    """La réponse brute n'a pas la structure SDMX-JSON attendue."""


def fetch_raw() -> str:
    """Télécharge la série complète ; relances limitées sur réseau / 5xx / 429."""
    import requests

    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    last_error = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        logger.info(
            "Requête HTTP GET %s?format=%s (tentative %d/%d)",
            URL, PARAMS["format"], attempt, MAX_ATTEMPTS,
        )
        try:
            resp = requests.get(URL, params=PARAMS, headers=headers, timeout=TIMEOUT_S)
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
    """Vérifie que l'historique est complet ; les seuils par défaut sont ceux de production."""
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


def write_jsonl(records: list[dict[str, Any]], path: Path) -> None:
    """Écrit via un fichier temporaire puis remplace : relance = même fichier écrasé."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    tmp.replace(path)


def run(now: datetime | None = None, output_dir: Path | None = None) -> Path:
    """Télécharge, valide entièrement, PUIS écrit le brut et le JSONL.

    Si un contrôle échoue, aucun fichier du jour n'est créé ni remplacé.
    """
    output_dir = output_dir or OUTPUT_DIR
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    ingestion_date = now.strftime("%Y-%m-%d")

    text = fetch_raw()
    payload = parse_json(text)
    rows, skipped = extract_observations(payload)
    if skipped:
        logger.info(
            "%d observations nulles ignorées (jours sans fixing), ex. : %s",
            len(skipped), ", ".join(skipped[:5]),
        )
    check_completeness(rows, now)
    records = [to_record(r, ingested_at) for r in rows]

    # Toutes les validations sont passées : écriture du brut tel que reçu, puis du JSONL.
    write_raw(text, output_dir / f"ecb_{FLOW}_{SERIES_KEY}_{ingestion_date}.json")
    out = output_dir / f"eurusd_{SOURCE}_{ingestion_date}.jsonl"
    write_jsonl(records, out)
    logger.info(
        "%d lignes écrites dans %s (du %s au %s)",
        len(records), out, records[0]["quote_date"], records[-1]["quote_date"],
    )
    return out


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    try:
        run()
    except FetchError as exc:
        logger.error("Téléchargement impossible : %s", exc)
        return 2
    except RawFormatError as exc:
        logger.error("Format inattendu : %s", exc)
        return 3
    except Exception:
        logger.exception("Échec du chargement de l'historique EUR/USD")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
