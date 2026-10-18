"""Historique des prix spot journaliers du pétrole (EIA API v2) : Brent (RBRTE) et WTI (RWTC).

- Route `petroleum/pri/spt/data/`, USD par baril, tout l'historique.
- L'API renvoie au plus 5000 lignes par requête : pagination par `offset` jusqu'à
  `response.total`, chaque page brute est gardée dans son propre fichier.
- La clé vient de `EIA_API_KEY` (fichier `.env` à la racine, via python-dotenv).
  Elle n'est jamais écrite en dur ni loggée : passée via `params`, URL loggée sans elle,
  masquée dans les messages d'erreur et dans les pages brutes (l'EIA la renvoie en écho
  dans `request.params`).

API publique : on re-télécharge à chaque exécution ; relancer le même jour écrase les
mêmes fichiers (idempotent).

Usage : python -m ingestion.oil.load_history
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

SOURCE = "eia"
URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
SERIES = {"RBRTE": "brent", "RWTC": "wti"}  # series_id -> product
PAGE_LENGTH = 5000  # plafond de l'API v2
MAX_PAGES = 50  # garde-fou contre une boucle infinie (~250 000 lignes)
EXPECTED_UNITS = "$/BBL"
USER_AGENT = "marches-data-platform/0.1"
TIMEOUT_S = 60
MAX_ATTEMPTS = 3
BACKOFF_S = 5
RETRY_STATUSES = {429, 500, 502, 503, 504}
API_KEY_ENV = "EIA_API_KEY"
REDACTED = "***REDACTED***"

REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = REPO_ROOT / ".env"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

logger = logging.getLogger("ingestion.oil")


class MissingApiKeyError(RuntimeError):
    """EIA_API_KEY absente : arrêt avant toute requête réseau."""


class InvalidApiKeyError(RuntimeError):
    """L'API refuse la clé (HTTP 401/403)."""


class FetchError(RuntimeError):
    """Échec HTTP ou réseau (après relances éventuelles)."""


class RawFormatError(ValueError):
    """La réponse brute n'a pas la structure attendue."""


def get_api_key(env_file: Path | None = None) -> str:
    """Charge `.env` (sans écraser l'environnement existant) puis lit EIA_API_KEY."""
    from dotenv import load_dotenv

    load_dotenv(env_file or ENV_FILE, override=False)
    key = (os.environ.get(API_KEY_ENV) or "").strip()
    if not key:
        raise MissingApiKeyError(
            f"Variable {API_KEY_ENV} absente ou vide. Ajoutez `{API_KEY_ENV}=<votre clé>` "
            f"dans {ENV_FILE} (voir .env.example ; clé gratuite sur "
            "https://www.eia.gov/opendata/register.php). Aucune requête envoyée."
        )
    # On mémorise la clé EXACTE envoyée (après strip) : c'est elle qu'il faut masquer partout.
    _LOADED_KEYS.add(key)
    return key


# Clés chargées pendant ce processus (après strip), masquées dans tous les logs.
_LOADED_KEYS: set[str] = set()


def redact(text: str, api_key: str = "") -> str:
    """Masque la clé donnée, les clés chargées et tout paramètre `api_key=` d'URL."""
    for key in {api_key, *_LOADED_KEYS}:
        if key:
            text = text.replace(key, REDACTED)
    return re.sub(r"(api_key=)[^&\s'\"]+", rf"\1{REDACTED}", text)


class RedactingFilter(logging.Filter):
    """Applique `redact` au message formaté (args compris) et à la trace d'exception."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # message mal formé : on garde le gabarit seul
            message = str(record.msg)
        record.msg = redact(message)
        record.args = None
        if record.exc_info and not record.exc_text:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = redact(record.exc_text)
        if record.stack_info:
            record.stack_info = redact(record.stack_info)
        return True


def install_log_redaction() -> None:
    """Défense en profondeur : urllib3 forcé en WARNING + filtre de masquage partout.

    urllib3 logge en DEBUG la ligne de requête complète (donc `?api_key=...`). Un filtre
    posé sur un logger ne voit que ses propres messages (pas ceux de ses enfants qui
    se propagent) : on le pose donc aussi sur les handlers du logger racine.
    """
    for name in ("urllib3", "urllib3.connectionpool"):
        lg = logging.getLogger(name)
        if name == "urllib3":
            lg.setLevel(logging.WARNING)
        if not any(isinstance(f, RedactingFilter) for f in lg.filters):
            lg.addFilter(RedactingFilter())
    for target in (logger, *logging.getLogger().handlers):
        if not any(isinstance(f, RedactingFilter) for f in target.filters):
            target.addFilter(RedactingFilter())


def build_params(offset: int) -> list[tuple[str, str]]:
    """Paramètres de requête SANS la clé (ordre stable : tri période puis série)."""
    params = [("frequency", "daily"), ("data[0]", "value")]
    params += [("facets[series][]", s) for s in SERIES]
    params += [
        ("sort[0][column]", "period"), ("sort[0][direction]", "asc"),
        ("sort[1][column]", "series"), ("sort[1][direction]", "asc"),
        ("offset", str(offset)), ("length", str(PAGE_LENGTH)),
    ]
    return params


def fetch_page(api_key: str, offset: int) -> str:
    """Télécharge une page ; relances limitées sur réseau / 5xx / 429."""
    import requests

    params = build_params(offset)
    safe_url = f"{URL}?{urlencode(params)}"  # URL loggable : jamais la clé
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    last_error = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        logger.info("Requête HTTP GET %s (tentative %d/%d)", safe_url, attempt, MAX_ATTEMPTS)
        try:
            resp = requests.get(
                URL, params=[("api_key", api_key), *params], headers=headers, timeout=TIMEOUT_S
            )
        except requests.RequestException as exc:
            # Le message d'exception de requests contient l'URL complète, donc la clé.
            last_error = redact(f"erreur réseau : {exc}", api_key)
            logger.warning("%s", last_error)
        else:
            if resp.status_code == 200:
                return resp.text
            body = redact(resp.text[:300], api_key)
            if resp.status_code in (401, 403):
                raise InvalidApiKeyError(f"HTTP {resp.status_code} : {body}")
            last_error = f"HTTP {resp.status_code} : {body}"
            if resp.status_code not in RETRY_STATUSES:
                raise FetchError(last_error)
            logger.warning("Réponse HTTP %d, nouvelle tentative possible", resp.status_code)
        if attempt < MAX_ATTEMPTS:
            wait = BACKOFF_S * attempt
            logger.info("Pause de %d s avant nouvelle tentative", wait)
            time.sleep(wait)
    raise FetchError(f"Échec après {MAX_ATTEMPTS} tentatives ({last_error})")


def parse_page(text: str) -> tuple[int, list[dict[str, Any]]]:
    """Valide une page JSON et renvoie (response.total, response.data)."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RawFormatError(f"JSON invalide : {exc}") from exc
    response = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(response, dict):
        raise RawFormatError("Clé racine 'response' absente ou invalide.")
    if "total" not in response or "data" not in response:
        raise RawFormatError("Clés attendues absentes : response.total / response.data")
    try:
        total = int(response["total"])  # l'API le renvoie parfois sous forme de chaîne
    except (TypeError, ValueError) as exc:
        raise RawFormatError(f"response.total invalide : {response['total']!r}") from exc
    data = response["data"]
    if not isinstance(data, list):
        raise RawFormatError("response.data n'est pas une liste.")
    return total, data


def write_raw(text: str, path: Path) -> None:
    """Écrit la page telle que reçue (atomique : temporaire puis remplacement)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="")
    tmp.replace(path)
    logger.info("Page brute écrite : %s (%d octets)", path, len(text))


RAW_DIR_PATTERN = re.compile(rf"raw_{SOURCE}_spt_\d{{4}}-\d{{2}}-\d{{2}}(\.tmp)?")


def _safe_rmtree(path: Path, output_dir: Path) -> None:
    """Supprime UNIQUEMENT un dossier de pages brutes `raw_eia_spt_<date>[.tmp]` de output_dir."""
    if path.parent.resolve() != output_dir.resolve() or not RAW_DIR_PATTERN.fullmatch(path.name):
        raise RuntimeError(f"Suppression refusée hors des dossiers de pages brutes : {path}")
    if path.exists():
        logger.info("Suppression de l'ancien dossier de pages : %s", path)
        shutil.rmtree(path)


def staging_dir(raw_dir: Path) -> Path:
    """Dossier temporaire où s'écrivent les pages avant publication."""
    return raw_dir.with_name(raw_dir.name + ".tmp")


def publish_raw_dir(raw_dir: Path) -> None:
    """Remplace d'un bloc raw_dir par son dossier temporaire complet et validé.

    Si le processus s'arrête entre la suppression et le renommage, le dossier .tmp
    complet reste en place et le prochain run le reconstruit : jamais de mélange.
    """
    tmp_dir = staging_dir(raw_dir)
    _safe_rmtree(raw_dir, raw_dir.parent)
    tmp_dir.rename(raw_dir)
    logger.info("Pages brutes publiées : %s", raw_dir)


def fetch_all_pages(api_key: str, raw_dir: Path) -> list[dict[str, Any]]:
    """Pagine jusqu'à response.total ; écrit chaque page dans <raw_dir>.tmp/ ; renvoie les lignes.

    Le dossier temporaire est vidé au départ ; raw_dir n'est pas touché ici (voir
    publish_raw_dir, appelé une fois toutes les pages validées).
    """
    tmp_dir = staging_dir(raw_dir)
    _safe_rmtree(tmp_dir, raw_dir.parent)  # reste éventuel d'un run interrompu
    tmp_dir.mkdir(parents=True)
    rows: list[dict[str, Any]] = []
    offset = 0
    total: int | None = None
    for page in range(1, MAX_PAGES + 1):
        text = fetch_page(api_key, offset)
        page_total, data = parse_page(text)  # validation JSON AVANT écriture
        if total is None:
            total = page_total
            logger.info("response.total = %d lignes (%d par page)", total, PAGE_LENGTH)
        elif page_total != total:
            raise RawFormatError(
                f"response.total a changé en cours de pagination ({total} -> {page_total}, "
                f"page {page}) : données mises à jour pendant le run, relancez le script."
            )
        # Seule altération du brut : la clé renvoyée en écho par l'API est masquée.
        safe_text = redact(text, api_key)
        if safe_text != text:
            logger.info("Clé API renvoyée en écho par l'API : masquée dans la page brute")
        write_raw(safe_text, tmp_dir / f"page_{page:04d}_offset_{offset}.json")
        rows.extend(data)
        offset += len(data)
        if offset >= total:
            if len(rows) != total:
                raise RawFormatError(
                    f"{len(rows)} lignes reçues pour response.total={total} : relancez le script."
                )
            return rows
        if not data:
            raise RawFormatError(
                f"Page vide à l'offset {offset} alors que total={total} : arrêt."
            )
    raise RawFormatError(f"Plus de {MAX_PAGES} pages : arrêt par sécurité.")


def to_record(row: dict[str, Any], ingested_at: str) -> dict[str, Any] | None:
    """Une ligne par jour et par série ; None si la valeur est absente (loggé par l'appelant)."""
    if not isinstance(row, dict):
        raise RawFormatError(f"Ligne inattendue : {row!r}")
    missing = [k for k in ("period", "series", "value", "units") if k not in row]
    if missing:
        raise RawFormatError(f"Ligne {row!r} : clés manquantes {missing}")
    series_id = row["series"]
    if series_id not in SERIES:
        raise RawFormatError(f"Série inattendue : {series_id!r}")
    if row["units"] != EXPECTED_UNITS:
        raise RawFormatError(f"Unité inattendue pour {series_id} : {row['units']!r}")
    value = row["value"]
    if value is None or value == "":
        return None
    try:
        price = float(value)  # l'API v2 renvoie les valeurs sous forme de chaînes
    except (TypeError, ValueError) as exc:
        raise RawFormatError(f"Valeur non numérique {value!r} ({series_id}, {row['period']})") from exc
    return {
        "source": SOURCE,
        "series_id": series_id,
        "product": SERIES[series_id],
        "product_name": row.get("product-name"),
        "series_description": row.get("series-description"),
        "quote_date": row["period"],
        "price_usd": price,
        "currency": "USD",
        "unit": "barrel",
        "raw_units": row["units"],
        "ingested_at": ingested_at,
    }


def build_records(rows: list[dict[str, Any]], ingested_at: str) -> list[dict[str, Any]]:
    """Mappe les lignes, ignore les valeurs nulles, refuse les doublons (jour, série)."""
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    skipped = 0
    for row in rows:
        rec = to_record(row, ingested_at)
        if rec is None:
            skipped += 1
            continue
        key = (rec["quote_date"], rec["series_id"])
        if key in seen:
            raise RawFormatError(f"Doublon (jour, série) : {key} (pagination instable ?)")
        seen.add(key)
        records.append(rec)
    if skipped:
        logger.info("%d lignes sans valeur ignorées", skipped)
    records.sort(key=lambda r: (r["quote_date"], r["series_id"]))
    return records


def write_jsonl(records: list[dict[str, Any]], path: Path) -> None:
    """Écrit via un fichier temporaire puis remplace : relance = même fichier écrasé."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    tmp.replace(path)


def run(now: datetime | None = None, output_dir: Path | None = None) -> Path:
    output_dir = output_dir or OUTPUT_DIR  # résolu à l'appel (testable)
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    ingestion_date = now.strftime("%Y-%m-%d")

    api_key = get_api_key()  # avant toute requête réseau
    raw_dir = output_dir / f"raw_{SOURCE}_spt_{ingestion_date}"
    rows = fetch_all_pages(api_key, raw_dir)  # pages dans <raw_dir>.tmp/
    records = build_records(rows, ingested_at)
    if not records:
        raise RawFormatError("Aucune ligne exploitable.")
    # Tout est récupéré et validé : on remplace d'un bloc le dossier des pages du jour.
    publish_raw_dir(raw_dir)

    out = output_dir / f"oil_{SOURCE}_{ingestion_date}.jsonl"
    write_jsonl(records, out)
    for series_id in SERIES:
        dates = [r["quote_date"] for r in records if r["series_id"] == series_id]
        if dates:
            logger.info("%s : %d jours (du %s au %s)", series_id, len(dates), dates[0], dates[-1])
        else:
            logger.warning("%s : aucune ligne", series_id)
    logger.info("%d lignes écrites dans %s", len(records), out)
    return out


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    install_log_redaction()
    try:
        run()
    except MissingApiKeyError as exc:
        logger.error("Clé API absente : %s", exc)
        return 4
    except InvalidApiKeyError as exc:
        logger.error("Clé API refusée : %s", exc)
        return 5
    except FetchError as exc:
        logger.error("Téléchargement impossible : %s", exc)
        return 2
    except RawFormatError as exc:
        logger.error("Format inattendu : %s", exc)
        return 3
    except Exception as exc:
        # Pas de logger.exception : la trace pourrait contenir la clé.
        # redact() masque la clé exacte chargée et envoyée (après strip, cf. get_api_key),
        # et non la valeur brute de os.environ (qui peut contenir des espaces).
        logger.error("Échec du chargement du pétrole : %s", redact(repr(exc)))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
