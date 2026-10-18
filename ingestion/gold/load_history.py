"""Chargement unique de l'historique de l'or (Boursorama, symbole _GC).

Décision pédagogique du propriétaire du projet (voir CLAUDE.md, section Scraping) :
- collecte UNIQUE, jamais planifiée ;
- si la réponse brute existe déjà localement, AUCUNE requête réseau ;
- arrêt net sur 403/429/captcha, sans contournement ;
- aucune donnée brute dans Git (ingestion/gold/output/ est ignoré).

Usage : python -m ingestion.gold.load_history
"""

import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SOURCE = "boursorama"
SYMBOL = "_GC"
LENGTH = 7300  # plafond accepté par l'endpoint (au-delà : réponse vide)
URL = (
    "https://www.boursorama.com/bourse/action/graph/ws/GetTicksEOD"
    f"?symbol={SYMBOL}&length={LENGTH}&period=0&guid="
)
# User-agent explicite avec une URL de contact (repo du projet), sans e-mail personnel.
USER_AGENT = "marches-data-platform/0.1 (+https://github.com/hrestif99-hub/marches-data-platform)"
TIMEOUT_S = 30
MAX_ATTEMPTS = 3
SNIPPET_CHARS = 300  # taille max de l'extrait de réponse loggué
BACKOFF_S = 5  # >= 3 s exigé par CLAUDE.md

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
RAW_FILE = OUTPUT_DIR / f"eod_{LENGTH}.json"

EPOCH = date(1970, 1, 1)
QUOTE_KEYS = ("d", "o", "h", "l", "c", "v")

logger = logging.getLogger("ingestion.gold")


class FetchBlockedError(RuntimeError):
    """Le site refuse l'accès (403/429/captcha) : on s'arrête, sans contournement."""


class RawFormatError(ValueError):
    """La réponse brute n'a pas la structure attendue."""


def day_index_to_date(day_index: int) -> date:
    """Convertit `d` (nombre de jours depuis 1970-01-01) en date."""
    if isinstance(day_index, bool) or not isinstance(day_index, int):
        raise RawFormatError(f"Index de jour invalide : {day_index!r}")
    return EPOCH + timedelta(days=day_index)


def fetch_raw() -> str:
    """Une seule requête (relances limitées sur erreur réseau / 5xx uniquement)."""
    import requests  # import local : inutile quand le fichier brut existe déjà

    # X-Requested-With : c'est l'en-tête que le JavaScript de la page envoie pour ces
    # endpoints XHR. L'analyse a montré que le fragment HTML associé renvoie 404 sans lui ;
    # pour GetTicksEOD, il est conservé par cohérence avec l'appel du navigateur.
    # Ce n'est PAS un contournement d'anti-bot : le user-agent reste explicite.
    headers = {"User-Agent": USER_AGENT, "X-Requested-With": "XMLHttpRequest"}
    last_error: Exception | None = None
    attempts_made = 0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        attempts_made = attempt
        logger.info("Requête HTTP GET %s (tentative %d/%d)", URL, attempt, MAX_ATTEMPTS)
        try:
            resp = requests.get(URL, headers=headers, timeout=TIMEOUT_S)
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Erreur réseau : %s", exc)
        else:
            if resp.status_code in (403, 429):
                raise FetchBlockedError(
                    f"HTTP {resp.status_code} : accès refusé, arrêt sans contournement."
                )
            body = resp.text
            if "captcha" in body.lower():
                raise FetchBlockedError("Captcha détecté : arrêt sans contournement.")
            if resp.status_code == 200:
                return body
            last_error = RuntimeError(f"HTTP {resp.status_code}")
            logger.warning("Réponse inattendue : HTTP %d", resp.status_code)
            if resp.status_code < 500:
                break  # erreur client : relancer ne servirait à rien
        if attempt < MAX_ATTEMPTS:
            wait = BACKOFF_S * attempt
            logger.info("Pause de %d s avant nouvelle tentative", wait)
            time.sleep(wait)
    raise RuntimeError(
        f"Échec du téléchargement après {attempts_made} tentative(s) : {last_error}"
    )


def write_text_atomic(text: str, path: Path) -> None:
    """Écrit via `<nom>.tmp` puis remplace ; le `.tmp` est supprimé en cas d'échec.

    newline="" : contenu écrit tel quel, sans conversion des fins de ligne.
    """
    tmp = path.with_name(path.name + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="") as f:
            f.write(text)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)  # no-op après un replace réussi


def save_invalid_response(text: str, raw_file: Path) -> Path:
    """Conserve une réponse non-JSON sous un nom distinct, jamais sous le nom du brut valide."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = raw_file.with_name(f"{raw_file.stem}.invalid.{stamp}.txt")
    write_text_atomic(text, path)
    return path


def load_raw(raw_file: Path = RAW_FILE) -> dict[str, Any]:
    """Lit la réponse brute locale ; sinon la télécharge et l'écrit telle quelle."""
    if raw_file.exists():
        logger.info("Fichier brut existant : %s -> aucune requête réseau", raw_file)
        text = raw_file.read_text(encoding="utf-8")
    else:
        logger.info("Fichier brut absent : %s -> téléchargement unique", raw_file)
        text = fetch_raw()
        raw_file.parent.mkdir(parents=True, exist_ok=True)
        # On valide que c'est du JSON avant d'écrire, sans reformater le contenu.
        try:
            json.loads(text)
        except json.JSONDecodeError as exc:
            # Ex. page HTML anti-bot sans le mot « captcha » : on garde la réponse
            # pour analyse, sous un nom distinct pour que la relance ne la prenne
            # pas pour un brut valide.
            kept = save_invalid_response(text, raw_file)
            logger.error(
                "Réponse non-JSON (%d caractères), conservée dans %s ; extrait : %r",
                len(text), kept, text[:SNIPPET_CHARS],
            )
            raise RawFormatError(f"Réponse non-JSON (voir {kept}) : {exc}") from exc
        write_text_atomic(text, raw_file)
        logger.info("Réponse brute écrite telle quelle : %s (%d octets)", raw_file, len(text))
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RawFormatError(f"JSON invalide dans {raw_file} : {exc}") from exc


def extract_quotes(payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Vérifie la structure et renvoie (en-tête, QuoteTab)."""
    header = payload.get("d") if isinstance(payload, dict) else None
    if not isinstance(header, dict):
        raise RawFormatError("Clé racine 'd' absente ou invalide.")
    for key in ("Name", "SymbolId", "QuoteTab"):
        if key not in header:
            raise RawFormatError(f"Clé attendue absente : d.{key}")
    quotes = header["QuoteTab"]
    if not isinstance(quotes, list) or not quotes:
        raise RawFormatError("QuoteTab est vide ou invalide.")
    for i, quote in enumerate(quotes):
        missing = [k for k in QUOTE_KEYS if not isinstance(quote, dict) or k not in quote]
        if missing:
            raise RawFormatError(f"QuoteTab[{i}] : clés manquantes {missing}")
    return header, quotes


def to_record(
    quote: dict[str, Any], instrument_name: str, symbol: str, ingested_at: str
) -> dict[str, Any]:
    """Mapping 1:1 d'un tick, sans correction OHLC ni comblement de trous."""
    return {
        "source": SOURCE,
        "symbol": symbol,
        "instrument_name": instrument_name,
        "quote_date": day_index_to_date(quote["d"]).isoformat(),
        "raw_day_index": quote["d"],
        "open_usd": quote["o"],
        "high_usd": quote["h"],
        "low_usd": quote["l"],
        "price_usd": quote["c"],
        "volume": quote["v"],
        "currency": "USD",
        "unit": "troy_ounce",
        "ingested_at": ingested_at,
    }


def write_jsonl(records: list[dict[str, Any]], path: Path) -> None:
    """Écrit via un fichier temporaire puis remplace : relance = même fichier écrasé."""
    tmp = path.with_suffix(".jsonl.tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="\n") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)  # no-op après un replace réussi


def run(now: datetime | None = None, raw_file: Path = RAW_FILE) -> Path:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    ingestion_date = now.strftime("%Y-%m-%d")

    payload = load_raw(raw_file)
    header, quotes = extract_quotes(payload)
    records = [to_record(q, header["Name"], header["SymbolId"], ingested_at) for q in quotes]

    out = raw_file.parent / f"gold_{SOURCE}_{ingestion_date}.jsonl"
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
    except FetchBlockedError as exc:
        logger.error("Accès bloqué : %s", exc)
        return 2
    except RawFormatError as exc:
        logger.error("Format inattendu : %s", exc)
        return 3
    except Exception:
        logger.exception("Échec du chargement de l'historique de l'or")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
