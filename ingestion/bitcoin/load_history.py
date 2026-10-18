"""Historique journalier BTCUSDT (bougies 1d) depuis les fichiers publics Binance Vision.

Source unique : https://data.binance.vision/data/spot/ (pas api.binance.com, dont le
robots.txt interdit tout, ni data-api.binance.vision). Licence CC BY-NC-SA 4.0 (voir README).

- Mois complets : monthly/klines/BTCUSDT/1d/BTCUSDT-1d-YYYY-MM.zip (+ .CHECKSUM),
  de 2017-08 jusqu'au mois précédent.
- Mois en cours : daily/klines/BTCUSDT/1d/BTCUSDT-1d-YYYY-MM-DD.zip (+ .CHECKSUM), du 1er
  du mois jusqu'à la veille (la bougie du jour n'est pas close).
- Si le zip mensuel du mois précédent n'est pas encore publié (404, début de mois), ses
  jours sont pris dans les fichiers quotidiens.
- 404 en fin de plage = pas encore publié (toléré) ; 404 au milieu de la plage = erreur.

Brut intact : zip et CHECKSUM gardés tels que reçus, sha256 vérifié. Les zip mensuels déjà
présents localement (dernier dossier brut publié) sont réutilisés si leur sha256 correspond
toujours à leur CHECKSUM local : un mois complet ne change plus. Attention : ces mois
réutilisés ne sont vérifiés que contre le CHECKSUM LOCAL, pas contre Binance ; si Binance
republiait un mois, on ne le verrait pas. Lancer `--full` de temps en temps resynchronise
tout avec Binance. Relancer le même jour remplace les mêmes fichiers (idempotent).

Usage : python -m ingestion.bitcoin.load_history [--full]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import math
import re
import shutil
import sys
import time
import zipfile
import zlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SOURCE = "binance_vision"
SYMBOL = "BTCUSDT"
INTERVAL = "1d"
BASE_URL = "https://data.binance.vision/data/spot"
BASE_CURRENCY = "BTC"
QUOTE_CURRENCY = "USDT"
USER_AGENT = "marches-data-platform/0.1"
TIMEOUT_S = 60
MAX_ATTEMPTS = 3
BACKOFF_S = 5
PAUSE_S = 1.0  # pause minimale entre deux requêtes
RETRY_STATUSES = {429, 500, 502, 503, 504}

COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_asset_volume", "number_of_trades", "taker_buy_base_volume",
    "taker_buy_quote_volume", "ignore",
]

# Contrôles de complétude (seuils de production).
FIRST_MONTH = date(2017, 8, 1)
EXPECTED_FIRST_DATE = "2017-08-17"  # première bougie BTCUSDT
MAX_STALENESS_DAYS = 3
MAX_MISSING_DAYS = 7  # trous tolérés (loggés) au-delà desquels on refuse le jeu

US_THRESHOLD = 10**15  # >= : microsecondes (fichiers spot depuis 2025-01-01), sinon ms
MS_THRESHOLD = 10**11
SHA256_RE = re.compile(r"[0-9a-f]{64}")

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
RAW_DIR_PATTERN = re.compile(rf"raw_{SOURCE}_(\d{{4}}-\d{{2}}-\d{{2}})(\.tmp)?")

logger = logging.getLogger("ingestion.bitcoin")


class FetchError(RuntimeError):
    """Échec HTTP ou réseau (après relances), ou fichier manquant au milieu de la plage."""


class RawFormatError(ValueError):
    """Fichier brut inattendu : checksum, zip, CSV, valeurs ou complétude."""


# --- plan des fichiers ---


@dataclass(frozen=True)
class Unit:
    """Un fichier de bougies : mensuel (period = 'YYYY-MM') ou quotidien ('YYYY-MM-DD')."""

    kind: str  # "monthly" | "daily"
    period: str

    @property
    def zip_name(self) -> str:
        return f"{SYMBOL}-{INTERVAL}-{self.period}.zip"

    @property
    def checksum_name(self) -> str:
        return self.zip_name + ".CHECKSUM"

    @property
    def url(self) -> str:
        return f"{BASE_URL}/{self.kind}/klines/{SYMBOL}/{INTERVAL}/{self.zip_name}"


def month_starts(first: date, last: date) -> list[date]:
    out, d = [], first.replace(day=1)
    while d <= last:
        out.append(d)
        d = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
    return out


def days_between(first: date, last: date) -> list[date]:
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def monthly_units(today: date) -> list[Unit]:
    """2017-08 .. mois précédant `today`."""
    last_full = today.replace(day=1) - timedelta(days=1)
    return [Unit("monthly", m.strftime("%Y-%m")) for m in month_starts(FIRST_MONTH, last_full)]


def daily_units(first: date, last: date) -> list[Unit]:
    return [Unit("daily", d.isoformat()) for d in days_between(first, last)] if first <= last else []


# --- réseau ---


class Fetcher:
    """GET avec user-agent, timeout, pause >= PAUSE_S entre requêtes et relances limitées."""

    def __init__(self) -> None:
        self.requests_sent = 0
        self._last: float | None = None

    def _throttle(self) -> None:
        """Attend que PAUSE_S se soit écoulé depuis la FIN de la requête précédente."""
        if self._last is not None:
            wait = PAUSE_S - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)

    def get(self, url: str) -> bytes | None:
        """Contenu binaire ; None si 404 (pas de relance) ; FetchError sinon.

        Redirections refusées (allow_redirects=False) : aucun appel ne peut sortir de
        data.binance.vision ; un 3xx lève FetchError.
        """
        import requests

        if not url.startswith(BASE_URL + "/"):
            raise FetchError(f"URL hors de {BASE_URL} refusée : {url}")
        headers = {"User-Agent": USER_AGENT}
        last_error = ""
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._throttle()
            self.requests_sent += 1
            logger.info("GET %s (tentative %d/%d)", url, attempt, MAX_ATTEMPTS)
            try:
                try:
                    resp = requests.get(
                        url, headers=headers, timeout=TIMEOUT_S, allow_redirects=False
                    )
                finally:
                    self._last = time.monotonic()  # fin de requête (réponse ou erreur)
            except requests.RequestException as exc:
                last_error = f"erreur réseau : {exc}"
                logger.warning("%s", last_error)
            else:
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code == 404:
                    return None
                if 300 <= resp.status_code < 400:
                    raise FetchError(
                        f"Redirection HTTP {resp.status_code} refusée sur {url} "
                        f"(vers {resp.headers.get('Location')!r}) : on ne suit aucune redirection."
                    )
                last_error = f"HTTP {resp.status_code} sur {url}"
                if resp.status_code not in RETRY_STATUSES:
                    raise FetchError(last_error)
                logger.warning("Réponse HTTP %d, nouvelle tentative possible", resp.status_code)
            if attempt < MAX_ATTEMPTS:
                wait = BACKOFF_S * attempt
                logger.info("Pause de %d s avant nouvelle tentative", wait)
                time.sleep(wait)
        raise FetchError(f"Échec après {MAX_ATTEMPTS} tentatives ({last_error})")


# --- checksum, zip, CSV ---


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_checksum(text: bytes, expected_name: str) -> str:
    """Format '<sha256>  <nom du zip>' ; renvoie le sha256 attendu."""
    try:
        parts = text.decode("ascii").split()
    except UnicodeDecodeError as exc:
        raise RawFormatError(f"CHECKSUM illisible pour {expected_name}") from exc
    if len(parts) != 2 or not SHA256_RE.fullmatch(parts[0].lower()) or parts[1] != expected_name:
        raise RawFormatError(f"CHECKSUM inattendu pour {expected_name} : {text[:120]!r}")
    return parts[0].lower()


def verify_checksum(zip_bytes: bytes, checksum_bytes: bytes, zip_name: str) -> None:
    expected = parse_checksum(checksum_bytes, zip_name)
    actual = sha256_hex(zip_bytes)
    if actual != expected:
        raise RawFormatError(f"sha256 incorrect pour {zip_name} : {actual} au lieu de {expected}")


def read_csv_from_zip(zip_bytes: bytes, zip_name: str) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            members = [n for n in zf.namelist() if not n.endswith("/")]
            if len(members) != 1 or not members[0].endswith(".csv"):
                raise RawFormatError(f"{zip_name} : un seul CSV attendu, trouvé {members}")
            return zf.read(members[0]).decode("utf-8-sig")
    except (zipfile.BadZipFile, zlib.error, EOFError) as exc:
        raise RawFormatError(f"{zip_name} : zip invalide ({exc})") from exc
    except UnicodeDecodeError as exc:
        raise RawFormatError(f"{zip_name} : CSV non UTF-8 ({exc})") from exc


def parse_timestamp(value: str, where: str) -> datetime:
    """Détecte l'unité : microsecondes (>= 1e15, depuis 2025) ou millisecondes. UTC."""
    if not value.isascii() or not value.isdigit():
        raise RawFormatError(f"{where} : timestamp non entier {value!r}")
    n = int(value)
    if n >= US_THRESHOLD:
        seconds = n / 1_000_000
    elif n >= MS_THRESHOLD:
        seconds = n / 1_000
    else:
        raise RawFormatError(f"{where} : timestamp d'unité inconnue {value!r}")
    return datetime.fromtimestamp(seconds, timezone.utc)


def _positive_float(value: str, where: str, allow_zero: bool = False) -> float:
    try:
        x = float(value)
    except ValueError as exc:
        raise RawFormatError(f"{where} : valeur non numérique {value!r}") from exc
    if not math.isfinite(x) or x < 0 or (x == 0 and not allow_zero):
        raise RawFormatError(f"{where} : valeur non finie ou {'< 0' if allow_zero else '<= 0'} ({value!r})")
    return x


def parse_csv(text: str, unit: Unit, raw_file: str) -> list[dict[str, Any]]:
    """Lignes du CSV (en-tête facultatif) ; vérifie que chaque date appartient à la période."""
    lines = list(csv.reader(io.StringIO(text)))
    lines = [ln for ln in lines if ln]
    if lines and not lines[0][0].strip().isdigit():
        # Seul en-tête accepté : celui de Binance, qui commence par open_time.
        if lines[0][0].strip() != "open_time":
            raise RawFormatError(
                f"{unit.zip_name} : première ligne inattendue (ni bougie ni en-tête "
                f"open_time) : {lines[0][:3]}"
            )
        logger.debug("%s : ligne d'en-tête ignorée %s", unit.zip_name, lines[0])
        lines = lines[1:]
    if not lines:
        raise RawFormatError(f"{unit.zip_name} : aucune bougie")
    rows = []
    for i, ln in enumerate(lines, start=1):
        where = f"{unit.zip_name} ligne {i}"
        if len(ln) != len(COLUMNS):
            raise RawFormatError(f"{where} : {len(ln)} colonnes au lieu de {len(COLUMNS)}")
        raw = dict(zip(COLUMNS, (v.strip() for v in ln)))
        opened = parse_timestamp(raw["open_time"], where)
        if (opened.hour, opened.minute, opened.second, opened.microsecond) != (0, 0, 0, 0):
            raise RawFormatError(f"{where} : open_time {opened.isoformat()} n'est pas minuit UTC")
        quote_date = opened.date().isoformat()
        if not quote_date.startswith(unit.period):
            raise RawFormatError(f"{where} : date {quote_date} hors de la période {unit.period}")
        o, h, low, c = (_positive_float(raw[k], f"{where} {k}") for k in ("open", "high", "low", "close"))
        # Cohérence OHLC : erreur (pas warning). Les bougies Binance sont calculées sur les
        # trades ; une incohérence signale un fichier corrompu ou un décalage de colonnes.
        if not (low <= min(o, c) and h >= max(o, c)):
            raise RawFormatError(f"{where} : OHLC incohérent (o={o}, h={h}, l={low}, c={c})")
        trades = raw["number_of_trades"]
        if not trades.isdigit():
            raise RawFormatError(f"{where} : number_of_trades invalide {trades!r}")
        rows.append({
            "quote_date": quote_date,
            "open": o, "high": h, "low": low, "close": c,
            "volume": _positive_float(raw["volume"], f"{where} volume", allow_zero=True),
            "quote_volume": _positive_float(
                raw["quote_asset_volume"], f"{where} quote_asset_volume", allow_zero=True
            ),
            "number_of_trades": int(trades),
            "raw_file": raw_file,
        })
    return rows


# --- dossier brut (staging puis publication d'un bloc) ---


def _safe_rmtree(path: Path, output_dir: Path) -> None:
    """Supprime UNIQUEMENT un dossier `raw_binance_vision_<date>[.tmp]` de output_dir."""
    if path.parent.resolve() != output_dir.resolve() or not RAW_DIR_PATTERN.fullmatch(path.name):
        raise RuntimeError(f"Suppression refusée hors des dossiers bruts : {path}")
    if path.exists():
        logger.info("Suppression de l'ancien dossier brut : %s", path)
        shutil.rmtree(path)


def staging_dir(raw_dir: Path) -> Path:
    return raw_dir.with_name(raw_dir.name + ".tmp")


def publish_raw_dir(raw_dir: Path) -> None:
    """Remplace d'un bloc raw_dir par son dossier temporaire complet et validé."""
    tmp_dir = staging_dir(raw_dir)
    _safe_rmtree(raw_dir, raw_dir.parent)
    tmp_dir.rename(raw_dir)
    logger.info("Fichiers bruts publiés : %s", raw_dir)


def latest_published_raw_dir(output_dir: Path) -> Path | None:
    """Dernier dossier brut publié (pas .tmp), source de réutilisation des zip mensuels."""
    if not output_dir.is_dir():
        return None
    dirs = [
        p for p in output_dir.iterdir()
        if p.is_dir() and (m := RAW_DIR_PATTERN.fullmatch(p.name)) and not m.group(2)
    ]
    return max(dirs, key=lambda p: p.name) if dirs else None


def reusable_local(unit: Unit, reuse_dir: Path | None) -> tuple[bytes, bytes] | None:
    """(zip, CHECKSUM) locaux d'un mois complet si le sha256 correspond toujours.

    Vérification contre le CHECKSUM local uniquement (aucune requête) : une republication
    par Binance n'est pas détectée ; `--full` resynchronise.
    """
    if reuse_dir is None or unit.kind != "monthly":
        return None
    zp, cp = reuse_dir / unit.zip_name, reuse_dir / unit.checksum_name
    if not (zp.is_file() and cp.is_file()):
        return None
    zip_bytes, checksum_bytes = zp.read_bytes(), cp.read_bytes()
    try:
        verify_checksum(zip_bytes, checksum_bytes, unit.zip_name)
    except RawFormatError as exc:
        logger.warning("Copie locale non réutilisée (%s) : re-téléchargement", exc)
        return None
    return zip_bytes, checksum_bytes


# --- collecte ---


def fetch_unit(fetcher: Fetcher, unit: Unit) -> tuple[bytes, bytes] | None:
    """(zip, CHECKSUM) vérifiés ; None si l'un des deux est absent (404)."""
    checksum = fetcher.get(unit.url + ".CHECKSUM")
    if checksum is None:
        return None
    zip_bytes = fetcher.get(unit.url)
    if zip_bytes is None:
        return None
    verify_checksum(zip_bytes, checksum, unit.zip_name)
    return zip_bytes, checksum


def collect(
    fetcher: Fetcher, today: date, tmp_dir: Path, reuse_dir: Path | None
) -> list[dict[str, Any]]:
    """Récupère, vérifie et écrit tous les fichiers dans tmp_dir ; renvoie les bougies."""
    raw_prefix = tmp_dir.name.removesuffix(".tmp")
    rows: list[dict[str, Any]] = []
    reused = 0

    def store(unit: Unit, zip_bytes: bytes, checksum: bytes) -> None:
        (tmp_dir / unit.zip_name).write_bytes(zip_bytes)
        (tmp_dir / unit.checksum_name).write_bytes(checksum)
        text = read_csv_from_zip(zip_bytes, unit.zip_name)
        rows.extend(parse_csv(text, unit, f"{raw_prefix}/{unit.zip_name}"))

    months = monthly_units(today)
    fallback_month: Unit | None = None
    for i, unit in enumerate(months):
        got = reusable_local(unit, reuse_dir)
        if got is not None:
            reused += 1
        else:
            got = fetch_unit(fetcher, unit)
        if got is None:
            if i == len(months) - 1:
                logger.warning("%s pas encore publié : jours pris en fichiers quotidiens", unit.zip_name)
                fallback_month = unit
                continue
            raise FetchError(f"{unit.url} introuvable (404) au milieu de la plage")
        store(unit, *got)
    if reused:
        logger.info("%d zip mensuels réutilisés depuis %s (sha256 vérifié)", reused, reuse_dir)

    first_daily = today.replace(day=1)
    if fallback_month is not None:
        first_daily = date.fromisoformat(fallback_month.period + "-01")
    days = daily_units(first_daily, today - timedelta(days=1))
    missing: list[Unit] = []
    for unit in days:
        got = fetch_unit(fetcher, unit)
        if got is None:
            missing.append(unit)
            continue
        if missing:
            raise FetchError(
                f"{missing[0].url} introuvable (404) au milieu de la plage "
                f"({unit.zip_name} existe)"
            )
        store(unit, *got)
    if missing:
        logger.info(
            "Pas encore publiés (fin de plage) : %s", ", ".join(u.period for u in missing)
        )
    return rows


# --- contrôles et JSONL ---


def check_rows(
    rows: list[dict[str, Any]],
    now: datetime,
    first_date: str = EXPECTED_FIRST_DATE,
    max_staleness_days: int = MAX_STALENESS_DAYS,
    max_missing_days: int = MAX_MISSING_DAYS,
) -> list[dict[str, Any]]:
    """Doublons, première date, trous (loggés, refusés au-delà du seuil), fraîcheur."""
    if not rows:
        raise RawFormatError("Aucune bougie.")
    seen: set[str] = set()
    for r in rows:
        if r["quote_date"] in seen:
            raise RawFormatError(f"quote_date en double : {r['quote_date']}")
        seen.add(r["quote_date"])
    rows = sorted(rows, key=lambda r: r["quote_date"])
    if rows[0]["quote_date"] != first_date:
        raise RawFormatError(f"Première date {rows[0]['quote_date']} au lieu de {first_date}")
    first, last = date.fromisoformat(rows[0]["quote_date"]), date.fromisoformat(rows[-1]["quote_date"])
    gaps = [d.isoformat() for d in days_between(first, last) if d.isoformat() not in seen]
    if gaps:
        logger.warning("%d jour(s) sans bougie : %s", len(gaps), ", ".join(gaps[:20]))
        if len(gaps) > max_missing_days:
            raise RawFormatError(
                f"{len(gaps)} jours manquants (maximum toléré {max_missing_days}) : "
                f"fichier tronqué ? ex. {', '.join(gaps[:5])}"
            )
    age = (now.astimezone(timezone.utc).date() - last).days
    if age > max_staleness_days:
        raise RawFormatError(
            f"Dernière date {last} vieille de {age} jours (maximum {max_staleness_days})"
        )
    return rows


def to_record(row: dict[str, Any], ingested_at: str) -> dict[str, Any]:
    """Mapping 1:1, sans conversion de devise (USDT n'est pas USD).

    Les valeurs du CSV sont converties en float (double IEEE 754) : la valeur décimale exacte
    n'est pas garantie au-delà d'environ 15 chiffres significatifs. Le brut (zip) garde la
    précision exacte publiée par Binance.
    """
    return {
        "source": SOURCE,
        "symbol": SYMBOL,
        "quote_date": row["quote_date"],
        "open_usdt": row["open"],
        "high_usdt": row["high"],
        "low_usdt": row["low"],
        "close_usdt": row["close"],
        "base_currency": BASE_CURRENCY,
        "quote_currency": QUOTE_CURRENCY,
        "volume_btc": row["volume"],
        "quote_volume_usdt": row["quote_volume"],
        "number_of_trades": row["number_of_trades"],
        "raw_file": row["raw_file"],
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


def run(
    now: datetime | None = None,
    output_dir: Path | None = None,
    reuse: bool = True,
    fetcher: Fetcher | None = None,
) -> Path:
    """Collecte dans <raw_dir>.tmp, valide tout, PUIS publie le brut et écrit le JSONL.

    En cas d'échec, le dossier brut publié et le JSONL du jour ne sont pas touchés.
    """
    output_dir = output_dir or OUTPUT_DIR
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    ingestion_date = now.strftime("%Y-%m-%d")
    fetcher = fetcher or Fetcher()

    raw_dir = output_dir / f"raw_{SOURCE}_{ingestion_date}"
    tmp_dir = staging_dir(raw_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    reuse_dir = latest_published_raw_dir(output_dir) if reuse else None
    _safe_rmtree(tmp_dir, output_dir)  # reste éventuel d'un run interrompu
    tmp_dir.mkdir()

    try:
        rows = collect(fetcher, now.date(), tmp_dir, reuse_dir)
        rows = check_rows(rows, now)
    finally:
        logger.info("%d requête(s) HTTP envoyée(s)", fetcher.requests_sent)
    records = [to_record(r, ingested_at) for r in rows]

    publish_raw_dir(raw_dir)
    out = output_dir / f"bitcoin_{SOURCE}_{ingestion_date}.jsonl"
    write_jsonl(records, out)
    logger.info(
        "%d lignes écrites dans %s (du %s au %s)",
        len(records), out, records[0]["quote_date"], records[-1]["quote_date"],
    )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--full", action="store_true", help="re-télécharger aussi les mois déjà présents localement"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    try:
        run(reuse=not args.full)
    except FetchError as exc:
        logger.error("Téléchargement impossible : %s", exc)
        return 2
    except RawFormatError as exc:
        logger.error("Format inattendu : %s", exc)
        return 3
    except Exception:
        logger.exception("Échec du chargement de l'historique BTCUSDT")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
