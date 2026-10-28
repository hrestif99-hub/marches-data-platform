"""Cloud Function HTTP (2e génération) : collecte quotidienne EUR/USD de la BCE vers GCS.

Appelée par Airflow (pas de Cloud Scheduler, cf. « Flux quotidien » du CLAUDE.md) :

    POST {"date": "YYYY-MM-DD"}   -> date LOGIQUE de la collecte (aujourd'hui UTC si absente)

Toute la collecte et la validation sont faites par `load_history.collect(mode="daily")` :
rien n'est réimplémenté ici. Cette fonction ne fait que :
1. lire la requête et la configuration ;
2. appeler `collect` (téléchargement + contrôles, SANS écriture) ;
3. écrire les deux objets aux chemins donnés par `Collection.gcs_paths`, le `.json` brut
   d'abord, le `.jsonl` EN DERNIER : le JSONL est ce que charge le COPY INTO, il n'apparaît
   (ou n'est remplacé) que si tout le reste a réussi ;
4. répondre en JSON.

Relance le même jour : les deux objets au nom fixe sont ÉCRASÉS (décision validée).

Configuration (variables d'environnement, aucun secret : l'API BCE n'a pas de clé) :
- `BUCKET` : bucket GCS cible, obligatoire en mode GCS ;
- `LOCAL_OUTPUT_DIR` : si définie, écriture dans ce dossier (même arborescence que le
  bucket) au lieu de GCS ; sert aux tests locaux sans GCP. `BUCKET` n'est alors pas lue.

Codes HTTP : 200 succès ; 400 corps ou date invalide / future ; 405 méthode autre que POST ;
422 format BCE ou contrôle invalide ; 502 erreur réseau ou HTTP de la BCE ; 500 erreur
imprévue ou configuration manquante (détail dans les logs, jamais de trace dans la réponse).

Dans le paquet déployé (voir build.sh), ce fichier est à la racine et `ingestion/eurusd/`
est copié à côté : l'import `from ingestion.eurusd import load_history` marche dans les
deux cas (repo et paquet).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import functions_framework

from ingestion.eurusd import load_history as lh

MODE = lh.MODE_DAILY
CONTENT_TYPE_JSONL = "application/x-ndjson; charset=utf-8"
CONTENT_TYPE_RAW = "application/json; charset=utf-8"
RESPONSE_HEADERS = {"Content-Type": "application/json; charset=utf-8"}

logger = logging.getLogger("ingestion.eurusd.function")


class ConfigError(RuntimeError):
    """Configuration manquante (variable d'environnement)."""


class BadRequest(ValueError):
    """Corps de requête illisible (JSON invalide, pas un objet)."""


# --- logs -------------------------------------------------------------------------------


class _CloudLoggingFormatter(logging.Formatter):
    """Une ligne JSON par log : Cloud Logging lit `severity` et `message` (logs structurés)."""

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)
        return json.dumps(
            {"severity": record.levelname, "message": message, "logger": record.name},
            ensure_ascii=False,
        )


def _configure_logging() -> None:
    """Handler sur le logger `ingestion` (logs du chargeur ET de la fonction), une seule fois.

    Sur Cloud Run / Cloud Functions 2e gén., K_SERVICE est défini : logs JSON structurés
    (sévérité correcte dans Cloud Logging). En local : format texte lisible.
    """
    root = logging.getLogger("ingestion")
    if any(getattr(h, "_eurusd_function", False) for h in root.handlers):
        return
    handler = logging.StreamHandler(sys.stdout)
    if os.environ.get("K_SERVICE"):
        handler.setFormatter(_CloudLoggingFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        )
    handler._eurusd_function = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(logging.INFO)


_configure_logging()


# --- points d'injection (remplacés dans les tests) ---------------------------------------


def _now() -> datetime:
    """Heure réelle UTC (date par défaut et `ingested_at`)."""
    return datetime.now(timezone.utc)


def _make_storage_client() -> Any:
    """Client GCS : identifiants du compte de service d'exécution (ADC), aucun secret."""
    from google.cloud import storage

    return storage.Client()


# --- écriture ---------------------------------------------------------------------------


def write_gcs(collection: lh.Collection, bucket_name: str, client: Any = None) -> tuple[str, str]:
    """Écrit le brut PUIS le JSONL dans GCS (écrasement) ; renvoie (uri jsonl, uri brut).

    Seules opérations : création / remplacement d'objets sous raw/eurusd/ (aucune lecture,
    aucun listing), compatibles avec le rôle conditionnel posé par setup_gcp.sh.
    Si le brut est écrit mais que le JSONL échoue, le brut reste seul : sans effet sur le
    COPY INTO (son PATTERN ne charge que *.jsonl) et remplacé à la relance.
    """
    from google.cloud.storage.retry import DEFAULT_RETRY

    client = client if client is not None else _make_storage_client()
    bucket = client.bucket(bucket_name)
    jsonl_path, raw_path = collection.gcs_paths
    # Relance automatique des erreurs transitoires : sans précondition de génération, la
    # bibliothèque ne relance pas par défaut. Réécrire les mêmes octets est sans risque ici.
    bucket.blob(raw_path).upload_from_string(
        collection.raw_text.encode("utf-8"), content_type=CONTENT_TYPE_RAW, retry=DEFAULT_RETRY
    )
    logger.info("Brut écrit : gs://%s/%s", bucket_name, raw_path)
    bucket.blob(jsonl_path).upload_from_string(
        lh.to_jsonl_text(collection.records).encode("utf-8"),
        content_type=CONTENT_TYPE_JSONL,
        retry=DEFAULT_RETRY,
    )
    logger.info("JSONL écrit (en dernier) : gs://%s/%s", bucket_name, jsonl_path)
    return f"gs://{bucket_name}/{jsonl_path}", f"gs://{bucket_name}/{raw_path}"


def write_local_tree(collection: lh.Collection, root: Path) -> tuple[str, str]:
    """Mode test local : même arborescence que le bucket, sous `root`. Brut puis JSONL."""
    jsonl_path, raw_path = collection.gcs_paths
    raw_file, jsonl_file = root / raw_path, root / jsonl_path
    lh.write_raw(collection.raw_text, raw_file)
    lh.write_jsonl(collection.records, jsonl_file)  # même format : lh.to_jsonl_text
    logger.info("JSONL écrit (en dernier) : %s", jsonl_file)
    return jsonl_file.resolve().as_uri(), raw_file.resolve().as_uri()


# --- requête / réponse -------------------------------------------------------------------


def _response(
    status: int, body: dict[str, Any], extra_headers: dict[str, str] | None = None
) -> tuple[str, int, dict[str, str]]:
    return json.dumps(body, ensure_ascii=False), status, {**RESPONSE_HEADERS, **(extra_headers or {})}


def _error(
    status: int, error: str, message: str, extra_headers: dict[str, str] | None = None
) -> tuple[str, int, dict[str, str]]:
    return _response(
        status, {"status": "error", "error": error, "message": message}, extra_headers
    )


def _requested_date(request: Any) -> str | None:
    """Lit `date` dans le corps JSON. Corps vide, clé absente ou null : None (aujourd'hui UTC).

    Le Content-Type n'est pas exigé (un `curl -d` sans en-tête marche aussi).
    """
    data = request.get_data(cache=True)
    if not data or not data.strip():
        return None
    try:
        payload = json.loads(data)
    except (ValueError, UnicodeDecodeError) as exc:
        raise BadRequest(f"Corps JSON invalide : {exc}") from exc
    if not isinstance(payload, dict):
        raise BadRequest('Le corps doit être un objet JSON, ex. {"date": "2026-10-27"}.')
    return payload.get("date")


def _target() -> tuple[str, str]:
    """('local', dossier) si LOCAL_OUTPUT_DIR est défini, sinon ('gcs', bucket)."""
    local = os.environ.get("LOCAL_OUTPUT_DIR", "").strip()
    if local:
        return "local", local
    bucket = os.environ.get("BUCKET", "").strip()
    if not bucket:
        raise ConfigError(
            "Variable d'environnement BUCKET manquante (bucket GCS cible), "
            "ou LOCAL_OUTPUT_DIR pour un test local."
        )
    return "gcs", bucket


@functions_framework.http
def collect_eurusd_daily(request: Any) -> tuple[str, int, dict[str, str]]:
    """Point d'entrée HTTP. Voir la docstring du module pour le contrat."""
    if request.method != "POST":
        return _error(
            405, "method_not_allowed",
            f"Méthode {request.method} non autorisée : utiliser POST.", {"Allow": "POST"},
        )
    try:
        logical_date = _requested_date(request)
        # Configuration vérifiée AVANT tout appel réseau : échec rapide et sans effet.
        kind, destination = _target()
        logger.info(
            "Collecte EUR/USD daily demandée : date=%s, sortie=%s (%s)",
            logical_date or "(défaut : aujourd'hui UTC)", kind, destination,
        )
        collection = lh.collect(mode=MODE, logical_date=logical_date, now=_now())
        if kind == "local":
            jsonl_uri, raw_uri = write_local_tree(collection, Path(destination))
        else:
            jsonl_uri, raw_uri = write_gcs(collection, destination)
    except BadRequest as exc:
        logger.warning("Requête refusée (400) : %s", exc)
        return _error(400, "bad_request", str(exc))
    except lh.UsageError as exc:
        logger.warning("Date refusée (400) : %s", exc)
        return _error(400, "invalid_date", str(exc))
    except lh.FetchError as exc:
        logger.error("API BCE injoignable ou en erreur (502) : %s", exc)
        return _error(502, "upstream_error", f"Erreur de l'API BCE : {exc}")
    except lh.RawFormatError as exc:
        logger.error("Format BCE inattendu ou contrôle échoué (422) : %s", exc)
        return _error(422, "invalid_data", str(exc))
    except ConfigError as exc:
        logger.error("Configuration invalide (500) : %s", exc)
        return _error(500, "config_error", str(exc))
    except Exception:
        # Trace complète dans Cloud Logging, jamais dans la réponse.
        logger.exception("Erreur imprévue pendant la collecte EUR/USD (500)")
        return _error(500, "internal_error", "Erreur interne imprévue : voir les logs de la fonction.")

    body = {
        "status": "ok",
        "ingestion_date": collection.ingestion_date,
        "rows": len(collection.records),
        "first_quote_date": collection.records[0]["quote_date"],
        "last_quote_date": collection.records[-1]["quote_date"],
        "gcs_jsonl_uri": jsonl_uri,
        "gcs_raw_uri": raw_uri,
    }
    logger.info(
        "Collecte OK : ingestion_date=%s, %d lignes (%s -> %s), %s",
        body["ingestion_date"], body["rows"], body["first_quote_date"],
        body["last_quote_date"], jsonl_uri,
    )
    return _response(200, body)
