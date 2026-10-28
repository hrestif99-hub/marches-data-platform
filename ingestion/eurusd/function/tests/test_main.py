"""Tests de la Cloud Function EUR/USD, sans réseau ni GCP.

fetch_raw est remplacé par un texte SDMX fixe, le client Storage par un faux client qui
enregistre les uploads (ordre, chemins, octets, content-type).
"""

import json
import os
import shutil
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path

import flask
import functions_framework
import pytest

from ingestion.eurusd import load_history as lh
from ingestion.eurusd.function import main
from ingestion.eurusd.tests.test_daily import WINDOW, business_days
from ingestion.eurusd.tests.test_load_history import make_payload

NOW = datetime(2026, 10, 28, 16, 30, tzinfo=timezone.utc)
DAY = "2026-10-17"
PREFIX = f"raw/eurusd/ecb/ingestion_date={DAY}"
JSONL_PATH = f"{PREFIX}/eurusd_ecb_daily_{DAY}.jsonl"
RAW_PATH = f"{PREFIX}/ecb_EXR_D.USD.EUR.SP00.A_daily_{DAY}.json"
BUCKET = "bucket-de-test"
FUNCTION_DIR = Path(main.__file__).resolve().parent
REPO_ROOT = FUNCTION_DIR.parents[2]


# --- doublures ---------------------------------------------------------------------------


class FakeBlob:
    def __init__(self, client, name):
        self.client, self.name = client, name

    def upload_from_string(self, data, content_type=None, retry=None):
        if self.client.fail_on == self.name:
            raise RuntimeError("panne GCS simulée")
        self.client.uploads.append((self.name, data, content_type))


class FakeBucket:
    def __init__(self, client, name):
        self.client, self.name = client, name

    def blob(self, name):
        return FakeBlob(self.client, name)


class FakeStorageClient:
    def __init__(self, fail_on=None):
        self.uploads = []
        self.buckets = []
        self.fail_on = fail_on

    def bucket(self, name):
        self.buckets.append(name)
        return FakeBucket(self, name)


class Recorder:
    def __init__(self, text=None, exc=None):
        self.text, self.exc, self.calls = text, exc, []

    def __call__(self, params=None):
        self.calls.append(params)
        if self.exc is not None:
            raise self.exc
        return self.text


RAW_TEXT = json.dumps(make_payload(WINDOW))
APP = flask.Flask(__name__)


@pytest.fixture
def env(monkeypatch):
    """Fonction prête : date fixe, BCE simulée, faux GCS, BUCKET défini, pas de mode local."""
    monkeypatch.setattr(main, "_now", lambda: NOW)
    fetch = Recorder(RAW_TEXT)
    monkeypatch.setattr(lh, "fetch_raw", fetch)
    client = FakeStorageClient()
    monkeypatch.setattr(main, "_make_storage_client", lambda: client)
    monkeypatch.setenv("BUCKET", BUCKET)
    monkeypatch.delenv("LOCAL_OUTPUT_DIR", raising=False)
    return {"fetch": fetch, "client": client}


def call(method="POST", body=None, data=None):
    """Appelle le point d'entrée avec une vraie requête Flask ; renvoie (code, json, en-têtes)."""
    kwargs = {"method": method}
    if body is not None:
        kwargs["json"] = body
    if data is not None:
        kwargs["data"] = data
    with APP.test_request_context("/", **kwargs):
        text, status, headers = main.collect_eurusd_daily(flask.request)
    return status, json.loads(text), headers


# --- succès et écriture GCS ----------------------------------------------------------------


def test_success_writes_raw_then_jsonl(env):
    status, body, headers = call(body={"date": DAY})
    assert status == 200
    assert headers["Content-Type"].startswith("application/json")
    assert body == {
        "status": "ok",
        "ingestion_date": DAY,
        "rows": 10,
        "first_quote_date": "2026-10-05",
        "last_quote_date": "2026-10-16",
        "gcs_jsonl_uri": f"gs://{BUCKET}/{JSONL_PATH}",
        "gcs_raw_uri": f"gs://{BUCKET}/{RAW_PATH}",
    }
    client = env["client"]
    assert client.buckets == [BUCKET]
    # Ordre : brut d'abord, JSONL en dernier (c'est lui que charge le COPY INTO).
    assert [(name, ctype) for name, _, ctype in client.uploads] == [
        (RAW_PATH, "application/json; charset=utf-8"),
        (JSONL_PATH, "application/x-ndjson; charset=utf-8"),
    ]
    raw_bytes, jsonl_bytes = client.uploads[0][1], client.uploads[1][1]
    assert raw_bytes == RAW_TEXT.encode("utf-8")  # brut tel que reçu
    lines = jsonl_bytes.decode("utf-8").splitlines()
    assert len(lines) == 10
    first = json.loads(lines[0])
    assert first["source"] == "ecb" and first["ingested_at"] == "2026-10-28T16:30:00Z"
    assert first["collection_mode"] == "daily"
    # La fenêtre envoyée à la BCE est bien celle du mode daily.
    assert env["fetch"].calls == [lh.build_params("daily", date(2026, 10, 17))]


def test_jsonl_identical_to_write_jsonl(env, tmp_path):
    """Les octets envoyés à GCS sont exactement ceux de write_jsonl (format partagé)."""
    call(body={"date": DAY})
    collection = lh.collect(mode="daily", logical_date=DAY, now=NOW)
    local = tmp_path / "ref.jsonl"
    lh.write_jsonl(collection.records, local)
    assert env["client"].uploads[1][1] == local.read_bytes()
    assert b"\r\n" not in local.read_bytes()


def test_to_jsonl_text_format():
    records = [{"a": 1, "b": "é"}, {"a": 2, "b": None}]
    assert lh.to_jsonl_text(records) == '{"a": 1, "b": "é"}\n{"a": 2, "b": null}\n'
    assert lh.to_jsonl_text([]) == ""


def test_rerun_same_day_overwrites_same_paths(env):
    call(body={"date": DAY})
    call(body={"date": DAY})
    names = [name for name, _, _ in env["client"].uploads]
    assert names == [RAW_PATH, JSONL_PATH, RAW_PATH, JSONL_PATH]


def test_default_date_is_today_utc(env, monkeypatch):
    # Fenêtre de la date par défaut (2026-10-28) : [2026-10-14, 2026-10-28].
    today_window = business_days(date(2026, 10, 14), date(2026, 10, 28))
    fetch = Recorder(json.dumps(make_payload(today_window)))
    monkeypatch.setattr(lh, "fetch_raw", fetch)
    for kwargs in ({}, {"body": {}}, {"body": {"date": None}}, {"data": b"  "}):
        env["client"].uploads.clear()
        status, body, _ = call(**kwargs)
        assert status == 200, kwargs
        assert body["ingestion_date"] == "2026-10-28"
        assert fetch.calls[-1]["endPeriod"] == "2026-10-28"
        assert body["gcs_jsonl_uri"].endswith(
            "ingestion_date=2026-10-28/eurusd_ecb_daily_2026-10-28.jsonl"
        )


def test_body_without_json_content_type_is_read(env):
    status, body, _ = call(data=json.dumps({"date": DAY}).encode())
    assert status == 200 and body["ingestion_date"] == DAY


# --- erreurs ------------------------------------------------------------------------------


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE", "PATCH"])
def test_other_methods_405(env, method):
    status, body, headers = call(method=method)
    assert status == 405
    assert headers["Allow"] == "POST"
    assert body["status"] == "error" and body["error"] == "method_not_allowed"
    assert env["fetch"].calls == [] and env["client"].uploads == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"body": {"date": "2026-13-01"}},
        {"body": {"date": "20261017"}},
        {"body": {"date": 20261017}},
        {"body": {"date": "2026-10-29"}},  # futur (aujourd'hui UTC : 2026-10-28)
        {"data": b"{pas du json"},
        {"body": ["2026-10-17"]},
    ],
)
def test_invalid_request_400(env, kwargs):
    status, body, _ = call(**kwargs)
    assert status == 400
    assert body["status"] == "error"
    assert body["error"] in {"invalid_date", "bad_request"}
    assert "Traceback" not in body["message"]
    assert env["fetch"].calls == [] and env["client"].uploads == []


def test_future_date_message(env):
    status, body, _ = call(body={"date": "2026-10-29"})
    assert status == 400 and "futur" in body["message"]


def test_fetch_error_502(env, monkeypatch):
    monkeypatch.setattr(lh, "fetch_raw", Recorder(exc=lh.FetchError("HTTP 503 : 'down'")))
    status, body, _ = call(body={"date": DAY})
    assert status == 502
    assert body["error"] == "upstream_error" and "HTTP 503" in body["message"]
    assert env["client"].uploads == []


@pytest.mark.parametrize(
    "raw",
    [
        "",  # fenêtre vide : HTTP 200 et corps vide côté BCE
        "{pas du json",
        json.dumps(make_payload(business_days(date(2026, 9, 1), date(2026, 9, 5)))),  # hors fenêtre
    ],
)
def test_format_or_check_error_422(env, monkeypatch, raw):
    monkeypatch.setattr(lh, "fetch_raw", Recorder(raw))
    status, body, _ = call(body={"date": DAY})
    assert status == 422
    assert body["error"] == "invalid_data"
    assert env["client"].uploads == []  # rien n'est écrit si collect échoue


def test_unexpected_error_500_logged_without_trace(env, monkeypatch, caplog):
    def boom(**kwargs):
        raise KeyError("secret interne")

    monkeypatch.setattr(lh, "collect", boom)
    status, body, _ = call(body={"date": DAY})
    assert status == 500
    assert body["error"] == "internal_error"
    assert "secret interne" not in body["message"] and "Traceback" not in body["message"]
    assert env["client"].uploads == []
    assert any(r.exc_info and "Erreur imprévue" in r.getMessage() for r in caplog.records)


def test_gcs_failure_on_jsonl_500_raw_only(env, monkeypatch):
    """Panne GCS au 2e upload : 500, et le JSONL (chargé par le COPY) n'existe pas."""
    client = FakeStorageClient(fail_on=JSONL_PATH)
    monkeypatch.setattr(main, "_make_storage_client", lambda: client)
    status, body, _ = call(body={"date": DAY})
    assert status == 500
    assert [name for name, _, _ in client.uploads] == [RAW_PATH]


@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_bucket_500_before_any_call(env, monkeypatch, value):
    if value is None:
        monkeypatch.delenv("BUCKET", raising=False)
    else:
        monkeypatch.setenv("BUCKET", value)
    status, body, _ = call(body={"date": DAY})
    assert status == 500
    assert body["error"] == "config_error" and "BUCKET" in body["message"]
    assert env["fetch"].calls == []  # échec rapide, aucun appel à la BCE
    assert env["client"].uploads == []


# --- mode local (LOCAL_OUTPUT_DIR) ---------------------------------------------------------


def test_local_output_dir_same_tree_as_bucket(env, monkeypatch, tmp_path):
    monkeypatch.delenv("BUCKET", raising=False)  # BUCKET inutile en mode local
    monkeypatch.setenv("LOCAL_OUTPUT_DIR", str(tmp_path))
    status, body, _ = call(body={"date": DAY})
    assert status == 200
    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file())
    assert files == sorted([JSONL_PATH, RAW_PATH])
    assert (tmp_path / RAW_PATH).read_text(encoding="utf-8") == RAW_TEXT
    collection = lh.collect(mode="daily", logical_date=DAY, now=NOW)
    assert (tmp_path / JSONL_PATH).read_bytes() == lh.to_jsonl_text(collection.records).encode()
    assert body["gcs_jsonl_uri"] == (tmp_path / JSONL_PATH).resolve().as_uri()
    assert env["client"].uploads == []  # aucun appel GCS


def test_local_output_dir_failure_writes_nothing(env, monkeypatch, tmp_path):
    monkeypatch.setenv("LOCAL_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr(lh, "fetch_raw", Recorder(""))
    status, _, _ = call(body={"date": DAY})
    assert status == 422
    assert list(tmp_path.iterdir()) == []


# --- câblage functions_framework ------------------------------------------------------------


def test_functions_framework_app(env, monkeypatch, tmp_path):
    """Le point d'entrée se charge via functions_framework comme en production."""
    monkeypatch.setenv("LOCAL_OUTPUT_DIR", str(tmp_path))
    app = functions_framework.create_app(
        target="collect_eurusd_daily", source=str(FUNCTION_DIR / "main.py")
    )
    client = app.test_client()
    assert client.get("/").status_code == 405
    # Module rechargé par functions_framework : son _now n'est pas patché, la date du
    # jour réelle serait utilisée ; on passe donc une date explicite.
    resp = client.post("/", json={"date": DAY})
    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert resp.get_json()["rows"] == 10
    assert (tmp_path / JSONL_PATH).is_file()


# --- build.sh -----------------------------------------------------------------------------


def _bash():
    bash = shutil.which("bash")
    # Sous Windows, C:\Windows\System32\bash.exe est WSL : chemins incompatibles, on l'évite.
    if bash is None or "system32" in bash.lower():
        return None
    return bash


@pytest.mark.skipif(_bash() is None, reason="bash (Git Bash) introuvable")
def test_build_sh_produces_minimal_package(tmp_path):
    out = tmp_path / "build"
    subprocess.run(
        [_bash(), (FUNCTION_DIR / "build.sh").as_posix(), out.as_posix()],
        check=True, capture_output=True, text=True,
    )
    files = sorted(p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file())
    assert files == [
        ".eurusd-function-build",
        "ingestion/__init__.py",
        "ingestion/eurusd/__init__.py",
        "ingestion/eurusd/load_history.py",
        "main.py",
        "requirements.txt",
    ]
    assert (out / "main.py").read_bytes() == (FUNCTION_DIR / "main.py").read_bytes()
    assert (out / "ingestion/eurusd/load_history.py").read_bytes() == (
        REPO_ROOT / "ingestion/eurusd/load_history.py"
    ).read_bytes()
    reqs = (out / "requirements.txt").read_text(encoding="utf-8")
    for pkg in ("functions-framework==", "google-cloud-storage==", "requests=="):
        assert pkg in reqs
    # Relance : dossier vidé puis recréé (un fichier parasite disparaît).
    (out / "parasite.txt").write_text("x")
    subprocess.run([_bash(), (FUNCTION_DIR / "build.sh").as_posix(), out.as_posix()], check=True,
                   capture_output=True)
    assert not (out / "parasite.txt").exists()


@pytest.mark.skipif(_bash() is None, reason="bash (Git Bash) introuvable")
def test_build_sh_refuses_foreign_non_empty_dir(tmp_path):
    (tmp_path / "important.txt").write_text("ne pas supprimer")
    result = subprocess.run(
        [_bash(), (FUNCTION_DIR / "build.sh").as_posix(), tmp_path.as_posix()],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert (tmp_path / "important.txt").exists()


def test_function_requirements_pinned_like_dev():
    """Mêmes versions exactes dans la fonction et dans requirements-dev.txt."""

    def pins(path):
        out = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.split("#", 1)[0].strip()
            if "==" in line:
                name, version = line.split("==")
                out[name.strip().lower()] = version.strip()
        return out

    fn = pins(FUNCTION_DIR / "requirements.txt")
    dev = {**pins(REPO_ROOT / "requirements.txt"), **pins(REPO_ROOT / "requirements-dev.txt")}
    assert set(fn) == {"functions-framework", "google-cloud-storage", "requests"}
    for name, version in fn.items():
        assert dev.get(name) == version, name
