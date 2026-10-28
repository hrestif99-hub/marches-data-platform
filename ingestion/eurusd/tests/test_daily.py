"""Tests sans réseau du mode daily (fenêtre glissante), de --date et de la CLI."""

import json
import re
from datetime import date, datetime, timedelta, timezone

import pytest
import requests

from ingestion.eurusd import load_history as lh
from ingestion.eurusd.tests.test_load_history import FakeResponse, full_history, make_payload

# Exécution réelle le 2026-10-28 à 16:30 UTC ; dates logiques passées ou du jour.
NOW = datetime(2026, 10, 28, 16, 30, tzinfo=timezone.utc)
DAY = "2026-10-17"  # samedi : dernier fixing le vendredi 16
JSONL_NAME = f"eurusd_ecb_daily_{DAY}.jsonl"
RAW_NAME = f"ecb_EXR_D.USD.EUR.SP00.A_daily_{DAY}.json"
# PATTERN du COPY INTO (snowflake/04_raw_tables.sql), appliqué au chemin complet.
COPY_PATTERN = re.compile(r".*ingestion_date=\d{4}-\d{2}-\d{2}/.*[.]jsonl")


def business_days(first, last, value=1.12):
    out, d = [], first
    while d <= last:
        if d.weekday() < 5:
            out.append((d.isoformat(), value))
        d += timedelta(days=1)
    return out


# Fenêtre [2026-10-03, 2026-10-17] : 10 jours ouvrés, du 2026-10-05 au 2026-10-16.
WINDOW = business_days(date(2026, 10, 3), date(2026, 10, 17))


class Recorder:
    """Remplace fetch_raw : mémorise les paramètres et renvoie un texte fixe."""

    def __init__(self, text):
        self.text = text
        self.calls = []

    def __call__(self, params=None):
        self.calls.append(params)
        return self.text


# --- paramètres envoyés à l'API ---


def test_build_params_daily_window():
    assert lh.build_params("daily", date(2026, 10, 17)) == {
        "format": "jsondata",
        "startPeriod": "2026-10-03",
        "endPeriod": "2026-10-17",
    }
    assert lh.WINDOW_DAYS == 14


def test_build_params_history_no_filter():
    assert lh.build_params("history", date(2026, 10, 17)) == {"format": "jsondata"}


def test_fetch_raw_sends_params(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append(kwargs)
        return FakeResponse(200, "{}")

    monkeypatch.setattr(requests, "get", fake_get)
    lh.fetch_raw()
    lh.fetch_raw(lh.build_params("daily", date(2026, 10, 17)))
    assert calls[0]["params"] == {"format": "jsondata"}
    assert calls[1]["params"] == {
        "format": "jsondata", "startPeriod": "2026-10-03", "endPeriod": "2026-10-17"
    }
    assert all(c["headers"]["User-Agent"] == "marches-data-platform/0.1" for c in calls)


def test_collect_daily_sends_window_history_sends_nothing(monkeypatch):
    rec = Recorder(json.dumps(make_payload(WINDOW)))
    monkeypatch.setattr(lh, "fetch_raw", rec)
    lh.collect(mode="daily", logical_date=DAY, now=NOW)
    assert rec.calls == [
        {"format": "jsondata", "startPeriod": "2026-10-03", "endPeriod": "2026-10-17"}
    ]

    rec = Recorder(json.dumps(make_payload(full_history(last=date(2026, 10, 27)))))
    monkeypatch.setattr(lh, "fetch_raw", rec)
    lh.collect(mode="history", now=NOW)
    assert rec.calls == [None]  # fetch_raw() sans argument : tout l'historique


# --- contrôles du mode daily ---


def test_daily_staleness_relative_to_logical_date_not_now():
    # Dernier fixing le 16 : 1 jour au 2026-10-17 (OK) alors qu'il a 12 jours au 28 (NOW).
    rows = [{"quote_date": d, "value": v, "obs_status": "A"} for d, v in WINDOW]
    lh.check_window(rows, [], date(2026, 10, 17))


def test_daily_staleness_too_old_raises():
    # Fenêtre [2026-10-14, 2026-10-28], dernier fixing le 2026-10-15 : 13 jours > 10.
    rows = [{"quote_date": "2026-10-15", "value": 1.1, "obs_status": "A"}]
    with pytest.raises(lh.RawFormatError, match="vieille de 13 jours au 2026-10-28"):
        lh.check_window(rows, [], date(2026, 10, 28))


def test_daily_staleness_limit_10_days_ok():
    rows = [{"quote_date": "2026-10-18", "value": 1.1, "obs_status": "A"}]
    lh.check_window(rows, [], date(2026, 10, 28))


def test_daily_no_min_rows_nor_first_date():
    # Une seule ligne, loin du 1999-01-04 : valide en daily.
    rows = [{"quote_date": "2026-10-16", "value": 1.1, "obs_status": "A"}]
    lh.check_window(rows, [], date(2026, 10, 17))


def test_daily_empty_body_raises_and_writes_nothing(tmp_path, monkeypatch):
    # Comportement réel de l'API : fenêtre sans observation -> HTTP 200, corps vide.
    monkeypatch.setattr(lh, "fetch_raw", Recorder(""))
    with pytest.raises(lh.RawFormatError, match="Réponse vide"):
        lh.run(now=NOW, output_dir=tmp_path, mode="daily", logical_date=DAY)
    assert list(tmp_path.iterdir()) == []


def test_daily_only_null_observations_raises(tmp_path, monkeypatch):
    payload = make_payload([("2026-10-12", None), ("2026-10-13", None)])
    monkeypatch.setattr(lh, "fetch_raw", Recorder(json.dumps(payload)))
    with pytest.raises(lh.RawFormatError, match="Aucune observation non nulle dans la fenêtre"):
        lh.run(now=NOW, output_dir=tmp_path, mode="daily", logical_date=DAY)
    assert list(tmp_path.iterdir()) == []


def test_check_window_empty_rows_raises():
    with pytest.raises(lh.RawFormatError, match="Aucune observation"):
        lh.check_window([], [], date(2026, 10, 17))


@pytest.mark.parametrize("outside", ["2026-10-02", "2026-10-18", "1999-01-04"])
def test_daily_date_outside_window_raises(tmp_path, monkeypatch, outside):
    payload = make_payload(sorted(WINDOW + [(outside, 1.1)]))
    monkeypatch.setattr(lh, "fetch_raw", Recorder(json.dumps(payload)))
    with pytest.raises(lh.RawFormatError, match="hors de la fenêtre"):
        lh.run(now=NOW, output_dir=tmp_path, mode="daily", logical_date=DAY)
    assert list(tmp_path.iterdir()) == []


def test_daily_null_date_outside_window_raises():
    rows = [{"quote_date": "2026-10-16", "value": 1.1, "obs_status": "A"}]
    with pytest.raises(lh.RawFormatError, match="hors de la fenêtre"):
        lh.check_window(rows, ["2026-10-18"], date(2026, 10, 17))


def test_daily_window_bounds_inclusive():
    rows = [
        {"quote_date": "2026-10-03", "value": 1.1, "obs_status": "A"},
        {"quote_date": "2026-10-17", "value": 1.1, "obs_status": "A"},
    ]
    lh.check_window(rows, [], date(2026, 10, 17))


def test_daily_keeps_parsing_checks(monkeypatch):
    payload = make_payload([("2026-10-15", 1.1), ("2026-10-15", 1.2)])
    monkeypatch.setattr(lh, "fetch_raw", Recorder(json.dumps(payload)))
    with pytest.raises(lh.RawFormatError, match="en double"):
        lh.collect(mode="daily", logical_date=DAY, now=NOW)
    monkeypatch.setattr(lh, "fetch_raw", Recorder(json.dumps(make_payload([("2026-10-15", -1.0)]))))
    with pytest.raises(lh.RawFormatError, match="non fini ou <= 0"):
        lh.collect(mode="daily", logical_date=DAY, now=NOW)


# --- date logique (--date) ---


@pytest.mark.parametrize("bad", ["2026-02-30", "20261017", "17/10/2026", "2026-10-1", "", "demain"])
def test_invalid_logical_date_rejected(bad):
    with pytest.raises(lh.UsageError, match="--date invalide"):
        lh.parse_logical_date(bad, NOW)


def test_future_logical_date_rejected():
    with pytest.raises(lh.UsageError, match="dans le futur"):
        lh.parse_logical_date("2026-10-29", NOW)


def test_logical_date_today_utc_by_default():
    # 23:30 à Paris le 28 = 22:30 UTC le 28 ; 00:30 à Paris le 29 = 22:30 UTC le 28.
    late = datetime(2026, 10, 29, 0, 30, tzinfo=timezone(timedelta(hours=1)))
    assert lh.parse_logical_date(None, late) == date(2026, 10, 28)
    assert lh.parse_logical_date("2026-10-28", NOW) == date(2026, 10, 28)


def test_invalid_date_fetches_nothing(monkeypatch):
    rec = Recorder("{}")
    monkeypatch.setattr(lh, "fetch_raw", rec)
    with pytest.raises(lh.UsageError):
        lh.collect(mode="daily", logical_date="2027-01-01", now=NOW)
    assert rec.calls == []


# --- collecte / écriture, idempotence ---


def test_collect_returns_everything_without_writing(tmp_path, monkeypatch):
    raw = json.dumps(make_payload(WINDOW))
    monkeypatch.setattr(lh, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(lh, "fetch_raw", Recorder(raw))
    c = lh.collect(mode="daily", logical_date=DAY, now=NOW)
    assert list(tmp_path.iterdir()) == []
    assert c.raw_text == raw
    assert (c.mode, c.ingestion_date, c.ingested_at) == ("daily", DAY, "2026-10-28T16:30:00Z")
    assert (c.window_start, c.window_end) == ("2026-10-03", "2026-10-17")
    assert (c.jsonl_filename, c.raw_filename) == (JSONL_NAME, RAW_NAME)
    assert c.gcs_paths == (
        f"raw/eurusd/ecb/ingestion_date={DAY}/{JSONL_NAME}",
        f"raw/eurusd/ecb/ingestion_date={DAY}/{RAW_NAME}",
    )
    assert [r["quote_date"] for r in c.records] == [d for d, _ in WINDOW]
    assert c.records[-1] == {
        "source": "ecb",
        "series_key": "EXR.D.USD.EUR.SP00.A",
        "quote_date": "2026-10-16",
        "rate_usd_per_eur": 1.12,
        "base_currency": "EUR",
        "quote_currency": "USD",
        "unit": "USD_per_EUR",
        "obs_status": "A",
        "ingested_at": "2026-10-28T16:30:00Z",  # heure réelle, pas la date logique
        "collection_mode": "daily",
        "window_start": "2026-10-03",
        "window_end": "2026-10-17",
    }


def test_history_records_have_null_window(monkeypatch):
    raw = json.dumps(make_payload(full_history(last=date(2026, 10, 27))))
    monkeypatch.setattr(lh, "fetch_raw", Recorder(raw))
    c = lh.collect(now=NOW)
    assert c.ingestion_date == "2026-10-28"
    # Noms d'historique inchangés.
    assert (c.jsonl_filename, c.raw_filename) == (
        "eurusd_ecb_2026-10-28.jsonl", "ecb_EXR_D.USD.EUR.SP00.A_2026-10-28.json"
    )
    assert c.gcs_paths == lh.gcs_object_paths("2026-10-28", "history")
    rec = c.records[0]
    assert (rec["collection_mode"], rec["window_start"], rec["window_end"]) == ("history", None, None)


def test_two_daily_runs_same_date_rewrite_same_files(tmp_path, monkeypatch):
    monkeypatch.setattr(lh, "fetch_raw", Recorder(json.dumps(make_payload(WINDOW))))
    out1 = lh.run(now=NOW, output_dir=tmp_path, mode="daily", logical_date=DAY)
    first = out1.read_text(encoding="utf-8")
    later = NOW + timedelta(hours=1)
    out2 = lh.run(now=later, output_dir=tmp_path, mode="daily", logical_date=DAY)

    assert out1 == out2 == tmp_path / JSONL_NAME
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted([JSONL_NAME, RAW_NAME])
    second = out2.read_text(encoding="utf-8")
    assert second != first  # réécrit : ingested_at a changé
    strip = lambda text: [  # noqa: E731
        {k: v for k, v in json.loads(line).items() if k != "ingested_at"}
        for line in text.splitlines()
    ]
    assert strip(first) == strip(second)
    assert json.loads(second.splitlines()[0])["ingested_at"] == "2026-10-28T17:30:00Z"


def test_daily_failure_keeps_previous_files(tmp_path, monkeypatch):
    (tmp_path / RAW_NAME).write_text("ANCIEN BRUT", encoding="utf-8")
    (tmp_path / JSONL_NAME).write_text("ANCIEN JSONL\n", encoding="utf-8")
    monkeypatch.setattr(lh, "fetch_raw", Recorder(""))
    with pytest.raises(lh.RawFormatError):
        lh.run(now=NOW, output_dir=tmp_path, mode="daily", logical_date=DAY)
    assert (tmp_path / RAW_NAME).read_text(encoding="utf-8") == "ANCIEN BRUT"
    assert (tmp_path / JSONL_NAME).read_text(encoding="utf-8") == "ANCIEN JSONL\n"


# --- chemins GCS ---


def test_file_names_depend_on_mode():
    assert lh.file_names("history", "2026-10-18") == (
        "eurusd_ecb_2026-10-18.jsonl", "ecb_EXR_D.USD.EUR.SP00.A_2026-10-18.json"
    )
    assert lh.file_names("daily", "2026-10-18") == (
        "eurusd_ecb_daily_2026-10-18.jsonl", "ecb_EXR_D.USD.EUR.SP00.A_daily_2026-10-18.json"
    )


def test_gcs_object_paths():
    assert lh.gcs_object_paths("2026-10-17", "daily") == (
        "raw/eurusd/ecb/ingestion_date=2026-10-17/eurusd_ecb_daily_2026-10-17.jsonl",
        "raw/eurusd/ecb/ingestion_date=2026-10-17/ecb_EXR_D.USD.EUR.SP00.A_daily_2026-10-17.json",
    )
    assert lh.gcs_object_paths("2026-10-18", "history") == (
        "raw/eurusd/ecb/ingestion_date=2026-10-18/eurusd_ecb_2026-10-18.jsonl",
        "raw/eurusd/ecb/ingestion_date=2026-10-18/ecb_EXR_D.USD.EUR.SP00.A_2026-10-18.json",
    )


def test_gcs_object_paths_requires_mode():
    with pytest.raises(TypeError):
        lh.gcs_object_paths("2026-10-17")  # pas de chemin d'historique par défaut
    with pytest.raises(lh.UsageError):
        lh.gcs_object_paths("2026-10-17", "weekly")


@pytest.mark.parametrize("mode", ["history", "daily"])
def test_copy_pattern_matches_jsonl_only(mode):
    jsonl, raw = lh.gcs_object_paths("2026-10-17", mode)
    # Snowflake compare le PATTERN au chemin complet (URL du stage comprise).
    assert COPY_PATTERN.fullmatch(jsonl)
    assert COPY_PATTERN.fullmatch(f"gcs://bucket/{jsonl}")
    assert not COPY_PATTERN.fullmatch(raw)
    assert not COPY_PATTERN.fullmatch(f"gcs://bucket/{raw}")


@pytest.mark.parametrize("bad", ["2026-10-32", "20261017", "../x"])
def test_gcs_object_paths_rejects_bad_date(bad):
    with pytest.raises(lh.RawFormatError):
        lh.gcs_object_paths(bad, "daily")


def test_daily_never_touches_history_file_same_date(tmp_path, monkeypatch):
    # Fichiers d'historique du 2026-10-18 (chargement unique) déjà présents.
    hist_jsonl = tmp_path / "eurusd_ecb_2026-10-18.jsonl"
    hist_raw = tmp_path / "ecb_EXR_D.USD.EUR.SP00.A_2026-10-18.json"
    hist_jsonl.write_text("HISTORIQUE JSONL\n", encoding="utf-8")
    hist_raw.write_text("HISTORIQUE BRUT", encoding="utf-8")
    window = business_days(date(2026, 10, 4), date(2026, 10, 18))
    monkeypatch.setattr(lh, "fetch_raw", Recorder(json.dumps(make_payload(window))))

    out = lh.run(now=NOW, output_dir=tmp_path, mode="daily", logical_date="2026-10-18")

    assert out == tmp_path / "eurusd_ecb_daily_2026-10-18.jsonl"
    assert out != hist_jsonl
    assert hist_jsonl.read_text(encoding="utf-8") == "HISTORIQUE JSONL\n"
    assert hist_raw.read_text(encoding="utf-8") == "HISTORIQUE BRUT"
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted([
        "eurusd_ecb_2026-10-18.jsonl",
        "ecb_EXR_D.USD.EUR.SP00.A_2026-10-18.json",
        "eurusd_ecb_daily_2026-10-18.jsonl",
        "ecb_EXR_D.USD.EUR.SP00.A_daily_2026-10-18.json",
    ])
    assert lh.gcs_object_paths("2026-10-18", "daily")[0] != lh.gcs_object_paths("2026-10-18", "history")[0]


# --- CLI et codes de sortie ---


def capture_run(monkeypatch):
    calls = []
    monkeypatch.setattr(lh, "run", lambda **kw: calls.append(kw))
    return calls


def test_main_passes_mode_and_date(monkeypatch):
    calls = capture_run(monkeypatch)
    assert lh.main(["--mode", "daily", "--date", DAY]) == 0
    assert lh.main(["--mode", "daily"]) == 0
    assert lh.main([]) == 0
    assert lh.main(["--mode", "history"]) == 0
    assert calls == [{"mode": "daily", "logical_date": DAY}, {"mode": "daily"}, {}, {}]


@pytest.mark.parametrize(
    "argv",
    [
        ["--mode", "weekly"],
        ["--date"],
        ["--unknown"],
        ["--mode", "daily", "--date", "2026-02-30"],
        ["--mode", "daily", "--date", "2999-01-01"],
        ["--date", "17/10/2026"],
    ],
)
def test_main_usage_errors_exit_4(tmp_path, monkeypatch, argv):
    monkeypatch.setattr(lh, "OUTPUT_DIR", tmp_path)
    rec = Recorder("{}")
    monkeypatch.setattr(lh, "fetch_raw", rec)
    assert lh.main(argv) == lh.EXIT_USAGE == 4
    assert rec.calls == []
    assert list(tmp_path.iterdir()) == []


def test_main_daily_exit_codes(tmp_path, monkeypatch):
    monkeypatch.setattr(lh, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(lh, "fetch_raw", Recorder(""))  # fenêtre vide
    assert lh.main(["--mode", "daily", "--date", DAY]) == 3

    def boom(params=None):
        raise lh.FetchError("x")

    monkeypatch.setattr(lh, "fetch_raw", boom)
    assert lh.main(["--mode", "daily", "--date", DAY]) == 2

    monkeypatch.setattr(lh, "fetch_raw", Recorder(json.dumps(make_payload(WINDOW))))
    assert lh.main(["--mode", "daily", "--date", DAY]) == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted([JSONL_NAME, RAW_NAME])
