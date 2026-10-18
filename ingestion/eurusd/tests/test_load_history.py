"""Tests unitaires sans réseau (structure SDMX-JSON calquée sur une vraie réponse BCE)."""

import json
from datetime import date, datetime, timedelta, timezone

import pytest
import requests

from ingestion.eurusd import load_history as lh

NOW = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)
RAW_NAME = "ecb_EXR_D.USD.EUR.SP00.A_2026-10-18.json"
JSONL_NAME = "eurusd_ecb_2026-10-18.jsonl"


def make_payload(values, keys=None):
    """values : liste de (date, valeur) ; valeur None = jour sans fixing (statut H).

    keys : clés d'observation à utiliser à la place de "0", "1", ... (tests d'index).
    """
    keys = keys or [str(i) for i in range(len(values))]
    observations = {
        k: [v, 0 if v is not None else 1, 0, None, None] for k, (_, v) in zip(keys, values)
    }
    return {
        "header": {"id": "x", "sender": {"id": "ECB"}},
        "dataSets": [{"action": "Replace", "series": {"0:0:0:0:0": {"observations": observations}}}],
        "structure": {
            "dimensions": {
                "series": [],
                "observation": [
                    {"id": "TIME_PERIOD", "role": "time", "values": [{"id": d, "name": d} for d, _ in values]}
                ],
            },
            "attributes": {
                "observation": [
                    {"id": "OBS_STATUS", "values": [{"id": "A"}, {"id": "H"}]},
                ]
            },
        },
    }


SAMPLE = [("1999-01-04", 1.1789), ("1999-01-05", 1.179), ("1999-04-02", None), ("2026-10-16", 1.1253)]


def full_history(first=date(1999, 1, 4), last=date(2026, 10, 16)):
    """Jours ouvrés first..last (~7250 lignes) : passe les seuils de production."""
    out, d = [], first
    while d <= last:
        if d.weekday() < 5:
            out.append((d.isoformat(), 1.1))
        d += timedelta(days=1)
    return out


def rows_of(values):
    return [{"quote_date": d, "value": v, "obs_status": "A"} for d, v in values]


class FakeResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


# --- parsing ---


def test_extract_observations_skips_nulls():
    rows, skipped = lh.extract_observations(make_payload(SAMPLE))
    assert [r["quote_date"] for r in rows] == ["1999-01-04", "1999-01-05", "2026-10-16"]
    assert skipped == ["1999-04-02"]
    assert rows[0] == {"quote_date": "1999-01-04", "value": 1.1789, "obs_status": "A"}


def test_to_record_mapping():
    rec = lh.to_record({"quote_date": "2026-10-16", "value": 1.1253, "obs_status": "A"}, "2026-10-18T12:00:00Z")
    assert rec == {
        "source": "ecb",
        "series_key": "EXR.D.USD.EUR.SP00.A",
        "quote_date": "2026-10-16",
        "rate_usd_per_eur": 1.1253,
        "base_currency": "EUR",
        "quote_currency": "USD",
        "unit": "USD_per_EUR",
        "obs_status": "A",
        "ingested_at": "2026-10-18T12:00:00Z",
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"dataSets": []},
        [],  # racine non objet
        {"dataSets": ["x"], "structure": {"dimensions": {"observation": []}}},
        {"dataSets": [{"series": []}], "structure": {"dimensions": {"observation": []}}},
        {"dataSets": [{"series": {"s": "x"}}], "structure": {"dimensions": {"observation": []}}},
        {"dataSets": [{}], "structure": "x"},
    ],
)
def test_extract_observations_unexpected_structure_raises(payload):
    with pytest.raises(lh.RawFormatError):
        lh.extract_observations(payload)


def test_extract_observations_non_numeric_raises():
    with pytest.raises(lh.RawFormatError, match="non numérique"):
        lh.extract_observations(make_payload([("2026-10-16", "1.12")]))


@pytest.mark.parametrize("key", ["-1", "01", "+1", " 1", "1.0"])
def test_non_canonical_index_raises(key):
    payload = make_payload([("1999-01-04", 1.1), ("1999-01-05", 1.2)], keys=["0", key])
    with pytest.raises(lh.RawFormatError, match="non canonique"):
        lh.extract_observations(payload)


def test_out_of_range_index_raises():
    payload = make_payload([("1999-01-04", 1.1)], keys=["1"])
    with pytest.raises(lh.RawFormatError, match="hors de TIME_PERIOD"):
        lh.extract_observations(payload)


def test_duplicate_quote_date_raises():
    payload = make_payload([("1999-01-04", 1.1), ("1999-01-04", 1.2)])
    with pytest.raises(lh.RawFormatError, match="en double"):
        lh.extract_observations(payload)


@pytest.mark.parametrize("bad_date", ["16/10/2026", "20261016", "2026-02-30", "", None, 20261016])
def test_invalid_date_raises(bad_date):
    payload = make_payload([("1999-01-04", 1.1)])
    payload["structure"]["dimensions"]["observation"][0]["values"][0]["id"] = bad_date
    with pytest.raises(lh.RawFormatError, match="date invalide"):
        lh.extract_observations(payload)


def test_missing_date_id_raises():
    payload = make_payload([("1999-01-04", 1.1)])
    del payload["structure"]["dimensions"]["observation"][0]["values"][0]["id"]
    with pytest.raises(lh.RawFormatError, match="date invalide"):
        lh.extract_observations(payload)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), 0, 0.0, -1.1])
def test_non_finite_or_non_positive_value_raises(value):
    with pytest.raises(lh.RawFormatError, match="non fini ou <= 0"):
        lh.extract_observations(make_payload([("1999-01-04", value)]))


def test_nan_in_raw_json_text_raises(tmp_path, monkeypatch):
    # json.loads accepte les jetons NaN / Infinity : ils doivent être refusés ensuite.
    raw = json.dumps(make_payload([("1999-01-04", 1.0)])).replace("[1.0,", "[NaN,")
    assert "NaN" in raw
    monkeypatch.setattr(lh, "fetch_raw", lambda: raw)
    with pytest.raises(lh.RawFormatError, match="non fini"):
        lh.run(now=NOW, output_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []


# --- complétude ---


def test_completeness_ok_with_production_thresholds():
    lh.check_completeness(rows_of(full_history()), NOW)


def test_completeness_wrong_first_date():
    with pytest.raises(lh.RawFormatError, match="Première date 1999-01-05"):
        lh.check_completeness(rows_of(full_history(first=date(1999, 1, 5))), NOW)


def test_completeness_too_few_rows():
    with pytest.raises(lh.RawFormatError, match="3 lignes seulement"):
        lh.check_completeness(rows_of([("1999-01-04", 1.1), ("1999-01-05", 1.1), ("2026-10-16", 1.1)]), NOW)


def test_completeness_stale_last_date():
    with pytest.raises(lh.RawFormatError, match="vieille de 11 jours"):
        lh.check_completeness(rows_of(full_history(last=date(2026, 10, 7))), NOW)


def test_completeness_tolerates_10_days():
    lh.check_completeness(rows_of(full_history(last=date(2026, 10, 8))), NOW)


def test_completeness_empty():
    with pytest.raises(lh.RawFormatError, match="Aucune observation"):
        lh.check_completeness([], NOW)


# --- run : écriture seulement après validation ---


def test_run_idempotent_and_raw_intact(tmp_path, monkeypatch):
    raw = json.dumps(make_payload(full_history() + [("2026-10-17", None)]))
    monkeypatch.setattr(lh, "fetch_raw", lambda: raw)

    out = lh.run(now=NOW, output_dir=tmp_path)
    out2 = lh.run(now=NOW, output_dir=tmp_path)  # relance : mêmes fichiers écrasés

    assert out == out2 == tmp_path / JSONL_NAME
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == len(full_history())
    assert json.loads(lines[0])["quote_date"] == "1999-01-04"
    assert json.loads(lines[-1])["quote_date"] == "2026-10-16"
    assert (tmp_path / RAW_NAME).read_text(encoding="utf-8") == raw
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted([JSONL_NAME, RAW_NAME])


@pytest.mark.parametrize(
    "bad_raw",
    [
        json.dumps({"dataSets": [{"series": {}}], "structure": {}}),  # JSON valide, structure fausse
        json.dumps(make_payload(SAMPLE)),  # structure correcte mais historique incomplet
        "<html>erreur</html>",  # JSON invalide
    ],
)
def test_failed_validation_keeps_previous_files(tmp_path, monkeypatch, bad_raw):
    # Fichiers d'un run précédent réussi le même jour : ils ne doivent pas bouger.
    (tmp_path / RAW_NAME).write_text("ANCIEN BRUT", encoding="utf-8")
    (tmp_path / JSONL_NAME).write_text("ANCIEN JSONL\n", encoding="utf-8")
    monkeypatch.setattr(lh, "fetch_raw", lambda: bad_raw)

    with pytest.raises(lh.RawFormatError):
        lh.run(now=NOW, output_dir=tmp_path)

    assert (tmp_path / RAW_NAME).read_text(encoding="utf-8") == "ANCIEN BRUT"
    assert (tmp_path / JSONL_NAME).read_text(encoding="utf-8") == "ANCIEN JSONL\n"
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted([JSONL_NAME, RAW_NAME])


def test_failed_validation_creates_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(lh, "fetch_raw", lambda: json.dumps(make_payload(SAMPLE)))
    with pytest.raises(lh.RawFormatError, match="lignes seulement"):
        lh.run(now=NOW, output_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_main_unexpected_structure_exit_3(tmp_path, monkeypatch):
    monkeypatch.setattr(lh, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(lh, "fetch_raw", lambda: json.dumps({"dataSets": "x", "structure": []}))
    assert lh.main() == 3
    assert list(tmp_path.iterdir()) == []


# --- réseau (simulé) ---


def test_fetch_raw_retries_on_5xx_then_succeeds(monkeypatch):
    calls = []
    responses = [FakeResponse(503, "busy"), FakeResponse(200, "{}")]

    def fake_get(url, **kwargs):
        calls.append(kwargs)
        return responses.pop(0)

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(lh.time, "sleep", lambda s: None)
    assert lh.fetch_raw() == "{}"
    assert len(calls) == 2
    assert calls[0]["headers"]["User-Agent"] == "marches-data-platform/0.1"
    assert calls[0]["timeout"] == lh.TIMEOUT_S


def test_fetch_raw_404_no_retry(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return FakeResponse(404, "No results found.")

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(lh.time, "sleep", lambda s: None)
    with pytest.raises(lh.FetchError, match="404"):
        lh.fetch_raw()
    assert len(calls) == 1


def test_fetch_raw_network_error_limited_retries(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        raise requests.ConnectionError("boom")

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(lh.time, "sleep", lambda s: None)
    with pytest.raises(lh.FetchError, match="3 tentatives"):
        lh.fetch_raw()
    assert len(calls) == lh.MAX_ATTEMPTS


def test_main_exit_codes(monkeypatch):
    def raise_(exc):
        def f(*a, **k):
            raise exc
        return f

    monkeypatch.setattr(lh, "run", raise_(lh.FetchError("x")))
    assert lh.main() == 2
    monkeypatch.setattr(lh, "run", raise_(lh.RawFormatError("x")))
    assert lh.main() == 3
    monkeypatch.setattr(lh, "run", raise_(RuntimeError("x")))
    assert lh.main() == 1
    monkeypatch.setattr(lh, "run", lambda: None)
    assert lh.main() == 0
