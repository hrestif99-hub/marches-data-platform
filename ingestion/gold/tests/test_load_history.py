"""Tests unitaires sans réseau."""

import json
from datetime import date, datetime, timezone

import pytest

from ingestion.gold import load_history as lh


def test_day_index_to_date_epoch():
    assert lh.day_index_to_date(0) == date(1970, 1, 1)


def test_day_index_to_date_known_value():
    assert lh.day_index_to_date(20742) == date(2026, 10, 16)


def test_day_index_to_date_rejects_non_int():
    with pytest.raises(lh.RawFormatError):
        lh.day_index_to_date("20742")


def test_to_record_mapping():
    quote = {"d": 20742, "o": 4170.82, "h": 4198.41, "l": 4147.77, "c": 4164.67, "v": 0}
    rec = lh.to_record(quote, "Gold", "_GC", "2026-10-18T12:00:00Z")
    assert rec == {
        "source": "boursorama",
        "symbol": "_GC",
        "instrument_name": "Gold",
        "quote_date": "2026-10-16",
        "raw_day_index": 20742,
        "open_usd": 4170.82,
        "high_usd": 4198.41,
        "low_usd": 4147.77,
        "price_usd": 4164.67,
        "volume": 0,
        "currency": "USD",
        "unit": "troy_ounce",
        "ingested_at": "2026-10-18T12:00:00Z",
    }


def test_to_record_keeps_inconsistent_ohlc():
    # Pas de correction métier : high < low est conservé tel quel.
    quote = {"d": 1, "o": 10, "h": 5, "l": 8, "c": 7, "v": 0}
    rec = lh.to_record(quote, "Gold", "_GC", "x")
    assert (rec["high_usd"], rec["low_usd"]) == (5, 8)


def test_extract_quotes_empty_raises():
    with pytest.raises(lh.RawFormatError, match="vide"):
        lh.extract_quotes({"d": {"Name": "Gold", "SymbolId": "_GC", "QuoteTab": []}})


def test_extract_quotes_missing_key_raises():
    payload = {"d": {"Name": "Gold", "SymbolId": "_GC", "QuoteTab": [{"d": 1, "o": 1}]}}
    with pytest.raises(lh.RawFormatError, match="manquantes"):
        lh.extract_quotes(payload)


def test_run_uses_local_file_without_network(tmp_path, monkeypatch):
    raw = '{"d":{"Name":"Gold","SymbolId":"_GC","Xperiod":0,"QuoteTab":[{"d":20742,"o":1,"h":2,"l":0.5,"c":1.5,"v":0}]}}'
    raw_file = tmp_path / "eod_7300.json"
    raw_file.write_text(raw, encoding="utf-8")
    monkeypatch.setattr(lh, "fetch_raw", lambda: pytest.fail("aucune requête attendue"))

    now = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)
    out = lh.run(now=now, raw_file=raw_file)
    out2 = lh.run(now=now, raw_file=raw_file)  # relance : même fichier, pas de doublon

    assert out == out2 == tmp_path / "gold_boursorama_2026-10-18.jsonl"
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["quote_date"] == "2026-10-16"
    assert raw_file.read_text(encoding="utf-8") == raw  # brut intact
