"""Tests unitaires sans réseau : parsing, pagination simulée, clé absente, clé jamais loggée."""

import json
import logging
from datetime import datetime, timezone

import pytest
import requests

from ingestion.oil import load_history as lh

NOW = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)
FAKE_KEY = "TESTKEY-not-a-real-key-123"


class FakeResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


def row(period, series, value="75.89"):
    names = {"RBRTE": "UK Brent Crude Oil", "RWTC": "WTI Crude Oil"}
    return {
        "period": period,
        "duoarea": "NUS",
        "product-name": names.get(series, "?"),
        "series": series,
        "series-description": f"{series} Spot Price FOB (Dollars per Barrel)",
        "value": value,
        "units": "$/BBL",
    }


def page_text(total, data, offset, echo_key=True):
    """Page au format EIA v2 ; l'API renvoie la clé en écho dans request.params."""
    params = {"frequency": "daily", "offset": offset, "length": lh.PAGE_LENGTH}
    if echo_key:
        params["api_key"] = FAKE_KEY
    return json.dumps(
        {
            "response": {"total": str(total), "frequency": "daily", "data": data},
            "request": {"command": "/v2/petroleum/pri/spt/data/", "params": params},
            "apiVersion": "2.1.8",
        }
    )


@pytest.fixture(autouse=True)
def isolate_state(monkeypatch, tmp_path):
    """Registre des clés vidé, sorties vers tmp_path, niveau urllib3 restauré."""
    monkeypatch.setattr(lh, "_LOADED_KEYS", set())
    monkeypatch.setattr(lh, "OUTPUT_DIR", tmp_path / "output")
    urllib3_logger = logging.getLogger("urllib3")
    level = urllib3_logger.level
    yield
    urllib3_logger.setLevel(level)


@pytest.fixture
def with_key(monkeypatch, tmp_path):
    monkeypatch.setattr(lh, "ENV_FILE", tmp_path / "absent.env")
    monkeypatch.setenv(lh.API_KEY_ENV, FAKE_KEY)
    monkeypatch.setattr(lh.time, "sleep", lambda s: None)


@pytest.fixture
def without_key(monkeypatch, tmp_path):
    monkeypatch.setattr(lh, "ENV_FILE", tmp_path / "absent.env")
    monkeypatch.delenv(lh.API_KEY_ENV, raising=False)


def fake_paginated_get(pages, calls):
    """pages : {offset: texte}. Vérifie que la clé passe par params, pas dans l'URL."""

    def fake_get(url, params=None, headers=None, timeout=None):
        assert FAKE_KEY not in url
        p = dict(params)
        assert p["api_key"] == FAKE_KEY
        calls.append(params)
        return FakeResponse(200, pages[int(p["offset"])])

    return fake_get


def test_to_record_mapping():
    rec = lh.to_record(row("2026-10-16", "RBRTE", "61.32"), "2026-10-18T12:00:00Z")
    assert rec == {
        "source": "eia",
        "series_id": "RBRTE",
        "product": "brent",
        "product_name": "UK Brent Crude Oil",
        "series_description": "RBRTE Spot Price FOB (Dollars per Barrel)",
        "quote_date": "2026-10-16",
        "price_usd": 61.32,
        "currency": "USD",
        "unit": "barrel",
        "raw_units": "$/BBL",
        "ingested_at": "2026-10-18T12:00:00Z",
    }


def test_to_record_null_value_returns_none():
    assert lh.to_record(row("2026-10-16", "RWTC", None), "x") is None


@pytest.mark.parametrize(
    "bad",
    [
        {**row("2026-10-16", "RBRTE"), "units": "$/GAL"},
        row("2026-10-16", "EER_EPMRU_PF4_Y35NY_DPG"),
        row("2026-10-16", "RBRTE", "n/a"),
        {"period": "2026-10-16"},
    ],
)
def test_to_record_rejects_unexpected(bad):
    with pytest.raises(lh.RawFormatError):
        lh.to_record(bad, "x")


def test_build_records_rejects_duplicates():
    with pytest.raises(lh.RawFormatError, match="Doublon"):
        lh.build_records([row("2026-10-16", "RBRTE"), row("2026-10-16", "RBRTE")], "x")


def test_parse_page_errors():
    with pytest.raises(lh.RawFormatError, match="JSON invalide"):
        lh.parse_page("not json")
    with pytest.raises(lh.RawFormatError, match="response"):
        lh.parse_page('{"error": "x"}')
    with pytest.raises(lh.RawFormatError, match="total"):
        lh.parse_page('{"response": {"total": "abc", "data": []}}')


def test_redact_masks_key_and_url_param():
    text = f"https://api.eia.gov/v2/x?api_key={FAKE_KEY}&frequency=daily"
    assert FAKE_KEY not in lh.redact(text, FAKE_KEY)
    assert "api_key=***REDACTED***&frequency" in lh.redact(text, "")


def test_missing_key_no_network(without_key, monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("aucune requête attendue"))
    with pytest.raises(lh.MissingApiKeyError, match="EIA_API_KEY"):
        lh.run(now=NOW, output_dir=tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_main_missing_key_exit_code(without_key, monkeypatch, caplog):
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("aucune requête attendue"))
    with caplog.at_level(logging.ERROR):
        assert lh.main() == 4
    assert "EIA_API_KEY absente" in caplog.text


def test_key_loaded_from_env_file(monkeypatch, tmp_path):
    env = tmp_path / ".env"
    env.write_text(f"{lh.API_KEY_ENV}={FAKE_KEY}\n", encoding="utf-8")
    monkeypatch.delenv(lh.API_KEY_ENV, raising=False)
    assert lh.get_api_key(env) == FAKE_KEY
    monkeypatch.delenv(lh.API_KEY_ENV, raising=False)  # ne pas polluer les autres tests


def test_run_paginates_until_total(with_key, monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(lh, "PAGE_LENGTH", 2)
    pages = {
        0: page_text(5, [row("1987-05-20", "RBRTE", "18.63"), row("1987-05-20", "RWTC", None)], 0),
        2: page_text(5, [row("1987-05-21", "RBRTE", "18.45"), row("1987-05-21", "RWTC", "19.1")], 2),
        4: page_text(5, [row("2026-10-16", "RWTC", "57.5")], 4),
    }
    calls = []
    monkeypatch.setattr(requests, "get", fake_paginated_get(pages, calls))

    with caplog.at_level(logging.DEBUG):
        out = lh.run(now=NOW, output_dir=tmp_path)
        out2 = lh.run(now=NOW, output_dir=tmp_path)  # relance : mêmes fichiers écrasés

    assert out == out2 == tmp_path / "oil_eia_2026-10-18.jsonl"
    assert [dict(c)["offset"] for c in calls] == ["0", "2", "4"] * 2
    first = dict(calls[0])
    assert first["frequency"] == "daily" and first["data[0]"] == "value"
    assert [v for k, v in calls[0] if k == "facets[series][]"] == ["RBRTE", "RWTC"]

    records = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 4  # 5 lignes - 1 valeur nulle
    assert {r["series_id"] for r in records} == {"RBRTE", "RWTC"}
    assert all(r["currency"] == "USD" and r["unit"] == "barrel" for r in records)

    raw_dir = tmp_path / "raw_eia_spt_2026-10-18"
    raw_files = sorted(p.name for p in raw_dir.iterdir())
    assert raw_files == [
        "page_0001_offset_0.json", "page_0002_offset_2.json", "page_0003_offset_4.json"
    ]
    for name, offset in zip(raw_files, (0, 2, 4)):
        stored = (raw_dir / name).read_text(encoding="utf-8")
        assert FAKE_KEY not in stored
        # Brut intact à l'exception du masquage de la clé renvoyée en écho.
        assert stored == pages[offset].replace(FAKE_KEY, lh.REDACTED)

    assert FAKE_KEY not in caplog.text
    assert "offset=0" in caplog.text  # URL loggée, sans la clé


def test_empty_page_before_total_raises(with_key, monkeypatch, tmp_path):
    monkeypatch.setattr(lh, "PAGE_LENGTH", 2)
    pages = {0: page_text(10, [row("2026-10-15", "RBRTE"), row("2026-10-15", "RWTC")], 0),
             2: page_text(10, [], 2)}
    monkeypatch.setattr(requests, "get", fake_paginated_get(pages, []))
    with pytest.raises(lh.RawFormatError, match="Page vide"):
        lh.run(now=NOW, output_dir=tmp_path)


def test_network_error_never_leaks_key(with_key, monkeypatch, caplog):
    def fake_get(url, params=None, **kwargs):
        # requests inclut l'URL complète (avec la clé) dans ses messages d'exception.
        raise requests.ConnectionError(
            f"Max retries exceeded with url: /v2/petroleum/pri/spt/data/?api_key={FAKE_KEY}&offset=0"
        )

    monkeypatch.setattr(requests, "get", fake_get)
    with caplog.at_level(logging.DEBUG):
        assert lh.main() == 2
    assert FAKE_KEY not in caplog.text
    assert lh.REDACTED in caplog.text


def test_invalid_key_403(with_key, monkeypatch, caplog):
    body = json.dumps({"error": {"code": "API_KEY_INVALID", "message": f"bad key {FAKE_KEY}"}})
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return FakeResponse(403, body)

    monkeypatch.setattr(requests, "get", fake_get)
    with caplog.at_level(logging.DEBUG):
        assert lh.main() == 5
    assert len(calls) == 1  # pas de relance sur 403
    assert "API_KEY_INVALID" in caplog.text
    assert FAKE_KEY not in caplog.text


def test_retries_on_429_then_succeeds(with_key, monkeypatch):
    responses = [FakeResponse(429, "slow down"), FakeResponse(200, page_text(0, [], 0))]
    monkeypatch.setattr(requests, "get", lambda url, **k: responses.pop(0))
    assert lh.fetch_page(FAKE_KEY, 0) == page_text(0, [], 0)


def test_unexpected_exception_is_redacted(with_key, monkeypatch, caplog):
    def boom(*a, **k):
        lh.get_api_key()  # comme run() : la clé chargée est celle qu'on masque
        raise RuntimeError(f"unexpected with {FAKE_KEY}")

    monkeypatch.setattr(lh, "run", boom)
    with caplog.at_level(logging.DEBUG):
        assert lh.main() == 1
    assert FAKE_KEY not in caplog.text


# --- Relecture : total instable, pages obsolètes, logs DEBUG, clé avec espace final ---


def test_total_changes_between_pages_raises(with_key, monkeypatch, tmp_path):
    monkeypatch.setattr(lh, "PAGE_LENGTH", 2)
    pages = {0: page_text(5, [row("2026-10-14", "RBRTE"), row("2026-10-14", "RWTC")], 0),
             2: page_text(6, [row("2026-10-15", "RBRTE"), row("2026-10-15", "RWTC")], 2)}
    monkeypatch.setattr(requests, "get", fake_paginated_get(pages, []))
    with pytest.raises(lh.RawFormatError, match="relancez"):
        lh.run(now=NOW, output_dir=tmp_path)
    assert not (tmp_path / "raw_eia_spt_2026-10-18").exists()  # rien de publié
    assert not (tmp_path / "oil_eia_2026-10-18.jsonl").exists()


def test_row_count_must_match_total(with_key, monkeypatch, tmp_path):
    monkeypatch.setattr(lh, "PAGE_LENGTH", 2)
    data = [row("2026-10-14", "RBRTE"), row("2026-10-14", "RWTC"), row("2026-10-15", "RBRTE")]
    monkeypatch.setattr(requests, "get", fake_paginated_get({0: page_text(2, data, 0)}, []))
    with pytest.raises(lh.RawFormatError, match="3 lignes reçues pour response.total=2"):
        lh.run(now=NOW, output_dir=tmp_path)


def _pages(n_pages):
    """n_pages pages de 2 lignes (PAGE_LENGTH=2), total = 2 * n_pages."""
    total = 2 * n_pages
    return {
        2 * i: page_text(
            total, [row(f"2026-10-{10 + i:02d}", "RBRTE"), row(f"2026-10-{10 + i:02d}", "RWTC")], 2 * i
        )
        for i in range(n_pages)
    }


def test_rerun_with_fewer_pages_removes_stale_pages(with_key, monkeypatch, tmp_path):
    monkeypatch.setattr(lh, "PAGE_LENGTH", 2)
    raw_dir = tmp_path / "raw_eia_spt_2026-10-18"

    monkeypatch.setattr(requests, "get", fake_paginated_get(_pages(3), []))
    lh.run(now=NOW, output_dir=tmp_path)
    assert len(list(raw_dir.iterdir())) == 3

    monkeypatch.setattr(requests, "get", fake_paginated_get(_pages(2), []))
    lh.run(now=NOW, output_dir=tmp_path)
    assert sorted(p.name for p in raw_dir.iterdir()) == [
        "page_0001_offset_0.json", "page_0002_offset_2.json"
    ]
    assert not (tmp_path / "raw_eia_spt_2026-10-18.tmp").exists()
    lines = (tmp_path / "oil_eia_2026-10-18.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4


def test_interrupted_run_keeps_previous_pages_intact(with_key, monkeypatch, tmp_path):
    monkeypatch.setattr(lh, "PAGE_LENGTH", 2)
    raw_dir = tmp_path / "raw_eia_spt_2026-10-18"
    monkeypatch.setattr(requests, "get", fake_paginated_get(_pages(3), []))
    lh.run(now=NOW, output_dir=tmp_path)
    before = {p.name: p.read_text(encoding="utf-8") for p in raw_dir.iterdir()}

    good = fake_paginated_get(_pages(2), [])

    def flaky_get(url, params=None, **kwargs):
        if dict(params)["offset"] == "2":
            raise requests.ConnectionError("coupure")
        return good(url, params=params, **kwargs)

    monkeypatch.setattr(requests, "get", flaky_get)
    with pytest.raises(lh.FetchError):
        lh.run(now=NOW, output_dir=tmp_path)
    after = {p.name: p.read_text(encoding="utf-8") for p in raw_dir.iterdir()}
    assert after == before  # aucun mélange ancien / nouveau

    # Le run suivant repart d'un dossier temporaire vide et publie un jeu cohérent.
    monkeypatch.setattr(requests, "get", fake_paginated_get(_pages(2), []))
    lh.run(now=NOW, output_dir=tmp_path)
    assert len(list(raw_dir.iterdir())) == 2
    assert not (tmp_path / "raw_eia_spt_2026-10-18.tmp").exists()


def test_safe_rmtree_refuses_other_dirs(tmp_path):
    other = tmp_path / "important"
    other.mkdir()
    with pytest.raises(RuntimeError, match="refusée"):
        lh._safe_rmtree(other, tmp_path)
    with pytest.raises(RuntimeError, match="refusée"):
        lh._safe_rmtree(tmp_path / "sub" / "raw_eia_spt_2026-10-18", tmp_path)
    assert other.exists()


def _urllib3_like_debug(url, params=None, **kwargs):
    """Imite le log DEBUG d'urllib3 (ligne de requête complète, donc avec la clé)."""
    query = "&".join(f"{k}={v}" for k, v in params)
    logging.getLogger("urllib3.connectionpool").debug(
        '%s://%s:%s "%s %s %s" %s %s', "https", "api.eia.gov", 443, "GET",
        f"/v2/petroleum/pri/spt/data/?{query}", "HTTP/1.1", 200, None,
    )


def test_key_never_logged_with_root_debug(with_key, monkeypatch, caplog):
    pages = {0: page_text(1, [row("2026-10-16", "RBRTE")], 0)}
    inner = fake_paginated_get(pages, [])

    def fake_get(url, params=None, **kwargs):
        _urllib3_like_debug(url, params=params)
        return inner(url, params=params, **kwargs)

    monkeypatch.setattr(requests, "get", fake_get)
    with caplog.at_level(logging.DEBUG):
        assert lh.main() == 0
    assert logging.getLogger("urllib3").level == logging.WARNING
    assert FAKE_KEY not in caplog.text


def test_redacting_filter_alone_masks_urllib3_debug(with_key, caplog):
    with caplog.at_level(logging.DEBUG):
        lh.get_api_key()
        lh.install_log_redaction()
        logging.getLogger("urllib3").setLevel(logging.DEBUG)  # filtre seul, sans le WARNING
        _urllib3_like_debug("x", params=[("api_key", FAKE_KEY), ("offset", "0")])
    assert "urllib3.connectionpool" in caplog.text
    assert FAKE_KEY not in caplog.text
    assert lh.REDACTED in caplog.text


def test_key_with_trailing_space_is_masked(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(lh, "ENV_FILE", tmp_path / "absent.env")
    monkeypatch.setenv(lh.API_KEY_ENV, FAKE_KEY + "  ")  # espaces finaux dans .env/env
    seen = {}

    def boom(api_key, raw_dir):
        seen["key"] = api_key
        raise RuntimeError(f"erreur imprévue avec {api_key}")

    monkeypatch.setattr(lh, "fetch_all_pages", boom)
    with caplog.at_level(logging.DEBUG):
        assert lh.main() == 1
    assert seen["key"] == FAKE_KEY  # clé envoyée = clé après strip
    assert FAKE_KEY not in caplog.text
    assert lh.REDACTED in caplog.text
