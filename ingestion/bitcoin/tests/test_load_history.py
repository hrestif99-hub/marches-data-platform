"""Tests unitaires sans réseau : faux site data.binance.vision servi depuis un dict."""

import hashlib
import io
import json
import logging
import zipfile
from datetime import date, datetime, timedelta, timezone

import pytest
import requests

from ingestion.bitcoin import load_history as lh

NOW = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)
RAW_DIR = "raw_binance_vision_2026-10-18"
JSONL_NAME = "bitcoin_binance_vision_2026-10-18.jsonl"
HEADER = (
    "open_time,open,high,low,close,volume,close_time,quote_volume,count,"
    "taker_buy_volume,taker_buy_quote_volume,ignore"
)


URL_U = f"{lh.BASE_URL}/monthly/klines/BTCUSDT/1d/u.zip"
URL_V = f"{lh.BASE_URL}/monthly/klines/BTCUSDT/1d/v.zip"


class FakeResponse:
    def __init__(self, status_code, content=b"", headers=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}


def csv_line(d, price=100.0, unit=None):
    """Bougie 1d du jour d ; unité µs à partir de 2025 (comme Binance), ms avant."""
    start = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())
    unit = unit or ("us" if d >= date(2025, 1, 1) else "ms")
    mult = 1_000_000 if unit == "us" else 1_000
    open_t, close_t = start * mult, (start + 86400) * mult - 1
    return (
        f"{open_t},{price:.8f},{price * 1.1:.8f},{price * 0.9:.8f},{price * 1.05:.8f},"
        f"12.5,{close_t},1250.0,42,6.0,600.0,0"
    )


def make_zip(csv_name, lines, header=False):
    text = "\n".join(([HEADER] if header else []) + lines) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, text)
    return buf.getvalue()


def checksum_for(zip_bytes, zip_name):
    return f"{hashlib.sha256(zip_bytes).hexdigest()}  {zip_name}\n".encode()


def add_unit(site, unit, lines, header=False):
    data = make_zip(unit.zip_name.replace(".zip", ".csv"), lines, header)
    site[unit.url] = data
    site[unit.url + ".CHECKSUM"] = checksum_for(data, unit.zip_name)


def make_site(now=NOW, first=date(2017, 8, 17), skip_days=(), header_from=None):
    """Mensuels 2017-08..mois précédent + quotidiens du mois en cours jusqu'à la veille."""
    site = {}
    today = now.date()
    for unit in lh.monthly_units(today):
        start = date.fromisoformat(unit.period + "-01")
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        days = [d for d in lh.days_between(max(start, first), end) if d not in skip_days]
        header = header_from is not None and start >= header_from
        add_unit(site, unit, [csv_line(d) for d in days], header)
    for unit in lh.daily_units(today.replace(day=1), today - timedelta(days=1)):
        d = date.fromisoformat(unit.period)
        if d not in skip_days:
            add_unit(site, unit, [csv_line(d)])
    return site


def serve(site, calls=None):
    def fake_get(url, headers=None, timeout=None, allow_redirects=True):
        assert allow_redirects is False  # aucune redirection suivie
        if calls is not None:
            calls.append((url, headers, timeout))
        return FakeResponse(200, site[url]) if url in site else FakeResponse(404)

    return fake_get


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch, tmp_path):
    monkeypatch.setattr(lh.time, "sleep", lambda s: None)
    monkeypatch.setattr(lh, "OUTPUT_DIR", tmp_path / "output")


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Tout requests.get non mocké par le test fait échouer le test."""

    def forbidden(*args, **kwargs):
        pytest.fail(f"Appel réseau non mocké : {args!r}")

    monkeypatch.setattr(requests, "get", forbidden)


def run_main_on(monkeypatch, tmp_path, site):
    """main([]) sur le faux site, à la date NOW, sorties dans tmp_path."""
    real_run = lh.run
    monkeypatch.setattr(requests, "get", serve(site))
    monkeypatch.setattr(
        lh, "run", lambda reuse=True: real_run(now=NOW, output_dir=tmp_path, reuse=reuse)
    )
    return lh.main([])


def monthly(period):
    return lh.Unit("monthly", period)


def daily(period):
    return lh.Unit("daily", period)


# --- plan des fichiers ---


def test_plan_months_and_days():
    months = lh.monthly_units(date(2026, 10, 18))
    assert months[0].period == "2017-08" and months[-1].period == "2026-09"
    assert len(months) == 110
    assert months[0].url == (
        "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2017-08.zip"
    )
    assert daily("2026-10-17").url.endswith("/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-10-17.zip")
    assert lh.daily_units(date(2026, 10, 1), date(2026, 9, 30)) == []


# --- parsing ---


@pytest.mark.parametrize("header", [False, True])
def test_parse_csv_with_or_without_header(header):
    unit = monthly("2017-08")
    lines = [csv_line(date(2017, 8, 17), 4261.48), csv_line(date(2017, 8, 18))]
    text = lh.read_csv_from_zip(make_zip("x.csv", lines, header), unit.zip_name)
    rows = lh.parse_csv(text, unit, "raw/x.zip")
    assert [r["quote_date"] for r in rows] == ["2017-08-17", "2017-08-18"]
    assert rows[0]["open"] == 4261.48 and rows[0]["number_of_trades"] == 42


@pytest.mark.parametrize("unit_name", ["ms", "us"])
def test_timestamps_ms_and_us(unit_name):
    d = date(2025, 1, 1)
    rows = lh.parse_csv(csv_line(d, unit=unit_name) + "\n", monthly("2025-01"), "x")
    assert rows[0]["quote_date"] == "2025-01-01"


def test_real_binance_lines_ms_and_us():
    ms = "1502928000000,4261.48000000,4485.39000000,4200.74000000,4285.08000000,795.15037700,1503014399999,3454770.05073206,3427,616.24854100,2678216.40060401,8733.91139481"
    us = "1735689600000000,93576.00000000,95151.15000000,92888.00000000,94591.79000000,10373.32613000,1735775999999999,975444194.13799830,1516556,5347.73648000,502914035.64059070,0"
    assert lh.parse_csv(ms, monthly("2017-08"), "x")[0]["quote_date"] == "2017-08-17"
    assert lh.parse_csv(us, monthly("2025-01"), "x")[0]["quote_date"] == "2025-01-01"


@pytest.mark.parametrize("unit_name", ["ms", "us"])
@pytest.mark.parametrize("offset_s", [1, 3600, 86399])
def test_open_time_not_midnight_utc_raises(unit_name, offset_s):
    # quote_date n'est fiable que si la bougie s'ouvre à minuit UTC : sinon exception, pas un log.
    line = csv_line(date(2025, 1, 2), unit=unit_name).split(",")
    mult = 1_000_000 if unit_name == "us" else 1_000
    line[0] = str(int(line[0]) + offset_s * mult)
    with pytest.raises(lh.RawFormatError, match="n'est pas minuit UTC"):
        lh.parse_csv(",".join(line), monthly("2025-01"), "x")


def test_timestamp_wrong_unit_detected_by_period_check():
    # Un timestamp µs lu comme ms donnerait une date absurde : l'appartenance au mois l'attrape.
    with pytest.raises(lh.RawFormatError, match="hors de la période"):
        lh.parse_csv(csv_line(date(2025, 2, 1)), monthly("2025-01"), "x")
    with pytest.raises(lh.RawFormatError, match="unité inconnue"):
        lh.parse_timestamp("1735689600", "x")


@pytest.mark.parametrize("price", ["nan", "inf", "0", "-1", "abc"])
def test_non_finite_or_non_positive_price_raises(price):
    line = csv_line(date(2017, 8, 17)).split(",")
    line[4] = price  # close
    with pytest.raises(lh.RawFormatError):
        lh.parse_csv(",".join(line), monthly("2017-08"), "x")


def test_ohlc_incoherent_raises():
    line = csv_line(date(2017, 8, 17)).split(",")
    line[2] = "50"  # high < open
    with pytest.raises(lh.RawFormatError, match="OHLC incohérent"):
        lh.parse_csv(",".join(line), monthly("2017-08"), "x")


def test_wrong_column_count_raises():
    with pytest.raises(lh.RawFormatError, match="colonnes"):
        lh.parse_csv("1502928000000,1,2,3", monthly("2017-08"), "x")


def test_bad_checksum_raises():
    data = make_zip("a.csv", [csv_line(date(2017, 8, 17))])
    name = "BTCUSDT-1d-2017-08.zip"
    lh.verify_checksum(data, checksum_for(data, name), name)
    with pytest.raises(lh.RawFormatError, match="sha256 incorrect"):
        lh.verify_checksum(data + b"x", checksum_for(data, name), name)
    with pytest.raises(lh.RawFormatError, match="CHECKSUM inattendu"):
        lh.verify_checksum(data, checksum_for(data, "autre.zip"), name)


# --- contrôles ---


def rows_of(first=date(2017, 8, 17), last=date(2026, 10, 17), skip=()):
    return [{"quote_date": d.isoformat()} for d in lh.days_between(first, last) if d not in skip]


def test_check_rows_ok_with_production_thresholds():
    assert len(lh.check_rows(rows_of(), NOW)) == (date(2026, 10, 17) - date(2017, 8, 17)).days + 1


def test_check_rows_duplicate():
    with pytest.raises(lh.RawFormatError, match="en double"):
        lh.check_rows(rows_of() + [{"quote_date": "2020-01-01"}], NOW)


def test_check_rows_wrong_first_date():
    with pytest.raises(lh.RawFormatError, match="Première date 2017-08-18"):
        lh.check_rows(rows_of(first=date(2017, 8, 18)), NOW)


def test_check_rows_stale():
    lh.check_rows(rows_of(last=date(2026, 10, 15)), NOW)  # 3 jours : toléré
    with pytest.raises(lh.RawFormatError, match="vieille de 4 jours"):
        lh.check_rows(rows_of(last=date(2026, 10, 14)), NOW)


def test_check_rows_small_gap_logged(caplog):
    with caplog.at_level(logging.WARNING):
        lh.check_rows(rows_of(skip={date(2020, 3, 12)}), NOW)
    assert "1 jour(s) sans bougie : 2020-03-12" in caplog.text


def test_check_rows_large_gap_raises():
    skip = set(lh.days_between(date(2020, 3, 1), date(2020, 3, 8)))
    with pytest.raises(lh.RawFormatError, match="8 jours manquants"):
        lh.check_rows(rows_of(skip=skip), NOW)


# --- run complet ---


def test_run_end_to_end_idempotent(tmp_path, monkeypatch):
    site = make_site(header_from=date(2022, 1, 1))  # en-têtes sur une partie des fichiers
    calls = []
    monkeypatch.setattr(requests, "get", serve(site, calls))

    out = lh.run(now=NOW, output_dir=tmp_path, reuse=False)
    out2 = lh.run(now=NOW, output_dir=tmp_path, reuse=False)
    assert out == out2 == tmp_path / JSONL_NAME
    assert len(calls) == 2 * (110 + 17) * 2  # zip + CHECKSUM, deux runs
    assert all(h["User-Agent"] == "marches-data-platform/0.1" and t == lh.TIMEOUT_S for _, h, t in calls)
    assert all(u.startswith(lh.BASE_URL + "/") for u, _, _ in calls)

    records = [json.loads(x) for x in out.read_text(encoding="utf-8").splitlines()]
    assert records[0]["quote_date"] == "2017-08-17" and records[-1]["quote_date"] == "2026-10-17"
    assert len(records) == (date(2026, 10, 17) - date(2017, 8, 17)).days + 1
    assert records[0] == {
        "source": "binance_vision", "symbol": "BTCUSDT", "quote_date": "2017-08-17",
        "open_usdt": 100.0, "high_usdt": 110.0, "low_usdt": 90.0, "close_usdt": 105.0,
        "base_currency": "BTC", "quote_currency": "USDT", "volume_btc": 12.5,
        "quote_volume_usdt": 1250.0, "number_of_trades": 42,
        "raw_file": f"{RAW_DIR}/BTCUSDT-1d-2017-08.zip", "ingested_at": "2026-10-18T12:00:00Z",
    }
    raw_dir = tmp_path / RAW_DIR
    assert len(list(raw_dir.iterdir())) == 2 * (110 + 17)
    unit = monthly("2025-03")
    assert (raw_dir / unit.zip_name).read_bytes() == site[unit.url]  # brut intact
    assert not (tmp_path / f"{RAW_DIR}.tmp").exists()


def test_404_at_end_of_range_is_tolerated(tmp_path, monkeypatch, caplog):
    site = make_site()
    u = daily("2026-10-17")
    del site[u.url], site[u.url + ".CHECKSUM"]
    monkeypatch.setattr(requests, "get", serve(site))
    with caplog.at_level(logging.INFO):
        out = lh.run(now=NOW, output_dir=tmp_path, reuse=False)
    last = json.loads(out.read_text(encoding="utf-8").splitlines()[-1])
    assert last["quote_date"] == "2026-10-16"
    assert "Pas encore publiés (fin de plage) : 2026-10-17" in caplog.text


@pytest.mark.parametrize("missing", [monthly("2020-05"), daily("2026-10-10")])
def test_404_in_middle_of_range_raises(tmp_path, monkeypatch, missing):
    site = make_site()
    del site[missing.url]
    monkeypatch.setattr(requests, "get", serve(site))
    with pytest.raises(lh.FetchError, match="au milieu de la plage"):
        lh.run(now=NOW, output_dir=tmp_path, reuse=False)


def test_last_month_not_published_falls_back_to_daily(tmp_path, monkeypatch):
    now = datetime(2026, 10, 2, 8, 0, tzinfo=timezone.utc)
    site = make_site(now=now)
    sep = monthly("2026-09")
    del site[sep.url], site[sep.url + ".CHECKSUM"]
    for unit in lh.daily_units(date(2026, 9, 1), date(2026, 9, 30)):
        add_unit(site, unit, [csv_line(date.fromisoformat(unit.period))])
    monkeypatch.setattr(requests, "get", serve(site))
    out = lh.run(now=now, output_dir=tmp_path, reuse=False)
    lines = [json.loads(x) for x in out.read_text(encoding="utf-8").splitlines()]
    assert lines[-1]["quote_date"] == "2026-10-01"
    assert lines[-2]["raw_file"].endswith("BTCUSDT-1d-2026-09-30.zip")


def test_bad_checksum_in_run_raises(tmp_path, monkeypatch):
    site = make_site()
    u = monthly("2019-01")
    site[u.url + ".CHECKSUM"] = b"0" * 64 + b"  " + u.zip_name.encode()
    monkeypatch.setattr(requests, "get", serve(site))
    with pytest.raises(lh.RawFormatError, match="sha256 incorrect"):
        lh.run(now=NOW, output_dir=tmp_path, reuse=False)
    assert not (tmp_path / RAW_DIR).exists() and not (tmp_path / JSONL_NAME).exists()


def test_failure_replaces_nothing(tmp_path, monkeypatch):
    site = make_site()
    monkeypatch.setattr(requests, "get", serve(site))
    lh.run(now=NOW, output_dir=tmp_path, reuse=False)
    raw_dir = tmp_path / RAW_DIR
    before = {p.name: p.read_bytes() for p in raw_dir.iterdir()}
    jsonl_before = (tmp_path / JSONL_NAME).read_bytes()

    broken = dict(site)
    u = monthly("2023-06")
    broken[u.url] = make_zip("x.csv", [csv_line(date(2023, 6, 1)), csv_line(date(2023, 6, 1))])
    broken[u.url + ".CHECKSUM"] = checksum_for(broken[u.url], u.zip_name)  # doublon
    monkeypatch.setattr(requests, "get", serve(broken))
    with pytest.raises(lh.RawFormatError, match="en double"):
        lh.run(now=NOW, output_dir=tmp_path, reuse=False)

    assert {p.name: p.read_bytes() for p in raw_dir.iterdir()} == before
    assert (tmp_path / JSONL_NAME).read_bytes() == jsonl_before


def test_rerun_removes_stale_files(tmp_path, monkeypatch):
    monkeypatch.setattr(requests, "get", serve(make_site()))
    lh.run(now=NOW, output_dir=tmp_path, reuse=False)
    raw_dir = tmp_path / RAW_DIR
    (raw_dir / "BTCUSDT-1d-2026-10-18.zip").write_bytes(b"obsolete")
    lh.run(now=NOW, output_dir=tmp_path, reuse=False)
    assert not (raw_dir / "BTCUSDT-1d-2026-10-18.zip").exists()
    assert len(list(raw_dir.iterdir())) == 2 * (110 + 17)


def test_reuse_local_monthly_zips(tmp_path, monkeypatch):
    site = make_site()
    monkeypatch.setattr(requests, "get", serve(site))
    yesterday = NOW - timedelta(days=1)
    first_out = lh.run(now=yesterday, output_dir=tmp_path, reuse=False)

    # Copie locale corrompue d'un mois : elle doit être re-téléchargée.
    old_dir = tmp_path / "raw_binance_vision_2026-10-17"
    (old_dir / monthly("2018-02").zip_name).write_bytes(b"corrompu")

    calls = []
    monkeypatch.setattr(requests, "get", serve(site, calls))
    out = lh.run(now=NOW, output_dir=tmp_path)
    urls = [u for u, _, _ in calls]
    assert monthly("2018-02").url in urls
    assert sum("/monthly/" in u for u in urls) == 2  # zip + CHECKSUM du seul mois corrompu
    assert sum("/daily/" in u for u in urls) == 2 * 17
    assert len(out.read_text(encoding="utf-8").splitlines()) == len(
        first_out.read_text(encoding="utf-8").splitlines()
    ) + 1
    assert (tmp_path / RAW_DIR / monthly("2018-02").zip_name).read_bytes() == site[monthly("2018-02").url]


def test_safe_rmtree_refuses_other_dirs(tmp_path):
    other = tmp_path / "important"
    other.mkdir()
    with pytest.raises(RuntimeError, match="refusée"):
        lh._safe_rmtree(other, tmp_path)
    with pytest.raises(RuntimeError, match="refusée"):
        lh._safe_rmtree(tmp_path / "sub" / RAW_DIR, tmp_path)
    assert other.exists()


# --- réseau (simulé) ---


def test_fetcher_retries_then_succeeds_and_pauses(monkeypatch):
    sleeps = []
    monkeypatch.setattr(lh.time, "sleep", sleeps.append)
    responses = [FakeResponse(429), FakeResponse(503), FakeResponse(200, b"ok"), FakeResponse(200, b"ok2")]
    monkeypatch.setattr(requests, "get", lambda url, **k: responses.pop(0))
    f = lh.Fetcher()
    assert f.get(URL_U) == b"ok"
    assert f.get(URL_V) == b"ok2"
    assert f.requests_sent == 4
    assert lh.BACKOFF_S in sleeps and 2 * lh.BACKOFF_S in sleeps
    # Pause entre deux requêtes rapprochées : au plus PAUSE_S, jamais négative.
    assert any(0 < s <= lh.PAUSE_S for s in sleeps)


def test_fetcher_404_returns_none_without_retry(monkeypatch):
    calls = []
    monkeypatch.setattr(requests, "get", lambda url, **k: calls.append(url) or FakeResponse(404))
    assert lh.Fetcher().get(URL_U) is None
    assert len(calls) == 1


def test_fetcher_403_no_retry(monkeypatch):
    calls = []
    monkeypatch.setattr(requests, "get", lambda url, **k: calls.append(url) or FakeResponse(403))
    with pytest.raises(lh.FetchError, match="HTTP 403"):
        lh.Fetcher().get(URL_U)
    assert len(calls) == 1


def test_fetcher_network_error_limited_retries(monkeypatch):
    calls = []

    def boom(url, **k):
        calls.append(url)
        raise requests.ConnectionError("coupure")

    monkeypatch.setattr(requests, "get", boom)
    with pytest.raises(lh.FetchError, match="3 tentatives"):
        lh.Fetcher().get(URL_U)
    assert len(calls) == lh.MAX_ATTEMPTS


def test_main_exit_codes(monkeypatch):
    def raise_(exc):
        def f(*a, **k):
            raise exc
        return f

    monkeypatch.setattr(lh, "run", raise_(lh.FetchError("x")))
    assert lh.main([]) == 2
    monkeypatch.setattr(lh, "run", raise_(lh.RawFormatError("x")))
    assert lh.main([]) == 3
    monkeypatch.setattr(lh, "run", raise_(RuntimeError("x")))
    assert lh.main([]) == 1
    seen = {}
    monkeypatch.setattr(lh, "run", lambda reuse=True: seen.setdefault("reuse", reuse))
    assert lh.main(["--full"]) == 0
    assert seen["reuse"] is False


def test_main_network_failure_exit_2_creates_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(lh, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(requests, "get", lambda url, **k: FakeResponse(500))
    assert lh.main([]) == 2
    # Seul reste : le dossier de staging .tmp (vidé au prochain run), rien de publié.
    assert all(p.name.endswith(".tmp") for p in tmp_path.iterdir())
    assert not list(tmp_path.glob("*.jsonl"))


# --- relecture : redirections, pause, en-tête, 404 partiel, réutilisation, seuils, corruption ---


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_redirect_refused_exit_2(tmp_path, monkeypatch, caplog, status):
    calls = []

    def redirect(url, **kwargs):
        assert kwargs["allow_redirects"] is False
        calls.append(url)
        return FakeResponse(status, headers={"Location": "https://ailleurs.example/x.zip"})

    real_run = lh.run
    monkeypatch.setattr(requests, "get", redirect)
    monkeypatch.setattr(lh, "run", lambda reuse=True: real_run(now=NOW, output_dir=tmp_path, reuse=reuse))
    with caplog.at_level(logging.ERROR):
        assert lh.main([]) == 2
    assert len(calls) == 1  # pas de relance, pas de suivi
    assert f"Redirection HTTP {status} refusée" in caplog.text


def test_fetcher_refuses_url_outside_base(monkeypatch):
    with pytest.raises(lh.FetchError, match="hors de"):
        lh.Fetcher().get("https://api.binance.com/api/v3/klines")


def test_pause_measured_after_response(monkeypatch):
    """Réponse lente (5 s) : la requête suivante attend quand même PAUSE_S complet."""
    clock = [100.0]
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)
        clock[0] += s

    def slow_get(url, **kwargs):
        clock[0] += 5.0
        return FakeResponse(200, b"ok")

    monkeypatch.setattr(lh.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(lh.time, "sleep", fake_sleep)
    monkeypatch.setattr(requests, "get", slow_get)
    f = lh.Fetcher()
    f.get(URL_U)
    f.get(URL_V)
    assert sleeps == [pytest.approx(lh.PAUSE_S)]


def test_pause_after_network_error(monkeypatch):
    clock = [0.0]
    sleeps = []
    monkeypatch.setattr(lh.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(lh.time, "sleep", lambda s: (sleeps.append(s), clock.__setitem__(0, clock[0] + s)))
    responses = [requests.ConnectionError("x"), FakeResponse(200, b"ok")]

    def get(url, **kwargs):
        clock[0] += 30.0  # timeout long
        r = responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(requests, "get", get)
    assert lh.Fetcher().get(URL_U) == b"ok"
    assert sleeps == [lh.BACKOFF_S]  # backoff 5 s >= pause : pas de pause supplémentaire


@pytest.mark.parametrize("first_line", ["timestamp,open,high", "date,o,h,l,c", "<html>"])
def test_non_open_time_header_raises(first_line):
    text = first_line + "\n" + csv_line(date(2017, 8, 17)) + "\n"
    with pytest.raises(lh.RawFormatError, match="première ligne inattendue"):
        lh.parse_csv(text, monthly("2017-08"), "x")


def test_open_time_header_accepted():
    text = HEADER + "\n" + csv_line(date(2017, 8, 17)) + "\n"
    assert lh.parse_csv(text, monthly("2017-08"), "x")[0]["quote_date"] == "2017-08-17"


def test_checksum_present_zip_404_at_end_tolerated(tmp_path, monkeypatch):
    site = make_site()
    del site[daily("2026-10-17").url]  # CHECKSUM publié, zip pas encore
    monkeypatch.setattr(requests, "get", serve(site))
    out = lh.run(now=NOW, output_dir=tmp_path, reuse=False)
    assert json.loads(out.read_text(encoding="utf-8").splitlines()[-1])["quote_date"] == "2026-10-16"
    assert not (tmp_path / RAW_DIR / "BTCUSDT-1d-2026-10-17.zip.CHECKSUM").exists()


def test_checksum_present_zip_404_in_middle_exit_2(tmp_path, monkeypatch):
    site = make_site()
    del site[daily("2026-10-05").url]
    assert run_main_on(monkeypatch, tmp_path, site) == 2
    assert not (tmp_path / RAW_DIR).exists()


def test_reuse_same_day_raw_dir(tmp_path, monkeypatch):
    site = make_site()
    monkeypatch.setattr(requests, "get", serve(site))
    first = lh.run(now=NOW, output_dir=tmp_path).read_bytes()

    calls = []
    monkeypatch.setattr(requests, "get", serve(site, calls))
    out = lh.run(now=NOW, output_dir=tmp_path)  # source de réutilisation = dossier du jour
    assert all("/daily/" in u for u, _, _ in calls) and len(calls) == 2 * 17
    assert out.read_bytes() == first
    raw_dir = tmp_path / RAW_DIR
    assert len(list(raw_dir.iterdir())) == 2 * (110 + 17)
    for unit in (monthly("2017-08"), monthly("2026-09")):
        assert (raw_dir / unit.zip_name).read_bytes() == site[unit.url]
    assert not (tmp_path / f"{RAW_DIR}.tmp").exists()


def test_check_rows_missing_days_threshold_exact():
    seven = set(lh.days_between(date(2020, 3, 1), date(2020, 3, 7)))
    lh.check_rows(rows_of(skip=seven), NOW)  # 7 : accepté
    with pytest.raises(lh.RawFormatError, match="8 jours manquants"):
        lh.check_rows(rows_of(skip=seven | {date(2021, 1, 1)}), NOW)


def _corrupt_deflate(csv_name, text):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, text)
    data = bytearray(buf.getvalue())
    start = 30 + len(csv_name)  # en-tête local + nom : début du flux compressé
    for i in range(start, start + 8):
        data[i] ^= 0xFF
    return bytes(data)


def _stored_zip(csv_name, payload):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr(csv_name, payload)
    return buf.getvalue()


@pytest.mark.parametrize(
    "make_bad",
    [
        lambda name: b"PK\x03\x04 tronque",  # zip tronqué
        lambda name: b"pas un zip du tout",
        lambda name: _corrupt_deflate(name, "\n".join(csv_line(date(2019, 1, 1)) for _ in range(50))),
        lambda name: _stored_zip(name, b"\xff\xfe\xfa invalide utf-8"),  # CSV non UTF-8
        lambda name: _stored_zip(name, b"1546300800000,1,2\n"),  # CSV à 3 colonnes
    ],
    ids=["tronque", "non-zip", "deflate-corrompu", "csv-non-utf8", "csv-colonnes"],
)
def test_corrupted_zip_or_csv_exit_3(tmp_path, monkeypatch, make_bad):
    site = make_site()
    u = monthly("2019-01")
    site[u.url] = make_bad("BTCUSDT-1d-2019-01.csv")
    site[u.url + ".CHECKSUM"] = checksum_for(site[u.url], u.zip_name)  # checksum cohérent
    assert run_main_on(monkeypatch, tmp_path, site) == 3
    assert not (tmp_path / RAW_DIR).exists() and not (tmp_path / JSONL_NAME).exists()
