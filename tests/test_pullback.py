"""Pullback Watch rule and ledger (pullback/scan.py) on synthetic prices — no network."""
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_spec = importlib.util.spec_from_file_location(
    "pullback_scan", Path(__file__).resolve().parents[1] / "pullback" / "scan.py")
pb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pb)

N = 320
DROP_AT = 300          # the five-session fall ends here (signal day)


def _frames(daily_after, drift=0.0008, fall=-0.007):
    """Every fund: a steady uptrend, a five-session fall ending at DROP_AT, then `daily_after`."""
    r = np.full(N, drift)
    r[DROP_AT - 4:DROP_AT + 1] = fall                     # -3.4% over five sessions; four are not enough
    r[DROP_AT + 1:DROP_AT + 1 + len(daily_after)] = daily_after
    close = pd.Series(100 * np.exp(np.cumsum(r)), index=pd.bdate_range("2025-06-02", periods=N))
    c = pd.DataFrame({t: close for t in pb.FUNDS})
    o = c.shift(1).fillna(c.iloc[0])                      # open = previous close
    return o, c, c.copy()


@pytest.fixture(autouse=True)
def _ledger_window(monkeypatch):
    o, c, _ = _frames(np.array([]))
    monkeypatch.setattr(pb, "LEDGER_START", str(c.index[DROP_AT - 20].date()))
    monkeypatch.setattr(pb, "LIVE_FROM", str(c.index[DROP_AT - 20].date()))


def test_signal_buys_next_open_and_sells_five_sessions_after_the_signal():
    o, c, a = _frames(np.full(10, 0.004))
    out = pb.compute(o, c, a)
    tr = [t for t in out["trades"] if t["ticker"] == "SPY"]
    assert len(tr) == 1 and tr[0]["status"] == "CLOSED"
    assert tr[0]["signal"] == str(c.index[DROP_AT].date())
    assert tr[0]["entry"] == str(c.index[DROP_AT + 1].date())
    assert tr[0]["exit"] == str(c.index[DROP_AT + pb.HOLD].date())
    expect = (c["SPY"].iloc[DROP_AT + pb.HOLD] / o["SPY"].iloc[DROP_AT + 1] - 1 - 2 * pb.COST_SIDE) * 100
    assert tr[0]["ret"] == pytest.approx(expect, abs=0.01) and tr[0]["ret"] > 0
    assert out["summary"]["all"]["n"] == len(pb.FUNDS)


def test_no_signal_below_the_200_session_average():
    o, c, a = _frames(np.full(10, 0.004), drift=-0.0015)   # a downtrend: the same fall must not signal
    out = pb.compute(o, c, a)
    assert out["trades"] == []
    assert all(f["status"] == "OFF" for f in out["funds"])


def test_signal_on_the_last_session_is_new_then_open():
    o, c, a = _frames(np.full(10, 0.004))
    cut = DROP_AT + 1
    out = pb.compute(o.iloc[:cut], c.iloc[:cut], a.iloc[:cut])
    assert {t["status"] for t in out["trades"]} == {"NEW"}
    assert {f["status"] for f in out["funds"]} == {"SIGNAL"}
    assert out["summary"]["new_signals"] == list(pb.FUNDS)
    nxt = pb.compute(o.iloc[:cut + 2], c.iloc[:cut + 2], a.iloc[:cut + 2], out)
    assert {t["status"] for t in nxt["trades"]} == {"OPEN"}
    assert nxt["funds"][0]["sessions_held"] == 2 and nxt["funds"][0]["sessions_left"] == pb.HOLD - 2


def test_a_recorded_trade_survives_revised_prices():
    o, c, a = _frames(np.full(10, 0.004))
    first = pb.compute(o, c, a)
    # Yahoo "revises" history so that the fall never reached -3%: the rebuild finds no signal
    o2, c2, a2 = _frames(np.full(10, 0.004), fall=-0.001)
    again = pb.compute(o2, c2, a2, first)
    assert len(again["trades"]) == len(first["trades"])
    assert again["trades"][0]["ret"] == first["trades"][0]["ret"]
    assert again["summary"]["all"]["n"] == len(pb.FUNDS)


def test_coverage_and_unfinished_session_guards():
    o, c, a = _frames(np.array([]))
    with pytest.raises(RuntimeError):
        pb.check_coverage(c.assign(SPY=np.nan), today=c.index[-1].to_pydatetime())
    with pytest.raises(RuntimeError):
        pb.check_coverage(c, today=(c.index[-1] + pd.Timedelta(days=9)).to_pydatetime())
    pb.check_coverage(c, today=c.index[-1].to_pydatetime())
    midday = datetime(c.index[-1].year, c.index[-1].month, c.index[-1].day, 17, 0, tzinfo=timezone.utc)  # 13:00 ET
    assert len(pb.drop_unfinished_session(o, c, a, midday)[1]) == len(c) - 1
    evening = midday.replace(hour=22)
    assert len(pb.drop_unfinished_session(o, c, a, evening)[1]) == len(c)


def test_settle_drops_a_trailing_bar_not_every_fund_has():
    o, c, a = _frames(np.array([]))
    # Yahoo's post-close placeholder: the day's row with empty prices for every fund
    blank = c.copy(); blank.iloc[-1] = np.nan
    o2, c2, a2 = pb.settle(o, blank, a)
    assert len(c2) == len(c) - 1 and c2.index[-1] == c.index[-2] and len(o2) == len(a2) == len(c2)
    # only part of the list has settled: the row is not scored either
    part = c.copy(); part.loc[part.index[-1], ["QQQ", "RSP"]] = np.nan
    assert len(pb.settle(o, part, a)[1]) == len(c) - 1
    # a complete last row is kept; an empty row in the middle is dropped too
    assert len(pb.settle(o, c, a)[1]) == len(c)
    hole = c.copy(); hole.iloc[10] = np.nan
    assert len(pb.settle(o, hole, a)[1]) == len(c) - 1


def test_download_retries_when_a_ticker_comes_back_empty(monkeypatch):
    """yf.download does not raise for a ticker that failed; the scan must retry, not publish."""
    import sys, types
    o, c, a = _frames(np.array([]))
    calls = []

    def fake_download(tickers, **kw):
        calls.append(kw)
        cols = pd.MultiIndex.from_product([tickers, ["Open", "Close", "Adj Close"]])
        d = pd.DataFrame(index=c.index, columns=cols, dtype=float)
        for tk in tickers:
            d[(tk, "Open")], d[(tk, "Close")], d[(tk, "Adj Close")] = o[tk], c[tk], a[tk]
        if len(calls) == 1:
            d[("QQQ", "Close")] = np.nan                       # "database is locked" on 2026-10-06
        return d

    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(download=fake_download))
    monkeypatch.setattr(pb.time, "sleep", lambda s: None)
    _, got, _ = pb.download()
    assert len(calls) == 2 and calls[0]["threads"] is False
    assert got["QQQ"].notna().all() and len(got) == len(c)


def test_page_renders_from_scan_output_and_without_data(monkeypatch, tmp_path):
    import themes_web.app as web
    from fastapi.testclient import TestClient

    monkeypatch.setattr(web, "start_scheduler", lambda: None)
    monkeypatch.setattr(web, "_PULLBACK_DIR", tmp_path)
    monkeypatch.setattr(web, "_NASDAQ10_DIR", tmp_path / "none")
    client = TestClient(web.app)
    empty = client.get("/pullback")
    assert empty.status_code == 200 and "No scan yet" in empty.text
    assert client.get("/api/pullback").status_code == 404

    o, c, a = _frames(np.full(3, 0.004))
    cut = DROP_AT + 3
    out = pb.compute(o.iloc[:cut], c.iloc[:cut], a.iloc[:cut])
    (tmp_path / "latest.json").write_text(json.dumps(out))
    (tmp_path / "runs.jsonl").write_text(json.dumps(
        {"run_at": "x", "asof": out["asof"], "since": None, "signals": ["SPY"], "exits": [],
         "open": 8, "closed": 0}) + "\n")
    page = client.get("/pullback")
    assert page.status_code == 200
    for needle in ("Pullback rule", "Why it is not confirmed", "Open trades", "Run log", "Evidence and weak points",
                   "sessions to the sell"):
        assert needle in page.text
    assert client.get("/api/pullback").json()["runs"][0]["signals"] == ["SPY"]

    # a file from an older scan with fewer fields must not break the page
    (tmp_path / "latest.json").write_text(json.dumps(
        {"version": 1, "asof": "2026-10-02", "run_at": "x", "funds": [], "trades": []}))
    assert client.get("/pullback").status_code == 200


def test_pullback_sync_is_scheduled_at_boot(monkeypatch):
    import themes_web.scheduler as sch

    monkeypatch.setattr(sch.BackgroundScheduler, "start", lambda self, *a, **k: None)
    monkeypatch.setattr(sch, "_scheduler", None)
    job = sch.start_scheduler().get_job("pullback_sync")
    delay = (job.next_run_time - datetime.now(timezone.utc)).total_seconds()
    assert -5 < delay < 60
