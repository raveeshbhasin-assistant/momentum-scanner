"""Ignition ledger rules (ignition/scan.py) on synthetic prices — no network."""
import importlib.util
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_spec = importlib.util.spec_from_file_location(
    "ignition_scan", Path(__file__).resolve().parents[1] / "ignition" / "scan.py")
ig = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ig)

N = 420
FIRE = 330


def _series(after: np.ndarray, refire_at: int | None = None):
    """Uptrend, a +15.6% ignition week ending at FIRE, then `after` daily log-returns."""
    lr = np.full(N, 0.001)
    lr[FIRE - 4:FIRE + 1] += 0.028
    lr[FIRE + 1:FIRE + 1 + len(after)] = after[: N - FIRE - 1]
    vol = np.ones(N)
    vol[FIRE - 14:FIRE + 1] = 3.0
    if refire_at is not None:
        lr[refire_at - 4:refire_at + 1] += 0.03
        vol[refire_at - 14:refire_at + 1] = 3.0
    idx = pd.bdate_range("2025-01-01", periods=N)
    close = pd.DataFrame({"T": 100 * np.exp(np.cumsum(lr))}, index=idx)
    return close, close * 1.0, pd.DataFrame({"T": vol * 1e6}, index=idx)


def _run(close, open_, vol, monkeypatch):
    monkeypatch.setattr(ig, "LEDGER_START", str(close.index[300].date()))
    f = ig.signals(close, vol)
    assert f["sig"]["T"].iloc[FIRE], "synthetic ignition should fire"
    return ig.build_ledger(open_, close, f)


def test_dropping_off_the_list_is_not_a_sell(monkeypatch):
    close, open_, vol = _series(np.full(N, 0.0005))
    positions, events = _run(close, open_, vol, monkeypatch)
    assert not ig.signals(close, vol)["sig"]["T"].iloc[FIRE + 6]   # it did drop off
    p = positions[0]
    assert p["exit_reason"] is None
    assert p["status"] == "EDGE_EXPIRED"                             # >63 quiet sessions
    assert [e["type"] for e in events] == ["FIRE", "EDGE_EXPIRED"]


def test_close_below_pre_ignition_base_sells_next_open(monkeypatch):
    after = np.concatenate([np.full(3, 0.002), np.full(10, -0.04), np.full(N, 0.001)])
    close, open_, vol = _run_inputs = _series(after)
    positions, events = _run(close, open_, vol, monkeypatch)
    p = positions[0]
    base = close["T"].iloc[FIRE - ig.BASE_LAG]
    sig_day = close.index[(close["T"] < base) & (close.index > close.index[FIRE])][0]
    assert p["exit_reason"] == "IGNITION_FAILED"
    assert p["sell_signal"] == str(sig_day.date())
    assert p["exit_date"] == str(close.index[close.index.get_loc(sig_day) + 1].date())
    assert p["status"] == "CLOSED" and p["ret"] < 0
    assert any(e["type"] == "SELL" for e in events)


def test_sell_on_last_session_is_pending(monkeypatch):
    after = np.concatenate([np.full(N - FIRE - 2, 0.001), [-0.30]])
    close, open_, vol = _series(after)
    positions, _ = _run(close, open_, vol, monkeypatch)
    assert positions[0]["status"] == "SELL_PENDING"
    assert positions[0]["exit"] is None


def test_refire_extends_position_and_flags_refired(monkeypatch):
    close, open_, vol = _series(np.full(N, 0.0005), refire_at=N - 10)
    positions, events = _run(close, open_, vol, monkeypatch)
    p = positions[0]
    assert p["refires"] >= 1 and p["status"] == "REFIRED"
    assert "REFIRE" in [e["type"] for e in events]


def test_page_renders_from_scan_output(monkeypatch, tmp_path):
    import json

    import themes_web.app as web
    from fastapi.testclient import TestClient

    after = np.concatenate([np.full(3, 0.002), np.full(10, -0.04), np.full(N, 0.001)])
    close, open_, vol = _series(after)
    monkeypatch.setattr(ig, "LEDGER_START", str(close.index[300].date()))
    out = ig.compute(open_, close, vol, None)
    (tmp_path / "latest.json").write_text(json.dumps(out))
    (tmp_path / "runs.jsonl").write_text(json.dumps(
        {"run_at": "x", "asof": out["asof"], "since": None, "fires": [], "sells": ["T"],
         "expired": [], "open": 0, "closed": 1}) + "\n")
    monkeypatch.setattr(web, "_IGNITION_DIR", tmp_path)
    monkeypatch.setattr(web, "start_scheduler", lambda: None)
    client = TestClient(web.app)
    page = client.get("/ignition")
    assert page.status_code == 200
    assert "Ignition failed" in page.text and "Run log" in page.text
    assert client.get("/api/ignition").json()["runs"][0]["sells"] == ["T"]


def test_ignition_sync_runs_at_boot_regardless_of_host_timezone(monkeypatch):
    from datetime import datetime, timezone

    import themes_web.scheduler as sch

    monkeypatch.setenv("TZ", "UTC")
    monkeypatch.setattr(sch.BackgroundScheduler, "start", lambda self, *a, **k: None)
    monkeypatch.setattr(sch, "_scheduler", None)
    job = sch.start_scheduler().get_job("ignition_sync")
    delay = (job.next_run_time - datetime.now(timezone.utc)).total_seconds()
    assert -5 < delay < 60


def test_summary_reports_events_since_previous_run(monkeypatch):
    after = np.concatenate([np.full(3, 0.002), np.full(10, -0.04), np.full(N, 0.001)])
    close, open_, vol = _series(after)
    monkeypatch.setattr(ig, "LEDGER_START", str(close.index[300].date()))
    prev = str(close.index[FIRE + 2].date())                     # last run saw the fire only
    out = ig.compute(open_, close, vol, prev)
    assert out["summary"]["new_sells"] == ["T"]
    assert out["summary"]["new_fires"] == []


def test_rerun_on_same_data_date_reports_nothing_new(monkeypatch):
    after = np.concatenate([np.full(N - FIRE - 2, 0.001), [-0.30]])   # sell signal on the last session
    close, open_, vol = _series(after)
    monkeypatch.setattr(ig, "LEDGER_START", str(close.index[300].date()))
    asof = str(close.index[-1].date())
    first = ig.compute(open_, close, vol, str(close.index[-2].date()))
    assert first["summary"]["new_sells"] == ["T"]
    rerun = ig.compute(open_, close, vol, asof)
    assert rerun["summary"]["new_events"] == []


def _long_series(daily: float, n: int = 520, fire: int = 330):
    """Like _series but long enough to pass the 126-session review (n - fire > 126)."""
    lr = np.full(n, 0.001)
    lr[fire - 4:fire + 1] += 0.028
    lr[fire + 1:] = daily
    vol = np.ones(n)
    vol[fire - 14:fire + 1] = 3.0
    idx = pd.bdate_range("2025-01-01", periods=n)
    close = pd.DataFrame({"T": 100 * np.exp(np.cumsum(lr))}, index=idx)
    return close, close * 1.0, pd.DataFrame({"T": vol * 1e6}, index=idx)


def test_six_month_review_flags_small_gain_without_selling(monkeypatch):
    close, open_, vol = _long_series(0.0003)          # ~+4% after 126 sessions
    positions, events = _run(close, open_, vol, monkeypatch)
    p = positions[0]
    entry_i = close.index.get_loc(pd.Timestamp(p["entry_date"]))
    flag_i = close.index.get_loc(pd.Timestamp(p["checkpoint"]))
    assert flag_i - entry_i == ig.CHECKPOINT_AFTER                 # first eligible session
    assert 0 < p["checkpoint_ret"] < 30
    assert p["exit_reason"] is None and p["status"] != "CLOSED"    # flag only, never a sell
    assert p["checkpoint_exit"] == pytest.approx(open_["T"].iloc[flag_i + 1], rel=1e-3)
    assert p["whatif_ret"] < p["ret"]                               # it kept drifting up after the flag
    assert "CHECKPOINT" in [e["type"] for e in events]


def test_six_month_review_lets_big_winners_run(monkeypatch):
    close, open_, vol = _long_series(0.004)           # ~+65% by session 126
    positions, events = _run(close, open_, vol, monkeypatch)
    p = positions[0]
    assert p["checkpoint"] is None and p["whatif_ret"] == p["ret"]
    assert "CHECKPOINT" not in [e["type"] for e in events]


def test_page_renders_six_month_review(monkeypatch, tmp_path):
    import json

    import themes_web.app as web
    from fastapi.testclient import TestClient

    close, open_, vol = _long_series(0.0003)
    monkeypatch.setattr(ig, "LEDGER_START", str(close.index[300].date()))
    out = ig.compute(open_, close, vol, None)
    assert out["summary"]["checkpoint_open"] == ["T"]
    assert out["summary"]["checkpoint_whatif"]["n"] == 1
    (tmp_path / "latest.json").write_text(json.dumps(out))
    (tmp_path / "runs.jsonl").write_text(json.dumps(
        {"run_at": "x", "asof": out["asof"], "since": None, "fires": [], "sells": [],
         "expired": [], "open": 1, "closed": 0}) + "\n")           # old run line: no "checkpoints" key
    monkeypatch.setattr(web, "_IGNITION_DIR", tmp_path)
    monkeypatch.setattr(web, "start_scheduler", lambda: None)
    page = TestClient(web.app).get("/ignition")
    assert page.status_code == 200
    assert "6-mo review" in page.text and "with the 6-month review" in page.text
    assert re.search(r'<span class="pill CHECKPOINT" title="6-month review since [0-9-]+ at [+-][0-9.]+%">6-mo review</span>', page.text)
