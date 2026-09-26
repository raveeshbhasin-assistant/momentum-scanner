"""Ignition ledger rules (ignition/scan.py) on synthetic prices — no network."""
import importlib.util
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
