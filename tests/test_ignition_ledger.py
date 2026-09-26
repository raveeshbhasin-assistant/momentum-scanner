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


def test_cap_review_flags_only_held_positions_over_2x():
    held = lambda tk, ret, status="HOLD": {"ticker": tk, "status": status, "ret": ret}
    positions = [held("A", 0.0), held("B", 0.0), held("C", 0.0), held("BIG", 500.0),   # mean gross = 2.25
                 held("GONE", 900.0, "CLOSED"), held("NEW", None, "NEW")]
    ig.cap_review(positions)
    by = {p["ticker"]: p for p in positions}
    assert by["BIG"]["weight_x"] == pytest.approx(6 / 2.25, abs=0.01)                  # 2.67x
    assert by["BIG"]["cap_trim"] == pytest.approx(100 * (1 - 2 / (6 / 2.25)), abs=0.1)  # sell 25%
    assert by["A"]["cap_trim"] is None and by["A"]["weight_x"] == pytest.approx(1 / 2.25, abs=0.01)
    assert by["GONE"]["weight_x"] is None and by["NEW"]["weight_x"] is None             # not in the held book


def test_page_renders_cap_review(monkeypatch, tmp_path):
    import json

    import themes_web.app as web
    from fastapi.testclient import TestClient

    close, open_, vol = _long_series(0.0003)
    monkeypatch.setattr(ig, "LEDGER_START", str(close.index[300].date()))
    out = ig.compute(open_, close, vol, None)
    p = next(p for p in out["positions"] if p["status"] != "CLOSED")
    assert p["weight_x"] == 1.0 and p["cap_trim"] is None          # a one-stock book is exactly 1x
    p["weight_x"], p["cap_trim"] = 3.1, 35.5                      # force a flag to check the markup
    out["summary"]["cap_review"] = ["T"]
    (tmp_path / "latest.json").write_text(json.dumps(out))
    (tmp_path / "runs.jsonl").write_text("")
    monkeypatch.setattr(web, "_IGNITION_DIR", tmp_path)
    monkeypatch.setattr(web, "start_scheduler", lambda: None)
    page = TestClient(web.app).get("/ignition").text
    assert re.search(r'<span class="pill CAP" title="About 3\.1x an equal share[^"<>]*">2× cap · trim 36%</span>', page)
    assert "over the 2× cap: T" in page


def test_page_renders_data_from_before_the_review_flags(monkeypatch, tmp_path):
    """A deploy can land before the next scan: the page must render latest.json
    written by an older scan.py that has none of the v1.7/v1.8 fields (the v1.8.0
    deploy served a 500 for exactly this)."""
    import json

    import themes_web.app as web
    from fastapi.testclient import TestClient

    close, open_, vol = _long_series(0.0003)
    monkeypatch.setattr(ig, "LEDGER_START", str(close.index[300].date()))
    out = ig.compute(open_, close, vol, None)
    for p in out["positions"]:
        for k in ("checkpoint", "checkpoint_ret", "checkpoint_exit", "whatif_ret", "weight_x", "cap_trim"):
            p.pop(k, None)
    for k in ("new_checkpoints", "checkpoint_open", "checkpoint_whatif", "cap_review"):
        out["summary"].pop(k, None)
    (tmp_path / "latest.json").write_text(json.dumps(out))
    (tmp_path / "runs.jsonl").write_text(json.dumps(
        {"run_at": "x", "asof": out["asof"], "since": None, "fires": [], "sells": [],
         "expired": [], "open": 1, "closed": 0}) + "\n")
    monkeypatch.setattr(web, "_IGNITION_DIR", tmp_path)
    monkeypatch.setattr(web, "start_scheduler", lambda: None)
    page = TestClient(web.app).get("/ignition")
    assert page.status_code == 200
    assert "pill CAP" not in page.text and "pill CHECKPOINT" not in page.text


# ---- history is append-only (carry_forward / check_coverage) ----------------

def _pos(tk, fired, status="HOLD", **kw):
    return {"ticker": tk, "fired": fired, "status": status, "ret": kw.pop("ret", 5.0), **kw}


def _close_with(tickers, last_nan=()):
    idx = pd.bdate_range("2026-09-21", periods=5)
    df = pd.DataFrame(1.0, index=idx, columns=list(tickers))
    for t in last_nan:
        df.loc[idx[-1], t] = np.nan
    return df


def test_open_position_for_a_ticker_with_no_data_is_carried_not_dropped():
    prev = [_pos("GONE", "2026-06-01", ret=12.0)]
    out = ig.carry_forward([], prev, _close_with(["AAA"]), "2026-09-24")
    assert len(out) == 1 and out[0]["ticker"] == "GONE" and out[0]["ret"] == 12.0
    assert "no price data" in out[0]["carried"] and out[0]["carried_since"] == "2026-09-24"
    # a later run still keeps it, and keeps the original carried_since
    again = ig.carry_forward([], out, _close_with(["AAA"]), "2026-09-25")
    assert again[0]["carried_since"] == "2026-09-24"


def test_open_position_the_rebuild_no_longer_produces_is_carried_with_reason():
    prev = [_pos("AAA", "2026-06-01")]
    out = ig.carry_forward([], prev, _close_with(["AAA"]), "2026-09-24")
    assert "adjusted prices revised" in out[0]["carried"]


def test_fresh_rebuild_replaces_a_carried_position_and_small_fire_shifts_match():
    prev = [_pos("AAA", "2026-06-01", carried="x", carried_since="2026-09-01")]
    fresh = [_pos("AAA", "2026-06-03", ret=20.0)]                  # fire moved 2 days: same position
    out = ig.carry_forward(fresh, prev, _close_with(["AAA"]), "2026-09-24")
    assert len(out) == 1 and out[0]["ret"] == 20.0 and not out[0].get("carried")


def test_recorded_closed_trade_is_final():
    rec = _pos("AAA", "2026-03-02", "CLOSED", exit_date="2026-05-04", exit=90.0, ret=-10.0)
    # rebuild lost it entirely -> kept
    out = ig.carry_forward([], [rec], _close_with(["AAA"]), "2026-09-24")
    assert out[0]["exit_date"] == "2026-05-04" and out[0]["kept"]
    # rebuild now says it never sold -> the recorded exit wins
    out = ig.carry_forward([_pos("AAA", "2026-03-02", "HOLD")], [rec], _close_with(["AAA"]), "2026-09-24")
    assert len(out) == 1 and out[0]["status"] == "CLOSED" and out[0]["exit_date"] == "2026-05-04"
    # rebuild agrees -> untouched, no flag
    same = dict(rec)
    out = ig.carry_forward([same], [rec], _close_with(["AAA"]), "2026-09-24")
    assert out[0] is same and not out[0].get("kept")


def test_carried_positions_are_left_out_of_the_cap_review_book():
    ps = [_pos("A", "2026-06-01", ret=0.0), _pos("B", "2026-06-01", ret=900.0, carried="x")]
    ig.cap_review(ps)
    assert ps[0]["weight_x"] == 1.0 and ps[1]["weight_x"] is None


def test_partial_download_is_refused():
    ig.check_coverage(_close_with([f"T{i}" for i in range(100)]), 100)          # 100% ok
    with pytest.raises(RuntimeError, match="refusing to publish"):
        ig.check_coverage(_close_with([f"T{i}" for i in range(100)],
                                      last_nan=[f"T{i}" for i in range(5)]), 100)  # 95% < 97%


def test_compute_keeps_history_across_runs_and_page_shows_it(monkeypatch, tmp_path):
    import json

    import themes_web.app as web
    from fastapi.testclient import TestClient

    close, open_, vol = _long_series(0.0003)
    monkeypatch.setattr(ig, "LEDGER_START", str(close.index[300].date()))
    prev = [_pos("OLD", "2025-11-03", ret=40.0, entry=10.0, price=14.0, base=8.0, to_stop=-42.9,
                 peak_ret=50.0, sessions_since_fire=120, refires=0),
            _pos("OLDC", "2025-10-01", "CLOSED", exit_date="2026-01-05", exit=9.0, entry=10.0, ret=-10.0,
                 exit_reason="IGNITION_FAILED", sell_signal="2026-01-02", held=60, peak_ret=5.0)]
    out = ig.compute(open_, close, vol, "2026-09-24", prev)
    tickers = {p["ticker"] for p in out["positions"]}
    assert {"T", "OLD", "OLDC"} <= tickers
    assert out["summary"]["carried"] == ["OLD"] and out["summary"]["kept_closed"] == 1
    (tmp_path / "latest.json").write_text(json.dumps(out))
    (tmp_path / "runs.jsonl").write_text("")
    monkeypatch.setattr(web, "_IGNITION_DIR", tmp_path)
    monkeypatch.setattr(web, "start_scheduler", lambda: None)
    page = TestClient(web.app).get("/ignition")
    assert page.status_code == 200
    assert "kept from earlier runs (no fresh data): OLD" in page.text
    assert re.search(r'<span class="pill KEPT" title="Kept from earlier runs: [^"<>]*">kept · 2026-09-24</span>', page.text)
    assert "(recorded)" in page.text
