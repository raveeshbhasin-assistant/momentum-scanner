"""Nasdaq Leaders rules and ledger (nasdaq10/scan.py) on synthetic prices — no network."""
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_spec = importlib.util.spec_from_file_location(
    "nasdaq10_scan", Path(__file__).resolve().parents[1] / "nasdaq10" / "scan.py")
nq = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(nq)

IDX = pd.bdate_range("2025-01-01", periods=420)
TICKERS = [f"T{k:02d}" for k in range(15)]
MEMBERS = {t: f"Company {t}" for t in TICKERS}
# T00 is the biggest company and the slowest riser; T14 the smallest and the fastest
SHARES = {t: (15 - k) * 1e9 for k, t in enumerate(TICKERS)}
APR30 = IDX.get_loc(pd.Timestamp("2026-04-30"))      # last session of April
MAY1 = APR30 + 1


def _frames(flip=False):
    """Steady climbers: the higher the number, the faster. `flip` reverses the order of speed."""
    cols = {}
    for k, t in enumerate(TICKERS):
        speed = (14 - k if flip else k) * 0.0002
        cols[t] = 100 * np.exp(np.arange(len(IDX)) * speed)
    cols[nq.BENCH] = 100 * np.exp(np.arange(len(IDX)) * 0.0005)
    c = pd.DataFrame(cols, index=IDX)
    return c, c.copy()


@pytest.fixture(autouse=True)
def _ledger_window(monkeypatch):
    monkeypatch.setattr(nq, "LEDGER_START", "2026-04-01")
    monkeypatch.setattr(nq, "LIVE_FROM", "2026-04-15")


def _strat(out, code):
    return next(s for s in out["strategies"] if s["code"] == code)


def test_lists_are_ranked_at_month_end_and_bought_at_the_next_close():
    c, a = _frames()
    cut = MAY1 + 5
    out = nq.compute(c.iloc[:cut], a.iloc[:cut], MEMBERS, SHARES)
    may = out["months"][-1]
    assert (may["month"], may["signal"], may["trade"], may["status"]) == ("2026-05", "2026-04-30", "2026-05-01", "OPEN")
    assert [p["ticker"] for p in may["picks"]["MOM"]] == TICKERS[:4:-1]            # T14 .. T05, fastest first
    by_value = sorted(TICKERS, key=lambda t: -SHARES[t] * c[t].iloc[APR30])
    assert [p["ticker"] for p in may["picks"]["SIZE"]] == by_value[:10]
    h = _strat(out, "MOM")["holdings"][0]
    assert h["ticker"] == "T14" and h["entry_px"] == pytest.approx(c["T14"].iloc[MAY1], abs=0.01)
    assert h["ret"] == pytest.approx((c["T14"].iloc[cut - 1] / c["T14"].iloc[MAY1] - 1) * 100, abs=0.01)
    # momentum skips the latest 21 sessions and looks back 252
    assert may["picks"]["MOM"][0]["metric"] == pytest.approx(
        (a["T14"].iloc[APR30 - 21] / a["T14"].iloc[APR30 - 252] - 1) * 100, abs=0.1)


def test_last_session_of_the_month_announces_the_trades_for_the_next_close():
    c, a = _frames()
    first = nq.compute(c.iloc[:APR30], a.iloc[:APR30], MEMBERS, SHARES)                 # 29 April: nothing pending
    assert first["next"] == {"rank_on": "2026-04-30", "trade_on": "2026-05-01", "sessions_to_rank": 1, "pending": False}
    assert _strat(first, "MOM")["action"] is None
    # the order of speed flips before the ranking day: the April list is frozen, May's is new
    c2, a2 = _frames(flip=True)
    eve = nq.compute(c2.iloc[:APR30 + 1], a2.iloc[:APR30 + 1], MEMBERS, SHARES, first)
    assert eve["next"]["pending"] and eve["months"][-1]["status"] == "NEW"
    act = _strat(eve, "MOM")["action"]
    assert act["buy"] == ["T00", "T01", "T02", "T03", "T04"] and act["sell"] == ["T10", "T11", "T12", "T13", "T14"]
    assert sorted(act["keep"]) == ["T05", "T06", "T07", "T08", "T09"]
    assert eve["summary"]["buys"]["MOM"] == act["buy"]
    assert [h["ticker"] for h in _strat(eve, "MOM")["holdings"]] == TICKERS[:4:-1]      # still April's list
    after = nq.compute(c2.iloc[:MAY1 + 1], a2.iloc[:MAY1 + 1], MEMBERS, SHARES, eve)
    assert [m["status"] for m in after["months"]] == ["CLOSED", "OPEN"]
    assert _strat(after, "MOM")["action"] is None and not after["next"]["pending"]
    assert [h["ticker"] for h in _strat(after, "MOM")["holdings"]][:5] == ["T00", "T01", "T02", "T03", "T04"]


def test_recorded_picks_and_closed_months_survive_revised_prices():
    c, a = _frames()
    first = nq.compute(c.iloc[:MAY1 + 3], a.iloc[:MAY1 + 3], MEMBERS, SHARES)
    c2, a2 = _frames(flip=True)                                                          # Yahoo "revises" everything
    again = nq.compute(c2.iloc[:MAY1 + 6], a2.iloc[:MAY1 + 6], MEMBERS, SHARES, first)
    assert again["months"][0] == first["months"][0]                                      # April was closed: untouched
    assert again["months"][1]["picks"] == first["months"][1]["picks"]                    # May's picks are frozen
    assert again["months"][1]["through"] == str(IDX[MAY1 + 5].date())


def test_month_return_is_equal_weight_net_of_costs_and_set_against_the_fund():
    c, a = _frames()
    out = nq.compute(c.iloc[:MAY1 + 1], a.iloc[:MAY1 + 1], MEMBERS, SHARES)
    april = out["months"][0]
    a0 = IDX.get_loc(pd.Timestamp("2026-04-01"))
    names = [p["ticker"] for p in april["picks"]["MOM"]]
    gross = np.mean([a[t].iloc[MAY1] / a[t].iloc[a0] - 1 for t in names])
    assert april["status"] == "CLOSED"
    assert april["ret"]["MOM"] == pytest.approx((gross - nq.COST_SIDE) * 100, abs=0.01)   # first month: buys only
    assert april["ret"][nq.BENCH] == pytest.approx((a[nq.BENCH].iloc[MAY1] / a[nq.BENCH].iloc[a0] - 1) * 100, abs=0.01)
    s = _strat(out, "MOM")
    assert s["months_closed"] == 1 and s["months_ahead"] == 1 and s["since_start"] > s["bench_since_start"]


def test_calendar_knows_market_holidays():
    assert nq.next_session(pd.Timestamp("2026-04-02")) == pd.Timestamp("2026-04-06")      # Good Friday 3 April
    assert nq.month_end_session(pd.Timestamp("2024-03-27")) == pd.Timestamp("2024-03-28")  # Good Friday 29 March
    assert nq.next_session(pd.Timestamp("2026-12-31")) == pd.Timestamp("2027-01-04")
    assert nq.month_end_session(pd.Timestamp("2026-10-05")) == pd.Timestamp("2026-10-30")


def test_second_share_class_is_dropped_and_wikitext_is_parsed():
    assert "GOOG" not in nq.one_class({"GOOGL": "Alphabet (Class A)", "GOOG": "Alphabet (Class C)", "AAPL": "Apple"})
    assert "GOOG" in nq.one_class({"GOOG": "Alphabet (Class C)"})
    raw = ('{| class="wikitable sortable" id="constituents"\n|-\n! Ticker !! Company !! Industry !! Subsector\n|-\n'
           "| ADBE || [[Adobe Inc.]] || Technology || Software\n|-\n"
           "| AMD || [[AMD|Advanced Micro Devices]] || Technology || Semiconductors\n|}\n")
    assert nq.wiki_universe(raw) == {"ADBE": "Adobe Inc.", "AMD": "Advanced Micro Devices"}
    pinned = nq.pinned_universe()
    assert 95 <= len(pinned) <= 110 and pinned["AAPL"].startswith("Apple")


def test_coverage_and_unfinished_session_guards():
    c, a = _frames()
    today = c.index[-1].to_pydatetime()
    nq.check_coverage(c, MEMBERS, SHARES, today=today)
    with pytest.raises(RuntimeError):
        nq.check_coverage(c.assign(**{nq.BENCH: np.nan}), MEMBERS, SHARES, today=today)
    with pytest.raises(RuntimeError):
        nq.check_coverage(c.assign(T00=np.nan, T01=np.nan), MEMBERS, SHARES, today=today)
    with pytest.raises(RuntimeError):
        nq.check_coverage(c, MEMBERS, {t: SHARES[t] for t in TICKERS[:5]}, today=today)
    with pytest.raises(RuntimeError):
        nq.check_coverage(c, MEMBERS, SHARES, today=(c.index[-1] + pd.Timedelta(days=9)).to_pydatetime())
    midday = datetime(today.year, today.month, today.day, 17, 0, tzinfo=timezone.utc)      # 13:00 New York
    assert len(nq.drop_unfinished_session(c, a, midday)[0]) == len(c) - 1
    assert len(nq.drop_unfinished_session(c, a, midday.replace(hour=22))[0]) == len(c)


def test_page_shows_the_lists_the_pending_trades_and_tolerates_missing_data(monkeypatch, tmp_path):
    import themes_web.app as web
    from fastapi.testclient import TestClient

    monkeypatch.setattr(web, "start_scheduler", lambda: None)
    monkeypatch.setattr(web, "_NASDAQ10_DIR", tmp_path)
    monkeypatch.setattr(web, "_PULLBACK_DIR", tmp_path / "none")
    client = TestClient(web.app)
    empty = client.get("/signals")
    assert empty.status_code == 200 and empty.text.count("No scan yet") >= 2
    assert client.get("/api/nasdaq10").status_code == 404

    c, a = _frames()
    held = nq.compute(c.iloc[:APR30], a.iloc[:APR30], MEMBERS, SHARES)
    (tmp_path / "latest.json").write_text(json.dumps(held))
    page = client.get("/signals")
    assert page.status_code == 200
    for needle in ("Ten largest", "Ten strongest", "Company T14", "If the month ended today", "Month by month",
                   "ranked at the close of 2026-04-30", "Neither rule is proven"):
        assert needle in page.text
    assert "Re-pick day" not in page.text
    assert client.get("/pullback").status_code == 200                                    # the old address still works
    assert client.get("/api/nasdaq10").json()["held_month"] == "2026-04"

    c2, a2 = _frames(flip=True)
    eve = nq.compute(c2.iloc[:APR30 + 1], a2.iloc[:APR30 + 1], MEMBERS, SHARES, held)
    (tmp_path / "latest.json").write_text(json.dumps(eve))
    page = client.get("/signals")
    assert "Re-pick day" in page.text and "Trade at the close of 2026-05-01" in page.text
    assert "T00, T01, T02, T03, T04" in page.text

    # a file from an older scan with fewer fields must not break the page
    (tmp_path / "latest.json").write_text(json.dumps({"version": 1, "asof": "2026-10-02", "run_at": "x"}))
    assert client.get("/signals").status_code == 200


def test_nasdaq10_sync_is_scheduled_at_boot(monkeypatch):
    import themes_web.scheduler as sch

    monkeypatch.setattr(sch.BackgroundScheduler, "start", lambda self, *a, **k: None)
    monkeypatch.setattr(sch, "_scheduler", None)
    job = sch.start_scheduler().get_job("nasdaq10_sync")
    delay = (job.next_run_time - datetime.now(timezone.utc)).total_seconds()
    assert -5 < delay < 60
