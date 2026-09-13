"""Test suite for the research harness. Run with:  python tests/run_tests.py   (no pytest needed)

Sections
  1. sim.simulate_trade on synthetic bars with known outcomes
  2. batch (_plain_batch / simulate_many) == single-trade engine on random synthetic days
  3. bars: indicators vs the `ta` library, scanner mirrors vs the REAL scanner.py (imported read-only with
     stubbed network modules; skipped if the repo is not present), daily-bar alignment, look-ahead checks
  4. evaluate: bootstrap, summarize, splits, walk-forward, compare
  5. all_bars_labels consistency
Tests that need the bar cache skip (with a note) when it is absent. Exit code 1 on any failure.
"""
from __future__ import annotations

import os
import sys
import time
import traceback
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))
sys.dont_write_bytecode = True

import numpy as np
import pandas as pd

from harness import bars as B
from harness import common, evaluate as E, sim
from harness.common import BAR_TD, ET, make_ts

REPO = os.environ.get("TRADER_REPO", "C:/Users/ravee/OneDrive/Documents/Claude/Projects/Trader v3")
HAVE_DATA = (common.BARS5M_DIR / "AAPL.csv").exists()


class Skip(Exception):
    pass


def need_data():
    if not HAVE_DATA:
        raise Skip("bar cache not present")


# ═══════════════════════════════════════════════════════════════════
#  helpers
# ═══════════════════════════════════════════════════════════════════
def mkday(over=None, base=(100.0, 100.5, 99.5, 100.0), date="2026-07-15", n=78, start="09:30"):
    """Flat synthetic RTH day (78 bars) with per-position OHLC overrides {pos: (o, h, l, c)}."""
    idx = pd.date_range(f"{date} {start}", periods=n, freq="5min", tz=ET)
    o, h, l, c = base
    df = pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c, "Volume": 1000.0}, index=idx)
    for k, (o_, h_, l_, c_) in (over or {}).items():
        df.loc[idx[k], ["Open", "High", "Low", "Close"]] = [o_, h_, l_, c_]
    return df


SIG = make_ts("2026-07-15", "09:35")
LIVE = dict(entry_mode="signal_price", cost_bps_per_side=0.0)


def close(a, b, tol=1e-9):
    return abs(a - b) <= tol


# ═══════════════════════════════════════════════════════════════════
#  1. simulate_trade on synthetic bars
# ═══════════════════════════════════════════════════════════════════
def test_target_first():
    d = mkday({2: (100, 105.5, 99.8, 105)})
    r = sim.simulate_trade(d, SIG, 100, 98, 105, **LIVE)
    assert r["result"] == "WIN" and r["exit_reason"] == "target"
    assert close(r["R"], 2.5) and close(r["exit_fill"], 105) and close(r["pnl_pct"], 5.0)
    assert r["entry_ts"] == SIG and r["exit_ts"] == make_ts("2026-07-15", "09:40")
    assert r["entry_idx"] == 1 and r["exit_idx"] == 2 and r["bars_held"] == 2
    assert close(r["mfe_R"], (105.5 - 100) / 2) and close(r["mae_R"], (99.5 - 100) / 2)


def test_stop_first():
    d = mkday({3: (100, 100.2, 97, 97.5)})
    r = sim.simulate_trade(d, SIG, 100, 98, 105, **LIVE)
    assert r["result"] == "LOSS" and r["exit_reason"] == "stop" and close(r["R"], -1.0)
    assert r["exit_ts"] == make_ts("2026-07-15", "09:45") and close(r["exit_fill"], 98.0)


def test_both_in_one_bar():
    green = mkday({2: (100, 105.5, 97, 101)})
    red = mkday({2: (100, 105.5, 97, 99)})
    assert sim.simulate_trade(green, SIG, 100, 98, 105, same_bar="loss", **LIVE)["result"] == "LOSS"
    assert sim.simulate_trade(green, SIG, 100, 98, 105, same_bar="win", **LIVE)["result"] == "WIN"
    assert sim.simulate_trade(green, SIG, 100, 98, 105, same_bar="open_close_tiebreak", **LIVE)["result"] == "LOSS"
    assert sim.simulate_trade(red, SIG, 100, 98, 105, same_bar="open_close_tiebreak", **LIVE)["result"] == "WIN"


def test_gap_through_stop():
    d = mkday({2: (96, 100, 95, 99)})
    r = sim.simulate_trade(d, SIG, 100, 98, 105, gap_aware_stops=True, **LIVE)
    assert r["result"] == "LOSS" and r["exit_reason"] == "gap_stop" and close(r["exit_fill"], 96) and close(r["R"], -2.0)
    r = sim.simulate_trade(d, SIG, 100, 98, 105, gap_aware_stops=False, **LIVE)
    assert r["result"] == "LOSS" and r["exit_reason"] == "stop" and close(r["exit_fill"], 98) and close(r["R"], -1.0)
    d = mkday({2: (106, 107, 105.5, 106.5)})
    r = sim.simulate_trade(d, SIG, 100, 98, 105, gap_aware_stops=True, **LIVE)
    assert r["result"] == "WIN" and r["exit_reason"] == "gap_target" and close(r["exit_fill"], 106) and close(r["R"], 3.0)


def test_eod_and_exit_time():
    d = mkday()
    r = sim.simulate_trade(d, SIG, 100, 98, 105, **LIVE)
    assert r["result"] == "EOD" and r["exit_ts"] == make_ts("2026-07-15", "15:55") and r["bars_held"] == 77 and close(r["R"], 0.0)
    r16 = sim.simulate_trade(d, SIG, 100, 98, 105, exit_time="16:00", **LIVE)
    assert r16["exit_ts"] == make_ts("2026-07-15", "15:55")
    r12 = sim.simulate_trade(d, SIG, 100, 98, 105, exit_time="12:00", **LIVE)
    assert r12["exit_ts"] == make_ts("2026-07-15", "12:00") and r12["bars_held"] == 30
    # a shortened session (missing tail) still exits on its last bar
    r_short = sim.simulate_trade(d.iloc[:40], SIG, 100, 98, 105, **LIVE)
    assert r_short["result"] == "EOD" and r_short["exit_ts"] == d.index[39]


def test_next_open_entry_above_signal_price():
    d = mkday({2: (101, 101.5, 100.5, 101), 4: (101, 106, 100.9, 105)})
    r = sim.simulate_trade(d, SIG, 100, 98, 105, entry_mode="next_open", cost_bps_per_side=5)
    c = 5e-4
    assert r["entry_ts"] == make_ts("2026-07-15", "09:40") and close(r["entry_fill"], 101 * (1 + c))
    assert r["result"] == "WIN" and close(r["exit_fill"], 105 * (1 - c))
    assert close(r["R"], (105 * (1 - c) - 101 * (1 + c)) / 2.0)                       # ORIGINAL risk (100-98)
    assert close(r["R_own"], (105 * (1 - c) - 101 * (1 + c)) / (101 * (1 + c) - 98))  # actual fill risk
    assert close(r["pnl_pct"], (105 * (1 - c) / (101 * (1 + c)) - 1) * 100)
    assert r["entry_idx"] == 2   # evaluation starts on the entry bar
    r = sim.simulate_trade(d, SIG, 100, 98, 105, entry_mode="first_open", cost_bps_per_side=0)
    assert r["entry_ts"] == make_ts("2026-07-15", "09:35") and close(r["entry_fill"], 100.0)
    r = sim.simulate_trade(d, make_ts("2026-07-15", "09:36"), 100, 98, 105, entry_mode="first_open", cost_bps_per_side=0)
    assert r["entry_ts"] == make_ts("2026-07-15", "09:40")   # mid-bar signal: first_open == next_open
    r = sim.simulate_trade(d, SIG, 100, 98, 105, entry_mode="next_close", cost_bps_per_side=0)
    assert r["entry_idx"] == 2 and close(r["entry_fill"], 100.0) and r["entry_ts"] == make_ts("2026-07-15", "09:40")
    # fill outside the levels -> NO_ENTRY
    d2 = mkday({2: (105.5, 106, 105.2, 105.8)})
    assert sim.simulate_trade(d2, SIG, 100, 98, 105, entry_mode="next_open")["exit_reason"] == "fill_above_target"
    d3 = mkday({2: (97.5, 99, 97, 98.5)})
    assert sim.simulate_trade(d3, SIG, 100, 98, 105, entry_mode="next_open")["exit_reason"] == "fill_below_stop"


def test_no_entry_cases():
    r = sim.simulate_trade(mkday(), make_ts("2026-07-15", "15:57"), 100, 98, 105, entry_mode="next_open")
    assert r["result"] == "NO_ENTRY" and r["exit_reason"] == "no_bar" and pd.isna(r["R"])
    r = sim.simulate_trade(mkday(), SIG, 100, 101, 105, **LIVE)
    assert r["result"] == "NO_ENTRY" and r["exit_reason"] == "bad_levels"
    r = sim.simulate_trade(mkday(), make_ts("2026-07-15", "15:55"), 100, 98, 105, **LIVE)
    assert r["result"] == "EOD" and r["bars_held"] == 1   # signal at the last bar's start: still evaluated


def test_time_stop():
    d = mkday()
    r = sim.simulate_trade(d, SIG, 100, 98, 105, time_stop_min=30, time_stop_min_r=0.5, **LIVE)
    # entry 09:35; the first bar ENDING >= 10:05 is the 10:00 bar -> exit at its close
    assert r["result"] == "TIME" and r["exit_reason"] == "time_stop" and r["exit_ts"] == make_ts("2026-07-15", "10:00")
    assert close(r["R"], 0.0)
    d2 = mkday({6: (100, 101.6, 100, 101.5)})   # 10:00 bar closes at +0.75R -> survives the check
    r = sim.simulate_trade(d2, SIG, 100, 98, 105, time_stop_min=30, time_stop_min_r=0.5, **LIVE)
    assert r["result"] == "EOD"


def test_breakeven():
    d = mkday(base=(101.0, 101.4, 100.6, 101.0), over={1: (100, 100.4, 99.8, 100.2), 3: (101, 102.5, 100.8, 102),
                                                    6: (101, 101.2, 99.9, 100.2)})
    r = sim.simulate_trade(d, SIG, 100, 98, 105, breakeven_after_r=1.0, **LIVE)
    # 09:45 high 102.5 >= 1R -> stop raised to entry from the 09:50 bar; 10:00 low 99.9 <= 100 -> be_stop
    assert r["result"] == "TRAIL" and r["exit_reason"] == "be_stop" and r["exit_ts"] == make_ts("2026-07-15", "10:00")
    assert close(r["R"], 0.0) and close(r["exit_fill"], 100.0)
    # the raise applies from the NEXT bar: a low <= entry on the trigger bar itself does not stop us out
    d2 = mkday(base=(101.0, 101.4, 100.6, 101.0), over={1: (100, 100.4, 99.8, 100.2), 3: (100.5, 102.5, 99.9, 102)})
    r = sim.simulate_trade(d2, SIG, 100, 98, 105, breakeven_after_r=1.0, **LIVE)
    assert r["result"] == "EOD"


def test_trailing():
    d = mkday(base=(103.0, 103.4, 102.6, 103.0), over={1: (100, 100.4, 99.8, 100.2), 2: (100.2, 100.6, 100, 100.4),
                                                    3: (100.4, 103, 100.2, 102.5), 4: (102.5, 104, 102.1, 103.5),
                                                    6: (103, 103.2, 101.5, 101.8)})
    r = sim.simulate_trade(d, SIG, 100, 98, 105, trail_after_r=1.0, trail_dist_r=1.0, **LIVE)
    # 09:45 high 103 -> stop 101; 09:50 high 104 -> stop 102 (from 09:55); 10:00 low 101.5 -> trail_stop at 102
    assert r["result"] == "TRAIL" and r["exit_reason"] == "trail_stop" and close(r["exit_fill"], 102.0) and close(r["R"], 1.0)
    assert r["exit_ts"] == make_ts("2026-07-15", "10:00")
    try:
        sim.simulate_trade(d, SIG, 100, 98, 105, trail_after_r=1.0, **LIVE)
        assert False, "trail_after_r without trail_dist_r must raise"
    except ValueError:
        pass


def test_scale_out():
    d = mkday(base=(101.0, 101.4, 100.6, 101.0), over={1: (100, 100.4, 99.8, 100.2), 3: (101, 102.5, 100.8, 102),
                                                    6: (101, 101.2, 97.5, 98)})
    r = sim.simulate_trade(d, SIG, 100, 98, 105, scale_out=[(1.0, 0.5)], **LIVE)
    assert r["result"] == "LOSS" and close(r["R"], 0.5 * 1.0 + 0.5 * (-1.0)) and "scaled 0.5@1R" in r["notes"]
    assert close(r["exit_fill"], 0.5 * 102 + 0.5 * 98)


def test_costs_signal_price():
    d = mkday({2: (100, 105.5, 99.8, 105)})
    r = sim.simulate_trade(d, SIG, 100, 98, 105, entry_mode="signal_price", cost_bps_per_side=10)
    c = 1e-3
    assert close(r["entry_fill"], 100 * (1 + c)) and close(r["exit_fill"], 105 * (1 - c))
    assert close(r["R"], (105 * (1 - c) - 100 * (1 + c)) / 2)
    assert close(E.recost_R(pd.DataFrame([r]), 0.0).iloc[0], 2.5)


def test_input_validation():
    for bad in [dict(entry_mode="x"), dict(same_bar="x")]:
        try:
            sim.simulate_trade(mkday(), SIG, 100, 98, 105, **bad)
            assert False
        except ValueError:
            pass
    # naive signal timestamps are treated as ET; 'HH:MM' strings take the date from the bars
    r1 = sim.simulate_trade(mkday(), pd.Timestamp("2026-07-15 09:35"), 100, 98, 105, **LIVE)
    r2 = sim.simulate_trade(mkday(), "09:35", 100, 98, 105, **LIVE)
    assert r1["entry_ts"] == r2["entry_ts"] == SIG
    # pre-market rows in the frame are ignored
    pm_day = pd.concat([mkday(start="08:00", n=18), mkday({2: (100, 105.5, 99.8, 105)})])
    assert sim.simulate_trade(pm_day, SIG, 100, 98, 105, **LIVE)["result"] == "WIN"


# ═══════════════════════════════════════════════════════════════════
#  2. batch == single
# ═══════════════════════════════════════════════════════════════════
def _random_day(rng, date="2026-07-15"):
    n = 78
    c = 100 * np.exp(np.cumsum(rng.normal(0, 0.002, n)))
    o = np.r_[100.0, c[:-1]] * (1 + rng.normal(0, 0.0005, n))
    h = np.maximum(o, c) * (1 + np.abs(rng.normal(0, 0.001, n)))
    l = np.minimum(o, c) * (1 - np.abs(rng.normal(0, 0.001, n)))
    idx = pd.date_range(f"{date} 09:30", periods=n, freq="5min", tz=ET)
    return pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c, "Volume": 1000.0}, index=idx)


def _same(r1: dict, r2) -> bool:
    for f in ("result", "exit_reason", "bars_held", "entry_idx", "exit_idx"):
        if r1[f] != r2[f]:
            return False
    for f in ("R", "R_own", "pnl_pct", "mfe_R", "mae_R", "entry_fill", "exit_fill"):
        a, b = r1[f], r2[f]
        if not ((pd.isna(a) and pd.isna(b)) or np.isclose(a, b, atol=1e-9, equal_nan=True)):
            return False
    for f in ("entry_ts", "exit_ts"):
        a, b = r1[f], r2[f]
        if not ((pd.isna(a) and pd.isna(b)) or pd.Timestamp(a) == pd.Timestamp(b)):
            return False
    return True


def test_batch_equals_single_random():
    rng = np.random.default_rng(7)
    settings = [dict(entry_mode=m, same_bar=s, gap_aware_stops=g, cost_bps_per_side=cb, exit_time=et)
                for m in sim.ENTRY_MODES for s in sim.SAME_BAR_MODES for g in (True, False)
                for cb, et in ((0.0, "15:55"), (5.0, "16:00"), (5.0, "13:00"))]
    n_checked = 0
    for k in range(12):
        day = _random_day(rng)
        D = sim.DayArrays(day)
        m = 40
        sig_min = rng.integers(0, 78 * 5 + 20, size=m)                       # includes after-hours signals
        sig = [make_ts("2026-07-15", f"{9 + (30 + int(x)) // 60:02d}:{(30 + int(x)) % 60:02d}") for x in sig_min]
        # levels around the price at signal time
        pos = np.minimum(np.searchsorted(D.mins, 570 + sig_min), 77)
        px = D.c[pos]
        ent = np.round(px * (1 + rng.normal(0, 0.001, m)), 2)
        stp = np.round(ent * (1 - np.abs(rng.normal(0.004, 0.003, m)) - 0.0005), 2)
        tgt = np.round(ent + (ent - stp) * rng.choice([1.0, 2.5, 4.0], m), 2)
        stp[:3] = ent[:3] + 0.1                                                # a few bad levels
        picks = pd.DataFrame({"ticker": "X", "signal_ts": sig, "entry": ent, "stop": stp, "target": tgt})
        sig_ns = sim._ts_to_ns(sim.signal_timestamps(picks))
        for kw in settings:
            b = sim._plain_batch(D, sig_ns, ent, stp, tgt, **kw)
            ets = sim._ns_to_ts(b["entry_ts"]); xts = sim._ns_to_ts(b["exit_ts"])
            for i in range(m):
                r1 = sim.simulate_trade(D, sig[i], ent[i], stp[i], tgt[i], **kw)
                r2 = {f: b[f][i] for f in b}
                r2["entry_ts"], r2["exit_ts"] = ets.iloc[i], xts.iloc[i]
                assert _same(r1, r2), f"batch/single mismatch day {k} pick {i} {kw}\n single={r1}\n batch={r2}"
                n_checked += 1
    assert n_checked > 10000


def test_simulate_many_paths():
    rng = np.random.default_rng(3)
    bars = {"AAA": _random_day(rng, "2026-07-15"), "BBB": pd.concat([_random_day(rng, "2026-07-15"), _random_day(rng, "2026-07-16")])}
    picks = pd.DataFrame({"ticker": ["AAA", "BBB", "BBB", "ZZZ"], "date": ["2026-07-15", "2026-07-15", "2026-07-16", "2026-07-15"],
                          "batch_time": ["09:41", "10:06", "14:00", "10:00"],
                          "entry": [100, 100, 100, 100], "stop": [99, 99, 99, 99], "target": [102.5, 102.5, 102.5, 102.5]})
    for t in ("AAA", "BBB"):
        pass
    # align entry to the price so the fills are inside the levels
    for i, row in picks.iterrows():
        if row.ticker in bars:
            D = sim.day_arrays(row.ticker, row.date, bars)
            p = float(D.c[np.searchsorted(D.mins, common.hhmm_to_minutes(row.batch_time))])
            picks.loc[i, ["entry", "stop", "target"]] = [p, p * 0.99, p * 1.025]
    out = sim.simulate_many(picks, bars, verbose=False)
    assert list(out.index) == list(picks.index) and out.loc[3, "result"] == "NO_DATA"
    for i in range(3):
        row = picks.loc[i]
        r = sim.simulate_trade(sim.day_arrays(row.ticker, row.date, bars), make_ts(row.date, row.batch_time),
                               row.entry, row.stop, row.target)
        assert _same(r, out.loc[i].to_dict())
    # management kwargs route through simulate_trade and stay row-aligned
    out2 = sim.simulate_many(picks, bars, verbose=False, breakeven_after_r=1.0, entry_mode="signal_price", cost_bps_per_side=0)
    for i in range(3):
        row = picks.loc[i]
        r = sim.simulate_trade(sim.day_arrays(row.ticker, row.date, bars), make_ts(row.date, row.batch_time),
                               row.entry, row.stop, row.target, breakeven_after_r=1.0, **LIVE)
        assert _same(r, out2.loc[i].to_dict())
    assert out2.loc[3, "result"] == "NO_DATA"
    sim._DAY_CACHE.clear()


# ═══════════════════════════════════════════════════════════════════
#  3. bars
# ═══════════════════════════════════════════════════════════════════
def test_session_helpers_and_reference_bar():
    day = pd.concat([mkday(start="04:00", n=66), mkday({2: (100, 106, 99, 105)}), mkday(start="16:00", n=48)])
    assert len(B.rth_slice(day)) == 78 and len(B.premarket_slice(day)) == 66 and len(B.afterhours_slice(day)) == 48
    assert B.reference_bar(day, "09:34") is None
    assert day.index[B.reference_bar(day, "09:35")] == make_ts("2026-07-15", "09:30")
    assert day.index[B.reference_bar(day, "10:06")] == make_ts("2026-07-15", "10:00")
    assert day.index[B.reference_bar(day, "10:05")] == make_ts("2026-07-15", "10:00")
    assert day.index[B.reference_bar(day, "10:04")] == make_ts("2026-07-15", "09:55")
    assert day.index[B.reference_bar(day, "16:30")] == make_ts("2026-07-15", "15:55")   # never an after-hours bar
    frames = B.session_frames(pd.concat([day, mkday(date="2026-07-16")]))
    assert set(frames) == {pd.Timestamp("2026-07-15").date(), pd.Timestamp("2026-07-16").date()}
    assert len(B.session_frame(pd.concat([day, mkday(date="2026-07-16")]), "2026-07-16")) == 78


def test_session_vwap_and_strong_components_synthetic():
    day = pd.concat([mkday(start="09:00", n=6, base=(99, 99.6, 98.8, 99.2)), mkday({2: (100, 106, 99, 105)})])
    day["Volume"] = np.arange(1, len(day) + 1, dtype=float)
    v = B.session_vwap(day)
    tp = (day.High + day.Low + day.Close) / 3
    assert np.allclose(v.to_numpy(), ((tp * day.Volume).cumsum() / day.Volume.cumsum()).to_numpy())
    vr = B.session_vwap(day, common.RTH_START_MIN)
    assert vr.iloc[:6].isna().all() and np.isclose(vr.iloc[6], tp.iloc[6])
    ind = B.add_indicators(day)
    assert ind["VWAP_rth"].iloc[:6].isna().all() and ind["ATR_rth"].iloc[:6].isna().all()
    s = B.strong_components(ind, 8)     # the 09:40 bar: green, new HOD, above the pre-market high
    assert s["bar_green"] and s["new_hod"] and s["pm_high_hold"] and s["above_vwap"] and s["strong"]
    s2 = B.strong_components(ind, 7)    # flat bar: not green
    assert not s2["bar_green"] and not s2["strong"]
    # no pre-market bars -> pm_high_hold False
    s3 = B.strong_components(B.add_indicators(mkday({2: (100, 106, 99, 105)})), 2)
    assert s3["new_hod"] and s3["bar_green"] and not s3["pm_high_hold"] and not s3["strong"]
    # extension features
    rth = mkday({0: (100, 101, 99.5, 100.5), 1: (100.5, 101.5, 100.2, 101), 2: (101, 101.8, 100.8, 101.6), 3: (101.6, 102.5, 101.4, 102.4)})
    e = B.extension_features(rth, 3)
    assert e["consec_green"] == 4 and e["above_orb_high"] == 1 and np.isclose(e["range_pos"], round((102.4 - 99.5) / (102.5 - 99.5), 3))
    assert B.extension_features(rth, 2)["above_orb_high"] is None     # OR not closed yet
    assert B.extension_features(rth, 4)["consec_green"] == 0         # flat bar breaks the run
    # vectorized table agrees with the per-bar functions on the synthetic frame
    st = B.strong_table(ind)
    for pos in range(len(ind)):
        sc = B.strong_components(ind, pos); ex = B.extension_features(ind, pos)
        row = st.iloc[pos]
        assert all(bool(row[k]) == sc[k] for k in ("bar_green", "above_vwap", "new_hod", "pm_high_hold", "strong"))
        for k in ("consec_green", "range_pos", "above_orb_high"):
            a, b_ = ex[k], row[k]
            assert (a is None and pd.isna(b_)) or np.isclose(float(a), float(b_)), (pos, k, a, b_)


def test_indicators_vs_ta():
    need_data()
    try:
        from ta.momentum import RSIIndicator
        from ta.trend import EMAIndicator, MACD
        from ta.volatility import AverageTrueRange, BollingerBands
    except ImportError:
        raise Skip("ta not installed")
    df = B.load_5m(["AAPL"])["AAPL"].iloc[:4000]
    ind = B.add_indicators(df)
    c, h, l = df["Close"], df["High"], df["Low"]
    ref = {"EMA_9": EMAIndicator(c, 9).ema_indicator(), "EMA_21": EMAIndicator(c, 21).ema_indicator(),
           "EMA_50": EMAIndicator(c, 50).ema_indicator(), "RSI": RSIIndicator(c, 14).rsi()}
    m = MACD(c, 26, 12, 9)
    ref["MACD_12_26_9"], ref["MACDs_12_26_9"], ref["MACDh_12_26_9"] = m.macd(), m.macd_signal(), m.macd_diff()
    bb = BollingerBands(c, 20, 2.0)
    ref["BBU_20_2.0"], ref["BBL_20_2.0"], ref["BBM_20_2.0"] = bb.bollinger_hband(), bb.bollinger_lband(), bb.bollinger_mavg()
    for k, v in ref.items():
        a, b_ = ind[k].to_numpy(), v.to_numpy()
        ok = ~np.isnan(a) & ~np.isnan(b_)
        assert ok.sum() > 3000 and np.allclose(a[ok], b_[ok], rtol=1e-9, atol=1e-9), k
        assert (np.isnan(a) == np.isnan(b_)).all(), f"{k}: NaN pattern differs"
    atr = AverageTrueRange(h, l, c, 14).average_true_range().to_numpy()
    a = ind["ATR"].to_numpy()
    assert np.isnan(a[:13]).all() and np.allclose(a[13:], atr[13:], rtol=1e-9, atol=1e-9)


def _import_scanner():
    if not os.path.exists(os.path.join(REPO, "scanner.py")):
        raise Skip("scanner.py not found")
    sys.dont_write_bytecode = True
    for name, attrs in {"news": {"get_sentiment_score": lambda *a, **k: 0.0},
                        "sector_rotation": {"classify_leadership": lambda *a, **k: {}, "TICKER_TO_SECTOR": {}},
                        "market_regime": {"get_regime": lambda *a, **k: {}},
                        "earnings": {"get_earnings_context": lambda *a, **k: {}}}.items():
        if name not in sys.modules:
            mod = types.ModuleType(name); mod.__dict__.update(attrs); sys.modules[name] = mod
    if REPO not in sys.path:
        sys.path.insert(0, REPO)
    try:
        import scanner  # noqa
        return scanner
    except Exception as e:  # pragma: no cover
        raise Skip(f"scanner import failed: {e}")


def test_scanner_mirror_real_data():
    """reference_bar / strong_components / extension_features / scanner_levels / rvol vs the REAL scanner code."""
    need_data()
    sc = _import_scanner()
    rng = np.random.default_rng(11)
    tickers = rng.choice(B.stock_tickers(), 12, replace=False)
    n_strong = n_ext = n_lvl = n_rvol = 0
    for t in tickers:
        raw = B.load_5m([t])[t]
        ind = B.get_indicators(t)
        dates = B.session_dates(raw)[5:]
        for d in rng.choice(dates, 4, replace=False):
            day_raw = B.session_frame(raw, d)
            day_ind = B.session_frame(ind, d)
            sc_day = sc.calculate_indicators(day_raw.copy())            # scanner VWAP is per-session => identical
            for hhmm in ("09:31", "09:36", "10:06", "11:00", "13:47", "15:59", "16:20"):
                ts = make_ts(d, hhmm)
                p_sc = sc._most_recent_closed_rth_pos(day_raw, ts)
                p_h = B.reference_bar(day_ind, ts)
                assert p_sc == p_h, (t, d, hhmm, p_sc, p_h)
                if p_h is None:
                    continue
                a = sc._evaluate_strong_on_bar(sc_day, p_sc)
                b_ = B.strong_components(day_ind, p_h)
                for k in ("bar_green", "above_vwap", "new_hod", "pm_high_hold", "strong"):
                    assert a[k] == b_[k], (t, d, hhmm, k, a, b_)
                n_strong += 1
                ea = sc.compute_extension_features(raw, now_ref=ts)
                eb = B.extension_features(day_ind, p_h)
                for k in ("consec_green", "range_pos", "above_orb_high"):
                    assert (ea[k] is None and eb[k] is None) or np.isclose(float(ea[k]), float(eb[k])), (t, d, hhmm, k, ea, eb)
                n_ext += 1
                # trade levels: scanner rounds entry/stop_distance/stop/target to 2dp from the latest row's Close/ATR
                bar_ts = day_ind.index[p_h]
                thru = ind.loc[:bar_ts]
                lv = sc.calculate_trade_levels(thru.iloc[-60:].copy())
                e_, s_, tg = sim.scanner_levels(np.array([thru["Close"].iloc[-1]]), np.array([thru["ATR"].iloc[-1]]))
                assert np.isclose(lv["entry"], e_[0]) and np.isclose(lv["stop_loss"], s_[0]) and np.isclose(lv["target"], tg[0]), (t, d, hhmm, lv, e_, s_, tg)
                n_lvl += 1
            # rvol: the scanner's frame is 5 sessions (4 prior + today) truncated at the reference bar
            sess = B.session_dates(raw)
            i = sess.index(d)
            frame = raw.loc[(raw.index >= pd.Timestamp(sess[i - 4], tz=ET))]
            for hhmm in ("10:06", "13:47"):
                ts = make_ts(d, hhmm)
                p_h = B.reference_bar(day_raw, ts)
                bar_ts = day_raw.index[p_h]
                sub = frame.loc[:bar_ts]
                live = sc.calculate_rvol(sub)
                mine = B.rvol_scanner(frame).loc[bar_ts]
                if np.isfinite(mine):
                    assert np.isclose(live, mine, rtol=1e-6), (t, d, hhmm, live, mine)
                    n_rvol += 1
    assert n_strong > 200 and n_ext > 200 and n_lvl > 200 and n_rvol > 50


def test_strong_table_vs_per_bar_real():
    need_data()
    for t in ("NVDA", "JPM"):
        ind = B.get_indicators(t)
        if ind is None:
            continue
        st = B.strong_table(ind)
        for d in B.session_dates(ind)[10:13]:
            day = B.session_frame(ind, d)
            sub = st.loc[day.index]
            for pos in range(0, len(day), 3):
                a = B.strong_components(day, pos); e = B.extension_features(day, pos); row = sub.iloc[pos]
                assert all(bool(row[k]) == a[k] for k in ("bar_green", "above_vwap", "new_hod", "pm_high_hold", "strong")), (t, d, pos)
                for k in ("consec_green", "range_pos", "above_orb_high"):
                    assert (e[k] is None and pd.isna(row[k])) or np.isclose(float(e[k]), float(row[k])), (t, d, pos, k)


def test_load_1d_alignment_and_daily_context():
    need_data()
    for t in ("AAPL", "SPY", "NVDA"):
        d1 = B.load_1d([t])[t]
        m5 = B.load_5m([t])[t]
        last = m5.loc[(m5.index.hour == 15) & (m5.index.minute == 55), "Close"]
        last.index = last.index.date
        n = 0
        for dt, row in d1.iloc[-40:].iterrows():
            if dt.date() in last.index:
                assert abs(row["Close"] / last[dt.date()] - 1) < 0.005, (t, dt, row["Close"], last[dt.date()])
                n += 1
        assert n >= 30
    # no look-ahead: the context row for D equals the raw features of D-1, gap uses D's open
    f = B.daily_context_table("AAPL")
    raw = B._CACHE_CTX["AAPL"][1]
    i = 300
    for k in B.CONTEXT_FIELDS:
        a, b_ = f[k].iloc[i], raw[k].iloc[i - 1]
        assert (pd.isna(a) and pd.isna(b_)) or np.isclose(a, b_), k
    d1 = B.load_1d(["AAPL"])["AAPL"]
    assert np.isclose(f["gap_pct"].iloc[i], (d1["Open"].iloc[i] / d1["Close"].iloc[i - 1] - 1) * 100)
    ctx = B.daily_context("AAPL", d1.index[i].date())
    assert ctx["ticker"] == "AAPL" and np.isclose(ctx["prev_close"], d1["Close"].iloc[i - 1])
    assert B.daily_context("NOPE", "2026-07-15") == {}
    uni = B.daily_context_universe(["AAPL", "MSFT"], dates=["2026-07-15", "2026-07-16"])
    assert len(uni) == 4 and set(uni["ticker"]) == {"AAPL", "MSFT"}


def test_market_state_and_breadth_asof():
    need_data()
    ms = B.market_state(make_ts("2026-07-15", "10:06"))
    assert ms and ms["bar_ts"] == make_ts("2026-07-15", "10:00") and "spy_ret_open_pct" in ms and "vix_chg_open_pts" in ms
    assert B.market_state(make_ts("2026-07-15", "09:34")) == {}
    tab = B.market_state_table()
    spy = B.load_5m(["SPY"])["SPY"]
    day = B.rth_slice(B.session_frame(spy, "2026-07-15"))
    assert np.isclose(tab.loc[make_ts("2026-07-15", "10:00"), "spy_ret_open_pct"], (day.loc[make_ts("2026-07-15", "10:00"), "Close"] / day["Open"].iloc[0] - 1) * 100)
    if (common.CACHE_DIR / "breadth_5m.csv.gz").exists():
        br = B.breadth(make_ts("2026-07-15", "10:06"))
        assert br["bar_ts"] == make_ts("2026-07-15", "10:00") and 0 <= br["share_above_vwap"] <= 1 and br["n"] > 150
    else:
        print("   (breadth cache absent — breadth() not checked)")


# ═══════════════════════════════════════════════════════════════════
#  4. evaluate
# ═══════════════════════════════════════════════════════════════════
def _toy_trades(seed=0, n_days=40, per_day=10, shift=0.0):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2026-06-01", periods=n_days).date
    rows = []
    for d in dates:
        for i in range(per_day):
            res = rng.choice(["WIN", "LOSS", "EOD"], p=[0.2, 0.5, 0.3])
            R = {"WIN": 2.5, "LOSS": -1.0, "EOD": rng.normal(0.2, 0.8)}[res] + shift
            rows.append({"date": d, "ticker": f"T{i}", "result": res, "R": R, "pnl_pct": R * 0.8,
                         "entry_raw": 100.0, "exit_raw": 100.0 + R * 0.8, "risk": 0.8, "score": rng.uniform() + 0.3 * R})
    rows.append({"date": dates[0], "ticker": "X", "result": "NO_DATA", "R": np.nan, "pnl_pct": np.nan,
                 "entry_raw": np.nan, "exit_raw": np.nan, "risk": np.nan, "score": 0})
    return pd.DataFrame(rows)


def test_cluster_bootstrap():
    rng = np.random.default_rng(0)
    v = rng.normal(0.3, 1.0, 2000); g = np.repeat(np.arange(50), 40)
    m, lo, hi = E.cluster_bootstrap_ci(v, g, n_boot=500)
    assert np.isclose(m, v.mean()) and lo < m < hi and (hi - lo) < 0.4
    m2, lo2, hi2 = E.cluster_bootstrap_ci(v, g, n_boot=500, stat=np.median)
    assert lo2 < m2 < hi2 and np.isclose(m2, np.median(v))
    assert E.cluster_bootstrap_ci([1, 2, 3], [1, 1, 1]) == (2.0, 2.0, 2.0)
    assert all(np.isnan(x) for x in E.cluster_bootstrap_ci([np.nan], [1]))
    # deterministic
    assert E.cluster_bootstrap_ci(v, g, n_boot=100, seed=5) == E.cluster_bootstrap_ci(v, g, n_boot=100, seed=5)


def test_summarize_and_monthly():
    t = _toy_trades()
    s = E.summarize(t, n_boot=300)
    assert s["n"] == 400 and s["n_no_entry"] == 1 and s["days"] == 40 and np.isclose(s["picks_per_day"], 10)
    assert np.isclose(s["hit_rate"] + s["stop_rate"] + s["eod_rate"], 1.0)
    assert np.isclose(s["total_R"], t["R"].sum()) and s["avgR_ci"][0] <= s["avgR"] <= s["avgR_ci"][1]
    ds = E.daily_series(t)
    assert np.isclose(s["daily_R_mean"], ds.mean()) and np.isclose(s["sharpe_like"], ds.mean() / ds.std(ddof=1) * np.sqrt(252))
    assert s["max_drawdown_R"] <= 0 and s["worst_day"] <= s["best_day"]
    cs = s["cost_sensitivity"]
    assert cs[0.0] > cs[5.0] > cs[10.0] and np.isclose(cs[0.0], s["avgR"])
    mt = E.monthly_table(t)
    assert set(mt.index) == {"2026-06", "2026-07"} and mt["n"].sum() == 400
    assert E.summarize(t.iloc[:0])["n"] == 0
    assert np.isclose(E.max_drawdown(pd.Series([1, -2, 1, -3, 5])), -4.0)


def test_temporal_split_walk_forward_compare():
    t = _toy_trades()
    sp = E.temporal_split(t, "2026-07-01")
    assert sp["train"]["n"] + sp["test"]["n"] == 400 and sp["train_df"]["date"].max() < pd.Timestamp("2026-07-01").date()
    calls = []

    def fit(train):
        calls.append((train["date"].min(), train["date"].max()))
        return float(train["score"].quantile(0.7))

    def select(model, test):
        return test[test["score"] >= model]

    tt = E.ThingsTried()
    wf = E.walk_forward(t, fit, select, train_days=20, test_days=5, step=5, things_tried=tt, verbose=False)
    folds = wf["folds"]
    assert len(folds) == 4 and tt.total == 1 and "things_tried=1" in tt.report()
    for _, f in folds.iterrows():
        assert f["train_end"] < f["test_start"]                      # no overlap, test strictly after train
    assert len(set(wf["selected"]["fold"])) == 4 and wf["pooled"]["n"] == len(wf["selected"])
    assert wf["selected"]["date"].min() >= pd.Timestamp(folds["test_start"].min()).date()
    assert len(wf["monthly"]) >= 1
    # compare: identical -> 0; shifted -> +shift per trade per day
    same = E.compare(t, t, n_boot=300)
    assert same["n_days"] == 40 and np.isclose(same["diff"], 0.0)
    better = E.compare(t, _toy_trades(shift=0.5), n_boot=300)
    assert np.isclose(better["diff"], 5.0) and better["diff_ci"][0] > 0 and better["p_boot"] < 0.05 and better["share_days_better"] == 1.0
    mean_cmp = E.compare(t, _toy_trades(shift=0.5), agg="mean", n_boot=300)
    assert np.isclose(mean_cmp["diff"], 0.5)


# ═══════════════════════════════════════════════════════════════════
#  5. all_bars_labels
# ═══════════════════════════════════════════════════════════════════
def test_all_bars_labels_real():
    need_data()
    ab = sim.all_bars_labels("AAPL", "2026-07-15")
    assert len(ab) == 60 and ab["hhmm"].iloc[0] == "09:35" and ab["hhmm"].iloc[-1] == "14:30"
    ind = B.get_indicators("AAPL")
    day = B.rth_slice(B.session_frame(ind, "2026-07-15"))
    assert ab["bar_ts"].iloc[0] == day.index[0] and ab["signal_ts"].iloc[0] == day.index[0] + BAR_TD
    assert np.isclose(ab["entry"].iloc[0], round(float(day["Close"].iloc[0]), 2))
    dist = round(float(day["ATR"].iloc[0]) * 2.0, 2)
    assert np.isclose(ab["stop"].iloc[0], round(ab["entry"].iloc[0] - dist, 2)) and np.isclose(ab["target"].iloc[0], round(ab["entry"].iloc[0] + dist * 2.5, 2))
    # the signal bar itself is never evaluated; sp_ == simulate_trade with the live-style kwargs
    for i in (0, 17, 59):
        row = ab.iloc[i]
        r = sim.simulate_trade(day, row["signal_ts"], row["entry"], row["stop"], row["target"], entry_mode="signal_price",
                               cost_bps_per_side=0, same_bar="loss", gap_aware_stops=False, exit_time="15:55")
        assert r["result"] == row["sp_result"] and np.isclose(r["R"], row["sp_R"]) and r["entry_idx"] == i + 1
        r2 = sim.simulate_trade(day, row["signal_ts"], row["entry"], row["stop"], row["target"], entry_mode="first_open",
                                cost_bps_per_side=5, same_bar="loss", gap_aware_stops=True, exit_time="15:55")
        assert r2["result"] == row["no_result"] and (pd.isna(r2["R"]) and pd.isna(row["no_R"]) or np.isclose(r2["R"], row["no_R"]))
        if r2["result"] != "NO_ENTRY":
            assert np.isclose(row["no_entry_fill"], float(day["Open"].iloc[i + 1]) * (1 + 5e-4))
    assert set(sim.ALLBARS_FEATURES).issubset(ab.columns)
    assert sim.all_bars_labels("CFLT", "2026-07-15").empty       # empty ticker -> empty frame, no error
    ab_rth = sim.all_bars_labels("AAPL", "2026-07-15", atr_kind="rth", first="10:00", last="10:30")
    assert len(ab_rth) == 7 and ab_rth["hhmm"].iloc[0] == "10:00"


# ═══════════════════════════════════════════════════════════════════
#  runner
# ═══════════════════════════════════════════════════════════════════
def main():
    tests = [(k, v) for k, v in globals().items() if k.startswith("test_") and callable(v)]
    n_pass = n_fail = n_skip = 0
    t_all = time.time()
    for name, fn in tests:
        t0 = time.time()
        try:
            fn()
            n_pass += 1
            print(f"PASS  {name}  ({time.time() - t0:.1f}s)")
        except Skip as e:
            n_skip += 1
            print(f"SKIP  {name}: {e}")
        except Exception:
            n_fail += 1
            print(f"FAIL  {name}")
            traceback.print_exc()
    print(f"\n{n_pass} passed, {n_fail} failed, {n_skip} skipped in {time.time() - t_all:.1f}s")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
