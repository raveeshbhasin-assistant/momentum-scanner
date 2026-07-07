"""
v3.8.0 unit tests — TRADEABLE tier + anti-extension shadow features.
Run from repo root with:  python -m pytest tests/test_v38_tradeable.py -v

Covers:
  Phase A — config.is_tradeable (STRONG + 09:30–10:00 window, single
            source of truth) and its relationship to is_elite
  Phase B — config.is_anti_ext tri-state logic
  Phase C — scanner.compute_extension_features on crafted DataFrames
            (look-ahead safety: OR gate, running-range, consec greens)
  Phase D — history.add_signals_to_daily persists the new fields
"""
import os, sys
import pandas as pd
import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config


# ────────────────────────────────────────────────────────────────
# Phase A — TRADEABLE tier
# ────────────────────────────────────────────────────────────────

def _sig(strong=True, ts="2026-07-06T09:45:00-04:00", **kw):
    s = {"strong_signal": strong, "timestamp": ts}
    s.update(kw)
    return s


def test_config_v38_constants():
    assert hasattr(config, "TRADEABLE_ENABLED")
    assert hasattr(config, "TRADEABLE_WINDOW_START")
    assert hasattr(config, "TRADEABLE_WINDOW_END")
    assert hasattr(config, "ANTIEXT_MAX_CONSEC_GREEN")
    assert hasattr(config, "ANTIEXT_MAX_RANGE_POS")
    assert config.TRADEABLE_WINDOW_START == (9, 30)
    assert config.TRADEABLE_WINDOW_END == (10, 0)


def test_tradeable_strong_in_window():
    assert config.is_tradeable(_sig(strong=True, ts="2026-07-06T09:35:00-04:00"))


def test_tradeable_window_boundaries_inclusive():
    # Matches ELITE window semantics: both endpoints inclusive.
    assert config.is_tradeable(_sig(ts="2026-07-06T09:30:00-04:00"))
    assert config.is_tradeable(_sig(ts="2026-07-06T10:00:00-04:00"))
    assert not config.is_tradeable(_sig(ts="2026-07-06T10:01:00-04:00"))
    assert not config.is_tradeable(_sig(ts="2026-07-06T09:29:00-04:00"))


def test_tradeable_requires_strong():
    assert not config.is_tradeable(_sig(strong=False, ts="2026-07-06T09:35:00-04:00"))


def test_tradeable_batch_time_fallback():
    # No ISO timestamp — falls back to batch_time "HH:MM"
    assert config.is_tradeable({"strong_signal": True, "batch_time": "09:45"})
    assert not config.is_tradeable({"strong_signal": True, "batch_time": "11:00"})
    # Neither field → not tradeable
    assert not config.is_tradeable({"strong_signal": True})


def test_elite_is_subset_of_tradeable():
    """Every ELITE pick must be TRADEABLE: same window, STRONG required.
    Build a signal passing all v3.7.5 elite gates and check both flags."""
    s = {
        "strong_signal": True,
        "timestamp": "2026-07-06T09:40:00-04:00",
        "leadership": {"label": None},          # → category D
        "rvol": 2.5,
        "indicators": {"rsi": 70},
        "trade": {"entry": 100.0, "stop_loss": 98.8, "risk_pct": 1.2},
    }
    assert config.is_elite(s)
    assert config.is_tradeable(s)


def test_tradeable_disabled_short_circuits(monkeypatch=None):
    orig = config.TRADEABLE_ENABLED
    try:
        config.TRADEABLE_ENABLED = False
        assert not config.is_tradeable(_sig())
    finally:
        config.TRADEABLE_ENABLED = orig


# ────────────────────────────────────────────────────────────────
# Phase B — anti-extension tri-state
# ────────────────────────────────────────────────────────────────

def test_anti_ext_none_when_features_missing():
    assert config.is_anti_ext({}) is None
    assert config.is_anti_ext({"extension": {}}) is None
    assert config.is_anti_ext({"extension": {"consec_green": None, "range_pos": None}}) is None


def test_anti_ext_true_on_pullback_shape():
    assert config.is_anti_ext({"extension": {"consec_green": 1, "range_pos": 0.6}}) is True
    assert config.is_anti_ext({"extension": {"consec_green": 2, "range_pos": 0.9}}) is True


def test_anti_ext_false_when_extended():
    # 3+ consecutive greens = extended
    assert config.is_anti_ext({"extension": {"consec_green": 3, "range_pos": 0.5}}) is False
    # top of running range = extended
    assert config.is_anti_ext({"extension": {"consec_green": 1, "range_pos": 0.95}}) is False


def test_anti_ext_partial_features():
    # Only one feature available — judge on what exists
    assert config.is_anti_ext({"extension": {"consec_green": 4, "range_pos": None}}) is False
    assert config.is_anti_ext({"extension": {"consec_green": None, "range_pos": 0.3}}) is True


# ────────────────────────────────────────────────────────────────
# Phase C — compute_extension_features on crafted bars
# ────────────────────────────────────────────────────────────────

def _mk_df(bars, date="2026-07-06"):
    """bars = list of (hh, mm, o, h, l, c). Volume constant."""
    idx = pd.DatetimeIndex(
        [pd.Timestamp(f"{date} {hh:02d}:{mm:02d}", tz=config.ET) for hh, mm, *_ in bars]
    )
    df = pd.DataFrame(
        {
            "Open":  [b[2] for b in bars],
            "High":  [b[3] for b in bars],
            "Low":   [b[4] for b in bars],
            "Close": [b[5] for b in bars],
            "Volume": [1000.0] * len(bars),
        },
        index=idx,
    )
    return df


def test_extension_basic_consec_green_and_range():
    from scanner import compute_extension_features
    # Four RTH bars; ref = 09:45 bar (closed as of 09:52).
    # Greens: 09:35 red, 09:40 green, 09:45 green → consec_green = 2.
    df = _mk_df([
        (9, 30, 100.0, 101.0,  99.5, 100.5),   # green (not in streak: broken later)
        (9, 35, 100.5, 100.8,  99.8, 100.0),   # red
        (9, 40, 100.0, 100.9, 100.0, 100.8),   # green
        (9, 45, 100.8, 101.5, 100.7, 101.2),   # green ← reference bar
    ])
    out = compute_extension_features(df, now_ref="2026-07-06T09:52:00")
    assert out["consec_green"] == 2
    # running range: hi=101.5, lo=99.5 → (101.2-99.5)/2.0 = 0.85
    assert abs(out["range_pos"] - 0.85) < 0.001
    # ref bar is the 4th RTH bar (index 3) → OR closed; orb_high = max(H of first 3) = 101.0
    assert out["above_orb_high"] == 1


def test_extension_orb_none_until_closed():
    from scanner import compute_extension_features
    # Only 2 RTH bars closed — the 09:30–09:45 OR is not complete: no ORB flag.
    df = _mk_df([
        (9, 30, 100.0, 101.0, 99.5, 100.5),
        (9, 35, 100.5, 101.2, 100.3, 101.0),
    ])
    out = compute_extension_features(df, now_ref="2026-07-06T09:42:00")
    assert out["above_orb_high"] is None
    assert out["consec_green"] == 2


def test_extension_premarket_bars_excluded():
    from scanner import compute_extension_features
    # PM bar with a huge high must NOT pollute the running RTH range.
    df = _mk_df([
        (8, 55, 100.0, 150.0,  99.0, 100.0),   # pre-market spike
        (9, 30, 100.0, 101.0,  99.5, 100.5),
        (9, 35, 100.5, 101.2, 100.3, 101.0),
        (9, 40, 101.0, 101.4, 100.9, 101.3),
        (9, 45, 101.3, 102.0, 101.2, 101.9),   # ref bar
    ])
    out = compute_extension_features(df, now_ref="2026-07-06T09:52:00")
    # RTH range: hi=102.0, lo=99.5 → (101.9-99.5)/2.5 = 0.96 — NOT diluted by the 150 PM high
    assert abs(out["range_pos"] - 0.96) < 0.001
    assert out["consec_green"] == 4  # all four RTH bars green


def test_extension_no_closed_bar_returns_nones():
    from scanner import compute_extension_features
    df = _mk_df([(9, 30, 100.0, 101.0, 99.5, 100.5)])
    # 09:33 — the 09:30 bar hasn't closed yet
    out = compute_extension_features(df, now_ref="2026-07-06T09:33:00")
    assert out == {"consec_green": None, "range_pos": None, "above_orb_high": None}


def test_extension_matches_strong_reference_bar():
    """Extension features must evaluate the SAME bar compute_strong_signal
    picks — the most-recently-closed RTH bar, not the partial live bar."""
    from scanner import compute_extension_features, _most_recent_closed_rth_pos
    df = _mk_df([
        (9, 30, 100.0, 101.0, 99.5, 100.5),
        (9, 35, 100.5, 101.2, 100.3, 101.0),
        (9, 40, 101.0, 101.4, 100.9, 101.3),   # ← most-recently-closed at 09:47
        (9, 45, 101.3, 102.0, 101.2, 101.9),   # partial (would close 09:50)
    ])
    ref = pd.Timestamp("2026-07-06 09:47", tz=config.ET)
    assert _most_recent_closed_rth_pos(df, ref) == 2
    out = compute_extension_features(df, now_ref="2026-07-06T09:47:00")
    # range through bar 2 only: hi=101.4, lo=99.5 → (101.3-99.5)/1.9 ≈ 0.947
    assert abs(out["range_pos"] - (101.3 - 99.5) / 1.9) < 0.001


# ────────────────────────────────────────────────────────────────
# Phase D — persistence through the slim daily record
# ────────────────────────────────────────────────────────────────

def test_daily_record_persists_v38_fields(tmp_path, monkeypatch):
    import history
    monkeypatch.setattr(history, "DATA_DIR", tmp_path)  # Path, matches history.DATA_DIR type
    sig = {
        "ticker": "TEST", "price": 100.0, "composite_score": 65,
        "trade": {"entry": 100.0, "target": 105.0, "stop_loss": 98.0,
                  "risk_reward_ratio": 2.5, "resistance_target": 0,
                  "resistance_level": ""},
        "indicators": {"rsi": 70},
        "strong_signal": True,
        "strong_components": {"bar_green": True, "above_vwap": True,
                              "new_hod": True, "pm_high_hold": True,
                              "complete_bar_used": True},
        "elite": True,
        "tradeable": True,
        "anti_ext": False,
        "extension": {"consec_green": 3, "range_pos": 0.95, "above_orb_high": 1},
    }
    history.add_signals_to_daily([sig])
    from datetime import datetime
    date_str = datetime.now(config.ET).strftime("%Y-%m-%d")
    rows = history.load_daily_finds(date_str)
    assert len(rows) == 1
    r = rows[0]
    assert r["elite"] is True
    assert r["tradeable"] is True
    assert r["anti_ext"] is False
    assert r["extension"]["consec_green"] == 3
    assert abs(r["extension"]["range_pos"] - 0.95) < 1e-9


# ────────────────────────────────────────────────────────────────
# Phase E — v3.8.1 hotfix: normalize_entry must NOT stamp default
# tier flags onto rows that never carried them (persisted-flag-wins
# consumers like performance.html _isElite would stop deriving and
# every historical ELITE badge would vanish — the v3.8.0 regression).
# ────────────────────────────────────────────────────────────────

def _base_perf_raw(**kw):
    d = {"ticker": "T", "score": 60, "entry": 100, "stop": 99, "target": 102,
         "batch_time": "09:45", "result": "WIN", "strong_signal": True}
    d.update(kw)
    return d


def test_normalize_entry_no_default_stamp_on_old_rows():
    from performance_engine import normalize_entry
    out = normalize_entry(_base_perf_raw())          # pre-v3.8.0 row shape
    assert "elite" not in out, "absent elite must stay absent (unknown ≠ False)"
    assert "tradeable" not in out
    assert "extension" not in out
    assert out["anti_ext"] is None                   # None = unknown, allowed


def test_normalize_entry_passes_through_lived_flags():
    from performance_engine import normalize_entry
    out = normalize_entry(_base_perf_raw(
        elite=True, tradeable=False, anti_ext=False,
        extension={"consec_green": 3, "range_pos": 0.95, "above_orb_high": 1},
    ))
    assert out["elite"] is True
    assert out["tradeable"] is False                 # lived False survives
    assert out["anti_ext"] is False
    assert out["extension"]["consec_green"] == 3
    # idempotent on re-normalize
    from performance_engine import normalize_entry as n2
    again = n2(out)
    assert again["elite"] is True and again["tradeable"] is False


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
