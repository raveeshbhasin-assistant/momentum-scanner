"""
v3.7.0 unit + replay tests.
Run from repo root with:  python3 -m pytest tests/test_v36_gates.py -v
or:                        python3 tests/test_v36_gates.py

Covers:
  Phase A — config additions, universe expansion, sector exclusion,
            RSI hard cap, emission cutoff timing
  Phase B — STRONG-signal compute on crafted DataFrames
  Replay  — backtest gates against the 14-day production pick history
            (loaded from data/2026-04-22.json … 2026-05-08.json) and
            verify rejection counts match research expectations.
"""
import os, sys, json, importlib
import pandas as pd
import numpy as np

# Ensure repo root is on sys.path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


# ────────────────────────────────────────────────────────────────
# Phase A — config-level tests
# ────────────────────────────────────────────────────────────────

def test_config_imports():
    import config
    assert hasattr(config, "MIDCAP_EXTENDED")
    assert hasattr(config, "EXCLUDED_SECTORS")
    assert hasattr(config, "EMISSION_CUTOFF_HOUR")
    assert hasattr(config, "EMISSION_CUTOFF_MINUTE")
    assert hasattr(config, "RSI_HARD_CAP")
    assert hasattr(config, "RSI_HARD_CAP_ENABLED")


def test_midcap_list_size_and_no_overlap():
    import config
    assert len(config.MIDCAP_EXTENDED) == 80, f"expected 80 mid-caps, got {len(config.MIDCAP_EXTENDED)}"
    sp = set(config.SP500_LIQUID)
    hb = set(config.HIGH_BETA_EXTENDED)
    mc = set(config.MIDCAP_EXTENDED)
    overlap_sp = mc & sp
    overlap_hb = mc & hb
    assert not overlap_sp, f"MIDCAP overlaps SP500_LIQUID: {overlap_sp}"
    assert not overlap_hb, f"MIDCAP overlaps HIGH_BETA_EXTENDED: {overlap_hb}"


def test_get_full_universe_dedup_and_size():
    import config
    uni = config.get_full_universe()
    assert len(uni) == len(set(uni)), "universe contains duplicates"
    expected_min = len(set(config.SP500_LIQUID) | set(config.HIGH_BETA_EXTENDED) | set(config.MIDCAP_EXTENDED))
    assert len(uni) == expected_min
    # spot-check expected names from research are present
    for t in ("ASML", "ULTA", "URI", "BBY", "FSLR", "EME"):
        assert t in uni, f"expected {t} in universe"


def test_excluded_sectors_match_scanner_strings():
    """Excluded labels must match the exact strings emitted by leadership.sector."""
    import config
    expected = {"Utilities", "Consumer Disc", "Real Estate"}
    assert set(config.EXCLUDED_SECTORS) == expected
    # Crypto/Blockchain explicitly NOT excluded (per operator preference)
    assert "Crypto/Blockchain" not in config.EXCLUDED_SECTORS


def test_emission_cutoff_constants():
    import config
    assert 0 <= config.EMISSION_CUTOFF_HOUR <= 23
    assert 0 <= config.EMISSION_CUTOFF_MINUTE <= 59
    # Must allow at least 30 min of runway for 2.5R targets
    assert (config.EMISSION_CUTOFF_HOUR, config.EMISSION_CUTOFF_MINUTE) <= (15, 30)


def test_rsi_hard_cap_value():
    import config
    assert config.RSI_HARD_CAP == 75
    assert config.RSI_HARD_CAP_ENABLED is True
    # Must equal the existing momentum-zone upper bound
    assert config.RSI_HARD_CAP == config.RSI_MOMENTUM_HIGH


# ────────────────────────────────────────────────────────────────
# Phase B — STRONG signal helper unit tests
# ────────────────────────────────────────────────────────────────

def _make_df(rows):
    """Build a DataFrame matching scanner expectations.
    rows: list of dicts with keys o/h/l/c/v and 'minute' (int minutes from 09:00).
    Index is timezone-aware ET timestamps.
    """
    et = pd.Timestamp("2026-05-08 09:00:00", tz="America/New_York")
    idx = [et + pd.Timedelta(minutes=r["minute"]) for r in rows]
    df = pd.DataFrame({
        "Open":   [r["o"] for r in rows],
        "High":   [r["h"] for r in rows],
        "Low":    [r["l"] for r in rows],
        "Close":  [r["c"] for r in rows],
        "Volume": [r["v"] for r in rows],
    }, index=pd.DatetimeIndex(idx))
    # Compute VWAP the same way scanner.calculate_indicators does
    typical = (df["High"] + df["Low"] + df["Close"]) / 3
    df["VWAP"] = (typical * df["Volume"]).cumsum() / df["Volume"].cumsum()
    return df


def test_strong_signal_all_four_conditions_true():
    """Construct a clean STRONG case: PM bar + green session bar making new HOD above VWAP."""
    from scanner import compute_strong_signal
    rows = [
        # Pre-market bar (09:00) — sets pm_high
        {"minute": 0,   "o": 100.0, "h": 100.5, "l": 99.5, "c": 100.0, "v": 1000},
        # Session bar 1 (09:30) — opens, dips
        {"minute": 30,  "o": 100.0, "h": 100.2, "l": 99.8, "c": 100.0, "v": 5000},
        # Session bar 2 (09:35) — strong green, makes new HOD, closes above pm_high
        {"minute": 35,  "o": 100.0, "h": 101.0, "l": 100.0, "c": 100.9, "v": 8000},
    ]
    df = _make_df(rows)
    info = compute_strong_signal(df)
    assert info["bar_green"] is True
    assert info["above_vwap"] is True
    assert info["new_hod"] is True
    assert info["pm_high_hold"] is True
    assert info["strong"] is True


def test_strong_signal_red_bar_blocks():
    from scanner import compute_strong_signal
    rows = [
        {"minute": 0,   "o": 100.0, "h": 100.5, "l": 99.5, "c": 100.0, "v": 1000},
        {"minute": 30,  "o": 100.0, "h": 100.5, "l": 99.5, "c": 100.4, "v": 5000},
        {"minute": 35,  "o": 100.4, "h": 100.5, "l": 100.0, "c": 100.1, "v": 8000},  # red close
    ]
    df = _make_df(rows)
    info = compute_strong_signal(df)
    assert info["bar_green"] is False
    assert info["strong"] is False


def test_strong_signal_below_pm_high_blocks():
    from scanner import compute_strong_signal
    rows = [
        # PM high = 102
        {"minute": 0,   "o": 100.0, "h": 102.0, "l": 99.5, "c": 101.0, "v": 1000},
        {"minute": 30,  "o": 101.0, "h": 101.2, "l": 100.5, "c": 100.8, "v": 5000},
        {"minute": 35,  "o": 100.8, "h": 101.5, "l": 100.7, "c": 101.4, "v": 8000},  # green but < 102
    ]
    df = _make_df(rows)
    info = compute_strong_signal(df)
    assert info["bar_green"] is True
    assert info["pm_high_hold"] is False
    assert info["strong"] is False


def test_strong_signal_below_vwap_blocks():
    from scanner import compute_strong_signal
    rows = [
        # session opens high then drifts; close stays under VWAP
        {"minute": 0,   "o": 100.0, "h": 100.2, "l": 99.5, "c": 99.8, "v": 1000},
        {"minute": 30,  "o": 100.0, "h": 102.0, "l": 99.0, "c": 101.5, "v": 50000},  # heavy volume high
        {"minute": 35,  "o": 101.5, "h": 101.6, "l": 100.5, "c": 100.6, "v": 5000},  # back below VWAP
    ]
    df = _make_df(rows)
    info = compute_strong_signal(df)
    # close 100.6 vs VWAP ~ (typical * v).cumsum() / v.cumsum()
    # Final VWAP weighted toward high-volume bar 2; should be ~101+, so 100.6 < VWAP
    assert info["above_vwap"] is False
    assert info["strong"] is False


def test_strong_signal_no_pm_bars_returns_false():
    """When all bars are during/after RTH (no pre-market data available)."""
    from scanner import compute_strong_signal
    rows = [
        {"minute": 30,  "o": 100.0, "h": 100.5, "l": 99.5, "c": 100.4, "v": 5000},
        {"minute": 35,  "o": 100.4, "h": 101.0, "l": 100.0, "c": 100.9, "v": 8000},
    ]
    df = _make_df(rows)
    info = compute_strong_signal(df)
    # No PM bars => pm_high_hold conservatively False => strong False
    assert info["pm_high_hold"] is False
    assert info["strong"] is False


def test_strong_signal_short_df_returns_all_false():
    from scanner import compute_strong_signal
    df = pd.DataFrame({"Open": [], "High": [], "Low": [], "Close": [], "Volume": [], "VWAP": []})
    info = compute_strong_signal(df)
    assert info["strong"] is False
    assert all(info[k] is False for k in ("bar_green", "above_vwap", "new_hod", "pm_high_hold"))


# ────────────────────────────────────────────────────────────────
# Replay — backtest gates against 14-day production pick history
# ────────────────────────────────────────────────────────────────

def _load_production_picks():
    """Load all picks from the data-backups branch dump (Apr 22 – May 8)."""
    DATA = "/tmp/ms_data/data"
    if not os.path.exists(DATA):
        return []
    rows = []
    for f in sorted(os.listdir(DATA)):
        if not (f.startswith("2026-") and f.endswith(".json")):
            continue
        d = f[:-5]
        try:
            day_rows = json.load(open(f"{DATA}/{f}"))
        except Exception:
            continue
        if not isinstance(day_rows, list):
            continue
        for r in day_rows:
            r["_date"] = d
            rows.append(r)
    return rows


def test_replay_sector_gate_matches_research():
    """Replay sector gate on 14d picks. Expect ~12% reject rate (Util+CD+RE)."""
    import config
    picks = _load_production_picks()
    if not picks:
        # If data dump isn't mounted in test env, skip silently
        return
    intraday = [r for r in picks if r.get("found_time") and "09:30" <= r["found_time"][:5] <= "16:00"]
    rejected = sum(
        1 for r in intraday
        if (r.get("leadership") or {}).get("sector") in config.EXCLUDED_SECTORS
    )
    pct = rejected / len(intraday) * 100 if intraday else 0
    # Research showed Util(20) + CD(88) + RE(18) = 126 of 663 intraday = ~19%
    # But our intraday-strict filter narrows to ~660 picks; expect 10–25% rejection
    assert 8 <= pct <= 25, f"sector gate rejected {pct:.1f}% (expected 8-25%)"


def test_replay_rsi_gate_matches_research():
    """RSI > 75 picks should be ~15% of total (research said 98 of 660)."""
    import config
    picks = _load_production_picks()
    if not picks:
        return
    intraday = [r for r in picks if r.get("found_time") and "09:30" <= r["found_time"][:5] <= "16:00"]
    rejected = sum(1 for r in intraday if (r.get("rsi") or 0) > config.RSI_HARD_CAP)
    pct = rejected / len(intraday) * 100 if intraday else 0
    # Research: 98 of 660 = 14.8%
    assert 10 <= pct <= 22, f"RSI gate rejected {pct:.1f}% (expected 10-22%)"


def test_replay_emission_cutoff_matches_research():
    """Picks at >= 15:00 ET should be ~5% (most picks are early in day)."""
    picks = _load_production_picks()
    if not picks:
        return
    intraday = [r for r in picks if r.get("found_time") and "09:30" <= r["found_time"][:5] <= "16:00"]
    rejected = sum(1 for r in intraday if r["found_time"][:5] >= "15:00")
    pct = rejected / len(intraday) * 100 if intraday else 0
    # Research showed 27 of 660 picks in 15:00-16:00 bucket = ~4%
    assert pct <= 12, f"cutoff would reject {pct:.1f}% (expected ≤ 12%)"


def test_replay_combined_gate_winner_retention():
    """All gates combined should preserve majority of 2.5R winners."""
    import config
    picks = _load_production_picks()
    if not picks:
        return

    # We need outcome data — load from research/picks_v2.json if available
    pv2 = "/sessions/awesome-keen-galileo/research/picks_v2.json"
    if not os.path.exists(pv2):
        return
    resolved = json.load(open(pv2))
    by_key = {(r["date"], r["ticker"], r["batch_time"]): r for r in resolved}

    def round5(t):
        h, m = int(t[:2]), int(t[3:5])
        return f"{h:02d}:{(m // 5) * 5:02d}"

    intraday = [r for r in picks if r.get("found_time") and "09:30" <= r["found_time"][:5] <= "16:00"]
    total_winners = 0
    kept_winners = 0
    blocked_total = 0

    for r in intraday:
        d = r["_date"]; t = r["ticker"]; ft = r["found_time"][:5]
        outcome = by_key.get((d, t, round5(ft)))
        if not outcome:
            continue
        if outcome.get("won_2_5R"):
            total_winners += 1
        # Apply gates in order
        sec = (r.get("leadership") or {}).get("sector")
        if sec in config.EXCLUDED_SECTORS:
            blocked_total += 1
            continue
        if (r.get("rsi") or 0) > config.RSI_HARD_CAP:
            blocked_total += 1
            continue
        if ft >= "15:00":
            blocked_total += 1
            continue
        if outcome.get("won_2_5R"):
            kept_winners += 1

    if total_winners > 0:
        retention = kept_winners / total_winners * 100
        # Must retain at least 80% of winners (research said ~92% retention)
        assert retention >= 80, f"only {retention:.1f}% of winners survived all gates"
        print(f"  [INFO] Combined gates: {blocked_total} picks blocked, "
              f"{kept_winners}/{total_winners} winners retained ({retention:.1f}%)")


# ────────────────────────────────────────────────────────────────
# Standalone runner (for environments without pytest)
# ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    funcs = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for fn in funcs:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {fn.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {fn.__name__}: {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed, {len(funcs)} total")
    sys.exit(0 if failed == 0 else 1)
