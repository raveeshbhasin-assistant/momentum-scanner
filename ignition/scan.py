"""
ignition/scan.py — daily Ignition Watch scan (standalone module).

Scans the S&P 500 + 400 universe (universe.txt) for the IGNITION signal and
writes ignition/data/latest.json for themes_web's /ignition page. Run as a
subprocess by themes_web/scheduler.py (weekdays 17:00 ET) or by hand:

    python ignition/scan.py

Signal (backtest 2016-2026, 903 tickers, 2.18M ticker-days — see README.md):
    IGNITION = 5-day return > +12%
               AND 21-day avg volume > 1.5x its 126-day avg
               AND 50-DMA > 200-DMA
               (price > $3, 21-day avg dollar volume > $5M)
    After a fire: 26.9% traded >= +40% above entry within 63 sessions vs
    4.6% baseline; mean 63-session return +17.0% vs +4.5%; 22.7% saw a
    -20% drawdown first vs 11.7% baseline.

Fires are derived from price history, so the 90-session fire log rebuilds
itself after a redeploy wipes the container. Only the "first seen" date and
NEW badge use the previous latest.json, with a days-ago fallback when absent.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
DATA_DIR = HERE / "data"
LATEST = DATA_DIR / "latest.json"
HISTORY_DIR = DATA_DIR / "history"
ET = ZoneInfo("America/New_York")

LOOKBACK_SESSIONS = 90   # fires shown on the page
NEW_FALLBACK_DAYS = 3    # NEW = fired within this many sessions when no prior scan exists
BATCH = 150

R5_MIN = 0.12
VOLR_MIN = 1.5
MIN_PRICE = 3.0
MIN_DOLLAR_VOL = 5e6

BACKTEST = {
    "window": "2016-2026, S&P 500+400 (903 tickers), 2.18M ticker-days, 2,271 fires",
    "p_plus40_63d": 26.9, "p_plus40_baseline": 4.6,
    "p_plus80_63d": 8.2, "p_plus80_baseline": 0.6,
    "mean_63d": 17.0, "mean_63d_baseline": 4.5,
    "p_dd20_63d": 22.7, "p_dd20_baseline": 11.7,
    "years_positive": "10 of 11",
}


def load_universe() -> list[str]:
    tickers: set[str] = set()
    for line in (HERE / "universe.txt").read_text(encoding="utf-8").splitlines():
        if line.startswith("#"):
            continue
        tickers.update(t for t in line.split() if t)
    return sorted(tickers)


def download(tickers: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """~2.5 years of adjusted daily closes + volume (252d lookback + 200dma)."""
    import yfinance as yf

    start = (datetime.now(timezone.utc) - timedelta(days=920)).strftime("%Y-%m-%d")
    closes, vols = [], []
    for i in range(0, len(tickers), BATCH):
        d = yf.download(tickers[i:i + BATCH], start=start, auto_adjust=True,
                        progress=False, threads=True)
        if d.empty:
            continue
        closes.append(d["Close"].astype("float32"))
        vols.append(d["Volume"].astype("float32"))
    if not closes:
        raise RuntimeError("yfinance returned no data for any batch")
    close = pd.concat(closes, axis=1)
    vol = pd.concat(vols, axis=1)
    close = close.loc[:, ~close.columns.duplicated()].sort_index()
    vol = vol.loc[:, ~vol.columns.duplicated()].sort_index()
    # Drop a trailing row that only a handful of tickers have (partial intraday bar).
    if len(close) > 1 and close.iloc[-1].notna().mean() < 0.5:
        close, vol = close.iloc[:-1], vol.iloc[:-1]
    return close, vol


def _pct(x: float) -> float | None:
    return None if x is None or not np.isfinite(x) else round(float(x) * 100, 1)


def compute(close: pd.DataFrame, vol: pd.DataFrame, prev: dict | None) -> dict:
    r5 = close.pct_change(5, fill_method=None)
    r63 = close.pct_change(63, fill_method=None)
    r252 = close.pct_change(252, fill_method=None)
    volr = vol.rolling(21).mean() / vol.rolling(126).mean()
    golden = close.rolling(50).mean() > close.rolling(200).mean()
    dollar_vol = (close * vol).rolling(21).mean()
    hi252 = close.rolling(252).max()
    rs63 = r63.sub(r63.mean(axis=1), axis=0)
    valid = (close > MIN_PRICE) & (dollar_vol > MIN_DOLLAR_VOL) & r252.notna()
    sig = (r5 > R5_MIN) & (volr > VOLR_MIN) & golden & valid

    asof = close.index[-1]
    scan_date = str(asof.date())
    prev_first_seen: dict[str, str] = (prev or {}).get("first_seen", {})
    prev_keys = set(prev_first_seen) if prev else None

    # A halted or late-printing ticker can be missing today's bar; carry its
    # last close so "now" columns stay numeric.
    last_px = close.ffill(limit=5).iloc[-1]
    last_r63 = close.ffill(limit=5).pct_change(63, fill_method=None).iloc[-1]
    last_rs63 = last_r63 - last_r63.mean()

    fires, first_seen = [], {}
    recent = sig.iloc[-LOOKBACK_SESSIONS:]
    n = len(close.index)
    for t in recent.columns[recent.any().values]:
        if not np.isfinite(last_px[t]):
            continue
        col = recent[t]
        d0 = col[col].index[-1]
        i = close.index.get_loc(d0)
        key = f"{t}|{d0.date()}"
        days_ago = n - 1 - i
        is_new = (key not in prev_keys) if prev_keys is not None else days_ago < NEW_FALLBACK_DAYS
        first_seen[key] = prev_first_seen.get(key, scan_date)
        ret_since = last_px[t] / close[t].iloc[i] - 1
        fires.append({
            "ticker": t,
            "fired": str(d0.date()),
            "days_ago": int(days_ago),
            "first_seen": first_seen[key],
            "new": bool(is_new),
            "r5_at_fire": _pct(r5[t].iloc[i]),
            "volr_at_fire": round(float(volr[t].iloc[i]), 2),
            "price_at_fire": round(float(close[t].iloc[i]), 2),
            "price": round(float(last_px[t]), 2),
            "ret_since": _pct(ret_since),
            "r63": _pct(last_r63[t]) or 0.0,
            "rs63": _pct(last_rs63[t]) or 0.0,
            "pct_from_hi": _pct(last_px[t] / hi252[t].ffill().iloc[-1] - 1) or 0.0,
            "hit_40": bool(close[t].iloc[i:].max() / close[t].iloc[i] - 1 >= 0.40),
            "active": bool(days_ago <= 63),
        })
    fires.sort(key=lambda f: (f["fired"], f["r5_at_fire"] or 0), reverse=True)

    last = pd.DataFrame({
        "price": close.iloc[-1], "r63": r63.iloc[-1], "rs63": rs63.iloc[-1],
        "volr": volr.iloc[-1], "pct_hi": close.iloc[-1] / hi252.iloc[-1] - 1,
        "golden": golden.iloc[-1], "valid": valid.iloc[-1],
    })
    lead = last[last.valid & last.golden & (last.pct_hi > -0.15) & (last.rs63 > 0.15)]
    lead = lead.sort_values("rs63", ascending=False).head(15)
    leaders = [{
        "ticker": t, "price": round(float(r.price), 2), "r63": _pct(r.r63),
        "rs63": _pct(r.rs63), "volr": round(float(r.volr), 2), "pct_from_hi": _pct(r.pct_hi),
    } for t, r in lead.iterrows()]

    # Near misses: uptrend + heavy volume, weekly gain 8-12% — the watch-for-tomorrow list.
    near = (r5.iloc[-1].between(0.08, R5_MIN)) & (volr.iloc[-1] > 1.3) & golden.iloc[-1] & valid.iloc[-1]
    near_misses = sorted(({
        "ticker": t, "r5": _pct(r5[t].iloc[-1]), "volr": round(float(volr[t].iloc[-1]), 2),
        "price": round(float(close[t].iloc[-1]), 2),
    } for t in near[near].index), key=lambda x: -(x["r5"] or 0))[:12]

    active = [f for f in fires if f["active"]]
    return {
        "asof": scan_date,
        "scanned_at": datetime.now(ET).strftime("%Y-%m-%d %H:%M ET"),
        "universe": int(close.shape[1]),
        "rule": {"r5_min": R5_MIN, "volr_min": VOLR_MIN, "min_price": MIN_PRICE,
                 "min_dollar_vol": MIN_DOLLAR_VOL, "lookback_sessions": LOOKBACK_SESSIONS},
        "backtest": BACKTEST,
        "summary": {
            "fires_logged": len(fires),
            "active": len(active),
            "new": [f["ticker"] for f in fires if f["new"]],
            "active_hit_40": sum(f["hit_40"] for f in active),
        },
        "fires": fires,
        "leaders": leaders,
        "near_misses": near_misses,
        "first_seen": first_seen,
    }


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    prev = None
    if LATEST.exists():
        try:
            prev = json.loads(LATEST.read_text(encoding="utf-8"))
        except Exception:
            prev = None

    tickers = load_universe()
    close, vol = download(tickers)
    result = compute(close, vol, prev)

    tmp = LATEST.with_suffix(".tmp")
    tmp.write_text(json.dumps(result, indent=1), encoding="utf-8")
    tmp.replace(LATEST)
    (HISTORY_DIR / f"{result['asof']}.json").write_text(
        json.dumps({k: result[k] for k in ("asof", "scanned_at", "summary", "fires")}, indent=1),
        encoding="utf-8")

    s = result["summary"]
    print(f"ignition scan asof={result['asof']} universe={result['universe']} "
          f"fires={s['fires_logged']} active={s['active']} new={','.join(s['new']) or '-'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
