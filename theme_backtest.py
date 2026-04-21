"""
ThemeHunter Backtest v0.1 — Intraday Replay
─────────────────────────────────────────────
Answers two questions for a given trading day:
  1. At what time of day would ThemeHunter have flagged each pick?
  2. Given the pick's own stop/target rules, how did the trade resolve?

Method:
  • SNAPSHOT PASS — call run_theme_scan(as_of_hhmm=t) for t in
    ["09:45","10:15","10:45","11:15","11:45","13:30","14:00",
     "14:30","15:00","15:30"]. Record the EARLIEST time each ticker
     qualifies (score ≥ min_score). Capture that snapshot's full
     signal (stop_distance_pct, target_R_pct, bucket, theme).
  • REPLAY PASS — fetch 5m bars for each qualifier, find the bar at
    or just after the qualifying snapshot time, and simulate:
       entry_px    = that bar's close
       stop_px     = entry × (1 − stop_distance_pct/100)
       target_px   = entry × (1 + target_R_pct/100)
    Walk bars forward:
       bar.low ≤ stop_px   → LOSS at stop_px
       bar.high ≥ target_px → WIN at target_px
       neither by 15:55     → EOD at last close
    Record exit time, exit price, R multiple, pnl_pct.

Outputs (JSON, rendered into templates/theme_backtest.html):
  summary { trades, winners, losers, eod, win_rate, total_R, avg_R,
            best_R, worst_R, avg_minutes_held }
  timeline [ {time, count} buckets of entries ]
  trades   [ per-trade detail ]
  by_bucket {A|B|C: {...}}
  by_theme  {theme: {...}}
  by_tier   {primary|secondary: {...}}

Design intent:
  • Snapshot cadence (30-min) is coarse on purpose — it models how a
    trader checks the scanner, not a tick-level oracle
  • Entries are at the snapshot's next-5m-bar close, which is
    conservative vs. the scanner's "news-wire +3min" trigger
  • Stops/targets come from the signal itself — each bucket has
    different risk profiles (theme leader 2.0% / gap+news 2.5% /
    low-float 3.5%), so R is apples-to-apples across tiers
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Any

import pandas as pd

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except ImportError:
    import pytz
    ET = pytz.timezone("America/New_York")

from theme_scanner import (
    run_theme_scan,
    _fetch_intraday_5m,
    _today_bars,
    _today_et,
)


def _to_et(ts) -> pd.Timestamp:
    """Convert a tz-aware pandas Timestamp to ET. If naive, assume UTC."""
    ts = pd.Timestamp(ts)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(ET)

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────

DATA_DIR = "data"
SNAPSHOT_TIMES = [
    "09:45", "10:15", "10:45", "11:15", "11:45",
    "13:30", "14:00", "14:30", "15:00", "15:30",
]
DEFAULT_MIN_SCORE = 60.0
EOD_HHMM = "15:55"


# ────────────────────────────────────────────────────────────────────
# Trade result dataclass
# ────────────────────────────────────────────────────────────────────

@dataclass
class ThemeTrade:
    ticker: str
    bucket: str
    theme: Optional[str]
    tier: str                       # primary | secondary | watch
    score_at_entry: float
    entry_time: str                 # HH:MM
    entry_snapshot: str             # snapshot label that first qualified
    entry_price: float
    stop_price: float
    target_price: float
    stop_distance_pct: float
    target_R_pct: float
    exit_time: str                  # HH:MM
    exit_price: float
    exit_reason: str                # WIN | LOSS | EOD
    pnl_pct: float
    r_multiple: float
    minutes_held: int
    max_favorable_pct: float        # MFE during hold (from entry, excl entry bar)
    max_adverse_pct: float          # MAE during hold (from entry, excl entry bar)
    size_R_pct: float
    catalyst_headline: Optional[str] = None
    second_order: Optional[str] = None

    def to_dict(self):
        return asdict(self)


# ────────────────────────────────────────────────────────────────────
# Snapshot pass
# ────────────────────────────────────────────────────────────────────

def _pick_tier(score: float) -> str:
    if score >= 70:
        return "primary"
    if score >= 60:
        return "secondary"
    return "watch"


def run_snapshot_pass(min_score: float = DEFAULT_MIN_SCORE,
                      snapshots: list[str] = None) -> dict[str, dict]:
    """
    Call run_theme_scan at each snapshot time and collect the earliest
    qualifying signal per ticker.

    Returns {ticker: {signal_dict + "first_qualified_at": "HH:MM"}}
    """
    snapshots = snapshots or SNAPSHOT_TIMES
    first_qualified: dict[str, dict] = {}
    per_snap_counts: dict[str, int] = {}

    for snap in snapshots:
        try:
            res = run_theme_scan(
                min_score=min_score,
                max_results=50,
                as_of_hhmm=snap,
                max_intraday=120,
            )
        except Exception as e:
            logger.warning(f"Snapshot {snap} failed: {e}")
            per_snap_counts[snap] = 0
            continue
        signals = res.get("signals", [])
        per_snap_counts[snap] = len(signals)

        for s in signals:
            tk = s["ticker"]
            if tk not in first_qualified:
                first_qualified[tk] = {**s, "first_qualified_at": snap}

        logger.info(f"[backtest snap {snap}] {len(signals)} qualifying signals")

    return {
        "first_qualified": first_qualified,
        "per_snap_counts": per_snap_counts,
    }


# ────────────────────────────────────────────────────────────────────
# Replay pass
# ────────────────────────────────────────────────────────────────────

def _find_entry_bar_idx(today_df: pd.DataFrame, snap_hhmm: str) -> Optional[int]:
    """
    Find the first bar whose index (converted to ET) is >= the snapshot time.
    Bars arrive tz-aware in UTC from yfinance; snapshot times are ET.
    """
    if today_df.empty:
        return None
    try:
        hh, mm = map(int, snap_hhmm.split(":"))
    except Exception:
        return None
    for i, ts in enumerate(today_df.index):
        ts_et = _to_et(ts)
        if (ts_et.hour, ts_et.minute) >= (hh, mm):
            return i
    return None


def _simulate_trade(bars: pd.DataFrame, sig: dict) -> Optional[ThemeTrade]:
    """
    Run the bar-by-bar replay for a single qualifying signal.
    """
    today = _today_bars(bars)
    if today.empty:
        return None

    snap = sig["first_qualified_at"]
    entry_idx = _find_entry_bar_idx(today, snap)
    if entry_idx is None or entry_idx >= len(today) - 1:
        return None  # qualified too late in the day

    entry_bar = today.iloc[entry_idx]
    entry_px = float(entry_bar["Close"])
    stop_pct = float(sig.get("stop_distance_pct") or 2.0)
    tgt_pct = float(sig.get("target_R_pct") or 4.0)
    stop_px = entry_px * (1 - stop_pct / 100.0)
    target_px = entry_px * (1 + tgt_pct / 100.0)
    entry_time = _to_et(today.index[entry_idx]).strftime("%H:%M")

    mfe_pct = 0.0
    mae_pct = 0.0
    exit_px = None
    exit_reason = None
    exit_time = None

    # Walk forward
    for j in range(entry_idx + 1, len(today)):
        bar = today.iloc[j]
        bar_high = float(bar["High"])
        bar_low = float(bar["Low"])
        bar_time = today.index[j]

        # update excursion from entry
        up_pct = (bar_high - entry_px) / entry_px * 100
        dn_pct = (bar_low - entry_px) / entry_px * 100
        if up_pct > mfe_pct:
            mfe_pct = up_pct
        if dn_pct < mae_pct:
            mae_pct = dn_pct

        # Ambiguity convention: if the same bar touches both stop and target,
        # treat as LOSS (conservative — many intraday systems assume stop
        # fills first on a volatile bar).
        hit_stop = bar_low <= stop_px
        hit_target = bar_high >= target_px
        if hit_stop:
            exit_px = stop_px
            exit_reason = "LOSS"
            exit_time = _to_et(bar_time).strftime("%H:%M")
            break
        if hit_target:
            exit_px = target_px
            exit_reason = "WIN"
            exit_time = _to_et(bar_time).strftime("%H:%M")
            break

    if exit_px is None:
        # EOD mark-to-market using last bar close
        last_bar = today.iloc[-1]
        exit_px = float(last_bar["Close"])
        exit_reason = "EOD"
        exit_time = _to_et(today.index[-1]).strftime("%H:%M")

    pnl_pct = (exit_px - entry_px) / entry_px * 100
    r_mult = pnl_pct / stop_pct if stop_pct else 0.0

    # minutes held (entry_time and exit_time are both HH:MM ET labels)
    try:
        e_h, e_m = map(int, entry_time.split(":"))
        x_h, x_m = map(int, exit_time.split(":"))
        minutes_held = (x_h * 60 + x_m) - (e_h * 60 + e_m)
        if minutes_held < 0:
            minutes_held = 0
    except Exception:
        minutes_held = 0

    return ThemeTrade(
        ticker=sig["ticker"],
        bucket=sig["bucket"],
        theme=sig.get("theme"),
        tier=_pick_tier(sig["score"]),
        score_at_entry=sig["score"],
        entry_time=entry_time,
        entry_snapshot=snap,
        entry_price=round(entry_px, 4),
        stop_price=round(stop_px, 4),
        target_price=round(target_px, 4),
        stop_distance_pct=stop_pct,
        target_R_pct=tgt_pct,
        exit_time=exit_time,
        exit_price=round(exit_px, 4),
        exit_reason=exit_reason,
        pnl_pct=round(pnl_pct, 3),
        r_multiple=round(r_mult, 3),
        minutes_held=minutes_held,
        max_favorable_pct=round(mfe_pct, 3),
        max_adverse_pct=round(mae_pct, 3),
        size_R_pct=float(sig.get("size_R_pct") or 0.35),
        catalyst_headline=(sig.get("catalyst") or {}).get("headline") if sig.get("catalyst") else None,
        second_order=sig.get("second_order"),
    )


def run_replay_pass(first_qualified: dict[str, dict]) -> list[ThemeTrade]:
    """Fetch bars for all qualifiers (batched) and simulate each trade."""
    tickers = list(first_qualified.keys())
    if not tickers:
        return []
    bars_map = _fetch_intraday_5m(tickers)

    trades: list[ThemeTrade] = []
    for tk, sig in first_qualified.items():
        df = bars_map.get(tk)
        if df is None or df.empty:
            logger.info(f"[replay] no bars for {tk}, skipping")
            continue
        t = _simulate_trade(df, sig)
        if t is not None:
            trades.append(t)
    return trades


# ────────────────────────────────────────────────────────────────────
# Aggregations
# ────────────────────────────────────────────────────────────────────

def _agg_stats(trades: list[ThemeTrade]) -> dict:
    if not trades:
        return {
            "trades": 0, "winners": 0, "losers": 0, "eod": 0,
            "win_rate_pct": 0.0, "total_R": 0.0, "avg_R": 0.0,
            "best_R": 0.0, "worst_R": 0.0, "avg_minutes_held": 0,
            "avg_pnl_pct": 0.0,
        }
    winners = [t for t in trades if t.exit_reason == "WIN"]
    losers = [t for t in trades if t.exit_reason == "LOSS"]
    eod = [t for t in trades if t.exit_reason == "EOD"]
    r_values = [t.r_multiple for t in trades]
    return {
        "trades": len(trades),
        "winners": len(winners),
        "losers": len(losers),
        "eod": len(eod),
        "win_rate_pct": round(len(winners) / len(trades) * 100, 1),
        "total_R": round(sum(r_values), 2),
        "avg_R": round(sum(r_values) / len(trades), 3),
        "best_R": round(max(r_values), 2),
        "worst_R": round(min(r_values), 2),
        "avg_minutes_held": int(sum(t.minutes_held for t in trades) / len(trades)),
        "avg_pnl_pct": round(sum(t.pnl_pct for t in trades) / len(trades), 2),
    }


def _group_by(trades: list[ThemeTrade], key) -> dict[str, dict]:
    out: dict[str, list[ThemeTrade]] = {}
    for t in trades:
        k = key(t) or "—"
        out.setdefault(k, []).append(t)
    return {k: {**_agg_stats(v), "tickers": [t.ticker for t in v]} for k, v in out.items()}


def _timeline_buckets(trades: list[ThemeTrade]) -> list[dict]:
    """
    Bucket entries into 30-min bins across the trading day for a
    distribution chart.
    """
    bins = [
        ("09:30", "10:00"), ("10:00", "10:30"), ("10:30", "11:00"),
        ("11:00", "11:30"), ("11:30", "12:00"), ("12:00", "12:30"),
        ("12:30", "13:00"), ("13:00", "13:30"), ("13:30", "14:00"),
        ("14:00", "14:30"), ("14:30", "15:00"), ("15:00", "15:30"),
        ("15:30", "16:00"),
    ]
    out = []
    for lo, hi in bins:
        entries = [t for t in trades
                   if lo <= t.entry_time < hi]
        winners = sum(1 for t in entries if t.exit_reason == "WIN")
        losers = sum(1 for t in entries if t.exit_reason == "LOSS")
        eod_c = sum(1 for t in entries if t.exit_reason == "EOD")
        out.append({
            "label": f"{lo}",
            "range": f"{lo}–{hi}",
            "count": len(entries),
            "winners": winners,
            "losers": losers,
            "eod": eod_c,
            "tickers": [t.ticker for t in entries],
        })
    return out


# ────────────────────────────────────────────────────────────────────
# Top-level
# ────────────────────────────────────────────────────────────────────

def run_backtest(min_score: float = DEFAULT_MIN_SCORE,
                 snapshots: list[str] = None) -> dict:
    snapshots = snapshots or SNAPSHOT_TIMES
    logger.info(f"Running ThemeHunter backtest with snapshots: {snapshots}")
    snap_out = run_snapshot_pass(min_score=min_score, snapshots=snapshots)
    first_qualified = snap_out["first_qualified"]
    per_snap_counts = snap_out["per_snap_counts"]

    trades = run_replay_pass(first_qualified)
    # Sort by entry time
    trades.sort(key=lambda t: t.entry_time)

    trade_dicts = [t.to_dict() for t in trades]
    summary = _agg_stats(trades)

    result = {
        "generated_at": _today_et().isoformat(),
        "trade_date": _today_et().strftime("%Y-%m-%d"),
        "snapshots_used": snapshots,
        "min_score": min_score,
        "summary": summary,
        "timeline": _timeline_buckets(trades),
        "trades": trade_dicts,
        "by_bucket": _group_by(trades, lambda t: t.bucket),
        "by_theme": _group_by(trades, lambda t: t.theme),
        "by_tier": _group_by(trades, lambda t: t.tier),
        "per_snap_counts": per_snap_counts,
        "qualified_count": len(first_qualified),
    }
    return result


# ────────────────────────────────────────────────────────────────────
# Persistence
# ────────────────────────────────────────────────────────────────────

def save_backtest(result: dict, name: str = "latest"):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"theme_backtest_{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    return path


def load_backtest(name: str = "latest") -> Optional[dict]:
    path = os.path.join(DATA_DIR, f"theme_backtest_{name}.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)
