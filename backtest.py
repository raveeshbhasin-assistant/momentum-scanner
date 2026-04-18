"""
Backtest Engine (Technicals-Only Historical Replay)
───────────────────────────────────────────────────
Replays the v3.3 scanner algorithm against historical 5-minute bars
and simulates each trade forward to compute win rate, P&L, equity
curve, and per-filter impact.

Scope & Limitations — read before trusting numbers:
  • TECHNICALS ONLY. News sentiment and pre-market catalysts are
    assumed zero. You cannot faithfully replay headlines that moved
    markets at 10:04 AM three weeks ago — Finnhub doesn't index back
    that far reliably and VADER scoring on stale headlines would be
    noisy. Result: backtest composite scores will be LOWER on average
    than live ones because the sentiment/premarket boosts are missing.
  • NO SURVIVORSHIP ADJUSTMENT. Universe is today's list; delistings
    and ticker changes are not modeled.
  • NO SLIPPAGE MODELING beyond a flat 0.02% per side. Wide-spread
    mid-caps get the same treatment as AAPL.
  • NO COMMISSIONS. Account is assumed $100k, sizing follows the
    live rule (1% risk per trade × regime size multiplier).
  • VIX REGIME uses daily VIX close, not intraday. Good enough for
    day-level regime classification but misses intraday spikes.
  • SECTOR LEADERSHIP uses historical SPY and sector-ETF 5-min bars
    for the same window, computed at each scan time. This part is
    faithful.
  • EARNINGS uses the earnings cache as it exists today. A backtest
    over dates PRIOR to current cache coverage will have no earnings
    context (filter effectively disabled for those dates).

All caveats are surfaced on the /backtest page.

Added in v3.3.
"""

import json
import logging
import time as _time
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

import config
from scanner import (
    calculate_indicators,
    calculate_rvol,
    score_technicals,
    calculate_trade_levels,
    _calc_ticker_intraday_pct,
)
from sector_rotation import (
    SECTOR_ETFS,
    TICKER_TO_SECTOR,
    classify_leadership,
    set_rotation_snapshot,
)
from market_regime import get_regime_at
from earnings import get_earnings_context

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
BASE_DIR = Path(__file__).parent
CACHE_DIR = BASE_DIR / "data" / "backtest_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Scan-time grid: mirror live scanner (every :00 and :30 between 10:00
# and 15:30) with dead-zone 11:30 + 12:00 removed by default. The engine
# honors the filter toggle so you can toggle dead-zone ON/OFF at replay.
SCAN_TIMES_FULL = [
    time(10, 0), time(10, 30),
    time(11, 0), time(11, 30),
    time(12, 0), time(12, 30),
    time(13, 0), time(13, 30),
    time(14, 0), time(14, 30),
    time(15, 0), time(15, 30),
]
DEAD_ZONE_TIMES = {time(11, 30), time(12, 0)}

# Trade exit cut-off — same as live (3:55 PM hard close)
EOD_EXIT_TIME = time(15, 55)

# Modeling assumptions
SLIPPAGE_PCT_PER_SIDE = 0.0002   # 0.02% each side = 0.04% roundtrip
ACCOUNT_SIZE = config.DEFAULT_ACCOUNT_SIZE
RISK_PCT = config.RISK_PER_TRADE_PCT / 100
YFINANCE_MAX_INTRADAY_DAYS = 59   # yfinance hard limit for 5-min data


# ═══════════════════════════════════════════════════════════════
#  DATA TYPES
# ═══════════════════════════════════════════════════════════════

@dataclass
class Filters:
    """All filter toggles. Default = FULL v3.3 pipeline enabled."""
    leadership_enabled: bool = True
    regime_enabled: bool = True
    earnings_enabled: bool = True
    dead_zone_enabled: bool = True
    reentry_enabled: bool = True
    min_rvol_enabled: bool = True

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "Filters":
        if not d:
            return cls()
        return cls(**{k: bool(d.get(k, getattr(cls(), k))) for k in cls.__dataclass_fields__})


@dataclass
class Trade:
    ticker: str
    scan_date: str
    scan_time: str
    entry: float
    stop: float
    target: float
    shares: int
    position_value: float
    composite_score: float
    tech_score: float
    rvol: float
    leadership_label: str
    leadership_adj: int
    regime_label: str
    earnings_badge: str
    earnings_adj: int
    # Outcome (filled by simulator)
    exit: float = 0.0
    exit_time: str = ""
    exit_reason: str = ""     # "TARGET" | "STOP" | "EOD"
    pnl_dollars: float = 0.0
    pnl_pct: float = 0.0
    r_multiple: float = 0.0   # P&L as multiple of 1R (initial risk)
    bars_held: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


# ═══════════════════════════════════════════════════════════════
#  DATA LAYER — bulk download + disk cache
# ═══════════════════════════════════════════════════════════════

def _cache_key(ticker: str, start: date, end: date) -> Path:
    # Pickle (not parquet) to avoid pyarrow C-build dependency on Windows.
    return CACHE_DIR / f"{ticker}_{start.isoformat()}_{end.isoformat()}.pkl"


def _load_or_fetch(ticker: str, start: date, end: date) -> Optional[pd.DataFrame]:
    """Load ticker 5-min bars from cache or fetch via yfinance."""
    cache_path = _cache_key(ticker, start, end)
    if cache_path.exists():
        try:
            return pd.read_pickle(cache_path)
        except Exception as e:
            logger.warning(f"Cache read failed for {ticker}: {e}; refetching")

    try:
        df = yf.download(
            ticker,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            interval="5m",
            progress=False,
            threads=False,
            auto_adjust=False,
        )
    except Exception as e:
        logger.warning(f"yfinance fetch failed for {ticker}: {e}")
        return None

    if df is None or df.empty:
        return None

    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.dropna(subset=["Close"])
    if df.empty:
        return None

    # Localize to ET so scan-time comparisons work
    try:
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(config.ET)
        else:
            df.index = df.index.tz_convert(config.ET)
    except Exception:
        pass

    try:
        df.to_pickle(cache_path)
    except Exception as e:
        logger.warning(f"Cache write failed for {ticker}: {e}")

    return df


def _bulk_download(tickers: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
    """Fetch all tickers for the window, using per-ticker cache files."""
    out: dict[str, pd.DataFrame] = {}
    for i, t in enumerate(tickers):
        df = _load_or_fetch(t, start, end)
        if df is not None and not df.empty:
            out[t] = df
        if i % 20 == 0:
            logger.info(f"Backtest fetch: {i+1}/{len(tickers)} tickers")
    return out


def _trading_days(start: date, end: date, reference_index: Optional[pd.DatetimeIndex] = None) -> list[date]:
    """Return all weekdays between start and end (inclusive). If reference
    index provided, intersect with actual bar dates (closer to truth)."""
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)

    if reference_index is not None and len(reference_index) > 0:
        real_dates = {ts.date() for ts in reference_index}
        days = [d for d in days if d in real_dates]
    return days


# ═══════════════════════════════════════════════════════════════
#  PER-SCAN-TIME HELPERS
# ═══════════════════════════════════════════════════════════════

def _slice_up_to(df: pd.DataFrame, day: date, scan_time: time) -> pd.DataFrame:
    """Return the subset of df with timestamps <= scan_time on day, plus
    history from prior days (needed for 50-EMA, pivots, etc.)."""
    try:
        cutoff = datetime.combine(day, scan_time).replace(tzinfo=config.ET)
        return df[df.index <= cutoff]
    except Exception:
        return df.iloc[0:0]


def _intraday_pct_from_slice(sliced: pd.DataFrame, day: date) -> Optional[float]:
    """Today's session open-to-last % change within a slice."""
    try:
        today_mask = sliced.index.date == day
        today_df = sliced[today_mask]
        if len(today_df) < 2:
            return None
        open_px = float(today_df["Open"].iloc[0])
        last_px = float(today_df["Close"].iloc[-1])
        if open_px <= 0:
            return None
        return (last_px - open_px) / open_px * 100
    except Exception:
        return None


def _build_sector_snapshot(
    etf_data: dict[str, pd.DataFrame],
    day: date,
    scan_time: time,
) -> tuple[float, dict[str, float]]:
    """Given per-ETF data and a scan time, compute SPY % and each sector %."""
    spy_df = etf_data.get("SPY")
    spy_slice = _slice_up_to(spy_df, day, scan_time) if spy_df is not None else pd.DataFrame()
    spy_pct = _intraday_pct_from_slice(spy_slice, day) or 0.0

    sector_changes: dict[str, float] = {}
    for sector_name, etf in SECTOR_ETFS.items():
        sdf = etf_data.get(etf)
        if sdf is None:
            continue
        s_slice = _slice_up_to(sdf, day, scan_time)
        pct = _intraday_pct_from_slice(s_slice, day)
        if pct is not None:
            sector_changes[sector_name] = pct
    return spy_pct, sector_changes


def _apply_regime_to_min_score(regime: dict, filters: Filters) -> int:
    if filters.regime_enabled:
        return regime.get("effective_min_score", config.MIN_COMPOSITE_SCORE)
    return config.MIN_COMPOSITE_SCORE


def _volume_score(rvol: float) -> float:
    if rvol > 5.0:
        return min(rvol / 3.0 * 100, 100) * 0.7
    return min(rvol / 3.0 * 100, 100)


# ═══════════════════════════════════════════════════════════════
#  TRADE SIMULATOR
# ═══════════════════════════════════════════════════════════════

def _simulate_trade(
    df: pd.DataFrame,
    entry_time: datetime,
    entry: float,
    stop: float,
    target: float,
) -> tuple[float, datetime, str, int]:
    """
    Walk forward through bars after entry_time until one of:
      - Low <= stop   → exit at stop
      - High >= target → exit at target
      - time >= EOD_EXIT_TIME → exit at close of that bar

    Returns (exit_price, exit_ts, reason, bars_held).
    """
    eod_ts = datetime.combine(entry_time.date(), EOD_EXIT_TIME).replace(tzinfo=config.ET)
    future = df[(df.index > entry_time)]

    bars = 0
    for ts, row in future.iterrows():
        bars += 1
        high = float(row["High"])
        low = float(row["Low"])
        close = float(row["Close"])

        # Resolve stop vs target intra-bar (stop-first is conservative)
        if low <= stop:
            return stop, ts, "STOP", bars
        if high >= target:
            return target, ts, "TARGET", bars
        if ts >= eod_ts:
            return close, ts, "EOD", bars

    # Ran out of bars without a decision (shouldn't happen on a full trading day)
    if not future.empty:
        last_ts = future.index[-1]
        last_close = float(future["Close"].iloc[-1])
        return last_close, last_ts, "EOD", bars
    return entry, entry_time, "NO_DATA", 0


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_backtest(
    start_date: date,
    end_date: date,
    filters: Optional[Filters] = None,
    tickers: Optional[list[str]] = None,
    max_tickers: Optional[int] = None,
) -> dict:
    """
    Run the full backtest over the date range. Returns an aggregate dict.

    Params:
      start_date, end_date:   inclusive date range (max 59 calendar days
                              back from today, yfinance limit).
      filters:                Filters() instance. Default = full v3.3.
      tickers:                override the scanning universe.
      max_tickers:            cap for quick runs (debugging).
    """
    t0 = _time.time()
    filters = filters or Filters()

    today = date.today()
    oldest_allowed = today - timedelta(days=YFINANCE_MAX_INTRADAY_DAYS)
    if start_date < oldest_allowed:
        logger.warning(
            f"start_date {start_date} earlier than yfinance 5m limit "
            f"({oldest_allowed}); clamping"
        )
        start_date = oldest_allowed
    if end_date > today:
        end_date = today

    universe = tickers or config.get_full_universe()
    if max_tickers is not None:
        universe = universe[:max_tickers]

    logger.info(
        f"Backtest: {start_date} → {end_date} × {len(universe)} tickers, "
        f"filters={asdict(filters)}"
    )

    # ── 1. Bulk download ──
    stock_data = _bulk_download(universe, start_date, end_date)
    etf_list = list(SECTOR_ETFS.values()) + ["SPY"]
    etf_data = _bulk_download(etf_list, start_date, end_date)

    if not stock_data:
        return {
            "error": "No historical data returned for the date range.",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "filters": asdict(filters),
            "trades": [],
            "summary": {},
        }

    # Use SPY as the "real trading days" reference
    ref = etf_data.get("SPY")
    ref_index = ref.index if ref is not None else None
    days = _trading_days(start_date, end_date, ref_index)

    # ── 2. Per-day replay ──
    all_trades: list[Trade] = []
    signals_total = 0          # signals PASSING all active filters
    signals_generated = 0      # signals PRE-filter (for funnel analysis)
    filter_drops = {
        "rvol": 0, "earnings_hard": 0, "dead_zone": 0,
        "reentry": 0, "min_score": 0,
    }

    # Pull regime per day (daily bars)
    day_regimes: dict[date, dict] = {}

    for d in days:
        regime = get_regime_at(datetime.combine(d, time(12, 0)))
        day_regimes[d] = regime
        effective_min = _apply_regime_to_min_score(regime, filters)
        size_mult = regime.get("size_multiplier", 1.0) if filters.regime_enabled else 1.0

        seen_today: set[str] = set()

        for st in SCAN_TIMES_FULL:
            # Dead zone filter
            if filters.dead_zone_enabled and st in DEAD_ZONE_TIMES:
                filter_drops["dead_zone"] += len(universe)  # rough accounting
                continue

            # Build sector snapshot for this scan tick
            spy_pct, sector_changes = _build_sector_snapshot(etf_data, d, st)
            set_rotation_snapshot(spy_pct, sector_changes)

            scan_ts = datetime.combine(d, st).replace(tzinfo=config.ET)

            for ticker in universe:
                df_full = stock_data.get(ticker)
                if df_full is None:
                    continue
                sliced = _slice_up_to(df_full, d, st)
                if len(sliced) < 50:   # need history for 50-EMA
                    continue
                # Need at least ONE bar stamped on day d (i.e. today started)
                today_bars = sliced[sliced.index.date == d]
                if today_bars.empty:
                    continue

                try:
                    df = calculate_indicators(sliced.copy())
                except Exception as e:
                    logger.debug(f"indicator calc failed for {ticker} at {d} {st}: {e}")
                    continue

                # RVOL
                rvol = calculate_rvol(df)
                if filters.min_rvol_enabled and rvol < config.MIN_RVOL:
                    filter_drops["rvol"] += 1
                    continue

                # Re-entry
                if filters.reentry_enabled and ticker in seen_today:
                    filter_drops["reentry"] += 1
                    continue

                # Earnings context (as-of backtest date)
                earnings_ctx = (
                    get_earnings_context(ticker, now=scan_ts)
                    if filters.earnings_enabled else None
                )
                if earnings_ctx and earnings_ctx.get("hard_filter"):
                    filter_drops["earnings_hard"] += 1
                    continue

                # Technical score
                tech_score, _ = score_technicals(df)
                vol_score = _volume_score(rvol)

                # Composite (no sentiment, no premarket — technicals-only caveat)
                composite = (
                    tech_score * config.TECHNICAL_WEIGHT +
                    0.0        * config.SENTIMENT_WEIGHT +   # sentiment = 0
                    vol_score  * config.VOLUME_WEIGHT
                )

                # Leadership
                leadership = {"label": "UNKNOWN", "score_adjustment": 0}
                if filters.leadership_enabled:
                    ticker_pct = _intraday_pct_from_slice(sliced, d) or 0.0
                    leadership = classify_leadership(
                        ticker, ticker_pct, now=scan_ts,
                    )
                    composite += leadership.get("score_adjustment", 0)

                # Earnings adjustment
                earn_adj = 0
                earn_badge = ""
                if earnings_ctx:
                    earn_adj = earnings_ctx.get("score_adjustment", 0)
                    earn_badge = earnings_ctx.get("badge_text", "")
                    composite += earn_adj

                signals_generated += 1

                # Min-score gate
                if composite < effective_min:
                    filter_drops["min_score"] += 1
                    continue

                # Trade levels (regime-scaled)
                try:
                    levels = calculate_trade_levels(df)
                except Exception as e:
                    logger.debug(f"trade levels failed for {ticker}: {e}")
                    continue

                shares = int(levels.get("suggested_shares", 0) * size_mult)
                if shares <= 0:
                    continue

                entry_px = float(levels["entry"]) * (1 + SLIPPAGE_PCT_PER_SIDE)
                stop_px = float(levels["stop_loss"])
                target_px = float(levels["target"])

                # Simulate outcome
                exit_px, exit_ts, reason, bars_held = _simulate_trade(
                    df_full, scan_ts, entry_px, stop_px, target_px,
                )
                # Apply exit slippage (loss side)
                if reason != "NO_DATA":
                    exit_px = exit_px * (1 - SLIPPAGE_PCT_PER_SIDE)

                pnl_dollars = (exit_px - entry_px) * shares
                pnl_pct = (exit_px - entry_px) / entry_px * 100 if entry_px else 0.0
                stop_distance = entry_px - stop_px
                r = ((exit_px - entry_px) / stop_distance) if stop_distance > 0 else 0.0

                tr = Trade(
                    ticker=ticker,
                    scan_date=d.isoformat(),
                    scan_time=st.strftime("%H:%M"),
                    entry=round(entry_px, 2),
                    stop=round(stop_px, 2),
                    target=round(target_px, 2),
                    shares=shares,
                    position_value=round(entry_px * shares, 2),
                    composite_score=round(composite, 1),
                    tech_score=round(tech_score, 1),
                    rvol=round(rvol, 2),
                    leadership_label=leadership.get("label", "UNKNOWN"),
                    leadership_adj=leadership.get("score_adjustment", 0),
                    regime_label=regime.get("label", "NORMAL"),
                    earnings_badge=earn_badge,
                    earnings_adj=earn_adj,
                    exit=round(exit_px, 2),
                    exit_time=exit_ts.strftime("%Y-%m-%d %H:%M") if hasattr(exit_ts, "strftime") else "",
                    exit_reason=reason,
                    pnl_dollars=round(pnl_dollars, 2),
                    pnl_pct=round(pnl_pct, 3),
                    r_multiple=round(r, 2),
                    bars_held=bars_held,
                )
                all_trades.append(tr)
                signals_total += 1
                seen_today.add(ticker)

        logger.info(f"Backtest day {d}: cumulative trades = {len(all_trades)}")

    # ── 3. Aggregate ──
    summary = _aggregate(all_trades, start_date, end_date)
    elapsed = round(_time.time() - t0, 1)
    logger.info(f"Backtest complete in {elapsed}s — {len(all_trades)} trades")

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "filters": asdict(filters),
        "universe_size": len(universe),
        "trading_days": [d.isoformat() for d in days],
        "signals_generated": signals_generated,
        "signals_total": signals_total,
        "filter_drops": filter_drops,
        "elapsed_seconds": elapsed,
        "trades": [t.to_dict() for t in all_trades],
        "summary": summary,
    }


# ═══════════════════════════════════════════════════════════════
#  AGGREGATION / METRICS
# ═══════════════════════════════════════════════════════════════

def _aggregate(trades: list[Trade], start_date: date, end_date: date) -> dict:
    """Compute headline stats, breakdowns, and equity curve."""
    if not trades:
        return {
            "trade_count": 0, "wins": 0, "losses": 0,
            "win_rate": 0.0, "avg_r": 0.0, "total_pnl": 0.0,
            "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
            "profit_factor": 0.0, "max_drawdown": 0.0,
            "expectancy_per_trade": 0.0,
            "equity_curve": [], "by_hour": {}, "by_sector": {},
            "by_leadership": {}, "by_regime": {}, "by_exit_reason": {},
        }

    wins = [t for t in trades if t.exit_reason == "TARGET"]
    losses = [t for t in trades if t.exit_reason == "STOP"]
    eod = [t for t in trades if t.exit_reason == "EOD"]
    decided = wins + losses
    wr = len(wins) / len(decided) * 100 if decided else 0.0

    avg_r = np.mean([t.r_multiple for t in trades]) if trades else 0.0
    total_pnl = float(sum(t.pnl_dollars for t in trades))

    avg_win_pct = float(np.mean([t.pnl_pct for t in wins])) if wins else 0.0
    avg_loss_pct = float(np.mean([t.pnl_pct for t in losses])) if losses else 0.0

    gross_profit = sum(t.pnl_dollars for t in trades if t.pnl_dollars > 0)
    gross_loss = abs(sum(t.pnl_dollars for t in trades if t.pnl_dollars < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0.0

    expectancy = total_pnl / len(trades) if trades else 0.0

    # Equity curve (ordered by scan_date then scan_time)
    ordered = sorted(trades, key=lambda t: (t.scan_date, t.scan_time))
    eq = 0.0
    equity_curve = []
    peak = 0.0
    max_dd = 0.0
    for t in ordered:
        eq += t.pnl_dollars
        peak = max(peak, eq)
        dd = peak - eq
        max_dd = max(max_dd, dd)
        equity_curve.append({
            "date": t.scan_date, "time": t.scan_time,
            "equity": round(eq, 2), "trade_pnl": t.pnl_dollars,
            "ticker": t.ticker,
        })

    # Breakdowns
    def bucket(key_fn) -> dict:
        d: dict[str, dict] = {}
        for t in trades:
            k = key_fn(t) or "—"
            b = d.setdefault(k, {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0})
            b["trades"] += 1
            b["pnl"] += t.pnl_dollars
            if t.exit_reason == "TARGET":
                b["wins"] += 1
            elif t.exit_reason == "STOP":
                b["losses"] += 1
        for k, b in d.items():
            decided_k = b["wins"] + b["losses"]
            b["win_rate"] = round(b["wins"] / decided_k * 100, 1) if decided_k else 0.0
            b["pnl"] = round(b["pnl"], 2)
        return d

    by_hour = bucket(lambda t: t.scan_time[:2] + ":00")
    by_sector = bucket(lambda t: TICKER_TO_SECTOR.get(t.ticker, "Other"))
    by_leadership = bucket(lambda t: t.leadership_label)
    by_regime = bucket(lambda t: t.regime_label)
    by_exit = bucket(lambda t: t.exit_reason)

    return {
        "trade_count": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "eod_exits": len(eod),
        "win_rate": round(wr, 1),
        "avg_r": round(float(avg_r), 3),
        "total_pnl": round(total_pnl, 2),
        "avg_win_pct": round(avg_win_pct, 2),
        "avg_loss_pct": round(avg_loss_pct, 2),
        "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else None,
        "max_drawdown": round(max_dd, 2),
        "expectancy_per_trade": round(expectancy, 2),
        "equity_curve": equity_curve,
        "by_hour": by_hour,
        "by_sector": by_sector,
        "by_leadership": by_leadership,
        "by_regime": by_regime,
        "by_exit_reason": by_exit,
    }


# ═══════════════════════════════════════════════════════════════
#  RESULT PERSISTENCE (so the UI can load without re-running)
# ═══════════════════════════════════════════════════════════════

RESULTS_DIR = BASE_DIR / "data" / "backtest_results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def save_result(result: dict, name: str = "latest") -> Path:
    """Save a backtest result JSON. Default name overwritten on each run."""
    path = RESULTS_DIR / f"{name}.json"
    with path.open("w") as f:
        json.dump(result, f, indent=2, default=str)
    return path


def load_result(name: str = "latest") -> Optional[dict]:
    path = RESULTS_DIR / f"{name}.json"
    if not path.exists():
        return None
    try:
        with path.open("r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Load backtest result {name} failed: {e}")
        return None


def list_results() -> list[dict]:
    """List saved backtest runs with basic metadata."""
    out = []
    for p in sorted(RESULTS_DIR.glob("*.json")):
        try:
            with p.open("r") as f:
                r = json.load(f)
            out.append({
                "name": p.stem,
                "start_date": r.get("start_date"),
                "end_date": r.get("end_date"),
                "trade_count": r.get("summary", {}).get("trade_count", 0),
                "win_rate": r.get("summary", {}).get("win_rate", 0),
                "total_pnl": r.get("summary", {}).get("total_pnl", 0),
                "mtime": datetime.fromtimestamp(p.stat().st_mtime).isoformat(),
            })
        except Exception:
            continue
    return out
