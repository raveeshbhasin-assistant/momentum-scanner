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
    """All filter toggles. Default = FULL v3.3 pipeline enabled.

    leader_filter_mode: how leadership interacts with entry decisions.
      - "score"  → current v3.3 behaviour: add +10/+3/-10 to composite score
      - "strict" → HARD FILTER: only LEADER trades allowed
      - "moderate" → HARD FILTER: LEADER + SOLO_MOVER allowed
      - "permissive" → HARD FILTER: LEADER + SOLO_MOVER + FOLLOWER allowed
        (blocks only LAGGARD and UNKNOWN)
    """
    leadership_enabled: bool = True
    regime_enabled: bool = True
    earnings_enabled: bool = True
    dead_zone_enabled: bool = True
    reentry_enabled: bool = True
    min_rvol_enabled: bool = True
    # v3.3.2: default flipped to "moderate" (LEADER + SOLO_MOVER hard filter)
    # based on 20-day backtest. See config.LEADER_FILTER_MODE docstring.
    leader_filter_mode: str = "moderate"   # "score" | "strict" | "moderate" | "permissive"
    # v3.4.1: which simulated exit strategy drives the headline pnl_dollars /
    # r_multiple / exit_reason fields on each Trade. All strategies are always
    # simulated for comparison; this switch just picks which is "primary."
    #   "flat"          → legacy stop/target/EOD
    #   "tiered"        → 33/33/33 tranche exits
    #   "abandon"       → flat + bar-12 chop abandonment (±0.5R)
    #   "combo"         → tiered + abandon on untaken
    #   "mfe_kill"      → flat + bar-8 MFE<0.3R check
    #   "tight_abandon" → flat + bar-12 (|R|<0.3 AND MAE<-0.5R)
    #   "mae_exit"      → flat + bar-12 (MAE<-0.75R AND R<0) ← v3.4.1 default
    exit_strategy: str = "mae_exit"

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "Filters":
        if not d:
            return cls()
        defaults = cls()
        kwargs = {}
        for k, f in cls.__dataclass_fields__.items():
            v = d.get(k, getattr(defaults, k))
            if f.type == "bool":
                v = bool(v)
            kwargs[k] = v
        return cls(**kwargs)


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
    # v3.4 — MFE / MAE excursion tracking (always computed)
    mfe_r: float = 0.0        # peak favorable R during trade
    mae_r: float = 0.0        # peak adverse R during trade (≤ 0)
    mfe_bar: int = 0          # bar index where MFE occurred
    mae_bar: int = 0          # bar index where MAE occurred
    # v3.4.1 — FLAT comparison fields (frozen before the headline-strategy
    # router rewrites pnl_dollars / r_multiple). Lets downstream code still
    # show "what flat would have been" alongside the chosen strategy.
    flat_pnl: float = 0.0
    flat_r: float = 0.0
    flat_exit_reason: str = ""
    # v3.4 — tiered-exit parallel simulation (33% at 1R / 33% at 1.75R / 33% runs)
    tiered_pnl: float = 0.0
    tiered_r: float = 0.0     # blended R across tranches
    tiered_exit_desc: str = ""    # e.g. "T1@1R+T2@1.75R+RUN@TARGET"
    tiered_exit_reason: str = ""  # aggregated outcome
    # v3.4 — 60-min chop abandonment ("1.3 rule")
    abandoned: bool = False
    abandon_pnl: float = 0.0      # full-P&L if abandon rule were applied instead of flat
    abandon_r: float = 0.0
    abandon_exit_reason: str = ""  # STOP | TARGET | EOD | ABANDON
    # v3.4 — combined: tiered exits + abandon rule on untaken trades
    combo_pnl: float = 0.0
    combo_r: float = 0.0
    combo_exit_desc: str = ""
    # v3.4.1 — MFE-based early kill: close at close of bar 8 if MFE < 0.3R
    mfe_kill_pnl: float = 0.0
    mfe_kill_r: float = 0.0
    mfe_kill_exit_reason: str = ""   # STOP | TARGET | EOD | MFE_KILL
    # v3.4.1 — tight abandon: bar 12 close + |current R| < 0.3 AND MAE < -0.5R
    tight_abandon_pnl: float = 0.0
    tight_abandon_r: float = 0.0
    tight_abandon_exit_reason: str = ""  # STOP | TARGET | EOD | TIGHT_ABANDON
    # v3.4.1 — MFE / MAE snapshots at the decision bars (useful for analysis)
    mfe_r_at_bar_8: float = 0.0
    mae_r_at_bar_8: float = 0.0
    r_at_bar_8: float = 0.0            # current R at close of bar 8
    mfe_r_at_bar_12: float = 0.0
    mae_r_at_bar_12: float = 0.0
    r_at_bar_12: float = 0.0
    # v3.4.1 — MAE exit (MAE<-0.75 AND curR<0 at bar 12) — best from sweep
    mae_exit_pnl: float = 0.0
    mae_exit_r: float = 0.0
    mae_exit_exit_reason: str = ""     # STOP | TARGET | EOD | MAE_EXIT

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

# v3.4 — tiered exit + abandon rule constants
TIERED_T1_R = 1.0             # first tranche hit point (R)
TIERED_T2_R = 1.75            # second tranche hit point (R)
TIERED_FRACTION = 1.0 / 3.0   # exit 1/3 at each tier, let 1/3 run
ABANDON_BAR = 12              # 60 min on 5-min bars
ABANDON_R_WINDOW = 0.5        # trade is "chop" if |r| < this at bar 12

# v3.4.1 — MFE-based early kill (the "40-minute check")
MFE_KILL_BAR = 8              # 40 min on 5-min bars (decision happens at close of bar 8)
MFE_KILL_MIN_R = 0.3          # MFE must have exceeded this R or trade is killed

# v3.4.1 — tight abandon (same bar 12 as ABANDON but with MAE gate)
TIGHT_ABANDON_BAR = 12        # 60 min
TIGHT_ABANDON_MAX_R = 0.3     # only close if current R is below this
TIGHT_ABANDON_MAE_R = -0.5    # AND peak adverse has gone below this

# v3.4.1 — MAE-EXIT rule (strongest from parameter sweep)
# Rationale: the 48 "never-profitable" trades can be partially caught by
# looking for trades that have ALREADY gone significantly against us (MAE
# past -0.75R) AND are not recovering (current R still below breakeven).
MAE_EXIT_BAR = 12             # 60 min
MAE_EXIT_MAE_R = -0.75        # MAE must have gone below this
MAE_EXIT_MAX_CUR_R = 0.0      # AND current R must be below this


def _simulate_trade(
    df: pd.DataFrame,
    entry_time: datetime,
    entry: float,
    stop: float,
    target: float,
) -> dict:
    """
    Walk forward through bars after entry_time and simulate SIX exit
    strategies in parallel so we can compare without re-scanning:

      1. FLAT          — legacy behaviour: first of stop/target/EOD close.
      2. TIERED        — 33% at +1R (then stop→breakeven on remainder),
                         33% at +1.75R (then trail stop at prev-bar low),
                         33% runs to target or EOD.
      3. ABANDON       — FLAT, but if at bar 12 the trade is inside ±0.5R
                         of entry, exit flat at that bar's close.
      4. COMBO         — TIERED + abandon rule on untaken trades.
      5. MFE_KILL      — FLAT, but at close of bar 8 check: if peak MFE
                         so far is below 0.3R, close flat ("40-minute
                         check"). Stops/targets that hit before bar 8
                         resolve normally first.
      6. TIGHT_ABANDON — FLAT, but at close of bar 12 check: if current
                         R < 0.3 AND peak MAE so far is below -0.5R,
                         close flat. Stricter than ABANDON — only kills
                         trades that have actually gone against us.

    Also records MFE / MAE (max favorable / adverse excursion in R units)
    lifetime AND at the bar-8 and bar-12 decision points.
    """
    eod_ts = datetime.combine(entry_time.date(), EOD_EXIT_TIME).replace(tzinfo=config.ET)
    future = df[(df.index > entry_time)]

    stop_distance = entry - stop
    if stop_distance <= 0:
        # Malformed trade — return a no-op zeroed result
        return _empty_sim_result(entry, entry_time)

    price_1r = entry + stop_distance * TIERED_T1_R
    price_175r = entry + stop_distance * TIERED_T2_R
    target_price = target  # usually entry + 2.5R

    # ── FLAT state ─────────────────────────────────────────────
    flat: Optional[tuple[float, object, str, int]] = None

    # ── ABANDON state (a second copy of FLAT that may short-circuit) ───
    abandon: Optional[tuple[float, object, str, int]] = None

    # ── MFE_KILL state (FLAT but with a bar-8 MFE check) ───
    mfe_kill: Optional[tuple[float, object, str, int]] = None

    # ── TIGHT_ABANDON state (FLAT but with a bar-12 MAE-gated chop check) ───
    tight_abandon: Optional[tuple[float, object, str, int]] = None

    # ── MAE_EXIT state (bar-12: close if MAE<-0.75 AND curR<0) ───
    mae_exit: Optional[tuple[float, object, str, int]] = None

    # MFE/MAE snapshots at the decision bars
    mfe_r_at_bar_8 = 0.0
    mae_r_at_bar_8 = 0.0
    r_at_bar_8 = 0.0
    mfe_r_at_bar_12 = 0.0
    mae_r_at_bar_12 = 0.0
    r_at_bar_12 = 0.0

    # ── TIERED state ───────────────────────────────────────────
    tiered_pnl_per_share = 0.0      # accumulated $/share across closed tranches
    tiered_remaining = 1.0          # fraction of position still open
    tiered_stop = stop              # current stop for the open remainder
    t1_hit = False
    t2_hit = False
    tiered_done: Optional[tuple[object, str, int]] = None
    tiered_desc_parts: list[str] = []

    # ── COMBO state (tiered + abandon-on-untaken) ──────────────
    combo_pnl_per_share = 0.0
    combo_remaining = 1.0
    combo_stop = stop
    combo_t1_hit = False
    combo_t2_hit = False
    combo_done: Optional[tuple[object, str, int]] = None
    combo_desc_parts: list[str] = []

    # ── MFE / MAE ──────────────────────────────────────────────
    mfe_r = 0.0
    mae_r = 0.0
    mfe_bar = 0
    mae_bar = 0

    prev_low: Optional[float] = None   # used for trailing stop after T2
    bars = 0
    last_ts = entry_time
    last_close = entry

    for ts, row in future.iterrows():
        bars += 1
        last_ts = ts
        high = float(row["High"])
        low = float(row["Low"])
        close = float(row["Close"])
        last_close = close

        # Track MFE / MAE in R units
        bar_high_r = (high - entry) / stop_distance
        bar_low_r = (low - entry) / stop_distance
        if bar_high_r > mfe_r:
            mfe_r = bar_high_r
            mfe_bar = bars
        if bar_low_r < mae_r:
            mae_r = bar_low_r
            mae_bar = bars

        # ── FLAT resolution (stop-first conservative) ──
        if flat is None:
            if low <= stop:
                flat = (stop, ts, "STOP", bars)
            elif high >= target_price:
                flat = (target_price, ts, "TARGET", bars)
            elif ts >= eod_ts:
                flat = (close, ts, "EOD", bars)

        # ── ABANDON resolution (same as flat, but add chop check at bar 12) ──
        if abandon is None:
            if low <= stop:
                abandon = (stop, ts, "STOP", bars)
            elif high >= target_price:
                abandon = (target_price, ts, "TARGET", bars)
            elif ts >= eod_ts:
                abandon = (close, ts, "EOD", bars)
            elif bars == ABANDON_BAR:
                cur_r = (close - entry) / stop_distance
                if abs(cur_r) < ABANDON_R_WINDOW:
                    abandon = (close, ts, "ABANDON", bars)

        # ── MFE_KILL resolution (stop/target/EOD plus bar-8 MFE check) ──
        if mfe_kill is None:
            if low <= stop:
                mfe_kill = (stop, ts, "STOP", bars)
            elif high >= target_price:
                mfe_kill = (target_price, ts, "TARGET", bars)
            elif ts >= eod_ts:
                mfe_kill = (close, ts, "EOD", bars)
            elif bars == MFE_KILL_BAR:
                # mfe_r here is already updated to include this bar's high
                if mfe_r < MFE_KILL_MIN_R:
                    mfe_kill = (close, ts, "MFE_KILL", bars)

        # ── TIGHT_ABANDON resolution (bar-12 chop + MAE gate) ──
        if tight_abandon is None:
            if low <= stop:
                tight_abandon = (stop, ts, "STOP", bars)
            elif high >= target_price:
                tight_abandon = (target_price, ts, "TARGET", bars)
            elif ts >= eod_ts:
                tight_abandon = (close, ts, "EOD", bars)
            elif bars == TIGHT_ABANDON_BAR:
                cur_r = (close - entry) / stop_distance
                # Only kill if the trade has actually gone against us (MAE gate)
                # AND it isn't currently showing meaningful upside progress
                if cur_r < TIGHT_ABANDON_MAX_R and mae_r < TIGHT_ABANDON_MAE_R:
                    tight_abandon = (close, ts, "TIGHT_ABANDON", bars)

        # ── MAE_EXIT resolution (stronger bar-12 gate: MAE<-0.75 AND curR<0) ──
        if mae_exit is None:
            if low <= stop:
                mae_exit = (stop, ts, "STOP", bars)
            elif high >= target_price:
                mae_exit = (target_price, ts, "TARGET", bars)
            elif ts >= eod_ts:
                mae_exit = (close, ts, "EOD", bars)
            elif bars == MAE_EXIT_BAR:
                cur_r = (close - entry) / stop_distance
                if mae_r < MAE_EXIT_MAE_R and cur_r < MAE_EXIT_MAX_CUR_R:
                    mae_exit = (close, ts, "MAE_EXIT", bars)

        # Snapshot MFE/MAE/R at the decision bars (use post-bar values)
        if bars == MFE_KILL_BAR:
            mfe_r_at_bar_8 = mfe_r
            mae_r_at_bar_8 = mae_r
            r_at_bar_8 = (close - entry) / stop_distance
        if bars == TIGHT_ABANDON_BAR:
            mfe_r_at_bar_12 = mfe_r
            mae_r_at_bar_12 = mae_r
            r_at_bar_12 = (close - entry) / stop_distance

        # ── TIERED resolution ──
        if tiered_done is None:
            # Check stop first (conservative — assume stop hit before upside if same bar)
            if low <= tiered_stop:
                tiered_pnl_per_share += (tiered_stop - entry) * tiered_remaining
                tiered_desc_parts.append(f"REST@STOP")
                tiered_done = (ts, _tiered_reason(t1_hit, t2_hit, "STOP"), bars)
                tiered_remaining = 0.0
            else:
                # Tranche 1 (1R)
                if (not t1_hit) and high >= price_1r:
                    tiered_pnl_per_share += (price_1r - entry) * TIERED_FRACTION
                    tiered_remaining -= TIERED_FRACTION
                    tiered_stop = entry  # move stop to breakeven
                    t1_hit = True
                    tiered_desc_parts.append("T1@1R")
                # Tranche 2 (1.75R) — can hit same bar as T1
                if t1_hit and (not t2_hit) and high >= price_175r:
                    tiered_pnl_per_share += (price_175r - entry) * TIERED_FRACTION
                    tiered_remaining -= TIERED_FRACTION
                    if prev_low is not None and prev_low > tiered_stop:
                        tiered_stop = prev_low
                    t2_hit = True
                    tiered_desc_parts.append("T2@1.75R")
                # Final target — can hit same bar if stars align
                if t2_hit and high >= target_price:
                    tiered_pnl_per_share += (target_price - entry) * tiered_remaining
                    tiered_desc_parts.append("RUN@TARGET")
                    tiered_done = (ts, "TIERED_TARGET", bars)
                    tiered_remaining = 0.0
                # After T2, update trailing stop to highest prev-bar low seen so far
                if t2_hit and tiered_done is None and prev_low is not None and prev_low > tiered_stop:
                    tiered_stop = prev_low
                # EOD on remaining
                if tiered_done is None and ts >= eod_ts:
                    tiered_pnl_per_share += (close - entry) * tiered_remaining
                    tiered_desc_parts.append("REST@EOD")
                    tiered_done = (ts, _tiered_reason(t1_hit, t2_hit, "EOD"), bars)
                    tiered_remaining = 0.0

        # ── COMBO resolution (tiered + abandon-on-untaken) ──
        if combo_done is None:
            if low <= combo_stop:
                combo_pnl_per_share += (combo_stop - entry) * combo_remaining
                combo_desc_parts.append("REST@STOP")
                combo_done = (ts, _tiered_reason(combo_t1_hit, combo_t2_hit, "STOP"), bars)
                combo_remaining = 0.0
            else:
                # Abandon-on-untaken: if bar 12 and no tranche banked, close flat
                if (bars == ABANDON_BAR) and (not combo_t1_hit):
                    cur_r = (close - entry) / stop_distance
                    if abs(cur_r) < ABANDON_R_WINDOW:
                        combo_pnl_per_share += (close - entry) * combo_remaining
                        combo_desc_parts.append("ABANDON@CHOP")
                        combo_done = (ts, "COMBO_ABANDON", bars)
                        combo_remaining = 0.0
                if combo_done is None:
                    if (not combo_t1_hit) and high >= price_1r:
                        combo_pnl_per_share += (price_1r - entry) * TIERED_FRACTION
                        combo_remaining -= TIERED_FRACTION
                        combo_stop = entry
                        combo_t1_hit = True
                        combo_desc_parts.append("T1@1R")
                    if combo_t1_hit and (not combo_t2_hit) and high >= price_175r:
                        combo_pnl_per_share += (price_175r - entry) * TIERED_FRACTION
                        combo_remaining -= TIERED_FRACTION
                        if prev_low is not None and prev_low > combo_stop:
                            combo_stop = prev_low
                        combo_t2_hit = True
                        combo_desc_parts.append("T2@1.75R")
                    if combo_t2_hit and high >= target_price:
                        combo_pnl_per_share += (target_price - entry) * combo_remaining
                        combo_desc_parts.append("RUN@TARGET")
                        combo_done = (ts, "COMBO_TARGET", bars)
                        combo_remaining = 0.0
                    if combo_t2_hit and combo_done is None and prev_low is not None and prev_low > combo_stop:
                        combo_stop = prev_low
                    if combo_done is None and ts >= eod_ts:
                        combo_pnl_per_share += (close - entry) * combo_remaining
                        combo_desc_parts.append("REST@EOD")
                        combo_done = (ts, _tiered_reason(combo_t1_hit, combo_t2_hit, "EOD"), bars)
                        combo_remaining = 0.0

        prev_low = low

        # Early out if all seven strategies have resolved
        if (flat is not None and abandon is not None
                and tiered_done is not None and combo_done is not None
                and mfe_kill is not None and tight_abandon is not None
                and mae_exit is not None):
            break

    # Fallbacks if we ran out of bars without resolution
    if flat is None:
        flat = (last_close, last_ts, "EOD", bars) if bars > 0 else (entry, entry_time, "NO_DATA", 0)
    if abandon is None:
        abandon = flat
    if mfe_kill is None:
        mfe_kill = flat
    if tight_abandon is None:
        tight_abandon = flat
    if mae_exit is None:
        mae_exit = flat
    if tiered_done is None and tiered_remaining > 0:
        tiered_pnl_per_share += (last_close - entry) * tiered_remaining
        tiered_desc_parts.append("REST@EOD")
        tiered_done = (last_ts, _tiered_reason(t1_hit, t2_hit, "EOD"), bars)
    if combo_done is None and combo_remaining > 0:
        combo_pnl_per_share += (last_close - entry) * combo_remaining
        combo_desc_parts.append("REST@EOD")
        combo_done = (last_ts, _tiered_reason(combo_t1_hit, combo_t2_hit, "EOD"), bars)

    return {
        "flat": flat,
        "abandon": abandon,
        "mfe_kill": mfe_kill,
        "tight_abandon": tight_abandon,
        "mae_exit": mae_exit,
        "tiered_pnl_per_share": tiered_pnl_per_share,
        "tiered_reason": tiered_done[1] if tiered_done else "NO_DATA",
        "tiered_desc": "+".join(tiered_desc_parts) if tiered_desc_parts else "NO_DATA",
        "tiered_bars": tiered_done[2] if tiered_done else 0,
        "combo_pnl_per_share": combo_pnl_per_share,
        "combo_reason": combo_done[1] if combo_done else "NO_DATA",
        "combo_desc": "+".join(combo_desc_parts) if combo_desc_parts else "NO_DATA",
        "combo_bars": combo_done[2] if combo_done else 0,
        "mfe_r": mfe_r,
        "mae_r": mae_r,
        "mfe_bar": mfe_bar,
        "mae_bar": mae_bar,
        "mfe_r_at_bar_8": mfe_r_at_bar_8,
        "mae_r_at_bar_8": mae_r_at_bar_8,
        "r_at_bar_8": r_at_bar_8,
        "mfe_r_at_bar_12": mfe_r_at_bar_12,
        "mae_r_at_bar_12": mae_r_at_bar_12,
        "r_at_bar_12": r_at_bar_12,
        "stop_distance": stop_distance,
    }


def _tiered_reason(t1: bool, t2: bool, end: str) -> str:
    """Summarize which tranches banked before the final leg closed."""
    # end ∈ {"STOP", "EOD", "TARGET"}
    if not t1:
        return f"TIERED_{end}"
    if not t2:
        return f"TIERED_1R_{end}"
    return f"TIERED_175R_{end}"


def _empty_sim_result(entry: float, entry_time: datetime) -> dict:
    return {
        "flat": (entry, entry_time, "NO_DATA", 0),
        "abandon": (entry, entry_time, "NO_DATA", 0),
        "mfe_kill": (entry, entry_time, "NO_DATA", 0),
        "tight_abandon": (entry, entry_time, "NO_DATA", 0),
        "mae_exit": (entry, entry_time, "NO_DATA", 0),
        "tiered_pnl_per_share": 0.0, "tiered_reason": "NO_DATA",
        "tiered_desc": "NO_DATA", "tiered_bars": 0,
        "combo_pnl_per_share": 0.0, "combo_reason": "NO_DATA",
        "combo_desc": "NO_DATA", "combo_bars": 0,
        "mfe_r": 0.0, "mae_r": 0.0, "mfe_bar": 0, "mae_bar": 0,
        "mfe_r_at_bar_8": 0.0, "mae_r_at_bar_8": 0.0, "r_at_bar_8": 0.0,
        "mfe_r_at_bar_12": 0.0, "mae_r_at_bar_12": 0.0, "r_at_bar_12": 0.0,
        "stop_distance": 0.0,
    }


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
        "reentry": 0, "min_score": 0, "leader_filter": 0,
    }

    # Which leadership labels are ALLOWED if leader_filter_mode is a hard
    # filter. "score" mode = allow everything (legacy v3.3 behaviour).
    _LEADER_FILTER_ALLOWED = {
        "score":      {"LEADER", "SOLO_MOVER", "FOLLOWER", "LAGGARD", "UNKNOWN"},
        "strict":     {"LEADER"},
        "moderate":   {"LEADER", "SOLO_MOVER"},
        "permissive": {"LEADER", "SOLO_MOVER", "FOLLOWER"},
    }
    allowed_labels = _LEADER_FILTER_ALLOWED.get(
        filters.leader_filter_mode, _LEADER_FILTER_ALLOWED["score"]
    )

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
                    # Hard-filter mode: drop ticker if label not allowed
                    if leadership.get("label") not in allowed_labels:
                        filter_drops["leader_filter"] += 1
                        continue
                    # Score-adjustment mode: leadership tweaks composite
                    if filters.leader_filter_mode == "score":
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

                # Simulate outcome — returns all four exit variants + MFE/MAE
                sim = _simulate_trade(
                    df_full, scan_ts, entry_px, stop_px, target_px,
                )
                exit_px_raw, exit_ts, reason, bars_held = sim["flat"]
                ab_px_raw,   ab_ts,    ab_reason, ab_bars = sim["abandon"]
                mk_px_raw,   mk_ts,    mk_reason, mk_bars = sim["mfe_kill"]
                ta_px_raw,   ta_ts,    ta_reason, ta_bars = sim["tight_abandon"]
                me_px_raw,   me_ts,    me_reason, me_bars = sim["mae_exit"]
                # Apply exit slippage (loss side) — only on real exits, not NO_DATA
                def _slip(px, rsn):
                    return px * (1 - SLIPPAGE_PCT_PER_SIDE) if rsn != "NO_DATA" else px
                exit_px = _slip(exit_px_raw, reason)
                ab_px   = _slip(ab_px_raw, ab_reason)
                mk_px   = _slip(mk_px_raw, mk_reason)
                ta_px   = _slip(ta_px_raw, ta_reason)
                me_px   = _slip(me_px_raw, me_reason)

                stop_distance = sim.get("stop_distance", entry_px - stop_px)

                # Flat (legacy) P&L
                pnl_dollars = (exit_px - entry_px) * shares
                pnl_pct = (exit_px - entry_px) / entry_px * 100 if entry_px else 0.0
                r = ((exit_px - entry_px) / stop_distance) if stop_distance > 0 else 0.0

                # Abandon-rule P&L
                ab_pnl = (ab_px - entry_px) * shares
                ab_r = ((ab_px - entry_px) / stop_distance) if stop_distance > 0 else 0.0

                # MFE_KILL P&L
                mk_pnl = (mk_px - entry_px) * shares
                mk_r = ((mk_px - entry_px) / stop_distance) if stop_distance > 0 else 0.0

                # TIGHT_ABANDON P&L
                ta_pnl = (ta_px - entry_px) * shares
                ta_r = ((ta_px - entry_px) / stop_distance) if stop_distance > 0 else 0.0

                # MAE_EXIT P&L
                me_pnl = (me_px - entry_px) * shares
                me_r = ((me_px - entry_px) / stop_distance) if stop_distance > 0 else 0.0

                # Tiered P&L — per-share already accounts for tranche sizing
                tiered_ps = sim["tiered_pnl_per_share"]
                # Apply a rough slippage haircut: half-trip on all three tranches
                tiered_ps_net = tiered_ps - (entry_px * SLIPPAGE_PCT_PER_SIDE)
                tiered_pnl = tiered_ps_net * shares
                tiered_r = tiered_ps_net / stop_distance if stop_distance > 0 else 0.0

                # Combo (tiered + abandon-on-untaken)
                combo_ps = sim["combo_pnl_per_share"]
                combo_ps_net = combo_ps - (entry_px * SLIPPAGE_PCT_PER_SIDE)
                combo_pnl = combo_ps_net * shares
                combo_r = combo_ps_net / stop_distance if stop_distance > 0 else 0.0

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
                    # v3.4 additions
                    mfe_r=round(sim["mfe_r"], 3),
                    mae_r=round(sim["mae_r"], 3),
                    mfe_bar=sim["mfe_bar"],
                    mae_bar=sim["mae_bar"],
                    tiered_pnl=round(tiered_pnl, 2),
                    tiered_r=round(tiered_r, 3),
                    tiered_exit_desc=sim["tiered_desc"],
                    tiered_exit_reason=sim["tiered_reason"],
                    abandoned=(ab_reason == "ABANDON"),
                    abandon_pnl=round(ab_pnl, 2),
                    abandon_r=round(ab_r, 3),
                    abandon_exit_reason=ab_reason,
                    combo_pnl=round(combo_pnl, 2),
                    combo_r=round(combo_r, 3),
                    combo_exit_desc=sim["combo_desc"],
                    # v3.4.1 additions
                    mfe_kill_pnl=round(mk_pnl, 2),
                    mfe_kill_r=round(mk_r, 3),
                    mfe_kill_exit_reason=mk_reason,
                    tight_abandon_pnl=round(ta_pnl, 2),
                    tight_abandon_r=round(ta_r, 3),
                    tight_abandon_exit_reason=ta_reason,
                    mae_exit_pnl=round(me_pnl, 2),
                    mae_exit_r=round(me_r, 3),
                    mae_exit_exit_reason=me_reason,
                    mfe_r_at_bar_8=round(sim.get("mfe_r_at_bar_8", 0.0), 3),
                    mae_r_at_bar_8=round(sim.get("mae_r_at_bar_8", 0.0), 3),
                    r_at_bar_8=round(sim.get("r_at_bar_8", 0.0), 3),
                    mfe_r_at_bar_12=round(sim.get("mfe_r_at_bar_12", 0.0), 3),
                    mae_r_at_bar_12=round(sim.get("mae_r_at_bar_12", 0.0), 3),
                    r_at_bar_12=round(sim.get("r_at_bar_12", 0.0), 3),
                )
                # v3.4.1: pick the headline exit based on filters.exit_strategy.
                # This mutates pnl_dollars/r_multiple/exit_reason on the trade
                # so every downstream metric reflects the chosen strategy.
                _apply_exit_strategy(tr, filters.exit_strategy)
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
#  HEADLINE-STRATEGY ROUTING
# ═══════════════════════════════════════════════════════════════

def _apply_exit_strategy(tr: "Trade", strategy: str) -> None:
    """v3.4.1 — rewrite the 'headline' fields (pnl_dollars / r_multiple /
    exit_reason) on a Trade so they reflect whichever parallel simulation
    the user selected as primary. All parallel-track fields remain intact.

    Note: exit / exit_time / pnl_pct are left pointing at the FLAT exit
    (they're legacy fields — the critical headline fields are pnl_dollars
    and r_multiple, which feed every aggregate metric)."""
    # Freeze FLAT values first so downstream comparisons still work
    tr.flat_pnl = tr.pnl_dollars
    tr.flat_r = tr.r_multiple
    tr.flat_exit_reason = tr.exit_reason
    if strategy in (None, "", "flat"):
        return  # flat is already the default — nothing to do
    if strategy == "tiered":
        tr.pnl_dollars = tr.tiered_pnl
        tr.r_multiple  = tr.tiered_r
        tr.exit_reason = tr.tiered_exit_reason or tr.exit_reason
    elif strategy == "abandon":
        tr.pnl_dollars = tr.abandon_pnl
        tr.r_multiple  = tr.abandon_r
        tr.exit_reason = tr.abandon_exit_reason or tr.exit_reason
    elif strategy == "combo":
        tr.pnl_dollars = tr.combo_pnl
        tr.r_multiple  = tr.combo_r
        # Combo reuses the tiered exit description for its "reason"
        tr.exit_reason = tr.tiered_exit_reason or tr.exit_reason
    elif strategy == "mfe_kill":
        tr.pnl_dollars = tr.mfe_kill_pnl
        tr.r_multiple  = tr.mfe_kill_r
        tr.exit_reason = tr.mfe_kill_exit_reason or tr.exit_reason
    elif strategy == "tight_abandon":
        tr.pnl_dollars = tr.tight_abandon_pnl
        tr.r_multiple  = tr.tight_abandon_r
        tr.exit_reason = tr.tight_abandon_exit_reason or tr.exit_reason
    elif strategy == "mae_exit":
        tr.pnl_dollars = tr.mae_exit_pnl
        tr.r_multiple  = tr.mae_exit_r
        tr.exit_reason = tr.mae_exit_exit_reason or tr.exit_reason
    # Unknown strategy → leave flat (graceful fallback)


# ═══════════════════════════════════════════════════════════════
#  AGGREGATION / METRICS
# ═══════════════════════════════════════════════════════════════

def _metrics_for(
    trades: list["Trade"],
    pnl_attr: str = "pnl_dollars",
    r_attr: str = "r_multiple",
) -> dict:
    """Compute the same headline metrics for any P&L column on the Trade object.
    Lets us report flat / tiered / abandon / combo side-by-side.
    Win definition: pnl > 0 (v3.4 redefinition — any positive counts as a win)."""
    if not trades:
        return {
            "trade_count": 0, "wins": 0, "losses": 0, "breakeven": 0,
            "win_rate": 0.0, "avg_r": 0.0, "total_pnl": 0.0,
            "profit_factor": 0.0, "max_drawdown": 0.0,
            "expectancy_per_trade": 0.0,
        }
    pnls = [getattr(t, pnl_attr) for t in trades]
    rs = [getattr(t, r_attr) for t in trades]

    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    breakeven = sum(1 for p in pnls if p == 0)
    total_pnl = float(sum(pnls))
    wr = wins / len(trades) * 100
    avg_r = float(np.mean(rs))

    gross_profit = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p < 0))
    if gross_loss > 0:
        pf = gross_profit / gross_loss
    elif gross_profit > 0:
        pf = None  # serialized as null = infinite
    else:
        pf = 0.0

    # Max drawdown (ordered by scan_date then scan_time)
    ordered = sorted(trades, key=lambda t: (t.scan_date, t.scan_time))
    eq = 0.0; peak = 0.0; max_dd = 0.0
    for t in ordered:
        eq += getattr(t, pnl_attr)
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)

    return {
        "trade_count": len(trades),
        "wins": wins, "losses": losses, "breakeven": breakeven,
        "win_rate": round(wr, 1),
        "avg_r": round(avg_r, 3),
        "total_pnl": round(total_pnl, 2),
        "profit_factor": round(pf, 2) if pf is not None else None,
        "max_drawdown": round(max_dd, 2),
        "expectancy_per_trade": round(total_pnl / len(trades), 2),
    }


def _aggregate(trades: list[Trade], start_date: date, end_date: date) -> dict:
    """Compute headline stats, breakdowns, equity curve, and side-by-side
    comparisons across flat / tiered / abandon / combo exit strategies.

    v3.4 changes:
      - Win = pnl > 0 (used to be exit_reason == "TARGET")
      - Added tiered_summary / abandon_summary / combo_summary
      - Added MFE / MAE aggregate stats
    """
    if not trades:
        return {
            "trade_count": 0, "wins": 0, "losses": 0,
            "win_rate": 0.0, "avg_r": 0.0, "total_pnl": 0.0,
            "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
            "profit_factor": 0.0, "max_drawdown": 0.0,
            "expectancy_per_trade": 0.0,
            "equity_curve": [], "by_hour": {}, "by_sector": {},
            "by_leadership": {}, "by_regime": {}, "by_exit_reason": {},
            "tiered_summary": {}, "abandon_summary": {}, "combo_summary": {},
            "mfe_mae_stats": {},
            "mfe_kill_summary": {}, "tight_abandon_summary": {},
            "mae_exit_summary": {},
            "exit_rule_diagnostics": {},
        }

    # ── Core metrics (HEADLINE strategy — whichever filters.exit_strategy
    #    picked. pnl_dollars / r_multiple already reflect that strategy.) ──
    flat_metrics = _metrics_for(trades, "pnl_dollars", "r_multiple")

    # Parallel exit-strategy summaries — always computed, always raw
    flat_summary = _metrics_for(trades, "flat_pnl", "flat_r")
    tiered_summary = _metrics_for(trades, "tiered_pnl", "tiered_r")
    abandon_summary = _metrics_for(trades, "abandon_pnl", "abandon_r")
    combo_summary = _metrics_for(trades, "combo_pnl", "combo_r")
    mfe_kill_summary = _metrics_for(trades, "mfe_kill_pnl", "mfe_kill_r")
    tight_abandon_summary = _metrics_for(trades, "tight_abandon_pnl", "tight_abandon_r")
    mae_exit_summary = _metrics_for(trades, "mae_exit_pnl", "mae_exit_r")

    # Exit-reason counts (keep legacy fields for UI back-compat)
    target_hits = sum(1 for t in trades if t.exit_reason == "TARGET")
    stop_outs = sum(1 for t in trades if t.exit_reason == "STOP")
    eod_exits = sum(1 for t in trades if t.exit_reason == "EOD")
    abandon_fires = sum(1 for t in trades if t.abandoned)

    # Avg win / loss % (flat, for context)
    win_trades = [t for t in trades if t.pnl_dollars > 0]
    loss_trades = [t for t in trades if t.pnl_dollars < 0]
    avg_win_pct = float(np.mean([t.pnl_pct for t in win_trades])) if win_trades else 0.0
    avg_loss_pct = float(np.mean([t.pnl_pct for t in loss_trades])) if loss_trades else 0.0

    # Equity curve (flat PnL)
    ordered = sorted(trades, key=lambda t: (t.scan_date, t.scan_time))
    eq = 0.0; equity_curve = []
    for t in ordered:
        eq += t.pnl_dollars
        equity_curve.append({
            "date": t.scan_date, "time": t.scan_time,
            "equity": round(eq, 2), "trade_pnl": t.pnl_dollars,
            "ticker": t.ticker,
        })

    # MFE / MAE aggregate stats (how much of the peak favorable we kept / gave back)
    mfes = [t.mfe_r for t in trades]
    maes = [t.mae_r for t in trades]
    gave_back = sum(1 for t in trades if t.mfe_r >= 1.0 and t.pnl_dollars <= 0)
    never_profitable = sum(1 for t in trades if t.mfe_r < 0.5)
    hit_1r_at_peak = sum(1 for t in trades if t.mfe_r >= 1.0)
    hit_175r_at_peak = sum(1 for t in trades if t.mfe_r >= 1.75)
    hit_25r_at_peak = sum(1 for t in trades if t.mfe_r >= 2.4)
    mfe_mae_stats = {
        "avg_mfe_r": round(float(np.mean(mfes)), 3),
        "avg_mae_r": round(float(np.mean(maes)), 3),
        "median_mfe_r": round(float(np.median(mfes)), 3),
        "median_mae_r": round(float(np.median(maes)), 3),
        "hit_1r_at_peak": hit_1r_at_peak,
        "hit_175r_at_peak": hit_175r_at_peak,
        "hit_25r_at_peak": hit_25r_at_peak,
        "never_profitable_trades": never_profitable,   # MFE < 0.5R
        "gave_back_trades": gave_back,                  # hit ≥1R then lost money
    }

    # v3.4.1 — MFE_KILL diagnostic stats
    # Classify every trade by what the MFE-kill rule would have done and
    # compare its final flat outcome to the rule's outcome.
    mfe_kill_fires = sum(1 for t in trades if t.mfe_kill_exit_reason == "MFE_KILL")
    tight_abandon_fires = sum(1 for t in trades if t.tight_abandon_exit_reason == "TIGHT_ABANDON")
    mae_exit_fires = sum(1 for t in trades if t.mae_exit_exit_reason == "MAE_EXIT")

    # Among the trades the MFE-kill rule would have closed at bar 8,
    # how did flat actually end up?
    killed_trades = [t for t in trades if t.mfe_kill_exit_reason == "MFE_KILL"]
    killed_flat_pnl = sum(t.pnl_dollars for t in killed_trades)
    killed_mfe_kill_pnl = sum(t.mfe_kill_pnl for t in killed_trades)
    killed_rule_saves = killed_flat_pnl - killed_mfe_kill_pnl    # positive = rule loses you money, negative = rule saves money
    killed_flat_winners = sum(1 for t in killed_trades if t.pnl_dollars > 0)  # false-positives (rule closed a winner)

    # Trades the rule DIDN'T kill — those that cleared the 0.3R MFE bar by bar 8
    survived_trades = [t for t in trades if t.mfe_kill_exit_reason != "MFE_KILL" and t.mfe_kill_exit_reason != "NO_DATA"]
    survived_flat_pnl = sum(t.pnl_dollars for t in survived_trades)

    # Same for TIGHT_ABANDON
    tight_killed = [t for t in trades if t.tight_abandon_exit_reason == "TIGHT_ABANDON"]
    tight_killed_flat_pnl = sum(t.pnl_dollars for t in tight_killed)
    tight_killed_rule_pnl = sum(t.tight_abandon_pnl for t in tight_killed)
    tight_killed_flat_winners = sum(1 for t in tight_killed if t.pnl_dollars > 0)

    exit_rule_diagnostics = {
        "mfe_kill": {
            "fires": mfe_kill_fires,
            "fires_pct": round(mfe_kill_fires / len(trades) * 100, 1) if trades else 0.0,
            "killed_flat_pnl": round(killed_flat_pnl, 2),
            "killed_rule_pnl": round(killed_mfe_kill_pnl, 2),
            "net_pnl_impact_on_killed": round(killed_mfe_kill_pnl - killed_flat_pnl, 2),
            "killed_that_were_flat_winners": killed_flat_winners,
            "survived_count": len(survived_trades),
            "survived_flat_pnl": round(survived_flat_pnl, 2),
        },
        "tight_abandon": {
            "fires": tight_abandon_fires,
            "fires_pct": round(tight_abandon_fires / len(trades) * 100, 1) if trades else 0.0,
            "killed_flat_pnl": round(tight_killed_flat_pnl, 2),
            "killed_rule_pnl": round(tight_killed_rule_pnl, 2),
            "net_pnl_impact_on_killed": round(tight_killed_rule_pnl - tight_killed_flat_pnl, 2),
            "killed_that_were_flat_winners": tight_killed_flat_winners,
        },
        "mae_exit": {
            "fires": mae_exit_fires,
            "fires_pct": round(mae_exit_fires / len(trades) * 100, 1) if trades else 0.0,
            "killed_flat_pnl": round(sum(t.pnl_dollars for t in trades if t.mae_exit_exit_reason == "MAE_EXIT"), 2),
            "killed_rule_pnl": round(sum(t.mae_exit_pnl for t in trades if t.mae_exit_exit_reason == "MAE_EXIT"), 2),
            "net_pnl_impact_on_killed": round(
                sum(t.mae_exit_pnl - t.pnl_dollars for t in trades if t.mae_exit_exit_reason == "MAE_EXIT"), 2
            ),
            "killed_that_were_flat_winners": sum(
                1 for t in trades if t.mae_exit_exit_reason == "MAE_EXIT" and t.pnl_dollars > 0
            ),
        },
    }

    # Breakdowns — by_hour / by_sector / etc. Win is now pnl > 0.
    def bucket(key_fn) -> dict:
        d: dict[str, dict] = {}
        for t in trades:
            k = key_fn(t) or "—"
            b = d.setdefault(k, {"trades": 0, "wins": 0, "losses": 0,
                                 "pnl": 0.0, "tiered_pnl": 0.0, "combo_pnl": 0.0})
            b["trades"] += 1
            b["pnl"] += t.pnl_dollars
            b["tiered_pnl"] += t.tiered_pnl
            b["combo_pnl"] += t.combo_pnl
            if t.pnl_dollars > 0:
                b["wins"] += 1
            elif t.pnl_dollars < 0:
                b["losses"] += 1
        for k, b in d.items():
            b["win_rate"] = round(b["wins"] / b["trades"] * 100, 1) if b["trades"] else 0.0
            b["pnl"] = round(b["pnl"], 2)
            b["tiered_pnl"] = round(b["tiered_pnl"], 2)
            b["combo_pnl"] = round(b["combo_pnl"], 2)
        return d

    by_hour = bucket(lambda t: t.scan_time[:2] + ":00")
    by_sector = bucket(lambda t: TICKER_TO_SECTOR.get(t.ticker, "Other"))
    by_leadership = bucket(lambda t: t.leadership_label)
    by_regime = bucket(lambda t: t.regime_label)
    by_exit = bucket(lambda t: t.exit_reason)

    return {
        **flat_metrics,
        # Legacy field names for UI back-compat
        "eod_exits": eod_exits,
        "target_hits": target_hits,
        "stop_outs": stop_outs,
        "abandon_fires": abandon_fires,
        "avg_win_pct": round(avg_win_pct, 2),
        "avg_loss_pct": round(avg_loss_pct, 2),
        "equity_curve": equity_curve,
        "by_hour": by_hour,
        "by_sector": by_sector,
        "by_leadership": by_leadership,
        "by_regime": by_regime,
        "by_exit_reason": by_exit,
        # v3.4 additions
        "tiered_summary": tiered_summary,
        "abandon_summary": abandon_summary,
        "combo_summary": combo_summary,
        "mfe_mae_stats": mfe_mae_stats,
        # v3.4.1 additions
        "flat_summary": flat_summary,
        "mfe_kill_summary": mfe_kill_summary,
        "tight_abandon_summary": tight_abandon_summary,
        "mae_exit_summary": mae_exit_summary,
        "exit_rule_diagnostics": exit_rule_diagnostics,
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
