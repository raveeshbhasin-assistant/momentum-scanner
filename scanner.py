"""
Momentum Scanner Engine
───────────────────────
Scans S&P 500 stocks for intraday momentum opportunities.
Combines technical analysis (65%), news sentiment (25%), and volume (10%)
into a composite score, then generates entry/exit recommendations.

Data source priority: FMP (real-time) → yfinance (free fallback)
"""

import logging
import time
from datetime import datetime, timedelta
import config as _config_module  # for ET timezone
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from ta.trend import EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands, AverageTrueRange

import config
from news import get_sentiment_score
from sector_rotation import classify_leadership, TICKER_TO_SECTOR
from market_regime import get_regime
from earnings import get_earnings_context

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  INTRADAY % CHANGE HELPER (v3.3)
# ═══════════════════════════════════════════════════════════════

def _calc_ticker_intraday_pct(df: pd.DataFrame) -> Optional[float]:
    """
    Compute today's intraday % change (session open → latest close) for a
    ticker's 5-min bars. Returns None if we can't isolate today's bars.

    v3.4.2: Replaced the previous normalize()-based today_mask, which could
    raise on tz-aware vs tz-naive comparisons and silently fall back to
    None for every ticker. We now compare dates directly via the index's
    per-element .date() accessor, which is tz-safe.
    """
    try:
        if df.empty:
            return None
        last_ts = df.index[-1]
        if not hasattr(last_ts, "date"):
            return None
        today = last_ts.date()
        # Tz-safe: compare each index entry's calendar date.
        today_mask = pd.Index([ts.date() == today for ts in df.index])
        today_df = df[today_mask]
        if len(today_df) < 2:
            return None
        open_px = float(today_df["Open"].iloc[0])
        last_px = float(today_df["Close"].iloc[-1])
        if open_px <= 0:
            return None
        return (last_px - open_px) / open_px * 100
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
#  DATA FETCHING — FMP (primary) with yfinance fallback
# ═══════════════════════════════════════════════════════════════

def fetch_intraday_data(tickers: list[str], interval: str = "5m", days: int = 5) -> dict[str, pd.DataFrame]:
    """
    Fetch intraday candle data for a list of tickers.
    Uses FMP (real-time) when API key is available, falls back to yfinance.
    Returns dict of {ticker: DataFrame with OHLCV columns}.
    """
    # Map yfinance-style interval to FMP-style
    interval_map = {"5m": "5min", "1m": "1min", "15m": "15min", "30m": "30min", "1h": "1hour"}
    fmp_interval = interval_map.get(interval, "5min")

    if config.FMP_API_KEY:
        logger.info("Using FMP for real-time intraday data")
        try:
            from fmp_data import fetch_intraday_data as fmp_fetch
            data = fmp_fetch(tickers, interval=fmp_interval, days=days)
            if data:
                return data
            logger.warning("FMP returned no data — falling back to yfinance")
        except Exception as e:
            logger.warning(f"FMP fetch failed: {e} — falling back to yfinance")

    # Fallback: yfinance (15-min delayed, free)
    logger.info("Using yfinance for intraday data (15-min delayed)")
    return _fetch_intraday_yfinance(tickers, interval, days)


def _fetch_intraday_yfinance(tickers: list[str], interval: str = "5m", days: int = 5) -> dict[str, pd.DataFrame]:
    """
    Fetch intraday candle data using yfinance (free, 15-min delayed).
    Kept as fallback when FMP is unavailable.
    """
    data = {}

    # Quick connectivity test with first 2 tickers
    test_batch = tickers[:2]
    try:
        test_df = yf.download(test_batch, period="1d", interval="5m", progress=False, threads=False)
        if test_df.empty:
            logger.warning("Yahoo Finance connectivity test failed — no data returned")
            return {}
    except Exception as e:
        logger.warning(f"Yahoo Finance unavailable: {e}")
        return {}

    # Full download in batches
    batch_size = 20
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            df = yf.download(
                tickers=batch,
                period=f"{days}d",
                interval=interval,
                group_by="ticker",
                progress=False,
                threads=True,
            )
            if df.empty:
                continue

            for ticker in batch:
                try:
                    if len(batch) == 1:
                        ticker_df = df.copy()
                    else:
                        ticker_df = df[ticker].copy()

                    ticker_df = ticker_df.dropna(subset=["Close"])
                    if len(ticker_df) >= 50:  # Need enough bars for indicators
                        # Flatten MultiIndex columns if present
                        if isinstance(ticker_df.columns, pd.MultiIndex):
                            ticker_df.columns = ticker_df.columns.get_level_values(0)
                        data[ticker] = ticker_df
                except (KeyError, TypeError):
                    continue
        except Exception as e:
            logger.warning(f"Failed to fetch batch {batch[:3]}...: {e}")
            continue
        time.sleep(0.5)  # Rate-limit courtesy

    logger.info(f"yfinance: Fetched intraday data for {len(data)} tickers")
    return data


# ═══════════════════════════════════════════════════════════════
#  TECHNICAL INDICATOR CALCULATIONS
# ═══════════════════════════════════════════════════════════════

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate all technical indicators on a DataFrame with OHLCV columns.
    Returns the DataFrame with indicator columns added.
    """
    close = df["Close"]
    high = df["High"]
    low = df["Low"]

    # EMAs
    df[f"EMA_{config.EMA_FAST}"] = EMAIndicator(close, window=config.EMA_FAST).ema_indicator()
    df[f"EMA_{config.EMA_MID}"] = EMAIndicator(close, window=config.EMA_MID).ema_indicator()
    df[f"EMA_{config.EMA_SLOW}"] = EMAIndicator(close, window=config.EMA_SLOW).ema_indicator()

    # RSI
    df["RSI"] = RSIIndicator(close, window=config.RSI_PERIOD).rsi()

    # MACD
    macd_ind = MACD(close, window_fast=config.MACD_FAST, window_slow=config.MACD_SLOW, window_sign=config.MACD_SIGNAL)
    df[f"MACD_{config.MACD_FAST}_{config.MACD_SLOW}_{config.MACD_SIGNAL}"] = macd_ind.macd()
    df[f"MACDs_{config.MACD_FAST}_{config.MACD_SLOW}_{config.MACD_SIGNAL}"] = macd_ind.macd_signal()
    df[f"MACDh_{config.MACD_FAST}_{config.MACD_SLOW}_{config.MACD_SIGNAL}"] = macd_ind.macd_diff()

    # Bollinger Bands
    bb_ind = BollingerBands(close, window=config.BB_PERIOD, window_dev=config.BB_STD)
    df[f"BBU_{config.BB_PERIOD}_{config.BB_STD}"] = bb_ind.bollinger_hband()
    df[f"BBL_{config.BB_PERIOD}_{config.BB_STD}"] = bb_ind.bollinger_lband()
    df[f"BBM_{config.BB_PERIOD}_{config.BB_STD}"] = bb_ind.bollinger_mavg()

    # ATR
    df["ATR"] = AverageTrueRange(high, low, close, window=config.ATR_PERIOD).average_true_range()

    # VWAP (intraday) — manual calculation
    try:
        typical_price = (high + low + close) / 3
        cumulative_tp_vol = (typical_price * df["Volume"]).cumsum()
        cumulative_vol = df["Volume"].cumsum()
        df["VWAP"] = cumulative_tp_vol / cumulative_vol
    except Exception:
        df["VWAP"] = (high + low + close) / 3

    return df


def calculate_rvol(df: pd.DataFrame) -> float:
    """
    Calculate Relative Volume (RVOL) adjusted for time of day.
    Compares current volume to average volume at the same time over past sessions.

    v3.4.2: If the most recent bar is still accumulating (i.e., "now" falls
    inside that bar's 5-minute window), we use the second-to-last bar — the
    most recent COMPLETE bar — as the reference. Comparing a 20-second
    partial bar to a full 5-min historical bar was deflating RVOL by ~15×
    and killing the whole scan whenever it coincided with the first seconds
    of a new candle.
    """
    if df.empty or "Volume" not in df.columns or len(df) < 2:
        return 0.0

    try:
        # Pick the latest COMPLETE bar as the reference.
        last_ts = df.index[-1]
        try:
            if hasattr(last_ts, "tz") and last_ts.tz is not None:
                now_ref = pd.Timestamp.now(tz=last_ts.tz)
            else:
                now_ref = pd.Timestamp.utcnow().tz_localize(None)
        except Exception:
            now_ref = pd.Timestamp.now()
        bar_end = last_ts + pd.Timedelta(minutes=5)
        is_partial = now_ref < bar_end

        if is_partial and len(df) >= 2:
            ref_idx = df.index[-2]
            current_vol = df["Volume"].iloc[-2]
        else:
            ref_idx = df.index[-1]
            current_vol = df["Volume"].iloc[-1]

        current_hour = ref_idx.hour
        current_minute = ref_idx.minute

        # Get volume bars from similar times on previous days
        historical_vols = []
        for idx, row in df.iterrows():
            if idx == ref_idx:
                continue
            if idx.hour == current_hour and abs(idx.minute - current_minute) <= 10:
                if idx.date() != ref_idx.date():
                    historical_vols.append(row["Volume"])

        if not historical_vols:
            # Fallback: compare to overall average (ignore the partial bar)
            avg_vol = df["Volume"].iloc[:-1].mean() if is_partial else df["Volume"].mean()
            return current_vol / avg_vol if avg_vol > 0 else 0.0

        avg_vol = np.mean(historical_vols)
        return current_vol / avg_vol if avg_vol > 0 else 0.0

    except Exception:
        return 1.0


# ═══════════════════════════════════════════════════════════════
#  SCORING ENGINE
# ═══════════════════════════════════════════════════════════════

def score_technicals(df: pd.DataFrame) -> tuple[float, dict]:
    """
    Score a stock's technical setup from 0-100.
    Returns (score, details_dict) where details explains each factor.
    """
    if len(df) < 2:
        return 0.0, {}

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    details = {}
    score = 0.0
    max_score = 0.0

    # ── 1. EMA Alignment (25 points) ─────────────────────────
    max_score += 25
    ema_f = latest.get(f"EMA_{config.EMA_FAST}")
    ema_m = latest.get(f"EMA_{config.EMA_MID}")
    ema_s = latest.get(f"EMA_{config.EMA_SLOW}")

    if pd.notna(ema_f) and pd.notna(ema_m) and pd.notna(ema_s):
        if ema_f > ema_m > ema_s:
            score += 25
            details["ema"] = {"score": 25, "max": 25, "status": "Bullish alignment (9 > 21 > 50)", "bullish": True}
        elif ema_f > ema_m:
            score += 15
            details["ema"] = {"score": 15, "max": 25, "status": "Partial alignment (9 > 21)", "bullish": True}
        elif latest["Close"] > ema_f:
            score += 8
            details["ema"] = {"score": 8, "max": 25, "status": "Price above fast EMA", "bullish": True}
        else:
            details["ema"] = {"score": 0, "max": 25, "status": "Bearish EMA structure", "bullish": False}

    # ── 2. RSI Momentum (20 points) ──────────────────────────
    max_score += 20
    rsi = latest.get("RSI")
    if pd.notna(rsi):
        if config.RSI_MOMENTUM_LOW <= rsi <= config.RSI_MOMENTUM_HIGH:
            rsi_score = 20
            details["rsi"] = {"score": 20, "max": 20, "value": round(rsi, 1),
                              "status": f"In momentum zone ({rsi:.0f})", "bullish": True}
        elif 50 <= rsi < config.RSI_MOMENTUM_LOW:
            rsi_score = 12
            details["rsi"] = {"score": 12, "max": 20, "value": round(rsi, 1),
                              "status": f"Building momentum ({rsi:.0f})", "bullish": True}
        elif rsi > config.RSI_MOMENTUM_HIGH:
            rsi_score = 5
            details["rsi"] = {"score": 5, "max": 20, "value": round(rsi, 1),
                              "status": f"Overbought — caution ({rsi:.0f})", "bullish": False}
        else:
            rsi_score = 0
            details["rsi"] = {"score": 0, "max": 20, "value": round(rsi, 1),
                              "status": f"Weak/bearish ({rsi:.0f})", "bullish": False}
        score += rsi_score

    # ── 3. MACD (20 points) ──────────────────────────────────
    max_score += 20
    macd_h_col = f"MACDh_{config.MACD_FAST}_{config.MACD_SLOW}_{config.MACD_SIGNAL}"
    macd_col = f"MACD_{config.MACD_FAST}_{config.MACD_SLOW}_{config.MACD_SIGNAL}"
    macd_s_col = f"MACDs_{config.MACD_FAST}_{config.MACD_SLOW}_{config.MACD_SIGNAL}"

    macd_h = latest.get(macd_h_col)
    prev_macd_h = prev.get(macd_h_col)

    if pd.notna(macd_h) and pd.notna(prev_macd_h):
        if macd_h > 0 and macd_h > prev_macd_h:
            score += 20
            details["macd"] = {"score": 20, "max": 20, "status": "Bullish & accelerating", "bullish": True}
        elif macd_h > 0:
            score += 14
            details["macd"] = {"score": 14, "max": 20, "status": "Bullish histogram", "bullish": True}
        elif macd_h > prev_macd_h:
            score += 8
            details["macd"] = {"score": 8, "max": 20, "status": "Bearish but improving", "bullish": True}
        else:
            details["macd"] = {"score": 0, "max": 20, "status": "Bearish & declining", "bullish": False}

    # ── 4. VWAP Position + Crossover Detection (20 points) ────
    # Upgraded: detect the MOMENT of VWAP crossover, not just position
    max_score += 20
    vwap = latest.get("VWAP")
    prev_vwap = prev.get("VWAP") if len(df) >= 2 else None

    if pd.notna(vwap) and vwap > 0:
        price_vs_vwap = (latest["Close"] - vwap) / vwap * 100
        prev_below = pd.notna(prev_vwap) and prev["Close"] < prev_vwap
        now_above = latest["Close"] > vwap

        if prev_below and now_above:
            # VWAP crossover detected — this is the earliest signal
            score += 20
            details["vwap"] = {"score": 20, "max": 20,
                               "status": f"VWAP CROSSOVER detected! Price just broke above VWAP", "bullish": True}
        elif price_vs_vwap > 0.5:
            score += 15
            details["vwap"] = {"score": 15, "max": 20,
                               "status": f"Price {price_vs_vwap:.1f}% above VWAP — strong", "bullish": True}
        elif price_vs_vwap > 0:
            score += 10
            details["vwap"] = {"score": 10, "max": 20,
                               "status": f"Slightly above VWAP", "bullish": True}
        else:
            details["vwap"] = {"score": 0, "max": 20,
                               "status": f"Below VWAP ({price_vs_vwap:.1f}%)", "bullish": False}

    # ── 5. Bollinger Band Position (20 points) ───────────────
    max_score += 20
    bbu_col = f"BBU_{config.BB_PERIOD}_{config.BB_STD}"
    bbl_col = f"BBL_{config.BB_PERIOD}_{config.BB_STD}"
    bbm_col = f"BBM_{config.BB_PERIOD}_{config.BB_STD}"

    bbu = latest.get(bbu_col)
    bbl = latest.get(bbl_col)
    bbm = latest.get(bbm_col)

    if pd.notna(bbu) and pd.notna(bbl) and pd.notna(bbm) and (bbu - bbl) > 0:
        bb_position = (latest["Close"] - bbl) / (bbu - bbl)
        bb_width = (bbu - bbl) / bbm * 100

        if 0.5 < bb_position < 0.85:
            score += 20
            details["bb"] = {"score": 20, "max": 20,
                             "status": f"Upper half of bands, room to run (width: {bb_width:.1f}%)", "bullish": True}
        elif bb_position >= 0.85:
            score += 8
            details["bb"] = {"score": 8, "max": 20,
                             "status": f"Near upper band — possible breakout or exhaustion", "bullish": True}
        elif 0.3 < bb_position <= 0.5:
            score += 10
            details["bb"] = {"score": 10, "max": 20,
                             "status": f"Middle of bands — neutral", "bullish": True}
        else:
            details["bb"] = {"score": 0, "max": 20,
                             "status": f"Lower bands — bearish", "bullish": False}

    # Normalize to 0-100
    normalized = (score / max_score * 100) if max_score > 0 else 0
    return round(normalized, 1), details


def calculate_pivot_levels(df: pd.DataFrame) -> dict:
    """
    Calculate Standard (Floor) Pivot Point resistance/support levels
    from the prior trading day's High, Low, Close.
    This mirrors how Barchart calculates its resistance levels.
    """
    try:
        # Get the prior day's OHLC — group by date and take the second-to-last day
        daily = df.copy()
        daily["date"] = daily.index.date
        grouped = daily.groupby("date").agg({"High": "max", "Low": "min", "Close": "last"})

        if len(grouped) < 2:
            return {}

        # Prior day's values (second-to-last complete day)
        prev_day = grouped.iloc[-2]
        h = float(prev_day["High"])
        l = float(prev_day["Low"])
        c = float(prev_day["Close"])

        # Standard Pivot Point formula
        pivot = (h + l + c) / 3.0
        r1 = (2.0 * pivot) - l
        r2 = pivot + (h - l)
        r3 = h + 2.0 * (pivot - l)
        s1 = (2.0 * pivot) - h
        s2 = pivot - (h - l)
        s3 = l - 2.0 * (h - pivot)

        return {
            "pivot": round(pivot, 2),
            "r1": round(r1, 2),
            "r2": round(r2, 2),
            "r3": round(r3, 2),
            "s1": round(s1, 2),
            "s2": round(s2, 2),
            "s3": round(s3, 2),
        }
    except Exception:
        return {}


def find_nearest_resistance(pivot_levels: dict, entry_price: float) -> dict:
    """
    Find the nearest resistance level above the entry price.
    Returns the level name and value.
    """
    if not pivot_levels:
        return {"level": "N/A", "price": 0.0}

    resistances = [
        ("R1", pivot_levels.get("r1", 0)),
        ("R2", pivot_levels.get("r2", 0)),
        ("R3", pivot_levels.get("r3", 0)),
    ]

    # Filter to levels above entry, pick nearest
    above = [(name, price) for name, price in resistances if price > entry_price]
    if above:
        nearest = min(above, key=lambda x: x[1])
        return {"level": nearest[0], "price": nearest[1]}

    # If price is above all resistance levels, return R3
    return {"level": "Above R3", "price": resistances[-1][1]}


def calculate_trade_levels(df: pd.DataFrame) -> dict:
    """
    Calculate entry, stop-loss, and profit target levels.
    Uses ATR-based stops with configurable risk-reward ratio.
    """
    latest = df.iloc[-1]
    atr = latest.get("ATR")

    if pd.isna(atr) or atr <= 0:
        atr = latest["Close"] * 0.02  # Fallback: 2% of price

    entry = round(latest["Close"], 2)
    stop_distance = round(atr * config.ATR_STOP_MULTIPLIER, 2)
    stop_loss = round(entry - stop_distance, 2)
    target = round(entry + (stop_distance * config.RISK_REWARD_RATIO), 2)

    # Position sizing
    risk_dollars = config.DEFAULT_ACCOUNT_SIZE * (config.RISK_PER_TRADE_PCT / 100)
    shares = int(risk_dollars / stop_distance) if stop_distance > 0 else 0
    position_value = round(shares * entry, 2)

    # Risk/reward as percentage
    risk_pct = round(stop_distance / entry * 100, 2)
    reward_pct = round((target - entry) / entry * 100, 2)

    # Pivot point resistance levels (Barchart-style)
    pivot_levels = calculate_pivot_levels(df)
    nearest_resistance = find_nearest_resistance(pivot_levels, entry)

    return {
        "entry": entry,
        "stop_loss": stop_loss,
        "target": target,
        "resistance_target": nearest_resistance["price"],
        "resistance_level": nearest_resistance["level"],
        "pivot_levels": pivot_levels,
        "stop_distance": stop_distance,
        "atr": round(atr, 2),
        "risk_pct": risk_pct,
        "reward_pct": reward_pct,
        "risk_reward_ratio": config.RISK_REWARD_RATIO,
        "suggested_shares": shares,
        "position_value": position_value,
        "risk_dollars": round(risk_dollars, 2),
    }


# ═══════════════════════════════════════════════════════════════
#  MAIN SCAN FUNCTION
# ═══════════════════════════════════════════════════════════════

def run_scan(tickers: Optional[list[str]] = None,
             sector_priority: Optional[list[str]] = None) -> list[dict]:
    """
    Run a full momentum scan across all tickers.
    Returns a list of signal dicts, sorted by composite score descending.

    v3.2 features:
    - Expanded universe (SP500 + high-beta extended)
    - Sector rotation boost for tickers in hot sectors
    - Pre-market catalyst boost (additive, never filters)
    - VWAP crossover detection for earlier signals

    v3.3 features:
    - Sector Leadership classification (Leader / Follower / Laggard / Solo Mover)
    - Market Regime via VIX (dynamic min-score threshold + size multiplier)
    - Earnings calendar context (badges + -5 pts if tomorrow; block after 2pm on AMC days)
    """
    if tickers is None:
        tickers = config.get_full_universe()

    logger.info(f"Starting scan of {len(tickers)} tickers...")
    start_time = time.time()

    # ── Market Regime (v3.3) ──
    regime = get_regime() if config.MARKET_REGIME_ENABLED else {
        "label": "NORMAL",
        "effective_min_score": config.MIN_COMPOSITE_SCORE,
        "size_multiplier": 1.0,
        "vix": None, "vix_change_pct": None, "color": "var(--text-muted)",
        "spiked": False, "error": None,
    }
    effective_min_score = regime.get("effective_min_score", config.MIN_COMPOSITE_SCORE)
    size_mult = regime.get("size_multiplier", 1.0)
    logger.info(
        f"Regime: {regime.get('label')} — min_score={effective_min_score}, "
        f"size_mult={size_mult}"
    )

    # Import pre-market module for score boost
    try:
        from premarket import get_premarket_boost, is_premarket_flagged
    except ImportError:
        get_premarket_boost = lambda t: 0.0
        is_premarket_flagged = lambda t: False

    # Build set of sector-priority tickers for boost
    sector_priority_set = set(sector_priority) if sector_priority else set()

    # Fetch data
    all_data = fetch_intraday_data(tickers, interval=config.CANDLE_INTERVAL, days=config.CANDLE_LOOKBACK_DAYS)

    signals = []

    for ticker, df in all_data.items():
        try:
            # Calculate indicators
            df = calculate_indicators(df)

            # Check RVOL filter
            rvol = calculate_rvol(df)
            if rvol < config.MIN_RVOL:
                continue

            # ── Earnings hard filter (v3.3): block entries after 2pm on AMC days ──
            earnings_ctx = get_earnings_context(ticker)
            if earnings_ctx.get("hard_filter"):
                logger.info(
                    f"EARNINGS FILTER: skipping {ticker} — {earnings_ctx.get('reason')}"
                )
                continue

            # Score technicals
            tech_score, tech_details = score_technicals(df)

            # Get news sentiment
            sentiment_score, news_items = get_sentiment_score(ticker)

            # Volume score (normalized RVOL, capped at 100)
            # Penalize extreme RVOL > 5x (possible exhaustion)
            if rvol > 5.0:
                vol_score = min(rvol / 3.0 * 100, 100) * 0.7  # 30% penalty
            else:
                vol_score = min(rvol / 3.0 * 100, 100)

            # Composite score (base)
            composite = (
                tech_score * config.TECHNICAL_WEIGHT +
                max(sentiment_score * 100, 0) * config.SENTIMENT_WEIGHT +
                vol_score * config.VOLUME_WEIGHT
            )

            # ── BOOST: Sector rotation ──────────────────────────
            sector_boost = 0.0
            if ticker in sector_priority_set:
                sector_boost = config.SECTOR_BOOST_POINTS
                composite += sector_boost

            # ── BOOST: Pre-market catalyst ──────────────────────
            pm_boost = get_premarket_boost(ticker)
            if pm_boost > 0:
                composite += pm_boost

            # ── TIER / ADJUST: Sector Leadership (v3.4.2) ───────
            # v3.4.2 change: leadership is a DISPLAY TIER hint in the live
            # scanner, not a hard filter. Every label reaches the signal
            # output; the UI groups by tier and user decides which to act
            # on. Backtest still supports hard-gate modes via Filters.
            # Modes retained for backwards compat on configured deploys:
            #   "display" (v3.4.2 default) — no hard gate, no score adj
            #   "moderate"/"strict"/"permissive"/"score" — legacy behaviour
            ticker_pct = _calc_ticker_intraday_pct(df)
            leadership = classify_leadership(
                ticker,
                ticker_pct if ticker_pct is not None else 0.0,
            )
            _LEADER_ALLOWED = {
                "display":    {"LEADER", "SOLO_MOVER", "FOLLOWER", "LAGGARD", "UNKNOWN"},
                "score":      {"LEADER", "SOLO_MOVER", "FOLLOWER", "LAGGARD", "UNKNOWN"},
                "strict":     {"LEADER"},
                "moderate":   {"LEADER", "SOLO_MOVER"},
                "permissive": {"LEADER", "SOLO_MOVER", "FOLLOWER"},
            }
            _mode = getattr(config, "LEADER_FILTER_MODE", "display")
            if leadership.get("label") not in _LEADER_ALLOWED.get(_mode, _LEADER_ALLOWED["display"]):
                continue  # legacy hard-filter drop
            # Score adjustment applies only in legacy "score" mode
            leader_adj = leadership.get("score_adjustment", 0) if _mode == "score" else 0
            composite += leader_adj
            # Display tier: primary / secondary / unclassified (v3.4.2)
            _lbl = leadership.get("label", "UNKNOWN")
            if _lbl in ("LEADER", "SOLO_MOVER"):
                leader_tier = "primary"
            elif _lbl == "FOLLOWER":
                leader_tier = "secondary"
            else:
                leader_tier = "unclassified"  # LAGGARD / UNKNOWN

            # ── ADJUST: Earnings context (v3.3) ─────────────────
            earnings_adj = earnings_ctx.get("score_adjustment", 0)
            composite += earnings_adj

            # ── SCORE GATE (v3.4.2 soft) ───────────────────────
            # Show anything at or above WEAK_SIGNAL_FLOOR; tag it as
            # strong (>= effective_min_score) or weak (in between).
            weak_floor = getattr(config, "WEAK_SIGNAL_FLOOR", 40)
            if composite < weak_floor:
                continue
            signal_strength = "strong" if composite >= effective_min_score else "weak"

            # Calculate trade levels
            levels = calculate_trade_levels(df)

            # Apply regime-based position-size multiplier to the recommendation
            if size_mult != 1.0:
                levels["suggested_shares"] = int(levels.get("suggested_shares", 0) * size_mult)
                levels["position_value"] = round(levels["suggested_shares"] * levels["entry"], 2)
                levels["risk_dollars"] = round(
                    levels["suggested_shares"] * levels["stop_distance"], 2
                )
                levels["size_multiplier"] = size_mult

            # Build signal
            latest = df.iloc[-1]
            signal = {
                "ticker": ticker,
                "timestamp": datetime.now(_config_module.ET).isoformat(),
                "price": round(float(latest["Close"]), 2),
                "composite_score": round(composite, 1),
                "technical_score": tech_score,
                "sentiment_score": round(sentiment_score, 3),
                "rvol": round(rvol, 2),
                "volume_score": round(vol_score, 1),
                "sector_boost": round(sector_boost, 1),
                "premarket_boost": round(pm_boost, 1),
                "leader_adjustment": leader_adj,
                "leader_tier": leader_tier,      # v3.4.2: primary/secondary/unclassified
                "signal_strength": signal_strength,   # v3.4.2: strong/weak
                "earnings_adjustment": earnings_adj,
                "is_premarket_flagged": is_premarket_flagged(ticker),
                "leadership": leadership,        # v3.3
                "earnings": earnings_ctx,        # v3.3
                "regime_label": regime.get("label"),     # v3.3
                "tech_details": tech_details,
                "trade": levels,
                "news": news_items[:3],
                "indicators": {
                    "rsi": round(float(latest.get("RSI", 0)), 1) if pd.notna(latest.get("RSI")) else None,
                    "ema_9": round(float(latest.get(f"EMA_{config.EMA_FAST}", 0)), 2) if pd.notna(latest.get(f"EMA_{config.EMA_FAST}")) else None,
                    "ema_21": round(float(latest.get(f"EMA_{config.EMA_MID}", 0)), 2) if pd.notna(latest.get(f"EMA_{config.EMA_MID}")) else None,
                    "ema_50": round(float(latest.get(f"EMA_{config.EMA_SLOW}", 0)), 2) if pd.notna(latest.get(f"EMA_{config.EMA_SLOW}")) else None,
                    "vwap": round(float(latest.get("VWAP", 0)), 2) if pd.notna(latest.get("VWAP")) else None,
                    "atr": round(float(latest.get("ATR", 0)), 2) if pd.notna(latest.get("ATR")) else None,
                },
                # Chart data: last 78 bars (~6.5 hours of 5-min candles)
                "chart_data": _prepare_chart_data(df.tail(78)),
            }
            signals.append(signal)

        except Exception as e:
            logger.warning(f"Error scanning {ticker}: {e}")
            continue

    # Sort by composite score
    signals.sort(key=lambda x: x["composite_score"], reverse=True)
    signals = signals[:config.MAX_SIGNALS_PER_SCAN]

    elapsed = round(time.time() - start_time, 1)
    logger.info(f"Scan complete: {len(signals)} signals found in {elapsed}s")

    return signals


def _prepare_chart_data(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of OHLCV dicts for TradingView Lightweight Charts."""
    records = []
    for idx, row in df.iterrows():
        ts = int(idx.timestamp()) if hasattr(idx, 'timestamp') else 0
        records.append({
            "time": ts,
            "open": round(float(row["Open"]), 2),
            "high": round(float(row["High"]), 2),
            "low": round(float(row["Low"]), 2),
            "close": round(float(row["Close"]), 2),
            "volume": int(row["Volume"]) if pd.notna(row["Volume"]) else 0,
        })
    return records
