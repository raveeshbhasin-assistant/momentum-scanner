"""
Momentum Scanner Engine
───────────────────────
Scans S&P 500 stocks for intraday momentum opportunities.
Combines technical analysis (65%), news sentiment (25%), and volume (10%)
into a composite score, then generates entry/exit recommendations.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from ta.trend import EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands, AverageTrueRange

import config
from news import get_sentiment_score, get_news_headlines

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_intraday_data(tickers: list[str], interval: str = "5m", days: int = 5) -> dict[str, pd.DataFrame]:
    """
    Fetch intraday candle data for a list of tickers using yfinance.
    Returns dict of {ticker: DataFrame with OHLCV columns}.
    Quick-fails if the first test batch returns no data (e.g., Yahoo blocked).
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

    logger.info(f"Fetched intraday data for {len(data)} tickers")
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
    """
    if df.empty or "Volume" not in df.columns:
        return 0.0

    try:
        now = df.index[-1]
        current_hour = now.hour
        current_minute = now.minute

        # Get volume bars from similar times on previous days
        historical_vols = []
        for idx, row in df.iterrows():
            if idx.hour == current_hour and abs(idx.minute - current_minute) <= 10:
                if idx.date() != now.date():
                    historical_vols.append(row["Volume"])

        if not historical_vols:
            # Fallback: compare to overall average
            avg_vol = df["Volume"].mean()
            current_vol = df["Volume"].iloc[-1]
            return current_vol / avg_vol if avg_vol > 0 else 0.0

        avg_vol = np.mean(historical_vols)
        current_vol = df["Volume"].iloc[-1]
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

    # ── 4. VWAP Position (15 points) ─────────────────────────
    max_score += 15
    vwap = latest.get("VWAP")
    if pd.notna(vwap) and vwap > 0:
        price_vs_vwap = (latest["Close"] - vwap) / vwap * 100
        if price_vs_vwap > 0.5:
            score += 15
            details["vwap"] = {"score": 15, "max": 15,
                               "status": f"Price {price_vs_vwap:.1f}% above VWAP — strong", "bullish": True}
        elif price_vs_vwap > 0:
            score += 10
            details["vwap"] = {"score": 10, "max": 15,
                               "status": f"Slightly above VWAP", "bullish": True}
        else:
            details["vwap"] = {"score": 0, "max": 15,
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

    return {
        "entry": entry,
        "stop_loss": stop_loss,
        "target": target,
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

def run_scan(tickers: Optional[list[str]] = None) -> list[dict]:
    """
    Run a full momentum scan across all tickers.
    Returns a list of signal dicts, sorted by composite score descending.
    """
    if tickers is None:
        tickers = config.SP500_LIQUID

    logger.info(f"Starting scan of {len(tickers)} tickers...")
    start_time = time.time()

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

            # Score technicals
            tech_score, tech_details = score_technicals(df)

            # Get news sentiment
            sentiment_score, news_items = get_sentiment_score(ticker)

            # Volume score (normalized RVOL, capped at 100)
            vol_score = min(rvol / 3.0 * 100, 100)

            # Composite score
            composite = (
                tech_score * config.TECHNICAL_WEIGHT +
                max(sentiment_score * 100, 0) * config.SENTIMENT_WEIGHT +
                vol_score * config.VOLUME_WEIGHT
            )

            if composite < config.MIN_COMPOSITE_SCORE:
                continue

            # Calculate trade levels
            levels = calculate_trade_levels(df)

            # Build signal
            latest = df.iloc[-1]
            signal = {
                "ticker": ticker,
                "timestamp": datetime.now().isoformat(),
                "price": round(float(latest["Close"]), 2),
                "composite_score": round(composite, 1),
                "technical_score": tech_score,
                "sentiment_score": round(sentiment_score, 3),
                "rvol": round(rvol, 2),
                "volume_score": round(vol_score, 1),
                "tech_details": tech_details,
                "trade": levels,
                "news": news_items[:3],  # Top 3 news items
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
