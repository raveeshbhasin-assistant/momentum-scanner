"""
Pre-Market Catalyst Scanner
────────────────────────────
Detects stocks with unusual pre-market volume and fresh news catalysts.
Runs at 8:00 and 9:00 AM ET to flag stocks BEFORE the 9:31 open scan.

This is ADDITIVE — it boosts scores for flagged tickers but never
filters out stocks that only start moving after the open.
"""

import logging
from datetime import datetime
from typing import Optional

import yfinance as yf
import pandas as pd

import config
from news import get_sentiment_score

logger = logging.getLogger(__name__)

# Tickers flagged pre-market this session (reset daily)
_premarket_flags: dict[str, dict] = {}  # ticker -> {reason, volume_ratio, sentiment, boost}


def reset_daily():
    """Reset pre-market flags at start of each trading day."""
    global _premarket_flags
    _premarket_flags = {}
    logger.info("Pre-market flags reset for new trading day")


def get_premarket_boost(ticker: str) -> float:
    """
    Return the score boost for a ticker that was flagged pre-market.
    Returns 0.0 if the ticker was not flagged.
    """
    flag = _premarket_flags.get(ticker)
    if flag:
        return flag.get("boost", 0.0)
    return 0.0


def get_premarket_flags() -> dict[str, dict]:
    """Return all pre-market flagged tickers and their details."""
    return _premarket_flags.copy()


def is_premarket_flagged(ticker: str) -> bool:
    """Check if a ticker was flagged in pre-market."""
    return ticker in _premarket_flags


def run_premarket_scan(tickers: list[str]) -> list[dict]:
    """
    Scan tickers for pre-market activity.
    Checks: pre-market volume vs average, news sentiment.

    Returns list of flagged tickers with boost details.
    Does NOT filter anything — only flags tickers for score boost.
    """
    if not tickers:
        return []

    logger.info(f"Running pre-market scan on {len(tickers)} tickers...")
    flagged = []

    # Batch download current data to check pre-market volume
    try:
        df = yf.download(
            tickers=tickers[:50],  # Limit batch size
            period="2d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            prepost=True,  # Include pre/post market data
        )
    except Exception as e:
        logger.warning(f"Pre-market data download failed: {e}")
        return []

    if df.empty:
        return []

    for ticker in tickers[:50]:
        try:
            # Get ticker's daily data
            if len(tickers) > 1:
                if ticker in df.columns.get_level_values(0):
                    ticker_df = df[ticker].dropna(subset=["Close"])
                else:
                    continue
            else:
                ticker_df = df.dropna(subset=["Close"])

            if len(ticker_df) < 2:
                continue

            # Compare today's pre-market volume to yesterday's full volume
            today_vol = ticker_df["Volume"].iloc[-1] if pd.notna(ticker_df["Volume"].iloc[-1]) else 0
            prev_vol = ticker_df["Volume"].iloc[-2] if pd.notna(ticker_df["Volume"].iloc[-2]) else 1
            vol_ratio = today_vol / max(prev_vol, 1)

            # Check for news catalyst
            sentiment, _ = get_sentiment_score(ticker)

            # Flag criteria: unusual volume OR strong news sentiment
            boost = 0.0
            reasons = []

            if vol_ratio > 2.0:
                boost += min(vol_ratio * 2, 10)  # Up to +10 points for 5x volume
                reasons.append(f"Pre-market volume {vol_ratio:.1f}x normal")

            if sentiment > 0.3:
                boost += sentiment * 10  # Up to +10 for very bullish news
                reasons.append(f"Bullish news sentiment ({sentiment:.2f})")
            elif sentiment < -0.2:
                # Negative sentiment = don't penalize, just don't boost
                pass

            if boost > 0:
                flag = {
                    "ticker": ticker,
                    "boost": round(min(boost, 15), 1),  # Cap at +15 points
                    "volume_ratio": round(vol_ratio, 2),
                    "sentiment": round(sentiment, 3),
                    "reasons": reasons,
                    "flagged_at": datetime.now(config.ET).strftime("%I:%M %p ET"),
                }
                _premarket_flags[ticker] = flag
                flagged.append(flag)
                logger.info(f"Pre-market flag: {ticker} boost={boost:.1f} — {', '.join(reasons)}")

        except Exception as e:
            continue

    logger.info(f"Pre-market scan complete: {len(flagged)} tickers flagged")
    return flagged
