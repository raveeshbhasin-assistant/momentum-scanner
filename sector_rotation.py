"""
Sector Rotation Detection
─────────────────────────
Identifies which sectors are leading intraday by comparing
sector ETF performance and breadth. Used to prioritize
scanning high-beta names within hot sectors.

Data source: yfinance (free) or FMP if available.
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

import config

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  SECTOR ETF DEFINITIONS
# ═══════════════════════════════════════════════════════════════

SECTOR_ETFS = {
    "Technology":    "XLK",
    "Semiconductors": "SMH",
    "Software":      "IGV",
    "AI/Cloud":      "CLOU",
    "Financials":    "XLF",
    "Healthcare":    "XLV",
    "Energy":        "XLE",
    "Consumer Disc": "XLY",
    "Industrials":   "XLI",
    "Materials":     "XLB",
    "Utilities":     "XLU",
    "Real Estate":   "XLRE",
    "Comm Services": "XLC",
    "Consumer Staples": "XLP",
    "Crypto/Blockchain": "BITO",
}

# High-beta stocks mapped to sectors — these are the names
# that move most when a sector rotates in
SECTOR_HIGH_BETA = {
    "Technology": ["AAPL", "MSFT", "GOOGL", "META"],
    "Semiconductors": [
        "NVDA", "AMD", "INTC", "QCOM", "AVGO", "AMAT", "LRCX", "ADI",
        "TXN", "MRVL", "MU", "SMCI", "ARM", "CRDO", "AAOI", "LITE",
        "COHR", "SNDK", "GLW",
    ],
    "Software": [
        "CRM", "ADBE", "INTU", "NOW", "SNOW", "MDB", "NET", "CRWD",
        "PANW", "DDOG", "PATH", "FSLY", "APP", "HIMS",
    ],
    "AI/Cloud": [
        "ORCL", "ANET", "DELL", "SMCI", "IREN", "APLD", "NBIS",
        "MARA", "COIN", "HOOD",
    ],
    "Financials": ["JPM", "GS", "BLK", "SCHW", "SOFI", "HOOD", "COIN"],
    "Healthcare": ["UNH", "LLY", "ABBV", "MRK", "TMO", "ISRG", "VRTX"],
    "Energy": ["XOM", "CVX"],
    "Consumer Disc": [
        "AMZN", "TSLA", "HD", "LOW", "NKE", "TGT", "BKNG",
        "UBER", "ABNB", "DKNG", "W", "WOOF",
    ],
    "Industrials": ["CAT", "DE", "GE", "HON", "BA", "UNP", "FDX", "EMR"],
    "Crypto/Blockchain": [
        "COIN", "HOOD", "MARA", "MSTR", "IREN", "APLD",
        "CRML", "SOL",
    ],
}


def detect_sector_rotation(top_n: int = 3) -> list[dict]:
    """
    Detect which sectors are leading today by comparing intraday
    ETF performance. Returns top N sectors sorted by strength.

    Each result: {
        "sector": str,
        "etf": str,
        "change_pct": float,     # Intraday % change
        "rel_strength": float,   # vs SPY
        "high_beta_tickers": list[str],
    }
    """
    etf_tickers = list(SECTOR_ETFS.values()) + ["SPY"]

    try:
        df = yf.download(
            tickers=etf_tickers,
            period="1d",
            interval="5m",
            group_by="ticker",
            progress=False,
            threads=True,
        )
    except Exception as e:
        logger.warning(f"Sector ETF download failed: {e}")
        return []

    if df.empty:
        return []

    # Calculate intraday % change for each ETF
    spy_change = _calc_intraday_change(df, "SPY")
    if spy_change is None:
        spy_change = 0.0

    results = []
    for sector, etf in SECTOR_ETFS.items():
        change = _calc_intraday_change(df, etf)
        if change is None:
            continue

        rel_strength = change - spy_change

        results.append({
            "sector": sector,
            "etf": etf,
            "change_pct": round(change, 3),
            "rel_strength": round(rel_strength, 3),
            "high_beta_tickers": SECTOR_HIGH_BETA.get(sector, []),
        })

    # Sort by relative strength (strongest first)
    results.sort(key=lambda x: x["rel_strength"], reverse=True)

    if results:
        top = results[:top_n]
        top_info = [(r["sector"], round(r["rel_strength"], 2)) for r in top]
        logger.info(f"Sector rotation: top sectors = {top_info}")

    return results[:top_n]


def get_sector_priority_tickers(top_sectors: list[dict]) -> list[str]:
    """
    Given the top sectors from detect_sector_rotation(),
    return a deduplicated list of high-beta tickers to prioritize.
    """
    tickers = []
    seen = set()
    for sector in top_sectors:
        for t in sector.get("high_beta_tickers", []):
            if t not in seen:
                tickers.append(t)
                seen.add(t)
    return tickers


def _calc_intraday_change(df: pd.DataFrame, ticker: str) -> Optional[float]:
    """Calculate intraday % change from open to latest close for a ticker."""
    try:
        if len(list(SECTOR_ETFS.values())) + 1 > 1:
            if ticker in df.columns.get_level_values(0):
                ticker_df = df[ticker]
            else:
                return None
        else:
            ticker_df = df

        ticker_df = ticker_df.dropna(subset=["Close"])
        if len(ticker_df) < 2:
            return None

        open_price = ticker_df["Open"].iloc[0]
        latest_close = ticker_df["Close"].iloc[-1]
        if open_price <= 0:
            return None

        return (latest_close - open_price) / open_price * 100
    except Exception:
        return None
