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


# ═══════════════════════════════════════════════════════════════
#  SECTOR LOOKUPS (v3.3 / v3.3.1)
# ═══════════════════════════════════════════════════════════════
# SECTOR_HIGH_BETA above powers *sector-rotation priority scanning* —
# it intentionally holds only high-beta names so that when Tech rotates
# in we prioritize NVDA/AMD etc., not IBM.
#
# For the LEADERSHIP classifier we also need sectors for stable blue-
# chips and the newer speculative adds, so classify_leadership() can
# always find a sector ETF benchmark. SECTOR_ADDITIONAL_TICKERS below
# extends the map without polluting the rotation priority list.

SECTOR_ADDITIONAL_TICKERS = {
    "Technology": [
        "CSCO", "ACN", "IBM", "ADP",
    ],
    "Healthcare": [
        "JNJ", "ABT", "DHR", "AMGN", "GILD", "MDT", "SYK",
        "REGN", "CI", "ZTS", "BDX",
    ],
    "Financials": [
        "BRK-B", "V", "MA", "MMC", "CB", "CME", "USB", "PNC",
        "ICE", "SQ",
    ],
    "Consumer Staples": [
        "PG", "PEP", "KO", "COST", "WMT", "MCD", "PM", "MDLZ",
        "MO", "CL", "SBUX",
    ],
    "Industrials": [
        "RTX", "MMM", "NOC", "WM", "OKLO", "EOSE", "MOD",
    ],
    "Consumer Disc": [
        "GM", "F", "RIVN", "AEVA",
    ],
    "Utilities": [
        "NEE", "SO", "DUK",
    ],
    "Real Estate": [
        "PLD", "EQIX",
    ],
    "Materials": [
        "APD", "SHW",
    ],
    "Comm Services": [
        "TMUS", "SNAP",
    ],
    "AI/Cloud": [
        "PLTR",
    ],
}


def _build_ticker_to_sector_map() -> dict[str, str]:
    """Merge SECTOR_HIGH_BETA and SECTOR_ADDITIONAL_TICKERS into a single
    ticker → sector map. High-beta list wins on collisions (on-theme)."""
    mapping: dict[str, str] = {}
    for sector, tickers in SECTOR_HIGH_BETA.items():
        for t in tickers:
            mapping.setdefault(t, sector)
    for sector, tickers in SECTOR_ADDITIONAL_TICKERS.items():
        for t in tickers:
            mapping.setdefault(t, sector)
    return mapping


TICKER_TO_SECTOR = _build_ticker_to_sector_map()


# ═══════════════════════════════════════════════════════════════
#  ROTATION DETECTION
# ═══════════════════════════════════════════════════════════════

# Cache last full sector-rotation result so classify_leadership() can
# use the same intraday change numbers without re-downloading ETF data
# on every ticker lookup.
_last_rotation_snapshot: dict = {
    "ts": None,
    "spy_change": 0.0,
    "sector_changes": {},   # sector_name -> intraday % change
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

    Also populates the module-level _last_rotation_snapshot used by
    classify_leadership() so the leader check is self-consistent with
    the rotation boost.
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
    sector_changes: dict[str, float] = {}
    for sector, etf in SECTOR_ETFS.items():
        change = _calc_intraday_change(df, etf)
        if change is None:
            continue

        rel_strength = change - spy_change
        sector_changes[sector] = change

        results.append({
            "sector": sector,
            "etf": etf,
            "change_pct": round(change, 3),
            "rel_strength": round(rel_strength, 3),
            "high_beta_tickers": SECTOR_HIGH_BETA.get(sector, []),
        })

    # Update the shared snapshot for downstream consumers
    _last_rotation_snapshot["ts"] = datetime.now()
    _last_rotation_snapshot["spy_change"] = spy_change
    _last_rotation_snapshot["sector_changes"] = sector_changes

    # Sort by relative strength (strongest first)
    results.sort(key=lambda x: x["rel_strength"], reverse=True)

    if results:
        top = results[:top_n]
        top_info = [(r["sector"], round(r["rel_strength"], 2)) for r in top]
        logger.info(f"Sector rotation: top sectors = {top_info}")

    return results[:top_n]


# ═══════════════════════════════════════════════════════════════
#  LEADERSHIP CLASSIFICATION (v3.3)
# ═══════════════════════════════════════════════════════════════

def classify_leadership(
    ticker: str,
    ticker_pct: float,
    min_minutes_since_open: int = 15,
    now: Optional[datetime] = None,
) -> dict:
    """
    Classify whether a ticker is leading, following, or lagging its sector.

    Rule set (intraday % vs prior close):
      • LEADER       — ticker > sector AND sector > SPY   (hot sector, on-theme leader)
      • SOLO_MOVER   — ticker > SPY but its sector is lagging SPY (counter-trend mover)
      • FOLLOWER     — ticker > SPY but below its sector average (participating)
      • LAGGARD      — ticker < sector                    (sector moving without it)
      • UNKNOWN      — no sector mapping or no rotation snapshot yet

    Args:
        ticker: symbol being classified
        ticker_pct: ticker's intraday % change (open → latest) as a percent
        min_minutes_since_open: skip classification early in the session when
            intraday % figures are noisy (defaults to 15 min — return UNKNOWN)
        now: override current time (used in backtests)

    Returns:
        {
          "label": "LEADER" | "FOLLOWER" | "LAGGARD" | "SOLO_MOVER" | "UNKNOWN"
          "score_adjustment": int  (+10 / 0 / -10 / +3 / 0)
          "sector": str | None
          "ticker_pct": float
          "sector_pct": float | None
          "spy_pct": float | None
          "reason": str
        }
    """
    result = {
        "label": "UNKNOWN",
        "score_adjustment": 0,
        "sector": TICKER_TO_SECTOR.get(ticker),
        "ticker_pct": round(ticker_pct, 3) if ticker_pct is not None else None,
        "sector_pct": None,
        "spy_pct": None,
        "reason": "",
    }

    # Early-session guard: noisy before 9:45 AM ET
    now = now or datetime.now(config.ET)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if (now - market_open).total_seconds() / 60 < min_minutes_since_open:
        result["reason"] = "Too early in session for reliable classification"
        return result

    sector = result["sector"]
    if not sector:
        result["reason"] = "Ticker not mapped to a sector"
        return result

    snap = _last_rotation_snapshot
    if snap["ts"] is None or sector not in snap["sector_changes"]:
        result["reason"] = "No sector rotation snapshot available"
        return result

    sector_pct = snap["sector_changes"][sector]
    spy_pct = snap["spy_change"]
    result["sector_pct"] = round(sector_pct, 3)
    result["spy_pct"] = round(spy_pct, 3)

    # Classification
    if ticker_pct > sector_pct and sector_pct > spy_pct:
        result["label"] = "LEADER"
        result["score_adjustment"] = config.SECTOR_LEADER_BOOST
        result["reason"] = (
            f"{ticker_pct:+.2f}% > {sector} {sector_pct:+.2f}% > SPY {spy_pct:+.2f}% — "
            "on-theme leader"
        )
    elif ticker_pct > spy_pct and sector_pct < spy_pct:
        result["label"] = "SOLO_MOVER"
        result["score_adjustment"] = config.SECTOR_SOLO_BOOST
        result["reason"] = (
            f"{ticker_pct:+.2f}% > SPY {spy_pct:+.2f}% but {sector} {sector_pct:+.2f}% "
            "is lagging — counter-trend move"
        )
    elif ticker_pct < sector_pct:
        result["label"] = "LAGGARD"
        result["score_adjustment"] = config.SECTOR_LAGGARD_PENALTY  # negative
        result["reason"] = (
            f"{ticker_pct:+.2f}% < {sector} {sector_pct:+.2f}% — "
            "sector moving without it"
        )
    else:
        # ticker > SPY, ticker <= sector, sector > SPY
        result["label"] = "FOLLOWER"
        result["score_adjustment"] = 0
        result["reason"] = (
            f"{ticker_pct:+.2f}% participating in {sector} ({sector_pct:+.2f}%) "
            f"but not leading it"
        )

    return result


def set_rotation_snapshot(spy_change: float, sector_changes: dict[str, float]) -> None:
    """
    Used by the backtest engine to inject historical sector change data
    so classify_leadership() can be called in replay mode.
    """
    _last_rotation_snapshot["ts"] = datetime.now()
    _last_rotation_snapshot["spy_change"] = spy_change
    _last_rotation_snapshot["sector_changes"] = dict(sector_changes)


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
