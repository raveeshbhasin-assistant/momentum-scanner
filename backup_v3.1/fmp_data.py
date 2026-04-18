"""
FMP (Financial Modeling Prep) Data Module
─────────────────────────────────────────
Replaces yfinance with FMP's real-time API for intraday data.
Provides the same DataFrame interface that scanner.py expects.

Uses FMP's "stable" API endpoints (post Aug 2025 migration):
  - /stable/quote?symbol=X            → real-time single quote
  - /stable/historical-chart/5min?symbol=X  → intraday 5-min candles
"""

import logging
import time
from datetime import datetime, timedelta

import httpx
import pandas as pd
import numpy as np

import config

logger = logging.getLogger(__name__)

FMP_STABLE_URL = "https://financialmodelingprep.com/stable"


# ═══════════════════════════════════════════════════════════════
#  REAL-TIME QUOTES
# ═══════════════════════════════════════════════════════════════

def fetch_single_quote(ticker: str, client: httpx.Client) -> dict | None:
    """Fetch a single real-time quote from FMP stable API."""
    try:
        resp = client.get(
            f"{FMP_STABLE_URL}/quote",
            params={"symbol": ticker, "apikey": config.FMP_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        logger.debug(f"FMP quote failed for {ticker}: {e}")
        return None


def fetch_batch_quotes(tickers: list[str]) -> dict[str, dict]:
    """
    Fetch real-time quotes for multiple tickers.
    FMP Starter plan doesn't support batch-quote, so we loop single calls.
    At 300 calls/min limit, 100 tickers takes ~35 seconds with pacing.
    Returns dict of {ticker: quote_dict}.
    """
    if not config.FMP_API_KEY:
        logger.warning("FMP_API_KEY not set — cannot fetch quotes")
        return {}

    quotes = {}
    with httpx.Client(timeout=10) as client:
        for i, ticker in enumerate(tickers):
            quote = fetch_single_quote(ticker, client)
            if quote:
                quotes[quote.get("symbol", ticker)] = quote

            # Rate limiting: 300/min = 5/sec → fetch 4/sec to be safe
            if (i + 1) % 4 == 0:
                time.sleep(1.0)

    logger.info(f"FMP: Fetched real-time quotes for {len(quotes)}/{len(tickers)} tickers")
    return quotes


# ═══════════════════════════════════════════════════════════════
#  INTRADAY CANDLE DATA
# ═══════════════════════════════════════════════════════════════

def fetch_intraday_candles(ticker: str, interval: str = "5min", days: int = 5,
                           client: httpx.Client = None) -> pd.DataFrame:
    """
    Fetch intraday candle data for a single ticker from FMP stable API.
    Returns DataFrame with OHLCV columns and DateTimeIndex (ET timezone).

    FMP stable endpoint: /stable/historical-chart/{interval}?symbol={symbol}
    Response: [{date, open, high, low, close, volume}, ...] (newest first)
    """
    if not config.FMP_API_KEY:
        return pd.DataFrame()

    now = datetime.now(config.ET)
    from_date = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")

    url = f"{FMP_STABLE_URL}/historical-chart/{interval}"
    params = {
        "symbol": ticker,
        "from": from_date,
        "to": to_date,
        "apikey": config.FMP_API_KEY,
    }

    try:
        _client = client or httpx.Client(timeout=15)
        should_close = client is None

        try:
            resp = _client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
        finally:
            if should_close:
                _client.close()

        if not data or not isinstance(data, list):
            return pd.DataFrame()

        # FMP returns newest first — reverse to chronological order
        data.reverse()

        df = pd.DataFrame(data)

        # Rename columns to match what scanner.py expects (capitalized OHLCV)
        df = df.rename(columns={
            "date": "Date",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        })

        # Parse dates and set as index
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date")

        # Localize to ET if not already timezone-aware
        if df.index.tz is None:
            df.index = df.index.tz_localize("America/New_York")

        # Ensure numeric types
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Drop any rows with missing critical data
        df = df.dropna(subset=["Close"])

        return df

    except Exception as e:
        logger.warning(f"FMP candle fetch failed for {ticker}: {e}")
        return pd.DataFrame()


def fetch_intraday_data(tickers: list[str], interval: str = "5min", days: int = 5) -> dict[str, pd.DataFrame]:
    """
    Fetch intraday candle data for multiple tickers from FMP.
    Drop-in replacement for the yfinance version — same return type.

    Falls back to yfinance if FMP key is missing or connectivity fails.
    """
    if not config.FMP_API_KEY:
        logger.warning("FMP_API_KEY not set — falling back to yfinance")
        from scanner import _fetch_intraday_yfinance
        return _fetch_intraday_yfinance(tickers, interval, days)

    data = {}

    with httpx.Client(timeout=15) as client:
        # Quick connectivity test with first ticker
        test_df = fetch_intraday_candles(tickers[0], interval, days, client=client)
        if test_df.empty:
            logger.warning("FMP connectivity test failed — falling back to yfinance")
            from scanner import _fetch_intraday_yfinance
            return _fetch_intraday_yfinance(tickers, interval, days)

        data[tickers[0]] = test_df
        logger.info(f"FMP connected — fetching {len(tickers)} tickers ({interval} candles)...")

        # Fetch remaining tickers
        for i, ticker in enumerate(tickers[1:], 1):
            try:
                df = fetch_intraday_candles(ticker, interval, days, client=client)
                if len(df) >= 50:  # Need enough bars for indicators
                    data[ticker] = df
            except Exception as e:
                logger.warning(f"FMP fetch failed for {ticker}: {e}")
                continue

            # Rate limiting: 300 calls/min = 5/sec → 4/sec with headroom
            if i % 4 == 0:
                time.sleep(1.0)

            # Progress log every 25 tickers
            if i % 25 == 0:
                logger.info(f"FMP progress: {i}/{len(tickers)} tickers fetched...")

    logger.info(f"FMP: Fetched intraday data for {len(data)}/{len(tickers)} tickers")
    return data
