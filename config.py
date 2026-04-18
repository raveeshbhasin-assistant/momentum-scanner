"""
Configuration for Momentum Scanner
"""
import os
from zoneinfo import ZoneInfo

# ── Timezone ─────────────────────────────────────────────────
ET = ZoneInfo("America/New_York")

# ── Data Sources ──────────────────────────────────────────────
# FMP: Starter plan — real-time quotes + 5-min intraday candles
FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FMP_BASE_URL = "https://financialmodelingprep.com/stable"  # Post Aug-2025 stable API

# Finnhub: Free tier (60 calls/min) for news + quotes
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "d7dv411r01qkuebibongd7dv411r01qkuebiboo0")

# ── Scanner Settings ──────────────────────────────────────────
SCAN_INTERVAL_MINUTES = 30          # How often to scan during market hours
CANDLE_INTERVAL = "5m"              # 5-minute candles for intraday
CANDLE_LOOKBACK_DAYS = 5            # yfinance free limit for intraday
RVOL_LOOKBACK_DAYS = 20             # Days for avg volume baseline

# ── Signal Thresholds ────────────────────────────────────────
MIN_RVOL = 1.33                     # Minimum relative volume (30%+ above avg)
RSI_MOMENTUM_LOW = 55               # RSI lower bound for bullish momentum
RSI_MOMENTUM_HIGH = 75              # RSI upper bound (avoid overbought)
MIN_COMPOSITE_SCORE = 60            # Minimum score (0-100) to generate signal
MAX_SIGNALS_PER_SCAN = 10           # Top N signals to display

# ── Risk Parameters (Aggressive Profile) ─────────────────────
ATR_STOP_MULTIPLIER = 2.0           # Stop-loss = Entry - (ATR × this)
RISK_REWARD_RATIO = 2.5             # Target = Entry + (stop_distance × this)
RISK_PER_TRADE_PCT = 1.0            # Risk 1% of account per trade
DEFAULT_ACCOUNT_SIZE = 100_000      # For position sizing display

# ── Technical Indicator Parameters ───────────────────────────
EMA_FAST = 9
EMA_MID = 21
EMA_SLOW = 50
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
BB_STD = 2.0
ATR_PERIOD = 14

# ── News Sentiment ───────────────────────────────────────────
SENTIMENT_WEIGHT = 0.25             # 25% of composite score
TECHNICAL_WEIGHT = 0.65             # 65% of composite score
VOLUME_WEIGHT = 0.10                # 10% of composite score
NEWS_RECENCY_HOURS = 4              # Only consider news from last N hours
BULLISH_SENTIMENT_THRESHOLD = 0.15  # Min sentiment to count as bullish

# ── Market Hours (ET) ────────────────────────────────────────
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MINUTE = 0
HARD_CLOSE_HOUR = 15                # Recommend closing by 3:55 PM
HARD_CLOSE_MINUTE = 55

# ── Stock Universe ───────────────────────────────────────────
# Core: Top ~100 liquid large-caps (original S&P 500 subset)
SP500_LIQUID = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK-B",
    "UNH", "XOM", "JNJ", "JPM", "V", "PG", "MA", "HD", "CVX", "MRK",
    "ABBV", "LLY", "PEP", "KO", "AVGO", "COST", "WMT", "MCD", "CSCO",
    "TMO", "ABT", "CRM", "ACN", "DHR", "NKE", "ADBE", "TXN", "NEE",
    "PM", "UNP", "RTX", "HON", "LOW", "AMGN", "IBM", "QCOM", "BA",
    "CAT", "GE", "SBUX", "AMD", "INTC", "INTU", "ISRG", "AMAT", "GS",
    "BLK", "GILD", "MDT", "ADP", "SYK", "BKNG", "ADI", "DE", "MMC",
    "VRTX", "REGN", "LRCX", "SCHW", "CB", "MDLZ", "CME", "PLD", "CI",
    "ZTS", "TMUS", "SO", "MO", "DUK", "CL", "TGT", "BDX", "EQIX",
    "USB", "PNC", "APD", "SHW", "ICE", "MMM", "EMR", "NOC", "FDX",
    "WM", "GM", "F", "RIVN", "SOFI", "PLTR", "COIN", "SQ", "SNAP",
    "UBER", "ABNB", "DKNG", "HOOD", "MARA", "ARM", "SMCI", "CRWD",
    "NET", "PANW", "SNOW", "MDB",
]

# Extended: High-beta mid/small caps from thematic clusters
# These are the stocks that move 3-10%+ intraday on sector rotation
HIGH_BETA_EXTENDED = [
    # Fiber optics / photonics (group chat's best cluster)
    "AAOI", "LITE", "COHR", "SNDK", "CRDO", "GLW",
    # AI infrastructure / data center
    "ORCL", "DELL", "ANET", "MU", "MRVL",
    # Crypto / fintech
    "MSTR", "IREN", "APLD", "CRML",
    # High-momentum mid-caps
    "APP", "HIMS", "PATH", "FSLY", "MOD", "NBIS",
    "DDOG", "NOW", "OKLO", "AEVA", "EOSE",
]

# Full scan universe = core + extended (deduped at runtime)
def get_full_universe() -> list[str]:
    """Return deduplicated full scan universe."""
    seen = set()
    result = []
    for t in SP500_LIQUID + HIGH_BETA_EXTENDED:
        if t not in seen:
            result.append(t)
            seen.add(t)
    return result

# ── Sector Rotation Settings ────────────────────────────────
SECTOR_TOP_N = 3                     # How many top sectors to prioritize
SECTOR_BOOST_POINTS = 8              # Score boost for tickers in hot sectors

# ── Sector Leadership (v3.3) ────────────────────────────────
# Classify ticker vs its sector vs SPY:
#   LEADER:     ticker% > sector% > SPY%   → +10
#   SOLO_MOVER: ticker% > SPY% but sector < SPY (counter-trend) → +3
#   FOLLOWER:   ticker% > SPY% but below sector → 0
#   LAGGARD:    ticker% < sector% → -10
# Score adjustment, NOT a hard filter.
SECTOR_LEADER_BOOST = 10
SECTOR_SOLO_BOOST = 3
SECTOR_LAGGARD_PENALTY = -10

# ── Market Regime / VIX (v3.3) ──────────────────────────────
# When VIX is elevated, raise the min composite score floor and cut
# suggested position size. See market_regime.py for the band table.
MARKET_REGIME_ENABLED = True

# ── Earnings Calendar (v3.3) ────────────────────────────────
EARNINGS_TOMORROW_PENALTY = -5       # Points off for earnings tomorrow BMO
EARNINGS_HARD_FILTER_HOUR = 14       # After 2 PM ET, block entries when earnings are AMC today

# ── Pre-Market Settings ─────────────────────────────────────
PREMARKET_BOOST_CAP = 15             # Max score boost from pre-market flags
PREMARKET_VOL_THRESHOLD = 2.0        # Min volume ratio to flag pre-market

# ── Confirmed Filters (from 3-day analysis) ──────────────────
# Lunch dead zone: 0 wins in 21 decided trades (p < 0.001)
DEAD_ZONE_BATCHES = {"11:31", "12:01", "12:02"}
# Re-entries: 1 win in 32 decided trades (3.1% WR)
SUPPRESS_REENTRIES = True

# ── Server ────────────────────────────────────────────────────
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", 8000))
