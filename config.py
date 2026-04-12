"""
Configuration for Momentum Scanner
"""
import os

# ── Data Sources ──────────────────────────────────────────────
# Finnhub: Free tier (60 calls/min) for news + quotes
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "d7dv411r01qkuebibongd7dv411r01qkuebiboo0")

# ── Scanner Settings ──────────────────────────────────────────
SCAN_INTERVAL_MINUTES = 30          # How often to scan during market hours
CANDLE_INTERVAL = "5m"              # 5-minute candles for intraday
CANDLE_LOOKBACK_DAYS = 5            # yfinance free limit for intraday
RVOL_LOOKBACK_DAYS = 20             # Days for avg volume baseline

# ── Signal Thresholds ────────────────────────────────────────
MIN_RVOL = 1.5                      # Minimum relative volume to consider
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

# ── S&P 500 Universe ─────────────────────────────────────────
# Top 100 most liquid S&P 500 stocks (full 500 is too slow on free tier)
# These are the highest average daily volume stocks
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
    "NET", "PANW", "SNOW", "MDB"
]

# ── Server ────────────────────────────────────────────────────
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", 8000))
