"""
Demo Data Generator
───────────────────
Generates realistic-looking stock data for dashboard preview
when live data sources are unavailable (sandbox, testing, demo).
Automatically disabled when real data is accessible.
"""

import random
import time
from datetime import datetime, timedelta

import numpy as np

from config import ET


def generate_demo_signals(count: int = 6) -> list[dict]:
    """Generate realistic demo signals for dashboard preview."""

    demo_stocks = [
        {"ticker": "NVDA", "base_price": 142.50, "sector": "Semiconductors", "trend": "strong_bull"},
        {"ticker": "META", "base_price": 612.30, "sector": "Tech/Social", "trend": "bull"},
        {"ticker": "AMZN", "base_price": 198.75, "sector": "E-Commerce/Cloud", "trend": "bull"},
        {"ticker": "AAPL", "base_price": 228.40, "sector": "Tech/Consumer", "trend": "mild_bull"},
        {"ticker": "CRWD", "base_price": 387.20, "sector": "Cybersecurity", "trend": "strong_bull"},
        {"ticker": "PLTR", "base_price": 112.85, "sector": "AI/Defense", "trend": "bull"},
        {"ticker": "AMD", "base_price": 118.60, "sector": "Semiconductors", "trend": "bull"},
        {"ticker": "TSLA", "base_price": 268.90, "sector": "EV/Energy", "trend": "volatile_bull"},
        {"ticker": "UBER", "base_price": 82.15, "sector": "Ride-sharing", "trend": "mild_bull"},
        {"ticker": "COIN", "base_price": 235.70, "sector": "Crypto/Fintech", "trend": "volatile_bull"},
    ]

    selected = random.sample(demo_stocks, min(count, len(demo_stocks)))
    signals = []

    for i, stock in enumerate(selected):
        signal = _generate_single_signal(stock, rank=i)
        signals.append(signal)

    signals.sort(key=lambda x: x["composite_score"], reverse=True)
    return signals


def _generate_single_signal(stock: dict, rank: int) -> dict:
    """Generate a single realistic signal."""
    ticker = stock["ticker"]
    base = stock["base_price"]
    trend = stock["trend"]

    # Price variation
    price = round(base * (1 + random.uniform(-0.02, 0.03)), 2)

    # Technical scores based on trend
    if trend == "strong_bull":
        tech_score = random.uniform(78, 95)
        rvol = round(random.uniform(2.0, 3.5), 2)
    elif trend == "bull":
        tech_score = random.uniform(65, 85)
        rvol = round(random.uniform(1.6, 2.8), 2)
    elif trend == "mild_bull":
        tech_score = random.uniform(60, 75)
        rvol = round(random.uniform(1.5, 2.2), 2)
    else:  # volatile_bull
        tech_score = random.uniform(62, 88)
        rvol = round(random.uniform(1.8, 3.2), 2)

    sentiment = round(random.uniform(-0.1, 0.6), 3)
    vol_score = min(rvol / 3.0 * 100, 100)

    composite = round(
        tech_score * 0.65 +
        max(sentiment * 100, 0) * 0.25 +
        vol_score * 0.10,
        1
    )

    # ATR and trade levels
    atr = round(price * random.uniform(0.012, 0.025), 2)
    stop_distance = round(atr * 2.0, 2)
    stop_loss = round(price - stop_distance, 2)
    target = round(price + stop_distance * 2.5, 2)
    risk_pct = round(stop_distance / price * 100, 2)
    reward_pct = round((target - price) / price * 100, 2)
    shares = int(1000 / stop_distance) if stop_distance > 0 else 0

    # RSI
    rsi = round(random.uniform(56, 72), 1) if tech_score > 70 else round(random.uniform(50, 65), 1)

    # EMA details
    ema_9 = round(price * (1 - random.uniform(0.001, 0.005)), 2)
    ema_21 = round(ema_9 * (1 - random.uniform(0.002, 0.008)), 2)
    ema_50 = round(ema_21 * (1 - random.uniform(0.003, 0.012)), 2)

    # Tech details
    tech_details = {}
    if tech_score > 75:
        tech_details["ema"] = {"score": 25, "max": 25, "status": "Bullish alignment (9 > 21 > 50)", "bullish": True}
    elif tech_score > 65:
        tech_details["ema"] = {"score": 15, "max": 25, "status": "Partial alignment (9 > 21)", "bullish": True}
    else:
        tech_details["ema"] = {"score": 8, "max": 25, "status": "Price above fast EMA", "bullish": True}

    if rsi >= 55 and rsi <= 75:
        tech_details["rsi"] = {"score": 20, "max": 20, "value": rsi, "status": f"In momentum zone ({rsi})", "bullish": True}
    else:
        tech_details["rsi"] = {"score": 12, "max": 20, "value": rsi, "status": f"Building momentum ({rsi})", "bullish": True}

    if tech_score > 80:
        tech_details["macd"] = {"score": 20, "max": 20, "status": "Bullish & accelerating", "bullish": True}
    elif tech_score > 65:
        tech_details["macd"] = {"score": 14, "max": 20, "status": "Bullish histogram", "bullish": True}
    else:
        tech_details["macd"] = {"score": 8, "max": 20, "status": "Bearish but improving", "bullish": True}

    if price > ema_9:
        tech_details["vwap"] = {"score": 15, "max": 15, "status": f"Price {round((price - ema_9)/ema_9*100, 1)}% above VWAP — strong", "bullish": True}
    else:
        tech_details["vwap"] = {"score": 10, "max": 15, "status": "Slightly above VWAP", "bullish": True}

    tech_details["bb"] = {"score": random.choice([15, 20]), "max": 20, "status": "Upper half of bands, room to run", "bullish": True}

    # Chart data
    chart_data = _generate_chart_data(price, atr, trend)

    # News
    news = _generate_demo_news(ticker, stock["sector"], sentiment)

    return {
        "ticker": ticker,
        "timestamp": datetime.now().isoformat(),
        "price": price,
        "composite_score": composite,
        "technical_score": round(tech_score, 1),
        "sentiment_score": sentiment,
        "rvol": rvol,
        "volume_score": round(vol_score, 1),
        "tech_details": tech_details,
        "trade": {
            "entry": price,
            "stop_loss": stop_loss,
            "target": target,
            "stop_distance": stop_distance,
            "atr": atr,
            "risk_pct": risk_pct,
            "reward_pct": reward_pct,
            "risk_reward_ratio": 2.5,
            "suggested_shares": shares,
            "position_value": round(shares * price, 2),
            "risk_dollars": 1000.0,
        },
        "news": news,
        "indicators": {
            "rsi": rsi,
            "ema_9": ema_9,
            "ema_21": ema_21,
            "ema_50": ema_50,
            "vwap": round(price * 0.998, 2),
            "atr": atr,
        },
        "chart_data": chart_data,
    }


def _generate_chart_data(current_price: float, atr: float, trend: str) -> list[dict]:
    """Generate realistic OHLCV candlestick data."""
    bars = 78  # ~6.5 hours of 5-min candles
    data = []

    # Start time: today 9:30 AM
    now = datetime.now(ET)
    start = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now.hour < 9:
        start -= timedelta(days=1)

    # Generate price series working backward from current price
    price = current_price * (1 - random.uniform(0.01, 0.025))  # Start lower for uptrend
    step = atr / 5  # Average bar movement

    for i in range(bars):
        ts = int((start + timedelta(minutes=i * 5)).timestamp())

        # Trend bias
        if trend == "strong_bull":
            drift = step * 0.15
        elif trend == "bull":
            drift = step * 0.08
        elif trend == "mild_bull":
            drift = step * 0.04
        else:
            drift = step * random.uniform(-0.05, 0.15)

        # Random movement
        change = drift + random.gauss(0, step * 0.7)
        open_p = round(price, 2)
        close_p = round(price + change, 2)
        high_p = round(max(open_p, close_p) + abs(random.gauss(0, step * 0.4)), 2)
        low_p = round(min(open_p, close_p) - abs(random.gauss(0, step * 0.4)), 2)

        # Volume (higher at open/close, lower midday)
        hour_of_day = 9.5 + (i * 5 / 60)
        if hour_of_day < 10.5 or hour_of_day > 15:
            vol_mult = random.uniform(1.5, 3.0)
        elif hour_of_day > 12 and hour_of_day < 14:
            vol_mult = random.uniform(0.4, 0.8)
        else:
            vol_mult = random.uniform(0.7, 1.3)
        volume = int(random.uniform(500000, 2000000) * vol_mult)

        data.append({
            "time": ts,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": volume,
        })

        price = close_p

    return data


def _generate_demo_news(ticker: str, sector: str, sentiment: float) -> list[dict]:
    """Generate plausible demo news headlines."""
    now = datetime.now(ET)

    bullish_headlines = [
        f"{ticker} shares surge on strong quarterly earnings beat",
        f"Analysts upgrade {ticker} citing accelerating revenue growth",
        f"{ticker} announces expanded AI partnership, stock climbs",
        f"Morgan Stanley raises {ticker} price target to new high",
        f"{sector} sector sees renewed institutional buying interest",
        f"{ticker} reports record customer growth in latest quarter",
        f"Options flow shows heavy call buying in {ticker}",
        f"{ticker} breaks above key resistance on strong volume",
    ]

    neutral_headlines = [
        f"{ticker} trades sideways ahead of upcoming earnings report",
        f"{sector} sector mixed as investors await Fed minutes",
        f"{ticker} holds steady despite broader market pullback",
        f"Volume picks up in {ticker} ahead of product announcement",
    ]

    bearish_headlines = [
        f"Concerns emerge over {ticker} valuation after recent run-up",
        f"{sector} faces headwinds from regulatory uncertainty",
    ]

    news = []
    for i in range(3):
        hours_ago = round(random.uniform(0.1, 3.0), 1)
        if sentiment > 0.2:
            headline = random.choice(bullish_headlines)
            sent = round(random.uniform(0.3, 0.8), 3)
        elif sentiment > 0:
            headline = random.choice(neutral_headlines if i > 0 else bullish_headlines)
            sent = round(random.uniform(-0.1, 0.5), 3)
        else:
            headline = random.choice(bearish_headlines if i == 0 else neutral_headlines)
            sent = round(random.uniform(-0.5, 0.1), 3)

        recency_weight = max(0.3, 1.0 - hours_ago * 0.25)

        news.append({
            "headline": headline,
            "source": random.choice(["Reuters", "Bloomberg", "CNBC", "MarketWatch", "Barron's", "WSJ"]),
            "datetime": (now - timedelta(hours=hours_ago)).isoformat(),
            "hours_ago": hours_ago,
            "url": "#",
            "sentiment": sent,
            "recency_weight": round(recency_weight, 2),
            "weighted_sentiment": round(sent * recency_weight, 3),
        })

    return news
