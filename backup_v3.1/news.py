"""
News Sentiment Engine
─────────────────────
Fetches recent news for a ticker and scores sentiment using VADER.
Applies recency weighting and detects first/second-order impacts.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import config

logger = logging.getLogger(__name__)
analyzer = SentimentIntensityAnalyzer()

# Add finance-specific terms to VADER's lexicon
FINANCE_LEXICON_UPDATES = {
    "bullish": 2.5, "bearish": -2.5,
    "upgrade": 2.0, "downgrade": -2.0,
    "beat": 1.8, "miss": -1.8, "missed": -1.8,
    "outperform": 2.0, "underperform": -2.0,
    "breakout": 1.5, "breakdown": -1.5,
    "rally": 2.0, "crash": -3.0, "plunge": -2.5, "surge": 2.5,
    "buy": 1.5, "sell": -1.0, "short": -1.5,
    "dividend": 1.0, "buyback": 1.5, "layoff": -1.5, "layoffs": -1.5,
    "acquisition": 1.0, "merger": 0.8,
    "FDA approval": 3.0, "approved": 1.5, "rejected": -2.5,
    "guidance raised": 2.5, "guidance lowered": -2.5,
    "revenue growth": 2.0, "revenue decline": -2.0,
    "record revenue": 2.5, "record earnings": 2.5,
    "strong demand": 2.0, "weak demand": -2.0,
    "all-time high": 2.0, "52-week low": -1.5,
    "overweight": 1.5, "underweight": -1.5,
    "price target raised": 2.0, "price target lowered": -2.0,
}
analyzer.lexicon.update(FINANCE_LEXICON_UPDATES)

# Sector mapping for second-order effects
SECTOR_PEERS = {
    "AAPL": ["MSFT", "GOOGL", "META", "AMZN"],
    "NVDA": ["AMD", "INTC", "AVGO", "QCOM", "ARM", "SMCI", "AMAT", "LRCX"],
    "TSLA": ["RIVN", "GM", "F"],
    "AMZN": ["WMT", "COST", "TGT", "BKNG"],
    "META": ["GOOGL", "SNAP", "PINS"],
    "GOOGL": ["META", "MSFT", "AMZN"],
    "JPM": ["GS", "MS", "BLK", "SCHW"],
    "XOM": ["CVX", "COP", "SLB"],
    "UNH": ["CI", "HUM", "ELV"],
    "LLY": ["MRK", "ABBV", "PFE", "REGN", "VRTX", "GILD"],
}

# ── Cache to avoid hammering Finnhub ─────────────────────────
_news_cache: dict[str, tuple[float, list]] = {}
CACHE_TTL = 300  # 5 minutes


def get_news_headlines(ticker: str, max_items: int = 10) -> list[dict]:
    """
    Fetch recent news headlines for a ticker from Finnhub (free tier).
    Returns list of {headline, source, datetime, url, sentiment}.
    Falls back gracefully if no API key is set.
    """
    # Check cache
    cache_key = ticker
    if cache_key in _news_cache:
        cached_time, cached_data = _news_cache[cache_key]
        if time.time() - cached_time < CACHE_TTL:
            return cached_data

    if not config.FINNHUB_API_KEY:
        return _generate_placeholder_news(ticker)

    try:
        now = datetime.now(timezone.utc)
        from_date = (now - timedelta(hours=config.NEWS_RECENCY_HOURS)).strftime("%Y-%m-%d")
        to_date = now.strftime("%Y-%m-%d")

        url = "https://finnhub.io/api/v1/company-news"
        params = {
            "symbol": ticker.replace("-", "."),  # BRK-B → BRK.B for Finnhub
            "from": from_date,
            "to": to_date,
            "token": config.FINNHUB_API_KEY,
        }

        with httpx.Client(timeout=10) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            articles = resp.json()

        if not articles:
            result = []
        else:
            result = []
            for article in articles[:max_items]:
                headline = article.get("headline", "")
                sentiment = analyzer.polarity_scores(headline)
                published = datetime.fromtimestamp(article.get("datetime", 0), tz=timezone.utc)
                hours_ago = (now - published).total_seconds() / 3600

                # Recency decay
                if hours_ago <= 0.25:
                    recency_weight = 1.0
                elif hours_ago <= 0.5:
                    recency_weight = 0.9
                elif hours_ago <= 1:
                    recency_weight = 0.7
                elif hours_ago <= 2:
                    recency_weight = 0.5
                else:
                    recency_weight = 0.3

                result.append({
                    "headline": headline,
                    "source": article.get("source", "Unknown"),
                    "datetime": published.isoformat(),
                    "hours_ago": round(hours_ago, 1),
                    "url": article.get("url", ""),
                    "sentiment": round(sentiment["compound"], 3),
                    "recency_weight": recency_weight,
                    "weighted_sentiment": round(sentiment["compound"] * recency_weight, 3),
                })

        _news_cache[cache_key] = (time.time(), result)
        return result

    except Exception as e:
        logger.warning(f"News fetch failed for {ticker}: {e}")
        return []


def get_sentiment_score(ticker: str) -> tuple[float, list[dict]]:
    """
    Get aggregate sentiment score for a ticker.
    Returns (score from -1 to +1, list of news items).
    """
    news = get_news_headlines(ticker)

    if not news:
        return 0.0, []

    # Weighted average of sentiment scores
    total_weight = sum(n["recency_weight"] for n in news)
    if total_weight == 0:
        return 0.0, news

    weighted_sum = sum(n["weighted_sentiment"] for n in news)
    score = weighted_sum / total_weight

    return round(score, 3), news


def get_second_order_sentiment(ticker: str) -> Optional[dict]:
    """
    Check if sector peers have strong recent sentiment that could spill over.
    Returns dict with peer info if significant second-order signal found.
    """
    peers = SECTOR_PEERS.get(ticker, [])
    if not peers:
        return None

    peer_sentiments = []
    for peer in peers[:3]:  # Check top 3 peers to save API calls
        score, news = get_sentiment_score(peer)
        if abs(score) > 0.3:  # Only significant sentiment
            peer_sentiments.append({
                "peer": peer,
                "sentiment": score,
                "top_headline": news[0]["headline"] if news else "",
            })

    if not peer_sentiments:
        return None

    avg_peer = sum(p["sentiment"] for p in peer_sentiments) / len(peer_sentiments)
    return {
        "avg_peer_sentiment": round(avg_peer, 3),
        "peers": peer_sentiments,
        "impact": "bullish" if avg_peer > 0 else "bearish",
    }


def _generate_placeholder_news(ticker: str) -> list[dict]:
    """
    When no API key is available, return empty list.
    The dashboard will show 'No Finnhub API key configured' message.
    """
    return []
