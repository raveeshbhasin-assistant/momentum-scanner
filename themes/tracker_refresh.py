"""
themes/tracker_refresh.py — Tracker-specific live data refresh.

Different cadence from candidates.json:
  - candidates.json refreshes price + fundamentals for the FULL universe (~25 names)
  - tracker_live.json refreshes 13F + earnings + news for the PROMOTED 7

Output: themes/<theme>/tracker_live.json — keyed by ticker, contains:
  {
    "13f_holders": [{fund, prev_pct, now_pct, delta_pp}, ...],
    "upcoming_earnings": [{date, type, eps_estimate, ...}, ...],
    "recent_news": [{date, headline, source, url, sentiment}, ...]
  }

Idempotent. Run via:
  python themes/tracker_refresh.py [theme1 theme2 ...]
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import httpx

_HERE = Path(__file__).parent
_PARENT = _HERE.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

import config  # noqa: E402

logger = logging.getLogger(__name__)

DATA_HISTORY_DIR_NAME = "history"
ACTIVE_THEMES = ["ai_data_center"]


# ═══════════════════════════════════════════════════════════════
#  13F — Institutional holders
# ═══════════════════════════════════════════════════════════════

def fetch_13f_holders(ticker: str, client: httpx.Client, top_n: int = 5) -> list[dict]:
    """
    Pull top institutional holders from FMP. The /stable endpoint returns
    a list of fund positions; we sort by % held and take the top N.

    Returns [{fund, now_pct, prev_pct, delta_pp, shares}, ...]
    """
    if not config.FMP_API_KEY:
        return []

    try:
        # Current institutional holders
        resp = client.get(
            "https://financialmodelingprep.com/stable/institutional-ownership/symbol-positions-summary",
            params={"symbol": ticker, "apikey": config.FMP_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.debug(f"FMP 13F summary failed for {ticker}: {e}")
        # Fall back to alternate endpoint format
        try:
            resp = client.get(
                "https://financialmodelingprep.com/stable/institutional-ownership/holder/list",
                params={"symbol": ticker, "apikey": config.FMP_API_KEY},
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e2:
            logger.debug(f"FMP 13F list fallback failed for {ticker}: {e2}")
            return []

    if not isinstance(data, list) or not data:
        return []

    # Each entry has different shapes depending on endpoint. Normalize.
    holders: list[dict] = []
    for entry in data:
        # Try several known FMP field names
        fund_name = (entry.get("investorName") or entry.get("holder")
                      or entry.get("ownerName") or entry.get("filerName"))
        if not fund_name:
            continue
        now_pct = (entry.get("ownership") or entry.get("ownershipPercent")
                    or entry.get("percentOwned") or entry.get("pctHeld"))
        change_pp = (entry.get("change") or entry.get("changeInOwnership")
                      or entry.get("changeOfShareValue"))
        shares = entry.get("shares") or entry.get("sharesNumber") or entry.get("currentShares")

        if now_pct is None and change_pp is None:
            continue
        prev_pct = (now_pct - change_pp) if (now_pct is not None and change_pp is not None) else None
        holders.append({
            "fund": str(fund_name)[:60],
            "now_pct": round(float(now_pct), 2) if now_pct is not None else None,
            "prev_pct": round(float(prev_pct), 2) if prev_pct is not None else None,
            "delta_pp": round(float(change_pp), 2) if change_pp is not None else None,
            "shares": int(shares) if shares is not None else None,
        })

    # Sort by current % held desc, take top N
    holders.sort(key=lambda h: -(h["now_pct"] or 0))
    return holders[:top_n]


# ═══════════════════════════════════════════════════════════════
#  Earnings calendar
# ═══════════════════════════════════════════════════════════════

def fetch_upcoming_earnings(ticker: str, client: httpx.Client,
                              lookback_days: int = 7, lookahead_days: int = 120) -> list[dict]:
    """
    Pull upcoming earnings dates from FMP. Returns events from `lookback_days`
    ago through `lookahead_days` ahead — historical context plus what's coming.
    """
    if not config.FMP_API_KEY:
        return []

    today = datetime.now().date()
    start = (today - timedelta(days=lookback_days)).isoformat()
    end = (today + timedelta(days=lookahead_days)).isoformat()

    try:
        resp = client.get(
            "https://financialmodelingprep.com/stable/earnings",
            params={"symbol": ticker, "from": start, "to": end, "apikey": config.FMP_API_KEY},
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.debug(f"FMP earnings failed for {ticker}: {e}")
        return []

    if not isinstance(data, list):
        return []

    events = []
    for d in data:
        date_str = d.get("date")
        if not date_str:
            continue
        events.append({
            "date": date_str,
            "eps_estimate": d.get("epsEstimated"),
            "eps_actual": d.get("eps"),
            "revenue_estimate": d.get("revenueEstimated"),
            "revenue_actual": d.get("revenue"),
            "time": d.get("time"),  # bmo | amc
        })
    events.sort(key=lambda e: e["date"])
    return events


# ═══════════════════════════════════════════════════════════════
#  News headlines (Finnhub)
# ═══════════════════════════════════════════════════════════════

def fetch_recent_news(ticker: str, client: httpx.Client = None,
                       lookback_days: int = 30, max_items: int = 5) -> list[dict]:
    """
    Fetch Finnhub company news directly with a 30-day window.
    The existing news.py uses NEWS_RECENCY_HOURS (intraday-focused) which is
    too narrow for a long-term tracker — we want last-month visibility, not
    last-hour.
    """
    if not config.FINNHUB_API_KEY:
        return []
    from_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")
    close_after = client is None
    if client is None:
        client = httpx.Client(timeout=10)
    try:
        resp = client.get(
            "https://finnhub.io/api/v1/company-news",
            params={
                "symbol": ticker.replace("-", "."),
                "from": from_date,
                "to": to_date,
                "token": config.FINNHUB_API_KEY,
            },
        )
        resp.raise_for_status()
        articles = resp.json()
    except Exception as e:
        logger.debug(f"News fetch failed for {ticker}: {e}")
        return []
    finally:
        if close_after:
            client.close()

    if not isinstance(articles, list):
        return []
    # Most recent first
    articles.sort(key=lambda a: -(a.get("datetime") or 0))
    out = []
    for a in articles[:max_items]:
        ts = a.get("datetime") or 0
        date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d") if ts else ""
        out.append({
            "date": date_str,
            "headline": (a.get("headline") or "")[:200],
            "source": (a.get("source") or "")[:40],
            "url": a.get("url") or "",
            "summary": (a.get("summary") or "")[:200] if a.get("summary") else "",
        })
    return out


# ═══════════════════════════════════════════════════════════════
#  Top-level refresh
# ═══════════════════════════════════════════════════════════════

def refresh_tracker_live(theme: str) -> dict:
    """Refresh 13F + earnings + news for the theme's tracker tickers."""
    theme_path = _HERE / theme
    tracker_path = theme_path / "tracker.json"
    if not tracker_path.exists():
        raise FileNotFoundError(f"{tracker_path} missing")

    tracker = json.loads(tracker_path.read_text(encoding="utf-8"))
    tickers = [h["ticker"] for h in (tracker.get("holdings") or [])]

    logger.info(f"[{theme}] Refreshing live tracker data for {len(tickers)} names")

    per_ticker: dict[str, dict] = {}
    with httpx.Client(timeout=15) as client:
        for i, tk in enumerate(tickers):
            row: dict[str, Any] = {"ticker": tk}
            try:
                row["13f_holders"] = fetch_13f_holders(tk, client)
            except Exception as e:
                logger.warning(f"{tk}: 13F failed — {e}")
                row["13f_holders"] = []
            try:
                row["upcoming_earnings"] = fetch_upcoming_earnings(tk, client)
            except Exception as e:
                logger.warning(f"{tk}: earnings failed — {e}")
                row["upcoming_earnings"] = []
            try:
                row["recent_news"] = fetch_recent_news(tk, client=client)
            except Exception as e:
                logger.warning(f"{tk}: news failed — {e}")
                row["recent_news"] = []
            per_ticker[tk] = row

            # Rate-limit pacing — roughly 3 FMP calls per ticker + 1 Finnhub
            if (i + 1) % 3 == 0:
                time.sleep(1.0)

    result = {
        "theme": theme,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "trade_date": datetime.now().strftime("%Y-%m-%d"),
        "ticker_count": len(tickers),
        "data_sources": {
            "13f": "fmp" if config.FMP_API_KEY else "none",
            "earnings": "fmp" if config.FMP_API_KEY else "none",
            "news": "finnhub" if config.FINNHUB_API_KEY else "none",
        },
        "per_ticker": per_ticker,
    }
    return result


def save_tracker_live(theme: str, result: dict):
    theme_path = _HERE / theme
    out_path = theme_path / "tracker_live.json"
    out_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")

    hist_dir = theme_path / DATA_HISTORY_DIR_NAME
    hist_dir.mkdir(exist_ok=True)
    date_str = result["trade_date"]
    (hist_dir / f"tracker_live_{date_str}.json").write_text(
        json.dumps(result, indent=2, default=str), encoding="utf-8")


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    themes = sys.argv[1:] if len(sys.argv) > 1 else ACTIVE_THEMES
    for theme in themes:
        logger.info(f"━━━━━ Refreshing tracker_live: {theme} ━━━━━")
        try:
            result = refresh_tracker_live(theme)
            save_tracker_live(theme, result)
            # Summary
            counts = {k: {
                "13f": len(v.get("13f_holders", [])),
                "earnings": len(v.get("upcoming_earnings", [])),
                "news": len(v.get("recent_news", [])),
            } for k, v in result["per_ticker"].items()}
            logger.info(f"[{theme}] per-ticker counts: {counts}")
        except Exception as e:
            logger.exception(f"Theme {theme} tracker_live refresh failed: {e}")


if __name__ == "__main__":
    main()
