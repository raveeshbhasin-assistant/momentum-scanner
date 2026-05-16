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

def fetch_13f_holders(ticker: str, client: httpx.Client = None, top_n: int = 5) -> list[dict]:
    """
    Pull top institutional holders. Source: yfinance.Ticker.institutional_holders.

    yfinance returns a DataFrame with columns:
      Date Reported · Holder · pctHeld (fraction) · Shares · Value · pctChange (fraction)

    Where pctChange is the change in SHARES held by that fund vs prior 13F filing.
    1.0000 means "new position" (no prior holding).

    We convert to the schema the tracker page expects:
      {fund, now_pct, prev_pct, delta_pp, shares, value, date_reported, status}

    Sources beyond yfinance considered and rejected:
    - FMP institutional-ownership endpoints: require higher plan tier (returned empty)
    - Finnhub /stock/ownership: 403 Forbidden on standard plan
    - SEC EDGAR direct XML parsing: works but heavyweight; revisit if yfinance breaks

    The `client` arg is unused (kept for signature compatibility with the call site).
    """
    import yfinance as yf
    try:
        t = yf.Ticker(ticker)
        df = t.institutional_holders
    except Exception as e:
        logger.debug(f"yfinance institutional_holders failed for {ticker}: {e}")
        return []
    if df is None or df.empty:
        return []

    holders: list[dict] = []
    for _, row in df.iterrows():
        fund = str(row.get("Holder") or "").strip()
        if not fund:
            continue
        # pctHeld is a fraction (0.0787 = 7.87%); convert to percentage
        pct_held = row.get("pctHeld")
        now_pct = round(float(pct_held) * 100, 2) if pct_held is not None and not _is_nan(pct_held) else None
        # pctChange is the change in SHARES held — convert to delta percentage points
        pct_change = row.get("pctChange")
        prev_pct = None
        delta_pp = None
        status = None
        if now_pct is not None and pct_change is not None and not _is_nan(pct_change):
            pc = float(pct_change)
            if pc >= 0.9999:
                # yfinance marks brand-new positions as pctChange = 1.0
                status = "new"
                prev_pct = 0.0
                delta_pp = round(now_pct, 2)
            elif pc <= -0.9999:
                status = "closed"
                prev_pct = round(now_pct / 0.0001, 2) if pc != -1 else None
                delta_pp = round(-(prev_pct or 0), 2)
            else:
                # prev shares = current / (1 + pctChange); assume float ≈ constant
                prev_pct = round(now_pct / (1 + pc), 2)
                delta_pp = round(now_pct - prev_pct, 2)

        shares = row.get("Shares")
        value = row.get("Value")
        date_rep = row.get("Date Reported")
        holders.append({
            "fund": fund[:60],
            "now_pct": now_pct,
            "prev_pct": prev_pct,
            "delta_pp": delta_pp,
            "shares": int(shares) if shares is not None and not _is_nan(shares) else None,
            "value": int(value) if value is not None and not _is_nan(value) else None,
            "date_reported": str(date_rep) if date_rep is not None else None,
            "status": status,
        })

    holders.sort(key=lambda h: -(h.get("now_pct") or 0))
    return holders[:top_n]


def _is_nan(v) -> bool:
    """True for NaN floats; safe for ints and strings."""
    try:
        import math
        return isinstance(v, float) and math.isnan(v)
    except Exception:
        return False


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
            "13f": "yfinance",  # institutional_holders endpoint — no plan limits, quarterly cadence
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
