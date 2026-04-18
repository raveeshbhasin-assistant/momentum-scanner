"""
Earnings Calendar Indicator
───────────────────────────
Flags tickers with upcoming earnings so the scanner can:
  1. Display context badges on the UI ("EARN TOMORROW", "EARN THIS WEEK", etc.)
  2. Apply light scoring pressure (-5 pts if earnings are tomorrow BMO)
  3. Hard-filter new entries after 2pm on days when a ticker reports AMC,
     because an intraday momentum trade going into a post-close earnings
     print is a gap-risk coin flip.

Data priority:
  • FMP /stable/earning-calendar (we already pay for Starter)
  • yfinance Ticker.calendar fallback (free, per-ticker, slower)

The full universe is fetched once per day and cached to
data/earnings_cache.json so the main scan only does a dict lookup.

Added in v3.3.
"""

import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import httpx
import yfinance as yf

import config

logger = logging.getLogger(__name__)

# ── Cache ──
BASE_DIR = Path(__file__).parent
CACHE_PATH = BASE_DIR / "data" / "earnings_cache.json"
CACHE_TTL_HOURS = 6    # refresh 4× per trading day is plenty


# ═══════════════════════════════════════════════════════════════
#  FETCHING
# ═══════════════════════════════════════════════════════════════

def _fmp_fetch_range(from_date: date, to_date: date) -> dict[str, dict]:
    """
    Fetch earnings calendar from FMP for a date window. Returns
    {ticker: {date: 'YYYY-MM-DD', time: 'amc'|'bmo'|''}}.
    """
    if not config.FMP_API_KEY:
        return {}

    url = f"{config.FMP_BASE_URL}/earning-calendar"
    params = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "apikey": config.FMP_API_KEY,
    }
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            rows = resp.json()
    except Exception as e:
        logger.warning(f"FMP earnings fetch failed: {e}")
        return {}

    out: dict[str, dict] = {}
    for row in rows or []:
        sym = row.get("symbol")
        d = row.get("date")
        if not sym or not d:
            continue
        # FMP returns "time" as "amc" / "bmo" / "" (during-market)
        t = (row.get("time") or "").strip().lower()
        # Take the SOONEST upcoming earnings for each ticker
        existing = out.get(sym)
        if existing is None or d < existing["date"]:
            out[sym] = {"date": d, "time": t}
    return out


def _yfinance_fetch_ticker(ticker: str) -> Optional[dict]:
    """Fallback: fetch next earnings date for a single ticker via yfinance."""
    try:
        t = yf.Ticker(ticker)
        cal = t.calendar
        if cal is None:
            return None
        # yfinance returns a dict-like or DataFrame depending on version
        if isinstance(cal, dict):
            ed = cal.get("Earnings Date")
            if not ed:
                return None
            d = ed[0] if isinstance(ed, list) else ed
        else:
            # DataFrame
            if "Earnings Date" in getattr(cal, "index", []):
                d = cal.loc["Earnings Date"].iloc[0]
            else:
                return None
        if isinstance(d, datetime):
            return {"date": d.date().isoformat(), "time": ""}
        if isinstance(d, date):
            return {"date": d.isoformat(), "time": ""}
        return None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
#  CACHE LOAD / BUILD
# ═══════════════════════════════════════════════════════════════

def _cache_is_fresh() -> bool:
    if not CACHE_PATH.exists():
        return False
    age_s = datetime.now().timestamp() - CACHE_PATH.stat().st_mtime
    return age_s < CACHE_TTL_HOURS * 3600


def _load_cache() -> dict[str, dict]:
    if not CACHE_PATH.exists():
        return {}
    try:
        with CACHE_PATH.open("r") as f:
            return json.load(f).get("earnings", {})
    except Exception as e:
        logger.warning(f"Earnings cache load failed: {e}")
        return {}


def _save_cache(data: dict[str, dict]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated": datetime.now().isoformat(),
        "earnings": data,
    }
    try:
        with CACHE_PATH.open("w") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        logger.warning(f"Earnings cache save failed: {e}")


def refresh_earnings_cache(tickers: Optional[list[str]] = None, force: bool = False) -> dict[str, dict]:
    """
    Rebuild the earnings cache. Called from the pre-market job and on demand.
    Returns the cache contents.
    """
    if not force and _cache_is_fresh():
        return _load_cache()

    if tickers is None:
        tickers = config.get_full_universe()

    today = date.today()
    window_end = today + timedelta(days=30)

    logger.info(
        f"Refreshing earnings cache: {len(tickers)} tickers, "
        f"window {today} → {window_end}"
    )

    # Try FMP first (one call covers the whole window)
    fmp_data = _fmp_fetch_range(today, window_end)
    by_ticker: dict[str, dict] = {t: fmp_data[t] for t in tickers if t in fmp_data}

    # yfinance fallback for anything FMP didn't cover
    missing = [t for t in tickers if t not in by_ticker]
    if missing and len(missing) <= 50:
        logger.info(f"Filling {len(missing)} tickers via yfinance fallback")
        for t in missing:
            r = _yfinance_fetch_ticker(t)
            if r:
                # Only keep if within window
                try:
                    d = date.fromisoformat(r["date"])
                    if today <= d <= window_end:
                        by_ticker[t] = r
                except Exception:
                    continue

    _save_cache(by_ticker)
    logger.info(f"Earnings cache refreshed: {len(by_ticker)} tickers with upcoming prints")
    return by_ticker


# ═══════════════════════════════════════════════════════════════
#  LOOKUP / CLASSIFICATION
# ═══════════════════════════════════════════════════════════════

# Badge level ordering (high → low severity)
BADGE_LEVELS = ("today_amc", "tomorrow", "this_week", "next_week", "none")


def get_earnings_context(
    ticker: str,
    now: Optional[datetime] = None,
) -> dict:
    """
    Return earnings context for a ticker.

    {
        "has_earnings":    bool,
        "date":            "YYYY-MM-DD" | None,
        "days_until":      int | None,
        "time_of_day":     "amc" | "bmo" | "",
        "badge_level":     one of BADGE_LEVELS,
        "badge_text":      human-readable short string for UI, e.g. "EARN TOMORROW"
        "score_adjustment": int  (e.g. -5 if tomorrow BMO)
        "hard_filter":     bool — True means drop the signal entirely
        "reason":          str
    }
    """
    now = now or datetime.now(config.ET)
    today = now.date()

    default = {
        "has_earnings": False,
        "date": None,
        "days_until": None,
        "time_of_day": "",
        "badge_level": "none",
        "badge_text": "",
        "score_adjustment": 0,
        "hard_filter": False,
        "reason": "",
    }

    cache = _load_cache()
    entry = cache.get(ticker)
    if not entry:
        return default

    try:
        earn_date = date.fromisoformat(entry["date"])
    except Exception:
        return default

    days_until = (earn_date - today).days
    time_of_day = (entry.get("time") or "").lower()

    out = {
        **default,
        "has_earnings": True,
        "date": earn_date.isoformat(),
        "days_until": days_until,
        "time_of_day": time_of_day,
    }

    # ── Past? ignore ──
    if days_until < 0:
        out["has_earnings"] = False
        out["days_until"] = None
        out["date"] = None
        return out

    # ── Today AMC (after close) ──
    if days_until == 0 and time_of_day == "amc":
        out["badge_level"] = "today_amc"
        out["badge_text"] = "EARN TODAY AMC"
        # Hard filter new entries after 2pm on days with post-close prints.
        if now.hour >= config.EARNINGS_HARD_FILTER_HOUR:
            out["hard_filter"] = True
            out["reason"] = (
                f"Earnings after close today; blocked after "
                f"{config.EARNINGS_HARD_FILTER_HOUR}:00 ET to avoid gap risk"
            )
        else:
            out["score_adjustment"] = config.EARNINGS_TOMORROW_PENALTY  # reuse -5
            out["reason"] = "Earnings after close today — early-day entry only"
        return out

    # ── Today BMO (already reported this morning, news is fresh) ──
    if days_until == 0 and time_of_day == "bmo":
        out["badge_level"] = "today_amc"
        out["badge_text"] = "EARN REPORTED TODAY"
        out["score_adjustment"] = 0
        out["reason"] = "Reported this morning — event risk already priced"
        return out

    # ── Tomorrow ──
    if days_until == 1:
        out["badge_level"] = "tomorrow"
        out["badge_text"] = "EARN TOMORROW"
        out["score_adjustment"] = config.EARNINGS_TOMORROW_PENALTY
        out["reason"] = "Earnings tomorrow — implied move distorts momentum signal"
        return out

    # ── This week (<= Friday of the current week) ──
    # days_until_to_friday: 0 on Mon is 4, on Fri is 0
    weekday = today.weekday()  # Mon=0..Sun=6
    days_until_friday = max(0, 4 - weekday)
    if days_until <= days_until_friday:
        out["badge_level"] = "this_week"
        out["badge_text"] = f"EARN {earn_date.strftime('%a').upper()}"
        out["score_adjustment"] = 0
        out["reason"] = "Earnings this week — informational"
        return out

    # ── Next 14 days ──
    if days_until <= 14:
        out["badge_level"] = "next_week"
        out["badge_text"] = f"EARN {earn_date.strftime('%b %-d').upper()}" \
            if hasattr(datetime, "strftime") else f"EARN {earn_date.isoformat()}"
        # Handle Windows strftime (%-d not portable)
        try:
            out["badge_text"] = f"EARN {earn_date.strftime('%b %d').upper().replace(' 0', ' ')}"
        except Exception:
            out["badge_text"] = f"EARN {earn_date.isoformat()}"
        out["score_adjustment"] = 0
        out["reason"] = "Earnings within 2 weeks — informational"
        return out

    # Beyond 14 days — no badge
    return out


def get_days_of_week_tickers(day_offset: int = 0) -> list[str]:
    """
    Helper: list all tickers reporting on today + day_offset. Used by
    the dashboard to show "tickers reporting today" summary.
    """
    target = date.today() + timedelta(days=day_offset)
    cache = _load_cache()
    return sorted([t for t, e in cache.items() if e.get("date") == target.isoformat()])
