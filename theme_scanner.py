"""
ThemeHunter v0.1 — Parallel Scanner (Prototype)
────────────────────────────────────────────────
Designed from the 115-pick group-chat log analysis. Runs ALONGSIDE the
v3.4.2 momentum scanner — does not touch existing logic.

Hypothesis (from log analysis):
  ~55% of winning picks are theme re-rides (hot sector basket milked)
  ~25% are second-order beneficiaries of another stock's news
  ~15% are low-float opening-range runners (USAR, MVRL, AVEX class)
   ~5% are macro hedges (SPY/QQQ options — not our beat)

Three parallel universes, scored independently:
  A: Theme Leaders   — top stocks in the top 3 sector baskets by RS
  B: Gap + News      — pre-market gappers with RVOL ≥ 3x AND a catalyst
  C: Low-Float Runners — float < 75M, pre-market volume > 1M, theme-tagged

Scoring stack (0–100 per candidate, trade if ≥ 65):
  catalyst_freshness (0–25)   — decays linearly over 24h from event time
  theme_rs           (0–20)   — percentile of ticker's theme basket today
  rvol_score         (0–20)   — log-scaled, saturates at 10x
  opening_structure  (0–15)   — ORB-5 break / VWAP reclaim / inside-bar
  tape_quality       (0–10)   — spread, depth, print cleanliness proxy
  second_order       (0–10)   — peer/supplier/customer printing on news

Entry triggers (pick one, any qualifies):
  1. Break of opening 5-min high with that-minute RVOL ≥ 3x
  2. VWAP reclaim from below with green 1m bar (after 9:45 ET)
  3. News-time entry within 3 min of wire when score ≥ 75

Sizing tiers (R = risk per trade):
  Theme leader, large cap:      R = 0.50% equity
  Gap + news, mid cap:          R = 0.35% equity
  Low-float runner:             R = 0.20% equity (slippage tax)

Exits (carry forward from v3.4.1 MAE research):
  Bar-12 kill: MAE < -0.75R AND currR < 0
  Low-float tightened to bar-6: MAE < -0.50R (they run or chop fast)
  Scale 1/3 at +1R, 1/3 at +2R, trail last 1/3 on 5m VWAP

Kill-switches:
  No entries 12:00-13:30 ET (lunch dead zone from v3.4 analysis)
  No re-entries same ticker same day
  Regime filter: skip if SPY -0.5% AND theme basket red

Data: Reuses FMP (real-time) → yfinance fallback via scanner.fetch_intraday_data,
      Finnhub for news, and yfinance for sector ETF RS.
"""

from __future__ import annotations

import logging
import math
import os
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd
import yfinance as yf

import config
from news import get_sentiment_score, get_news_headlines
from sector_rotation import SECTOR_ETFS, SECTOR_HIGH_BETA

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  THEME BASKETS — sector ETFs + synthetic themes
# ═══════════════════════════════════════════════════════════════
# Matches how the group chat actually thinks. A ticker can belong to
# multiple baskets — it's scored against the *best* (strongest) one.

THEME_BASKETS: dict[str, dict] = {
    "AI_INFRA":       {"etf": "SMH", "bonus": 1.0, "tickers": [
        "NVDA", "AMD", "AVGO", "MRVL", "SMCI", "ANET", "ORCL", "DELL",
        "ARM", "MU", "NBIS", "CRWV",
    ]},
    "OPTICAL":        {"etf": "SMH", "bonus": 1.2, "tickers": [
        "AAOI", "LITE", "COHR", "CRDO", "GLW", "CIEN",
    ]},
    "MEMORY":         {"etf": "SMH", "bonus": 1.0, "tickers": [
        "MU", "SNDK", "WDC", "STX",
    ]},
    "SOFTWARE":       {"etf": "IGV", "bonus": 0.9, "tickers": [
        "NET", "CRWD", "PANW", "SNOW", "MDB", "DDOG", "NOW", "CRM",
        "PATH", "FSLY", "APP", "HIMS",
    ]},
    "CRYPTO_MINERS":  {"etf": "BITO", "bonus": 1.1, "tickers": [
        "MSTR", "COIN", "MARA", "IREN", "APLD", "HOOD",
    ]},
    "NUCLEAR":        {"etf": "URA", "bonus": 1.3, "tickers": [
        "OKLO", "SMR", "LEU", "CCJ", "UEC", "NNE",
    ]},
    "RARE_EARTH":     {"etf": "REMX", "bonus": 1.4, "tickers": [
        "CRML", "USAR", "MP", "UUUU", "TMC", "NAK",
    ]},
    "DEFENSE":        {"etf": "ITA", "bonus": 1.0, "tickers": [
        "LMT", "RTX", "NOC", "GD", "BA", "DFEN",
    ]},
    "OIL_SERVICES":   {"etf": "XOP", "bonus": 0.9, "tickers": [
        "XOM", "CVX", "COP", "SLB", "USO", "WTI",
    ]},
    "PRECIOUS_METAL": {"etf": "GLD", "bonus": 1.0, "tickers": [
        "GLD", "SLV", "UGL",
    ]},
    "EV_AUTO":        {"etf": "XLY", "bonus": 0.9, "tickers": [
        "TSLA", "RIVN", "LCID", "GM", "F",
    ]},
    "BATTERY":        {"etf": "LIT", "bonus": 1.1, "tickers": [
        "EOSE", "QS", "ALB",
    ]},
}


# Low-float runner seed list — candidates we'd scan aggressively in
# the first hour. Builds the C universe. Pulled from the group-chat
# history PLUS today's mentions (USAR, MVRL, AVEX).
LOW_FLOAT_SEED = [
    "USAR", "MVRL", "AVEX", "CRML", "AAOI", "AXTI", "BIRD", "WOOF",
    "NAK", "EOSE", "GSIT", "CPR", "NBIS", "IREN", "APLD", "OKLO",
    "NNE", "LEU", "UUUU", "TMC", "MP", "SMR",
]


# Second-order linkage map — if stock X has news, these also benefit
SECOND_ORDER_LINKS: dict[str, list[str]] = {
    "NVDA":  ["SMCI", "DELL", "ANET", "COHR", "LITE", "AAOI", "CRDO", "MRVL", "MU"],
    "META":  ["NET", "CRWD", "PANW", "AAOI", "COHR"],
    "MSFT":  ["ORCL", "SNOW", "MDB", "NBIS"],
    "AMZN":  ["ORCL", "NBIS"],
    "ORCL":  ["NBIS", "CRWV", "SMCI"],
    "TSM":   ["NVDA", "AMD", "AVGO", "MRVL"],
    "ASML":  ["AMAT", "LRCX", "KLAC"],
    "GOOGL": ["ANET", "CRWV"],
}


# ═══════════════════════════════════════════════════════════════
#  DATA HELPERS
# ═══════════════════════════════════════════════════════════════

def _today_et() -> datetime:
    return datetime.now(config.ET)


def fetch_theme_rs(lookback_days: int = 2) -> dict[str, dict]:
    """
    Compute today's intraday return for every theme basket's ETF.
    Returns {theme: {etf, return_pct, percentile, bonus}}.

    Uses yfinance 1-day bars with pre/post=False — simple and reliable
    for ETF tape (FMP would also work but yfinance is free and these
    are always covered).
    """
    etfs = sorted({v["etf"] for v in THEME_BASKETS.values()} | {"SPY", "QQQ"})
    out: dict[str, dict] = {}

    try:
        df = yf.download(
            tickers=etfs,
            period=f"{max(lookback_days, 2)}d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
        )
    except Exception as e:
        logger.warning(f"Theme RS fetch failed: {e}")
        return {}

    if df is None or df.empty:
        return {}

    returns: dict[str, float] = {}
    for etf in etfs:
        try:
            if isinstance(df.columns, pd.MultiIndex) and etf in df.columns.get_level_values(0):
                etf_df = df[etf].dropna(subset=["Close"])
            else:
                etf_df = df.dropna(subset=["Close"]) if len(etfs) == 1 else pd.DataFrame()
            if len(etf_df) < 2:
                continue
            prev_close = float(etf_df["Close"].iloc[-2])
            last_close = float(etf_df["Close"].iloc[-1])
            if prev_close <= 0:
                continue
            returns[etf] = (last_close - prev_close) / prev_close * 100
        except Exception:
            continue

    if not returns:
        return {}

    # Percentile rank within the set of baskets (not all ETFs incl SPY/QQQ)
    basket_etfs = {v["etf"] for v in THEME_BASKETS.values()}
    basket_returns = [r for e, r in returns.items() if e in basket_etfs]
    if not basket_returns:
        return {}

    sorted_r = sorted(basket_returns)
    n = len(sorted_r)

    for theme, meta in THEME_BASKETS.items():
        etf = meta["etf"]
        if etf not in returns:
            continue
        r = returns[etf]
        # Percentile: fraction of baskets this one beats
        rank = sum(1 for x in sorted_r if x < r)
        pct = rank / max(n - 1, 1) * 100 if n > 1 else 50.0
        out[theme] = {
            "etf": etf,
            "return_pct": round(r, 2),
            "percentile": round(pct, 1),
            "bonus": meta["bonus"],
        }

    # Attach SPY/QQQ for regime filter
    out["_SPY"] = {"return_pct": round(returns.get("SPY", 0.0), 2)}
    out["_QQQ"] = {"return_pct": round(returns.get("QQQ", 0.0), 2)}
    return out


def _ticker_themes(ticker: str) -> list[str]:
    """All themes a ticker belongs to."""
    return [t for t, m in THEME_BASKETS.items() if ticker in m["tickers"]]


def best_theme_for_ticker(ticker: str, theme_rs: dict) -> Optional[dict]:
    """Pick the strongest (highest percentile) theme this ticker belongs to."""
    candidates = []
    for theme in _ticker_themes(ticker):
        if theme in theme_rs:
            candidates.append((theme, theme_rs[theme]))
    if not candidates:
        return None
    best = max(candidates, key=lambda x: x[1]["percentile"])
    return {"theme": best[0], **best[1]}


# ═══════════════════════════════════════════════════════════════
#  QUOTE + INTRADAY FETCH (reuses existing scanner helpers)
# ═══════════════════════════════════════════════════════════════

def _fetch_quote_pack(tickers: list[str]) -> dict[str, dict]:
    """
    Grab: regular-session % change, current price, avg volume, market cap,
    float. Uses ONE batched yf.download call for prices (fast) and yf.Ticker
    ONLY for market cap / float on the stocks that made the short-list.
    """
    out: dict[str, dict] = {}
    if not tickers:
        return out

    # ── 1. Batched daily history (fast) ──
    try:
        df = yf.download(
            tickers=tickers,
            period="10d",
            interval="1d",
            group_by="ticker",
            progress=False,
            threads=True,
            auto_adjust=False,
        )
    except Exception as e:
        logger.warning(f"Batch yf.download failed: {e}")
        return out

    if df is None or df.empty:
        return out

    for t in tickers:
        try:
            if isinstance(df.columns, pd.MultiIndex) and t in df.columns.get_level_values(0):
                hist = df[t].dropna(subset=["Close"])
            elif len(tickers) == 1:
                hist = df.dropna(subset=["Close"])
            else:
                continue
            if len(hist) < 2:
                continue
            prev_close = float(hist["Close"].iloc[-2])
            last_close = float(hist["Close"].iloc[-1])
            avg_vol = float(hist["Volume"].mean()) if "Volume" in hist else 0
            last_vol = float(hist["Volume"].iloc[-1]) if "Volume" in hist else 0
            intraday_pct = (last_close - prev_close) / prev_close * 100 if prev_close > 0 else 0

            out[t] = {
                "price": last_close,
                "prev_close": prev_close,
                "pct": round(intraday_pct, 2),
                "avg_vol": avg_vol,
                "last_vol": last_vol,
                "rvol_daily": round(last_vol / avg_vol, 2) if avg_vol > 0 else 0,
                "shares_float": None,   # filled in for short-list only
                "market_cap": None,
            }
        except Exception as e:
            logger.debug(f"Quote post-process failed for {t}: {e}")
            continue
    return out


def _enrich_float_mcap(tickers: list[str], quotes: dict[str, dict]) -> None:
    """Fill in shares_float / market_cap for the short-list only (slow path)."""
    for t in tickers:
        if t not in quotes:
            continue
        try:
            info = yf.Ticker(t).fast_info
            try:
                quotes[t]["shares_float"] = float(getattr(info, "shares", 0) or 0)
            except Exception:
                pass
            try:
                quotes[t]["market_cap"] = float(getattr(info, "market_cap", 0) or 0)
            except Exception:
                pass
        except Exception:
            continue


def _fetch_intraday_5m(tickers: list[str], days: int = 5) -> dict[str, pd.DataFrame]:
    """Delegate to the existing data helper (FMP → yfinance fallback).
    Uses a 5-day window so rewind-mode RVOL has prior sessions to compare
    against (2-day window fails on Mondays when weekend swallows the prior day).
    """
    from scanner import fetch_intraday_data
    return fetch_intraday_data(tickers, interval="5m", days=days)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL COMPUTATION (the six-component score)
# ═══════════════════════════════════════════════════════════════

def _today_bars(df: pd.DataFrame, as_of: Optional[datetime] = None) -> pd.DataFrame:
    """
    Slice to today's session bars only. If as_of is given, also truncate
    to bars at-or-before that timestamp — used for rewind/backtest mode.
    """
    if df.empty:
        return df
    if as_of is None:
        today = df.index[-1].date()
    else:
        today = as_of.date()
    mask = pd.Index([ts.date() == today for ts in df.index])
    out = df[mask]
    if as_of is not None and not out.empty:
        # tz-safe: compare timestamps directly
        out = out[out.index <= pd.Timestamp(as_of)]
    return out


def _vwap(df: pd.DataFrame) -> pd.Series:
    """Anchored session VWAP from today's bars."""
    typical = (df["High"] + df["Low"] + df["Close"]) / 3
    cum_vp = (typical * df["Volume"]).cumsum()
    cum_v = df["Volume"].cumsum().replace(0, np.nan)
    return cum_vp / cum_v


def _rvol_current(df_full: pd.DataFrame, df_today: pd.DataFrame,
                   as_of: Optional[datetime] = None) -> float:
    """
    Live mode (as_of is None): last-bar volume vs average 5m volume over history.
    Rewind/backtest mode (as_of is set): cumulative volume through `as_of` vs
      average cumulative volume through the same time-of-day on prior session(s).
    Cumulative metric is much more stable across the day — a single quiet 5-min
    bar can make last-bar RVOL collapse, which unfairly crushes rewind scores.
    """
    if df_today.empty or df_full.empty:
        return 0.0

    if as_of is None:
        last = df_today.iloc[-1]
        avg_5m = df_full["Volume"].mean()
        if avg_5m <= 0:
            return 0.0
        return float(last["Volume"]) / float(avg_5m)

    # Cumulative RVOL ───────────────────────────────────────────
    try:
        today_date = df_today.index[-1].date()
    except Exception:
        return 0.0
    today_cum = float(df_today["Volume"].sum())
    if today_cum <= 0:
        return 0.0

    # Build prior-session cumulative up to same (hour, minute).
    cutoff_hm = (as_of.hour, as_of.minute)
    prior = df_full[pd.Index([ts.date() != today_date for ts in df_full.index])]
    if prior.empty:
        # fallback: simple bar-by-bar mean scaled by bars-so-far
        avg_5m = float(df_full["Volume"].mean())
        bars_so_far = max(1, len(df_today))
        baseline = avg_5m * bars_so_far
        return today_cum / baseline if baseline > 0 else 0.0

    # Group prior bars by session date, keep those at-or-before same HH:MM
    cum_by_day: list[float] = []
    for d, grp in prior.groupby(prior.index.date):
        mask = pd.Index([(ts.hour, ts.minute) <= cutoff_hm for ts in grp.index])
        cum = float(grp[mask]["Volume"].sum())
        if cum > 0:
            cum_by_day.append(cum)
    if cum_by_day:
        baseline = sum(cum_by_day) / len(cum_by_day)
        if baseline > 0:
            return today_cum / baseline

    # Final fallback: extrapolate avg 5m bar volume * bars-so-far today
    avg_5m = float(prior["Volume"].mean()) if not prior.empty else 0.0
    bars_so_far = max(1, len(df_today))
    baseline = avg_5m * bars_so_far
    return today_cum / baseline if baseline > 0 else 0.0


def _catalyst_freshness_score(ticker: str) -> tuple[float, Optional[dict]]:
    """
    0-25 based on how fresh a strong-sentiment headline is.
    Returns (score, top_news) where top_news is the driving headline.
    """
    sentiment, news = get_sentiment_score(ticker)
    if not news:
        return 0.0, None
    # Best single news item: strongest |weighted_sentiment|
    top = max(news, key=lambda n: abs(n.get("weighted_sentiment", 0)))
    if top.get("weighted_sentiment", 0) < 0.10:
        return 0.0, None
    hours = top.get("hours_ago", 24)
    # Linear decay 25→0 over 24h
    freshness = max(0, 25 * (1 - hours / 24))
    # Scale by sentiment magnitude (cap at 1.0)
    magnitude = min(abs(top["weighted_sentiment"]) / 0.5, 1.0)
    return round(freshness * magnitude, 1), top


def _theme_rs_score(best_theme: Optional[dict]) -> float:
    """0-20 based on ticker's theme basket percentile today."""
    if not best_theme:
        return 0.0
    pct = best_theme.get("percentile", 0)
    bonus = best_theme.get("bonus", 1.0)
    return round(min(20.0, (pct / 100) * 20 * bonus), 1)


def _rvol_score(rvol: float) -> float:
    """0-20, log-scaled, saturates at 10x."""
    if rvol <= 1.0:
        return 0.0
    return round(min(20.0, 20 * math.log10(max(rvol, 1.01)) / math.log10(10)), 1)


def _opening_structure(df_today: pd.DataFrame) -> tuple[float, str]:
    """
    0-15.
      15 — break of first 5-min high with volume confirm
      10 — VWAP reclaim from below after 9:45
       7 — inside-bar continuation (consolidation-then-push)
       0 — below ORB-5 low or below VWAP with no reclaim
    """
    if df_today.empty or len(df_today) < 2:
        return 0.0, "no_data"

    first_bar = df_today.iloc[0]
    last_bar = df_today.iloc[-1]
    orb_high = float(first_bar["High"])
    orb_low = float(first_bar["Low"])
    last_close = float(last_bar["Close"])

    # Compute session VWAP
    try:
        vwap = _vwap(df_today)
        last_vwap = float(vwap.iloc[-1])
    except Exception:
        last_vwap = last_close

    avg_vol = df_today["Volume"].mean() if len(df_today) > 1 else 0
    last_vol = float(last_bar["Volume"])
    vol_confirm = last_vol > 1.5 * avg_vol if avg_vol > 0 else False

    # ORB-5 break?
    if last_close > orb_high and vol_confirm:
        return 15.0, "orb5_break_with_volume"
    if last_close > orb_high:
        return 12.0, "orb5_break_no_volume"

    # VWAP reclaim? Was below earlier, now above
    try:
        below_earlier = (df_today["Close"][:-1] < vwap[:-1]).any()
        if below_earlier and last_close > last_vwap:
            return 10.0, "vwap_reclaim"
    except Exception:
        pass

    # Inside bar? Recent bar inside prior
    if len(df_today) >= 3:
        prev = df_today.iloc[-2]
        if float(last_bar["High"]) < float(prev["High"]) and float(last_bar["Low"]) > float(prev["Low"]):
            if last_close > last_vwap:
                return 7.0, "inside_bar_above_vwap"

    # Below ORB low — kill
    if last_close < orb_low:
        return 0.0, "below_orb_low"

    return 3.0, "no_clean_trigger"


def _tape_quality(q: dict) -> float:
    """
    0-10 tape proxy. We don't have L2, so use:
      - daily RVOL (higher = more active tape)
      - price range (too-cheap = wider %-spreads)
      - market cap (tiny = worse fills)
    """
    score = 5.0  # neutral default
    rvol_d = q.get("rvol_daily", 0)
    if rvol_d >= 3:
        score += 3
    elif rvol_d >= 1.5:
        score += 1.5
    price = q.get("price", 0)
    if price < 3:
        score -= 3
    elif price < 5:
        score -= 1
    mcap = q.get("market_cap") or 0
    if mcap and mcap < 200_000_000:  # sub-$200M caps — wild tape
        score -= 2
    elif mcap and mcap > 10_000_000_000:
        score += 1
    return round(max(0, min(10, score)), 1)


def _second_order_score(ticker: str) -> tuple[float, Optional[str]]:
    """0-10 if a linked leader is printing on strong-sentiment news today."""
    # Inverse-map: who do I benefit from?
    for leader, beneficiaries in SECOND_ORDER_LINKS.items():
        if ticker in beneficiaries:
            try:
                s, news = get_sentiment_score(leader)
                if news and abs(s) > 0.15:
                    # Freshness matters
                    top = max(news, key=lambda n: abs(n.get("weighted_sentiment", 0)))
                    if top.get("hours_ago", 24) < 6 and top.get("weighted_sentiment", 0) > 0.15:
                        return 10.0, f"{leader}: {top['headline'][:80]}"
            except Exception:
                continue
    return 0.0, None


# ═══════════════════════════════════════════════════════════════
#  UNIVERSE BUILDERS
# ═══════════════════════════════════════════════════════════════

def build_universe_A(theme_rs: dict, top_n_themes: int = 3, top_k_per_theme: int = 5) -> list[str]:
    """Top K tickers in top N theme baskets by percentile."""
    ranked_themes = sorted(
        [(t, m) for t, m in theme_rs.items() if not t.startswith("_")],
        key=lambda x: -x[1]["percentile"],
    )[:top_n_themes]
    out: list[str] = []
    seen: set[str] = set()
    for theme, _ in ranked_themes:
        for t in THEME_BASKETS[theme]["tickers"][:top_k_per_theme]:
            if t not in seen:
                out.append(t)
                seen.add(t)
    return out


def build_universe_B(quotes: dict[str, dict], min_gap_pct: float = 3.0,
                     min_rvol_daily: float = 2.0) -> list[str]:
    """Tickers with >= min_gap_pct daily move AND >= min_rvol_daily RVOL."""
    return [
        t for t, q in quotes.items()
        if abs(q.get("pct", 0)) >= min_gap_pct
        and q.get("rvol_daily", 0) >= min_rvol_daily
    ]


def build_universe_C(quotes: dict[str, dict], max_mcap: float = 5e9,
                     min_price: float = 1.0) -> list[str]:
    """Low-float / small-cap runners with decent price + volume."""
    out = []
    for t, q in quotes.items():
        mcap = q.get("market_cap") or 0
        price = q.get("price", 0)
        rvol = q.get("rvol_daily", 0)
        pct = abs(q.get("pct", 0))
        if mcap == 0:  # unknown cap — only include if in seed list
            if t in LOW_FLOAT_SEED and rvol >= 1.5:
                out.append(t)
                continue
        if 0 < mcap < max_mcap and price >= min_price and rvol >= 1.5 and pct >= 2.0:
            out.append(t)
    return out


# ═══════════════════════════════════════════════════════════════
#  MAIN SCAN
# ═══════════════════════════════════════════════════════════════

@dataclass
class ThemeSignal:
    ticker: str
    bucket: str                # "A" | "B" | "C"
    score: float
    score_breakdown: dict      # per-component
    theme: Optional[str]
    theme_pct: Optional[float]
    price: float
    pct: float
    rvol: float
    rvol_daily: float
    opening_structure: str
    catalyst: Optional[dict]
    second_order: Optional[str]
    market_cap: Optional[float]
    shares_float: Optional[float]
    entry_trigger: str
    size_tier: str
    size_R_pct: float
    stop_distance_pct: float
    target_R_pct: float
    kill_switch: Optional[str]

    def dict(self):
        d = asdict(self)
        return d


def _size_tier(bucket: str, q: dict) -> tuple[str, float]:
    mcap = q.get("market_cap") or 0
    if bucket == "A" or mcap >= 10_000_000_000:
        return "theme_leader", 0.50
    if bucket == "C" or (mcap and mcap < 2_000_000_000):
        return "low_float", 0.20
    return "gap_news", 0.35


def _entry_trigger(structure: str, score: float) -> str:
    if structure == "orb5_break_with_volume":
        return "ORB-5 break + volume"
    if structure == "orb5_break_no_volume":
        return "ORB-5 break (await volume)"
    if structure == "vwap_reclaim":
        return "VWAP reclaim + 1m green"
    if structure == "inside_bar_above_vwap":
        return "Inside-bar continuation"
    if score >= 75:
        return "News-time entry (wire +3min)"
    return "Wait for confirmation"


def _kill_switch(now_et: datetime, theme_rs: dict, best_theme: Optional[dict]) -> Optional[str]:
    # Lunch dead zone
    if 12 <= now_et.hour < 13 or (now_et.hour == 13 and now_et.minute < 30):
        return "lunch_dead_zone"
    # Regime filter: SPY red + theme red
    spy = theme_rs.get("_SPY", {}).get("return_pct", 0)
    theme_r = (best_theme or {}).get("return_pct", 0)
    if spy <= -0.5 and theme_r < 0:
        return "regime_red_both"
    return None


def run_theme_scan(now_override: Optional[datetime] = None,
                   min_score: float = 65.0,
                   max_results: int = 30,
                   as_of_hhmm: Optional[str] = None,
                   max_intraday: int = 120) -> dict[str, Any]:
    """
    Top-level entry point. Runs all three universes, scores each candidate,
    returns a ranked dict suitable for the /theme-scanner page.

    as_of_hhmm: "HH:MM" — if set, rewinds intraday bars to that time and
      scores as-if we were looking at the tape at that minute TODAY. Great
      for "what would the scanner have said at 9:45 this morning?"
    """
    now = now_override or _today_et()
    as_of = None
    if as_of_hhmm:
        try:
            hh, mm = as_of_hhmm.split(":")
            as_of = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        except Exception:
            logger.warning(f"Bad as_of_hhmm: {as_of_hhmm}")
    logger.info(f"ThemeHunter scan starting at {now.isoformat()}")

    # ── 1. Theme basket RS ──
    theme_rs = fetch_theme_rs()
    if not theme_rs:
        logger.warning("Theme RS unavailable — continuing with empty RS map")

    # ── 2. Build candidate universe from 3 sub-builders ──
    # Quote pack covers the widest universe we might care about
    candidate_pool = sorted(set(
        list(config.SP500_LIQUID)
        + list(config.HIGH_BETA_EXTENDED)
        + LOW_FLOAT_SEED
        + [t for m in THEME_BASKETS.values() for t in m["tickers"]]
    ))
    quotes = _fetch_quote_pack(candidate_pool)
    logger.info(f"Quote pack: {len(quotes)}/{len(candidate_pool)} tickers")

    uni_A = build_universe_A(theme_rs)
    # Before B/C, enrich market_cap on low-float seed + biggest movers so the
    # filter has the data it needs (batched yf.download doesn't carry mcap).
    movers = sorted(quotes.keys(),
                    key=lambda t: -abs(quotes[t].get("pct", 0)))[:40]
    _enrich_float_mcap(sorted(set(LOW_FLOAT_SEED + movers)), quotes)
    uni_B = build_universe_B(quotes)
    uni_C = build_universe_C(quotes)
    logger.info(f"Universe sizes — A: {len(uni_A)}, B: {len(uni_B)}, C: {len(uni_C)}")

    # Merge + track which bucket each came from (prefer A > B > C when overlap)
    bucket_map: dict[str, str] = {}
    for t in uni_A: bucket_map.setdefault(t, "A")
    for t in uni_B: bucket_map.setdefault(t, "B")
    for t in uni_C: bucket_map.setdefault(t, "C")
    scan_universe = sorted(bucket_map.keys())
    if not scan_universe:
        return {
            "generated_at": now.isoformat(),
            "theme_rs": theme_rs,
            "universe_sizes": {"A": 0, "B": 0, "C": 0},
            "signals": [],
            "note": "Empty universe — market may be closed or all data sources failed.",
        }

    # ── 3. Pull intraday 5m for scoring ──
    intraday = _fetch_intraday_5m(scan_universe[:max_intraday])
    logger.info(f"Intraday 5m: {len(intraday)}/{len(scan_universe[:max_intraday])} tickers")

    # ── 4. Score every candidate ──
    signals: list[ThemeSignal] = []
    for t in scan_universe[:max_intraday]:
        q = quotes.get(t)
        if not q:
            continue

        best_theme = best_theme_for_ticker(t, theme_rs)
        cat_score, cat_news = _catalyst_freshness_score(t)
        theme_score = _theme_rs_score(best_theme)

        df_full = intraday.get(t, pd.DataFrame())
        df_today = _today_bars(df_full, as_of=as_of)
        rvol = _rvol_current(df_full, df_today, as_of=as_of) if not df_today.empty else q.get("rvol_daily", 0)
        rvol_s = _rvol_score(rvol)

        # In rewind mode, override the daily pct with intraday pct as of the rewind bar
        ticker_pct = q["pct"]
        if as_of is not None and not df_today.empty and len(df_today) >= 1:
            try:
                first_open = float(df_today["Open"].iloc[0])
                last_close = float(df_today["Close"].iloc[-1])
                if first_open > 0:
                    ticker_pct = round((last_close - first_open) / first_open * 100, 2)
            except Exception:
                pass

        struct_score, struct_tag = _opening_structure(df_today)
        tape_s = _tape_quality(q)
        so_score, so_news = _second_order_score(t)

        score = round(cat_score + theme_score + rvol_s + struct_score + tape_s + so_score, 1)

        bucket = bucket_map[t]
        size_tier, size_R = _size_tier(bucket, q)
        trigger = _entry_trigger(struct_tag, score)
        kill = _kill_switch(now, theme_rs, best_theme)

        # Stop/target in %
        stop_pct = 2.0 if size_tier == "theme_leader" else (2.5 if size_tier == "gap_news" else 3.5)
        target_pct = stop_pct * 2.5

        signals.append(ThemeSignal(
            ticker=t,
            bucket=bucket,
            score=score,
            score_breakdown={
                "catalyst_freshness": cat_score,
                "theme_rs": theme_score,
                "rvol": rvol_s,
                "opening_structure": struct_score,
                "tape_quality": tape_s,
                "second_order": so_score,
            },
            theme=best_theme.get("theme") if best_theme else None,
            theme_pct=best_theme.get("percentile") if best_theme else None,
            price=q["price"],
            pct=ticker_pct,
            rvol=round(rvol, 2),
            rvol_daily=q["rvol_daily"],
            opening_structure=struct_tag,
            catalyst={
                "headline": cat_news["headline"],
                "hours_ago": cat_news["hours_ago"],
                "sentiment": cat_news["sentiment"],
                "source": cat_news.get("source"),
                "url": cat_news.get("url"),
            } if cat_news else None,
            second_order=so_news,
            market_cap=q.get("market_cap"),
            shares_float=q.get("shares_float"),
            entry_trigger=trigger,
            size_tier=size_tier,
            size_R_pct=size_R,
            stop_distance_pct=stop_pct,
            target_R_pct=target_pct,
            kill_switch=kill,
        ))

    # ── 5. Rank + filter ──
    signals.sort(key=lambda s: -s.score)
    qualifying = [s for s in signals if s.score >= min_score and not s.kill_switch]
    watchlist = [s for s in signals if min_score - 10 <= s.score < min_score and not s.kill_switch]
    killed = [s for s in signals if s.kill_switch]

    logger.info(f"ThemeHunter scan complete: {len(qualifying)} QUALIFY, {len(watchlist)} WATCH, {len(killed)} KILLED")

    return {
        "generated_at": now.isoformat(),
        "as_of": as_of.isoformat() if as_of else None,
        "theme_rs": theme_rs,
        "universe_sizes": {"A": len(uni_A), "B": len(uni_B), "C": len(uni_C)},
        "min_score": min_score,
        "signals": [s.dict() for s in qualifying[:max_results]],
        "watchlist": [s.dict() for s in watchlist[:max_results]],
        "killed": [s.dict() for s in killed[:10]],
    }


# ═══════════════════════════════════════════════════════════════
#  CACHE HELPERS — save/load scan results as JSON
# ═══════════════════════════════════════════════════════════════

import json
from pathlib import Path

_CACHE_DIR = Path(__file__).parent / "data"
_CACHE_DIR.mkdir(exist_ok=True)


def save_scan(result: dict, name: str = "latest"):
    path = _CACHE_DIR / f"theme_scan_{name}.json"
    with open(path, "w") as f:
        json.dump(result, f, indent=2, default=str)
    return path


def load_scan(name: str = "latest") -> Optional[dict]:
    path = _CACHE_DIR / f"theme_scan_{name}.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


if __name__ == "__main__":
    # Quick local smoke test
    import json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = run_theme_scan(min_score=50.0)
    print(json.dumps({
        "generated_at": result["generated_at"],
        "universe_sizes": result["universe_sizes"],
        "themes_top3": sorted(
            [(k, v.get("percentile", 0), v.get("return_pct", 0))
             for k, v in result["theme_rs"].items() if not k.startswith("_")],
            key=lambda x: -x[1],
        )[:3],
        "signal_count": len(result["signals"]),
        "top_signals": [
            {"ticker": s["ticker"], "bucket": s["bucket"], "score": s["score"],
             "theme": s["theme"], "pct": s["pct"], "trigger": s["entry_trigger"]}
            for s in result["signals"][:10]
        ],
    }, indent=2))
