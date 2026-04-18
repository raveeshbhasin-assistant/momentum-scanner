"""
Market Regime Detection
───────────────────────
Monitors broad-market volatility (VIX) to dynamically tighten or loosen
scanner scoring. When the market is in a risk-off regime, the scanner
raises the minimum composite score bar and cuts position-size guidance —
a proxy for "things could unravel, demand higher conviction."

Data source: yfinance ^VIX (free, ~15-min delayed, good enough for a
regime banner that refreshes every scan cycle).

Put/Call ratio is deferred to a future release (no reliable free source
on the plans we have today).

Added in v3.3.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import yfinance as yf

import config

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  REGIME BANDS
# ═══════════════════════════════════════════════════════════════
# Each band: (vix_upper, regime_name, min_score, size_multiplier, color)
# Score floor is ADDITIVE on top of config.MIN_COMPOSITE_SCORE.
# So CALM = 60, ELEVATED = 65, HIGH = 70, EXTREME = 80.
REGIME_BANDS = [
    # (upper_vix, label,     bump, size_mult,  color_css_var)
    (15.0,  "CALM",     0,   1.00,  "var(--green)"),
    (20.0,  "NORMAL",   0,   1.00,  "var(--green)"),
    (25.0,  "ELEVATED", 5,   0.75,  "var(--amber)"),
    (30.0,  "HIGH",    10,   0.50,  "var(--orange)"),
    (999.0, "EXTREME", 20,   0.25,  "var(--red)"),
]

# VIX intraday pop (absolute % change) that bumps us up a band regardless
VIX_SPIKE_INTRADAY_PCT = 10.0

# ── Cache ──
_cache: dict = {"ts": None, "data": None}
CACHE_TTL_SECONDS = 300  # 5 min — VIX 15-min delay anyway


def _classify_vix(vix_level: float, vix_pct_change: float) -> dict:
    """Map a VIX reading to a regime band. Intraday spike bumps up one band."""
    band_idx = 0
    for i, (upper, *_rest) in enumerate(REGIME_BANDS):
        if vix_level < upper:
            band_idx = i
            break
    else:
        band_idx = len(REGIME_BANDS) - 1

    # Spike override: if VIX is up >10% today, treat as one band worse
    spiked = vix_pct_change is not None and vix_pct_change >= VIX_SPIKE_INTRADAY_PCT
    if spiked:
        band_idx = min(band_idx + 1, len(REGIME_BANDS) - 1)

    upper, label, bump, size_mult, color = REGIME_BANDS[band_idx]
    return {
        "label": label,
        "min_score_bump": bump,
        "size_multiplier": size_mult,
        "color": color,
        "spiked": spiked,
    }


def get_regime() -> dict:
    """
    Fetch current VIX and classify the market regime.

    Returns:
        {
            "vix":              float   — latest VIX level
            "vix_change_pct":   float   — intraday % change (today's open → now)
            "label":            str     — CALM / NORMAL / ELEVATED / HIGH / EXTREME
            "min_score_bump":   int     — added to config.MIN_COMPOSITE_SCORE
            "effective_min_score": int  — the actual min score threshold to use
            "size_multiplier":  float   — 0.25-1.00, suggested position-size scaler
            "color":            str     — CSS color for the banner
            "spiked":           bool    — True if VIX up >10% intraday
            "as_of":            str     — ISO timestamp
            "error":            str|None — set if fetch failed
        }

    On failure returns a safe "NORMAL" default so scanning never breaks
    because the regime probe is unreachable.
    """
    # Cache check
    now = datetime.now()
    if _cache["ts"] and (now - _cache["ts"]).total_seconds() < CACHE_TTL_SECONDS:
        return _cache["data"]

    default = {
        "vix": None,
        "vix_change_pct": None,
        "label": "NORMAL",
        "min_score_bump": 0,
        "effective_min_score": config.MIN_COMPOSITE_SCORE,
        "size_multiplier": 1.00,
        "color": "var(--text-muted)",
        "spiked": False,
        "as_of": now.isoformat(),
        "error": None,
    }

    try:
        # Pull last 2 trading days of daily bars to compute today's % change.
        vix = yf.Ticker("^VIX")
        hist = vix.history(period="5d", interval="1d")
        if hist.empty:
            default["error"] = "No VIX data returned"
            _cache.update(ts=now, data=default)
            return default

        latest = float(hist["Close"].iloc[-1])

        # Try to get an intraday quote for a fresher read
        try:
            intraday = vix.history(period="1d", interval="5m")
            if not intraday.empty:
                latest = float(intraday["Close"].iloc[-1])
                today_open = float(intraday["Open"].iloc[0])
                pct = ((latest - today_open) / today_open * 100) if today_open else 0.0
            else:
                prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else latest
                pct = ((latest - prev_close) / prev_close * 100) if prev_close else 0.0
        except Exception:
            prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else latest
            pct = ((latest - prev_close) / prev_close * 100) if prev_close else 0.0

        classification = _classify_vix(latest, pct)

        result = {
            "vix": round(latest, 2),
            "vix_change_pct": round(pct, 2),
            "label": classification["label"],
            "min_score_bump": classification["min_score_bump"],
            "effective_min_score": config.MIN_COMPOSITE_SCORE + classification["min_score_bump"],
            "size_multiplier": classification["size_multiplier"],
            "color": classification["color"],
            "spiked": classification["spiked"],
            "as_of": now.isoformat(),
            "error": None,
        }
        _cache.update(ts=now, data=result)
        logger.info(
            f"Market regime: {result['label']} (VIX {result['vix']}, "
            f"{result['vix_change_pct']:+.1f}%) → min_score={result['effective_min_score']}, "
            f"size_mult={result['size_multiplier']}"
        )
        return result

    except Exception as e:
        logger.warning(f"get_regime: VIX fetch failed, defaulting to NORMAL: {e}")
        default["error"] = str(e)
        _cache.update(ts=now, data=default)
        return default


def get_regime_at(timestamp: datetime) -> dict:
    """
    Historical VIX regime at a given timestamp. Used by the backtest
    to apply v3.3 filters faithfully during replay.

    Returns the same shape as get_regime() but keyed to the given date.
    """
    safe_default = {
        "vix": None, "vix_change_pct": None,
        "label": "NORMAL", "min_score_bump": 0,
        "effective_min_score": config.MIN_COMPOSITE_SCORE,
        "size_multiplier": 1.00, "color": "var(--text-muted)",
        "spiked": False, "as_of": timestamp.isoformat(), "error": None,
    }
    try:
        vix = yf.Ticker("^VIX")
        # Grab a small window around the date
        start = (timestamp - timedelta(days=5)).date()
        end = (timestamp + timedelta(days=1)).date()
        hist = vix.history(start=start, end=end, interval="1d")
        if hist.empty or len(hist) < 2:
            return safe_default

        # Find the last row on or before the timestamp's date
        target_date = timestamp.date()
        on_or_before = hist[hist.index.date <= target_date]
        if on_or_before.empty:
            return safe_default

        latest = float(on_or_before["Close"].iloc[-1])
        prev = float(on_or_before["Close"].iloc[-2]) if len(on_or_before) >= 2 else latest
        pct = ((latest - prev) / prev * 100) if prev else 0.0
        c = _classify_vix(latest, pct)

        return {
            "vix": round(latest, 2),
            "vix_change_pct": round(pct, 2),
            "label": c["label"],
            "min_score_bump": c["min_score_bump"],
            "effective_min_score": config.MIN_COMPOSITE_SCORE + c["min_score_bump"],
            "size_multiplier": c["size_multiplier"],
            "color": c["color"],
            "spiked": c["spiked"],
            "as_of": timestamp.isoformat(),
            "error": None,
        }
    except Exception as e:
        logger.warning(f"get_regime_at({timestamp}): {e}")
        safe_default["error"] = str(e)
        return safe_default
