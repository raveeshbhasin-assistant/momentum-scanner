"""
Configuration for Momentum Scanner
"""
import os
from zoneinfo import ZoneInfo

# ── App version — SINGLE SOURCE (v3.8.3) ─────────────────────
# Everything version-shaped derives from this one constant: the FastAPI
# version (app.py), the footer on every page (_footer.html via the
# APP_VERSION Jinja global), and the static-asset cache-bust query string
# (_head.html). Before v3.8.3 each surface was hand-bumped and drifted
# (footer was stuck at 3.7.4, cache-bust at 3.8.0 while the app was 3.8.2).
# Bump THIS on every release; the pre-push hygiene hook checks the rest.
APP_VERSION = "3.8.3"

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

# v3.7.0: Hard reject above this RSI. 13-day research showed picks with
# RSI > 75 hit 2.5R only 14.3% vs ~23% for the 55–75 band. Previously was
# only a soft -15 point penalty.
RSI_HARD_CAP = 75                   # If True, reject before composite scoring
RSI_HARD_CAP_ENABLED = True

# v3.7.0: Sector exclusion list. These four sectors had 7–14% 2.5R hit
# rates in the 13-day research window, well below the 22% baseline.
# Crypto/Blockchain was retained per operator preference despite a 13.3%
# rate (small n; volatile but high-conviction names).
EXCLUDED_SECTORS = [
    "Utilities",
    "Consumer Disc",
    "Real Estate",
]

# v3.7.0: Block emission of new picks after this ET time. Open-position
# management (alerts, exits, dashboard) continues to run. Research showed
# 15:00–16:00 picks hit 2.5R only 3.7% of the time — not enough runway
# left in the day for the 2.5R target.
EMISSION_CUTOFF_HOUR = 15
EMISSION_CUTOFF_MINUTE = 0
MIN_COMPOSITE_SCORE = 60            # Strong signal threshold (0-100)
WEAK_SIGNAL_FLOOR = 40              # v3.4.2: show down to this, label <MIN as weak
MAX_SIGNALS_PER_SCAN = 20           # v3.4.2: bumped from 10 to show weak tier too


# ── ELITE tier (precision subset of STRONG) ───────────────────
# ELITE is a precision-refinement layer on top of STRONG.
#
# ⚠️ HISTORY / DATA-INTEGRITY NOTE (v3.7.5, 2026-07-06):
# The old headline — "ELITE rule: 75.8% WR, +1.42R" from a "19-day
# diagnostic 2026-04-20 → 2026-05-15" — was a BACKFILL ARTIFACT and has
# been retired. The `strong_signal` field did not exist in the persisted
# record until 2026-05-12; every pick before that had strong_signal
# reconstructed retroactively via /api/backfill_strong + strong_overrides.
# The 75.8% diagnostic was computed almost entirely on that backfilled
# (look-ahead-contaminated) window, not on lived signals.
#
# TRULY-LIVE ELITE performance (strong_signal logged, 2026-05-12 → 07-06,
# 408 picks, first-appearance, ex post-close):
#   5/12–5/22 (early live):  50.6% WR, +0.43R/pick
#   5/26–6/12 (mid):         43.0% WR, +0.27R/pick
#   6/15–7/06 (last month):  36.5% WR, +0.07R/pick   ← decayed to break-even
# The old cat=D + rvol≥2 rule barely separates from plain STRONG live.
#
# v3.7.5 TIGHTENED ELITE — precision-over-recall. On 6/01+ live data the
# stack below lifts ELITE from 38.6% → ~45% WR and R/pick +0.12 → +0.40,
# keeping ~1/3 of picks; the discarded 2/3 were net-negative on R. Rule:
#   STRONG  AND  category == "D"
#   AND  ELITE_MIN_RVOL <= rvol < ELITE_MAX_RVOL        (2.0–5.0; 5–6 band = 7% WR)
#   AND  rsi >= ELITE_MIN_RSI                           (≥68; 60–70 band is dead)
#   AND  entry inside [ELITE_WINDOW_START, ELITE_WINDOW_END] ET (09:30–10:00)
#   AND  stop_distance_pct >= ELITE_MIN_STOP_PCT        (≥0.9%; tight/quiet = noise-stopped)
#
# Every consumer derives the flag through config.is_elite(signal) — the
# single source of truth. scanner.scan ALSO persists it as signal["elite"]
# so templates/notifier read the boolean instead of re-deriving it.
# Set ELITE_ENABLED=False to short-circuit every site.
ELITE_ENABLED = True
ELITE_REQUIRES_CATEGORY = "D"
ELITE_MIN_RVOL = 2.0
ELITE_MAX_RVOL = 5.0                 # v3.7.5: 5–6 RVOL band collapsed to 7% WR (blow-off/exhaustion)
ELITE_MIN_RSI = 68                   # v3.7.5: RSI≥68 → 43.5% WR vs 36% for 60–70 band
ELITE_MIN_STOP_PCT = 0.9             # v3.7.5: stop-distance < ~0.9% of entry → 17–30% WR (dead range)
ELITE_WINDOW_START = (9, 30)         # v3.7.5: inclusive ET (hour, minute)
ELITE_WINDOW_END = (10, 0)           # v3.7.5: inclusive ET; 10:00–11:00 slot = 12.5% WR
# Master switch: apply the v3.7.5 tightening. Set False to fall back to the
# legacy cat=D + rvol≥2 ELITE rule (still ex-75.8%-claim).
ELITE_TIGHTEN_V375 = True


def _elite_category(signal) -> str:
    """Post-hoc category from leadership label (matches notifier._signal_category).
    'D' = no sector-leadership classification (idiosyncratic breakout group)."""
    lbl = ((signal.get("leadership") or {}).get("label") or "").upper()
    if lbl in ("LEADER", "FOLLOWER", "LAGGARD", "SOLO_MOVER"):
        return {"LEADER": "A", "FOLLOWER": "B", "LAGGARD": "C", "SOLO_MOVER": "C"}.get(lbl, "D")
    return "D"


def _elite_rsi(signal):
    """RSI lives at signal['indicators']['rsi']; fall back to top-level 'rsi'."""
    v = (signal.get("indicators") or {}).get("rsi")
    if v is None:
        v = signal.get("rsi")
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _elite_stop_pct(signal):
    """Stop distance as % of entry = risk_pct on the trade levels dict."""
    trade = signal.get("trade") or {}
    v = trade.get("risk_pct")
    if v is None:
        try:
            entry = float(trade.get("entry"))
            stop = float(trade.get("stop_loss"))
            v = abs(entry - stop) / entry * 100 if entry else None
        except (TypeError, ValueError, ZeroDivisionError):
            v = None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _signal_in_window(signal, start, end) -> bool:
    """True if the signal timestamp (ET ISO) falls in [start, end] inclusive,
    where start/end are (hour, minute) ET tuples. Shared by the ELITE and
    TRADEABLE (v3.8.0) window gates."""
    ts = signal.get("timestamp") or ""
    hh = mm = None
    try:
        # ISO like '2026-07-06T09:35:00-04:00' — grab HH:MM after the 'T'
        t = ts.split("T", 1)[1]
        hh, mm = int(t[0:2]), int(t[3:5])
    except (IndexError, ValueError):
        # Fall back to batch_time 'HH:MM' if present
        bt = signal.get("batch_time") or ""
        try:
            hh, mm = int(bt[0:2]), int(bt[3:5])
        except (IndexError, ValueError):
            return False
    cur = hh * 60 + mm
    lo = start[0] * 60 + start[1]
    hi = end[0] * 60 + end[1]
    return lo <= cur <= hi


def _elite_in_window(signal) -> bool:
    """True if the signal timestamp falls in the ELITE entry window."""
    return _signal_in_window(signal, ELITE_WINDOW_START, ELITE_WINDOW_END)


def is_elite(signal) -> bool:
    """SINGLE SOURCE OF TRUTH for the ELITE flag. All consumers (scanner sort,
    notifier styling, templates via persisted signal['elite']) route here.

    Legacy base rule: STRONG + category=='D' + rvol >= ELITE_MIN_RVOL.
    v3.7.5 (ELITE_TIGHTEN_V375): additionally require rvol < ELITE_MAX_RVOL,
    rsi >= ELITE_MIN_RSI, entry in the 09:30–10:00 window, and
    stop_distance_pct >= ELITE_MIN_STOP_PCT.
    """
    if not ELITE_ENABLED:
        return False
    if not bool(signal.get("strong_signal")):
        return False
    if _elite_category(signal) != ELITE_REQUIRES_CATEGORY:
        return False
    try:
        rvol = float(signal.get("rvol") or 0)
    except (TypeError, ValueError):
        return False
    if rvol < ELITE_MIN_RVOL:
        return False

    if not ELITE_TIGHTEN_V375:
        return True

    # ── v3.7.5 precision gates ──
    if rvol >= ELITE_MAX_RVOL:
        return False
    rsi = _elite_rsi(signal)
    if rsi is None or rsi < ELITE_MIN_RSI:
        return False
    if not _elite_in_window(signal):
        return False
    stop_pct = _elite_stop_pct(signal)
    if stop_pct is None or stop_pct < ELITE_MIN_STOP_PCT:
        return False
    return True


# ── TRADEABLE tier (v3.8.0) ────────────────────────────────────
# The "should a human act on this pick at all" flag. Source: 52-day live
# analysis (research_findings_live52d_elite_precision.md, 2026-07-06),
# 4,399 truly-live first-appearance picks 2026-05-12 → 07-06:
#   • non-STRONG picks:            avgR −0.029, totR −100.4  (n=3,458)
#   • STRONG 09:30–10:00:          avgR +0.285, 25.8% 2.5R-hit (n=577)
#   • STRONG 10:00–11:00:          avgR −0.123,  6.8% 2.5R-hit (n=221) ← actively bad
# So TRADEABLE = STRONG AND entry inside [09:30, 10:00] ET (inclusive).
# ELITE is a strict subset of TRADEABLE (same window, STRONG required).
# This is a DISPLAY/NOTIFY tier, not an emission gate — non-tradeable
# picks still emit (operator preference: see everything, act on the tier).
TRADEABLE_ENABLED = True
TRADEABLE_WINDOW_START = (9, 30)     # inclusive ET (hour, minute)
TRADEABLE_WINDOW_END = (10, 0)       # inclusive ET


def is_tradeable(signal) -> bool:
    """SINGLE SOURCE OF TRUTH for the TRADEABLE flag (v3.8.0).
    STRONG + entry inside the 09:30–10:00 ET window. All consumers
    (scanner sort, templates via persisted signal['tradeable']) route here."""
    if not TRADEABLE_ENABLED:
        return False
    if not bool(signal.get("strong_signal")):
        return False
    return _signal_in_window(signal, TRADEABLE_WINDOW_START, TRADEABLE_WINDOW_END)


# ── Anti-extension shadow badge (v3.8.0) ───────────────────────
# SHADOW ONLY — no gating, no scoring effect. Persisted per pick so the
# dashboard can warn on extended entries and so ~4 weeks of live data can
# confirm before any promotion to a gate.
# Source: same 52-day analysis, bar-level features on 941 live STRONG picks
# (verified look-ahead-safe by independent recomputation):
#   • range_pos (position in running day range) 0.5–0.75: 34.9% 2.5R-hit,
#     +0.55 avgR vs 0.9–1.0 (buying the running high): 14.9%, +0.06 avgR.
#   • consec_green ≥ 3 at signal (within the 09:30–10:00 window):
#     12.5% hit, −0.088 avgR vs ≤2: 26.7–33.3% hit, +0.30/+0.60 avgR.
#   • Entries above the opening-range high: −0.06 avgR vs +0.39 inside.
# CAVEAT (from adversarial verification): the residual effect inside the
# proper catD+window control is modest (+1.3–6 hit-points); features were
# selected from ~9 candidates on one regime window. Hence: shadow, not gate.
ANTIEXT_MAX_CONSEC_GREEN = 2         # >2 green closed bars at signal = extended
ANTIEXT_MAX_RANGE_POS = 0.9          # >0.9 of running day range = extended


def is_anti_ext(signal):
    """Tri-state anti-extension check (v3.8.0). Reads the look-ahead-safe
    extension features the scanner attaches at signal['extension']:
        consec_green  — consecutive green closed RTH bars ending at the
                        STRONG reference bar
        range_pos     — ref-bar close position in the running RTH day range
                        (0=session low so far, 1=session high so far)
    Returns True  (pullback-quality entry: cg ≤ 2 and range_pos ≤ 0.9),
            False (extended entry — the historically losing shape),
            None  (features unavailable — pre-v3.8.0 pick or compute failed)."""
    ext = signal.get("extension") or {}
    cg = ext.get("consec_green")
    rp = ext.get("range_pos")
    if cg is None and rp is None:
        return None
    if cg is not None and cg > ANTIEXT_MAX_CONSEC_GREEN:
        return False
    if rp is not None and rp > ANTIEXT_MAX_RANGE_POS:
        return False
    return True


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

# v3.7.0: Mid-cap expansion — 80 high-liquidity names that scanner historically
# missed but which produced clean 2.5R moves in the 13-day research window
# (Apr 22 – May 8 2026). Ranked by 30-day average dollar-volume; range
# $210M–$2.5B/day ADV. Median price ~$105.
# Source: research/midcap_top80.json
MIDCAP_EXTENDED = [
    'ASML', 'VRT', 'RKLB', 'BE', 'BSX', 'TER', 'SLB', 'SATS',
    'CEG', 'DVN', 'PYPL', 'MPWR', 'TJX', 'HAL', 'ON', 'FANG',
    'WDAY', 'MCHP', 'VST', 'PWR', 'BKR', 'PL', 'CRH', 'HWM',
    'EQT', 'FTNT', 'TEAM', 'EBAY', 'HOLX', 'ULTA', 'URI', 'DOCN',
    'TTD', 'CFLT', 'DHI', 'PCG', 'EW', 'FN', 'STM', 'ZS',
    'EXC', 'APA', 'FSLR', 'XYZ', 'SMR', 'INSM', 'ETR', 'HUBS',
    'VMC', 'MRNA', 'CTRA', 'FIVE', 'LEN', 'ENTG', 'OKTA', 'PINS',
    'IDXX', 'MTZ', 'WAT', 'NVT', 'MLM', 'AFRM', 'DT', 'EME',
    'HUBB', 'BBY', 'AVAV', 'ROKU', 'WSM', 'W', 'PHM', 'AR',
    'DKS', 'OVV', 'TLN', 'BURL', 'DOCU', 'LII', 'ENPH', 'CHWY',
]

# Full scan universe = core + extended + midcap (deduped at runtime)
def get_full_universe() -> list[str]:
    """Return deduplicated full scan universe."""
    seen = set()
    result = []
    for t in SP500_LIQUID + HIGH_BETA_EXTENDED + MIDCAP_EXTENDED:
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
#   SOLO_MOVER: ticker% > SPY% but sector < SPY (counter-trend) → 0 (tightened v3.3.1)
#   FOLLOWER:   ticker% > SPY% but below sector → 0
#   LAGGARD:    ticker% < sector% → -10
# Score adjustment applied ONLY when LEADER_FILTER_MODE == "score".
# In "moderate" mode (v3.3.2 default) leadership becomes a HARD GATE:
# only LEADER and SOLO_MOVER are admitted; score_adjustment is skipped.
SECTOR_LEADER_BOOST = 10
SECTOR_SOLO_BOOST = 0
SECTOR_LAGGARD_PENALTY = -10

# v3.3.2 — Leadership as hard filter
# 20-day backtest (Mar 23 → Apr 17, 2026) showed the +10 LEADER boost
# was letting marginal-technical leaders through the min-score gate
# during ELEVATED/HIGH VIX regimes, where they bled ~$16k. Switching
# LEADER + SOLO_MOVER to a hard filter tightens that leak:
#   20d "score" mode:    337 trades / 18.9% WR / $15,184 / PF 1.09
#   20d "moderate" mode: 126 trades / 26.2% WR / $15,086 / PF 1.24
# Same P&L, half the churn, much cleaner regime interaction.
#
# Options:
#   "display"    — (v3.4.2 default) no hard gate; all labels emit as signals,
#                   UI groups by leader_tier (primary/secondary/weak).
#                   Rationale: 0-signal days were too common because the
#                   gate assumed backtest-quality data; user prefers to
#                   see everything and decide manually on Fidelity.
#   "moderate"   — v3.3.2 default: only LEADER + SOLO_MOVER admitted
#   "strict"     — only LEADER admitted (best quality, fewest trades)
#   "permissive" — LEADER + SOLO + FOLLOWER (blocks LAGGARD + UNKNOWN)
#   "score"      — legacy v3.3 behaviour: no hard gate, boosts still apply
LEADER_FILTER_MODE = "display"

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
# v3.8.0 re-confirmed on 52-day live data: appearance-2 picks ran
# 10.8% 2.5R-hit / −0.009 avgR (n=343) — dead money. Keep suppressed.
# Also confirmed: only 9.6% of stopped-out STRONG losers ever printed
# their target later the same day — a stopped pick is a wrong selection,
# not a mistimed entry, so no "wait for a better bar" logic is warranted.
SUPPRESS_REENTRIES = True

# ── Server ────────────────────────────────────────────────────
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", 8000))
