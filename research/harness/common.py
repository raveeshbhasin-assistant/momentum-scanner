"""Shared paths, time helpers and small utilities for the research harness.

Conventions (see README.md):
  * All bar timestamps are tz-aware America/New_York and mark the bar START.
    A 5-minute bar stamped T covers [T, T+5min) and is "closed" at T+5min.
  * RTH = 09:30 <= t < 16:00 ET (78 five-minute bars on a full session).
  * Session date = the ET calendar date of the bar.
"""
from __future__ import annotations

import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

HARNESS_DIR = Path(__file__).resolve().parent
RESEARCH_DIR = HARNESS_DIR.parent
SP = RESEARCH_DIR.parent                       # scratchpad root
BARS5M_DIR = SP / "bars5m"
BARS1D_DIR = SP / "bars1d"
CACHE_DIR = HARNESS_DIR / "cache"
PICKS_MASTER = RESEARCH_DIR / "picks_master.csv"

ET = ZoneInfo("America/New_York")
BAR_MINUTES = 5
BAR_TD = pd.Timedelta(minutes=BAR_MINUTES)
RTH_START_MIN = 9 * 60 + 30     # 570
RTH_END_MIN = 16 * 60           # 960 (exclusive)

ETF_TICKERS = ["SPY", "QQQ", "IWM", "DIA", "XLK", "XLF", "XLE", "XLV", "XLY", "XLP",
               "XLI", "XLB", "XLU", "XLC", "XLRE", "SMH", "ARKK", "TLT", "UUP", "GLD",
               "USO", "BITO"]
INDEX_TICKERS = ["_VIX", "_VIX9D", "_TNX"]
OHLCV = ["Open", "High", "Low", "Close", "Volume"]

# ── logging ──────────────────────────────────────────────────────────
logger = logging.getLogger("harness")
if not logger.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(asctime)s harness %(levelname)s %(message)s", "%H:%M:%S"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)
    logger.propagate = False


class Progress:
    """Tiny progress logger for long loops: logs every `every` items or `secs` seconds."""

    def __init__(self, total: int, label: str = "", every: int = 25, secs: float = 15.0):
        self.total, self.label, self.every, self.secs = total, label, every, secs
        self.t0 = self.t_last = time.time()
        self.n = 0

    def step(self, k: int = 1, extra: str = ""):
        self.n += k
        now = time.time()
        if self.n % self.every == 0 or self.n == self.total or now - self.t_last >= self.secs:
            self.t_last = now
            el = now - self.t0
            rate = self.n / el if el > 0 else 0
            eta = (self.total - self.n) / rate if rate > 0 else float("nan")
            logger.info(f"{self.label} {self.n}/{self.total} elapsed {el:.0f}s eta {eta:.0f}s {extra}")


# ── time helpers ─────────────────────────────────────────────────────
def minutes_of_day(index: pd.DatetimeIndex) -> np.ndarray:
    """Minutes since midnight ET for each timestamp (int array)."""
    idx = pd.DatetimeIndex(index)
    return (idx.hour * 60 + idx.minute).to_numpy()


def rth_mask(index: pd.DatetimeIndex) -> np.ndarray:
    m = minutes_of_day(index)
    return (m >= RTH_START_MIN) & (m < RTH_END_MIN)


def premarket_mask(index: pd.DatetimeIndex) -> np.ndarray:
    return minutes_of_day(index) < RTH_START_MIN


def afterhours_mask(index: pd.DatetimeIndex) -> np.ndarray:
    return minutes_of_day(index) >= RTH_END_MIN


def hhmm_to_minutes(hhmm: str) -> int:
    h, m = hhmm.strip().split(":")[:2]
    return int(h) * 60 + int(m)


def to_date(d) -> date:
    """Coerce str / Timestamp / datetime / date to datetime.date."""
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    return pd.Timestamp(d).date()


def make_ts(d, hhmm: str) -> pd.Timestamp:
    """Build a tz-aware ET timestamp from a date and 'HH:MM' (or 'HH:MM:SS')."""
    d = to_date(d)
    parts = [int(x) for x in hhmm.strip().split(":")]
    h, m = parts[0], parts[1]
    s = parts[2] if len(parts) > 2 else 0
    return pd.Timestamp(year=d.year, month=d.month, day=d.day, hour=h, minute=m, second=s, tz=ET)


def as_et(ts) -> pd.Timestamp:
    """Coerce to a tz-aware ET Timestamp (naive input is assumed to be ET)."""
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize(ET)
    return t.tz_convert(ET)


def session_date_array(index: pd.DatetimeIndex) -> np.ndarray:
    """ET calendar date per bar as an object array of datetime.date (fast path via normalize)."""
    idx = pd.DatetimeIndex(index)
    return idx.tz_convert(ET).date
