"""Bar loading, session slicing, scanner-mirroring indicators/signals, daily context, market state, breadth.

All DataFrames are indexed by tz-aware America/New_York bar-START timestamps (5m) or by session date (1d).

Look-ahead rule used throughout: a bar stamped T is only known at T+5min. Every "as of at_ts" helper
(reference_bar, market_state, breadth) uses the last bar with start <= at_ts - 5min.
"""
from __future__ import annotations

import os
from datetime import date
from functools import lru_cache
from typing import Iterable

import numpy as np
import pandas as pd

from .common import (BAR_TD, BARS1D_DIR, BARS5M_DIR, CACHE_DIR, ET, ETF_TICKERS, OHLCV,
                     RTH_START_MIN, Progress, afterhours_mask, as_et, logger, make_ts,
                     minutes_of_day, premarket_mask, rth_mask, session_date_array, to_date)

_STRONG_EPS = 0.9995          # scanner._STRONG_EPS: 0.05% tolerance for new_hod
_MIN_FILE_BYTES = 100         # files smaller than this are the known-empty tickers

_CACHE_5M: dict[str, pd.DataFrame | None] = {}
_CACHE_1D: dict[str, pd.DataFrame | None] = {}
_CACHE_IND: dict[str, pd.DataFrame | None] = {}
_CACHE_CTX: dict[str, tuple | None] = {}
_CACHE_MS: dict[str, pd.DataFrame] = {}


# ═══════════════════════════════════════════════════════════════════
#  Loading
# ═══════════════════════════════════════════════════════════════════
def available_tickers(kind: str = "5m", include_etfs: bool = True, include_index: bool = True) -> list[str]:
    """Tickers with a non-empty CSV in bars5m/ (or bars1d/). Sorted."""
    d = BARS5M_DIR if kind == "5m" else BARS1D_DIR
    out = []
    for f in sorted(os.listdir(d)):
        if not f.endswith(".csv") or os.path.getsize(d / f) < _MIN_FILE_BYTES:
            continue
        t = f[:-4]
        if t in ETF_TICKERS and not include_etfs:
            continue
        if t.startswith("_") and not include_index:
            continue
        out.append(t)
    return out


def stock_tickers() -> list[str]:
    """Stocks only (no ETFs, no _index series) with a non-empty 5m file."""
    return available_tickers("5m", include_etfs=False, include_index=False)


def empty_tickers() -> list[str]:
    d = BARS5M_DIR
    return sorted(f[:-4] for f in os.listdir(d) if f.endswith(".csv") and os.path.getsize(d / f) < _MIN_FILE_BYTES)


def _read_5m_csv(path, float32: bool) -> pd.DataFrame | None:
    if os.path.getsize(path) < _MIN_FILE_BYTES:
        return None
    df = pd.read_csv(path, index_col=0)
    if df.empty:
        return None
    # pandas 3 parses these strings at microsecond resolution; force ns so that asi8 / Timestamp.value
    # (always ns) and every integer-nanosecond comparison in sim.py agree.
    df.index = pd.to_datetime(df.index, utc=True).tz_convert(ET).as_unit("ns")
    df.index.name = "ts"
    df = df[OHLCV].astype("float32" if float32 else "float64")
    df = df.dropna(subset=["Close"])
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def load_5m(tickers: Iterable[str] | None = None, *, float32: bool = False, verbose: bool = False) -> dict[str, pd.DataFrame]:
    """Load 5-minute OHLCV (pre-market + RTH + after-hours) for `tickers` (default: all non-empty files).

    Cached in-process; the same DataFrame objects are returned on repeat calls (do not mutate them —
    copy first). Empty/missing tickers are skipped with a warning and simply absent from the result.
    """
    if tickers is None:
        tickers = available_tickers("5m")
    tickers = list(tickers)
    missing = [t for t in tickers if t not in _CACHE_5M]
    if missing:
        prog = Progress(len(missing), "load_5m", every=50, secs=20) if verbose else None
        for t in missing:
            p = BARS5M_DIR / f"{t}.csv"
            if not p.exists():
                logger.warning(f"load_5m: no 5m file for {t}")
                _CACHE_5M[t] = None
            else:
                df = _read_5m_csv(p, float32)
                if df is None:
                    logger.warning(f"load_5m: empty 5m file for {t} — skipped")
                _CACHE_5M[t] = df
            if prog:
                prog.step()
    return {t: _CACHE_5M[t] for t in tickers if _CACHE_5M.get(t) is not None}


def load_1d(tickers: Iterable[str] | None = None) -> dict[str, pd.DataFrame]:
    """Load daily OHLCV keyed by SESSION DATE (tz-naive midnight DatetimeIndex named 'date').

    GOTCHA: the bars1d CSVs stamp each session at 00:00 UTC, which renders as the PREVIOUS evening in
    ET ("2026-09-10 20:00-04:00" is the 2026-09-11 session). We therefore take the UTC date. A test
    verifies the alignment against the 5m 15:55 closes.
    """
    if tickers is None:
        tickers = available_tickers("1d")
    out = {}
    for t in tickers:
        if t not in _CACHE_1D:
            p = BARS1D_DIR / f"{t}.csv"
            if not p.exists() or os.path.getsize(p) < _MIN_FILE_BYTES:
                _CACHE_1D[t] = None
            else:
                df = pd.read_csv(p, index_col=0)
                idx = pd.to_datetime(df.index, utc=True)
                df.index = pd.DatetimeIndex(idx.tz_localize(None).normalize(), name="date").as_unit("ns")
                df = df[OHLCV].astype("float64").dropna(subset=["Close"])
                df = df[~df.index.duplicated(keep="last")].sort_index()
                _CACHE_1D[t] = df
        if _CACHE_1D.get(t) is not None:
            out[t] = _CACHE_1D[t]
    return out


# ═══════════════════════════════════════════════════════════════════
#  Session slicing
# ═══════════════════════════════════════════════════════════════════
def session_dates(df: pd.DataFrame) -> list[date]:
    return sorted(set(session_date_array(df.index)))


def session_frames(df: pd.DataFrame) -> dict[date, pd.DataFrame]:
    """Split a multi-day frame into {ET date: frame of that session (pre + RTH + post bars)}."""
    out: dict[date, pd.DataFrame] = {}
    if len(df) == 0:
        return out
    dates = session_date_array(df.index)
    change = np.flatnonzero(dates[1:] != dates[:-1]) + 1
    starts = np.concatenate([[0], change])
    ends = np.concatenate([change, [len(df)]])
    for s, e in zip(starts, ends):
        out[dates[s]] = df.iloc[s:e]
    return out


def session_frame(df: pd.DataFrame, d) -> pd.DataFrame:
    """Bars of one ET session date (pre + RTH + post). Empty frame if the date is absent."""
    d = to_date(d)
    lo = pd.Timestamp(d, tz=ET)
    hi = lo + pd.Timedelta(days=1)
    return df.loc[(df.index >= lo) & (df.index < hi)]


def rth_slice(df_day: pd.DataFrame) -> pd.DataFrame:
    return df_day.loc[rth_mask(df_day.index)]


def premarket_slice(df_day: pd.DataFrame) -> pd.DataFrame:
    return df_day.loc[premarket_mask(df_day.index)]


def afterhours_slice(df_day: pd.DataFrame) -> pd.DataFrame:
    return df_day.loc[afterhours_mask(df_day.index)]


# ═══════════════════════════════════════════════════════════════════
#  Indicators (exact mirror of scanner.calculate_indicators, which uses the `ta` library)
# ═══════════════════════════════════════════════════════════════════
def ema(s: pd.Series, n: int) -> pd.Series:
    """ta.trend.EMAIndicator: ewm(span=n, min_periods=n, adjust=False)."""
    return s.ewm(span=n, min_periods=n, adjust=False).mean()


def rsi(close: pd.Series, n: int = 14) -> pd.Series:
    """ta.momentum.RSIIndicator (Wilder smoothing via ewm(alpha=1/n, adjust=False))."""
    diff = close.diff(1)
    up = diff.where(diff > 0, 0.0)
    dn = -diff.where(diff < 0, 0.0)
    emaup = up.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    emadn = dn.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    rs = emaup / emadn
    out = np.where(emadn == 0, 100.0, 100.0 - 100.0 / (1.0 + rs))
    return pd.Series(out, index=close.index)


def true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    pc = close.shift(1)
    return pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)


def atr_wilder(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    """ta.volatility.AverageTrueRange: seed = mean(TR[0:n]) at position n-1, then
    atr[i] = (atr[i-1]*(n-1) + TR[i]) / n. Positions < n-1 are NaN here (ta returns 0 there)."""
    tr = true_range(high, low, close)
    if len(tr) < n:
        return pd.Series(np.nan, index=close.index)
    seed = tr.iloc[:n].mean()
    x = tr.copy()
    x.iloc[: n - 1] = np.nan
    x.iloc[n - 1] = seed
    out = x.ewm(alpha=1 / n, adjust=False, min_periods=1).mean()
    out.iloc[: n - 1] = np.nan
    return out


def session_vwap(df: pd.DataFrame, start_min: int | None = None) -> pd.Series:
    """Per-session VWAP = cumsum(typical price × volume) / cumsum(volume), grouped by ET date.

    start_min=None mirrors the scanner (the cumsum starts at the first bar of the session, i.e.
    pre-market bars are INCLUDED; bars with zero cumulative volume fall back to typical price).
    start_min=570 gives the RTH-only variant (NaN before 09:30).
    """
    tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
    vol = df["Volume"]
    keys = session_date_array(df.index)
    if start_min is not None:
        m = minutes_of_day(df.index) >= start_min
        tp = tp.where(m)
        vol = vol.where(m)
    tpv = (tp * vol).groupby(keys).cumsum()
    cv = vol.groupby(keys).cumsum()
    out = tpv / cv
    return out.where(cv > 0, tp)


def add_indicators(df: pd.DataFrame, *, rth_variants: bool = True) -> pd.DataFrame:
    """Return a copy of a multi-day 5m frame with the scanner's indicator columns added.

    Scanner-identical columns (computed over ALL bars in the frame, pre/post included — that is what
    the live scanner does on its 5-day prepost frame):
        EMA_9, EMA_21, EMA_50, RSI, MACD_12_26_9, MACDs_12_26_9, MACDh_12_26_9,
        BBU_20_2.0, BBL_20_2.0, BBM_20_2.0, ATR, VWAP (per-session, from the first pre-market bar).
    RTH-only variants (rth_variants=True):
        ATR_rth  — ATR14 over the RTH bars only (previous close = prior RTH bar, so the 09:30 bar's
                   true range spans the overnight gap); NaN on non-RTH bars.
        VWAP_rth — per-session VWAP accumulated from 09:30; NaN on pre-market bars.
    Differences: with pre-market bars in the frame, ATR (scanner) is dragged DOWN by thin overnight
    bars and EMAs/RSI/MACD carry pre-market prices. Pass an RTH-only frame to get all-RTH indicators.
    Warm-up: values are NaN until enough bars exist (the live 5-day frame starts fresh each day, so its
    first ~50 bars of EMA_50 differ slightly from a 60-day frame; negligible after a few sessions).
    """
    out = df.copy()
    c, h, l = out["Close"], out["High"], out["Low"]
    out["EMA_9"] = ema(c, 9)
    out["EMA_21"] = ema(c, 21)
    out["EMA_50"] = ema(c, 50)
    out["RSI"] = rsi(c, 14)
    ef, es = ema(c, 12), ema(c, 26)
    macd = ef - es
    sig = ema(macd, 9)
    out["MACD_12_26_9"] = macd
    out["MACDs_12_26_9"] = sig
    out["MACDh_12_26_9"] = macd - sig
    mavg = c.rolling(20, min_periods=20).mean()
    mstd = c.rolling(20, min_periods=20).std(ddof=0)
    out["BBU_20_2.0"] = mavg + 2.0 * mstd
    out["BBL_20_2.0"] = mavg - 2.0 * mstd
    out["BBM_20_2.0"] = mavg
    out["ATR"] = atr_wilder(h, l, c, 14)
    out["VWAP"] = session_vwap(out, None)
    if rth_variants:
        m = rth_mask(out.index)
        r = out.loc[m]
        out["ATR_rth"] = atr_wilder(r["High"], r["Low"], r["Close"], 14).reindex(out.index)
        out["VWAP_rth"] = session_vwap(out, RTH_START_MIN)
    return out


def get_indicators(ticker: str) -> pd.DataFrame | None:
    """Cached add_indicators(load_5m([ticker])[ticker]). None if the ticker has no data."""
    if ticker not in _CACHE_IND:
        d = load_5m([ticker])
        _CACHE_IND[ticker] = add_indicators(d[ticker]) if ticker in d else None
    return _CACHE_IND[ticker]


def rvol_scanner(df: pd.DataFrame, lookback_days: int = 4) -> pd.Series:
    """Time-of-day relative volume mirroring scanner.calculate_rvol, for EVERY bar at once.

    For a bar at (date D, time HH:MM): baseline = pooled mean volume of bars on the previous
    `lookback_days` sessions whose hour == HH and |minute - MM| <= 10 (the scanner's quirky window —
    e.g. 09:30's baseline pools 09:20/09:25 pre-market bars with 09:30/09:35/09:40). The live scanner
    had a 5-day frame, i.e. up to 4 prior sessions; the baseline excludes the current session. Bars with
    no baseline get NaN (the live code falls back to the frame-wide mean; we do not).
    """
    keys = session_date_array(df.index)
    mins = minutes_of_day(df.index)
    wide = pd.DataFrame({"d": keys, "m": mins, "v": df["Volume"].to_numpy()}).pivot_table(
        index="d", columns="m", values="v", aggfunc="sum")
    cols = wide.columns.to_numpy()
    present = wide.notna().to_numpy().astype(float)
    vals = wide.fillna(0.0).to_numpy()
    hour = cols // 60
    nb = ((hour[:, None] == hour[None, :]) & (np.abs(cols[:, None] - cols[None, :]) <= 10)).astype(float)
    S = pd.DataFrame(vals @ nb.T, index=wide.index, columns=cols)      # per (date, slot): pooled sum over neighbour slots
    C = pd.DataFrame(present @ nb.T, index=wide.index, columns=cols)
    S_prior = S.shift(1).rolling(lookback_days, min_periods=1).sum()
    C_prior = C.shift(1).rolling(lookback_days, min_periods=1).sum()
    base = S_prior / C_prior.replace(0, np.nan)
    lookup = base.stack()
    b = lookup.reindex(pd.MultiIndex.from_arrays([keys, mins])).to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        out = df["Volume"].to_numpy() / b
    out[~np.isfinite(out)] = np.nan          # zero baseline (thin pre-market slots) -> undefined
    return pd.Series(out, index=df.index)


# ═══════════════════════════════════════════════════════════════════
#  Reference bar, STRONG components, extension features (mirror scanner.py)
# ═══════════════════════════════════════════════════════════════════
def reference_bar(df_day: pd.DataFrame, at_time) -> int | None:
    """Positional index into df_day of the most-recently-CLOSED RTH bar as of `at_time`
    (bar start <= at_time - 5min and 09:30 <= start < 16:00). None if no RTH bar has closed.
    Mirrors scanner._most_recent_closed_rth_pos. `at_time` may be a Timestamp or 'HH:MM' (date from df_day).
    """
    if df_day is None or len(df_day) == 0:
        return None
    if isinstance(at_time, str):
        at_time = make_ts(session_date_array(df_day.index[:1])[0], at_time)
    at_time = as_et(at_time)
    idx = df_day.index
    ok = rth_mask(idx) & ((idx + BAR_TD) <= at_time)
    if not ok.any():
        return None
    return int(np.flatnonzero(ok)[-1])


def strong_components(df_day: pd.DataFrame, pos: int) -> dict:
    """Four STRONG conditions on df_day.iloc[pos] (mirrors scanner._evaluate_strong_on_bar).

    df_day must be ONE session's bars (pre-market + RTH [+ post]) in order, with a 'VWAP' column
    (scanner per-session VWAP); if VWAP is absent it is computed on the fly.
      bar_green    close > open
      above_vwap   close > VWAP
      new_hod      high >= max(high of today's bars through pos, INCLUDING pre-market) * 0.9995
      pm_high_hold close > max(pre-market highs)   (False when there are no pre-market bars)
    """
    out = {"bar_green": False, "above_vwap": False, "new_hod": False, "pm_high_hold": False,
           "strong": False, "bar_ts": None}
    if df_day is None or pos is None or pos < 0 or pos >= len(df_day):
        return out
    o = df_day["Open"].to_numpy(); h = df_day["High"].to_numpy(); c = df_day["Close"].to_numpy()
    vw = df_day["VWAP"].to_numpy() if "VWAP" in df_day.columns else session_vwap(df_day).to_numpy()
    out["bar_ts"] = df_day.index[pos]
    if np.isfinite(o[pos]) and np.isfinite(c[pos]):
        out["bar_green"] = bool(c[pos] > o[pos])
    if np.isfinite(c[pos]) and np.isfinite(vw[pos]):
        out["above_vwap"] = bool(c[pos] > vw[pos])
    sh = np.nanmax(h[: pos + 1])
    if np.isfinite(sh) and np.isfinite(h[pos]):
        out["new_hod"] = bool(h[pos] >= sh * _STRONG_EPS)
    pm = premarket_mask(df_day.index)
    if pm.any() and np.isfinite(c[pos]):
        out["pm_high_hold"] = bool(c[pos] > np.nanmax(h[pm]))
    out["strong"] = bool(out["bar_green"] and out["above_vwap"] and out["new_hod"] and out["pm_high_hold"])
    return out


def extension_features(df_day: pd.DataFrame, pos: int) -> dict:
    """consec_green / range_pos / above_orb_high on RTH bars through df_day.iloc[pos]
    (mirrors scanner.compute_extension_features; pos must point at an RTH bar, else all None)."""
    out = {"consec_green": None, "range_pos": None, "above_orb_high": None}
    if df_day is None or pos is None or pos < 0 or pos >= len(df_day):
        return out
    m = rth_mask(df_day.index)
    if not m[pos]:
        return out
    rpos = int(m[: pos + 1].sum()) - 1          # position within the RTH-only frame
    r = df_day.loc[m]
    o = r["Open"].to_numpy(); h = r["High"].to_numpy(); l = r["Low"].to_numpy(); c = r["Close"].to_numpy()
    cg = 0
    for j in range(rpos, -1, -1):
        if np.isfinite(c[j]) and np.isfinite(o[j]) and c[j] > o[j]:
            cg += 1
        else:
            break
    out["consec_green"] = cg
    hi = float(np.nanmax(h[: rpos + 1])); lo = float(np.nanmin(l[: rpos + 1]))
    if hi > lo and np.isfinite(c[rpos]):
        out["range_pos"] = round((float(c[rpos]) - lo) / (hi - lo), 3)
    if rpos >= 3 and np.isfinite(c[rpos]):
        out["above_orb_high"] = int(float(c[rpos]) > float(np.nanmax(h[:3])))
    return out


def strong_table(df_ind: pd.DataFrame) -> pd.DataFrame:
    """Vectorized STRONG components + extension features for EVERY bar of an indicator frame
    (one row per bar; extension features only on RTH bars, NaN elsewhere). Same definitions as the
    per-bar functions above (a test checks agreement); used by the all-bars builder."""
    idx = df_ind.index
    keys = session_date_array(idx)
    o = df_ind["Open"]; h = df_ind["High"]; c = df_ind["Close"]; vw = df_ind["VWAP"]
    run_high_all = h.groupby(keys).cummax()
    pm = premarket_mask(idx)
    pm_high_sess = h.where(pm).groupby(keys).transform("max")   # session pre-market high (NaN if none)
    out = pd.DataFrame(index=idx)
    out["bar_green"] = (c > o)
    out["above_vwap"] = (c > vw)
    out["new_hod"] = (h >= run_high_all * _STRONG_EPS)
    out["pm_high_hold"] = (c > pm_high_sess)                    # NaN comparison -> False
    out["strong"] = out[["bar_green", "above_vwap", "new_hod", "pm_high_hold"]].all(axis=1)
    m = rth_mask(idx)
    r = df_ind.loc[m]
    rk = keys[m]
    green = (r["Close"] > r["Open"]).astype(int)
    blocks = (green == 0).groupby(rk).cumsum()                 # new block after each red bar
    consec = green.groupby([rk, blocks.to_numpy()]).cumsum()
    rh = r["High"].groupby(rk).cummax(); rl = r["Low"].groupby(rk).cummin()
    rng = rh - rl
    range_pos = ((r["Close"] - rl) / rng.where(rng > 0)).round(3)
    nth = pd.Series(np.arange(len(r)), index=r.index).groupby(rk).cumcount()
    # ORB high = max high of the first three RTH bars; cummax leaves NaN after the window, so ffill it
    orb_high = r["High"].where(nth <= 2).groupby(rk).cummax().groupby(rk).ffill()
    above_orb = (r["Close"] > orb_high).astype(float).where(nth >= 3)
    out["consec_green"] = consec.reindex(idx)
    out["range_pos"] = range_pos.reindex(idx)
    out["above_orb_high"] = above_orb.reindex(idx)
    return out


# ═══════════════════════════════════════════════════════════════════
#  Daily context (through the PRIOR close, + today's open for the gap)
# ═══════════════════════════════════════════════════════════════════
@lru_cache(maxsize=None)
def _spy_daily_returns() -> pd.DataFrame:
    d = load_1d(["SPY"])["SPY"]
    c = d["Close"]
    return pd.DataFrame({"spy_ret_5d": c.pct_change(5) * 100, "spy_ret_20d": c.pct_change(20) * 100})


CONTEXT_FIELDS = ["ret_1d", "ret_5d", "ret_20d", "ret_60d", "dist_20d_high", "dist_52w_high", "dist_ma20",
                  "dist_ma50", "dist_ma200", "vol_20d", "atr14_daily_pct", "adv20_dollar", "prior_day_range_pct",
                  "prior_day_close_pos", "days_since_20d_high", "rs_vs_spy_5d", "rs_vs_spy_20d", "prev_close"]


def _raw_daily_features(d: pd.DataFrame) -> pd.DataFrame:
    """Features of each session AS OF ITS OWN CLOSE (unshifted)."""
    c, o, h, l, v = d["Close"], d["Open"], d["High"], d["Low"], d["Volume"]
    f = pd.DataFrame(index=d.index)
    f["ret_1d"] = c.pct_change(1) * 100
    f["ret_5d"] = c.pct_change(5) * 100
    f["ret_20d"] = c.pct_change(20) * 100
    f["ret_60d"] = c.pct_change(60) * 100
    f["dist_20d_high"] = (c / h.rolling(20, min_periods=10).max() - 1) * 100
    f["dist_52w_high"] = (c / h.rolling(252, min_periods=120).max() - 1) * 100
    f["dist_ma20"] = (c / c.rolling(20).mean() - 1) * 100
    f["dist_ma50"] = (c / c.rolling(50).mean() - 1) * 100
    f["dist_ma200"] = (c / c.rolling(200).mean() - 1) * 100
    f["vol_20d"] = c.pct_change().rolling(20).std() * np.sqrt(252) * 100
    f["atr14_daily_pct"] = atr_wilder(h, l, c, 14) / c * 100
    f["adv20_dollar"] = (c * v).rolling(20).mean()
    f["prior_day_range_pct"] = (h - l) / c * 100
    rng = h - l
    f["prior_day_close_pos"] = (c - l) / rng.where(rng > 0)
    f["days_since_20d_high"] = h.rolling(20, min_periods=1).apply(lambda x: len(x) - 1 - int(np.argmax(x)), raw=True)
    spy = _spy_daily_returns().reindex(d.index)
    f["rs_vs_spy_5d"] = f["ret_5d"] - spy["spy_ret_5d"]
    f["rs_vs_spy_20d"] = f["ret_20d"] - spy["spy_ret_20d"]
    f["prev_close"] = c
    return f


def daily_context_table(ticker: str) -> pd.DataFrame | None:
    """Per-session daily-context features for `ticker`, indexed by session date.

    Row for date D uses ONLY daily bars through D-1 (prior close), except gap_pct / today_open which
    use D's official open (known at 09:30:00). Units: all ret_*/dist_*/rs_*/vol/atr/gap/range fields
    are PERCENT (1.5 == 1.5%); prior_day_close_pos in [0,1]; adv20_dollar in dollars; vol_20d is the
    annualized std of daily returns in percent.
    Fields: ret_1d ret_5d ret_20d ret_60d dist_20d_high dist_52w_high dist_ma20 dist_ma50 dist_ma200
            vol_20d atr14_daily_pct adv20_dollar prior_day_range_pct prior_day_close_pos
            days_since_20d_high rs_vs_spy_5d rs_vs_spy_20d prev_close today_open gap_pct ticker
    """
    if ticker in _CACHE_CTX:
        return _CACHE_CTX[ticker][0] if _CACHE_CTX[ticker] else None
    dd = load_1d([ticker])
    if ticker not in dd:
        _CACHE_CTX[ticker] = None
        return None
    d = dd[ticker]
    raw = _raw_daily_features(d)
    f = raw.shift(1)
    f["today_open"] = d["Open"]
    f["gap_pct"] = (d["Open"] / f["prev_close"] - 1) * 100
    f["ticker"] = ticker
    _CACHE_CTX[ticker] = (f, raw)
    return f


def daily_context(ticker: str, d) -> dict:
    """Daily context dict for one ticker/date (see daily_context_table for fields/units).

    If the daily frame has no row for `d` (e.g. the daily cache lags), the "through prior close"
    fields come from the last daily row before `d`, and gap/today_open from the 5m 09:30 bar.
    Empty dict if the ticker has no daily data at all.
    """
    f = daily_context_table(ticker)
    if f is None:
        return {}
    ts = pd.Timestamp(to_date(d))
    if ts in f.index:
        row = f.loc[ts].to_dict()
    else:
        raw = _CACHE_CTX[ticker][1]
        prior = raw.loc[raw.index < ts]
        if prior.empty:
            return {}
        row = prior.iloc[-1].to_dict()
        row["today_open"] = np.nan
        row["gap_pct"] = np.nan
    if not np.isfinite(row.get("today_open", np.nan)):
        m5 = load_5m([ticker]).get(ticker)
        if m5 is not None:
            day = rth_slice(session_frame(m5, d))
            if len(day) and np.isfinite(row.get("prev_close", np.nan)):
                row["today_open"] = float(day["Open"].iloc[0])
                row["gap_pct"] = (row["today_open"] / row["prev_close"] - 1) * 100
    row["ticker"] = ticker
    row["date"] = to_date(d)
    return row


def daily_context_universe(tickers: Iterable[str] | None = None, dates: Iterable | None = None) -> pd.DataFrame:
    """Long table (one row per ticker × session date) of daily-context features, optionally restricted
    to `dates`. Default tickers = stock universe."""
    tickers = list(tickers) if tickers is not None else stock_tickers()
    ds = {to_date(x) for x in dates} if dates is not None else None
    frames = []
    for t in tickers:
        f = daily_context_table(t)
        if f is None:
            continue
        g = f.copy()
        g.insert(0, "date", g.index.date)
        if ds is not None:
            g = g[g["date"].isin(ds)]
        frames.append(g.reset_index(drop=True))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════
#  Market state (SPY/QQQ/IWM/_VIX) and breadth
# ═══════════════════════════════════════════════════════════════════
def _prev_close_map(ticker: str) -> pd.Series:
    """Prior-session official close keyed by session date (from bars1d, UTC-date aligned)."""
    d = load_1d([ticker]).get(ticker)
    if d is None:
        return pd.Series(dtype=float)
    s = d["Close"].shift(1)
    s.index = s.index.date
    return s


def _per_bar_day_stats(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """RTH-bar table for one symbol: ret_open_pct, ret_day_pct (vs prior official close), above_vwap, near_hod."""
    idx = df.index
    keys = session_date_array(idx)
    vwap = session_vwap(df)
    m = rth_mask(idx)
    r = df.loc[m]
    rk = keys[m]
    open_0930 = r["Open"].groupby(rk).transform("first")
    run_high = r["High"].groupby(rk).cummax()
    prev = pd.Series(rk, index=r.index).map(_prev_close_map(ticker)).astype(float)
    out = pd.DataFrame(index=r.index)
    out["ret_open_pct"] = (r["Close"] / open_0930 - 1) * 100
    out["ret_day_pct"] = (r["Close"] / prev - 1) * 100
    out["above_vwap"] = (r["Close"] > vwap.loc[m]).astype(float)
    out["near_hod"] = (r["Close"] >= run_high * 0.998).astype(float)
    return out


def market_state_table() -> pd.DataFrame:
    """Per-RTH-bar market state (indexed by bar START; a row is known at start+5min):
       {spy,qqq,iwm}_ret_open_pct  close vs that session's 09:30 open
       {spy,qqq,iwm}_ret_day_pct   close vs prior official close
       {spy,qqq,iwm}_above_vwap    close > session VWAP (scanner VWAP incl. pre-market)
       vix_level, vix_chg_open_pts, vix_chg_open_pct   VIX close vs its 09:30 bar open
       vix9d_level, vix_term (vix9d/vix - 1) when available
    Rows follow SPY's RTH grid.
    """
    if "table" in _CACHE_MS:
        return _CACHE_MS["table"]
    parts = []
    for t in ["SPY", "QQQ", "IWM"]:
        df = load_5m([t]).get(t)
        if df is None:
            continue
        st = _per_bar_day_stats(df, t)[["ret_open_pct", "ret_day_pct", "above_vwap"]]
        st.columns = [f"{t.lower()}_{c}" for c in st.columns]
        parts.append(st)
    vix = load_5m(["_VIX"]).get("_VIX")
    if vix is not None:
        keys = session_date_array(vix.index)
        m = rth_mask(vix.index)
        r = vix.loc[m]
        o0 = r["Open"].groupby(keys[m]).transform("first")
        v = pd.DataFrame(index=r.index)
        v["vix_level"] = r["Close"]
        v["vix_chg_open_pts"] = r["Close"] - o0
        v["vix_chg_open_pct"] = (r["Close"] / o0 - 1) * 100
        parts.append(v)
    v9 = load_5m(["_VIX9D"]).get("_VIX9D")
    if v9 is not None:
        r = v9.loc[rth_mask(v9.index)]
        parts.append(pd.DataFrame({"vix9d_level": r["Close"]}))
    tab = pd.concat(parts, axis=1).sort_index()
    if "vix9d_level" in tab and "vix_level" in tab:
        tab["vix_term"] = tab["vix9d_level"] / tab["vix_level"] - 1
    if "spy_ret_open_pct" in tab:
        tab = tab.loc[tab["spy_ret_open_pct"].notna()]
    tab.index.name = "bar_ts"
    _CACHE_MS["table"] = tab
    return tab


def _asof_row(table: pd.DataFrame, at_ts) -> pd.Series | None:
    """Row of the last bar that CLOSED at or before at_ts (start <= at_ts - 5min), same session only."""
    at_ts = as_et(at_ts)
    cutoff = at_ts - BAR_TD
    pos = table.index.searchsorted(cutoff, side="right") - 1
    if pos < 0:
        return None
    ts = table.index[pos]
    if ts.date() != at_ts.date():
        return None
    return table.iloc[pos]


def market_state(at_ts) -> dict:
    """Market state as of `at_ts` (last SPY/QQQ/IWM/VIX bar closed by then). {} if none yet today."""
    row = _asof_row(market_state_table(), at_ts)
    if row is None:
        return {}
    d = row.to_dict()
    d["bar_ts"] = row.name
    return d


def breadth_table(force: bool = False) -> pd.DataFrame:
    """Per-RTH-bar breadth across the stock universe (cached at cache/breadth_5m.csv.gz):
       share_above_vwap  close > session VWAP (scanner VWAP)
       share_up_day      close > prior official close
       share_near_hod    close >= 0.998 × running RTH high through this bar
       share_up_1pct     close / prior close - 1 > 1%
       n                 stocks contributing at that timestamp
    Indexed by bar START; a row is known at start+5min. First call builds it (~30-60 s).
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / "breadth_5m.csv.gz"
    if "breadth" in _CACHE_MS and not force:
        return _CACHE_MS["breadth"]
    if path.exists() and not force:
        b = pd.read_csv(path, index_col=0)
        b.index = pd.to_datetime(b.index, utc=True).tz_convert(ET).as_unit("ns")
        b.index.name = "bar_ts"
        _CACHE_MS["breadth"] = b
        return b
    tickers = stock_tickers()
    bars = load_5m(tickers, verbose=True)
    cols = {"above_vwap": [], "up_day": [], "near_hod": [], "up_1pct": []}
    prog = Progress(len(bars), "breadth", every=50)
    for t, df in bars.items():
        st = _per_bar_day_stats(df, t)
        rd = st["ret_day_pct"]
        cols["above_vwap"].append(st["above_vwap"].rename(t))
        cols["near_hod"].append(st["near_hod"].rename(t))
        cols["up_day"].append((rd > 0).astype(float).where(rd.notna()).rename(t))
        cols["up_1pct"].append((rd > 1.0).astype(float).where(rd.notna()).rename(t))
        prog.step()
    out = pd.DataFrame()
    for k, lst in cols.items():
        w = pd.concat(lst, axis=1)
        out[f"share_{k}"] = w.mean(axis=1)
        if k == "above_vwap":
            out["n"] = w.notna().sum(axis=1)
    out = out.sort_index()
    out = out.loc[rth_mask(out.index)]
    out.index.name = "bar_ts"
    out.to_csv(path, compression="gzip")
    _CACHE_MS["breadth"] = out
    return out


def breadth(at_ts) -> dict:
    """Breadth as of `at_ts` (last bar closed by then; same session). {} if none yet."""
    row = _asof_row(breadth_table(), at_ts)
    if row is None:
        return {}
    d = row.to_dict()
    d["bar_ts"] = row.name
    return d
