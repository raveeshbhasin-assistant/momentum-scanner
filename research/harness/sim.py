"""Canonical barrier simulator for long intraday trades on 5-minute RTH bars.

Conventions (see README.md):
  * Bar timestamps are bar STARTS; a bar stamped T covers [T, T+5min).
  * `signal_ts` is the instant the signal became known. For a live scanner pick that is
    `date + batch_time` (the scan ran a few seconds into that bar).
  * Stop and target are ABSOLUTE prices fixed at signal time. R is measured in units of the ORIGINAL
    risk (entry_price - stop); R_own uses the actual fill risk (entry_fill - stop).
  * Costs: `cost_bps_per_side` is applied to both fills (buy at price*(1+c), sell at price*(1-c)).
  * Entry modes (i0 = first bar evaluated for barriers):
      'signal_price' fill = entry_price at signal_ts; i0 = first bar with start >= signal_ts   (live resolver)
      'next_open'    fill = Open of the first bar with start >  signal_ts; i0 = that bar        (default)
      'first_open'   fill = Open of the first bar with start >= signal_ts; i0 = that bar
                     (only look-ahead-safe when signal_ts is a bar-close instant of a CLOSED bar)
      'next_close'   fill = Close of the bar in progress at signal_ts (start <= signal_ts < start+5m);
                     i0 = the following bar
  * exit_time names the LAST BAR held: exit at that bar's close. '16:00' is an alias for '15:55'
    (the live resolver's "close of the last bar before 16:00").
  * gap_aware_stops=True: a bar that OPENS through the stop (or target) fills at the open; False fills
    at the level exactly (the live resolver's behaviour).
  * same_bar: 'loss' (stop assumed first — live resolver, conservative), 'win' (optimistic bound),
    'open_close_tiebreak' (green bar => path O-L-H-C => stop first; red bar => O-H-L-C => target first).
  * Results: WIN (target), LOSS (initial stop), TRAIL (breakeven/trailing stop), TIME (time stop),
    EOD (exit_time close), NO_ENTRY (no bar after the signal, or the fill is outside (stop, target)),
    NO_DATA (simulate_many only: ticker/date missing from the bar cache).
    `exit_reason` is finer: target|gap_target|stop|gap_stop|be_stop|trail_stop|gap_trail|time_stop|eod|...
  * R-based management rules (breakeven_after_r, trail_after_r/trail_dist_r, scale_out, time_stop_min_r)
    measure R from entry_fill in units of the original risk; raised stops apply from the NEXT bar.
  * mfe_R / mae_R span the entry bar through the exit bar inclusive (the exit bar's full range counts).
"""
from __future__ import annotations

import time
from typing import Iterable

import numpy as np
import pandas as pd

from . import bars as B
from .common import (BAR_TD, CACHE_DIR, ET, RTH_END_MIN, Progress, as_et, hhmm_to_minutes, logger,
                     make_ts, minutes_of_day, rth_mask, session_date_array, to_date)

ENTRY_MODES = ("signal_price", "next_open", "first_open", "next_close")
SAME_BAR_MODES = ("loss", "win", "open_close_tiebreak")
RESULT_FIELDS = ["result", "exit_reason", "entry_ts", "entry_fill", "exit_ts", "exit_fill", "R", "R_own",
                 "pnl_pct", "mfe_R", "mae_R", "bars_held", "notes", "entry_raw", "exit_raw", "risk",
                 "cost_bps_per_side", "entry_idx", "exit_idx"]
LIVE_RESOLVER_KW = dict(entry_mode="signal_price", cost_bps_per_side=0.0, same_bar="loss",
                        gap_aware_stops=False, exit_time="16:00")

_NS_5MIN = 5 * 60 * 1_000_000_000
_NAT_NS = np.iinfo(np.int64).min          # sentinel for "no timestamp" in int64 arrays


# ═══════════════════════════════════════════════════════════════════
#  Array preparation
# ═══════════════════════════════════════════════════════════════════
class DayArrays:
    """RTH bars of ONE session as numpy arrays (built once per ticker-day and reused)."""
    __slots__ = ("index", "t_ns", "mins", "o", "h", "l", "c", "n", "date")

    def __init__(self, day_rth: pd.DataFrame):
        idx = pd.DatetimeIndex(day_rth.index)
        if idx.tz is None:
            idx = idx.tz_localize(ET)
        self.index = idx
        # as_unit("ns"): pandas 3 indexes are often microsecond-resolution, and asi8 is in the index's
        # own unit — every signal timestamp in this module is int64 NANOSECONDS (Timestamp.value).
        self.t_ns = idx.as_unit("ns").asi8.astype(np.int64)
        self.mins = minutes_of_day(idx)
        self.o = day_rth["Open"].to_numpy(dtype=np.float64)
        self.h = day_rth["High"].to_numpy(dtype=np.float64)
        self.l = day_rth["Low"].to_numpy(dtype=np.float64)
        self.c = day_rth["Close"].to_numpy(dtype=np.float64)
        self.n = len(idx)
        self.date = idx[0].date() if self.n else None


def _prep_day(bars_day) -> DayArrays:
    if isinstance(bars_day, DayArrays):
        return bars_day
    df = bars_day
    if len(df) == 0:
        raise ValueError("empty bar frame")
    m = rth_mask(df.index)
    if not m.all():
        df = df.loc[m]
    dates = session_date_array(df.index)
    if len(df) and (dates != dates[0]).any():
        raise ValueError("bars_day must contain a single session")
    return DayArrays(df)


def _exit_index(mins: np.ndarray, exit_time: str) -> int:
    """Index of the last bar with start <= exit_time (and < 16:00). -1 if none."""
    em = min(hhmm_to_minutes(exit_time), RTH_END_MIN)
    ok = np.flatnonzero(mins <= em - (1 if em >= RTH_END_MIN else 0))
    return int(ok[-1]) if len(ok) else -1


def _locate_entry(mode: str, D: DayArrays, sig_ns: np.ndarray):
    """Vectorized entry location. Returns (i0, entry_raw, entry_ts_ns); entries past the day get i0 = n."""
    n = D.n
    if mode == "signal_price":
        i0 = np.searchsorted(D.t_ns, sig_ns, side="left")
        return i0, None, sig_ns
    if mode == "next_open":
        i0 = np.searchsorted(D.t_ns, sig_ns, side="right")
    elif mode == "first_open":
        i0 = np.searchsorted(D.t_ns, sig_ns, side="left")
    elif mode == "next_close":
        ib = np.searchsorted(D.t_ns, sig_ns, side="right") - 1
        ib = np.clip(ib, 0, n - 1)
        i0 = ib + 1
        raw = D.c[ib]
        return i0, raw, D.t_ns[ib] + _NS_5MIN
    else:
        raise ValueError(f"unknown entry_mode {mode!r}; expected one of {ENTRY_MODES}")
    ok = i0 < n
    raw = np.where(ok, D.o[np.minimum(i0, n - 1)], np.nan)
    ets = np.where(ok, D.t_ns[np.minimum(i0, n - 1)], sig_ns)
    return i0, raw, ets


def _empty_result(reason: str, cost: float, risk: float) -> dict:
    return dict(result="NO_ENTRY", exit_reason=reason, entry_ts=pd.NaT, entry_fill=np.nan, exit_ts=pd.NaT,
                exit_fill=np.nan, R=np.nan, R_own=np.nan, pnl_pct=np.nan, mfe_R=np.nan, mae_R=np.nan,
                bars_held=0, notes=reason, entry_raw=np.nan, exit_raw=np.nan, risk=risk,
                cost_bps_per_side=cost, entry_idx=-1, exit_idx=-1)


# ═══════════════════════════════════════════════════════════════════
#  General single-trade engine (all features)
# ═══════════════════════════════════════════════════════════════════
def simulate_trade(bars_day_rth, signal_ts, entry_price: float, stop: float, target: float, *,
                   entry_mode: str = "next_open", cost_bps_per_side: float = 5.0, exit_time: str = "15:55",
                   same_bar: str = "loss", gap_aware_stops: bool = True,
                   time_stop_min: float | None = None, time_stop_min_r: float = 0.5,
                   breakeven_after_r: float | None = None, trail_after_r: float | None = None,
                   trail_dist_r: float | None = None, scale_out: list | None = None) -> dict:
    """Simulate ONE long trade on one session's RTH bars. See the module docstring for every convention.

    bars_day_rth : DataFrame of the session's bars (non-RTH rows are dropped) or a DayArrays.
    signal_ts    : tz-aware Timestamp (naive = ET) or 'HH:MM' (date taken from the bars).
    Returns a dict with RESULT_FIELDS.
    """
    if entry_mode not in ENTRY_MODES:
        raise ValueError(f"entry_mode must be one of {ENTRY_MODES}")
    if same_bar not in SAME_BAR_MODES:
        raise ValueError(f"same_bar must be one of {SAME_BAR_MODES}")
    if trail_after_r is not None and trail_dist_r is None:
        raise ValueError("trail_dist_r is required when trail_after_r is set")
    if scale_out:
        fr = sum(f for _, f in scale_out)
        if fr >= 1.0 or any(f <= 0 for _, f in scale_out):
            raise ValueError("scale_out fractions must be positive and sum to < 1")
        scale_out = sorted(scale_out, key=lambda x: x[0])
    D = _prep_day(bars_day_rth)
    if isinstance(signal_ts, str):
        signal_ts = make_ts(D.date, signal_ts)
    signal_ts = as_et(signal_ts)
    cost = cost_bps_per_side / 1e4
    risk = float(entry_price - stop)
    if not (risk > 0) or not (target > entry_price):
        return _empty_result("bad_levels", cost_bps_per_side, risk)
    e = _exit_index(D.mins, exit_time)
    if e < 0:
        return _empty_result("no_bar", cost_bps_per_side, risk)
    sig_ns = np.array([signal_ts.value], dtype=np.int64)
    i0_a, raw_a, ets_a = _locate_entry(entry_mode, D, sig_ns)
    i0 = int(i0_a[0])
    if i0 >= D.n or i0 > e:
        return _empty_result("no_bar", cost_bps_per_side, risk)
    entry_raw = float(entry_price) if entry_mode == "signal_price" else float(raw_a[0])
    entry_ts = pd.Timestamp(int(ets_a[0]), tz="UTC").tz_convert(ET)
    if entry_raw <= stop:
        return _empty_result("fill_below_stop", cost_bps_per_side, risk)
    if entry_raw >= target:
        return _empty_result("fill_above_target", cost_bps_per_side, risk)
    entry_fill = entry_raw * (1 + cost)

    o, h, l, c, t_ns = D.o, D.h, D.l, D.c, D.t_ns
    stop_eff = float(stop)
    remaining = 1.0
    pieces: list[tuple[float, float, int, str]] = []   # (fraction, exit_raw, bar_idx, reason)
    notes: list[str] = []
    run_high = -np.inf
    time_checked = time_stop_min is None
    pending_scale = list(scale_out) if scale_out else []
    final = None   # (result, reason, exit_raw, j)

    def stop_kind():
        if stop_eff <= stop:
            return "LOSS", "stop"
        return "TRAIL", ("be_stop" if abs(stop_eff - entry_fill) < 1e-12 else "trail_stop")

    for j in range(i0, e + 1):
        oj, hj, lj, cj = o[j], h[j], l[j], c[j]
        # 1. gaps at the open
        if gap_aware_stops and oj <= stop_eff:
            res, why = stop_kind()
            final = (res, "gap_" + ("stop" if res == "LOSS" else "trail"), oj, j)
            break
        if gap_aware_stops and oj >= target:
            final = ("WIN", "gap_target", oj, j)
            break
        s_hit = lj <= stop_eff
        t_hit = hj >= target
        scales = [(r, f) for r, f in pending_scale if hj >= entry_fill + r * risk]
        if s_hit and (t_hit or scales):
            if same_bar == "loss":
                upside_first = False
            elif same_bar == "win":
                upside_first = True
            else:
                upside_first = cj < oj          # red bar: O-H-L-C, the high came first
            if not upside_first:
                res, why = stop_kind()
                final = (res, why, stop_eff, j)
                break
        if s_hit and not (t_hit or scales):
            res, why = stop_kind()
            final = (res, why, stop_eff, j)
            break
        # upside processing (scale-outs then target)
        for r, f in scales:
            pieces.append((f, entry_fill + r * risk, j, f"scale@{r}R"))
            remaining -= f
            pending_scale.remove((r, f))
            notes.append(f"scaled {f:g}@{r:g}R")
        if t_hit:
            final = ("WIN", "target", float(target), j)
            break
        if s_hit:   # upside_first with scales but no target: stop hit afterwards in the same bar
            res, why = stop_kind()
            final = (res, why, stop_eff, j)
            break
        # 2. time stop (one-time check at the first bar that ends >= entry_ts + time_stop_min)
        if not time_checked and (t_ns[j] + _NS_5MIN - entry_ts.value) >= time_stop_min * 60e9:
            time_checked = True
            if (cj - entry_fill) / risk < time_stop_min_r:
                final = ("TIME", "time_stop", cj, j)
                break
        # 3. management state for the NEXT bar
        run_high = max(run_high, hj)
        if breakeven_after_r is not None and hj >= entry_fill + breakeven_after_r * risk:
            stop_eff = max(stop_eff, entry_fill)
        if trail_after_r is not None and hj >= entry_fill + trail_after_r * risk:
            stop_eff = max(stop_eff, run_high - trail_dist_r * risk)
        if j == e:
            final = ("EOD", "eod", cj, j)
            break
    if final is None:   # cannot happen (j == e always terminates) — defensive
        final = ("EOD", "eod", c[e], e)
    res, why, exit_raw_last, j_exit = final
    pieces.append((remaining, float(exit_raw_last), j_exit, why))
    # aggregate pieces
    fills = np.array([p[1] * (1 - cost) for p in pieces])
    fracs = np.array([p[0] for p in pieces])
    exit_fill = float((fills * fracs).sum() / fracs.sum())
    exit_raw = float((np.array([p[1] for p in pieces]) * fracs).sum() / fracs.sum())
    R = float(((fills - entry_fill) * fracs).sum() / risk)
    R_own = float(((fills - entry_fill) * fracs).sum() / (entry_fill - stop))
    pnl_pct = float(((fills / entry_fill - 1) * fracs).sum() * 100)
    mfe = float((h[i0:j_exit + 1].max() - entry_fill) / risk)
    mae = float((l[i0:j_exit + 1].min() - entry_fill) / risk)
    exit_ts = pd.Timestamp(int(t_ns[j_exit]), tz="UTC").tz_convert(ET)
    return dict(result=res, exit_reason=why, entry_ts=entry_ts, entry_fill=float(entry_fill), exit_ts=exit_ts,
                exit_fill=exit_fill, R=R, R_own=R_own, pnl_pct=pnl_pct, mfe_R=mfe, mae_R=mae,
                bars_held=int(j_exit - i0 + 1), notes=";".join(notes), entry_raw=entry_raw, exit_raw=exit_raw,
                risk=risk, cost_bps_per_side=cost_bps_per_side, entry_idx=i0, exit_idx=int(j_exit))


# ═══════════════════════════════════════════════════════════════════
#  Vectorized plain-barrier batch (no management rules)
# ═══════════════════════════════════════════════════════════════════
def _plain_batch(D: DayArrays, sig_ns: np.ndarray, entry_price: np.ndarray, stop: np.ndarray, target: np.ndarray,
                 *, entry_mode: str, cost_bps_per_side: float, exit_time: str, same_bar: str,
                 gap_aware_stops: bool) -> dict:
    """Simulate m plain barrier trades on one session at once. Same semantics as simulate_trade without
    management rules. Returns dict of arrays (RESULT_FIELDS minus notes)."""
    m = len(sig_ns)
    n = D.n
    cost = cost_bps_per_side / 1e4
    out = {
        "result": np.full(m, "NO_ENTRY", dtype=object), "exit_reason": np.full(m, "no_bar", dtype=object),
        "entry_ts": np.full(m, _NAT_NS, dtype=np.int64), "entry_fill": np.full(m, np.nan),
        "exit_ts": np.full(m, _NAT_NS, dtype=np.int64),
        "exit_fill": np.full(m, np.nan), "R": np.full(m, np.nan), "R_own": np.full(m, np.nan),
        "pnl_pct": np.full(m, np.nan), "mfe_R": np.full(m, np.nan), "mae_R": np.full(m, np.nan),
        "bars_held": np.zeros(m, dtype=int), "entry_raw": np.full(m, np.nan), "exit_raw": np.full(m, np.nan),
        "risk": entry_price - stop, "cost_bps_per_side": np.full(m, cost_bps_per_side),
        "entry_idx": np.full(m, -1), "exit_idx": np.full(m, -1),
    }
    e = _exit_index(D.mins, exit_time)
    risk = entry_price - stop
    bad = ~(risk > 0) | ~(target > entry_price)
    out["exit_reason"][bad] = "bad_levels"
    if e < 0 or n == 0:
        return out
    i0, raw, ets = _locate_entry(entry_mode, D, sig_ns)
    if entry_mode == "signal_price":
        raw = entry_price.astype(np.float64)
    has_bar = (i0 < n) & (i0 <= e) & ~bad
    below = has_bar & (raw <= stop)
    above = has_bar & (raw >= target)
    out["exit_reason"][below] = "fill_below_stop"
    out["exit_reason"][above] = "fill_above_target"
    ok = has_bar & ~below & ~above
    if not ok.any():
        return out
    idx = np.flatnonzero(ok)
    i0k = i0[idx]; S = stop[idx][:, None]; T = target[idx][:, None]
    J = np.arange(n)[None, :]
    inwin = (J >= i0k[:, None]) & (J <= e)
    o, h, l, c = D.o[None, :], D.h[None, :], D.l[None, :], D.c[None, :]
    s_hit = inwin & (l <= S)
    t_hit = inwin & (h >= T)
    fs = np.where(s_hit.any(1), s_hit.argmax(1), n)
    ft = np.where(t_hit.any(1), t_hit.argmax(1), n)
    k = len(idx)
    res = np.full(k, "EOD", dtype=object); why = np.full(k, "eod", dtype=object)
    jx = np.full(k, e); xr = np.full(k, D.c[e], dtype=np.float64)
    Sf = stop[idx]; Tf = target[idx]
    stop_first = fs < ft
    tgt_first = ft < fs
    same = (fs == ft) & (fs < n)
    # stop first
    j = np.minimum(fs, n - 1)
    gap_s = gap_aware_stops & (D.o[j] <= Sf)
    sel = stop_first
    res[sel] = "LOSS"; jx[sel] = fs[sel]
    xr[sel] = np.where(gap_s[sel], D.o[j][sel], Sf[sel]); why[sel] = np.where(gap_s[sel], "gap_stop", "stop")
    # target first
    j = np.minimum(ft, n - 1)
    gap_t = gap_aware_stops & (D.o[j] >= Tf)
    sel = tgt_first
    res[sel] = "WIN"; jx[sel] = ft[sel]
    xr[sel] = np.where(gap_t[sel], D.o[j][sel], Tf[sel]); why[sel] = np.where(gap_t[sel], "gap_target", "target")
    # same bar
    if same.any():
        j = fs[same]
        oj, cj = D.o[j], D.c[j]
        Ss, Ts = Sf[same], Tf[same]
        if gap_aware_stops:
            gs = oj <= Ss
            gt = (oj >= Ts) & ~gs
        else:
            gs = np.zeros(len(j), bool); gt = gs
        if same_bar == "loss":
            up = np.zeros(len(j), bool)
        elif same_bar == "win":
            up = np.ones(len(j), bool)
        else:
            up = cj < oj
        win = gt | (~gs & up)
        r_ = np.where(win, "WIN", "LOSS")
        w_ = np.where(gs, "gap_stop", np.where(gt, "gap_target", np.where(win, "target", "stop")))
        x_ = np.where(gs, oj, np.where(gt, oj, np.where(win, Ts, Ss)))
        res[same] = r_; why[same] = w_; xr[same] = x_; jx[same] = j
    entry_raw = raw[idx].astype(np.float64)
    entry_fill = entry_raw * (1 + cost)
    exit_fill = xr * (1 - cost)
    rk = risk[idx]
    upto = (J >= i0k[:, None]) & (J <= jx[:, None])
    mfe = (np.where(upto, h, -np.inf).max(1) - entry_fill) / rk
    mae = (np.where(upto, l, np.inf).min(1) - entry_fill) / rk
    out["result"][idx] = res; out["exit_reason"][idx] = why
    out["entry_ts"][idx] = ets[idx].astype(np.int64); out["exit_ts"][idx] = D.t_ns[jx].astype(np.int64)
    out["entry_fill"][idx] = entry_fill; out["exit_fill"][idx] = exit_fill
    out["entry_raw"][idx] = entry_raw; out["exit_raw"][idx] = xr
    out["R"][idx] = (exit_fill - entry_fill) / rk
    out["R_own"][idx] = (exit_fill - entry_fill) / (entry_fill - Sf)
    out["pnl_pct"][idx] = (exit_fill / entry_fill - 1) * 100
    out["mfe_R"][idx] = mfe; out["mae_R"][idx] = mae
    out["bars_held"][idx] = jx - i0k + 1
    out["entry_idx"][idx] = i0k; out["exit_idx"][idx] = jx
    return out


def _ns_to_ts(a: np.ndarray) -> pd.Series:
    """int64 ns-since-epoch (sentinel _NAT_NS = missing) -> tz-aware ET timestamps (NaT where missing)."""
    a = np.asarray(a, dtype=np.int64)
    s = pd.Series(a).astype("Int64").mask(a == _NAT_NS)
    return pd.to_datetime(s, unit="ns", utc=True).dt.tz_convert(ET)


_MGMT_KEYS = ("time_stop_min", "breakeven_after_r", "trail_after_r", "trail_dist_r", "scale_out")


def _uses_management(kw: dict) -> bool:
    return any(kw.get(k) is not None for k in _MGMT_KEYS)


# ═══════════════════════════════════════════════════════════════════
#  Batch API
# ═══════════════════════════════════════════════════════════════════
_DAY_CACHE: dict[tuple[str, object], DayArrays | None] = {}


def day_arrays(ticker: str, d, bars: dict | None = None) -> DayArrays | None:
    """Cached DayArrays for a ticker-date (RTH bars). None if the ticker/date is not in the cache."""
    d = to_date(d)
    key = (ticker, d)
    if key not in _DAY_CACHE:
        df = (bars or {}).get(ticker)
        if df is None:
            df = B.load_5m([ticker]).get(ticker)
        if df is None:
            _DAY_CACHE[key] = None
        else:
            day = B.rth_slice(B.session_frame(df, d))
            _DAY_CACHE[key] = DayArrays(day) if len(day) else None
    return _DAY_CACHE[key]


def signal_timestamps(picks_df: pd.DataFrame, date_col="date", time_col="batch_time", signal_ts_col="signal_ts") -> pd.Series:
    """signal_ts per row (tz-aware ET, ns resolution): the `signal_ts` column if present, else
    date + batch_time ('HH:MM' or 'HH:MM:SS') in ET."""
    if signal_ts_col in picks_df.columns:
        s = pd.to_datetime(picks_df[signal_ts_col], utc=True).dt.tz_convert(ET)
    else:
        d = pd.to_datetime(picks_df[date_col]).dt.strftime("%Y-%m-%d")
        s = pd.to_datetime(d + " " + picks_df[time_col].astype(str)).dt.tz_localize(ET)
    return s.dt.as_unit("ns")


def _ts_to_ns(s: pd.Series) -> np.ndarray:
    """tz-aware datetime Series -> int64 ns since the epoch (UTC), independent of the Series' unit."""
    return s.dt.tz_convert("UTC").dt.tz_localize(None).dt.as_unit("ns").to_numpy(dtype="datetime64[ns]").astype(np.int64)


def simulate_many(picks_df: pd.DataFrame, bars: dict | None = None, *, ticker_col="ticker", date_col="date",
                  time_col="batch_time", signal_ts_col="signal_ts", entry_col="entry", stop_col="stop",
                  target_col="target", verbose: bool = True, **kw) -> pd.DataFrame:
    """Simulate every row of `picks_df` (columns: ticker, date+batch_time or signal_ts, entry, stop, target).

    Returns a DataFrame aligned to picks_df.index with ticker, date, signal_ts and RESULT_FIELDS.
    Plain barrier settings run vectorized per ticker-day; management rules fall back to simulate_trade
    per row (still fast: numpy arrays, no per-row pandas). Missing bars -> result 'NO_DATA'.
    `bars` is an optional preloaded dict from load_5m (loaded on demand otherwise).
    """
    kw = {**dict(entry_mode="next_open", cost_bps_per_side=5.0, exit_time="15:55", same_bar="loss",
                 gap_aware_stops=True), **kw}
    sig = signal_timestamps(picks_df, date_col, time_col, signal_ts_col)
    dates = sig.dt.date.to_numpy()
    tick = picks_df[ticker_col].astype(str).to_numpy()
    ent = picks_df[entry_col].to_numpy(dtype=np.float64)
    stp = picks_df[stop_col].to_numpy(dtype=np.float64)
    tgt = picks_df[target_col].to_numpy(dtype=np.float64)
    sig_ns = _ts_to_ns(sig)
    n = len(picks_df)
    cols = {f: np.full(n, np.nan) for f in ["entry_fill", "exit_fill", "R", "R_own", "pnl_pct",
                                             "mfe_R", "mae_R", "entry_raw", "exit_raw"]}
    cols["entry_ts"] = np.full(n, _NAT_NS, dtype=np.int64); cols["exit_ts"] = np.full(n, _NAT_NS, dtype=np.int64)
    cols["result"] = np.full(n, "NO_DATA", dtype=object)
    cols["exit_reason"] = np.full(n, "no_data", dtype=object)
    cols["notes"] = np.full(n, "", dtype=object)
    cols["bars_held"] = np.zeros(n, dtype=int)
    cols["risk"] = ent - stp
    cols["cost_bps_per_side"] = np.full(n, kw["cost_bps_per_side"], dtype=float)
    cols["entry_idx"] = np.full(n, -1); cols["exit_idx"] = np.full(n, -1)
    mgmt = _uses_management(kw)
    plain_kw = {k: kw[k] for k in ("entry_mode", "cost_bps_per_side", "exit_time", "same_bar", "gap_aware_stops")}
    groups = pd.DataFrame({"t": tick, "d": dates}).groupby(["t", "d"], sort=False).indices
    prog = Progress(len(groups), "simulate_many", every=500, secs=15) if verbose else None
    t0 = time.time()
    for (t, d), rows in groups.items():
        D = day_arrays(t, d, bars)
        if prog:
            prog.step()
        if D is None:
            continue
        rows = np.asarray(rows)
        if not mgmt:
            r = _plain_batch(D, sig_ns[rows], ent[rows], stp[rows], tgt[rows], **plain_kw)
            for f, v in r.items():
                cols[f][rows] = v
        else:
            for i in rows:
                r = simulate_trade(D, pd.Timestamp(int(sig_ns[i]), tz="UTC"), ent[i], stp[i], tgt[i], **kw)
                for f in RESULT_FIELDS:
                    v = r[f]
                    if f in ("entry_ts", "exit_ts"):
                        v = _NAT_NS if pd.isna(v) else int(pd.Timestamp(v).value)
                    cols[f][i] = v
    out = pd.DataFrame(index=picks_df.index)
    out["ticker"] = tick
    out["date"] = dates
    out["signal_ts"] = sig.to_numpy()
    for f in RESULT_FIELDS:
        if f in ("entry_ts", "exit_ts"):
            out[f] = _ns_to_ts(cols[f]).to_numpy()
        else:
            out[f] = cols[f]
    if verbose:
        logger.info(f"simulate_many: {n} picks, {len(groups)} ticker-days in {time.time() - t0:.1f}s")
    return out


def resolve_like_live(picks_df: pd.DataFrame, bars: dict | None = None, **overrides) -> pd.DataFrame:
    """simulate_many with the LIVE resolver's rules: entry at the recorded price, no costs, both-in-bar
    = LOSS, fills exactly at the levels, EOD at the 15:55 close (`exit_time='16:00'`)."""
    return simulate_many(picks_df, bars, **{**LIVE_RESOLVER_KW, **overrides})


# ═══════════════════════════════════════════════════════════════════
#  Whole-universe per-bar labels (base rate)
# ═══════════════════════════════════════════════════════════════════
_FEAT_CACHE: dict[str, pd.DataFrame] = {}
ALLBARS_FEATURES = ["bar_green", "above_vwap", "new_hod", "pm_high_hold", "strong", "consec_green", "range_pos",
                    "above_orb_high", "rsi", "rvol", "vwap_dist_pct", "ret_open_pct", "ema9_gt_21", "atr_pct", "atr_rth_pct"]


def _ticker_features(ticker: str) -> pd.DataFrame | None:
    """Per-bar features for a ticker (all bars), cached: indicators + STRONG table + rvol."""
    if ticker in _FEAT_CACHE:
        return _FEAT_CACHE[ticker]
    ind = B.get_indicators(ticker)
    if ind is None:
        _FEAT_CACHE[ticker] = None
        return None
    st = B.strong_table(ind)
    f = ind[["Open", "High", "Low", "Close", "Volume", "ATR", "ATR_rth", "RSI", "VWAP", "EMA_9", "EMA_21"]].copy()
    for c in st.columns:
        f[c] = st[c]
    f["rvol"] = B.rvol_scanner(ind)
    f["rsi"] = f["RSI"]
    f["vwap_dist_pct"] = (f["Close"] / f["VWAP"] - 1) * 100
    keys = session_date_array(f.index)
    m = rth_mask(f.index)
    open_0930 = f["Open"].where(m).groupby(keys).transform("first")
    f["ret_open_pct"] = (f["Close"] / open_0930 - 1) * 100
    f["ema9_gt_21"] = (f["EMA_9"] > f["EMA_21"])
    f["atr_pct"] = f["ATR"] / f["Close"] * 100
    f["atr_rth_pct"] = f["ATR_rth"] / f["Close"] * 100
    _FEAT_CACHE[ticker] = f
    return f


def scanner_levels(close: np.ndarray, atr: np.ndarray, stop_mult: float = 2.0, target_r: float = 2.5):
    """Mirror scanner.calculate_trade_levels rounding: entry=round(close,2); dist=round(atr*mult,2);
    stop=round(entry-dist,2); target=round(entry+dist*target_r,2)."""
    entry = np.round(close, 2)
    dist = np.round(atr * stop_mult, 2)
    stop = np.round(entry - dist, 2)
    target = np.round(entry + dist * target_r, 2)
    return entry, stop, target


def all_bars_labels(ticker: str, d, stop_mult: float = 2.0, target_r: float = 2.5, atr_kind: str = "scanner",
                    first: str = "09:35", last: str = "14:30", variants: dict | None = None) -> pd.DataFrame:
    """Per-bar outcome table for one ticker-day: every CLOSED RTH bar whose close time (signal_ts =
    bar start + 5min) lies in [first, last] is treated as a signal with scanner-style levels
    (entry = bar close, stop = entry - stop_mult*ATR, target = entry + target_r*(entry-stop), 2dp rounding
    like the scanner). ATR is the scanner's (pre/post-inclusive) ATR14 or the RTH-only one (atr_kind='rth').

    `variants` maps a column prefix to simulate kwargs. Default:
        sp_ : LIVE-mirror   (signal_price entry at the signal bar's close, 0 bps, same_bar='loss', no gap fills,
                             exit at the 15:55 close) — comparable to picks_master results.
        no_ : realistic     (entry at the OPEN of the bar that starts at signal_ts, i.e. the very next bar
                             (entry_mode='first_open'; signal_ts is a bar boundary here so 'next_open' would
                             skip one more bar), 5 bps/side, gap-aware fills, exit at the 15:55 close).
    Columns: ticker date bar_ts signal_ts hhmm entry atr stop target stop_pct + features (ALLBARS_FEATURES)
             + per variant: result exit_ts entry_fill exit_fill R pnl_pct mfe_R mae_R bars_held.
    """
    if variants is None:
        variants = {"sp_": dict(entry_mode="signal_price", cost_bps_per_side=0.0, same_bar="loss",
                                gap_aware_stops=False, exit_time="15:55"),
                    "no_": dict(entry_mode="first_open", cost_bps_per_side=5.0, same_bar="loss",
                                gap_aware_stops=True, exit_time="15:55")}
    f = _ticker_features(ticker)
    if f is None:
        return pd.DataFrame()
    d = to_date(d)
    day = B.session_frame(f, d)
    day = day.loc[rth_mask(day.index)]
    if len(day) == 0:
        return pd.DataFrame()
    D = DayArrays(day)
    mins = D.mins
    lo, hi = hhmm_to_minutes(first) - 5, hhmm_to_minutes(last) - 5
    sel = np.flatnonzero((mins >= lo) & (mins <= hi))
    atr = (day["ATR"] if atr_kind == "scanner" else day["ATR_rth"]).to_numpy(dtype=np.float64)
    good = sel[np.isfinite(atr[sel]) & (atr[sel] > 0)]
    if len(good) == 0:
        return pd.DataFrame()
    entry, stop, target = scanner_levels(D.c[good], atr[good], stop_mult, target_r)
    sig_ns = D.t_ns[good] + _NS_5MIN
    out = pd.DataFrame({
        "ticker": ticker, "date": d, "bar_ts": day.index[good], "signal_ts": day.index[good] + BAR_TD,
    })
    out["hhmm"] = out["signal_ts"].dt.strftime("%H:%M")
    out["entry"] = entry; out["atr"] = atr[good]; out["stop"] = stop; out["target"] = target
    out["stop_pct"] = (entry - stop) / entry * 100
    feats = day.iloc[good]
    for c in ALLBARS_FEATURES:
        out[c] = feats[c].to_numpy()
    for prefix, kw in variants.items():
        r = _plain_batch(D, sig_ns, entry, stop, target, **kw)
        out[prefix + "result"] = r["result"]
        out[prefix + "exit_ts"] = _ns_to_ts(r["exit_ts"]).to_numpy()
        for c in ["entry_fill", "exit_fill", "R", "pnl_pct", "mfe_R", "mae_R", "bars_held"]:
            out[prefix + c] = r[c]
    return out


def allbars_path(stop_mult: float = 2.0, target_r: float = 2.5, atr_kind: str = "scanner"):
    suffix = "" if atr_kind == "scanner" else f"_{atr_kind}"
    return CACHE_DIR / f"allbars_{stop_mult}_{target_r}{suffix}.csv.gz"


def load_allbars(stop_mult: float = 2.0, target_r: float = 2.5, atr_kind: str = "scanner") -> pd.DataFrame:
    """Load a built all-bars table (parses timestamps back to ET)."""
    p = allbars_path(stop_mult, target_r, atr_kind)
    df = pd.read_csv(p, low_memory=False)
    for c in df.columns:
        if c.endswith("_ts"):
            df[c] = pd.to_datetime(df[c], utc=True).dt.tz_convert(ET)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def build_allbars(stop_mult: float = 2.0, target_r: float = 2.5, atr_kind: str = "scanner", first: str = "09:35",
                  last: str = "14:30", tickers: Iterable[str] | None = None, resume: bool = True,
                  variants: dict | None = None) -> pd.DataFrame:
    """Build the whole-universe per-bar label table and write cache/allbars_<stop>_<target>[ _rth].csv.gz.

    Resumable: one part file per ticker under cache/allbars_parts/<key>/; existing parts are reused when
    resume=True. Logs progress. Returns the concatenated table. Stocks only (ETFs/index excluded).
    """
    tickers = list(tickers) if tickers is not None else B.stock_tickers()
    key = f"{stop_mult}_{target_r}_{atr_kind}_{first.replace(':', '')}_{last.replace(':', '')}"
    part_dir = CACHE_DIR / "allbars_parts" / key
    part_dir.mkdir(parents=True, exist_ok=True)
    prog = Progress(len(tickers), f"build_allbars[{key}]", every=10, secs=20)
    parts = []
    t0 = time.time()
    for t in tickers:
        pp = part_dir / f"{t}.csv.gz"
        if resume and pp.exists():
            parts.append(pd.read_csv(pp, low_memory=False))
            prog.step(extra="(cached)")
            continue
        df = B.load_5m([t]).get(t)
        if df is None:
            prog.step(extra=f"{t}: no data")
            continue
        frames = [all_bars_labels(t, d, stop_mult, target_r, atr_kind, first, last, variants)
                  for d in B.session_dates(df)]
        frames = [x for x in frames if len(x)]
        part = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        part.to_csv(pp, index=False, compression="gzip")
        parts.append(part)
        # free per-ticker feature memory
        _FEAT_CACHE.pop(t, None)
        prog.step(extra=f"{t}: {len(part)} rows")
    full = pd.concat([p for p in parts if len(p)], ignore_index=True)
    out = allbars_path(stop_mult, target_r, atr_kind)
    full.to_csv(out, index=False, compression="gzip")
    logger.info(f"build_allbars: {len(full)} rows from {len(tickers)} tickers -> {out} in {time.time() - t0:.0f}s")
    return full


if __name__ == "__main__":   # python -m harness.sim  -> builds the default 2.0x / 2.5R table
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--stop-mult", type=float, default=2.0)
    ap.add_argument("--target-r", type=float, default=2.5)
    ap.add_argument("--atr-kind", default="scanner")
    ap.add_argument("--first", default="09:35")
    ap.add_argument("--last", default="14:30")
    ap.add_argument("--no-resume", action="store_true")
    a = ap.parse_args()
    build_allbars(a.stop_mult, a.target_r, a.atr_kind, a.first, a.last, resume=not a.no_resume)
