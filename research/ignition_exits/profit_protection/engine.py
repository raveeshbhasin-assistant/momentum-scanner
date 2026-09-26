"""
Profit-protection exit harness for the ignition study (shared by all agents).

Entries are IDENTICAL to research/ignition_exits/exit_study.py (748 trades):
every ignition fire 2016-2026 in S&P 500+400, buy at the next open, a new
entry needs no fire in the prior 20 sessions, max hold 252 sessions.
Close-based triggers sell at the NEXT session's open (no look-ahead).

The current live rule ("base_fail": close < pre-ignition base) is the FLOOR
and is always active unless a rule is evaluated with floor=False. A rule ADDS
exits on top of the floor.

RULE API
  A rule is fn(p) -> bool array (length = len(p["c"])), True where the close
  triggers a full exit. Or fn(p) -> list of (bool_array, weight) legs for
  partial exits (weights sum to 1); each leg exits at its own first trigger
  (the floor still applies to every leg).
  p fields (arrays aligned to sessions k=0..last after entry; k=0 = entry day):
    c, o, h, l      closes / opens / highs / lows
    k               session index since entry
    entry           entry price (scalar, open of k=0)
    ret             c/entry - 1
    peak            running max close (inclusive of today)
    peak_ret        peak/entry - 1
    dd              c/peak - 1 (<= 0)
    base            ORIGINAL pre-ignition base (close 5 sessions before fire, scalar)
    base_dyn        base that ratchets UP on each re-fire (close 5 sessions before
                    the latest fire day seen so far, k>=5; max with previous)
    sig             ignition signal on each day
    since_fire      sessions since the most recent fire (entry fire => k+1)
    refired         True once any fire occurs at k>=5 after entry
    m20, m50, m200  moving averages;  atr = 20-day ATR;  hi52 = 252-day high close
  Every p field at index k uses data up to and including close k only.

EVALUATION
  evaluate(rule, splits=("train",)) -> {split: metrics}
  HOLDOUT/LIVE ARE LOCKED during rule development: use only "train".
"""
from __future__ import annotations

import glob
import os

import numpy as np
import pandas as pd

H = 252
REENTRY_GAP = 20
PX = os.environ.get("IGN_PX", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "px"))

_cache: dict = {}


def _load():
    if _cache:
        return _cache
    parts = [pd.read_pickle(f) for f in sorted(glob.glob(f"{PX}/c*.pkl"))]
    F = {}
    for fld in ("Open", "High", "Low", "Close", "Volume"):
        df = pd.concat([p[fld] for p in parts], axis=1)
        F[fld] = df.loc[:, ~df.columns.duplicated()].sort_index()
    O, Hh, L, C, V = (F[k] for k in ("Open", "High", "Low", "Close", "Volume"))
    r5 = C.pct_change(5, fill_method=None)
    r252 = C.pct_change(252, fill_method=None)
    volr = V.rolling(21).mean() / V.rolling(126).mean()
    ma20, ma50, ma200 = (C.rolling(n).mean() for n in (20, 50, 200))
    golden = ma50 > ma200
    dv = (C * V).rolling(21).mean()
    valid = (C > 3) & (dv > 5e6) & r252.notna()
    sig = ((r5 > 0.12) & (volr > 1.5) & golden & valid).values
    prev_c = C.shift(1)
    tr = pd.concat([(Hh - L), (Hh - prev_c).abs(), (L - prev_c).abs()]).groupby(level=0).max().reindex(C.index)
    atr20 = tr.rolling(20).mean()
    hi52 = C.rolling(252, min_periods=20).max()
    fwd63 = (C.shift(-63) / C - 1).values
    Cv, Ov, Hv, Lv = C.values, O.values, Hh.values, L.values
    n_days, n_tk = Cv.shape
    entries = []
    for j in range(n_tk):
        idx = np.flatnonzero(sig[:, j])
        last = -10_000
        for t in idx:
            if t - last > REENTRY_GAP and t + 1 < n_days and np.isfinite(Ov[t + 1, j]):
                entries.append((t, j))
            last = t
    trades = []
    for t, j in entries:
        e = t + 1
        end = min(e + H, n_days - 1)
        if end <= e:
            continue
        sl = slice(e, end + 1)
        c = Cv[sl, j]
        if not np.isfinite(c).all():
            c = pd.Series(c).ffill().values
        o = Ov[sl, j]; o = np.where(np.isfinite(o), o, c)
        h = Hv[sl, j]; h = np.where(np.isfinite(h), h, c)
        l = Lv[sl, j]; l = np.where(np.isfinite(l), l, c)
        k = np.arange(len(c))
        entry = Ov[e, j]
        s_path = sig[sl, j]
        last_fire = np.maximum.accumulate(np.where(s_path, k, -1))
        since_fire = np.where(last_fire >= 0, k - last_fire, k + 1)
        base = Cv[t - 5, j] if t >= 5 else -np.inf
        bd = np.empty(len(c), dtype=float)
        cur = base
        for m in range(len(c)):
            if s_path[m] and m >= 5:
                cand = Cv[e + m - 5, j]
                if np.isfinite(cand):
                    cur = max(cur, cand)
            bd[m] = cur
        refired = np.maximum.accumulate(s_path & (k >= 5))
        peak = np.maximum.accumulate(c)
        p = {
            "c": c, "o": o, "h": h, "l": l, "k": k, "entry": entry, "ret": c / entry - 1,
            "peak": peak, "peak_ret": peak / entry - 1, "dd": c / peak - 1,
            "base": base, "base_dyn": bd, "sig": s_path, "since_fire": since_fire,
            "refired": refired, "m20": ma20.values[sl, j], "m50": ma50.values[sl, j],
            "m200": ma200.values[sl, j], "atr": atr20.values[sl, j], "hi52": hi52.values[sl, j],
        }
        trades.append({
            "ticker": C.columns[j], "fire": C.index[t], "e": e, "j": j, "p": p,
            "complete": (e + H) <= n_days - 1, "max_run": np.nanmax(c) / entry - 1,
        })
    for tr_ in trades:
        tr_["split"] = ("train" if tr_["fire"] < pd.Timestamp("2022-01-01")
                        else "holdout" if tr_["complete"] else "live")
    base_fwd63 = float(np.nanmean(np.where(valid.values, fwd63, np.nan)))
    _cache.update(trades=trades, Ov=Ov, Cv=Cv, fwd63=fwd63, base_fwd63=base_fwd63)
    return _cache


def _first(trig, last):
    hit = np.flatnonzero(np.asarray(trig, dtype=bool)[:last])
    return int(hit[0]) if len(hit) else None


def _leg(tr_, trig, floor, Ov, Cv, fwd63):
    p = tr_["p"]; c = p["c"]; last = len(c) - 1; e, j = tr_["e"], tr_["j"]
    trig = np.asarray(trig, dtype=bool)
    if floor:
        trig = trig | (c < p["base"])
    d = _first(trig, last)
    if d is not None:
        xi = e + d + 1
        xp = Ov[xi, j] if np.isfinite(Ov[xi, j]) else Cv[xi, j]
        return xp / p["entry"] - 1, d + 1, True, fwd63[xi, j], p["peak_ret"][d]
    return c[last] / p["entry"] - 1, last, False, np.nan, p["peak_ret"][last]


def run(rule, floor=True, splits=("train",)):
    """Per-trade results for the requested splits (DataFrame)."""
    D = _load()
    rows = []
    for tr_ in D["trades"]:
        if tr_["split"] not in splits:
            continue
        out = rule(tr_["p"])
        legs = out if isinstance(out, list) else [(out, 1.0)]
        rs = [(_leg(tr_, trig, floor, D["Ov"], D["Cv"], D["fwd63"]), w) for trig, w in legs]
        ret = sum(r[0] * w for r, w in rs)
        held = sum(r[1] * w for r, w in rs)
        ew = sum(w for r, w in rs if r[2])
        post = (sum(r[3] * w for r, w in rs if r[2] and np.isfinite(r[3])) / ew) if ew else np.nan
        rows.append({"ticker": tr_["ticker"], "fire": tr_["fire"], "split": tr_["split"], "ret": ret,
                     "held": held, "early": ew > 0, "post63": post,
                     "peak_before_exit": max(r[4] for r, _ in rs), "max_run": tr_["max_run"]})
    return pd.DataFrame(rows)


def metrics(df):
    r = df.ret; w, l = r[r > 0].sum(), -r[r < 0].sum()
    big = df.max_run >= 0.8
    rt = (df.peak_before_exit >= 0.20) & (df.ret < 0)
    return {
        "n": int(len(df)), "mean": float(r.mean()), "median": float(r.median()),
        "win": float((r > 0).mean()), "pf": float(w / l) if l else float("nan"),
        "worst10": float(r.quantile(0.10)), "held": float(df.held.mean()),
        "ret_per_100d": float(r.mean() / df.held.mean() * 100),
        "big_capture": float(r[big].mean()) if big.any() else float("nan"),
        "roundtrip20": int(rt.sum()),           # were up >=20% then exited at a loss
        "giveback": float((df.peak_before_exit - r).mean()),
        "early_exits": float(df.early.mean()),
        "post_exit_fwd63": float(df.post63[df.early].mean()) if df.early.any() else float("nan"),
    }


def evaluate(rule, floor=True, splits=("train",)):
    df = run(rule, floor, splits)
    return {s: metrics(df[df.split == s]) for s in splits}


def never(p):
    """No extra exit. evaluate(never) = current live rule (base_fail);
    evaluate(never, floor=False) = hold 252 sessions."""
    return np.zeros(len(p["c"]), bool)
