"""Top-N backtest engine: hold the N largest members of an index, rebalance on a schedule.

Timing: rank on the last close of the month, trade at the next session's close (one-day lag).
Weights drift between rebalances. Cost is charged per dollar traded (default 5 bps a side).
A name that stops trading mid-period is carried at its last price until the next rebalance.
"""
from pathlib import Path

import numpy as np
import pandas as pd

TN = Path("C:/dev/Trader-v3-data/top_n")
ETF = Path("C:/dev/Trader-v3-data/blank_slate/etf")


class Data:
    def __init__(self):
        z = np.load(TN / "tn_panel.npz", allow_pickle=True)
        self.cal = pd.DatetimeIndex(z["cal"])
        self.syms = list(z["syms"])
        self.col = {s: j for j, s in enumerate(self.syms)}
        adj = pd.DataFrame(z["adj"])
        # a single bad print (>60% move that fully reverses next day) is dropped
        r = adj.pct_change(fill_method=None)
        bad = (r.abs() > 0.6) & ((1 + r) * (1 + r.shift(-1)) - 1).abs().lt(0.1)
        adj = adj.mask(bad)
        self.fresh = adj.notna().rolling(5, min_periods=1).max().to_numpy().astype(bool)
        self.P = adj.ffill().to_numpy()
        self.mcap = pd.DataFrame(z["mcap"]).ffill(limit=10).to_numpy()
        self.me = pd.DatetimeIndex(z["me"])
        self.mem = {"sp": z["sp_m"], "nd": z["nd_m"]}
        self.T = len(self.cal)
        m = self.cal.month
        self.fm = np.array([i for i in range(1, self.T) if m[i] != m[i - 1]])   # first session of each month
        self.me_row = {d: i for i, d in enumerate(self.me)}
        shy = pd.read_csv(ETF / "SHY.csv", parse_dates=["date"]).set_index("date")["Adj Close"]
        shy.index = pd.DatetimeIndex(shy.index).tz_localize(None).normalize()
        self.cash = shy.reindex(self.cal).ffill().bfill().to_numpy()

    def px(self, s):
        return pd.Series(self.P[:, self.col[s]], index=self.cal)


def ranked(D, uni, s):
    """Member symbols with a live price and a market cap at session s, largest first."""
    row = D.me_row[D.cal[s]]
    ok = D.mem[uni][row] & D.fresh[s] & np.isfinite(D.mcap[s]) & np.isfinite(D.P[s])
    idx = np.flatnonzero(ok)
    return idx[np.argsort(-D.mcap[s, idx])]


def run(D, uni, n, freq=1, offset=0, weight="ew", rule="cap", pool=None, buffer=1.0, trend=None,
        cost=0.0005, start="2005-01-01", end=None, log=None, picker=None):
    fm = D.fm[D.cal[D.fm] >= pd.Timestamp(start)]
    trades = np.unique(np.r_[fm[0], fm[offset::freq]])   # every offset starts invested on the same day
    monthly = set(fm.tolist())
    last = D.T - 1 if end is None else int(D.cal.searchsorted(pd.Timestamp(end), side="right") - 1)
    eq = np.full(D.T, np.nan)
    v, w_old, held = 1.0, {}, []
    eq[trades[0]] = 1.0
    turn = []
    risk_on, custom = True, None
    bounds = list(trades[trades <= last]) + [last]
    # with a trend overlay the on/off switch is checked monthly even when names change less often
    points = sorted(set(bounds) | ({i for i in monthly if trades[0] <= i <= last} if trend else set()))
    for a, b in zip(points[:-1], points[1:]):
        s = a - 1
        if a in set(trades.tolist()):
            r = ranked(D, uni, s)
            if picker is not None:      # any other definition of "top": callable(session) -> column indices,
                pk = picker(s)          # or {column: weight} for a mix of sleeves
                custom = dict(pk) if isinstance(pk, dict) else None
                pick = list(pk)
            elif rule == "cap":
                keep = [j for j in held if j in set(r[:int(round(n * buffer))].tolist())] if buffer > 1 else []
                pick = keep + [j for j in r if j not in keep][:n - len(keep)]
            elif rule == "mom":
                p = r[:pool]
                mom = D.P[s - 21, p] / D.P[max(s - 252, 0), p] - 1
                pick = list(p[np.argsort(-np.nan_to_num(mom, nan=-9))][:n])
            elif rule == "lowmom":      # placebo: the weakest instead of the strongest
                p = r[:pool]
                mom = D.P[s - 21, p] / D.P[max(s - 252, 0), p] - 1
                pick = list(p[np.argsort(np.nan_to_num(mom, nan=9))][:n])
            held = pick
            if log is not None:
                log.append((D.cal[a], [D.syms[j] for j in pick]))
        if trend:
            t = D.P[:s + 1, D.col[trend]]
            risk_on = t[-1] > t[-200:].mean()
        if custom:
            w = dict(custom)
        elif weight == "ew":
            w = {j: 1.0 / len(held) for j in held}
        else:
            mc = D.mcap[s, held]
            w = dict(zip(held, mc / mc.sum()))
        if not risk_on:
            w = {-1: 1.0}
        # between name rebalances weights drift, unless this point is a name rebalance or a switch
        if a not in set(trades.tolist()) and set(w) == set(w_old) and w_old:
            w = w_old
        keys = set(w) | set(w_old)
        traded = sum(abs(w.get(k, 0) - w_old.get(k, 0)) for k in keys)
        v *= 1 - cost * traded
        turn.append((D.cal[a], traded / 2))
        ks = list(w)
        ws = np.array([w[k] for k in ks])
        seg = np.column_stack([D.cash[a:b + 1] if k == -1 else D.P[a:b + 1, k] for k in ks])
        rel = seg / seg[0]
        path = rel @ ws
        eq[a:b + 1] = v * path
        v *= path[-1]
        w_old = dict(zip(ks, ws * rel[-1] / path[-1]))
    e = pd.Series(eq, index=D.cal).dropna()
    t = pd.Series(dict(turn))
    return e, t


def stats(e, bench=None):
    yrs = (e.index[-1] - e.index[0]).days / 365.25
    d = e.pct_change().dropna()
    out = {"cagr": (e.iloc[-1] / e.iloc[0]) ** (1 / yrs) - 1, "vol": d.std() * np.sqrt(252),
           "maxdd": (e / e.cummax() - 1).min(), "growth": e.iloc[-1] / e.iloc[0]}
    out["ret_vol"] = out["cagr"] / out["vol"]
    if bench is not None:
        b = bench.reindex(e.index)
        m = e.resample("ME").last().pct_change().dropna()
        mb = b.resample("ME").last().pct_change().dropna()
        act = m - mb
        out["excess"] = out["cagr"] - ((b.iloc[-1] / b.iloc[0]) ** (1 / yrs) - 1)
        out["te"] = act.std() * np.sqrt(12)
        out["ir"] = act.mean() * 12 / out["te"]
        out["t"] = act.mean() / act.std() * np.sqrt(len(act))
        ye = e.resample("YE").last().pct_change().dropna()
        yb = b.resample("YE").last().pct_change().dropna()
        out["yrs_won"] = f"{int((ye > yb).sum())}/{len(ye)}"
    return out
