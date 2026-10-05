"""Blank-slate map for ETFs and the calendar, TRAINING YEARS ONLY (signal days 2006-01-03 .. 2019-09-30).

ETFs have no survivorship problem (every fund used is still listed) and cost almost nothing to
trade, so index-level timing is tested on its own: for each condition known at the close, the
next 1..21 sessions against that ETF's own unconditional record.

  python research/blank_slate/bs_etf.py     -> blank_slate/etf_map_train.csv, calendar_train.csv
"""
import numpy as np
import pandas as pd

from bs_common import BS, TRAIN, load_etf
from bs_map import rsi

GROUPS = {"index": "SPY QQQ IWM DIA MDY IJR VTI RSP".split(),
          "sector": "XLB XLE XLF XLI XLK XLP XLU XLV XLY SMH XBI IBB KRE XHB XRT XME ITB IYT VNQ".split(),
          "other": "EFA EEM TLT IEF LQD HYG GLD SLV GDX USO DBC".split()}
COST = 0.0002  # per side
H = (1, 2, 3, 5, 10, 21)


def conditions(b):
    c = b.close
    ret = c / c.shift(1) - 1
    vol20 = ret.rolling(20).std()
    sma200, sma50, sma20, sd20 = (c.rolling(200).mean(), c.rolling(50).mean(), c.rolling(20).mean(),
                                  c.rolling(20).std())
    r2 = rsi(c.to_frame("x"), 2).x
    d = np.sign(c.diff()).fillna(0)
    grp = (d != d.shift()).cumsum()
    streak = d.groupby(grp).cumsum()
    up = c > sma200
    base = {"rsi2<5": r2 < 5, "rsi2<10": r2 < 10, "rsi2<20": r2 < 20, "rsi2>90": r2 > 90, "rsi2>95": r2 > 95,
            "down>=2": streak <= -2, "down>=3": streak <= -3, "down>=4": streak <= -4, "up>=3": streak >= 3,
            "r1<=-1.5sd": ret <= -1.5 * vol20.shift(1), "r1<=-2sd": ret <= -2 * vol20.shift(1),
            "5d_low_close": c <= c.rolling(5).min(), "10d_low_close": c <= c.rolling(10).min(),
            "z20<=-2": (c - sma20) / sd20 <= -2, "r5<=-3%": c / c.shift(5) - 1 <= -0.03,
            "r5<=-5%": c / c.shift(5) - 1 <= -0.05}
    out = {"ALWAYS": pd.Series(True, index=c.index), "above_sma200": up, "below_sma200": ~up & sma200.notna(),
           "sma50>sma200": sma50 > sma200, "mom_12_1>0": c.shift(21) / c.shift(252) - 1 > 0,
           "mom_12_1<0": c.shift(21) / c.shift(252) - 1 < 0}
    for k, v in base.items():
        out[k] = v
        out[k + " & above_sma200"] = v & up
        out[k + " & below_sma200"] = v & ~up & sma200.notna()
    return out


def main():
    lo, hi = pd.Timestamp(TRAIN[0]), pd.Timestamp(TRAIN[1])
    rows = []
    for grp, syms in GROUPS.items():
        for sym in syms:
            b = load_etf(sym)
            ao = b.open * b.adj / b.close
            fwd = {}
            for h in H:
                fwd[("oc", h)] = b.adj.shift(-h) / ao.shift(-1) - 1 - 2 * COST
                if h <= 5:
                    fwd[("cc", h)] = b.adj.shift(-h) / b.adj - 1 - 2 * COST
            inwin = (b.index >= lo) & (b.index <= hi)
            for name, m in conditions(b).items():
                m = m.fillna(False).to_numpy() & inwin
                for (kind, h), r in fwd.items():
                    x = r[m].dropna()
                    if len(x):
                        rows.append({"group": grp, "etf": sym, "cond": name, "entry": kind, "h": h, "n": len(x),
                                     "sum": x.sum(), "wins": int((x > 0).sum()), "sumsq": float((x ** 2).sum()),
                                     **{f"y{y}": x[x.index.year == y].sum() for y in range(2006, 2020)},
                                     **{f"n{y}": int((x.index.year == y).sum()) for y in range(2006, 2020)}})
    d = pd.DataFrame(rows)
    ycols = [f"y{y}" for y in range(2006, 2020)]
    ncols = [f"n{y}" for y in range(2006, 2020)]

    def agg(g):
        n = g.n.sum()
        mean = g["sum"].sum() / n
        sd = np.sqrt(max(g.sumsq.sum() / n - mean ** 2, 1e-12))
        h = g.name[-1]
        ym = g[ycols].sum().to_numpy() / np.maximum(g[ncols].sum().to_numpy(), 1)
        yn = g[ncols].sum().to_numpy()
        return pd.Series({"n": n, "per_year": n / 13.75, "mean": mean, "win": g.wins.sum() / n,
                          "t": mean / (sd / np.sqrt(max(n / h, 1))),
                          "years_pos": float((ym[yn >= 3] > 0).mean()) if (yn >= 3).any() else np.nan})

    pooled = d.groupby(["group", "cond", "entry", "h"]).apply(agg).reset_index()
    spy = d[d.etf == "SPY"].groupby(["cond", "entry", "h"]).apply(agg).reset_index().assign(group="SPY")
    out = pd.concat([pooled, spy])
    base = out[out.cond == "ALWAYS"].set_index(["group", "entry", "h"])[["mean", "win"]]
    out = out.join(base, on=["group", "entry", "h"], rsuffix="_base")
    out["edge"] = out["mean"] - out.mean_base
    out["lift"] = out.win - out.win_base
    out.to_csv(BS / "etf_map_train.csv", index=False)
    pd.set_option("display.width", 220)
    pd.set_option("display.float_format", "{:.4f}".format)
    for g in ("SPY", "index", "sector", "other"):
        t = out[(out.group == g) & (out.n >= 150) & (out.cond != "ALWAYS")]
        ub = out[(out.group == g) & (out.cond == "ALWAYS") & (out.entry == "oc") & out.h.isin([1, 5, 21])]
        print(f"\n== {g}: unconditional " + "; ".join(
            f"h{int(r.h)} mean {r['mean']:+.4f} win {r.win:.3f}" for _, r in ub.iterrows()))
        print(t.sort_values("win", ascending=False).head(14)[
            ["cond", "entry", "h", "n", "per_year", "mean", "win", "win_base", "lift", "edge", "t", "years_pos"]].to_string(index=False))

    # ---- calendar, SPY / QQQ / IWM
    cal_rows = []
    for sym in ("SPY", "QQQ", "IWM"):
        b = load_etf(sym)
        b = b[(b.index >= lo) & (b.index <= hi)]
        ao = b.open * b.adj / b.close
        day = b.adj / b.adj.shift(1) - 1
        night, intra = ao / b.adj.shift(1) - 1, b.adj / ao - 1
        ym = b.index.to_period("M")
        pos = pd.Series(np.arange(len(b)), index=b.index).groupby(ym).transform(lambda s: s - s.iloc[0] + 1)
        rev = pd.Series(np.arange(len(b)), index=b.index).groupby(ym).transform(lambda s: s.iloc[-1] - s + 1)
        sets = {"all days": pd.Series(True, index=b.index), "last day of month": rev == 1,
                "first 3 days of month": pos <= 3, "turn of month (last 1 + first 3)": (rev == 1) | (pos <= 3),
                "rest of month": ~((rev == 1) | (pos <= 3)),
                **{f"weekday {w}": b.index.dayofweek == i for i, w in enumerate("Mon Tue Wed Thu Fri".split())}}
        for nm, m in sets.items():
            m = np.asarray(m)
            for part, r in (("close-to-close", day), ("overnight", night), ("open-to-close", intra)):
                x = r[m].dropna()
                yr = x.groupby(x.index.year).mean()
                cal_rows.append({"etf": sym, "days": nm, "part": part, "n": len(x), "mean_bp": x.mean() * 1e4,
                                 "win": (x > 0).mean(), "t": x.mean() / (x.std() / np.sqrt(len(x))),
                                 "years_pos": (yr > 0).mean()})
    cd = pd.DataFrame(cal_rows)
    cd.to_csv(BS / "calendar_train.csv", index=False)
    print("\n== calendar (train)")
    print(cd[cd.etf == "SPY"].round(3).to_string(index=False))


if __name__ == "__main__":
    main()
