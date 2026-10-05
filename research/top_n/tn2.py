"""TN2: other definitions of "top" (see PREREG_TN2.md). Train is free to look; the holdout runs once
on whatever tn2_promoted.json froze.

  python research/top_n/tn2.py build      signal matrices at every month-end -> tn2_signals.npz
  python research/top_n/tn2.py train      stage 1 table + tn2_promoted.json
  python research/top_n/tn2.py holdout    stage 2, once
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tn_bt import TN, Data, run  # noqa: E402

CODES = ["MOM", "LOWVOL", "PROF", "VALUE", "GROWTH", "YIELD", "EARN"]
UNI = {"sp": ("S&P 500", "SPY", 20, "2005-02-01"), "nd": ("Nasdaq-100", "QQQ", 10, "2007-03-01")}
TRAIN_END, HOLD_START, HOLD_END = "2015-12-31", "2016-01-01", "2026-09-30"
COST = 0.001
# statements not in US dollars: dollar-denominated definitions (VALUE, EARN) skip these
NONUSD = {"BIDU", "JD", "NTES", "PDD", "TCOM", "ERIC", "VOD", "RYAAY", "ASML", "CCEP", "BNTX"}
pd.set_option("display.width", 250)


def fundamentals(D, sig_dates):
    """PROF, GROWTH, EARN (and trailing net income for VALUE) as of each signal date."""
    out = {k: np.full((len(sig_dates), len(D.syms)), np.nan) for k in ("PROF", "GROWTH", "EARN")}
    sd = np.array(sig_dates, dtype="datetime64[ns]")
    for j, s in enumerate(D.syms):
        p = TN / "fund" / f"{s}.csv"
        if not p.exists():
            continue
        f = pd.read_csv(p, parse_dates=["date", "filingDate"]).dropna(subset=["date"]).sort_values("date")
        f = f[~f.date.duplicated(keep="last")].reset_index(drop=True)
        if len(f) < 4:
            continue
        for c in ("revenue", "grossProfit", "netIncome", "totalAssets"):
            if c not in f:
                f[c] = np.nan
        span = (f.date - f.date.shift(3)).dt.days            # four consecutive quarters cover ~9 months
        ok = span.between(240, 310)
        ttm = {c: f[c].rolling(4).sum().where(ok) for c in ("revenue", "grossProfit", "netIncome")}
        span8 = (f.date - f.date.shift(7)).dt.days
        growth = (ttm["revenue"] / ttm["revenue"].shift(4) - 1).where(span8.between(600, 680) & (ttm["revenue"].shift(4) > 0))
        prof = (ttm["grossProfit"] / f.totalAssets).where(f.totalAssets > 0)
        avail = np.maximum(f.filingDate.fillna(f.date + pd.Timedelta(days=45)).to_numpy(),
                           (f.date + pd.Timedelta(days=30)).to_numpy())
        avail = np.maximum.accumulate(avail)                 # keeps the search monotone
        k = np.searchsorted(avail, sd, side="left") - 1      # last statement available strictly before the date
        good = k >= 0
        kk = np.clip(k, 0, None)
        stale = (sd - f.date.to_numpy()[kk]) > np.timedelta64(200, "D")
        good &= ~stale
        for name, ser in (("PROF", prof), ("GROWTH", growth), ("EARN", ttm["netIncome"])):
            out[name][:, j] = np.where(good, ser.to_numpy()[kk], np.nan)
    return out


def build():
    D = Data()
    sig = D.fm - 1                                           # signal session for each monthly trade
    dates = D.cal[sig]
    P = pd.DataFrame(D.P)
    vol = P.pct_change(fill_method=None).rolling(252, min_periods=200).std().to_numpy()
    S = {}
    lag = np.clip(sig - 252, 0, None)
    has = (sig - 252 >= 0)[:, None]
    S["MOM"] = np.where(has, D.P[sig - 21] / D.P[lag] - 1, np.nan)
    S["LOWVOL"] = -vol[sig]
    S["YIELD"] = np.where(has, (D.P[sig] / D.P[lag]) - (D.mcap[sig] / D.mcap[lag]), np.nan)
    F = fundamentals(D, dates)
    S["PROF"], S["GROWTH"] = F["PROF"], F["GROWTH"]
    nonusd = np.array([s in NONUSD for s in D.syms])
    S["EARN"] = np.where(nonusd[None, :], np.nan, F["EARN"])
    S["VALUE"] = S["EARN"] / D.mcap[sig]
    # implausible values are data errors (symbol lineage, one-off gains), not signals
    S["VALUE"] = np.where((S["VALUE"] > 0.5) | (S["VALUE"] < -1), np.nan, S["VALUE"])
    S["YIELD"] = np.where(np.abs(S["YIELD"]) > 0.5, np.nan, S["YIELD"])
    S["GROWTH"] = np.where(S["GROWTH"] > 5, np.nan, S["GROWTH"])
    S["PROF"] = np.where(np.abs(S["PROF"]) > 3, np.nan, S["PROF"])
    np.savez(TN / "tn2_signals.npz", sig=sig, **S)
    for uni in UNI:
        rows = np.array([D.me_row[d] for d in dates])
        base = D.mem[uni][rows] & D.fresh[sig] & np.isfinite(D.mcap[sig])
        cov = {c: [int((base[i] & np.isfinite(S[c][i])).sum()) for i in (12, 60, 132, 200, len(sig) - 1) if dates[i] >= pd.Timestamp(UNI[uni][3])] for c in CODES}
        print(uni, "eligible members, base:", [int(base[i].sum()) for i in (12, 60, 132, 200, len(sig) - 1)], "at", [str(dates[i].date()) for i in (12, 60, 132, 200, len(sig) - 1)])
        print(pd.DataFrame(cov).T.to_string())


class Signals:
    def __init__(self, D):
        z = np.load(TN / "tn2_signals.npz")
        self.D = D
        self.row = {int(s): i for i, s in enumerate(z["sig"])}
        self.S = {c: z[c] for c in CODES}

    def eligible(self, uni, s, code=None, drop=()):
        D = self.D
        ok = D.mem[uni][D.me_row[D.cal[s]]] & D.fresh[s] & np.isfinite(D.mcap[s]) & np.isfinite(D.P[s])
        if code:
            for c in ([code] if isinstance(code, str) else code):
                ok = ok & np.isfinite(self.S[c][self.row[s]])
        for j in drop:
            ok[j] = False
        return np.flatnonzero(ok)

    def score(self, code, s, idx):
        """One definition, or the mean percentile rank of several (the composite)."""
        if isinstance(code, str):
            return self.S[code][self.row[s], idx]
        return np.mean([pd.Series(self.S[c][self.row[s], idx]).rank(pct=True).to_numpy() for c in code], axis=0)

    def picker(self, uni, code, n, drop=()):
        def f(s):
            idx = self.eligible(uni, s, code, drop)
            return idx[np.argsort(-self.score(code, s, idx))][:n]
        return f

    def everyone(self, uni, code):
        return lambda s: self.eligible(uni, s, code)


def monthly(e):
    return e.resample("ME").last().pct_change().dropna()


def cagr(e):
    return (e.iloc[-1] / e.iloc[0]) ** (365.25 / (e.index[-1] - e.index[0]).days) - 1


def boot_p(act, draws=10000, L=12, seed=11):
    """One-sided p that the mean monthly excess is not above zero (circular block bootstrap)."""
    rng = np.random.default_rng(seed)
    a = np.asarray(act)
    m = len(a)
    st = rng.integers(0, m, size=(draws, m // L + 1))
    idx = (st[:, :, None] + np.arange(L)[None, None, :]).reshape(draws, -1)[:, :m] % m
    means = a[idx].mean(axis=1)
    return float((means <= 0).mean()), np.percentile(means, [2.5, 97.5]) * 12


def evaluate(D, G, uni, code, n, start, end, drop=(), log=None):
    name, etf, _, _ = UNI[uni]
    e, t = run(D, uni, n, start=start, end=end, cost=COST, picker=G.picker(uni, code, n, drop), log=log)
    u, _ = run(D, uni, n, start=start, end=end, cost=COST, picker=G.everyone(uni, code))
    b = D.px(etf).reindex(e.index)
    me, mu, mb = monthly(e), monthly(u), monthly(b)
    a_idx, a_ewu = (me - mb).dropna(), (me - mu).dropna()
    yrs = (e.index[-1] - e.index[0]).days / 365.25
    return {"cagr": cagr(e), "etf": cagr(b), "ewu": cagr(u), "x_etf": cagr(e) - cagr(b), "x_ewu": cagr(e) - cagr(u),
            "t_etf": a_idx.mean() / a_idx.std() * np.sqrt(len(a_idx)), "t_ewu": a_ewu.mean() / a_ewu.std() * np.sqrt(len(a_ewu)),
            "vol": e.pct_change().std() * np.sqrt(252), "maxdd": (e / e.cummax() - 1).min(),
            "turnover": t.iloc[1:].sum() / yrs, "act": a_idx, "eq": e}


def table(rows):
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("act", "eq")} for r in rows])
    for c in ("cagr", "etf", "ewu", "x_etf", "x_ewu", "vol", "maxdd", "turnover"):
        if c in df:
            df[c] = (df[c] * 100).round(1)
    for c in ("t_etf", "t_ewu", "p_boot", "p_holm"):
        if c in df:
            df[c] = df[c].round(3 if c.startswith("p") else 2)
    return df.to_string(index=False)


def name_of(code):
    return code if isinstance(code, str) else "+".join(code)


def train():
    D = Data()
    G = Signals(D)
    rows = []
    for uni, (label, etf, n, start) in UNI.items():
        for code in CODES:
            for nn in ((n,) if uni == "nd" else (20, 10, 30)):
                r = evaluate(D, G, uni, code, nn, start, TRAIN_END)
                rows.append({"universe": label, "def": code, "n": nn, **r})
    print(table(rows))
    prim = [r for r in rows if r["universe"] == "S&P 500" and r["n"] == 20 and r["x_etf"] > 0 and r["x_ewu"] > 0]
    prim.sort(key=lambda r: -r["t_ewu"])
    promoted = [r["def"] for r in prim[:3]]
    out = {"promoted": promoted, "composite": promoted if len(promoted) >= 2 else None}
    json.dump(out, open(TN / "tn2_promoted.json", "w"))
    print("\npromoted:", out)
    pd.DataFrame([{k: v for k, v in r.items() if k not in ("act", "eq")} for r in rows]).to_csv(TN / "tn2_train.csv", index=False)


def holdout():
    D = Data()
    G = Signals(D)
    fz = json.load(open(TN / "tn2_promoted.json"))
    tests = list(fz["promoted"]) + ([fz["composite"]] if fz["composite"] else [])
    rows, conf = [], []
    for uni, (label, etf, n, start) in UNI.items():
        for code in tests + [c for c in CODES if c not in fz["promoted"]]:
            log = []
            r = evaluate(D, G, uni, code, n, HOLD_START, HOLD_END, log=log)
            p, ci = boot_p(r["act"])
            row = {"universe": label, "def": name_of(code), "n": n, "promoted": code in tests, **r, "p_boot": p,
                   "ci_lo": round(ci[0] * 100, 1), "ci_hi": round(ci[1] * 100, 1)}
            if code in tests:
                # the name that added most: sum of its monthly returns while held
                contrib = {}
                for (d0, names), (d1, _) in zip(log[:-1], log[1:]):
                    a, b = D.cal.get_loc(d0), D.cal.get_loc(d1)
                    for s in names:
                        contrib[s] = contrib.get(s, 0) + (D.P[b, D.col[s]] / D.P[a, D.col[s]] - 1) / len(names)
                top = max(contrib, key=contrib.get)
                r2 = evaluate(D, G, uni, code, n, HOLD_START, HOLD_END, drop=(D.col[top],))
                row["top_name"], row["x_etf_wo"] = top, round(r2["x_etf"] * 100, 1)
                pd.DataFrame([(d.date(), " ".join(x)) for d, x in log], columns=["date", "names"]).to_csv(
                    TN / f"tn2_holdings_{uni}_{name_of(code)}.csv", index=False)
                r["eq"].to_csv(TN / f"tn2_eq_{uni}_{name_of(code)}.csv")
                if uni == "sp":
                    conf.append(row)
            rows.append(row)
    for k, row in enumerate(sorted(conf, key=lambda r: r["p_boot"])):        # Holm across the promoted set
        row["p_holm"] = min(1.0, max([(len(conf) - i) * r["p_boot"] for i, r in enumerate(sorted(conf, key=lambda r: r["p_boot"]))][:k + 1]))
    print(table(rows))
    pd.DataFrame([{k: v for k, v in r.items() if k not in ("act", "eq")} for r in rows]).to_csv(TN / "tn2_holdout.csv", index=False)


if __name__ == "__main__":
    {"build": build, "train": train, "holdout": holdout}[sys.argv[1]]()
