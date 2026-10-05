"""TN3: do definitions of "top" work in phases, and can the recent leader be followed?
(see PREREG_TN3.md)

  python research/top_n/tn3.py sleeves    eight sleeves + ETF per universe, full history -> tn3_sleeves_{uni}.csv
  python research/top_n/tn3.py phases     part A: year-by-year excess, run lengths, persistence test
  python research/top_n/tn3.py follow     part B: walk-forward leader-following, six configurations
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tn_bt import TN, Data, ranked, run  # noqa: E402
from tn2 import CODES, COST, Signals, boot_p, cagr, monthly  # noqa: E402

UNI = {"sp": ("S&P 500", "SPY", 20, "2005-02-01", "2010-02-01"), "nd": ("Nasdaq-100", "QQQ", 10, "2007-03-01", "2012-03-01")}
SLEEVES = CODES + ["SIZE"]
END = "2026-09-30"
GRID = [(12, 1), (12, 2), (36, 1), (36, 2), (60, 1), (60, 2)]
PRIMARY = (12, 2)
pd.set_option("display.width", 250)


def sleeve_picker(D, G, uni, code, n):
    if code == "SIZE":
        return lambda s: ranked(D, uni, s)[:n]
    return G.picker(uni, code, n)


def sleeves():
    D = Data()
    G = Signals(D)
    for uni, (label, etf, n, start, _) in UNI.items():
        out = {}
        for code in SLEEVES:
            out[code], _ = run(D, uni, n, start=start, end=END, cost=COST, picker=sleeve_picker(D, G, uni, code, n))
        out["EWU"], _ = run(D, uni, n, start=start, end=END, cost=COST, picker=G.everyone(uni, None))
        df = pd.DataFrame(out)
        df[etf] = D.px(etf).reindex(df.index)
        df[etf] /= df[etf].iloc[0]
        df.to_csv(TN / f"tn3_sleeves_{uni}.csv")
        print(label, "sleeves", df.index[0].date(), "..", df.index[-1].date(),
              {c: round(cagr(df[c]) * 100, 1) for c in df})


def load(uni):
    return pd.read_csv(TN / f"tn3_sleeves_{uni}.csv", index_col=0, parse_dates=True)


def phases():
    for uni, (label, etf, n, start, _) in UNI.items():
        df = load(uni)
        yr = df.resample("YE").last().pct_change().dropna()
        yr.index = yr.index.year
        ex = (yr[SLEEVES].sub(yr[etf], axis=0) * 100).round(1)
        print(f"\n== {label}: calendar-year return minus {etf}, points")
        print(ex.T.to_string())
        ex.to_csv(TN / f"tn3_yearly_excess_{uni}.csv")
        # which sleeve led each year, and did last year's leader beat the index this year
        lead = ex.idxmax(axis=1)
        nxt = [ex.loc[y, lead[y - 1]] for y in ex.index[1:]]
        print("leader by year:", " ".join(f"{y}:{lead[y]}" for y in ex.index))
        print(f"last year's leader, this year: beat {etf} in {sum(v > 0 for v in nxt)} of {len(nxt)} years, mean {np.mean(nxt):+.1f} pts, median {np.median(nxt):+.1f}")
        same = [(np.sign(ex[c]).shift(1) == np.sign(ex[c])).iloc[1:].mean() for c in SLEEVES]
        print(f"a sleeve's excess keeps last year's sign {np.mean(same) * 100:.0f}% of the time (chance 50%)")
        m = df.resample("ME").last()
        r12 = m.pct_change(12)
        runs = []
        for c in SLEEVES:
            sign = (r12[c] > r12[etf]).dropna().astype(int)
            sign = sign[r12[c].notna()]
            grp = (sign != sign.shift()).cumsum()
            runs.append((c, round(sign.mean() * 100), round(sign.groupby(grp).size().mean(), 1), int(sign.groupby(grp).size().max())))
        print(pd.DataFrame(runs, columns=["sleeve", "% of months ahead on 12m", "average run, months", "longest run"]).to_string(index=False))
        mr = m[SLEEVES].pct_change()
        print("persistence test: rank correlation, trailing L-month return vs next month, across the eight sleeves")
        for L in (12, 36, 60):
            past = m[SLEEVES].pct_change(L)
            ics = []
            for t in range(L, len(m) - 1):
                ics.append(past.iloc[t].rank().corr(mr.iloc[t + 1].rank()))
            p, ci = boot_p(np.array(ics))
            print(f"  L={L:2}: mean {np.mean(ics):+.3f} over {len(ics)} months, 95% range of the mean [{ci[0] / 12:+.3f}, {ci[1] / 12:+.3f}], p {p:.3f}")


def follow():
    D = Data()
    G = Signals(D)
    rows = []
    for uni, (label, etf, n, start, test_start) in UNI.items():
        df = load(uni)
        cands = SLEEVES + [etf]
        eq = df[cands].reindex(D.cal).to_numpy()
        pos = {int(a): i for i, a in enumerate(D.fm)}
        pickers = {c: sleeve_picker(D, G, uni, c, n) for c in SLEEVES}
        mix = monthly(df[SLEEVES]).mean(axis=1)                 # equal mix of the eight sleeves, rebalanced monthly
        b_all, u_all = df[etf], df["EWU"]

        def make(L, K, chosen):
            def f(s):
                i = pos[s + 1]
                s0 = int(D.fm[i - L]) - 1
                r = eq[s] / eq[s0]
                best = np.argsort(-r)[:K]
                chosen.append((D.cal[s + 1], [cands[k] for k in best]))
                w = {}
                for k in best:
                    if cands[k] == etf:
                        w[D.col[etf]] = w.get(D.col[etf], 0) + 1 / K
                    else:
                        names = pickers[cands[k]](s)
                        for j in names:
                            w[int(j)] = w.get(int(j), 0) + 1 / K / len(names)
                return w
            return f

        for L, K in GRID:
            chosen = []
            e, t = run(D, uni, n, start=test_start, end=END, cost=COST, picker=make(L, K, chosen))
            b = b_all.reindex(e.index)
            me, mb = monthly(e), monthly(b)
            act = (me - mb).dropna()
            p, ci = boot_p(act)
            mid = e.index[len(e) // 2]
            h = [cagr(e.loc[:mid]) - cagr(b.loc[:mid]), cagr(e.loc[mid:]) - cagr(b.loc[mid:])]
            mx = (1 + mix.reindex(me.index)).cumprod()
            u = u_all.reindex(e.index)
            yrs = (e.index[-1] - e.index[0]).days / 365.25
            cagr_mix = mx.iloc[-1] ** (12 / len(mx)) - 1
            picks = pd.Series([c for _, cs in chosen for c in cs]).value_counts(normalize=True)
            rows.append({"universe": label, "L": L, "K": K, "primary": (L, K) == PRIMARY, "cagr": cagr(e), "etf": cagr(b),
                         "x_etf": cagr(e) - cagr(b), "x_mix": cagr(e) - cagr_mix, "x_ewu": cagr(e) - cagr(u),
                         "h1": h[0], "h2": h[1], "p_boot": p, "ci_lo": ci[0], "ci_hi": ci[1],
                         "vol": e.pct_change().std() * np.sqrt(252), "maxdd": (e / e.cummax() - 1).min(),
                         "turnover": t.iloc[1:].sum() / yrs,
                         "most chosen": " ".join(f"{k}:{v * 100:.0f}%" for k, v in picks.head(4).items())})
            e.to_csv(TN / f"tn3_eq_{uni}_L{L}_K{K}.csv")
            pd.DataFrame([(d.date(), " ".join(c)) for d, c in chosen], columns=["date", "chosen"]).to_csv(
                TN / f"tn3_chosen_{uni}_L{L}_K{K}.csv", index=False)
        # context: every sleeve on the same window
        w = df.loc[test_start:]
        print(f"{label} {test_start[:7]}..: " + "  ".join(f"{c} {cagr(w[c]) * 100:.1f}%" for c in cands + ["EWU"]))
    out = pd.DataFrame(rows)
    out.to_csv(TN / "tn3_follow.csv", index=False)
    for c in ("cagr", "etf", "x_etf", "x_mix", "x_ewu", "h1", "h2", "ci_lo", "ci_hi", "vol", "maxdd", "turnover"):
        out[c] = (out[c] * 100).round(1)
    out["p_boot"] = out["p_boot"].round(3)
    print(out.to_string(index=False))


if __name__ == "__main__":
    {"sleeves": sleeves, "phases": phases, "follow": follow}[sys.argv[1]]()
