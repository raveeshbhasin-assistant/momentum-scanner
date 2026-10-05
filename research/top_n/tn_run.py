"""Top-N study: base case, refinements, robustness. Prints tables and writes CSVs to the data dir.

  python research/top_n/tn_run.py qa        data checks (who is in the top 10, gaps, odd splits)
  python research/top_n/tn_run.py base      top 10/20/30, both indexes, vs benchmarks
  python research/top_n/tn_run.py grid      N x rebalance frequency, weighting, buffer, tilts, overlay
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tn_bt import TN, Data, ranked, run, stats  # noqa: E402

BENCH = {"sp": "SPY", "nd": "QQQ"}
START = {"sp": "2005-01-01", "nd": "2007-03-01"}
NAME = {"sp": "S&P 500", "nd": "Nasdaq-100"}
pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 30)


def fmt(d):
    o = {}
    for k, v in d.items():
        if k in ("cagr", "vol", "maxdd", "excess", "te", "turnover", "cagr_lo", "cagr_hi"):
            o[k] = f"{v * 100:.1f}%"
        elif k in ("ret_vol", "ir", "t", "growth"):
            o[k] = f"{v:.2f}"
        else:
            o[k] = v
    return o


def bench(D, uni, start, end=None):
    e = D.px(BENCH[uni])
    fm = D.fm[D.cal[D.fm] >= pd.Timestamp(start)]
    e = e.iloc[fm[0]:]
    if end:
        e = e.loc[:end]
    return e / e.iloc[0]


def avg_run(D, uni, n, freq, **kw):
    """Average the statistics over every start month of the schedule (removes rebalance-timing luck)."""
    rows = []
    for o in range(freq):
        e, t = run(D, uni, n, freq=freq, offset=o, **kw)
        b = bench(D, uni, kw.get("start", "2005-01-01"), kw.get("end"))
        st = stats(e, b)
        yrs = (e.index[-1] - e.index[0]).days / 365.25
        st["turnover"] = t.iloc[1:].sum() / yrs
        rows.append(st)
    df = pd.DataFrame(rows)
    out = df.drop(columns=["yrs_won"]).mean().to_dict()
    out["cagr_lo"], out["cagr_hi"] = df.cagr.min(), df.cagr.max()
    out["yrs_won"] = rows[0]["yrs_won"]
    return out


def qa(D):
    for uni in ("sp", "nd"):
        print(f"\n== {NAME[uni]}: ten largest at each year-end (market cap $B)")
        for y in range(2004 if uni == "sp" else 2007, 2026):
            s = int(D.cal.searchsorted(pd.Timestamp(f"{y}-12-31"), side="right") - 1)
            r = ranked(D, uni, s)[:10]
            print(y, " ".join(f"{D.syms[j]}:{D.mcap[s, j] / 1e9:.0f}" for j in r))
        row_counts = [(d.year, int((D.mem[uni][i] & D.fresh[D.cal.get_loc(d)]).sum()))
                      for i, d in enumerate(D.me) if d.month == 12 and d.year >= (2004 if uni == "sp" else 2007)]
        print("members with a live price each December:", row_counts)
    miss = json.load(open(TN / "members_without_data.json"))
    for k in ("sp", "ndx"):
        m = {t: v for t, v in miss[k].items() if v[1] >= ("2005-01-01" if k == "sp" else "2007-02-01")}
        print(f"\n{k}: {len(m)} member tickers with no data file:", " ".join(f"{t}({v[0][:4]}-{v[1][:4]})" for t, v in sorted(m.items())))
    odd = pd.read_csv(TN / "odd_splits.csv")
    big = set()      # only names that ever rank in a top 60 matter
    for uni in ("sp", "nd"):
        for i in D.fm[::3]:
            if uni == "sp" or D.cal[i] >= pd.Timestamp("2007-03-01"):
                big |= {D.syms[j] for j in ranked(D, uni, i - 1)[:60]}
    print("\nodd-ratio splits treated as spin-offs (names ever in a top 60):")
    print(odd[odd.symbol.isin(big)].assign(ratio=lambda d: (d.num / d.den).round(3)).to_string(index=False))


def base(D):
    rows = []
    for uni in ("sp", "nd"):
        for start in sorted({START[uni], "2007-03-01"}):
            b = bench(D, uni, start)
            rows.append({"index": NAME[uni], "from": start[:7], "portfolio": BENCH[uni] + " (benchmark)", **stats(b)})
            for w in ("ew", "cw"):
                for n in (10, 20, 30):
                    log = []
                    e, t = run(D, uni, n, weight=w, start=start, log=log)
                    st = stats(e, b)
                    yrs = (e.index[-1] - e.index[0]).days / 365.25
                    st["turnover"] = t.iloc[1:].sum() / yrs
                    rows.append({"index": NAME[uni], "from": start[:7],
                                 "portfolio": f"top {n} {'equal' if w == 'ew' else 'cap'}-wt", **st})
                    e.to_csv(TN / f"eq_{uni}_{n}_{w}_{start[:4]}.csv")
                    if w == "ew" and start == START[uni]:
                        pd.DataFrame([(d.date(), " ".join(x)) for d, x in log],
                                     columns=["date", "names"]).to_csv(TN / f"holdings_{uni}_{n}.csv", index=False)
            b.to_csv(TN / f"eq_{uni}_bench_{start[:4]}.csv")
    pd.DataFrame(rows).to_csv(TN / "base_table.csv", index=False)
    print(pd.DataFrame([fmt(r) for r in rows]).fillna("").to_string(index=False))
    print("\nCAGR by sub-period (equal-weight, monthly)")
    per = [("2005-01-01", "2009-12-31"), ("2010-01-01", "2014-12-31"), ("2015-01-01", "2019-12-31"),
           ("2020-01-01", "2026-09-30")]
    out = []
    for uni in ("sp", "nd"):
        e_all = {n: pd.read_csv(TN / f"eq_{uni}_{n}_ew_{START[uni][:4]}.csv", index_col=0, parse_dates=True).iloc[:, 0]
                 for n in (10, 20, 30)}
        e_all["bench"] = bench(D, uni, START[uni])
        for a, z in per:
            r = {"index": NAME[uni], "period": f"{a[:4]}-{z[:4]}"}
            for k, e in e_all.items():
                x = e.loc[a:z]
                if len(x) < 200:
                    continue
                yrs = (x.index[-1] - x.index[0]).days / 365.25
                r[str(k)] = f"{((x.iloc[-1] / x.iloc[0]) ** (1 / yrs) - 1) * 100:.1f}%"
            out.append(r)
        yr = pd.DataFrame({str(k): e.resample("YE").last().pct_change() for k, e in e_all.items()}).dropna()
        yr.index = yr.index.year
        yr.to_csv(TN / f"yearly_{uni}.csv")
        print(NAME[uni], "calendar years (%):")
        print((yr * 100).round(1).T.to_string())
    print(pd.DataFrame(out).fillna("").to_string(index=False))


def grid(D):
    rows = []
    for uni in ("sp", "nd"):
        for n in (1, 3, 5, 10, 15, 20, 30, 50):
            for f in (1, 3, 6, 12):
                st = avg_run(D, uni, n, f, start=START[uni])
                rows.append({"index": NAME[uni], "n": n, "months": f, **st})
    df = pd.DataFrame(rows)
    df.to_csv(TN / "grid_n_freq.csv", index=False)
    labs = (("cagr", "CAGR %"), ("excess", "excess over benchmark, % a year"), ("maxdd", "max drawdown %"),
            ("ret_vol", "return / volatility"), ("turnover", "turnover % a year"), ("t", "t-stat of monthly excess"))
    for uni in ("S&P 500", "Nasdaq-100"):
        g = df[df["index"] == uni]
        for c, lab in labs:
            pv = g.pivot(index="n", columns="months", values=c)
            raw = c in ("ret_vol", "t")
            print(f"\n{uni}: {lab} (rows = N, columns = months between rebalances; equal weight, net of 5 bps a side)")
            print((pv * (1 if raw else 100)).round(2 if raw else 1).to_string())
    var = []
    for uni in ("sp", "nd"):
        cases = [("top 10 equal-wt monthly (base)", dict(n=10, freq=1)),
                 ("top 10 cap-wt monthly", dict(n=10, freq=1, weight="cw")),
                 ("top 10 equal-wt, keep until out of top 15", dict(n=10, freq=1, buffer=1.5)),
                 ("top 10 equal-wt annual", dict(n=10, freq=12)),
                 ("top 10 annual, keep until out of top 15", dict(n=10, freq=12, buffer=1.5)),
                 ("10 strongest 12-1 momentum of top 30", dict(n=10, freq=1, rule="mom", pool=30)),
                 ("10 weakest 12-1 momentum of top 30 (placebo)", dict(n=10, freq=1, rule="lowmom", pool=30)),
                 ("10 strongest momentum of top 50", dict(n=10, freq=1, rule="mom", pool=50)),
                 ("top 10 + 200-day trend switch to short Treasuries", dict(n=10, freq=1, trend=BENCH[uni])),
                 ("top 10 equal-wt monthly, 20 bps a side", dict(n=10, freq=1, cost=0.002))]
        for lab, c in cases:
            c = dict(c)
            n, f = c.pop("n"), c.pop("freq")
            st = avg_run(D, uni, n, f, start=START[uni], **c)
            var.append({"index": NAME[uni], "variant": lab, **st})
    pd.DataFrame(var).to_csv(TN / "variants.csv", index=False)
    print()
    cols = ["index", "variant", "cagr", "excess", "vol", "maxdd", "ret_vol", "ir", "t", "turnover", "yrs_won"]
    print(pd.DataFrame([fmt(r) for r in var])[cols].to_string(index=False))


def qa_ret(D):
    """Held-month returns from FMP against the Yahoo-based blank-slate panel, where both have the name."""
    z = np.load("C:/dev/Trader-v3-data/blank_slate/panel.npz", allow_pickle=True)
    keys = list(z["keys"])
    pcal = pd.DatetimeIndex(z["cal"])
    by_sym = {}
    for j, k in enumerate(keys):
        by_sym.setdefault(k.split("_", 1)[-1], []).append(j)
    padj = z["adj"]
    rows = []
    for uni in ("sp", "nd"):
        h = pd.read_csv(TN / f"holdings_{uni}_30.csv", parse_dates=["date"])
        for a, b, names in zip(h.date[:-1], h.date[1:], h.names[:-1]):
            ia, ib = D.cal.get_loc(a), D.cal.get_loc(b)
            pa, pb = pcal.get_indexer([a])[0], pcal.get_indexer([b])[0]
            for s in names.split():
                r1 = D.P[ib, D.col[s]] / D.P[ia, D.col[s]] - 1
                best = None
                for j in by_sym.get(s, []):
                    if pa >= 0 and pb >= 0 and np.isfinite(padj[pa, j]) and np.isfinite(padj[pb, j]):
                        best = padj[pb, j] / padj[pa, j] - 1
                rows.append((uni, a.date(), s, r1, best))
    df = pd.DataFrame(rows, columns=["uni", "date", "sym", "fmp", "yahoo"])
    both = df.dropna()
    both = both.assign(diff=both.fmp - both.yahoo)
    print(f"{len(df):,} held name-months; {len(both):,} also in the Yahoo panel; "
          f"|diff| > 2 pts: {(both['diff'].abs() > 0.02).sum()}; mean diff {both['diff'].mean() * 100:.3f} pts")
    print(both[both["diff"].abs() > 0.02].sort_values("diff").to_string(index=False))
    print("held names never in the Yahoo panel:", sorted(set(df[df.yahoo.isna()].sym) - set(both.sym)))
    ext = df[(df.fmp.abs() > 0.4)]
    print("held months with a move beyond 40%:")
    print(ext.to_string(index=False))


if __name__ == "__main__":
    D = Data()
    {"qa": qa, "base": base, "grid": grid, "qa_ret": qa_ret}[sys.argv[1]](D)
