"""Top-N study robustness: is the result one stock, one era, or a data gap?

  python research/top_n/tn_robust.py

1. leave one name out of the top-10 portfolios
2. halves of the sample
3. block-bootstrap interval for the excess return
4. off-the-shelf ETFs over the same windows
5. the two largest missing names (old Dell, Wachovia) put back from year-end closes
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tn_bt  # noqa: E402
from tn_bt import TN, Data, run, stats  # noqa: E402
from tn_run import BENCH, NAME, START, bench  # noqa: E402

# year-end closes (split-adjusted to the last trading day) and shares outstanding, billions.
# Dividends are ignored for Wachovia (it paid about 4% a year), so its loss is slightly overstated.
GAPS = {
    "DELL_OLD": {"px": {"2004-12-31": 42.14, "2005-12-30": 29.95, "2006-12-29": 25.09, "2007-12-31": 24.51,
                        "2008-12-31": 10.24, "2009-12-31": 14.36, "2010-12-31": 13.55, "2011-12-30": 14.63,
                        "2012-12-31": 10.14, "2013-10-28": 13.88},
                 "sh": {2005: 2.45, 2006: 2.30, 2007: 2.24, 2008: 1.98, 2009: 1.95, 2010: 1.94, 2011: 1.83,
                        2012: 1.75, 2013: 1.75}, "in": ("sp", "nd")},
    "WB_OLD": {"px": {"2004-12-31": 52.60, "2005-12-30": 52.86, "2006-12-29": 56.95, "2007-12-31": 38.03,
                      "2008-12-31": 5.54},
               "sh": {2005: 1.57, 2006: 1.60, 2007: 1.90, 2008: 2.10}, "in": ("sp",)},
}


def cagr(e):
    return (e.iloc[-1] / e.iloc[0]) ** (365.25 / (e.index[-1] - e.index[0]).days) - 1


def with_gaps(D):
    """Append the missing names with a price path interpolated between year-end closes."""
    for name, g in GAPS.items():
        pts = pd.Series({pd.Timestamp(k): np.log(v) for k, v in g["px"].items()})
        path = np.exp(pts.reindex(D.cal.union(pts.index)).interpolate("time").reindex(D.cal))
        path[(D.cal < pts.index[0]) | (D.cal > pts.index[-1])] = np.nan
        live = path.notna().to_numpy()
        mc = path.to_numpy() * np.array([g["sh"].get(d.year, np.nan) for d in D.cal]) * 1e9
        D.P = np.column_stack([D.P, path.ffill().to_numpy()])
        D.fresh = np.column_stack([D.fresh, live])
        D.mcap = np.column_stack([D.mcap, mc])
        D.syms.append(name)
        D.col[name] = len(D.syms) - 1
        for uni in ("sp", "nd"):
            member = np.array([live[D.cal.get_loc(d)] for d in D.me]) & (uni in g["in"])
            D.mem[uni] = np.column_stack([D.mem[uni], member])
    return D


def main():
    D = Data()
    print("1. leave one name out (top 10 equal weight, monthly): CAGR and excess over the benchmark")
    for uni in ("sp", "nd"):
        b = bench(D, uni, START[uni])
        base, _ = run(D, uni, 10, start=START[uni])
        print(f"  {NAME[uni]:11} all names      {cagr(base) * 100:5.1f}%  excess {(cagr(base) - cagr(b)) * 100:+.1f}")
        keep = D.mem[uni].copy()
        for s in ("NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "AVGO"):
            D.mem[uni] = keep.copy()
            D.mem[uni][:, D.col[s]] = False
            e, _ = run(D, uni, 10, start=START[uni])
            print(f"  {NAME[uni]:11} without {s:6} {cagr(e) * 100:5.1f}%  excess {(cagr(e) - cagr(b)) * 100:+.1f}")
        D.mem[uni] = keep

    print("\n2. halves of the sample: excess CAGR over the benchmark, points a year")
    for uni in ("sp", "nd"):
        b = bench(D, uni, START[uni])
        for n in (3, 5, 10, 20, 30):
            e, _ = run(D, uni, n, start=START[uni])
            cut = "2015-12-31"
            h1 = cagr(e.loc[:cut]) - cagr(b.loc[:cut])
            h2 = cagr(e.loc[cut:]) - cagr(b.loc[cut:])
            print(f"  {NAME[uni]:11} top {n:2}  to 2015 {h1 * 100:+5.1f}   2016 on {h2 * 100:+5.1f}")

    print("\n3. block bootstrap (12-month blocks, 5,000 draws) of the annualised monthly excess return")
    rng = np.random.default_rng(7)
    for uni in ("sp", "nd"):
        b = bench(D, uni, START[uni])
        for n, w in ((10, "ew"), (10, "cw"), (5, "ew")):
            e, _ = run(D, uni, n, weight=w, start=START[uni])
            act = (e.resample("ME").last().pct_change() - b.resample("ME").last().pct_change()).dropna().to_numpy()
            L, m = 12, len(act)
            draws = []
            for _ in range(5000):
                st = rng.integers(0, m, size=m // L + 1)
                idx = (st[:, None] + np.arange(L)[None, :]).ravel()[:m] % m
                draws.append(act[idx].mean() * 12)
            lo, hi = np.percentile(draws, [2.5, 97.5])
            print(f"  {NAME[uni]:11} top {n} {w}: mean {act.mean() * 1200:+.1f} pts/yr, 95% interval [{lo * 100:+.1f}, {hi * 100:+.1f}], "
                  f"share of draws below zero {np.mean(np.array(draws) < 0) * 100:.0f}%")

    print("\n4. ETFs you could simply hold (total return, same windows)")
    for start in ("2005-06-01", "2007-03-01", "2016-01-01"):
        row = []
        for s in ("SPY", "RSP", "OEF", "XLG", "QQQ", "MGK"):
            x = D.px(s).loc[start:"2026-09-30"].dropna()
            row.append(f"{s} {cagr(x) * 100:.1f}% (dd {(x / x.cummax() - 1).min() * 100:.0f}%)")
        print(f"  from {start[:7]}: " + "  ".join(row))
    for uni in ("sp", "nd"):
        for start in ("2005-06-01", "2007-03-01", "2016-01-01"):
            if start < START[uni]:
                continue
            e, _ = run(D, uni, 10, start=start, end="2026-09-30")
            print(f"  {NAME[uni]} top 10 equal weight from {start[:7]}: {cagr(e) * 100:.1f}% (dd {(e / e.cummax() - 1).min() * 100:.0f}%)")

    print("\n5. old Dell and Wachovia put back (interpolated from year-end closes)")
    before = {}
    for uni in ("sp", "nd"):
        for n in (10, 20, 30):
            before[uni, n] = cagr(run(D, uni, n, start=START[uni])[0])
    D = with_gaps(D)
    for uni in ("sp", "nd"):
        for n in (10, 20, 30):
            log = []
            e, _ = run(D, uni, n, start=START[uni], log=log)
            held = {k: sum(k in names for _, names in log) for k in GAPS}
            print(f"  {NAME[uni]:11} top {n}: CAGR {before[uni, n] * 100:.2f}% -> {cagr(e) * 100:.2f}%   months held {held}")


if __name__ == "__main__":
    main()
