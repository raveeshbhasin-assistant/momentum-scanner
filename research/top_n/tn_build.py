"""Top-N study panel: daily total-return price and point-in-time market cap for every index member,
plus month-by-month membership of the S&P 500 and the Nasdaq-100.

  python research/top_n/tn_build.py        -> C:/dev/Trader-v3-data/top_n/tn_panel.npz (+ prints QA)

Market cap. FMP's history is (split-adjusted close) x (shares restated for real splits). FMP also
records large spin-offs as "splits" (GE 1253:1000, ABT 5000:2399 ...), which lowers the old closes but
not the old share counts, so the cap before a spin-off is understated by that ratio. SPINS lists the
ratios that are spin-offs; caps before each date are multiplied back up.
"""
import json
import sys
from fractions import Fraction
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tn_members import TN, ndx_changes, ndx_now, sp500  # noqa: E402
from tn_fetch import ALIAS  # noqa: E402

START = "2004-01-01"
# one line per company: other share classes fold into the first
CLASS = {"GOOG": "GOOGL", "BRK-A": "BRK-B", "FOX": "FOXA", "NWS": "NWSA", "DISCK": "WBD", "DISCB": "WBD",
         "CMCSK": "CMCSA", "LBTYK": "LBTYA", "LBTYB": "LBTYA", "UA": "UAA", "LMCK": "LMCA", "VIAB": "PARA",
         "LSXMK": "LSXMA", "LSXMB": "LSXMA", "FWONK": "FWONA", "Z": "ZG", "HEI-A": "HEI"}
# Yahoo (YHOO) is not on FMP: prices from an archived Yahoo Finance download (to 2016-09-14),
# shares outstanding in billions by year from the 10-Ks (rounded; only used for ranking)
YHOO_SHARES = {2004: 1.38, 2005: 1.42, 2006: 1.36, 2007: 1.34, 2008: 1.39, 2009: 1.40, 2010: 1.31,
               2011: 1.22, 2012: 1.12, 2013: 1.01, 2014: 0.94, 2015: 0.95, 2016: 0.95}
# Nasdaq-100 change-table rows that are not real changes (share-class notes, renames)
NDX_SKIP = {("2014-12-22", "CMCSA"), ("2016-02-01", "AVGO")}
NDX_FIX = {("2015-11-11", "AVGO"): "BRCM"}      # the row removes Broadcom Corp (BRCM), not Avago


def sym_of(t):
    s = ALIAS.get(t, t).replace(".", "-")
    return CLASS.get(s, s)


def simple(num, den):
    f = Fraction(num / den).limit_denominator(1000)
    return f.numerator == 1 or f.denominator == 1 or max(f.numerator, f.denominator) <= 10


def load_meta():
    out = {}
    with open(TN / "fmp_meta.jsonl") as f:
        for l in f:
            if l.strip():
                d = json.loads(l)
                out[d["symbol"]] = d
    return out


def ndx_by_date(dates):
    ch = ndx_changes()
    cur = {sym_of(x) for x in ndx_now()}
    out, k = {}, 0
    rows = list(ch.itertuples())
    for d in sorted(dates, reverse=True):
        while k < len(rows) and rows[k].date > d:       # undo every change dated after d
            r = rows[k]
            key = r.date.strftime("%Y-%m-%d")
            # a second share class joining is not a new company
            if r.added and (key, r.added) not in NDX_SKIP and r.added.replace(".", "-") not in CLASS:
                cur.discard(sym_of(r.added))
            if r.removed:
                cur.add(sym_of(NDX_FIX.get((key, r.removed), r.removed)))
            k += 1
        out[d] = set(cur)
    return out


def main():
    meta = load_meta()
    spy = pd.read_csv(TN / "fmp" / "SPY.csv", parse_dates=["date"])
    cal = pd.DatetimeIndex(spy.date[spy.date >= START]).sort_values()
    syms = sorted(s for s, m in meta.items() if m["n_px"] > 0 and (TN / "fmp" / f"{s}.csv").exists())
    syms = [s for s in syms if s not in CLASS] + ["YHOO"]
    T, N = len(cal), len(syms)
    adj = np.full((T, N), np.nan)
    mcap = np.full((T, N), np.nan)
    spins = []
    for j, s in enumerate(syms):
        if s == "YHOO":
            y = pd.read_csv(TN / "yhoo_wayback_20160915.csv", parse_dates=["Date"]).set_index("Date").sort_index()
            y = y.reindex(cal)
            adj[:, j] = y["Adj Close"]
            # the archived Close is as-traded; Adj Close is split-adjusted. Shares are as of each year.
            mcap[:, j] = y["Close"].to_numpy() * np.array([YHOO_SHARES.get(d.year, np.nan) for d in cal]) * 1e9
            first = y["Adj Close"].first_valid_index()
            continue
        df = pd.read_csv(TN / "fmp" / f"{s}.csv", parse_dates=["date"]).set_index("date")
        df = df[~df.index.duplicated()].reindex(cal)
        a = pd.to_numeric(df["adj"], errors="coerce")
        a = a.where(a > 0).to_numpy(dtype="float64", copy=True)
        if s in ADJ_FIX:           # FMP applied the spin-off adjustment twice; matched to Yahoo's return that day
            a[cal < pd.Timestamp(ADJ_FIX[s][0])] *= ADJ_FIX[s][1]
        adj[:, j] = a
        if "mcap" in df:
            m = pd.to_numeric(df["mcap"], errors="coerce")
            m = m.where(m > 0).to_numpy(dtype="float64", copy=True)
            for d, num, den in meta[s].get("splits", []):
                if den and num and not simple(num, den) and abs(num / den - 1) > 0.03 and d > START:
                    spins.append((s, d, num, den))
                    if (s, d) not in NOT_SPIN and s != "DD":
                        m[cal < pd.Timestamp(d)] *= num / den
            for d, f in MANUAL.get(s, []):                 # spin-offs FMP folded into the price only
                m[cal < pd.Timestamp(d)] *= f
            if s == "DD":      # DowDuPont: FMP's ratios do not reconcile; scale to the reported $167B at 2017-12
                m[(cal >= pd.Timestamp("2017-09-01")) & (cal < pd.Timestamp("2019-06-03"))] *= 1.69
            mcap[:, j] = m
    col = {s: j for j, s in enumerate(syms)}
    # month-end membership
    me = pd.Series(cal, index=cal).groupby([cal.year, cal.month]).last().to_numpy()
    me = pd.DatetimeIndex(me)
    sp = sp500()
    sp_dates = np.array(sorted(sp))
    ndx = ndx_by_date(me[me >= "2007-02-01"])
    sp_m = np.zeros((len(me), N), bool)
    nd_m = np.zeros((len(me), N), bool)
    miss_sp, miss_nd = {}, {}
    for i, d in enumerate(me):
        snap = sp[sp_dates[sp_dates.searchsorted(np.datetime64(d), side="right") - 1]]
        for t in snap:
            s = sym_of(t)
            if s in col:
                sp_m[i, col[s]] = True
            else:
                miss_sp.setdefault(t, []).append(d)
        for s in ndx.get(d, ()):
            if s in col:
                nd_m[i, col[s]] = True
            else:
                miss_nd.setdefault(s, []).append(d)
    np.savez(TN / "tn_panel.npz", cal=cal.values, syms=np.array(syms), adj=adj.astype("float64"),
             mcap=mcap.astype("float64"), me=me.values, sp_m=sp_m, nd_m=nd_m)
    print(f"panel {T} sessions x {N} symbols; {len(me)} month-ends {me[0].date()}..{me[-1].date()}")
    pd.DataFrame(spins, columns=["symbol", "date", "num", "den"]).to_csv(TN / "odd_splits.csv", index=False)
    json.dump({"sp": {k: [str(v[0].date()), str(v[-1].date()), len(v)] for k, v in miss_sp.items()},
               "ndx": {k: [str(v[0].date()), str(v[-1].date()), len(v)] for k, v in miss_nd.items()}},
              open(TN / "members_without_data.json", "w"), indent=0)
    print(f"S&P member tickers with no FMP file: {len(miss_sp)}; Nasdaq-100: {len(miss_nd)}")


# odd-ratio "splits" that really are share splits / stock dividends (cap history is already right)
# (class-C share dividends, merger exchange ratios, share consolidations, ADR ratio changes)
NOT_SPIN = {("GOOGL", "2014-04-03"), ("LBTYA", "2014-03-04"), ("WBD", "2014-08-07"), ("ASML", "2012-11-29"),
            ("BK", "2007-07-02"), ("CHTR", "2016-05-18"), ("GEN", "2013-04-04"), ("HON", "2026-06-28"),
            ("JCI", "2016-09-06"), ("RIG", "2007-11-27"), ("RYAAY", "2024-09-30"), ("RYAAY", "2015-10-28"),
            ("TRI", "2023-06-23"), ("TRI", "2018-11-27"), ("DD", "2019-06-03"), ("DELL", "2018-12-28")}

# Time Warner: Time Warner Cable (2009-03) and AOL (2009-12) spin-offs; checked against $78B at 2005-06
# Kraft Foods -> Mondelez: Kraft Foods Group spin-off (2012-10); checked against $73B at 2012-09
MANUAL = {"TWX": [("2009-03-30", 1.31), ("2009-12-10", 1.03)], "MDLZ": [("2012-10-02", 1.52)]}
ADJ_FIX = {"MDLZ": ("2012-10-02", 2.0778)}

if __name__ == "__main__":
    main()
