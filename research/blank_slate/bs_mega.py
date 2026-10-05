"""Does the index-fund pullback rule (D1) carry over to the 20 largest S&P 500 companies?

Membership is point in time: each month, the 20 largest S&P 500 companies by market value at
the previous month-end (FMP daily market-cap history, which starts 2012-06; S&P 500 list as of
2026-09, so a company that has since left the index is missing - none of the large ones has).
Alphabet's two share classes count once (GOOGL).

Two versions are the test; neither is fitted to stock outcomes:
  M1 literal        5-session return <= -3%, close > 200-session average, buy next open, hold 5
  M2 same severity  the fall is scaled to each stock's own volatility: 5-session return
                    <= -K x (63-session daily volatility x sqrt 5), where K is how many of its own
                    standard deviations a 3% five-session fall is for the index funds (their
                    median over 2012-2019). A stock moves more than an index, so -3% is a much
                    more ordinary event for a stock than for SPY.
Everything else printed is descriptive.

  python research/blank_slate/bs_mega.py train      signals 2012-07-02 .. 2019-09-30
  python research/blank_slate/bs_mega.py holdout    signals 2020-01-02 .. 2026-06-30 (run once)
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from bs_common import BS, HOLD, load_etf
from bs_dip import INDEX, boot
from wf_common import WF, _finish, load_survivor

TRAIN = ("2012-07-02", "2019-09-30")
TOP_N, H, SMA, LOOKBACK = 20, 5, 200, 5
COST = 0.0002  # per side; these are the most liquid stocks in the market
RESULT = BS / "mega_holdout_report.csv"
VARIANTS = [("M1 literal -3%", "fixed", -0.03, True, True), ("M2 same severity", "vol", None, True, True),
            ("fixed -5%", "fixed", -0.05, True, False), ("fixed -7%", "fixed", -0.07, True, False),
            ("vol-scaled, 1.5x the index severity", "vol15", None, True, False),
            ("M1 without the 200-day condition", "fixed", -0.03, False, False),
            ("M2 without the 200-day condition", "vol", None, False, False)]


def sp500():
    lines = (Path(__file__).resolve().parents[2] / "ignition" / "universe.txt").read_text().splitlines()
    return set(lines[lines.index("# sp500") + 1].split())


def load_stock(t):
    b = load_survivor(t)
    if b is not None:
        return b
    p = BS / "extra" / f"{t}.csv"
    if not p.exists():
        import yfinance as yf
        p.parent.mkdir(exist_ok=True)
        d = yf.Ticker(t).history(start="2004-01-01", auto_adjust=False, actions=True)
        d.index = pd.to_datetime(d.index).tz_localize(None)
        d.index.name = "date"
        d.to_csv(p)
    d = pd.read_csv(p, parse_dates=["date"]).set_index("date").rename(columns={
        "Open": "open", "High": "high", "Low": "low", "Close": "close", "Adj Close": "adj", "Volume": "volume",
        "Stock Splits": "split"})
    d = d[d.close > 0]
    return _finish(d, d.split.where(d.split > 0, 1.0))


def membership():
    """{month period: [tickers]} - top 20 by market value at the previous month-end."""
    m = pd.read_pickle(BS / "mcap_daily.pkl")
    m = m[[c for c in m.columns if c in sp500() and c != "GOOG"]]
    me = m.resample("ME").last()
    return {(d + pd.offsets.MonthBegin(1)).to_period("M"): list(r.dropna().nlargest(TOP_N).index)
            for d, r in me.iterrows()}


def index_k(window):
    """How many of its own 5-session standard deviations a 3% fall is, for the index funds."""
    ks = []
    for s in INDEX:
        c = load_etf(s).close
        v = (c / c.shift(1) - 1).rolling(63).std() * np.sqrt(LOOKBACK)
        ks.append((0.03 / v)[(v.index >= window[0]) & (v.index <= window[1])].dropna())
    return float(pd.concat(ks).median())


def run(window, k):
    mem = membership()
    names = sorted({t for v in mem.values() for t in v})
    spy = load_etf("SPY")
    spy_ao = spy.open * spy.adj / spy.close
    lo, hi = pd.Timestamp(window[0]), pd.Timestamp(window[1])
    data = {}
    for t in names:
        b = load_stock(t)
        if b is None:
            print("no bars for", t)
            continue
        c = b.close
        vol5 = (c / c.shift(1) - 1).rolling(63).std() * np.sqrt(LOOKBACK)
        ym = b.index.to_period("M")
        data[t] = pd.DataFrame({
            "r5": c / c.shift(LOOKBACK) - 1, "up": c > c.rolling(SMA).mean(), "vol5": vol5,
            "member": [t in mem.get(p, ()) for p in ym],
            "fwd": b.adj.shift(-H) / (b.open * b.adj / b.close).shift(-1) - 1 - 2 * COST,
            "spy_fwd": (spy.adj.shift(-H) / spy_ao.shift(-1) - 1).reindex(b.index)})
    rng = np.random.default_rng(20261005)
    rows, yearly = [], {}
    for name, kind, thr, uptrend, primary in VARIANTS:
        trades, base = [], []
        for t, d in data.items():
            inwin = d.member & (d.index >= lo) & (d.index <= hi)
            base.append(d.fwd[inwin].dropna())
            limit = thr if kind == "fixed" else -(k if kind == "vol" else 1.5 * k) * d.vol5
            sig = inwin & (d.r5 <= limit) & (d.up if uptrend else True)
            last = -99
            for i in np.flatnonzero(sig.to_numpy()):
                if i - last < H or pd.isna(d.fwd.iloc[i]):
                    continue                      # still holding the previous trade in this stock
                trades.append((t, d.index[i], d.fwd.iloc[i], d.spy_fwd.iloc[i], d.r5.iloc[i]))
                last = i
        tr = pd.DataFrame(trades, columns=["ticker", "signal", "ret", "spy", "r5"])
        b = pd.concat(base)
        if len(tr) == 0:
            continue
        win = (tr.ret > 0).astype(float)
        bw, bm = float((b > 0).mean()), float(b.mean())
        res = {}
        for nm, x in (("lift", win - bw), ("ret", tr.ret), ("edge", tr.ret - bm), ("vs_spy", tr.ret - tr.spy)):
            a1 = boot(x, tr.signal.dt.to_period("M").astype(str), rng)
            a2 = boot(x, tr.signal.dt.to_period("W").astype(str), rng)
            res[nm] = (min(a1[0], a2[0]), max(a1[1], a2[1]), max(a1[2], a2[2]))
        yr = tr.assign(y=tr.signal.dt.year, win=win).groupby("y").agg(n=("win", "size"), win=("win", "mean"),
                                                                     mean=("ret", "mean"))
        yy = yr[yr.n >= 5]
        years = (hi - lo).days / 365.25
        rows.append({"rule": name, "primary": primary, "trades": len(tr), "per_year": len(tr) / years,
                     "weeks": int(tr.signal.dt.to_period("W").nunique()),
                     "median_fall": float(tr.r5.median()), "hit": win.mean(), "base_hit": bw,
                     "lift": win.mean() - bw, "lift_lo": res["lift"][0], "lift_hi": res["lift"][1],
                     "p_lift": res["lift"][2], "mean": tr.ret.mean(), "mean_lo": res["ret"][0],
                     "mean_hi": res["ret"][1], "base_mean": bm, "edge": tr.ret.mean() - bm,
                     "edge_lo": res["edge"][0], "p_edge": res["edge"][2],
                     "beat_spy_share": float((tr.ret > tr.spy).mean()), "mean_vs_spy": (tr.ret - tr.spy).mean(),
                     "vs_spy_lo": res["vs_spy"][0], "vs_spy_hi": res["vs_spy"][1],
                     "worst": tr.ret.min(), "years_above_base": float((yy.win > bw).mean()) if len(yy) else np.nan})
        yearly[name] = yr
        tr.to_csv(BS / f"mega_trades_{name.split()[0]}_{window[0][:4]}.csv", index=False)
    return pd.DataFrame(rows), yearly, len(data)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 40)
    pd.set_option("display.float_format", "{:.4f}".format)
    k = index_k(TRAIN)
    show = ["rule", "trades", "per_year", "weeks", "median_fall", "hit", "base_hit", "lift", "lift_lo", "lift_hi",
            "p_lift", "mean", "mean_lo", "base_mean", "edge", "p_edge", "beat_spy_share", "mean_vs_spy", "vs_spy_lo",
            "worst", "years_above_base"]
    if mode == "train":
        d, yearly, n = run(TRAIN, k)
        d.to_csv(BS / "mega_train_report.csv", index=False)
        print(f"index-fund severity K = {k:.2f} standard deviations; {n} companies were ever in the top 20")
        print(d[show].to_string(index=False))
    elif mode == "holdout":
        if RESULT.exists():
            sys.exit("holdout already run: it is run once")
        d, yearly, n = run(HOLD, k)
        prim = d.primary.to_numpy()
        p = d.loc[prim, "p_lift"].fillna(1.0).to_numpy()
        order, adj, runmax = np.argsort(p), np.empty(len(p)), 0.0
        for rank, i in enumerate(order):
            runmax = max(runmax, (len(p) - rank) * p[i])
            adj[i] = min(runmax, 1.0)
        d.loc[prim, "holm_p"] = adj
        verdict = {}
        for _, r in d[prim].iterrows():
            g = {"trades>=60": r.trades >= 60, "hit>=60%": r.hit >= 0.60, "lift_holm_p<0.05": r.holm_p < 0.05,
                 "mean_trade_lo>0": r.mean_lo > 0, "edge_vs_any_day_lo>0": r.edge_lo > 0,
                 "years>=60%": r.years_above_base >= 0.6}
            g = {a: bool(b) for a, b in g.items()}
            verdict[r.rule] = {"verdict": "CONFIRMED" if all(g.values()) else
                               "PARTIAL" if g["trades>=60"] and g["lift_holm_p<0.05"] else "NOT CONFIRMED", "gates": g}
        d.to_csv(RESULT, index=False)
        (BS / "mega_holdout_verdict.json").write_text(json.dumps(verdict, indent=1))
        print(f"index-fund severity K = {k:.2f}; {n} companies were ever in the top 20")
        print(d[show + ["holm_p"]].to_string(index=False))
        for nm in ("M1 literal -3%", "M2 same severity"):
            print(nm, yearly[nm].round(3).T.to_string())
        print(json.dumps(verdict, indent=1))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
