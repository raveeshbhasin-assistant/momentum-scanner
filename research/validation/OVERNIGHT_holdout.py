"""Pre-registered holdout test (preregistration_overnight.json): overnight / next-close follow-through after four event types,
2014-09-01 .. 2024-08-31 on bars1d_10y. Run ONCE. Costs 5 bp per side. Benchmark = SPY over the identical leg."""
import os, glob, numpy as np, pandas as pd, warnings, yfinance as yf; warnings.filterwarnings("ignore")
SP = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); R = os.path.join(SP, "research"); D = os.path.join(SP, "bars1d_10y")
A, B = "2014-09-01", "2024-08-31"; COST = 0.0005; rng = np.random.default_rng(0)
import time
spy=None
for attempt in range(2):
    try:
        time.sleep(15*attempt)
        cand = yf.download("SPY", start="2014-01-01", end="2024-09-05", interval="1d", progress=False, auto_adjust=False, threads=False)
        if cand is not None and not cand.empty:
            if isinstance(cand.columns, pd.MultiIndex): cand.columns = [c[0] for c in cand.columns]
            spy = cand; break
    except Exception as ex: print("SPY attempt failed:", str(ex)[:80])
BENCH = "SPY"
if spy is not None:
    spy.index = pd.to_datetime(spy.index, utc=True).date; spy = spy[["Open", "Close"]].astype(float); spy = spy[~spy.index.duplicated()]
    spy_on = (spy.Open.shift(-1) / spy.Close - 1); spy_nc = (spy.Close.shift(-1) / spy.Close - 1); print("SPY rows", len(spy))
else:
    BENCH = "universe equal-weighted (SPY unavailable: Yahoo throttled)"; spy_on = spy_nc = None; print("SPY unavailable -> universe EW benchmark")
bench_sum = {}; bench_cnt = {}
rows = []; nfiles = 0
for f in glob.glob(os.path.join(D, "*.csv")):
    d = pd.read_csv(f, index_col=0, parse_dates=True)
    if len(d) < 250: continue
    nfiles += 1; d = d.astype(float); d.index = d.index.date; t = os.path.basename(f)[:-4]
    pc = d.Close.shift(1); adv = (d.Close * d.Volume).rolling(20).mean().shift(1); v20 = d.Volume.rolling(20).mean().shift(1)
    base = (pc >= 5) & (adv >= 5e7) & (d.Close.rolling(250).count() >= 250)
    ret = d.Close / pc - 1; gap = d.Open / pc - 1; rngp = (d.Close - d.Low) / (d.High - d.Low).replace(0, np.nan); vr = d.Volume / v20
    on1 = d.Open.shift(-1) / d.Close - 1; nc1 = d.Close.shift(-1) / d.Close - 1
    if spy_on is None:
        bm = pd.DataFrame({"on": on1[base.values], "nc": nc1[base.values]}).dropna()
        for dt, r_ in zip(bm.index, bm.values):
            a_ = bench_sum.setdefault(dt, [0.0, 0.0]); a_[0] += r_[0]; a_[1] += r_[1]; bench_cnt[dt] = bench_cnt.get(dt, 0) + 1
    ev = {"E1_big_catalyst": base & (gap >= 0.08) & (vr >= 3) & (d.Close > d.Open),
          "E2_gapup_held": base & (gap >= 0.03) & (d.Close >= d.Open) & (vr >= 2),
          "E3_strong_close": base & (ret >= 0.03) & (rngp >= 0.8) & (vr >= 2),
          "E4_cand": base & (vr >= 2)}
    for name, m in ev.items():
        idx = d.index[m.values]
        if not len(idx): continue
        sub = pd.DataFrame({"date": idx, "ticker": t, "event": name, "ret": ret.loc[idx].values, "on1": on1.loc[idx].values, "nc1": nc1.loc[idx].values})
        rows.append(sub)
X = pd.concat(rows, ignore_index=True); X["date_s"] = X.date.astype(str); X = X[(X.date_s >= A) & (X.date_s <= B)].dropna(subset=["on1", "nc1"])
# E4: top-20 by day return among candidates per day
e4 = X[X.event == "E4_cand"].sort_values(["date_s", "ret"], ascending=[True, False]).groupby("date_s").head(20).assign(event="E4_top20_ret_vol")
X = pd.concat([X[X.event != "E4_cand"], e4], ignore_index=True)
if spy_on is None:
    spy_on = pd.Series({k: v[0] / bench_cnt[k] for k, v in bench_sum.items()}); spy_nc = pd.Series({k: v[1] / bench_cnt[k] for k, v in bench_sum.items()})
X["spy_on"] = X.date.map(spy_on); X["spy_nc"] = X.date.map(spy_nc)
print("benchmark:", BENCH, "| mapped share", X.spy_on.notna().mean().round(3))
X["X1_net"] = ((1 + X.on1) * (1 - COST) / (1 + COST) - 1) - X.spy_on      # excess, net of 5 bp/side
X["X2_net"] = ((1 + X.nc1) * (1 - COST) / (1 + COST) - 1) - X.spy_nc
X["X1_raw"] = X.on1; X["X2_raw"] = X.nc1
X["q"] = pd.to_datetime(X.date_s).dt.to_period("Q").astype(str); X["y"] = X.date_s.str[:4]
X.to_csv(os.path.join(R, "OVERNIGHT_holdout_trades.csv.gz"), index=False, compression="gzip")
def boot(x, g, n=1000):
    ug = np.unique(g); gm = {k: x[g == k] for k in ug}; ms = np.array([np.concatenate([gm[k] for k in rng.choice(ug, len(ug))]).mean() for _ in range(n)])
    return np.percentile(ms, 2.5), np.percentile(ms, 97.5), (ms <= 0).mean()
cells = []
for ev_, g in X.groupby("event"):
    for exit_, col in (("X1_overnight", "X1_net"), ("X2_next_close", "X2_net")):
        x = g[col].values * 100; lo, hi, p = boot(x, g.q.values); qm = g.groupby("q")[col].mean();
        # per-capital K=5: first by E1 membership not applicable within a cell; take top-5 by day return per day, 20% each
        top = g.sort_values(["date_s", "ret"], ascending=[True, False]).groupby("date_s").head(5)
        daily = top.groupby("date_s")[col.replace("_net", "_raw")].apply(lambda s: ((1 + s) * (1 - COST) / (1 + COST) - 1).mean() * min(len(s), 5) / 5 * 1.0)  # 20% per position, up to 5 positions
        eq = np.cumprod(1 + daily.values); dd = (eq / np.maximum.accumulate(eq) - 1).min()
        cells.append(dict(cell=f"{ev_}/{exit_}", n=len(g), days=g.date_s.nunique(), quarters=g.q.nunique(), mean_excess_net_pct=round(x.mean(), 3), ci_low=round(lo, 3), ci_high=round(hi, 3), p_one_sided=round(p, 4),
                          quarters_positive_share=round((qm > 0).mean(), 3), mean_raw_pct=round(g[col.replace("_net", "_raw")].mean() * 100, 3), win_share=round((g[col] > 0).mean(), 3),
                          K5_total_return_pct=round((eq[-1] - 1) * 100, 1), K5_maxdd_pct=round(dd * 100, 1), K5_days=len(daily)))
C = pd.DataFrame(cells)
pv = C.p_one_sided.values; order = np.argsort(pv); m = len(pv); adj = np.empty(m); run = 0
for rank, i in enumerate(order):
    run = max(run, pv[i] * (m - rank)); adj[i] = min(1.0, run)
C["p_holm"] = adj.round(4)
C["pass"] = (C.ci_low > 0) & (C.quarters_positive_share >= 0.6) & (C.n >= 500) & (C.p_holm < 0.05)
C.to_csv(os.path.join(R, "OVERNIGHT_holdout_cells.csv"), index=False)
pd.set_option("display.width", 260); pd.set_option("display.max_columns", 30)
print(f"files used {nfiles}; window {A}..{B}; events total {len(X)}")
print(C.to_string(index=False))
yearly = X.groupby(["event", "y"])[["X1_net", "X2_net"]].mean().mul(100).round(3).unstack("y")
print("\nyearly mean excess net % (X1 overnight / X2 next close):"); print(yearly.to_string())
print("\nevents per year:"); print(X.groupby(["event", "y"]).size().unstack("y").to_string())
with open(os.path.join(R, "OVERNIGHT_holdout_report.md"), "w", encoding="utf-8") as fh:
    fh.write("# Overnight-after-event holdout (2014-09-01 .. 2024-08-31), run once\n\n" + C.to_string(index=False) + "\n\n## Yearly mean excess net %\n\n" + yearly.to_string() + "\n\n## Events per year\n\n" + X.groupby(["event", "y"]).size().unstack("y").to_string() + "\n")
