"""VAL3 — pre-registered U3 multi-day swing family on bars1d_all (2 years, broad universe). Single run.
Rules (signal on day T close; enter T+1 OPEN; exit at CLOSE of the holding day; 10 bp per side; prev_close>=5; ADV20$>=50M):
  W0 benchmark: gap >= +3%  -> hold 3 sessions
  W1: gap >= +5%, close in top third of day range, volume >= 2x ADV20 -> hold 3
  W2: close > prior 20d high AND ret_20d > SPY ret_20d AND close > MA50 -> hold 5; max 10 new entries/day by 20d RS
  W3: gap >= +8%, volume >= 3x ADV20, close > open -> hold 5
Benchmark: SPY over the identical window (T+1 open -> exit close). Survivorship: names with >= 480 daily rows only (share excluded reported).
"""
import os, glob, numpy as np, pandas as pd, warnings; warnings.filterwarnings("ignore")
SP = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); R = os.path.join(SP, "research"); D1 = os.path.join(SP, "bars1d_all")
COST = 0.0010; rng = np.random.default_rng(0)
spy = pd.read_csv(os.path.join(SP, "bars1d", "SPY.csv"), index_col=0); spy.index = pd.to_datetime(spy.index, utc=True).date; spy = spy[~spy.index.duplicated()]
spy_o, spy_c = spy.Open.astype(float), spy.Close.astype(float); spy_dates = np.array(sorted(spy.index)); spy_pos = {d: i for i, d in enumerate(spy_dates)}
spy_ret20 = spy_c.pct_change(20)
files = glob.glob(os.path.join(D1, "*.csv")); n_all = len(files); trades = []; kept = 0
for f in files:
    t = os.path.basename(f)[:-4]
    d = pd.read_csv(f, index_col=0, parse_dates=True)
    if len(d) < 480: continue
    kept += 1
    d = d.astype(float); d.index = d.index.date
    d["prev_close"] = d.Close.shift(1); d["adv20"] = (d.Close * d.Volume).rolling(20).mean().shift(1); d["vol20"] = d.Volume.rolling(20).mean().shift(1)
    d["gap"] = d.Open / d.prev_close - 1; d["hi20_prev"] = d.High.rolling(20).max().shift(1); d["ma50"] = d.Close.rolling(50).mean(); d["ret20"] = d.Close.pct_change(20)
    d["rng_pos"] = (d.Close - d.Low) / (d.High - d.Low).replace(0, np.nan)
    d["next_open"] = d.Open.shift(-1)
    base = (d.prev_close >= 5) & (d.adv20 >= 5e7)
    sig = {
        "W0": base & (d.gap >= 0.03),
        "W1": base & (d.gap >= 0.05) & (d.rng_pos >= 2 / 3) & (d.Volume >= 2 * d.vol20),
        "W2": base & (d.Close > d.hi20_prev) & (d.Close > d.ma50),
        "W3": base & (d.gap >= 0.08) & (d.Volume >= 3 * d.vol20) & (d.Close > d.Open),
    }
    idx = list(d.index); pos = {x: i for i, x in enumerate(idx)}
    for rule, hold in [("W0", 3), ("W1", 3), ("W2", 5), ("W3", 5)]:
        for day in d.index[sig[rule].values]:
            i = pos[day]
            if i + hold >= len(idx): continue
            if rule == "W2":
                sr = spy_ret20.get(day, np.nan)
                if not (np.isfinite(sr) and d.ret20.iloc[i] > sr): continue
            e = d.Open.iloc[i + 1] * (1 + COST); x = d.Close.iloc[i + hold] * (1 - COST)
            ed, xd = idx[i + 1], idx[i + hold]
            if ed not in spy_pos or xd not in spy_pos: continue
            sret = spy_c.iloc[spy_pos[xd]] / spy_o.iloc[spy_pos[ed]] - 1
            trades.append(dict(rule=rule, ticker=t, signal_date=str(day), entry_date=str(ed), exit_date=str(xd), ret=e and (x / e - 1), spy_ret=sret, excess=(x / e - 1) - sret,
                               rs20=d.ret20.iloc[i], gap=d.gap.iloc[i], adv20=d.adv20.iloc[i]))
T = pd.DataFrame(trades)
# W2: max 10 new entries per day ranked by 20d RS
w2 = T[T.rule == "W2"].sort_values(["signal_date", "rs20"], ascending=[True, False]).groupby("signal_date").head(10)
T = pd.concat([T[T.rule != "W2"], w2], ignore_index=True)
T["quarter"] = pd.to_datetime(T.signal_date).dt.to_period("Q").astype(str)
T.to_csv(os.path.join(R, "VAL3_trades.csv.gz"), index=False, compression="gzip")
print(f"universe files {n_all}, kept (>=480 rows) {kept} ({kept/n_all:.1%}); excluded share {1-kept/n_all:.1%}")

def boot(x, groups, n=1000):
    ug = np.unique(groups); gm = {g: x[groups == g] for g in ug}; ms = []
    for _ in range(n):
        pick = rng.choice(ug, len(ug)); ms.append(np.concatenate([gm[g] for g in pick]).mean())
    ms = np.array(ms); return np.percentile(ms, 2.5), np.percentile(ms, 97.5)
rows = []
for rule, g in T.groupby("rule"):
    qlo, qhi = boot(g.excess.values * 100, g.quarter.values); dlo, dhi = boot(g.excess.values * 100, g.entry_date.values)
    q = g.groupby("quarter").excess.mean(); qpos = int((q > 0).sum()); nq = len(q)
    # per-capital K=10 equal risk (position = 10% of capital each, max 10 open, first-come by signal date)
    open_until = []; daily = {}; taken = 0
    for _, r in g.sort_values(["signal_date", "rs20"], ascending=[True, False]).iterrows():
        open_until = [u for u in open_until if u >= r.entry_date]
        if len(open_until) >= 10: continue
        open_until.append(r.exit_date); taken += 1
        daily[r.exit_date] = daily.get(r.exit_date, 0) + r.ret * 0.10
    ser = pd.Series(daily).sort_index(); eq = np.cumprod(1 + ser.values); dd = (eq / np.maximum.accumulate(eq) - 1).min() if len(eq) else 0
    rows.append(dict(cell=rule, n=len(g), quarters=nq, quarters_positive=qpos, mean_ret_pct=round(g.ret.mean() * 100, 3), mean_spy_pct=round(g.spy_ret.mean() * 100, 3),
                     mean_excess_pct=round(g.excess.mean() * 100, 3), ci_q_low=round(qlo, 3), ci_q_high=round(qhi, 3), ci_d_low=round(dlo, 3), ci_d_high=round(dhi, 3),
                     win_share=round((g.ret > 0).mean(), 3), trades_taken_K10=taken, total_return_K10_pct=round((eq[-1] - 1) * 100, 1) if len(eq) else None, maxdd_K10_pct=round(dd * 100, 1),
                     pass_=(qlo > 0) and (qpos >= 6) and (len(g) >= 300) and (rule != "W0")))
cells = pd.DataFrame(rows); cells.to_csv(os.path.join(R, "VAL3_cells.csv"), index=False)
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
print(cells.to_string(index=False))
print("\nquarterly mean excess % by rule:"); print(T.groupby(["rule", "quarter"]).excess.mean().unstack(0).mul(100).round(2).to_string())
print("\nquarterly n:"); print(T.groupby(["rule", "quarter"]).size().unstack(0).to_string())
