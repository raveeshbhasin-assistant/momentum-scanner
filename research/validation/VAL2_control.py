"""VAL2 — U2 static-universe control arm on the live picks (post-06-17), realistic accounting.
C0: all picks. C1: K=10 first-come per day, no 12:00-13:00 entries, stop floor 0.6%. C2: C1 + tape gate (SPY and QQQ 09:30-bar close > prior close).
Entry = open of the first 5-min bar starting after batch_time (+5 bp); stop = fill*(1-stop_pct) with stop_pct=max(log stop_pct, floor); target = fill + 2.5*(fill-stop); exit 15:55 (-5 bp).
"""
import os, numpy as np, pandas as pd, warnings; warnings.filterwarnings("ignore")
SP = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); R = os.path.join(SP, "research"); D5 = os.path.join(SP, "bars5m")
COST = 0.0005; rng = np.random.default_rng(0)
pm = pd.read_csv(os.path.join(R, "picks_master.csv"), low_memory=False)
pm = pm[(pm.date >= "2026-06-17") & (pm.date <= "2026-09-11")].copy()
pm["bt_min"] = pm.batch_time.str[:2].astype(int) * 60 + pm.batch_time.str[3:5].astype(int)
pm["ticker"] = pm.ticker.replace({"SQ": "XYZ"})
def load(t):
    p = os.path.join(D5, t + ".csv")
    if not os.path.exists(p): return None
    df = pd.read_csv(p, index_col=0); df.index = pd.to_datetime(df.index, utc=True).tz_convert("America/New_York")
    return df[["Open", "High", "Low", "Close"]].astype(float)
# tape gate per day
gate = {}
S, Q = load("SPY"), load("QQQ")
for name, df in (("SPY", S), ("QQQ", Q)):
    rth = df[((df.index.hour * 60 + df.index.minute) >= 570) & (df.index.hour < 16)]
    daily_close = rth.groupby(rth.index.date).Close.last()
    first = rth[(rth.index.hour == 9) & (rth.index.minute == 30)].Close
    for ts, c in first.items():
        d = ts.date(); prev = daily_close.shift(1).get(d, np.nan)
        gate.setdefault(str(d), {})[name] = bool(np.isfinite(prev) and c > prev)
pm["tape_ok"] = [all(gate.get(d, {}).get(k, False) for k in ("SPY", "QQQ")) for d in pm.date]

def simulate(O, H, L, C, mins, j, stop, target, cost, exit_min=955):
    fill = O[j] * (1 + cost)
    if fill <= stop or fill >= target: return None
    for k in range(j, len(O)):
        if mins[k] > exit_min: break
        if k > j and O[k] <= stop: ex = O[k]; res = "LOSS"; break
        if L[k] <= stop: ex = stop; res = "LOSS"; break
        if H[k] >= target: ex = target; res = "WIN"; break
        if mins[k] == exit_min: ex = C[k]; res = "EOD"; break
    else:
        ex = C[min(len(O) - 1, k)]; res = "EOD"
    ex_net = ex * (1 - cost); risk = fill - stop
    return dict(result=res, R=(ex_net - fill) / risk, pnl_pct=(ex_net / fill - 1) * 100, stop_pct_used=risk / fill * 100)

out = []
for t, g in pm.groupby("ticker"):
    df = load(t)
    if df is None: continue
    dts = df.index.date
    for _, r in g.iterrows():
        day = df[dts == pd.Timestamp(r.date).date()]; m_all = day.index.hour * 60 + day.index.minute
        rth = day[(m_all >= 570) & (day.index.hour < 16)]
        if len(rth) < 70: continue
        O, H, L, C = (rth[c].values for c in ["Open", "High", "Low", "Close"]); mins = (rth.index.hour * 60 + rth.index.minute).values
        js = np.where(mins > r.bt_min)[0]
        if not len(js): continue
        j = js[0]
        for variant, floor in (("raw", 0.0), ("floor06", 0.6)):
            sp = max(r.stop_pct, floor) / 100
            fill = O[j] * (1 + COST); stop = fill * (1 - sp); target = fill + 2.5 * (fill - stop)
            s = simulate(O, H, L, C, mins, j, stop, target, COST)
            if s: out.append(dict(date=r.date, ticker=t, bt_min=r.bt_min, entry_min=int(mins[j]), strong=r.strong_signal, tape_ok=r.tape_ok, variant=variant, **s))
T = pd.DataFrame(out); T.to_csv(os.path.join(R, "VAL2_trades.csv.gz"), index=False, compression="gzip")
def boot(x, dates, n=1000):
    ud = np.unique(dates); gm = {d: x[dates == d] for d in ud}; ms = []
    for _ in range(n):
        pick = rng.choice(ud, len(ud)); ms.append(np.concatenate([gm[d] for d in pick]).mean())
    ms = np.array(ms); return np.percentile(ms, 2.5), np.percentile(ms, 97.5)
def firstcome(g, K):
    return g.sort_values(["date", "entry_min"]).groupby("date").head(K)
cells = []; pcs = []
for win, (a, b) in {"DISC": ("2026-06-17", "2026-07-31"), "TEST": ("2026-08-01", "2026-09-11")}.items():
    W = T[(T.date >= a) & (T.date <= b)]
    C0 = W[W.variant == "raw"]
    C1 = firstcome(W[(W.variant == "floor06") & ~((W.bt_min >= 720) & (W.bt_min < 780))], 10)
    C2 = C1[C1.tape_ok]
    for name, g in (("C0_all_realistic", C0), ("C1_K10_nomidday_floor06", C1), ("C2_C1_plus_tapegate", C2)):
        lo, hi = boot(g.R.values, g.date.values)
        cells.append(dict(cell=name, window=win, n=len(g), days=g.date.nunique(), avgR_net=round(g.R.mean(), 4), ci_low=round(lo, 4), ci_high=round(hi, 4), hit=round((g.result == "WIN").mean(), 3), stop=round((g.result == "LOSS").mean(), 3), eod=round((g.result == "EOD").mean(), 3), pnl_pct_net=round(g.pnl_pct.mean(), 4), stop_pct_med=round(g.stop_pct_used.median(), 3)))
        daily = g.groupby("date").R.sum() * 0.01
        if name == "C0_all_realistic": daily = firstcome(g, 10).groupby("date").R.sum() * 0.01
        eq = np.cumprod(1 + daily.values); dd = (eq / np.maximum.accumulate(eq) - 1).min() if len(eq) else 0
        pcs.append(dict(cell=name, window=win, K=10, total_return_pct=round((eq[-1] - 1) * 100, 2) if len(eq) else None, max_drawdown_pct=round(dd * 100, 2), days_positive_share=round((daily > 0).mean(), 3), trades=int(len(g) if name != "C0_all_realistic" else len(firstcome(g, 10)))))
cells = pd.DataFrame(cells); pcs = pd.DataFrame(pcs)
cells.to_csv(os.path.join(R, "VAL2_cells.csv"), index=False); pcs.to_csv(os.path.join(R, "VAL2_percapital.csv"), index=False)
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
print(cells.to_string(index=False)); print(); print(pcs.to_string(index=False))
print("\ntape gate keeps days:", pm.groupby("date").tape_ok.first().mean().round(3), "| picks simulated", len(T) // 2, "of", len(pm))
