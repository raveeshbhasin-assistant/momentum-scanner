"""VAL1 — pre-registered U1 in-play intraday family (preregistration_v4.json), run per window.
Usage: python VAL1_inplay.py DISC|TEST
Accounting: entry at next-bar OPEN (+cost), skip if open beyond stop/target, same-bar -> loss, gap-aware stops,
exit 15:55 close (-cost), one position per ticker-day, cost tier by ADV (>=1e8: 5bp, else 10bp), R in OWN risk units.
"""
import os, sys, json, numpy as np, pandas as pd, warnings; warnings.filterwarnings("ignore")
SP = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); R = os.path.join(SP, "research"); D5 = os.path.join(SP, "bars5m_inplay")
WIN = sys.argv[1] if len(sys.argv) > 1 else "DISC"
MINBARS = int(os.environ.get("VAL1_MINBARS", "70")); SUF = "" if MINBARS == 70 else f"_minbars{MINBARS}"
WINDOWS = {"DISC": ("2026-06-17", "2026-07-31"), "TEST": ("2026-08-01", "2026-09-11")}
a, b = WINDOWS[WIN]
rng = np.random.default_rng(0)

# ---------- watchlist (ex-ante fields only) ----------
ip = pd.read_csv(os.path.join(R, "inplay_candidates.csv"))
ip = ip[(ip.gap_pct >= 3.0) & (ip.prev_close >= 5) & (ip.adv20_dollar >= 2e7) & (ip.date >= a) & (ip.date <= b)]
ip = ip.sort_values(["date", "gap_pct"], ascending=[True, False]).groupby("date").head(20)
have = {f[:-4] for f in os.listdir(D5)}
ip = ip[ip.ticker.isin(have)].copy()
ip["cost"] = np.where(ip.adv20_dollar >= 1e8, 0.0005, 0.0010)
print(f"{WIN}: watchlist rows {len(ip)} days {ip.date.nunique()}")

def atr14(df):
    h, l, c = df.High.values, df.Low.values, df.Close.values; pc = np.r_[np.nan, c[:-1]]
    tr = np.nanmax(np.c_[h - l, np.abs(h - pc), np.abs(l - pc)], axis=1)
    return pd.Series(tr).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean().values

def simulate(O, H, L, C, mins, j, stop, target, cost, exit_min=955):
    """Enter at open of bar j (index into RTH arrays). Returns (result, exit_px_net, fill, R_own, pnl_pct, bars)."""
    fill = O[j] * (1 + cost)
    if fill <= stop or (target is not None and fill >= target): return None
    for k in range(j, len(O)):
        if mins[k] > exit_min: break
        gap_stop = (k > j) and (O[k] <= stop)
        if gap_stop:
            ex = O[k]; res = "LOSS"; break
        hit_s = L[k] <= stop; hit_t = target is not None and H[k] >= target
        if hit_s: ex = stop; res = "LOSS"; break
        if hit_t: ex = target; res = "WIN"; break
        if mins[k] == exit_min: ex = C[k]; res = "EOD"; break
    else:
        ex = C[min(len(O) - 1, k)]; res = "EOD"
    ex_net = ex * (1 - cost)
    risk = fill - stop
    return dict(result=res, fill=fill, exit=ex_net, R=(ex_net - fill) / risk, pnl_pct=(ex_net / fill - 1) * 100, stop_pct=risk / fill * 100, bars_held=k - j + 1)

trades = []; allbars = []
for t, g in ip.groupby("ticker"):
    df = pd.read_csv(os.path.join(D5, t + ".csv"), index_col=0); df.index = pd.to_datetime(df.index, utc=True).tz_convert("America/New_York")
    df = df[["Open", "High", "Low", "Close", "Volume"]].astype(float); df["atr"] = atr14(df)
    dts = df.index.date
    for _, r in g.iterrows():
        d = pd.Timestamp(r.date).date(); day = df[dts == d]
        m_all = day.index.hour * 60 + day.index.minute
        rth = day[(m_all >= 570) & (day.index.hour < 16)]
        if len(rth) < MINBARS: continue
        O, H, L, C, V, A = (rth[c].values for c in ["Open", "High", "Low", "Close", "Volume", "atr"])
        mins = (rth.index.hour * 60 + rth.index.minute).values
        tp = (H + L + C) / 3; cv = np.cumsum(V); vwap = np.where(cv > 0, np.cumsum(tp * V) / np.where(cv > 0, cv, 1), tp)   # RTH-only VWAP (PM volume is zero in yfinance)
        rth_open = O[0]; daylow = np.minimum.accumulate(L); orh = H[:3].max(); orl = L[:3].min()
        cost = r.cost
        base = dict(date=r.date, ticker=t, gap=r.gap_pct, adv=r.adv20_dollar, cost=cost)
        def add(rule, struct, i, stop_fn, target_r):
            """i = signal bar index (closed); entry at bar i+1 open."""
            j = i + 1
            if j >= len(O) or mins[j] > 900: return
            fill = O[j] * (1 + cost)
            stop = stop_fn(fill)
            if stop is None or not np.isfinite(stop) or stop >= fill: return
            target = fill + target_r * (fill - stop) if target_r else None
            s = simulate(O, H, L, C, mins, j, stop, target, cost)
            if s: trades.append({**base, "rule": rule, "struct": struct, "sig_min": int(mins[i]), "entry_min": int(mins[j]), **s})
        # index of the 10:00 bar
        idx10 = np.where(mins == 600)[0]
        if len(idx10):
            i = idx10[0]
            if np.isfinite(A[i]) and A[i] > 0:
                add("R0", "S_a", i, lambda f: f - 2 * A[i], 2.5)
                add("R0", "S_b", i, lambda f: min(daylow[i], f * 0.99), None)
                cond = (C[i] > rth_open) and (C[i] > vwap[i]) and (C[i] >= rth_open * 1.005)
                if cond:
                    for rule, ok in [("R1", True), ("R4", r.adv20_dollar < 1e8), ("R5", r.gap_pct >= 5.0)]:
                        if not ok: continue
                        add(rule, "S_a", i, lambda f: f - 2 * A[i], 2.5)
                        add(rule, "S_b", i, lambda f: min(daylow[i], f * 0.99), None)
        # R2: first VWAP pullback 10:00 <= start < 12:00
        for i in range(len(O)):
            if mins[i] < 600 or mins[i] >= 720: continue
            if L[i] <= vwap[i] and C[i] > vwap[i] and C[i] > O[i] and np.isfinite(A[i]):
                add("R2", "S_a", i, lambda f, i=i: L[i] - 0.25 * A[i], 2.5)
                add("R2", "S_b", i, lambda f, i=i: min(L[i], f * 0.99), None)
                break
        # R3: ORB15 — first close above OR high, start >= 09:45 and < 11:00, close > VWAP
        for i in range(len(O)):
            if mins[i] < 585 or mins[i] >= 660: continue
            if C[i] > orh and C[i] > vwap[i]:
                add("R3", "S_a", i, lambda f: min(orl, f * 0.99), 2.0)
                add("R3", "S_b", i, lambda f: min(orl, f * 0.99), None)
                break
        # all-bars benchmark labels (S_a, next-bar open, costs), decision bars 09:35..14:30
        for i in range(len(O)):
            if mins[i] < 575 or mins[i] > 870 or not np.isfinite(A[i]) or A[i] <= 0: continue
            j = i + 1
            if j >= len(O): continue
            fill = O[j] * (1 + cost); stop = fill - 2 * A[i]; target = fill + 2.5 * (fill - stop)
            s = simulate(O, H, L, C, mins, j, stop, target, cost)
            if s: allbars.append(dict(date=r.date, ticker=t, sig_min=int(mins[i]), bucket=int(mins[i] // 15), R=s["R"]))

T = pd.DataFrame(trades); AB = pd.DataFrame(allbars)
T["bucket"] = T.sig_min // 15
bm = AB.groupby(["date", "bucket"]).R.mean().rename("bm_random_bar")
T = T.merge(bm, left_on=["date", "bucket"], right_index=True, how="left")
T.to_csv(os.path.join(R, f"VAL1_trades_{WIN}{SUF}.csv.gz"), index=False, compression="gzip")
AB.to_csv(os.path.join(R, f"VAL1_allbars_{WIN}{SUF}.csv.gz"), index=False, compression="gzip")

def boot(x, dates, n=1000):
    ud = np.unique(dates); groups = {d: x[dates == d] for d in ud}
    ms = []
    for _ in range(n):
        pick = rng.choice(ud, len(ud)); ms.append(np.concatenate([groups[d] for d in pick]).mean())
    ms = np.array(ms); return np.percentile(ms, 2.5), np.percentile(ms, 97.5), (ms <= 0).mean()

rows = []
r0 = {s: T[(T.rule == "R0") & (T.struct == s)] for s in ["S_a", "S_b"]}
for (rule, struct), g in T.groupby(["rule", "struct"]):
    lo, hi, p = boot(g.R.values, g.date.values)
    r0g = r0[struct]; ex_r0 = g.R.mean() - r0g.R.mean() if len(r0g) else np.nan
    ex_rand = (g.R - g.bm_random_bar).mean()
    rows.append(dict(cell=f"{rule}/{struct}", window=WIN, n=len(g), days=g.date.nunique(), avgR_net=round(g.R.mean(), 4), ci_low=round(lo, 4), ci_high=round(hi, 4), p_one_sided=round(p, 4),
                     hit=round((g.result == "WIN").mean(), 3), stop=round((g.result == "LOSS").mean(), 3), eod=round((g.result == "EOD").mean(), 3),
                     pnl_pct_net=round(g.pnl_pct.mean(), 4), stop_pct_med=round(g.stop_pct.median(), 3), excess_vs_R0=round(ex_r0, 4) if pd.notna(ex_r0) else None, excess_vs_random_bar=round(ex_rand, 4),
                     avgR_10bp=None, picks_per_day=round(len(g) / max(1, g.date.nunique()), 2)))
cells = pd.DataFrame(rows)
# Holm across the 11 non-benchmark cells (R1..R5 x structs = 10 + R2/R3 counted) on this window's one-sided p
mask = ~cells.cell.str.startswith("R0")
pv = cells.loc[mask, "p_one_sided"].values; order = np.argsort(pv); m = len(pv); adj = np.empty(m)
running = 0
for rank, idx in enumerate(order):
    running = max(running, pv[idx] * (m - rank)); adj[idx] = min(1.0, running)
cells.loc[mask, "p_holm"] = adj
cells["pass"] = mask & (cells.ci_low > 0) & (cells.n >= 300) & (cells.days >= 25) & (cells.excess_vs_R0.fillna(-1) > 0) & (cells.excess_vs_random_bar > 0)
cells.to_csv(os.path.join(R, f"VAL1_cells_{WIN}{SUF}.csv"), index=False)

# per-capital K=5 / K=10 first-come per rule/struct, 1% risk per position
pc_rows = []
for (rule, struct), g in T.groupby(["rule", "struct"]):
    for K in (5, 10):
        daily = []
        for d, gd in g.sort_values(["date", "entry_min"]).groupby("date"):
            taken = gd.head(K); daily.append(taken.R.sum() * 0.01)
        eq = np.cumprod(1 + np.array(daily)); dd = (eq / np.maximum.accumulate(eq) - 1).min()
        pc_rows.append(dict(cell=f"{rule}/{struct}", window=WIN, K=K, total_return_pct=round((eq[-1] - 1) * 100, 2), max_drawdown_pct=round(dd * 100, 2), days_positive_share=round((np.array(daily) > 0).mean(), 3), trades=int(min(len(g), sum(min(K, len(gd)) for _, gd in g.groupby("date"))))))
pc = pd.DataFrame(pc_rows); pc.to_csv(os.path.join(R, f"VAL1_percapital_{WIN}{SUF}.csv"), index=False)

# flat-cost sensitivity (10 / 15 bp) via approximate re-costing: R shifts by (extra cost x 2 sides) / stop_pct
for extra, col in [(0.0005, "avgR_10bp_approx"), (0.0010, "avgR_15bp_approx")]:
    cells[col] = [round((T[(T.rule == c.split('/')[0]) & (T.struct == c.split('/')[1])].pipe(lambda g: (g.R - (2 * extra * 100 / g.stop_pct) * (g.cost < 0.0010 + 1e-9)).mean())), 4) for c in cells.cell]
cells.to_csv(os.path.join(R, f"VAL1_cells_{WIN}{SUF}.csv"), index=False)

monthly = T.assign(month=T.date.str[:7]).groupby(["rule", "struct", "month"]).R.agg(["mean", "size"]).round(3)
pd.set_option("display.width", 220); pd.set_option("display.max_columns", 30)
print(cells[["cell", "n", "days", "avgR_net", "ci_low", "ci_high", "hit", "stop", "eod", "pnl_pct_net", "stop_pct_med", "excess_vs_R0", "excess_vs_random_bar", "avgR_10bp_approx", "p_holm", "pass"]].to_string(index=False))
print("\nper-capital:"); print(pc.to_string(index=False))
print("\nmonthly:"); print(monthly.to_string())
print("\nall-bars benchmark (watchlist, S_a, next-open, costs): n", len(AB), "avgR", round(AB.R.mean(), 4), "by bucket:", AB.groupby("bucket").R.mean().round(3).to_dict())
