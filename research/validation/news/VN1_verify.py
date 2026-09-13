"""VN1: independent recomputation of N1 (Chan 2003 news vs no-news) from raw filings + bars.
Written without reading N1_run.py. Uses only preregistration_news_vs_nonews.json definitions."""
import os, sys, json, time, pickle, hashlib, gzip
import numpy as np, pandas as pd

BASE = "C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad"
R = BASE + "/research"
OUT = R + "/VN1_"
t0 = time.time()
def log(*a):
    print(f"[{time.time()-t0:6.1f}s]", *a, flush=True)

pre = open(R + "/preregistration_news_vs_nonews.json", "rb").read()
log("prereg sha256", hashlib.sha256(pre).hexdigest(), "sidecar:", open(R + "/N1_prereg_sha256.txt").read().strip()[:80])

# ---------------- calendar ----------------
spy = pd.read_csv(R + "/EXEC_v5_SPY_2004_2026.csv", parse_dates=["date"])
spy = spy[spy.date >= "2004-01-01"].sort_values("date").drop_duplicates("date").reset_index(drop=True)
cal = spy.date.values.astype("datetime64[D]")
N = len(cal)
spy_close = spy.Close.values.astype(float)
spy_open = spy.Open.values.astype(float)
spy_ret = spy_close / np.r_[np.nan, spy_close[:-1]] - 1
log("calendar", N, cal[0], cal[-1])
TEST0, TEST1 = np.datetime64("2015-01-01"), np.datetime64("2026-08-31")
test_mask = (cal >= TEST0) & (cal <= TEST1)

def roll_mean(x, w=20, minp=15):
    isn = np.isnan(x)
    cs = np.cumsum(np.where(isn, 0.0, x)); cn = np.cumsum(~isn)
    s = cs.copy(); c = cn.copy().astype(float)
    s[w:] = cs[w:] - cs[:-w]; c[w:] = cn[w:] - cn[:-w]
    out = np.where(c >= minp, s / np.where(c == 0, np.nan, c), np.nan)
    return out

# ---------------- bars -> events ----------------
PKL = OUT + "events_stage1.pkl"
if os.path.exists(PKL):
    ev, syms = pickle.load(open(PKL, "rb"))
    log("loaded stage1 pickle", len(ev))
else:
    dirs = [BASE + "/bars1d_2004_2013", BASE + "/bars1d_10y", BASE + "/bars1d_all"]
    syms = {}
    for d in dirs:
        for f in os.listdir(d):
            if f.endswith(".csv"):
                syms.setdefault(f[:-4], []).append(d + "/" + f)
    log("symbols with bars", len(syms))
    recs = []
    H = (1, 5, 20)
    for k, (sym, files) in enumerate(sorted(syms.items())):
        parts = [pd.read_csv(f, usecols=["date", "Open", "Close", "Volume"]) for f in files]
        df = pd.concat(parts, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"]).values.astype("datetime64[D]")
        df = df.sort_values("date").drop_duplicates("date", keep="last")
        d = df["date"].values.astype("datetime64[D]")
        pos = np.searchsorted(cal, d)
        ok = (pos < N)
        ok[ok] = cal[pos[ok]] == d[ok]
        if ok.sum() < 260:
            continue
        close = np.full(N, np.nan); open_ = np.full(N, np.nan); vol = np.full(N, np.nan)
        close[pos[ok]] = df.Close.values[ok].astype(float)
        open_[pos[ok]] = df.Open.values[ok].astype(float)
        vol[pos[ok]] = df.Volume.values[ok].astype(float)
        valid = ~np.isnan(close)
        prior_valid = np.r_[0, np.cumsum(valid)[:-1]]
        prev_close = np.r_[np.nan, close[:-1]]
        adv_inc = roll_mean(close * vol)            # window ending T (look-ahead version, for audit only)
        adv = np.r_[np.nan, adv_inc[:-1]]           # window ending T-1
        advsh = np.r_[np.nan, roll_mean(vol)[:-1]]
        d0 = close / prev_close - 1 - spy_ret
        open_T1 = np.r_[open_[1:], np.nan]
        elig = (valid & ~np.isnan(open_) & ~np.isnan(vol) & ~np.isnan(open_T1) & ~np.isnan(d0)
                & (prev_close >= 5.0) & (adv >= 50e6) & (prior_valid >= 250) & test_mask)
        up8 = elig & (d0 >= 0.08)
        up3v = elig & (d0 >= 0.03) & (vol >= 2.0 * advsh)
        dn8 = elig & (d0 <= -0.08)
        sel = np.where(up8 | up3v | dn8)[0]
        for i in sel:
            r = dict(ticker=sym, idx=int(i), T=cal[i], UP8=bool(up8[i]), UP3V=bool(up3v[i]), DN8=bool(dn8[i]),
                     day0_exc=d0[i], adv_d=adv[i], adv_incT=adv_inc[i], volratio=vol[i] / advsh[i] if advsh[i] else np.nan,
                     prev_close=prev_close[i], close_T=close[i], open_T1=open_T1[i])
            for h in H:
                j = i + h
                r[f"exit_{h}"] = close[j] if j < N else np.nan
            recs.append(r)
        if k % 500 == 0:
            log("bars", k, sym, len(recs))
    ev = pd.DataFrame(recs)
    pickle.dump((ev, syms), open(PKL, "wb"))
    log("events built", len(ev))

# ---------------- filings ----------------
fil = pd.read_csv(R + "/news8k_events.csv", usecols=["ticker", "tickers_all", "acceptance_et", "et_date", "timing", "items_new"], dtype=str)
log("filings", len(fil))
tm = fil.acceptance_et.str.slice(11, 19)
my_timing = np.where(tm < "09:30:00", "pre", np.where(tm < "16:00:00", "intra", "post"))
log("timing mismatch vs file column:", int((my_timing != fil.timing.values).sum()), "of", len(fil))
etd = pd.to_datetime(fil.et_date, errors="coerce").values.astype("datetime64[D]")
pos_ge = np.searchsorted(cal, etd, side="left")
pos_gt = np.searchsorted(cal, etd, side="right")
sess = np.where(my_timing == "post", pos_gt, pos_ge)
fil["sess"] = sess
fil["tk"] = fil.tickers_all.fillna(fil.ticker).fillna("").str.upper().str.split("|")
fx = fil[["tk", "sess", "items_new", "acceptance_et", "timing"]].explode("tk")
fx = fx[(fx.sess < N) & (fx.sess >= 0) & fx.tk.ne("")]
# alias file (optional)
alias = {}
for cand in [BASE + "/us_common_symbols.csv", R + "/us_common_symbols.csv", R + "/news8k/us_common_symbols.csv", BASE + "/../us_common_symbols.csv"]:
    if os.path.exists(cand):
        a = pd.read_csv(cand, dtype=str)
        log("alias file", cand, list(a.columns)[:8])
        cols = {c.lower(): c for c in a.columns}
        sc = next((cols[c] for c in cols if c in ("symbol", "ticker")), None)
        yc = next((cols[c] for c in cols if "yf" in c), None)
        if sc and yc:
            for s_, y_ in zip(a[sc].astype(str).str.upper(), a[yc].astype(str).str.upper()):
                if y_ in syms and s_ not in syms:
                    alias[s_] = y_
        break
def map_sym(t):
    if t in syms: return t
    t2 = t.replace(".", "-")
    if t2 in syms: return t2
    return alias.get(t)
umap = {t: map_sym(t) for t in fx.tk.unique()}
fx["sym"] = fx.tk.map(umap)
log("filing-ticker rows", len(fx), "matched", int(fx.sym.notna().sum()), "unmatched", int(fx.sym.isna().sum()), "aliases used", len(alias))
fx = fx[fx.sym.notna()].copy()
symcode = {s: i for i, s in enumerate(sorted(syms))}
fx["code"] = fx.sym.map(symcode).astype(np.int64)
fx["key"] = fx.code * N + fx.sess.astype(np.int64)
it = fx.items_new.fillna("")
fx["has202"] = it.str.split(",").apply(lambda L: "2.02" in [x.strip() for x in L])
N5 = {"3.02", "4.02", "3.01", "1.03", "5.03"}
fx["hasN5"] = it.str.split(",").apply(lambda L: any(x.strip() in N5 for x in L))
keys_all = np.unique(fx.key.values)
keys_202 = np.unique(fx.key.values[fx.has202.values])
n5_by_code = {c: np.sort(g.sess.values) for c, g in fx[fx.hasN5].groupby("code")}

ev["code"] = ev.ticker.map(symcode).astype(np.int64)
kT = ev.code.values * N + ev.idx.values
ev["news_Tm1"] = np.isin(kT - 1, keys_all)
ev["news_T"] = np.isin(kT, keys_all)
ev["news_Tp1"] = np.isin(kT + 1, keys_all)
ev["news202_T"] = np.isin(kT, keys_202)
ev["arm"] = np.where(ev.news_T, "NEWS", np.where(ev.news_Tm1 | ev.news_Tp1, "AMBIG", "NONEWS"))
ev["subarm"] = np.where(ev.arm.eq("NEWS"), np.where(ev.news202_T, "NEWS_202", "NEWS_non202"), ev.arm)
def bad60(row):
    a = n5_by_code.get(row.code)
    if a is None: return False
    lo = np.searchsorted(a, row.idx - 60, side="left"); hi = np.searchsorted(a, row.idx, side="right")
    return hi > lo
ev["bad60"] = [bad60(r) for r in ev[["code", "idx"]].itertuples(index=False)]
ev["cost"] = np.where(ev.adv_d >= 100e6, 0.001, 0.002)
ev["year"] = pd.DatetimeIndex(ev["T"]).year
ev["q"] = ev.year * 4 + (pd.DatetimeIndex(ev["T"]).month - 1) // 3
ev["day"] = ev.idx
sc_T = spy_close[ev.idx.values]; so_T1 = spy_open[np.minimum(ev.idx.values + 1, N - 1)]
for h in (1, 5, 20):
    j = np.minimum(ev.idx.values + h, N - 1)
    se = np.where(ev.idx.values + h < N, spy_close[j], np.nan)
    ev[f"gross_c_{h}"] = (ev[f"exit_{h}"] / ev.close_T - 1) - (se / sc_T - 1)
    ev[f"net_c_{h}"] = ev[f"gross_c_{h}"] - ev.cost
    ev[f"gross_o_{h}"] = (ev[f"exit_{h}"] / ev.open_T1 - 1) - (se / so_T1 - 1)
    ev[f"net_o_{h}"] = ev[f"gross_o_{h}"] - ev.cost
ev.to_pickle(OUT + "events.pkl")

# ---------------- stats ----------------
REPS, SEED = 2000, 20260913
def boot_mean(vals, clus, direction):
    codes, _ = pd.factorize(clus)
    G = codes.max() + 1
    sums = np.bincount(codes, weights=vals, minlength=G); cnts = np.bincount(codes, minlength=G).astype(float)
    rng = np.random.default_rng(SEED)
    idx = rng.integers(0, G, size=(REPS, G))
    m = sums[idx].sum(1) / cnts[idx].sum(1)
    lo, hi = np.percentile(m, [2.5, 97.5])
    p = (m <= 0).mean() if direction > 0 else (m >= 0).mean()
    return lo, hi, max(p, 0.5 / REPS)
def boot_diff(va, ca, vb, cb):
    allc = np.concatenate([ca, cb]); codes, _ = pd.factorize(allc); G = codes.max() + 1
    na = len(va); ca_, cb_ = codes[:na], codes[na:]
    sa = np.bincount(ca_, weights=va, minlength=G); na_ = np.bincount(ca_, minlength=G).astype(float)
    sb = np.bincount(cb_, weights=vb, minlength=G); nb_ = np.bincount(cb_, minlength=G).astype(float)
    rng = np.random.default_rng(SEED)
    idx = rng.integers(0, G, size=(REPS, G))
    d = sa[idx].sum(1) / na_[idx].sum(1) - sb[idx].sum(1) / nb_[idx].sum(1)
    lo, hi = np.percentile(d, [2.5, 97.5])
    return lo, hi, max((d <= 0).mean(), 0.5 / REPS)
def cell(df, col, direction):
    d = df[~df[col].isna()]
    v = d[col].values * 100
    n = len(v); mean = v.mean()
    ld, hd, pd_ = boot_mean(v, d.day.values, direction)
    lq, hq, pq = boot_mean(v, d.q.values, direction)
    wider = "day" if (hd - ld) >= (hq - lq) else "quarter"
    lo, hi = (ld, hd) if wider == "day" else (lq, hq)
    yr = d.groupby("year")[col].agg(["count", "mean"]); yr = yr[yr["count"] >= 10]
    yp = float((yr["mean"] > 0).mean()) if len(yr) else np.nan
    return dict(n=n, mean=mean, lo=lo, hi=hi, lo_day=ld, hi_day=hd, lo_q=lq, hi_q=hq, p=max(pd_, pq), p_day=pd_, p_q=pq, wider=wider, years_pos=yp)
def diff(dfa, dfb, col):
    a = dfa[~dfa[col].isna()]; b = dfb[~dfb[col].isna()]
    va, vb = a[col].values * 100, b[col].values * 100
    ld, hd, pd_ = boot_diff(va, a.day.values, vb, b.day.values)
    lq, hq, pq = boot_diff(va, a.q.values, vb, b.q.values)
    wider = "day" if (hd - ld) >= (hq - lq) else "quarter"
    lo, hi = (ld, hd) if wider == "day" else (lq, hq)
    return dict(diff=va.mean() - vb.mean(), dlo=lo, dhi=hi, dlo_day=ld, dhi_day=hd, dlo_q=lq, dhi_q=hq, dp=max(pd_, pq), dwider=wider)

U = ev[ev.UP8]
news, nonews, ambig = U[U.arm == "NEWS"], U[U.arm == "NONEWS"], U[U.arm == "AMBIG"]
log(f"TEST UP8 events: total={len(U)} NEWS={len(news)} NONEWS={len(nonews)} AMBIG={len(ambig)} NONEWS share={len(nonews)/len(U):.3f} (excl AMBIG: {len(nonews)/(len(news)+len(nonews)):.3f}); tickers={U.ticker.nunique()}")
log(f"NEWS_202={int((news.subarm=='NEWS_202').sum())} NEWS_non202={int((news.subarm=='NEWS_non202').sum())}")
rows = []
def add(label, dfa, col, direction, dfb=None, family=False):
    c = cell(dfa, col, direction)
    if dfb is not None:
        c.update(diff(dfa, dfb, col))
    c.update(label=label, family=family)
    rows.append(c)
    s = f"{label:44s} n={c['n']:5d} mean={c['mean']:+.3f} [{c['lo']:+.3f},{c['hi']:+.3f}]({c['wider'][0]}) p={c['p']:.3f} yrs+={c['years_pos']:.2f}"
    if dfb is not None:
        s += f" | diff={c['diff']:+.3f} [{c['dlo']:+.3f},{c['dhi']:+.3f}]({c['dwider'][0]}) day[{c['dlo_day']:+.3f},{c['dhi_day']:+.3f}] dp={c['dp']:.3f}"
    log(s)
for h in (1, 5, 20):
    add(f"FAMILY NEWS UP8 MOC_T h{h}", news, f"net_c_{h}", +1, nonews, family=True)
for h in (1, 5, 20):
    add(f"FAMILY NONEWS UP8 MOC_T h{h}", nonews, f"net_c_{h}", -1, family=True)
# Holm
fam = [r for r in rows if r["family"]]
ps = np.array([r["p"] for r in fam]); order = np.argsort(ps); m = len(ps); holm = np.empty(m)
running = 0.0
for rank, i in enumerate(order):
    running = max(running, ps[i] * (m - rank)); holm[i] = min(running, 1.0)
for r, hp in zip(fam, holm):
    r["holm"] = hp
    passed = (r["lo"] > 0 and r.get("dlo", -1) > 0 and r["years_pos"] >= 0.6 and r["n"] >= 500 and hp < 0.05) if "NEWS UP8" in r["label"] and "NONEWS" not in r["label"] else False
    log(f"HOLM {r['label']:36s} p={r['p']:.4f} holm={hp:.3f} pass={passed}")
add("NEWS UP8 MOO_T1 h5", news, "net_o_5", +1, nonews)
add("NONEWS UP8 MOO_T1 h1", nonews, "net_o_1", -1)
add("NEWS_202 UP8 MOC_T h20", news[news.subarm == "NEWS_202"], "net_c_20", +1, nonews)
add("NEWS_non202 UP8 MOC_T h20", news[news.subarm == "NEWS_non202"], "net_c_20", +1, nonews)
add("NEWS UP8 MOC_T h20 N5-excl", news[~news.bad60], "net_c_20", +1, nonews[~nonews.bad60])
V = ev[ev.UP3V]
add("NEWS UP3V MOC_T h5", V[V.arm == "NEWS"], "net_c_5", +1, V[V.arm == "NONEWS"])
add("NONEWS UP3V MOC_T h5", V[V.arm == "NONEWS"], "net_c_5", -1)
D = ev[ev.DN8]
add("DN8 NEWS MOC_T h20 (long-side)", D[D.arm == "NEWS"], "net_c_20", -1, D[D.arm == "NONEWS"])
add("DN8 NONEWS MOC_T h20 (long-side)", D[D.arm == "NONEWS"], "net_c_20", +1)
log(f"UP3V: NEWS={int((V.arm=='NEWS').sum())} NONEWS={int((V.arm=='NONEWS').sum())} AMBIG={int((V.arm=='AMBIG').sum())}; DN8: NEWS={int((D.arm=='NEWS').sum())} NONEWS={int((D.arm=='NONEWS').sum())}")
# regime split
for lab, msk in (("2020-2024", news.year.between(2020, 2024)), ("other", ~news.year.between(2020, 2024))):
    a = news[msk]; b = nonews[nonews.year.between(2020, 2024)] if lab == "2020-2024" else nonews[~nonews.year.between(2020, 2024)]
    log(f"regime {lab}: NEWS h5={a.net_c_5.mean()*100:+.2f} h20={a.net_c_20.mean()*100:+.2f} (n={len(a)}) | NONEWS h5={b.net_c_5.mean()*100:+.2f} h20={b.net_c_20.mean()*100:+.2f} (n={len(b)})")
pd.DataFrame(rows).to_csv(OUT + "cells.csv", index=False)

# ---------------- audit vs executor events ----------------
X = pd.read_csv(R + "/N1_events.csv.gz")
X["T"] = pd.to_datetime(X["T"]).values.astype("datetime64[D]")
for c in ("UP8", "UP3V", "DN8", "news_Tm1", "news_T", "news_Tp1", "bad60"):
    X[c] = X[c].astype(str).eq("True")
XT = X[(X.window == "TEST") & X.UP8].copy()
log(f"executor TEST UP8 rows={len(XT)} arms: {XT.arm.value_counts().to_dict()}")
mine = U.copy(); mine["T"] = mine["T"].values.astype("datetime64[D]")
M = mine.merge(XT, on=["ticker", "T"], how="outer", suffixes=("_v", "_x"), indicator=True)
log("merge:", M._merge.value_counts().to_dict())
both = M[M._merge == "both"]
log(f"arm agreement: {(both.arm_v==both.arm_x).mean():.4f}; news_T agree {(both.news_T_v==both.news_T_x).mean():.4f}; news_Tm1 {(both.news_Tm1_v==both.news_Tm1_x).mean():.4f}; news_Tp1 {(both.news_Tp1_v==both.news_Tp1_x).mean():.4f}; bad60 {(both.bad60_v==both.bad60_x).mean():.4f}; cost {(both.cost_v==both.cost_x).mean():.4f}")
for c in ("day0_exc", "adv_d", "close_T", "open_T1", "net_c_1", "net_c_5", "net_c_20", "net_o_5"):
    dd = (both[c + "_v"] - both[c + "_x"]).abs()
    log(f"  max|diff| {c}: {dd.max():.3e}  (n compared {dd.notna().sum()}) ; #>1e-6: {(dd>1e-6).sum()}")
# ADV look-ahead check: executor adv_d vs my through-T-1 vs through-T
d_prev = (both.adv_d_v - both.adv_d_x).abs() / both.adv_d_x; d_inc = (both.adv_incT - both.adv_d_x).abs() / both.adv_d_x
log(f"ADV: median rel diff vs through-T-1 {d_prev.median():.2e}, vs through-T {d_inc.median():.2e}")
only_x = M[M._merge == "right_only"]; only_v = M[M._merge == "left_only"]
log("executor-only sample:", only_x[["ticker", "T", "arm_x", "day0_exc_x", "adv_d_x"]].head(8).to_string(index=False) if len(only_x) else "none")
log("mine-only sample:", only_v[["ticker", "T", "arm_v", "day0_exc_v", "adv_d_v", "prev_close"]].head(8).to_string(index=False) if len(only_v) else "none")
# 40 random rows
rng = np.random.default_rng(20260913)
samp = XT.sample(40, random_state=20260913)
fx["sym_code"] = fx.code
by_code = {c: g for c, g in fx.groupby("code")}
print("\n=== 40-ROW AUDIT (executor row vs VN1 recompute) ===")
print("ticker     T          arm_x   arm_v   nTm1/T/Tp1 x|v  d0_x   d0_v   advd_x(M) advd_v(M) adv_incT(M) net1 x|v      net5 x|v       net20 x|v      filings[T-2..T+2] (off:timing:items)")
for _, r in samp.iterrows():
    mrow = mine[(mine.ticker == r.ticker) & (mine["T"] == r["T"])]
    if len(mrow) == 0:
        print(f"{r.ticker:8s} {str(r['T'])[:10]} {r.arm:7s} MISSING-IN-VN1 d0_x={r.day0_exc:+.4f} advd_x={r.adv_d/1e6:.1f}M")
        continue
    v = mrow.iloc[0]
    code = symcode[r.ticker]; g = by_code.get(code)
    fl = ""
    if g is not None:
        gg = g[(g.sess >= v.idx - 2) & (g.sess <= v.idx + 2)]
        fl = " ".join(f"{int(s - v.idx):+d}:{t}:{str(i)[:12]}" for s, t, i in zip(gg.sess, gg.timing, gg.items_new))
    flags_x = f"{int(r.news_Tm1)}{int(r.news_T)}{int(r.news_Tp1)}"; flags_v = f"{int(v.news_Tm1)}{int(v.news_T)}{int(v.news_Tp1)}"
    def f(a, b): return f"{a*100:+.2f}|{b*100:+.2f}" if pd.notna(a) and pd.notna(b) else f"{a}|{b}"
    print(f"{r.ticker:8s} {str(r['T'])[:10]} {r.arm:7s} {v.arm:7s} {flags_x}|{flags_v}      {r.day0_exc:+.3f} {v.day0_exc:+.3f} {r.adv_d/1e6:8.1f} {v.adv_d/1e6:8.1f} {v.adv_incT/1e6:8.1f}  {f(r.net_c_1, v.net_c_1):13s} {f(r.net_c_5, v.net_c_5):13s} {f(r.net_c_20, v.net_c_20):13s} {fl[:90]}")
log("done")
