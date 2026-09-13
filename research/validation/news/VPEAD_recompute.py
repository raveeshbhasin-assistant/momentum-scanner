"""VPEAD independent recompute of PEAD test cells C1 (h=5,20), C2 (h=20) + matched-control excess,
from raw sec_sub_raw_*.jsonl and daily bars. Written without reading PEAD_run.py."""
import os, json, glob, sys, time
import numpy as np, pandas as pd

BASE = "C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad"
RES = BASE + "/research"
OUT = RES + "/VPEAD_"
t0 = time.time()
def log(*a): print(f"[{time.time()-t0:6.1f}s]", *a, flush=True)

# ---------- 1. calendar ----------
spy = pd.read_csv(RES + "/EXEC_v5_SPY_2004_2026.csv")
dcol = 'date' if 'date' in spy.columns else spy.columns[0]
spy[dcol] = pd.to_datetime(spy[dcol]).dt.tz_localize(None).dt.normalize()
spy = spy.set_index(dcol).sort_index(); spy = spy[~spy.index.duplicated()]
spy = spy[spy.index >= '2004-01-01']
cal = spy.index; NC = len(cal)
cal_np = cal.values.astype('datetime64[D]')
log("SPY calendar", cal[0].date(), cal[-1].date(), NC)

# ---------- 2. bar panel ----------
cache = OUT + "panel.pkl"
if os.path.exists(cache):
    O, C, V = pd.read_pickle(cache)
else:
    frames = {}
    for d in ["bars1d_2004_2013", "bars1d_10y", "bars1d_all"]:
        n = 0
        for fp in glob.glob(f"{BASE}/{d}/*.csv"):
            tk = os.path.basename(fp)[:-4]
            try:
                df = pd.read_csv(fp)
                dc = 'date' if 'date' in df.columns else df.columns[0]
                df = df.rename(columns={dc: 'date'})[['date', 'Open', 'Close', 'Volume']]
                df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None).dt.normalize()
            except Exception as e:
                continue
            frames.setdefault(tk, []).append(df); n += 1
        log(d, "files", n)
    Os, Cs, Vs = {}, {}, {}
    for tk, lst in frames.items():
        df = pd.concat(lst).drop_duplicates('date', keep='last').set_index('date').sort_index().reindex(cal)
        Os[tk] = df['Open'].values.astype(float); Cs[tk] = df['Close'].values.astype(float); Vs[tk] = df['Volume'].values.astype(float)
    O = pd.DataFrame(Os, index=cal); C = pd.DataFrame(Cs, index=cal); V = pd.DataFrame(Vs, index=cal)
    del frames
    pd.to_pickle((O, C, V), cache)
tickers = list(O.columns); tset = set(tickers); col = {t: i for i, t in enumerate(tickers)}
log("panel", O.shape)

# ---------- 3. symbol map ----------
sym = pd.read_csv(RES + "/us_common_symbols.csv")
s2y = dict(zip(sym['symbol'].astype(str), sym['yf'].astype(str)))
def map_tk(t):
    t = str(t)
    for cand in (t, s2y.get(t), t.replace('.', '-'), s2y.get(t.replace('-', '.'))):
        if cand and cand in tset:
            return cand
    return None

# ---------- 4. events from raw SEC files ----------
rows = []; n_cik = 0; n_cik_mapped = 0; n_8ka = 0
for i in range(5):
    with open(f"{RES}/earn/sec_sub_raw_{i}.jsonl") as fh:
        for line in fh:
            rec = json.loads(line); n_cik += 1
            tk = None
            for t in rec.get('tickers', []):
                tk = map_tk(t)
                if tk: break
            if tk is None: continue
            n_cik_mapped += 1
            for f in rec['f']:
                if f.get('form') != '8-K':
                    n_8ka += 1; continue
                rows.append((rec['cik'], tk, f['a'], f['acc']))
ev = pd.DataFrame(rows, columns=['cik', 'ticker', 'acc_no', 'acc'])
ev['acc_utc'] = pd.to_datetime(ev['acc'], utc=True).dt.tz_localize(None)
log(f"CIKs {n_cik}, mapped {n_cik_mapped}, 8-K rows {len(ev)}, non-8-K skipped {n_8ka}")

# UTC -> ET with US DST rules (manual, independent of tz databases)
def nth_sunday(y, m, n):
    d = pd.Timestamp(year=y, month=m, day=1); first = d + pd.Timedelta(days=(6 - d.weekday()) % 7)
    return first + pd.Timedelta(days=7 * (n - 1))
def last_sunday(y, m):
    d = pd.Timestamp(year=y, month=m, day=1) + pd.offsets.MonthEnd(0)
    return d - pd.Timedelta(days=(d.weekday() + 1) % 7)
dst = {}
for y in range(2003, 2028):
    if y >= 2007: s, e = nth_sunday(y, 3, 2), nth_sunday(y, 11, 1)
    else: s, e = nth_sunday(y, 4, 1), last_sunday(y, 10)
    dst[y] = (s + pd.Timedelta(hours=7), e + pd.Timedelta(hours=6))  # 2:00 local -> UTC
yrs = ev['acc_utc'].dt.year.values
s_arr = np.array([dst[y][0].to_datetime64() for y in yrs]); e_arr = np.array([dst[y][1].to_datetime64() for y in yrs])
acc64 = ev['acc_utc'].values
off = np.where((acc64 >= s_arr) & (acc64 < e_arr), -4, -5)
ev['acc_et'] = ev['acc_utc'] + pd.to_timedelta(off, unit='h')
et_date = ev['acc_et'].values.astype('datetime64[D]')
tod = ev['acc_et'].dt.hour.values * 60 + ev['acc_et'].dt.minute.values + ev['acc_et'].dt.second.values / 60.0
idx_ge = np.searchsorted(cal_np, et_date, side='left')
inb = idx_ge < NC
is_td = np.zeros(len(ev), bool); is_td[inb] = cal_np[idx_ge[inb]] == et_date[inb]
R_idx = np.where(tod < 570, idx_ge, np.where((tod < 960) & is_td, idx_ge, np.where(is_td, idx_ge + 1, idx_ge)))
timing = np.where(tod < 570, 'pre', np.where((tod < 960) & is_td, 'intra', 'post'))
ev['R_idx'] = R_idx; ev['timing'] = timing
ev = ev[ev['R_idx'] < NC].copy()
ev['R'] = cal[ev['R_idx'].values]
ev_all = ev.copy()  # pre-dedupe, used for the +-5 session 8-K exclusion of controls

# dedupe: per ticker drop a filing whose R is within 5 sessions after a kept filing
ev = ev.sort_values(['ticker', 'R_idx', 'acc_utc']).reset_index(drop=True)
keep = np.zeros(len(ev), bool); last_tk = None; last_i = -999
for i, (tk, ri) in enumerate(zip(ev['ticker'].values, ev['R_idx'].values)):
    if tk != last_tk or ri - last_i > 5:
        keep[i] = True; last_tk = tk; last_i = ri
ev = ev[keep].reset_index(drop=True)
log("events after dedupe", len(ev))

# ---------- 5. panel signals ----------
Cv, Ov, Vv = C.values, O.values, V.values
ADV = (C * V).rolling(20, min_periods=15).mean().shift(1).values   # ending R-1
AVV = V.rolling(20, min_periods=15).mean().shift(1).values
prevC = C.shift(1).values
spyC = spy['Close'].values.astype(float); spyO = spy['Open'].values.astype(float)
spy_ret = spyC / np.r_[np.nan, spyC[:-1]] - 1
EXC = (Cv / prevC - 1) - spy_ret[:, None]
# 8-K reaction-day panel (all mapped 8-K filings, pre-dedupe) and +-5 session neighbourhood
E = np.zeros(Cv.shape, np.int8)
E[ev_all['R_idx'].values, ev_all['ticker'].map(col).values] = 1
NEAR = pd.DataFrame(E, index=cal).rolling(11, center=True, min_periods=1).max().values > 0
del E
log("signals ready")

ci = ev['ticker'].map(col).values; ri = ev['R_idx'].values
ev['prev_close'] = prevC[ri, ci]; ev['adv'] = ADV[ri, ci]; ev['reaction'] = EXC[ri, ci]
ev['volratio'] = Vv[ri, ci] / AVV[ri, ci]; ev['gap'] = Ov[ri, ci] / prevC[ri, ci] - 1
ev['close_R'] = Cv[ri, ci]
ev['open_R1'] = np.where(ri + 1 < NC, Ov[np.minimum(ri + 1, NC - 1), ci], np.nan)
elig = (ev['prev_close'] >= 5) & (ev['adv'] >= 50e6) & ev['reaction'].notna()
el = ev[elig].copy().reset_index(drop=True)
el['ym'] = el['R'].dt.to_period('M')
g = el.groupby('ym')['reaction']
el['n_month'] = g.transform('size'); el['q_month'] = (g.rank(method='first') - 1) / el['n_month']
el['C1'] = (el['n_month'] >= 10) & (el['q_month'] >= 0.8)
el['C2'] = (el['reaction'] >= 0.08) & (el['volratio'] >= 3.0)
el['C3'] = (el['n_month'] >= 10) & (el['q_month'] < 0.2)
el['cost'] = np.where(el['adv'] >= 100e6, 0.001, 0.002)
el['window'] = np.where(el['R'] >= '2015-01-01', 'TEST', 'DISC')
el.loc[el['R'] > '2026-08-31', 'window'] = 'POST'
ci = el['ticker'].map(col).values; ri = el['R_idx'].values
en = ri + 1
for h in (1, 5, 20):
    ex = ri + h; ok = ex < NC; exc_ = np.minimum(ex, NC - 1); enc = np.minimum(en, NC - 1)
    ret = Cv[exc_, ci] / Ov[enc, ci] - 1; sret = spyC[exc_] / spyO[enc] - 1
    net = ret - sret - el['cost'].values
    net[~ok] = np.nan
    el[f'ret_{h}'] = np.where(ok, ret, np.nan); el[f'spy_{h}'] = np.where(ok, sret, np.nan); el[f'net_{h}'] = net
log("eligible events", len(el), "TEST", (el['window'] == 'TEST').sum())

# ---------- 6. matched controls (all TEST events, sorted by R then ticker) ----------
rng = np.random.default_rng(20260913)
te = el[el['window'] == 'TEST'].sort_values(['R', 'ticker'])
ctrl_n = {}; ctrl_net = {5: {}, 20: {}}; ctrl_ids = {}
cache_r = {}
tick_arr = np.array(tickers)
for r, grp in te.groupby('R_idx', sort=True):
    em = (prevC[r] >= 5) & (ADV[r] >= 50e6) & ~np.isnan(EXC[r]) & ~np.isnan(ADV[r])
    eidx = np.where(em)[0]; n = len(eidx)
    rank = np.empty(n, int); rank[np.argsort(ADV[r, eidx], kind='stable')] = np.arange(n)
    dec = np.full(Cv.shape[1], -1); dec[eidx] = (rank * 10) // n
    near = NEAR[r]
    for _, e in grp.iterrows():
        c = col[e['ticker']]
        cand = np.where(em & (dec == dec[c]) & (np.abs(EXC[r] - e['reaction']) <= 0.01) & ~near)[0]
        cand = cand[cand != c]
        if len(cand) == 0:
            ctrl_n[e.name] = 0; continue
        pick = rng.choice(cand, size=min(5, len(cand)), replace=False)
        ctrl_n[e.name] = len(pick); ctrl_ids[e.name] = list(tick_arr[pick])
        ccost = np.where(ADV[r, pick] >= 100e6, 0.001, 0.002)
        for h in (5, 20):
            ex = r + h
            if ex >= NC or r + 1 >= NC:
                ctrl_net[h][e.name] = np.nan; continue
            cn = (Cv[ex, pick] / Ov[r + 1, pick] - 1) - (spyC[ex] / spyO[r + 1] - 1) - ccost
            ctrl_net[h][e.name] = np.nanmean(cn) if np.isfinite(cn).any() else np.nan
el['n_ctrl'] = pd.Series(ctrl_n); el['ctrl_net_5'] = pd.Series(ctrl_net[5]); el['ctrl_net_20'] = pd.Series(ctrl_net[20])
el['ce_5'] = el['net_5'] - el['ctrl_net_5']; el['ce_20'] = el['net_20'] - el['ctrl_net_20']
log("controls done")

# ---------- 7. inference ----------
def cboot(x, clus, reps=2000, seed=1):
    df = pd.DataFrame({'x': x, 'c': clus}).dropna(); agg = df.groupby('c')['x'].agg(['sum', 'count'])
    s = agg['sum'].values; n = agg['count'].values; k = len(s)
    idx = np.random.default_rng(seed).integers(0, k, size=(reps, k))
    m = s[idx].sum(1) / n[idx].sum(1)
    return np.percentile(m, 2.5), np.percentile(m, 97.5), m
def infer(sub, colname, sign=1):
    x = sub[colname].values * sign; ok = np.isfinite(x); x = x[ok]; sub = sub[ok]
    mean = x.mean()
    lo_d, hi_d, md = cboot(x, sub['R'].values); lo_q, hi_q, mq = cboot(x, sub['R'].dt.to_period('Q').astype(str).values)
    lo, hi = (lo_q, hi_q) if (hi_q - lo_q) >= (hi_d - lo_d) else (lo_d, hi_d)
    wrong = (lambda m: (m <= 0).mean() if mean > 0 else (m >= 0).mean())
    p = max(wrong(md), wrong(mq), 1 / 4000)
    yr = pd.Series(x, index=sub['R'].dt.year.values).groupby(level=0).agg(['mean', 'size'])
    yr = yr[yr['size'] >= 10]
    return dict(n=len(x), mean=100 * mean, lo=100 * lo, hi=100 * hi, p=p, years=len(yr), years_pos=(yr['mean'] > 0).mean())
res = []
T = el[el['window'] == 'TEST']
for cell, mask, sign in (('C1', T['C1'], 1), ('C2', T['C2'], 1), ('C3', T['C3'], -1)):
    for h in (1, 5, 20):
        d = infer(T[mask], f'net_{h}', sign); d.update(cell=cell, h=h, stat='net_excess_vs_SPY'); res.append(d)
        if h in (5, 20):
            sub = T[mask & T['n_ctrl'].gt(0)]
            d2 = infer(sub, f'ce_{h}', sign); d2.update(cell=cell, h=h, stat='ctrl_excess'); d2['ctrl_share'] = len(sub) / max(1, mask.sum()); res.append(d2)
resdf = pd.DataFrame(res)[['cell', 'h', 'stat', 'n', 'mean', 'lo', 'hi', 'p', 'years', 'years_pos', 'ctrl_share']]
pd.set_option('display.width', 200); pd.set_option('display.max_columns', 30)
print("\n=== VPEAD recomputed TEST cells (entry Open_{R+1}, exit Close_{R+h}, % net over SPY; ctrl rows = event net minus mean control net) ===")
print(resdf.round(3).to_string(index=False))
resdf.to_csv(OUT + "cells.csv", index=False)
el.to_pickle(OUT + "events.pkl")

# ---------- 8. audit of PEAD_trades.csv.gz ----------
tr = pd.read_csv(RES + "/PEAD_trades.csv.gz")
tr['R'] = pd.to_datetime(tr['R']); tr['acc_utc'] = pd.to_datetime(tr['acc_utc'])
print(f"\n=== PEAD_trades: {len(tr)} rows; TEST rows {(tr['window']=='TEST').sum()}; C1 TEST {((tr['window']=='TEST')&tr['C1']).sum()}; C2 TEST {((tr['window']=='TEST')&tr['C2']).sum()}")
# full-file structural checks
mine = ev.set_index('acc_no')
j = tr.join(mine[['R', 'timing', 'ticker']].rename(columns={'R': 'R_v', 'timing': 'timing_v', 'ticker': 'ticker_v'}), on='acc_no')
print("acc_no found in my deduped event set:", j['R_v'].notna().mean().round(4))
jj = j[j['R_v'].notna()]
print("R match:", (jj['R'] == jj['R_v']).mean().round(4), " timing match:", (jj['timing'] == jj['timing_v']).mean().round(4), " ticker match:", (jj['ticker'] == jj['ticker_v']).mean().round(4))
# unmatched acc_no: check against pre-dedupe set
m2 = ev_all.drop_duplicates('acc_no').set_index('acc_no')
miss = j[j['R_v'].isna()]
print("unmatched rows:", len(miss), " of which present pre-dedupe:", miss['acc_no'].isin(m2.index).sum())
tr['R_idx'] = np.searchsorted(cal_np, tr['R'].values.astype('datetime64[D]'))
trs = tr.sort_values(['ticker', 'R_idx']); dd = trs.groupby('ticker')['R_idx'].diff()
print("dedupe violations in PEAD_trades (same ticker, R within 5 sessions):", int((dd <= 5).sum()))
# overlap of my TEST-eligible set with theirs
mine_te = set(el.loc[el['window'] == 'TEST', 'acc_no']); their_te = set(tr.loc[tr['window'] == 'TEST', 'acc_no'])
print(f"TEST event sets: mine {len(mine_te)} theirs {len(their_te)} both {len(mine_te & their_te)}")
# their filters through R-1 (whole file): prev close and ADV recomputed by me
ci_t = tr['ticker'].map(col); okc = ci_t.notna()
cit = ci_t[okc].astype(int).values; rit = tr.loc[okc, 'R_idx'].values
adv_me = ADV[rit, cit]; pc_me = prevC[rit, cit]
print("ADV20(R-1) matches to 1e-6 rel:", np.nanmean(np.abs(adv_me / tr.loc[okc, 'adv_d'].values - 1) < 1e-6).round(4), " prev_close>=5:", np.nanmean(pc_me >= 5).round(4), " adv>=50M:", np.nanmean(adv_me >= 50e6).round(4))
# 40-row deep audit
rs = np.random.default_rng(7); pick = rs.choice(len(tr), 40, replace=False)
chk_names = ['acc_et', 'timing', 'R', 'prev_close>=5', 'adv_d', 'tier_cost', 'close_R', 'open_R1', 'reaction', 'gap', 'volratio', 'C1flag', 'C2flag', 'ret_o_5', 'spy_o_5', 'net_o_5', 'ret_o_20', 'spy_o_20', 'net_o_20', 'ce_20=net-ctrl']
tab = []
for i in pick:
    r = tr.iloc[i]; c = col.get(r['ticker']); ridx = int(r['R_idx']); res_ = {}
    mrow = mine.loc[r['acc_no']] if r['acc_no'] in mine.index else None
    res_['acc_et'] = (mrow is not None) and abs((pd.Timestamp(r['acc_et']) - mrow['acc_et']).total_seconds()) < 1
    res_['timing'] = (mrow is not None) and r['timing'] == mrow['timing']
    res_['R'] = (mrow is not None) and r['R'] == mrow['R']
    if c is None:
        tab.append((r['ticker'], str(r['R'].date()), r['window'], 'NO BAR COL', res_)); continue
    res_['prev_close>=5'] = prevC[ridx, c] >= 5
    res_['adv_d'] = abs(ADV[ridx, c] / r['adv_d'] - 1) < 1e-6
    res_['tier_cost'] = np.isclose(r['tier_cost'], 0.001 if r['adv_d'] >= 100e6 else 0.002)
    res_['close_R'] = abs(Cv[ridx, c] / r['close_R'] - 1) < 1e-6
    res_['open_R1'] = abs(Ov[ridx + 1, c] / r['open_R1'] - 1) < 1e-6
    res_['reaction'] = abs(EXC[ridx, c] - r['reaction']) < 1e-6
    res_['gap'] = abs(Ov[ridx, c] / prevC[ridx, c] - 1 - r['gap']) < 1e-6
    res_['volratio'] = abs(Vv[ridx, c] / AVV[ridx, c] - r['volratio']) < 1e-4
    res_['C1flag'] = bool(r['C1']) == bool((r['n_month'] >= 10) and (r['q_month'] >= 0.8))
    res_['C2flag'] = bool(r['C2']) == bool((r['reaction'] >= 0.08) and (r['volratio'] >= 3.0))
    for h in (5, 20):
        ex = ridx + h
        if ex >= NC or not np.isfinite(r[f'ret_o_{h}']):
            res_[f'ret_o_{h}'] = res_[f'spy_o_{h}'] = res_[f'net_o_{h}'] = 'na'; continue
        res_[f'ret_o_{h}'] = abs(Cv[ex, c] / Ov[ridx + 1, c] - 1 - r[f'ret_o_{h}']) < 1e-6
        res_[f'spy_o_{h}'] = abs(spyC[ex] / spyO[ridx + 1] - 1 - r[f'spy_o_{h}']) < 1e-6
        res_[f'net_o_{h}'] = abs(r[f'ret_o_{h}'] - r[f'spy_o_{h}'] - r['tier_cost'] - r[f'net_o_{h}']) < 1e-6
    res_['ce_20=net-ctrl'] = 'na' if not np.isfinite(r['ce_20']) else abs(r['net_o_20'] - r['ctrl_net_20'] - r['ce_20']) < 1e-6
    tab.append((r['ticker'], str(r['R'].date()), r['window'], r['timing'], res_))
fails = {}
print("\n=== 40-row audit (F = mismatch) ===")
for tk, R, w, tm, d in tab:
    bad = [k for k, v in d.items() if v is False]
    for k in bad: fails[k] = fails.get(k, 0) + 1
    print(f"{tk:6s} {R} {w:4s} {tm:6s} {'OK' if not bad else 'FAIL:' + ','.join(bad)}")
print("fail counts by check:", fails if fails else "none")
# control comparison for audited TEST rows: my ctrl_net_20 vs theirs (different RNG draw -> only loose agreement expected)
elk = el.set_index('acc_no')
cmp = []
for i in pick:
    r = tr.iloc[i]
    if r['acc_no'] in elk.index and np.isfinite(r['ctrl_net_20']) and np.isfinite(elk.loc[r['acc_no'], 'ctrl_net_20']):
        cmp.append((r['ticker'], str(r['R'].date()), int(r['n_ctrl']), int(elk.loc[r['acc_no'], 'n_ctrl']), round(100 * r['ctrl_net_20'], 2), round(100 * elk.loc[r['acc_no'], 'ctrl_net_20'], 2), ctrl_ids.get(elk.index.get_loc(r['acc_no']) if False else -1, '')))
print("\naudited rows with controls: ticker,R,n_ctrl(theirs),n_ctrl(mine),ctrl_net_20% theirs,mine")
for x in cmp: print(x[:6])
# whole-file control agreement
jj2 = tr[tr['window'] == 'TEST'].join(elk[['n_ctrl', 'ctrl_net_20', 'ctrl_net_5']].rename(columns=lambda s: s + '_v'), on='acc_no')
both = jj2['ctrl_net_20'].notna() & jj2['ctrl_net_20_v'].notna()
print("TEST rows with controls: theirs", jj2['ctrl_net_20'].notna().mean().round(3), " mine", jj2['ctrl_net_20_v'].notna().mean().round(3), " n_ctrl exact match share", (jj2['n_ctrl'] == jj2['n_ctrl_v']).mean().round(3))
print("corr(ctrl_net_20 theirs, mine) =", np.corrcoef(jj2.loc[both, 'ctrl_net_20'], jj2.loc[both, 'ctrl_net_20_v'])[0, 1].round(3), " mean theirs/mine % (C1):", (100 * jj2.loc[both & jj2['C1'], 'ctrl_net_20'].mean()).round(3), (100 * jj2.loc[both & jj2['C1'], 'ctrl_net_20_v'].mean()).round(3))
# their C1 cell recomputed from their own trade file (sanity)
tt = tr[(tr['window'] == 'TEST') & tr['C1']]
print("their-file C1 h5/h20 mean net %:", (100 * tt['net_o_5'].mean()).round(3), (100 * tt['net_o_20'].mean()).round(3), " ce_20 mean %:", (100 * tt['ce_20'].mean()).round(3))
log("done")
