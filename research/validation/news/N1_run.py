#!/usr/bin/env python
"""N1 executor: Chan (2003) news vs no-news continuation. Registered: preregistration_news_vs_nonews.json (KEY N1). Single run."""
import os, sys, json, time, hashlib, warnings
import numpy as np, pandas as pd

warnings.filterwarnings("ignore")
BASE = "C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad"
RES = BASE + "/research"
PFX = RES + "/N1_"
PREREG = RES + "/preregistration_news_vs_nonews.json"
T0 = time.time()
def log(*a): print(f"[{time.time()-T0:7.1f}s]", *a, flush=True)

reg = json.load(open(PREREG, encoding='utf-8'))
SHA = hashlib.sha256(open(PREREG, 'rb').read()).hexdigest()
open(PFX + "prereg_sha256.txt", "w").write(SHA + "  preregistration_news_vs_nonews.json\n")
log("prereg sha256", SHA)

HORIZONS = [1, 5, 20]; REPS = 2000; SEED = 20260913
DISC = (pd.Timestamp('2005-01-01'), pd.Timestamp('2014-12-31')); TEST = (pd.Timestamp('2015-01-01'), pd.Timestamp('2026-08-31'))
BAD_ITEMS = {'3.02', '4.02', '3.01', '1.03', '5.03'}
LOOKBACK = 60
ITEM_CODES = ['2.02', '7.01', '8.01', '1.01', '2.01', '5.02', '3.02', '5.07', '5.03', '2.03', '3.01', '4.02', '1.03', '9.01']

# ----------------------------------------------------------------------------- calendar & panel
spy = pd.read_csv(RES + "/EXEC_v5_SPY_2004_2026.csv", parse_dates=['date']).set_index('date').sort_index()
spy = spy[spy.index >= '2004-01-01']; spy = spy[~spy.index.duplicated()]
cal = spy.index; nd = len(cal)
spyO = spy['Open'].to_numpy(float); spyC = spy['Close'].to_numpy(float)
log("calendar", cal[0].date(), cal[-1].date(), nd)

sym2yf = {}
try:
    uni = pd.read_csv(RES + "/us_common_symbols.csv", dtype=str); sym2yf = dict(zip(uni['symbol'], uni['yf']))
except Exception:
    pass

CACHE = PFX + "cache_panel.npz"
if os.path.exists(CACHE) and '--fresh' not in sys.argv:
    z = np.load(CACHE, allow_pickle=True); On = z['O']; Cn = z['C']; Vn = z['V']; tickers = list(z['tickers'])
    log("panel from cache", Cn.shape)
else:
    dirs = [BASE + "/bars1d_2004_2013", BASE + "/bars1d_10y", BASE + "/bars1d_all"]
    files = {}
    for d in dirs:
        for f in os.listdir(d):
            if f.endswith('.csv'):
                files.setdefault(f[:-4], []).append(os.path.join(d, f))
    O, C, V = {}, {}, {}
    for i, (t, fl) in enumerate(files.items()):
        parts = []
        for p in fl:
            try:
                df = pd.read_csv(p, usecols=['date', 'Open', 'Close', 'Volume'])
                df['date'] = pd.to_datetime(df['date'], errors='coerce'); df = df.dropna(subset=['date']); parts.append(df)
            except Exception:
                continue
        if not parts:
            continue
        df = pd.concat(parts).drop_duplicates('date', keep='last').set_index('date').sort_index()
        df = df[(df['Close'] > 0) & (df['Open'] > 0)].reindex(cal)
        O[t] = df['Open'].to_numpy(float); C[t] = df['Close'].to_numpy(float); V[t] = df['Volume'].to_numpy(float)
        if i % 1000 == 0:
            log("loaded", i)
    tickers = list(O.keys())
    On = np.column_stack([O[t] for t in tickers]); Cn = np.column_stack([C[t] for t in tickers]); Vn = np.column_stack([V[t] for t in tickers])
    del O, C, V
    np.savez(CACHE, O=On, C=Cn, V=Vn, tickers=np.array(tickers, dtype=object))
    log("panel built", Cn.shape)
nt = len(tickers); col = {t: i for i, t in enumerate(tickers)}

Cdf = pd.DataFrame(Cn); Vdf = pd.DataFrame(Vn)
prevC = Cdf.shift(1)
adv_d = (Cdf * Vdf).rolling(20, min_periods=15).mean().shift(1)
adv_sh = Vdf.rolling(20, min_periods=15).mean().shift(1)
nrows = Cdf.notna().cumsum().shift(1).fillna(0)
spy_ret = pd.Series(spyC).pct_change()
exc = (Cdf / prevC - 1).sub(spy_ret, axis=0)
elig = (prevC >= 5) & (adv_d >= 50e6) & (nrows >= 250) & Cdf.notna() & Vdf.notna() & pd.DataFrame(On).notna()
volr = Vdf / adv_sh
excn = exc.to_numpy(); elign = elig.to_numpy(); advn = adv_d.to_numpy(); volrn = volr.to_numpy()
del Cdf, Vdf, prevC, adv_d, adv_sh, nrows, exc, elig, volr
log("derived matrices")

# ----------------------------------------------------------------------------- 8-K filings -> (session, ticker)
f8 = pd.read_csv(RES + "/news8k_events.csv", usecols=['cik', 'ticker', 'tickers_all', 'form', 'et_date', 'timing', 'items_new'], dtype=str)
n_f_raw = len(f8)
f8['tk'] = f8['tickers_all'].fillna(f8['ticker']).str.split('|')
f8 = f8.explode('tk'); f8['tk'] = f8['tk'].str.strip().str.upper()
def mapt(t):
    for cand in (t, sym2yf.get(t), t.replace('.', '-')):
        if cand in col:
            return col[cand]
    return -1
mp = {t: mapt(t) for t in f8['tk'].unique()}
f8['ti'] = f8['tk'].map(mp).astype(int)
n_unmapped = int((f8['ti'] < 0).sum()); n_tk_unmapped = sum(1 for v in mp.values() if v < 0)
f8 = f8[f8['ti'] >= 0].copy()
f8['et_date'] = pd.to_datetime(f8['et_date'])
pos = np.minimum(cal.searchsorted(f8['et_date'].to_numpy()), nd - 1)
is_td = cal.to_numpy()[pos] == f8['et_date'].to_numpy().astype(cal.dtype)
timing = f8['timing'].to_numpy()
r_pos = np.where((timing == 'post') & is_td, pos + 1, pos)
f8['r_pos'] = r_pos
f8 = f8[f8['r_pos'] < nd].copy()
f8['items_new'] = f8['items_new'].fillna('')
f8['has202'] = f8['items_new'].str.split(',').map(lambda L: '2.02' in [x.strip() for x in L])
f8['bad'] = f8['items_new'].str.split(',').map(lambda L: any(x.strip() in BAD_ITEMS for x in L))
f8['known_open'] = f8['timing'] != 'intra'
log(f"filings raw={n_f_raw:,} exploded rows mapped={len(f8):,} unmapped rows={n_unmapped:,} unmapped tickers={n_tk_unmapped}")

news_any = np.zeros((nd, nt), bool); news_202 = np.zeros((nd, nt), bool); news_bad = np.zeros((nd, nt), bool); news_open = np.zeros((nd, nt), bool)
rp = f8['r_pos'].to_numpy(); ti8 = f8['ti'].to_numpy()
news_any[rp, ti8] = True
m = f8['has202'].to_numpy(); news_202[rp[m], ti8[m]] = True
m = f8['bad'].to_numpy(); news_bad[rp[m], ti8[m]] = True
m = f8['known_open'].to_numpy(); news_open[rp[m], ti8[m]] = True
cs = np.vstack([np.zeros((1, nt), np.int32), np.cumsum(news_bad, axis=0, dtype=np.int32)])
# items string per (r_pos, ti) for descriptive item splits
f8['items_clean'] = f8['items_new'].str.replace(' ', '')
items_at = f8.groupby(['r_pos', 'ti'])['items_clean'].agg(lambda s: ','.join(s)).rename('items_T')
log("news matrices")

# ----------------------------------------------------------------------------- events
t_lo = cal.searchsorted(DISC[0]); t_hi = cal.searchsorted(TEST[1], side='right')  # T in [DISC0, TEST1]
up8 = elign & (excn >= 0.08); up3v = elign & (excn >= 0.03) & (volrn >= 2.0); dn8 = elign & (excn <= -0.08)
anym = up8 | up3v | dn8
anym[:max(t_lo, 1)] = False; anym[t_hi:] = False; anym[nd - 1:] = False
tt, ii = np.nonzero(anym)
ev = pd.DataFrame({'t': tt, 'ti': ii})
ev['ticker'] = np.array(tickers, dtype=object)[ii]
ev['T'] = cal[tt]
ev['UP8'] = up8[tt, ii]; ev['UP3V'] = up3v[tt, ii]; ev['DN8'] = dn8[tt, ii]
ev['day0_exc'] = excn[tt, ii]; ev['volratio'] = volrn[tt, ii]; ev['adv_d'] = advn[tt, ii]
ev['close_T'] = Cn[tt, ii]; ev['open_T1'] = On[tt + 1, ii]
ev = ev[ev['open_T1'].notna()].copy()
tt = ev['t'].to_numpy(); ii = ev['ti'].to_numpy()
nT = news_any[tt, ii]; nTm1 = news_any[tt - 1, ii]; nTp1 = news_any[tt + 1, ii]
ev['news_T'] = nT; ev['news_Tm1'] = nTm1; ev['news_Tp1'] = nTp1
ev['arm'] = np.where(nT, 'NEWS', np.where(~nTm1 & ~nTp1, 'NONEWS', 'AMBIG'))
ev['subarm'] = np.where(nT & news_202[tt, ii], 'NEWS_202', np.where(nT, 'NEWS_non202', ev['arm']))
ev['news_known_open'] = news_open[tt, ii]
ev['bad60'] = (cs[tt + 1, ii] - cs[np.maximum(tt - LOOKBACK, 0), ii]) > 0
ev = ev.merge(items_at, left_on=['t', 'ti'], right_index=True, how='left')
ev['items_T'] = ev['items_T'].fillna('')
ev['window'] = np.where(ev['T'] <= DISC[1], 'DISC', 'TEST')
ev['year'] = ev['T'].dt.year; ev['quarter'] = (ev['T'].dt.year * 4 + ev['T'].dt.quarter).astype(int)
ev['cost'] = np.where(ev['adv_d'] >= 100e6, 0.0010, 0.0020)
cost = ev['cost'].to_numpy()
for h in HORIZONS:
    x = tt + h; ok = x < nd; xc = np.minimum(x, nd - 1)
    exitC = np.where(ok, Cn[xc, ii], np.nan); spyx = np.where(ok, spyC[xc], np.nan)
    gc = exitC / ev['close_T'].to_numpy() - 1 - (spyx / spyC[tt] - 1)
    go = exitC / ev['open_T1'].to_numpy() - 1 - (spyx / spyO[tt + 1] - 1)
    ev[f'gross_c_{h}'] = gc; ev[f'gross_o_{h}'] = go
    ev[f'net_c_{h}'] = gc - cost; ev[f'net_o_{h}'] = go - cost
ev = ev.reset_index(drop=True)
log(f"events {len(ev):,}; UP8 {int(ev['UP8'].sum()):,} UP3V {int(ev['UP3V'].sum()):,} DN8 {int(ev['DN8'].sum()):,}")
del news_any, news_202, news_bad, news_open, cs

# ----------------------------------------------------------------------------- statistics
def boot(x, cl, reps=REPS, seed=SEED):
    x = np.asarray(x, float); codes, _ = pd.factorize(pd.Series(cl)); m = codes.max() + 1 if len(codes) else 0
    if m < 3 or len(x) < 3:
        return np.nan, np.nan, np.nan, np.nan
    sums = np.bincount(codes, weights=x, minlength=m); cnts = np.bincount(codes, minlength=m).astype(float)
    rng = np.random.default_rng(seed); idx = rng.integers(0, m, size=(reps, m))
    means = sums[idx].sum(1) / cnts[idx].sum(1)
    lo, hi = np.percentile(means, [2.5, 97.5])
    return float(lo), float(hi), max(float((means <= 0).mean()), 0.5 / reps), max(float((means >= 0).mean()), 0.5 / reps)

def wider(x, days, qtrs):
    ld, hd, ppd, pnd = boot(x, days); lq, hq, ppq, pnq = boot(x, qtrs)
    if np.isnan(lq) or (not np.isnan(ld) and (hd - ld) >= (hq - lq)):
        lo, hi, which = ld, hd, 'day'
    else:
        lo, hi, which = lq, hq, 'quarter'
    return lo, hi, np.nanmax([ppd, ppq]), np.nanmax([pnd, pnq]), which, (ld, hd, lq, hq)

def boot_diff(xa, cla, xb, clb, reps=REPS, seed=SEED):
    codes, _ = pd.factorize(pd.Series(np.concatenate([cla, clb]))); m = codes.max() + 1
    ca = codes[:len(cla)]; cb = codes[len(cla):]
    sa = np.bincount(ca, weights=xa, minlength=m); na = np.bincount(ca, minlength=m).astype(float)
    sb = np.bincount(cb, weights=xb, minlength=m); nb = np.bincount(cb, minlength=m).astype(float)
    rng = np.random.default_rng(seed); idx = rng.integers(0, m, size=(reps, m))
    with np.errstate(invalid='ignore', divide='ignore'):
        d = sa[idx].sum(1) / na[idx].sum(1) - sb[idx].sum(1) / nb[idx].sum(1)
    d = d[np.isfinite(d)]
    if len(d) < 100:
        return np.nan, np.nan, np.nan
    lo, hi = np.percentile(d, [2.5, 97.5])
    return float(lo), float(hi), max(float((d <= 0).mean()), 0.5 / reps)

def wider_diff(xa, da, qa, xb, db, qb):
    ld, hd, pd_ = boot_diff(xa, da, xb, db); lq, hq, pq = boot_diff(xa, qa, xb, qb)
    if np.isnan(lq) or (not np.isnan(ld) and (hd - ld) >= (hq - lq)):
        return ld, hd, np.nanmax([pd_, pq]), 'day'
    return lq, hq, np.nanmax([pd_, pq]), 'quarter'

def cell_stats(s, colname, sign):
    s = s[s[colname].notna()]
    out = {'n': int(len(s))}
    if len(s) < 3:
        return out
    x = s[colname].to_numpy()
    out['mean'] = x.mean() * 100; out['median'] = np.median(x) * 100; out['win'] = (x > 0).mean()
    lo, hi, pp, pn, which, (ld, hd, lq, hq) = wider(x, s['t'].to_numpy(), s['quarter'].to_numpy())
    out.update({'lo': lo * 100, 'hi': hi * 100, 'ci_cluster': which, 'lo_day': ld * 100, 'hi_day': hd * 100, 'lo_q': lq * 100, 'hi_q': hq * 100,
                'p_pos': pp, 'p_neg': pn, 'p_h1': pp if sign > 0 else pn, 'h1': '>0' if sign > 0 else '<0'})
    yr = s.groupby('year')[colname].agg(['mean', 'count']); yr = yr[yr['count'] >= 10]
    out['years_n'] = int(len(yr)); out['years_pos'] = float((yr['mean'] > 0).mean()) if len(yr) else np.nan
    return out

def holm(p):
    p = np.asarray(p, float); m = len(p); order = np.argsort(p); adj = np.empty(m); run = 0.0
    for k, i in enumerate(order):
        run = max(run, min(1.0, (m - k) * p[i])); adj[i] = run
    return adj

ROWS = {'UP8': 'day0 excess >= +8%', 'UP3V': 'day0 excess >= +3% & vol >= 2x ADV20', 'DN8': 'day0 excess <= -8% (mirror)'}
ARMS = ['NEWS', 'NEWS_202', 'NEWS_non202', 'NONEWS', 'AMBIG', 'ALL']
ENTRIES = {'MOC_T': 'net_c_', 'MOO_T1': 'net_o_'}
def arm_mask(df, arm):
    if arm == 'ALL': return np.ones(len(df), bool)
    if arm in ('NEWS', 'NONEWS', 'AMBIG'): return (df['arm'] == arm).to_numpy()
    return (df['subarm'] == arm).to_numpy()
def sign_for(row, arm):
    if arm in ('AMBIG', 'ALL'): return 1
    up = row != 'DN8'
    return (1 if up else -1) if arm.startswith('NEWS') else (-1 if up else 1)

cells = []; diffs = []
for win in ['TEST', 'DISC']:
    W = ev[ev['window'] == win]
    for row, rdesc in ROWS.items():
        R = W[W[row]]
        for excl in [False, True]:
            if excl and row == 'DN8':
                continue
            E = R[~R['bad60']] if excl else R
            for entry, pfx in ENTRIES.items():
                for h in HORIZONS:
                    colname = f'{pfx}{h}'
                    for arm in ARMS:
                        S = E[arm_mask(E, arm)]
                        st = cell_stats(S, colname, sign_for(row, arm))
                        gcol = colname.replace('net_', 'gross_')
                        g = S[gcol].dropna()
                        rec = {'window': win, 'row': row, 'row_desc': rdesc, 'excl_N5': excl, 'entry': entry, 'horizon': h, 'arm': arm,
                               'in_family': (win == 'TEST' and row == 'UP8' and not excl and entry == 'MOC_T' and arm in ('NEWS', 'NONEWS')),
                               'gross_mean': g.mean() * 100 if len(g) else np.nan}
                        if row == 'DN8' and len(g):
                            rec['net_short_mean'] = (-g - S.loc[g.index, 'cost']).mean() * 100
                        rec.update(st); cells.append(rec)
                    # differences vs NONEWS
                    B = E[arm_mask(E, 'NONEWS')]; B = B[B[colname].notna()]
                    for arm in ['NEWS', 'NEWS_202', 'NEWS_non202']:
                        A = E[arm_mask(E, arm)]; A = A[A[colname].notna()]
                        if len(A) < 3 or len(B) < 3:
                            continue
                        lo, hi, p, which = wider_diff(A[colname].to_numpy(), A['t'].to_numpy(), A['quarter'].to_numpy(), B[colname].to_numpy(), B['t'].to_numpy(), B['quarter'].to_numpy())
                        diffs.append({'window': win, 'row': row, 'excl_N5': excl, 'entry': entry, 'horizon': h, 'arm': arm, 'vs': 'NONEWS', 'n_a': len(A), 'n_b': len(B),
                                      'diff': (A[colname].mean() - B[colname].mean()) * 100, 'diff_lo': lo * 100, 'diff_hi': hi * 100, 'diff_p_pos': p, 'diff_cluster': which})
    log("cells", win)
cells = pd.DataFrame(cells); diffs = pd.DataFrame(diffs)
cells = cells.merge(diffs[['window', 'row', 'excl_N5', 'entry', 'horizon', 'arm', 'diff', 'diff_lo', 'diff_hi', 'diff_p_pos', 'diff_cluster']],
                    on=['window', 'row', 'excl_N5', 'entry', 'horizon', 'arm'], how='left')
fam = cells[cells['in_family']].copy()
fam['p_holm'] = holm(fam['p_h1'].fillna(1.0).to_numpy())
fam['pass_ci'] = fam['lo'] > 0; fam['pass_diff'] = fam['diff_lo'] > 0; fam['pass_years'] = fam['years_pos'] >= 0.60; fam['pass_n'] = fam['n'] >= 500; fam['pass_holm'] = fam['p_holm'] < 0.05
fam['PASS'] = (fam['arm'] == 'NEWS') & fam[['pass_ci', 'pass_diff', 'pass_years', 'pass_n', 'pass_holm']].all(axis=1)
fam['REVERTS'] = (fam['arm'] == 'NONEWS') & (fam['hi'] < 0)
cells = cells.merge(fam[['window', 'row', 'excl_N5', 'entry', 'horizon', 'arm', 'p_holm', 'pass_ci', 'pass_diff', 'pass_years', 'pass_n', 'pass_holm', 'PASS', 'REVERTS']],
                    on=['window', 'row', 'excl_N5', 'entry', 'horizon', 'arm'], how='left')
cells.to_csv(PFX + "cells.csv", index=False)
log("cells written", len(cells))

# ----------------------------------------------------------------------------- arm sizes, regime, yearly, descriptive splits
sizes = []
for win in ['TEST', 'DISC']:
    for row in ROWS:
        S = ev[(ev['window'] == win) & ev[row]]
        vc = S['arm'].value_counts(); vs = S['subarm'].value_counts()
        sizes.append({'window': win, 'row': row, 'n': len(S), 'NEWS': int(vc.get('NEWS', 0)), 'NEWS_202': int(vs.get('NEWS_202', 0)), 'NEWS_non202': int(vs.get('NEWS_non202', 0)),
                      'NONEWS': int(vc.get('NONEWS', 0)), 'AMBIG': int(vc.get('AMBIG', 0)), 'share_NONEWS': vc.get('NONEWS', 0) / max(len(S), 1), 'share_NEWS': vc.get('NEWS', 0) / max(len(S), 1),
                      'share_bad60': S['bad60'].mean() if len(S) else np.nan, 'tickers': S['ticker'].nunique()})
sizes = pd.DataFrame(sizes)

tst = ev[(ev['window'] == 'TEST')]
tst = tst.assign(regime=np.where(tst['year'].between(2020, 2024), '2020-2024', 'other'))
regime = []
for row in ['UP8', 'DN8']:
    for reg, G in tst[tst[row]].groupby('regime'):
        for h in [5, 20]:
            for arm in ['NEWS', 'NONEWS']:
                st = cell_stats(G[arm_mask(G, arm)], f'net_c_{h}', sign_for(row, arm))
                regime.append({'row': row, 'regime': reg, 'horizon': h, 'arm': arm, 'n': st.get('n'), 'mean': st.get('mean'), 'lo': st.get('lo'), 'hi': st.get('hi'), 'years_pos': st.get('years_pos')})
            A = G[arm_mask(G, 'NEWS')]; B = G[arm_mask(G, 'NONEWS')]; c = f'net_c_{h}'; A = A[A[c].notna()]; B = B[B[c].notna()]
            if len(A) > 3 and len(B) > 3:
                lo, hi, p, which = wider_diff(A[c].to_numpy(), A['t'].to_numpy(), A['quarter'].to_numpy(), B[c].to_numpy(), B['t'].to_numpy(), B['quarter'].to_numpy())
                regime.append({'row': row, 'regime': reg, 'horizon': h, 'arm': 'NEWS-NONEWS', 'n': len(A) + len(B), 'mean': (A[c].mean() - B[c].mean()) * 100, 'lo': lo * 100, 'hi': hi * 100})
regime = pd.DataFrame(regime)

yearly = []
for y, G in ev[ev['UP8']].groupby('year'):
    r = {'year': y, 'window': G['window'].iloc[0], 'UP8_n': len(G), 'share_NONEWS': (G['arm'] == 'NONEWS').mean(), 'share_NEWS': (G['arm'] == 'NEWS').mean()}
    for arm in ['NEWS', 'NONEWS']:
        S = G[arm_mask(G, arm)]
        for h in [1, 5, 20]:
            s = S[f'net_c_{h}'].dropna(); r[f'{arm}_h{h}_n'] = len(s); r[f'{arm}_h{h}_%'] = s.mean() * 100 if len(s) else np.nan
    yearly.append(r)
yearly = pd.DataFrame(yearly); yearly.to_csv(PFX + "yearly.csv", index=False)

# descriptive: intra-only vs known-at-open; item codes (TEST, UP8, MOC_T)
U = tst[tst['UP8']]
desc = []
for lab, M in [('NEWS known-at-open (pre or prior post)', (U['arm'] == 'NEWS') & U['news_known_open']), ('NEWS intra-only', (U['arm'] == 'NEWS') & ~U['news_known_open'])]:
    for h in [1, 5, 20]:
        st = cell_stats(U[M], f'net_c_{h}', 1); desc.append({'split': lab, 'horizon': h, 'n': st.get('n'), 'mean': st.get('mean'), 'lo': st.get('lo'), 'hi': st.get('hi'), 'years_pos': st.get('years_pos')})
itemtab = []
N = U[U['arm'] == 'NEWS']
itemsets = N['items_T'].str.split(',').map(lambda L: set(x for x in L if x))
for code in ITEM_CODES:
    if code == '9.01':
        M = itemsets.map(lambda s: s <= {'9.01'}).to_numpy(); lab = '9.01 only'
    else:
        M = itemsets.map(lambda s, c=code: c in s).to_numpy(); lab = code
    S = N[M]
    r = {'item_at_T': lab, 'n': len(S), 'share_of_NEWS': len(S) / max(len(N), 1)}
    for h in [1, 5, 20]:
        st = cell_stats(S, f'net_c_{h}', 1); r[f'h{h}_%'] = st.get('mean'); r[f'h{h}_lo'] = st.get('lo'); r[f'h{h}_hi'] = st.get('hi')
    itemtab.append(r)
itemtab = pd.DataFrame(itemtab)
# non-2.02 news with a press-release-type item (7.01/8.01/1.01/2.01) vs routine-only
prtype = itemsets.map(lambda s: bool(s & {'7.01', '8.01', '1.01', '2.01', '2.05', '2.06', '1.02', '5.01', '2.04', '1.05'})).to_numpy()
for lab, M in [('NEWS_non202 with PR-type item (7.01/8.01/1.01/2.01/2.05/2.06/1.02/5.01/2.04/1.05)', (N['subarm'] == 'NEWS_non202').to_numpy() & prtype),
               ('NEWS_non202 routine-only items (5.02/5.07/9.01/5.03/2.03/3.03/4.01/...)', (N['subarm'] == 'NEWS_non202').to_numpy() & ~prtype)]:
    for h in [1, 5, 20]:
        st = cell_stats(N[M], f'net_c_{h}', 1); desc.append({'split': lab, 'horizon': h, 'n': st.get('n'), 'mean': st.get('mean'), 'lo': st.get('lo'), 'hi': st.get('hi'), 'years_pos': st.get('years_pos')})
desc = pd.DataFrame(desc)
# liquidity tier (TEST UP8 MOC h5/h20) NEWS vs NONEWS
ev['size_tier'] = pd.cut(ev['adv_d'], [50e6, 100e6, 500e6, 2000e6, np.inf], right=False, labels=['50-100M', '100-500M', '500M-2B', '>2B']).astype(str)
U = ev[(ev['window'] == 'TEST') & ev['UP8']]
size = []
for tier, G in U.groupby('size_tier'):
    for arm in ['NEWS', 'NONEWS']:
        for h in [5, 20]:
            st = cell_stats(G[arm_mask(G, arm)], f'net_c_{h}', sign_for('UP8', arm))
            size.append({'size_tier': tier, 'arm': arm, 'horizon': h, 'n': st.get('n'), 'mean': st.get('mean'), 'lo': st.get('lo'), 'hi': st.get('hi')})
size = pd.DataFrame(size)
log("splits done")

# ----------------------------------------------------------------------------- events file
keep = ['ticker', 'T', 'window', 'year', 'UP8', 'UP3V', 'DN8', 'day0_exc', 'volratio', 'adv_d', 'size_tier', 'cost', 'close_T', 'open_T1', 'news_Tm1', 'news_T', 'news_Tp1', 'arm', 'subarm',
        'news_known_open', 'bad60', 'items_T'] + [f'{p}{h}' for h in HORIZONS for p in ['gross_c_', 'net_c_', 'gross_o_', 'net_o_']]
ev[keep].to_csv(PFX + "events.csv.gz", index=False, compression='gzip')
log("events written")

# ----------------------------------------------------------------------------- things_tried
tt_rows = []
def note_of(r):
    return (f"net excess over SPY % CI[{r.get('lo', np.nan):.2f},{r.get('hi', np.nan):.2f}] {r.get('ci_cluster', '')}-cluster; years_pos {r.get('years_pos', np.nan):.2f}; "
            f"diff vs NONEWS {r.get('diff', np.nan):+.2f} [{r.get('diff_lo', np.nan):.2f},{r.get('diff_hi', np.nan):.2f}]")
for _, r in fam.iterrows():
    tt_rows.append(('N1', f"FAMILY {r['arm']} UP8 MOC_T h{int(r['horizon'])} (8-K news vs no-news, Chan test)", 'TEST 2015-01..2026-08', int(r['n']), round(r['mean'], 3),
                    f"registered family cell; {note_of(r)}; Holm p {r['p_holm']:.3f}; PASS={bool(r['PASS'])}; REVERTS={bool(r['REVERTS'])}"))
for _, r in cells[~cells['in_family'] & (cells['n'] >= 30)].iterrows():
    tt_rows.append(('N1', f"{r['row']} {r['arm']} {r['entry']} h{int(r['horizon'])}" + (" EXCL-N5" if r['excl_N5'] else ""), r['window'], int(r['n']),
                    round(r['mean'], 3) if not np.isnan(r.get('mean', np.nan)) else '', f"descriptive cell (declared in prereg), not in family; {note_of(r)}"))
for _, r in regime.iterrows():
    tt_rows.append(('N1', f"regime {r['regime']} {r['row']} {r['arm']} MOC_T h{int(r['horizon'])}", 'TEST', int(r['n']) if not pd.isna(r['n']) else 0, round(r['mean'], 3) if not pd.isna(r['mean']) else '',
                    f"regime split; CI[{r['lo']:.2f},{r['hi']:.2f}]"))
for _, r in desc.iterrows():
    tt_rows.append(('N1', f"descriptive split: {r['split']} UP8 MOC_T h{int(r['horizon'])}", 'TEST', int(r['n']) if not pd.isna(r['n']) else 0, round(r['mean'], 3) if not pd.isna(r['mean']) else '', f"CI[{r['lo']:.2f},{r['hi']:.2f}]"))
for _, r in itemtab.iterrows():
    tt_rows.append(('N1', f"item-at-T {r['item_at_T']} within NEWS UP8 MOC_T", 'TEST', int(r['n']), round(r['h5_%'], 3) if not pd.isna(r['h5_%']) else '', f"h5 shown; h20 {r['h20_%']:.2f} [{r['h20_lo']:.2f},{r['h20_hi']:.2f}]; overlapping item categories"))
for _, r in size.iterrows():
    tt_rows.append(('N1', f"size tier {r['size_tier']} {r['arm']} UP8 MOC_T h{int(r['horizon'])}", 'TEST', int(r['n']) if not pd.isna(r['n']) else 0, round(r['mean'], 3) if not pd.isna(r['mean']) else '', f"CI[{r['lo']:.2f},{r['hi']:.2f}]"))
ttdf = pd.DataFrame(tt_rows, columns=['who', 'variant', 'window', 'n', 'avgR_net', 'note'])
ttdf['note'] = ttdf['note'].str.replace('\n', ' ')
ttdf.to_csv(RES + "/things_tried.csv", mode='a', header=False, index=False)
log("things_tried appended", len(ttdf))

# ----------------------------------------------------------------------------- report
def md(df, fmt="{:.2f}"):
    cols = list(df.columns); L = ["| " + " | ".join(map(str, cols)) + " |", "|" + "|".join(["---"] * len(cols)) + "|"]
    for _, r in df.iterrows():
        L.append("| " + " | ".join(("" if (isinstance(v, float) and np.isnan(v)) else (fmt.format(v) if isinstance(v, (float, np.floating)) else str(v))) for v in r) + " |")
    return "\n".join(L)

def fam_line(arm, h):
    r = fam[(fam['arm'] == arm) & (fam['horizon'] == h)].iloc[0]
    return r
L = []
L.append("# N1: news vs no-news continuation (Chan 2003) on 8-K filings, KEY N1\n")
L.append(f"Registered: `preregistration_news_vs_nonews.json`, SHA-256 `{SHA}` (sidecar `N1_prereg_sha256.txt`). Single run {pd.Timestamp.now():%Y-%m-%d %H:%M}. Units: percent net excess over SPY per trade after round-trip costs (10 bp if ADV20 >= $100M else 20 bp); CI = 95% wider-of-day/quarter cluster bootstrap ({REPS} reps).\n")
L.append(f"Filings: {n_f_raw:,} 8-K-family rows; {len(f8):,} mapped to bar tickers ({n_tk_unmapped} filing tickers with no bars). Universe through T-1: prev close >= $5, ADV20 >= $50M, >= 250 prior rows. Events = ticker-days.\n")
L.append("## 0. Verdict (plain)\n")
n5 = fam_line('NEWS', 5); n20 = fam_line('NEWS', 20); n1 = fam_line('NEWS', 1); z5 = fam_line('NONEWS', 5); z20 = fam_line('NONEWS', 20); z1 = fam_line('NONEWS', 1)
sz = sizes[(sizes['window'] == 'TEST') & (sizes['row'] == 'UP8')].iloc[0]
L.append(f"- TEST 2015-01..2026-08, day-0 excess >= +8%: {sz['n']:,} events; NEWS (8-K reacting on T) {sz['NEWS']:,} ({sz['share_NEWS']:.0%}; 2.02 earnings {sz['NEWS_202']:,}, non-2.02 {sz['NEWS_non202']:,}), NO-NEWS (no 8-K in T-1..T+1) {sz['NONEWS']:,} ({sz['share_NONEWS']:.0%}), ambiguous {sz['AMBIG']:,}.")
L.append(f"- NEWS arm, MOC T entry: h1 {n1['mean']:+.2f}% [{n1['lo']:.2f},{n1['hi']:.2f}], h5 {n5['mean']:+.2f}% [{n5['lo']:.2f},{n5['hi']:.2f}], h20 {n20['mean']:+.2f}% [{n20['lo']:.2f},{n20['hi']:.2f}]; years positive {n1['years_pos']:.0%}/{n5['years_pos']:.0%}/{n20['years_pos']:.0%}; Holm p {n1['p_holm']:.3f}/{n5['p_holm']:.3f}/{n20['p_holm']:.3f}.")
L.append(f"- NO-NEWS arm, MOC T entry: h1 {z1['mean']:+.2f}% [{z1['lo']:.2f},{z1['hi']:.2f}], h5 {z5['mean']:+.2f}% [{z5['lo']:.2f},{z5['hi']:.2f}], h20 {z20['mean']:+.2f}% [{z20['lo']:.2f},{z20['hi']:.2f}]; years positive {z1['years_pos']:.0%}/{z5['years_pos']:.0%}/{z20['years_pos']:.0%}; Holm p {z1['p_holm']:.3f}/{z5['p_holm']:.3f}/{z20['p_holm']:.3f}.")
L.append(f"- NEWS minus NO-NEWS: h1 {n1['diff']:+.2f}% [{n1['diff_lo']:.2f},{n1['diff_hi']:.2f}], h5 {n5['diff']:+.2f}% [{n5['diff_lo']:.2f},{n5['diff_hi']:.2f}], h20 {n20['diff']:+.2f}% [{n20['diff_lo']:.2f},{n20['diff_hi']:.2f}].")
L.append(f"- PASS (tradeable NEWS long): {', '.join(f'h{int(r.horizon)}={bool(r.PASS)}' for r in fam[fam['arm']=='NEWS'].itertuples())}. NO-NEWS reverts (upper CI < 0): {', '.join(f'h{int(r.horizon)}={bool(r.REVERTS)}' for r in fam[fam['arm']=='NONEWS'].itertuples())}.\n")
L.append("## 1. Registered family (TEST, UP8, MOC_T, no exclusion)\n")
show = fam[['arm', 'horizon', 'n', 'mean', 'lo', 'hi', 'ci_cluster', 'diff', 'diff_lo', 'diff_hi', 'years_pos', 'years_n', 'h1', 'p_h1', 'p_holm', 'PASS', 'REVERTS']].copy()
show.columns = ['arm', 'h', 'n', 'net excess %', 'lo', 'hi', 'cluster', 'news-nonews %', 'd lo', 'd hi', 'years+', 'yrs', 'H1', 'p', 'Holm p', 'PASS', 'REVERTS']
L.append(md(show, "{:.3f}"))
L.append("\n## 2. Arm sizes (share of big moves with no 8-K)\n")
L.append(md(sizes, "{:.3f}"))
L.append("\n## 3. All cells, both windows, both entries (UP rows with/without the N5 exclusion; DN8 long-side)\n")
show2 = cells[['window', 'row', 'excl_N5', 'entry', 'horizon', 'arm', 'n', 'gross_mean', 'mean', 'lo', 'hi', 'ci_cluster', 'diff', 'diff_lo', 'diff_hi', 'years_pos', 'net_short_mean']].copy()
show2.columns = ['window', 'row', 'exclN5', 'entry', 'h', 'arm', 'n', 'gross %', 'net %', 'lo', 'hi', 'cluster', 'vs NONEWS %', 'd lo', 'd hi', 'years+', 'net short % (DN8)']
L.append(md(show2[show2['n'] >= 3], "{:.2f}"))
L.append("\n## 4. N5 exclusion effect (TEST, UP8, MOC_T): with vs without\n")
ex = cells[(cells['window'] == 'TEST') & (cells['row'] == 'UP8') & (cells['entry'] == 'MOC_T') & cells['arm'].isin(['NEWS', 'NEWS_202', 'NEWS_non202', 'NONEWS'])]
piv = ex.pivot_table(index=['arm', 'horizon'], columns='excl_N5', values=['n', 'mean', 'lo', 'hi'])
piv.columns = [f"{a}_{'excl' if b else 'all'}" for a, b in piv.columns]; piv = piv.reset_index()
L.append(md(piv[['arm', 'horizon', 'n_all', 'mean_all', 'lo_all', 'hi_all', 'n_excl', 'mean_excl', 'lo_excl', 'hi_excl']], "{:.2f}"))
L.append("\n## 5. Regime: 2020-2024 vs other TEST years (MOC_T)\n")
L.append(md(regime, "{:.2f}"))
L.append("\n## 6. Yearly (UP8, MOC_T, net excess %)\n")
L.append(md(yearly, "{:.2f}"))
L.append("\n## 7. Descriptive splits within NEWS (TEST, UP8, MOC_T)\n")
L.append(md(desc, "{:.2f}"))
L.append("\n**By item code present at T (overlapping categories):**\n")
L.append(md(itemtab, "{:.2f}"))
L.append("\n## 8. Liquidity tiers (TEST, UP8, MOC_T)\n")
L.append(md(size, "{:.2f}"))
L.append("\n## 9. Caveats\n")
L.append("- Survivorship: symbol list as of 2026-09 in both the bars and the filings crawl; delisted names are absent (flatters long cells, understates reversal in the no-news arm, since no-news pumps that later delist are missing).\n- 'News' = any 8-K item, including routine ones (5.02 appointments, 5.07 vote results, 9.01-only); press releases without an 8-K, 6-K filers, analyst actions and media are invisible, so part of NO-NEWS has a public catalyst this data cannot see. Item splits in section 7 are descriptive.\n- Intra-day filings on T are counted as NEWS even when accepted after the move (flagged; split in section 7).\n- Discovery 2005-2014 rests on 1,225 names for 2005-2013 and is descriptive.\n- 2024-09-03..09-11 bars hole: events and exits touching it are dropped.\n- No haircut is applied; NEWSARCH guidance is +0.25% (1-5 d) / +0.40% (20 d) against any long cell.\n")
open(PFX + "report.md", "w", encoding='utf-8').write("\n".join(L))
summary = {'sha': SHA, 'family': fam.to_dict(orient='records'), 'sizes': sizes.to_dict(orient='records'), 'regime': regime.to_dict(orient='records'), 'desc': desc.to_dict(orient='records'),
           'itemtab': itemtab.to_dict(orient='records'), 'size': size.to_dict(orient='records'), 'yearly': yearly.to_dict(orient='records'), 'things_tried_rows': len(ttdf), 'n_events': len(ev)}
json.dump(summary, open(PFX + "summary.json", "w"), indent=1, default=lambda o: None if (isinstance(o, float) and np.isnan(o)) else (o.item() if hasattr(o, 'item') else str(o)))
log("report written")
print(md(show, "{:.3f}"))
print(md(sizes, "{:.3f}"))
print("--- key non-family (TEST UP8 MOC/MOO all arms, DN8, UP3V, EXCL) ---")
k = cells[(cells['window'] == 'TEST') & cells['arm'].isin(['NEWS', 'NEWS_202', 'NEWS_non202', 'NONEWS']) & (cells['n'] >= 3)][['row', 'excl_N5', 'entry', 'horizon', 'arm', 'n', 'mean', 'lo', 'hi', 'diff', 'diff_lo', 'diff_hi', 'years_pos', 'net_short_mean']]
print(k.to_string(float_format=lambda v: f"{v:.2f}"))
print("--- DISC UP8 MOC ---")
print(cells[(cells['window'] == 'DISC') & (cells['row'] == 'UP8') & (cells['entry'] == 'MOC_T') & ~cells['excl_N5'] & cells['arm'].isin(['NEWS', 'NONEWS'])][['horizon', 'arm', 'n', 'mean', 'lo', 'hi', 'diff', 'diff_lo', 'diff_hi', 'years_pos']].to_string(float_format=lambda v: f"{v:.2f}"))
print("--- regime ---"); print(regime.to_string(float_format=lambda v: f"{v:.2f}"))
print("--- desc ---"); print(desc.to_string(float_format=lambda v: f"{v:.2f}"))
print("--- items ---"); print(itemtab.to_string(float_format=lambda v: f"{v:.2f}"))
print("--- size ---"); print(size.to_string(float_format=lambda v: f"{v:.2f}"))
print("--- yearly ---"); print(yearly.to_string(float_format=lambda v: f"{v:.2f}"))
log("done")
