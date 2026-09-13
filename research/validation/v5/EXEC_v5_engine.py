"""EXEC_v5 engine: literal implementation of preregistration_v5.json cells on daily bars.
Windows run here: refinement-on-seen 2014-09-01..2024-08-31 (halves), discovery 2024-09-01..2026-09-12 (descriptive).
Confirmatory 2004-2013: NOT run (982/3179 files < 1500 -> reported unavailable per executor brief); only the
pre-run earnings-coverage check (event flags, no forward returns) is computed on it.
"""
import numpy as np, pandas as pd, glob, os, sys, time, zlib, json, hashlib, warnings
from scipy import stats as sst
warnings.filterwarnings('ignore')
SP = 'C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad/'
R = SP + 'research/'; KEY = 'EXEC_v5'
T0 = time.time()
def log(*a): print(f'[{time.time()-T0:7.1f}s]', *a, flush=True)
NB = 2000
def netret(leg, c): return (1 + leg) * (1 - c) / (1 + c) - 1
CUT_SEEN = ('2014-09-01', '2024-08-31'); H1 = ('2014-09-01', '2019-12-31'); H2 = ('2020-01-01', '2024-08-31')
DISC = ('2024-09-01', '2026-09-12')
FLD = ['Open', 'High', 'Low', 'Close', 'Volume']

# ---------------------------------------------------------------- loading
def load_dir(d, tag):
    files = sorted(glob.glob(os.path.join(d, '*.csv'))); ser = {}
    for i, f in enumerate(files):
        sym = os.path.basename(f)[:-4].upper().replace('.', '-')
        try: x = pd.read_csv(f)
        except Exception: continue
        dc = 'date' if 'date' in x.columns else ('ts' if 'ts' in x.columns else x.columns[0])
        if not set(FLD) <= set(x.columns): continue
        dt = pd.to_datetime(x[dc].astype(str).str[:10], errors='coerce')
        x = x.assign(d=dt).dropna(subset=['d']).drop_duplicates('d').set_index('d').sort_index()[FLD].astype(float)
        ser[sym] = x
        if (i + 1) % 500 == 0: log(tag, i + 1, '/', len(files))
    big = pd.concat(ser, axis=1).sort_index()
    P = {k: big.xs(k, axis=1, level=1) for k in FLD}
    wd = pd.Series(P['Close'].index.weekday)
    sundays = int((wd == 6).sum()); sats = int((wd == 5).sum())
    log(tag, 'files', len(files), 'loaded', len(ser), 'dates', P['Close'].index.min().date(), '..', P['Close'].index.max().date(), 'sat', sats, 'sun', sundays)
    if sundays > 0:  # midnight-UTC labelled bars printed as previous day 20:00 ET -> true trading day = label + 1 day
        P = {k: v.set_index(v.index + pd.Timedelta(days=1)) for k, v in P.items()}
        log(tag, 'DATE SHIFT APPLIED (+1 calendar day): labels contained Sundays')
    return P, [os.path.basename(f) for f in files]

spy = pd.read_csv(R + 'EXEC_v5_SPY_2004_2026.csv'); spy['d'] = pd.to_datetime(spy.date); spy = spy.set_index('d').sort_index()
P10, files10 = load_dir(SP + 'bars1d_10y', '10y')
PA, filesA = load_dir(SP + 'bars1d_all', 'all')
# alignment check of bars1d_all vs 10y via return correlation on overlap
com = [s for s in PA['Close'].columns if s in P10['Close'].columns][:300]
ov = PA['Close'].index.intersection(P10['Close'].index)
best = None
for k in (-1, 0, 1):
    a = PA['Close'][com].pct_change().shift(k).loc[ov]; b = P10['Close'][com].pct_change().loc[ov]
    c = np.nanmean([a[s].corr(b[s]) for s in com if a[s].notna().sum() > 20])
    log('align check shift', k, 'mean corr', round(float(c), 4))
    if best is None or c > best[1]: best = (k, c)
if best[0] != 0 and np.isfinite(best[1]) and best[1] > 0.5:
    PA = {k: v.shift(best[0]) for k, v in PA.items()}; log('APPLIED row shift', best[0], 'to bars1d_all')
else: log('no row shift applied to bars1d_all (no overlap with 10y or shift 0 best); Sunday check passed')
# splice: 10y before cut, all from cut; per-ticker price scaling from overlap median ratio
cut = max(PA['Close'].index.min(), pd.Timestamp('2024-09-01'))
ov = PA['Close'].index.intersection(P10['Close'].index); ov = ov[ov >= cut]
ratio = (P10['Close'].reindex(ov)[com if False else [s for s in PA['Close'].columns if s in P10['Close'].columns]] /
         PA['Close'].reindex(ov)[[s for s in PA['Close'].columns if s in P10['Close'].columns]]).median()
ratio = ratio.replace(0, np.nan).fillna(1.0)
log('splice cut', cut.date(), 'scaled tickers', int((ratio - 1).abs().gt(0.001).sum()), 'ratio>5% off', int((ratio - 1).abs().gt(0.05).sum()))
P = {}
for k in FLD:
    a = P10[k].loc[P10[k].index < cut].copy()
    if k != 'Volume':
        sc = ratio.reindex(a.columns).fillna(1.0); a = a / sc
    P[k] = pd.concat([a, PA[k].loc[PA[k].index >= cut]], axis=0).sort_index()
del P10, PA
cal_ok = P['Close'].index.isin(spy.index)
log('panel dates', len(cal_ok), 'not in SPY calendar (dropped)', int((~cal_ok).sum()))
P = {k: v.loc[cal_ok] for k, v in P.items()}
dates = P['Close'].index; syms = np.array(P['Close'].columns); ND, NS = len(dates), len(syms)
log('combined panel', ND, 'days x', NS, 'names', dates.min().date(), '..', dates.max().date())
spy_full = spy.copy(); spy = spy.reindex(dates)
spy_x1 = (spy.Open.shift(-1) / spy.Close - 1).values; spy_dret = (spy.Close / spy.Close.shift(1) - 1).values

# ---------------------------------------------------------------- features (universe through T-1)
def features(P):
    O, H, L, C, V = P['Open'], P['High'], P['Low'], P['Close'], P['Volume']
    pc = C.shift(1)
    adv_sh = V.shift(1).rolling(20, min_periods=20).mean()
    adv_d = (C * V).shift(1).rolling(20, min_periods=20).mean()
    hist = C.notna().cumsum().shift(1)
    elig = (pc >= 5) & (adv_d >= 5e7) & (hist >= 250) & C.notna() & O.notna() & V.notna() & (V > 0) & (O > 0)
    gap = O / pc - 1; rvol = V / adv_sh; dret = C / pc - 1
    F = dict(pc=pc, adv_sh=adv_sh, adv_d=adv_d, elig=elig, gap=gap, rvol=rvol, dret=dret,
             E1=elig & (gap >= .08) & (rvol >= 3) & (C > O), E2=elig & (gap >= .03) & (rvol >= 2) & (C >= O),
             M1=elig & (gap <= -.08) & (rvol >= 3) & (C < O), deal=(gap >= .15) & ((H - L) / O <= .02), extreme=dret >= .25)
    return F

F = features(P)
O, H, L, C, V = P['Open'], P['High'], P['Low'], P['Close'], P['Volume']
pc = F['pc']
tr = np.maximum(np.maximum(H - L, (H - pc).abs()), (L - pc).abs())
atr = tr.shift(1).rolling(20, min_periods=20).mean() / pc; del tr
cost = F['adv_d'].ge(1e8).astype(float) * 5e-4 + F['adv_d'].lt(1e8).astype(float) * 1e-3
X1 = O.shift(-1) / C - 1; X2 = C.shift(-1) / C - 1; PLC = O.shift(-6) / C.shift(-5) - 1
LAD = {k: O.shift(-(k + 1)) / C.shift(-k) - 1 for k in (1, 2, 3)}
COND = {k: C.shift(-k) >= C for k in (1, 2, 3)}
log('features done')

# daily universe series for B2 and companions
elig = F['elig']; dr_e = F['dret'].where(elig)
n_elig = elig.sum(axis=1)
csd = dr_e.std(axis=1).where(n_elig >= 50)
dv = (C * V).where(elig)
absr = np.nan_to_num(dr_e.abs().values, nan=-1.0); dvv = np.nan_to_num(dv.values, nan=0.0)
top20 = np.zeros(ND)
for i in range(ND):
    if n_elig.iloc[i] >= 50:
        idx = np.argpartition(-absr[i], 20)[:20]; top20[i] = dvv[i, idx].sum() / max(dvv[i].sum(), 1)
daily = pd.DataFrame(dict(n_elig=n_elig.values, csd=csd.values, top20=top20, spy_dret=spy_dret, spy_x1=spy_x1,
                          nE1=F['E1'].sum(axis=1).values, nE2=F['E2'].sum(axis=1).values), index=dates)
m21 = daily.csd.rolling(21, min_periods=21).mean()
med504 = m21.rolling(504, min_periods=504).median()
daily['m21'] = m21.shift(1); daily['med504'] = med504.shift(1)
daily['B2'] = (daily.m21 >= daily.med504) & daily.med504.notna()
daily['att21'] = pd.Series(top20, index=dates).rolling(21, min_periods=21).mean().shift(1)
daily['catrate63'] = (daily.nE2 / daily.n_elig.replace(0, np.nan) * 1000).rolling(63, min_periods=30).mean().shift(1)
log('daily series done; B2 defined from', daily.index[daily.B2.values | daily.med504.notna().values].min().date() if daily.med504.notna().any() else None)

# ---------------------------------------------------------------- long tables
um = elig.values; udi, uti = np.nonzero(um)
def gv(A, di, ti): return A.values[di, ti]
U = pd.DataFrame(dict(di=udi, ti=uti, gap=gv(F['gap'], udi, uti), rvol=gv(F['rvol'], udi, uti), dret=gv(F['dret'], udi, uti),
                      adv_d=gv(F['adv_d'], udi, uti), isE2=gv(F['E2'], udi, uti)))
cu = gv(cost, udi, uti)
U['x1n'] = netret(gv(X1, udi, uti), cu); U['plcn'] = netret(gv(PLC, udi, uti), cu)
for k in (1, 2, 3): U[f'lad{k}n'] = netret(gv(LAD[k], udi, uti), cu)
U['dec'] = np.minimum((U.groupby('di').adv_d.rank(pct=True) * 10).astype(int), 9)
log('U rows', len(U))

evm = (F['E2'] | F['M1']).values; di, ti = np.nonzero(evm)
EV = pd.DataFrame(dict(di=di, ti=ti, ticker=syms[ti], date=dates[di]))
for nm, A in [('isE1', F['E1']), ('isE2', F['E2']), ('isM1', F['M1']), ('gap', F['gap']), ('rvol', F['rvol']), ('dret', F['dret']),
              ('adv_d', F['adv_d']), ('cost', cost), ('close_T', C), ('open_T', O), ('open_T1', O.shift(-1)), ('x1', X1), ('x2', X2),
              ('plc', PLC), ('atr20pct', atr), ('deal_pin', F['deal']), ('extreme_mover', F['extreme'])]:
    EV[nm] = gv(A, di, ti)
for k in (1, 2, 3):
    EV[f'lad{k}'] = gv(LAD[k], di, ti); EV[f'cond{k}'] = gv(COND[k], di, ti)
EV = EV.merge(U[['di', 'ti', 'dec']], on=['di', 'ti'], how='left')
del F, X1, X2, PLC, LAD, COND, atr, cost
EV['net'] = netret(EV.x1, EV.cost); EV['net10'] = netret(EV.x1, 1e-3); EV['net5'] = netret(EV.x1, 5e-4)
EV['spy_leg'] = spy_x1[EV.di]; EV['spy_day_ret'] = spy_dret[EV.di]
EV['exc'] = EV.net - EV.spy_leg; EV['exc10'] = EV.net10 - EV.spy_leg; EV['exc5'] = EV.net5 - EV.spy_leg
EV['cf_next_close_net'] = netret(EV.x2, EV.cost); EV['cf_next_close_exc'] = EV.cf_next_close_net - (spy.Close.shift(-1) / spy.Close - 1).values[EV.di]
EV['plc_net'] = netret(EV.plc, EV.cost)
i5 = np.minimum(EV.di + 5, ND - 1); EV['plc_spy'] = np.where(EV.di + 5 < ND, spy_x1[i5], np.nan); EV['plc_exc_spy'] = EV.plc_net - EV.plc_spy
for k in (1, 2, 3):
    EV[f'lad{k}n'] = netret(EV[f'lad{k}'], EV.cost); ik = np.minimum(EV.di + k, ND - 1)
    EV[f'lad{k}_spy'] = np.where(EV.di + k < ND, spy_x1[ik], np.nan); EV[f'lad{k}_exc'] = EV[f'lad{k}n'] - EV[f'lad{k}_spy']
    EV[f'lad{k}_date'] = dates[ik]
EV['adv_tier'] = np.where(EV.adv_d >= 5e8, '>=500M', np.where(EV.adv_d >= 1e8, '100-500M', '50-100M'))
EV['year'] = EV.date.dt.year; EV['q'] = EV.date.dt.to_period('Q').astype(str)
EV['half'] = np.where(EV.date <= H1[1], 'H1_2014-09..2019-12', np.where(EV.date <= H2[1], 'H2_2020-01..2024-08', 'DISC'))
# macro_eve approximation: T+1 is the first Friday of a month (NFP; BLS calendar not on disk, CPI not flagged) -> disclosed
nxt = dates[np.minimum(EV.di + 1, ND - 1)]
EV['macro_eve_nfp_approx'] = (nxt.weekday == 4) & (nxt.day <= 7)
log('EV rows', len(EV), 'E1', int(EV.isE1.sum()), 'E2', int(EV.isE2.sum()), 'M1', int(EV.isM1.sum()))

# earnings flags
earn = pd.read_csv(R + 'earnings_dates.csv'); earn['ticker'] = earn.ticker.str.upper().str.replace('.', '-', regex=False).str.replace('/', '-', regex=False)
ed = {}
for t, d, tm in zip(earn.ticker.values, earn.date.values, earn.timing.values): ed.setdefault((t, d), set()).add(tm)
etk = earn.groupby('ticker').date.apply(lambda s: np.array(sorted(pd.to_datetime(s).values))).to_dict()
ds = dates.strftime('%Y-%m-%d').values
def eflags(tk, i):
    T = ds[i]; Tp = ds[i - 1] if i > 0 else None; Tn = ds[i + 1] if i + 1 < ND else None
    a = ed.get((tk, T), set()); b = ed.get((tk, Tp), set()) if Tp else set(); c = ed.get((tk, Tn), set()) if Tn else set()
    e = bool(a & {'bmo', 'unknown'}) or bool(b & {'amc', 'unknown'})
    tonight = bool(a & {'amc'}) or bool(c & {'bmo', 'unknown'})
    arr = etk.get(tk)
    if arr is None: res = False
    else:
        t64 = np.datetime64(dates[i].date()); lo = t64 - np.timedelta64(130, 'D'); hi = t64 + np.timedelta64(130, 'D')
        res = bool(((arr >= lo) & (arr <= t64)).any() and ((arr >= t64) & (arr <= hi)).any())
    return e, tonight, res
ef = np.array([eflags(t, i) for t, i in zip(EV.ticker.values, EV.di.values)])
EV['earnings'] = ef[:, 0]; EV['earnings_tonight'] = ef[:, 1]; EV['earn_resolvable'] = ef[:, 2]
log('earnings flags done; resolvable share', round(float(EV.earn_resolvable.mean()), 3))

# ---------------------------------------------------------------- matched controls C1 / C2
cand = U[(~U.isE2.astype(bool)) & (U.gap.abs() < .01) & (U.rvol < 1.5) & U.x1n.notna()].reset_index(drop=True)
g1 = cand.groupby(['di', 'dec']).indices
cx = cand[['x1n', 'plcn', 'lad1n', 'lad2n', 'lad3n']].values
cand2 = U[(~U.isE2.astype(bool)) & (U.rvol < 1.5) & U.x1n.notna()].reset_index(drop=True)
g2 = cand2.groupby('di').indices; c2r = cand2.dret.values; c2x = cand2.x1n.values
c1 = np.full((len(EV), 6), np.nan); c2 = np.full((len(EV), 2), np.nan)
for j, (d_, dec_, tk, r) in enumerate(zip(EV.di.values, EV.dec.values, EV.ticker.values, EV.dret.values)):
    ix = g1.get((d_, dec_))
    if ix is not None and len(ix) > 0:
        rng = np.random.default_rng(zlib.crc32(f'{ds[d_]}|{tk}'.encode()))
        pick = rng.choice(ix, size=min(5, len(ix)), replace=False)
        c1[j, :5] = np.nanmean(cx[pick], axis=0); c1[j, 5] = len(pick)
    ix2 = g2.get(d_)
    if ix2 is not None and len(ix2) > 0:
        d = c2r[ix2]; lo_, hi_ = min(.75 * r, 1.25 * r), max(.75 * r, 1.25 * r)
        m = (np.abs(d - r) <= .01) & (d >= lo_) & (d <= hi_)   # stricter reading: +-1pp AND [0.75x,1.25x]
        if m.any():
            sel = ix2[m][np.argsort(np.abs(d[m] - r))[:3]]; c2[j] = [c2x[sel].mean(), len(sel)]
    if (j + 1) % 5000 == 0: log('controls', j + 1, '/', len(EV))
EV['c1_net'], EV['c1_plc'], EV['c1_lad1'], EV['c1_lad2'], EV['c1_lad3'], EV['c1_n'] = [c1[:, i] for i in range(6)]
EV['c2_net'], EV['c2_n'] = c2[:, 0], c2[:, 1]
EV['exc_c1'] = EV.net - EV.c1_net; EV['exc_c2'] = EV.net - EV.c2_net; EV['plc_exc_c1'] = EV.plc_net - EV.c1_plc
for k in (1, 2, 3): EV[f'lad{k}_exc_c1'] = EV[f'lad{k}n'] - EV[f'c1_lad{k}']
log('controls done; C1 coverage', round(float(EV.c1_n.notna().mean()), 3), 'C2 coverage', round(float(EV.c2_n.notna().mean()), 3))
del U, cand, cand2

# ---------------------------------------------------------------- B1 series
def b1_series(ND_, ev_di, ev_val):
    s = np.bincount(ev_di, weights=ev_val, minlength=ND_); n = np.bincount(ev_di, minlength=ND_).astype(float)
    CS = np.concatenate([[0.], np.cumsum(s)]); CN = np.concatenate([[0.], np.cumsum(n)])
    on = np.zeros(ND_, bool); used = np.zeros(ND_, int); mean = np.full(ND_, np.nan)
    for i in range(ND_):
        b = i - 2
        if b < 0: continue
        for W in (63, 126):
            a = max(0, b - W + 1); cnt = CN[b + 1] - CN[a]
            if cnt >= 30:
                m = (CS[b + 1] - CS[a]) / cnt; on[i] = m > 0; used[i] = W; mean[i] = m; break
    return on, used, mean
e2v = EV[EV.isE2.astype(bool) & EV.exc.notna()]
daily['B1'], daily['B1_win'], daily['B1_mean'] = b1_series(ND, e2v.di.values, e2v.exc.values)
EV['B1'] = daily.B1.values[EV.di]; EV['B2'] = daily.B2.values[EV.di]
for k in (1, 2, 3): EV[f'B1_lad{k}'] = daily.B1.values[np.minimum(EV.di + k, ND - 1)]
# realized trailing-40-event sd of E1 exc (companion)
e1v = EV[EV.isE1.astype(bool) & EV.exc.notna()].sort_values('date')
rs = e1v.exc.rolling(40, min_periods=40).std(); rsd = pd.Series(rs.values, index=e1v.date.values).groupby(level=0).last()
daily['e1_sd40'] = rsd.reindex(dates).ffill().shift(2).values

# B1 seen-window sanity check on the holdout file (as registered)
ho = pd.read_csv(R + 'OVERNIGHT_holdout_trades.csv.gz'); ho = ho[ho.event == 'E2_gapup_held'].copy(); ho['date'] = pd.to_datetime(ho.date)
hcal = spy_full.index; hpos = pd.Series(np.arange(len(hcal)), index=hcal); ho = ho[ho.date.isin(hcal)]
hon, _, _ = b1_series(len(hcal), hpos.loc[ho.date].values, ho.X1_net.values)
hs = pd.Series(hon, index=hcal)
share_off_1519 = 1 - hs['2015-01-01':'2019-12-31'].mean(); share_on_2021 = hs['2020-01-01':'2021-12-31'].mean()
b1_sanity_pass = bool(share_off_1519 >= .6 and share_on_2021 >= .6)
# engine-based B1 shares for comparison
es = daily.B1
eng_off_1519 = 1 - es['2015-01-01':'2019-12-31'].mean(); eng_on_2021 = es['2020-01-01':'2021-12-31'].mean()
log(f'B1 sanity (holdout file): OFF share 2015-19 {share_off_1519:.3f} (need>=0.6), ON share 2020-21 {share_on_2021:.3f} (need>=0.6) -> pass={b1_sanity_pass}; engine: {eng_off_1519:.3f}/{eng_on_2021:.3f}')

# ---------------------------------------------------------------- earnings coverage check on 2004-2013 (event flags only; NO forward returns)
P04, files04 = load_dir(SP + 'bars1d_2004_2013', '2004')
P04 = {k: v.loc[v.index.isin(spy_full.index) & (v.index <= '2013-12-31')] for k, v in P04.items()}
F04 = features(P04); d04 = P04['Close'].index; s04 = np.array(P04['Close'].columns)
log('P04 diag: shape', P04['Close'].shape, 'elig', int(F04['elig'].values.sum()), 'E2', int(F04['E2'].values.sum()), 'E1', int(F04['E1'].values.sum()),
    'median pc', float(np.nanmedian(F04['pc'].values)), 'median adv_d', float(np.nanmedian(F04['adv_d'].values)), 'V>0 share', float(np.nanmean((P04['Volume'] > 0).values)),
    'elig by year', F04['elig'].sum(axis=1).groupby(d04.year).mean().round(0).to_dict())
m04 = F04['E1'].values & (d04.values[:, None] >= np.datetime64('2004-01-01'))
di4, ti4 = np.nonzero(m04)
res4 = []
for i, t in zip(di4, ti4):
    arr = etk.get(s04[t]); t64 = np.datetime64(d04[i].date())
    res4.append(arr is not None and bool(((arr >= t64 - np.timedelta64(130, 'D')) & (arr <= t64)).any() and ((arr >= t64) & (arr <= t64 + np.timedelta64(130, 'D'))).any()))
n_e1_04 = len(res4); cov04 = float(np.mean(res4)) if res4 else float('nan')
names_by_year_04 = P04['Close'].notna().groupby(P04['Close'].index.year).any().sum(axis=1).to_dict()
promote_7c = bool(n_e1_04 > 0 and cov04 >= .6)
with open(R + 'EXEC_v5_files_2004_2013_at_precheck.txt', 'w') as fh: fh.write('\n'.join(files04))
del P04, F04
m_holm = 5 + (1 if promote_7c else 0) - (0 if b1_sanity_pass else 2)
log(f'2004-2013 precheck: files {len(files04)} (<1500 -> confirmatory NOT run); E1 events(partial files) {n_e1_04}; earnings-resolvable {cov04:.3f}; promote 7c={promote_7c}; Holm m={m_holm}')

# ---------------------------------------------------------------- K5 fill
EV['valid'] = EV.exc.notna() & EV.open_T1.notna()
e = EV[EV.valid & EV.isE1.astype(bool)].sort_values(['date', 'dret'], ascending=[True, False])
EV['rank_E1cell'] = np.nan; EV.loc[e.index, 'rank_E1cell'] = e.groupby('date').cumcount().values + 1
e = EV[EV.valid & EV.isE2.astype(bool)].assign(k=lambda x: -x.isE1.astype(int)).sort_values(['date', 'k', 'dret'], ascending=[True, True, False])
EV['rank_E2cell'] = np.nan; EV.loc[e.index, 'rank_E2cell'] = e.groupby('date').cumcount().values + 1
EV['fill_E1cell'] = EV.rank_E1cell <= 5; EV['fill_E2cell'] = EV.rank_E2cell <= 5
EV['w_v5'] = np.where(EV.isE1, .075, .0375); EV['w_v5'] = np.minimum(EV.w_v5, .02 / (3 * EV.atr20pct.fillna(.02).clip(lower=1e-4)))

# ---------------------------------------------------------------- statistics
def cboot(x, cl, B=NB, seed=0):
    cl = pd.factorize(cl)[0]; G = cl.max() + 1
    s = np.bincount(cl, weights=x, minlength=G); n = np.bincount(cl, minlength=G).astype(float)
    idx = np.random.default_rng(seed).integers(0, G, size=(B, G))
    return s[idx].sum(1) / n[idx].sum(1)
def cstats(x, days, qs, seed=0):
    x = np.asarray(x, float); n = len(x)
    out = dict(n=n, mean=np.nan, lo=np.nan, hi=np.nan, dlo=np.nan, dhi=np.nan, qlo=np.nan, qhi=np.nan, wider='', p=np.nan, qpos=np.nan, nq=0,
               days=0, winsor=np.nan, trim2=np.nan, median=np.nan, win=np.nan, sd=np.nan)
    if n < 3: return out
    m = x.mean(); bd = cboot(x, days, seed=seed); bq = cboot(x, qs, seed=seed + 1)
    dlo, dhi = np.percentile(bd, [2.5, 97.5]); qlo, qhi = np.percentile(bq, [2.5, 97.5])
    wd = 'day' if (dhi - dlo) >= (qhi - qlo) else 'quarter'; b = bd if wd == 'day' else bq; lo, hi = (dlo, dhi) if wd == 'day' else (qlo, qhi)
    p = max(float((b <= 0).mean()), float(((b - m) >= m).mean()))
    qm = pd.Series(x).groupby(np.asarray(qs)).mean()
    w = np.clip(x, np.percentile(x, 1), np.percentile(x, 99)); srt = np.sort(x); kdrop = int(np.ceil(.02 * n))
    out.update(mean=m * 100, lo=lo * 100, hi=hi * 100, dlo=dlo * 100, dhi=dhi * 100, qlo=qlo * 100, qhi=qhi * 100, wider=wd, p=p,
               qpos=float((qm > 0).mean()), nq=len(qm), days=int(pd.Series(days).nunique()), winsor=w.mean() * 100,
               trim2=srt[:n - kdrop].mean() * 100 if n - kdrop > 0 else np.nan, median=np.median(x) * 100, win=float((x > 0).mean()), sd=x.std() * 100)
    return out
def diffboot(x_on, d_on, x_off, d_off, B=NB, seed=7):
    if len(x_on) < 3 or len(x_off) < 3: return np.nan, np.nan, np.nan
    bo = cboot(np.asarray(x_on, float), d_on, B, seed); bf = cboot(np.asarray(x_off, float), d_off, B, seed + 1); d = bo - bf
    m = float(np.mean(x_on) - np.mean(x_off)); p = max(float((d <= 0).mean()), float(((d - m) >= m).mean()))
    lo, hi = np.percentile(d, [2.5, 97.5]); return m * 100, p, (lo * 100, hi * 100)

ROWS = []
def row(cell, window, variant, d, col='exc', benchmark='SPY', note='', family=False, daycol='date', qcol='q'):
    d = d[d[col].notna()]
    st = cstats(d[col].values, d[daycol].values, d[qcol].values)
    r = dict(cell=cell, window=window, variant=variant, benchmark=benchmark, n=st['n'], days=st['days'], mean_excess_net_pct=st['mean'],
             ci_low=st['lo'], ci_high=st['hi'], wider_cluster=st['wider'], ci_day=(st['dlo'], st['dhi']), ci_quarter=(st['qlo'], st['qhi']),
             p_one_sided=st['p'], p_holm=np.nan, quarters_positive_share=st['qpos'], n_quarters=st['nq'], winsor_mean=st['winsor'],
             trim_top2_mean=st['trim2'], median=st['median'], win_share=st['win'], sd_pct=st['sd'], family=family, note=note, passed=False)
    if col == 'exc':
        for cc, nm in (('exc_c1', 'c1'), ('exc_c2', 'c2')):
            dd = d[d[cc].notna()]
            if len(dd) >= 3:
                s2 = cstats(dd[cc].values, dd[daycol].values, dd[qcol].values)
                r[f'matched_{nm}_excess_pct'] = s2['mean']; r[f'{nm}_ci_low'] = s2['lo']; r[f'{nm}_ci_high'] = s2['hi']; r[f'{nm}_n'] = s2['n']
            else: r[f'matched_{nm}_excess_pct'] = np.nan; r[f'{nm}_ci_low'] = np.nan; r[f'{nm}_ci_high'] = np.nan; r[f'{nm}_n'] = len(dd)
        r['raw_net_mean_pct'] = d.net.mean() * 100 if 'net' in d else np.nan
        r['flat10bp_excess_pct'] = d.exc10.mean() * 100 if 'exc10' in d else np.nan
    ROWS.append(r); return r

def win(d, a, b): return d[(d.date >= a) & (d.date <= b)]
PC = []  # per-capital rows
def percap(cell, window, trades, cal, wcol, K):
    if len(trades) == 0: return
    r = trades.groupby('date').apply(lambda g: float((g[wcol] * g.net).sum())).reindex(cal).fillna(0.0)
    eq = (1 + r).cumprod(); mdd = float((eq / eq.cummax() - 1).min())
    yr = (1 + r).groupby(r.index.year).prod() - 1
    xs = trades.groupby('date').apply(lambda g: float((g[wcol] * g.exc).sum())).reindex(cal).fillna(0.0)
    yx = (1 + xs).groupby(xs.index.year).prod() - 1
    PC.append(dict(cell=cell, window=window, K=K, weights=('20% each' if wcol == 'w20' else 'v5 7.5%/3.75% ATR-capped'), total_return_pct=float(eq.iloc[-1] - 1) * 100,
                   max_drawdown_pct=mdd * 100, years_positive_share=float((yr > 0).mean()), n_trades=len(trades),
                   yearly_pct=json.dumps({int(k): round(v * 100, 2) for k, v in yr.items()}), yearly_excess_pct=json.dumps({int(k): round(v * 100, 2) for k, v in yx.items()})))
EV['w20'] = .2

def run_window(EVw, wl, cal, tag):
    """all cells on one window; EVw already filtered to window dates."""
    E1 = EVw[EVw.isE1.astype(bool) & EVw.valid]; E2 = EVw[EVw.isE2.astype(bool) & EVw.valid]; M1 = EVw[EVw.isM1.astype(bool) & EVw.exc.notna()]
    out = {}
    for cell, base, gate, k5col in (('V5-1', E1, None, 'fill_E1cell'), ('V5-2', E2, None, 'fill_E2cell'), ('V5-3', E1, 'B1', 'fill_E1cell'),
                                    ('V5-4', E2, 'B1', 'fill_E2cell'), ('V5-5', E1, 'B2', 'fill_E1cell')):
        if gate is None:
            d = base; r = row(cell, wl, 'primary_all_events', d, family=True)
        else:
            d = base[base[gate].astype(bool)]; off = base[~base[gate].astype(bool)]
            r = row(cell, wl, f'ON_{gate}', d, family=True); ro = row(cell, wl, f'OFF_{gate}', off)
            dm, dp, dci = diffboot(d.exc.values, d.date.values, off.exc.values, off.date.values)
            r['on_minus_off_pct'] = dm; r['on_minus_off_p'] = dp; r['on_minus_off_ci'] = dci
            r['on_share_days'] = float(daily.loc[cal, gate].mean()); r['off_lower_ci'] = ro['ci_low']; r['off_n'] = ro['n']
            for hn, (a, b) in (('H1', H1), ('H2', H2)):
                dh, oh = win(d, a, b), win(off, a, b)
                if len(dh) >= 3: row(cell, wl, f'ON_{gate}_{hn}', dh)
                if len(oh) >= 3: row(cell, wl, f'OFF_{gate}_{hn}', oh)
                r[f'on_ge_off_{hn}'] = bool(dh.exc.mean() >= oh.exc.mean()) if len(dh) >= 3 and len(oh) >= 3 else None
            if gate == 'B2':
                qq = EVw[EVw.isE2.astype(bool) & EVw.valid].groupby('q').exc.mean(); qd = daily.loc[cal].groupby(daily.loc[cal].index.to_period('Q').astype(str)).m21.mean()
                jj = pd.concat([qq, qd], axis=1).dropna()
                if len(jj) >= 6:
                    rho, pp = sst.spearmanr(jj.iloc[:, 0], jj.iloc[:, 1]); r['spearman_rho_disp_vs_E2q'] = rho; r['spearman_p_one_sided'] = pp / 2 if rho > 0 else 1 - pp / 2
        row(cell, wl, 'K5_filled', d[d[k5col].astype(bool)], note='trades actually filled at K=5 E1-first by day return')
        row(cell, wl, 'flat10bp', d, col='exc10', note='cost sensitivity flat 10bp/side')
        for hn, (a, b) in (('H1_2014-09..2019-12', H1), ('H2_2020-01..2024-08', H2)):
            dh = win(d, a, b)
            if len(dh) >= 3: row(cell, wl, hn, dh)
        for tier in ('>=500M', '100-500M', '50-100M'):
            dt = d[d.adv_tier == tier]
            if len(dt) >= 3: row(cell, wl, f'tier_{tier}', dt)
        if len(d) >= 9:
            terc = pd.qcut(d.adv_d.rank(method='first'), 3, labels=['T1_least_liquid', 'T2', 'T3_most_liquid'])
            for t in ['T1_least_liquid', 'T2', 'T3_most_liquid']: row(cell, wl, f'liq_{t}', d[terc == t])
        if cell in ('V5-1', 'V5-2'):
            row(cell, wl, 'placebo_C5toO6_vs_C1', d, col='plc_exc_c1', benchmark='C1', note='same events, Close_T+5 -> Open_T+6, minus C1 controls same leg')
            row(cell, wl, 'placebo_C5toO6_vs_SPY', d, col='plc_exc_spy', benchmark='SPY')
            row(cell, wl, 'cf_next_close_X2', d, col='cf_next_close_exc', note='counterfactual: hold to Close_T+1 (never traded)')
            row(cell, wl, 'holdout_repro_flat5bp', d, col='exc5', note='flat 5bp/side as in OVERNIGHT holdout; compare n and mean')
        if cell == 'V5-1':
            row('M1', wl, 'mirror_gapdown_overnight_long', M1, note='mechanism falsifier; long leg of gap<=-8%, vol>=3x, red close')
        # per-capital
        percap(cell, wl, d[d[k5col].astype(bool)], cal, 'w20', 5); percap(cell, wl, d[d[k5col].astype(bool)], cal, 'w_v5', 5)
        out[cell] = r
    # V5-6 ladder (E1)
    lad = []
    for k in (1, 2, 3):
        dk = E1[E1[f'cond{k}'].astype(bool) & E1[f'lad{k}_exc'].notna()].copy()
        dk['exc'] = dk[f'lad{k}_exc']; dk['exc_c1'] = dk[f'lad{k}_exc_c1']; dk['exc_c2'] = np.nan; dk['net'] = dk[f'lad{k}n']; dk['exc10'] = netret(dk[f'lad{k}'], 1e-3) - dk[f'lad{k}_spy']
        dk['date'] = dk[f'lad{k}_date']; dk['q'] = dk.date.dt.to_period('Q').astype(str); dk['B1'] = dk[f'B1_lad{k}']; dk['k'] = k; lad.append(dk)
        row('V5-6', wl, f'ladder_k{k}_ungated', dk, note='buy MOC Close_T+k if Close_T+k>=Close_T; sell MOO T+k+1')
        row('V5-6', wl, f'ladder_k{k}_B1', dk[dk.B1.astype(bool)])
    lad = pd.concat(lad) if lad else E1.iloc[0:0]
    if len(lad):
        row('V5-6', wl, 'ladder_pooled_ungated', lad); row('V5-6', wl, 'ladder_pooled_B1', lad[lad.B1.astype(bool)])
        for hn, (a, b) in (('H1_2014-09..2019-12', H1), ('H2_2020-01..2024-08', H2)):
            dh = win(lad, a, b)
            if len(dh) >= 3: row('V5-6', wl, f'ladder_pooled_{hn}', dh)
    # V5-7 slices
    row('V5-7a', wl, 'E1_extreme_ge25', E1[E1.extreme_mover.astype(bool)]); row('V5-7a', wl, 'E1_8to25', E1[~E1.extreme_mover.astype(bool)])
    row('V5-7b', wl, 'E1_deal_pin_only', E1[E1.deal_pin.astype(bool)]); r7b = row('V5-7b', wl, 'E1_ex_deal_pin', E1[~E1.deal_pin.astype(bool)])
    row('V5-7b', wl, 'E2_deal_pin_only', E2[E2.deal_pin.astype(bool)]); row('V5-7b', wl, 'E2_ex_deal_pin', E2[~E2.deal_pin.astype(bool)])
    for nm, base in (('E1', E1), ('E2', E2)):
        res = base[base.earn_resolvable.astype(bool)]
        row('V5-7c', wl, f'{nm}_earnings', res[res.earnings.astype(bool)], note=f'coverage(resolvable)={base.earn_resolvable.mean():.2f}')
        row('V5-7c', wl, f'{nm}_non_earnings_resolvable', res[~res.earnings.astype(bool)])
        row('V5-7c', wl, f'{nm}_earnings_tonight_flag', base[base.earnings_tonight.astype(bool)], note='report dated T amc or T+1 bmo/unknown (new binary overnight)')
        row('V5-7e', wl, f'{nm}_spy_le_-3pct_nights', base[base.spy_day_ret <= -.03]); row('V5-7e', wl, f'{nm}_ex_spy_le_-3pct', base[~(base.spy_day_ret <= -.03)])
        row('V5-7e', wl, f'{nm}_macro_eve_nfp_approx', base[base.macro_eve_nfp_approx.astype(bool)], note='approx: T+1 is first Friday of month; BLS calendar not on disk; CPI not flagged')
        row('V5-7e', wl, f'{nm}_ex_macro_eve_nfp_approx', base[~base.macro_eve_nfp_approx.astype(bool)])
    # 7f capacity
    pn = E1.groupby('date').size(); pn2 = E2.groupby('date').size()
    cap = dict(E1_events_per_night=pn.describe().to_dict(), E2_events_per_night=pn2.describe().to_dict(),
               E1_share_nights_gt5=float((pn > 5).mean()), E1_share_events_unfilled=float(1 - E1.fill_E1cell.mean()),
               E2_share_nights_gt5=float((pn2 > 5).mean()), E2_share_events_unfilled=float(1 - E2.fill_E2cell.mean()),
               E1_nights=int(len(pn)), E2_nights=int(len(pn2)), trading_days=int(len(cal)))
    # 7g rank non-inferiority on nights with >5 E2 qualifiers
    big = E2[E2.date.isin(pn2[pn2 > 5].index)]
    if len(big):
        t_d = big.sort_values(['date', 'dret'], ascending=[True, False]).groupby('date').head(5)
        t_r = big.sort_values(['date', 'rvol'], ascending=[True, False]).groupby('date').head(5)
        row('V5-7g', wl, 'E2_top5_by_dayret_nights_gt5', t_d); row('V5-7g', wl, 'E2_top5_by_rvol_nights_gt5', t_r)
        cap['rank_diff_rvol_minus_dret_pct'] = float((t_r.exc.mean() - t_d.exc.mean()) * 100)
    return out, cap

cal_all = daily.index
seen_cal = cal_all[(cal_all >= CUT_SEEN[0]) & (cal_all <= CUT_SEEN[1])]; disc_cal = cal_all[(cal_all >= DISC[0]) & (cal_all <= DISC[1])]
EVs = win(EV, *CUT_SEEN); EVd = win(EV, *DISC)
log('running seen window', len(EVs)); res_seen, cap_seen = run_window(EVs, 'refinement on a seen window 2014-09..2024-08', seen_cal, 'seen')
log('running discovery window', len(EVd)); res_disc, cap_disc = run_window(EVd, 'discovery (seen) 2024-09..2026-09 descriptive', disc_cal, 'disc')
# pooled context (seen+disc)
row('V5-1', 'pooled 2014-09..2026-09 (context only)', 'primary_all_events', pd.concat([EVs, EVd])[lambda x: x.isE1.astype(bool) & x.valid])
row('V5-2', 'pooled 2014-09..2026-09 (context only)', 'primary_all_events', pd.concat([EVs, EVd])[lambda x: x.isE2.astype(bool) & x.valid])

# Holm across the family on the seen window (reported; no pass possible without the confirmatory window)
if promote_7c:
    for r in ROWS:
        if r['cell'] == 'V5-7c' and r['variant'] == 'E1_earnings' and r['window'].startswith('refinement'): r['family'] = True; r['cell'] = 'V5-7c(promoted)'
fam = [r for r in ROWS if r['family'] and r['window'].startswith('refinement')]
if not b1_sanity_pass: fam = [r for r in fam if r['cell'] not in ('V5-3', 'V5-4')]
ps = np.array([r['p_one_sided'] for r in fam]); order = np.argsort(ps); adj = np.empty(len(ps)); m = m_holm
run = 0
for rank_, i in enumerate(order):
    run = max(run, min(1.0, (m - rank_) * ps[i])); adj[i] = run
for r, a in zip(fam, adj): r['p_holm'] = float(a)

# gates checkable on the seen window (informational)
for r in fam:
    g = []
    g.append(('H1_lowerCI>0', bool(next((x['ci_low'] > 0 for x in ROWS if x['cell'] == r['cell'] and x['window'] == r['window'] and x['variant'].startswith('H1')), False))))
    g.append(('qpos>=0.60', bool(r['quarters_positive_share'] >= .6))); g.append(('holm<0.05', bool(r['p_holm'] < .05)))
    hc = .25 if r['cell'] in ('V5-1', 'V5-3', 'V5-5') else .15
    g.append(('C1>=haircut&lowerCI>0', bool(r.get('matched_c1_excess_pct', np.nan) >= hc and r.get('c1_ci_low', np.nan) > 0)))
    g.append(('C2>0', bool(r.get('matched_c2_excess_pct', np.nan) > 0))); g.append(('winsor&trim>0', bool(r['winsor_mean'] > 0 and r['trim_top2_mean'] > 0)))
    if 'on_minus_off_pct' in r:
        thr = .3 if r['cell'] == 'V5-4' else .5
        g.append((f'ON-OFF>={thr}&p<.05', bool(r['on_minus_off_pct'] >= thr and r['on_minus_off_p'] < .05)))
        g.append(('ON_share_25-75', bool(.25 <= r['on_share_days'] <= .75))); g.append(('OFF_not_sig_pos', bool(not (r['off_lower_ci'] > 0))))
    r['gates_met_seen'] = ';'.join(f'{k}={int(v)}' for k, v in g); r['passed'] = False
    r['note'] = 'confirmatory 2004-2013 NOT run (982/3179 files); seen-window refinement only -> no pass possible. ' + r['note']

# ---------------------------------------------------------------- outputs
cells = pd.DataFrame(ROWS); cells.to_csv(R + f'{KEY}_cells.csv', index=False)
pcdf = pd.DataFrame(PC); pcdf.to_csv(R + f'{KEY}_percapital.csv', index=False)
daily.to_csv(R + f'{KEY}_daily_gates.csv')
tcols = [c for c in EV.columns if c not in ('di', 'ti')]
EV.loc[EV.date >= CUT_SEEN[0], tcols].to_csv(R + f'{KEY}_trades.csv.gz', index=False, compression='gzip')
# yearly table of excess per cell
def ytab(d, name):
    g = d.groupby('year').exc.agg(['size', 'mean']); g['mean'] = g['mean'] * 100; g.columns = [f'{name}_n', f'{name}_mean%']; return g
E1a = EV[EV.isE1.astype(bool) & EV.valid & (EV.date >= CUT_SEEN[0])]; E2a = EV[EV.isE2.astype(bool) & EV.valid & (EV.date >= CUT_SEEN[0])]
Y = pd.concat([ytab(E1a, 'E1'), ytab(E2a, 'E2'), ytab(E1a[E1a.B1.astype(bool)], 'E1|B1on'), ytab(E2a[E2a.B1.astype(bool)], 'E2|B1on'), ytab(E1a[E1a.B2.astype(bool)], 'E1|B2on'),
               daily.loc[daily.index >= CUT_SEEN[0]].groupby(daily.loc[daily.index >= CUT_SEEN[0]].index.year)[['B1', 'B2']].mean().rename(columns={'B1': 'B1_on_share', 'B2': 'B2_on_share'})], axis=1)
Y.to_csv(R + f'{KEY}_yearly.csv')
summary = dict(b1_sanity=dict(off_share_2015_2019=float(share_off_1519), on_share_2020_2021=float(share_on_2021), passed=b1_sanity_pass, engine_off_1519=float(eng_off_1519), engine_on_2021=float(eng_on_2021)),
               precheck_2004_2013=dict(files=len(files04), e1_events_partial=n_e1_04, earnings_resolvable_share=cov04, promote_7c=promote_7c, names_by_year=names_by_year_04),
               holm_m=m_holm, capacity_seen=cap_seen, capacity_disc=cap_disc, panel=dict(days=ND, names=NS), earnings_rows=len(earn),
               earn_resolvable_share_seen=float(EVs.earn_resolvable.mean()), earn_resolvable_by_year=EV[EV.date >= CUT_SEEN[0]].groupby('year').earn_resolvable.mean().round(3).to_dict(),
               c1_cov=float(EV.c1_n.notna().mean()), c2_cov=float(EV.c2_n.notna().mean()), splice_cut=str(cut.date()))
json.dump(summary, open(R + f'{KEY}_summary.json', 'w'), indent=1, default=str)
log('DONE. rows', len(cells)); print(Y.round(3).to_string())
fam_show = cells[cells.family & cells.window.str.startswith('refinement')][['cell', 'variant', 'n', 'mean_excess_net_pct', 'ci_low', 'ci_high', 'p_one_sided', 'p_holm', 'quarters_positive_share', 'matched_c1_excess_pct', 'c1_ci_low', 'matched_c2_excess_pct', 'gates_met_seen']]
print(fam_show.round(3).to_string())
