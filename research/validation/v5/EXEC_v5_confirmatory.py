"""EXEC_v5_confirmatory.py -- the single pre-declared PARTIAL confirmatory run of preregistration_v5.json
(sha256 6222b79d...2b1e1) on scratchpad/bars1d_2004_2013.  Label: 'confirmatory, unseen, partial universe (1,225 names)'.
Every function/convention is copied from EXEC_v5_engine.py; NO threshold or definition is changed.  Run ONCE (2026-09-13).
Additions relative to the engine: (a) frozen file list, (b) B1 one-day-shifted sensitivity variant [T-63,T-2] (verifier flag),
(c) registered gate-by-gate pass evaluation and the GREEN/AMBER/RED decision tree in code, (d) kill rules K1-K4 in code.
"""
import numpy as np, pandas as pd, glob, os, sys, time, zlib, json, hashlib, warnings, csv, datetime
from scipy import stats as sst
warnings.filterwarnings('ignore')
SP = 'C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad/'
R = SP + 'research/'; KEY = 'EXEC_v5_confirmatory'
LABEL = 'confirmatory, unseen, partial universe (1,225 names)'
W0, W1 = '2004-01-01', '2013-12-31'
H1 = ('2004-01-01', '2008-12-31'); H2 = ('2009-01-01', '2013-12-31'); H1N = 'H1_2004-01..2008-12'; H2N = 'H2_2009-01..2013-12'
NB = 2000; T0 = time.time()
def log(*a): print(f'[{time.time()-T0:7.1f}s]', *a, flush=True)
def netret(leg, c): return (1 + leg) * (1 - c) / (1 + c) - 1
FLD = ['Open', 'High', 'Low', 'Close', 'Volume']
RUN_TS = datetime.datetime.now().isoformat(timespec='seconds')

# ---------------------------------------------------------------- 1. freeze the file list
DIR = SP + 'bars1d_2004_2013'
files = sorted(glob.glob(os.path.join(DIR, '*.csv'))); fnames = [os.path.basename(f) for f in files]
frozen_txt = '\n'.join(fnames) + '\n'
with open(R + 'EXEC_v5_files_2004_2013_frozen.txt', 'w') as fh: fh.write(frozen_txt)
frozen_sha = hashlib.sha256(frozen_txt.encode()).hexdigest()
pre = [l.strip() for l in open(R + 'EXEC_v5_files_2004_2013_at_precheck.txt') if l.strip()]
same_as_precheck = set(pre) == set(fnames)
PARTIAL = len(fnames) < 1500
log('frozen', len(fnames), 'files; sha256', frozen_sha[:16], '; identical to precheck list:', same_as_precheck, '; partial(<1500):', PARTIAL)

# ---------------------------------------------------------------- loading (verbatim from engine)
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
    if sundays > 0:
        P = {k: v.set_index(v.index + pd.Timedelta(days=1)) for k, v in P.items()}
        log(tag, 'DATE SHIFT APPLIED (+1 calendar day): labels contained Sundays')
    return P, [os.path.basename(f) for f in files]

spy = pd.read_csv(R + 'EXEC_v5_SPY_2004_2026.csv'); spy['d'] = pd.to_datetime(spy.date); spy = spy.set_index('d').sort_index()
P04, files04 = load_dir(DIR, '2004')
assert files04 == fnames
cal = spy.index[(spy.index >= W0) & (spy.index <= W1)]
raw_dates = P04['Close'].index
n_spy_days_absent_from_panel = int(len(cal.difference(raw_dates))); n_panel_days_not_in_spy = int(len(raw_dates[(raw_dates >= W0) & (raw_dates <= W1)].difference(cal)))
P = {k: v.reindex(cal) for k, v in P04.items()}   # panel on the SPY calendar (stricter: T+1 is always the next SPY trading day)
names_by_year = P04['Close'].notna().groupby(P04['Close'].index.year).any().sum(axis=1).to_dict()
del P04
dates = P['Close'].index; syms = np.array(P['Close'].columns); ND, NS = len(dates), len(syms)
log('panel', ND, 'days x', NS, 'names', dates.min().date(), '..', dates.max().date(), '| SPY days absent from panel', n_spy_days_absent_from_panel, '| panel days not in SPY', n_panel_days_not_in_spy)
spy_full = spy.copy(); spy = spy.reindex(dates)
spy_x1 = (spy.Open.shift(-1) / spy.Close - 1).values; spy_dret = (spy.Close / spy.Close.shift(1) - 1).values

# ---------------------------------------------------------------- features (verbatim)
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
daily['B2_defined'] = daily.med504.notna()
daily['att21'] = pd.Series(top20, index=dates).rolling(21, min_periods=21).mean().shift(1)
daily['catrate63'] = (daily.nE2 / daily.n_elig.replace(0, np.nan) * 1000).rolling(63, min_periods=30).mean().shift(1)
B2_first = daily.index[daily.B2_defined.values].min() if daily.B2_defined.any() else None
elig_by_year = daily.n_elig.groupby(daily.index.year).mean().round(0).astype(int).to_dict()
log('daily series done; B2 defined from', B2_first.date() if B2_first is not None else None, '| eligible names by year', elig_by_year)

# ---------------------------------------------------------------- long tables (verbatim)
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
EV['half'] = np.where(EV.date <= H1[1], H1N, H2N)
nxt = dates[np.minimum(EV.di + 1, ND - 1)]
EV['macro_eve_nfp_approx'] = (nxt.weekday == 4) & (nxt.day <= 7)
log('EV rows', len(EV), 'E1', int(EV.isE1.sum()), 'E2', int(EV.isE2.sum()), 'M1', int(EV.isM1.sum()), 'first event', EV.date.min().date())

# earnings flags (verbatim)
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

# ---------------------------------------------------------------- matched controls C1 / C2 (verbatim, registered seed)
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
        m = (np.abs(d - r) <= .01) & (d >= lo_) & (d <= hi_)
        if m.any():
            sel = ix2[m][np.argsort(np.abs(d[m] - r))[:3]]; c2[j] = [c2x[sel].mean(), len(sel)]
EV['c1_net'], EV['c1_plc'], EV['c1_lad1'], EV['c1_lad2'], EV['c1_lad3'], EV['c1_n'] = [c1[:, i] for i in range(6)]
EV['c2_net'], EV['c2_n'] = c2[:, 0], c2[:, 1]
EV['exc_c1'] = EV.net - EV.c1_net; EV['exc_c2'] = EV.net - EV.c2_net; EV['plc_exc_c1'] = EV.plc_net - EV.c1_plc
for k in (1, 2, 3): EV[f'lad{k}_exc_c1'] = EV[f'lad{k}n'] - EV[f'c1_lad{k}']
c1_cov = float(EV.c1_n.notna().mean()); c2_cov = float(EV.c2_n.notna().mean())
log('controls done; C1 coverage', round(c1_cov, 3), 'C2 coverage', round(c2_cov, 3))
del U, cand, cand2

# ---------------------------------------------------------------- B1 series: executor window [T-64,T-2] (shift=0) and verifier variant [T-63,T-2] (shift=1)
def b1_series(ND_, ev_di, ev_val, shift=0):
    s = np.bincount(ev_di, weights=ev_val, minlength=ND_); n = np.bincount(ev_di, minlength=ND_).astype(float)
    CS = np.concatenate([[0.], np.cumsum(s)]); CN = np.concatenate([[0.], np.cumsum(n)])
    on = np.zeros(ND_, bool); used = np.zeros(ND_, int); mean = np.full(ND_, np.nan)
    for i in range(ND_):
        b = i - 2
        if b < 0: continue
        for W in (63, 126):
            a = max(0, b - W + 1 + shift); cnt = CN[b + 1] - CN[a]
            if cnt >= 30:
                m = (CS[b + 1] - CS[a]) / cnt; on[i] = m > 0; used[i] = W; mean[i] = m; break
    return on, used, mean
e2v = EV[EV.isE2.astype(bool) & EV.exc.notna()]
daily['B1'], daily['B1_win'], daily['B1_mean'] = b1_series(ND, e2v.di.values, e2v.exc.values, 0)
daily['B1v'], daily['B1v_win'], daily['B1v_mean'] = b1_series(ND, e2v.di.values, e2v.exc.values, 1)
daily['B1_defined'] = daily.B1_win > 0
B1_first = daily.index[daily.B1_defined.values].min() if daily.B1_defined.any() else None
EV['B1'] = daily.B1.values[EV.di]; EV['B1v'] = daily.B1v.values[EV.di]; EV['B2'] = daily.B2.values[EV.di]
for k in (1, 2, 3): EV[f'B1_lad{k}'] = daily.B1.values[np.minimum(EV.di + k, ND - 1)]
e1v = EV[EV.isE1.astype(bool) & EV.exc.notna()].sort_values('date')
rs = e1v.exc.rolling(40, min_periods=40).std(); rsd = pd.Series(rs.values, index=e1v.date.values).groupby(level=0).last()
daily['e1_sd40'] = rsd.reindex(dates).ffill().shift(2).values
b1_agree = float((daily.B1 == daily.B1v).mean())
log('B1 defined from', B1_first.date() if B1_first is not None else None, '| ON share full window', round(float(daily.B1.mean()), 3), 'variant', round(float(daily.B1v.mean()), 3),
    '| day agreement B1 vs variant', round(b1_agree, 4), '| 63-day used share', round(float((daily.B1_win == 63).mean()), 3), '126:', round(float((daily.B1_win == 126).mean()), 3))

# ---------------------------------------------------------------- K5 fill (verbatim)
EV['valid'] = EV.exc.notna() & EV.open_T1.notna()
e = EV[EV.valid & EV.isE1.astype(bool)].sort_values(['date', 'dret'], ascending=[True, False])
EV['rank_E1cell'] = np.nan; EV.loc[e.index, 'rank_E1cell'] = e.groupby('date').cumcount().values + 1
e = EV[EV.valid & EV.isE2.astype(bool)].assign(k=lambda x: -x.isE1.astype(int)).sort_values(['date', 'k', 'dret'], ascending=[True, True, False])
EV['rank_E2cell'] = np.nan; EV.loc[e.index, 'rank_E2cell'] = e.groupby('date').cumcount().values + 1
EV['fill_E1cell'] = EV.rank_E1cell <= 5; EV['fill_E2cell'] = EV.rank_E2cell <= 5
EV['w_v5'] = np.where(EV.isE1, .075, .0375); EV['w_v5'] = np.minimum(EV.w_v5, .02 / (3 * EV.atr20pct.fillna(.02).clip(lower=1e-4)))
EV['w20'] = .2

# ---------------------------------------------------------------- statistics (verbatim)
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
    if len(x_on) < 3 or len(x_off) < 3: return np.nan, np.nan, (np.nan, np.nan)
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
PC = []
def percap(cell, window, trades, cal_, wcol, K):
    if len(trades) == 0: return
    r = trades.groupby('date').apply(lambda g: float((g[wcol] * g.net).sum())).reindex(cal_).fillna(0.0)
    eq = (1 + r).cumprod(); mdd = float((eq / eq.cummax() - 1).min())
    yr = (1 + r).groupby(r.index.year).prod() - 1
    xs = trades.groupby('date').apply(lambda g: float((g[wcol] * g.exc).sum())).reindex(cal_).fillna(0.0)
    yx = (1 + xs).groupby(xs.index.year).prod() - 1
    PC.append(dict(cell=cell, window=window, K=K, weights=('20% each' if wcol == 'w20' else 'v5 7.5%/3.75% ATR-capped'), total_return_pct=float(eq.iloc[-1] - 1) * 100,
                   max_drawdown_pct=mdd * 100, years_positive_share=float((yr > 0).mean()), n_trades=len(trades), nights=int((r != 0).sum()),
                   yearly_pct=json.dumps({int(k): round(v * 100, 2) for k, v in yr.items()}), yearly_excess_pct=json.dumps({int(k): round(v * 100, 2) for k, v in yx.items()})))

# ---------------------------------------------------------------- 4. cells on the confirmatory window
E1 = EV[EV.isE1.astype(bool) & EV.valid]; E2 = EV[EV.isE2.astype(bool) & EV.valid]; M1 = EV[EV.isM1.astype(bool) & EV.exc.notna()]
def gated(cell, base, gate, tag, family):
    d = base[base[gate].astype(bool)]; off = base[~base[gate].astype(bool)]
    r = row(cell, LABEL, f'ON_{tag}', d, family=family); ro = row(cell, LABEL, f'OFF_{tag}', off)
    dm, dp, dci = diffboot(d.exc.values, d.date.values, off.exc.values, off.date.values)
    defined_col = 'B1_defined' if gate.startswith('B1') else 'B2_defined'
    r.update(on_minus_off_pct=dm, on_minus_off_p=dp, on_minus_off_ci=dci, on_share_days=float(daily[gate].mean()),
             on_share_defined_days=float(daily.loc[daily[defined_col], gate].mean()) if daily[defined_col].any() else np.nan,
             gate_defined_from=str(daily.index[daily[defined_col].values].min().date()) if daily[defined_col].any() else None,
             off_lower_ci=ro['ci_low'], off_upper_ci=ro['ci_high'], off_n=ro['n'], off_mean_pct=ro['mean_excess_net_pct'], off_qpos=ro['quarters_positive_share'])
    for hn, (a, b), nm in ((H1N, H1, 'H1'), (H2N, H2, 'H2')):
        dh, oh = win(d, a, b), win(off, a, b)
        if len(dh) >= 3: row(cell, LABEL, f'ON_{tag}_{hn}', dh)
        if len(oh) >= 3: row(cell, LABEL, f'OFF_{tag}_{hn}', oh)
        r[f'on_ge_off_{nm}'] = bool(dh.exc.mean() >= oh.exc.mean()) if len(dh) >= 3 and len(oh) >= 3 else None
        r[f'on_mean_{nm}'] = dh.exc.mean() * 100 if len(dh) else np.nan; r[f'off_mean_{nm}'] = oh.exc.mean() * 100 if len(oh) else np.nan
        r[f'on_n_{nm}'] = len(dh); r[f'off_n_{nm}'] = len(oh)
    return r, d
def spearman_block(r):
    qq = E2.groupby('q').exc.mean(); qd = daily.groupby(daily.index.to_period('Q').astype(str)).m21.mean()
    jj = pd.concat([qq, qd], axis=1).dropna()
    if len(jj) >= 6:
        rho, pp = sst.spearmanr(jj.iloc[:, 0], jj.iloc[:, 1]); r['spearman_rho_disp_vs_E2q'] = float(rho); r['spearman_p_one_sided'] = float(pp / 2 if rho > 0 else 1 - pp / 2); r['spearman_nq'] = len(jj)
OUT = {}; SENS = {}
for cell, base, gate, k5col in (('V5-1', E1, None, 'fill_E1cell'), ('V5-2', E2, None, 'fill_E2cell'), ('V5-3', E1, 'B1', 'fill_E1cell'),
                                ('V5-4', E2, 'B1', 'fill_E2cell'), ('V5-5', E1, 'B2', 'fill_E1cell')):
    if gate is None: d = base; r = row(cell, LABEL, 'primary_all_events', d, family=True)
    else:
        r, d = gated(cell, base, gate, gate, True)
        if gate == 'B1': SENS[cell], _ = gated(cell, base, 'B1v', 'B1v_[T-63,T-2]_sensitivity', False)
        if gate == 'B2': spearman_block(r)
    row(cell, LABEL, 'K5_filled', d[d[k5col].astype(bool)], note='trades actually filled at K=5 E1-first by day return')
    row(cell, LABEL, 'flat10bp', d, col='exc10', note='cost sensitivity flat 10bp/side')
    for hn, (a, b) in ((H1N, H1), (H2N, H2)):
        dh = win(d, a, b)
        if len(dh) >= 3: row(cell, LABEL, hn, dh)
    for tier in ('>=500M', '100-500M', '50-100M'):
        dt = d[d.adv_tier == tier]
        if len(dt) >= 3: row(cell, LABEL, f'tier_{tier}', dt)
    if len(d) >= 9:
        terc = pd.qcut(d.adv_d.rank(method='first'), 3, labels=['T1_least_liquid', 'T2', 'T3_most_liquid'])
        for t in ['T1_least_liquid', 'T2', 'T3_most_liquid']: row(cell, LABEL, f'liq_{t}', d[terc == t])
    if cell in ('V5-1', 'V5-2'):
        row(cell, LABEL, 'placebo_C5toO6_vs_C1', d, col='plc_exc_c1', benchmark='C1', note='same events, Close_T+5 -> Open_T+6, minus C1 controls same leg')
        row(cell, LABEL, 'placebo_C5toO6_vs_SPY', d, col='plc_exc_spy', benchmark='SPY')
        row(cell, LABEL, 'cf_next_close_X2', d, col='cf_next_close_exc', note='counterfactual: hold to Close_T+1 (never traded)')
        row(cell, LABEL, 'flat5bp', d, col='exc5', note='flat 5bp/side (holdout convention)')
    if cell == 'V5-1':
        row('M1', LABEL, 'mirror_gapdown_overnight_long', M1, note='mechanism falsifier; long leg of gap<=-8%, vol>=3x, red close')
        for hn, (a, b) in ((H1N, H1), (H2N, H2)):
            mh = win(M1, a, b)
            if len(mh) >= 3: row('M1', LABEL, f'mirror_{hn}', mh)
    percap(cell, LABEL, d[d[k5col].astype(bool)], dates, 'w20', 5); percap(cell, LABEL, d[d[k5col].astype(bool)], dates, 'w_v5', 5)
    OUT[cell] = r
# V5-6 ladder
lad = []
for k in (1, 2, 3):
    dk = E1[E1[f'cond{k}'].astype(bool) & E1[f'lad{k}_exc'].notna()].copy()
    dk['exc'] = dk[f'lad{k}_exc']; dk['exc_c1'] = dk[f'lad{k}_exc_c1']; dk['exc_c2'] = np.nan; dk['net'] = dk[f'lad{k}n']; dk['exc10'] = netret(dk[f'lad{k}'], 1e-3) - dk[f'lad{k}_spy']
    dk['date'] = dk[f'lad{k}_date']; dk['q'] = dk.date.dt.to_period('Q').astype(str); dk['B1'] = dk[f'B1_lad{k}']; dk['k'] = k; lad.append(dk)
    row('V5-6', LABEL, f'ladder_k{k}_ungated', dk, note='buy MOC Close_T+k if Close_T+k>=Close_T; sell MOO T+k+1')
    row('V5-6', LABEL, f'ladder_k{k}_B1', dk[dk.B1.astype(bool)])
lad = pd.concat(lad) if lad else E1.iloc[0:0]
if len(lad):
    r6 = row('V5-6', LABEL, 'ladder_pooled_ungated', lad); r6b = row('V5-6', LABEL, 'ladder_pooled_B1', lad[lad.B1.astype(bool)])
    for hn, (a, b) in ((H1N, H1), (H2N, H2)):
        dh = win(lad, a, b)
        if len(dh) >= 3: row('V5-6', LABEL, f'ladder_pooled_{hn}', dh)
# V5-7 slices
row('V5-7a', LABEL, 'E1_extreme_ge25', E1[E1.extreme_mover.astype(bool)]); row('V5-7a', LABEL, 'E1_8to25', E1[~E1.extreme_mover.astype(bool)])
row('V5-7b', LABEL, 'E1_deal_pin_only', E1[E1.deal_pin.astype(bool)]); row('V5-7b', LABEL, 'E1_ex_deal_pin', E1[~E1.deal_pin.astype(bool)])
row('V5-7b', LABEL, 'E2_deal_pin_only', E2[E2.deal_pin.astype(bool)]); row('V5-7b', LABEL, 'E2_ex_deal_pin', E2[~E2.deal_pin.astype(bool)])
for nm, base in (('E1', E1), ('E2', E2)):
    res = base[base.earn_resolvable.astype(bool)]
    row('V5-7c', LABEL, f'{nm}_earnings', res[res.earnings.astype(bool)], note=f'NOT promoted (coverage<60%); coverage(resolvable)={base.earn_resolvable.mean():.3f}')
    row('V5-7c', LABEL, f'{nm}_non_earnings_resolvable', res[~res.earnings.astype(bool)])
    row('V5-7e', LABEL, f'{nm}_spy_le_-3pct_nights', base[base.spy_day_ret <= -.03]); row('V5-7e', LABEL, f'{nm}_ex_spy_le_-3pct', base[~(base.spy_day_ret <= -.03)])
    row('V5-7e', LABEL, f'{nm}_macro_eve_nfp_approx', base[base.macro_eve_nfp_approx.astype(bool)], note='approx: T+1 is first Friday of month'); row('V5-7e', LABEL, f'{nm}_ex_macro_eve_nfp_approx', base[~base.macro_eve_nfp_approx.astype(bool)])
pn = E1.groupby('date').size(); pn2 = E2.groupby('date').size()
cap = dict(E1_events_per_night=pn.describe().to_dict(), E2_events_per_night=pn2.describe().to_dict(), E1_share_nights_gt5=float((pn > 5).mean()),
           E1_share_events_unfilled=float(1 - E1.fill_E1cell.mean()), E2_share_nights_gt5=float((pn2 > 5).mean()), E2_share_events_unfilled=float(1 - E2.fill_E2cell.mean()),
           E1_nights=int(len(pn)), E2_nights=int(len(pn2)), trading_days=int(ND))
big = E2[E2.date.isin(pn2[pn2 > 5].index)]
if len(big):
    t_d = big.sort_values(['date', 'dret'], ascending=[True, False]).groupby('date').head(5); t_r = big.sort_values(['date', 'rvol'], ascending=[True, False]).groupby('date').head(5)
    row('V5-7g', LABEL, 'E2_top5_by_dayret_nights_gt5', t_d); row('V5-7g', LABEL, 'E2_top5_by_rvol_nights_gt5', t_r)
    cap['rank_diff_rvol_minus_dret_pct'] = float((t_r.exc.mean() - t_d.exc.mean()) * 100)
# pooled 2004-2026 context (never a pass criterion)
try:
    old = pd.read_csv(R + 'EXEC_v5_trades.csv.gz'); old['date'] = pd.to_datetime(old.date); old = old[old.date >= '2014-09-01']
    slim = ['date', 'q', 'isE1', 'isE2', 'valid', 'exc', 'exc_c1', 'exc_c2', 'net', 'exc10']
    pooled = pd.concat([EV[slim], old[slim]], ignore_index=True)
    row('V5-1', 'pooled 2004-2026 (context only)', 'primary_all_events', pooled[pooled.isE1.astype(bool) & pooled.valid.astype(bool)])
    row('V5-2', 'pooled 2004-2026 (context only)', 'primary_all_events', pooled[pooled.isE2.astype(bool) & pooled.valid.astype(bool)])
except Exception as ex: log('pooled context skipped:', ex)
log('cells done; rows', len(ROWS))

# ---------------------------------------------------------------- Holm m=5 (fixed before the run) across V5-1..V5-5 on the confirmatory window
M_HOLM = 5
fam = [OUT[c] for c in ('V5-1', 'V5-2', 'V5-3', 'V5-4', 'V5-5')]
ps = np.array([r['p_one_sided'] for r in fam]); order = np.argsort(ps); adj = np.empty(len(ps)); run_ = 0
for rank_, i in enumerate(order):
    run_ = max(run_, min(1.0, (M_HOLM - rank_) * ps[i])); adj[i] = run_
for r, a in zip(fam, adj): r['p_holm'] = float(a)
for c in SENS: SENS[c]['p_holm'] = np.nan

# ---------------------------------------------------------------- 5. registered gates, kill rules, decision tree (in code)
def g(cell, variant, window=LABEL): return next((x for x in ROWS if x['cell'] == cell and x['variant'] == variant and x['window'] == window), None)
def fnum(x): return None if x is None or (isinstance(x, float) and np.isnan(x)) else x
seen = pd.read_csv(R + 'EXEC_v5_cells.csv')
def seen_h1(cell):
    s = seen[(seen.cell == cell) & (seen.window.str.startswith('refinement')) & (seen.variant == 'H1_2014-09..2019-12')]
    return (float(s.ci_low.iloc[0]), float(s.mean_excess_net_pct.iloc[0]), int(s.n.iloc[0])) if len(s) else (np.nan, np.nan, 0)
def placebo_ok(cell):
    p = g(cell, 'placebo_C5toO6_vs_C1')
    if p is None or np.isnan(p['mean_excess_net_pct']): return False, p
    return bool(abs(p['mean_excess_net_pct']) < .20 and not (p['ci_low'] > 0 or p['ci_high'] < 0)), p
def liq(cell):
    t5, t1, t2, t3 = g(cell, 'tier_>=500M'), g(cell, 'liq_T1_least_liquid'), g(cell, 'liq_T2'), g(cell, 'liq_T3_most_liquid')
    mv = lambda t: t['mean_excess_net_pct'] if t is not None else np.nan
    tier500_pos = bool(mv(t5) > 0)
    confined = bool(mv(t1) > 0 and mv(t2) <= 0 and mv(t3) <= 0)
    return tier500_pos and not confined, dict(tier500=fnum(mv(t5)), T1=fnum(mv(t1)), T2=fnum(mv(t2)), T3=fnum(mv(t3)), tier500_pos=tier500_pos, confined_T1=confined)
m1r = g('M1', 'mirror_gapdown_overnight_long'); v1 = OUT['V5-1']; v2 = OUT['V5-2']
mirror_sig_pos_at_half = bool(m1r is not None and m1r['ci_low'] > 0 and m1r['mean_excess_net_pct'] >= .5 * v1['mean_excess_net_pct'])
mirror_ok = not mirror_sig_pos_at_half
GATES = {}
for r in fam:
    c = r['cell']; e2cell = c in ('V5-2', 'V5-4'); nmin = 600 if e2cell else 300; hc = .15 if e2cell else .25; gtd = c in ('V5-3', 'V5-4', 'V5-5')
    G = []
    G.append(('holm<0.05', bool(r['p_holm'] < .05)))
    G.append(('lowerCI>0_confirmatory', bool(r['ci_low'] > 0)))
    if not gtd:
        h1lo, h1m, h1n = seen_h1(c); G.append(('lowerCI>0_2014-09..2019-12_half', bool(h1lo > 0)))
        ha, hb = g(c, H1N), g(c, H2N)
        G.append(('both_conf_halves>=0', bool(ha is not None and hb is not None and ha['mean_excess_net_pct'] >= 0 and hb['mean_excess_net_pct'] >= 0)))
    G.append(('quarters_pos>=0.60', bool(r['quarters_positive_share'] >= .6)))
    G.append((f'n>={nmin}', bool(r['n'] >= nmin)))
    G.append((f'C1>={hc}&C1_lowerCI>0', bool(r.get('matched_c1_excess_pct', np.nan) >= hc and r.get('c1_ci_low', np.nan) > 0)))
    G.append(('C2>0', bool(r.get('matched_c2_excess_pct', np.nan) > 0)))
    G.append(('winsor&trim2>0', bool(r['winsor_mean'] > 0 and r['trim_top2_mean'] > 0)))
    pok1, _ = placebo_ok('V5-1'); pok2, _ = placebo_ok('V5-2')
    G.append(('placebo_|mean|<0.20&n.s.', bool(pok1 and (pok2 if e2cell else True))))
    G.append(('mirror_not_sig_pos_at_0.5x', bool(mirror_ok)))
    lo_own, _ = liq(c); lo_v1, _ = liq('V5-1')
    G.append(('tier500>0&not_confined_T1', bool(lo_own and (lo_v1 if gtd else True))))
    if gtd:
        thr = .3 if c == 'V5-4' else .5
        G.append((f'ON-OFF>={thr}&p<0.05', bool(r['on_minus_off_pct'] >= thr and r['on_minus_off_p'] < .05)))
        G.append(('OFF<=ON_H1', bool(r['on_ge_off_H1']))); G.append(('OFF<=ON_H2', bool(r['on_ge_off_H2'])))
        G.append(('ON_share_25-75%', bool(.25 <= r['on_share_days'] <= .75)))
        G.append(('OFF_not_sig_pos', bool(not (r['off_lower_ci'] > 0))))
        if c == 'V5-5': G.append(('spearman_rho>0&p<0.05', bool(r.get('spearman_rho_disp_vs_E2q', np.nan) > 0 and r.get('spearman_p_one_sided', 1.0) < .05)))
    failed = [k for k, v in G if not v]
    passed = len(failed) == 0
    amber = gtd and len(failed) > 0 and set(failed) <= {f'n>={nmin}', 'holm<0.05'} and (('holm<0.05' not in failed) or r['p_one_sided'] < .05)
    r['gates_met'] = ';'.join(f'{k}={int(v)}' for k, v in G); r['gates_failed'] = failed; r['passed'] = passed; r['amber_eligible'] = amber
    r['n_gates'] = len(G); r['n_gates_met'] = len(G) - len(failed)
    GATES[c] = dict(gates=G, failed=failed, passed=passed, amber=amber)
# kill rules
best = max(fam, key=lambda r: r['mean_excess_net_pct'] if not np.isnan(r['mean_excess_net_pct']) else -1e9)
def halves_of(r):
    c = r['cell']
    if c in ('V5-3', 'V5-4', 'V5-5'):
        gate = 'B1' if c in ('V5-3', 'V5-4') else 'B2'; return g(c, f'ON_{gate}_{H1N}'), g(c, f'ON_{gate}_{H2N}')
    return g(c, H1N), g(c, H2N)
bh1, bh2 = halves_of(best)
K1a = bool(v1.get('matched_c1_excess_pct', np.nan) <= 0 and v2.get('matched_c1_excess_pct', np.nan) <= 0)
K1b = bool(any(h is not None and h['mean_excess_net_pct'] < -.20 for h in (bh1, bh2)))
K2 = mirror_sig_pos_at_half
K3 = bool(any((g(c, 'placebo_C5toO6_vs_C1') or {}).get('ci_low', np.nan) > 0 for c in ('V5-1', 'V5-2')))
def k4_cell(c):
    r = OUT[c]
    if not (r['mean_excess_net_pct'] > 0): return False
    _, L = liq(c); t = lambda v: g(c, f'tier_{v}')
    mv = lambda x: x['mean_excess_net_pct'] if x is not None else np.nan
    only_small_tier = bool(mv(t('50-100M')) > 0 and not (mv(t('100-500M')) > 0) and not (mv(t('>=500M')) > 0))
    return bool((L['confined_T1'] or only_small_tier) and not L['tier500_pos'])
K4 = bool(any(k4_cell(c) for c in ('V5-1', 'V5-2')))
KILLS = dict(K1_regime_artefact=K1a or K1b, K1a_C1_le_0_both_base=K1a, K1b_best_cell_half_lt_minus020=K1b, K1_best_cell=best['cell'],
             K2_mirror=K2, K3_placebo=K3, K4_survivorship_signature=K4, K5_B1_sanity=False)
kills_fired = [k for k in ('K1_regime_artefact', 'K2_mirror', 'K3_placebo', 'K4_survivorship_signature') if KILLS[k]]
gated_pass = [c for c in ('V5-3', 'V5-4', 'V5-5') if GATES[c]['passed']]; gated_amber = [c for c in ('V5-3', 'V5-4', 'V5-5') if GATES[c]['amber']]
V58_PART_A_PASSED = False   # not run (5-min engine); GREEN also requires it
if kills_fired: decision = 'RED'; why = f'kill rule(s) fired on the confirmatory window: {kills_fired}'
elif gated_pass:
    decision = 'AMBER' if (PARTIAL or not V58_PART_A_PASSED) else 'GREEN'
    why = f'gated cell(s) {gated_pass} pass every gate incl. Holm; GREEN withheld because ' + ('the run is partial (<1,500 files) ' if PARTIAL else '') + ('and V5-8 Part A has not been run' if not V58_PART_A_PASSED else '')
elif gated_amber: decision = 'AMBER'; why = f'gated cell(s) {gated_amber} meet every gate except n and/or Holm (raw p<0.05) with C1 and mechanism gates met'
else: decision = 'RED'; why = 'no gated cell meets the registered gates (stricter reading: the RED clause "no gated cell meets the gates" takes precedence over the AMBER partial-run clause; the 1,225-name universe is effectively complete for 2004-2013, so the pre-declared completed-download re-run cannot change this)'
log('DECISION', decision, '|', why)

# ---------------------------------------------------------------- outputs
cells = pd.DataFrame(ROWS); cells.to_csv(R + f'{KEY}_cells.csv', index=False)
pcdf = pd.DataFrame(PC); pcdf.to_csv(R + f'{KEY}_percapital.csv', index=False)
daily.to_csv(R + f'{KEY}_daily_gates.csv')
tcols = [c for c in EV.columns if c not in ('di', 'ti')]
EV[tcols].to_csv(R + f'{KEY}_trades.csv.gz', index=False, compression='gzip')
def ytab(d, name):
    gg = d.groupby('year').exc.agg(['size', 'mean']); gg['mean'] = gg['mean'] * 100; gg.columns = [f'{name}_n', f'{name}_mean%']; return gg
Y = pd.concat([ytab(E1, 'E1'), ytab(E2, 'E2'), ytab(E1[E1.B1.astype(bool)], 'E1|B1on'), ytab(E2[E2.B1.astype(bool)], 'E2|B1on'), ytab(E1[E1.B2.astype(bool)], 'E1|B2on'),
               daily.groupby(daily.index.year)[['B1', 'B1v', 'B2']].mean().rename(columns={'B1': 'B1_on_share', 'B1v': 'B1v_on_share', 'B2': 'B2_on_share'}),
               pd.Series(names_by_year, name='names_with_data'), pd.Series(elig_by_year, name='eligible_mean')], axis=1)
Y.index.name = 'year'; Y.to_csv(R + f'{KEY}_yearly.csv')
yearly_str = Y.round(2).to_string()

def f2(x): return 'n/a' if x is None or (isinstance(x, float) and np.isnan(x)) else f'{x:+.2f}'
def f3(x): return 'n/a' if x is None or (isinstance(x, float) and np.isnan(x)) else f'{x:.3f}'
def ci(r): return f"[{f2(r['ci_low'])}, {f2(r['ci_high'])}] {r['wider_cluster'][:1]}"
subset = {'V5-1': 'E1 all', 'V5-2': 'E2 all', 'V5-3': 'E1, B1 ON', 'V5-4': 'E2, B1 ON', 'V5-5': 'E1, B2 ON'}
fam_lines = ['| Cell | Subset | n | days | mean | wider 95% CI | p (1-sided) | Holm (m=5) | quarters>0 (nq) | C1 excess [lo] | C2 excess (n) | winsor / trim-top-2% | median / win | PASS |', '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
for r in fam:
    fam_lines.append(f"| {r['cell']} | {subset[r['cell']]} | {r['n']} | {r['days']} | {f2(r['mean_excess_net_pct'])} | {ci(r)} | {f3(r['p_one_sided'])} | {f3(r['p_holm'])} | {f3(r['quarters_positive_share'])} ({r['n_quarters']}) | {f2(r.get('matched_c1_excess_pct'))} [{f2(r.get('c1_ci_low'))}] | {f2(r.get('matched_c2_excess_pct'))} ({r.get('c2_n')}) | {f2(r['winsor_mean'])} / {f2(r['trim_top2_mean'])} | {f2(r['median'])} / {f3(r['win_share'])} | {'PASS' if r['passed'] else ('AMBER-eligible' if r['amber_eligible'] else 'FAIL')} |")
gate_lines = []
for r in fam:
    gate_lines.append(f"\n**{r['cell']}** ({r['n_gates_met']}/{r['n_gates']} gates met; failed: {', '.join(r['gates_failed']) if r['gates_failed'] else 'none'})\n")
    gate_lines.append('| gate | met |'); gate_lines.append('|---|---|')
    for k, v in GATES[r['cell']]['gates']: gate_lines.append(f'| {k} | {"1" if v else "0"} |')
reg_lines = ['| Cell | gate | ON share days (full / defined from) | mean(ON) - mean(OFF) [CI], p | OFF cell n, mean [CI], q+ | ON>=OFF H1 (ON/OFF means, n) | ON>=OFF H2 (ON/OFF means, n) | other |', '|---|---|---|---|---|---|---|---|']
def regline(r, tag):
    oth = ''
    if 'spearman_rho_disp_vs_E2q' in r: oth = f"Spearman rho={f3(r['spearman_rho_disp_vs_E2q'])}, one-sided p={f3(r['spearman_p_one_sided'])}, nq={r.get('spearman_nq')}"
    return (f"| {r['cell']} | {tag} | {f3(r['on_share_days'])} / {f3(r.get('on_share_defined_days'))} from {r.get('gate_defined_from')} | {f2(r['on_minus_off_pct'])} [{f2(r['on_minus_off_ci'][0])}, {f2(r['on_minus_off_ci'][1])}], p={f3(r['on_minus_off_p'])} "
            f"| n={r['off_n']}, {f2(r['off_mean_pct'])} [{f2(r['off_lower_ci'])}, {f2(r['off_upper_ci'])}], q+={f3(r['off_qpos'])} | {r['on_ge_off_H1']} ({f2(r['on_mean_H1'])}/{f2(r['off_mean_H1'])}, {r['on_n_H1']}/{r['off_n_H1']}) "
            f"| {r['on_ge_off_H2']} ({f2(r['on_mean_H2'])}/{f2(r['off_mean_H2'])}, {r['on_n_H2']}/{r['off_n_H2']}) | {oth} |")
for c in ('V5-3', 'V5-4'): reg_lines.append(regline(OUT[c], 'B1 [T-64,T-2] (executor, primary)'))
reg_lines.append(regline(OUT['V5-5'], 'B2'))
sens_lines = ['| Cell | B1 window | n_ON | mean ON | wider CI | quarters>0 | ON share | ON-OFF, p | OFF mean [lo] |', '|---|---|---|---|---|---|---|---|---|']
for c in ('V5-3', 'V5-4'):
    for tag, r in (('[T-64,T-2] executor (primary)', OUT[c]), ('[T-63,T-2] verifier variant', SENS[c])):
        sens_lines.append(f"| {c} | {tag} | {r['n']} | {f2(r['mean_excess_net_pct'])} | {ci(r)} | {f3(r['quarters_positive_share'])} | {f3(r['on_share_days'])} | {f2(r['on_minus_off_pct'])}, p={f3(r['on_minus_off_p'])} | {f2(r['off_mean_pct'])} [{f2(r['off_lower_ci'])}] |")
b1_sens_str = f"B1 day-state agreement between [T-64,T-2] and [T-63,T-2]: {b1_agree:.4f}. " + ' || '.join(
    f"{c}: primary n={OUT[c]['n']} mean {f2(OUT[c]['mean_excess_net_pct'])} CI {ci(OUT[c])} q+={f3(OUT[c]['quarters_positive_share'])} ON-OFF {f2(OUT[c]['on_minus_off_pct'])} p={f3(OUT[c]['on_minus_off_p'])}; "
    f"variant n={SENS[c]['n']} mean {f2(SENS[c]['mean_excess_net_pct'])} CI {ci(SENS[c])} q+={f3(SENS[c]['quarters_positive_share'])} ON-OFF {f2(SENS[c]['on_minus_off_pct'])} p={f3(SENS[c]['on_minus_off_p'])}" for c in ('V5-3', 'V5-4'))
half_lines = ['| Cell | half | n | mean | wider CI | quarters>0 | C1 |', '|---|---|---|---|---|---|---|']
for c in ('V5-1', 'V5-2', 'V5-3', 'V5-4', 'V5-5'):
    for hn in (H1N, H2N):
        r = g(c, hn)
        if r: half_lines.append(f"| {c} | {hn} | {r['n']} | {f2(r['mean_excess_net_pct'])} | {ci(r)} | {f3(r['quarters_positive_share'])} | {f2(r.get('matched_c1_excess_pct'))} |")
pc_lines = ['| Cell | weights | total return | max DD | years>0 | trades | nights | yearly % | yearly excess % |', '|---|---|---|---|---|---|---|---|---|']
for p in PC: pc_lines.append(f"| {p['cell']} | {p['weights']} | {p['total_return_pct']:+.1f} | {p['max_drawdown_pct']:+.1f} | {p['years_positive_share']:.2f} | {p['n_trades']} | {p['nights']} | {p['yearly_pct']} | {p['yearly_excess_pct']} |")
percap_str = ' || '.join(f"{p['cell']} {p['weights']}: total {p['total_return_pct']:+.1f}%, MDD {p['max_drawdown_pct']:+.1f}%, years>0 {p['years_positive_share']:.2f}, trades {p['n_trades']}, yearly {p['yearly_pct']}" for p in PC)
mech_lines = []
for c in ('V5-1', 'V5-2'):
    p = g(c, 'placebo_C5toO6_vs_C1'); ps_ = g(c, 'placebo_C5toO6_vs_SPY'); ok, _ = placebo_ok(c)
    mech_lines.append(f"- {c} placebo Close_T+5 -> Open_T+6 vs C1: n={p['n']} {f2(p['mean_excess_net_pct'])} {ci(p)} -> gate {'met' if ok else 'NOT met'}; vs SPY {f2(ps_['mean_excess_net_pct'])} {ci(ps_)}")
mech_lines.append(f"- Mirror M1 (long overnight of gap<=-8%, vol>=3x, red close): n={m1r['n']} {f2(m1r['mean_excess_net_pct'])} {ci(m1r)} p={f3(m1r['p_one_sided'])}; 0.5 x V5-1 mean = {f2(.5 * v1['mean_excess_net_pct'])} -> {'K2 FIRES' if K2 else 'K2 not triggered'}")
for c in ('V5-1', 'V5-2', 'V5-3', 'V5-4', 'V5-5'):
    _, L = liq(c); mech_lines.append(f"- {c} tiers/terciles: >=500M {f2(L['tier500'])} (n={(g(c, 'tier_>=500M') or {}).get('n')}), 100-500M {f2((g(c, 'tier_100-500M') or {}).get('mean_excess_net_pct', np.nan))}, 50-100M {f2((g(c, 'tier_50-100M') or {}).get('mean_excess_net_pct', np.nan))}; terciles T1 {f2(L['T1'])} / T2 {f2(L['T2'])} / T3 {f2(L['T3'])}; confined to T1: {L['confined_T1']}")
r7a1, r7a2 = g('V5-7a', 'E1_extreme_ge25'), g('V5-7a', 'E1_8to25'); r7c1, r7c2 = g('V5-7c', 'E1_earnings'), g('V5-7c', 'E1_non_earnings_resolvable')
r6p, r6bp = g('V5-6', 'ladder_pooled_ungated'), g('V5-6', 'ladder_pooled_B1')
pooled1, pooled2 = g('V5-1', 'primary_all_events', 'pooled 2004-2026 (context only)'), g('V5-2', 'primary_all_events', 'pooled 2004-2026 (context only)')
cf1, cf2 = g('V5-1', 'cf_next_close_X2'), g('V5-2', 'cf_next_close_X2')
f10 = {c: g(c, 'flat10bp') for c in subset}; k5 = {c: g(c, 'K5_filled') for c in subset}

report = f"""# EXEC_v5_confirmatory -- the single pre-declared confirmatory run of preregistration_v5.json (run {RUN_TS})

**Window label: `{LABEL}`.** Registration SHA-256 `6222b79d...2b1e1` unchanged; no threshold or definition changed; engine functions copied verbatim from `EXEC_v5_engine.py`. This run was executed ONCE. The 2004-2013 window is now **SEEN** (things_tried.csv).

## 0. Decision

# **{decision}**

Rationale (computed in code from the registered decision tree): {why}.

- Gated cells passing every gate incl. Holm: {gated_pass or 'none'}. AMBER-eligible gated cells (all gates but n / Holm): {gated_amber or 'none'}.
- Kill rules on the confirmatory window: K1 regime artefact = {KILLS['K1_regime_artefact']} (C1<=0 for both base cells: {K1a}; best cell {best['cell']} half < -0.20%: {K1b}; best-cell halves {f2(bh1['mean_excess_net_pct'] if bh1 else np.nan)} / {f2(bh2['mean_excess_net_pct'] if bh2 else np.nan)}), K2 mirror = {K2}, K3 placebo = {K3}, K4 survivorship signature = {K4}, K5 (B1 sanity, pre-run) = passed.
- Partial-run clause: {len(fnames)} files < 1,500, so the run is labelled partial. Precedence used (stricter reading): RED (kill or no gated cell meets the gates) > AMBER > GREEN. GREEN additionally requires V5-8 Part A (not run).

## 1. Data and freeze

- Frozen file list: `EXEC_v5_files_2004_2013_frozen.txt`, {len(fnames)} files, sha256 `{frozen_sha}`; identical to the pre-check list: {same_as_precheck}.
- Universe = every symbol of the 2026 list that Yahoo served for 2004-2013, i.e. essentially all names that existed with >= $20M median dollar volume in early 2014 (names listed after 2014 cannot have this history). Prices are split/dividend-adjusted (yfinance): the $5 prev-close floor therefore acts on adjusted prices (see survivorship section).
- Panel: {ND} SPY trading days x {NS} names, {dates.min().date()}..{dates.max().date()}; SPY days absent from the panel: {n_spy_days_absent_from_panel}; panel days not in the SPY calendar: {n_panel_days_not_in_spy}. Names with data by year: {names_by_year}. Mean eligible names per day by year: {elig_by_year}.
- Effective event window starts {EV.date.min().date()} (the >= 250-row history filter clears ~2005-01; 2004 contributes no events, exactly as 2014-09..12 contributed none on the seen window). Events: E1 {int(EV.isE1.sum())}, E2 {int(EV.isE2.sum())}, M1 {int(EV.isM1.sum())}; valid (T+1 open exists) E1 {len(E1)}, E2 {len(E2)}.
- Controls: C1 coverage {c1_cov:.3f}, C2 coverage {c2_cov:.3f} (C2 = stricter intersection of +-1 pp and [0.75x,1.25x]; C2 for E1 n = {v1.get('c2_n')}).
- Earnings: resolvable share of E1 events {float(E1.earn_resolvable.mean()):.3f} (< 60% -> V5-7c NOT promoted, Holm m = 5 as logged before the run).
- Gates: B1 defined from {B1_first.date() if B1_first is not None else None} (63-day window used on {float((daily.B1_win == 63).mean()):.1%} of days, 126-day on {float((daily.B1_win == 126).mean()):.1%}, OFF-undefined on {float((daily.B1_win == 0).mean()):.1%}); B2 defined from {B2_first.date() if B2_first is not None else None} (undefined = OFF).

## 2. Trading-cell family on the confirmatory window (net excess over SPY, %, tier costs 5/10 bp per side)

{chr(10).join(fam_lines)}

Cost sensitivity flat 10 bp/side: {', '.join(f"{c} {f2(f10[c]['mean_excess_net_pct'])} {ci(f10[c])}" for c in subset)}.
K5-filled subsets: {', '.join(f"{c} n={k5[c]['n']} {f2(k5[c]['mean_excess_net_pct'])} {ci(k5[c])}" for c in subset)}.
Counterfactual next-close X2 (never traded): V5-1 {f2(cf1['mean_excess_net_pct'])} {ci(cf1)}, V5-2 {f2(cf2['mean_excess_net_pct'])} {ci(cf2)}.

### 2a. Confirmatory halves (2004-01..2008-12 / 2009-01..2013-12)

{chr(10).join(half_lines)}

### 2b. Regime-gate validation (the only out-of-sample test of B1/B2)

{chr(10).join(reg_lines)}

### 2c. B1 window sensitivity (verifier flag: [T-64,T-2] executor vs [T-63,T-2])

Day-state agreement {b1_agree:.4f}.

{chr(10).join(sens_lines)}

## 3. Gate-by-gate table per trading cell (1 = met)
{chr(10).join(gate_lines)}

Gate conventions (stricter readings): unconditional cells need lower CI > 0 in the confirmatory window AND in the 2014-09..2019-12 half (from `EXEC_v5_cells.csv`: V5-1 {f2(seen_h1('V5-1')[1])} [{f2(seen_h1('V5-1')[0])}, ...] n={seen_h1('V5-1')[2]}; V5-2 {f2(seen_h1('V5-2')[1])} [{f2(seen_h1('V5-2')[0])}, ...] n={seen_h1('V5-2')[2]}); gated cells replace that with the regime validation (ON-OFF threshold with one-sided day-cluster p, OFF <= ON in both confirmatory halves, ON share 25-75% of all confirmatory trading days with undefined gate days counted OFF, OFF cell not significantly positive; V5-5 also the quarterly Spearman). C2 > 0 and winsor/trim > 0 are applied to every trading cell (common criteria). Placebo/mirror gates are inherited from V5-1 (E2 cells additionally from V5-2). The liquidity gate is evaluated on the cell's own subset and, for gated cells, also on V5-1. "Confined to the least-liquid tercile" = T1 > 0 with T2 <= 0 and T3 <= 0. Missing values fail a gate.

## 4. Mechanism checks

{chr(10).join(mech_lines)}

## 5. Yearly table (mean net excess over SPY, %; B1/B2 = share of days ON; names/eligible counts)

```
{yearly_str}
```

## 6. Per-capital K = 5 (E1 first, then E2, by day return; net of tier costs; idle nights = 0)

{chr(10).join(pc_lines)}

## 7. Research and descriptive cells

- **V5-6 overnight ladder (E1):** k1 n={g('V5-6', 'ladder_k1_ungated')['n']} {f2(g('V5-6', 'ladder_k1_ungated')['mean_excess_net_pct'])}, k2 n={g('V5-6', 'ladder_k2_ungated')['n']} {f2(g('V5-6', 'ladder_k2_ungated')['mean_excess_net_pct'])}, k3 n={g('V5-6', 'ladder_k3_ungated')['n']} {f2(g('V5-6', 'ladder_k3_ungated')['mean_excess_net_pct'])}; pooled n={r6p['n']} {f2(r6p['mean_excess_net_pct'])} {ci(r6p)} q+={f3(r6p['quarters_positive_share'])} C1 {f2(r6p.get('matched_c1_excess_pct'))} [{f2(r6p.get('c1_ci_low'))}]; B1-gated pooled n={r6bp['n']} {f2(r6bp['mean_excess_net_pct'])} {ci(r6bp)}. Research pass requires pooled lower CI > 0 AND (2014-2019 half lower CI > 0 [known: -0.13 [-0.29,+0.05], fails] or a passing B1 gate), each k >= 0, n >= 300, q+ >= 0.60, C1 lower CI > 0 -> {'research PASS' if (r6p['ci_low'] > 0 and r6p['quarters_positive_share'] >= .6 and r6p['n'] >= 300 and r6p.get('c1_ci_low', np.nan) > 0 and all(g('V5-6', f'ladder_k{k}_ungated')['mean_excess_net_pct'] >= 0 for k in (1, 2, 3)) and len(gated_pass) > 0) else 'research FAIL; design stays one-night'}.
- **V5-7a extreme mover:** E1 >= 25% n={r7a1['n']} {f2(r7a1['mean_excess_net_pct'])} {ci(r7a1)} vs 8-25% n={r7a2['n']} {f2(r7a2['mean_excess_net_pct'])} {ci(r7a2)} (expectation ">= 25% carries the mean": {'met' if r7a1['mean_excess_net_pct'] > r7a2['mean_excess_net_pct'] else 'NOT met'}).
- **V5-7b deal-pin:** E1 flagged n={g('V5-7b', 'E1_deal_pin_only')['n']}, E2 flagged n={g('V5-7b', 'E2_deal_pin_only')['n']}.
- **V5-7c earnings (descriptive only, coverage {float(E1.earn_resolvable.mean()):.1%}):** E1 earnings n={r7c1['n']} {f2(r7c1['mean_excess_net_pct'])} vs non-earnings n={r7c2['n']} {f2(r7c2['mean_excess_net_pct'])}.
- **V5-7e risk rules:** SPY <= -3% nights E1 n={g('V5-7e', 'E1_spy_le_-3pct_nights')['n']} {f2(g('V5-7e', 'E1_spy_le_-3pct_nights')['mean_excess_net_pct'])} (ex: {f2(g('V5-7e', 'E1_ex_spy_le_-3pct')['mean_excess_net_pct'])}); E2 n={g('V5-7e', 'E2_spy_le_-3pct_nights')['n']} {f2(g('V5-7e', 'E2_spy_le_-3pct_nights')['mean_excess_net_pct'])} (ex: {f2(g('V5-7e', 'E2_ex_spy_le_-3pct')['mean_excess_net_pct'])}); NFP-approx macro eve E1 n={g('V5-7e', 'E1_macro_eve_nfp_approx')['n']} {f2(g('V5-7e', 'E1_macro_eve_nfp_approx')['mean_excess_net_pct'])} (ex {f2(g('V5-7e', 'E1_ex_macro_eve_nfp_approx')['mean_excess_net_pct'])}).
- **V5-7f capacity:** E1 nights {cap['E1_nights']} of {ND} days, > 5 on {cap['E1_share_nights_gt5']:.1%}, unfilled {cap['E1_share_events_unfilled']:.1%}; E2 nights {cap['E2_nights']}, mean {cap['E2_events_per_night']['mean']:.2f} / max {cap['E2_events_per_night']['max']:.0f} per night, > 5 on {cap['E2_share_nights_gt5']:.1%}, unfilled {cap['E2_share_events_unfilled']:.1%}.
- **V5-7g rank rule:** RVOL top-5 minus day-return top-5 on nights > 5 E2 qualifiers: {f2(cap.get('rank_diff_rvol_minus_dret_pct'))} pp.
- **Pooled 2004-2026 (context only, never a pass criterion):** V5-1 n={pooled1['n'] if pooled1 else 'n/a'} {f2(pooled1['mean_excess_net_pct']) if pooled1 else ''} {ci(pooled1) if pooled1 else ''}; V5-2 n={pooled2['n'] if pooled2 else 'n/a'} {f2(pooled2['mean_excess_net_pct']) if pooled2 else ''} {ci(pooled2) if pooled2 else ''}.

## 8. Survivorship discussion

The universe is the 2026 symbol list restricted to names Yahoo could serve for 2004-2013: {NS} names, i.e. survivors that (a) still trade in 2026 and (b) already existed and were liquid in 2014. Every 2004-2013 delisting, bankruptcy, and acquisition target is absent, and names that were small then and large later are over-represented. For a long-only continuation leg this bias is IN the rule's favour (the registration: "the confirmatory 2004-2013 window is the MOST biased in the rule's favour; a pass there is necessary, not sufficient; a fail is decisive"). C1 (five same-day, same-ADV-decile non-event survivors) cancels the universe-level component, which is why the C1 haircut (+0.25% E1 / +0.15% E2) is a gate. Two second-order effects of the adjusted prices: the $5 floor on split-adjusted prices excludes names with large later splits from the early years (works against the rule: big future winners drop out), and the ADV$ filter uses adjusted close x split-adjusted volume (approximately split-invariant, slightly deflated by dividend adjustment). The 1,225-name panel is effectively the complete 2004-2013 universe reachable from the 2026 list; the registration's completed-download re-run would add nothing material, so the partial label is formal.

## 9. Ambiguities resolved (stricter reading, documented)
- B1 primary window = executor's [T-64, T-2] (63 event-days whose T+1 open <= T-1); verifier variant [T-63, T-2] reported as sensitivity only; pass decisions use the primary.
- Gate ON share denominator = all confirmatory trading days (undefined gate days = OFF); the share over defined days is co-reported.
- "OFF <= ON in both confirmatory halves" applied to V5-5 as well as V5-3/V5-4 (task brief lists it for the gated route generally).
- C2 > 0 and winsor/trim > 0 required for every trading cell; placebo gate requires |mean| < 0.20 and a CI that includes 0; mirror gate fails if M1 lower CI > 0 and M1 mean >= 0.5 x V5-1 mean.
- Decision precedence RED > AMBER > GREEN when clauses overlap; GREEN withheld while V5-8 Part A is unrun or the run is partial.
- K1 "best cell" = family cell with the highest confirmatory mean (ON subset for gated cells); its halves are the ON-subset halves.
- K4 fires if a positive base cell's effect is confined to T1 or to the $50-100M tier while the >= $500M tier is <= 0.
- Panel reindexed to the SPY calendar so that T+1 is always the next SPY trading day (a missing stock bar = NaN, not a skipped day).

## 10. Files
`{KEY}.py`, `{KEY}_cells.csv` ({len(cells)} rows), `{KEY}_trades.csv.gz` ({len(EV)} event rows), `{KEY}_yearly.csv`, `{KEY}_percapital.csv`, `{KEY}_daily_gates.csv`, `{KEY}_summary.json`, `EXEC_v5_files_2004_2013_frozen.txt`; outcome rows appended to `things_tried.csv` and 2004-2013 marked SEEN.
"""
with open(R + f'{KEY}_report.md', 'w', encoding='utf-8') as fh: fh.write(report)

def cellobj(r):
    return dict(cell=r['cell'], window=LABEL, n=int(r['n']), mean_excess_net_pct=fnum(r['mean_excess_net_pct']), ci_low=fnum(r['ci_low']), ci_high=fnum(r['ci_high']), wider=r['wider_cluster'],
                p_one_sided=fnum(r['p_one_sided']), p_holm=fnum(r['p_holm']), quarters_positive_share=fnum(r['quarters_positive_share']), n_quarters=r['n_quarters'],
                matched_c1_excess_pct=fnum(r.get('matched_c1_excess_pct')), c1_ci_low=fnum(r.get('c1_ci_low')), matched_c2_excess_pct=fnum(r.get('matched_c2_excess_pct')), c2_n=r.get('c2_n'),
                winsor=fnum(r['winsor_mean']), trim2=fnum(r['trim_top2_mean']), median=fnum(r['median']), win_share=fnum(r['win_share']),
                on_share=fnum(r.get('on_share_days')), on_share_defined=fnum(r.get('on_share_defined_days')), on_off_diff_pct=fnum(r.get('on_minus_off_pct')), on_off_p=fnum(r.get('on_minus_off_p')),
                off_mean_pct=fnum(r.get('off_mean_pct')), off_lower_ci=fnum(r.get('off_lower_ci')), off_n=r.get('off_n'), on_ge_off_H1=r.get('on_ge_off_H1'), on_ge_off_H2=r.get('on_ge_off_H2'),
                spearman_rho=fnum(r.get('spearman_rho_disp_vs_E2q')), spearman_p=fnum(r.get('spearman_p_one_sided')),
                gates_met=r.get('gates_met'), gates_failed=r.get('gates_failed'), passed=bool(r.get('passed')), amber_eligible=bool(r.get('amber_eligible')))
summary = dict(run_ts=RUN_TS, label=LABEL, decision=decision, decision_rationale=why, frozen_files=len(fnames), frozen_sha256=frozen_sha, same_as_precheck=same_as_precheck, partial=PARTIAL,
               panel=dict(days=ND, names=NS, first_event=str(EV.date.min().date()), names_by_year=names_by_year, elig_by_year=elig_by_year, spy_days_absent=n_spy_days_absent_from_panel),
               events=dict(E1=int(EV.isE1.sum()), E2=int(EV.isE2.sum()), M1=int(EV.isM1.sum()), E1_valid=len(E1), E2_valid=len(E2)), c1_cov=c1_cov, c2_cov=c2_cov,
               earn_resolvable_E1=float(E1.earn_resolvable.mean()), holm_m=M_HOLM, cells={r['cell']: cellobj(r) for r in fam}, b1_sensitivity={c: cellobj(SENS[c]) for c in SENS}, b1_day_agreement=b1_agree,
               b1_defined_from=str(B1_first.date()) if B1_first is not None else None, b2_defined_from=str(B2_first.date()) if B2_first is not None else None,
               kills=KILLS, kills_fired=kills_fired, gated_pass=gated_pass, gated_amber=gated_amber, v58_part_a_passed=V58_PART_A_PASSED, capacity=cap, percapital=PC,
               halves={c: {hn: dict(n=g(c, hn)['n'], mean=fnum(g(c, hn)['mean_excess_net_pct']), lo=fnum(g(c, hn)['ci_low']), hi=fnum(g(c, hn)['ci_high'])) for hn in (H1N, H2N) if g(c, hn)} for c in subset},
               mirror=cellobj(m1r) if m1r else None, placebo={c: cellobj(g(c, 'placebo_C5toO6_vs_C1')) for c in ('V5-1', 'V5-2')}, ladder_pooled=cellobj(r6p) if r6p else None)
json.dump(summary, open(R + f'{KEY}_summary.json', 'w'), indent=1, default=str)

# ---------------------------------------------------------------- things_tried: outcome rows + SEEN marker
tt = R + 'things_tried.csv'
with open(tt, 'rb') as fh: fh.seek(-1, 2); last = fh.read(1)
rows_tt = []
for r in fam:
    rows_tt.append(['EXEC_v5_confirmatory', f"{r['cell']} {subset[r['cell']]}", LABEL, r['n'], f"{r['mean_excess_net_pct']:+.3f}% excess over SPY",
                    f"wider CI [{f2(r['ci_low'])},{f2(r['ci_high'])}] {r['wider_cluster']}; p={f3(r['p_one_sided'])}; Holm(m=5)={f3(r['p_holm'])}; q+={f3(r['quarters_positive_share'])}; C1 {f2(r.get('matched_c1_excess_pct'))} [{f2(r.get('c1_ci_low'))}]; "
                    + (f"ON-OFF {f2(r['on_minus_off_pct'])} p={f3(r['on_minus_off_p'])}; ON share {f3(r['on_share_days'])}; OFF {f2(r['off_mean_pct'])} [{f2(r['off_lower_ci'])}]; " if 'on_minus_off_pct' in r else '')
                    + f"verdict={'PASS' if r['passed'] else ('AMBER-eligible' if r['amber_eligible'] else 'FAIL')}; failed gates: {', '.join(r['gates_failed']) or 'none'}"])
for c in SENS:
    r = SENS[c]; rows_tt.append(['EXEC_v5_confirmatory', f"{c} B1 window sensitivity [T-63,T-2]", LABEL, r['n'], f"{r['mean_excess_net_pct']:+.3f}%", f"CI [{f2(r['ci_low'])},{f2(r['ci_high'])}]; q+={f3(r['quarters_positive_share'])}; ON-OFF {f2(r['on_minus_off_pct'])} p={f3(r['on_minus_off_p'])}; sensitivity only, not a pass test"])
rows_tt.append(['EXEC_v5_confirmatory', 'V5-6 ladder pooled ungated (research)', LABEL, r6p['n'], f"{r6p['mean_excess_net_pct']:+.3f}%", f"CI [{f2(r6p['ci_low'])},{f2(r6p['ci_high'])}]; q+={f3(r6p['quarters_positive_share'])}; research cell, outside Holm family"])
rows_tt.append(['EXEC_v5_confirmatory', 'M1 mirror gap-down overnight long (falsifier)', LABEL, m1r['n'], f"{m1r['mean_excess_net_pct']:+.3f}%", f"CI [{f2(m1r['ci_low'])},{f2(m1r['ci_high'])}]; K2={'FIRED' if K2 else 'not triggered'}"])
rows_tt.append(['EXEC_v5_confirmatory', 'DECISION (decision_tree_computed_in_code)', LABEL, 'NA', decision, why[:400] + f"; kills fired: {kills_fired}; frozen sha256={frozen_sha[:16]}"])
rows_tt.append(['EXEC_v5_confirmatory', '2004-2013 window status', '2004-01-01..2013-12-31', 'NA', 'NA', f"SEEN as of {RUN_TS} (single pre-declared partial confirmatory run executed on the frozen 1,225-file list); any further rule test on this window joins the Holm family retroactively (m recomputed) or voids every v5 pass verdict"])
with open(tt, 'a', newline='', encoding='utf-8') as fh:
    if last != b'\n': fh.write('\n')
    w = csv.writer(fh)
    for rr in rows_tt: w.writerow(rr)
log('things_tried appended', len(rows_tt), 'rows; 2004-2013 marked SEEN')

# ---------------------------------------------------------------- stdout digest
print('\n=== DECISION', decision, '|', why)
print('KILLS', json.dumps(KILLS, default=str))
print('\n'.join(fam_lines)); print(); print('\n'.join(reg_lines)); print(); print('\n'.join(sens_lines)); print(); print('\n'.join(half_lines)); print()
for r in fam: print(r['cell'], 'gates:', r['gates_met'])
print(); print('\n'.join(mech_lines)); print(); print(yearly_str); print(); print('\n'.join(pc_lines))
print('\nB1SENS:', b1_sens_str)
print('\nLADDER pooled', r6p['n'], f2(r6p['mean_excess_net_pct']), ci(r6p), '| 7a >=25%', r7a1['n'], f2(r7a1['mean_excess_net_pct']), 'vs 8-25%', r7a2['n'], f2(r7a2['mean_excess_net_pct']), '| pooled04-26 V5-1', pooled1 and (pooled1['n'], f2(pooled1['mean_excess_net_pct']), ci(pooled1)), 'V5-2', pooled2 and (pooled2['n'], f2(pooled2['mean_excess_net_pct']), ci(pooled2)))
print('CAP', json.dumps(cap, default=str)[:600])
log('DONE')
