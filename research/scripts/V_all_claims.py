"""Adversarial verifier: independent recompute of every headline claim from raw files.
No analyst scripts imported. Output -> V_all_claims_out.txt (redirect)."""
import pandas as pd, numpy as np, os, time, warnings, traceback
warnings.filterwarnings('ignore')
from scipy.stats import spearmanr
t0 = time.time()
SP = 'C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad'
R = SP + '/research'
BIG = 10**6
ETFS = set('SPY QQQ IWM DIA XLK XLF XLE XLV XLY XLP XLI XLB XLU XLC XLRE SMH ARKK TLT UUP GLD USO BITO'.split())

def P(*a): print(*a, flush=True)
def tb(s): return s.astype(str).str.strip().str.lower().isin(['true', '1', '1.0', 'yes'])
def hm_of(m): return f'{m//60:02d}:{m%60:02d}'
def section(name): P('=' * 78); P(name)

def cb(d, col='R', reps=1000, seed=0):
    g = d.groupby('date')[col].agg(['sum', 'count']); n = len(g)
    if n == 0: return (np.nan, np.nan)
    rng = np.random.default_rng(seed); idx = rng.integers(0, n, (reps, n))
    m = g['sum'].values[idx].sum(1) / g['count'].values[idx].sum(1)
    return tuple(np.round(np.percentile(m, [2.5, 97.5]), 3))

def cdiff(a, b, col='R', reps=1000, seed=0):
    dates = sorted(set(a.date) | set(b.date)); n = len(dates)
    if n == 0: return (np.nan, np.nan)
    ga = a.groupby('date')[col].agg(['sum', 'count']).reindex(dates).fillna(0)
    gb = b.groupby('date')[col].agg(['sum', 'count']).reindex(dates).fillna(0)
    rng = np.random.default_rng(seed); idx = rng.integers(0, n, (reps, n))
    ma = ga['sum'].values[idx].sum(1) / np.maximum(ga['count'].values[idx].sum(1), 1)
    mb = gb['sum'].values[idx].sum(1) / np.maximum(gb['count'].values[idx].sum(1), 1)
    return tuple(np.round(np.percentile(ma - mb, [2.5, 97.5]), 3))

def S(name, d, col='R'):
    if len(d) == 0: P(f'  {name}: n=0'); return
    hit = (d.result == 'WIN').mean() if 'result' in d else np.nan
    stp = (d.result == 'LOSS').mean() if 'result' in d else np.nan
    P(f'  {name}: n={len(d)} days={d.date.nunique()} avg={d[col].mean():+.3f} CI{cb(d, col)} hit={hit:.3f} stop={stp:.3f}')

# ---------------------------------------------------------------- load picks
df = pd.read_csv(R + '/picks_master.csv', dtype={'date': str, 'batch_time': str, 'ticker': str, 'category': str})
df['date'] = df.date.str[:10]; df['bt'] = df.batch_time.str[:5].str.zfill(5)
df['mins'] = df.bt.str[:2].astype(int) * 60 + df.bt.str[3:5].astype(int)
df['f0'] = (df.mins // 5 * 5).map(hm_of); df['f1'] = (df.mins // 5 * 5 - 5).map(hm_of)
df['R'] = df.r_realized.astype(float)
df['risk'] = (df.entry - df.stop) / df.entry
for c in ['strong_signal', 'elite', 'tradeable', 'anti_ext', 'sc_bar_green', 'sc_above_vwap', 'sc_new_hod', 'sc_pm_high_hold']:
    df[c + '_b'] = tb(df[c]); df[c + '_na'] = df[c].isna()
DISC = df.date <= '2026-06-30'; TEST = df.date >= '2026-07-01'
df['hb'] = pd.cut(df.mins, [0, 600, 660, 720, 780, 840, 900, 1000], labels=['09:30-10', '10-11', '11-12', '12-13', '13-14', '14-15', '15+'])
P(f'picks n={len(df)} dates={df.date.nunique()} {df.date.min()}..{df.date.max()} avgR={df.R.mean():+.4f} results={df.result.value_counts().to_dict()}')
P('  strong raw:', df.strong_signal.astype(str).value_counts().head(3).to_dict(), '| elite raw:', df.elite.astype(str).value_counts().head(3).to_dict(), '| bt sample', df.bt.value_counts().head(3).to_dict())
P('  stop_pct median', df.stop_pct.median(), '| risk median', round(df.risk.median(), 5), '| pnl_pct mean', round(df.pnl_pct.mean(), 4), '| category', df.category.value_counts().to_dict())

# ---------------------------------------------------------------- F1-1
try:
    section('CLAIM F1-1 barrier identity / driftless / EOD bucket')
    hit = (df.result == 'WIN').mean(); stp = (df.result == 'LOSS').mean(); eod = (df.result == 'EOD').mean()
    eR = df.loc[df.result == 'EOD', 'R'].mean()
    P(f'  n={len(df)} hit={hit:.4f} stop={stp:.4f} eod={eod:.4f} E[R|EOD]={eR:.3f} CI{cb(df[df.result=="EOD"])} identity={2.5*hit-stp+eod*eR:+.4f} avgR={df.R.mean():+.4f} zero-drift-required E[R|EOD]={(stp-2.5*hit)/eod:.3f}')
    P(f'  avgR all CI {cb(df)} ; median R {df.R.median():+.2f}; mean pnl_pct={df.pnl_pct.mean():+.4f} CI{cb(df, "pnl_pct")} ; WIN/LOSS part={2.5*hit-stp:+.3f}R EOD part={eod*eR:+.3f}R')
    # EOD-bucket by half
    for lab, m in (('DISC', DISC), ('TEST', TEST)):
        g = df[m]; h = (g.result == 'WIN').mean(); s_ = (g.result == 'LOSS').mean(); e = (g.result == 'EOD').mean(); er = g.loc[g.result == 'EOD', 'R'].mean()
        P(f'  {lab}: n={len(g)} avgR={g.R.mean():+.3f} CI{cb(g)} hit={h:.3f} stop={s_:.3f} eod={e:.3f} E[R|EOD]={er:+.3f} required={(s_-2.5*h)/e:.3f}')
    for lab, g in df.groupby('hb', observed=True):
        gd = g[g.date <= '2026-06-30']; gt = g[g.date >= '2026-07-01']
        P(f'  bucket {lab}: n={len(g)} avgR={g.R.mean():+.3f} CI{cb(g)} hit={(g.result=="WIN").mean():.3f} stop={(g.result=="LOSS").mean():.3f} | DISC {gd.R.mean():+.3f} (n={len(gd)}) TEST {gt.R.mean():+.3f} (n={len(gt)}) | pnl_pct {g.pnl_pct.mean():+.3f}')
    mid = df[df.hb == '12-13']; oth = df[df.hb != '12-13']
    P('  12-13 minus others diff CI', cdiff(mid, oth), '| by month 12-13:', mid.groupby('month').R.mean().round(3).to_dict() if 'month' in df else '')
except Exception: P(traceback.format_exc()[-1500:])

# ---------------------------------------------------------------- F1-2
try:
    section('CLAIM F1-2 shipped tiers disc vs test')
    live = df[df.date >= '2026-05-12']; sd = live[live.date <= '2026-06-30']; st = df[TEST]
    S('STRONG disc(05-12..06-30)', sd[sd.strong_signal_b]); S('nonSTRONG disc', sd[~sd.strong_signal_b])
    S('STRONG test(>=07-01)', st[st.strong_signal_b]); S('nonSTRONG test', st[~st.strong_signal_b])
    P('  diff STRONG-nonSTRONG: disc CI', cdiff(sd[sd.strong_signal_b], sd[~sd.strong_signal_b]), '| test CI', cdiff(st[st.strong_signal_b], st[~st.strong_signal_b]))
    for lab, g in df[df.date >= '2026-05-12'].groupby('hb', observed=True):
        a = g[g.strong_signal_b]; b = g[~g.strong_signal_b]
        ad = a[a.date <= '2026-06-30']; bd = b[b.date <= '2026-06-30']; at = a[a.date >= '2026-07-01']; bt_ = b[b.date >= '2026-07-01']
        if len(a) >= 50: P(f'   within-bucket {lab}: DISC strong n={len(ad)} {ad.R.mean():+.3f} vs non {bd.R.mean():+.3f} | TEST strong n={len(at)} {at.R.mean():+.3f} vs non {bt_.R.mean():+.3f}')
    S('TRADEABLE-recon(STRONG&bt<=10:00) disc', sd[sd.strong_signal_b & (sd.bt <= '10:00')]); S('TRADEABLE-recon test', st[st.strong_signal_b & (st.bt <= '10:00')])
    S('nonTRADEABLE early (bt<=10:00,!STRONG) test', st[~st.strong_signal_b & (st.bt <= '10:00')])
    l78 = df[df.date >= '2026-07-08']
    S('tradeable col (>=07-08)', l78[l78.tradeable_b]); S('elite col (>=07-08)', l78[l78.elite_b]); S('non-elite (>=07-08)', l78[~l78.elite_b])
    er = df.strong_signal_b & (df.category == 'D') & (df.rvol >= 2) & (df.rvol < 5) & (df.rsi >= 68) & (df.bt <= '10:00') & (df.risk >= 0.009)
    S('ELITE-recon disc(05-12..06-30)', df[er & (df.date >= '2026-05-12') & (df.date <= '2026-06-30')]); S('ELITE-recon test', df[er & TEST])
    m78 = df.date >= '2026-07-08'
    P('  ELITE-recon vs elite col agreement (>=07-08):', round((er[m78] == df.elite_b[m78]).mean(), 4), '| recon n', int(er[m78].sum()), 'col n', int(df.elite_b[m78].sum()))
    S('anti_ext True (>=07-08)', l78[l78.anti_ext_b]); S('anti_ext False (>=07-08)', l78[~l78.anti_ext_b & ~l78.anti_ext_na])
    P('  anti_ext diff CI', cdiff(l78[l78.anti_ext_b], l78[~l78.anti_ext_b & ~l78.anti_ext_na]))
    rv = (df.rvol >= 3) & (df.rvol < 5)
    S('TRADEABLE-recon & rvol3-5 disc', sd[sd.strong_signal_b & (sd.bt <= '10:00') & rv.loc[sd.index]]); S('TRADEABLE-recon & rvol3-5 test', st[st.strong_signal_b & (st.bt <= '10:00') & rv.loc[st.index]])
    # month control for STRONG test
    P('  STRONG test by month:', st[st.strong_signal_b].groupby(st.date.str[5:7]).R.mean().round(3).to_dict(), '| nonSTRONG:', st[~st.strong_signal_b].groupby(st.date.str[5:7]).R.mean().round(3).to_dict())
except Exception: P(traceback.format_exc()[-1500:])

# ---------------------------------------------------------------- bars5m
def load5(t):
    p = f'{SP}/bars5m/{t}.csv'
    if not os.path.exists(p): return None
    b = pd.read_csv(p, index_col=0)
    if len(b) == 0: return None
    b.index = pd.to_datetime(b.index, utc=True).tz_convert('America/New_York')
    b.columns = [c.lower() for c in b.columns]
    b = b.sort_index(); b['d'] = b.index.strftime('%Y-%m-%d'); b['hm'] = b.index.strftime('%H:%M')
    pc = b.close.shift(1)
    tr = np.maximum(b.high - b.low, np.maximum((b.high - pc).abs(), (b.low - pc).abs()))
    b['atrp'] = tr.rolling(14).mean().shift(1)
    return b

DAY = {}
try:
    section('LOADING bars5m')
    tick = sorted(set(df.ticker.unique()))
    miss = []
    for t in tick:
        b = load5(t)
        if b is None: miss.append(t); continue
        for d, g in b.groupby('d'):
            DAY[(t, d)] = {k: g[k].values for k in ['hm', 'open', 'high', 'low', 'close', 'volume', 'atrp']}
    P(f'  tickers in picks {len(tick)}, missing/empty {len(miss)} {miss[:8]}, ticker-days {len(DAY)}, {time.time()-t0:.0f}s')
except Exception: P(traceback.format_exc()[-1500:])

def resolve(h, l, c, j0, stop, target):
    hs = l[j0:] <= stop; ht = h[j0:] >= target
    js = int(np.argmax(hs)) if hs.any() else BIG; jt = int(np.argmax(ht)) if ht.any() else BIG
    if js < BIG and js <= jt: return ('LOSS', stop, j0 + js, js == jt)
    if jt < BIG: return ('WIN', target, j0 + jt, False)
    return ('EOD', c[-1], len(c) - 1, False)

# ---------------------------------------------------------------- per-pick replica
pk = df[df.date >= '2026-06-17'].copy()
try:
    section('CLAIMS F2-1 / F2-2 / F3-1 / F3-2  per-pick replica from bars5m (n picks >=06-17 = %d)' % len(pk))
    recs = []
    for r in pk.itertuples(index=True):
        rec = {'idx': r.Index}
        day = DAY.get((r.ticker, r.date))
        if day is None: recs.append(rec); continue
        hm = day['hm']; m = (hm >= '09:30') & (hm < '16:00')
        o, h, l, c, v = (day[k][m] for k in ['open', 'high', 'low', 'close', 'volume']); hmr = hm[m]
        w = np.where(hmr == r.f0)[0]
        if len(w) == 0 or len(o) < 10: recs.append(rec); continue
        i0 = int(w[0]); i1 = i0 - 1
        E, Sg, T = float(r.entry), float(r.stop), float(r.target); risk = E - Sg
        if not risk > 0: recs.append(rec); continue
        rec['found'] = True
        rec['d_f0open'] = abs(E / o[i0] - 1) * 1e4; rec['in_f0'] = bool(l[i0] - 1e-9 <= E <= h[i0] + 1e-9)
        rec['sgn_f0'] = (E / o[i0] - 1) * 1e4
        if i1 >= 0: rec['d_f1close'] = abs(E / c[i1] - 1) * 1e4
        if i0 + 1 < len(o): rec['d_nextopen'] = abs(E / o[i0 + 1] - 1) * 1e4
        for tag, j0 in (('A', i0), ('B', i0 + 1)):
            if j0 >= len(o): res, px, both = 'EOD', c[-1], False
            else: res, px, _, both = resolve(h, l, c, j0, Sg, T)
            rec['res' + tag] = res; rec['R' + tag] = (px - E) / risk; rec['both' + tag] = both; rec['px' + tag] = px
        rec['close_last'] = c[-1]
        if i0 + 1 < len(o):
            En = o[i0 + 1]
            if En >= T or En <= Sg: rec['s4'] = 'unenterable'
            else:
                res, px, _, _ = resolve(h, l, c, i0 + 1, Sg, T); rec['s4'] = res; rec['R4'] = (px - En) / risk; rec['cost'] = 2 * 0.0005 * En / risk
        if i1 >= 0:
            rec['g'] = bool(c[i1] > o[i1])
            rec['hod_r'] = bool(h[i1] >= np.nanmax(h[:i1 + 1]))
            pm = hm < '09:30'
            pmh = float(np.nanmax(day['high'][pm])) if pm.any() else np.nan
            rec['hod_a'] = bool(h[i1] >= max(np.nanmax(h[:i1 + 1]), pmh if np.isfinite(pmh) else -np.inf))
            tp = (h + l + c) / 3; cv = np.nansum(v[:i1 + 1])
            vw = np.nansum(tp[:i1 + 1] * v[:i1 + 1]) / cv if cv > 0 else np.nan
            rec['vw_r'] = bool(c[i1] > vw) if np.isfinite(vw) else np.nan
            allm = hm < '16:00'; k = int(np.where(hm[allm] == hmr[i1])[0][0])
            hh, hl, hc, hv = (day[x][allm] for x in ['high', 'low', 'close', 'volume'])
            tpa = (hh + hl + hc) / 3; cva = np.nansum(hv[:k + 1]); vwa = np.nansum(tpa[:k + 1] * hv[:k + 1]) / cva if cva > 0 else np.nan
            rec['vw_a'] = bool(c[i1] > vwa) if np.isfinite(vwa) else np.nan
            rec['pmh_c'] = bool(c[i1] > pmh) if np.isfinite(pmh) else np.nan
            rec['pmh_l'] = bool(l[i1] > pmh) if np.isfinite(pmh) else np.nan
            rec['pmh_h'] = bool(h[i1] > pmh) if np.isfinite(pmh) else np.nan
        recs.append(rec)
    rep = pd.DataFrame(recs).set_index('idx'); pk = pk.join(rep)
    ok = pk[pk.found == True]
    P(f'  matched {len(ok)}/{len(pk)} picks to an F0 bar; {time.time()-t0:.0f}s')
    # F2-1
    P('-- F2-1 outcome faithfulness')
    for tag in 'AB':
        agree = (ok['res' + tag] == ok.result).mean(); ct = pd.crosstab(ok.result, ok['res' + tag])
        P(f'  replica {tag} (start {"F0" if tag=="A" else "F0+1"}): result agreement={agree:.4f} avgR log={ok.R.mean():+.4f} replica={ok["R"+tag].mean():+.4f} mean|dR|={(ok["R"+tag]-ok.R).abs().mean():.4f} same-bar-both-at-first-touch={int(ok["both"+tag].sum())}')
        P('    crosstab rows=log cols=replica:', {r_: ct.loc[r_].to_dict() for r_ in ct.index})
    fchk = ((ok.resolve_price - ok.entry) / (ok.entry - ok.stop) - ok.R).abs()
    e_ = ok[ok.result == 'EOD']
    P(f'  r_realized formula |err| mean={fchk.mean():.2e} max={fchk.max():.3f}; EOD log exit within 1c of 15:55 close: {((e_.resolve_price-e_.close_last).abs()<=0.01).mean():.3f}; mean EOD R log {e_.R.mean():+.3f} vs replicaA {e_.RA.mean():+.3f}')
    # F2-2 / F3-1 timing
    P('-- F2-2 / F3-1 entry timing')
    P(f'  |entry-F0.open|<5bp: {(ok.d_f0open<5).mean():.3f} | <0.5bp(==): {(ok.d_f0open<0.5).mean():.3f} | |entry-F1.close|<5bp: {(ok.d_f1close<5).mean():.3f} (==:{(ok.d_f1close<0.5).mean():.3f}) | |entry-next open|<5bp: {(ok.d_nextopen<5).mean():.3f} | inside F0 range: {ok.in_f0.mean():.3f}')
    P(f'  median |entry-F0.open| bp={ok.d_f0open.median():.1f} (09:35 only {ok[ok.bt=="09:35"].d_f0open.median():.1f}) | median |entry-next open| bp={ok.d_nextopen.median():.1f} | mean signed entry-F0.open bp={ok.sgn_f0.mean():+.1f}')
    # S4 / S5
    s4 = ok[ok.s4.isin(['WIN', 'LOSS', 'EOD'])].copy(); s4['R5'] = s4.R4 - s4.cost; s4['R5b'] = s4.R4 - 2 * s4.cost
    P(f'  S4 rows {len(s4)} unenterable {(ok.s4=="unenterable").sum()} missing {ok.s4.isna().sum()}; cost/trade R median {s4.cost.median():.3f} mean {s4.cost.mean():.3f}')
    def tiers(x):
        return [('ALL', x), ('STRONG', x[x.strong_signal_b]), ('TRADEABLE-recon', x[x.strong_signal_b & (x.bt <= '10:00')])]
    for per, mask in (('full>=06-17', s4.date >= '2026-06-17'), ('test>=07-01', s4.date >= '2026-07-01')):
        for tn, x in tiers(s4[mask]):
            P(f'  {per} {tn}: n={len(x)} S0 log {x.R.mean():+.3f} CI{cb(x)} | S4 next-open {x.R4.mean():+.3f} CI{cb(x,"R4")} | S5 +5bp/side {x.R5.mean():+.3f} CI{cb(x,"R5")} | +10bp/side {x.R5b.mean():+.3f}')
    # F3-1 gap effect on STRONG
    sg = ok[ok.strong_signal_b].copy(); sg['gapb'] = pd.cut(sg.sgn_f0, [-1e9, -10, 10, 1e9], labels=['<-10bp', '-10..10', '>10bp'])
    for lab, g in sg.groupby('gapb', observed=True):
        gd = g[g.date <= '2026-07-31']; gt = g[g.date >= '2026-08-01']
        P(f'  STRONG entry-F0.open {lab}: disc(06-17..07-31) n={len(gd)} {gd.R.mean():+.3f} CI{cb(gd)} | test(08-01..) n={len(gt)} {gt.R.mean():+.3f} CI{cb(gt)}')
    P('  spearman(signed gap bp, R) STRONG all:', np.round(spearmanr(sg.sgn_f0, sg.R, nan_policy='omit')[0], 3))
    # F3-2 flags
    P('-- F3-2 STRONG components recomputed on F1 vs persisted sc_* (rows with sc_* not NA)')
    for col, vars_ in (('sc_bar_green', ['g']), ('sc_new_hod', ['hod_r', 'hod_a']), ('sc_above_vwap', ['vw_r', 'vw_a']), ('sc_pm_high_hold', ['pmh_c', 'pmh_l', 'pmh_h'])):
        x = ok[~ok[col + '_na']]
        out = []
        for vname in vars_:
            xx = x[x[vname].notna()]
            out.append(f'{vname}: agree={(xx[vname].astype(bool)==xx[col+"_b"]).mean():.3f} (n={len(xx)}, live_true={xx[col+"_b"].mean():.3f}, recon_true={xx[vname].astype(bool).mean():.3f})')
        P(f'  {col}: ' + ' | '.join(out))
    x = ok[~ok.strong_signal_na].copy()
    best = x.g.fillna(False).astype(bool) & x.hod_r.fillna(False).astype(bool) & x.vw_r.fillna(False).astype(bool) & x.pmh_c.fillna(False).astype(bool)
    best2 = x.g.fillna(False).astype(bool) & x.hod_a.fillna(False).astype(bool) & x.vw_a.fillna(False).astype(bool) & x.pmh_c.fillna(False).astype(bool)
    P(f'  STRONG recon (rth variants) vs strong_signal: agree={(best==x.strong_signal_b).mean():.4f} n={len(x)} | (all-session variants) agree={(best2==x.strong_signal_b).mean():.4f} | live STRONG rate {x.strong_signal_b.mean():.3f} recon {best.mean():.3f}')
    P(f'  live STRONG but recon False: {int((x.strong_signal_b & ~best).sum())}; recon True but live False: {int((~x.strong_signal_b & best).sum())}')
    keep = ['date', 'ticker', 'bt', 'entry', 'stop', 'target', 'result', 'R', 'resA', 'RA', 'resB', 'RB', 's4', 'R4', 'cost', 'd_f0open', 'd_f1close', 'sgn_f0', 'in_f0', 'g', 'hod_r', 'vw_r', 'pmh_c']
    pk[[c for c in keep if c in pk]].to_csv(R + '/V_pick_replica.csv', index=False)
except Exception: P(traceback.format_exc()[-1500:])

# ---------------------------------------------------------------- bars1d features (F4-1, F4-2)
def load1(t):
    p = f'{SP}/bars1d/{t}.csv'
    if not os.path.exists(p): return None
    b = pd.read_csv(p, index_col=0)
    if len(b) == 0: return None
    b.index = pd.to_datetime(b.index, utc=True).strftime('%Y-%m-%d')
    b.columns = [c.lower() for c in b.columns]
    return b[~b.index.duplicated()].sort_index()

def feats(b):
    f = pd.DataFrame(index=b.index); pc = b.close.shift(1)
    f['gap_pct'] = b.open / pc - 1
    f['ret_5d'] = pc / b.close.shift(6) - 1
    f['ret_20d'] = pc / b.close.shift(21) - 1
    f['vol_20d'] = np.log(b.close).diff().shift(1).rolling(20).std()
    tr = np.maximum(b.high - b.low, np.maximum((b.high - pc).abs(), (b.low - pc).abs()))
    f['atr14_daily_pct'] = tr.rolling(14).mean().shift(1) / pc
    f['dist_ma20'] = pc / b.close.shift(1).rolling(20).mean() - 1
    return f

CTX = ['gap_pct', 'ret_5d', 'ret_20d', 'vol_20d', 'atr14_daily_pct', 'dist_ma20', 'rs_vs_spy_20d', 'spy_dist_ma20', 'spy_gap', 'spy_ret_5d']
try:
    section('CLAIM F4-1 multi-day context features (recomputed from bars1d, UTC dates)')
    spy1 = load1('SPY'); sf = feats(spy1)
    P('  SPY daily rows', len(spy1), 'last dates', list(spy1.index[-2:]), 'last close', float(spy1.close.iloc[-1]))
    fr = []
    for t in sorted(df.ticker.unique()):
        b = load1(t)
        if b is None: continue
        f = feats(b); f['ticker'] = t; f['date'] = f.index; fr.append(f)
    F = pd.concat(fr, ignore_index=True)
    F = F.merge(sf[['ret_20d', 'dist_ma20', 'gap_pct', 'ret_5d']].rename(columns={'ret_20d': 'spy_ret_20d', 'dist_ma20': 'spy_dist_ma20', 'gap_pct': 'spy_gap', 'ret_5d': 'spy_ret_5d'}), left_on='date', right_index=True, how='left')
    F['rs_vs_spy_20d'] = F.ret_20d - F.spy_ret_20d
    df = df.merge(F[['ticker', 'date'] + CTX], on=['ticker', 'date'], how='left')
    DISC = df.date <= '2026-06-30'; TEST = df.date >= '2026-07-01'
    P(f'  feature coverage: gap_pct notna {df.gap_pct.notna().mean():.3f}; n DISC {DISC.sum()} TEST {TEST.sum()}')
    P('  feature   rho_DISC  rho_TEST  | TEST Q5-Q1 avgR (n/quintile)')
    for c in CTX:
        rd = spearmanr(df.loc[DISC, c], df.loc[DISC, 'R'], nan_policy='omit')[0]
        rt = spearmanr(df.loc[TEST, c], df.loc[TEST, 'R'], nan_policy='omit')[0]
        t = df[TEST & df[c].notna()].copy(); t['q'] = pd.qcut(t[c].rank(method='first'), 5, labels=False)
        qm = t.groupby('q').R.mean(); q5 = t[t.q == 4]; q1 = t[t.q == 0]
        P(f'  {c:17s} {rd:+.3f}    {rt:+.3f}   | {q5.R.mean()-q1.R.mean():+.3f} CI{cdiff(q5,q1)}  quintiles {np.round(qm.values,3).tolist()}')
    # gap_pct top-quintile by month in TEST
    t = df[TEST & df.gap_pct.notna()].copy(); t['q'] = pd.qcut(t.gap_pct.rank(method='first'), 5, labels=False)
    P('  gap_pct TEST top-quintile by month:', t[t.q == 4].groupby(t.date.str[5:7]).R.mean().round(3).to_dict(), '| all:', t.groupby(t.date.str[5:7]).R.mean().round(3).to_dict())
except Exception: P(traceback.format_exc()[-1500:])

try:
    section('CLAIM F4-2 HGB payload vs payload+context (train<=06-30, predict>=07-01)')
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.metrics import roc_auc_score
    pay = [c for c in ['score', 'rvol', 'rsi', 'df_technical_score', 'minutes_from_open', 'stop_pct'] if c in df]
    y = (df.result == 'WIN').astype(int)
    base = df[TEST]; P(f'  payload cols {pay}; base TEST avgR {base.R.mean():+.3f} CI{cb(base)} n={len(base)} hit={(base.result=="WIN").mean():.3f}')
    for name, cols in (('payload', pay), ('payload+ctx', pay + CTX)):
        for seed in (0, 1):
            X = df[cols].astype(float)
            clf = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.03, max_depth=3, min_samples_leaf=200, l2_regularization=1.0, random_state=seed)
            clf.fit(X[DISC], y[DISC]); p = clf.predict_proba(X[TEST])[:, 1]; pdisc = clf.predict_proba(X[DISC])[:, 1]
            t = df[TEST].copy(); t['p'] = p
            top = t[t.p >= np.quantile(p, 0.9)]; bot = t[t.p <= np.quantile(p, 0.2)]; topx = t[t.p >= np.quantile(pdisc, 0.9)]
            P(f'  {name} seed{seed}: AUC_test={roc_auc_score(y[TEST], p):.3f} AUC_train={roc_auc_score(y[DISC], pdisc):.3f} | top-decile(test-quantile) n={len(top)} avgR {top.R.mean():+.3f} CI{cb(top)} hit {(top.result=="WIN").mean():.3f} | ex-ante DISC-thr n={len(topx)} avgR {topx.R.mean() if len(topx) else np.nan:+.3f} CI{cb(topx) if len(topx) else ""} | bottom20% {bot.R.mean():+.3f}')
            if seed == 0:
                t['q5'] = t.p >= np.quantile(p, 0.8)
                P('    top-quintile by month:', t[t.q5].groupby(t.date.str[5:7]).R.mean().round(3).to_dict(), '| top-decile by hb:', top.groupby('hb', observed=True).R.size().to_dict())
except Exception: P(traceback.format_exc()[-1500:])

# ---------------------------------------------------------------- F5 all-bars
L = None
try:
    section('CLAIM F5-1 / F5-2 all-bars base rate (entry=open of decision bar, stop=2xATR14(5m, prev 14 bars incl. pre/post), target=+2.5R, same-bar->LOSS, EOD at last RTH close)')
    out = []
    for (t, d), day in DAY.items():
        hm = day['hm']; m = (hm >= '09:30') & (hm < '16:00')
        if m.sum() < 40: continue
        o, h, l, c, a = (day[k][m] for k in ['open', 'high', 'low', 'close', 'atrp']); hmr = hm[m]; nb = len(o)
        dec = np.arange(1, nb); dec = dec[(hmr[dec] >= '09:35') & (hmr[dec] <= '14:30')]
        E = o[dec]; A = a[dec]; okm = np.isfinite(A) & (A > 0) & np.isfinite(E)
        dec, E, A = dec[okm], E[okm], A[okm]
        if len(dec) == 0: continue
        stops = E - 2 * A; targ = E + 5 * A
        valid = np.arange(nb)[None, :] >= dec[:, None]
        hs = (l[None, :] <= stops[:, None]) & valid; ht = (h[None, :] >= targ[:, None]) & valid
        js = np.where(hs.any(1), hs.argmax(1), BIG); jt = np.where(ht.any(1), ht.argmax(1), BIG)
        loss = (js <= jt) & (js < BIG); win = (~loss) & (jt < BIG)
        Rr = np.where(loss, -1.0, np.where(win, 2.5, (c[-1] - E) / (2 * A)))
        res = np.where(loss, 'LOSS', np.where(win, 'WIN', 'EOD'))
        out.append(pd.DataFrame({'ticker': t, 'date': d, 'hm': hmr[dec], 'R': Rr, 'result': res, 'stop_pct': 2 * A / E, 'pnl_pct': Rr * 2 * A / E}))
    L = pd.concat(out, ignore_index=True)
    L['mins'] = L.hm.str[:2].astype(int) * 60 + L.hm.str[3:].astype(int); L['b'] = (L.mins - 575) // 15; L['month'] = L.date.str[5:7]
    pset = set(zip(pk.ticker, pk.date, pk.f0)); L['picked'] = [k in pset for k in zip(L.ticker, L.date, L.hm)]
    P(f'  bars={len(L)} tickers={L.ticker.nunique()} dates={L.date.nunique()} picked={int(L.picked.sum())} {time.time()-t0:.0f}s')
    S('ALL BARS', L); P(f'  mean pnl_pct {L.pnl_pct.mean()*100:+.4f}% ; median stop_pct {L.stop_pct.median()*100:.3f}% ; EOD share {(L.result=="EOD").mean():.4f} E[R|EOD] {L.loc[L.result=="EOD","R"].mean():+.3f}')
    by = L.groupby('hm').R.agg(['mean', 'size']); hb = L.groupby('hm').apply(lambda g: (g.result == 'WIN').mean())
    P(f'  per-decision-time avgR: min {by["mean"].min():+.3f} max {by["mean"].max():+.3f} negative {(by["mean"]<0).sum()}/{len(by)} | 09:35 {by.loc["09:35","mean"]:+.3f} 12:00 {by.loc["12:00","mean"]:+.3f} 14:20 {by.loc["14:20","mean"]:+.3f} | hit 09:35 {hb["09:35"]:.3f} 12:00 {hb["12:00"]:.3f} 14:30 {hb["14:30"]:.3f} | stop_pct 09:35 {L[L.hm=="09:35"].stop_pct.median()*100:.2f}% 14:30 {L[L.hm=="14:30"].stop_pct.median()*100:.2f}%')
    P('  month avgR:', L.groupby('month').R.mean().round(3).to_dict(), '| n', L.groupby('month').size().to_dict())
    for mo, g in L.groupby('month'): P(f'   month {mo} CI {cb(g)}')
    P('  bucket-hour avgR (b//4):', L.groupby(L.b // 4).R.mean().round(3).to_dict())
    # F5-2
    P('-- F5-2 picked vs same-day-same-bucket non-picked')
    S('picked bars', L[L.picked]); S('non-picked bars', L[~L.picked])
    L['hit'] = (L.result == 'WIN').astype(float)
    mnp = L[~L.picked].groupby(['date', 'b'])[['R', 'hit']].mean().rename(columns={'R': 'Rnp', 'hit': 'hnp'})
    X = L[L.picked].join(mnp, on=['date', 'b']); X['ex'] = X.R - X.Rnp; X['exh'] = X.hit - X.hnp
    P(f'  excess avgR {X.ex.mean():+.4f} CI{cb(X,"ex")} | excess hit {X.exh.mean()*100:+.2f}pp CI{tuple(np.round(np.array(cb(X,"exh"))*100,2))} | bucket-mix expectation avgR {X.Rnp.mean():+.4f} hit {X.hnp.mean():.4f}')
    for per, mk in (('disc 06-17..07-31', X.date <= '2026-07-31'), ('test 08-01..09-11', X.date >= '2026-08-01')):
        x = X[mk]; x9 = x[x.hm == '09:35']
        P(f'  {per}: excess {x.ex.mean():+.4f} CI{cb(x,"ex")} n={len(x)} | 09:35 bucket excess {x9.ex.mean():+.4f} CI{cb(x9,"ex")} n={len(x9)} picked avgR {x9.R.mean():+.3f} nonpicked {x9.Rnp.mean():+.3f}')
    x9 = X[X.hm == '09:35']; P(f'  09:35 all: picked n={len(x9)} hit {x9.hit.mean():.3f} avgR {x9.R.mean():+.3f} vs same-day nonpicked hit {x9.hnp.mean():.3f} avgR {x9.Rnp.mean():+.3f}')
    # within-hour excess
    P('  excess by hour bucket:', X.groupby(X.b // 4).ex.mean().round(3).to_dict())
    # label vs log for picked
    J = X.merge(pk[['ticker', 'date', 'f0', 'R', 'strong_signal_b', 'elite_b', 'tradeable_b']].rename(columns={'R': 'Rlog', 'f0': 'hm'}), on=['ticker', 'date', 'hm'], how='left')
    P(f'  picked: label avgR {J.R.mean():+.4f} vs log avgR {J.Rlog.mean():+.4f} (n={J.Rlog.notna().sum()})')
    for tn, mk in (('STRONG', J.strong_signal_b == True), ('TRADEABLE col', J.tradeable_b == True), ('ELITE col', J.elite_b == True)):
        g = J[mk]; P(f'  {tn}: n={len(g)} label avgR {g.R.mean():+.3f} hit {g.hit.mean():.3f} | log avgR {g.Rlog.mean():+.3f} | excess vs same-day-bucket {g.ex.mean():+.3f} CI{cb(g,"ex")}')
    L.drop(columns=['mins']).to_csv(R + '/V_allbars_labels.csv.gz', index=False)
except Exception: P(traceback.format_exc()[-1500:])

# ---------------------------------------------------------------- A5-F3 / A5-F6
try:
    section('CLAIMS A5-F3 (SPY day buckets) and A5-F6 (opening-tape gate)')
    try:
        s = open(R + '/A5_result.json', encoding='utf-8').read()
        for key in ['A5-F3', 'A5-F6']:
            i = s.find(key); P(f'  A5 json [{key}]:', s[i:i + 600].replace('\n', ' ') if i >= 0 else 'not found')
    except Exception as e: P('  A5 json err', e)
    spy1 = load1('SPY'); qqq1 = load1('QQQ')
    spy1['oc'] = spy1.close / spy1.open - 1; spy1['cc'] = spy1.close.pct_change(); spy1['sgap'] = spy1.open / spy1.close.shift(1) - 1
    qqq1['qgap'] = qqq1.open / qqq1.close.shift(1) - 1
    m = df.join(spy1[['oc', 'cc', 'sgap']], on='date').join(qqq1[['qgap']], on='date')
    P(f'  SPY merge coverage {m.oc.notna().mean():.3f}; days up>0.3% (oc) {m[m.oc>0.003].date.nunique()} flat {m[(m.oc<=0.003)&(m.oc>=-0.003)].date.nunique()} down {m[m.oc<-0.003].date.nunique()}')
    for col in ['oc', 'cc']:
        cat = pd.cut(m[col] * 100, [-99, -0.3, 0.3, 99], labels=['down', 'flat', 'up'])
        for lab in ['up', 'flat', 'down']:
            S(f'SPY {col} {lab}', m[cat == lab])
        cu = m[cat == 'up']; P(f'  {col} up: DISC {cu[cu.date<="2026-06-30"].R.mean():+.3f} (n={(cu.date<="2026-06-30").sum()}) TEST {cu[cu.date>="2026-07-01"].R.mean():+.3f} (n={(cu.date>="2026-07-01").sum()}) | naive corr(day avgR, SPY {col}) {np.corrcoef(m.groupby("date").R.mean(), m.groupby("date")[col].first())[0,1]:.3f}')
    # F6 gate from 5-min bars: close of 09:30 bar (price at 09:35) vs prior official close (bars1d, UTC date)
    def p0935(t):
        b = load5(t); return b[b.hm == '09:30'].groupby('d').close.first()
    gate = pd.DataFrame({'s': p0935('SPY'), 'q': p0935('QQQ')}).join(spy1.close.shift(1).rename('sp')).join(qqq1.close.shift(1).rename('qp'))
    gate['both'] = (gate.s > gate.sp) & (gate.q > gate.qp); gate['spy_only'] = gate.s > gate.sp; gate['either'] = (gate.s > gate.sp) | (gate.q > gate.qp)
    m = m.join(gate[['both', 'spy_only', 'either']], on='date')
    tt = m[m.date >= '2026-07-01']
    P(f'  Jul-Sep days with gate data {tt[tt.both.notna()].date.nunique()}; kept days {tt[tt.both==True].date.nunique()}; kept pick share {(tt.both==True).sum()/tt.both.notna().sum():.3f}')
    for gname in ['both', 'spy_only', 'either']:
        k = tt[tt[gname] == True]; dnp = tt[tt[gname] == False]
        P(f'  gate {gname}: kept n={len(k)} avgR {k.R.mean():+.3f} CI{cb(k)} | dropped n={len(dnp)} {dnp.R.mean():+.3f} CI{cb(dnp)} | diff CI {cdiff(k,dnp)} | kept after 0.15R cost {k.R.mean()-0.15:+.3f}')
    k = tt[tt.both == True]; dnp = tt[tt.both == False]
    P('  gate both by month kept:', k.groupby(k.date.str[5:7]).R.mean().round(3).to_dict(), '| dropped:', dnp.groupby(dnp.date.str[5:7]).R.mean().round(3).to_dict())
    P('  gate both by hour bucket kept:', k.groupby('hb', observed=True).R.mean().round(3).to_dict(), '| dropped:', dnp.groupby('hb', observed=True).R.mean().round(3).to_dict())
    P('  kept-day SPY oc mean %+.3f%% vs dropped-day %+.3f%% (gate is beta proxy?)' % (k.groupby('date').oc.first().mean() * 100, dnp.groupby('date').oc.first().mean() * 100))
    # proxy gate with official open vs prior close (available Apr-Sep)
    m['gate_open'] = (m.sgap > 0) & (m.qgap > 0)
    for per, mk in (('Apr-Jun', m.date <= '2026-06-30'), ('Jul-Sep', m.date >= '2026-07-01'), ('Apr-May', m.date <= '2026-05-31'), ('Jun', (m.date >= '2026-06-01') & (m.date <= '2026-06-30'))):
        x = m[mk & m.sgap.notna()]; k2 = x[x.gate_open]; d2 = x[~x.gate_open]
        P(f'  proxy gate (open>prev close both) {per}: kept n={len(k2)} {k2.R.mean():+.3f} CI{cb(k2)} dropped n={len(d2)} {d2.R.mean():+.3f} | diff CI {cdiff(k2,d2)} | kept share {len(k2)/max(len(x),1):.2f}')
    x = m[(m.date >= '2026-07-01') & m.both.notna()]
    P(f'  agreement 09:35-gate vs open-gate Jul-Sep (by pick): {(x.both==x.gate_open).mean():.3f}')
except Exception: P(traceback.format_exc()[-1500:])
P(f'DONE {time.time()-t0:.0f}s')
