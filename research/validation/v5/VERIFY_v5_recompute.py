import os, glob, json, hashlib, time, zlib, warnings, re
import numpy as np, pandas as pd
warnings.filterwarnings('ignore')
S='C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad'
R=S+'/research'
t0=time.time()
def log(*a): print(f'[{time.time()-t0:6.1f}s]',*a, flush=True)

# ---------- inspection prints (things_tried, trades columns) ----------
tt=open(R+'/things_tried.csv',encoding='utf-8',errors='replace').read().splitlines()
print('things_tried header:', tt[0][:300]); print('things_tried rows:', len(tt)-1)
for i,l in enumerate(tt):
    if re.search(r'v5|judge|2004', l, re.I): print(f'  L{i}: {l[:230]}')
T=pd.read_csv(R+'/EXEC_v5_trades.csv.gz')
print('trades cols:', list(T.columns))
pd.set_option('display.max_rows',200); pd.set_option('display.width',200)
print(T[T.isE1].head(2).T.to_string())
print('SPY.csv in 10y:', os.path.exists(S+'/bars1d_10y/SPY.csv'), 'in all:', os.path.exists(S+'/bars1d_all/SPY.csv'))

h=hashlib.sha256(open(R+'/preregistration_v5.json','rb').read()).hexdigest()
log('prereg sha256', h, '| recorded:', open(R+'/V5_prereg_hash.txt').read().strip().replace('\n',' | ')[:220])
us=pd.read_csv(R+'/us_common_symbols.csv')
col=[c for c in us.columns if c.lower() in ('symbol','ticker','sym')]
symcol=col[0] if col else us.columns[0]
syms=set(us[symcol].astype(str).str.upper()); log('symbols', len(syms), 'col', symcol)

def load_dir(d):
    out={}
    for f in glob.glob(d+'/*.csv'):
        tk=os.path.basename(f)[:-4]
        try: df=pd.read_csv(f, usecols=['date','Open','Close','Volume'])
        except Exception: continue
        df['date']=pd.to_datetime(df['date'], errors='coerce')
        df=df.dropna(subset=['date']).drop_duplicates('date').set_index('date').sort_index()
        out[tk]=df
    return out
P10=load_dir(S+'/bars1d_10y'); log('10y files', len(P10))
PAL=load_dir(S+'/bars1d_all'); log('all files', len(PAL))
wd10=np.concatenate([df.index.weekday.values for df in list(P10.values())[:800]])
wdal=np.concatenate([df.index.weekday.values for df in list(PAL.values())[:800]])
log('weekend rows 10y', int((wd10>=5).sum()), 'all', int((wdal>=5).sum()))
bad=[]; ov=0
for tk in set(P10)&set(PAL):
    a=P10[tk]['Close']; b=PAL[tk]['Close']; idx=a.index.intersection(b.index)
    if len(idx)>=5:
        ov+=1; r=(a.loc[idx]/b.loc[idx]).median()
        if abs(r-1)>0.02: bad.append(tk)
log('overlap tickers', ov, 'scale-mismatch>2%', len(bad), bad[:10])
comb={}
for tk in set(P10)|set(PAL):
    parts=[]
    if tk in P10: parts.append(P10[tk])
    if tk in PAL and tk not in bad: parts.append(PAL[tk])
    df=pd.concat(parts); df=df[~df.index.duplicated(keep='last')].sort_index(); comb[tk]=df
spy=comb.get('SPY')
spyx=pd.read_csv(R+'/EXEC_v5_SPY_2004_2026.csv', parse_dates=['date']).set_index('date').sort_index()
if spy is None:
    spy=spyx; log('SPY: using executor file (no SPY.csv in bars dirs)')
else:
    idx=spy.index.intersection(spyx.index)
    l1=spy['Open'].shift(-1)/spy['Close']-1; l2=spyx['Open'].shift(-1)/spyx['Close']-1
    log('SPY own vs executor: overlap', len(idx), 'max |leg diff| bp', round(float((l1.loc[idx]-l2.loc[idx]).abs().max()*1e4),3))
tick=[t for t in comb if t in syms and t!='SPY']
log('tickers in panel', len(tick), 'dropped (not in symbol list)', len(comb)-len(tick)-(1 if 'SPY' in comb else 0))
O=pd.DataFrame({t: comb[t]['Open'] for t in tick}); C=pd.DataFrame({t: comb[t]['Close'] for t in tick}); V=pd.DataFrame({t: comb[t]['Volume'] for t in tick})
del P10, PAL, comb
pmin,pmax=C.index.min(),C.index.max()
cal=spy.index[(spy.index>=pmin)&(spy.index<=pmax)]
notcal=len(C.index.difference(cal)); log('panel', C.shape, pmin.date(), pmax.date(), 'panel dates not in SPY cal', notcal)
O=O.reindex(cal); C=C.reindex(cal); V=V.reindex(cal); spy=spy.reindex(cal)
Cp=C.shift(1)
adv_d=(C*V).rolling(20,min_periods=20).mean().shift(1)
adv_sh=V.rolling(20,min_periods=20).mean().shift(1)
nrows=C.notna().cumsum().shift(1).fillna(0)
valid=C.notna()&O.notna()&V.notna()&Cp.notna()&(V>0)
elig=valid&(Cp>=5)&(adv_d>=50e6)&(nrows>=250)
del nrows
gap=O/Cp-1; rvol=V/adv_sh; dret=C/Cp-1
E1=elig&(gap>=0.08)&(rvol>=3)&(C>O)
E2=elig&(gap>=0.03)&(rvol>=2)&(C>=O)
M1=elig&(gap<=-0.08)&(rvol>=3)&(C<O)
On=O.shift(-1)
cost=pd.DataFrame(np.where(adv_d.values>=100e6,5e-4,1e-3), index=cal, columns=C.columns)
net=On*(1-cost)/(C*(1+cost))-1
net10=On*(1-1e-3)/(C*(1+1e-3))-1
spy_leg=spy['Open'].shift(-1)/spy['Close']-1
spy_day=spy['Close']/spy['Close'].shift(1)-1
exc=net.sub(spy_leg,axis=0); exc10=net10.sub(spy_leg,axis=0)
log('features done; elig cells', int(elig.values.sum()))
dec=np.ceil(adv_d.where(elig).rank(axis=1,pct=True)*10).clip(1,10)
cand=elig&~E2&(gap.abs()<0.01)&(rvol<1.5)&On.notna()
def stack(mask):
    idx=np.where(mask.values)
    df=pd.DataFrame({'date':cal[idx[0]], 'ticker':C.columns.values[idx[1]]})
    for nm,fr in [('gap',gap),('rvol',rvol),('dret',dret),('adv_d',adv_d),('close_T',C),('open_T',O),('open_T1',On),('cost',cost),('net',net),('exc',exc),('exc10',exc10),('dec',dec)]:
        df[nm]=fr.values[idx]
    df['spy_leg']=spy_leg.values[idx[0]]; df['spy_day']=spy_day.values[idx[0]]
    df['isE1']=E1.values[idx]
    return df.dropna(subset=['open_T1'])
EV=stack(E2); MV=stack(M1)
log('events E2', len(EV), 'E1', int(EV.isE1.sum()), 'M1', len(MV))
netv=net.values; decv=dec.values; candv=cand.values; dpos={d:i for i,d in enumerate(cal)}
def controls(df):
    out=[]; nn=[]
    for d,tk,dc in zip(df.date, df.ticker, df.dec):
        i=dpos[d]; ids=np.where(candv[i]&(decv[i]==dc))[0]
        if len(ids)==0: out.append(np.nan); nn.append(0); continue
        r=np.random.default_rng(zlib.crc32(f'{d.date()}|{tk}'.encode()))
        pick=r.choice(ids,size=min(5,len(ids)),replace=False)
        out.append(float(np.nanmean(netv[i,pick]))); nn.append(len(pick))
    df['c1_net']=out; df['c1_n']=nn; df['exc_c1']=df.net-df.c1_net
controls(EV); controls(MV); log('controls done; C1 coverage', round(EV.c1_net.notna().mean(),3))
# B1 gate (registered): trailing 63 td, events resolved by T-1 (event date j<=i-2); <30 -> 126; else OFF
ds=EV.groupby('date')['exc'].sum().reindex(cal,fill_value=0.0); dc=EV.groupby('date')['exc'].count().reindex(cal,fill_value=0)
def b1gate(k1,k2,shift):
    s1=ds.rolling(k1-shift+1).sum().shift(shift); c1=dc.rolling(k1-shift+1).sum().shift(shift)
    s2=ds.rolling(k2-shift+1).sum().shift(shift); c2=dc.rolling(k2-shift+1).sum().shift(shift)
    m=pd.Series(np.where(c1>=30, s1/c1.replace(0,np.nan), np.where(c2>=30, s2/c2.replace(0,np.nan), np.nan)), index=cal)
    return (m>0)&m.notna(), m
B1,B1m=b1gate(63,126,2)       # j in [i-63, i-2]
B1alt,_=b1gate(63,126,1)      # j in [i-63, i-1] (sensitivity)
ret=dret.where(elig); csd=ret.std(axis=1); m21=csd.rolling(21,min_periods=21).mean().shift(1); med=m21.rolling(504,min_periods=504).median()
B2=(m21>=med)&med.notna()
log('B1 defined from', B1m.dropna().index.min().date(), 'B2 defined from', med.dropna().index.min().date())
try:
    G=pd.read_csv(R+'/EXEC_v5_daily_gates.csv'); dcol=[c for c in G.columns if 'date' in c.lower()][0]; G[dcol]=pd.to_datetime(G[dcol]); G=G.set_index(dcol)
    log('gates cols', list(G.columns))
    for c in G.columns:
        cl=c.lower()
        try:
            if 'b1' in cl and G[c].dropna().isin([0,1,True,False]).all():
                idx=G.index.intersection(cal); g=G.loc[idx,c].fillna(False).astype(bool).values
                log(f'executor col {c}: agree w/ my B1 {(g==B1.loc[idx].values).mean():.3f}, w/ B1alt {(g==B1alt.loc[idx].values).mean():.3f}; exec ON share {g.mean():.3f} mine {B1.loc[idx].mean():.3f}')
            if 'b2' in cl and G[c].dropna().isin([0,1,True,False]).all():
                idx=G.index.intersection(cal); g=G.loc[idx,c].fillna(False).astype(bool).values
                log(f'executor col {c}: agree w/ my B2 {(g==B2.loc[idx].values).mean():.3f}; exec ON share {g.mean():.3f} mine {B2.loc[idx].mean():.3f}')
        except Exception as e: log('col', c, 'compare failed', e)
except Exception as e: log('gate compare failed', e)
EV['B1']=B1.reindex(EV.date).values; EV['B1alt']=B1alt.reindex(EV.date).values; EV['B2']=B2.reindex(EV.date).values
MV['B1']=B1.reindex(MV.date).values
EV=EV.sort_values(['date','isE1','dret'],ascending=[True,False,False]); EV['rank']=EV.groupby('date').cumcount()+1; EV['filled']=EV['rank']<=5
rng=np.random.default_rng(20260913)
def cstats(d, col='exc', reps=2000):
    d=d.dropna(subset=[col]); n=len(d)
    if n<2: return dict(n=n)
    mean=d[col].mean()
    def boot(keys):
        g=d.groupby(keys)[col].agg(['sum','count']); s=g['sum'].values; c=g['count'].values; k=len(s)
        idx=rng.integers(0,k,size=(reps,k)); bm=s[idx].sum(1)/c[idx].sum(1)
        return np.percentile(bm,[2.5,97.5]), float((bm<=0).mean())
    (dlo,dhi),pdd=boot(d['date']); q=d['date'].dt.to_period('Q'); (qlo,qhi),pq=boot(q)
    wider='Q' if (qhi-qlo)>(dhi-dlo) else 'D'; lo,hi,p=(qlo,qhi,pq) if wider=='Q' else (dlo,dhi,pdd)
    qm=d.groupby(q)[col].mean()
    return dict(n=n, mean=round(mean*100,3), lo=round(lo*100,3), hi=round(hi*100,3), wider=wider, p=round(p,4), dayCI=[round(dlo*100,3),round(dhi*100,3)], qCI=[round(qlo*100,3),round(qhi*100,3)], qpos=round(float((qm>0).mean()),3), nq=int(len(qm)), median=round(d[col].median()*100,3), win=round(float((d[col]>0).mean()),3), days=int(d['date'].nunique()))
def onoff(on, off, reps=2000):
    on=on.dropna(subset=['exc']); off=off.dropna(subset=['exc'])
    if len(on)<2 or len(off)<2: return None
    def agg(d): g=d.groupby('date')['exc'].agg(['sum','count']); return g['sum'].values, g['count'].values
    s1,c1=agg(on); s0,c0=agg(off)
    i1=rng.integers(0,len(s1),size=(reps,len(s1))); i0=rng.integers(0,len(s0),size=(reps,len(s0)))
    diff=s1[i1].sum(1)/c1[i1].sum(1)-s0[i0].sum(1)/c0[i0].sum(1)
    return dict(diff=round((on.exc.mean()-off.exc.mean())*100,3), p=round(float((diff<=0).mean()),4), lo=round(np.percentile(diff,2.5)*100,3), hi=round(np.percentile(diff,97.5)*100,3), n_on=len(on), n_off=len(off))
W={'seen 2014-09..2024-08':('2014-09-01','2024-08-31'),'H1 2014-09..2019-12':('2014-09-01','2019-12-31'),'H2 2020-01..2024-08':('2020-01-01','2024-08-31'),'disc 2024-09..2026-09':('2024-09-01','2026-09-12')}
cells={'V5-1':lambda d:d[d.isE1],'V5-2':lambda d:d,'V5-3':lambda d:d[d.isE1&d.B1],'V5-4':lambda d:d[d.B1],'V5-5':lambda d:d[d.isE1&d.B2]}
offs={'V5-3':lambda d:d[d.isE1&~d.B1],'V5-4':lambda d:d[~d.B1],'V5-5':lambda d:d[d.isE1&~d.B2]}
res=[]
for wn,(a,b) in W.items():
    dw=EV[(EV.date>=a)&(EV.date<=b)]
    for cn,f in cells.items():
        d=f(dw); r=dict(cell=cn, window=wn, **cstats(d)); r['C1']=cstats(d,'exc_c1'); r['flat10']=cstats(d,'exc10'); r['raw_net']=round(d.net.mean()*100,3) if len(d) else None
        if cn in offs:
            r['ONOFF']=onoff(d, offs[cn](dw)); gs=B2 if cn=='V5-5' else B1
            r['on_share_days']=round(float(gs[(gs.index>=a)&(gs.index<=b)].mean()),3)
        if cn in ('V5-1','V5-2'): r['K5filled']=cstats(d[d.filled])
        if cn=='V5-3': r['B1alt']=cstats(dw[dw.isE1&dw.B1alt])
        if cn=='V5-4': r['B1alt']=cstats(dw[dw.B1alt])
        res.append(r)
    m=MV[(MV.date>=a)&(MV.date<=b)]; res.append(dict(cell='M1', window=wn, **cstats(m), C1=cstats(m,'exc_c1')))
seen=[r for r in res if r['window'].startswith('seen') and r['cell'].startswith('V5')]
ps=np.array([r['p'] for r in seen]); order=np.argsort(ps); m=len(ps); adj=np.empty(m); run=0
for k,i in enumerate(order): run=max(run, ps[i]*(m-k)); adj[i]=min(1,run)
for r,a in zip(seen,adj): r['p_holm']=round(float(a),4)
EV['year']=EV.date.dt.year
yr=EV.groupby('year').apply(lambda d: pd.Series({'E1_n':int(d.isE1.sum()),'E1_mean':round(d[d.isE1].exc.mean()*100,3),'E2_n':len(d),'E2_mean':round(d.exc.mean()*100,3),'E1_B1on_n':int((d.isE1&d.B1).sum()),'E1_B1on_mean':round(d[d.isE1&d.B1].exc.mean()*100,3)}))
yr['B1_on_share']=B1.groupby(B1.index.year).mean().round(3); yr['B2_on_share']=B2.groupby(B2.index.year).mean().round(3)
print(yr.to_string())
for r in res:
    C1=r.get('C1',{}); K=r.get('K5filled',{}); BA=r.get('B1alt',{})
    print(f"{r['cell']:5s} | {r['window']:22s} | n={r.get('n')} mean={r.get('mean')} CI=[{r.get('lo')},{r.get('hi')}] {r.get('wider','')} p={r.get('p')} holm={r.get('p_holm','-')} q+={r.get('qpos')} nq={r.get('nq')} | C1 n={C1.get('n')} {C1.get('mean')} [{C1.get('lo')},{C1.get('hi')}] | flat10={r.get('flat10',{}).get('mean')} | K5={K.get('n')}/{K.get('mean')} | ONOFF={r.get('ONOFF')} onshare={r.get('on_share_days')} | B1alt={BA.get('n')}/{BA.get('mean')} [{BA.get('lo')},{BA.get('hi')}]")
json.dump(dict(sha256=h, results=res, yearly=json.loads(yr.to_json(orient='index')), events_E2=len(EV), events_E1=int(EV.isE1.sum()), B1_from=str(B1m.dropna().index.min().date()), B2_from=str(med.dropna().index.min().date()), overlap_mismatch=bad), open(R+'/VERIFY_v5_recompute.json','w'), indent=1, default=str)
EV.to_csv(R+'/VERIFY_v5_events.csv.gz', index=False, compression='gzip')
log('DONE')
