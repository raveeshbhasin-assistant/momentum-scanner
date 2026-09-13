"""EXPLORATORY, DISCOVERY WINDOW ONLY (2026-06-17..07-31): short-side (fade) base rates. Not registered. Equal-per-day weighting via fixed
watchlists; entry at next-bar OPEN; costs per side + borrow proxy; stop above entry; cover at 15:55.
Cells: FS (static 211 universe, 10:00 close >= +1% above RTH open and above VWAP -> short 10:05 open),
       FG (gap-up top-20 watchlist -> short 10:05 open), FP (scanner's own live picks -> short at next bar open).
Structures: a) stop = fill + 2xATR14, target 2.5R; b) stop = fill + 2xATR14, no target."""
import os, numpy as np, pandas as pd, warnings; warnings.filterwarnings("ignore")
SP=os.path.dirname(os.path.dirname(os.path.abspath(__file__))); R=os.path.join(SP,"research")
A,B="2026-06-17","2026-07-31"; rng=np.random.default_rng(0)
def atr14(df):
    h,l,c=df.High.values,df.Low.values,df.Close.values; pc=np.r_[np.nan,c[:-1]]
    tr=np.nanmax(np.c_[h-l,np.abs(h-pc),np.abs(l-pc)],axis=1); return pd.Series(tr).ewm(alpha=1/14,adjust=False,min_periods=14).mean().values
def load(folder,t):
    p=os.path.join(SP,folder,t+".csv")
    if not os.path.exists(p): return None
    df=pd.read_csv(p,index_col=0); df.index=pd.to_datetime(df.index,utc=True).tz_convert("America/New_York")
    df=df[["Open","High","Low","Close","Volume"]].astype(float); df["atr"]=atr14(df); return df
def sim_short(O,H,L,C,mins,j,stop,target,cost,borrow):
    fill=O[j]*(1-cost)
    if fill>=stop or (target is not None and fill<=target): return None
    for k in range(j,len(O)):
        if mins[k]>955: break
        if k>j and O[k]>=stop: ex=O[k]; res="LOSS"; break
        if H[k]>=stop: ex=stop; res="LOSS"; break
        if target is not None and L[k]<=target: ex=target; res="WIN"; break
        if mins[k]==955: ex=C[k]; res="EOD"; break
    else: ex=C[min(len(O)-1,k)]; res="EOD"
    ex_net=ex*(1+cost)*(1+borrow); risk=stop-fill
    return dict(result=res,R=(fill-ex_net)/risk,pnl_pct=(fill/ex_net-1)*100,stop_pct=risk/fill*100)
rows=[]
def day_arrays(df,d):
    day=df[df.index.date==d]; m=day.index.hour*60+day.index.minute; rth=day[(m>=570)&(day.index.hour<16)]
    if len(rth)<70: return None
    O,H,L,C,V,At=(rth[c].values for c in ["Open","High","Low","Close","Volume","atr"]); mins=(rth.index.hour*60+rth.index.minute).values
    tp=(H+L+C)/3; cv=np.cumsum(V); vwap=np.where(cv>0,np.cumsum(tp*V)/np.where(cv>0,cv,1),tp)
    return O,H,L,C,At,mins,vwap
# FS: static universe
import json; uni=json.load(open(os.path.join(R,"universe.json")))["stocks"]
for t in uni:
    df=load("bars5m",t)
    if df is None: continue
    for d in sorted(set(df.index.date)):
        ds=str(d)
        if ds<A or ds>B: continue
        arr=day_arrays(df,d)
        if arr is None: continue
        O,H,L,C,At,mins,vwap=arr; i=np.where(mins==600)[0]
        if not len(i): continue
        i=i[0]
        if not(np.isfinite(At[i]) and At[i]>0): continue
        if C[i]>=O[0]*1.01 and C[i]>vwap[i]:
            j=i+1; fill=O[j]*(1-0.0005)
            for st,tr in (("a",2.5),("b",None)):
                stop=fill+2*At[i]; target=fill-tr*(stop-fill) if tr else None
                s=sim_short(O,H,L,C,mins,j,stop,target,0.0005,0.0002)
                if s: rows.append(dict(cell="FS_static_ext_"+st,date=ds,ticker=t,**s))
# FG: gap-up watchlist
ip=pd.read_csv(os.path.join(R,"inplay_candidates.csv")); ip=ip[(ip.gap_pct>=3)&(ip.prev_close>=5)&(ip.adv20_dollar>=2e7)&(ip.date>=A)&(ip.date<=B)]
ip=ip.sort_values(["date","gap_pct"],ascending=[True,False]).groupby("date").head(20)
for t,g in ip.groupby("ticker"):
    df=load("bars5m_inplay",t)
    if df is None: continue
    for _,r in g.iterrows():
        arr=day_arrays(df,pd.Timestamp(r.date).date())
        if arr is None: continue
        O,H,L,C,At,mins,vwap=arr; i=np.where(mins==600)[0]
        if not len(i): continue
        i=i[0]
        if not(np.isfinite(At[i]) and At[i]>0): continue
        cost=0.0005 if r.adv20_dollar>=1e8 else 0.0010; borrow=0.0005
        j=i+1; fill=O[j]*(1-cost)
        for st,tr in (("a",2.5),("b",None)):
            stop=fill+2*At[i]; target=fill-tr*(stop-fill) if tr else None
            s=sim_short(O,H,L,C,mins,j,stop,target,cost,borrow)
            if s: rows.append(dict(cell="FG_gapup_1005_"+st,date=r.date,ticker=t,**s))
# FP: fade the scanner's own picks
pm=pd.read_csv(os.path.join(R,"picks_master.csv"),low_memory=False); pm=pm[(pm.date>=A)&(pm.date<=B)]
pm["bt_min"]=pm.batch_time.str[:2].astype(int)*60+pm.batch_time.str[3:5].astype(int); pm["ticker"]=pm.ticker.replace({"SQ":"XYZ"})
for t,g in pm.groupby("ticker"):
    df=load("bars5m",t)
    if df is None: continue
    for _,r in g.iterrows():
        arr=day_arrays(df,pd.Timestamp(r.date).date())
        if arr is None: continue
        O,H,L,C,At,mins,vwap=arr; js=np.where(mins>r.bt_min)[0]
        if not len(js): continue
        j=js[0]; i=j-1
        if not(np.isfinite(At[i]) and At[i]>0): continue
        fill=O[j]*(1-0.0005)
        for st,tr in (("a",2.5),("b",None)):
            stop=fill+2*At[i]; target=fill-tr*(stop-fill) if tr else None
            s=sim_short(O,H,L,C,mins,j,stop,target,0.0005,0.0002)
            if s: rows.append(dict(cell="FP_fade_picks_"+st,date=r.date,ticker=t,strong=r.strong_signal,**s))
T=pd.DataFrame(rows); T.to_csv(os.path.join(R,"explore_fade_disc_trades.csv.gz"),index=False,compression="gzip")
def boot(x,dates,n=1000):
    ud=np.unique(dates); gm={d:x[dates==d] for d in ud}; ms=[]
    for _ in range(n):
        pick=rng.choice(ud,len(ud)); ms.append(np.concatenate([gm[d] for d in pick]).mean())
    ms=np.array(ms); return np.percentile(ms,2.5),np.percentile(ms,97.5)
print("EXPLORATORY DISC-ONLY short-side base rates (net of costs + borrow proxy):")
for cell,g in T.groupby("cell"):
    lo,hi=boot(g.R.values,g.date.values)
    print(f"{cell:22s} n={len(g):6d} days={g.date.nunique():3d} picks/day={len(g)/g.date.nunique():6.1f} avgR={g.R.mean():+.3f} [{lo:+.3f},{hi:+.3f}] hit={(g.result=='WIN').mean():.3f} stop={(g.result=='LOSS').mean():.3f} pnl%={g.pnl_pct.mean():+.3f} stop%={g.stop_pct.median():.2f}")
g=T[(T.cell=="FP_fade_picks_b")]
if "strong" in g: print("FP_b by STRONG:", g.groupby(g.strong.astype(str)).R.agg(['mean','size']).round(3).to_dict())
print("by month:"); print(T.assign(m=T.date.str[:7]).groupby(["cell","m"]).R.agg(['mean','size']).round(3).unstack('m').to_string())
