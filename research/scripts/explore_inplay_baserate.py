"""DISCOVERY-WINDOW ONLY (2026-06-17 .. 2026-07-31) exploratory base rate of the scanner's trade structure
(long, entry=bar close, stop=close-2xATR14(5m), target=+2.5R, exit 15:55) on ex-ante gap days from the broad universe.
No rule selection here; just: does the in-play universe have a different base rate than the static universe (-0.066R)?"""
import os, numpy as np, pandas as pd, warnings; warnings.filterwarnings("ignore")
SP=os.path.dirname(os.path.dirname(os.path.abspath(__file__))); D5=os.path.join(SP,"bars5m_inplay")
ip=pd.read_csv(os.path.join(SP,"research","inplay_candidates.csv"))
ip=ip[(ip.date>="2026-06-17")&(ip.date<="2026-07-31")]
have={f[:-4] for f in os.listdir(D5)}; ip=ip[ip.ticker.isin(have)]
print("DISC gap ticker-days with 5m bars:", len(ip), "| up-gaps:", (ip.gap_pct>0).sum(), "| down-gaps:", (ip.gap_pct<0).sum())
def atr14(df):
    h,l,c=df.High.values,df.Low.values,df.Close.values; pc=np.r_[np.nan,c[:-1]]
    tr=np.nanmax(np.c_[h-l,np.abs(h-pc),np.abs(l-pc)],axis=1)
    return pd.Series(tr).ewm(alpha=1/14,adjust=False,min_periods=14).mean().values
rows=[]; cache={}
for t,g in ip.groupby("ticker"):
    df=pd.read_csv(os.path.join(D5,t+".csv"),index_col=0); df.index=pd.to_datetime(df.index,utc=True).tz_convert("America/New_York")
    df=df[["Open","High","Low","Close","Volume"]].astype(float); df["atr"]=atr14(df)
    dates=df.index.date
    for _,r in g.iterrows():
        d=pd.Timestamp(r.date).date(); day=df[dates==d]
        if day.empty: continue
        pm=day[(day.index.hour*60+day.index.minute)<570]; rth=day[((day.index.hour*60+day.index.minute)>=570)&(day.index.hour<16)]
        if len(rth)<70: continue
        pmvol=pm.Volume.sum(); pm_rng=(pm.High.max()/pm.Low.min()-1)*100 if len(pm) else np.nan
        H,L,C,A=rth.High.values,rth.Low.values,rth.Close.values,rth.atr.values
        mins=(rth.index.hour*60+rth.index.minute).values
        for i in range(len(rth)):
            m=mins[i]
            if m<575 or m>660: continue           # decision bars 09:35..11:00
            if not np.isfinite(A[i]) or A[i]<=0: continue
            e=C[i]; s=e-2*A[i]; tg=e+5*A[i]; res="EOD"; ex=C[-2] if len(C)>1 else C[-1]; R=None
            for j in range(i+1,len(rth)):
                if mins[j]>=955: break
                if L[j]<=s: res="LOSS"; ex=s; break
                if H[j]>=tg: res="WIN"; ex=tg; break
            R=(ex-e)/(e-s)
            rows.append(dict(ticker=t,date=r.date,gap=r.gap_pct,adv=r.adv20_dollar,pmvol=pmvol,pm_rng=pm_rng,minute=m,res=res,R=R,stop_pct=(e-s)/e*100,pnl_pct=(ex-e)/e*100,
                             above_open=e>rth.Open.values[0], ret_from_open=(e/rth.Open.values[0]-1)*100))
L=pd.DataFrame(rows); print("decision bars:",len(L),"ticker-days:",L.groupby(["ticker","date"]).ngroups)
def summ(x,name):
    if len(x)<200: print(f"{name:42s} n={len(x):6d} (too few)"); return
    days=x.date.nunique(); dm=x.groupby("date").R.mean(); 
    b=[np.random.default_rng(k).choice(dm.values,len(dm)).mean() for k in range(500)]
    print(f"{name:42s} n={len(x):6d} days={days:3d} avgR={x.R.mean():+.3f} [{np.percentile(b,2.5):+.3f},{np.percentile(b,97.5):+.3f}] hit={(x.res=='WIN').mean():.3f} stop={(x.res=='LOSS').mean():.3f} stop%={x.stop_pct.median():.2f} pnl%={x.pnl_pct.mean():+.3f}")
summ(L,"ALL gap>=3% ticker-days")
summ(L[L.gap>0],"gap UP")
summ(L[L.gap<0],"gap DOWN")
summ(L[L.gap>=5],"gap >= +5%")
summ(L[L.gap>=8],"gap >= +8%")
summ(L[(L.gap>0)&(L.pmvol>=5e5)],"gap up & pm vol >= 500k sh")
summ(L[(L.gap>0)&(L.above_open)],"gap up & price above RTH open at decision")
summ(L[(L.gap>0)&(L.ret_from_open>0.5)],"gap up & >+0.5% from open (holding gap)")
summ(L[(L.gap>0)&(L.minute<=600)],"gap up, decisions 09:35-10:00")
summ(L[(L.gap>0)&(L.minute>600)],"gap up, decisions 10:05-11:00")
summ(L[(L.gap>0)&(L.adv<1e8)],"gap up & ADV < $100M (smaller names)")
summ(L[(L.gap>0)&(L.adv>=1e8)],"gap up & ADV >= $100M")
L.to_csv(os.path.join(SP,"research","explore_inplay_disc_labels.csv.gz"),index=False,compression="gzip")
