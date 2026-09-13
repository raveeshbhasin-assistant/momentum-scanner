"""DISC-only reconciliation: why did the exploratory base rate (+0.08 gross, entry at signal close, all gap-ups, 09:35-11:00)
differ from the registered benchmark (-0.19 net, next-open entry, top-20 watchlist, 09:35-14:30)? Vary one factor at a time."""
import os, numpy as np, pandas as pd, warnings; warnings.filterwarnings("ignore")
SP=os.path.dirname(os.path.dirname(os.path.abspath(__file__))); R=os.path.join(SP,"research"); D5=os.path.join(SP,"bars5m_inplay")
ip=pd.read_csv(os.path.join(R,"inplay_candidates.csv")); ip=ip[(ip.date>="2026-06-17")&(ip.date<="2026-07-31")&(ip.prev_close>=5)&(ip.adv20_dollar>=2e7)&(ip.gap_pct>=3)]
have={f[:-4] for f in os.listdir(D5)}; ip=ip[ip.ticker.isin(have)].copy()
top20=set(map(tuple, ip.sort_values(["date","gap_pct"],ascending=[True,False]).groupby("date").head(20)[["date","ticker"]].values))
def atr14(df):
    h,l,c=df.High.values,df.Low.values,df.Close.values; pc=np.r_[np.nan,c[:-1]]
    tr=np.nanmax(np.c_[h-l,np.abs(h-pc),np.abs(l-pc)],axis=1); return pd.Series(tr).ewm(alpha=1/14,adjust=False,min_periods=14).mean().values
rows=[]
for t,g in ip.groupby("ticker"):
    df=pd.read_csv(os.path.join(D5,t+".csv"),index_col=0); df.index=pd.to_datetime(df.index,utc=True).tz_convert("America/New_York")
    df=df[["Open","High","Low","Close","Volume"]].astype(float); df["atr"]=atr14(df); dts=df.index.date
    for _,r in g.iterrows():
        d=pd.Timestamp(r.date).date(); day=df[dts==d]; m_all=day.index.hour*60+day.index.minute
        rth=day[(m_all>=570)&(day.index.hour<16)]
        if len(rth)<70: continue
        O,H,L,C,A=(rth[c].values for c in ["Open","High","Low","Close","atr"]); mins=(rth.index.hour*60+rth.index.minute).values
        cost=0.0005 if r.adv20_dollar>=1e8 else 0.0010
        for i in range(len(O)):
            if mins[i]<575 or mins[i]>870 or not np.isfinite(A[i]) or A[i]<=0 or i+1>=len(O): continue
            for entry_mode in ("close","next_open"):
                e=C[i] if entry_mode=="close" else O[i+1]
                s=e-2*A[i]; tg=e+5*A[i]; start=i+1
                if e<=s: continue
                res="EOD"; ex=None
                for k in range(start,len(O)):
                    if mins[k]>955: break
                    if entry_mode=="next_open" and k>start and O[k]<=s: res="LOSS"; ex=O[k]; break
                    if L[k]<=s: res="LOSS"; ex=s; break
                    if H[k]>=tg: res="WIN"; ex=tg; break
                    if mins[k]==955: ex=C[k]; break
                if ex is None: ex=C[min(k,len(O)-1)]
                Rg=(ex-e)/(e-s); Rn=((ex*(1-cost))-(e*(1+cost)))/(e*(1+cost)-s)
                rows.append(dict(date=r.date,ticker=t,top20=(r.date,t) in top20,minute=mins[i],entry=entry_mode,R_gross=Rg,R_net=Rn,slip=(O[i+1]/C[i]-1)*100))
X=pd.DataFrame(rows)
def s(x,name): print(f"{name:70s} n={len(x):6d} R_gross={x.R_gross.mean():+.3f} R_net={x.R_net.mean():+.3f}")
early=X.minute<=660
s(X[(X.entry=="close")&early],"exploratory replica: all gap-ups, close entry, 09:35-11:00")
s(X[(X.entry=="next_open")&early],"  -> next-open entry")
s(X[(X.entry=="close")&early&X.top20],"  -> top-20 watchlist only (close entry)")
s(X[(X.entry=="next_open")&early&X.top20],"  -> top-20 + next-open")
s(X[(X.entry=="next_open")&X.top20],"  -> top-20 + next-open + 09:35-14:30 (registered benchmark)")
s(X[(X.entry=="close")],"all gap-ups, close entry, 09:35-14:30")
print("\nmean next-open slippage vs signal close (%):", round(X[X.entry=="next_open"].slip.mean(),4), "| median", round(X[X.entry=="next_open"].slip.median(),4))
print("by month (top20, next_open, early):"); print(X[(X.entry=="next_open")&early&X.top20].assign(m=lambda d:d.date.str[:7]).groupby("m").R_net.agg(["mean","size"]).round(3))
print("by gap rank bucket (close entry, early): "); X2=X[(X.entry=="close")&early].merge(ip[["date","ticker","gap_pct"]],on=["date","ticker"]); X2["gb"]=pd.cut(X2.gap_pct,[3,4,5,7,10,100]); print(X2.groupby("gb").R_gross.agg(["mean","size"]).round(3))
