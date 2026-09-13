"""EXPLORATORY (2y daily, broad universe): for 'momentum-building' signal days, decompose follow-through into
overnight (close T -> open T+1), next-day intraday (open T+1 -> close T+1), 2nd overnight, and 5-day close-to-close.
Universe filters through T-1: prev_close>=5, ADV20$>=50M. Excess = minus SPY on the identical leg. Gross, then note 10 bp round trip."""
import os, glob, numpy as np, pandas as pd, warnings; warnings.filterwarnings("ignore")
SP=os.path.dirname(os.path.dirname(os.path.abspath(__file__))); D1=os.path.join(SP,"bars1d_all")
spy=pd.read_csv(os.path.join(SP,"bars1d","SPY.csv"),index_col=0); spy.index=pd.to_datetime(spy.index,utc=True).date; spy=spy[~spy.index.duplicated()].astype(float)
spy=spy.rename(columns=str.title)[["Open","Close"]]
def legs(df):
    o,c=df.Open,df.Close
    return pd.DataFrame({"on1":o.shift(-1)/c-1,"id1":c.shift(-1)/o.shift(-1)-1,"on2":o.shift(-2)/c.shift(-1)-1,"id2":c.shift(-2)/o.shift(-2)-1,"cc5":c.shift(-5)/c-1,"cc1":c.shift(-1)/c-1},index=df.index)
SL=legs(spy)
rows=[]
for f in glob.glob(os.path.join(D1,"*.csv")):
    d=pd.read_csv(f,index_col=0,parse_dates=True)
    if len(d)<480: continue
    d=d.astype(float); d.index=d.index.date; t=os.path.basename(f)[:-4]
    pc=d.Close.shift(1); adv=(d.Close*d.Volume).rolling(20).mean().shift(1); v20=d.Volume.rolling(20).mean().shift(1)
    base=(pc>=5)&(adv>=5e7)
    ret=d.Close/pc-1; gap=d.Open/pc-1; rng=(d.Close-d.Low)/(d.High-d.Low).replace(0,np.nan); vr=d.Volume/v20
    L=legs(d)
    sig={"S1_strongclose_vol":base&(ret>=0.03)&(rng>=0.8)&(vr>=2),
         "S2_gapup_held":base&(gap>=0.03)&(d.Close>=d.Open)&(vr>=2),
         "S3_bigcatalyst":base&(gap>=0.08)&(vr>=3)&(d.Close>d.Open),
         "S4_up3_lowvol":base&(ret>=0.03)&(vr<1.2),
         "S5_all":base}
    for name,m in sig.items():
        idx=d.index[m.values]
        sub=L.loc[idx].copy(); sub["ret"]=ret.loc[idx]; sub["vr"]=vr.loc[idx]; sub["rule"]=name; sub["ticker"]=t; sub["date"]=idx
        rows.append(sub)
X=pd.concat(rows,ignore_index=True).dropna(subset=["on1","id1"])
X=X.merge(SL.add_prefix("spy_"),left_on="date",right_index=True,how="left")
for leg in ["on1","id1","on2","id2","cc1","cc5"]: X["x_"+leg]=X[leg]-X["spy_"+leg]
X["q"]=pd.to_datetime(X.date).dt.to_period("Q").astype(str)
# S4 cross-sectional top-20 per day by return among vr>=2 (built from S5 rows)
s5=X[X.rule=="S5_all"].copy(); s5=s5[s5.vr>=2].sort_values(["date","ret"],ascending=[True,False]).groupby("date").head(20); s5["rule"]="S6_top20_ret_vol2"
X=pd.concat([X,s5],ignore_index=True)
rng_=np.random.default_rng(0)
def ci(x,g,n=600):
    ug=np.unique(g); gm={k:x[g==k] for k in ug}; ms=[np.concatenate([gm[k] for k in rng_.choice(ug,len(ug))]).mean() for _ in range(n)]
    return np.percentile(ms,2.5),np.percentile(ms,97.5)
print(f"{'rule':20s} {'n':>7s} | {'overnight1':>22s} | {'intraday1':>22s} | {'overnight2':>12s} {'intraday2':>10s} | {'cc1':>8s} {'cc5':>8s} | q+ on1/id1")
for rule,g in X.groupby("rule"):
    o=g.x_on1.values*100; i=g.x_id1.values*100; dts=g.date.astype(str).values
    lo,hi=ci(o,dts); lo2,hi2=ci(i,dts)
    qo=(g.groupby("q").x_on1.mean()>0).sum(); qi=(g.groupby("q").x_id1.mean()>0).sum(); nq=g.q.nunique()
    print(f"{rule:20s} {len(g):7d} | {o.mean():+.3f}% [{lo:+.2f},{hi:+.2f}] | {i.mean():+.3f}% [{lo2:+.2f},{hi2:+.2f}] | {g.x_on2.mean()*100:+.3f}%   {g.x_id2.mean()*100:+.3f}%  | {g.x_cc1.mean()*100:+.3f}% {g.x_cc5.mean()*100:+.3f}% | {qo}/{nq} {qi}/{nq}")
print("\nRAW (not SPY-adjusted) overnight1 / intraday1 means by rule:")
print(X.groupby("rule")[["on1","id1","on2","id2","cc5"]].mean().mul(100).round(3).to_string())
print("\nS1 by year: overnight1 / intraday1 (excess %)"); X["y"]=X.date.astype(str).str[:4]
print(X[X.rule.isin(["S1_strongclose_vol","S3_bigcatalyst","S6_top20_ret_vol2"])].groupby(["rule","y"])[["x_on1","x_id1"]].mean().mul(100).round(3).to_string())
X.to_csv(os.path.join(SP,"research","explore_overnight_decomp.csv.gz"),index=False,compression="gzip")
