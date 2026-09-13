"""A5_regime: build daily market-context table for the 100 scanner trading days.
NOTE on bars1d timestamps: rows look like '2026-09-10 20:00:00-04:00' but that row IS the 2026-09-11 session
(it is midnight UTC of the trading date rendered in New York time). Verified against bars5m: SPY daily Open on
'2026-09-10 20:00' = 764.72 = the 09:30 5-min bar open on 2026-09-11. So trading_date = UTC date of the timestamp.
"""
import pandas as pd, numpy as np, json, os
SP="C:/Users/ravee/AppData/Local/Temp/claude/C--Users-ravee-OneDrive-Documents-Claude-Projects-Trader-v3/78b21416-ab81-443d-91e4-e5b336ec1c07/scratchpad"
uni=json.load(open(f"{SP}/research/universe.json"))
def load_d(t):
    p=f"{SP}/bars1d/{t}.csv"
    if not os.path.exists(p): return None
    d=pd.read_csv(p,index_col=0)
    if len(d)==0: return None
    d.index=pd.to_datetime(d.index,utc=True).date   # trading date = UTC date
    d.index=pd.to_datetime(d.index)
    d=d[~d.index.duplicated()].sort_index()
    return d
def load_5(t):
    p=f"{SP}/bars5m/{t}.csv"
    if not os.path.exists(p): return None
    m=pd.read_csv(p,index_col=0)
    if len(m)==0: return None
    m.index=pd.to_datetime(m.index,utc=True).tz_convert('America/New_York')
    return m.sort_index()

pk=pd.read_csv(f"{SP}/research/picks_master.csv",low_memory=False)
days=pd.to_datetime(sorted(pk.date.unique()))
print("scanner days",len(days))

# ---------- index / ETF daily features ----------
spy=load_d("SPY"); qqq=load_d("QQQ"); iwm=load_d("IWM"); dia=load_d("DIA"); vix=load_d("_VIX")
# sanity: verify offset against 5m
s5=load_5("SPY"); rth=s5[(s5.index.time>=pd.Timestamp('09:30').time())&(s5.index.time<pd.Timestamp('16:00').time())]
g=rth.groupby(rth.index.normalize().tz_localize(None)).agg(o=('Open','first'),c=('Close','last'))
chk=spy.join(g,how='inner'); print("daily-vs-5m open mismatch (should be ~0):", (chk.Open-chk.o).abs().max().round(3), "close", (chk.Close-chk.c).abs().max().round(3))

M=pd.DataFrame(index=spy.index)
def idx_feats(d,pfx):
    c=d.Close; o=d.Open
    M[f"{pfx}_close"]=c
    M[f"{pfx}_ret_1d"]=c.pct_change()
    M[f"{pfx}_gap"]=o/c.shift(1)-1
    M[f"{pfx}_open_to_close"]=c/o-1
    M[f"{pfx}_ret_5d"]=c.pct_change(5)
    M[f"{pfx}_ret_20d"]=c.pct_change(20)
    M[f"{pfx}_dist_ma20"]=c/c.rolling(20).mean()-1
    M[f"{pfx}_dist_ma50"]=c/c.rolling(50).mean()-1
    M[f"{pfx}_range_pct"]=(d.High-d.Low)/o
    M[f"{pfx}_prev_ret_1d"]=c.pct_change().shift(1)       # ex-ante
    M[f"{pfx}_prev_ret_5d"]=c.pct_change(5).shift(1)      # ex-ante
    M[f"{pfx}_prev_dist_ma20"]=(c/c.rolling(20).mean()-1).shift(1)  # ex-ante
    M[f"{pfx}_prev_dist_ma50"]=(c/c.rolling(50).mean()-1).shift(1)
    M[f"{pfx}_close_pos"]=(c-d.Low)/(d.High-d.Low)          # where close sits in day range
    M[f"{pfx}_prev_close_pos"]=M[f"{pfx}_close_pos"].shift(1)
    M[f"{pfx}_rvol20"]=d.Volume/d.Volume.rolling(20).mean().shift(1)
for d,p in [(spy,'spy'),(qqq,'qqq'),(iwm,'iwm'),(dia,'dia')]: idx_feats(d,p)
M['qqq_minus_spy']=M.qqq_ret_1d-M.spy_ret_1d
M['iwm_minus_spy']=M.iwm_ret_1d-M.spy_ret_1d
M['qqq_minus_spy_oc']=M.qqq_open_to_close-M.spy_open_to_close
M['iwm_minus_spy_oc']=M.iwm_open_to_close-M.spy_open_to_close
M['spy_realvol_10d']=M.spy_ret_1d.rolling(10).std()*np.sqrt(252)
M['spy_prev_realvol_10d']=M.spy_realvol_10d.shift(1)
# VIX
vc=vix.Close.reindex(M.index); vo=vix.Open.reindex(M.index)
M['vix_close']=vc; M['vix_open']=vo
M['vix_chg_1d']=vc.pct_change()
M['vix_ma20']=vc.rolling(20).mean()
M['vix_vs_ma20']=vc/M.vix_ma20-1
M['vix_prev_close']=vc.shift(1)                           # ex-ante
M['vix_prev_vs_ma20']=(vc/vc.rolling(20).mean()-1).shift(1)  # ex-ante
M['vix_open_vs_prev']=vo/vc.shift(1)-1                   # ex-ante at 09:30 (VIX daily open = 09:30 print)
M['vix_open_to_close']=vc/vo-1
M['vix_prev_chg_1d']=vc.pct_change().shift(1)
# sector ETFs
secs=["XLK","XLF","XLE","XLV","XLY","XLP","XLI","XLB","XLU","XLC","XLRE"]
sr={}; so={}
for s in secs:
    d=load_d(s); sr[s]=d.Close.pct_change().reindex(M.index); so[s]=(d.Close/d.Open-1).reindex(M.index)
sr=pd.DataFrame(sr); so=pd.DataFrame(so)
M['sector_disp_1d']=sr.std(axis=1)
M['sector_share_up']=(sr>0).mean(axis=1)
M['sector_disp_oc']=so.std(axis=1)
M['sector_prev_share_up']=M.sector_share_up.shift(1)
M['xlk_minus_spy']=sr.XLK-M.spy_ret_1d
M['smh_ret_1d']=load_d("SMH").Close.pct_change().reindex(M.index)
M['tlt_ret_1d']=load_d("TLT").Close.pct_change().reindex(M.index)
M['arkk_ret_1d']=load_d("ARKK").Close.pct_change().reindex(M.index)
M['arkk_minus_spy']=M.arkk_ret_1d-M.spy_ret_1d
# ---------- breadth across universe ----------
closes={}; opens={}; highs={}
for t in uni['stocks']:
    tt='XYZ' if t=='SQ' else t
    d=load_d(tt)
    if d is None or len(d)<60: continue
    closes[t]=d.Close; opens[t]=d.Open; highs[t]=d.High
C=pd.DataFrame(closes).reindex(M.index); O=pd.DataFrame(opens).reindex(M.index); H=pd.DataFrame(highs).reindex(M.index)
print("breadth universe size", C.shape[1])
R1=C.pct_change()
M['br_up_1d']=(R1>0).mean(axis=1)
M['br_above_ma20']=(C>C.rolling(20).mean()).mean(axis=1)
M['br_above_ma50']=(C>C.rolling(50).mean()).mean(axis=1)
M['br_20d_high']=(C>=C.rolling(20).max()).mean(axis=1)
M['br_ret5_pos']=(C.pct_change(5)>0).mean(axis=1)
M['br_gap_up']=(O>C.shift(1)).mean(axis=1)                  # ex-ante at 09:30
M['br_open_to_close_up']=(C>O).mean(axis=1)
M['br_avg_ret_1d']=R1.mean(axis=1)                          # equal-weight universe return
M['br_avg_oc']=(C/O-1).mean(axis=1)
M['br_avg_gap']=(O/C.shift(1)-1).mean(axis=1)
M['br_disp_1d']=R1.std(axis=1)                              # cross-sectional dispersion
M['br_disp_oc']=(C/O-1).std(axis=1)
M['br_prev_up_1d']=M.br_up_1d.shift(1)                      # ex-ante
M['br_prev_above_ma20']=M.br_above_ma20.shift(1)
M['br_prev_above_ma50']=M.br_above_ma50.shift(1)
M['br_prev_20d_high']=M.br_20d_high.shift(1)
M['br_prev_ret5_pos']=M.br_ret5_pos.shift(1)
M['br_prev_disp_1d']=M.br_disp_1d.shift(1)
M['br_prev_avg_ret_1d']=M.br_avg_ret_1d.shift(1)
# universe 'momentum-friendliness': share of tickers whose high exceeded open by >1% (intraday upside)
M['br_up1pct_intraday']=((H/O-1)>0.01).mean(axis=1)

# ---------- 5-min features (06-17 onward) ----------
def intraday_feats(t,pfx):
    m=load_5(t)
    m=m[m.index.normalize().tz_localize(None).isin(days)]
    out={}
    for day,g in m.groupby(m.index.normalize().tz_localize(None)):
        r=g[(g.index.time>=pd.Timestamp('09:30').time())&(g.index.time<pd.Timestamp('16:00').time())]
        pm=g[g.index.time<pd.Timestamp('09:30').time()]
        if len(r)<70: continue
        o=r.Open.iloc[0]; c=r.Close.iloc[-1]
        def px_at(hhmm):  # price at hhmm = close of the bar ending at hhmm
            end=pd.Timestamp(hhmm).time(); sub=r[r.index.time<end]
            return sub.Close.iloc[-1] if len(sub) else np.nan
        p0935=px_at('09:35'); p1000=px_at('10:00'); p1200=px_at('12:00'); p1030=px_at('10:30')
        tp=(r.High+r.Low+r.Close)/3; vw=(tp*r.Volume).cumsum()/r.Volume.cumsum().replace(0,np.nan)
        vw_at=lambda hhmm: vw[r.index.time<pd.Timestamp(hhmm).time()].iloc[-1]
        f={f'{pfx}_5m_open_to_close':c/o-1, f'{pfx}_open_to_1000':p1000/o-1, f'{pfx}_1000_to_close':c/p1000-1,
           f'{pfx}_open_to_0935':p0935/o-1, f'{pfx}_0935_to_close':c/p0935-1, f'{pfx}_open_to_1030':p1030/o-1,
           f'{pfx}_above_vwap_1000':float(p1000>vw_at('10:00')), f'{pfx}_above_vwap_1200':float(p1200>vw_at('12:00')),
           f'{pfx}_pm_last':pm.Close.iloc[-1] if len(pm) else np.nan, f'{pfx}_pm_range_pct':((pm.High.max()-pm.Low.min())/pm.Close.iloc[-1]) if len(pm) else np.nan,
           f'{pfx}_p0935':p0935, f'{pfx}_p1000':p1000, f'{pfx}_open5':o, f'{pfx}_close5':c,
           f'{pfx}_first_bar_green':float(r.Close.iloc[0]>r.Open.iloc[0]),
           f'{pfx}_or_range_pct':(r.High.iloc[:6].max()-r.Low.iloc[:6].min())/o,
           f'{pfx}_1000_to_1200':p1200/p1000-1}
        out[day]=f
    return pd.DataFrame(out).T
I=intraday_feats('SPY','spy').join(intraday_feats('QQQ','qqq')).join(intraday_feats('IWM','iwm'))
M=M.join(I)
# ex-ante at 09:35 relative to prior close
M['spy_pm_chg']=M.spy_pm_last/M.spy_close.shift(1)-1        # pre-market last (09:25 bar) vs prior close
M['qqq_pm_chg']=M.qqq_pm_last/M.qqq_close.shift(1)-1
M['spy_0935_vs_prev']=M.spy_p0935/M.spy_close.shift(1)-1   # SPY at 09:35 vs prior close
M['qqq_0935_vs_prev']=M.qqq_p0935/M.qqq_close.shift(1)-1
M['spy_1000_vs_prev']=M.spy_p1000/M.spy_close.shift(1)-1
# VIX intraday
v5=load_5('_VIX'); v5=v5[v5.index.normalize().tz_localize(None).isin(days)]
vo={}
for day,g in v5.groupby(v5.index.normalize().tz_localize(None)):
    r=g[(g.index.time>=pd.Timestamp('09:30').time())&(g.index.time<pd.Timestamp('16:00').time())]
    if len(r)<60: continue
    o=r.Open.iloc[0]; c=r.Close.iloc[-1]
    p0935=r[r.index.time<pd.Timestamp('09:35').time()].Close.iloc[-1]
    p1000=r[r.index.time<pd.Timestamp('10:00').time()].Close.iloc[-1]
    p1200=r[r.index.time<pd.Timestamp('12:00').time()].Close.iloc[-1]
    vo[day]={'vix_5m_open':o,'vix_0935':p0935,'vix_1000':p1000,'vix_1200':p1200,'vix_5m_close':c,
             'vix_open_to_1000':p1000/o-1,'vix_open_to_1200':p1200/o-1,'vix_open_to_close5':c/o-1}
M=M.join(pd.DataFrame(vo).T)
M['vix_0935_vs_prev']=M.vix_0935/M.vix_prev_close-1          # ex-ante overnight VIX move at 09:35
M['vix_0935_vs_ma20']=M.vix_0935/M.vix_ma20.shift(1)-1
# ---------- scanner daily aggregates ----------
pk['date']=pd.to_datetime(pk.date)
pk['win_pnl']=(pk.pnl_dollar>0).astype(float)
agg=pk.groupby('date').agg(n_picks=('r_realized','size'),avgR=('r_realized','mean'),totR=('r_realized','sum'),
    avg_pnl_pct=('pnl_pct','mean'),hit_rate=('hit_25R','mean'),stop_rate=('stopped','mean'),wr=('win_pnl','mean'),
    eod_rate=('result',lambda s:(s=='EOD').mean()),avg_stop_pct=('stop_pct','mean'),
    n_first=('appearance',lambda s:(s==1).sum()),n_tickers=('ticker','nunique'))
# early-window subset (09:30-10:00) avgR
e=pk[pk.minutes_from_open<=30].groupby('date').r_realized.agg(['mean','size']); agg['avgR_early']=e['mean']; agg['n_early']=e['size']
out=agg.join(M,how='left')
out.index.name='date'
out['dow']=out.index.day_name()
out['month']=out.index.strftime('%Y-%m')
out['day_idx']=np.arange(len(out))
out['period']=np.where(out.index<=pd.Timestamp('2026-06-30'),'discovery','test')
out.to_csv(f"{SP}/research/A5_market_daily.csv")
print(out.shape); print(out[['n_picks','avgR','spy_open_to_close','spy_gap','vix_close','br_up_1d','br_above_ma20','spy_5m_open_to_close','spy_0935_vs_prev','vix_0935_vs_prev']].describe().T.round(4))
print("missing per col (top):"); print(out.isna().sum().sort_values(ascending=False).head(12))
