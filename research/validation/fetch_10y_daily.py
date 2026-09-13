"""Fetch 2014-01-01 .. 2024-08-31 daily bars for the US common-stock symbol list -> bars1d_10y/<T>.csv (date index). Resumable. EVENT lines for the monitor."""
import os, time, warnings; warnings.filterwarnings("ignore")
import pandas as pd, yfinance as yf
SP=os.path.dirname(os.path.dirname(os.path.abspath(__file__))); D=os.path.join(SP,"bars1d_10y")
syms=pd.read_csv(os.path.join(SP,"research","us_common_symbols.csv"))["yf"].astype(str).tolist()
have={f[:-4] for f in os.listdir(D)}; todo=[t for t in syms if t not in have]
print(f"EVENT 10y todo {len(todo)} have {len(have)}", flush=True)
saved=0; t0=time.time()
for i in range(0,len(todo),150):
    chunk=todo[i:i+150]
    try: data=yf.download(chunk,start="2014-01-01",end="2024-09-01",interval="1d",group_by="ticker",progress=False,auto_adjust=False,threads=True)
    except Exception as ex: print("chunk err",i,ex,flush=True); time.sleep(10); continue
    for t in chunk:
        try:
            sub=data[t] if len(chunk)>1 else data
            if sub is None or sub.empty: continue
            sub=sub.copy()
            if isinstance(sub.columns,pd.MultiIndex): sub.columns=[c[0] for c in sub.columns]
            sub=sub[["Open","High","Low","Close","Volume"]].dropna(subset=["Close"])
            if len(sub)<250: continue
            sub.index=pd.to_datetime(sub.index,utc=True).date; sub.index.name="date"
            sub.to_csv(os.path.join(D,t+".csv")); saved+=1
        except Exception: pass
    if (i//150)%6==0: print(f"EVENT 10y progress {min(i+150,len(todo))}/{len(todo)} saved={saved} elapsed={int(time.time()-t0)}s",flush=True)
    time.sleep(1.5)
print(f"EVENT 10y DONE saved={saved} files={len(os.listdir(D))}",flush=True)
