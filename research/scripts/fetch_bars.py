"""Build the shared bar cache in the scratchpad.
  bars5m/<TICKER>.csv  : 5-min bars, last 60 days, prepost=True, tz=America/New_York
  bars1d/<TICKER>.csv  : daily bars, 2 years
Universe = scanner universe (config.get_full_universe) + every ticker in the perf log + ETFs/indices.
"""
import os, sys, json, time, warnings
warnings.filterwarnings("ignore")
import pandas as pd, yfinance as yf
SP = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, r"C:\Users\ravee\OneDrive\Documents\Claude\Projects\Trader v3")
import config
perf = json.load(open(os.path.join(SP, "live_data", "performance_log.json")))["entries"]
tickers = set(config.get_full_universe()) | {e["ticker"] for e in perf}
tickers.discard("SQ"); tickers.add("XYZ")
ETFS = ["SPY","QQQ","IWM","DIA","XLK","XLF","XLE","XLV","XLY","XLP","XLI","XLB","XLU","XLC","XLRE","SMH","ARKK","TLT","UUP","GLD","USO","BITO"]
IDX = ["^VIX","^VIX9D","^TNX"]
def save(df, folder, t):
    if df is None or df.empty: return False
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df = df[["Open","High","Low","Close","Volume"]].dropna(subset=["Close"])
    if df.index.tz is None: df.index = df.index.tz_localize("UTC")
    df.index = df.index.tz_convert("America/New_York")
    df.index.name = "ts"
    df.to_csv(os.path.join(SP, folder, t.replace("^","_") + ".csv"))
    return True
def fetch_group(group, interval, period, folder, prepost):
    ok = 0
    for i in range(0, len(group), 40):
        chunk = group[i:i+40]
        for attempt in range(3):
            try:
                data = yf.download(chunk, period=period, interval=interval, prepost=prepost, group_by="ticker",
                                   progress=False, auto_adjust=False, threads=True)
                break
            except Exception as ex:
                print("retry", attempt, ex, flush=True); time.sleep(5); data = None
        if data is None: continue
        for t in chunk:
            try:
                sub = data[t] if len(chunk) > 1 else data
                if save(sub, folder, t): ok += 1
            except Exception as ex:
                print("skip", t, ex, flush=True)
        print(f"{folder} {interval}: {min(i+40,len(group))}/{len(group)} saved={ok}", flush=True)
        time.sleep(1)
    return ok
stocks = sorted(tickers)
print("universe", len(stocks), "stocks +", len(ETFS), "etfs +", len(IDX), "idx", flush=True)
n = fetch_group(stocks + ETFS, "5m", "60d", "bars5m", prepost=True)
print("5m done", n, flush=True)
n = fetch_group(IDX, "5m", "60d", "bars5m", prepost=False)
print("5m idx done", n, flush=True)
n = fetch_group(stocks + ETFS + IDX, "1d", "2y", "bars1d", prepost=False)
print("1d done", n, flush=True)
json.dump({"stocks": stocks, "etfs": ETFS, "idx": IDX}, open(os.path.join(SP, "research", "universe.json"), "w"), indent=1)
print("ALL DONE", flush=True)
