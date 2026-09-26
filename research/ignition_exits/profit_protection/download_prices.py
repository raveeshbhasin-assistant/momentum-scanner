"""Download daily OHLCV (2015+) for ignition/universe.txt into <out_dir>/c*.pkl.
Run: python download_prices.py ../../../ignition/universe.txt <out_dir>
Then: IGN_PX=<out_dir> python family_A.py  (engine.py and portfolio_B.py read IGN_PX;
on Windows use a Windows-style path, e.g. from `cygpath -w`)."""
import os, sys, time
import yfinance as yf
out = sys.argv[2]
tk = sorted({t for line in open(sys.argv[1]) if not line.startswith('#') for t in line.split()})
print("universe", len(tk), flush=True)
chunks = [tk[i:i+150] for i in range(0, len(tk), 150)]
for i, ch in enumerate(chunks):
    f = f"{out}/c{i:02d}.pkl"
    if os.path.exists(f): continue
    for attempt in range(3):
        d = yf.download(ch, start="2015-01-01", auto_adjust=True, progress=False, threads=True)
        if len(d): break
        time.sleep(10)
    d[["Open","High","Low","Close","Volume"]].to_pickle(f)
    print("chunk", i, d.shape, "missing", int(d["Close"].isna().all().sum()), flush=True)
print("DONE", flush=True)
