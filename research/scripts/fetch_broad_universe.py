"""Stage 1: US common-stock symbol list (nasdaqtrader) -> 2y daily bars for liquid names -> SP/bars1d_all/
   Stage 2: ex-ante gap screen over the last ~65 sessions -> SP/research/inplay_candidates.csv
   Stage 3: 5-min bars (60d, prepost) for the gapper tickers -> SP/bars5m_inplay/
Resumable: skips files that already exist. Prints EVENT lines for the monitor."""
import os, sys, io, time, json, warnings
warnings.filterwarnings("ignore")
import requests, pandas as pd, numpy as np, yfinance as yf
SP = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
D1 = os.path.join(SP, "bars1d_all"); D5 = os.path.join(SP, "bars5m_inplay"); R = os.path.join(SP, "research")
def ev(msg): print("EVENT " + msg, flush=True)

# ---------- Stage 1a: symbols ----------
sym_path = os.path.join(R, "us_common_symbols.csv")
if not os.path.exists(sym_path):
    rows = []
    for url, kind in [("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt", "nasdaq"),
                      ("https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt", "other")]:
        txt = requests.get(url, timeout=60).text
        df = pd.read_csv(io.StringIO(txt), sep="|")
        df = df[~df.iloc[:, 0].astype(str).str.startswith("File Creation")]
        if kind == "nasdaq":
            df = df[(df["ETF"] == "N") & (df["Test Issue"] == "N")]
            df = df.rename(columns={"Symbol": "symbol", "Security Name": "name"}); df["exchange"] = "Q"
        else:
            df = df[(df["ETF"] == "N") & (df["Test Issue"] == "N") & (df["Exchange"].isin(["N", "A"]))]
            df = df.rename(columns={"ACT Symbol": "symbol", "Security Name": "name", "Exchange": "exchange"})
        rows.append(df[["symbol", "name", "exchange"]])
    s = pd.concat(rows, ignore_index=True)
    s["symbol"] = s["symbol"].astype(str)
    bad = s["name"].str.contains(r"Warrant|Unit|Right|Preferred|Depositary|Notes|Debenture|Bond|% |Trust Preferred|Subordinated|ETN|Fund", case=False, regex=True)
    s = s[~bad & ~s["symbol"].str.contains(r"[\$\.\^]", regex=True) & (s["symbol"].str.len() <= 5)]
    s["yf"] = s["symbol"].str.replace("/", "-", regex=False)
    s = s.drop_duplicates("yf")
    s.to_csv(sym_path, index=False)
syms = pd.read_csv(sym_path)["yf"].astype(str).tolist()
ev(f"symbols {len(syms)}")

# ---------- Stage 1b: daily bars ----------
def save_daily(sub, t):
    if sub is None or sub.empty: return False
    sub = sub.copy()
    if isinstance(sub.columns, pd.MultiIndex): sub.columns = [c[0] for c in sub.columns]
    sub = sub[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
    if len(sub) < 120: return False
    dv = (sub["Close"] * sub["Volume"]).tail(60).median()
    if sub["Close"].iloc[-1] < 3 or dv < 3e6: return False   # liquidity floor: $3M median dollar volume
    idx = pd.to_datetime(sub.index, utc=True)
    sub.index = idx.date; sub.index.name = "date"
    sub.to_csv(os.path.join(D1, t + ".csv")); return True
have = {f[:-4] for f in os.listdir(D1)}
todo = [t for t in syms if t not in have]
ev(f"daily todo {len(todo)} (have {len(have)})")
saved = 0; t0 = time.time()
for i in range(0, len(todo), 150):
    chunk = todo[i:i+150]
    try:
        data = yf.download(chunk, period="2y", interval="1d", group_by="ticker", progress=False, auto_adjust=False, threads=True)
    except Exception as ex:
        print("chunk err", i, ex, flush=True); time.sleep(10); continue
    for t in chunk:
        try:
            sub = data[t] if len(chunk) > 1 else data
            if save_daily(sub, t): saved += 1
        except Exception: pass
    if (i // 150) % 8 == 0: ev(f"daily progress {min(i+150,len(todo))}/{len(todo)} saved={saved} elapsed={int(time.time()-t0)}s")
    time.sleep(1.5)
ev(f"daily done saved={saved} total_files={len(os.listdir(D1))}")

# ---------- Stage 2: ex-ante gap screen ----------
files = [f for f in os.listdir(D1) if f.endswith(".csv")]
recs = []
for f in files:
    t = f[:-4]
    try:
        d = pd.read_csv(os.path.join(D1, f), index_col=0, parse_dates=True)
    except Exception: continue
    if len(d) < 60: continue
    d["prev_close"] = d["Close"].shift(1)
    d["adv20_dollar"] = (d["Close"] * d["Volume"]).rolling(20).mean().shift(1)
    d["gap_pct"] = (d["Open"] / d["prev_close"] - 1) * 100
    d["ret_20d_prev"] = (d["prev_close"] / d["Close"].shift(21) - 1) * 100
    d["vol_ratio_day"] = d["Volume"] / d["Volume"].rolling(20).mean().shift(1)   # NOT ex-ante (whole day); for reference only
    d["range_pct_day"] = (d["High"] - d["Low"]) / d["Open"] * 100
    d["oc_ret_pct"] = (d["Close"] / d["Open"] - 1) * 100
    tail = d.tail(70)
    m = (tail["gap_pct"].abs() >= 3.0) & (tail["adv20_dollar"] >= 2e7) & (tail["prev_close"] >= 5)
    for dt, r in tail[m].iterrows():
        recs.append(dict(date=str(dt.date()), ticker=t, gap_pct=round(r.gap_pct, 2), prev_close=r.prev_close, open=r.Open,
                         adv20_dollar=r.adv20_dollar, ret_20d_prev=round(r.ret_20d_prev, 2) if pd.notna(r.ret_20d_prev) else None,
                         vol_ratio_day=round(r.vol_ratio_day, 2) if pd.notna(r.vol_ratio_day) else None,
                         range_pct_day=round(r.range_pct_day, 2), oc_ret_pct=round(r.oc_ret_pct, 2)))
ip = pd.DataFrame(recs).sort_values(["date", "gap_pct"], key=lambda s: s if s.name != "gap_pct" else -s.abs())
ip.to_csv(os.path.join(R, "inplay_candidates.csv"), index=False)
ev(f"inplay candidates {len(ip)} rows, {ip['date'].nunique()} days, {ip['ticker'].nunique()} tickers")

# ---------- Stage 3: 5-min bars for gapper tickers (60d window) ----------
recent = ip[ip["date"] >= "2026-06-17"]
# cap per day: top 40 by |gap| among liquid names
top = recent.assign(a=recent["gap_pct"].abs()).sort_values(["date", "a"], ascending=[True, False]).groupby("date").head(40)
tick5 = sorted(set(top["ticker"]))
have5 = {f[:-4] for f in os.listdir(D5)}
todo5 = [t for t in tick5 if t not in have5]
ev(f"5m tickers {len(tick5)} todo {len(todo5)}")
saved5 = 0
for i in range(0, len(todo5), 40):
    chunk = todo5[i:i+40]
    try:
        data = yf.download(chunk, period="60d", interval="5m", prepost=True, group_by="ticker", progress=False, auto_adjust=False, threads=True)
    except Exception as ex:
        print("5m chunk err", i, ex, flush=True); time.sleep(10); continue
    for t in chunk:
        try:
            sub = data[t] if len(chunk) > 1 else data
            if sub is None or sub.empty: continue
            sub = sub.copy()
            if isinstance(sub.columns, pd.MultiIndex): sub.columns = [c[0] for c in sub.columns]
            sub = sub[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
            if len(sub) < 500: continue
            sub.index = pd.to_datetime(sub.index, utc=True).tz_convert("America/New_York"); sub.index.name = "ts"
            sub.to_csv(os.path.join(D5, t + ".csv")); saved5 += 1
        except Exception: pass
    if (i // 40) % 5 == 0: ev(f"5m progress {min(i+40,len(todo5))}/{len(todo5)} saved={saved5}")
    time.sleep(1.5)
ev(f"ALL DONE 5m saved={saved5} files={len(os.listdir(D5))}")
