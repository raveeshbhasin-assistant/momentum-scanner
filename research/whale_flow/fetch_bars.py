"""One consistent daily panel for the v6 Whale Flow study: 2004-01-01 -> today, every symbol
in us_common_symbols.csv (2026 list -> survivorship: delisted names absent).

Why a new panel: the three older panels (bars1d_2004_2013 / _10y / _all) cover different
ticker sets (about 1,000 names in the 2024-26 panel, incl. JPM and XOM, are missing from the
2014-24 panel) and carry no split or dividend columns. Raw Form 4 prices and FINRA volumes
need the split factors; 63-day holds need dividends.

Per ticker CSV: date, Open, High, Low, Close (split-adjusted), Adj Close (split+dividend),
Volume (split-adjusted), Dividends, Stock Splits.  Resumable.

  python research/whale_flow/fetch_bars.py
"""
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

DATA = Path("C:/dev/Trader-v3-data")
OUT = DATA / "whale_flow" / "bars"
NODATA = DATA / "whale_flow" / "bars_nodata.txt"
START = "2004-01-01"
BATCH = 25
COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends", "Stock Splits"]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    syms = sorted(set(pd.read_csv(DATA / "research" / "us_common_symbols.csv").yf.dropna()) | {"SPY"})
    nodata = set(NODATA.read_text().split()) if NODATA.exists() else set()
    todo = [s for s in syms if not (OUT / f"{s}.csv").exists() and s not in nodata]
    print(f"{len(syms)} symbols, {len(todo)} to fetch", flush=True)
    empty_streak = 0
    for i in range(0, len(todo), BATCH):
        batch = todo[i:i + BATCH]
        try:
            df = yf.download(batch, start=START, auto_adjust=False, actions=True, group_by="ticker",
                             threads=True, progress=False)
        except Exception as e:
            print(f"batch {i} error: {str(e)[:100]}", flush=True)
            time.sleep(30)
            continue
        saved = 0
        for t in batch:
            try:
                x = df[t].dropna(subset=["Close"])
            except KeyError:
                x = pd.DataFrame()
            if len(x) == 0:
                continue  # not marked nodata here: an empty batch usually means throttling
            x = x[[c for c in COLS if c in x.columns]].copy()
            x.index = pd.to_datetime(x.index).tz_localize(None)
            x.index.name = "date"
            x.to_csv(OUT / f"{t}.csv")
            saved += 1
        if saved == 0:
            empty_streak += 1
            print(f"batch {i}: nothing returned (streak {empty_streak})", flush=True)
            if empty_streak >= 5:
                print("5 empty batches in a row - throttled; stop and re-run later", flush=True)
                return
            time.sleep(60)
        else:
            empty_streak = 0
            if saved < len(batch):  # some returned, so the missing ones are real no-data names
                with open(NODATA, "a") as f:
                    f.write("\n".join(t for t in batch if not (OUT / f"{t}.csv").exists()) + "\n")
        if (i // BATCH) % 10 == 0:
            print(f"{i + len(batch)}/{len(todo)}", flush=True)
        time.sleep(1.0)
    print("bars done", flush=True)


if __name__ == "__main__":
    main()
