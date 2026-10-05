"""TN2 inputs: quarterly income statement and balance sheet (with filing dates) for every symbol
in the top-N panel.

-> C:/dev/Trader-v3-data/top_n/fund/{SYMBOL}.csv
   (date, filingDate, revenue, grossProfit, operatingIncome, netIncome, totalAssets, equity)
Needs FMP_API_KEY: `railway run python research/top_n/tn2_fetch.py`. Resumable, ~200 calls/min.
"""
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whale_flow"))
import fetch_delisted as fd  # noqa: E402  (throttled FMP client)
from tn_members import TN  # noqa: E402

OUT = TN / "fund"
INC = ["date", "filingDate", "revenue", "grossProfit", "operatingIncome", "netIncome"]
BAL = ["date", "totalAssets", "totalStockholdersEquity"]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    meta_path = TN / "fund_meta.jsonl"
    done = set()
    if meta_path.exists():
        with open(meta_path) as f:
            done = {json.loads(l)["symbol"] for l in f if l.strip()}
    syms = sorted(p.stem for p in (TN / "fmp").glob("*.csv"))
    todo = [s for s in syms if s not in done]
    print(f"{len(todo):,} symbols to fetch", flush=True)
    lock = threading.Lock()

    def one(s):
        inc = fd.get("income-statement", symbol=s, period="quarter", limit=120) or []
        bal = fd.get("balance-sheet-statement", symbol=s, period="quarter", limit=120) or []
        rec = {"symbol": s, "n_inc": len(inc), "n_bal": len(bal)}
        if inc:
            df = pd.DataFrame(inc)
            df = df[[c for c in INC if c in df.columns]]
            if bal:
                b = pd.DataFrame(bal)
                df = df.merge(b[[c for c in BAL if c in b.columns]], on="date", how="left")
            df.rename(columns={"totalStockholdersEquity": "equity"}).sort_values("date").to_csv(OUT / f"{s}.csv", index=False)
        with lock:
            mf.write(json.dumps(rec) + "\n")
            mf.flush()
        return bool(inc)

    hits = 0
    with open(meta_path, "a") as mf, ThreadPoolExecutor(fd.WORKERS) as ex:
        for i, hit in enumerate(ex.map(one, todo), 1):
            hits += hit
            if i % 100 == 0:
                print(f"{i}/{len(todo)} with data {hits}", flush=True)
    print(f"done: {hits} of {len(todo)} returned statements; {len(fd.FAILED)} calls failed after retries", flush=True)


if __name__ == "__main__":
    main()
