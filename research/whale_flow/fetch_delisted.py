"""Prices for issuers that are no longer on the 2026 symbol list (delisted, acquired, bankrupt),
to take the survivorship bias out of the event side of the v6 Whale Flow study.

FMP's delisted-company *list* is paywalled on the Starter plan, but per-symbol history for a
delisted ticker is served. The Form 4 data itself names the issuers that matter: every issuer
with an open-market purchase whose CIK has no 2026 ticker. Each (issuer CIK, symbol typed on
the form) pair is fetched over the dates it was used. Symbols get reused by other companies,
so nothing here is trusted until the event build checks Form 4 trade prices against the bars.

Output: bars_delisted/{cik}_{SYMBOL}.csv (date, open, high, low, close, volume, adjClose),
        delisted_meta.jsonl (one line per pair: rows fetched, splits)

Needs FMP_API_KEY (production key), so run through Railway. Resumable.

  railway run python research/whale_flow/fetch_delisted.py
"""
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests

WF = Path("C:/dev/Trader-v3-data/whale_flow")
BASE = "https://financialmodelingprep.com/stable/"
KEY = os.environ.get("FMP_API_KEY", "")
PAUSE = 0.30  # global spacing between calls: at most ~200/min, leaving a third of the 300/min plan limit for production
WORKERS = 4
_gate, _last = threading.Lock(), [0.0]
FAILED = []
MIN_VALUE = 25_000
PAD = pd.Timedelta(days=400)
SYM = re.compile(r"^[A-Z]{1,5}([.\-][A-Z]{1,2})?$")


def _throttle():
    with _gate:
        wait = _last[0] + PAUSE - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.monotonic()


def get(path, **params):
    params["apikey"] = KEY
    for attempt in range(4):
        _throttle()
        try:
            r = requests.get(BASE + path, params=params, timeout=60)
        except requests.RequestException:
            time.sleep(3 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            time.sleep(20)
            continue
        return None  # 402/404: not on this plan or no data
    FAILED.append(path)  # retries exhausted: a transient failure, not "no data"
    return None


def targets():
    t = pd.read_pickle(WF / "insider_trans.pkl")
    p = t[(t.code == "P") & (t.acq_disp == "A") & t.ticker.isna() & t.cik.notna()].copy()
    p["value"] = p.shares * p.price
    p["sym"] = (p.symbol_on_form.fillna("").str.upper().str.strip()
                .str.replace(r"^(NYSE|NASDAQ|AMEX|NYSEMKT)\s*[:\-]\s*", "", regex=True)
                .str.replace(".", "-", regex=False))
    p = p[p.sym.map(lambda s: bool(SYM.match(s))) & ~p.sym.isin(["NONE", "NA", "N-A"])]
    g = p.groupby(["cik", "sym"]).agg(first=("filing_date", "min"), last=("filing_date", "max"),
                                      value=("value", "sum"), n=("value", "size")).reset_index()
    return g[g.value >= MIN_VALUE].sort_values("value", ascending=False)


def main():
    out = WF / "bars_delisted"
    out.mkdir(exist_ok=True)
    tg = targets()
    meta_path = WF / "delisted_meta.jsonl"
    done = set()
    if meta_path.exists():
        with open(meta_path) as f:
            done = {json.loads(l)["key"] for l in f if l.strip()}
    if "--retry-empty" in sys.argv:  # pairs recorded with no rows are asked once more
        with open(meta_path) as f:
            done = {json.loads(l)["key"] for l in f if l.strip() and json.loads(l)["n"] > 0}
    todo = [r for r in tg.itertuples() if f"{int(r.cik)}_{r.sym}" not in done]
    print(f"{len(tg):,} issuer-symbol pairs without a 2026 ticker, {len(todo):,} to fetch", flush=True)
    out_lock = threading.Lock()

    def one(r):
        key = f"{int(r.cik)}_{r.sym}"
        rng = {"from": max(r.first - PAD, pd.Timestamp("2004-01-01")).strftime("%Y-%m-%d"),
               "to": (r.last + PAD).strftime("%Y-%m-%d")}
        px = get("historical-price-eod/full", symbol=r.sym, **rng)
        rec = {"key": key, "cik": int(r.cik), "symbol": r.sym, "n": 0}
        if px:
            df = pd.DataFrame(px)[["date", "open", "high", "low", "close", "volume"]]
            adj = get("historical-price-eod/dividend-adjusted", symbol=r.sym, **rng)
            if adj:
                df = df.merge(pd.DataFrame(adj)[["date", "adjClose"]], on="date", how="left")
            df.sort_values("date").to_csv(out / f"{key}.csv", index=False)
            rec["n"] = len(df)
            rec["has_adj"] = bool(adj)
            rec["splits"] = [[x["date"], x["numerator"], x["denominator"]]
                             for x in (get("splits", symbol=r.sym) or [])]
        with out_lock:
            mf.write(json.dumps(rec) + "\n")
            mf.flush()
        return rec["n"] > 0

    hits = 0
    with open(meta_path, "a") as mf, ThreadPoolExecutor(WORKERS) as ex:
        for i, hit in enumerate(ex.map(one, todo), 1):
            hits += hit
            if i % 200 == 0:
                print(f"{i}/{len(todo)} with data {hits}", flush=True)
    print(f"delisted bars done: {hits} of {len(todo)} pairs returned prices; "
          f"{len(FAILED)} calls failed after retries", flush=True)


if __name__ == "__main__":
    if not KEY:
        sys.exit("FMP_API_KEY missing - run through `railway run`")
    main()
